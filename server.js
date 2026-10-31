'use strict';
// Multiplayer server for the 3D sandbox.
// - Serves public/index.html
// - Relays player positions, block edits and time of day over WebSocket
// - One shared world: everyone is auto-joined to the 'main' room (no lobby, no room codes)
// - Keeps EVERY block edit forever (placed or broken) so a room's world stays one shared map
// - Every edit carries a server timestamp. Players keep a copy of the world in their browser and
//   hand over whatever the server is missing when they rejoin (e.g. after a Render restart)
// - Saves to world.json and (optionally) backs it up to a GitHub repo so it survives
//   Render free-plan restarts
// - Admin (password from ADMIN_PASSWORD) can reset the world
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const WORLD_FILE = process.env.WORLD_FILE || path.join(__dirname, 'world.json');
const INDEX_FILE = path.join(__dirname, 'public', 'index.html');
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';
const GH_REPO = process.env.WORLD_GITHUB_REPO || '';        // e.g. "myuser/sandbox-world"
const GH_TOKEN = process.env.WORLD_GITHUB_TOKEN || '';      // fine-grained token, Contents: read & write
const GH_BRANCH = process.env.WORLD_GITHUB_BRANCH || 'main';
const GH_FILE = process.env.WORLD_GITHUB_FILE || 'world.json';
const GH_API = process.env.WORLD_GITHUB_API || 'https://api.github.com';
const GH_ON = !!(GH_REPO && GH_TOKEN);
const MAX_PLAYERS = 20;          // per room
const MAX_EDITS = 500000;        // per room
const MAX_ROOMS = 50, MAX_CONNS = 300;
const CYCLE = 600;           // seconds: 300 day + 300 night (must match the client)
const Y_MIN = -64, Y_MAX = 47;
const LOCAL_SAVE_MS = 10000, GH_SAVE_MS = 60000;

// ---------- rooms / shared world state ----------
// room: { id, name, pw, resetAt (ms time of the last admin reset), edits: Map("x,y,z" -> ts*64 + block id (0 = broken)), players: Map, timeBase, timeStamp }
const rooms = new Map();
// dropped items (the small blocks you get when mining) live on the server so every player sees and can pick up the same ones
const DROP_ITEMS = { has: i => Number.isInteger(i) && i >= 1 && i <= 200 && i !== 15 && i !== 16 && i !== 17 && i !== 25 && !(i >= 11 && i <= 13) }, MAX_DROPS = 300, DROP_LIFE = 300000;
let dropSeq = 0;
const dropsOf = r => r.drops || (r.drops = new Map());
function addDrop(room, item, x, y, z) {
  const d = dropsOf(room);
  if (d.size >= MAX_DROPS) { const old = d.keys().next().value; d.delete(old); broadcast(room, { t: 'gone', did: old }); }
  const did = ++dropSeq; d.set(did, { i: item, x, y, z, t: Date.now() });
  broadcast(room, { t: 'drop', did, i: item, x, y, z });
}
let dirtyLocal = false, dirtyGh = false, lastGhPush = 0;
const sha = x => crypto.createHash('sha256').update(String(x)).digest('hex');
const roomTime = r => (r.timeBase + (Date.now() - r.timeStamp) / 1000) % CYCLE;
function newRoom(id, name, pwHash) {
  const r = { id, name, pw: pwHash || '', resetAt: 0, edits: new Map(), players: new Map(), timeBase: 0, timeStamp: Date.now(), born: Date.now() };
  rooms.set(id, r); return r;
}
function ensureMain() { if (!rooms.has('main')) newRoom('main', 'Main World', ''); }

function loadFromText(text) {
  const raw = JSON.parse(text);
  const out = new Map();
  const readEdits = (list, m) => {
    for (const e of list || []) {
      if (!Array.isArray(e) || (e.length !== 4 && e.length !== 5) || !e.every(Number.isInteger)) continue;
      const ts = e.length === 5 ? e[4] : 0;           // old save files have no timestamps
      if (e[3] < 0 || e[3] > 63 || ts < 0) continue;
      m.set(`${e[0]},${e[1]},${e[2]}`, ts * 64 + e[3]);
    }
  };
  if (Array.isArray(raw.rooms)) {
    for (const x of raw.rooms) {
      if (!x || typeof x.id !== 'string') continue;
      const r = { id: x.id, name: String(x.name || x.id).slice(0, 20), pw: typeof x.pw === 'string' ? x.pw : '', resetAt: Number.isInteger(x.reset) && x.reset > 0 ? x.reset : 0, edits: new Map(), players: new Map(), timeBase: 0, timeStamp: Date.now() };
      readEdits(x.edits, r.edits); out.set(r.id, r);
    }
  } else if (Array.isArray(raw.edits)) {            // old single-world file -> becomes the main room
    const r = { id: 'main', name: 'Main World', pw: '', resetAt: 0, edits: new Map(), players: new Map(), timeBase: 0, timeStamp: Date.now() };
    readEdits(raw.edits, r.edits); out.set('main', r);
  }
  return out;
}
function totalEdits() { let n = 0; for (const r of rooms.values()) n += r.edits.size; return n; }
function serialize() {
  const out = [];
  for (const r of rooms.values()) {
    if (r.id !== 'main' && r.edits.size === 0 && r.players.size === 0) continue;   // do not save empty, unused rooms
    const list = [];
    for (const [k, v] of r.edits) { const [x, y, z] = k.split(',').map(Number); list.push([x, y, z, v % 64, Math.floor(v / 64)]); }
    out.push({ id: r.id, name: r.name, pw: r.pw, reset: r.resetAt, edits: list });
  }
  return JSON.stringify({ rooms: out });
}

// ---------- GitHub backup ----------
const ghUrl = p => `${GH_API}/repos/${GH_REPO}/${p}`;
function ghFetch(url, opts = {}) {
  return fetch(url, Object.assign({}, opts, {
    headers: Object.assign({ Authorization: `Bearer ${GH_TOKEN}`, 'User-Agent': 'sandbox-server', 'X-GitHub-Api-Version': '2022-11-28' }, opts.headers || {})
  }));
}
async function ghLoad() {        // returns text, or null when the file does not exist yet
  const r = await ghFetch(ghUrl(`contents/${GH_FILE}?ref=${encodeURIComponent(GH_BRANCH)}`), { headers: { Accept: 'application/vnd.github.raw+json' } });
  if (r.status === 404) return null;
  if (!r.ok) throw new Error('GitHub load failed: HTTP ' + r.status);
  return await r.text();
}
async function ghSha() {         // sha of the existing file (needed to update it)
  const r = await ghFetch(ghUrl(`contents?ref=${encodeURIComponent(GH_BRANCH)}`), { headers: { Accept: 'application/vnd.github+json' } });
  if (r.status === 404) return null;
  if (!r.ok) throw new Error('GitHub list failed: HTTP ' + r.status);
  const list = await r.json();
  const f = Array.isArray(list) ? list.find(x => x.name === GH_FILE) : null;
  return f ? f.sha : null;
}
async function ghPush(text) {
  const content = Buffer.from(text).toString('base64');
  for (let i = 0; i < 2; i++) {
    const sha = await ghSha();
    const payload = { message: `World save (${totalEdits()} edits, ${rooms.size} rooms)`, content, branch: GH_BRANCH };
    if (sha) payload.sha = sha;
    const r = await ghFetch(ghUrl(`contents/${GH_FILE}`), {
      method: 'PUT', headers: { 'Content-Type': 'application/json', Accept: 'application/vnd.github+json' }, body: JSON.stringify(payload)
    });
    if (r.ok) return;
    if (r.status !== 409 && r.status !== 422) throw new Error('GitHub save failed: HTTP ' + r.status);
  }
  throw new Error('GitHub save failed: conflict');
}

let saving = false;
async function saveAll(force) {
  if (saving) return;
  saving = true;
  try {
    if (dirtyLocal) {
      dirtyLocal = false;
      try {
        fs.writeFileSync(WORLD_FILE + '.tmp', serialize());
        fs.renameSync(WORLD_FILE + '.tmp', WORLD_FILE);
      } catch (e) { dirtyLocal = true; console.error('Local save failed:', e.message); }
    }
    if (GH_ON && dirtyGh && (force || Date.now() - lastGhPush >= GH_SAVE_MS)) {
      dirtyGh = false;
      try { await ghPush(serialize()); lastGhPush = Date.now(); console.log(`Saved ${totalEdits()} edits (${rooms.size} rooms) to GitHub`); }
      catch (e) { dirtyGh = true; lastGhPush = Date.now(); console.error(e.message); }
    }
  } finally { saving = false; }
}
function markDirty() { dirtyLocal = dirtyGh = true; }
setInterval(() => saveAll(false), LOCAL_SAVE_MS);

// ---------- HTTP (static page + health check) ----------
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' }); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    return fs.readFile(INDEX_FILE, (err, data) => {
      if (err) { res.writeHead(500); return res.end('index.html missing'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server, maxPayload: 65536 });
const conns = new Set();
let nextId = 1;

const num = (v, lo, hi) => (typeof v === 'number' && Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : null);
const int = (v, lo, hi) => (Number.isInteger(v) && v >= lo && v <= hi ? v : null);
const clean = (n, max) => (typeof n === 'string' ? n.replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, max) : '');
const cleanName = n => clean(n, 16);
function uniqueName(room, base, selfId) {
  const n = base || 'Player';
  const taken = new Set([...room.players.values()].filter(q => q.id !== selfId).map(q => q.name.toLowerCase()));
  if (!taken.has(n.toLowerCase())) return n;
  for (let i = 2; ; i++) { const s = String(i), c = n.slice(0, 16 - s.length) + s; if (!taken.has(c.toLowerCase())) return c; }
}
function pwOk(s) {
  if (!ADMIN_PASSWORD) return true;            // no password set: anyone using /?addmode is admin
  if (typeof s !== 'string' || s.length > 200) return false;
  return crypto.timingSafeEqual(crypto.createHash('sha256').update(s).digest(), crypto.createHash('sha256').update(ADMIN_PASSWORD).digest());
}

function send(p, obj) { if (p.ws.readyState === 1) p.ws.send(JSON.stringify(obj)); }
function broadcast(room, obj, exceptId) {
  const s = JSON.stringify(obj);
  for (const p of room.players.values()) if (p.id !== exceptId && p.ws.readyState === 1) p.ws.send(s);
}
const pub = p => ({ id: p.id, name: p.name, x: p.x, y: p.y, z: p.z, yaw: p.yaw, pitch: p.pitch });
function roomList() {
  return [...rooms.values()]
    .map(r => ({ id: r.id, name: r.name, n: r.players.size, max: MAX_PLAYERS, lock: !!r.pw }))
    .sort((a, b) => (b.id === 'main') - (a.id === 'main') || b.n - a.n || a.name.localeCompare(b.name));
}
function sendWorld(p, room) {          // the full saved world of the room, in batches
  let batch = [];
  for (const [k, v] of room.edits) {
    const [x, y, z] = k.split(',').map(Number);
    batch.push([x, y, z, v % 64, Math.floor(v / 64)]);
    if (batch.length >= 3000) { send(p, { t: 'edits', e: batch }); batch = []; }
  }
  if (batch.length) send(p, { t: 'edits', e: batch });
  send(p, { t: 'synced' });                 // the client now knows what the server has and can hand over what is missing
}
function dropIfUnused(room) {
  if (room.id !== 'main' && room.players.size === 0 && room.edits.size === 0) rooms.delete(room.id);
}
const newId = () => { for (;;) { const id = crypto.randomBytes(4).toString('hex').slice(0, 6); if (!rooms.has(id)) return id; } };

function handle(p, m) {
  if (m.t === 'hello' && !p.room) {
    ensureMain();
    const room = rooms.get('main');
    if (room.players.size >= MAX_PLAYERS) { send(p, { t: 'err', c: 'full' }); return; }
    p.room = room; p.joinedAt = Date.now();
    p.name = uniqueName(room, cleanName(m.name) || p.name, p.id);
    send(p, { t: 'welcome', id: p.id, name: p.name, room: { id: room.id, name: room.name }, c: roomTime(room), st: Date.now(), r: room.resetAt, players: [...room.players.values()].map(pub), drops: [...dropsOf(room)].map(([id, d]) => [id, d.i, d.x, d.y, d.z]), admin: ADMIN_PASSWORD ? 'pw' : 'open' });
    sendWorld(p, room);
    room.players.set(p.id, p);
    broadcast(room, { t: 'join', p: pub(p) }, p.id);
    return;
  }
  const room = p.room;
  if (!room) return;

  if (m.t === 'pos') {
    const x = num(m.x, -30000, 30000), y = num(m.y, -100, 300), z = num(m.z, -30000, 30000), yaw = num(m.yaw, -100, 100), pitch = num(m.pitch, -2, 2) || 0;
    if (x === null || y === null || z === null || yaw === null) return;
    p.x = x; p.y = y; p.z = z; p.yaw = yaw; p.pitch = pitch;
    const h = Array.isArray(m.h) && m.h.length === 6 && m.h.every(v => typeof v === 'number' && Math.abs(v) < 8) ? m.h.map(v => Math.round(v * 100) / 100) : undefined;   // VR hands, relative to the feet
    broadcast(room, { t: 'pos', id: p.id, x, y, z, yaw, pitch, h }, p.id);
  } else if (m.t === 'block') {
    if (p.tokens < 1) return;           // rate limit
    p.tokens--;
    const x = int(m.x, -30000, 30000), y = int(m.y, Y_MIN, Y_MAX), z = int(m.z, -30000, 30000), b = int(m.b, 0, 63);
    if (x === null || y === null || z === null || b === null) return;
    const key = `${x},${y},${z}`;
    if (!room.edits.has(key) && room.edits.size >= MAX_EDITS) return;
    const ts = Date.now();
    room.edits.set(key, ts * 64 + b); markDirty();
    broadcast(room, { t: 'block', x, y, z, b, ts }, p.id);
    send(p, { t: 'a', x, y, z, ts });        // tell the sender the official timestamp of its edit
    const di = int(m.d, 1, 200), cnt = int(m.n, 1, 4) || 1;
    if (b === 0 && di !== null && DROP_ITEMS.has(di)) for (let k = 0; k < cnt; k++) addDrop(room, di, x, y, z);   // a mined block drops its items for everybody
  } else if (m.t === 'name') {
    const n = uniqueName(room, cleanName(m.name), p.id);
    if (n) { p.name = n; send(p, { t: 'name', id: p.id, name: n }); broadcast(room, { t: 'name', id: p.id, name: n }, p.id); }
  } else if (m.t === 'pick') {          // somebody walks over a dropped item: the first one wins
    if (p.pickTokens < 1) return; p.pickTokens--;
    const did = int(m.did, 1, 1e12), d = did !== null ? dropsOf(room).get(did) : null;
    if (!d || Math.hypot(p.x - d.x, p.z - d.z) > 8 || Math.abs(p.y - d.y) > 10) return;
    dropsOf(room).delete(did);
    send(p, { t: 'got', did, i: d.i });
    broadcast(room, { t: 'gone', did }, p.id);
  } else if (m.t === 'swing') {         // arm swing animation for the others
    if (p.swTokens < 1) return; p.swTokens--;
    broadcast(room, { t: 'swing', id: p.id }, p.id);
  } else if (m.t === 'hit') {           // player vs player: the server checks it and tells the victim
    if (p.hitTokens < 1) return; p.hitTokens--;
    const q = Number.isInteger(m.id) ? room.players.get(m.id) : null;
    if (!q || q === p || Date.now() - q.joinedAt < 3000) return;      // 3 seconds of spawn protection
    const dx = q.x - p.x, dy = q.y - p.y, dz = q.z - p.z;
    if (Math.hypot(dx, dy, dz) > 6.5) return;                         // too far away (reach is 4 blocks, plus lag)
    const h = Math.hypot(dx, dz) || 1;
    send(q, { t: 'hurt', from: p.id, name: p.name, kx: dx / h, kz: dz / h });
    for (const o of room.players.values()) if (o !== p && o !== q) send(o, { t: 'hit', id: q.id });
  } else if (m.t === 'admin') {
    if (p.fails >= 5) return;
    if (pwOk(m.pw)) { p.admin = true; send(p, { t: 'admin', ok: true }); }
    else { p.fails++; send(p, { t: 'admin', ok: false }); }
  } else if (m.t === 'backup') {
    // A player hands over edits the server is missing (for example after the server restarted).
    // Untrusted input: validated, rate limited and capped per connection.
    if (p.bkTokens < 1 || !Array.isArray(m.e) || m.e.length > 400) return;
    p.bkTokens--;
    const now = Date.now();
    if (room.resetAt === 0 && Number.isInteger(m.r) && m.r > 0 && m.r <= now) room.resetAt = m.r;   // the server forgot a reset it had: learn it again
    const accepted = [];
    for (const e of m.e) {
      if (p.bkBudget < 1) break;
      if (!Array.isArray(e) || e.length !== 5 || !e.every(Number.isInteger)) continue;
      const [x, y, z, b, ts] = e;
      if (x < -30000 || x > 30000 || z < -30000 || z > 30000 || y < Y_MIN || y > Y_MAX || b < 0 || b > 63) continue;
      if (ts <= 0 || ts > now + 5000 || ts < room.resetAt) continue;          // no future times, nothing from before the last reset
      p.bkBudget--;
      const key = `${x},${y},${z}`, cur = room.edits.get(key);
      if (cur === undefined) { if (room.edits.size >= MAX_EDITS) continue; }
      else if (ts <= Math.floor(cur / 64) || p.overrides >= 2000) continue;   // the server's copy is newer (or equal): keep it
      else p.overrides++;
      room.edits.set(key, ts * 64 + b);
      accepted.push([x, y, z, b, ts]);
    }
    if (accepted.length) { markDirty(); broadcast(room, { t: 'edits', e: accepted }, p.id); }
  } else if (m.t === 'reset') {
    if (!p.admin) return;
    console.log(`World reset by ${p.name} in room ${room.name} (${room.edits.size} edits deleted)`);
    room.edits.clear(); dropsOf(room).clear(); room.resetAt = Date.now(); markDirty();
    broadcast(room, { t: 'reset', r: room.resetAt });
    saveAll(true);
  }
}

wss.on('connection', ws => {
  if (conns.size >= MAX_CONNS) { ws.close(1013, 'server full'); return; }
  const p = { id: nextId++, ws, name: '', x: 0, y: 20, z: 0, yaw: 0, room: null, tokens: 40, alive: true, admin: false, fails: 0, creates: 0, bkTokens: 20, bkBudget: 100000, overrides: 0, pickTokens: 20, hitTokens: 4, swTokens: 8, joinedAt: 0, pitch: 0 };
  p.name = 'Player' + p.id;
  conns.add(p);
  ws.on('pong', () => { p.alive = true; });
  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (m && typeof m.t === 'string') handle(p, m);
  });
  ws.on('close', () => {
    conns.delete(p);
    const room = p.room;
    if (room && room.players.delete(p.id)) { broadcast(room, { t: 'leave', id: p.id }); dropIfUnused(room); }
  });
  ws.on('error', () => {});
});

// rooms that were created but never used disappear after 2 minutes
setInterval(() => { for (const r of [...rooms.values()]) if (r.id !== 'main' && !r.players.size && !r.edits.size && Date.now() - (r.born || 0) > 120000) rooms.delete(r.id); }, 30000);
setInterval(() => { for (const p of conns) { p.tokens = Math.min(40, p.tokens + 40); p.bkTokens = Math.min(20, p.bkTokens + 20); p.pickTokens = Math.min(20, p.pickTokens + 20); p.hitTokens = Math.min(4, p.hitTokens + 3); p.swTokens = Math.min(8, p.swTokens + 8); } }, 1000);
setInterval(() => { for (const r of rooms.values()) if (r.players.size) broadcast(r, { t: 'time', c: roomTime(r) }); }, 20000);
setInterval(() => { const now = Date.now(); for (const r of rooms.values()) for (const [id, d] of dropsOf(r)) if (now - d.t > DROP_LIFE) { dropsOf(r).delete(id); broadcast(r, { t: 'gone', did: id }); } }, 5000);
// keep connections alive behind proxies and drop dead ones
setInterval(() => {
  for (const p of [...conns]) { if (!p.alive) p.ws.terminate(); else { p.alive = false; try { p.ws.ping(); } catch (e) {} } }
}, 30000);

let stopping = false;
async function shutdown() {
  if (stopping) return; stopping = true;
  const t = setTimeout(() => process.exit(0), 9000);   // Render gives ~30s after SIGTERM
  dirtyLocal = true;
  while (saving) await new Promise(r => setTimeout(r, 50));
  await saveAll(true);
  clearTimeout(t); process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// ---------- startup: load the saved world first ----------
(async () => {
  let loaded = false;
  if (GH_ON) {
    for (let i = 1; i <= 5 && !loaded; i++) {
      try {
        const text = await ghLoad();
        if (text) for (const [k, v] of loadFromText(text)) rooms.set(k, v);
        loaded = true;
        console.log(`Loaded ${rooms.size} rooms, ${totalEdits()} block edits from GitHub (${GH_REPO})`);
      } catch (e) {
        console.error(`GitHub load attempt ${i} failed: ${e.message}`);
        await new Promise(r => setTimeout(r, 3000));
      }
    }
    // Never start with an empty world if the backup could not be read: we would overwrite it.
    if (!loaded) { console.error('Could not read the GitHub backup, exiting so the world is not overwritten.'); process.exit(1); }
  }
  if (totalEdits() === 0) {
    try {
      for (const [k, v] of loadFromText(fs.readFileSync(WORLD_FILE, 'utf8'))) rooms.set(k, v);
      console.log(`Loaded ${rooms.size} rooms, ${totalEdits()} block edits from ${WORLD_FILE}`);
      if (GH_ON && totalEdits()) dirtyGh = true;
    } catch (e) { /* first start */ }
  }
  ensureMain();
  console.log(GH_ON ? 'GitHub backup: ON' : 'GitHub backup: OFF (set WORLD_GITHUB_REPO + WORLD_GITHUB_TOKEN)');
  console.log(ADMIN_PASSWORD ? 'Admin reset: password required' : 'Admin reset: OPEN to anyone using /?addmode (set ADMIN_PASSWORD to protect it)');
  server.listen(PORT, () => console.log(`Sandbox server running on port ${PORT}`));
})();
