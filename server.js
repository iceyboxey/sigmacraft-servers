'use strict';
// Multiplayer server for the 3D sandbox.
// - Serves public/index.html
// - Relays player positions, block edits and time of day over WebSocket
// - Keeps EVERY block edit forever (placed or broken) so the world stays one shared map
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
const MAX_PLAYERS = 20;
const MAX_EDITS = 500000;
const CYCLE = 600;           // seconds: 300 day + 300 night (must match the client)
const Y_MIN = -16, Y_MAX = 31;
const LOCAL_SAVE_MS = 10000, GH_SAVE_MS = 60000;

// ---------- shared world state ----------
const edits = new Map();     // "x,y,z" -> block id (0 = broken). Never removed except by admin reset.
let dirtyLocal = false, dirtyGh = false, lastGhPush = 0;
let timeBase = 0, timeStamp = Date.now();
const curTime = () => (timeBase + (Date.now() - timeStamp) / 1000) % CYCLE;

function loadFromText(text) {
  const raw = JSON.parse(text);
  const m = new Map();
  for (const e of raw.edits || []) {
    if (Array.isArray(e) && e.length === 4 && e.every(Number.isInteger)) m.set(`${e[0]},${e[1]},${e[2]}`, e[3]);
  }
  return m;
}
function serialize() {
  const out = [];
  for (const [k, b] of edits) { const [x, y, z] = k.split(',').map(Number); out.push([x, y, z, b]); }
  return JSON.stringify({ edits: out });
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
    const payload = { message: `World save (${edits.size} edits)`, content, branch: GH_BRANCH };
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
      try { await ghPush(serialize()); lastGhPush = Date.now(); console.log(`Saved ${edits.size} edits to GitHub`); }
      catch (e) { dirtyGh = true; lastGhPush = Date.now(); console.error(e.message); }
    }
  } finally { saving = false; }
}
function markDirty() { dirtyLocal = dirtyGh = true; }
setInterval(() => saveAll(false), LOCAL_SAVE_MS);

// ---------- HTTP (static page + health check) ----------
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
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
const wss = new WebSocketServer({ server, maxPayload: 2048 });
const players = new Map();
let nextId = 1;

const num = (v, lo, hi) => (typeof v === 'number' && Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : null);
const int = (v, lo, hi) => (Number.isInteger(v) && v >= lo && v <= hi ? v : null);
const cleanName = n => (typeof n === 'string' ? n.replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16) : '');
function uniqueName(base, selfId) {
  const n = base || 'Player';
  const taken = new Set([...players.values()].filter(q => q.id !== selfId).map(q => q.name.toLowerCase()));
  if (!taken.has(n.toLowerCase())) return n;
  for (let i = 2; ; i++) { const s = String(i), c = n.slice(0, 16 - s.length) + s; if (!taken.has(c.toLowerCase())) return c; }
}
function pwOk(s) {
  if (!ADMIN_PASSWORD || typeof s !== 'string' || s.length > 200) return false;
  const h = x => crypto.createHash('sha256').update(x).digest();
  return crypto.timingSafeEqual(h(s), h(ADMIN_PASSWORD));
}

function send(p, obj) { if (p.ws.readyState === 1) p.ws.send(JSON.stringify(obj)); }
function broadcast(obj, exceptId) {
  const s = JSON.stringify(obj);
  for (const p of players.values()) if (p.id !== exceptId && p.ws.readyState === 1) p.ws.send(s);
}
const pub = p => ({ id: p.id, name: p.name, x: p.x, y: p.y, z: p.z, yaw: p.yaw });

function sendWorld(p) {          // the full saved world, in batches
  let batch = [];
  for (const [k, b] of edits) {
    const [x, y, z] = k.split(',').map(Number);
    batch.push([x, y, z, b]);
    if (batch.length >= 4000) { send(p, { t: 'edits', e: batch }); batch = []; }
  }
  if (batch.length) send(p, { t: 'edits', e: batch });
}

function handle(p, m) {
  if (m.t === 'hello' && !p.hello) {
    p.name = uniqueName(cleanName(m.name) || p.name, p.id);
    p.hello = true;
    send(p, { t: 'welcome', id: p.id, name: p.name, c: curTime(), players: [...players.values()].map(pub), adminEnabled: !!ADMIN_PASSWORD });
    sendWorld(p);
    players.set(p.id, p);
    broadcast({ t: 'join', p: pub(p) }, p.id);
    return;
  }
  if (!p.hello) return;

  if (m.t === 'pos') {
    const x = num(m.x, -30000, 30000), y = num(m.y, -100, 300), z = num(m.z, -30000, 30000), yaw = num(m.yaw, -100, 100);
    if (x === null || y === null || z === null || yaw === null) return;
    p.x = x; p.y = y; p.z = z; p.yaw = yaw;
    broadcast({ t: 'pos', id: p.id, x, y, z, yaw }, p.id);
  } else if (m.t === 'block') {
    if (p.tokens < 1) return;           // rate limit
    p.tokens--;
    const x = int(m.x, -30000, 30000), y = int(m.y, Y_MIN, Y_MAX), z = int(m.z, -30000, 30000), b = int(m.b, 0, 9);
    if (x === null || y === null || z === null || b === null) return;
    const key = `${x},${y},${z}`;
    if (!edits.has(key) && edits.size >= MAX_EDITS) return;
    edits.set(key, b); markDirty();
    broadcast({ t: 'block', x, y, z, b }, p.id);
  } else if (m.t === 'name') {
    const n = uniqueName(cleanName(m.name), p.id);
    if (n) { p.name = n; send(p, { t: 'name', id: p.id, name: n }); broadcast({ t: 'name', id: p.id, name: n }, p.id); }
  } else if (m.t === 'time') {
    const c = num(m.c, 0, CYCLE - 0.001);
    if (c === null) return;
    timeBase = c; timeStamp = Date.now();
    broadcast({ t: 'time', c }, p.id);
  } else if (m.t === 'admin') {
    if (p.fails >= 5) return;
    if (pwOk(m.pw)) { p.admin = true; send(p, { t: 'admin', ok: true }); }
    else { p.fails++; send(p, { t: 'admin', ok: false }); }
  } else if (m.t === 'reset') {
    if (!p.admin) return;
    console.log(`World reset by ${p.name} (${edits.size} edits deleted)`);
    edits.clear(); markDirty();
    broadcast({ t: 'reset' });
    saveAll(true);
  }
}

wss.on('connection', ws => {
  if (players.size >= MAX_PLAYERS) { ws.close(1013, 'server full'); return; }
  const p = { id: nextId++, ws, name: '', x: 0, y: 20, z: 0, yaw: 0, hello: false, tokens: 40, alive: true, admin: false, fails: 0 };
  p.name = 'Player' + p.id;
  ws.on('pong', () => { p.alive = true; });
  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (m && typeof m.t === 'string') handle(p, m);
  });
  ws.on('close', () => { if (players.delete(p.id) && p.hello) broadcast({ t: 'leave', id: p.id }); });
  ws.on('error', () => {});
});

setInterval(() => { for (const p of players.values()) p.tokens = Math.min(40, p.tokens + 40); }, 1000);
setInterval(() => broadcast({ t: 'time', c: curTime() }), 20000);
// keep connections alive behind proxies and drop dead ones
setInterval(() => {
  for (const ws of wss.clients) ws.ping();
  for (const p of [...players.values()]) { if (!p.alive) p.ws.terminate(); else p.alive = false; }
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
        if (text) for (const [k, v] of loadFromText(text)) edits.set(k, v);
        loaded = true;
        console.log(`Loaded ${edits.size} block edits from GitHub (${GH_REPO})`);
      } catch (e) {
        console.error(`GitHub load attempt ${i} failed: ${e.message}`);
        await new Promise(r => setTimeout(r, 3000));
      }
    }
    // Never start with an empty world if the backup could not be read: we would overwrite it.
    if (!loaded) { console.error('Could not read the GitHub backup, exiting so the world is not overwritten.'); process.exit(1); }
  }
  if (edits.size === 0) {
    try {
      for (const [k, v] of loadFromText(fs.readFileSync(WORLD_FILE, 'utf8'))) edits.set(k, v);
      console.log(`Loaded ${edits.size} block edits from ${WORLD_FILE}`);
      if (GH_ON && edits.size) dirtyGh = true;
    } catch (e) { /* first start */ }
  }
  console.log(GH_ON ? 'GitHub backup: ON' : 'GitHub backup: OFF (set WORLD_GITHUB_REPO + WORLD_GITHUB_TOKEN)');
  console.log(ADMIN_PASSWORD ? 'Admin reset: ON' : 'Admin reset: OFF (set ADMIN_PASSWORD)');
  server.listen(PORT, () => console.log(`Sandbox server running on port ${PORT}`));
})();
