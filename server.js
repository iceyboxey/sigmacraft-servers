'use strict';
// Multiplayer server for the 3D sandbox.
// - Serves public/index.html
// - Relays player positions, block edits and time of day over WebSocket
// - Keeps all block edits in memory and (optionally) saves them to world.json
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const WORLD_FILE = process.env.WORLD_FILE || path.join(__dirname, 'world.json');
const INDEX_FILE = path.join(__dirname, 'public', 'index.html');
const MAX_PLAYERS = 20;
const MAX_EDITS = 100000;
const CYCLE = 600;           // seconds: 300 day + 300 night (must match the client)
const Y_MIN = -16, Y_MAX = 31;

// ---------- shared world state ----------
const edits = new Map();     // "x,y,z" -> block id (0 = removed)
let dirty = false;
let timeBase = 0, timeStamp = Date.now();
const curTime = () => (timeBase + (Date.now() - timeStamp) / 1000) % CYCLE;

try {
  const raw = JSON.parse(fs.readFileSync(WORLD_FILE, 'utf8'));
  for (const e of raw.edits || []) edits.set(`${e[0]},${e[1]},${e[2]}`, e[3]);
  console.log(`Loaded ${edits.size} block edits from ${WORLD_FILE}`);
} catch (e) { /* first start: no world file yet */ }

function saveWorld() {
  if (!dirty) return;
  dirty = false;
  const out = [];
  for (const [k, b] of edits) { const [x, y, z] = k.split(',').map(Number); out.push([x, y, z, b]); }
  try {
    fs.writeFileSync(WORLD_FILE + '.tmp', JSON.stringify({ edits: out }));
    fs.renameSync(WORLD_FILE + '.tmp', WORLD_FILE);
  } catch (e) { console.error('Saving world failed:', e.message); }
}
setInterval(saveWorld, 30000);

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
const cleanName = n => (typeof n === 'string' ? n.replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, 16) : '');

function send(p, obj) { if (p.ws.readyState === 1) p.ws.send(JSON.stringify(obj)); }
function broadcast(obj, exceptId) {
  const s = JSON.stringify(obj);
  for (const p of players.values()) if (p.id !== exceptId && p.ws.readyState === 1) p.ws.send(s);
}
const pub = p => ({ id: p.id, name: p.name, x: p.x, y: p.y, z: p.z, yaw: p.yaw });

function handle(p, m) {
  if (m.t === 'hello' && !p.hello) {
    p.name = cleanName(m.name) || p.name;
    p.hello = true;
    send(p, {
      t: 'welcome', id: p.id, c: curTime(),
      players: [...players.values()].map(pub),
      edits: [...edits].map(([k, b]) => { const [x, y, z] = k.split(',').map(Number); return [x, y, z, b]; })
    });
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
    edits.set(key, b); dirty = true;
    broadcast({ t: 'block', x, y, z, b }, p.id);
  } else if (m.t === 'name') {
    const n = cleanName(m.name);
    if (n) { p.name = n; broadcast({ t: 'name', id: p.id, name: n }, p.id); }
  } else if (m.t === 'time') {
    const c = num(m.c, 0, CYCLE - 0.001);
    if (c === null) return;
    timeBase = c; timeStamp = Date.now();
    broadcast({ t: 'time', c }, p.id);
  }
}

wss.on('connection', ws => {
  if (players.size >= MAX_PLAYERS) { ws.close(1013, 'server full'); return; }
  const p = { id: nextId++, ws, name: '', x: 0, y: 20, z: 0, yaw: 0, hello: false, tokens: 40, alive: true };
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

function shutdown() { saveWorld(); process.exit(0); }
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.listen(PORT, () => console.log(`Sandbox server running on port ${PORT}`));
