// قلاع المعرفة — relay server.
// Serves the game page and links players in rooms over WebSocket.
// The server holds no game logic: each player keeps a small "presence" object
// (name, role, answer, and for the host the whole game state), and the server
// shares every player's presence with everyone else in the same room.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PAGE = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')].find(p => fs.existsSync(p)) || path.join(__dirname, 'index.html');
const MAX_PEERS_PER_ROOM = 6;
const MAX_PRESENCE_BYTES = 16 * 1024;
const ROOM_NAME = /^[a-z0-9][a-z0-9_.-]{0,47}$/;

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  if (url !== '/' && url !== '/index.html') { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('Not found'); }
  fs.readFile(PAGE, (err, data) => {
    if (err) { res.writeHead(500); return res.end('Page missing'); }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    res.end(data);
  });
});

// rooms: name -> Map(peerId -> { ws, presence })
const rooms = new Map();

function broadcast(roomName) {
  const room = rooms.get(roomName);
  if (!room) return;
  const list = [...room.entries()].map(([peer, p]) => ({ peer, presence: p.presence }));
  for (const [peer, p] of room) {
    if (p.ws.readyState !== 1) continue;
    p.ws.send(JSON.stringify({
      t: 'peers', room: roomName,
      peers: list.map(x => ({ ...x, isMe: x.peer === peer, sameTab: x.peer === peer, kind: 'viewer' })),
    }));
  }
}

function leaveRoom(ws) {
  if (!ws.room) return;
  const room = rooms.get(ws.room);
  if (room && room.get(ws.peer)?.ws === ws) {
    room.delete(ws.peer);
    if (room.size === 0) rooms.delete(ws.room); else broadcast(ws.room);
  }
  ws.room = null;
}

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

wss.on('connection', ws => {
  ws.peer = crypto.randomBytes(8).toString('hex');
  ws.room = null;
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'join') {
      if (typeof m.room !== 'string' || !ROOM_NAME.test(m.room)) return ws.send(JSON.stringify({ t: 'error', code: 'invalid_argument' }));
      leaveRoom(ws);
      let room = rooms.get(m.room);
      if (!room) { room = new Map(); rooms.set(m.room, room); }
      // A player who reconnects asks for their old id back, so the game still knows them.
      if (typeof m.peer === 'string' && /^[0-9a-f]{16}$/.test(m.peer)) {
        const old = room.get(m.peer);
        if (!old || old.ws.readyState !== 1) { if (old) room.delete(m.peer); ws.peer = m.peer; }
      }
      if (room.size >= MAX_PEERS_PER_ROOM) return ws.send(JSON.stringify({ t: 'error', code: 'limit_reached' }));
      const presence = m.presence && typeof m.presence === 'object' ? m.presence : {};
      room.set(ws.peer, { ws, presence });
      ws.room = m.room;
      ws.send(JSON.stringify({ t: 'joined', room: m.room, peer: ws.peer }));
      broadcast(m.room);
    } else if (m.t === 'presence') {
      if (!ws.room || !m.presence || typeof m.presence !== 'object') return;
      if (JSON.stringify(m.presence).length > MAX_PRESENCE_BYTES) return ws.send(JSON.stringify({ t: 'error', code: 'invalid_argument' }));
      const p = rooms.get(ws.room)?.get(ws.peer);
      if (p) { p.presence = m.presence; broadcast(ws.room); }
    } else if (m.t === 'leave') {
      leaveRoom(ws);
    }
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

// Drop connections that stopped answering, so a closed tab leaves its room.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    try { ws.ping(); } catch {}
  }
}, 25000);

server.listen(PORT, () => console.log('قلاع المعرفة running on port ' + PORT));
