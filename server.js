import http from 'http';
import fs from 'fs';
import path from 'path';
import { randomBytes } from 'crypto';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';

const DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'public');
const PORT = process.env.PORT || 3000;
const MAX_ROOMS = 1000;
const rooms = new Map();
const rid = (n) => randomBytes(n).toString('hex');
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.svg': 'image/svg+xml',
};

const json = (res, code, obj) => {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
};

const sendFile = (res, name) => {
  const file = path.join(DIR, name);
  fs.readFile(file, (err, buf) => {
    if (err) {
      res.writeHead(404);
      return res.end('Not found');
    }
    res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(buf);
  });
};

const readBody = (req) =>
  new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => {
      data += c;
      if (data.length > 10_000) {
        reject(new Error('Body too large'));
        req.destroy();
      }
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });

const isLive = (room) => (room.mode === 'url' ? true : room.live);

function send(ws, msg) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function pushStatus(room) {
  const msg = { type: 'status', live: isLive(room), count: room.viewers.size };
  for (const v of room.viewers.values()) send(v, msg);
  send(room.host, msg);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const p = url.pathname;

  if (req.method === 'POST' && p === '/api/rooms') {
    try {
      if (rooms.size >= MAX_ROOMS) return json(res, 503, { error: 'Server is full, try later' });
      const body = JSON.parse((await readBody(req)) || '{}');
      const title = String(body.title || 'Live coverage').trim().slice(0, 80) || 'Live coverage';
      let streamUrl = String(body.url || '').trim();
      if (streamUrl) {
        let u;
        try {
          u = new URL(streamUrl);
        } catch {
          return json(res, 400, { error: 'Invalid video link' });
        }
        if (!['http:', 'https:'].includes(u.protocol)) return json(res, 400, { error: 'Link must be http(s)' });
        streamUrl = u.toString();
      }
      const room = {
        id: rid(4),
        token: rid(12),
        title,
        mode: streamUrl ? 'url' : 'webrtc',
        url: streamUrl,
        host: null,
        live: false,
        viewers: new Map(),
        created: Date.now(),
      };
      rooms.set(room.id, room);
      return json(res, 201, {
        id: room.id,
        mode: room.mode,
        watchPath: `/w/${room.id}`,
        hostPath: room.mode === 'webrtc' ? `/b/${room.id}?t=${room.token}` : null,
      });
    } catch (e) {
      return json(res, 400, { error: 'Bad request' });
    }
  }

  const m = p.match(/^\/api\/rooms\/([a-f0-9]+)$/);
  if (req.method === 'GET' && m) {
    const room = rooms.get(m[1]);
    if (!room) return json(res, 404, { error: 'Not found' });
    return json(res, 200, { title: room.title, mode: room.mode, url: room.url, live: isLive(room) });
  }

  if (p === '/') return sendFile(res, 'index.html');
  if (p.startsWith('/w/')) return sendFile(res, 'watch.html');
  if (p.startsWith('/b/')) return sendFile(res, 'broadcast.html');
  if (/^\/[\w.-]+$/.test(p)) return sendFile(res, p.slice(1));
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://localhost').searchParams;
  const room = rooms.get(q.get('room'));
  if (!room) return ws.close(4004, 'no room');
  const role = q.get('role');
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  if (role === 'host') {
    if (room.mode !== 'webrtc' || q.get('token') !== room.token) return ws.close(4003, 'forbidden');
    if (room.host) room.host.close(4009, 'replaced');
    room.host = ws;
    send(ws, { type: 'viewers', ids: [...room.viewers.keys()] });
    pushStatus(room);

    ws.on('message', (raw) => {
      let m;
      try {
        m = JSON.parse(raw);
      } catch {
        return;
      }
      if (m.type === 'live') {
        room.live = !!m.on;
        pushStatus(room);
      } else if (m.type === 'signal') {
        send(room.viewers.get(m.to), { type: 'signal', data: m.data });
      }
    });
    ws.on('close', () => {
      if (room.host === ws) {
        room.host = null;
        room.live = false;
        pushStatus(room);
      }
    });
  } else {
    const vid = rid(4);
    room.viewers.set(vid, ws);
    send(ws, { type: 'welcome', id: vid });
    send(room.host, { type: 'viewer-joined', id: vid });
    pushStatus(room);

    ws.on('message', (raw) => {
      let m;
      try {
        m = JSON.parse(raw);
      } catch {
        return;
      }
      if (m.type === 'signal') send(room.host, { type: 'signal', from: vid, data: m.data });
    });
    ws.on('close', () => {
      room.viewers.delete(vid);
      send(room.host, { type: 'viewer-left', id: vid });
      pushStatus(room);
    });
  }
});

// Drop dead sockets
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30_000);

// Drop idle rooms after 24h
setInterval(() => {
  const cutoff = Date.now() - 24 * 3600 * 1000;
  for (const [id, r] of rooms) if (!r.host && r.viewers.size === 0 && r.created < cutoff) rooms.delete(id);
}, 10 * 60_000);

server.listen(PORT, '0.0.0.0', () => console.log(`LiveCover running on http://localhost:${PORT}`));
