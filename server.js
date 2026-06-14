/* ============================================================
   THE TRENCHES — Multiplayer Server
   A tiny authoritative-ish relay for up to 12 players per room.
   Node.js + ws. No database. Rooms live in memory.
   Deploy on Render/Railway/Fly. See SETUP steps in chat.
   ============================================================ */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const TICK = 1000 / 20;          // 20 broadcasts/sec
const MAX_PLAYERS = 12;          // hard cap per room
const ROOM_EMPTY_MS = 60 * 1000; // close empty room after 60s

// ---- public quick-play matchmaking ----
const QUEUE_START = 4;           // start a game once this many are waiting
const QUEUE_COUNTDOWN = 8;       // seconds of "starting soon" once minimum is met
const QUEUE_MAX_WAIT = 90;       // safety: start anyway after this long if 2+ waiting
const QUEUE_MIN_FALLBACK = 2;    // ...with at least this many

// health check so hosts know the server is alive
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('THE TRENCHES server OK\n');
});
const wss = new WebSocketServer({ server });

/* ---------- rooms ---------- */
const rooms = new Map(); // code -> { code, mode, players:Map, hostId, createdAt, lastSeen }

/* ---------- quick-play queues (one per mode) ---------- */
const queues = { gun: [], tdm: [] }; // mode -> [ {ws, info} ] waiting players
const queueState = { gun: { t: 0, counting: false }, tdm: { t: 0, counting: false } };

function queueBroadcast(mode) {
  const q = queues[mode]; const n = q.length;
  const st = queueState[mode];
  const msg = JSON.stringify({
    t: 'queue', mode, count: n, need: QUEUE_START,
    counting: st.counting, secs: st.counting ? Math.ceil(st.t) : 0
  });
  for (const e of q) if (e.ws.readyState === 1) e.ws.send(msg);
}
function leaveAllQueues(ws) {
  for (const mode of Object.keys(queues)) {
    const i = queues[mode].findIndex(e => e.ws === ws);
    if (i >= 0) { queues[mode].splice(i, 1); resetQueueTimer(mode); queueBroadcast(mode); }
  }
}
function resetQueueTimer(mode) {
  const q = queues[mode], st = queueState[mode];
  if (q.length >= QUEUE_START && !st.counting) { st.counting = true; st.t = QUEUE_COUNTDOWN; }
  else if (q.length < QUEUE_MIN_FALLBACK) { st.counting = false; st.t = 0; }
  // track total wait for the safety-start
  if (q.length >= QUEUE_MIN_FALLBACK && q[0] && !q[0].waitStart) q[0].waitStart = Date.now();
}
function launchQueue(mode) {
  const q = queues[mode];
  const take = q.splice(0, MAX_PLAYERS); // up to 12 into this game
  queueState[mode] = { t: 0, counting: false };
  if (take.length < 2) { take.forEach(e => q.unshift(e)); return; } // need at least 2
  // build a fresh room for them
  const code = makeCode();
  const room = { code, mode, players: new Map(), hostId: null,
                 createdAt: Date.now(), lastSeen: Date.now(), auto: true };
  rooms.set(code, room);
  take.forEach((e, idx) => {
    const p = e.player; p.ws = e.ws;
    // team balance for tdm
    p.team = (mode === 'tdm') ? (idx % 2) : 0;
    room.players.set(p.id, p);
    if (room.hostId === null) room.hostId = p.id;
    // make the socket's message handler resolve to this room/player
    e.ws._room = room; e.ws._player = p; e.ws._qmode = null;
  });
  // tell everyone the match is starting and drop them in
  broadcast(room, { t: 'matchstart', mode, room: roomState(room) });
  queueBroadcast(mode); // refresh anyone still waiting
}


function makeCode() {
  const a = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c = '';
  do { c = ''; for (let i = 0; i < 5; i++) c += a[(Math.random() * a.length) | 0]; }
  while (rooms.has(c));
  return c;
}
function getRoom(code) { return rooms.get(code); }
function roomState(room) {
  return {
    code: room.code, mode: room.mode, hostId: room.hostId,
    count: room.players.size, max: MAX_PLAYERS,
    players: [...room.players.values()].map(p => ({
      id: p.id, name: p.name, wallet: p.wallet, team: p.team,
      kills: p.kills, deaths: p.deaths, ready: p.ready
    }))
  };
}
function broadcast(room, obj, exceptId) {
  const msg = JSON.stringify(obj);
  for (const p of room.players.values()) {
    if (p.id === exceptId) continue;
    if (p.ws.readyState === 1) p.ws.send(msg);
  }
}
function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }

let nextId = 1;

wss.on('connection', (ws) => {
  let player = null;
  let room = null;

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }

    /* ----- create / join a room ----- */
    if (m.t === 'create') {
      const code = makeCode();
      room = { code, mode: m.mode || 'tdm', players: new Map(),
               hostId: null, createdAt: Date.now(), lastSeen: Date.now() };
      rooms.set(code, room);
      player = mkPlayer(ws, m);
      room.hostId = player.id;
      room.players.set(player.id, player);
      send(ws, { t: 'joined', you: player.id, host: true, room: roomState(room) });
      return;
    }

    if (m.t === 'join') {
      room = getRoom((m.code || '').toUpperCase());
      if (!room) { send(ws, { t: 'error', msg: 'ROOM NOT FOUND' }); return; }
      if (room.players.size >= MAX_PLAYERS) { send(ws, { t: 'error', msg: 'ROOM FULL (12 MAX)' }); return; }
      player = mkPlayer(ws, m);
      // simple team balance
      let t0 = 0, t1 = 0;
      for (const p of room.players.values()) (p.team === 0 ? t0++ : t1++);
      player.team = (t0 <= t1) ? 0 : 1;
      room.players.set(player.id, player);
      send(ws, { t: 'joined', you: player.id, host: false, room: roomState(room) });
      broadcast(room, { t: 'lobby', room: roomState(room) }, null);
      return;
    }

    /* ----- public quick-play: join the rolling queue for a mode ----- */
    if (m.t === 'quickplay') {
      const mode = (m.mode === 'tdm') ? 'tdm' : 'gun';
      leaveAllQueues(ws);                 // never in two queues at once
      player = mkPlayer(ws, m);           // pre-build the player record
      queues[mode].push({ ws, player, waitStart: null });
      resetQueueTimer(mode);
      // remember which queue this socket is in so we can pull them into the room later
      ws._qmode = mode;
      send(ws, { t: 'queued', mode });
      queueBroadcast(mode);
      return;
    }
    if (m.t === 'leavequeue') { leaveAllQueues(ws); return; }

    // queued players get pulled into a room by launchQueue — pick that up here
    if (ws._room && ws._player) { room = ws._room; player = ws._player; }

    if (!room || !player) return;
    room.lastSeen = Date.now();

    /* ----- lobby actions ----- */
    if (m.t === 'ready') { player.ready = !!m.ready; broadcast(room, { t: 'lobby', room: roomState(room) }); return; }
    if (m.t === 'start' && player.id === room.hostId) {
      room.mode = m.mode || room.mode;
      broadcast(room, { t: 'start', mode: room.mode, room: roomState(room) });
      return;
    }

    /* ----- in-match relay ----- */
    if (m.t === 'state') {            // player movement/aim snapshot
      player.x = m.x; player.y = m.y; player.z = m.z;
      player.yaw = m.yaw; player.pitch = m.pitch;
      player.anim = m.anim; player.wep = m.wep; player.hp = m.hp;
      return;                          // sent out in the tick loop, not per-message
    }
    if (m.t === 'shot') {             // someone fired — relay the tracer/sound
      broadcast(room, { t: 'shot', id: player.id, x: m.x, y: m.y, z: m.z,
                        dx: m.dx, dy: m.dy, dz: m.dz, wep: m.wep }, player.id);
      return;
    }
    if (m.t === 'hit') {              // shooter claims a hit on target id
      const tgt = room.players.get(m.id);
      if (tgt) {
        tgt.hp = Math.max(0, (tgt.hp ?? 100) - (m.dmg || 0));
        send(tgt.ws, { t: 'hurt', by: player.id, dmg: m.dmg, hp: tgt.hp });
        if (tgt.hp <= 0) {
          player.kills++; tgt.deaths++;
          broadcast(room, { t: 'kill', killer: player.id, victim: tgt.id,
                            kn: player.name, vn: tgt.name, head: !!m.head });
          tgt.hp = 100;
        }
      }
      return;
    }
    if (m.t === 'chat') {
      broadcast(room, { t: 'chat', name: player.name, msg: ('' + m.msg).slice(0, 120) });
      return;
    }
  });

  ws.on('close', () => {
    leaveAllQueues(ws);
    // resolve room/player in case they were pulled from a queue
    if (ws._room && ws._player) { room = ws._room; player = ws._player; }
    if (room && player) {
      room.players.delete(player.id);
      if (player.id === room.hostId) {
        // hand host to someone else, or mark room for cleanup
        const next = room.players.values().next().value;
        room.hostId = next ? next.id : null;
      }
      broadcast(room, { t: 'left', id: player.id, room: roomState(room) });
    }
  });

  function mkPlayer(ws, m) {
    return {
      id: nextId++, ws,
      name: ('' + (m.name || 'OPERATOR')).toUpperCase().slice(0, 16),
      wallet: ('' + (m.wallet || '')).slice(0, 44),
      team: 0, ready: false,
      x: 0, y: 1, z: 0, yaw: 0, pitch: 0, anim: 0, wep: 0, hp: 100,
      kills: 0, deaths: 0
    };
  }
});

/* ---------- 20Hz world broadcast ---------- */
setInterval(() => {
  const now = Date.now();

  // ---- quick-play queue countdowns ----
  for (const mode of Object.keys(queues)) {
    const q = queues[mode], st = queueState[mode];
    if (q.length >= QUEUE_START && !st.counting) { st.counting = true; st.t = QUEUE_COUNTDOWN; queueBroadcast(mode); }
    if (st.counting) {
      st.t -= TICK / 1000;
      if (st.t <= 0) { launchQueue(mode); }
      else if (Math.ceil(st.t) !== st._last) { st._last = Math.ceil(st.t); queueBroadcast(mode); }
    }
    // safety start: enough have waited too long
    if (!st.counting && q.length >= QUEUE_MIN_FALLBACK && q[0] && q[0].waitStart
        && now - q[0].waitStart > QUEUE_MAX_WAIT * 1000) { launchQueue(mode); }
  }
  for (const [code, room] of rooms) {
    // snapshot of everyone's position for everyone
    if (room.players.size > 0) {
      const snap = [...room.players.values()].map(p => ({
        id: p.id, x: p.x, y: p.y, z: p.z, yaw: p.yaw, pitch: p.pitch,
        anim: p.anim, wep: p.wep, hp: p.hp, team: p.team,
        kills: p.kills, deaths: p.deaths
      }));
      broadcast(room, { t: 'snap', players: snap });
    }
    // reap empty rooms
    if (room.players.size === 0 && now - room.lastSeen > ROOM_EMPTY_MS) rooms.delete(code);
  }
}, TICK);

server.listen(PORT, () => console.log('THE TRENCHES server on :' + PORT));
