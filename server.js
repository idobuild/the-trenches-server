/* ============================================================
   THE TRENCHES — Multiplayer Server
   A tiny authoritative-ish relay for up to 12 players per room.
   Node.js + ws. No database. Rooms live in memory.
   Deploy on Render/Railway/Fly. See SETUP steps in chat.

   RESET THE LEADERBOARD FOR EVERYONE:
     After this is deployed, open this URL once in your browser:
       https://the-trenches-server.onrender.com/reset?key=trench-wipe-9271
     It clears all player stats. (Change the key below if you want.)
   ============================================================ */
const http = require('http');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');

/* secret key for the /reset endpoint — change it to whatever you like */
const RESET_KEY = process.env.RESET_KEY || 'trench-wipe-9271';

/* ---------- persistent player stats (keyed by wallet) ---------- */
// On Render, attach a free Disk and set DATA_DIR to its mount path for true persistence.
const DATA_DIR = process.env.DATA_DIR || __dirname;
const DB_FILE = path.join(DATA_DIR, 'players.json');
let DB = {}; // wallet -> { name, kills, deaths, wins, matches, score, bestStreak }
try { DB = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch (e) { DB = {}; }
let _dbDirty = false;
function dbGet(wallet, name) {
  if (!wallet) wallet = 'guest:' + (name || 'OPERATOR');
  if (!DB[wallet]) DB[wallet] = { name: name || 'OPERATOR', kills: 0, deaths: 0, wins: 0, matches: 0, score: 0, bestStreak: 0, nukes: 0 };
  if (DB[wallet].nukes == null) DB[wallet].nukes = 0; // migrate older records
  if (name) DB[wallet].name = name;
  return DB[wallet];
}
function dbMark() { _dbDirty = true; dbSaveSoon(); }
let _dbSaveT = null;
function dbSaveSoon(){ if(_dbSaveT) return; _dbSaveT = setTimeout(()=>{ _dbSaveT=null; dbSave(); }, 500); }
function dbSave() { if (!_dbDirty) return; _dbDirty = false;
  try { const tmp = DB_FILE + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(DB)); fs.renameSync(tmp, DB_FILE); } catch (e) {} }
setInterval(dbSave, 5000);
// flush to disk on shutdown so a redeploy/restart never loses the latest stats
process.on('SIGTERM', () => { try{dbSave();zSave&&zSave();}catch(e){} process.exit(0); });
process.on('SIGINT',  () => { try{dbSave();zSave&&zSave();}catch(e){} process.exit(0); });

function leaderboard(cat, limit) {
  const key = ['kd', 'kills', 'score', 'wins', 'bestStreak', 'nukes'].includes(cat) ? cat : 'kills';
  const rows = Object.entries(DB).map(([wallet, s]) => {
    const kd = s.deaths > 0 ? s.kills / s.deaths : s.kills;
    return { name: s.name, wallet: wallet.length > 8 ? wallet.slice(0, 4) + '\u2026' + wallet.slice(-4) : wallet,
             walletFull: wallet,
             kills: s.kills, deaths: s.deaths, wins: s.wins, score: s.score,
             bestStreak: s.bestStreak, nukes: s.nukes||0, kd: +kd.toFixed(2), matches: s.matches };
  }).filter(r => r.matches > 0 || r.kills > 0 || r.deaths > 0);
  rows.sort((a, b) => b[key] - a[key]);
  return rows.slice(0, limit || 100);
}

/* ---------- ZOMBIES global stats (keyed by wallet) ---------- */
const ZDB_FILE = path.join(DATA_DIR, 'zombies.json');
let ZDB = {}; // wallet -> { name, bestRound, bestScore, kills, hs, games, totalPoints }
try { ZDB = JSON.parse(fs.readFileSync(ZDB_FILE, 'utf8')); } catch (e) { ZDB = {}; }
let _zDirty = false;
function zGet(wallet, name) {
  if (!wallet) wallet = 'guest:' + (name || 'OPERATOR');
  if (!ZDB[wallet]) ZDB[wallet] = { name: name || 'OPERATOR', bestRound: 0, bestScore: 0, kills: 0, hs: 0, games: 0, totalPoints: 0 };
  if (name) ZDB[wallet].name = name;
  return ZDB[wallet];
}
let _zSaveT = null;
function zMark(){ _zDirty = true; if(_zSaveT) return; _zSaveT = setTimeout(()=>{ _zSaveT=null; zSave(); }, 500); }
function zSave() { if (!_zDirty) return; _zDirty = false;
  try { const tmp = ZDB_FILE + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(ZDB)); fs.renameSync(tmp, ZDB_FILE); } catch (e) {} }
setInterval(zSave, 5000);
function zleaderboard(cat, limit) {
  const key = ['bestRound', 'bestScore', 'kills', 'hs', 'games', 'totalPoints'].includes(cat) ? cat : 'bestRound';
  const rows = Object.entries(ZDB).map(([wallet, s]) => ({
    name: s.name, wallet: wallet.length > 8 ? wallet.slice(0, 4) + '\u2026' + wallet.slice(-4) : wallet,
    walletFull: wallet,
    bestRound: s.bestRound||0, bestScore: s.bestScore||0, kills: s.kills||0,
    hs: s.hs||0, games: s.games||0, totalPoints: s.totalPoints||0
  })).filter(r => r.games > 0 || r.bestRound > 0 || r.kills > 0);
  rows.sort((a, b) => b[key] - a[key]);
  return rows.slice(0, limit || 100);
}

const PORT = process.env.PORT || 8080;
const TICK = 1000 / 20;          // 20 broadcasts/sec
const MAX_PLAYERS = 12;          // hard cap per room
const ROOM_EMPTY_MS = 60 * 1000; // close empty room after 60s

// ---- public quick-play matchmaking ----
const QUEUE_START = 2;           // start a game once this many are waiting
const QUEUE_COUNTDOWN = 6;       // seconds of "starting soon" once minimum is met
const QUEUE_MAX_WAIT = 45;       // safety: start anyway after this long if 2+ waiting
const QUEUE_MIN_FALLBACK = 2;    // ...with at least this many

// HTTP: health check + leaderboard endpoint + reset endpoint
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');

  // ---- live online count: ?  -> {n: number of connected sockets} ----
  if (req.url && req.url.startsWith('/online')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ n: wss.clients.size }));
    return;
  }

  // ---- RESET the whole leaderboard (wipes everyone's stats) ----
  if (req.url && req.url.startsWith('/reset')) {
    const u = new URL(req.url, 'http://x');
    if (u.searchParams.get('key') !== RESET_KEY) {
      res.writeHead(403, { 'Content-Type': 'text/plain' });
      res.end('FORBIDDEN — wrong or missing key\n');
      return;
    }
    DB = {};                       // wipe all player stats in memory
    _dbDirty = true; dbSave();     // overwrite players.json with {}
    ZDB = {};                      // wipe zombies stats too
    _zDirty = true; zSave();
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('LEADERBOARD RESET — ALL STATS CLEARED\n');
    return;
  }

  if (req.url && req.url.startsWith('/zleaderboard')) {
    const u = new URL(req.url, 'http://x');
    const cat = u.searchParams.get('cat') || 'bestRound';
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(zleaderboard(cat, 100)));
    return;
  }

  // ---- debug: see exactly what the server has stored (open in a browser) ----
  if (req.url && req.url.startsWith('/debug')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      players_count: Object.keys(DB).length,
      zombies_count: Object.keys(ZDB).length,
      dataDir: DATA_DIR,
      players: DB,
      zombies: ZDB
    }, null, 2));
    return;
  }

  if (req.url && req.url.startsWith('/leaderboard')) {
    const u = new URL(req.url, 'http://x');
    const cat = u.searchParams.get('cat') || 'kills';
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(leaderboard(cat, 100)));
    return;
  }
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
  // tell each player the match is starting, with their own id
  for (const p of room.players.values()) {
    send(p.ws, { t: 'matchstart', mode, you: p.id, room: roomState(room) });
  }
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

  // tell this client the current online count, and refresh everyone's
  broadcastOnline();

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }

    /* ----- client asking for the live online count ----- */
    if (m.t === 'online') { send(ws, { t: 'online', n: wss.clients.size }); return; }

    /* ----- submit a Zombies result to the global Zombies board ----- */
    if (m.t === 'zsubmit') {
      const s = zGet(('' + (m.wallet || '')).slice(0, 44), ('' + (m.name || 'OPERATOR')).toUpperCase().slice(0, 16));
      s.games = (s.games || 0) + 1;
      s.kills = (s.kills || 0) + (m.kills | 0);
      s.hs = (s.hs || 0) + (m.hs | 0);
      s.totalPoints = (s.totalPoints || 0) + (m.points | 0);
      if ((m.round | 0) > (s.bestRound || 0)) s.bestRound = m.round | 0;
      if ((m.points | 0) > (s.bestScore || 0)) s.bestScore = m.points | 0;
      zMark();
      send(ws, { t: 'zok' });
      return;
    }

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
      player = mkPlayer(ws, m);           // build the player record
      // FIRST: try to drop into an existing in-progress game of this mode that has room
      let joined = null;
      for (const [, rm] of rooms) {
        if (rm.auto && rm.mode === mode && rm.players.size > 0 && rm.players.size < MAX_PLAYERS) { joined = rm; break; }
      }
      if (joined) {
        // balance teams for tdm by filling the smaller side
        if (mode === 'tdm') {
          let t0 = 0, t1 = 0; for (const p of joined.players.values()) (p.team === 0 ? t0++ : t1++);
          player.team = (t0 <= t1) ? 0 : 1;
        } else player.team = 0;
        joined.players.set(player.id, player);
        if (joined.hostId == null) joined.hostId = player.id;
        ws._room = joined; ws._player = player; ws._qmode = null;
        room = joined;
        // tell the newcomer to start, and let everyone see them
        send(ws, { t: 'matchstart', mode, you: player.id, room: roomState(joined) });
        broadcast(joined, { t: 'playerjoined', id: player.id, room: roomState(joined) }, player.id);
        return;
      }
      // otherwise wait in the queue for a fresh game
      queues[mode].push({ ws, player, waitStart: null });
      resetQueueTimer(mode);
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
      // never credit a hit on yourself, and ignore hits on someone already downed
      if (tgt && tgt.id !== player.id && !tgt.dead) {
        // ---- ANTI-CHEAT VALIDATION ----
        const now = Date.now();
        // 1) rate limit: no more than ~20 damage events/sec from one shooter
        player._hitWin = player._hitWin || [];
        player._hitWin = player._hitWin.filter(t => now - t < 1000);
        if (player._hitWin.length >= 20) return;     // firing impossibly fast -> drop
        player._hitWin.push(now);
        // 2) distance sanity: shooter & target positions are known from 'state' updates.
        //    A bullet can't land if the two are absurdly far apart (shooting across/through the map).
        const dx = (player.x||0)-(tgt.x||0), dy=(player.y||0)-(tgt.y||0), dz=(player.z||0)-(tgt.z||0);
        const dist = Math.sqrt(dx*dx+dy*dy+dz*dz);
        if (dist > 140) return;                       // beyond any sightline on this map -> drop
        // 3) clamp damage to a believable per-hit maximum (kills the 9999 one-shot hacks)
        let dmg = +m.dmg || 0;
        if (dmg <= 0) return;
        dmg = Math.min(dmg, 220);                     // allow sniper-HS / knife tiers, block 9999 hacks
        tgt.hp = Math.max(0, (tgt.hp ?? 100) - dmg);
        send(tgt.ws, { t: 'hurt', by: player.id, dmg, hp: tgt.hp });
        if (tgt.hp <= 0) {
          tgt.dead = true;            // lock out further hits until they respawn
          player.kills++; tgt.deaths++;
          player.streak = (player.streak || 0) + 1;
          tgt.streak = 0;
          const ks = dbGet(player.wallet, player.name);
          ks.kills++; ks.score += 100; if (player.streak > ks.bestStreak) ks.bestStreak = player.streak;
          const vs = dbGet(tgt.wallet, tgt.name); vs.deaths++;
          dbMark();
          // a single kill event, tagged with a unique id so the client can de-dup
          broadcast(room, { t: 'kill', id: 'k' + (nextId++), killer: player.id, victim: tgt.id,
                            kn: player.name, vn: tgt.name, head: !!m.head, wep: player.wep });
          // clear the dead-lock + restore HP after the respawn delay
          tgt.hp = 100;
          setTimeout(() => { if (tgt) { tgt.dead = false; tgt.hp = 100; } }, 2500);
        }
      }
      return;
    }
    if (m.t === 'lb') {              // client asking for the global leaderboard
      send(ws, { t: 'lb', cat: m.cat || 'kills', rows: leaderboard(m.cat, 50) });
      return;
    }
    if (m.t === 'endmatch') {        // client reports its match finished (records match + win)
      const s = dbGet(player.wallet, player.name);
      s.matches++; if (m.won) s.wins++; dbMark();
      return;
    }
    if (m.t === 'nuke') {            // client earned a tactical nuke
      const s = dbGet(player.wallet, player.name);
      s.nukes = (s.nukes || 0) + 1; dbMark();
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
      // if only one player is left alone in a match, pull them out and send them to re-matchmake
      if (room.players.size === 1) {
        const last = room.players.values().next().value;
        if (last && last.ws && last.ws.readyState === 1) {
          send(last.ws, { t: 'alone' });
          // free the lone player from this now-dead room so they can join/queue fresh
          room.players.delete(last.id);
          last.ws._room = null; last.ws._player = null;
        }
        room.hostId = null;
        room.lastSeen = 0; // mark for immediate reap on next tick
      }
    }
    broadcastOnline();
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

/* broadcast the current online count to every connected socket */
function broadcastOnline() {
  const msg = JSON.stringify({ t: 'online', n: wss.clients.size });
  for (const c of wss.clients) if (c.readyState === 1) c.send(msg);
}

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
        anim: p.anim, wep: p.wep, hp: p.hp, team: p.team, name: p.name, wallet: p.wallet,
        kills: p.kills, deaths: p.deaths
      }));
      broadcast(room, { t: 'snap', players: snap });
    }
    // reap empty rooms
    if (room.players.size === 0 && now - room.lastSeen > ROOM_EMPTY_MS) rooms.delete(code);
  }
}, TICK);

server.listen(PORT, () => console.log('THE TRENCHES server on :' + PORT));
