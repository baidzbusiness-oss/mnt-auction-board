// TikTok auction connector. Run: node server.js mntyclan
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');
const tiktok = require('tiktok-live-connector');
const username = String(process.env.TIKTOK_USERNAME || process.argv[2] || '').replace(/^@/, '').trim();
if (!username) { console.error('Usage: node server.js TikTokUsername'); process.exit(1); }
const PORT = Number(process.env.PORT || 8080);
const server = http.createServer((req, res) => {
  if (req.url?.split('?')[0]?.split('#')[0] !== '/') { res.writeHead(404); return res.end('Not found'); }
  fs.readFile(path.join(__dirname, 'auction-board.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('Missing auction-board.html'); }
    res.writeHead(200, {'Content-Type':'text/html; charset=utf-8', 'Cache-Control':'no-store'});
    res.end(data);
  });
});
const wss = new WebSocketServer({server});
let latestState = null;
wss.on('connection', ws => {
  console.log('Board browser connected; total browsers:', wss.clients.size);
  if (latestState) ws.send(JSON.stringify({type:'state',s:latestState}));
  ws.on('message', message => {
    // Relay state from the controls page to the overlay page.
    let parsed;
    try { parsed = JSON.parse(message.toString()); } catch { return; }
    if (parsed?.type !== 'state' || !parsed.s || typeof parsed.s !== 'object') return;
    latestState = parsed.s;
    for (const client of wss.clients) if (client !== ws && client.readyState === WebSocket.OPEN) client.send(JSON.stringify(parsed));
  });
});
function broadcast(payload) {
  const json = JSON.stringify(payload);
  let n = 0;
  for (const client of wss.clients) if (client.readyState === WebSocket.OPEN) { client.send(json); n++; }
  console.log('Sent gift to', n, 'board browser(s)');
}
const avatarCache = new Map();
async function avatarData(url) {
  if (!url || typeof url !== 'string' || !/^https:\/\//i.test(url)) return undefined;
  if (avatarCache.has(url)) return avatarCache.get(url);
  try {
    const response = await fetch(url, {signal: AbortSignal.timeout(4000)});
    if (!response.ok || Number(response.headers.get('content-length') || 0) > 500000) return undefined;
    const mime = (response.headers.get('content-type') || '').split(';')[0];
    if (!/^image\/(jpeg|png|webp|gif)$/.test(mime)) return undefined;
    const bytes = Buffer.from(await response.arrayBuffer());
    if (bytes.length > 500000) return undefined;
    const result = `data:${mime};base64,${bytes.toString('base64')}`;
    if (avatarCache.size > 150) avatarCache.clear();
    avatarCache.set(url, result);
    return result;
  } catch { return undefined; }
}
function giftValue(event) {
  // Gift metadata is the price PER gift. Top-level diamondCount can represent
  // the accumulated value of an entire streak and must not be multiplied again.
  const detail = event.giftDetails || event.extendedGiftInfo || event.gift || {};
  for (const item of [detail.diamondCount, detail.diamond_count, detail.coinCount, detail.cost, event.gift?.diamondCount, event.gift?.diamond_count]) {
    const n = Number(item);
    if (Number.isFinite(n) && n > 0) return n;
  }
  const total = Number(event.diamondCount ?? event.diamond_count);
  const count = Number(event.repeatCount ?? event.repeat_count ?? 1);
  if (Number.isFinite(total) && total > 0 && Number.isFinite(count) && count > 1 && total % count === 0) return total / count;
  return Number.isFinite(total) && total > 0 && count === 1 ? total : 0;
}
let activeConnection = null;
let retryTimer = null;
let connecting = false;
let stopped = false;
const processed = new Map();
function scheduleReconnect() {
  if (stopped || retryTimer) return;
  retryTimer = setTimeout(() => { retryTimer = null; connectTikTok(); }, 15000);
  console.log('Retrying TikTok connection in 15 seconds...');
}
const streakCounts = new Map();
async function onGift(d) {
  const u = d.user || {};
  const details = d.giftDetails || d.gift || d.extendedGiftInfo || {};
  const name = d.uniqueId || u.uniqueId || d.nickname || u.nickname || u.userId || d.userId;
  const unit = giftValue(d);
  const count = Number(d.repeatCount ?? d.repeat_count ?? 1);
  const giftType = d.giftType ?? details.giftType;
  const repeatEnd = d.repeatEnd === true || d.repeatEnd === 1 || d.repeatEndFlag === true || d.repeatEndFlag === 1;
  const giftId = String(d.giftId ?? details.id ?? d.gift?.id ?? d.giftName ?? 'gift');
  const streakId = String(d.groupId ?? d.group_id ?? d.gift?.groupId ?? '');
  console.log('GIFT EVENT RECEIVED:', JSON.stringify({user:name, gift:d.giftName || details.name, unit, count, giftType, repeatEnd, msgId:d.msgId, streakId}));
  if (!name || !unit || !Number.isSafeInteger(count) || count < 1) { console.log('GIFT NOT ADDED: invalid data'); return; }
  const id = String(d.msgId || d.messageId || '');
  if (id && processed.has(id)) { console.log('Duplicate event skipped'); return; }
  if (id) { processed.set(id, Date.now()); if (processed.size > 2000) processed.clear(); }
  // Some TikTok connector versions omit giftType even for streakable gifts.
  // A group/streak ID or repeatEnd is enough to identify cumulative updates.
  const isStreak = giftType === 1 || giftType === '1' || Boolean(streakId) || repeatEnd || count > 1;
  let quantity = count;
  if (isStreak) {
    const key = `${name}|${giftId}|${streakId}`;
    const previous = streakCounts.get(key);
    // TikTok repeatCount is cumulative during a streak. Only add the difference.
    // A repeatCount reset to 1 indicates a new streak.
    const priorCount = previous && count >= previous.count && Date.now() - previous.time < 120000 ? previous.count : 0;
    quantity = Math.max(0, count - priorCount);
    streakCounts.set(key, {count, time:Date.now()});
    if (streakCounts.size > 1000) streakCounts.clear();
    // Retain the final count briefly so a repeated final packet adds zero.
    // Expire old entries on the next gift to avoid an unbounded cache.
    if (streakCounts.size > 500) {
      for (const [k, v] of streakCounts) if (Date.now() - v.time > 120000) streakCounts.delete(k);
    }
  }
  const coins = Math.round(unit * quantity);
  if (coins <= 0) return;
  const pic = d.profilePictureUrl || u.profilePictureUrl || u.profilePicture?.url?.[0] || u.profilePicture?.urls?.[0];
  console.log('ADDING', coins, 'coins for', name);
  broadcast({user:String(name), coins, avatar:await avatarData(pic)});
}
function connectTikTok() {
  if (connecting || stopped) return;
  connecting = true;
  const Connection = tiktok.TikTokLiveConnection || tiktok.WebcastPushConnection;
  if (!Connection) { console.error('TikTok connector has no supported connection class'); connecting = false; return; }
  const conn = new Connection(username, {});
  activeConnection = conn;
  const events = tiktok.WebcastEvent || {};
  const giftEvents = new Set(['gift', events.GIFT].filter(Boolean));
  for (const eventName of giftEvents) conn.on(eventName, onGift);
  console.log('Listening for gift event names:', [...giftEvents].join(', '));
  conn.on('chat', d => console.log('CHAT RECEIVED:', d.uniqueId || d.user?.uniqueId || 'viewer', String(d.comment || '').slice(0, 80)));
  conn.on('rawData', (type) => console.log('RAW EVENT:', String(type)));
  conn.on('streamEnd', () => console.log('TikTok stream ended'));
  conn.on('roomUser', d => console.log('VIEWER EVENT RECEIVED:', d.viewerCount ?? 'unknown'));
  conn.on('error', err => console.error('TIKTOK ERROR:', err?.message || err));
  conn.on('disconnected', () => { if (activeConnection !== conn) return; console.log('TikTok disconnected'); connecting = false; scheduleReconnect(); });
  conn.connect().then(() => {
    if (activeConnection !== conn) return;
    connecting = false;
    console.log(`Connected to @${username}'s LIVE. Waiting for chat and gift events...`);
  }).catch(err => {
    if (activeConnection !== conn) return;
    connecting = false;
    console.error('TikTok connection failed:', err?.message || err);
    scheduleReconnect();
  });
}
server.on('error', err => { console.error('Server error:', err.message); if (err.code === 'EADDRINUSE') console.error('Port 8080 is already used. Stop the other auction server first.'); });
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Controls: http://localhost:${PORT}`);
  console.log(`Overlay:  http://localhost:${PORT}/#board`);
  connectTikTok();
});
process.on('SIGINT', () => { stopped = true; if (retryTimer) clearTimeout(retryTimer); activeConnection?.disconnect?.(); server.close(); process.exit(0); });
