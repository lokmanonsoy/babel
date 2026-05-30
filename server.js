const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const path     = require('path');
const fs       = require('fs');
const readline = require('readline');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: ['https://lokmanonsoy.com', 'https://www.lokmanonsoy.com', 'https://babel.lokmanonsoy.com'] }
});

// ── Kalıcı havuz dosyası ──────────────────────────────────
const POOL_FILE = path.join(__dirname, 'pool.json');
const LOG_FILE  = path.join(__dirname, 'word-log.jsonl');

function savePool() {
  fs.writeFile(POOL_FILE, JSON.stringify({ wordPool, totalSubmitted, totalPopped }), err => {
    if (err) console.error('Havuz kayıt hatası:', err);
  });
}

function loadPool() {
  try {
    if (!fs.existsSync(POOL_FILE)) return;
    const data = JSON.parse(fs.readFileSync(POOL_FILE, 'utf8'));
    if (Array.isArray(data.wordPool)) wordPool.push(...data.wordPool);
    if (data.totalSubmitted) totalSubmitted = data.totalSubmitted;
    if (data.totalPopped)    totalPopped    = data.totalPopped;
    console.log(`Havuz yüklendi: ${wordPool.length} kelime`);
  } catch(e) {
    console.error('Havuz yükleme hatası:', e);
  }
}

// ── Log ───────────────────────────────────────────────────
function writeLog(entry) {
  const line = JSON.stringify({ ...entry, ts: new Date().toISOString() }) + '\n';
  fs.appendFile(LOG_FILE, line, err => { if (err) console.error('Log hatası:', err); });
}

// ── Sabitler ──────────────────────────────────────────────
const POOL_MAX        = 5000;
const RATE_LIMIT_MS   = 2000;
const MAX_WORD_LENGTH = 30;
const ALLOWED_CHARS   = /^[\p{L}\p{N}\s\-'.]+$/u;

function sanitize(raw) {
  if (typeof raw !== 'string') return null;
  const clean = raw.trim().slice(0, MAX_WORD_LENGTH);
  if (!clean) return null;
  if (!ALLOWED_CHARS.test(clean)) return null;
  return clean;
}

// ── Havuz ─────────────────────────────────────────────────
const wordPool     = [];
let totalSubmitted = 0;
let totalPopped    = 0;
let userCount      = 0;
const lastSubmit   = new Map();

// Başlangıçta dosyadan yükle
loadPool();

// ── Keep-alive: kendi kendine ping ────────────────────────
// Render 15dk işlem olmazsa uyutur — her 10dk'da health kontrolü
const SELF_URL = process.env.RENDER_EXTERNAL_URL || null;
if (SELF_URL) {
  setInterval(() => {
    const https = require('https');
    https.get(`${SELF_URL}/health`, res => {
      console.log(`Keep-alive ping: ${res.statusCode}`);
    }).on('error', e => console.error('Keep-alive hatası:', e.message));
  }, 10 * 60 * 1000); // 10 dakika
}

// ── Socket.io ─────────────────────────────────────────────
io.on('connection', (socket) => {
  userCount++;
  writeLog({ event: 'connect', socketId: socket.id, userCount });
  socket.emit('pool:sync', { pool: wordPool, totalSubmitted, totalPopped, userCount });
  io.emit('users:update', userCount);

  socket.on('word:submit', (word, cb) => {
    const now  = Date.now();
    const last = lastSubmit.get(socket.id) || 0;

    if (now - last < RATE_LIMIT_MS) {
      writeLog({ event: 'rate_limited', socketId: socket.id, word });
      if (typeof cb === 'function') cb({ ok: false, reason: 'rate_limited' });
      return;
    }
    lastSubmit.set(socket.id, now);

    const clean = sanitize(word);
    if (!clean) {
      writeLog({ event: 'rejected', socketId: socket.id, raw: String(word).slice(0,50) });
      if (typeof cb === 'function') cb({ ok: false, reason: 'invalid' });
      return;
    }

    if (wordPool.length >= POOL_MAX) {
      writeLog({ event: 'pool_full', socketId: socket.id, word: clean });
      if (typeof cb === 'function') cb({ ok: false, reason: 'pool_full' });
      return;
    }

    wordPool.push({ text: clean, author: socket.id.slice(0,6) });
    totalSubmitted++;
    writeLog({ event: 'submitted', socketId: socket.id.slice(0,6), word: clean, poolSize: wordPool.length });
    io.emit('pool:update', { pool: wordPool, totalSubmitted, totalPopped });
    savePool();

    // Gönderim onayı istemciye döner
    if (typeof cb === 'function') cb({ ok: true, word: clean, poolSize: wordPool.length });
  });

  const lastPop = new Map();
  const POP_LIMIT_MS = 500;

  socket.on('pool:pop', (cb) => {
    if (typeof cb !== 'function') return;
    const now  = Date.now();
    const last = lastPop.get(socket.id) || 0;
    if (now - last < POP_LIMIT_MS) return;
    lastPop.set(socket.id, now);

    if (wordPool.length === 0) { cb(null); return; }
    const idx  = Math.floor(Math.random() * wordPool.length);
    const word = wordPool.splice(idx, 1)[0];
    totalPopped++;
    writeLog({ event: 'popped', socketId: socket.id.slice(0,6), word: word.text, poolSize: wordPool.length });
    io.emit('pool:update', { pool: wordPool, totalSubmitted, totalPopped });
    savePool();
    cb(word);
  });

  socket.on('disconnect', () => {
    userCount--;
    lastSubmit.delete(socket.id);
    lastPop.delete(socket.id);
    writeLog({ event: 'disconnect', socketId: socket.id, userCount });
    io.emit('users:update', userCount);
  });
});

// ── Log endpoint ──────────────────────────────────────────
const LOG_TOKEN = process.env.LOG_TOKEN;
if (!LOG_TOKEN) console.warn('⚠️  LOG_TOKEN set edilmedi — /logs devre dışı');

app.get('/logs', (req, res) => {
  if (!LOG_TOKEN || req.query.token !== LOG_TOKEN)
    return res.status(401).json({ error: 'Yetkisiz erişim' });
  const limit  = Math.min(parseInt(req.query.limit) || 100, 1000);
  const filter = req.query.event;
  if (!fs.existsSync(LOG_FILE)) return res.json({ total: 0, logs: [] });
  const entries = [];
  const rl = readline.createInterface({ input: fs.createReadStream(LOG_FILE), crlfDelay: Infinity });
  rl.on('line', line => {
    try {
      const e = JSON.parse(line);
      if (!filter || e.event === filter) entries.push(e);
      if (entries.length > limit * 2) entries.splice(0, entries.length - limit);
    } catch {}
  });
  rl.on('close', () => res.json({ total: entries.length, logs: entries.slice(-limit) }));
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (_, res) => res.sendFile(path.join(__dirname, 'public', 'babel.html')));
app.get('/health', (_, res) => res.json({ status: 'ok', pool: wordPool.length, users: userCount, totalSubmitted }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Babel sunucusu: http://localhost:${PORT}`));
