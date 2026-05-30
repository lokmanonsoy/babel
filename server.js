const express = require('express');
const http    = require('http');
const { Server } = require('socket.io');
const path    = require('path');
const fs      = require('fs');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: ['https://lokmanonsoy.com', 'https://www.lokmanonsoy.com', 'https://babel.lokmanonsoy.com'] }
});

// ── Log dosyası ───────────────────────────────────────────
const LOG_FILE = path.join(__dirname, 'word-log.jsonl');

function writeLog(entry) {
  const line = JSON.stringify({ ...entry, ts: new Date().toISOString() }) + '\n';
  fs.appendFile(LOG_FILE, line, err => { if (err) console.error('Log hatası:', err); });
}

// ── Sabitler ──────────────────────────────────────────────
const POOL_MAX        = 500;
const RATE_LIMIT_MS   = 2000;
const MAX_WORD_LENGTH = 30;
const ALLOWED_CHARS   = /^[\p{L}\p{N}\s\-'.]+$/u;

// ── Input temizleme ───────────────────────────────────────
function sanitize(raw) {
  if (typeof raw !== 'string') return null;
  const clean = raw.trim().slice(0, MAX_WORD_LENGTH);
  if (!clean) return null;
  if (!ALLOWED_CHARS.test(clean)) return null;
  return clean;
}

// ── Kelime havuzu ─────────────────────────────────────────
const wordPool     = [];
let totalSubmitted = 0;
let totalPopped    = 0;
let userCount      = 0;
const lastSubmit   = new Map();

io.on('connection', (socket) => {
  userCount++;
  writeLog({ event: 'connect', socketId: socket.id, userCount });
  socket.emit('pool:sync', { pool: wordPool, totalSubmitted, totalPopped, userCount });
  io.emit('users:update', userCount);

  socket.on('word:submit', (word) => {
    const now  = Date.now();
    const last = lastSubmit.get(socket.id) || 0;

    // Rate limit ihlali — logla ama reddet
    if (now - last < RATE_LIMIT_MS) {
      writeLog({ event: 'rate_limited', socketId: socket.id, word });
      return;
    }
    lastSubmit.set(socket.id, now);

    const clean = sanitize(word);
    // Geçersiz input — logla ama reddet
    if (!clean) {
      writeLog({ event: 'rejected', socketId: socket.id, raw: String(word).slice(0,50) });
      return;
    }

    if (wordPool.length >= POOL_MAX) {
      writeLog({ event: 'pool_full', socketId: socket.id, word: clean });
      return;
    }

    wordPool.push({ text: clean, author: socket.id.slice(0,6) });
    totalSubmitted++;
    writeLog({ event: 'submitted', socketId: socket.id.slice(0,6), word: clean, poolSize: wordPool.length });
    io.emit('pool:update', { pool: wordPool, totalSubmitted, totalPopped });
  });

  const lastPop = new Map(); // pool:pop rate limit
  const POP_LIMIT_MS = 500; // max 2 pop/sn per user

  socket.on('pool:pop', (cb) => {
    if (typeof cb !== 'function') return;
    const now = Date.now();
    const last = lastPop.get(socket.id) || 0;
    if (now - last < POP_LIMIT_MS) return; // sessizce reddet
    lastPop.set(socket.id, now);

    if (wordPool.length === 0) { cb(null); return; }
    const idx  = Math.floor(Math.random() * wordPool.length);
    const word = wordPool.splice(idx, 1)[0];
    totalPopped++;
    writeLog({ event: 'popped', socketId: socket.id.slice(0,6), word: word.text, poolSize: wordPool.length });
    io.emit('pool:update', { pool: wordPool, totalSubmitted, totalPopped });
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

// ── Log okuma endpoint'i ──────────────────────────────────
// ⚠️  LOG_TOKEN'ı mutlaka environment variable olarak set et:
// LOG_TOKEN=gizli-token node server.js
const LOG_TOKEN = process.env.LOG_TOKEN;
if (!LOG_TOKEN) console.warn('⚠️  LOG_TOKEN set edilmedi — /logs endpoint\'i devre dışı');

const readline = require('readline');

app.get('/logs', (req, res) => {
  if (!LOG_TOKEN || req.query.token !== LOG_TOKEN) {
    return res.status(401).json({ error: 'Yetkisiz erişim' });
  }
  const limit  = Math.min(parseInt(req.query.limit) || 100, 1000);
  const filter = req.query.event;

  if (!fs.existsSync(LOG_FILE)) return res.json({ total: 0, logs: [] });

  const entries = [];
  const rl = readline.createInterface({ input: fs.createReadStream(LOG_FILE), crlfDelay: Infinity });

  rl.on('line', (line) => {
    try {
      const entry = JSON.parse(line);
      if (!filter || entry.event === filter) entries.push(entry);
      // Bellek koruma: çok büyük log dosyaları için sadece son N'i tut
      if (entries.length > limit * 2) entries.splice(0, entries.length - limit);
    } catch {}
  });

  rl.on('close', () => {
    res.json({ total: entries.length, logs: entries.slice(-limit) });
  });
});

app.use(express.static(path.join(__dirname, 'public')));

// Kök URL'yi babel.html'ye yönlendir
app.get('/', (_, res) => res.sendFile(path.join(__dirname, 'public', 'babel.html')));

app.get('/health', (_, res) => res.json({ status: 'ok', pool: wordPool.length, users: userCount, totalSubmitted }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Babel sunucusu: http://localhost:${PORT}`));
