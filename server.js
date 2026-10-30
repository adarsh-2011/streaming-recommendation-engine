// Streaming Recommendation Engine (demo)
// ----------------------------------------
// Zero-dependency Node.js server. The event stream is SIMULATED (no Kafka,
// no external services, no API keys). A hybrid recommender — item-item
// collaborative filtering plus a trending/popular fallback for cold-start
// users — recomputes live as events arrive.
//
// Run:  node server.js            (listens on http://localhost:3000)
//       PORT=4000 node server.js  (custom port)
// State persists to state.json next to this file so it survives restarts.

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(__dirname, 'state.json');

// ===========================================================================
// 1. Catalog — 24 products across 6 categories
// ===========================================================================
const CATALOG = [
  // Electronics
  { id: 'p01', name: 'Wireless Noise-Cancelling Headphones', category: 'Electronics', price: 129 },
  { id: 'p02', name: 'Mechanical Keyboard', category: 'Electronics', price: 89 },
  { id: 'p03', name: 'Wireless Mouse', category: 'Electronics', price: 45 },
  { id: 'p04', name: '27" 4K Monitor', category: 'Electronics', price: 299 },
  // Home & Kitchen
  { id: 'p05', name: 'Air Fryer', category: 'Home & Kitchen', price: 99 },
  { id: 'p06', name: 'Espresso Machine', category: 'Home & Kitchen', price: 199 },
  { id: 'p07', name: "Chef's Knife Set", category: 'Home & Kitchen', price: 79 },
  { id: 'p08', name: 'Stand Mixer', category: 'Home & Kitchen', price: 249 },
  // Sports & Outdoors
  { id: 'p09', name: 'Running Shoes', category: 'Sports & Outdoors', price: 119 },
  { id: 'p10', name: 'Yoga Mat', category: 'Sports & Outdoors', price: 35 },
  { id: 'p11', name: 'Insulated Bottle', category: 'Sports & Outdoors', price: 29 },
  { id: 'p12', name: 'Fitness Tracker', category: 'Sports & Outdoors', price: 79 },
  // Books
  { id: 'p13', name: 'The Pragmatic Programmer', category: 'Books', price: 39 },
  { id: 'p14', name: 'Designing Data-Intensive Applications', category: 'Books', price: 49 },
  { id: 'p15', name: 'Clean Code', category: 'Books', price: 42 },
  { id: 'p16', name: 'Atomic Habits', category: 'Books', price: 27 },
  // Fashion
  { id: 'p17', name: 'Leather Wallet', category: 'Fashion', price: 49 },
  { id: 'p18', name: 'Sunglasses', category: 'Fashion', price: 89 },
  { id: 'p19', name: 'Backpack', category: 'Fashion', price: 69 },
  { id: 'p20', name: 'Denim Jacket', category: 'Fashion', price: 99 },
  // Beauty
  { id: 'p21', name: 'Vitamin C Serum', category: 'Beauty', price: 29 },
  { id: 'p22', name: 'Moisturizer', category: 'Beauty', price: 24 },
  { id: 'p23', name: 'Shampoo', category: 'Beauty', price: 19 },
  { id: 'p24', name: 'Lipstick Set', category: 'Beauty', price: 34 },
];
const productById = Object.fromEntries(CATALOG.map((p) => [p.id, p]));

// ===========================================================================
// 2. State, seed history, and persistence
// ===========================================================================
const ACTION_WEIGHTS = { view: 1, click: 2, 'add-to-cart': 3, purchase: 5 };

// Fixed seed history so the recommender has signal from the first second.
// [userId, productId, action]
const SEED_EVENTS = [
  ['u1', 'p01', 'purchase'], ['u1', 'p02', 'purchase'], ['u1', 'p03', 'click'], ['u1', 'p04', 'view'], ['u1', 'p13', 'view'],
  ['u2', 'p05', 'purchase'], ['u2', 'p06', 'click'], ['u2', 'p21', 'purchase'], ['u2', 'p22', 'view'], ['u2', 'p08', 'view'],
  ['u3', 'p09', 'purchase'], ['u3', 'p10', 'purchase'], ['u3', 'p11', 'click'], ['u3', 'p12', 'view'], ['u3', 'p01', 'view'],
  ['u4', 'p13', 'purchase'], ['u4', 'p14', 'purchase'], ['u4', 'p15', 'click'], ['u4', 'p16', 'view'], ['u4', 'p02', 'view'],
  ['u5', 'p02', 'purchase'], ['u5', 'p04', 'click'], ['u5', 'p14', 'purchase'], ['u5', 'p01', 'view'], ['u5', 'p15', 'view'],
  ['u6', 'p17', 'purchase'], ['u6', 'p18', 'click'], ['u6', 'p21', 'purchase'], ['u6', 'p22', 'purchase'], ['u6', 'p23', 'view'],
];

function freshState() {
  const now = Date.now();
  const users = [
    { id: 'u1', name: 'Aarav', createdAt: now },
    { id: 'u2', name: 'Priya', createdAt: now },
    { id: 'u3', name: 'Rohan', createdAt: now },
    { id: 'u4', name: 'Sneha', createdAt: now },
    { id: 'u5', name: 'Vikram', createdAt: now },
    { id: 'u6', name: 'Ananya', createdAt: now },
  ];
  // Seed events are spaced 1 minute apart in the past so recency math works.
  const events = SEED_EVENTS.map(([userId, productId, action], i) => ({
    id: `seed-${i}`,
    userId,
    productId,
    action,
    ts: now - (SEED_EVENTS.length - i) * 60000,
  }));
  return {
    users,
    events,
    eventSeq: events.length,
    totalEvents: events.length,
    streamRunning: true,
    speed: 1,
    startedAt: now,
    lastModelUpdate: now,
    nextUserNum: 7,
  };
}

function loadState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf8');
    const s = JSON.parse(raw);
    if (Array.isArray(s.users) && Array.isArray(s.events)) {
      s.streamRunning = true; // always resume streaming on restart
      s.startedAt = Date.now();
      s.lastModelUpdate = Date.now();
      return s;
    }
  } catch (_) { /* missing or corrupt -> fresh state */ }
  return freshState();
}

let state = loadState();
let eventsSinceSave = 0;

function saveState() {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  } catch (err) {
    console.error('Failed to save state:', err.message);
  }
}

// ===========================================================================
// 3. Simulated live event stream
// ===========================================================================
function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

function weightedAction() {
  const r = Math.random();
  if (r < 0.55) return 'view';
  if (r < 0.80) return 'click';
  if (r < 0.92) return 'add-to-cart';
  return 'purchase';
}

// Pick a product the user is likely to touch: same-category items they already
// know (50%), items co-occurring with their history (30%), anything (20%).
function pickProductForUser(userId) {
  const mine = state.events.filter((e) => e.userId === userId);
  const r = Math.random();
  if (mine.length > 0 && r < 0.5) {
    const cats = [...new Set(mine.map((e) => productById[e.productId].category))];
    const pool = CATALOG.filter((p) => cats.includes(p.category));
    return pick(pool).id;
  }
  if (mine.length > 0 && r < 0.8) {
    const co = buildCooccurrence(state.events);
    const pool = new Set();
    for (const e of mine) {
      const row = co.get(e.productId);
      if (row) for (const [other] of row) pool.add(other);
    }
    if (pool.size > 0) return pick([...pool]);
  }
  return pick(CATALOG).id;
}

function recordEvent(userId, productId, action) {
  const ev = {
    id: `ev-${state.eventSeq++}`,
    userId,
    productId,
    action,
    ts: Date.now(),
  };
  state.events.push(ev);
  state.totalEvents += 1;
  state.lastModelUpdate = Date.now();
  if (state.events.length > 5000) state.events.splice(0, state.events.length - 5000);
  if (++eventsSinceSave >= 10) { eventsSinceSave = 0; saveState(); }
  return ev;
}

function streamTick() {
  if (!state.streamRunning) return;
  const user = pick(state.users);
  recordEvent(user.id, pickProductForUser(user.id), weightedAction());
}

// Base interval 900ms at 1x speed; speed multiplies the event rate.
let timer = null;
function restartTimer() {
  if (timer) clearInterval(timer);
  const interval = Math.max(100, Math.round(900 / state.speed));
  timer = setInterval(streamTick, interval);
}
restartTimer();

// ===========================================================================
// 4. Hybrid recommender — clearly separated functions
// ===========================================================================

// Item-item co-occurrence: for every user, every pair of distinct items they
// touched adds weight(action_i) * weight(action_j) to co[i][j] and co[j][i].
function buildCooccurrence(events) {
  const byUser = new Map();
  for (const e of events) {
    if (!byUser.has(e.userId)) byUser.set(e.userId, new Map());
    const m = byUser.get(e.userId);
    m.set(e.productId, Math.max(m.get(e.productId) || 0, ACTION_WEIGHTS[e.action]));
  }
  const co = new Map();
  for (const items of byUser.values()) {
    const ids = [...items.keys()];
    for (let i = 0; i < ids.length; i++) {
      for (let j = 0; j < ids.length; j++) {
        if (i === j) continue;
        if (!co.has(ids[i])) co.set(ids[i], new Map());
        const row = co.get(ids[i]);
        row.set(ids[j], (row.get(ids[j]) || 0) + items.get(ids[i]) * items.get(ids[j]));
      }
    }
  }
  return co;
}

// Weighted popularity over all events (used for trending + cold start).
function trendingScores(events) {
  const scores = new Map();
  for (const e of events) {
    scores.set(e.productId, (scores.get(e.productId) || 0) + ACTION_WEIGHTS[e.action]);
  }
  return [...scores.entries()].sort((a, b) => b[1] - a[1]);
}

function trendingItems(n) {
  return trendingScores(state.events).slice(0, n).map(([id, score]) => ({
    product: productById[id],
    score: Math.round(score * 100) / 100,
    reason: 'Trending now',
  }));
}

// Score every candidate the user has NOT purchased yet:
//   score(candidate) = sum over user's items i of co[i][candidate] * weight(i)
//                      + 0.05 * trendingScore(candidate)   (tie-breaker)
function recommendForUser(userId, n = 5) {
  const mine = state.events.filter((e) => e.userId === userId);
  if (mine.length === 0) {
    // Cold start: no history -> sensible popular recommendations.
    return trendingItems(n);
  }
  const strongest = new Map(); // itemId -> strongest action weight
  const purchased = new Set();
  for (const e of mine) {
    strongest.set(e.productId, Math.max(strongest.get(e.productId) || 0, ACTION_WEIGHTS[e.action]));
    if (e.action === 'purchase') purchased.add(e.productId);
  }
  const co = buildCooccurrence(state.events);
  const trend = new Map(trendingScores(state.events));
  const scored = [];
  for (const cand of CATALOG) {
    if (purchased.has(cand.id)) continue;
    let score = 0;
    let topContributor = null;
    let topContribution = 0;
    for (const [itemId, w] of strongest) {
      const c = (co.get(itemId) && co.get(itemId).get(cand.id)) || 0;
      const contrib = c * w;
      if (contrib > topContribution) { topContribution = contrib; topContributor = itemId; }
      score += contrib;
    }
    score += 0.05 * (trend.get(cand.id) || 0);
    let reason;
    if (topContributor) {
      const src = productById[topContributor];
      reason = strongest.get(topContributor) >= 5
        ? `Because you bought ${src.name} — users like you also picked this`
        : `Because you viewed ${src.name}`;
    } else {
      reason = 'Trending now';
    }
    scored.push({ product: cand, score: Math.round(score * 100) / 100, reason });
  }
  return scored.sort((a, b) => b.score - a.score).slice(0, n);
}

// ===========================================================================
// 5. JSON API
// ===========================================================================
function eventsPerSecond() {
  const cutoff = Date.now() - 10000;
  const recent = state.events.filter((e) => e.ts >= cutoff).length;
  return Math.round((recent / 10) * 10) / 10;
}

function sendJson(res, obj, status = 200) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (chunk) => { data += chunk; });
    req.on('end', () => {
      try { resolve(data ? JSON.parse(data) : {}); } catch (_) { resolve({}); }
    });
  });
}

const server = http.createServer(async (req, res) => {
  const parsed = new URL(req.url, 'http://localhost');
  const pathname = parsed.pathname;

  // Static frontend
  if (pathname === '/' || pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(fs.readFileSync(path.join(__dirname, 'public', 'index.html')));
    return;
  }

  if (pathname === '/api/status') {
    return sendJson(res, {
      running: state.streamRunning,
      speed: state.speed,
      uptimeSec: Math.round((Date.now() - state.startedAt) / 1000),
      startedAt: state.startedAt,
    });
  }

  if (pathname === '/api/control' && req.method === 'POST') {
    const body = await readBody(req);
    if (body.action === 'pause') state.streamRunning = false;
    if (body.action === 'resume') state.streamRunning = true;
    if (typeof body.speed === 'number' && body.speed > 0 && body.speed <= 10) {
      state.speed = body.speed;
      restartTimer();
    }
    saveState();
    return sendJson(res, { running: state.streamRunning, speed: state.speed });
  }

  if (pathname === '/api/events') {
    const limit = Math.min(200, Math.max(1, parseInt(parsed.searchParams.get('limit'), 10) || 50));
    const recent = state.events.slice(-limit).reverse().map((e) => ({
      ...e,
      userName: (state.users.find((u) => u.id === e.userId) || {}).name || e.userId,
      product: productById[e.productId],
    }));
    return sendJson(res, recent);
  }

  if (pathname === '/api/users') {
    if (req.method === 'POST') {
      const body = await readBody(req);
      const id = `u${state.nextUserNum++}`;
      const user = { id, name: (body.name || `User ${id}`).trim() || id, createdAt: Date.now() };
      state.users.push(user);
      saveState();
      return sendJson(res, { user, recommendations: recommendForUser(id, 5), coldStart: true }, 201);
    }
    return sendJson(res, state.users.map((u) => ({
      ...u,
      eventCount: state.events.filter((e) => e.userId === u.id).length,
    })));
  }

  if (pathname === '/api/recommendations') {
    const userId = parsed.searchParams.get('userId');
    const user = state.users.find((u) => u.id === userId);
    if (!user) return sendJson(res, { error: 'unknown user' }, 404);
    const hasHistory = state.events.some((e) => e.userId === userId);
    return sendJson(res, { user, recommendations: recommendForUser(userId, 5), coldStart: !hasHistory });
  }

  if (pathname === '/api/trending') {
    return sendJson(res, trendingItems(5));
  }

  if (pathname === '/api/metrics') {
    return sendJson(res, {
      eventsPerSec: eventsPerSecond(),
      totalEvents: state.totalEvents,
      lastModelUpdate: state.lastModelUpdate,
      userCount: state.users.length,
      catalogSize: CATALOG.length,
    });
  }

  if (pathname === '/api/simulate' && req.method === 'POST') {
    const body = await readBody(req);
    const user = state.users.find((u) => u.id === body.userId);
    if (!user || !productById[body.productId] || !ACTION_WEIGHTS[body.action]) {
      return sendJson(res, { error: 'bad userId, productId, or action' }, 400);
    }
    const ev = recordEvent(body.userId, body.productId, body.action);
    saveState();
    return sendJson(res, { event: ev, recommendations: recommendForUser(body.userId, 5) });
  }

  if (pathname === '/api/catalog') {
    return sendJson(res, CATALOG);
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found' }));
});

server.listen(PORT, () => {
  console.log(`Streaming Recommendation Engine running at http://localhost:${PORT}`);
});

function shutdown() {
  clearInterval(timer);
  saveState();
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
