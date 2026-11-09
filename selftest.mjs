import { DatabaseSync } from 'node:sqlite';
import worker from './worker.js';
const sqlite = new DatabaseSync(':memory:');
const fix = (a) => a.map((x) => x instanceof ArrayBuffer ? Buffer.from(x) : x);
const mk = (sql, args = []) => ({ bind: (...a) => mk(sql, fix(a)), first: async () => sqlite.prepare(sql).get(...args) ?? null, all: async () => ({ results: sqlite.prepare(sql).all(...args) }), run: async () => { const r = sqlite.prepare(sql).run(...args); return { meta: { changes: r.changes } }; } });
const D1MODE = process.env.MODE === 'd1';
const store = new Map();
const BUCKET = {
  put: async (k, body, o) => { const buf = Buffer.from(await new Response(body).arrayBuffer()); store.set(k, { buf, type: o?.httpMetadata?.contentType }); },
  head: async (k) => store.has(k) ? { size: store.get(k).buf.length } : null,
  get: async (k, o) => { const e = store.get(k); if (!e) return null; const b = o?.range ? e.buf.subarray(o.range.offset, o.range.offset + o.range.length) : e.buf; return { body: new Response(b).body }; },
  delete: async (k) => { store.delete(k); },
};
const users = { tokA: { phone: '01011112222', name: 'Ahmed', dev: true }, tokB: { phone: '01099998888', name: 'Bob', dev: false }, tokC: { phone: '01055556666', name: 'Eve', dev: false } };
const MAIN = { fetch: async (req) => { const t = req.headers.get('x-token'); return users[t] ? new Response(JSON.stringify(users[t])) : new Response('{}', { status: 401 }); } };
const env = { DB: { prepare: (s) => mk(s), batch: async (qs) => { for (const q of qs) await q.run(); return []; } }, BUCKET, MAIN, DEBUG: 'true' };
if (D1MODE) { delete env.BUCKET; env.CHUNK_SIZE = 1000; }
console.log('== mode:', D1MODE ? 'D1' : 'R2');
const call = async (path, { method = 'GET', body, token, headers = {} } = {}) => {
  const h = { ...headers }; if (token) h['x-token'] = token;
  if (body && !(typeof body === 'string')) { h['content-type'] = h['content-type'] || 'application/json'; }
  const raw = body === undefined ? undefined : (Buffer.isBuffer(body) ? body : (typeof body === 'string' ? body : JSON.stringify(body)));
  if (Buffer.isBuffer(raw)) h['content-length'] = String(raw.length);
  const r = await worker.fetch(new Request('https://reels.test' + path, { method, headers: h, body: raw }), env);
  return r;
};
let fails = 0; const ok = (n, c, x = '') => { console.log((c ? 'PASS ' : 'FAIL ') + n + (c ? '' : '  ' + x)); if (!c) fails++; };
const J = async (r) => r.json();

let r = await call('/ping'); let j = await J(r); ok('ping storage', j.ok && j.db && j.storage === (D1MODE ? 'd1' : 'r2'), JSON.stringify(j));
r = await call('/reels/feed'); ok('feed requires login', r.status === 401);
r = await call('/reels/feed', { token: 'bad' }); ok('bad token rejected', r.status === 401);

const video = Buffer.from(Array.from({ length: 3000 }, (_, i) => i % 251));
r = await call('/reels/upload?caption=' + encodeURIComponent('أول ريل'), { method: 'PUT', token: 'tokB', body: video, headers: { 'content-type': 'video/mp4' } });
j = await J(r); ok('upload reel', r.status === 200 && /^[0-9a-f]{32}$/.test(j.id), JSON.stringify(j)); const id = j.id;
r = await call('/reels/upload', { method: 'PUT', token: 'tokB', body: Buffer.alloc(61 * 1024 * 1024) , headers: { 'content-type': 'video/mp4' } });
ok('reel over 60MB rejected', r.status === 413, String(r.status));

r = await call('/reels/feed', { token: 'tokC' }); j = await J(r);
ok('feed shows reel', j.reels.length === 1 && j.reels[0].caption === 'أول ريل' && j.reels[0].name === 'Bob' && !j.reels[0].can_delete && j.reels[0].liked === false, JSON.stringify(j));
const url = new URL(j.reels[0].url);
r = await call(url.pathname); const full = Buffer.from(await r.arrayBuffer());
ok('public video download (no login)', r.status === 200 && full.equals(video) && r.headers.get('accept-ranges') === 'bytes');
r = await call(url.pathname, { headers: { range: 'bytes=100-199' } }); const part = Buffer.from(await r.arrayBuffer());
ok('range request 206', r.status === 206 && part.equals(video.subarray(100, 200)) && r.headers.get('content-range') === 'bytes 100-199/3000', r.headers.get('content-range'));
r = await call(url.pathname, { headers: { range: 'bytes=900-2100' } }); const cross = Buffer.from(await r.arrayBuffer());
ok('range across chunks', r.status === 206 && cross.equals(video.subarray(900, 2101)));
r = await call(url.pathname, { headers: { range: 'bytes=2900-' } }); const tail = Buffer.from(await r.arrayBuffer());
ok('open-ended range', r.status === 206 && tail.equals(video.subarray(2900)));
r = await call(url.pathname, { headers: { range: 'bytes=-50' } }); const suf = Buffer.from(await r.arrayBuffer());
ok('suffix range', r.status === 206 && suf.equals(video.subarray(2950)));
r = await call(url.pathname, { headers: { range: 'bytes=9999-' } }); ok('unsatisfiable range 416', r.status === 416);

r = await call('/reels/like', { method: 'POST', token: 'tokC', body: { id } }); j = await J(r); ok('like', j.liked === true && j.likes === 1, JSON.stringify(j));
r = await call('/reels/like', { method: 'POST', token: 'tokA', body: { id } }); j = await J(r); ok('second user like', j.likes === 2);
r = await call('/reels/like', { method: 'POST', token: 'tokC', body: { id } }); j = await J(r); ok('unlike', j.liked === false && j.likes === 1, JSON.stringify(j));
r = await call('/reels/feed', { token: 'tokA' }); j = await J(r); ok('dev can_delete + liked flag', j.reels[0].can_delete === true && j.reels[0].liked === true);

r = await call('/reels/delete', { method: 'POST', token: 'tokC', body: { id } }); ok('stranger cannot delete', r.status === 403);
r = await call('/reels/delete', { method: 'POST', token: 'tokA', body: { id } }); ok('developer deletes', r.status === 200);
r = await call(url.pathname); ok('deleted reel gone', r.status === 404 && (D1MODE ? sqlite.prepare('SELECT COUNT(*) c FROM file_chunks WHERE key=?').get('reels/' + id).c === 0 : !store.has('reels/' + id)));
r = await call('/reels/feed', { token: 'tokB' }); j = await J(r); ok('feed empty after delete', j.reels.length === 0);

// rate limit: 10/hour for non-dev
let last; for (let i = 0; i < 11; i++) last = await call('/reels/upload', { method: 'PUT', token: 'tokC', body: Buffer.alloc(10, 1), headers: { 'content-type': 'video/mp4' } });
ok('rate limit after 10 reels', last.status === 429, String(last.status));

// chat media
const voice = Buffer.alloc(5000, 9);
r = await call('/media/upload', { method: 'PUT', token: 'tokB', body: voice, headers: { 'content-type': 'audio/mp4' } }); j = await J(r);
ok('media upload', r.status === 200 && j.url.startsWith('https://reels.test/media/'), JSON.stringify(j));
r = await call(new URL(j.url).pathname, { headers: { range: 'bytes=0-9' } }); ok('media range', r.status === 206 && r.headers.get('content-type') === 'audio/mp4');
r = await call('/media/upload', { method: 'PUT', body: voice, headers: { 'content-type': 'audio/mp4' } }); ok('media upload needs login', r.status === 401);
r = await call('/media/upload', { method: 'PUT', token: 'tokB', body: Buffer.alloc(51 * 1024 * 1024), headers: { 'content-type': 'video/mp4' } }); ok('media over limit rejected', r.status === 413);
// cleanup cron
sqlite.prepare('UPDATE media SET created_at=1').run(); await worker.scheduled({}, env);
ok('cron removes old media', sqlite.prepare('SELECT COUNT(*) c FROM media').get().c === 0 && (D1MODE ? sqlite.prepare("SELECT COUNT(*) c FROM file_chunks WHERE key LIKE 'media/%'").get().c === 0 : ![...store.keys()].some((k) => k.startsWith('media/'))));
console.log(fails ? `\n${fails} FAILED` : '\nALL PASSED'); process.exit(fails ? 1 : 0);
