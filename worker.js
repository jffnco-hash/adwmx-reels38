// ADWMX Reels & Media Worker — فيديوهات الريلز + ميديا الشات (فيديو/صوت/ملفات) على Cloudflare R2 لو متوصّل (binding BUCKET) وإلا على D1 (من غير R2 ولا بطاقة)، والبيانات على D1.
// الحسابات بتتأكد من السيرفر الأساسي (service binding "MAIN" ← /whoami)، فمفيش حسابات تانية هنا.
const MAX_REEL = 60 * 1024 * 1024, MAX_MEDIA = 50 * 1024 * 1024, MAX_REEL_D1 = 25 * 1024 * 1024, MAX_MEDIA_D1 = 15 * 1024 * 1024, D1_TOTAL = 400 * 1024 * 1024, MEDIA_TTL = 14 * 24 * 3600 * 1000, REELS_PER_HOUR = 10;
const CORS = { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type,x-token,range', 'access-control-allow-methods': 'GET,POST,PUT,OPTIONS', 'access-control-expose-headers': 'content-range,accept-ranges,content-length' };
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8', ...CORS } });
const clean = (v, n = 300) => String(v ?? '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, n);
const rid = () => [...crypto.getRandomValues(new Uint8Array(16))].map((x) => x.toString(16).padStart(2, '0')).join('');

const SCHEMA = [
  "CREATE TABLE IF NOT EXISTS reels (id TEXT PRIMARY KEY, owner TEXT NOT NULL, owner_name TEXT NOT NULL, caption TEXT NOT NULL DEFAULT '', key TEXT NOT NULL, mime TEXT NOT NULL, size INTEGER NOT NULL, created_at INTEGER NOT NULL, likes INTEGER NOT NULL DEFAULT 0, deleted INTEGER NOT NULL DEFAULT 0)",
  'CREATE INDEX IF NOT EXISTS reels_feed ON reels(deleted, created_at DESC)',
  'CREATE TABLE IF NOT EXISTS reel_likes (reel_id TEXT NOT NULL, phone TEXT NOT NULL, PRIMARY KEY(reel_id, phone))',
  'CREATE TABLE IF NOT EXISTS file_chunks (key TEXT NOT NULL, idx INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(key, idx))',
  'CREATE TABLE IF NOT EXISTS media (id TEXT PRIMARY KEY, owner TEXT NOT NULL, key TEXT NOT NULL, mime TEXT NOT NULL, size INTEGER NOT NULL, created_at INTEGER NOT NULL)',
];
let ready = false;
async function ensureSchema(env) { if (ready) return; await env.DB.batch(SCHEMA.map((s) => env.DB.prepare(s))); ready = true; }

// ---- هوية المستخدم من السيرفر الأساسي (كاش دقيقة)
const cache = new Map();
async function whoami(env, token) {
  if (!token) return null;
  const c = cache.get(token); if (c && c.exp > Date.now()) return c.v;
  let res;
  try {
    res = env.MAIN ? await env.MAIN.fetch(new Request('https://main.internal/whoami', { headers: { 'x-token': token } }))
                   : await fetch(String(env.MAIN_URL || '') + '/whoami', { headers: { 'x-token': token } });
  } catch { return null; }
  if (!res.ok) return null;
  const v = await res.json().catch(() => null); if (!v || !v.phone) return null;
  if (cache.size > 500) cache.clear(); cache.set(token, { v, exp: Date.now() + 60000 }); return v;
}

// ---- التخزين: R2 لو موجود، وإلا D1 (قطع BLOB ~1MB للقطعة)
const CHUNK = (env) => Number(env.CHUNK_SIZE) || 1000000;
const toU8 = (d) => d instanceof Uint8Array ? d : Array.isArray(d) ? Uint8Array.from(d) : new Uint8Array(d);
const maxReel = (env) => env.BUCKET ? MAX_REEL : MAX_REEL_D1, maxMedia = (env) => env.BUCKET ? MAX_MEDIA : MAX_MEDIA_D1;
async function delFile(env, key) { if (env.BUCKET) await env.BUCKET.delete(key); else await env.DB.prepare('DELETE FROM file_chunks WHERE key=?').bind(key).run(); }

// ---- بث مع دعم Range (عشان الفيديو يشتغل ويتقدّم)
async function serve(env, key, mime, req, size) {
  if (env.BUCKET) { const head = await env.BUCKET.head(key); if (!head) return json({ error: 'غير موجود' }, 404); size = head.size; }
  const base = { 'content-type': mime, 'accept-ranges': 'bytes', 'cache-control': 'public, max-age=86400', ...CORS };
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.get('range') || '');
  let s = 0, e = size - 1, partial = false;
  if (m && (m[1] !== '' || m[2] !== '')) {
    s = m[1] === '' ? size - Number(m[2]) : Number(m[1]); e = m[1] === '' || m[2] === '' ? size - 1 : Math.min(Number(m[2]), size - 1); if (s < 0) s = 0;
    if (s >= size || s > e) return new Response(null, { status: 416, headers: { 'content-range': `bytes */${size}`, ...CORS } });
    partial = true;
  }
  const headers = partial ? { ...base, 'content-range': `bytes ${s}-${e}/${size}`, 'content-length': String(e - s + 1) } : { ...base, 'content-length': String(size) };
  const status = partial ? 206 : 200;
  if (req.method === 'HEAD') return new Response(null, { status, headers });
  if (env.BUCKET) {
    const o = partial ? await env.BUCKET.get(key, { range: { offset: s, length: e - s + 1 } }) : await env.BUCKET.get(key);
    return new Response(o.body, { status, headers });
  }
  const cs = CHUNK(env), last = Math.floor(e / cs); let i = Math.floor(s / cs);
  const stream = new ReadableStream({
    async pull(ctrl) {
      if (i > last) { ctrl.close(); return; }
      const r = await env.DB.prepare('SELECT data FROM file_chunks WHERE key=? AND idx=?').bind(key, i).first();
      if (!r) { ctrl.error(new Error('missing chunk')); return; }
      const d = toU8(r.data), c0 = i * cs, a = Math.max(s, c0) - c0, b = Math.min(e, c0 + d.length - 1) - c0;
      ctrl.enqueue(d.subarray(a, b + 1)); i++;
    },
  });
  return new Response(stream, { status, headers });
}

async function putD1(env, body, key) {
  const cs = CHUNK(env), reader = body.getReader(); let buf = new Uint8Array(cs), fill = 0, idx = 0, total = 0;
  const flush = async (n) => { await env.DB.prepare('INSERT INTO file_chunks(key,idx,data) VALUES(?,?,?)').bind(key, idx++, buf.slice(0, n).buffer).run(); };
  try {
    for (;;) {
      const { done, value } = await reader.read(); if (done) break;
      total += value.length; let off = 0;
      while (off < value.length) { const n = Math.min(cs - fill, value.length - off); buf.set(value.subarray(off, off + n), fill); fill += n; off += n; if (fill === cs) { await flush(cs); fill = 0; } }
    }
    if (fill > 0) await flush(fill);
  } catch (err) { await delFile(env, key); throw err; }
  return total;
}

// يخزّن الجسم بشرط طول معروف وحد أقصى. يرجع {len} أو Response خطأ
async function putBody(env, req, key, mime, max) {
  const len = Number(req.headers.get('content-length') || 0);
  if (!len) return json({ error: 'لازم Content-Length' }, 411);
  if (len > max) return json({ error: 'الملف أكبر من ' + Math.round(max / 1048576) + 'MB' }, 413);
  if (env.BUCKET) {
    await env.BUCKET.put(key, req.body, { httpMetadata: { contentType: mime } });
    const h = await env.BUCKET.head(key);
    if (!h || h.size !== len) { await env.BUCKET.delete(key); return json({ error: 'الرفع ناقص، حاول تاني' }, 400); }
    return { len };
  }
  const used = await env.DB.prepare('SELECT (SELECT COALESCE(SUM(size),0) FROM reels WHERE deleted=0) + (SELECT COALESCE(SUM(size),0) FROM media) AS t').first();
  if ((used?.t || 0) + len > D1_TOTAL) return json({ error: 'مساحة التخزين امتلأت' }, 507);
  const total = await putD1(env, req.body, key);
  if (total !== len) { await delFile(env, key); return json({ error: 'الرفع ناقص، حاول تاني' }, 400); }
  return { len };
}
const okMime = (m, pre) => { m = clean(m, 80).split(';')[0].toLowerCase(); return m.startsWith(pre) ? m : pre + (pre === 'video/' ? 'mp4' : 'octet-stream'); };

export default {
  async fetch(req, env) {
    const u = new URL(req.url), p = u.pathname.replace(/\/+$/, '') || '/', M = req.method;
    if (M === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
    try {
      await ensureSchema(env);
      if (p === '/' || p === '/ping') { let r2 = false; try { if (env.BUCKET) { await env.BUCKET.head('x'); r2 = true; } else await env.DB.prepare('SELECT COUNT(*) c FROM file_chunks').first(); } catch { return json({ ok: false, service: 'ADWMX Reels', db: true, storage: env.BUCKET ? 'r2' : 'd1', r2: false }); } return json({ ok: true, service: 'ADWMX Reels', db: true, storage: env.BUCKET ? 'r2' : 'd1', r2 }); }

      // ---- تحميل عام (الـ id عشوائي 128 بت ومش بيتعرف إلا من الفيد/الرسالة)
      let m = /^\/reels\/video\/([0-9a-f]{32})$/.exec(p);
      if (m && (M === 'GET' || M === 'HEAD')) { const r = await env.DB.prepare('SELECT key,mime,size FROM reels WHERE id=? AND deleted=0').bind(m[1]).first(); return r ? serve(env, r.key, r.mime, req, r.size) : json({ error: 'غير موجود' }, 404); }
      m = /^\/media\/([0-9a-f]{32})$/.exec(p);
      if (m && (M === 'GET' || M === 'HEAD')) { const r = await env.DB.prepare('SELECT key,mime,size FROM media WHERE id=?').bind(m[1]).first(); return r ? serve(env, r.key, r.mime, req, r.size) : json({ error: 'الملف انتهت صلاحيته أو غير موجود' }, 404); }

      // ---- من هنا لازم تسجيل دخول
      const me = await whoami(env, req.headers.get('x-token') || '');
      if (!me) return json({ error: 'يلزم تسجيل الدخول' }, 401);

      if (p === '/reels/upload' && M === 'PUT') {
        const recent = await env.DB.prepare('SELECT COUNT(*) c FROM reels WHERE owner=? AND created_at>?').bind(me.phone, Date.now() - 3600000).first();
        if (recent.c >= REELS_PER_HOUR && !me.dev) return json({ error: 'وصلت الحد الأقصى للنشر في الساعة، جرّب بعد شوية' }, 429);
        const id = rid(), key = 'reels/' + id, mime = okMime(req.headers.get('content-type'), 'video/');
        const r = await putBody(env, req, key, mime, maxReel(env)); if (r instanceof Response) return r;
        await env.DB.prepare('INSERT INTO reels(id,owner,owner_name,caption,key,mime,size,created_at) VALUES(?,?,?,?,?,?,?,?)').bind(id, me.phone, clean(me.name, 40) || me.phone, clean(u.searchParams.get('caption'), 200), key, mime, r.len, Date.now()).run();
        return json({ ok: true, id });
      }
      if (p === '/reels/feed') {
        const before = Number(u.searchParams.get('before')) || Date.now() + 1, limit = Math.min(20, Math.max(1, Number(u.searchParams.get('limit')) || 8));
        const q = await env.DB.prepare('SELECT r.*, EXISTS(SELECT 1 FROM reel_likes l WHERE l.reel_id=r.id AND l.phone=?) AS liked FROM reels r WHERE r.deleted=0 AND r.created_at<? ORDER BY r.created_at DESC LIMIT ?').bind(me.phone, before, limit).all();
        const rows = q.results || [];
        return json({ reels: rows.map((r) => ({ id: r.id, name: r.owner_name, caption: r.caption, size: r.size, ts: r.created_at, likes: r.likes, liked: !!r.liked, mine: r.owner === me.phone, can_delete: r.owner === me.phone || !!me.dev, url: u.origin + '/reels/video/' + r.id })), next: rows.length === limit ? rows[rows.length - 1].created_at : null });
      }
      if (p === '/reels/like' && M === 'POST') {
        const b = await req.json().catch(() => ({})), id = clean(b.id, 40);
        const r = await env.DB.prepare('SELECT id FROM reels WHERE id=? AND deleted=0').bind(id).first(); if (!r) return json({ error: 'الريل مش موجود' }, 404);
        const had = await env.DB.prepare('SELECT 1 x FROM reel_likes WHERE reel_id=? AND phone=?').bind(id, me.phone).first();
        if (had) { await env.DB.prepare('DELETE FROM reel_likes WHERE reel_id=? AND phone=?').bind(id, me.phone).run(); await env.DB.prepare('UPDATE reels SET likes=MAX(0,likes-1) WHERE id=?').bind(id).run(); }
        else { await env.DB.prepare('INSERT OR IGNORE INTO reel_likes(reel_id,phone) VALUES(?,?)').bind(id, me.phone).run(); await env.DB.prepare('UPDATE reels SET likes=likes+1 WHERE id=?').bind(id).run(); }
        const n = await env.DB.prepare('SELECT likes FROM reels WHERE id=?').bind(id).first(); return json({ ok: true, liked: !had, likes: n.likes });
      }
      if (p === '/reels/delete' && M === 'POST') {
        const b = await req.json().catch(() => ({})), id = clean(b.id, 40);
        const r = await env.DB.prepare('SELECT owner,key FROM reels WHERE id=? AND deleted=0').bind(id).first(); if (!r) return json({ error: 'الريل مش موجود' }, 404);
        if (r.owner !== me.phone && !me.dev) return json({ error: 'مش مسموح' }, 403);
        await env.DB.prepare('UPDATE reels SET deleted=1 WHERE id=?').bind(id).run(); await delFile(env, r.key); return json({ ok: true });
      }
      if (p === '/media/upload' && M === 'PUT') {
        const id = rid(), key = 'media/' + id, mime = clean(req.headers.get('content-type'), 80).split(';')[0].toLowerCase() || 'application/octet-stream';
        const r = await putBody(env, req, key, mime, maxMedia(env)); if (r instanceof Response) return r;
        await env.DB.prepare('INSERT INTO media(id,owner,key,mime,size,created_at) VALUES(?,?,?,?,?,?)').bind(id, me.phone, key, mime, r.len, Date.now()).run();
        return json({ ok: true, id, url: u.origin + '/media/' + id, size: r.len });
      }
      return json({ error: 'المسار غير موجود' }, 404);
    } catch (e) { return json({ error: env.DEBUG === 'true' ? String(e && e.message || e) : 'خطأ في السيرفر' }, 500); }
  },
  // تنضيف يومي: ميديا الشات أقدم من 14 يوم بتتمسح (الريلز بتفضل)
  async scheduled(_e, env) {
    await ensureSchema(env);
    const old = await env.DB.prepare('SELECT id,key FROM media WHERE created_at<? LIMIT 500').bind(Date.now() - MEDIA_TTL).all();
    for (const r of old.results || []) { try { await delFile(env, r.key); } catch {} await env.DB.prepare('DELETE FROM media WHERE id=?').bind(r.id).run(); }
  },
};
