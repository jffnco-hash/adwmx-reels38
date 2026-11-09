// node check.mjs [الرابط]
const base = process.argv[2] || 'https://adwmx-reels38.jffnco.workers.dev';
const r = await fetch(base + '/ping'); const t = await r.text(); console.log(r.status, t);
try { const j = JSON.parse(t); console.log(j.ok && j.db ? '✅ شغّال (تخزين: ' + (j.storage || 'r2').toUpperCase() + ')' : '❌ فيه مشكلة: ' + t); } catch { console.log('❌ الرد مش JSON'); }
