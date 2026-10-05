const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'../..'),transition=require('../fixtures/chart-census-additional/transition.json'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function normalize(raw,file){const row=transition.changes[file];if(!row)return raw;const digest=hash(raw);if(digest===row.before_sha256)return raw;
for(const name of ['chart-census-watchlist','chart-census','chart-oecd-additional','chart-oecd','chart-bis-monthly','chart-bis-fx','chart-market-repair','chart-bis','chart-cftc','chart-provider-browser']){const t=require('../fixtures/'+name+'/transition.json'),r=t.changes[file];if(r&&[r.before_sha256,r.after_sha256].includes(digest))return raw;}
assert.equal(digest,row.after_sha256,file==="jh-chart-tvwatch.js"?file+" exact watchlist candidate":file);const before=fs.readFileSync(path.join(R,row.before_path),'utf8');assert.equal(hash(before),row.before_sha256);
if(row.mode==='catalogue_additions'){const c=JSON.parse(raw),b=JSON.parse(before),added=new Set(row.added_datasets);for(const k of Object.keys(c.series)){if(added.has(c.series[k].dataset))delete c.series[k];}for(const k of added)delete c.dataset_definitions[k];assert.deepEqual(c,b);return before;}
for(const e of [...row.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file);raw=raw.replace(e.after,e.before);}assert.equal(hash(raw),row.before_sha256,file);assert.equal(raw,before);return raw;}
module.exports={normalize,transition};
