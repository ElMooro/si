// Exact byte reversal of the independently reviewed watchlist changes only.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const ROOT=path.join(__dirname,'../..'),manifest=require('../fixtures/watchlist-correctness/source-transition.json'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function reverse(raw,file){const r=manifest[file];if(!r)return raw;const old=fs.readFileSync(path.join(ROOT,r.before_path),'utf8');assert.equal(hash(old),r.before_sha256);if(hash(raw)===r.before_sha256)return raw;assert.equal(hash(raw),r.after_sha256,file+' exact watchlist candidate');for(const e of [...r.edits].reverse()){assert.equal(raw.slice(e.start,e.end),e.after);raw=raw.slice(0,e.start)+e.before+raw.slice(e.end);}assert.equal(raw,old);assert.equal(hash(raw),r.before_sha256);return raw;}
module.exports={normalize:raw=>reverse(raw,'jh-chart-engine.js'),normalizeHelper:reverse,normalizeFile:reverse,manifest};
