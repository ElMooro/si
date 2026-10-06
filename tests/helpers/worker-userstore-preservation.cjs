// 2026-10-06 layer: maps the current data-proxy worker back to the byte-exact state the worker-oecd-path layer certifies.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.join(__dirname,'../..'),manifest=require('../fixtures/worker-userstore/transition.json'),hash=x=>crypto.createHash('sha256').update(x).digest('hex');
function normalize(raw,file){const t=manifest.entries[file];if(!t)return raw;assert.equal(hash(raw),t.after_sha256,file);for(const e of [...t.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file);raw=raw.replace(e.after,e.before);}assert.equal(hash(raw),t.before_sha256,file);assert.equal(raw,fs.readFileSync(path.join(R,t.before_path),'utf8'),file);return raw;}
module.exports={normalize,manifest};
