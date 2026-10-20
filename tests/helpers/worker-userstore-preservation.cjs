// 2026-10-06 layer: maps the current data-proxy worker back to the byte-exact state the worker-oecd-path layer certifies.
// 2026-10-14: peel the heatmap 1y Yahoo range so nowarehouse no longer forces max history.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.join(__dirname,'../..'),manifest=require('../fixtures/worker-userstore/transition.json'),hash=x=>crypto.createHash('sha256').update(x).digest('hex');
function peelHeatRange(raw,file){
 const AFTER='4c4c7c9bb4ebec4bea6a733c722bda35991e8ea1ac3500d9a35d1dcbd41b4c99';
 const BEFORE='128f5848c65c334aaddc03b668de0f0a025a7b14fb69e6a00ac555a42dae1015';
 if(file!=='cloudflare/workers/justhodl-data-proxy/src/index.js'||hash(raw)!==AFTER)return raw;
 const from='        const yUrl = range === "max"\n';
 const to='        const yUrl = range === "max" || forceYahoo\n';
 assert.equal(raw.split(from).length,2,'yf range');
 raw=raw.replace(from,to);
 assert.equal(hash(raw),BEFORE,file);
 return raw;
}
// 2026-10-20 (ops 6551): peel the revalidated-edge-copy change (hunks and hashes live in fixtures/worker-edge-revalidate).
function peelEdgeRevalidate(raw,file){
 const t=require('../fixtures/worker-edge-revalidate/transition.json');
 if(file!==t.path||hash(raw)!==t.after_sha256)return raw;
 for(const e of [...t.replacements].reverse()){assert.equal(raw.split(e.after).length,2,'edge revalidate hunk');raw=raw.replace(e.after,()=>e.before);}
 assert.equal(hash(raw),t.before_sha256,file);
 return raw;
}
function normalize(raw,file){raw=peelEdgeRevalidate(raw,file);raw=peelHeatRange(raw,file);const t=manifest.entries[file];if(!t)return raw;assert.equal(hash(raw),t.after_sha256,file);for(const e of [...t.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file);raw=raw.replace(e.after,e.before);}assert.equal(hash(raw),t.before_sha256,file);assert.equal(raw,fs.readFileSync(path.join(R,t.before_path),'utf8'),file);return raw;}
module.exports={normalize,manifest};
