// Exact, independently reviewed source-text comparison; never runtime data repair.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const ROOT=path.join(__dirname,'../..'),manifest=require('../fixtures/volume-containment/source-transitions.json');
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function restore(raw,key){
 const t=manifest.sources[key];assert.ok(t,key);assert.equal(hash(raw),t.after_sha256,key+' exact reviewed containment candidate');
 for(const e of [...t.edits].reverse()){assert.equal(raw.split(e.after).length,2,key+' unique reviewed site');raw=raw.replace(e.after,e.before);}
 const old=fs.readFileSync(path.join(ROOT,t.before_path),'utf8');assert.equal(hash(old),t.before_sha256);assert.equal(raw,old);assert.equal(hash(raw),t.before_sha256);return raw;
}
module.exports={restoreWorker:raw=>restore(raw,'worker'),restoreEngine:raw=>restore(raw,'engine'),manifest,hash};
