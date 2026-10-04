// Exact source-only inverse; never a runtime cache or quantity repair.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const ROOT=path.join(__dirname,'../..'),manifest=require('../fixtures/chart-cache-transition/source-transition.json');
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function restore(raw,t,allowPrior){
 const old=fs.readFileSync(path.join(ROOT,t.before_path),'utf8');assert.equal(hash(old),t.before_sha256);
 if(allowPrior&&hash(raw)===t.before_sha256)return raw;
 assert.equal(hash(raw),t.after_sha256,t.path+' exact reviewed cache transition');
 for(const e of [...t.edits].reverse()){assert.equal(raw.split(e.after).length,2);raw=raw.replace(e.after,e.before);}
 assert.equal(raw,old);assert.equal(hash(raw),t.before_sha256);return raw;
}
function normalizeReader(raw,file){const t=Object.values(manifest.hooks).find(t=>t.path===file);return t?restore(raw,t,false):raw;}
module.exports={restoreHtml:raw=>restore(raw,manifest.sources.html,true),restoreHook:(raw,key)=>restore(raw,manifest.hooks[key],false),normalizeReader,manifest,hash};
