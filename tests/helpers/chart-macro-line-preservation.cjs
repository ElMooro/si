// Exact byte reversal of the 2026-10-05 macro line/candles/change-modes, auto-hide workspace and file-import change only.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'../..'),transition=require('../fixtures/chart-macro-line/transition.json'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function normalize(raw,file){const row=transition.changes[file];if(!row)return normalizeLegacyTest(raw,file);const digest=hash(raw);
 if(digest===row.before_sha256||digest!==row.after_sha256)return raw;
 for(const e of [...row.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file+': '+e.after.slice(0,60));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),row.before_sha256,file);assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;
}
// The approved behaviour change (points → line/candles) rewrote three scalar tests; earlier gates see their exact prior bytes.
const adaptations=require('../fixtures/chart-macro-line/test-adaptations.json');
function normalizeLegacyTest(raw,file){const row=adaptations[file];if(!row||hash(raw)!==row.after_sha256)return raw;
 const old=fs.readFileSync(path.join(R,row.before_path),'utf8');assert.equal(hash(old),row.before_sha256,file);return old;}
module.exports={normalize,normalizeLegacyTest,transition,adaptations};
