const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'../..'),transition=require('../fixtures/chart-cftc/transition.json'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function normalize(raw,file){
 const ledger=transition.ledger;
 if(file===ledger.path){if(hash(raw)===ledger.before_sha256)return raw;assert.equal(hash(raw),ledger.after_sha256);const old=fs.readFileSync(path.join(R,ledger.before_path),'utf8');assert.equal(hash(old),ledger.before_sha256);return old;}
 const row=transition.changes[file];if(!row||hash(raw)===row.before_sha256)return raw;
 assert.equal(hash(raw),row.after_sha256);for(const e of [...row.edits].reverse()){assert.equal(raw.slice(e.start,e.end),e.after);raw=raw.slice(0,e.start)+e.before+raw.slice(e.end);}
 assert.equal(hash(raw),row.before_sha256);assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;
}
module.exports={normalize,transition};
