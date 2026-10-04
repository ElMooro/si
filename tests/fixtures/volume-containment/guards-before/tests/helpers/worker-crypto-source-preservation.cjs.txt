const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const D=path.join(__dirname,'../fixtures/worker-crypto-source'),read=p=>fs.readFileSync(path.join(D,p),'utf8'),t=JSON.parse(read('transition.json')),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function normalize(raw){assert.equal(hash(raw),t.candidate_sha256);assert.equal(raw.split(t.after).length,2);raw=raw.replace(t.after,t.before);assert.equal(raw,read('predecessor/index.js'));assert.equal(hash(raw),t.source_sha256);return raw;}
module.exports={normalize};
