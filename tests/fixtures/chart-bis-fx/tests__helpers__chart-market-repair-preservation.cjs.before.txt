const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'../..'),transition=require('../fixtures/chart-market-repair/transition.json'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function normalize(raw,file){
 const row=file===transition.ledger.path?transition.ledger:transition.changes[file];if(!row)return raw;
 const digest=hash(raw);
 if(digest===row.before_sha256){assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;}
 for(const name of ['chart-bis','chart-cftc','chart-provider-browser']){
  const older=require('../fixtures/'+name+'/transition.json'),previous=file===older.ledger.path?older.ledger:older.changes[file];
  if(previous&&digest===previous.before_sha256){assert.equal(raw,fs.readFileSync(path.join(R,previous.before_path),'utf8'));return raw;}
 }
 assert.equal(digest,row.after_sha256,file);
 if(row.edits)for(const e of [...row.edits].reverse()){assert.equal(raw.slice(e.start,e.end),e.after);raw=raw.slice(0,e.start)+e.before+raw.slice(e.end);}
 else raw=fs.readFileSync(path.join(R,row.before_path),'utf8');
 assert.equal(hash(raw),row.before_sha256);assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;
}
module.exports={normalize,transition};
