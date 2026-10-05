const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'../..'),transition=require('../fixtures/chart-defillama/transition.json'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function normalize(raw,file){const row=transition.changes[file];if(!row)return raw;const digest=hash(raw);
 if(digest===row.before_sha256)return raw;
 if(digest!==row.after_sha256){
  // Only byte-for-byte retained earlier revisions may pass through the chain.
  const retained=transition.predecessor_hashes[file]||[];assert.ok(retained.includes(digest),file==='jh-chart-tvwatch.js'?file+' exact watchlist candidate':file+' unreviewed source');return raw;
 }
 for(const e of [...row.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file);raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),row.before_sha256,file);assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;
}
module.exports={normalize,transition};
