// Preserve the prior HTML proof across one exact peer script insertion.
// This does not validate the new script's runtime behavior or financial meaning.
const assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path');
const D=path.join(__dirname,'../fixtures/ticker-lookup/incoming-chart'),plan=require(path.join(D,'transition.json'));
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function normalize(raw){
 if(!raw.includes(plan.added))return raw;
 const source='/jh-khalid-sniper.js?v=20261001-user-scope',legacy='/jh-khalid-sniper.js?v=20261001-snapshot';
 const canonical=raw.replace(legacy,source);assert.equal(hash(canonical),plan.after_sha256);
 assert.equal(canonical,fs.readFileSync(path.join(D,'chart-after.html.txt'),'utf8'));assert.equal(raw.split(plan.added).length,2);
 const prior=canonical.replace(plan.added,'');assert.equal(hash(prior),plan.before_sha256);assert.equal(prior,fs.readFileSync(path.join(D,'chart-before.html.txt'),'utf8'));
 return raw.replace(plan.added,'');
}
module.exports={normalize};
