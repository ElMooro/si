// Permit only the exact reviewed Squeeze renderer change before older gates.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.join(__dirname,'../..');
function normalize(raw,file){
 if(!['jh-chart-tvsearch.js','tests/warehouse-observation-preservation.test.js'].includes(file))return raw;
 const item=require('../fixtures/squeeze-research/preservation.json').files[file];
 const old=fs.readFileSync(path.join(R,item.predecessor),'utf8');
 assert.equal(Buffer.byteLength(old),item.bytes);assert.equal(crypto.createHash('sha256').update(old).digest('hex'),item.sha256);
 if(raw===old)return old;
 let expected=old;for(const [before,after]of item.edits){assert.equal(expected.split(before).length,2);expected=expected.replace(before,after);}
 assert.equal(raw,item.prefix+expected+item.suffix,file+': unexpected source outside the reviewed Squeeze edit');
 return old;
}
function normalizeTest(raw,file){
 return file==='tests/warehouse-observation-preservation.test.js'?normalize(raw,file):raw;
}
module.exports={normalize,normalizeTest};
