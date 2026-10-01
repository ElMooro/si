// Narrow normalization for preceding whole-module regression gates. Every
// intentionally changed function/statement must match its retained exact hash.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.join(__dirname,'../..'),manifest=require('../fixtures/chart-observations/source-delta.json');
const parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);
const parse=raw=>parser.exports.parse(raw,{ecmaVersion:'latest'});
const clean=tree=>JSON.parse(JSON.stringify(tree,(k,v)=>['start','end'].includes(k)?undefined:v));
function body(raw,file){const call=parse(raw).body.find(n=>n.expression?.type==='CallExpression').expression;return file==='jh-chart-stock-desk.js'?call.arguments[1].body.body:call.callee.body.body;}
const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
function normalize(raw,file){
 raw=require("./observation-cache-preservation.cjs").normalize(raw,file);
 const record=manifest.entries[file];assert.ok(record,file);
 const old=fs.readFileSync(path.join(R,record.prior.path),'utf8');assert.equal(hash(old),record.prior.sha256);
 const a=body(old,file),b=body(raw,file),oldFns=new Map(a.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,n]));
 const currentFns=new Map(b.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,n])),edits=[];
 for(const item of [...record.changed,...record.added]){
  const n=currentFns.get(item.name);assert.ok(n,item.name);assert.equal(hash(raw.slice(n.start,n.end)),item.sha256,file+':'+item.name);
  const prior=oldFns.get(item.name);edits.push({start:n.start,end:n.end,text:prior?old.slice(prior.start,prior.end):''});
 }
 const oldStatements=a.filter(n=>n.type!=='FunctionDeclaration'),statements=b.filter(n=>n.type!=='FunctionDeclaration');
 for(const item of record.outer){const n=statements[item.ordinal],p=oldStatements[item.ordinal];assert.equal(hash(raw.slice(n.start,n.end)),item.sha256);assert.equal(hash(old.slice(p.start,p.end)),item.prior_sha256);edits.push({start:n.start,end:n.end,text:old.slice(p.start,p.end)});}
 for(const edit of edits.sort((a,b)=>b.start-a.start))raw=raw.slice(0,edit.start)+edit.text+raw.slice(edit.end);
 assert.deepEqual(clean(parse(raw)),clean(parse(old)),file+': unrelated source changed');
 return raw;
}
module.exports={normalize,manifest};
