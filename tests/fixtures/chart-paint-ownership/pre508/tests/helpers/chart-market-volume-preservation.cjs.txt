// Normalize only hash-bound, reviewed changes to the complete preceding modules.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.join(__dirname,'../..'),manifest=require('../fixtures/chart-market-volume/source-delta.json');
const parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);
const parse=raw=>parser.exports.parse(raw,{ecmaVersion:'latest'}),hash=x=>crypto.createHash('sha256').update(x).digest('hex');
const clean=tree=>JSON.parse(JSON.stringify(tree,(k,v)=>['start','end'].includes(k)?undefined:v));
function body(raw,file){const call=parse(raw).body.find(n=>n.expression?.type==='CallExpression').expression;return ['jh-stock-desk-research.js','jh-chart-stock-desk.js','jh-observation-series.js','jh-observation-cache.js'].includes(file)?call.arguments[1].body.body:call.callee.body.body;}
function normalize(raw,file){
 const record=manifest.entries[file];assert.ok(record,file);const old=fs.readFileSync(path.join(R,record.prior.path),'utf8');assert.equal(hash(old),record.prior.sha256);
 const a=body(old,file),b=body(raw,file),oldFns=new Map(a.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,n]));
 const newFns=new Map(b.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,n])),edits=[];
 for(const item of [...record.changed,...record.added]){const n=newFns.get(item.name);assert.ok(n,item.name);assert.equal(hash(raw.slice(n.start,n.end)),item.sha256,file+':'+item.name);const before=oldFns.get(item.name);edits.push({start:n.start,end:n.end,text:before?old.slice(before.start,before.end):''});}
 const outerOld=a.filter(n=>n.type!=='FunctionDeclaration'),outerNew=b.filter(n=>n.type!=='FunctionDeclaration');
 assert.deepEqual(outerOld.map(n=>({sha256:hash(old.slice(n.start,n.end))})),record.outer.prior);
 assert.deepEqual(outerNew.map(n=>({sha256:hash(raw.slice(n.start,n.end))})),record.outer.current,file+' outer');
 // Exact complete top-level statements are retained; replacing their serialization
 // here permits only this manifest's reviewed exports/constants/curated labels.
 outerNew.forEach((n,i)=>edits.push({start:n.start,end:n.end,text:i===0?outerOld.map(x=>old.slice(x.start,x.end)).join('\n'):''}));
 for(const edit of edits.sort((a,b)=>b.start-a.start))raw=raw.slice(0,edit.start)+edit.text+raw.slice(edit.end);
 // Top-level initialization must still have exactly the predecessor's semantics;
 // compare functions and outer statements separately, since normalization groups
 // the original initialization statements at the start for this comparison only.
 const after=body(raw,file),sort=nodes=>({functions:nodes.filter(n=>n.type==='FunctionDeclaration').map(clean),outer:nodes.filter(n=>n.type!=='FunctionDeclaration').map(clean)});
 assert.deepEqual(sort(after),sort(a),file+': unrelated source changed');
 // Feed the literal predecessor to earlier preservation gates after validating
 // every unrelated function and every exact reviewed statement above.
 return old;
}
module.exports={normalize,manifest};
