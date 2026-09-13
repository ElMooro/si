const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const crypto=require('node:crypto');
const path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../factory-desk.js'),'utf8');
const context={};vm.createContext(context);
vm.runInContext(source.slice(source.indexOf('  function checksumBody('),source.indexOf('  async function checkedState('))+'\nthis.check=checksumBody;',context);
test('state integrity preserves Python numeric tokens and nested checksum fields',()=>{
  const raw='{"agents":[{"checksum":"nested", "text":"a,b\\\"c"}],"checksum":"ROOT","fit":{"score":1.0,"small":1e-07},"gen":1}';
  const expected='{"agents":[{"checksum":"nested", "text":"a,b\\\"c"}],"fit":{"score":1.0,"small":1e-07},"gen":1}';
  assert.equal(context.check(raw),expected);
  assert.equal(crypto.createHash('sha256').update(context.check(raw)).digest('hex'),crypto.createHash('sha256').update(expected).digest('hex'));
});
test('missing or duplicate root checksum is rejected',()=>{
  assert.throws(()=>context.check('{"gen":1}'));
  assert.throws(()=>context.check('{"checksum":"one","checksum":"two","gen":1}'));
});

test('factory pane stays visible outside the collapsed engineering controls',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../ai.html'),'utf8');
  assert.ok(html.indexOf('<section id="factory-pane"') < html.indexOf('<div id="eng"'));
  assert.ok(html.includes('"/data/ai.json"'));
});
