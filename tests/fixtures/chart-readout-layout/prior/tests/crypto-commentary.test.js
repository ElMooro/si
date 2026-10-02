const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict'),test=require('node:test'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),D=path.join(R,'tests/fixtures/crypto-commentary'),html=fs.readFileSync(path.join(R,'crypto/index.html'),'utf8');
const source=html.slice(html.indexOf('// Typed commentary presentation:'),html.indexOf('function render(){'));
const ctx={};vm.runInNewContext(source,ctx);const render=ctx.renderCryptoCommentary;
const prior=fs.readFileSync(path.join(D,'predecessor.html.txt'),'utf8'),start=prior.indexOf('// ========== AI INTEL =========='),end=prior.indexOf('// ========== STABLECOINS',start);const old={};vm.runInNewContext('function legacy(ai){var h="";\n'+prior.slice(start,end)+'return h;}',old);
const valid={status:'ok',analysis:'**Invented** research\nSecond line',model:'Invented model',generated_at:'2020-01-01T00:00:00Z',system_sources:['Invented ledger']};
test('predecessor injects markup and crashes on malformed commentary',()=>{
 assert.match(old.legacy({...valid,analysis:'<img src=x onerror=alert(1)>'}),/<img src=x onerror=/);
 assert.throws(()=>old.legacy({...valid,analysis:17}));assert.throws(()=>old.legacy({...valid,system_sources:'bad'}));assert.throws(()=>old.legacy({...valid,generated_at:{}}));
});
test('every packet text field is inert before emphasis formatting',()=>{
 const payload='<img src=x onerror=alert(1)><script>alert(2)</script>&"\'';
 const out=render({...valid,analysis:'**'+payload+'**',model:payload,system_sources:[payload]});
 assert.ok(!/<(?:img|script)\b/i.test(out));assert.match(out,/<strong>&lt;img/);assert.match(out,/&amp;&quot;&#39;/);
 assert.ok(!/<img/.test(render({error:payload})));
});
test('typed missing or malformed fields never abort a whole-page render',()=>{
 for(const x of [null,undefined,[],false,'text',{}, {status:'ok',analysis:1}, {status:'ok',analysis:[]}, {status:'ok',analysis:' '},{status:'ok',analysis:'x'.repeat(100001)}])assert.match(render(x),/unavailable/i);
 for(const x of [1,{},[],true])assert.doesNotThrow(()=>render({...valid,generated_at:x,model:x,system_sources:x}));
});
test('only safe emphasis and line breaks survive, content remains complete',()=>{
 const out=render(valid);assert.match(out,/<strong>Invented<\/strong> research<br>Second line/);assert.match(out,/portfolio suitability are unverified/);assert.match(out,/Reported generation time/);
 assert.ok(!/Wyckoff \+ Predictions/.test(out));
});
test('source list is typed as a whole and no subset implies complete sourcing',()=>{
 for(const sources of [['safe',null],['safe',4],Array(101).fill('safe'),['x'.repeat(257)]])assert.match(render({...valid,system_sources:sources}),/Reported sources: unavailable/);
});
test('unavailable report cannot leak cached model text through ok-like states',()=>{
 for(const status of ['unavailable','error',null,{},true])assert.ok(!render({...valid,status}).includes('Second line'));
});
test('reported time requires a real calendar date and explicit timezone',()=>{
 for(const generated_at of ['2026-02-30T12:00:00Z','2026-01-01T24:00:00Z','2026-01-01T12:00:00','2026-01-01T12:00:00+24:00'])assert.match(render({...valid,generated_at}),/Reported generation time: <strong>Unavailable/);
 for(const generated_at of ['2024-02-29T12:00:00Z','2026-01-01T12:00:00.123456+05:30'])assert.ok(render({...valid,generated_at}).includes(generated_at));
});
test('complete predecessor and all unrelated page bytes are preserved',()=>{
 const p=JSON.parse(fs.readFileSync(path.join(D,'edits.json'))),raw=fs.readFileSync(path.join(D,'predecessor.html.txt'));
 assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),p.predecessor_sha256);let s=raw.toString();
 for(const [a,b] of p.edits){assert.equal(s.split(a).length-1,1);s=s.replace(a,b);}assert.equal(s,html);assert.equal(crypto.createHash('sha256').update(s).digest('hex'),p.candidate_sha256);
});

test('earlier full-page gates retain exact preservation through only this reviewed repair',()=>{
 const helper=require('./helpers/chart-refresh-status-preservation.cjs');assert.equal(helper.normalizeHtml(html,'crypto/index.html'),prior);
 assert.throws(()=>helper.normalizeHtml(html.replace('function pct(v)','function changedPct(v)'),'crypto/index.html'));
 for(const [planName,archive] of [['preservation-helper-edit.json','preservation-helper.before.cjs.txt'],['parent-preservation-test-edit.json','parent-preservation-test.before.js.txt']]){
 const plan=JSON.parse(fs.readFileSync(path.join(D,planName))),raw=fs.readFileSync(path.join(D,archive));
 assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),plan.predecessor_sha256);let expected=raw.toString();
 for(const [a,b] of plan.edits){assert.equal(expected.split(a).length-1,1);expected=expected.replace(a,b);}
 assert.equal(fs.readFileSync(path.join(R,plan.target),'utf8'),expected);
 }
});
