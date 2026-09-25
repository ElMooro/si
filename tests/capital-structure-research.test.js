const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const view=require('../jh-capital-structure-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/capital-structure-browser.json'),'utf8'));
const clone=()=>structuredClone(fixture.files);
function fetcher(files,seen=[]){return async url=>{seen.push(url);const body=files[url.slice(1)];return {ok:body!==undefined,status:body===undefined?404:200,arrayBuffer:async()=>new TextEncoder().encode(body).buffer};};}

test('real compiler output loads by current and recorded run; every original row remains inspectable',async()=>{
 const f=fetcher(clone()),state=await view.load(f),r=await view.record(f,state,'ABC');
 assert.equal(state.packet.reported_names,1);assert.equal(r.shard.source_row_count,7);assert.equal(r.shard.records.length,2);assert.equal(r.shard.snapshots.length,3);
 const same=await view.load(f,state.runId);assert.deepEqual(same.packet,state.packet);
 const html=view.renderRecord(r);assert.match(html,/Cash paid for common-stock repurchases/);assert.match(html,/USD/);assert.match(html,/source_row/);assert.match(html,/floatShares/);assert.match(html,/2026-03-31/);assert.match(html,/commonStockIssuance/);
});

test('legacy scores, corrupted bytes, unknown compiler and arbitrary artifact paths are rejected',async()=>{
 const files=clone();files['data/share-flows.json']=JSON.stringify({tickers:{ABC:{read:'BUYBACK_HEAVY'}},call:'LONG'});
 await assert.rejects(view.load(fetcher(files)),/not yet qualified/);
 for(const key of [fixture.reference.manifest_key,JSON.parse(fixture.files[fixture.reference.manifest_key]).output.key]){const copy=clone();copy[key]+=' ';await assert.rejects(view.load(fetcher(copy)),/identity|bytes/);}
 for(const key of ['https://example.com/x','data/portfolio.json','data/capital-structure-research/runs/../current.json'])await assert.rejects(view.get(fetcher(clone()),key),/path/);
 await assert.rejects(view.load(fetcher(clone()),'../../private'),/Invalid recorded run/);
 const state=await view.load(fetcher(clone()));await assert.rejects(view.record(fetcher(clone()),state,'NOT_IN_POPULATION'),/exact symbol/);
 const p=JSON.parse(fixture.files['data/share-flows.json']);p.calls_eligible=true;assert.equal(view.valid(p),false);
 const changed=clone(),run=JSON.parse(changed[fixture.reference.manifest_key]);run.compilers.capital_structure_source.sha256='0'.repeat(64);
 const text=view.canonical(run),digest=crypto.createHash('sha256').update(text).digest('hex');changed['data/capital-structure-research/runs/'+digest+'.json']=text;
 await assert.rejects(view.load(fetcher(changed),digest),/Unreviewed compiler/);
});

test('exact rational arithmetic rejects wrong values, unit changes and source substitutions',async()=>{
 const f=fetcher(clone()),state=await view.load(f),r=await view.record(f,state,'ABC');
 for(const row of r.shard.records)for(const [name,m] of Object.entries(row.measurements.metrics))view.metric(name,m);
 const m=r.shard.records[0].measurements.metrics.cash_repurchase_outflow;
 for(const change of [x=>x.value='999.000000000000',x=>x.unit='shares',x=>x.exact.numerator='1',x=>x.inputs[0].field='commonStockIssued']){const bad=structuredClone(m);change(bad);assert.throws(()=>view.metric('cash_repurchase_outflow',bad));}
 const missing=structuredClone(m);missing.inputs[0].numeric_value=null;missing.value=null;missing.exact=null;missing.status='required_provider_field_unavailable';view.metric('cash_repurchase_outflow',missing);
 missing.value='0.000000000000';assert.throws(()=>view.metric('cash_repurchase_outflow',missing));
 assert.equal(view.rounded([1n,8n]),'0.125000000000');assert.equal(view.rounded([1n,2000000000000n]),'0.000000000000');
 assert.equal(view.rounded([3n,2000000000000n]),'0.000000000002');assert.equal(view.rounded([-1n,2000000000000n]),'0.000000000000');
});

test('actual page renders unavailable cleanly and escapes reported facts',async()=>{
 const nodes={};const doc={getElementById:id=>nodes[id]||(nodes[id]={addEventListener(){}})};
 await view.mount(doc,fetcher({'data/share-flows.json':'{}'}),'');assert.equal(nodes['issuer-form'].hidden,true);assert.match(nodes['issuer-record'].textContent,/No legacy score or forecast/);
 const f=fetcher(clone()),state=await view.load(f),r=await view.record(f,state,'ABC');r.shard.records[0].identity.acceptedDate='<img src=x onerror=bad()>';
 const html=view.renderRecord(r);assert.doesNotMatch(html,/<img/);assert.match(html,/&lt;img/);
});

test('compiler pins and whole page predecessor match their repository bytes',()=>{
 for(const [name,sha] of Object.entries(view.COMPILERS)){const raw=fs.readFileSync(path.join(__dirname,'../aws/shared/'+name+'.py'));assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),sha);}
 const m=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/capital-structure-research-page-migration.json'),'utf8'));
 const old=fs.readFileSync(path.join(__dirname,'..',m.predecessor));assert.equal(old.length,m.bytes);assert.equal(crypto.createHash('sha256').update(old).digest('hex'),m.sha256);
 const html=fs.readFileSync(path.join(__dirname,'../share-flows.html'),'utf8');assert.match(html,/jh-capital-structure-research.js/);assert.match(html,/does not replay protected provider originals/);
});
