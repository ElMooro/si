const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
globalThis.crypto=crypto.webcrypto;
const api=require('../jh-statement-research.js'),fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/statement-research-public.json'),'utf8'));
const run=fixture.run.split('/').pop().slice(0,64);
function network(change){return async url=>{const key=url.replace(/^\//,'');assert(key.startsWith('data/statement-research/')||key===api.CURRENT);const raw=fixture.objects[key];return new Response(change?change(key,raw):raw,{status:raw===undefined?404:200});};}
async function ready(app){const end=Date.now()+5000;while(Date.now()<end){if(/verified|differs|unavailable|Invalid|Unqualified|Unsupported/.test(app.get('status').textContent)&&!app.get('status').textContent.startsWith('Verifying'))return;await new Promise(r=>setTimeout(r,2));}throw Error('Page verification did not finish: '+app.get('status').textContent);}

test('qualified compiler byte identities match the frozen Python sources',()=>{
 for(const [name,digest]of Object.entries(api.COMPILERS))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(__dirname,'../aws/shared',name+'.py'))).digest('hex'),digest);
});
test('legacy accounting grades cannot pass through page decision views',()=>{
 const gate=require('../jh-statement-context.js');
 const legacy={all_results:[{symbol:'ABC',strength_grade:'A+',m_score:5,concern_score:100}],tickers:{ABC:{beneish_flag:true}},sector_valuation_medians:{Technology:{pe_ttm:1}}};
 const before=structuredClone(legacy),view=gate.decisionView(legacy);
 assert.deepEqual(view.all_results,[]);assert.deepEqual(view.tickers,{});assert.deepEqual(view.sector_valuation_medians,{});
 assert.equal(view.independent_investment_votes,0);assert.equal(view.grade,null);assert.equal(view.sizing_eligible,false);
 assert.equal(view.research_context.output_digest_verified,false);assert.deepEqual(legacy,before);
});
test('whole native and pinned records expose identical qualified measurements',async()=>{
 const current=await api.load(network(),null),pinned=await api.load(network(),run);assert.deepEqual(current.packet,pinned.packet);
 const record=await api.record(network(),current,'ABC');assert.equal(record.shard.records.length,2);
 assert.equal(record.shard.records[0].measurements.metrics.gross_margin_pct.value,'40.000000000000');
 assert.equal(record.shard.records[0].measurements.metrics.sga_to_revenue_pct.value,'0.000000000000');
 assert.equal(current.packet.independent_investment_votes,0);
});
test('missing statement and zero denominator stay unavailable, empty issuer retained',async()=>{
 const state=await api.load(network(),run),part=await api.record(network(),state,'PART'),empty=await api.record(network(),state,'EMPTY');
 assert.equal(empty.shard.records.length,0);assert.equal(empty.summary.status,'no_provider_statements');
 assert.equal(part.shard.records[0].measurements.metrics.operating_cash_margin_pct.value,null);
 assert.equal(part.shard.records[0].measurements.metrics.gross_margin_pct.status,'nonpositive_denominator');
});
test('tampered current, output and record bytes cannot verify',async()=>{
 for(const mutate of [p=>p.calls_eligible=true,p=>p.provider_rows=1,p=>p.quality.original_sec_filings_replayed=true]){
  await assert.rejects(()=>api.load(network((key,raw)=>{if(key===api.CURRENT){const p=JSON.parse(raw);mutate(p);return JSON.stringify(p);}return raw;}),null));
 }
 await assert.rejects(()=>api.load(network((key,raw)=>key.includes('/outputs/')?raw+' ':raw),run),/verification|identity/);
 const state=await api.load(network(),run);await assert.rejects(()=>api.record(network((key,raw)=>key.includes('/records/')?raw+' ':raw),state,'ABC'),/verification/);
 await assert.rejects(()=>api.load(network(),'../../portfolio'),/Invalid/);
});
test('hash-valid unknown compiler versions are refused',async()=>{
 const value=JSON.parse(fixture.objects[fixture.run]);value.compilers.statement_measurements.sha256='0'.repeat(64);value.compilers.statement_measurements.key=api.PREFIX+'compilers/'+'0'.repeat(64)+'.py';
 const body=api.canonical(value),digest=await api.hash(new TextEncoder().encode(body));await assert.rejects(()=>api.load(async()=>new Response(body),digest),/Unqualified accounting compiler/);
});
test('browser independently verifies field definitions and exact signed arithmetic',async()=>{
 const state=await api.load(network(),run),record=await api.record(network(),state,'ABC');
 const original=record.shard.records[0].measurements.metrics;
 for(const [name,metric]of Object.entries(original))assert.equal(api.verifyMetric(name,metric).available,true);
 for(const mutate of [m=>m.value='41.000000000000',m=>m.value=null,m=>m.inputs[0].field='netIncome',m=>m.inputs[0].reported_value=null,m=>m.supports_investment_action=true]){
  const m=structuredClone(original.gross_margin_pct);mutate(m);assert.throws(()=>api.verifyMetric('gross_margin_pct',m));
 }
 const m=structuredClone(original.current_ratio);m.inputs[0].reported_value='-3';m.inputs[1].reported_value='2000000000000';m.value='-0.000000000002';assert.equal(api.verifyMetric('current_ratio',m).exact_value,m.value);
 m.inputs[0].reported_value='-1';m.value='0.000000000000';assert.equal(api.verifyMetric('current_ratio',m).exact_value,m.value);
});
test('current SEC identity conflicts and bad dates retain original metadata without company calculations',async()=>{
 const state=await api.load(network(),run),bad=await api.record(network(),state,'BADCIK'),dates=await api.record(network(),state,'BADDATE');
 assert(bad.shard.records.every(v=>v.measurements===null&&!v.current_ticker_cik_corroborated));
 assert.equal(bad.shard.records[0].identity_evidence[0].reported_identity.cik,'456');
 assert.deepEqual(bad.shard.records[0].identity_evidence[0].current_sec_ciks,['0000000123']);
 assert(dates.shard.records.every(v=>v.measurements===null&&v.identity_evidence[0].reported_identity.acceptedDate==='2025-01-01 12:00:00'));
 for(const mutate of [r=>r.current_ticker_cik_corroborated=true,r=>r.identity_evidence[0].current_sec_ciks=['0000000456'],r=>r.measurements={metrics:{}}]){
  const record=structuredClone(bad.shard.records[0]);mutate(record);assert.throws(()=>api.verifyIdentity(record,'BADCIK',state.pairs));
 }
});
test('assumed exposure is exact, signed and independent of accounting ratios',()=>{
 const value=api.scenario({shares:'3.125',price:'100.01',shock:'-10.005',cost:'0.003',currency:'EUR'});
 assert.equal(value.pnl,'-31.2717515625');assert.equal(value.signed_notional,'312.53125');assert.equal(value.forecast,false);assert.equal(value.currency,'EUR');
 assert.equal(api.scenario({shares:'-100',price:'50',shock:'20',cost:'7',currency:'USD'}).pnl,'-1007');
 for(const change of [{price:'0'},{cost:'-1'},{currency:'usd'},{shock:'-101'},{shares:'1e9'}])assert.throws(()=>api.scenario({shares:'1',price:'1',shock:'1',cost:'0',currency:'USD',...change}));
});

function ui(search,custom=api){
 class El{constructor(tag){this.tagName=tag;this.children=[];this.events={};this.value='';this.disabled=false;this.textContent='';}append(...v){this.children.push(...v);}replaceChildren(...v){this.children=[...v];this.textContent='';}addEventListener(k,v){this.events[k]=v;}setAttribute(k,v){this[k]=v;}focus(){this.focused=true;}scrollIntoView(){this.scrolled=true;}click(){this.clicked=true;}remove(){this.removed=true;}}
 const nodes=new Map(),get=name=>{if(!nodes.has(name))nodes.set(name,new El(name));return nodes.get(name);};
 const location={search,href:'https://justhodl.ai/statement-research.html'+search},document={querySelector:()=>({querySelector:s=>get(s.slice(9,-1))}),createElement:t=>new El(t),getElementById:get,body:new El('body')};
 const history={replaceState(a,b,url){location.href=String(url);location.search=url.search;}};let blob;
 class U extends URL{}U.createObjectURL=b=>{blob=b;return'blob:evidence';};U.revokeObjectURL=()=>{};
 class F{constructor(form){this.form=form;}entries(){return Object.entries(this.form.values||{});}}
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-statement-page.js'),'utf8'),{window:{JHStatementResearch:custom},fetch:network(),document,location,history,URLSearchParams,URL:U,Blob,FormData:F,setTimeout(){}});
 return{get,location,blob:()=>blob};
}
function text(el){return el.textContent+el.children.map(text).join(' ');}
test('actual page selects, explains, exports and resets exact evidence',async()=>{
 const app=ui('?run='+run+'&symbol=ABC');await ready(app);
 for(const value of ['ABC','2025-12-31','EUR','0000000123','timezone unverified'])assert(app.get('identity').textContent.includes(value));
 assert.equal(app.get('export').disabled,false);assert.match(text(app.get('measurements')),/40/);
 assert.match(text(app.get('inputs')),/JSON array index 0/);
 app.get('form').values={shares:'3.125',price:'100.01',shock:'-10.005',cost:'0.003',currency:'EUR'};
 app.get('form').events.submit({preventDefault(){},target:app.get('form')});assert.match(app.get('scenario').textContent,/-31\.2717515625 EUR/);
 app.get('export').events.click();const doc=JSON.parse(await app.blob().text());assert.equal(doc.selected_record.identity.symbol,'ABC');assert.equal(doc.complete_issuer_history.records.length,2);assert.equal(doc.assumed_exposure.forecast,false);
 app.get('query').value='PART';app.get('query').events.input();assert.equal(app.get('export').disabled,true);assert.equal(app.get('scenario').textContent,'');assert.equal(app.get('measurements').children.length,0);
});
test('invalid pinned selection is not silently replaced',async()=>{
 const app=ui('?run='+run+'&symbol=ABC&record='+'f'.repeat(64));await ready(app);
 assert.equal(app.get('export').disabled,true);assert.equal(app.get('choice').value,'');assert.match(app.get('selection-status').textContent,/no record was substituted/);
});
test('empty and missing statement histories render without fabricated values',async()=>{
 const app=ui('?run='+run+'&symbol=EMPTY');await ready(app);assert.match(app.get('selection-status').textContent,/No annual statements/);assert.equal(app.get('export').disabled,true);
 const part=ui('?run='+run+'&symbol=PART');await ready(part);assert.match(text(part.get('measurements')),/Required matching statement unavailable/);assert.match(text(part.get('measurements')),/Denominator is zero/);
});
test('actual page exposes original invalid dates and issuer correspondence',async()=>{
 const state=await api.load(network(),run),dates=await api.record(network(),state,'BADDATE');
 const app=ui('?run='+run+'&symbol=BADDATE&record='+dates.shard.records[0].record_id);await ready(app);
 assert.match(text(app.get('identities')),/2025-01-01 12:00:00/);
 assert.match(text(app.get('identities')),/AcceptedDate precedes period end|acceptedDate precedes period end/);
 assert.match(text(app.get('measurements')),/unavailable/);
 const bad=ui('?run='+run+'&symbol=BADCIK');await ready(bad);
 assert.match(text(bad.get('identities')),/0000000123/);assert.match(text(bad.get('identities')),/456/);
 assert.match(bad.get('selection-status').textContent,/cannot be corroborated/);
});
test('delayed validation and changing ticker cannot show an older request',async()=>{
 let release;const gate=new Promise(r=>release=r);const delayed={...api,record:async(...args)=>{await gate;return api.record(...args);}};
 const app=ui('?run='+run+'&symbol=ABC',delayed);
 const deadline=Date.now()+5000;while(!app.get('status').textContent.includes('complete issuer')&&Date.now()<deadline)await new Promise(r=>setTimeout(r,2));
 app.get('query').value='XYZ';app.get('query').events.input();release();await new Promise(r=>setTimeout(r,40));
 assert.equal(app.get('identity').textContent,'');assert.equal(app.get('export').disabled,true);
});
