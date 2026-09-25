const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const ROOT=path.join(__dirname,'..'),html=fs.readFileSync(path.join(ROOT,'why.html'),'utf8'),api=require('../jh-provider-flows.js');
const fixture=JSON.parse(fs.readFileSync(path.join(ROOT,'tests/fixtures/provider-flow-native.json')));
const reference=fixture.publications.flow.replay,run=JSON.parse(fixture.artifacts[reference.manifest_key]);
const packet={...JSON.parse(fixture.artifacts[run.output.key]),replay:reference};
const a=html.indexOf("const RESEARCH_FLOW_KEY="),b=html.indexOf('function fmtZ(v){',a),code=html.slice(a,b);
const esc=s=>String(s??'Unavailable').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
const response=raw=>({ok:typeof raw==='string',arrayBuffer:async()=>new TextEncoder().encode(raw).buffer});
function harness(options={}){
 const requested=[],inspected=[],elements={};const now=options.now??Date.parse(packet.generated_at)+1;
 function select(ticker){elements['flow-section']={dataset:{ticker}};elements['flow-body']={innerHTML:'',textContent:'',append(){}};elements['flow-pill']={textContent:''};}
 const fetcher=async url=>{
  requested.push(url);
  if(options.fetch)return options.fetch(url);
  const raw=url==='/data/provider-fund-flow-research.json'?JSON.stringify(options.packet||packet):fixture.artifacts[url.slice(1)];
  return response(raw);
 };
 const context={window:{JHProviderFlows:{...api,current:p=>api.current(p,now)},
  JHResearchInspection:{show:(body,rows)=>inspected.push(...rows.map(r=>({doc:r.document,label:r.label,url:r.source})))},fetch:fetcher},
  document:{getElementById:id=>elements[id],createElement:()=>({})},Date:{parse:Date.parse,now:()=>now},esc};
 vm.runInNewContext(code,context);select('ABC');return{context,requested,inspected,elements,select};
}
const company=ticker=>({ticker,company:{sector:'Technology'},verdict:{rating:'BUY'}});

test('Research uses actual retained fund-flow output/run verification and keeps the entire native packet',async()=>{
 const h=harness();await h.context.fetchSectorFlow(company('ABC'));
 assert.equal(h.requested.length,3);assert.equal(h.requested[0],'/data/provider-fund-flow-research.json');
 assert(h.requested.slice(1).every(url=>/^\/data\/provider-flow-research\/(runs|outputs)\/[a-f0-9]{64}\.json$/.test(url)));
 assert.equal(h.elements['flow-pill'].textContent,'descriptive');assert.equal(h.inspected.length,1);
 assert.deepEqual(h.inspected[0].doc,packet);assert.equal(Object.keys(h.inspected[0].doc.funds).length,300);
 const body=h.elements['flow-body'].innerHTML;assert.match(body,/XLK/);assert.match(body,/USD exact/);assert.match(body,/classification is unverified/);
 assert.match(body,/do not identify who invested/);assert.doesNotMatch(body,/Smart money|institutional capital|CONSENSUS|DIVERGENCE|STRONG_INFLOW/);
});

test('tampered fund numbers, unavailable verification and future packets cannot paint source context',async()=>{
 const altered=structuredClone(packet);altered.funds.XLK.aligned_windows['5'].flow_usd_decimal='99999999';
 for(const option of [{packet:altered},{now:Date.parse(packet.generated_at)-1},{missing:true}]){
  const h=harness(option);if(option.missing)delete h.context.window.JHProviderFlows;
  await h.context.fetchSectorFlow(company('ABC'));assert.equal(h.inspected.length,0);
  assert.equal(h.elements['flow-pill'].textContent,'unavailable');assert.match(h.elements['flow-body'].textContent,/could not be verified/);
 }
});

test('overdue evidence remains explicitly dated and unmatched sector labels do not invent fund mappings',async()=>{
 const h=harness({now:Date.parse(packet.source_valid_until)+1});await h.context.fetchSectorFlow({ticker:'ABC',company:{sector:'Unmapped <sector>'}});
 assert.equal(h.elements['flow-pill'].textContent,'source check overdue');
 const body=h.elements['flow-body'].innerHTML;assert.match(body,/No configured fund exactly matches/);assert.match(body,/source check overdue/);
 assert.match(body,/Unmapped &lt;sector&gt;/);assert.doesNotMatch(body,/<sector>/);assert.equal(h.inspected.length,1);
});

test('old selection responses cannot repaint a new stock or a replaced same-stock section',async()=>{
 for(const ticker of ['ABC','XYZ']){
  const queue=[];const h=harness({fetch:url=>url==='/data/provider-fund-flow-research.json'?new Promise(resolve=>queue.push(resolve)):response(fixture.artifacts[url.slice(1)])});
  const first=h.context.fetchSectorFlow(company('ABC'));h.select(ticker);const second=h.context.fetchSectorFlow(company(ticker));
  queue[0](response(JSON.stringify(packet)));await first;assert.equal(h.inspected.length,0);
  queue[1](response(JSON.stringify(packet)));await second;assert.equal(h.inspected.length,1);
 }
});

test('exact decimal zero and incomplete coverage stay distinct without fabricated sector totals',()=>{
 const p=structuredClone(packet),row=p.funds.XLK.aligned_windows['5'];row.flow_usd_decimal='0';
 const h=harness();h.context.paintSectorFlow(company('ABC'),p,true);assert.match(h.elements['flow-body'].innerHTML,/<td>0<\/td>/);
 row.flow_usd_decimal=null;row.status='incomplete';row.available_observations=0;row.reasons=['missing observation'];
 h.context.paintSectorFlow(company('ABC'),p,true);assert.match(h.elements['flow-body'].innerHTML,/<td>Unavailable<\/td>/);
 assert.match(h.elements['flow-body'].innerHTML,/0 \/ 5/);assert.match(h.elements['flow-body'].innerHTML,/incomplete · missing observation/);
});

test('shared verifier is exposed without changing its API and whole predecessors are retained',()=>{
 assert.equal(globalThis.JHProviderFlows,api);assert(html.includes('/jh-provider-flows.js?v=20260925-research-context'));
 assert.doesNotMatch(code,/per-ticker-context\.json|composite\.json/);
 const manifest=JSON.parse(fs.readFileSync(path.join(ROOT,'tests/fixtures/equity-flow-context-migration.json')));
 for(const row of manifest.files){const raw=fs.readFileSync(path.join(ROOT,row.predecessor));assert.equal(raw.length,row.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),row.sha256);}
});
