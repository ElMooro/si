const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const test=require('node:test');
const root=path.join(__dirname,'..');
const tick=()=>new Promise(resolve=>setImmediate(resolve));

function loadPage(request,saved=[{id:'UNRATE',name:'Fixture monthly rate (%)',cat:'macro',weight:50,isEcb:false}]){
  const elements=new Map(),calls=[],listeners={};
  const element=()=>({textContent:'',innerHTML:'',value:'',style:{},dataset:{},addEventListener(){},appendChild(){},classList:{add(){},remove(){},toggle(){}}});
  const get=id=>{if(!elements.has(id))elements.set(id,element());return elements.get(id);};
  const context=vm.createContext({Date,JSON,console,encodeURIComponent,localStorage:{getItem:()=>JSON.stringify(saved),setItem(){}},
    document:{getElementById:get,querySelectorAll:()=>[],createElement:element,addEventListener:(name,fn)=>{listeners[name]=fn;}},
    fetch:async url=>{calls.push(url);return request(url);}});
  context.window=context;
  vm.runInContext(fs.readFileSync(path.join(root,'jh-euro.js'),'utf8'),context);
  listeners.DOMContentLoaded();
  return {get,calls,context};
}
const reply=data=>({ok:true,json:async()=>data});
function fixtures(){return {
  detail:{schema_version:'1.0',method:'ecb_eurosystem_liquidity_detail',generated_at:'2026-09-09T00:00:00Z',ok:false,
    headline:'ECB <img src=x> partial data',policy_rates:{main_refinancing_pct:1.5,deposit_facility_pct:0,marginal_lending_pct:1.75},
    liquidity:{excess_liquidity_eur_bn:1234},errors:['missing source'],extra:{zero:0,missing:null,flag:false}},
  derived:{engine:'ecb-derived',version:'3.4.0',generated_at:'2026-09-09T00:00:00Z',headline:'Published headline',
    indicators:{ciss_acceleration:{ciss_level:0.4,signal:'WATCH'},ea_unemployment:{unemployment_rate_pct:6.5,as_of:'2026-07'},
      country_unemployment:{countries:{DE:{rate_pct:2,chg_3m_pp:0,as_of:'2026-07'},IT:{rate_pct:20,chg_3m_pp:1,as_of:'2026-06'}}},
      eurodollar_stress_index:{esi_0_100:0,tier:'NORMAL'}},credit:{m3_yoy:0,m3_as_of:'2026-07'},
    charts:{ciss_level:{points:[['2026-08-01',0.4]]}},fx:{eurusd:1.12,as_of:'2026-09-08'},
    ai_brief:{base_case:'<script>ownerless market fixture</script>',confidence_note:'Small sample'},flashing:['ciss_acceleration'],n_flashing:1,extra:{nested:[0,null,false]}},
  fred:{series:'UNRATE',bars:[{date:'2026-06-01',value:80},{date:'2026-07-01',value:90},{date:'2026-08-01',value:100},{date:'2026-09-01',value:130}],count:4},
};}
const route=(docs,url)=>reply(url.includes('ecb-detail')?docs.detail:url.includes('ecb-derived')?docs.derived:docs.fred);

test('Euro uses actual ECB fields, zero values, source dates and complete engine responses',async()=>{
  const docs=fixtures(),view=loadPage(url=>route(docs,url));await tick();
  assert.equal(view.calls.length,3);
  assert.ok(view.calls.some(url=>url.endsWith('/data/ecb-detail.json')));
  assert.ok(view.calls.some(url=>url.endsWith('/data/ecb-derived.json')));
  assert.equal(view.get('ecbCissMain').textContent,'0.4');
  assert.match(view.get('mainBody').innerHTML,/6\.5/); // published EA unemployment, not country mean 11
  assert.match(view.get('ecbRatesGrid').innerHTML,/>0<.*2026-07/s);
  assert.equal(view.get('kiNum').textContent,'0');
  assert.match(view.get('ecbRatesGrid').innerHTML,/Snapshot 2026-09-09/);
  assert.deepEqual(JSON.parse(view.get('detail-json').textContent),docs.detail);
  assert.deepEqual(JSON.parse(view.get('derived-json').textContent),docs.derived);
  assert.deepEqual(JSON.parse(view.get('fred-json').textContent),{UNRATE:docs.fred});
  assert.match(view.get('aiEcbReport').innerHTML,/&lt;script&gt;/);
  assert.doesNotMatch(view.get('aiEcbReport').innerHTML,/<script>/);
});

test('Monthly FRED calendar comparison uses previous month, not 22 observations',async()=>{
  const docs=fixtures(),view=loadPage(url=>route(docs,url));await tick();
  const row=view.get('mainBody').innerHTML.split('</tr>')[0];
  assert.match(row,/title="Comparison unavailable">—<\/td>/); // monthly data has no weekly comparison
  assert.match(row,/Baseline 2026-08-01">30%/);
  assert.match(row,/2026-09-01 · FRED UNRATE/);
});

test('Month-end calendar comparison finds nearby prior month observation',async()=>{
  const docs=fixtures();docs.fred={series:'UNRATE',bars:[{date:'2025-12-31',value:80},{date:'2026-01-31',value:100},{date:'2026-02-28',value:120}]};
  const view=loadPage(url=>route(docs,url));await tick();
  assert.match(view.get('mainBody').innerHTML,/Baseline 2026-01-31">20%/);
});

test('Failed refresh clears old ECB/FRED values and never fabricates a neutral score',async()=>{
  const docs=fixtures();let valid=true;
  const view=loadPage(url=>valid?route(docs,url):reply({wrong_schema:true}));await tick();
  valid=false;await vm.runInContext('refreshAll()',view.context);
  assert.equal(view.get('kiNum').textContent,'—');
  assert.equal(view.get('statNormal').textContent,'—');
  assert.match(view.get('kiRegime').textContent,/UNAVAILABLE/);
  assert.equal(view.get('ecbCissMain').textContent,'Unavailable');
  assert.match(view.get('detail-json').textContent,/No verified engine/);
  assert.match(view.get('derived-json').textContent,/No verified engine/);
  assert.equal(view.get('fred-json').textContent,'{}');
});

test('Euro HTML removes dead gateways and embedded simulated rates while retaining custom settings',()=>{
  const html=fs.readFileSync(path.join(root,'euro/index.html'),'utf8'),js=fs.readFileSync(path.join(root,'jh-euro.js'),'utf8');
  assert.match(html,/src="\/jh-euro\.js"/);
  assert.doesNotMatch(html+js,/2ijajv2pntkgj5yw5c3ukh5oq40xsyaf|s3.amazonaws.com\/(?:data|report)\.json|simulated from known data|current: 3\.65|hy - 400/);
  assert.match(js,/khalid-metrics-config/);
  assert.match(html,/local preferences/);
});
