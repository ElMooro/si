const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../credit-before-equity.html'),'utf8');
const script=[...source.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]).find(s=>s.includes('var POS='));
const issuer=()=>({ticker:'TEST',name:'Invented issuer',signal:'NONE',credit_direction:null,distance_to_default:0,d_distance_to_default:0,synthetic_cds_bp:0,d_synthetic_cds_bp:0,d_price_pct:0});
const packet=()=>({generated_at:'2026-10-01T00:00:00Z',n_names:1,n_leads:0,n_awaiting_history:0,names:[issuer()],leads:[],degraded:[],gaps:[],thresholds:{equity_flat_band_pct:0}});
async function render(d,options={}){const elements={},requests=[];await vm.runInNewContext(script,{Date,console:{error(){}},fetch:async url=>{requests.push(url);if(options.network)throw Error('Network unavailable');return {ok:!options.http,status:503,json:async()=>{if(options.json)throw Error('Invalid JSON');return d;}};},document:{getElementById:id=>elements[id]??={innerHTML:'',textContent:'',insertAdjacentHTML(_p,s){this.innerHTML+=s;}}}});return {elements,requests};}
// Entire inline consumer executes; no data/provider/network access.
test('absent, null and wrong typed counts never become zero',async()=>{for(const value of [undefined,null,'0',false,{},[],NaN,Infinity,-1,0.5]){const d=packet();d.n_names=d.n_leads=d.n_awaiting_history=value;const {elements:e}=await render(d);assert.equal((e.hero.innerHTML.match(/Unavailable/g)||[]).length,3);assert.match(e.evidence.innerHTML,/Incomplete packet/);assert.doesNotMatch(e.leads.innerHTML,/No leads reported by/);}});
test('real zeros and zero threshold survive; malformed thresholds do not invent 3%',async()=>{let {elements:e}=await render(packet());assert.match(e.hero.innerHTML,/±0%/);assert.match(e.tb.innerHTML,/>0\.00</);assert.match(e.leads.innerHTML,/No leads reported by the engine for the 1/);for(const value of [null,undefined,'0',false,{},NaN,Infinity,-1]){const d=packet();d.thresholds.equity_flat_band_pct=value;const {elements}=await render(d);assert.match(elements.hero.innerHTML,/Flat band<\/div><div class="big">Unavailable/);assert.doesNotMatch(elements.hero.innerHTML,/±3%/);}});
test('degraded empty, zero universe and awaiting history have distinct truthful states',async()=>{const d=packet();d.names=[];d.n_names=0;d.degraded=['No credit leg'];let {elements:e}=await render(d);assert.match(e.evidence.innerHTML,/Degraded evidence[\s\S]*No credit leg/);assert.match(e.leads.innerHTML,/unavailable or degraded/);d.degraded=[];({elements:e}=await render(d));assert.match(e.leads.innerHTML,/No issuers reported/);d.names=[{...issuer(),signal:'INSUFFICIENT_HISTORY'}];d.n_names=1;d.n_awaiting_history=1;({elements:e}=await render(d));assert.match(e.leads.innerHTML,/1 issuers await history/);});
test('populated rows and engine signals stay intact with degraded evidence',async()=>{const d=packet();d.names[0].signal='CREDIT_LEADS_DOWN';d.leads=d.names;d.n_leads=1;d.degraded=['Missing optional leg'];const {elements:e,requests}=await render(d);assert.match(e.evidence.innerHTML,/Degraded/);assert.match(e.leads.innerHTML,/CREDIT LEADS DOWN/);assert.match(e.tb.innerHTML,/TEST/);assert.match(e.leads.innerHTML,/Showing 1 supplied leads; engine total 1/);assert.equal(requests.length,1);assert.match(requests[0],/\/data\/credit-before-equity.json\?t=/);});
test('transport counts respect backend 20-lead preview; malformed arrays and rows are explicit',async()=>{const d=packet();d.names=Array.from({length:21},(_,i)=>({...issuer(),ticker:'TEST'+i,signal:'CREDIT_LEADS_UP'}));d.leads=d.names.slice(0,20);d.n_names=d.n_leads=21;let {elements:e}=await render(d);assert.match(e.evidence.innerHTML,/Published packet received/);assert.equal((e.tb.innerHTML.match(/<tr>/g)||[]).length,21);for(const key of ['names','leads','degraded','gaps'])for(const value of [null,{},'wrong',[null]]){const f=packet();f[key]=value;const {elements}=await render(f);assert.match(elements.evidence.innerHTML,/Incomplete packet/);assert.doesNotMatch(elements.leads.innerHTML,/No leads reported by/);}d.n_names=0;({elements:e}=await render(d));assert.match(e.evidence.innerHTML,/counts exceed|count does not match/);});
test('payload text is escaped, invalid row numeric values do not coerce',async()=>{const attack='<img src=x onerror=alert(1)>';const d=packet();d.names=[{...issuer(),ticker:attack,name:attack,signal:attack,credit_direction:attack,regime:attack,prior_obs_date:attack,d_price_pct:'4',synthetic_cds_bp:{}}];d.leads=d.names;d.n_leads=1;d.degraded=[attack];d.gaps=[attack];const {elements:e}=await render(d);for(const key of ['evidence','leads','tb','gaps']){assert.doesNotMatch(e[key].innerHTML,/<img/);assert.match(e[key].innerHTML,/&lt;img/);}assert.match(e.tb.innerHTML,/Unavailable/);assert.doesNotMatch(e.tb.innerHTML,/\+4/);});
test('generation time never claims source freshness, including ancient and future timestamps',async()=>{for(const time of ['2000-01-01T00:00:00Z','2099-01-01T00:00:00Z',null,'invalid']){const d=packet();d.generated_at=time;const {elements:e}=await render(d);assert.match(e.evidence.innerHTML,/not the observation time/);assert.match(e.evidence.innerHTML,/source freshness are not supplied/);assert.doesNotMatch(e.evidence.innerHTML,/fresh source|LIVE|healthy/i);}});
test('HTTP, network, JSON and non-object failures cannot show a no-signal result',async()=>{for(const [d,options] of [[{}, {http:true}],[{}, {network:true}],[{}, {json:true}],[null,{}],[[],{}]]){const {elements:e}=await render(d,options);assert.match(e.evidence.innerHTML,/Evidence unavailable/);assert.equal(e.hero.textContent,'Summary unavailable');}});

test('review P2: zero reported leads cannot hide a supplied lead signal',async()=>{
 const d=packet();d.names[0].signal='CREDIT_LEADS_UP';const before=JSON.stringify(d);
 const {elements:e}=await render(d);
 assert.match(e.evidence.innerHTML,/Incomplete packet/);assert.match(e.evidence.innerHTML,/Lead count does not match the supplied issuer signals/);
 assert.match(e.hero.innerHTML,/Leads firing<\/div><div class="big"[^>]*>0<\/div>/);
 assert.match(e.tb.innerHTML,/CREDIT LEADS UP/);assert.match(e.leads.innerHTML,/unavailable or incomplete/);
 assert.doesNotMatch(e.leads.innerHTML,/No leads reported/);assert.equal(JSON.stringify(d),before);
});
test('review P2: lead and awaiting-history counts are mutually exclusive',async()=>{
 const d=packet();d.names[0].signal='CREDIT_LEADS_UP';d.leads=d.names;d.n_leads=1;d.n_awaiting_history=1;
 const before=JSON.stringify(d),{elements:e}=await render(d);
 assert.match(e.evidence.innerHTML,/Incomplete packet/);assert.match(e.evidence.innerHTML,/Lead and awaiting-history counts together exceed/);
 assert.match(e.evidence.innerHTML,/Awaiting-history count does not match/);
 assert.match(e.hero.innerHTML,/Awaiting history<\/div><div class="big"[^>]*>1<\/div>/);
 assert.match(e.leads.innerHTML,/CREDIT LEADS UP/);assert.match(e.tb.innerHTML,/CREDIT LEADS UP/);assert.equal(JSON.stringify(d),before);
});
test('signal count and preview contradictions remain incomplete without rescoring',async()=>{
 for(const amend of [
  d=>{d.names[0].signal='INSUFFICIENT_HISTORY';},
  d=>{d.n_awaiting_history=1;},
  d=>{d.n_leads=1;d.leads=[{...issuer(),signal:'CREDIT_LEADS_UP'}];},
  d=>{d.names[0].signal='CREDIT_LEADS_DOWN';d.n_leads=1;d.leads=[{...d.names[0],signal:'CREDIT_LEADS_UP'}];},
  d=>{d.names[0].signal='CREDIT_LEADS_UP';d.n_leads=1;d.leads=[{...d.names[0],ticker:'OTHER'}];},
  d=>{d.names[0].signal='UNRECOGNIZED';}
 ]){const d=packet();amend(d);const before=JSON.stringify(d),{elements:e}=await render(d);assert.match(e.evidence.innerHTML,/Incomplete packet/);assert.doesNotMatch(e.leads.innerHTML,/No leads reported/);assert.equal(JSON.stringify(d),before);}
 const d=packet();d.names=[{...issuer(),signal:'CREDIT_LEADS_DOWN'},{...issuer(),ticker:'WAIT',signal:'INSUFFICIENT_HISTORY'}, {...issuer(),ticker:'NONE'}];d.n_names=3;d.n_leads=1;d.n_awaiting_history=1;d.leads=[d.names[0]];
 const {elements:e}=await render(d);assert.match(e.evidence.innerHTML,/Published packet received/);assert.equal((e.tb.innerHTML.match(/<tr>/g)||[]).length,3);
});

test('missing and invalid measurements have no percent or bp suffix in rows or leads',async()=>{
 for(const value of [undefined,null,'0',false,{},[],NaN,Infinity,-Infinity]){
  const d=packet();d.names=[{...issuer(),ticker:'GOOGL',signal:'CREDIT_LEADS_UP',d_price_pct:value,synthetic_cds_bp:value,default_prob_5y_pct:value}];d.leads=d.names;d.n_leads=1;
  const {elements:e}=await render(d);
  assert.match(e.tb.innerHTML,/>Unavailable<\/td>/);
  assert.match(e.leads.innerHTML,/CDS Unavailable \(/);
  assert.match(e.leads.innerHTML,/price Unavailable · 5y PD Unavailable ·/);
  for(const key of ['tb','leads'])assert.doesNotMatch(e[key].innerHTML,/Unavailable(?:%|bp)/);
 }
});
test('finite measurements retain exact signs precision and units including literal zero',async()=>{
 for(const value of [0,1.234,-1.234]){
  const d=packet();d.names=[{...issuer(),signal:'CREDIT_LEADS_UP',d_price_pct:value,synthetic_cds_bp:value,default_prob_5y_pct:value}];d.leads=d.names;d.n_leads=1;
  const {elements:e}=await render(d),signed=(value>0?'+':'')+value.toFixed(2);
  assert.ok(e.tb.innerHTML.includes('>'+signed+'%</td>'));
  assert.ok(e.leads.innerHTML.includes('CDS '+value.toFixed(1)+'bp ('));
  assert.ok(e.leads.innerHTML.includes('price '+signed+'% · 5y PD '+value.toFixed(2)+'%'));
  assert.match(e.hero.innerHTML,/±0%/);
 }
});
