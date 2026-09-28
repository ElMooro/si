const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const root=path.join(__dirname,'..'),api=require('../jh-table-values.js');
const flush=async()=>{for(let i=0;i<3;i++)await new Promise(r=>setImmediate(r));};
function page(file,loader,scripts={}){
 const nodes=new Map(),calls=[],intervals=new Set(),listeners={};
 let document;
 const get=id=>{
  if(!nodes.has(id)){
   const n={value:'',headers:[],querySelectorAll(selector){return selector.startsWith('button')?[]:this.headers;},addEventListener(k,fn){this['on'+k]=fn;},setAttribute(k,v){this[k]=v;}};
   let html='',text='';
   Object.defineProperty(n,'innerHTML',{get:()=>html,set:v=>{html=v;text='';n.headers=[...v.matchAll(/<th\b[^>]*data-k=(?:"([^"]+)"|([^ >]+))[^>]*>/g)].map(m=>({dataset:{k:m[1]||m[2]},ownerDocument:document,setAttribute(k,v){this[k]=v;},focus(){document.activeElement=this;}}));}});
   Object.defineProperty(n,'textContent',{get:()=>text,set:v=>{text=String(v);html='';n.headers=[];}});
   nodes.set(id,n);
  }
  return nodes.get(id);
 };
 document={getElementById:get,querySelectorAll:selector=>selector.startsWith('#')?get(selector.slice(1).split(' ')[0]).headers:[...nodes.values()].flatMap(n=>n.headers),activeElement:null};
 const ctx=vm.createContext({console,Date,Number,String,Array,Object,JSON,TextDecoder,atob,Uint8Array,encodeURIComponent,document,JHTableValues:{...api,load:async p=>{calls.push(p);return loader(p);}},setInterval(fn){intervals.add(fn);return fn;},clearInterval(fn){intervals.delete(fn);},addEventListener(k,fn){listeners[k]=fn;}});ctx.window=ctx;
 const html=fs.readFileSync(path.join(root,file),'utf8');
 if(html.includes('/jh-leader-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-leader-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-momentum-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-momentum-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-price-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-price-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-activist-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-activist-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-option-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-option-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-microcap-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-microcap-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-pead-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,scripts.pead||'jh-pead-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-revenue-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,scripts.revenue||'jh-revenue-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-eps-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-eps-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-hiring-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-hiring-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-scarcity-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-scarcity-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-estimate-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-estimate-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-earnings-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-earnings-observations.js'),'utf8'),ctx);
 for(const m of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))vm.runInContext(m[1],ctx);
 return{ctx,get,calls,document,intervals,listeners,html};
}
const raw=p=>({packet:p,raw:JSON.stringify(p)});

function resumedEarnings(){
 const selected=['TEST','TEST','LATER'].map(ticker=>({ticker})),keys=['TEST#0','TEST#1','LATER#0'];
 return{measurement_contract:'earnings-event-observations.v1',universe_membership:{selected},
  request_records:selected.map((member,i)=>({ticker:member.ticker,request_index:i,universe_member:member,
   acquisitions:[{endpoint:'earnings',status:i===0?'received':i===1?'rate_limited':'not_attempted_runtime_rate_or_size_limit'}],event_observations:[],event_differences:[],price_coverage:{records:0}})),
  acquisition_progress:{contract:'earnings-acquisition-progress.v1',planned_request_indices:[0,1,2],planned_occurrence_keys:keys,visited_request_indices:[0,1],
   remaining_occurrence_keys:['LATER#0'],visited_occurrences:2,pending_occurrences:1,cycle_complete:false,stop_reason:'provider_rate_limit',schedule_accelerated:false,request_order_is_rank:false}};
}

test('earnings desk distinguishes returned data, visits and unattempted occurrences',async()=>{
 const M=require('../jh-pead-observations.js'),p=resumedEarnings(),s=page('earnings-pead.html',async()=>raw(p));await flush();
 assert.match(s.get('status').textContent,/1 \/ 3 selected request occurrences returned earnings data/);
 assert.match(s.get('status').textContent,/1 not attempted; 1 other acquisition outcomes/);
 assert.match(s.get('coverage').textContent,/1 request occurrences remain/);assert.match(s.get('coverage').textContent,/not simultaneous/);
 s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('rows').textContent,/3 selected requests occurrences/);
 assert.match(s.get('board').innerHTML,/LATER/);s.get('packet-details').open=true;s.get('packet-details').ontoggle();assert.equal(s.get('original').textContent,JSON.stringify(p));
 const old=structuredClone(p);delete old.acquisition_progress;const c=M.coverage(old);
 assert.equal(c.received,1);assert.equal(c.unattempted,1);assert.equal(c.pending,null);assert.match(M.coverageMessage(c),/older publication has no resumable progress/);
});

test('earnings acquisition progress corruption cannot imply complete coverage',async()=>{
 const M=require('../jh-pead-observations.js');
 for(const edit of [p=>p.acquisition_progress.pending_occurrences=0,p=>p.acquisition_progress.visited_request_indices=[true],
  p=>p.acquisition_progress.planned_request_indices=[0,0,2],p=>p.acquisition_progress.remaining_occurrence_keys=[],
  p=>p.request_records[2].acquisitions[0].status='received',p=>p.acquisition_progress.stop_reason='planned_window_complete']){
  const p=resumedEarnings();edit(p);assert.throws(()=>M.coverage(p),/progress|outcomes/);
 }
 const p=resumedEarnings();p.acquisition_progress.pending_occurrences=0;const s=page('earnings-pead.html',async()=>raw(p));await flush();
 assert.match(s.get('status').textContent,/unavailable/);assert.equal(s.get('board').textContent,'No verified display population');assert.equal(s.get('next').disabled,true);
 for(const mode of ['requests','prices','events']){s.get('mode').value=mode;s.get('mode').onchange();s.get('q').oninput();s.get('next').onclick();assert.equal(s.get('board').textContent,'No verified display population');assert.equal(s.get('rows').textContent,'');}
});

function resumedRevenue(){
 const p=resumedEarnings();p.measurement_contract='revenue-statement-observations.v1';p.acquisition_progress.contract='revenue-acquisition-progress.v1';
 for(const r of p.request_records){r.acquisitions[0].endpoint='income-statement';r.statement_observations=[];r.period_comparisons=[];r.quote_records=[];delete r.event_observations;delete r.event_differences;delete r.price_coverage;}
 return p;
}

test('complete predecessors reproduce the validation bypass repaired on both desks',async()=>{
 for(const [file,p,scripts]of [
  ['earnings-pead.html',resumedEarnings(),{pead:'tests/fixtures/pre-pead-lazy-jh-pead-observations.js.txt'}],
  ['revenue-acceleration.html',resumedRevenue(),{revenue:'tests/fixtures/pre-revenue-validation-jh-revenue-observations.js.txt'}]]){
  p.acquisition_progress.pending_occurrences=0;
  const old=page(file,async()=>raw(p),scripts);await flush();assert.match(old.get('status').textContent,/unavailable/);
  old.get('mode').value='requests';old.get('mode').onchange();assert.match(old.get('board').innerHTML,/LATER/);
  const fixed=page(file,async()=>raw(p));await flush();fixed.get('mode').value='requests';fixed.get('mode').onchange();
  assert.equal(fixed.get('board').textContent,'No verified display population');assert.equal(fixed.get('next').disabled,true);
 }
});

test('revenue coverage separates actual statement responses, attempts and deferred occurrences',async()=>{
 const M=require('../jh-revenue-observations.js'),p=resumedRevenue(),s=page('revenue-acceleration.html',async()=>raw(p));await flush();
 assert.match(s.get('status').textContent,/1 \/ 3 selected request occurrences returned income-statement data/);
 assert.match(s.get('status').textContent,/1 not attempted; 1 other acquisition outcomes/);assert.match(s.get('coverage').textContent,/1 request occurrences remain/);
 s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('rows').textContent,/3 selected requests occurrences/);
 s.get('q').value='not_attempted';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching \/ 3/);assert.match(s.get('board').innerHTML,/LATER/);
 assert.equal(s.get('original').textContent,JSON.stringify(p));const old=structuredClone(p);delete old.acquisition_progress;
 assert.equal(M.coverage(old).pending,null);assert.match(M.coverageMessage(M.coverage(old)),/older publication/);
});

test('revenue forged progress withholds coverage while retaining whole original access',async()=>{
 const M=require('../jh-revenue-observations.js');
 for(const edit of [p=>p.acquisition_progress.pending_occurrences=0,p=>p.acquisition_progress.visited_request_indices=[false],
  p=>p.acquisition_progress.planned_request_indices=[0,0,2],p=>p.request_records[2].acquisitions[0].status='received']){
  const p=resumedRevenue();edit(p);assert.throws(()=>M.coverage(p),/progress|outcomes/);
 }
 const p=resumedRevenue();p.acquisition_progress.remaining_occurrence_keys=[];const s=page('revenue-acceleration.html',async()=>raw(p));await flush();
 assert.match(s.get('status').textContent,/unavailable/);assert.equal(s.get('board').textContent,'No verified display population');assert.equal(s.get('original').textContent,JSON.stringify(p));
 for(const mode of ['requests','statements']){s.get('mode').value=mode;s.get('mode').onchange();s.get('q').oninput();s.get('next').onclick();assert.equal(s.get('board').textContent,'No verified display population');assert.equal(s.get('rows').textContent,'');}
});

test('price source desk preserves complete request, measurement, window and universe populations',async()=>{
 const M=require('../jh-price-observations.js'),fixture=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/price-compression-synthetic.json'),'utf8')),p=fixture.packet;
 const model=M.model(p);assert.equal(model.requests.length,2);assert.equal(model.measurements.length,26);assert.equal(model.windows.length,2);assert.equal(model.universe.length,3);
 const s=page('volatility-observations.html',async()=>raw(p));await flush();assert.equal(s.calls[0],'/data/volatility-squeeze.json');assert.equal(s.get('original').textContent,JSON.stringify(p));
 for(const [mode,n]of [['measurements',26],['windows',2],['universe',3]]){s.get('mode').value=mode;s.get('mode').onchange();assert.match(s.get('rows').textContent,new RegExp(n+' received '+mode));assert.match(s.get('board').innerHTML,/Inspect whole original/);}
 assert.match(s.get('board').innerHTML,/&lt;img/);assert.ok(!s.get('board').innerHTML.includes('<img'));
 s.get('mode').value='measurements';s.get('mode').onchange();assert.match(s.get('board').innerHTML,/<td>0<\/td>/);
 const h=s.get('board').headers.find(x=>x.dataset.k==='value');h.focus();h.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'value');
 for(const field of ['calls_eligible','sizing_eligible','execution_eligible'])assert.throws(()=>M.model({...p,[field]:true}),/permissions/);
 const bad=structuredClone(p);bad.request_records[0].observations.selected_indices=[999999];assert.throws(()=>M.model(bad),/coordinate/);
 assert.throws(()=>M.model({...p,request_records:[]}),/populations/);
});

test('legacy price occurrences retain full pagination, search, sorting and failure clearing',async()=>{
 const p={all_qualifying:Array.from({length:201},(_,i)=>({symbol:'T'+i,score:99})),summary:{top_25_overall:[{symbol:'T0'}],tier_s:['T0']}};
 const s=page('volatility-observations.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/203 received/);assert.match(s.get('status').textContent,/Scores, tiers, source dates and coverage are unqualified/);
 s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='T200';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/203 matching/);
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({all_qualifying:{bad:true}})]){const f=page('volatility-observations.html',loader);await flush();assert.match(f.get('status').textContent,/unavailable/);assert.equal(f.get('board').textContent,'No verified display population');}
 assert.ok(api.PUBLIC_PATHS.test('/data/volatility-squeeze.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/volatility-squeeze-state.json'));
});

test('price originals require complete verified bytes and prohibit arbitrary paths',async()=>{
 const M=require('../jh-price-observations.js'),fixture=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/price-compression-synthetic.json'),'utf8')),ref=fixture.packet.request_records[0].acquisition.original_ref,body=Buffer.from(fixture.sources[ref.key]),calls=[];
 const fetcher=async(url,options)=>{calls.push(url);assert.equal(options.credentials,'omit');assert.equal(options.redirect,'error');return new Response(body);};
 assert.equal((await M.loadSource(ref,{fetcher})).raw,body.toString());assert.deepEqual(calls,['/'+ref.key+'?exact=1&nogen=1']);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(body.subarray(0,4))}),/byte count/);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(Buffer.alloc(body.length,97))}),/SHA-256/);
 await assert.rejects(()=>M.loadSource({...ref,key:'data/private.json'},{fetcher}),/source identity/);assert.equal(calls.length,1);
 await assert.rejects(()=>M.loadSource(ref,{timeout:5,fetcher:()=>new Promise(()=>{})}),/timed out/);
});

test('three price preview components abstain and clear score text on unavailable input',async()=>{
 const M=require('../jh-price-observations.js'),p={all_qualifying:[{symbol:'TEST',score:99}],summary:{top_25_overall:[{symbol:'TEST',score:99}]}};
 for(const file of ['intel/index.html','web/intel/index.html','options.html']){
  const html=fs.readFileSync(path.join(root,file),'utf8'),intel=file!=='options.html';let block=intel?html.split('// Price compression source observations;')[1].split('// Revenue statement research:')[0]:html.split('// Price compression source observations;')[1].split("document.getElementById('foot')")[0];
  block=block.slice(block.indexOf('\n'));assert.ok(!block.includes('top_25_overall'));assert.match(html,/href="\/volatility-observations.html"/);
  const nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{textContent:'old score'});return nodes.get(id);};let fail=false;
  const context=vm.createContext({document:{getElementById:get},JHPriceObservations:M,fetchJson:async()=>fail?null:p,vsq:p});
  const actual='(async()=>{'+block+'\n})()';await vm.runInContext(actual,context);assert.match(get(intel?'vol-squeeze':'vsq').textContent,/2 legacy occurrences/);assert.equal(get(intel?'vs-fresh':'f-vs').textContent,'RESEARCH');
  fail=true;context.vsq=null;await vm.runInContext(actual,context);assert.match(get(intel?'vol-squeeze':'vsq').textContent,/unavailable/);assert.equal(get(intel?'vs-fresh':'f-vs').textContent,'UNAVAILABLE');
 }
});
const quality=()=>({as_of:'2026-09-27T12:00:00Z',all_ranked:[{ticker:'TEN',name:'A > B',sloan_accruals_pct_assets:'10',cash_conversion_ratio:0,quality_score:'10'},{ticker:'TWO',sloan_accruals_pct_assets:'2',cash_conversion_ratio:false,quality_score:'2'},{ticker:'MISSING',sloan_accruals_pct_assets:' ',cash_conversion_ratio:'',quality_score:null},{ticker:'ZERO',sloan_accruals_pct_assets:0,quality_score:0},null],unknown:{retain:true}});

test('both accounting pages use native issuer occurrences with explicit units and periods',async()=>{
 const p={measurement_contract:'earnings-accounting-measurements.v1',as_of:'2026-09-27T12:00:00Z',all_ranked:[],issuer_rows:[
  {ticker:'ZERO',name:'A > B',quality_score:999,reported_currency:'JPY',amounts:{net_income:10,operating_cash_flow:0,free_cash_flow_derived:-2},measurements:{earnings_cash_gap_pct_end_assets:10,cash_flow_accruals_pct_average_assets:5,cash_conversion_ratio:0},windows:{current:{income:{start_date:'2025-07-01',end_date:'2026-06-30'}}},status:'aligned_reported_accounting_windows'},
  {ticker:'ZERO',amounts:{},measurements:{},windows:{},status:'accounting_alignment_unavailable'},null]};
 for(const file of ['earnings-quality.html','cash-profitability.html']){
  const s=page(file,async()=>raw(p));await flush();const html=s.get('board').innerHTML;
  assert.equal(s.get('original').textContent,JSON.stringify(p));assert.match(html,/JPY/);assert.match(html,/2025-07-01/);assert.match(html,/2026-06-30/);
  assert.match(html,/0\.00/);assert.match(html,/issuer_rows\[0\]/);assert.match(html,/issuer_rows\[1\]/);assert.ok(!html.includes('999'));
  assert.match(s.get('kpis').textContent+html,/1 malformed/);assert.match(html,/accounting_alignment_unavailable/);
 }
});

test('native display projections never recover an unavailable source from a legacy ranking',()=>{
 const M=require('../jh-earnings-observations.js');
 assert.deepEqual(M.rows({measurement_contract:'earnings-accounting-measurements.v1',issuer_rows:[],all_ranked:[{ticker:'BAD'}]}),[]);
 assert.deepEqual(M.rows({measurement_contract:'earnings-accounting-measurements.v1',issuer_rows:{bad:true}}),{bad:true});
 assert.equal(M.rows({top_20_high_quality:[{ticker:'A'}]})[0].source_record,'top_20_high_quality[0]');
 const original={ticker:'A',quality_score:0};assert.match(M.rows({all_ranked:[original]})[0].measurement_status,/Legacy/);assert.deepEqual(original,{ticker:'A',quality_score:0});
});

test('blank, boolean, compound, unsafe, overflow and underflow inputs cannot become measured zero',()=>{
 for(const v of [null,undefined,'',' ',false,true,[],{},'0x10','Infinity',Infinity,'1e999','1e-999',9007199254740992]){
  assert.equal(api.number(v),null,String(v));assert.equal(api.format(v),'');assert.equal(api.percent(v),'');assert.equal(api.sign(v),'');
 }
 for(const v of [0,'0','0.00',-0])assert.equal(api.format(v,1),'0.0');
 assert.equal(api.number(' -1.25e2 '),-125);assert.equal(api.integer(1.5),null);assert.equal(api.integer(-1),null);assert.equal(api.count(0),'0');assert.equal(api.count(false),'Unavailable');
});

test('typed numeric sorting handles numeric strings, ties and missing values in both directions',()=>{
 const rows=[{x:'10',id:'a'},{x:'2',id:'b'},{x:null,id:'c'},{x:false,id:'d'},{x:' ',id:'e'},{x:0,id:'f'},{x:'2.0',id:'g'}];
 assert.deepEqual(rows.slice().sort((a,b)=>api.compare(a,b,'x',1)).map(r=>r.id),['f','b','g','a','c','d','e']);
 assert.deepEqual(rows.slice().sort((a,b)=>api.compare(a,b,'x',-1)).map(r=>r.id),['a','b','g','f','c','d','e']);
 assert.equal(api.compare({x:'false'},{x:true},'x',1,'boolean'),1);assert.equal(api.compare({x:false},{x:true},'x',1,'boolean'),-1);
});

test('earnings and cash pages use the complete quality population and preserve missing/zero distinctions',async()=>{
 for(const file of ['earnings-quality.html','cash-profitability.html']){
  const p=quality(),received=raw(p),s=page(file,async()=>received);await flush();const html=s.get('board').innerHTML;
  assert.equal(s.calls.length,1);assert.equal(s.calls[0],'/data/earnings-quality.json');assert.equal(s.get('original').textContent,received.raw);
  assert.match(html,/0\.0/);assert.ok(!html.includes('NaN'));assert.match(s.get('kpis').textContent+html,/1 malformed/);
  assert.equal(p.all_ranked.length,5);assert.ok(!s.html.includes('await jhGrowthStack.boot()'));
  const key=file==='earnings-quality.html'?'quality_score':'sloan',header=s.get('board').headers.find(x=>x.dataset.k===key);
  header.focus();let prevented=false;header.onkeydown({key:'Enter',preventDefault(){prevented=true;}});assert.equal(prevented,true);
  assert.equal(s.document.activeElement.dataset.k,key);assert.ok(['ascending','descending'].includes(s.document.activeElement['aria-sort']));
  const oldDirection=s.document.activeElement['aria-sort'];s.document.activeElement.onkeydown({key:' ',preventDefault(){}});assert.notEqual(s.document.activeElement['aria-sort'],oldDirection);
  s.get('q').value='MISSING';s.get('q').oninput();assert.match(s.get('board').innerHTML,/MISSING/);assert.ok(!s.get('board').innerHTML.includes('TEN'));
 }
});

test('hiring retains every original subset and refuses coercive headcounts, false inflections and text injection',async()=>{
 const p={generated_at:'2026-09-27T12:00:00Z',top_50:[{symbol:'VALID',name:'A > B',headcount_latest:0,headcount_yoy_pct:0,headcount_accel_pp:false,revenue_per_employee:' ',expansion_score:'10'},{symbol:'BAD',headcount_latest:false,headcount_yoy_pct:'',revenue_per_employee:'<img src=x>',inflection:'false',state:'<img src=x>'},null],double_confirmed:[{symbol:'OTHER'}]};
 const s=page('hiring-velocity.html',async()=>raw(p));await flush();const h=s.get('board').innerHTML;
 assert.match(h,/A &gt; B/);assert.match(h,/Legacy top_50/);assert.match(s.get('rows').textContent,/4 received occurrences/);assert.ok(!h.includes('<img'));assert.ok(!h.includes('$0'));assert.ok(!h.includes(' · inflection'));assert.match(h,/<td>0<\/td>/);assert.match(h,/unverified/);assert.deepEqual(JSON.parse(s.get('original').textContent),p);
});

test('failed and malformed publications are visibly distinct from an empty received population',async()=>{
 for(const file of ['earnings-quality.html','cash-profitability.html']){
  const fail=page(file,async()=>{throw Error('HTTP 403');});await flush();assert.match(fail.get('board').textContent,/HTTP 403/);
  const malformed=page(file,async()=>raw({all_ranked:{wrong:true},top_50:{wrong:true}}));await flush();assert.match(malformed.get('board').textContent,/malformed/);assert.match(malformed.get('original').textContent,/wrong/);
 }
});

test('estimate revisions escapes all display fields, preserves zero and never labels a score as a return',async()=>{
 const p={status:'<img src=x>',n_tracked:'<img src=x>',n_with_history:false,top_picks:[{ticker:'<script>x</script>',score:3,days_to_earnings:'<img src=x>'}],upward_revisions:[{ticker:'TEST',eps_rev_pct:'',rev_rev_pct:false,current_eps_est:0,importance:0,revenue_confirms:'false',days_to_earnings:'<img src=x>'},null],downward_revisions:[]};
 const s=page('estimate-revisions.html',async()=>raw(p));await flush();const h=['banner','statbar','content','foot'].map(k=>s.get(k).innerHTML).join('');
 assert.ok(!h.includes('<img'));assert.ok(!h.includes('<script>'));assert.match(h,/Unavailable/);assert.match(h,/qualification pending/);assert.ok(!h.includes('3.00%'));assert.ok(!h.includes('✓'));assert.match(h,/<td>0<\/td>/);assert.deepEqual(JSON.parse(s.get('original').textContent),p);s.listeners.pagehide();assert.equal(s.intervals.size,0);
 assert.equal(vm.runInContext('days(-3)',s.ctx),'-3d');
});

test('estimate refresh failure and late earlier responses cannot leave stale measurements',async()=>{
 let count=0;const s=page('estimate-revisions.html',async()=>{if(count++)throw Error('HTTP 429');return raw({status:'LIVE',upward_revisions:[{ticker:'FIRST',eps_rev_pct:3}]});});await flush();assert.match(s.get('content').innerHTML,/FIRST/);await vm.runInContext('refresh()',s.ctx);assert.match(s.get('content').textContent,/HTTP 429/);assert.equal(s.get('original').textContent,'Unavailable');assert.equal(s.get('statbar').innerHTML,'');s.listeners.pagehide();
 const pending=[],r=page('estimate-revisions.html',()=>new Promise(resolve=>pending.push(resolve)));const next=vm.runInContext('refresh()',r.ctx);pending[1](raw({status:'LIVE',upward_revisions:[{ticker:'LATEST'}]}));await next;pending[0](raw({status:'LIVE',upward_revisions:[{ticker:'OLD'}]}));await flush();assert.match(r.get('content').innerHTML,/LATEST/);assert.ok(!r.get('content').innerHTML.includes('OLD'));r.listeners.pagehide();
});

test('bounded read uses one exact public route, retains numeric text and has no retry after failure',async()=>{
 let calls=0;const source='{"all_ranked":[],"unknown":9007199254740993}',result=await api.load('/data/earnings-quality.json',{fetcher:async(url,options)=>{calls++;assert.equal(url,'/data/earnings-quality.json?exact=1&nogen=1');assert.equal(options.redirect,'error');return new Response(source);}});assert.equal(result.raw,source);assert.equal(calls,1);
 await assert.rejects(api.load('/data/private-account.json',{fetcher:()=>{throw Error('Must not request');}}),/Unreviewed/);
 calls=0;await assert.rejects(api.load('/data/hiring-velocity.json',{fetcher:async()=>{calls++;return new Response('',{status:429});}}),/429/);assert.equal(calls,1);
 await assert.rejects(api.load('/data/estimate-revisions.json',{timeout:5,fetcher:()=>new Promise(()=>{})}),/timed out/);
 for(const body of ['{','[]',new Uint8Array([255])])await assert.rejects(api.load('/data/earnings-quality.json',{fetcher:async()=>new Response(body)}));
 for(const body of ['{"x":1,"x":2}','{"x":1e999}','{"x":1e-999}'])await assert.rejects(api.load('/data/earnings-quality.json',{fetcher:async()=>new Response(body)}),e=>e.original_text===body);
});

test('rejected JSON remains inert inspectable source text on every desk',async()=>{
 const original='{"x":"<img src=x>","x":2}';
 for(const file of ['earnings-quality.html','cash-profitability.html','hiring-velocity.html','estimate-revisions.html']){
  const s=page(file,async()=>{const e=Error('Duplicate JSON key');e.original_text=original;throw e;});await flush();assert.equal(s.get('original').textContent,original);assert.ok(!s.get('board').innerHTML.includes('<img'));assert.ok(!s.get('content').innerHTML.includes('<img'));s.listeners.pagehide?.();
 }
});

test('all four complete predecessors are retained and tables remain keyboard-scrollable',()=>{
 const hashes={'earnings-quality.html':'0e30458ec4b63879ec3269b73a041ef6e611c08fa0af4caea656b0d06a2fa1ae','cash-profitability.html':'ded12b28cc35ef6b5445245a8a874ac663fda7ea765260927e18ffac798a9a39','hiring-velocity.html':'a2bb015cc9356aab7d816dcb4d4e9b78f8e8c9b8095ab8ac2d2c5f24c514e42e','estimate-revisions.html':'51dbbd81ed5f65b57de563a19e4a65e862948df5759b4d618cefa6a9a465db6f'};
 for(const[file,hash]of Object.entries(hashes)){assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(__dirname,'fixtures','pre-desk-numeric-'+file+'.txt'))).digest('hex'),hash);const current=fs.readFileSync(path.join(root,file),'utf8');assert.match(current,/jh-table-values\.js/);assert.match(current,/tabindex="0"/);assert.match(current,/id="original"/);}
});

test('annual estimates retain every target and failed occurrence without restoring scores or event-period joins',async()=>{
 const p={measurement_contract:'estimate-observations.v1',calendar_rows:[{ticker:'TEST',date:'2030-01-01',fiscal_period:'Q1'}],request_records:[
  {ticker:'TEST',calendar_index:0,observations:[{target_period_end:'2030-12-31',reported_currency:'JPY',eps_basis:'reported',values:{epsAvg:0,numAnalystsEps:3},target_status:'current_or_future_target',measurement_status:'reported_estimate_observation'}],comparisons:[{eps_change:null,status:'no_unique_comparable_prior_observation'}]},
  {ticker:'<script>BAD</script>',calendar_index:0,acquisition:{status:'unavailable'},observations:[],comparisons:[]}],top_picks:[{ticker:'SHOULD_NOT_SHOW',score:999}]};
 const s=page('estimate-revisions.html',async()=>raw(p));await flush();const h=s.get('content').innerHTML;
 assert.match(h,/2030-12-31/);assert.match(h,/2030-01-01/);assert.match(h,/JPY/);assert.match(h,/<td>0<\/td>/);assert.match(h,/2 received display records/);
 assert.match(h,/&lt;script&gt;BAD/);assert.ok(!h.includes('SHOULD_NOT_SHOW'));assert.ok(!h.includes('<script>'));
 assert.match(s.get('banner').textContent,/Missing issuer, currency or EPS basis/);assert.equal(s.get('original').textContent,JSON.stringify(p));
 s.get('observationFilter').value='TEST';s.get('observationFilter').oninput();assert.match(s.get('content').innerHTML,/1 shown \/ 2/);s.listeners.pagehide();
});

test('legacy full map and every published list occurrence are retained as unverified estimates',()=>{
 const m=require('../jh-estimate-observations.js'),p={by_ticker:{A:{ticker:'A',current_eps_est:0}},estimate_strength_leaders:[{ticker:'A'}],upward_revisions:[{ticker:'A'}],top_picks:[{ticker:'A'}]};
 const copy=JSON.stringify(p),r=m.rows(p);assert.equal(r.length,4);assert.equal(r[3].eps,0);assert.ok(r.every(x=>x.target===null&&x.change===null));assert.equal(JSON.stringify(p),copy);
 assert.throws(()=>m.rows({measurement_contract:m.CONTRACT,request_records:[],calendar_rows:null}),/Complete/);
 assert.throws(()=>m.rows({measurement_contract:m.CONTRACT,request_records:[{observations:null}],calendar_rows:[]}),/Malformed/);
});


test('scarcity retains every legacy subset and never rebrands scores as shortage evidence',async()=>{
 const p={vertical_tightness:[{theme_etf:'GDX',tightness:0}],stealth_shortage_board:[{ticker:'TEST',composite:13,why:'<img src=x>'}],prime_setups:[{ticker:'TEST'}]};
 const s=page('scarcity-radar.html',async()=>raw(p));await flush();assert.equal(s.calls.length,1);assert.equal(s.calls[0],'/data/scarcity-radar.json');
 assert.match(s.get('rows').textContent,/3 received/);assert.match(s.get('status').textContent,/Legacy publication/);assert.ok(!s.get('board').innerHTML.includes('<img'));
 assert.equal(s.get('original').textContent,JSON.stringify(p));assert.match(s.get('board').innerHTML,/&lt;img/);
 s.get('q').value='TEST';s.get('q').oninput();assert.match(s.get('rows').textContent,/2 shown \/ 3/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/3 shown/);
 const h=s.get('board').headers.find(x=>x.dataset.k==='ticker');h.focus();h.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'ticker');assert.equal(s.document.activeElement['aria-sort'],'ascending');
 s.document.activeElement.onkeydown({key:' ',preventDefault(){}});assert.equal(s.document.activeElement['aria-sort'],'descending');
});

test('scarcity native donor meaning, coordinates, zero and whole records stay intact',async()=>{
 const p={measurement_contract:'scarcity-donor-observations.v1',source_inventory:[{key:'data/narrative-vs-tape.json',label:'Narrative',status:'retained',producer_generated_at:'2026-09-27T00:00:00Z',capture:{},populations:[]}],donor_occurrences:[{ticker:'TEST',source_key:'data/narrative-vs-tape.json',source_pointer:'/quiet_accumulation/0',raw:{edge:'No inference of institutional buying',score:0,missing:null}}],stealth_shortage_board:[{ticker:'DO_NOT_USE'}]};
 const s=page('scarcity-radar.html',async()=>raw(p));await flush();const html=s.get('board').innerHTML;
 assert.match(html,/No inference of institutional buying/);assert.match(html,/\/quiet_accumulation\/0/);assert.match(html,/&quot;score&quot;: 0/);assert.ok(!html.includes('DO_NOT_USE'));
 assert.match(s.get('status').textContent,/no qualified shortage forecast/);assert.equal(s.get('original').textContent,JSON.stringify(p));
 const m=require('../jh-scarcity-observations.js');assert.equal(m.rows({...p,donor_occurrences:Array.from({length:400},()=>p.donor_occurrences[0])}).length,400);
 assert.throws(()=>m.rows({...p,donor_occurrences:{}}),/Complete/);assert.throws(()=>m.rows({prime_setups:{}}),/Malformed/);
});

test('scarcity failures preserve inert source and never show an empty valid board',async()=>{
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({stealth_shortage_board:{bad:true}}),async()=>{const e=Error('Duplicate JSON key');e.original_text='{"x":"<script>bad</script>","x":2}';throw e;}]){
  const s=page('scarcity-radar.html',loader);await flush();assert.match(s.get('status').textContent,/unavailable/);assert.equal(s.get('board').textContent,'No verified display population');assert.equal(s.get('rows').textContent,'');assert.ok(!s.get('original').innerHTML.includes('<script>'));
 }
 assert.ok(api.PUBLIC_PATHS.test('/data/scarcity-radar.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/scarcity-radar/private.json'));
});

test('workforce native rows keep date/currency/source and no legacy ranking fallback',async()=>{
 const m=require('../jh-hiring-observations.js'),record={symbol:'TEST',universe_record:{name:'A > B',sector:'Tech'},acquisitions:[{endpoint:'historical-employee-count',status:'received'}],
 employee_observations:[{source_index:0,report_period_end:'2025-12-31',employee_count:100,measurement_status:'reported_count'}],
 annual_comparisons:[{source_index:0,annual_interval_change_pct:0}],income_observations:[{employee_source_index:0,status:'aligned_descriptive_ratio',annual_revenue_per_ending_employee:0,reported_currency:'JPY'}]};
 const p={measurement_contract:m.CONTRACT,universe_occurrences:2,request_records:[record,{...record,symbol:'FAILED',employee_observations:[],annual_comparisons:[],income_observations:[],acquisitions:[{endpoint:'historical-employee-count',status:'unavailable'}]}],top_50:[{symbol:'NEVER_USE'}]};
 const s=page('hiring-velocity.html',async()=>raw(p));await flush();const h=s.get('board').innerHTML;
 assert.equal(s.calls.length,1);assert.match(h,/2025-12-31/);assert.match(h,/0\.00 JPY/);assert.match(h,/0\.00%/);assert.match(h,/FAILED/);assert.match(h,/Unavailable/);assert.ok(!h.includes('NEVER_USE'));assert.equal(s.get('original').textContent,JSON.stringify(p));
 const th=s.get('board').headers.find(x=>x.dataset.k==='ticker');th.focus();th.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'ticker');assert.equal(s.document.activeElement['aria-sort'],'ascending');
 s.get('q').value='FAILED';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching \/ 2 received/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/2 matching/);
 const all=m.rows({...p,request_records:Array.from({length:4202},(_,i)=>({...record,symbol:'T'+i}))});assert.equal(all.length,4202);
 let seen=[];for(let i=0;i<43;i++)seen.push(...m.pageRows(all,'','index',1,i,100,api).rows.map(x=>x.index));assert.equal(seen.length,4202);assert.equal(new Set(seen).size,4202);
 assert.equal(m.pageRows(all,'T4201','index',1,42,100,api).page,0);
 const ambiguous={...record,employee_observations:[...record.employee_observations,...record.employee_observations]};assert.equal(m.rows({...p,request_records:[ambiguous]})[0].headcount,null);
 assert.throws(()=>m.rows({...p,request_records:null}),/Whole/);assert.throws(()=>m.rows({...p,request_records:[{}]}),/Complete/);
});

test('workforce page pagination and failures retain all original evidence without executable markup',async()=>{
 const p={top_50:Array.from({length:205},(_,i)=>({symbol:'T'+i,name:i===0?'<img src=x>':'Company'}))};
 const s=page('hiring-velocity.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/Page 1 of 3/);assert.ok(!s.get('board').innerHTML.includes('<img'));assert.match(s.get('board').innerHTML,/&lt;img/);
 s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 2 of 3/);s.get('next').onclick();assert.match(s.get('board').innerHTML,/T204/);assert.equal(s.get('next').disabled,true);s.get('previous').onclick();assert.match(s.get('rows').textContent,/Page 2 of 3/);
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({top_50:{bad:true}})]){
  const fail=page('hiring-velocity.html',loader);await flush();assert.match(fail.get('status').textContent,/unavailable/);assert.equal(fail.get('board').textContent,'No verified display population');assert.equal(fail.get('next').disabled,true);
 }
});

test('EPS forecasts, rating actions and request coverage are separate complete populations',async()=>{
 const source=fs.readFileSync(path.join(root,'eps-velocity.html'),'utf8');
 assert.match(source,/data-feeds="data\/revision-breadth\.json\|justhodl-gap-metrics\|REVISION BREADTH · MARKET LEVEL"/);
 assert.match(source,/not company-level EPS revision evidence/);
 const M=require('../jh-eps-observations.js');const p={measurement_contract:M.CONTRACT,request_records:[{ticker:'TEST',acquisitions:[{endpoint:'grades',status:'received',original_base64:'EXACT_BYTES',original_sha256:'identity'}],quote_records:[{price:0}],
  estimate_observations:[{source_index:0,raw:{symbol:'TEST'},target_period_end:'2026-01-31',target_status:'past_target',values:{epsAvg:0},reported_currency:'JPY',measurement_status:'reported_forecast_observation'}],
  same_target_comparisons:[{source_index:0,eps_change:null}],rating_observations:[{raw:{symbol:'TEST'},reported_date:'2026-09-26',previous_grade:'Sell',new_grade:'Sell',grading_company:'A > B',reported_action:'maintain',status:'reported_rating_record'}]}],all_qualifying:[{symbol:'NEVER_USE'}]};
 const s=page('eps-velocity.html',async()=>raw(p));await flush();let html=s.get('board').innerHTML;
 assert.equal(s.calls.length,1);assert.equal(s.calls[0],'/data/eps-revision-velocity.json');assert.match(html,/2026-01-31/);assert.match(html,/<td>0\.000<\/td>/);assert.match(html,/JPY/);assert.match(html,/past_target/);assert.ok(!html.includes('NEVER_USE'));assert.equal(s.get('original').textContent,JSON.stringify(p));
 s.get('mode').value='ratings';s.get('mode').onchange();html=s.get('board').innerHTML;assert.match(html,/maintain/);assert.match(html,/A &gt; B/);assert.match(s.get('rows').textContent,/1 received rating records/);assert.ok(!html.includes('earnings_revision_breadth'));
 const h=s.get('board').headers.find(x=>x.dataset.k==='ticker');h.focus();h.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'ticker');assert.equal(s.document.activeElement['aria-sort'],'ascending');
 s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('board').innerHTML,/grades: received/);assert.ok(!s.get('board').innerHTML.includes('EXACT_BYTES'));assert.match(s.get('original').textContent,/EXACT_BYTES/);
 const all=M.model({...p,request_records:Array.from({length:501},()=>p.request_records[0])});assert.equal(all.targets.length,501);assert.equal(all.ratings.length,501);assert.equal(all.requests.length,501);
 assert.throws(()=>M.model({...p,request_records:null}),/Complete/);assert.throws(()=>M.model({...p,request_records:[{}]}),/Complete/);
});

test('EPS legacy complete company and summary occurrences stay unverified, paginated and escaped',async()=>{
 const p={all_qualifying:Array.from({length:194},(_,i)=>({symbol:'T'+i,score:99,rationale:'<img src=x>'})),summary:{top_25_overall:[{symbol:'T0'}],tier_a:['T0'],tier_b_symbols:['T1']}};
 const s=page('eps-velocity.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/197 received forecast records/);assert.match(s.get('status').textContent,/unverified/);assert.ok(!s.get('board').innerHTML.includes('<img'));assert.match(s.get('board').innerHTML,/&lt;img/);
 s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 2 of 2/);assert.match(s.get('board').innerHTML,/T193/);
 s.get('q').value='T193';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching \/ 197/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/197 matching/);
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({all_qualifying:{}})]){const failed=page('eps-velocity.html',loader);await flush();assert.match(failed.get('status').textContent,/unavailable/);assert.equal(failed.get('board').textContent,'No verified display population');}
 assert.ok(api.PUBLIC_PATHS.test('/data/eps-revision-velocity.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/eps-revision-velocity/private.json'));
});

test('EPS retained comparison exposes the original date and full verified source without refreshing it',async()=>{
 const M=require('../jh-eps-observations.js'),f=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/eps-retained-synthetic.json'),'utf8')),p=f.packet;
 const m=M.model(p),r=m.targets[0],d=p.request_records[0].comparison_source.baseline;
 assert.equal(r.change,1);assert.equal(r.value,3);assert.equal(r.source.received_at,d.received_at);assert.notEqual(d.received_at,p.generated_at);
 const s=page('eps-velocity.html',async()=>raw(p));await flush();assert.match(s.get('board').innerHTML,/Inspect complete retained original/);assert.ok(s.get('board').innerHTML.includes(d.received_at));
 assert.equal(s.get('original').textContent,JSON.stringify(p));s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('board').innerHTML,/not_attempted_runtime_rate_or_size_limit/);
 const ref=d.original_ref,body=Buffer.from(f.sources[ref.key]),calls=[];
 const fetcher=async(url,options)=>{calls.push(url);assert.equal(options.credentials,'omit');assert.equal(options.redirect,'error');return new Response(body);};
 assert.equal((await M.loadSource(ref,{fetcher})).raw,body.toString());assert.deepEqual(calls,['/'+ref.key+'?exact=1&nogen=1']);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(body.subarray(0,4))}),/byte count/);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(Buffer.alloc(body.length,97))}),/SHA-256/);
 await assert.rejects(()=>M.loadSource({...ref,key:'private/account.json'},{fetcher}),/source identity/);assert.equal(calls.length,1);
 await assert.rejects(()=>M.loadSource(ref,{timeout:5,fetcher:()=>new Promise(()=>{})}),/timed out/);
});

test('EPS malformed comparisons cannot be recovered by switching tabs or searching',async()=>{
 const M=require('../jh-eps-observations.js'),p=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/eps-retained-synthetic.json'),'utf8')).packet;
 for(const edit of [p=>p.request_records[0].same_target_comparisons.push(p.request_records[0].same_target_comparisons[0]),
  p=>p.request_records[0].comparison_source.baseline.original_ref=null,p=>p.request_records[0].comparison_source.baseline.ticker='OTHER',
  p=>p.request_records[0].comparison_source.status='not_read_runtime_reserve',p=>p.request_records[0].comparison_source.baseline.received_at='yesterday']){
  const q=structuredClone(p);edit(q);assert.throws(()=>M.model(q));const s=page('eps-velocity.html',async()=>raw(q));await flush();
  assert.match(s.get('status').textContent,/unavailable/);assert.equal(s.get('original').textContent,JSON.stringify(q));
  for(const mode of ['requests','targets','ratings']){s.get('mode').value=mode;s.get('mode').onchange();s.get('q').oninput();s.get('next').onclick();assert.equal(s.get('board').textContent,'No verified display population');assert.equal(s.get('rows').textContent,'');}
 }
});

test('EPS coverage distinguishes received arrays from visits and a capped visit cycle',async()=>{
 const M=require('../jh-eps-observations.js'),p=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/eps-resumption-synthetic.json'),'utf8')).packet;
 const m=M.model(p);assert.deepEqual([m.coverage.selected,m.coverage.visited,m.coverage.pending,m.coverage.received],[3,1,2,1]);assert.equal(m.targets[0].change,1);
 const s=page('eps-velocity.html',async()=>raw(p));await flush();assert.match(s.get('coverage').textContent,/1 of 3 selected request records visited/);assert.match(s.get('coverage').textContent,/2 remain/);assert.match(s.get('coverage').textContent,/not complete data coverage/);
 s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('rows').textContent,/3 selected request occurrences/);assert.doesNotMatch(s.get('rows').textContent,/received requests/);assert.match(s.get('board').innerHTML,/not_attempted_runtime_rate_or_size_limit/);assert.equal(s.get('original').textContent,JSON.stringify(p));
});

test('EPS false coverage cannot display a valid-looking population after navigation',async()=>{
 const M=require('../jh-eps-observations.js'),p=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/eps-resumption-synthetic.json'),'utf8')).packet;
 for(const edit of [p=>delete p.acquisition_progress,p=>p.acquisition_progress.remaining_symbols=[],p=>p.acquisition_progress.visited_occurrences=2,
  p=>p.acquisition_progress.planned_request_indices=[0,2,1],p=>p.acquisition_progress.visited_request_indices=[true],p=>p.acquisition_progress.retained_provider_bytes=0,
  p=>p.request_records[1].acquisitions[0].status='received']){
  const q=structuredClone(p);edit(q);assert.throws(()=>M.model(q));const s=page('eps-velocity.html',async()=>raw(q));await flush();
  assert.match(s.get('status').textContent,/unavailable/);assert.equal(s.get('original').textContent,JSON.stringify(q));
  for(const mode of ['targets','requests']){s.get('mode').value=mode;s.get('mode').onchange();s.get('q').oninput();assert.equal(s.get('board').textContent,'No verified display population');}
 }
});

test('revenue statements retain all periods, zero amounts, units, calculations and request evidence',async()=>{
 const M=require('../jh-revenue-observations.js');const record={ticker:'TEST',acquisitions:[{endpoint:'income-statement',status:'received',original_base64:'WHOLE_BYTES'}],quote_records:[],
 statement_observations:[{source_index:0,raw:{symbol:'TEST',unknown:'<img src=x>'},period_start:'2026-04-01',period_end:'2026-06-30',reported_period:'Q2',reported_currency:'JPY',values:{revenue:0},gross_margin_pct:null,status:'reported_statement_amounts'}],
 period_comparisons:[{source_index:0,yoy:{changes:{revenue:{pct_positive_base:-100}}},revenue_growth_acceleration_pp:-20,ttm_revenue:400,status:'explicit_comparable_calendar_quarters'}]};
 const p={measurement_contract:M.CONTRACT,request_records:[record],all_qualifying:[{symbol:'IGNORE'}]};const s=page('revenue-acceleration.html',async()=>raw(p));await flush();let html=s.get('board').innerHTML;
 assert.equal(s.calls[0],'/data/revenue-acceleration.json');assert.match(html,/2026-04-01/);assert.match(html,/JPY/);assert.match(html,/<td>0\.00<\/td>/);assert.match(html,/-100\.00/);assert.match(html,/400\.00/);assert.match(html,/&lt;img/);assert.ok(!html.includes('<img'));assert.ok(!html.includes('IGNORE'));assert.equal(s.get('original').textContent,JSON.stringify(p));
 s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('rows').textContent,/1 selected requests/);assert.match(s.get('board').innerHTML,/income-statement: received/);assert.ok(!s.get('board').innerHTML.includes('WHOLE_BYTES'));
 const h=s.get('board').headers.find(x=>x.dataset.k==='ticker');h.focus();h.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'ticker');
 const many=M.model({...p,request_records:Array.from({length:501},()=>record)});assert.equal(many.statements.length,501);assert.equal(many.requests.length,501);
 assert.throws(()=>M.model({...p,request_records:null}),/Complete/);assert.throws(()=>M.model({...p,request_records:[{}]}),/Complete/);
});

test('revenue legacy populations, pagination, failures and Intel summary cannot recover unsupported tiers',async()=>{
 const M=require('../jh-revenue-observations.js');const p={all_qualifying:Array.from({length:201},(_,i)=>({symbol:'T'+i,score:99,tier:'TIER_S'})),summary:{top_25_overall:[{symbol:'T0'}],tier_s:['T0'],microcap_picks:[{symbol:'T1'}]}};
 const s=page('revenue-acceleration.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/204 received/);s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='T200';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/204 matching/);
 assert.match(M.summary(p).message,/unverified/);assert.ok(!M.summary(p).message.includes('99'));
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({all_qualifying:{bad:true}})]){const f=page('revenue-acceleration.html',loader);await flush();assert.match(f.get('status').textContent,/unavailable/);assert.equal(f.get('board').textContent,'No verified display population');}
 assert.ok(api.PUBLIC_PATHS.test('/data/revenue-acceleration.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/revenue-acceleration/private.json'));
 for(const file of ['intel/index.html','web/intel/index.html']){
  const html=fs.readFileSync(path.join(root,file),'utf8'),block=html.split('// Revenue statement research:')[1].split('// Microcap source observations;')[0];
  assert.match(block,/JHTableValues.load\('\/data\/revenue-acceleration.json'\)/);assert.match(block,/JHRevenueObservations.summary/);assert.ok(!block.includes('top_25_overall'));assert.ok(!block.includes('r.score'));assert.match(block,/catch/);assert.match(html,/href="\/revenue-acceleration.html"/);
  const nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{textContent:'old score'});return nodes.get(id);};
  let fail=false;const seen=[];
  const context=vm.createContext({document:{getElementById:get},JHRevenueObservations:M,JHTableValues:{load:async path=>{seen.push(path);if(fail)throw Error('HTTP 403');return{packet:p};}}});
  const actual='(async()=>{\n// Revenue statement research:'+block+'\n})()';
  await vm.runInContext(actual,context);assert.match(get('rev-accel').textContent,/204 published legacy occurrences/);assert.equal(get('ra-fresh').textContent,'RESEARCH');
  fail=true;await vm.runInContext(actual,context);assert.match(get('rev-accel').textContent,/unavailable/);assert.equal(get('ra-meta').textContent,'');assert.equal(get('ra-fresh').textContent,'UNAVAILABLE');assert.deepEqual(seen,['/data/revenue-acceleration.json','/data/revenue-acceleration.json']);
 }
 assert.equal(fs.readFileSync(path.join(root,'intel/index.html'),'utf8'),fs.readFileSync(path.join(root,'web/intel/index.html'),'utf8'));
 for(const name of ['pre-revenue-intel-index.html.txt','pre-revenue-web-intel-index.html.txt'])assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,'tests/fixtures',name))).digest('hex'),'b13437d6ea6b612b49678b05178857fc694b5cda543cdf69d2dfbe4c32e79716');
});

test('PEAD retains typed event amounts, full price responses, source coordinates and request coverage',async()=>{
 const M=require('../jh-pead-observations.js'),prices=[{symbol:'TEST',date:'2026-08-01',close:0,volume:0,unknown:'<img src=x>'},{symbol:'TEST',date:'2026-08-02',close:true,volume:null},null];
 const body=Buffer.from(JSON.stringify(prices)),record={ticker:'TEST',acquisitions:[{endpoint:'earnings',status:'received'},{endpoint:'historical-price-eod/full',status:'received',original_base64:body.toString('base64'),original_bytes:body.length}],price_coverage:{records:3},
 event_observations:[{source_index:0,raw:{symbol:'TEST',unknown:'<img src=x>'},announcement_date:'2026-08-01',fiscal_period_end:'2026-06-30',reported_period:'Q2',reported_fiscal_year:'2026',reported_currency:'JPY',reported_eps_basis:'diluted_gaap',fields:{eps_actual:{value:0},eps_estimate:{value:1},revenue_actual:{value:200},revenue_estimate:{value:100}},status:'reported_event_values'}],
 event_differences:[{source_index:0,differences:{eps:{actual_minus_reported_estimate:-1,status:'descriptive_current_record_difference'},revenue:{actual_minus_reported_estimate:100,status:'descriptive_current_record_difference'}}}]};
 const p={measurement_contract:M.CONTRACT,request_records:[record],all_qualifying:[{symbol:'IGNORE'}]},s=page('earnings-pead.html',async()=>raw(p));await flush();let h=s.get('board').innerHTML;
 assert.equal(s.calls[0],'/data/earnings-pead.json');assert.match(h,/2026-08-01/);assert.match(h,/JPY/);assert.match(h,/<td>0\.00<\/td>/);assert.match(h,/-1\.00/);assert.match(h,/200\.00/);assert.match(h,/&lt;img/);assert.ok(!h.includes('<img'));assert.ok(!h.includes('IGNORE'));s.get('packet-details').open=true;s.get('packet-details').ontoggle();assert.equal(s.get('original').textContent,JSON.stringify(p));
 s.get('mode').value='prices';s.get('mode').onchange();assert.match(s.get('rows').textContent,/3 received prices/);h=s.get('board').innerHTML;assert.match(h,/0\.00/);assert.match(h,/Unavailable/);assert.match(h,/original_base64/);assert.ok(!h.includes('<img'));
 s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('rows').textContent,/1 selected requests/);assert.match(s.get('board').innerHTML,/historical-price-eod\/full: received/);assert.ok(!s.get('board').innerHTML.includes(body.toString('base64')));
 const header=s.get('board').headers.find(x=>x.dataset.k==='ticker');header.focus();header.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'ticker');
 const many=M.model({...p,request_records:Array.from({length:501},()=>record)});assert.equal(many.events.length,501);assert.equal(many.prices.length,1503);
 assert.throws(()=>M.model({...p,request_records:null}),/Complete/);assert.throws(()=>M.model({...p,request_records:[{}]}),/Complete/);
 const bad=structuredClone(p);bad.request_records[0].acquisitions[1].original_bytes=1;assert.throws(()=>M.model(bad),/byte count/);
});

function earningsWithPrices(){
 const p=resumedEarnings();
 for(let i=0;i<2;i++){
  const body=Buffer.from(JSON.stringify(Array.from({length:101},(_,n)=>({symbol:'PRICE'+i,date:'2026-08-01',close:n,volume:0,unknown:'<img src=x>'}))));
  p.request_records[i].acquisitions.push({endpoint:'historical-price-eod/full',status:'received',original_base64:body.toString('base64'),original_bytes:body.length});
 }
 return p;
}

test('PEAD price sources decode only on demand, once, with full pagination and keyboard sorting',async()=>{
 const p=earningsWithPrices(),s=page('earnings-pead.html',async()=>raw(p));let decodes=0;s.ctx.atob=x=>{decodes++;return atob(x);};await flush();
 assert.equal(decodes,0);assert.match(s.get('original').textContent,/Open this section/);
 s.get('mode').value='requests';s.get('mode').onchange();assert.equal(decodes,0);
 s.get('mode').value='prices';s.get('mode').onchange();assert.equal(decodes,2);assert.match(s.get('rows').textContent,/202 received prices/);
 s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='PRICE1';s.get('q').oninput();assert.match(s.get('rows').textContent,/101 matching \/ 202/);
 const h=s.get('board').headers.find(x=>x.dataset.k==='close');h.focus();h.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'close');
 s.get('q').value='';s.get('q').oninput();s.get('mode').value='events';s.get('mode').onchange();s.get('mode').value='prices';s.get('mode').onchange();assert.equal(decodes,2);
 assert.equal(s.calls.length,1);assert.ok(!s.get('board').innerHTML.includes('<img'));
});

test('PEAD failed price decoding exposes no partial population and keeps valid other views',async()=>{
 const p=earningsWithPrices();p.request_records[1].acquisitions[1].original_bytes++;
 const s=page('earnings-pead.html',async()=>raw(p));let decodes=0;s.ctx.atob=x=>{decodes++;return atob(x);};await flush();assert.equal(decodes,0);
 s.get('mode').value='prices';s.get('mode').onchange();assert.match(s.get('board').textContent,/Original price byte count differs/);assert.equal(s.get('rows').textContent,'No verified price population');assert.equal(decodes,2);
 s.get('q').oninput();assert.equal(decodes,2);assert.equal(s.get('next').disabled,true);
 s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('rows').textContent,/3 selected requests/);assert.match(s.get('board').innerHTML,/LATER/);
 s.get('mode').value='prices';s.get('mode').onchange();assert.equal(decodes,2);assert.equal(s.get('board').innerHTML,'');
 s.get('packet-details').open=true;s.get('packet-details').ontoggle();assert.equal(s.get('original').textContent,JSON.stringify(p));
});

test('PEAD complete originals stay exact across closed, opened, reopened and failed parsing states',async()=>{
 const p=resumedEarnings(),received={packet:p,raw:'\n'+JSON.stringify(p,null,2)+'\n'};
 for(const initiallyOpen of [false,true]){
  const s=page('earnings-pead.html',async()=>received),d=s.get('packet-details');d.open=initiallyOpen;await flush();
  if(initiallyOpen)assert.equal(s.get('original').textContent,received.raw);else assert.match(s.get('original').textContent,/Open this section/);
  for(const open of [true,false,true]){d.open=open;d.ontoggle();if(open)assert.equal(s.get('original').textContent,received.raw);else assert.match(s.get('original').textContent,/Open this section/);}
 }
 const original='{"whole malformed source":',s=page('earnings-pead.html',async()=>{throw Object.assign(Error('Malformed JSON'),{original_text:original});});await flush();
 assert.match(s.get('status').textContent,/Malformed JSON/);s.get('packet-details').open=true;s.get('packet-details').ontoggle();assert.equal(s.get('original').textContent,original);
 s.get('mode').value='prices';s.get('mode').onchange();assert.equal(s.get('board').textContent,'No verified display population');
});

test('PEAD legacy occurrences paginate, malformed feeds fail visibly and both Intel cards clear stale content',async()=>{
 const M=require('../jh-pead-observations.js'),p={all_qualifying:Array.from({length:201},(_,i)=>({symbol:'T'+i,score:99,tier:'TIER_S'})),summary:{top_25_overall:[{symbol:'T0'}],tier_s:['T0'],tier_s_full:[{symbol:'T0'}]}};
 const s=page('earnings-pead.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/204 received/);s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='T200';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/204 matching/);
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({all_qualifying:{bad:true}})]){const f=page('earnings-pead.html',loader);await flush();assert.match(f.get('status').textContent,/unavailable/);assert.equal(f.get('board').textContent,'No verified display population');}
 assert.ok(api.PUBLIC_PATHS.test('/data/earnings-pead.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/earnings-pead/private.json'));
 for(const file of ['intel/index.html','web/intel/index.html']){
  const html=fs.readFileSync(path.join(root,file),'utf8'),block=html.split('// Earnings event research:')[1].split('// Options Flow')[0];assert.ok(!block.includes('top_25_overall'));assert.ok(!block.includes('r.score'));assert.match(html,/href="\/earnings-pead.html"/);
  const nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{textContent:'old score'});return nodes.get(id);};let fail=false;const seen=[];
  const context=vm.createContext({document:{getElementById:get},JHPEADObservations:M,JHTableValues:{load:async path=>{seen.push(path);if(fail)throw Error('HTTP 403');return{packet:p};}}});
  const actual='(async()=>{\n// Earnings event research:'+block+'\n})()';await vm.runInContext(actual,context);assert.match(get('pead').textContent,/204 published legacy occurrences/);assert.equal(get('pead-fresh').textContent,'RESEARCH');
  fail=true;await vm.runInContext(actual,context);assert.match(get('pead').textContent,/unavailable/);assert.equal(get('pead-meta').textContent,'');assert.equal(get('pead-fresh').textContent,'UNAVAILABLE');assert.deepEqual(seen,['/data/earnings-pead.json','/data/earnings-pead.json']);
 }
 assert.equal(fs.readFileSync(path.join(root,'intel/index.html'),'utf8'),fs.readFileSync(path.join(root,'web/intel/index.html'),'utf8'));
});


test('PEAD original pages are whole and all current tables remain accessible',()=>{
 for(const [name,digest] of [['pre-earnings-pead-observations.html.txt','b9dcf3e8b86f94260c4b2943d77149ee1b13bade12820da9995108792559963c'],['pre-pead-intel-index-observations.html.txt','bd1507bda262824fba5e5b1fd38b042056b5b3cf81f68d358f80ce1cf38903bc'],['pre-pead-web-intel-index-observations.html.txt','bd1507bda262824fba5e5b1fd38b042056b5b3cf81f68d358f80ce1cf38903bc']])assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,'tests/fixtures',name))).digest('hex'),digest);
 const html=fs.readFileSync(path.join(root,'earnings-pead.html'),'utf8');assert.match(html,/role="region"[^>]+tabindex="0"/);assert.match(html,/<label for="q">/);assert.match(html,/overflow:auto/);
});

test('Microcap flow shares preserve exact decimals, inclusions, zero volumes and complete source links',async()=>{
 const M=require('../jh-microcap-observations.js'),digest='a'.repeat(64),ref={key:'data/microcap-float-squeeze/sources/'+digest+'.txt',bytes:100,sha256:digest,format:'txt'};
 const p={measurement_contract:M.CONTRACT,finra_acquisitions:[{status:'received',original_ref:ref}],finra_file_coverage:{files:[{acquisition_index:0,observation_date:'2026-09-25',reported_rows:13175,matched_rows:1,status:'whole_cnms_file_parsed'}]},request_records:[{ticker:'TEST',acquisitions:[],latest_parsed_finra_date:'2026-09-25',price_evidence:{records:95,averages:{'30':{reported_volume_mean:0},'60':{reported_volume_mean:50}}},finra_observations:[{acquisition_index:0,observation_date:'2026-09-25',short_volume_shares:'1.125',short_exempt_volume_shares:'0.125',total_volume_shares:'2.25',short_volume_pct:'50.000000000000',source_fields:{Market:'Q,N',unknown:'<img src=x>'}}]}]};
 const s=page('microcap-float-squeeze.html',async()=>raw(p));await flush();let h=s.get('board').innerHTML;
 assert.equal(s.calls[0],'/data/microcap-float-squeeze.json');assert.match(h,/<td>1\.125<\/td>/);assert.match(h,/0\.125/);assert.match(h,/50\.000000000000/);assert.match(h,/Q,N/);assert.match(h,new RegExp('href="/'+ref.key+'"'));assert.match(h,/&lt;img/);assert.ok(!h.includes('<img'));assert.equal(s.get('original').textContent,JSON.stringify(p));
 s.get('mode').value='requests';s.get('mode').onchange();h=s.get('board').innerHTML;assert.match(h,/<td>0\.00<\/td>/);assert.match(h,/50\.00/);assert.match(s.get('rows').textContent,/1 received requests/);
 s.get('mode').value='files';s.get('mode').onchange();assert.match(s.get('board').innerHTML,/13175/);assert.match(s.get('rows').textContent,/1 received files/);
 const header=s.get('board').headers.find(x=>x.dataset.k==='date');header.focus();header.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'date');assert.equal(s.document.activeElement['aria-sort'],'ascending');
 const all=M.model({...p,request_records:Array.from({length:601},()=>p.request_records[0])});assert.equal(all.flows.length,601);assert.equal(all.requests.length,601);
 for(const change of [{key:'private/account.json'},{bytes:9000000},{sha256:[digest]},{format:'html'}])assert.throws(()=>M.source({...ref,...change}),/source identity/);
 assert.ok(M.compareDecimal({index:0,v:'9999999999999999999999999999.1'},{index:1,v:'9999999999999999999999999999.2'},'v',1)<0);
 assert.ok(M.compareDecimal({index:0,v:null},{index:1,v:'0'},'v',-1)>0);
 assert.throws(()=>M.model({...p,request_records:[{}]}),/Complete/);
});

test('Microcap legacy complete populations paginate, malformed feeds fail and Intel summaries stay unqualified',async()=>{
 const M=require('../jh-microcap-observations.js'),p={all_qualifying:Array.from({length:201},(_,i)=>({symbol:'T'+i,score:99,tier:'PARABOLIC'})),summary:{top_25_overall:[{symbol:'T0'}],tier_s:['T0']}};
 const s=page('microcap-float-squeeze.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/203 received/);s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='T200';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/203 matching/);
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({all_qualifying:{bad:true}})]){const f=page('microcap-float-squeeze.html',loader);await flush();assert.match(f.get('status').textContent,/unavailable/);assert.equal(f.get('board').textContent,'No verified display population');}
 assert.ok(api.PUBLIC_PATHS.test('/data/microcap-float-squeeze.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/microcap-float-squeeze/private.json'));
 for(const file of ['intel/index.html','web/intel/index.html']){
  const html=fs.readFileSync(path.join(root,file),'utf8'),block=html.split('// Microcap source observations;')[1].split('// Earnings event research:')[0];assert.ok(!block.includes('top_25_overall'));assert.match(html,/href="\/microcap-float-squeeze.html"/);
  const nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{textContent:'old score'});return nodes.get(id);};let fail=false;
  const context=vm.createContext({document:{getElementById:get},JHMicrocapObservations:M,JHTableValues:{load:async()=>{if(fail)throw Error('HTTP 403');return{packet:p};}}});
  const actual='(async()=>{\n// Microcap source observations;'+block+'\n})()';await vm.runInContext(actual,context);assert.match(get('microcap-squeeze').textContent,/203 legacy occurrences/);assert.equal(get('ms-fresh').textContent,'RESEARCH');
  fail=true;await vm.runInContext(actual,context);assert.match(get('microcap-squeeze').textContent,/unavailable/);assert.equal(get('ms-meta').textContent,'');assert.equal(get('ms-fresh').textContent,'UNAVAILABLE');
 }
});

test('Microcap original viewer verifies exact bytes and hashes before showing retained text',async()=>{
 const M=require('../jh-microcap-observations.js'),body=Buffer.from('Date|Symbol\nTEST|<img src=x>\n'),digest=crypto.createHash('sha256').update(body).digest('hex'),ref={key:'data/microcap-float-squeeze/sources/'+digest+'.txt',sha256:digest,bytes:body.length,format:'txt'};
 const calls=[],fetcher=async(url,options)=>{calls.push(url);assert.equal(options.credentials,'omit');assert.equal(options.redirect,'error');return new Response(body);};
 assert.equal((await M.loadSource(ref,{fetcher})).raw,body.toString());assert.deepEqual(calls,['/'+ref.key+'?exact=1&nogen=1']);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(body.subarray(0,3))}),/byte count/);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(Buffer.alloc(body.length,97))}),/SHA-256/);
 await assert.rejects(()=>M.loadSource({...ref,key:'private/accounts.json'},{fetcher}),/source identity/);assert.equal(calls.length,1);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response('denied',{status:403})}),/unavailable/);
 await assert.rejects(()=>M.loadSource(ref,{timeout:5,fetcher:()=>new Promise(()=>{})}),/timed out/);
});

test('concurrent Quality-sector source parses and its text formatter escapes markup',()=>{
 const html=fs.readFileSync(path.join(root,'quality-sector.html'),'utf8'),scripts=[...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)];for(const match of scripts)new vm.Script(match[1]);
 const escLine=html.split('\n').find(line=>line.startsWith('function esc(')),context=vm.createContext({});vm.runInContext(escLine,context);assert.equal(context.esc('<img src="x">&'), '&lt;img src=&quot;x&quot;&gt;&amp;');
 assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,'tests/fixtures/pre-quality-sector-escape.html.txt'))).digest('hex'),'c8ee8fbd0349ebf1c97e0429b0c1792573b5e54b4e82ae9b27411f4e00313d3d');
});

test('Option original viewer verifies exact bytes and hashes before showing retained text',async()=>{
 const M=require('../jh-option-observations.js'),body=Buffer.from('Date|Symbol\nTEST|<img src=x>\n'),digest=crypto.createHash('sha256').update(body).digest('hex'),ref={key:'data/options-flow-scanner/sources/'+digest+'.txt',sha256:digest,bytes:body.length,format:'txt'};
 const calls=[],fetcher=async(url,options)=>{calls.push(url);assert.equal(options.credentials,'omit');assert.equal(options.redirect,'error');return new Response(body);};
 assert.equal((await M.loadSource(ref,{fetcher})).raw,body.toString());assert.deepEqual(calls,['/'+ref.key+'?exact=1&nogen=1']);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(body.subarray(0,3))}),/byte count/);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(Buffer.alloc(body.length,97))}),/SHA-256/);
 await assert.rejects(()=>M.loadSource({...ref,key:'private/accounts.json'},{fetcher}),/source identity/);assert.equal(calls.length,1);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response('denied',{status:403})}),/unavailable/);
 await assert.rejects(()=>M.loadSource(ref,{timeout:5,fetcher:()=>new Promise(()=>{})}),/timed out/);
});


test('option legacy occurrences retain pagination, search, keyboard sorting and failure clearing',async()=>{
 const M=require('../jh-option-observations.js'),p={all_qualifying:Array.from({length:201},(_,i)=>({symbol:'T'+i,score:99})),summary:{top_25_overall:[{symbol:'T0'}],tier_a:['T0']}};
 const s=page('options-scanner.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/203 received/);s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='T200';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/203 matching/);
 assert.equal(s.get('original').textContent,JSON.stringify(p));assert.equal(s.calls[0],'/data/options-flow-scanner.json');
 const h=s.get('board').headers.find(h=>h.dataset.k==='ticker');h.focus();h.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'ticker');
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({all_qualifying:{bad:true}})]){const f=page('options-scanner.html',loader);await flush();assert.match(f.get('status').textContent,/unavailable/);assert.equal(f.get('board').textContent,'No verified display population');}
 for(const file of ['intel/index.html','web/intel/index.html']){
  const html=fs.readFileSync(path.join(root,file),'utf8'),block=html.split('// Options Flow:')[1].split('// Activist')[0],nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{textContent:'old score'});return nodes.get(id);};let fail=false;
  const context=vm.createContext({document:{getElementById:get},JHOptionObservations:M,fetchJson:async()=>fail?null:p});
  const actual='(async()=>{\n// Options Flow:'+block+'\n})()';await vm.runInContext(actual,context);assert.match(get('options-flow').textContent,/203 legacy occurrences/);assert.equal(get('of-fresh').textContent,'RESEARCH');
  fail=true;await vm.runInContext(actual,context);assert.match(get('options-flow').textContent,/unavailable/);assert.equal(get('of-meta').textContent,'');assert.equal(get('of-fresh').textContent,'UNAVAILABLE');
 }
});
test('option native page exposes every contract, bar, request, daily ratio and FINRA population without granting authority',async()=>{
 const M=require('../jh-option-observations.js'),digest='a'.repeat(64),ref={key:'data/options-flow-scanner/sources/'+digest+'.json',sha256:digest,bytes:100,format:'json'},a={status:'received',original_ref:ref};
 const p={measurement_contract:M.CONTRACT,finra_acquisitions:[a],finra_file_coverage:{files:[{observation_date:'2026-09-25',status:'whole_cnms_file_parsed',reported_rows:1}]},request_records:[{ticker:'<img src=x>',acquisitions:{quote:a,contract_pages:[a],bar_requests:[a]},contract_population:{records:[{contract_id:'O:TEST',page_index:0,expiration_date:'2026-11-20',identity_issues:[]}],selected_record_indices:[0]},bar_populations:[{records:[{contract_id:'O:TEST',observation_date:'2026-09-25',reported_volume_contracts:'0',issues:[]}]}],daily_observations:[{observation_date:'2026-09-25',call_volume_contracts:'0',put_volume_contracts:'10',call_put_volume_ratio:'0.000000000000'}],finra_observations:[{acquisition_index:0,observation_date:'2026-09-25',short_volume_shares:'1.125',short_exempt_volume_shares:'0.125',total_volume_shares:'2.25',short_volume_pct:'50.000000000000'}]}]};
 const s=page('options-scanner.html',async()=>raw(p));await flush();assert.match(s.get('board').innerHTML,/<td>0<\/td>/);assert.match(s.get('board').innerHTML,/&lt;img/);assert.ok(!s.get('board').innerHTML.includes('<img'));
 for(const mode of ['contracts','daily','requests','flows','files']){s.get('mode').value=mode;s.get('mode').onchange();assert.match(s.get('rows').textContent,/1 matching/);assert.match(s.get('board').innerHTML,/Inspect whole original/);}
 s.get('mode').value='flows';s.get('mode').onchange();assert.match(s.get('board').innerHTML,/1\.125/);assert.match(s.get('board').innerHTML,/0\.125/);
 const model=M.model(p);for(const k of ['bars','contracts','daily','flows','files','requests'])assert.equal(model[k].length,1);
 assert.equal(M.compareDecimal({index:0,volume:'9007199254740992'},{index:1,volume:'9007199254740993'},'volume',1),-1);
 assert.throws(()=>M.model({...p,request_records:[{acquisitions:{}}]}),/Incomplete/);
});


test('selected leader price observations retain acquisition ancestry and unavailable annual windows',async()=>{
 const M=require('../jh-leader-observations.js'),fixture=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/leader-price-synthetic.json'),'utf8')),p=fixture.packet;
 const model=M.model(p);assert.equal(model.requests.length,4);assert.equal(model.windows.length,2);assert.equal(model.universe.length,3);assert.equal(model.measurements.length,9);
 assert.equal(model.measurements.find(r=>r.name==='fifty_two_week_high').value,null);
 const s=page('leader-observations.html',async()=>raw(p));await flush();assert.equal(s.calls[0],'/data/momentum-leaders.json');assert.equal(s.get('original').textContent,JSON.stringify(p));
 for(const [mode,n]of [['measurements',9],['windows',2],['universe',3]]){s.get('mode').value=mode;s.get('mode').onchange();assert.match(s.get('rows').textContent,new RegExp(n+' received '+mode));}
 s.get('mode').value='measurements';s.get('mode').onchange();const head=s.get('board').headers.find(x=>x.dataset.k==='value');head.focus();head.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'value');
 for(const field of ['calls_eligible','sizing_eligible','execution_eligible'])assert.throws(()=>M.model({...p,[field]:true}),/permissions/);
 const missing=structuredClone(p);delete missing.input_acquisitions['data/ticker-trends.json'];assert.throws(()=>M.model(missing),/outcomes/);
 const wrong=structuredClone(p);wrong.input_acquisitions['data/ticker-trends.json'].original_ref.key='private/accounts.json';assert.throws(()=>M.model(wrong),/source identity/);
 const unsafe=structuredClone(p);unsafe.universe_membership.occurrences[0].category='<img src=x onerror=alert(1)>';
 const escaped=page('leader-observations.html',async()=>raw(unsafe));await flush();escaped.get('mode').value='universe';escaped.get('mode').onchange();assert.match(escaped.get('board').innerHTML,/&lt;img/);assert.ok(!escaped.get('board').innerHTML.includes('<img'));
});

test('selected leader legacy populations paginate fully and whole originals verify by hash',async()=>{
 const M=require('../jh-leader-observations.js'),p={all_scored:Array.from({length:201},(_,i)=>({ticker:'T'+i,momentum_score:99})),leaders:[{ticker:'T0'}],pump_confirmed:[]};
 const s=page('leader-observations.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/202 received/);s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='T200';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/202 matching/);
 const fixture=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/leader-price-synthetic.json'),'utf8')),ref=fixture.packet.input_acquisitions['data/convergence-radar.json'].original_ref,body=Buffer.from(fixture.sources[ref.key]);
 assert.equal((await M.loadSource(ref,{fetcher:async()=>new Response(body)})).raw,body.toString());
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(body.subarray(0,4))}),/byte count/);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(Buffer.alloc(body.length,97))}),/SHA-256/);
 assert.ok(api.PUBLIC_PATHS.test('/data/momentum-leaders.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/momentum-leaders-state.json'));
 const f=page('leader-observations.html',async()=>{throw Error('HTTP 403');});await flush();assert.match(f.get('status').textContent,/unavailable/);assert.equal(f.get('board').textContent,'No verified display population');
});

test('pre-pump selected-price component abstains without fetching legacy ranks',()=>{
 const html=fs.readFileSync(path.join(root,'pre-pump-radar.html'),'utf8');
 const active=html.split('function renderMomentumPanel(){')[1].split('function _legacy_renderMomentumPanel(){')[0];
 assert.match(active,/href="\/leader-observations.html"/);assert.ok(!active.includes('momentum_score'));assert.ok(!html.includes('fetch(MOMENTUM_URL'));
 const predecessor=fs.readFileSync(path.join(root,'tests/fixtures/pre-leader-price-pre-pump-radar.html.txt'),'utf8');assert.ok(predecessor.includes('fetch(MOMENTUM_URL'));assert.ok(predecessor.includes('The Actual Movers'));
});

test('momentum observations expose matched benchmark sources and every selected occurrence',async()=>{
 const M=require('../jh-momentum-observations.js'),fixture=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/momentum-price-synthetic.json'),'utf8')),p=fixture.packet;
 const model=M.model(p);assert.equal(model.requests.length,3);assert.equal(model.windows.length,3);assert.equal(model.universe.length,3);assert.equal(model.measurements.length,22);
 const comparison=model.measurements.find(r=>r.pointer.endsWith('/benchmark_comparisons/20'));assert.equal(comparison.value,0);assert.equal(comparison.sources.length,2);
 const s=page('momentum-observations.html',async()=>raw(p));await flush();assert.equal(s.calls[0],'/data/momentum-breakout.json');assert.equal(s.get('original').textContent,JSON.stringify(p));
 for(const [mode,n]of [['measurements',22],['windows',3],['universe',3]]){s.get('mode').value=mode;s.get('mode').onchange();assert.match(s.get('rows').textContent,new RegExp(n+' received '+mode));}
 assert.match(s.get('board').innerHTML,/&lt;img/);assert.ok(!s.get('board').innerHTML.includes('<img'));
 s.get('mode').value='measurements';s.get('mode').onchange();const head=s.get('board').headers.find(x=>x.dataset.k==='value');head.focus();head.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'value');
 for(const field of ['calls_eligible','sizing_eligible','execution_eligible'])assert.throws(()=>M.model({...p,[field]:true}),/permissions/);
 const bad=structuredClone(p);bad.benchmark.acquisition.original_ref.key='data/private.json';assert.throws(()=>M.model(bad),/source identity/);
 const missing=structuredClone(p);missing.request_records[0].observations.benchmark_comparisons['20'].value=null;
 assert.equal(M.model(missing).measurements.find(r=>r.pointer==='/request_records/0/observations/benchmark_comparisons/20').value,null);
});

test('momentum legacy population pagination and public-source integrity',async()=>{
 const M=require('../jh-momentum-observations.js'),p={all_qualifying:Array.from({length:201},(_,i)=>({symbol:'T'+i,score:99})),summary:{top_25_overall:[{symbol:'T0'}]}};
 const s=page('momentum-observations.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/202 received/);s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='T200';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/202 matching/);
 const fixture=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/momentum-price-synthetic.json'),'utf8')),ref=fixture.packet.benchmark.acquisition.original_ref,body=Buffer.from(fixture.sources[ref.key]);
 assert.equal((await M.loadSource(ref,{fetcher:async()=>new Response(body)})).raw,body.toString());
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(body.subarray(0,4))}),/byte count/);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(Buffer.alloc(body.length,97))}),/SHA-256/);
 assert.ok(api.PUBLIC_PATHS.test('/data/momentum-breakout.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/momentum-breakout-state.json'));
 const f=page('momentum-observations.html',async()=>{throw Error('HTTP 403');});await flush();assert.match(f.get('status').textContent,/unavailable/);assert.equal(f.get('board').textContent,'No verified display population');
});

test('momentum compound-page components abstain and retain full original source access',()=>{
 const upside=fs.readFileSync(path.join(root,'upside-radar.html'),'utf8').split('   momentum:\n')[1].split('   pead:')[0];
 assert.match(upside,/\/momentum-observations.html/);assert.ok(!upside.includes('all_qualifying'));assert.ok(!upside.includes('TIER A'));
 const why=fs.readFileSync(path.join(root,'why-cross-signal.html'),'utf8');assert.match(why,/k === "momentum" \? \{status:"research_only_abstain",investment_votes:0\}/);assert.match(why,/href="\/momentum-observations.html"/);
});

test('ownership filings show complete roles, share classes, coverage and inert original evidence',async()=>{
 const M=require('../jh-activist-observations.js'),fixture=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/ownership-filings-synthetic.json'),'utf8')),p=fixture.packet;
 const model=M.model(p);assert.equal(model.entries.length,3);assert.equal(model.groups.length,1);assert.equal(model.feeds.length,8);assert.equal(model.mapping.length,3);assert.equal(model.universe.length,3);
 const s=page('activist-filings.html',async()=>raw(p));await flush();assert.equal(s.calls[0],'/data/activist-filings.json');assert.equal(s.get('original').textContent,JSON.stringify(p));
 assert.match(s.get('board').innerHTML,/&lt;img/);assert.ok(!s.get('board').innerHTML.includes('<img'));assert.match(s.get('rows').textContent,/3 received entries/);
 for(const [mode,n]of [['groups',1],['feeds',8],['mapping',3],['universe',3]]){s.get('mode').value=mode;s.get('mode').onchange();assert.match(s.get('rows').textContent,new RegExp(n+' received '+mode));assert.match(s.get('board').innerHTML,/Inspect whole original/);}
 s.get('mode').value='groups';s.get('mode').onchange();assert.match(s.get('board').innerHTML,/TEST.A, TEST.B/);assert.match(s.get('board').innerHTML,/Co filer/);
 const header=s.get('board').headers.find(x=>x.dataset.k==='accession');header.focus();header.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'accession');
 for(const field of ['calls_eligible','sizing_eligible','execution_eligible'])assert.throws(()=>M.model({...p,[field]:true}),/permissions/);
 const bad=structuredClone(p);bad.filing_groups[0].occurrence_indices=[99];assert.throws(()=>M.model(bad),/coordinate/);
 assert.throws(()=>M.model({...p,feed_coverage:[]}),/feed outcomes/);
});

test('ownership legacy records retain full pagination, missing feed meaning and error clearing',async()=>{
 const p={all_filings:Array.from({length:201},(_,i)=>({subject_ticker:'T'+i,score:99,filer_name:'Reporting '+i})),summary:{top_25_overall:[{subject_ticker:'T0'}]}};
 const s=page('activist-filings.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/202 received/);assert.match(s.get('status').textContent,/Feed success, identity and investment tiers are unqualified/);
 s.get('next').onclick();s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 3 of 3/);
 s.get('q').value='T200';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/202 matching/);
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({all_filings:{bad:true}})]){const f=page('activist-filings.html',loader);await flush();assert.match(f.get('status').textContent,/unavailable/);assert.equal(f.get('board').textContent,'No verified display population');}
 assert.ok(api.PUBLIC_PATHS.test('/data/activist-filings.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/activist-filings-state.json'));
});

test('ownership original viewer checks exact XML bytes and refuses altered or private references',async()=>{
 const M=require('../jh-activist-observations.js'),fixture=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/ownership-filings-synthetic.json'),'utf8')),ref=fixture.packet.acquisitions.feeds[4].original_ref,body=Buffer.from(fixture.sources[ref.key]),calls=[];
 const fetcher=async(url,options)=>{calls.push(url);assert.equal(options.credentials,'omit');assert.equal(options.redirect,'error');return new Response(body);};
 assert.equal((await M.loadSource(ref,{fetcher})).raw,body.toString());assert.deepEqual(calls,['/'+ref.key+'?exact=1&nogen=1']);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(body.subarray(0,4))}),/byte count/);
 await assert.rejects(()=>M.loadSource(ref,{fetcher:async()=>new Response(Buffer.alloc(body.length,97))}),/SHA-256/);
 await assert.rejects(()=>M.loadSource({...ref,key:'data/activist-filings-state.json'},{fetcher}),/source identity/);assert.equal(calls.length,1);
 await assert.rejects(()=>M.loadSource(ref,{timeout:5,fetcher:()=>new Promise(()=>{})}),/timed out/);
});

test('both ownership preview components abstain and clear old score content on failures',async()=>{
 const M=require('../jh-activist-observations.js'),p={all_filings:[{subject_ticker:'TEST',score:99}],summary:{top_25_overall:[{subject_ticker:'TEST',score:99}]}};
 for(const file of ['intel/index.html','web/intel/index.html']){
  const html=fs.readFileSync(path.join(root,file),'utf8'),block=html.split('// Activist source observations;')[1].split('// Theme Rotation')[0];assert.ok(!block.includes('top_25_overall'));assert.match(html,/href="\/activist-filings.html"/);
  const nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{textContent:'old score'});return nodes.get(id);};let fail=false;
  const context=vm.createContext({document:{getElementById:get},JHActivistObservations:M,fetchJson:async()=>fail?null:p});
  const actual='(async()=>{\n// Activist source observations;'+block+'\n})()';await vm.runInContext(actual,context);assert.match(get('activist').textContent,/2 legacy occurrences/);assert.equal(get('af-fresh').textContent,'RESEARCH');
  fail=true;await vm.runInContext(actual,context);assert.match(get('activist').textContent,/unavailable/);assert.equal(get('af-meta').textContent,'');assert.equal(get('af-fresh').textContent,'UNAVAILABLE');
 }
});
