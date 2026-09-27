const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const root=path.join(__dirname,'..'),api=require('../jh-table-values.js');
const flush=async()=>{for(let i=0;i<3;i++)await new Promise(r=>setImmediate(r));};
function page(file,loader){
 const nodes=new Map(),calls=[],intervals=new Set(),listeners={};
 let document;
 const get=id=>{
  if(!nodes.has(id)){
   const n={value:'',headers:[],querySelectorAll(){return this.headers;},addEventListener(k,fn){this['on'+k]=fn;},setAttribute(k,v){this[k]=v;}};
   let html='',text='';
   Object.defineProperty(n,'innerHTML',{get:()=>html,set:v=>{html=v;text='';n.headers=[...v.matchAll(/<th\b[^>]*data-k=(?:"([^"]+)"|([^ >]+))[^>]*>/g)].map(m=>({dataset:{k:m[1]||m[2]},ownerDocument:document,setAttribute(k,v){this[k]=v;},focus(){document.activeElement=this;}}));}});
   Object.defineProperty(n,'textContent',{get:()=>text,set:v=>{text=String(v);html='';n.headers=[];}});
   nodes.set(id,n);
  }
  return nodes.get(id);
 };
 document={getElementById:get,querySelectorAll:selector=>selector.startsWith('#')?get(selector.slice(1).split(' ')[0]).headers:[...nodes.values()].flatMap(n=>n.headers),activeElement:null};
 const ctx=vm.createContext({console,Date,Number,String,Array,Object,JSON,encodeURIComponent,document,JHTableValues:{...api,load:async p=>{calls.push(p);return loader(p);}},setInterval(fn){intervals.add(fn);return fn;},clearInterval(fn){intervals.delete(fn);},addEventListener(k,fn){listeners[k]=fn;}});ctx.window=ctx;
 const html=fs.readFileSync(path.join(root,file),'utf8');
 if(html.includes('/jh-eps-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-eps-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-hiring-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-hiring-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-scarcity-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-scarcity-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-estimate-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-estimate-observations.js'),'utf8'),ctx);
 if(html.includes('/jh-earnings-observations.js'))vm.runInContext(fs.readFileSync(path.join(root,'jh-earnings-observations.js'),'utf8'),ctx);
 for(const m of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))vm.runInContext(m[1],ctx);
 return{ctx,get,calls,document,intervals,listeners,html};
}
const raw=p=>({packet:p,raw:JSON.stringify(p)});
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
 const M=require('../jh-eps-observations.js');const p={measurement_contract:M.CONTRACT,request_records:[{ticker:'TEST',acquisitions:[{endpoint:'grades',status:'received',original_base64:'EXACT_BYTES',original_sha256:'identity'}],quote_records:[{price:0}],
  estimate_observations:[{source_index:0,raw:{symbol:'TEST'},target_period_end:'2026-01-31',target_status:'past_target',values:{epsAvg:0},reported_currency:'JPY',measurement_status:'reported_forecast_observation'}],
  same_target_comparisons:[{source_index:0,eps_change:null}],rating_observations:[{raw:{symbol:'TEST'},reported_date:'2026-09-26',previous_grade:'Sell',new_grade:'Sell',grading_company:'A > B',reported_action:'maintain',status:'reported_rating_record'}]}],all_qualifying:[{symbol:'NEVER_USE'}]};
 const s=page('eps-velocity.html',async()=>raw(p));await flush();let html=s.get('board').innerHTML;
 assert.equal(s.calls.length,1);assert.equal(s.calls[0],'/data/eps-revision-velocity.json');assert.match(html,/2026-01-31/);assert.match(html,/<td>0\.000<\/td>/);assert.match(html,/JPY/);assert.match(html,/past_target/);assert.ok(!html.includes('NEVER_USE'));assert.equal(s.get('original').textContent,JSON.stringify(p));
 s.get('mode').value='ratings';s.get('mode').onchange();html=s.get('board').innerHTML;assert.match(html,/maintain/);assert.match(html,/A &gt; B/);assert.match(s.get('rows').textContent,/1 received ratings/);assert.ok(!html.includes('earnings_revision_breadth'));
 const h=s.get('board').headers.find(x=>x.dataset.k==='ticker');h.focus();h.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'ticker');assert.equal(s.document.activeElement['aria-sort'],'ascending');
 s.get('mode').value='requests';s.get('mode').onchange();assert.match(s.get('board').innerHTML,/grades: received/);assert.ok(!s.get('board').innerHTML.includes('EXACT_BYTES'));assert.match(s.get('original').textContent,/EXACT_BYTES/);
 const all=M.model({...p,request_records:Array.from({length:501},()=>p.request_records[0])});assert.equal(all.targets.length,501);assert.equal(all.ratings.length,501);assert.equal(all.requests.length,501);
 assert.throws(()=>M.model({...p,request_records:null}),/Complete/);assert.throws(()=>M.model({...p,request_records:[{}]}),/Complete/);
});

test('EPS legacy complete company and summary occurrences stay unverified, paginated and escaped',async()=>{
 const p={all_qualifying:Array.from({length:194},(_,i)=>({symbol:'T'+i,score:99,rationale:'<img src=x>'})),summary:{top_25_overall:[{symbol:'T0'}],tier_a:['T0'],tier_b_symbols:['T1']}};
 const s=page('eps-velocity.html',async()=>raw(p));await flush();assert.match(s.get('rows').textContent,/197 received targets/);assert.match(s.get('status').textContent,/unverified/);assert.ok(!s.get('board').innerHTML.includes('<img'));assert.match(s.get('board').innerHTML,/&lt;img/);
 s.get('next').onclick();assert.match(s.get('rows').textContent,/Page 2 of 2/);assert.match(s.get('board').innerHTML,/T193/);
 s.get('q').value='T193';s.get('q').oninput();assert.match(s.get('rows').textContent,/1 matching \/ 197/);s.get('q').value='';s.get('q').oninput();assert.match(s.get('rows').textContent,/197 matching/);
 for(const loader of [async()=>{throw Error('HTTP 403');},async()=>raw({all_qualifying:{}})]){const failed=page('eps-velocity.html',loader);await flush();assert.match(failed.get('status').textContent,/unavailable/);assert.equal(failed.get('board').textContent,'No verified display population');}
 assert.ok(api.PUBLIC_PATHS.test('/data/eps-revision-velocity.json'));assert.ok(!api.PUBLIC_PATHS.test('/data/eps-revision-velocity/private.json'));
});
