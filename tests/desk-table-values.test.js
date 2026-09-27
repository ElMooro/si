const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const root=path.join(__dirname,'..'),api=require('../jh-table-values.js');
const flush=async()=>{for(let i=0;i<3;i++)await new Promise(r=>setImmediate(r));};
function page(file,loader){
 const nodes=new Map(),calls=[],intervals=new Set(),listeners={};
 let document;
 const get=id=>{
  if(!nodes.has(id)){
   const n={value:'',headers:[],addEventListener(k,fn){this['on'+k]=fn;},setAttribute(k,v){this[k]=v;}};
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
 for(const m of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))vm.runInContext(m[1],ctx);
 return{ctx,get,calls,document,intervals,listeners,html};
}
const raw=p=>({packet:p,raw:JSON.stringify(p)});
const quality=()=>({as_of:'2026-09-27T12:00:00Z',all_ranked:[{ticker:'TEN',name:'A > B',sloan_accruals_pct_assets:'10',cash_conversion_ratio:0,quality_score:'10'},{ticker:'TWO',sloan_accruals_pct_assets:'2',cash_conversion_ratio:false,quality_score:'2'},{ticker:'MISSING',sloan_accruals_pct_assets:' ',cash_conversion_ratio:'',quality_score:null},{ticker:'ZERO',sloan_accruals_pct_assets:0,quality_score:0},null],unknown:{retain:true}});

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
 assert.match(h,/A &gt; B/);assert.match(h,/top_50 subset/);assert.match(h,/1 malformed/);assert.ok(!h.includes('<img'));assert.ok(!h.includes('$0'));assert.ok(!h.includes(' · inflection'));assert.match(h,/0\.0%/);assert.deepEqual(JSON.parse(s.get('original').textContent),p);
});

test('failed and malformed publications are visibly distinct from an empty received population',async()=>{
 for(const file of ['earnings-quality.html','cash-profitability.html','hiring-velocity.html']){
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
