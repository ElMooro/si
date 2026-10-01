const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),source=fs.readFileSync(path.join(R,'jh-chart-engine.js'),'utf8');
const bundled=process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'],parser={exports:{}};Function('exports','module',bundled)(parser.exports,parser);
function parse(raw){return parser.exports.parse(raw,{ecmaVersion:'latest'});}
function top(raw){return parse(raw).body.find(n=>n.type==='ExpressionStatement' && n.expression.type==='CallExpression').expression.callee.body.body;}
const named=Object.fromEntries(top(source).filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,source.slice(n.start,n.end)]));
const flush=async()=>{for(let i=0;i<30;i++)await Promise.resolve();};
function setup(handler){
 const requests=[],timers=new Map();let n=0;
 const box={innerHTML:'',className:'on',classList:{contains:()=>true,remove:()=>{}},dataset:{dest:'chart'}};
 const c={ssRows:[],ssFacets:[],ssProv:'',ssYq:'',ssRequest:0,ssRemoteState:null,PROXY:'https://invented.justhodl.test',Date,AbortController,
  window:{JHChartCatalog:{search:()=>[],indexStatus:()=>({sources:[]})}},document:{getElementById:()=>box},
  chartId:s=>s,classifySym:()=> 'stock',displayTicker:s=>s,aliasHits:()=>[],pinBest:()=>{},syncSsSelection:()=>{},bindSsRows:()=>{},bindFacets:()=>{},
  paintFacets:()=>'',paintSsList:()=>'',setTimeout:fn=>{timers.set(++n,fn);return n;},clearTimeout:id=>timers.delete(id),
  fetch:async(url,options)=>{requests.push({url,options});const value=await handler(new URL(url,'https://invented.justhodl.test'),options);
   return value && value.response ? value.response : {ok:true,status:200,json:async()=>structuredClone(value)};
  }};
 vm.createContext(c);vm.runInContext(['escHtml','paintSsStatus','searchJson','dirSearch','closeSymSearch'].map(name=>named[name]).join('\n'),c);
 return {c,requests,timers,box,tick:async()=>{for(const [id,fn]of [...timers]){timers.delete(id);fn();}await flush();}};
}
const empty=url=>url.pathname==='/symsearch'?{rows:[],facets:[]}:url.pathname==='/tv-search'?{symbols:[]}:{quotes:[]};
test('complete modules preserve every existing unrelated function and outer statement',()=>{
 const cases=[['jh-chart-catalog.js','pre-browser-cache.js.txt',['ensureIndex','loadJson'],['INDEX_STATE']],['jh-chart-engine.js','pre-search-status-engine.js.txt',['openSymSearch','closeSymSearch','renderSymSearch','dirSearch'],['ssRequest']]];
 for(const [file,prior,allowed,newVars] of cases){
  const raw=require('./helpers/chart-observation-preservation.cjs').normalize(fs.readFileSync(path.join(R,file),'utf8'),file),old=fs.readFileSync(path.join(__dirname,'fixtures/symbol-directory',prior),'utf8'),a=top(old),b=top(raw);
  const after=new Map(b.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,raw.slice(n.start,n.end)]));
  const beforeFunctions=a.filter(n=>n.type==='FunctionDeclaration');assert.ok(beforeFunctions.length>20);
  for(const fn of beforeFunctions)if(!allowed.includes(fn.id.name))assert.equal(after.get(fn.id.name),old.slice(fn.start,fn.end),file+':'+fn.id.name);
  const clean=n=>JSON.parse(JSON.stringify(n,(k,v)=>['start','end'].includes(k)?undefined:v));
  const statements=nodes=>nodes.filter(n=>n.type!=='FunctionDeclaration' && !(n.type==='VariableDeclaration' && n.declarations.some(d=>newVars.includes(d.id.name)))).map(clean);
  const actual=statements(b);
  if(file==='jh-chart-catalog.js'){
   const exportNode=actual.find(n=>n.type==='ExpressionStatement' && n.expression.type==='AssignmentExpression' && n.expression.left.property?.name==='JHChartCatalog');
   assert.equal(exportNode.expression.right.properties.filter(p=>p.key.name==='indexStatus').length,1);
   exportNode.expression.right.properties=exportNode.expression.right.properties.filter(p=>p.key.name!=='indexStatus');
  }
  assert.deepEqual(actual,statements(a));
 }
});
test('cached, absent, overdue and future generation checks cannot be shown as current',()=>{
 const {c}=setup(empty);const now=Date.now();
 for(const head of [null,{status:'unavailable'},{status:'checked_within_interval',checked_at:new Date(now-300001).toISOString(),serving_cached_generation:false},{status:'checked_within_interval',checked_at:new Date(now+60000).toISOString(),serving_cached_generation:false}]){
  c.ssRemoteState={checked:head};const text=c.paintSsStatus();assert.match(text,/unavailable|overdue/);assert.doesNotMatch(text,/generation checked \d/);assert.match(text,/do not verify observation freshness/);
 }
 c.ssRemoteState={checked:{status:'checked_within_interval',checked_at:new Date(now).toISOString(),serving_cached_generation:false}};assert.match(c.paintSsStatus(),/generation checked \d/);
});
test('partial catalog labels remain text, with explicit retry and incomplete coverage',()=>{
 const {c}=setup(empty);c.window.JHChartCatalog.indexStatus=()=>({sources:[{label:'<img src=x>',status:'unavailable',retry_after_s:30},{label:'Old instrument list',status:'cached'}]});
 const html=c.paintSsStatus();assert.doesNotMatch(html,/<img/);assert.match(html,/&lt;img/);assert.match(html,/coverage is incomplete/);assert.match(html,/30 seconds/);assert.match(html,/Using cached catalogs/);
});
test('native cache and provider-warehouse failures reach the visible search status',async()=>{
 const {c,box}=setup(url=>url.pathname==='/symsearch'?{rows:[{symbol:'ZZCACHED'}],facets:[],index_integrity:{head_check:{status:'unavailable',serving_cached_generation:true}},warehouse_integrity:{status:'unavailable'}}:empty(url));
 await c.dirSearch('ZZ');assert.equal(c.ssRows[0].s,'ZZCACHED');assert.match(box.innerHTML,/cached generation/);assert.match(box.innerHTML,/warehouse search is unavailable/);
});
test('HTTP failure is not treated as valid empty results and other sources remain usable',async()=>{
 const {c,box}=setup(url=>url.pathname==='/symsearch'?{response:{ok:false,status:503,json:()=>{throw Error('failure body must not parse');}}}:url.pathname==='/tv-search'?{symbols:[{symbol:'ZZTV',name:'Invented TV fallback'}]}:empty(url));
 await c.dirSearch('ZZ');assert.equal(c.ssRows[0].s,'ZZTV');assert.match(box.innerHTML,/Directory search unavailable/);
});
test('malformed search rows fail explicitly instead of escaping as an unhandled rejection',async()=>{
 for(const value of [null,[],{rows:true},{rows:{}}]){const h=setup(url=>url.pathname==='/symsearch'?value:empty(url));await h.c.dirSearch('ZZ');assert.equal(h.c.ssRemoteState.unavailable,true);}
});
test('A to B to A cannot let the first A response replace the newest A search',async()=>{
 let release,count=0;const h=setup(url=>{
  if(url.pathname==='/symsearch' && url.searchParams.get('q')==='AA'){
   if(++count===1)return new Promise(r=>{release=()=>r({rows:[{symbol:'ZZOLD'}],facets:[{provider:'OLD'}]});});
   return {rows:[{symbol:'ZZNEW'}],facets:[{provider:'NEW'}]};
  }return empty(url);
 });
 const first=h.c.dirSearch('AA');await flush();h.c.ssYq='';await h.c.dirSearch('BB');h.c.ssYq='';h.c.ssRows=[];await h.c.dirSearch('AA');release();await first;
 assert.deepEqual(Array.from(h.c.ssRows,r=>r.s),['ZZNEW']);assert.equal(h.c.ssFacets[0].provider,'NEW');
});
test('delayed FRED fallback from an old query cannot insert rows into the new query',async()=>{
 let release;const h=setup(url=>url.pathname==='/fred-search'?new Promise(r=>{release=()=>r({series:[{id:'ZZLATE'}]});}):url.pathname==='/symsearch' && url.searchParams.get('q')==='OLD'?{rows:[],failed:true}:empty(url));
 const old=h.c.dirSearch('OLD');await flush();h.c.ssYq='';h.c.ssRows=[];await h.c.dirSearch('NEW');release();await old;assert.equal(h.c.ssRows.length,0);
});
test('closing search invalidates every pending response and cancels the pending debounce',async()=>{
 let release;const h=setup(url=>url.pathname==='/symsearch'?new Promise(r=>{release=()=>r({rows:[{symbol:'ZZLATE'}],facets:[]});}):empty(url));
 const pending=h.c.dirSearch('ZZ');await flush();h.c.closeSymSearch();release();await pending;assert.equal(h.c.ssRows.length,0);assert.equal(h.c.ssRemoteState,null);
});
test('hung directory request times out, aborts, and still allows additional-source results',async()=>{
 const h=setup(url=>url.pathname==='/symsearch'?new Promise(()=>{}):url.pathname==='/tv-search'?{symbols:[{symbol:'ZZAVAILABLE'}]}:empty(url));
 const pending=h.c.dirSearch('ZZ');await flush();await h.tick();await pending;
 assert.equal(h.requests[0].options.signal.aborted,true);assert.equal(h.c.ssRows[0].s,'ZZAVAILABLE');assert.equal(h.c.ssRemoteState.unavailable,true);assert.equal(h.timers.size,0);
});
test('received remote rows are retained separately for catalog-triggered repaint',async()=>{
 const h=setup(url=>url.pathname==='/symsearch'?{rows:[{symbol:'ZZREMOTE',name:'Invented directory entry'}],facets:[]}:empty(url));
 await h.c.dirSearch('ZZ');assert.equal(h.c.ssRemoteState.rows[0].s,'ZZREMOTE');
});
test('legacy explicit failure without rows preserves the existing FRED fallback',async()=>{
 const h=setup(url=>url.pathname==='/symsearch'?{failed:true}:url.pathname==='/fred-search'?{series:[{id:'ZZFRED',title:'Invented FRED fallback'}]}:empty(url));
 await h.c.dirSearch('ZZ');assert.equal(h.c.ssRemoteState.unavailable,true);assert.equal(h.c.ssRows[0].s,'fred:ZZFRED');
});
