const test=require('node:test'),assert=require('node:assert/strict'),fs=require('fs'),path=require('path'),vm=require('vm');
const {execFileSync}=require('child_process');const root=path.join(__dirname,'..');
const html=fs.readFileSync(path.join(root,'tape-reader.html'),'utf8');
const source=html.match(/<script>([\s\S]*?)<\/script>/)[1];
const old=fs.readFileSync(path.join(__dirname,'fixtures/tape-reader-notice/jh-enhance-before.js.txt'),'utf8');
const current=fs.readFileSync(path.join(root,'jh-enhance.js'),'utf8');
const fixture=JSON.parse(execFileSync('python3',['-B','aws/lambdas/justhodl-tape-reader/tests/run_tests.py','--fixture'],{cwd:root,encoding:'utf8'}));
const NOW=Date.parse('2026-10-01T23:00:00Z');class Clock extends Date{static now(){return NOW;}}
const flush=()=>new Promise(r=>setImmediate(r));
const attrs={'data-feed':'data/tape-reader.json','data-bars':'top_loud_tape:ticker:score','data-contract':'tape-reader-activity.v2','data-publication-notice':'tape-reader','data-strict-numbers':'1','data-footnote':'Published daily aggregate activity; no participant or trade-direction inference'};
function env(fetch){const el={},listeners={};const get=id=>el[id]||={innerHTML:'',textContent:'',style:{},addEventListener(){},setAttribute(){},getBoundingClientRect:()=>({left:0,width:100})};
 const ctx=vm.createContext({Date:Clock,console:{error(){}},setInterval(){},fetch,CustomEvent:class {constructor(type){this.type=type;}},addEventListener:(k,f)=>(listeners[k]||=[]).push(f),dispatchEvent:e=>{for(const f of listeners[e.type]||[])f(e);},document:{getElementById:get,querySelectorAll:()=>[],currentScript:{getAttribute:k=>attrs[k]||null},createElement:()=>({style:{},innerHTML:''}),querySelector:()=>null,body:{insertBefore(){}}}});return {ctx,el};}
async function main(fetch){const e=env(fetch);vm.runInContext(source,e.ctx);await flush();return e;}
async function enhance(code,data,a=attrs,fail=null){let count=0;const requests=[];const e=env(async u=>{count++;requests.push(u);if(fail==='fetch')throw Error('network');if(fail==='http')return {ok:false,status:503};return {ok:true,json:async()=>{if(fail==='parse')throw Error('bad JSON');return data;}};});
 e.ctx.document.currentScript.getAttribute=k=>a[k]||null;
 if(a['data-publication-notice'])e.ctx.__JH_TAPE_READER_STATE={packet:fail?null:data,error:fail==='parse'?'Data response could not be parsed as JSON.':fail==='http'?'Data request failed: HTTP 503':fail==='fetch'?'Data request failed: network':null};
 vm.runInContext(code.replace(/\}\)\(\);\s*$/, 'globalThis.__noticeTest={getJSON,run,tapePublication:typeof tapePublication==="function"?tapePublication:null};})();'),e.ctx);
 await flush();return {...e,count:()=>count,requests,body:()=>e.el['jhviz-body']?.innerHTML||''};}
const response=d=>async()=>({ok:true,json:async()=>d});
function hidden(e){assert.equal(vm.runInContext('DATA',e.ctx),null);for(const k of ['nUniverse','nLoud','nSize','adRatio','topScore'])assert.equal(e.el[k].textContent,'—');assert.doesNotMatch(e.el.tableHost.innerHTML,/<table|BLOCK_PRINTS/);}

test('old, absent and unknown contracts withhold both surfaces without mislabelling a fetch failure',async()=>{
 for(const contract of [undefined,'tape-reader-activity.v1','unknown']){
  const d={...fixture,measurement_contract:contract,as_of:'2026-09-30T22:00:33+00:00'};
  const m=await main(response(d)),e=await enhance(current,d);hidden(m);
  for(const text of [m.el.tableHost.innerHTML,e.body()]){assert.match(text,/Qualification withheld — awaiting a compatible publication/);assert.match(text,/Packet publication time: 2026-09-30T22:00:33\+00:00/);assert.match(text,/not observation freshness/);assert.doesNotMatch(text,/Failed|failed|BLOCK_PRINTS|width:.*%/);}
  assert.equal(e.count(),0);
 }
});
test('current contract keeps activity and never formats missing, invalid or future timestamps as freshness',async()=>{
 for(const stamp of [undefined,null,true,'','2026-02-30T22:00:00Z','2026-10-01T22:00:00','2026-10-01T24:00:00Z','2026-10-01T22:00:00+00:60','2099-01-01T00:00:00Z','<img src=x onerror=alert(1)>','2026-09-30T22:00:33+00:00']){
  const d={...fixture,as_of:stamp};const m=await main(response(d)),e=await enhance(current,d);
  assert.match(m.el.tableHost.innerHTML,/<table/);assert.equal(m.el.nSize.textContent,'1 / 2');assert.match(e.body(),/Published daily aggregate activity/);
  const valid=stamp==='2026-09-30T22:00:33+00:00';
  for(const text of [m.el.metaLine.innerHTML,e.body()]){assert.match(text,valid?/Packet publication time:/:/Publication time unavailable/);assert.doesNotMatch(text,/<img|Invalid Date|2099-01/);}
 }
});
test('table and enhancement timestamp validators remain identical without introducing an extra asset request',async()=>{
 const helper=code=>code.slice(code.indexOf('function tapePublication('),code.indexOf('\n}',code.indexOf('function tapePublication('))+2);
 assert.equal(helper(source),helper(current));assert.doesNotMatch(html,/src=.*tape.*notice.*\.js/);
 const e=await main(response(fixture));for(const x of ['2024-02-29T12:00:00Z','0001-01-01T00:00:00Z','2026-09-30T22:00:33.123456-04:00'])assert.match(vm.runInContext(`tapePublication({as_of:${JSON.stringify(x)}})`,e.ctx),/Packet publication time:/);
});
test('fetch, HTTP, parse and malformed current-row failures are distinct and remain withheld',async()=>{
 const scenarios=[['fetch',async()=>{throw Error('<img src=x>');},/Data request failed/],['http',async()=>({ok:false,status:503}),/Data request failed.*HTTP 503/],['parse',async()=>({ok:true,json:async()=>{throw Error('bad JSON <img>');}}),/could not be parsed/]];
 for(const [kind,fetch,match] of scenarios){const m=await main(fetch),e=await enhance(current,null,attrs,kind);hidden(m);assert.match(m.el.tableHost.innerHTML,match);assert.match(e.body(),kind==='parse'?/could not be parsed/:/Data request failed/);assert.doesNotMatch(m.el.tableHost.innerHTML,/<img|awaiting a compatible/);assert.doesNotMatch(e.body(),/awaiting a compatible/);assert.equal(e.count(),0);}
 const d={...fixture,top_loud_tape:null};const m=await main(response(d)),e=await enhance(current,d);hidden(m);assert.match(m.el.tableHost.innerHTML,/compatible activity rows unavailable/);assert.match(e.body(),/compatible activity rows unavailable/);
 for(const packet of [null,false,3,{}]){const e=await enhance(current,packet);assert.match(e.body(),/Qualification withheld/);assert.doesNotMatch(e.body(),/Data request failed/);}
});
test('repeated current/legacy/error/current updates clear old rows, metrics and prior timestamps',async()=>{
 const e=await main(response(fixture));
 for(const d of [{...fixture,measurement_contract:'old',as_of:'2026-09-29T00:00:00Z'},{...fixture,measurement_contract:'unknown',as_of:'<script>'},fixture]){
  e.ctx.fetch=response(d);await vm.runInContext('load()',e.ctx);
  if(d===fixture){assert.equal(e.el.nSize.textContent,'1 / 2');assert.match(e.el.tableHost.innerHTML,/<table/);}else hidden(e);
  if(d.as_of==='<script>')assert.doesNotMatch(e.el.tableHost.innerHTML,/2026-09-29|<script>/);
 }
 e.ctx.fetch=async()=>{throw Error('offline');};await vm.runInContext('load()',e.ctx);hidden(e);assert.equal(e.el.metaLine.textContent,'Publication time unavailable.');
 e.ctx.fetch=response(fixture);await vm.runInContext('load()',e.ctx);assert.equal(e.el.nSize.textContent,'1 / 2');
 const chart=await enhance(current,fixture);
 for(const packet of [{...fixture,measurement_contract:'old',as_of:'2026-09-29T00:00:00Z'},fixture]){
  chart.ctx.__JH_TAPE_READER_STATE={packet,error:null};vm.runInContext("dispatchEvent(new CustomEvent('jh:tape-reader-state'))",chart.ctx);
  if(packet===fixture){assert.match(chart.body(),/linear-gradient/);assert.doesNotMatch(chart.body(),/awaiting a compatible/);}
  else {assert.match(chart.body(),/awaiting a compatible/);assert.doesNotMatch(chart.body(),/linear-gradient/);}
 }
 chart.ctx.__JH_TAPE_READER_STATE={packet:null,error:'Data request failed: <img src=x>'};vm.runInContext("dispatchEvent(new CustomEvent('jh:tape-reader-state'))",chart.ctx);assert.match(chart.body(),/Data request failed: &lt;img/);assert.doesNotMatch(chart.body(),/linear-gradient|<img/);assert.equal(chart.count(),0);
});
test('late success or failure cannot overwrite a newer update',async()=>{
 const e=await main(response(fixture));let release;
 e.ctx.fetch=()=>new Promise(r=>{release=r;});const oldLoad=vm.runInContext('load()',e.ctx);
 e.ctx.fetch=response({...fixture,measurement_contract:'legacy'});await vm.runInContext('load()',e.ctx);release({ok:true,json:async()=>fixture});await oldLoad;hidden(e);
 e.ctx.fetch=()=>new Promise((r,j)=>{release=j;});const failing=vm.runInContext('load()',e.ctx);e.ctx.fetch=response(fixture);await vm.runInContext('load()',e.ctx);release(Error('late error'));await failing;assert.equal(e.el.nSize.textContent,'1 / 2');
});
test('all 59 non-opted importer pages preserve predecessor output and request counts across 413 comparisons',async()=>{
 const files=execFileSync('git',['ls-files','*.html'],{cwd:root,encoding:'utf8'}).trim().split('\n');let pages=0,comparisons=0;
 const set=(obj,key,val)=>{const p=key.split('.');let x=obj;for(const k of p.slice(0,-1))x=x[k]||=( {} );x[p.at(-1)]=val;};
 for(const file of files){if(file==='tape-reader.html')continue;const s=fs.readFileSync(path.join(root,file),'utf8');const tag=s.match(/<script\b[^>]*src=["'][^"']*jh-enhance\.js[^"']*["'][^>]*>/i);if(!tag)continue;
  const a=Object.fromEntries([...tag[0].matchAll(/(data-[\w-]+)=["']([^"']*)["']/g)].map(m=>[m[1],m[2]]));assert.equal(a['data-publication-notice'],undefined);pages++;
  for(let i=0;i<7;i++){const d={};if(a['data-bars']){const [p,k,v]=a['data-bars'].split(':');set(d,p,[{[k]:'A<&',[v]:i===1?'1,234':i===2?null:0},{[k]:'B',[v]:-3},{[k]:'C',[v]:4}]);}
   if(a['data-line'])set(d,a['data-line'],i===3?[{date:'2020-01-01',value:2},{date:'2021-01-01',value:3}]:[['2020-01-01',1],['2021-01-01',2]]);
   if(a['data-metrics'])for(const part of a['data-metrics'].split('|'))set(d,part.split(':')[0],i===2?null:4);
   const payload=i===4?{}:i===5?null:d,fail=i===6?'http':null;
   const before=await enhance(old,payload,a,fail),after=await enhance(current,payload,a,fail);assert.equal(after.body(),before.body(),file+' scenario '+i);assert.deepEqual(after.requests,before.requests,file);comparisons++;
  }
 }
 assert.equal(pages,59);assert.equal(comparisons,413);
});
