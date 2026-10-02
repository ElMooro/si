const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url');
const root=path.join(__dirname,'..'),sourceDir=path.join(root,'cloudflare/workers/justhodl-data-proxy/src');
const body=JSON.stringify({q:'INVENTED',rows:[{id:'INVENTED',name:'Complete invented search row',provider:'instrument'}],facets:[],total:1,index_integrity:{status:'hash_bound_generation',generation:'invented-generation',investment_authority:false,head_check:{status:'unavailable',http_max_age_s:0}},warehouse_integrity:{status:'unavailable'}});
async function setup({previous=false,header='public, max-age=120',status=200,extra={},delay=0,throwFetch=false}={}) {
 const realFetch=global.fetch,realCaches=global.caches,realNow=Date.now;
 let now=946857600000;
 const saved=new Map(),calls=[],puts=[],waits=[];
 global.caches={default:{match:async request=>saved.get(request.url)?.clone(),put:async(request,response)=>{puts.push(request.url);saved.set(request.url,response.clone());}}};
 global.fetch=async(input)=>{
  const url=new URL(typeof input==='string'?input:input.url);calls.push(url.href);
  assert.equal(url.origin,'https://invented-origin.test');assert.ok(['/search','/browse','/series','/quote'].includes(url.pathname));
  if(throwFetch)throw new Error('Invented origin unavailable');
  now+=delay;
  return new Response(body,{status,headers:{'Content-Type':'application/json',...(header===null?{}:{'Cache-Control':header}),...extra}});
 };
 Date.now=()=>now;
 let worker;
 try {
  if(previous){
   let code=fs.readFileSync(path.join(root,'tests/fixtures/symbol-directory/pre-resident-proxy.js.txt'),'utf8');
   code=code.replace(/from (['"])(\.\/[^'"]+)\1/g,(_,q,relative)=>'from '+JSON.stringify(pathToFileURL(path.join(sourceDir,relative)).href));
   worker=(await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'))).default;
  }else worker=(await import(pathToFileURL(path.join(sourceDir,'index.js')))).default;
 }catch(error){global.fetch=realFetch;global.caches=realCaches;Date.now=realNow;throw error;}
 return {calls,puts,saved,advance:ms=>{now+=ms;},
  request:async(route='/symsearch?q=INVENTED')=>{
   const response=await worker.fetch(new Request('https://invented-worker.test'+route),{SYMDIR_URL:'https://invented-origin.test'},{waitUntil:promise=>waits.push(promise)});
   await Promise.all(waits);if(!throwFetch)assert.equal(await response.clone().text(),body);return response;
  },
  close:()=>{global.fetch=realFetch;global.caches=realCaches;Date.now=realNow;}
 };
}
async function use(options,fn){const env=await setup(options);try{await fn(env);}finally{env.close();}}

test('whole predecessor wrongly caches a native no-store result for two minutes',async()=>{
 await use({previous:true,header:'no-store'},async env=>{
  const response=await env.request();assert.equal(response.headers.get('Cache-Control'),'public, max-age=60, s-maxage=120');
  await env.request();assert.equal(env.puts.length,1);assert.equal(env.calls.length,1);
 });
});
test('native zero and no-store responses keep their complete body and never enter edge cache',async()=>{
 for(const header of ['no-store','public, max-age=0','max-age=120, s-maxage=0'])await use({header},async env=>{
  const first=await env.request(),second=await env.request();assert.equal(first.headers.get('Cache-Control'),'no-store');
  assert.equal(second.headers.get('Cache-Control'),'no-store');assert.equal(env.puts.length,0);assert.equal(env.calls.length,2);
 });
});
test('a cache hit cannot restart native lifetime and an expired entry is fetched again',async()=>{
 await use({},async env=>{
  const first=await env.request();assert.equal(first.headers.get('Cache-Control'),'public, max-age=60, s-maxage=120');
  env.advance(110000);const hit=await env.request();assert.equal(hit.headers.get('X-Edge-Cache'),'HIT');
  assert.equal(hit.headers.get('Cache-Control'),'public, max-age=10, s-maxage=10');assert.equal(env.calls.length,1);
  env.advance(10000);const next=await env.request();assert.equal(next.headers.get('X-Edge-Cache'),'MISS');assert.equal(env.calls.length,2);
 });
});
test('missing malformed ambiguous or private native cache policy fails closed',async()=>{
 for(const header of [null,'','max-age=-1','max-age=NaN','max-age=1.5','max-age=10, max-age=10','max-age=120, s-maxage=bad','private, max-age=120','max-age=120, no-cache','max-age=99999999999999999999999'])await use({header},async env=>{
  const response=await env.request();assert.equal(response.headers.get('Cache-Control'),'no-store',header);assert.equal(env.puts.length,0,header);
 });
});
test('quoted directives, upstream Age and transport delay only shorten lifetime',async()=>{
 await use({header:'public, max-age="120"',extra:{Age:'100'},delay:5000},async env=>{
  const response=await env.request();assert.equal(response.headers.get('Cache-Control'),'public, max-age=15, s-maxage=15');
 });
 await use({header:'max-age=1',delay:1000},async env=>{assert.equal((await env.request()).headers.get('Cache-Control'),'no-store');assert.equal(env.puts.length,0);});
});
test('invalid Age and uncacheable upstream headers cannot be promoted to shared storage',async()=>{
 for(const extra of [{Age:'-1'},{Age:'bad'},{'Set-Cookie':'invented=1'},{Vary:'*'}])await use({extra},async env=>{
  assert.equal((await env.request()).headers.get('Cache-Control'),'no-store');assert.equal(env.puts.length,0);
 });
});
test('origin errors remain uncacheable even if origin supplies a positive lifetime',async()=>{
 for(const status of [400,403,500,503])await use({status},async env=>{
  const response=await env.request();assert.equal(response.status,status);assert.equal(response.headers.get('Cache-Control'),'no-store');assert.equal(env.puts.length,0);
 });
});
test('transport failure returns an uncached error and does not manufacture empty success',async()=>{
 await use({throwFetch:true},async env=>{
  const response=await env.request();assert.equal(response.status,502);assert.equal(response.headers.get('Cache-Control'),'no-store');
  assert.equal((await response.json()).error,'symdir fetch failed');assert.equal(env.puts.length,0);
 });
});
test('explicit bypass cannot be cached by the edge or downstream browser',async()=>{
 await use({},async env=>{
  await env.request('/symsearch?q=INVENTED&nocache=1');const response=await env.request('/symsearch?q=INVENTED&nocache=1');
  assert.equal(response.headers.get('Cache-Control'),'no-store');assert.equal(env.calls.length,2);assert.equal(env.puts.length,0);
 });
});
test('cache metadata is bounded, rejects clock regression and is not returned to clients',async()=>{
 await use({},async env=>{
  let response=await env.request();assert.equal(response.headers.get('X-Symdir-Cache-Until'),null);assert.equal(response.headers.get('X-Symdir-Cached-At'),null);
  env.advance(-1000);response=await env.request();assert.equal(response.headers.get('X-Edge-Cache'),'MISS');assert.equal(env.calls.length,2);
  const key=[...env.saved.keys()][0],stored=env.saved.get(key);stored.headers.set('X-Symdir-Cache-Until','9999999999999999');
  response=await env.request();assert.equal(response.headers.get('X-Edge-Cache'),'MISS');assert.equal(env.calls.length,3);
 });
});
test('native cache policy cannot increase the existing route ceiling',async()=>{
 await use({header:'max-age=900'},async env=>assert.equal((await env.request()).headers.get('Cache-Control'),'public, max-age=60, s-maxage=120'));
});
test('other directory routes retain their previous cache policy and keys',async()=>{
 for(const [route,ttl] of [['/browse?ds=INVENTED',300],['/series?id=INVENTED',900],['/quote?ids=INVENTED',600]])await use({header:'no-store'},async env=>{
  assert.equal((await env.request(route)).headers.get('Cache-Control'),`public, max-age=60, s-maxage=${ttl}`);
  assert.match(env.puts[0],/__symdir_v5116a__/);
 });
});
test('whole worker outside the directory route and helper import remains unchanged',()=>{
 const parserModule={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parserModule.exports,parserModule);
 const normalize=file=>{
  const tree=parserModule.exports.parse(fs.readFileSync(file,'utf8'),{ecmaVersion:'latest',sourceType:'module'});let replaced=0;
  tree.body=tree.body.filter(n=>!(n.type==='ImportDeclaration'&&n.source.value==='./symsearch-cache.js'));
  const clean=value=>{
   if(Array.isArray(value))return value.map(clean);
   if(!value||typeof value!=='object')return value;
   if(value.type==='IfStatement'&&JSON.stringify(value.test).includes('"value":"/symsearch"')){replaced++;return {type:'ReviewedDirectoryRoute'};}
   return Object.fromEntries(Object.entries(value).filter(([key])=>!['start','end','loc','raw'].includes(key)).map(([key,child])=>[key,clean(child)]));
  };
  const result=clean(tree);assert.equal(replaced,1);return result;
 };
 assert.deepEqual(normalize(path.join(sourceDir,'index.js')),normalize(path.join(root,'tests/fixtures/symbol-directory/pre-resident-proxy.js.txt')));
});
