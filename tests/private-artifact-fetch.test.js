const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../private-artifacts.js'), 'utf8');
function setup({uid='owner-a', fetcher, dynamic=false}={}) {
  const state={uid,calls:[],loads:[],reloads:0,inits:0,handlers:{},panels:[]};
  const auth={async init(){state.inits++;},getUser(){return state.uid?{id:state.uid}:null;},async getAccessToken(){return 'fixture-'+state.uid;},onChange(fn){state.change=fn;},openSignIn(){state.signIn=true;}};
  const element=()=>({style:{},children:[],setAttribute(){},appendChild(child){this.children.push(child);}});
  const document={baseURI:'https://justhodl.ai/portfolio/index.html',documentElement:element(),getElementById(id){return state.panels.find(p=>p.id===id);},createElement:element,
    body:{prepend(node){state.panels.push(node);}},addEventListener(name,fn){state.handlers[name]=fn;},head:{appendChild(script){state.loads.push(script.src);if(script.src.startsWith('/auth-config'))window.JUSTHODL_AUTH_CONFIG={};else if(script.src.includes('supabase'))window.supabase={};else if(script.src.startsWith('/auth.js'))window.JustHodlAuth=auth;script.onload();}}};
  const location={hostname:'justhodl.ai',href:document.baseURI,reload(){state.reloads++;}};
  const window={fetch:async(input,init)=>{state.calls.push({input,init});return fetcher?fetcher(input,init):Response.json({positions:[{ticker:'FIXTURE',quantity:7}]});},addEventListener(name,fn){state.handlers[name]=fn;}};
  if(!dynamic)window.JustHodlAuth=auth;
  const context=vm.createContext({window,document,location,URL,Request,Response,console});vm.runInContext(source,context);
  return {state,window,document,auth};
}
test('public model outputs use original request with no authentication work',async()=>{
  const {state,window}=setup();const init={cache:'default',headers:{'X-Public':'unchanged'}};
  const response=await window.fetch('/data/portfolio-analytics.json',init);
  assert.equal(response.status,200);assert.equal(state.inits,0);assert.equal(state.calls[0].input,'/data/portfolio-analytics.json');assert.equal(state.calls[0].init,init);
  await window.fetch('https://unrelated.invalid/portfolio/snapshot.json');assert.equal(state.inits,0);
});
test('signed-out private requests never reach network and show sign-in action',async()=>{
  const {state,window}=setup({uid:null});const response=await window.fetch('/portfolio/snapshot.json');
  assert.equal(response.status,401);assert.equal(state.calls.length,0);assert.equal(state.panels.length,1);
  state.panels[0].children[1].onclick();assert.equal(state.signIn,true);
});
test('every owner feed uses its dedicated private kind with bearer and no-store',async()=>{
  const {state,window}=setup();const paths={'/data/portfolio-manager-brief.json':'portfolio-manager-brief',
    '/portfolio/snapshot.json':'portfolio-snapshot','/portfolio/risk.json':'portfolio-risk','/portfolio/sizing.json':'portfolio-sizing','/portfolio/catalysts.json':'portfolio-catalysts',
    '/data/risk-sizer.json':'risk-sizer','/risk/recommendations.json':'risk-sizer','/data/pm-decision.json':'pm-decision','/data/pm-decision-history.json':'pm-decision-history',
    '/data/behavior-mirror.json':'behavior-mirror','/data/ai-brief.json':'ai-brief',
    '/data/user-watchlist.json':'user-watchlist','/data/vol-regime-private.json':'vol-regime-private','/data/user-trades.json':'personal-trades','/data/user-trades-stats.json':'personal-trades-stats',
    '/portfolio/catalyst-alert-history.json':'portfolio-catalyst-history','/portfolio/risk-alert-history.json':'portfolio-risk-history',
    '/portfolio/sizing-alert-history.json':'portfolio-sizing-history','/data/history/behavior-mirror-history.json':'behavior-mirror-history'};
  for(const [key,kind] of Object.entries(paths)){
    const result=await window.fetch('https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com'+key+'?t=fixture',{headers:{Authorization:'do-not-forward'}});
    assert.equal((await result.json()).positions[0].quantity,7);const call=state.calls.at(-1);
    assert.equal(call.input,'https://api.justhodl.ai/private-artifact?kind='+kind);
    assert.equal(call.init.headers.Authorization,'Bearer fixture-owner-a');assert.equal(call.init.cache,'no-store');
  }
  assert.equal(state.inits,1);
});
test('private HEAD and Request objects retain their method; writes are refused locally',async()=>{
  const {state,window}=setup();assert.equal((await window.fetch(new Request('https://justhodl.ai/data/ai-brief.json',{method:'HEAD'}))).status,200);
  assert.equal(state.calls[0].init.method,'HEAD');assert.equal((await window.fetch('/data/ai-brief.json',{method:'PUT',body:'{}'})).status,405);assert.equal(state.calls.length,1);
});
test('unrelated authenticated identity receives upstream denial without fallback',async()=>{
  const {state,window}=setup({uid:'other',fetcher:()=>Response.json({error:'owner artifact'},{status:403})});
  assert.equal((await window.fetch('/data/ai-brief.json')).status,403);assert.equal(state.calls.length,1);assert.match(state.panels[0].children[0].textContent,/only to its owner/);
});
test('identity switch discards both delayed private body and previously rendered DOM',async()=>{
  let release;const {state,window,document}=setup({fetcher:()=>new Promise(resolve=>release=resolve)});
  const pending=window.fetch('/portfolio/snapshot.json');await new Promise(setImmediate);
  state.uid='owner-b';state.change({id:state.uid});assert.equal(document.documentElement.style.visibility,'hidden');assert.equal(state.reloads,1);
  release(Response.json({private:'prior-account fixture'}));const result=await pending;
  assert.equal(result.status,401);assert.ok(!(await result.text()).includes('prior-account'));
});
test('persisted-page restoration clears old personal DOM; tier-only changes do not reload',async()=>{
  const {state,window,document}=setup();await window.fetch('/portfolio/risk.json');state.change({id:'owner-a'});assert.equal(state.reloads,0);
  state.handlers.pageshow({persisted:true});assert.equal(state.reloads,1);assert.equal(document.documentElement.style.visibility,'hidden');
});
test('pages without auth assets load them once before concurrent private reads',async()=>{
  const {state,window}=setup({dynamic:true});await Promise.all([window.fetch('/portfolio/risk.json'),window.fetch('/portfolio/snapshot.json')]);
  assert.equal(state.loads.length,3);assert.ok(state.loads[0].startsWith('/auth-config.js'));assert.ok(state.loads[1].includes('supabase'));assert.ok(state.loads[2].startsWith('/auth.js'));assert.equal(state.inits,1);
});
test('private request failures produce fixed unavailable response and no public retries',async()=>{
  const {state,window}=setup({fetcher:()=>{throw new Error('fixture internal detail');}});const response=await window.fetch('/data/ai-brief.json');
  assert.equal(response.status,503);assert.ok(!(await response.text()).includes('internal detail'));assert.equal(state.calls.length,1);
});
test('every known account-consuming page installs the helper before inline fetches',()=>{
  for(const name of ['classic-dashboard.html','desk.html','sizing/index.html','why.html','portfolio/index.html','ticker.html','risk.html','pm-decision.html','catalyst/index.html','desk-v2.html','brief.html','engine.html','engines.html','index.html','watchlist.html','trade-journal.html','master-rank.html','vol-regime.html']){
    const html=fs.readFileSync(path.join(__dirname,'..',name),'utf8');const marker=html.indexOf('src="/private-artifacts.js?v=20260909"');
    assert.ok(marker>=0,name);const fetch=html.indexOf('fetch(');if(fetch>=0)assert.ok(marker<fetch,name+' installs late');
  }
});
test('owner CRUD uses session bearer, exact account path, preserved JSON, and no browser service secret',async()=>{
  const {state,window}=setup();const body=JSON.stringify({ticker:'FIXTURE',thesis:'synthetic'});
  const response=await window.fetch('https://api.justhodl.ai/owner-api/trades/add',{method:'POST',body,headers:{'x-justhodl-token':'never-forward'}});
  assert.equal(response.status,200);assert.equal(state.calls[0].input,'https://api.justhodl.ai/owner-api/trades/add');
  assert.equal(state.calls[0].init.body,body);assert.equal(state.calls[0].init.headers.Authorization,'Bearer fixture-owner-a');assert.equal(state.calls[0].init.headers['x-justhodl-token'],undefined);
  assert.equal((await window.fetch('/owner-api/watchlist',{method:'DELETE'})).status,405);assert.equal(state.calls.length,1);
});
test('signed-out account mutations never reach network',async()=>{
  const {state,window}=setup({uid:null});assert.equal((await window.fetch('/owner-api/watchlist',{method:'POST',body:'{}'})).status,401);assert.equal(state.calls.length,0);
});
test('manual watchlist and trade pages use account identity without service-token forms or raw fallbacks',()=>{
  for(const name of ['watchlist.html','trade-journal.html']){
    const source=fs.readFileSync(path.join(__dirname,'..',name),'utf8');
    assert.ok(source.includes('https://api.justhodl.ai/owner-api/'));assert.ok(source.includes('JustHodlPrivateArtifacts.ready()'));
    assert.ok(!source.includes('lambda-url.us-east-1.on.aws'));assert.ok(!source.includes('x-justhodl-token'));assert.ok(!source.includes('setItem(\'jh_admin_token\''));
  }
});
