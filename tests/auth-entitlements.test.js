const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'..','auth.js'),'utf8');
async function harness(projection){
  const state={now:1000000,calls:[],timers:[],session:{user:{id:'user-a',email:'fixture@example.invalid',user_metadata:{plan:'enterprise',tier:'elite'}},access_token:'token-a'},projection};
  const client={auth:{getSession:async()=>({data:{session:state.session}}),onAuthStateChange(cb){state.change=cb},signOut:async()=>{state.session=null}},from(){throw new Error('profile tier must never be read')}};
  const c=vm.createContext({window:{JUSTHODL_AUTH_CONFIG:{enabled:true,supabaseUrl:'https://sb.invalid',supabaseAnonKey:'anonymous',syncBase:'https://worker.invalid'},supabase:{createClient(){return client}}},document:{},console,AbortSignal,
    Date:class extends Date{static now(){return state.now}},setTimeout(fn,ms){state.timers.push({fn,ms});return state.timers.length},clearTimeout(){},
    fetch:async(url,init)=>{state.calls.push({url,init});const d=typeof state.projection==='function'?await state.projection():state.projection;return Response.json(d,{status:state.status||200})}});
  vm.runInContext(source,c);const auth=c.window.JustHodlAuth;auth._injectCSS=()=>{};auth._ensureSlot=()=>{};auth._renderAuthUI=()=>{};
  await auth.init();return {auth,state};
}
const paid=()=>({src:'durable_stripe_projection',version:1,valid_until_ms:1000500,plan:'pro'});
test('auth ignores editable metadata and stale profile mirrors; only authenticated durable projection grants access',async()=>{
  const {auth,state}=await harness(paid());assert.equal(auth.getTier(),'pro');assert.equal(auth.hasAccess('options_flow'),true);
  assert.equal(state.calls[0].url,'https://worker.invalid/plan/self');assert.equal(state.calls[0].init.headers.Authorization,'Bearer token-a');assert.equal(state.calls[0].init.cache,'no-store');
  state.projection={plan:'enterprise',src:'supabase',version:9,valid_until_ms:1000500};await auth._loadTier();assert.equal(auth.getTier(),'free');assert.equal(auth.hasAccess('risk_desk'),false);
  state.projection={...paid(),plan:'enterprise',valid_until_ms:999999};await auth._loadTier();assert.equal(auth.getTier(),'free');
});
test('auth access expires even when browser timers are suspended; failed refresh grants nothing',async()=>{
  const {auth,state}=await harness(paid());state.now=1000501;assert.equal(auth.getTier(),'free');assert.equal(auth.hasAccess('options_flow'),false);
  state.status=503;await auth._loadTier();assert.equal(auth.getTier(),'free');
});
test('late tier response from signed-out or replaced account cannot authorize current user',async()=>{
  const {auth,state}=await harness(paid());let finish;state.projection=()=>new Promise(resolve=>finish=resolve);
  const pending=auth._loadTier();await new Promise(setImmediate);await auth.signOut();finish(paid());await pending;assert.equal(auth.getTier(),'free');assert.equal(auth.getUser(),null);
  state.session={user:{id:'user-b'},access_token:'token-b'};state.projection={...paid(),plan:'free'};await state.change('SIGNED_IN',state.session);assert.equal(auth.getUser().id,'user-b');assert.equal(auth.getTier(),'free');
});
