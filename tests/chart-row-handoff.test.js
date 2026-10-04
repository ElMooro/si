const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('jh-chart-row-handoff.js','utf8');
function harness(){
 const listeners=[],calls=[],q={value:'original search',dispatchEvent(e){calls.push(['input',e.type]);}};
 const c={window:{jhWatchlistOpen:id=>calls.push(['open',id]),jhGoSymbol(){throw Error('Unqualified path forbidden');}},document:{addEventListener(...args){listeners.push(args);},getElementById:id=>id==='q'?q:null},Event:class{constructor(type){this.type=type;}},fetch(){throw Error('Source-text download forbidden');},Promise,setTimeout(){throw Error('Delayed selection forbidden');}};
 vm.runInNewContext(source,c);assert.equal(listeners.length,1);assert.equal(listeners[0][2],true);
 function click(id,{search=false,control=false,missing=false}={}){
  const row={getAttribute:()=>id,closest:()=>search?{}:null};let prevented=0,stopped=0;
  listeners[0][1]({target:{closest(selector){if(selector==='.w-cmp,.grip,.tvflag,.wx')return control?{}:null;return missing?null:row;}},preventDefault(){prevented++;},stopPropagation(){stopped++;}});
  return {prevented,stopped};
 }
 return {c,calls,q,listeners,click};
}
test('row bridge keeps exact requested identity for the qualified router',()=>{
 const h=harness();for(const id of ['TVC:US10Y','ECONOMICS:THINTR','COT3:11700_F_AMP_L','cftc:gpe5-46if|1170E1|asset_mgr_positions_long','NASDAQ:AAPL','DATA:unavailable']){
  assert.deepEqual(h.click(id),{prevented:1,stopped:1});assert.deepEqual(h.calls.at(-1),['open',id]);
 }
});
test('search cleanup does not change the requested identifier',()=>{
 const h=harness();h.click('ECONOMICS:USBOT',{search:true});assert.equal(h.q.value,'');assert.deepEqual(h.calls,[['input','input'],['open','ECONOMICS:USBOT']]);
});
test('rapid clicks remain synchronous and cannot replay stale requests',()=>{
 const h=harness();h.click('TVC:GOLD');h.click('FRED:DGS10');assert.deepEqual(h.calls,[['open','TVC:GOLD'],['open','FRED:DGS10']]);
});
test('editing controls, unrelated targets and absent router retain existing handlers',()=>{
 const h=harness();for(const options of [{control:true},{missing:true}])assert.deepEqual(h.click('AAPL',options),{prevented:0,stopped:0});
 delete h.c.window.jhWatchlistOpen;assert.deepEqual(h.click('AAPL'),{prevented:0,stopped:0});assert.deepEqual(h.calls,[]);
});
test('duplicate script execution cannot register a second handler',()=>{
 const h=harness();vm.runInNewContext(source,h.c);assert.equal(h.listeners.length,1);
});
