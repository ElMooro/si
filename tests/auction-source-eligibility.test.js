const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const root=path.join(__dirname,'..');
class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.style={};this.attrs={};this.className='';this._text='';this.innerHTML='';}
 set textContent(value){this._text=String(value);this.children=[];}
 get textContent(){return this._text+this.children.map(v=>v.textContent||'').join('');}
 append(...nodes){this.children.push(...nodes);}
 replaceChildren(...nodes){this.children=[...nodes];this._text='';}
 setAttribute(key,value){this.attrs[key]=value;}
}
function fixture(observations=0){
 const generated_at=new Date().toISOString(),prefix='data/auction-observation-originals/';
 return {generated_at,quality:{status:'unavailable'},composite_score:99,regime:'ACUTE_STRESS',
  recent_auctions:[{btc:9.9}],auction_originals:{contract:'auction-original-replay.v1',generated_at,
   coverage:{pages:1,observations,complete_requested_window:true,current_acquisition_vintage:true,
    historical_publication_vintages_verified:false,provider_snapshot_atomicity_verified:false},
   measurements:{key:prefix+'measurements/'+'a'.repeat(64)+'.json',sha256:'a'.repeat(64),bytes:100},
   manifest:{key:prefix+'runs/'+'b'.repeat(64)+'.json',sha256:'b'.repeat(64),bytes:100},
   original_bytes_replayed:true,direct_measurements_replayed:true,historical_point_in_time_verified:false,
   calls_eligible:false,forecast_eligible:false,sizing_eligible:false}};
}
function page(packet){
 const nodes=new Map(),requests=[];
 const get=id=>{if(!nodes.has(id))nodes.set(id,new Element('div'));return nodes.get(id);};
 const env={document:{getElementById:get,createElement:tag=>new Element(tag)},Date,console,AbortController,
  fetch:async url=>{requests.push(url);return {ok:true,json:async()=>packet};}};
 vm.createContext(env);vm.runInContext(fs.readFileSync(path.join(root,'jh-auction-originals.js'),'utf8'),env);
 const source=fs.readFileSync(path.join(root,'auction-crisis.js'),'utf8');
 vm.runInContext(source.slice(0,source.lastIndexOf('load();')),env);
 return {env,get,requests};
}
test('complete empty acquisition remains inspectable while all heuristic measurements are withheld',async()=>{
 const {env,get,requests}=page(fixture());await env.load();
 assert.match(get('auction-originals').textContent,/0 observations across 1 retained page/);
 assert.match(get('auction-originals').textContent,/Inspect every measurement/);
 assert.equal(get('composite-score').textContent,'—');assert.equal(get('regime-text').textContent,'UNAVAILABLE');
 assert.doesNotMatch(get('auctions-tbl').innerHTML,/9\.90/);assert.match(get('decisive-text').textContent,/WAIT/);
 assert.equal(requests.length,1);
});
test('current source capture remains visible when observation freshness or heuristic eligibility is unavailable',async()=>{
 for(const status of ['stale','unavailable','invalid']){
  const packet=fixture(52);packet.quality.status=status;const {env,get}=page(packet);await env.load();
  assert.match(get('auction-originals').textContent,/52 observations/);
  assert.equal(get('composite-score').textContent,'—');assert.notEqual(get('errorBanner').style.display,'none');
 }
});
test('an old publication cannot display its source capture as current after heuristic rejection',async()=>{
 const packet=fixture(52);packet.generated_at=packet.auction_originals.generated_at='2000-01-01T00:00:00Z';
 const {env,get}=page(packet);await env.load();
 assert.match(get('auction-originals').textContent,/current publication clock/);
 assert.doesNotMatch(get('auction-originals').textContent,/Inspect every measurement/);
});
test('a subsequent transport failure clears retained source references as well as heuristic fields',async()=>{
 const {env,get}=page(fixture(52));await env.load();assert.match(get('auction-originals').textContent,/52 observations/);
 env.fetch=async()=>{throw Error('synthetic unavailable transport');};await env.load();
 assert.doesNotMatch(get('auction-originals').textContent,/52 observations|Inspect every measurement/);
 assert.equal(get('composite-score').textContent,'—');
 assert.match(get('errorBanner').textContent,/synthetic unavailable transport/);
});
