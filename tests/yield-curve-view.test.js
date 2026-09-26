const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const view=require('../jh-yield-curve-view.js');
const now=Date.parse('2026-09-26T03:00:00Z');
function packet(){return JSON.parse(fs.readFileSync('tests/fixtures/yield-curve-view-original.json','utf8'));}
test('all five actual real-yield keys plot at their correct tenors without mutating inputs',()=>{
 const p=packet(),before=JSON.stringify(p),points=view.points(p.real_yields,true);
 assert.deepEqual(points.map(p=>[p.tenor,p.x,p.y]),[['5Y',60,2.65],['7Y',84,2.7],['10Y',120,2.76],['20Y',240,2.99],['30Y',360,3.14]]);
 assert.equal(view.points(p.nominal_yields)[0].x,1);assert.equal(JSON.stringify(p),before);
});
test('missing values cannot become zero while finite zero and negative yields remain visible',()=>{
 assert.deepEqual(view.points({'1M':{value:0},'3M':{value:-.25},'6M':{value:null},'1Y':{value:NaN},'2Y':{value:Infinity}}),[{tenor:'1M',x:1,y:0},{tenor:'3M',x:3,y:-.25}]);
});
test('current view expires without acquisition and rejects bad or missing clocks',()=>{
 assert.equal(view.validLegacy(packet(),now),true);assert.equal(view.validLegacy(packet(),now+48*36e5),false);
 assert.equal(view.validLegacy({...packet(),generated_at:'2026-09-25T23:00:00-03:00'},now),true);
 for(const fields of [{generated_at:null},{generated_at:'2026-09-25'},{generated_at:'2026-09-31T00:00:00Z'},{generated_at:'2099-01-01T00:00:00Z'},{as_of_date:'2026-09-31'},{as_of_date:'2026-09-18'},{quality:{status:'stale'}}])assert.equal(view.validLegacy({...packet(),...fields},now),false);
});
test('the complete pre-repair page is retained byte for byte',()=>{
 const crypto=require('node:crypto'),original=fs.readFileSync('tests/fixtures/legacy-yield-curve-before-view-repair.html.txt');
 assert.equal(crypto.createHash('sha256').update(original).digest('hex'),'44f8e30900d1e11fa93ca8377cd16a39cbdf6f8b003b59f5482a0c1185dc64c7');
});
test('bounded request times out even if the transport or response body ignores cancellation',async()=>{
 for(const fetcher of [()=>new Promise(()=>{}),async()=>({ok:true,json:()=>new Promise(()=>{})})])await assert.rejects(view.json('/test',10,fetcher),/timed out/);
 await assert.rejects(view.json('/test',50,async()=>({ok:false})),/unavailable/);
 assert.deepEqual(await view.json('/test',50,async()=>({ok:true,json:async()=>({value:0})})),{value:0});
});
test('the real inline page renders and timestamps measurements before optional history resolves',async()=>{
 const page=fs.readFileSync('yield-curve.html','utf8'),inline=[...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].find(m=>m[1].includes('const URL_='))[1];
 const elements=new Map(),intervals=[];let clock=now;
 class FakeDate extends Date{static now(){return clock;}}
 const document={getElementById(id){if(!elements.has(id))elements.set(id,{innerHTML:'LOADING',textContent:''});return elements.get(id);}};
 const api={...view,json:async url=>{if(url.includes('yield-curve.json'))return packet();return new Promise(()=>{});}};
 vm.runInNewContext(inline,{document,Date:FakeDate,JHYieldCurve:api,setInterval(fn){intervals.push(fn);}});
 await new Promise(setImmediate);
 assert.match(elements.get('impact').innerHTML,/no asset allocation call/);
 assert.match(elements.get('ts').textContent,/2026-09-25T13:52:50/);
 assert.match(elements.get('chart').innerHTML,/stroke="var\(--purple\)"/);
 assert.match(elements.get('chart').innerHTML,/16 points/);assert.equal((elements.get('chart').innerHTML.match(/<tr>/g)||[]).length,17);
 assert.equal(elements.get('histctx'),undefined);
 clock+=48*36e5;intervals[0]();assert.match(elements.get('hero').innerHTML,/Current curve unavailable/);
 for(const id of ['chart','metrics','impact','signals','bottomline'])assert.equal(elements.get(id).innerHTML,'');
});
test('historical observations show their own dates and do not masquerade as the current curve',()=>{
 const page=fs.readFileSync('yield-curve.html','utf8'),inline=[...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].find(m=>m[1].includes('const URL_='))[1];
 const output={innerHTML:''};
 const context=vm.createContext({document:{getElementById(){return output;}},JHYieldCurve:{...view,json:()=>new Promise(()=>{})},
  reference:{generated_at:'2026-09-25T10:01:36Z',episodes:[],indicators:{T10Y2Y:{label:'2s10s',unit:'%',current:.31,current_date:'2026-09-24',percentile:28.8,at_episodes:{},nearest_episode:{name:'Listed episode',type:'CRISIS',value:.19}}}}});
 vm.runInContext(inline,context);vm.runInContext("renderHistCtx(reference,['T10Y2Y'])",context);
 assert.match(output.innerHTML,/observed 2026-09-24/);assert.match(output.innerHTML,/published 2026-09-25T10:01:36Z/);
 assert.match(output.innerHTML,/not a probability of recurrence/);assert(!output.innerHTML.includes('YOU ARE HERE'));
});
