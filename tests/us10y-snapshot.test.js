const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const view=require('../jh-us10y-snapshot.js');
const NOW=Date.parse('2026-09-26T14:00:00Z');
const packet=()=>({generated_at:'2026-09-26T12:00:00Z',fred_date:'2026-09-24',methodology_version:'daily-price-episodes.v2',quality:{status:'fresh'},
 level:5.18,tier:'CRITICAL',distance_to_5pct_bps:-18,velocity:{d60_bps:74},real_10y:2.85,real_10y_date:'2026-09-23',pct_rank_since_1990:70,tier_reason:'Source note',
 episode_study:{'cross_4.75':{n_valid_3m:3,median_spx_3m:1.25},'cross_5.00':{n_valid_3m:0,median_spx_3m:null}},
 history_260d:[{d:'2026-09-21',v:4.96},{d:'2026-09-22',v:null},{d:'2026-09-23',v:5.11},{d:'2026-09-24',v:5.18}]});
test('observation expiry follows the producer seven-day UTC calendar rule',()=>{
 const p={...packet(),fred_date:'2026-09-19',real_10y_date:'2026-09-19'};
 assert.equal(view.eligible(p,Date.parse('2026-09-26T23:59:59Z')),true);
 assert.match(view.render(p,Date.parse('2026-09-26T23:59:59Z')),/real 10Y: 2.85%/);
 assert.equal(view.eligible(p,Date.parse('2026-09-27T00:00:00Z')),false);
 p.fred_date='2026-09-24';assert.match(view.render(p,Date.parse('2026-09-27T00:00:00Z')),/real 10Y: unavailable/);
});
test('numeric measurements retain units, dates, observation windows and uncalibrated scope',()=>{
 const html=view.render(packet(),NOW);assert.match(html,/18.0 basis points above 5%/);assert.match(html,/60-observation yield change: \+74.0 basis points/);
 assert.match(html,/Producer band: CRITICAL \(uncalibrated\)/);assert.match(html,/median 1.25%/);assert.match(html,/0 completed 63-observation episodes/);
 assert.doesNotMatch(html,/Validated SP500|Δ60d|NaN|undefined/);assert.match(html,/Original-source replay.*not verified/);
});
test('missing numbers, coerced strings and false values never become zero or throw',()=>{
 for(const missing of [null,undefined,false,'0',NaN,Infinity]){
  const p=packet();p.distance_to_5pct_bps=p.real_10y=p.pct_rank_since_1990=p.velocity.d60_bps=missing;p.episode_study['cross_4.75']={n_valid_3m:missing,median_spx_3m:0};
  const html=view.render(p,NOW);assert.match(html,/Distance to 5% unavailable/);assert.match(html,/yield change: unavailable/);assert.match(html,/Reported real 10Y: unavailable/);
  assert.match(html,/percentile since 1990: unavailable/);assert.match(html,/unavailable completed 63-observation episodes/);assert.doesNotMatch(html,/median 0.00%|NaN|undefined/);
 }
 const p=packet();p.real_10y=p.pct_rank_since_1990=p.velocity.d60_bps=0;const html=view.render(p,NOW);assert.match(html,/real 10Y: 0.00%/);assert.match(html,/yield change: 0.0 basis points/);
 p.distance_to_5pct_bps=18;assert.match(view.render(p,NOW),/Distance to 5% unavailable/);
});
test('invalid levels, dates, methodology and expired or future clocks withhold the panel',()=>{
 for(const changes of [{level:'5.18'},{level:false},{level:Infinity},{fred_date:'2026-02-30'},{fred_date:'2026-09-28'},
  {generated_at:'2026-09-27T00:00:00Z'},{generated_at:'2026-09-23T00:00:00Z'},{generated_at:'2026-09-26T12:00:00'},
  {quality:{status:'stale'}},{methodology_version:'legacy'}]){
  assert.match(view.render({...packet(),...changes},NOW),/snapshot unavailable/);
 }
});
test('native source disclosure exposes full counts and a bounded public manifest, never private reads or claims',()=>{
 const p={...packet(),contract:'us10y-sentinel-research.v1',replay:{manifest_key:'data/us10y-sentinel-research/runs/'+'a'.repeat(64)+'.json',output_sha256:'b'.repeat(64)},
  original_sources:{DGS10:{unit:'Percent',frequency:'Daily',original_rows:16000,missing_rows:300,first_observation:'1962-01-02',last_observation:'2026-09-24'},
   SP500:{unit:'<img src=x>',frequency:'Daily, Close',original_rows:2600,missing_rows:0,first_observation:'2016-09-26',last_observation:'2026-09-25'}}};
 const html=view.render(p,NOW);assert.match(html,/16000 original rows, 300 source gaps/);assert.match(html,/2600 original rows, 0 source gaps/);
 assert.match(html,/DFII10: source inventory unavailable/);assert.match(html,/has not independently repeated/);assert.doesNotMatch(html,/<img|href="private|fetch\(/);
 assert.match(html,/historical|Historical/);p.replay.manifest_key='https://evil.example/';assert.match(view.evidence(p),/manifest unavailable/);
 assert.equal(view.evidence(packet()),'');
});
test('real yield retains its own date and cannot inherit nominal-yield freshness',()=>{
 assert.match(view.render(packet(),NOW),/real 10Y: 2.85% \(observed 2026-09-23\)/);
 for(const value of [null,'2026-09-01','2026-09-28','2026-02-30']){
  const html=view.render({...packet(),real_10y_date:value},NOW);assert.match(html,/real 10Y: unavailable/);assert.match(html,/5.18%/);
 }
});
test('reported narrative and labels cannot inject HTML',()=>{
 const p=packet();p.tier='<img src=x onerror=alert(1)>';p.tier_reason='<script>alert("x")</script><img onerror=alert(2)>';
 const html=view.render(p,NOW);assert.doesNotMatch(html,/<script>|<img/);assert.match(html,/&lt;script&gt;/);assert.match(html,/Producer band: unavailable/);
});
test('chart uses dates and preserves missing rows as gaps, with invalid histories withheld',()=>{
 const p=packet(),html=view.spark(p.history_260d,p.fred_date);assert.match(html,/1 missing observations remain gaps/);assert.match(html,/3 reported yields, 1 unavailable/);
 assert.equal((html.match(/<circle /g)||[]).length,1);assert.equal((html.match(/<polyline /g)||[]).length,1);
 for(const rows of [[{d:'2026-09-24',v:1},{d:'2026-09-24',v:2}], [{d:'2026-09-24',v:1},{d:'2026-09-23',v:2}],
  [{d:'2026-09-23',v:1},{d:'2026-09-25',v:2}], [{d:'2026-09-23',v:null},{d:'2026-09-24',v:2}],
  [{d:'2026-09-23',v:-1e308},{d:'2026-09-24',v:1e308}]])assert.doesNotMatch(view.spark(rows,p.fred_date),/<svg/);
});
test('mounted panel expires without acquiring data and failed refresh clears previous data',async()=>{
 const el={},doc={getElementById:()=>el},callbacks=[],timers={setInterval:f=>(callbacks.push(f),callbacks.length),clearInterval(){}};let now=NOW,requests=0;
 const fetcher=async(url,options)=>{requests++;assert.equal(url,'/data/us10y-sentinel.json?exact=1&nogen=1');assert.equal(options.cache,'no-store');return {ok:true,json:async()=>packet()};};
 const state=await view.mount(doc,fetcher,()=>now,timers);assert.match(el.innerHTML,/5.18%/);
 el.innerHTML+='USER_DISCLOSURE_STATE';callbacks[0]();assert.match(el.innerHTML,/USER_DISCLOSURE_STATE/);
 now+=3*864e5;callbacks[0]();assert.match(el.innerHTML,/snapshot unavailable/);assert.doesNotMatch(el.innerHTML,/USER_DISCLOSURE_STATE/);assert.equal(requests,1);
 await view.mount(doc,async()=>({ok:false}),()=>NOW,timers);assert.match(el.innerHTML,/snapshot unavailable/);state.dispose();
});
test('slower loads cannot restore data after a newer failed refresh',async()=>{
 const el={},doc={getElementById:()=>el},timers={setInterval(){return 1;},clearInterval(){}};let finish;
 const older=view.mount(doc,()=>new Promise(r=>finish=r),()=>NOW,timers);
 await view.mount(doc,async()=>({ok:false}),()=>NOW,timers);finish({ok:true,json:async()=>packet()});await older;
 assert.match(el.innerHTML,/snapshot unavailable/);
});
test('transport and body timeouts complete even when cancellation is ignored',async()=>{
 for(const fetcher of [()=>new Promise(()=>{}),async()=>({ok:true,json:()=>new Promise(()=>{})})])
  await assert.rejects(view.get(fetcher,10),/timed out/);
});
test('actual curve page loads the reviewed snapshot renderer without the old inline coercions',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../yield-curve.html'),'utf8');
 assert.match(html,/<script src="\/jh-us10y-snapshot.js" defer><\/script>/);assert.doesNotMatch(html,/Math.round\(d.distance_to_5pct_bps\)|\+d.tier\+|Validated SP500 price-only history/);
});
