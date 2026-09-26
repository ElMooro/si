/* Separate daily-price producer context; never an investment or crisis signal. */
(function(root){
 'use strict';
 const finite=x=>typeof x==='number'&&Number.isFinite(x);
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const integer=x=>Number.isInteger(x)&&x>=0;
 const number=(x,d=0)=>finite(x)?x.toFixed(d):'unavailable';
 const date=x=>typeof x==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(x)&&Number.isFinite(Date.parse(x))&&new Date(x).toISOString().slice(0,10)===x;
 const observationAge=(d,now)=>date(d)?Math.floor(now/864e5)-Date.parse(d)/864e5:NaN;
 const states=new WeakMap();
 const unavailable='<div class="loading">Current 10-year producer snapshot unavailable; stale, missing or invalid values excluded.</div>';
 async function get(fetcher,timeoutMs=12000){
  const controller=new AbortController();let timer;
  try{return await Promise.race([(async()=>{
   const response=await fetcher('/data/us10y-sentinel.json?exact=1&nogen=1',{cache:'no-store',signal:controller.signal});
   if(!response.ok)throw new Error('Unavailable');return await response.json();
  })(),new Promise((resolve,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new Error('Snapshot request timed out'));},timeoutMs);})]);
  }finally{clearTimeout(timer);}
 }
 function eligible(d,now){
  const age=now-Date.parse(d?.generated_at),obsAge=observationAge(d?.fred_date,now);
  return !!d&&typeof d.generated_at==='string'&&finite(d.level)&&d.methodology_version==='daily-price-episodes.v2'&&d.quality?.status==='fresh'&&date(d.fred_date)&&
   Number.isFinite(age)&&age>=0&&age<=48*36e5&&Number.isFinite(obsAge)&&obsAge>=0&&obsAge<=7;
 }
 function distance(d){
  if(!finite(d.distance_to_5pct_bps)||Math.abs(d.distance_to_5pct_bps-(5-d.level)*100)>.11)return 'Distance to 5% unavailable';
  const gap=d.distance_to_5pct_bps;
  return Math.abs(gap)<.05?'At the 5% threshold':number(Math.abs(gap),1)+' basis points '+(gap<0?'above':'below')+' 5%';
 }
 function episode(row){
  const n=row?.n_valid_3m,median=row?.median_spx_3m;
  return (integer(n)?n:'unavailable')+' completed 63-observation episodes; median '+
   (integer(n)&&n>=3&&finite(median)?number(median,2)+'%':'unavailable (requires at least 3 valid episodes)');
 }
 function spark(rows,asOf){
  if(!Array.isArray(rows)||rows.length<2)return '<p>Reported yield history unavailable.</p>';
  if(rows.some((r,i)=>!r||!date(r.d)||r.d>asOf||(i&&r.d<=rows[i-1].d)))return '<p>Reported yield chart unavailable: invalid, repeated, future or unordered dates.</p>';
  const values=rows.filter(r=>finite(r.v));
  if(values.length<2)return '<p>Reported yield chart unavailable: fewer than two numeric observations.</p>';
  const mn=values.reduce((a,r)=>Math.min(a,r.v),Infinity),mx=values.reduce((a,r)=>Math.max(a,r.v),-Infinity),span=mx-mn;
  if(!Number.isFinite(span))return '<p>Reported yield chart unavailable: numeric range invalid.</p>';
  const start=Date.parse(rows[0].d),end=Date.parse(rows[rows.length-1].d),parts=[];let part=[];
  const flush=()=>{if(part.length)parts.push(part);part=[];};
  for(const r of rows){
   if(!finite(r.v)){flush();continue;}
   part.push({x:((Date.parse(r.d)-start)/(end-start)*560).toFixed(2),y:(50-(r.v-mn)/(span||1)*46).toFixed(2)});
  }
  flush();
  const missing=rows.length-values.length;
  return '<figure style="margin:12px 0 0"><svg role="img" aria-label="Reported ten-year nominal yield history, percent; '+missing+' missing observations remain gaps" width="100%" height="72" viewBox="0 0 560 56" preserveAspectRatio="none">'+
   parts.map(p=>p.length===1?'<circle cx="'+p[0].x+'" cy="'+p[0].y+'" r="2" fill="currentColor"/>':'<polyline points="'+p.map(v=>v.x+','+v.y).join(' ')+'" fill="none" stroke="currentColor" stroke-width="1.6"/>').join('')+
   '</svg><figcaption>'+esc(rows[0].d)+' → '+esc(rows[rows.length-1].d)+' · '+values.length+' reported yields, '+missing+' unavailable · percent, calendar-date axis</figcaption></figure>';
 }
 function render(d,now=Date.now()){
  if(!eligible(d,now))return unavailable;
  const tiers=['BENIGN','WATCH','ELEVATED','HIGH','RED','CRITICAL'],tier=tiers.includes(d.tier)?d.tier:'unavailable';
  const velocity=d.velocity?.d60_bps,rank=d.pct_rank_since_1990;
  const realAge=observationAge(d.real_10y_date,now),realCurrent=finite(d.real_10y)&&date(d.real_10y_date)&&Number.isFinite(realAge)&&realAge>=0&&realAge<=7;
  return '<section style="border:1px solid #6b7280;border-radius:10px;padding:14px 16px" aria-label="Separate 10-year yield producer snapshot">'+
   '<p><strong style="font:700 28px monospace">'+number(d.level,2)+'%</strong> · Observed '+esc(d.fred_date)+' · '+distance(d)+'</p>'+
   '<p>60-observation yield change: '+(finite(velocity)?(velocity>0?'+':'')+number(velocity,1)+' basis points':'unavailable')+
   ' · Reported real 10Y: '+(realCurrent?number(d.real_10y,2)+'% (observed '+esc(d.real_10y_date)+')':'unavailable')+
   ' · Reported percentile since 1990: '+(finite(rank)&&rank>=0&&rank<=100?number(rank):'unavailable')+'</p>'+
   '<p>Producer band: '+esc(tier)+' (uncalibrated). Yield-level bands do not establish a crisis probability, asset-return forecast or position size. A 60-observation change is not a 60-calendar-day return.</p>'+
   '<p>Reported SP500 price-only episodes (dividends excluded): 4.75% — '+episode(d.episode_study?.['cross_4.75'])+' · 5.00% — '+episode(d.episode_study?.['cross_5.00'])+'. Earlier outcomes outside daily price coverage are excluded. Original-source replay of this separate producer is not verified by this panel.</p>'+
   (typeof d.tier_reason==='string'?'<details><summary>Original producer note · unvalidated interpretation</summary><p>'+esc(d.tier_reason)+'</p></details>':'')+
   spark(d.history_260d,d.fred_date)+'<p><a href="/data/us10y-sentinel.json?exact=1&amp;nogen=1">Inspect complete producer JSON</a> · Snapshot '+esc(d.generated_at)+'</p></section>';
 }
 async function mount(doc,fetcher,clock=Date.now,timers=root){
  const el=doc.getElementById('us10y-sentinel');if(!el)return;
  const prior=states.get(el);if(prior)prior.dispose();
  let timer;const state={dispose(){if(timer!==undefined)timers.clearInterval(timer);if(states.get(el)===state)states.delete(el);}};
  states.set(el,state);el.textContent='Checking dated 10-year producer snapshot…';
  try{
   const packet=await get(fetcher);if(states.get(el)!==state)return state;
   let lastHTML;
   const refresh=()=>{if(states.get(el)!==state)return;const html=render(packet,clock());if(html!==lastHTML){el.innerHTML=html;lastHTML=html;}};refresh();
   timer=timers.setInterval(refresh,60000);
  }catch(e){if(states.get(el)===state)el.innerHTML=unavailable;}
  return state;
 }
 const api={get,eligible,distance,episode,spark,render,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 root.JHUS10YSnapshot=api;
 if(root.document)root.document.addEventListener('DOMContentLoaded',()=>{mount(root.document,root.fetch.bind(root)).then(state=>{
  if(state)root.addEventListener('pagehide',()=>state.dispose(),{once:true});
 });});
})(typeof window==='undefined'?globalThis:window);
