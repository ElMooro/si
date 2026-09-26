/* Allocation hypotheses stay inspectable without becoming portfolio instructions. */
(function(root){
 'use strict';
 const finite=x=>typeof x==='number'&&Number.isFinite(x);
 const obj=x=>!!x&&typeof x==='object'&&!Array.isArray(x);
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=(x,d=2)=>finite(x)?x.toFixed(d):'unavailable';
 const pct=x=>finite(x)?num(x*100)+'%':'unavailable';
 const url='/data/master-allocation.json?exact=1&nogen=1';
 const link='<a href="/data/master-allocation.json?exact=1&amp;nogen=1">Inspect full published JSON</a>';
 const states=new WeakMap();
 const boundary='<section class="hero" aria-label="Allocation permission"><div class="ptag">Allocation authority</div><h2>WAIT · No qualified target</h2><p>No independently qualified allocation model and reconciled portfolio mandate are registered. WAIT means no new model-authorized allocation; it is not an instruction to liquidate existing holdings.</p><p>Forecast confidence, position sizes and portfolio-risk estimates are unavailable.</p></section>';
 function current(p,now){
  const stamp=p?.generated_at||p?.as_of,age=now-Date.parse(stamp);
  return typeof stamp==='string'&&/T.*(?:Z|[+-]\d{2}:\d{2})$/.test(stamp)&&Number.isFinite(age)&&age>=0&&age<=30*36e5;
 }
 function table(headers,rows){return '<div class="ma-scroll" tabindex="0"><table class="alloctab"><thead><tr>'+headers.map(x=>'<th>'+esc(x)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(row=>'<tr>'+row.map(x=>'<td>'+esc(x)+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';}
 function render(p,now=Date.now()){
  if(!obj(p)||!current(p,now))return boundary+'<p role="status">Research snapshot unavailable, invalid or expired. Previous values have been cleared.</p>'+link;
  const native=p.contract==='master-allocation-research.v1',s=native?p.unqualified_projection:p;
  if(!obj(s))return boundary+'<p role="status">Complete research projection unavailable.</p>'+link;
  const b=obj(s.benchmark)?s.benchmark:{},t=obj(s.target_allocation)?s.target_allocation:{},labels=obj(s.asset_labels)?s.asset_labels:{};
  const assets=[...new Set([...Object.keys(b),...Object.keys(t)])],deltas=obj(s.deltas_from_benchmark)?s.deltas_from_benchmark:{};
  let html=boundary+'<p class="note">Publication '+esc(p.generated_at||p.as_of)+' · '+(native?'Research-only contract':'Legacy publication; qualification not established')+'. Publication time does not establish the observation dates of its inputs. Original-source replay and historical model calibration remain unverified.</p>';
  html+='<section aria-label="Retained allocation hypothesis"><h2>Retained allocation hypothesis</h2><p>These are reported reference weights and heuristic calculations, not your approved mandate or a proposed trade.</p>'+
   (assets.length?table(['Asset class','Reported reference (%)','Heuristic weight (%)','Reported difference (pp)'],assets.map(a=>[labels[a]||a,num(b[a]),num(t[a]),num(deltas[a])])):'<p>Reported allocation weights unavailable.</p>')+'</section>';
  const best=obj(s.best_asset)?s.best_asset:{},ranked=Array.isArray(best.ranked)?best.ranked:[];
  html+='<section aria-label="Reported price momentum"><h2>Reported price momentum · unqualified</h2><p>Reported closing-price changes do not establish total returns. Dividends, instrument adjustments and exact session dates are unverified here. The BIL price proxy is not the yield or earned return on cash. No winner or cash hurdle is authorized.</p>'+
   (ranked.length?table(['Reported instrument','Reported long-window change','125-observation change','62-observation change','Heuristic score'],ranked.map(r=>{r=obj(r)?r:{};return [r.asset==='CASH'?'BIL price proxy (reported as CASH)':r.asset||'unavailable',pct(r.r12_1),pct(r.r6),pct(r.r3),num(r.score)];})):'<p>Reported momentum observations unavailable.</p>')+'</section>';
  html+='<section><h2>Evidence still required</h2><p>Original dated price and income records; a reproducible, independently evaluated model; costs and uncertainty; an approved benchmark and constraints; reconciled holdings, NAV and a common-sample portfolio-risk model.</p></section>';
  html+='<details><summary>Inspect every retained calculation, input and assumption</summary><p>Unqualified producer output. A field named confidence or active risk does not certify a forecast or covariance risk estimate.</p><pre>'+esc(JSON.stringify(s,null,2))+'</pre></details><p>'+link+'</p>';
  return html;
 }
 async function get(fetcher,timeoutMs=12000){
  const abort=new AbortController();let timer;
  try{return await Promise.race([(async()=>{const response=await fetcher(url,{cache:'no-store',signal:abort.signal});if(!response.ok)throw new Error('Unavailable');const text=await response.text();if(text.length>2*1024*1024)throw new Error('Snapshot exceeds bound');return JSON.parse(text);})(),new Promise((resolve,reject)=>{timer=setTimeout(()=>{abort.abort();reject(new Error('Snapshot timeout'));},timeoutMs);})]);}
  finally{clearTimeout(timer);}
 }
 async function mount(doc,fetcher,clock=Date.now,timers=root){
  const el=doc.getElementById('root');if(!el)return;
  states.get(el)?.dispose();let timer;
  const state={dispose(){if(timer!==undefined)timers.clearInterval(timer);if(states.get(el)===state)states.delete(el);}};states.set(el,state);
  el.innerHTML=boundary+'<p role="status">Checking the research publication…</p>';
  try{const packet=await get(fetcher);if(states.get(el)!==state)return state;const show=()=>{if(states.get(el)===state)el.innerHTML=render(packet,clock());};show();timer=timers.setInterval(show,60000);}
  catch(error){if(states.get(el)===state)el.innerHTML=render(null,clock());}
  return state;
 }
 const api={render,current,get,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;
 root.JHMasterAllocationResearch=api;
 if(root.document)root.document.addEventListener('DOMContentLoaded',()=>{mount(root.document,root.fetch.bind(root)).then(state=>{if(state)root.addEventListener('pagehide',()=>state.dispose(),{once:true});});});
})(typeof window==='undefined'?globalThis:window);
