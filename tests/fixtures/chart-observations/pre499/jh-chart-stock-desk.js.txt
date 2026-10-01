/* Complete descriptive chart-frame research. Predecessor retained in tests/fixtures. */
(function(root,factory){
 if(typeof module==='object'&&module.exports)module.exports=factory(require('./jh-stock-desk-research.js'));
 else{const api=factory(root.JHStockDeskResearch);root.JHStockDeskView=api;root.JHStockDeskController=api.install(root);}
})(typeof window==='object'?window:null,function(core){
 'use strict';
 const esc=x=>String(x??'Unavailable').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const finite=x=>typeof x==='number'&&Number.isFinite(x);
 const number=x=>finite(x)?x.toLocaleString('en-US',{maximumFractionDigits:4}):'Unavailable';
 const date=x=>typeof x==='string'?x.replace('T',' ').replace('.000Z',' UTC'):'Unavailable';
 function rowHTML(p){
  return '<tr><td>'+esc(p.kind.replace('_',' '))+'</td><td>'+esc(date(p.left.at))+'<br>'+number(p.left.price)+'</td><td>'+esc(date(p.right.at))+'<br>'+number(p.right.price)+'</td>'+
   '<td>'+number(p.distance_observations)+' bars<br>'+number(p.observations_since_second)+' bars since second</td><td>'+number(p.neckline)+'</td>'+
   '<td>'+number(p.comparison_close)+' · '+esc(p.latest_close_relation)+'<br>'+esc(date(p.comparison_at))+'</td>'+
   '<td>'+number(p.left.volume)+' → '+number(p.right.volume)+'<br>'+(p.second_extremum_quieter===null?'Volume comparison unavailable':p.second_extremum_quieter?'Second extremum has lower reported volume':'Second extremum does not have lower reported volume')+'</td>'+
   '<td>'+esc(date(p.right.identifiable_after))+'</td></tr>';
 }
 function page(model,index=0){
  const count=model.patterns.length,pages=Math.max(1,Math.ceil(count/25));index=Math.max(0,Math.min(Number.isInteger(index)?index:0,pages-1));
  return {index,pages,total:count,first:count?index*25+1:0,last:Math.min((index+1)*25,count),rows:model.patterns.slice(index*25,(index+1)*25)};
 }
 function markup(model){
  const title='<b>CHART PRICE / VOLUME OBSERVATIONS</b>';
  if(!model.valid)return title+'<p>Research unavailable: '+model.errors.map(esc).join('; ')+'. Complete input rows remain in the chart; no zero or trade instruction is substituted.</p>';
  const r=model.relative_volume,b=model.bollinger;
  return title+'<p>'+esc(model.symbol)+' · '+esc(model.interval)+' · '+model.bar_count+' retained bars<br>'+esc(date(model.first_observation))+' → '+esc(date(model.last_observation))+'</p>'+
   '<p>Reported source: '+esc(model.source)+'. Source acquisition time, completed-bar status, adjustment basis, upstream volume defaults and volume units are unverified here. These measurements grant no Calls vote, forecast or position size.</p>'+
   '<div class="cell"><span>Latest volume / prior 20-bar mean</span><span>'+(finite(r.ratio)?number(r.ratio)+'×':'Unavailable')+'</span></div>'+
   '<p>Prior window: '+esc(date(r.start))+' → '+esc(date(r.end))+' · '+r.available_volume_rows+'/'+r.observations+' volumes available. Latest bar is excluded from the denominator. Missing volume is not zero.</p>'+
   '<div class="cell"><span>Latest reported volume</span><span>'+number(r.latest)+'</span></div>'+
   (b?'<div class="cell"><span>20-bar Bollinger width / mean close</span><span>'+number(b.ratio*100)+'%</span></div><p>Empirical midrank '+number(b.percentile*100)+'% across all '+b.history_windows+' complete windows. Four population standard deviations / mean close; this is not a probability or a trading signal.</p>':'<p>20-bar price dispersion unavailable: at least 20 valid observations required.</p>')+
   '<details data-stock-pairs><summary>Inspect all '+model.patterns.length+' historical geometric pairs</summary><p>Five bars on each side identify an extremum retrospectively. Pair separation: 8–90 observed bars; extremum price difference ≤3%. Latest close is compared with every retained pair’s neckline; no breakout date or predictive edge is established. Overlapping pairs are not independent votes.'+(model.bar_count<40?' At least 40 bars are required for this screen.':'')+'</p>'+
   '<div style="overflow:auto;max-width:100%;max-height:450px" tabindex="0" role="region" aria-label="Complete historical chart pattern pairs"><table><thead><tr><th>Geometry</th><th>First extremum</th><th>Second extremum</th><th>Observed intervals</th><th>Neckline</th><th>Latest close comparison</th><th>Reported volume</th><th>Second extremum identifiable after</th></tr></thead><tbody data-stock-rows></tbody></table></div>'+
   '<div><button type="button" data-stock-prev>Previous pairs</button> <span data-stock-range role="status" aria-live="polite"></span> <button type="button" data-stock-next>Next pairs</button></div></details>'+
   '<button type="button" data-stock-export>Download complete frame and calculations</button><p>The export retains every input bar, every geometric pair, both volume-denominator definitions and all limitations. It is a browser calculation record, not an independently replayed original provider response.</p>';
 }
 function install(win){
  let previous=null,lastHTML=null,model=null,pairPage=0,host=null,stopped=false,interval=null;
  function replace(html){if(html!==lastHTML||host.getAttribute('data-stock-desk-owned')!=='true'){host.innerHTML=html;host.setAttribute('data-stock-desk-owned','true');lastHTML=html;}}
  function paintPairs(){
   const result=page(model,pairPage);pairPage=result.index;
   host.querySelector('[data-stock-rows]').innerHTML=result.rows.map(rowHTML).join('')||'<tr><td colspan="8">No candidate pairs in this retained frame.</td></tr>';
   host.querySelector('[data-stock-range]').textContent=result.first+'–'+result.last+' of '+result.total+' pairs';
   host.querySelector('[data-stock-prev]').disabled=pairPage===0;
   host.querySelector('[data-stock-next]').disabled=pairPage+1===result.pages;
  }
  function update(){
   if(stopped)return;
   const currentHost=win.document.getElementById('tech')||win.document.getElementById('intel');if(!currentHost)return;
   if(currentHost!==host){host=currentHost;previous=null;lastHTML=null;}
   const frame=win.jhChartEvidence,selected=win.document.querySelector('#tabs .tab.on[data-id]')?.getAttribute('data-id');
   if(!core||!core.bind(frame,win.lastBars,selected)){
    previous=null;model=null;replace('<b>CHART PRICE / VOLUME OBSERVATIONS</b><p>No matching chart frame for the selected ticker. Waiting for identified data; no earlier ticker calculation is shown.</p>');return;
   }
   // Compare complete content, not just length/last close: an interior revision
   // must invalidate the calculation too. Invalid numbers remain explicit.
   const raw=JSON.stringify({symbol:frame.symbol,interval:frame.interval,source:frame.source,bars:frame.bars},
    (key,value)=>typeof value==='number'&&!Number.isFinite(value)?{invalid_number:String(value)}:value);
   if(raw===previous&&(model?.valid?host.querySelector('[data-stock-pairs]'):host.innerHTML===lastHTML))return;
   lastHTML=null;
   previous=raw;const snapshot=JSON.parse(raw);snapshot.published_at=frame.published_at;
   model=core.calculate(snapshot);pairPage=0;replace(markup(model));if(!model.valid)return;
   const details=host.querySelector('[data-stock-pairs]');details.addEventListener('toggle',()=>{if(details.open)paintPairs();});
   host.querySelector('[data-stock-prev]').onclick=()=>{pairPage--;paintPairs();};
   host.querySelector('[data-stock-next]').onclick=()=>{pairPage++;paintPairs();};
   host.querySelector('[data-stock-export]').onclick=()=>{
    const url=win.URL.createObjectURL(new win.Blob([JSON.stringify(model,null,2)+'\n'],{type:'application/json'}));
    const link=win.document.createElement('a');link.href=url;link.download='chart-frame-research.json';link.click();
    win.setTimeout(()=>win.URL.revokeObjectURL(url),1000);
   };
  }
  function refresh(){try{update();}catch(error){previous=null;model=null;lastHTML=null;if(host)replace('<b>CHART PRICE / VOLUME OBSERVATIONS</b><p>Research unavailable: chart frame could not be validated. No previous calculation is retained.</p>');}}
  function start(){stopped=false;if(interval===null)interval=win.setInterval(refresh,2500);refresh();}
  win.addEventListener('load',refresh);start();
  win.addEventListener('pagehide',()=>{stopped=true;if(interval!==null)win.clearInterval(interval);interval=null;});
  win.addEventListener('pageshow',start);
  return {refresh,getModel:()=>model};
 }
 return {rowHTML,page,markup,install};
});
