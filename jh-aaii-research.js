/* AAII: verify retained output, distinguish survey opinions from trade authority. */
(function(root){
 'use strict';
 const PREFIX='data/aaii-research/',CURRENT='data/aaii-sentiment.json';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const finite=v=>typeof v==='number'&&Number.isFinite(v);
 const fmt=v=>finite(v)?v.toLocaleString('en-US',{maximumFractionDigits:4}):'Unavailable';
 const labels={bullish_pct:'Bullish',neutral_pct:'Neutral',bearish_pct:'Bearish',bull_bear_spread_pp:'Bull minus bear'};
 const safe=key=>/^data\/aaii-research\/(?:runs|inputs|outputs|compilers)\/[a-f0-9]{64}\.(?:json|py)$/.test(key||'');
 const link=(key,label)=>safe(key)?'<a href="/'+key+'">'+esc(label)+'</a>':esc(label+' unavailable');
 const typed=p=>p?.contract==='aaii-native-research.v1'&&Array.isArray(p.history)&&p.call===null&&p.decision?.abstain===true&&p.decision?.verb==='WAIT'&&['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible'].every(k=>p[k]===false);
 function current(p,now=Date.now()){
  const age=now-Date.parse(p.generated_at),until=Date.parse(p.quality?.freshness?.valid_until);
  return p.quality?.status==='fresh'&&Number.isFinite(age)&&age>=0&&age<=36*3600000&&Number.isFinite(until)&&now<until;
 }
 function render(p,now=Date.now()){
  if(!typed(p))return '<section class="notice" role="alert"><h2>Verified survey unavailable</h2><p>The native research contract could not be established.</p></section>';
  const fresh=current(p,now),r=p.observation||{};
  let html='<section class="notice"><p class="eyebrow">DESCRIPTIVE SURVEY</p><h2>WAIT · research abstains</h2><p>This survey supplies opinions, not a qualified return forecast, trade vote or target position.</p><p>Week ending '+esc(p.as_of||'unavailable')+' · collected '+esc(p.generated_at)+' · source checks '+esc(p.quality?.status)+'.</p>';
  if(!fresh)html+='<p class="warning" role="status">Current survey use is withheld. Any dated readings below are historical observations; a recent compilation time does not make an old release current.</p>';
  html+='<p>'+link(p.replay?.manifest_key,'Run manifest')+' · <a href="/data/aaii-sentiment.json">Download packet</a> · <button type="button" data-aaii-refresh>Refresh</button></p></section>';
  html+='<section><h2>One survey week, four measurements</h2><div class="grid">'+Object.entries(labels).map(([key,label])=>'<article><h3>'+esc(label)+'</h3><p class="value">'+fmt(r[key])+' <small>'+(key.endsWith('_pp')?'pp':'%')+'</small></p><p>'+esc(key.endsWith('_pp')?'Bullish percentage minus bearish percentage.':'Share of participating respondents.')+'</p><p class="muted">Week ending '+esc(r.week_ending||'unavailable')+'</p></article>').join('')+'</div><p class="muted">Printed categories sum to '+fmt(r.printed_sum_pct)+'%. Allowed rounding interval: ±'+fmt(r.rounding_tolerance_pp)+' percentage points. Published values are not renormalized.</p></section>';
  html+='<section><h2>Explore public weekly history</h2><p>'+esc(p.history_scope?.rows)+' rows from '+esc(p.history_scope?.first_week||'unavailable')+' to '+esc(p.history_scope?.last_week||'unavailable')+'. This is the public recent-results window, not the full history since 1987 or a point-in-time backtest.</p><label for="aaii-metric">Measure</label><select id="aaii-metric">'+Object.entries(labels).map(([key,label])=>'<option value="'+key+'"'+(key==='bull_bear_spread_pp'?' selected':'')+'>'+esc(label)+'</option>').join('')+'</select><div id="aaii-history"></div></section>';
  html+='<section class="two"><article><h2>Population and timing</h2><p>Participating AAII members answer one question about the market over the next six months. Online participation is self-selected. These percentages are not allocations, transactions or a representative poll of all investors.</p><p>Respondent count is unavailable in the reviewed source sections; no sampling confidence interval is asserted.</p><p>Voting runs Thursday through Wednesday in Eastern time. The publisher states Thursday morning publication; an exact release timestamp is unavailable.</p><p class="muted">Next source monitoring deadline: '+esc(p.quality?.freshness?.source_due_at||'unavailable')+'. Next pipeline check due: '+esc(p.quality?.freshness?.pipeline_check_due_at||'unavailable')+'. These are monitoring rules, not publisher timestamps.</p></article><article><h2>Portfolio consequences</h2><p>No automatic position change, target weight or liquidation follows from this reading. Any contrarian strategy needs a separate point-in-time, after-cost performance test.</p><p><a href="/position-sizer.html">Open Portfolio scenarios</a> to test explicit shocks against entered exposures.</p><p>Published gauge averages: bullish '+fmt(p.publisher_gauge_averages?.values?.bullish_pct)+'%, neutral '+fmt(p.publisher_gauge_averages?.values?.neutral_pct)+'%, bearish '+fmt(p.publisher_gauge_averages?.values?.bearish_pct)+'%. These are publisher-displayed context, not independently rebuilt full-history estimates.</p></article></section>';
  html+='<section><h2>Evidence and reproduction</h2><p>Reconciled '+esc(p.quality?.cross_checked_weeks?.length||0)+' overlapping weeks between the same publisher’s two pages. This checks consistency; it is not independent source confirmation.</p><p>The authorized AWS runner replays complete retained HTML before publication. This page verifies the run and exact retained output binding. Original HTML stays in the protected source archive.</p>'+Object.entries(p.source_evidence||{}).map(([key,item])=>'<article><h3>'+esc(key==='main'?'Dated survey and recent results':'Labelled public history table')+'</h3><p><a href="'+(key==='main'?'https://www.aaii.com/sentimentsurvey':'https://www.aaii.com/sentimentsurvey/sent_results')+'">Publisher source</a></p><p>Acquired '+esc(item.acquired_at||'unavailable')+' · '+fmt(item.evidence?.bytes)+' original bytes</p><p>SHA-256 <code>'+esc(item.evidence?.sha256||'unavailable')+'</code></p>'+ (item.error?'<p class="warning">'+esc(item.error)+'</p>':'')+'</article>').join('')+'<p class="muted">Missing weeks: '+esc((p.quality?.history_missing_weeks||[]).join(', ')||'none in the returned window')+'. Earlier publications and their revisions remain archived; current source pages do not establish when old values first became available.</p></section>';
  return html;
 }
 function history(p,key){
  if(!Object.hasOwn(labels,key))return '';
  const rows=p.history||[],byDay=new Map(rows.map(r=>[r.week_ending,r])),days=[];
  if(rows.length){for(let d=Date.parse(rows[0].week_ending+'T00:00:00Z'),end=Date.parse(rows.at(-1).week_ending+'T00:00:00Z');Number.isFinite(d)&&d<=end&&days.length<54;d+=7*86400000)days.push(new Date(d).toISOString().slice(0,10));}
  const lower=key.endsWith('_pp')?-100:0,upper=100,w=1040,h=280,left=65,right=1020,top=20,bottom=232;
  const x=i=>left+i*(right-left)/Math.max(1,days.length-1),y=v=>bottom-(v-lower)*(bottom-top)/(upper-lower);
  let segments=[],part=[];days.forEach((day,i)=>{const v=byDay.get(day)?.[key];if(finite(v))part.push(x(i).toFixed(2)+','+y(v).toFixed(2));else if(part.length){segments.push(part);part=[];}});if(part.length)segments.push(part);
  const chart='<figure><svg viewBox="0 0 '+w+' '+h+'" role="img" aria-label="'+esc(labels[key])+': weekly survey history; missing weeks are gaps"><line x1="'+left+'" x2="'+right+'" y1="'+y(0)+'" y2="'+y(0)+'" stroke="currentColor" opacity="0.4"/><text x="5" y="25">'+upper+'</text><text x="5" y="232">'+lower+'</text>'+segments.map(points=>points.length===1?'<circle cx="'+points[0].split(',')[0]+'" cy="'+points[0].split(',')[1]+'" r="4" fill="currentColor"/>':'<polyline points="'+points.join(' ')+'" stroke="currentColor" fill="none" stroke-width="2"/>').join('')+'<text x="'+left+'" y="266">'+esc(days[0]||'')+'</text><text x="'+right+'" y="266" text-anchor="end">'+esc(days.at(-1)||'')+'</text></svg><figcaption>'+esc(labels[key])+' · '+(key.endsWith('_pp')?'percentage points':'percent of respondents')+' · fixed scale; missing weeks remain gaps.</figcaption></figure>';
  return chart+'<div class="table-wrap" tabindex="0" role="region" aria-label="Weekly survey observations"><table><thead><tr><th>Week ending</th><th>Bullish %</th><th>Neutral %</th><th>Bearish %</th><th>Spread pp</th><th>Original evidence</th></tr></thead><tbody>'+days.slice().reverse().map(day=>{const row=byDay.get(day)||{};return '<tr><td>'+esc(day)+'</td>'+Object.keys(labels).map(k=>'<td>'+fmt(row[k])+'</td>').join('')+'<td>'+esc((row.source_evidence||[]).map(e=>e.source+(e.row?' row '+e.row:' line '+e.line)).join('; ')||'Missing week')+'</td></tr>';}).join('')+'</tbody></table></div>';
 }
 function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),x=>x.toString(16).padStart(2,'0')).join('');}
 async function load(key,fetcher,signal){
  if(key!==CURRENT&&!safe(key))throw Error('Unapproved research path');const r=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});if(!r.ok)throw Error('HTTP '+r.status);
  const raw=await r.arrayBuffer();if(raw.byteLength>2*1024*1024)throw Error('Research response bound');return {raw,doc:JSON.parse(new TextDecoder().decode(raw))};
 }
 async function verifyPacket(packet,fetcher,signal){
  const key=packet?.replay?.manifest_key;if(!typed(packet)||!/^data\/aaii-research\/runs\/[a-f0-9]{64}\.json$/.test(key||''))throw Error('Native research required');
  const run=await load(key,fetcher,signal),m=run.doc;
  if(key!==PREFIX+'runs/'+await sha(run.raw)+'.json'||m.contract!=='aaii-native-replay.v1'||m.output_sha256!==packet.replay.output_sha256||m.generated_at!==packet.generated_at)throw Error('Run differs');
  const ref=m.output;if(ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Output reference differs');
  const out=await load(ref.key,fetcher,signal);if(out.raw.byteLength!==ref.bytes||await sha(out.raw)!==ref.sha256)throw Error('Output bytes differ');
  const {replay,...body}=packet;if(JSON.stringify(stable(body))!==JSON.stringify(stable(out.doc)))throw Error('Current packet differs');return packet;
 }
 let pending;
 async function mount(){
  const host=root.document?.getElementById('aaii-research');if(!host)return;if(pending)pending.abort();const controller=new AbortController();pending=controller;
  host.innerHTML='<p role="status">Loading and verifying survey observations…</p>';
  try{
   const p=await verifyPacket((await load(CURRENT,root.fetch.bind(root),controller.signal)).doc,root.fetch.bind(root),controller.signal);if(controller.signal.aborted)return;
   host.innerHTML=render(p);host.querySelector('#aaii-history').innerHTML=history(p,'bull_bear_spread_pp');
   host.querySelector('#aaii-metric').onchange=e=>{host.querySelector('#aaii-history').innerHTML=history(p,e.target.value);};
   host.querySelector('[data-aaii-refresh]').onclick=mount;
  }catch(e){if(e.name!=='AbortError'){host.innerHTML='<section class="notice" role="alert"><h2>Verified survey unavailable</h2><p>The current packet or retained output could not be verified.</p><button type="button" id="aaii-retry">Retry</button></section>';host.querySelector('#aaii-retry').onclick=mount;}}
 }
 const api={render,history,verifyPacket,current,typed};root.JHAAIIResearch=api;if(typeof module!=='undefined')module.exports=api;
 if(root.document){if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',mount);else mount();}
})(typeof globalThis!=='undefined'?globalThis:this);
