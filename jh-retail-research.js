/* Exact-output verified, bounded public attention research. */
(function(root){
 'use strict';
 const PREFIX='data/retail-research/',CURRENT='data/retail-sentiment.json';
 const permissions=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const finite=v=>typeof v==='number'&&Number.isFinite(v);
 const fmt=v=>finite(v)?v.toLocaleString('en-US',{maximumFractionDigits:4}):'Unavailable';
 const safe=key=>/^data\/retail-research\/(?:runs|inputs|outputs|compilers)\/[a-f0-9]{64}\.(?:json|py)$/.test(key||'');
 const typed=p=>p?.contract==='retail-native-research.v1'&&p.call===null&&p.market_regime===null&&p.portfolio_action==='WAIT'&&permissions.every(k=>p[k]===false)&&p.quality?.population_complete===false&&p.communities&&p.stocktwits;
 function current(p,now=Date.now()){
  const g=Date.parse(p.generated_at),d=Date.parse(p.freshness?.pipeline_check_due_at),s=Date.parse(p.freshness?.sample_valid_until),a=Date.parse(p.freshness?.collection_started_at);
  return [g,d,s,a].every(Number.isFinite)&&a<=g&&g<=now&&now<s&&s<=d&&s-a<=7200000&&p.quality?.status==='partial';
 }
 const table=(label,heads,rows)=>'<div class="ir-scroll" tabindex="0" role="region" aria-label="'+esc(label)+'"><table><thead><tr>'+heads.map(h=>'<th>'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.join('')+'</tbody></table></div>';
 const cells=values=>'<tr>'+values.map(v=>'<td>'+v+'</td>').join('')+'</tr>';
 function render(p,now=Date.now()){
  if(!typed(p))return '<p role="alert">Verified attention research unavailable.</p>';
  const fresh=p.freshness;
  let h='<h2>Public attention samples</h2><p class="ir-state">'+(current(p,now)?'Recently collected partial sample':'Retained sample · current use withheld')+' · Research only · WAIT</p><p>Collected '+esc(p.generated_at)+'. Sample-use deadline '+esc(fresh.sample_valid_until)+'. Next daily pipeline check allowance '+esc(fresh.pipeline_check_due_at)+'.</p><p>The actual AWS schedule is daily at 19:10 UTC. These acquisition clocks do not establish the vendor’s observation cutoff. A sample expires for current use after two hours, even when the next scheduled run is tomorrow.</p>';
  h+='<h3>Community coverage</h3><p>ApeWisdom counts ticker mentions in sampled communities. The combined stock sample overlaps individual communities; do not sum these rows. Counts do not measure unique investors, holdings or capital flows.</p>';
  h+=table('Community sample coverage',['Community','Captured symbols','Vendor universe','Sample mentions','Missing baseline','Zero baseline'],Object.values(p.communities).map(c=>cells([esc(c.category),fmt(c.eligible_symbols),fmt(c.reported_symbol_count),fmt(c.sample_mentions),fmt(c.baseline_status_counts?.missing_or_invalid??0),fmt(c.baseline_status_counts?.zero_baseline??0)])));
  for(const c of Object.values(p.communities)){
   h+='<details'+(c.category==='all-stocks'?' open':'')+'><summary>'+esc(c.category)+' · '+fmt(c.eligible_symbols)+' captured symbols</summary><p>'+esc(c.time_window)+' Paired-symbol aggregate uses '+fmt(c.paired_sample.symbols)+' current-sample symbols: '+fmt(c.paired_sample.current_mentions)+' versus '+fmt(c.paired_sample.prior_mentions)+' supplied prior mentions; change '+fmt(c.paired_sample.change_pct)+'%. This is not whole-market growth.</p>';
   h+='<p>Conflicting symbol representations removed: '+fmt(c.conflicting_symbols_removed)+'. Pagination changed during collection: '+esc(c.pagination_changed_between_requests?'yes':'no')+'. Top 10 vendor-ranked symbols account for '+fmt(c.top10_share_of_sample_mentions_pct)+'% of this captured sample’s mentions.</p>';
   h+=table(c.category+' captured mention rows',['Symbol / vendor rank','Mentions','Supplied prior','Count change','Change · %','Baseline','Original row'],c.rows.map(r=>cells([esc(r.symbol)+' / '+fmt(r.provider_rank),fmt(r.mentions),fmt(r.mentions_24h_ago),fmt(r.mention_change),fmt(r.mention_change_pct),esc(r.baseline_status.replaceAll('_',' ')),'page '+fmt(r.source_page)+', row '+fmt(r.source_row)])));
   h+='<p>Zero baseline means percentage growth is undefined. Missing prior counts remain unavailable. Provider tickers are not reconciled issuer or share-class identities.</p><ul>'+c.sources.map(s=>'<li>'+esc(s.identity)+' page '+fmt(s.page)+' · '+esc(s.status)+' · HTTP '+fmt(s.http_status)+' · acquired '+esc(s.acquired_at)+'</li>').join('')+'</ul></details>';
  }
  h+='<h3>StockTwits tagged-message samples</h3><p>At most 30 recent messages per selected symbol; timestamps show the actual span. Bullish share divides bullish tags by bullish plus bearish tags, excluding unclassified messages. It is not StockTwits’ official sentiment score or a representative survey. With zero bearish tags, the ratio remains unavailable.</p>';
  h+=table('StockTwits bounded tagged messages',['Symbol','Eligible / received','Bull / bear / unclassified','Bullish share · %','Unique sampled users','Oldest → newest UTC','Status'],p.stocktwits.streams.map(s=>cells([esc(s.identity),fmt(s.eligible_messages)+' / '+fmt(s.messages_received),[s.bullish,s.bearish,s.unclassified].map(fmt).join(' / '),fmt(s.bullish_share_of_classified_pct),fmt(s.unique_sample_user_count),esc(s.oldest_message_at||'unavailable')+' → '+esc(s.newest_message_at||'unavailable'),esc(s.status)])));
  h+='<p>'+esc(p.stocktwits.stream_selection)+' Symbols and users can overlap across streams. Missing streams are not neutral readings.</p><details><summary>Vendor trending list and retained context</summary><p>Trending list status: '+esc(p.stocktwits.trending_source?.status)+'. Provider order is displayed without converting its opaque score into an investment rank.</p><p>'+p.stocktwits.trending.map(t=>esc(t.symbol)+' (#'+fmt(t.provider_order)+')').join(' · ')+'</p><ul>'+Object.entries(p.context_evidence).map(([key,v])=>'<li>'+esc(key)+' · '+esc(v.status)+'</li>').join('')+'</ul><p>These previous research inputs are preserved whole and privately. They supply no price confirmation, squeeze probability, account flag, alert or investment vote in this model. Historical snapshots remain unchanged and unqualified.</p></details>';
  h+='<p>'+(safe(p.replay?.manifest_key)?'<a href="/'+p.replay.manifest_key+'">Inspect this retained run</a>':'Retained run unavailable')+' · <a href="/data/retail-research-verification.json">Deployment acceptance</a> · <a href="https://apewisdom.io/api/">ApeWisdom definitions</a> · <a href="https://help.stocktwits.com/c/faqs/articles/using-sentiment-on-stocktwits">StockTwits methodology</a></p><p>Whole provider responses, message text and account-derived legacy fields stay in the protected archive. This page verifies the current body against its retained run and output; original-source replay requires the matching reviewed compiler and archive access.</p><button type="button" data-retail-refresh>Refresh and verify</button>';
  return h;
 }
 function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),x=>x.toString(16).padStart(2,'0')).join('');}
 async function load(key,fetcher,signal){
  if(key!==CURRENT&&!safe(key))throw Error('Unapproved research path');const r=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});if(!r.ok)throw Error('HTTP '+r.status);
  const raw=await r.arrayBuffer();if(raw.byteLength>4*1024*1024)throw Error('Research response bound');return {raw,doc:JSON.parse(new TextDecoder().decode(raw))};
 }
 async function verifyPacket(packet,fetcher,signal){
  const key=packet?.replay?.manifest_key;if(!typed(packet)||!/^data\/retail-research\/runs\/[a-f0-9]{64}\.json$/.test(key||''))throw Error('Native retail research required');
  const run=await load(key,fetcher,signal),m=run.doc;
  if(key!==PREFIX+'runs/'+await sha(run.raw)+'.json'||m.contract!=='retail-native-replay.v1'||m.output_sha256!==packet.replay.output_sha256||m.generated_at!==packet.generated_at)throw Error('Run differs');
  const ref=m.output;if(ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Output reference differs');
  const out=await load(ref.key,fetcher,signal);if(out.raw.byteLength!==ref.bytes||await sha(out.raw)!==ref.sha256)throw Error('Output bytes differ');
  const {replay,...body}=packet;if(JSON.stringify(stable(body))!==JSON.stringify(stable(out.doc)))throw Error('Current packet differs');return packet;
 }

 let pending;
 async function mount(){
  const host=root.document?.getElementById('retail-research');if(!host)return;
  if(pending)pending.abort();const c=new AbortController();pending=c;const timer=setTimeout(()=>c.abort(),15000);
  host.innerHTML='<p role="status">Verifying retail sample against retained output…</p>';
  try{const p=await verifyPacket((await load(CURRENT,root.fetch.bind(root),c.signal)).doc,root.fetch.bind(root),c.signal);
   if(c!==pending)return;host.innerHTML=render(p);host.querySelector('[data-retail-refresh]').onclick=mount;
  }catch(e){if(c===pending){host.innerHTML='<p role="alert">Verified retail sample unavailable. No substitute ratio or timing signal is shown.</p><button data-retail-refresh>Retry</button>';host.querySelector('[data-retail-refresh]').onclick=mount;}}
  finally{clearTimeout(timer);}
 }
 function scenario(exposure,shock){
  if(typeof exposure!=='string'||typeof shock!=='string'||!exposure.trim()||!shock.trim())throw Error('Enter both assumptions.');
  const x=Number(exposure),s=Number(shock);
  if(!Number.isFinite(x)||!Number.isFinite(s)||Math.abs(x)>1e12||s<-100||s>1000)throw Error('Use a signed USD exposure within 1 trillion and a shock from -100% to +1000%.');
  return x===0||s===0?0:x*s/100;
 }
 function bindScenario(){
  const form=root.document?.getElementById('retail-scenario');if(!form)return;
  form.oninput=()=>{root.document.getElementById('retail-scenario-result').textContent='Assumptions changed; calculate the entered scenario again.';};
  form.onsubmit=e=>{e.preventDefault();const out=root.document.getElementById('retail-scenario-result');try{
   const value=scenario(form.elements.exposure.value,form.elements.shock.value);out.textContent='Entered scenario: '+value.toLocaleString('en-US',{style:'currency',currency:'USD'})+' price P&L. Signed USD exposure × entered price shock. Excludes FX, dividends, fees, financing and hedges; the attention sample does not predict this shock.';
  }catch(e){out.textContent=e.message;}};
 }
 const api={render,typed,current,verifyPacket,mount,scenario,bindScenario};root.JHRetailResearch=api;if(typeof module!=='undefined')module.exports=api;
 if(root.document){const start=()=>{mount();bindScenario();};if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',start);else start();}
})(typeof globalThis!=='undefined'?globalThis:this);
