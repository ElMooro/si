(async function(){
 'use strict';
 const api=window.HoldingsResearch,$=id=>document.getElementById(id);
 try{
  const params=new URL(location.href).searchParams,pinned=params.has('run');let run=params.get('run'),pointer=null;
  if(!run){const response=await fetch('/data/holdings-research.json',{cache:'no-store'});if(!response.ok)throw Error('Holdings source snapshot has not been published.');pointer=await response.json();run=pointer.replay?.manifest_key?.match(/^data\/holdings-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  if(!/^[a-f0-9]{64}$/.test(run||''))throw Error('A retained holdings run is required.');
  const key=api.PREFIX+'runs/'+run+'.json',response=await fetch('/'+key,{cache:'no-store'});if(!response.ok)throw Error('Retained holdings run unavailable');const raw=await response.arrayBuffer();if(raw.byteLength>1024*1024)throw Error('Holdings manifest bound');
  const sha=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');if(sha!==run)throw Error('Holdings manifest hash differs');const manifest=JSON.parse(new TextDecoder().decode(raw));
  if(manifest.contract!=='holdings-native-replay.v1'||manifest.output_sha256!==manifest.output.sha256||pointer&&pointer.replay.output_sha256!==manifest.output_sha256)throw Error('Holdings replay binding differs');
  const packet=api.boundary(await api.verified(fetch,manifest.output));if(packet.generated_at!==manifest.generated_at)throw Error('Holdings generation binding differs');
  let selected=Object.hasOwn(packet.funds,params.get('fund'))?params.get('fund'):'BERKSHIRE',doc=null,page=0,ticket=0;
  if(!Object.hasOwn(packet.funds,selected))selected=Object.keys(packet.funds)[0];
  $('selection').innerHTML=Object.entries(packet.funds).map(([id,f])=>'<option value="'+api.esc(id)+'">'+api.esc(f.official_name)+' · '+api.esc(f.current_holdings_period||'Unavailable')+'</option>').join('');$('selection').value=selected;
  $('coverage').innerHTML=api.table(['Report period','Managers in this snapshot'],Object.entries(packet.report_period_cohorts).map(([period,names])=>[api.esc(period),api.esc(names.map(name=>packet.funds[name].official_name).join('; '))]));
  $('asof').textContent='Originals acquired: '+packet.source_generated_at+' · Research compiled: '+packet.generated_at+'.';
  $('refresh').textContent=packet.refresh_status;
  function overview(){$('state').textContent=api.status(packet,Date.now(),pinned)+'. '+packet.fund_count+' configured managers; '+packet.current_cohort_count+' with acquired holdings for '+packet.required_period_for_current_cohort+'.';}
  function link(){const url=new URL('/holdings-research.html',location.origin);url.searchParams.set('run',run);url.searchParams.set('fund',selected);if($('search').value)url.searchParams.set('search',$('search').value);if($('filter').value)url.searchParams.set('filter',$('filter').value);$('permanent').href=url.pathname+url.search;return url.pathname+url.search;}
  function records(){link();if(!doc)return;const result=api.positions(doc,$('search').value,$('filter').value,page);page=result.page;$('positions').innerHTML=result.html;$('page-count').textContent=result.count+' matching securities · page '+(page+1)+' of '+result.pages;$('prev').disabled=page===0;$('next').disabled=page+1>=result.pages;$('row-evidence').innerHTML='Select “Trace rows” to inspect original evidence.';}
  async function select(){const current=++ticket;selected=$('selection').value;page=0;doc=null;link();$('positions').textContent='Verifying complete fund disclosure…';$('fund-summary').textContent='';$('filing-chain').textContent='';$('download').disabled=true;$('prev').disabled=true;$('next').disabled=true;$('row-evidence').textContent='';
   try{const loaded=api.fund(await api.verified(fetch,packet.funds[selected].detail),selected);if(current!==ticket)return;doc=loaded;$('fund-summary').innerHTML=api.summary(doc);$('filing-chain').innerHTML=api.chain(doc);$('download').disabled=false;records();}catch(error){if(current===ticket)$('positions').textContent=error.message;}}
  $('evidence').innerHTML='<p><a href="/'+api.esc(key)+'">Immutable replay manifest</a> · <a href="/'+api.esc(manifest.input.key)+'">Acquisition and preservation receipts</a> · <a href="/'+api.esc(manifest.output.key)+'">Exact research output</a></p><p>Reproduce every full filing and comparison with the matching reviewed code:</p><code>python scripts/replay_holdings_research.py --run '+run+'</code>';
  $('filter').innerHTML='<option value="">All disclosure comparisons</option>'+Object.entries(api.LABELS).map(([key,label])=>'<option value="'+key+'">'+api.esc(label)+'</option>').join('');
  $('search').value=(params.get('search')||'').slice(0,120);if(Object.hasOwn(api.LABELS,params.get('filter')))$('filter').value=params.get('filter');
  $('selection').addEventListener('change',select);for(const id of ['search','filter'])$(id).addEventListener('input',()=>{page=0;records();});
  $('prev').addEventListener('click',()=>{page=Math.max(0,page-1);records();});$('next').addEventListener('click',()=>{page++;records();});
  $('positions').addEventListener('click',event=>{const button=event.target.closest('button[data-evidence]');if(button&&doc){try{$('row-evidence').innerHTML=api.evidence(doc,button.dataset.evidence);}catch(error){$('row-evidence').textContent=error.message;}}});
  $('download').addEventListener('click',()=>{if(!doc)return;const value={contract:'holdings-research-export.v1',manifest:key,output_sha256:manifest.output_sha256,snapshot_link:link(),fund:doc},url=URL.createObjectURL(new Blob([JSON.stringify(value,null,2)],{type:'application/json'})),a=document.createElement('a');a.href=url;a.download='holdings-'+selected+'-'+run.slice(0,12)+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
  overview();await select();setInterval(overview,30000);
 }catch(error){$('state').textContent=error.message;$('asof').textContent='No substitute values or buying signals have been generated.';}
})();
