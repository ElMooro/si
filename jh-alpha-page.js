(async function(){
 'use strict';const api=window.AlphaResearch,$=id=>document.getElementById(id);
 $('payoff').addEventListener('submit',event=>{
  event.preventDefault();try{const r=api.scenario($('notional').value,$('shock').value,$('cost').value,$('side').value);
   $('payoff-result').textContent=`Gross price P&L: USD ${r.gross_usd}. Entered costs: USD ${r.entered_cost_usd}. After entered costs: USD ${r.after_entered_cost_usd}. ${r.formula}. ${r.scope}`;
  }catch(error){$('payoff-result').textContent=error.message;}
 });
 try{
  const requested=new URL(location.href).searchParams.get('run');let id=requested,pointer=null;
  if(!id){const r=await fetch('/data/alpha-compass.json',{cache:'no-store'});if(!r.ok)throw Error('Current Alpha research unavailable');pointer=await r.json();
   if(pointer.contract!==api.CONTRACT)throw Error('Reproducible Alpha research has not been published yet');id=pointer.replay?.manifest_key?.match(/^data\/alpha-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);
  if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from immutable output');
  let last=null;
  function refresh(){const view=api.render(packet,Date.now(),Boolean(requested)),signature=JSON.stringify(view);if(signature===last)return;last=signature;
   for(const name of ['hero','ideas','regimes','dependency','sources','fails','evidence'])$(name).innerHTML=view[name];$('ts').textContent=view.sub;}
  refresh();setInterval(refresh,60000);
  // A pinned Compass never silently borrows a later brief.
  if(requested){$('brief').textContent='Pinned snapshot. The immutable decision records above are the research for this run. Load latest research for the current daily brief.';return;}
  try{
   const response=await fetch('/data/alpha-brief.json',{cache:'no-store'});if(!response.ok)throw Error('Brief unavailable');const current=await response.json();
   const briefId=current.replay?.manifest_key?.match(/^data\/alpha-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];const brief=await api.loadSnapshot(briefId,window.fetch.bind(window),'brief');
   if(current.replay.output_sha256!==brief.replay.output_sha256||current.generated_at!==brief.generated_at)throw Error('Brief pointer differs');
   $('brief').innerHTML=api.boundBrief(brief,packet.replay.manifest_key);
   const link=document.createElement('a');link.href=api.path(brief.source_replay.manifest_key);link.textContent='Daily brief source snapshot manifest';$('brief-links').replaceChildren(link);
   function briefAge(){$('brief-age').textContent=api.recent(brief.quality.source_generated_at,Date.now(),4)?'Brief source within the 4-hour research snapshot policy.':'Brief source is stale; retained context only.';}briefAge();setInterval(briefAge,60000);
  }catch(error){$('brief').textContent='Daily brief unavailable: '+error.message+'. Research decisions remain visible above.';}
 }catch(error){$('hero').textContent='Alpha research unavailable: '+error.message;$('ts').textContent='Use Load latest research to leave an unavailable snapshot. No direction or allocation inferred.';}
})();
