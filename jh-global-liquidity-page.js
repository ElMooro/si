(async function(){
 'use strict';const api=window.GlobalLiquidityResearch,$=id=>document.getElementById(id);
 try{
  const requested=new URL(location.href).searchParams.get('run');let pointer=null,id=requested;
  if(!requested){const response=await fetch('/data/global-liquidity.json',{cache:'no-store'});if(!response.ok)throw Error('Current research unavailable');
   pointer=await response.json();if(pointer.contract!==api.CONTRACT)throw Error('Original-bound research has not been published yet');
   id=pointer.replay?.manifest_key?.match(/^data\/global-liquidity-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);
  if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from its immutable snapshot');
  const view=api.render(packet,Date.now(),Boolean(requested));
  for(const name of ['summary','components','series','chart','evidence'])$(name).innerHTML=view[name];
  for(const name of ['status','stamp','history'])$(name).textContent=view[name];
  $('methodology').textContent=packet.methodology;$('reason').textContent=packet.decision.reason;
  $('dates').innerHTML=view.options;const refresh=()=>{$('sample').innerHTML=api.inspect(packet,$('dates').value);};
  $('dates').addEventListener('change',refresh);refresh();
  const change=()=>{$('decomposition').innerHTML=api.change(packet,$('horizon').value);};$('horizon').addEventListener('change',change);change();
 }catch(error){$('status').textContent='Research unavailable: '+error.message;$('status').classList.add('error');$('reason').textContent='No unverified values or investment directions are displayed. Use Load latest research to leave an invalid or unavailable pinned snapshot.';}
})();
