(async function(){
 'use strict';const api=window.CBResearch,$=id=>document.getElementById(id);
 try{
  const requested=new URL(location.href).searchParams.get('run');let id=requested,pointer=null;
  if(!id){const r=await fetch('/data/cb-injection.json',{cache:'no-store'});if(!r.ok)throw Error('Current research unavailable');pointer=await r.json();
   if(pointer.contract!==api.CONTRACT)throw Error('Original-bound research has not been published yet');
   id=pointer.replay?.manifest_key?.match(/^data\/cb-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);
  if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from immutable output');
  let lastRendered=null;
  function refresh(){const view=api.render(packet,Date.now(),Boolean(requested),$('horizon').value),signature=JSON.stringify(view);
   if(signature===lastRendered)return;lastRendered=signature;
   for(const name of ['hero','cbs','carry','edollar','fails','evidence'])$(name).innerHTML=view[name];
   for(const name of ['note','ts'])$(name).textContent=view[name];}
  $('horizon').addEventListener('change',refresh);refresh();setInterval(refresh,60000);
 }catch(error){$('hero').textContent='Research unavailable: '+error.message;$('note').textContent='Use Load latest research to leave an invalid or unavailable pinned snapshot. No investment direction is inferred from unavailable inputs.';}
})();
