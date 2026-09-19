(async function(){
 'use strict';const api=window.HotMoneyResearch,$=id=>document.getElementById(id);
 try{
  const requested=new URL(location.href).searchParams.get('run');let id=requested,pointer=null;
  if(!id){const response=await fetch('/data/hot-money.json',{cache:'no-store'});if(!response.ok)throw Error('Current exchange research unavailable');pointer=await response.json();
   if(pointer.contract!==api.CONTRACT)throw Error('Original exchange research has not been published yet');
   id=pointer.replay?.manifest_key?.match(/^data\/hot-money-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);
  if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from immutable output');
  let last=null;
  function refresh(){const view=api.render(packet,Date.now(),Boolean(requested),$('board').value),signature=JSON.stringify(view);
   if(signature===last)return;last=signature;
   for(const name of ['hero','countries','combined','history','fails','evidence'])$(name).innerHTML=view[name];$('sub').textContent=view.sub;}
  $('board').addEventListener('change',refresh);refresh();setInterval(refresh,60000);
 }catch(error){$('hero').textContent='Exchange research unavailable: '+error.message;$('sub').textContent='Use Load latest research to leave an unavailable snapshot. No investment direction is inferred.';}
})();
