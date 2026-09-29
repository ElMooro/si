(async function(){
 'use strict';const api=window.CBResearch,$=id=>document.getElementById(id);
 try{
  const choices=new URL(location.href).searchParams.getAll('run');
  if(choices.length>1)throw Error('One snapshot identifier is required');
  const pinned=choices.length===1;
  const packet=pinned?await api.loadSnapshot(choices[0]):await api.loadCurrent();
  let lastRendered=null;
  function refresh(){const view=api.render(packet,Date.now(),pinned,$('horizon').value),signature=JSON.stringify(view);
   if(signature===lastRendered)return;lastRendered=signature;
   for(const name of ['hero','cbs','carry','edollar','fails','evidence'])$(name).innerHTML=view[name];
   for(const name of ['note','ts'])$(name).textContent=view[name];}
  $('horizon').addEventListener('change',refresh);refresh();setInterval(refresh,60000);
 }catch(error){$('hero').textContent='Research unavailable: '+error.message;$('note').textContent='Use Load latest research to leave an invalid or unavailable pinned snapshot. No investment direction is inferred from unavailable inputs.';}
})();
