(async function(){
 'use strict';const api=window.FailsResearch,$=id=>document.getElementById(id);
 try{
  const requested=new URL(location.href).searchParams.get('run');let id=requested,pointer=null;
  if(!id){const r=await fetch('/data/settlement-fails.json',{cache:'no-store'});if(!r.ok)throw Error('Current research unavailable');pointer=await r.json();id=pointer.replay?.manifest_key?.match(/^data\/fails-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from retained output');
  const options=api.scopes(packet);let selected=packet.treasury,page=0,last='';
  function render(){
   const at=Date.now(),pinned=Boolean(requested),view=api.overview(packet,at,pinned),list=api.history(selected,page),range=Number($('range').value);page=list.page;
   const signature=JSON.stringify({view,scope:selected.scope_id,current:api.current(selected,at,pinned),range,page});if(signature===last)return;last=signature;
   $('hero').innerHTML=view.hero;$('asof').textContent=view.asof;$('scopes').innerHTML=view.scopes;$('evidence').innerHTML=view.evidence;
   $('detail').innerHTML=api.detail(packet,selected,at,pinned,range);$('definitions').innerHTML=api.definitions(packet);
   $('history').innerHTML=list.html;$('page-count').textContent=`${selected.label} · ${list.count} reported dates · page ${page+1} of ${list.pages}`;
   $('prev').disabled=page===0;$('next').disabled=page+1>=list.pages;
  }
  $('scopes').addEventListener('click',event=>{const button=event.target.closest('button[data-scope]');if(!button)return;selected=options.find(row=>row.scope_id===button.dataset.scope);page=0;render();$('detail').focus();});
  $('range').addEventListener('change',render);$('prev').addEventListener('click',()=>{page--;render();});$('next').addEventListener('click',()=>{page++;render();});
  render();setInterval(render,60000);
 }catch(error){$('hero').textContent='Settlement research unavailable: '+error.message+'. No portfolio signal is available.';}
})();
