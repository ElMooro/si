(async function(){
 'use strict';const api=window.DealerResearch,$=id=>document.getElementById(id);
 try{
  const params=new URL(location.href).searchParams,requested=params.get('run');let id=requested,pointer=null;
  if(!id){const r=await fetch('/data/nyfed-primary-dealer.json',{cache:'no-store'});if(!r.ok)throw Error('Current dealer research unavailable');pointer=await r.json();id=pointer.replay?.manifest_key?.match(/^data\/dealer-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from retained output');
  const choices=api.selections(packet);$('selection').innerHTML=choices.map(v=>`<option value="${api.esc(v.id)}">${api.esc(v.label)}</option>`).join('');
  let selected=choices.some(v=>v.id===params.get('item'))?params.get('item'):'group:corp_bonds',page=0,last='';$('selection').value=selected;
  function render(){
   const at=Date.now(),pinned=Boolean(requested),view=api.overview(packet,at,pinned),hist=api.history(packet,selected,page),range=Number($('range').value);page=hist.page;
   const signature=JSON.stringify({view,selected,page,range,current:api.current(api.selected(packet,selected).quality,at,pinned)});if(signature===last)return;last=signature;
   $('hero').innerHTML=view.hero;$('asof').textContent=view.asof;$('coverage').innerHTML=view.coverage;$('review').innerHTML=view.review;
   $('detail').innerHTML=api.detail(packet,selected,at,pinned,range);$('history').innerHTML=hist.html;$('evidence').innerHTML=view.evidence;
   $('reconciliations').innerHTML=api.reconciliations(packet,at,pinned);$('permanent').href+='&item='+encodeURIComponent(selected);
   $('page-count').textContent=`${hist.count} reported dates · page ${page+1} of ${hist.pages}`;$('prev').disabled=page===0;$('next').disabled=page+1>=hist.pages;
  }
  function select(value){if(!choices.some(v=>v.id===value))return;selected=value;$('selection').value=value;page=0;render();$('detail').focus();}
  $('selection').addEventListener('change',()=>select($('selection').value));$('range').addEventListener('change',render);
  for(const name of ['review','detail'])$(name).addEventListener('click',event=>{const b=event.target.closest('button[data-select]');if(b)select(b.dataset.select);});
  $('prev').addEventListener('click',()=>{page--;render();});$('next').addEventListener('click',()=>{page++;render();});render();setInterval(render,60000);
 }catch(error){$('hero').textContent='Dealer research unavailable: '+error.message+'. No portfolio signal is available.';}
})();
