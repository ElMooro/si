(async function(){
 'use strict';const api=window.ReversalResearch,$=id=>document.getElementById(id);
 try{
  const requested=new URL(location.href).searchParams.get('run');let id=requested,pointer=null;
  if(!id){const response=await fetch('/data/liquidity-reversal.json',{cache:'no-store'});if(!response.ok)throw Error('Current research unavailable');pointer=await response.json();id=pointer.replay?.manifest_key?.match(/^data\/reversal-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from retained output');
  let page=0,selected=packet.rows.find(r=>r.symbol==='FRED:WALCL'&&r.original_provider_verified),last='';
  function render(){
   const at=Date.now(),view=api.overview(packet,at,Boolean(requested)),list=api.inventory(packet,$('search').value,$('scope').value,page,at,Boolean(requested));page=list.page;
   const signature=JSON.stringify({view,list,selected:selected?.symbol,eligible:selected&&api.usable(selected,at,Boolean(requested))});if(signature===last)return;last=signature;
   for(const name of ['hero','families','fails','evidence'])$(name).innerHTML=view[name];$('asof').textContent=view.ts;
   $('rows').innerHTML=list.html;$('page-count').textContent=`${list.count} matching entries · page ${page+1} of ${list.pages}`;
   $('prev').disabled=page===0;$('next').disabled=page+1>=list.pages;
   $('detail').innerHTML=api.detail(selected,at,Boolean(requested));
  }
  $('search').addEventListener('input',()=>{page=0;render();});$('scope').addEventListener('change',()=>{page=0;render();});
  $('prev').addEventListener('click',()=>{page--;render();});$('next').addEventListener('click',()=>{page++;render();});
  $('rows').addEventListener('click',event=>{const button=event.target.closest('button[data-index]');if(!button)return;selected=packet.rows[Number(button.dataset.index)];render();$('detail').focus();});
  render();setInterval(render,60000);
 }catch(error){$('hero').textContent='Research unavailable: '+error.message+'. No policy or portfolio signal is available.';}
})();
