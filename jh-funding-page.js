(async function(){
 'use strict';const api=window.FundingResearch,$=id=>document.getElementById(id);
 try{
  const params=new URL(location.href).searchParams,requested=params.get('run');let id=requested,pointer=null;
  if(!id){const r=await fetch('/data/eurodollar-plumbing.json',{cache:'no-store'});if(!r.ok)throw Error('Current funding research unavailable');pointer=await r.json();id=pointer.replay?.manifest_key?.match(/^data\/funding-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from retained output');
  const choices=api.selections(packet),cache=new Map();$('selection').innerHTML=choices.map(v=>`<option value="${api.esc(v.id)}">${api.esc(v.label)}</option>`).join('');
  let selected=choices.some(v=>v.id===params.get('item'))?params.get('item'):'sofr_iorb',page=0,history=null,request=0,last='';$('selection').value=selected;
  function render(force=false){
   const at=Date.now(),pinned=Boolean(requested),view=api.summary(packet,at,pinned),range=Number($('range').value);
   const signature=JSON.stringify({view,selected,page,range,status:api.status(api.selected(packet,selected),at,pinned)});if(!force&&signature===last)return;last=signature;
   $('hero').innerHTML=view.hero;$('asof').textContent=view.asof;$('coverage').innerHTML=view.coverage;$('board').innerHTML=view.board;
   $('detail').innerHTML=api.detail(packet,selected,at,pinned);$('evidence').innerHTML=view.evidence;$('gaps').innerHTML=view.gaps;
   $('fails').innerHTML=api.fails(packet,at,pinned);$('vintages').innerHTML=api.vintages(packet);$('retention').innerHTML=api.retention(packet);
   $('permanent').href+='&item='+encodeURIComponent(selected);$('chart').innerHTML=history?api.chart(history,range):'<p>Select a measurement to inspect its retained history.</p>';
   const hist=api.history(history,page);page=hist.page;$('history').innerHTML=hist.html;$('page-count').textContent=`${hist.count} reported dates · page ${page+1} of ${hist.pages}`;$('prev').disabled=page===0;$('next').disabled=page+1>=hist.pages;
  }
  async function select(value,focus=true){
   if(!choices.some(v=>v.id===value))return;const ticket=++request;selected=value;$('selection').value=value;page=0;history=null;render(true);$('history').textContent='Verifying retained history…';
   try{if(!cache.has(value))cache.set(value,await api.loadHistory(api.selected(packet,value)));if(ticket!==request)return;history=cache.get(value);render(true);if(focus)$('detail').focus();}
   catch(error){if(ticket===request){$('history').textContent='History unavailable: '+error.message;$('chart').textContent='History verification failed.';}}
  }
  $('selection').addEventListener('change',()=>select($('selection').value));$('range').addEventListener('change',()=>render(true));
  for(const name of ['board','detail'])$(name).addEventListener('click',event=>{const b=event.target.closest('button[data-select]');if(b)select(b.dataset.select);});
  $('prev').addEventListener('click',()=>{page--;render(true);});$('next').addEventListener('click',()=>{page++;render(true);});
  $('scenario-form').addEventListener('submit',event=>{
   event.preventDefault();try{const fields=['notional','shock','days','basis'];if(fields.some(k=>$(k).value.trim()===''))throw Error('Enter all assumptions');
    const r=api.scenario(...fields.map(k=>Number($(k).value)));$('scenario-result').textContent=`Entered scenario: ${r.incremental_usd.toLocaleString('en-US',{style:'currency',currency:'USD'})} incremental simple funding cost. ${r.formula}. Positive means higher cost; negative means lower cost. This is not a market forecast or a position recommendation.`;
   }catch(error){$('scenario-result').textContent=error.message;}
  });
  await select(selected,false);setInterval(()=>render(),60000);
 }catch(error){$('hero').textContent='Funding research unavailable: '+error.message+'. No portfolio signal is available.';}
})();
