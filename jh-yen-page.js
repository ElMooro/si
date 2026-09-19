(async function(){
 'use strict';const api=window.YenResearch,$=id=>document.getElementById(id);
 try{
  const params=new URL(location.href).searchParams,requested=params.get('run');let id=requested,pointer=null;
  if(!id){const r=await fetch('/data/yen-carry.json',{cache:'no-store'});if(!r.ok)throw Error('Current yen research unavailable');pointer=await r.json();id=pointer.replay?.manifest_key?.match(/^data\/yen-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from retained output');
  const choices=api.choices(packet),cache=new Map();$('selection').innerHTML=choices.map(v=>`<option value="${api.esc(v.id)}">${api.esc(v.name||v.id)}</option>`).join('');
  let selected=choices.some(v=>v.id===params.get('item'))?params.get('item'):(packet.measurements.DEXJPUS?'DEXJPUS':choices[0]?.id),page=0,history=null,request=0,last='',scenario=null;
  function render(force=false){
   const at=Date.now(),pinned=Boolean(requested),view=api.summary(packet,at,pinned),range=Number($('range').value);
   const signature=JSON.stringify({view,selected,page,range,status:api.status(api.selected(packet,selected),at,pinned)});if(!force&&signature===last)return;last=signature;
   for(const name of ['board','comparisons','evidence'])$(name).innerHTML=view[name];$('state').textContent=view.state;$('asof').textContent=view.asof;
   $('positioning').innerHTML=api.positioning(packet,at,pinned);$('detail').innerHTML=api.detail(packet,selected,at,pinned);
   $('permanent').href='/yen-carry.html?run='+id+'&item='+encodeURIComponent(selected);
   $('chart').innerHTML=api.chart(history,range);const h=api.history(history,page);page=h.page;$('history').innerHTML=h.html;
   $('page-count').textContent=`${h.count} retained report dates · page ${page+1} of ${h.pages}`;$('prev').disabled=page===0;$('next').disabled=page+1>=h.pages;
  }
  async function select(value,focus=true){
   if(!choices.some(v=>v.id===value))return;const ticket=++request;selected=value;$('selection').value=value;page=0;history=null;render(true);$('history').textContent='Verifying retained history…';
   try{if(!cache.has(value))cache.set(value,await api.loadHistory(api.selected(packet,value)));if(ticket!==request)return;history=cache.get(value);render(true);if(focus)$('detail').focus();}
   catch(error){if(ticket===request){$('history').textContent='History unavailable: '+error.message;$('chart').textContent='History verification failed.';}}
  }
  $('selection').addEventListener('change',()=>select($('selection').value));$('range').addEventListener('change',()=>render(true));
  for(const name of ['board','comparisons','positioning','detail'])$(name).addEventListener('click',event=>{const b=event.target.closest('button[data-select]');if(b)select(b.dataset.select);});
  $('prev').addEventListener('click',()=>{page--;render(true);});$('next').addEventListener('click',()=>{page++;render(true);});
  $('scenario-form').addEventListener('input',()=>{scenario=null;$('download-scenario').disabled=true;$('scenario-result').textContent='Assumptions changed. Calculate to update the result.';});
  $('scenario-form').addEventListener('submit',event=>{
   event.preventDefault();try{const fields=['notional','start','end','usd-rate','jpy-rate','days','usd-basis','jpy-basis','fees'];if(fields.some(k=>$(k).value.trim()===''))throw Error('Enter every assumption');
    const r=api.scenario(...fields.map(k=>Number($(k).value)));scenario={...r,research_snapshot:packet.replay,calculated_at:new Date().toISOString()};
    $('scenario-result').innerHTML=api.table(['Entered scenario component','JPY'],[['FX on principal',api.number(r.fx_principal_pnl_jpy)],['USD interest converted at ending FX',api.number(r.usd_interest_converted_jpy)],['JPY funding interest subtracted',api.number(r.jpy_funding_interest)],['Entered fees subtracted',api.number(r.assumptions.fees_jpy)],['Net modeled P&L',api.number(r.net_pnl_jpy)]])+`<p>Break-even ending FX: ${api.number(r.break_even_jpy_per_usd)} JPY per USD. Negative net P&amp;L means a modeled loss. ${api.esc(r.formula)}</p>`;$('download-scenario').disabled=false;
   }catch(error){scenario=null;$('download-scenario').disabled=true;$('scenario-result').textContent=error.message;}
  });
  $('download-scenario').addEventListener('click',()=>{if(!scenario)return;const url=URL.createObjectURL(new Blob([JSON.stringify(scenario,null,2)],{type:'application/json'})),a=document.createElement('a');a.href=url;a.download='yen-entered-scenario.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
  await select(selected,false);setInterval(()=>render(),60000);
 }catch(error){$('state').textContent='Yen research unavailable: '+error.message+'. No portfolio signal is available.';}
})();
