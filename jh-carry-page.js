(async function(){
 'use strict';const api=window.CarryResearch,$=id=>document.getElementById(id);
 try{
  const params=new URL(location.href).searchParams,requested=params.get('run');let id=requested,pointer=null;
  if(!id){const r=await fetch('/data/carry-surface.json',{cache:'no-store'});if(!r.ok)throw Error('Current carry research unavailable');pointer=await r.json();id=pointer.replay?.manifest_key?.match(/^data\/carry-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];}
  const packet=await api.loadSnapshot(id);if(pointer&&(pointer.generated_at!==packet.generated_at||pointer.replay.output_sha256!==packet.replay.output_sha256))throw Error('Current pointer differs from retained output');
  const choices=api.choices(packet),cache=new Map();$('selection').innerHTML=choices.map(v=>`<option value="${api.esc(v.id)}">${api.esc(v.name||v.id)} · ${api.esc(v.id)}</option>`).join('');
  let selected=choices.some(v=>v.id===params.get('item'))?params.get('item'):(packet.equities.HDV?'FMP:HDV':choices[0]?.id),kind='history',page=0,boardPage=0,history=null,request=0,last='',scenario=null;
  function render(force=false){
   const at=Date.now(),pinned=Boolean(requested),view=api.summary(packet,$('asset-class').value,$('search').value,boardPage,at,pinned),range=Number($('range').value);
   const signature=JSON.stringify({view,selected,kind,page,range,status:api.status(api.selected(packet,selected),at,pinned)});if(!force&&signature===last)return;last=signature;boardPage=view.page;
   $('board').innerHTML=view.board;$('evidence').innerHTML=view.evidence;$('state').textContent=view.state;$('asof').textContent=view.asof;
   $('board-count').textContent=`${view.count} instruments · page ${boardPage+1} of ${view.pages}`;$('board-prev').disabled=boardPage===0;$('board-next').disabled=boardPage+1>=view.pages;
   $('detail').innerHTML=api.detail(packet,selected,at,pinned);$('permanent').href='/carry.html?run='+id+'&item='+encodeURIComponent(selected);
   $('chart').innerHTML=api.chart(history,range);const h=api.history(history,page);page=h.page;$('history').innerHTML=h.html;
   $('page-count').textContent=`${h.count} retained records · page ${page+1} of ${h.pages}`;$('prev').disabled=page===0;$('next').disabled=page+1>=h.pages;
  }
  async function select(value,focus=true,keepKind=false){
   if(!choices.some(v=>v.id===value))return;const ticket=++request;selected=value;$('selection').value=value;page=0;history=null;
   const row=api.selected(packet,value),kinds=api.histories(row);if(!keepKind||!kinds.includes(kind))kind=kinds[0]||'history';
   $('history-kind').innerHTML=kinds.map(k=>`<option value="${api.esc(k)}">${api.words(k)}</option>`).join('');$('history-kind').value=kind;render(true);$('history').textContent='Verifying retained history…';
   const key=value+':'+kind;
   try{if(!cache.has(key))cache.set(key,await api.loadHistory({...row,history:row[kind]}));if(ticket!==request)return;history=cache.get(key);render(true);if(focus)$('detail').focus();}
   catch(error){if(ticket===request){$('history').textContent='History unavailable: '+error.message;$('chart').textContent='History verification failed.';}}
  }
  $('selection').addEventListener('change',()=>select($('selection').value));$('history-kind').addEventListener('change',()=>{kind=$('history-kind').value;select(selected,false,true);});$('range').addEventListener('change',()=>render(true));
  for(const name of ['board','detail'])$(name).addEventListener('click',event=>{const b=event.target.closest('button[data-select]');if(b)select(b.dataset.select);});
  $('asset-class').addEventListener('change',()=>{boardPage=0;render(true);});$('search').addEventListener('input',()=>{boardPage=0;render(true);});
  $('board-prev').addEventListener('click',()=>{boardPage--;render(true);});$('board-next').addEventListener('click',()=>{boardPage++;render(true);});
  $('prev').addEventListener('click',()=>{page--;render(true);});$('next').addEventListener('click',()=>{page++;render(true);});
  function invalidate(){scenario=null;$('download-scenario').disabled=true;$('scenario-result').textContent='Assumptions changed. Calculate to update the result.';}
  function scenarioForm(){const type=$('scenario-kind').value;invalidate();$('scenario-limit').textContent=api.scenarioLimits[type];$('scenario-form').innerHTML=api.scenarioFields[type].map(([k,label,min,max,step])=>`<label for="s-${k}">${api.esc(label)}${k.toLowerCase().includes('basis')?`<select id="s-${k}" required><option value="">Choose convention</option><option value="360">Actual / 360</option><option value="365">Actual / 365</option></select>`:`<input id="s-${k}" type="number" min="${min}" max="${max}" step="${step||'any'}" required>`}</label>`).join('')+'<button type="submit">Calculate entered scenario</button>';}
  $('scenario-kind').addEventListener('change',scenarioForm);$('scenario-form').addEventListener('input',invalidate);$('scenario-form').addEventListener('change',invalidate);
  $('scenario-form').addEventListener('submit',event=>{
   event.preventDefault();try{const type=$('scenario-kind').value,inputs={};for(const [k] of api.scenarioFields[type]){const v=$('s-'+k).value;if(!v.trim())throw Error('Enter every assumption');inputs[k]=Number(v);}
    const r=api.scenario(type,inputs);scenario={...r,research_snapshot:packet.replay,calculated_at:new Date().toISOString()};
    $('scenario-result').innerHTML=api.table(['Entered scenario component',r.currency],r.components.map(([k,v])=>[api.esc(k),api.number(v)]).concat([['Net modeled P&L',api.number(r.net_pnl)]]))+`<p>Modeled P&amp;L / initial value: ${api.number(r.return_on_initial_value_pct)}%. ${r.break_even_end_price_or_fx===null?'':`Break-even terminal ${type==='fx'?'FX':'price'}: ${api.number(r.break_even_end_price_or_fx)}.`} Negative P&amp;L means a modeled loss.</p><p>${api.esc(r.formula)}</p>`;$('download-scenario').disabled=false;
   }catch(error){invalidate();$('scenario-result').textContent=error.message;}
  });
  $('download-scenario').addEventListener('click',()=>{if(!scenario)return;const url=URL.createObjectURL(new Blob([JSON.stringify(scenario,null,2)],{type:'application/json'})),a=document.createElement('a');a.href=url;a.download='carry-entered-scenario.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
  scenarioForm();await select(selected,false);setInterval(()=>render(),60000);
 }catch(error){$('state').textContent='Carry research unavailable: '+error.message+'. No portfolio signal is available.';}
})();
