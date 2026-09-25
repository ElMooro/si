(function(){
  'use strict';
  const api=window.JHShortVolumeResearch,desk=document.querySelector('[data-short-volume]');if(!desk||!api)return;
  const el=name=>desk.querySelector('[data-sv-'+name+']');
  const query=new URLSearchParams(location.search);let pinned=query.has('run')?query.get('run'):null;
  let state=null,selected=null,scenario=null,dateIndex=60,page=0,loadToken=0,selectToken=0;
  const fmt=value=>value===null||value===undefined?'Unavailable':String(value);
  function node(tag,text){const item=document.createElement(tag);if(text!==undefined)item.textContent=text;return item;}
  function clearScenario(){scenario=null;el('scenario').replaceChildren();}
  function link(target,key,text){const a=node('a',text);a.href='/'+key;a.target='_blank';a.rel='noopener';target.append(a);}
  function table(target,columns,rows){
    const host=el(target);host.replaceChildren();const wrap=node('div');wrap.className='or-table-wrap';wrap.style.overflowX='auto';
    const t=node('table'),head=node('thead'),h=node('tr'),body=node('tbody');
    columns.forEach(c=>h.append(node('th',c[0])));head.append(h);t.append(head);
    rows.forEach(row=>{const tr=node('tr');columns.forEach(c=>tr.append(node('td',fmt(c[1](row)))));body.append(tr);});t.append(body);wrap.append(t);host.append(wrap);
  }
  function resetSelection(){
    selected=null;clearScenario();['export','calculate','date','prev','next'].forEach(k=>el(k).disabled=true);
    ['identity','observation-status','observation','source','windows','window-details','history','chart','audit','page','date'].forEach(k=>el(k).replaceChildren());
  }
  function observation(){
    clearScenario();if(!state||!selected)return;const o=api.observation(state,selected,dateIndex),p=o.point;
    el('observation-status').textContent=selected.row.symbol+' · '+o.date+' · '+(o.missing_reason||'Reported source row available');
    table('observation',[['Short volume, shares',()=>p?.[2]],['Short-exempt subset, shares',()=>p?.[3]],['Reported total, shares',()=>p?.[4]],['Short volume, %',()=>o.short_volume_pct],['Facilities',()=>p?.[5]],['Original line',()=>p?.[1]]],[o]);
    el('source').replaceChildren(node('p','Requested '+o.source.source_requested_at+' · received '+o.source.source_received_at));
    const a=node('a','Official source file');a.href=o.source.url;a.target='_blank';a.rel='noopener';el('source').append(a);
    const hash=node('p','Original SHA-256 '+o.source.original.sha256);hash.style.overflowWrap='anywhere';el('source').append(hash);
  }
  function historyTable(){
    const rows=state.packet.dates.map((_,i)=>api.observation(state,selected,i)).reverse().slice(page*20,page*20+20);
    table('history',[['Date',r=>r.date],['Short volume, shares',r=>r.point?.[2]],['Reported total, shares',r=>r.point?.[4]],['Short volume, %',r=>r.short_volume_pct],['Source line',r=>r.point?.[1]],['Missingness',r=>r.missing_reason||'Reported']],rows);
    el('page').textContent='Published files '+(page*20+1)+'–'+Math.min(page*20+20,61)+' of 61';
    el('prev').disabled=page===0;el('next').disabled=page*20+20>=61;
  }
  function chart(){
    const host=el('chart');host.replaceChildren();const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');
    svg.setAttribute('viewBox','0 0 720 220');svg.setAttribute('role','img');svg.setAttribute('aria-label','Daily short volume as a percentage of reported volume. Missing observations are gaps.');svg.style.width='100%';svg.style.maxHeight='260px';
    const start=Date.parse(state.packet.dates[0]),end=Date.parse(state.packet.dates[60]);
    function shape(tag,attributes,text){const item=document.createElementNS(ns,tag);Object.entries(attributes).forEach(([k,v])=>item.setAttribute(k,String(v)));if(text!==undefined)item.textContent=text;svg.append(item);return item;}
    for(const value of [0,50,100]){const y=180-value*1.5;shape('line',{x1:55,y1:y,x2:695,y2:y,stroke:'#344355'});shape('text',{x:5,y:y+5,fill:'#b8c6d9','font-size':14},value+'%');}
    let segment=[];
    function flush(){if(segment.length>1)shape('polyline',{points:segment.join(' '),fill:'none',stroke:'#67dbe6','stroke-width':2});segment=[];}
    state.packet.dates.forEach((date,i)=>{const o=api.observation(state,selected,i);if(o.short_volume_pct===null){flush();return;}
      const x=55+(Date.parse(date)-start)/(end-start)*640,y=180-Number(o.short_volume_pct)*1.5;segment.push(x+','+y);
      const dot=shape('circle',{cx:x,cy:y,r:2.7,fill:'#67dbe6'}),title=document.createElementNS(ns,'title');title.textContent=date+' · '+o.short_volume_pct+'%';dot.append(title);
    });flush();shape('text',{x:55,y:210,fill:'#b8c6d9','font-size':14},state.packet.dates[0]);shape('text',{x:695,y:210,'text-anchor':'end',fill:'#b8c6d9','font-size':14},state.packet.dates[60]);host.append(svg);
  }
  function renderRecord(){
    const row=selected.row;dateIndex=60;page=0;
    el('identity').textContent=row.symbol+' · '+row.observations+' reported rows across 61 published files · identity basis: literal source symbol.';
    const choices=el('date');choices.replaceChildren();state.packet.dates.forEach((date,i)=>{const opt=node('option',date+(i===60?' · latest':''));opt.value=String(i);choices.append(opt);});choices.value='60';choices.disabled=false;
    observation();historyTable();chart();
    const windows=Object.values(row.windows).sort((a,b)=>a.prior_published_files-b.prior_published_files);
    table('windows',[['Prior files',r=>r.prior_published_files],['Reported / required',r=>r.observed_rows+' / '+r.prior_published_files],['Daily mean, %',r=>r.mean_daily_short_volume_pct],['Pooled ratio, %',r=>r.pooled_short_volume_pct],['Sample SD, pp',r=>r.sample_sd_below_display_precision?r.sample_sd_scientific:r.sample_sd_percentage_points],['Latest minus mean, pp',r=>r.latest_minus_mean_percentage_points],['Descriptive z-score',r=>r.descriptive_z_score]],windows);
    el('window-details').replaceChildren();windows.forEach(w=>{el('window-details').append(node('p',w.prior_published_files+' prior files: '+w.first_date+' through '+w.last_date+'. '+(w.missing_reason||'Complete daily-ratio baseline')+'. Missing: '+(w.missing_dates.join(', ')||'none')+'. Zero volume: '+(w.zero_volume_dates.join(', ')||'none')+'. Z-score unavailable reason: '+(w.z_score_missing_reason||'none')+'. Reported facility scope consistent: '+w.reported_facility_scope_consistent+'.'));});
    el('audit').replaceChildren();link(el('audit'),state.reference,'Recorded run');el('audit').append(document.createTextNode(' · '));link(el('audit'),selected.artifact.key,'Selected history bucket');
    const detail=node('details'),summary=node('summary','Inspect exact history and calculated windows'),pre=node('pre',JSON.stringify(row,null,2));pre.style.whiteSpace='pre-wrap';pre.style.overflowWrap='anywhere';detail.append(summary,pre);el('audit').append(detail);
    el('export').disabled=false;el('calculate').disabled=false;
  }
  function suggest(){const list=document.getElementById('sv-symbols');list.replaceChildren();if(!state)return;const term=el('symbol').value.trim().toUpperCase();state.packet.symbols.filter(r=>r.symbol.toUpperCase().startsWith(term)).slice(0,40).forEach(r=>{const option=node('option');option.value=r.symbol;list.append(option);});}
  async function selectSymbol(name){
    const token=++selectToken,snapshot=state;resetSelection();if(!snapshot)return;el('identity').textContent='Checking '+name+'…';
    try{const value=await api.record(window.fetch.bind(window),snapshot,name);if(token!==selectToken||state!==snapshot)return;selected=value;renderRecord();
      const url=new URL(location.href);url.searchParams.set('run',snapshot.runId);url.searchParams.set('symbol',name);history.replaceState(null,'',url);pinned=snapshot.runId;
    }catch(error){if(token!==selectToken||state!==snapshot)return;resetSelection();el('identity').textContent=error.message;}
  }
  async function load(){
    const token=++loadToken;++selectToken;state=null;resetSelection();el('load').disabled=true;el('status').textContent='Checking the recorded publication…';el('record').replaceChildren();el('coverage').replaceChildren();
    try{const result=await api.load(window.fetch.bind(window),pinned);if(token!==loadToken)return;state=result;el('load').disabled=false;
      const age=(Date.now()-Date.parse(state.packet.generated_at))/36e5;el('status').textContent='WAIT · Descriptive evidence; no qualified investment vote. '+(pinned?'Recorded snapshot.':age>48?'Publication is older than 48 hours; inspect the dated evidence.':'Recorded publication verified.');
      const p=state.packet,c=p.counts;el('record').textContent='Compiled '+p.generated_at+' · latest source date '+p.data_date+' · '+c.source_rows.toLocaleString()+' source rows / '+c.source_symbols.toLocaleString()+' symbols. '+c.symbols_with_complete_prior_daily_ratios['60'].toLocaleString()+' symbols have a complete prior 60-file daily-ratio baseline.';
      table('coverage',[['Source date',r=>r.observation_date],['Rows',r=>r.rows],['Requested',r=>r.source_requested_at],['Received',r=>r.source_received_at]],p.daily_sources.slice().reverse());
      const desired=new URLSearchParams(location.search).get('symbol')||el('symbol').value.trim(),item=p.symbols.find(r=>r.symbol.toUpperCase()===desired.toUpperCase());el('symbol').value=item?item.symbol:desired;suggest();await selectSymbol(el('symbol').value);
    }catch(error){if(token!==loadToken)return;el('status').textContent='Research unavailable: '+error.message;}
  }
  el('search').addEventListener('submit',event=>{event.preventDefault();if(!state)return;const typed=el('symbol').value.trim(),item=state.packet.symbols.find(r=>r.symbol.toUpperCase()===typed.toUpperCase());selectSymbol(item?item.symbol:typed);});
  el('symbol').addEventListener('input',()=>{++selectToken;resetSelection();suggest();});
  el('date').addEventListener('change',()=>{dateIndex=Number(el('date').value);observation();});
  el('prev').addEventListener('click',()=>{page=Math.max(0,page-1);historyTable();});el('next').addEventListener('click',()=>{page=Math.min(3,page+1);historyTable();});
  el('refresh').addEventListener('click',load);el('form').addEventListener('input',clearScenario);
  el('form').addEventListener('submit',event=>{event.preventDefault();clearScenario();if(!selected)return;try{scenario=api.scenario(Object.fromEntries(new FormData(el('form')).entries()));el('scenario').textContent=selected.row.symbol+' · assumed P&L '+scenario.pnl_usd+' USD · signed reference exposure '+scenario.signed_notional_usd+' USD. User assumptions only; no forecast.';}catch(error){el('scenario').textContent=error.message;}});
  el('export').addEventListener('click',()=>{if(!state||!selected)return;const data={contract:'short-volume-selected-evidence-export.v1',run:state.reference,output_sha256:state.run.output_sha256,artifact:selected.artifact,point_fields:state.packet.point_fields,dates:state.packet.dates,daily_sources:state.packet.daily_sources,record:selected.row,selected_observation:api.observation(state,selected,dateIndex),scenario};
    const blob=new Blob([JSON.stringify(data,null,2)],{type:'application/json'}),url=URL.createObjectURL(blob),a=node('a');a.href=url;a.download='short-volume-'+selected.row.symbol+'-'+state.runId.slice(0,12)+'.json';document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),60000);
  });load();
})();
