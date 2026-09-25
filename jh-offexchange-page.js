(function(){
  'use strict';
  const api=window.JHOffexchangeResearch,desk=document.querySelector('[data-offexchange]');if(!desk||!api)return;
  const el=name=>desk.querySelector('[data-oe-'+name+']');let state=null,selected=null,monthly=[],monthIndex=0,firmPage=0,scenario=null,loadToken=0,selectToken=0;
  const query=new URLSearchParams(location.search);let pinned=query.has('run')?query.get('run'):null;
  function node(tag,text){const item=document.createElement(tag);if(text!==undefined)item.textContent=text;return item;}
  const fmt=value=>value===null||value===undefined?'Unavailable':String(value);
  function clearScenario(){scenario=null;el('scenario').replaceChildren();}
  function table(target,columns,rows,empty){
    const host=el(target);host.replaceChildren();if(!rows.length){host.append(node('p',empty||'No matching records in this snapshot.'));return;}
    const wrap=node('div');wrap.className='or-table-wrap';wrap.style.overflowX='auto';const t=node('table'),head=node('thead'),h=node('tr');
    columns.forEach(column=>h.append(node('th',column[0])));head.append(h);t.append(head);const body=node('tbody');
    rows.forEach(row=>{const tr=node('tr');columns.forEach(column=>tr.append(node('td',fmt(column[1](row)))));body.append(tr);});t.append(body);wrap.append(t);host.append(wrap);
  }
  function link(target,key,text){const a=node('a',text);a.href='/'+key;a.target='_blank';a.rel='noopener';target.append(a);}
  function resetSelection(){
    selected=null;monthly=[];clearScenario();el('export').disabled=true;el('calculate').disabled=true;el('month').disabled=true;el('prev').disabled=true;el('next').disabled=true;
    ['identity','daily-date','daily','weekly','month-status','concentration','firms','page','audit'].forEach(key=>el(key).replaceChildren());el('month').replaceChildren();
  }
  function drawFirms(){
    const row=monthly[monthIndex];clearScenario();if(!row)return;
    el('month-status').textContent=row.reported_issue_name+' · '+row.month_start+' · '+row.tier+' · identity unresolved beyond the reported description';
    table('concentration',[['Reported non-ATS shares',r=>r.reported_non_ats_shares],['Named firms',r=>r.named_reporting_firms],['De-minimis share, %',r=>r.de_minimis_pct],['Reporting HHI lower bound',r=>r.reported_activity_hhi_lower_bound],['Reporting HHI upper bound',r=>r.reported_activity_hhi_upper_bound]],[row]);
    const firms=row.source_rows.slice().sort((a,b)=>a.reporting_firm.localeCompare(b.reporting_firm)),page=firms.slice(firmPage*25,firmPage*25+25);
    table('firms',[['Reporting firm',r=>r.reporting_firm],['Identity',r=>r.firm_identity_kind],['CRD',r=>r.firm_crd],['Shares',r=>r.shares],['Trades',r=>r.trades],['Published',r=>r.initialPublishedDate],['Updated',r=>r.lastUpdateDate],['Original row',r=>r.source_row]],page);
    el('page').textContent='Firms '+(firmPage*25+1)+'–'+Math.min((firmPage+1)*25,firms.length)+' of '+firms.length;el('prev').disabled=firmPage===0;el('next').disabled=(firmPage+1)*25>=firms.length;
  }
  function renderRecord(value){
    const row=value.row;el('identity').textContent=row.symbol+' · '+(row.reported_issue_names.join(' / ')||'Daily symbol-only reporting')+(row.multiple_reported_issue_names?' · Multiple reported issue names; records stay separate.':'');
    el('daily-date').textContent=row.daily.map(r=>'Observation '+r.observation_date+' · acquired '+r.source_received_at).join('; ');
    table('daily',[['Date',r=>r.observation_date],['Short volume, shares',r=>r.short_volume_shares],['Short-exempt subset, shares',r=>r.short_exempt_volume_shares],['Reported total, shares',r=>r.total_volume_shares],['Short share, %',r=>r.short_volume_pct],['Original line',r=>r.source_line]],row.daily);
    table('weekly',[['Reported issue',r=>r.reported_issue_name],['Week / tier',r=>r.week_start+' / '+r.tier],['ATS shares',r=>r.ats?.shares],['Non-ATS shares',r=>r.non_ats?.shares],['Combined shares',r=>r.reported_offexchange_shares],['ATS share of combined, %',r=>r.ats_pct_of_reported_offexchange],['Source published',r=>[r.ats?.initialPublishedDate,r.non_ats?.initialPublishedDate].filter(Boolean).join(' / ')],['Missingness',r=>r.missing_reason||'Both reported legs available']],row.weekly);
    monthly=row.monthly.slice().sort((a,b)=>b.month_start.localeCompare(a.month_start)||a.reported_issue_name.localeCompare(b.reported_issue_name));monthIndex=0;firmPage=0;
    const select=el('month');select.replaceChildren();monthly.forEach((r,i)=>{const option=node('option',r.month_start+' · '+r.reported_issue_name);option.value=String(i);select.append(option);});select.disabled=!monthly.length;
    if(monthly.length)drawFirms();else el('month-status').textContent='No monthly reporting-firm records for this symbol in the declared partitions.';
    el('audit').replaceChildren();link(el('audit'),state.reference,'Recorded run');el('audit').append(document.createTextNode(' · '));link(el('audit'),value.artifact.key,'Selected data bucket');
    const detail=node('details'),summary=node('summary','Inspect exact selected records and source locators'),pre=node('pre',JSON.stringify(row,null,2));pre.style.whiteSpace='pre-wrap';pre.style.overflowWrap='anywhere';detail.append(summary,pre);el('audit').append(detail);
    el('calculate').disabled=false;el('export').disabled=false;
  }
  function suggest(){
    const list=document.getElementById('oe-symbols');list.replaceChildren();if(!state)return;
    const term=el('symbol').value.trim().toUpperCase();state.packet.symbols.filter(r=>r.symbol.toUpperCase().startsWith(term)).slice(0,40).forEach(r=>{const option=node('option');option.value=r.symbol;option.label=r.reported_issue_names.join(' / ');list.append(option);});
  }
  async function selectSymbol(name){
    const token=++selectToken,snapshot=state;resetSelection();if(!snapshot)return;
    el('identity').textContent='Checking '+name+'…';
    try{const value=await api.record(window.fetch.bind(window),snapshot,name);if(token!==selectToken||state!==snapshot)return;selected=value;renderRecord(value);
      const url=new URL(location.href);url.searchParams.set('run',snapshot.runId);url.searchParams.set('symbol',name);history.replaceState(null,'',url);pinned=snapshot.runId;
    }catch(error){if(token!==selectToken||state!==snapshot)return;resetSelection();el('identity').textContent=error.message;}
  }
  async function load(){
    const token=++loadToken;++selectToken;state=null;resetSelection();el('load').disabled=true;el('status').textContent='Checking the recorded publication…';el('record').replaceChildren();el('coverage').replaceChildren();
    try{const result=await api.load(window.fetch.bind(window),pinned);if(token!==loadToken)return;state=result;el('load').disabled=false;
      const age=(Date.now()-Date.parse(state.packet.generated_at))/36e5;
      el('status').textContent='WAIT · Descriptive evidence; no qualified investment vote. '+(pinned?'Recorded snapshot.':age>48?'The latest publication is older than 48 hours; inspect its source dates.':'Published records verified.');
      el('record').textContent='Compiled '+state.packet.generated_at+' · '+state.packet.counts.source_symbols.toLocaleString()+' source symbols · '+state.packet.counts.daily_rows.toLocaleString()+' daily rows. Observation dates and acquisition times are shown separately.';
      table('coverage',[['Dataset',r=>r.dataset],['Category',r=>r.category],['Period / tier',r=>r.period_start+' / '+r.tier],['Rows',r=>r.rows],['First publication',r=>r.initial_publication_dates.join(', ')],['Acquired through',r=>r.last_received_at]],state.packet.coverage);
      const desired=new URLSearchParams(location.search).get('symbol')||el('symbol').value.trim();const item=state.packet.symbols.find(r=>r.symbol.toUpperCase()===desired.toUpperCase());el('symbol').value=item?item.symbol:desired;suggest();await selectSymbol(el('symbol').value);
    }catch(error){if(token!==loadToken)return;el('status').textContent='Research unavailable: '+error.message;}
  }
  el('search').addEventListener('submit',event=>{event.preventDefault();if(!state)return;const typed=el('symbol').value.trim(),item=state.packet.symbols.find(r=>r.symbol.toUpperCase()===typed.toUpperCase());selectSymbol(item?item.symbol:typed);});
  el('symbol').addEventListener('input',()=>{++selectToken;resetSelection();suggest();});
  el('month').addEventListener('change',()=>{monthIndex=Number(el('month').value);firmPage=0;drawFirms();});
  el('prev').addEventListener('click',()=>{firmPage=Math.max(0,firmPage-1);drawFirms();});el('next').addEventListener('click',()=>{firmPage++;drawFirms();});
  el('refresh').addEventListener('click',load);el('form').addEventListener('input',clearScenario);
  el('form').addEventListener('submit',event=>{event.preventDefault();clearScenario();if(!selected)return;try{const values=Object.fromEntries(new FormData(el('form')).entries());scenario=api.scenario(values);el('scenario').textContent=selected.row.symbol+' · assumed P&L '+scenario.pnl_usd+' USD · signed reference exposure '+scenario.signed_notional_usd+' USD. This is an assumption calculation, not a forecast.';}catch(error){el('scenario').textContent=error.message;}});
  el('export').addEventListener('click',()=>{if(!state||!selected)return;const exportData={contract:'offexchange-selected-evidence-export.v1',run:state.reference,output_sha256:state.run.output_sha256,artifact:selected.artifact,record:selected.row,scenario};const blob=new Blob([JSON.stringify(exportData,null,2)],{type:'application/json'}),url=URL.createObjectURL(blob),a=node('a');a.href=url;a.download='offexchange-'+selected.row.symbol+'-'+state.runId.slice(0,12)+'.json';document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),60000);});
  load();
})();
