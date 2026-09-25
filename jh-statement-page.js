(function () {
  'use strict';
  const api=window.JHStatementResearch,desk=document.querySelector('[data-statement-desk]');
  if (!api || !desk) return;
  const el=name=>desk.querySelector('[data-st-'+name+']'),params=new URLSearchParams(location.search);
  let pinned=params.has('run')?params.get('run'):null,requestedRecord=params.get('record'),state=null,selected=null,point=null,assumption=null;
  let loadToken=0,selectToken=0;
  const reasons={complete_aligned_statements:'Three statements aligned',partial_aligned_statements:'Partial matching statement set',
    repeated_statement_endpoint:'Repeated statement endpoint; affected calculations withheld',
    required_statement_record_missing:'Required matching statement unavailable',required_provider_field_missing:'Required provider field unavailable',
    nonpositive_denominator:'Denominator is zero or negative',reviewed_statement_records_required:'No unique qualified statement is available',
    reported_issuer_identity_invalid:'Provider issuer identity is invalid',statement_identity_missing:'Provider statement identity is incomplete',
    requested_and_reported_symbol_mismatch:'Requested and reported ticker differ',requested_and_reported_period_mismatch:'Requested and reported period differ',
    reported_date_after_source_acquisition:'Reported date is after acquisition',filing_precedes_period_end:'Filing or acceptance date precedes period end',
    current_issuer_identity_not_corroborated:'Issuer identity cannot be corroborated against the captured SEC index'};
  const explain=value=>reasons[value]||String(value||'Unavailable').replace(/_/g,' ');
  const number=value=>value===null||value===undefined?'Unavailable':String(value).replace(/(\.\d*?)0+$/,'$1').replace(/\.$/,'').replace(/^(-?\d+)(\.\d+)?$/,(_,whole,fraction)=>whole.replace(/\B(?=(\d{3})+(?!\d))/g,',')+(fraction||''));
  function node(tag,text){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;}
  function paragraph(host,text){host.append(node('p',text));}
  function link(host,key,label){const a=node('a',label);a.href='/'+key;a.target='_blank';a.rel='noopener';host.append(a);}
  function table(host,headers,rows){
    host.replaceChildren();const wrap=node('div'),t=node('table'),head=node('thead'),h=node('tr'),body=node('tbody');wrap.className='st-scroll';
    headers.forEach(text=>{const th=node('th',text);th.scope='col';h.append(th);});head.append(h);t.append(head);
    rows.forEach(values=>{const tr=node('tr');values.forEach(value=>{const td=node('td');if(value && typeof value==='object')td.append(value);else td.textContent=String(value??'Unavailable');tr.append(td);});body.append(tr);});
    t.append(body);wrap.append(t);host.append(wrap);
  }
  function clearPoint(){
    point=null;assumption=null;['measurements','inputs','identity','identities','selection-status','scenario'].forEach(k=>el(k).replaceChildren());
    el('export').disabled=true;el('calculate').disabled=true;
  }
  function resetSelection(){clearPoint();selected=null;['choice','history','audit'].forEach(k=>el(k).replaceChildren());el('choice').disabled=true;}
  function permalink(){
    if(!state)return;const url=new URL(location.href);url.searchParams.set('run',state.runId);
    if(selected)url.searchParams.set('symbol',selected.summary.symbol);else url.searchParams.delete('symbol');
    url.searchParams.set('period',el('period').value);
    if(point)url.searchParams.set('record',point.record_id);else url.searchParams.delete('record');
    history.replaceState(null,'',url);
  }
  function inputEvidence(name,focus){
    const host=el('inputs');host.replaceChildren();const metric=point?.measurements?.metrics[name];if(!metric)return;
    host.append(node('h3',metric.definition));paragraph(host,(metric.value===null?explain(metric.status):number(metric.value)+' '+metric.unit)+' · no annualization · half-even rounding to twelve decimal places.');
    metric.inputs.forEach(input=>{
      const coord=point.source_rows.find(c=>c.source_id===input.source_id&&c.source_row===input.source_row&&c.endpoint===input.endpoint);
      const origin=coord?state.packet.sources[coord.capture_id]:null;
      paragraph(host,input.endpoint+' / '+input.field+' = '+(input.reported_value??'Unavailable'));
      if(origin){
        paragraph(host,'Original JSON array index '+input.source_row+' (zero-based) · acquired '+origin.received_at+'.');
        paragraph(host,'Original SHA-256: '+input.source_id+' · '+number(origin.original_bytes)+' bytes.');
        paragraph(host,'Request: '+origin.request.url);
      }else paragraph(host,'No unique matching source row is available for this input.');
    });
    link(host,selected.artifact.key,'Complete recorded issuer history');
    if(focus){host.focus();host.scrollIntoView({block:'nearest',behavior:'smooth'});}
  }
  function reportedIdentity(row){return row.identity||row.identity_evidence?.[0]?.reported_identity||{};}
  function identityEvidence(row){
    const host=el('identities');host.replaceChildren();
    paragraph(host,'SEC current ticker index acquired '+state.packet.identity_index.received_at+'. This corroborates a current ticker/CIK pair, not historical security continuity or filing values.');
    link(host,state.packet.identity_index.original.key,'Complete captured SEC ticker index');
    row.identity_evidence.forEach(item=>{
      host.append(node('h3',item.endpoint+' · original row '+item.source_row));
      paragraph(host,explain(item.current_identity_status)+'. SEC current CIK: '+(item.current_sec_ciks.join(', ')||'Unavailable')+'.');
      item.clock_issues.forEach(issue=>paragraph(host,explain(issue)));
      const values=node('div');table(values,['Provider field','Original value','Source type'],Object.entries(item.reported_identity).map(([field,value])=>[field,item.missing_fields.includes(field)?'Field missing':value===null?'Explicit null':typeof value==='object'?JSON.stringify(value):String(value),item.reported_identity_types[field]]));host.append(values);
      paragraph(host,'Original SHA-256 '+item.source_id+' · capture '+item.capture_id+'.');
    });
  }
  function choose(id){
    clearPoint();if(!selected)return;const row=selected.shard.records.find(r=>r.record_id===id&&r.request_period===el('period').value);
    if(!row){el('selection-status').textContent='Choose an exact period and filing vintage; no record was substituted.';el('choice').value='';return;}
    point=row;requestedRecord=id;el('choice').value=id;
    const ident=row.identity;identityEvidence(row);
    el('identity').textContent=ident?selected.summary.symbol+' · reported CIK '+ident.cik+' · '+ident.reportedCurrency+' · period ended '+ident.date+' · '+ident.fiscalYear+' '+ident.period+' · filing '+ident.filingDate+' · provider accepted '+ident.acceptedDate+' (timezone unverified).':selected.summary.symbol+' · source identity is unqualified.';
    el('selection-status').textContent=explain(row.status)+'. '+row.source_rows.length+' original statement rows are retained in this record.';
    el('export').disabled=false;el('calculate').disabled=false;
    if(row.measurements){
      table(el('measurements'),['Measurement / inspect inputs','Recorded value','Unit','Interpretation'],Object.keys(api.FORMULAS).map(name=>{
        const m=row.measurements.metrics[name];
        const b=node('button',m.definition);b.type='button';b.addEventListener('click',()=>inputEvidence(name,true));
        return[b,number(m.value),m.unit,m.value===null?explain(m.status):'Descriptive calculation'];
      }));
      inputEvidence('gross_margin_pct',false);
    }else{
      paragraph(el('measurements'),'Calculations are unavailable because the original row identity or current issuer correspondence is unqualified. The source coordinate is retained.');
      row.source_rows.forEach(c=>paragraph(el('inputs'),c.endpoint+' · original JSON array index '+c.source_row+' · SHA-256 '+c.source_id));
    }
    permalink();
  }
  function historyTable(){
    const records=selected.shard.records.filter(v=>v.request_period===el('period').value);
    table(el('history'),['Reported period end','Fiscal label','Currency / CIK','Provider filing date','Provider accepted (timezone unverified)','Matching status','Record'],records.map(row=>{
      const b=node('button','Inspect');b.type='button';b.addEventListener('click',()=>choose(row.record_id));
      const i=reportedIdentity(row);return[i.date,i.fiscalYear+' '+i.period,i.reportedCurrency+' / '+i.cik,i.filingDate,i.acceptedDate,explain(row.status),b];
    }));
  }
  function options(){
    const records=selected.shard.records.filter(v=>v.request_period===el('period').value);el('choice').replaceChildren();
    const blank=node('option','Choose a period and filing vintage');blank.value='';el('choice').append(blank);
    [...records].reverse().forEach(row=>{const i=reportedIdentity(row);const option=node('option',i?i.date+' · '+i.fiscalYear+' '+i.period+' · '+i.reportedCurrency+' / CIK '+i.cik+' · accepted '+i.acceptedDate+' · '+explain(row.status):'Unqualified source identity · '+explain(row.status));option.value=row.record_id;el('choice').append(option);});
    el('choice').disabled=!records.length;historyTable();
    if(requestedRecord){choose(requestedRecord);return;}
    const dated=records.filter(r=>r.identity),last=dated.at(-1)?.identity.date,latest=dated.filter(r=>r.identity.date===last);
    if(latest.length===1)choose(latest[0].record_id);
    else el('selection-status').textContent=records.length?(dated.length?'Several filing identities share the latest period. Choose the exact record.':'Statement identities are unqualified. Choose a record to inspect the original reported metadata.'):'No '+el('period').value+' statements are reported in this source capture.';
  }
  async function select(symbol){
    const token=++selectToken;resetSelection();el('status').textContent='Verifying the complete issuer history and accounting arithmetic…';
    try{
      const result=await api.record(fetch,state,symbol);if(token!==selectToken)return;selected=result;
      const host=el('audit');link(host,state.reference,'Recorded run');link(host,state.run.input.key,'Input inventory');link(host,state.run.output.key,'Recorded output');link(host,result.artifact.key,'Issuer evidence');
      paragraph(host,'Output SHA-256: '+state.run.output_sha256+'.');Object.entries(state.run.compilers).forEach(([name,ref])=>link(host,ref.key,name+'.py'));
      options();permalink();el('status').textContent='Recorded output, current SEC ticker correspondence, issuer history and displayed arithmetic verified. Descriptive research; no qualified investment signal.';
    }catch(error){if(token===selectToken){resetSelection();el('status').textContent=error.message;}}
  }
  async function load(){
    const token=++loadToken;++selectToken;resetSelection();state=null;el('load').disabled=true;el('stats').replaceChildren();el('coverage').replaceChildren();el('publication').replaceChildren();
    el('status').textContent='Verifying the recorded accounting publication…';
    try{
      const result=await api.load(fetch,pinned);if(token!==loadToken)return;state=result;pinned=result.runId;
      const p=state.packet;el('publication').textContent='Capture completed '+p.generated_at+'. Sources acquired '+p.source_acquisition_started_at+' through '+p.source_acquisition_completed_at+'. This was not a simultaneous snapshot.';
      [['Names in retained universe',p.reported_names],['Original statement rows',p.provider_rows],['Aligned three-statement records',p.complete_aligned_records],['Qualified investment votes',0]].forEach(([label,value])=>{const d=node('div');d.append(node('strong',number(value)),node('span',label));el('stats').append(d);});
      paragraph(el('coverage'),number(p.provider_responses)+' responses · '+number(p.empty_responses)+' empty responses · '+number(p.rows_with_identity_problems)+' rows with identity problems · '+number(p.records_with_repeated_endpoints)+' records with repeated endpoints.');
      Object.entries(p.record_statuses).forEach(([key,value])=>paragraph(el('coverage'),explain(key)+': '+number(value)));
      Object.entries(p.multiple_labels_per_issuer).forEach(([key,labels])=>paragraph(el('coverage'),key+' appears for '+labels.join(', ')+' in the captured current SEC ticker index; historical continuity is unverified.'));
      document.getElementById('statement-labels').replaceChildren(...p.issuers.map(item=>{const option=node('option');option.value=item.symbol;return option;}));
      el('load').disabled=false;permalink();
      const symbol=el('query').value.trim().toUpperCase();
      if(symbol)await select(symbol);else el('status').textContent='Recorded publication verified. Choose a name to inspect its statement history.';
    }catch(error){if(token===loadToken){state=null;resetSelection();el('status').textContent=error.message;}}
  }
  el('query').value=params.get('symbol')||params.get('ticker')||'';
  el('period').value=params.get('period')==='quarter'?'quarter':'annual';
  el('query').addEventListener('input',()=>{++selectToken;requestedRecord=null;resetSelection();el('selection-status').textContent='Inspect the entered ticker to load its recorded statements.';});
  el('search').addEventListener('submit',event=>{event.preventDefault();if(state){requestedRecord=null;select(el('query').value.trim().toUpperCase());}});
  el('period').addEventListener('change',()=>{requestedRecord=null;clearPoint();if(selected)options();});
  el('choice').addEventListener('change',()=>choose(el('choice').value));
  el('refresh').addEventListener('click',load);
  el('form').addEventListener('submit',event=>{event.preventDefault();if(!point)return;try{assumption=api.scenario(Object.fromEntries(new FormData(event.target).entries()));el('scenario').textContent='Assumed price-change P&L after entered costs: '+assumption.pnl+' '+assumption.currency+'. Signed reference exposure: '+assumption.signed_notional+' '+assumption.currency+'. User assumptions; not an accounting forecast.';}catch(error){assumption=null;el('scenario').textContent=error.message;}});
  el('export').addEventListener('click',()=>{
    if(!point||!selected)return;const sources=Object.fromEntries([...new Set(selected.shard.records.flatMap(r=>r.source_rows.map(c=>c.capture_id)))].map(id=>[id,state.packet.sources[id]]));
    const body={contract:'statement-research-selection.v2',run:state.reference,output_sha256:state.run.output_sha256,
      identity_index:state.packet.identity_index,historical_security_continuity_verified:false,issuer:selected.summary,selected_record:point,complete_issuer_history:selected.shard,sources,assumed_exposure:assumption,
      original_provider_responses_included:false,original_sec_filings_verified:false,forecast_qualified:false};
    const url=URL.createObjectURL(new Blob([JSON.stringify(body,null,2)],{type:'application/json'})),a=node('a');a.href=url;a.download=selected.summary.symbol+'-statement-evidence.json';document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
  });
  load();
})();
