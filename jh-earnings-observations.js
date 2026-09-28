/* Display projection only. Preserve source populations; never restore legacy scores. */
(function(root){
 'use strict';
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
 function rows(packet){
  const modern=packet?.measurement_contract==='earnings-accounting-measurements.v1';
  const sourceKey=modern?'issuer_rows':packet?.all_ranked?'all_ranked':'top_20_high_quality';
  const source=modern?packet.issuer_rows:packet?.all_ranked||packet?.top_20_high_quality||[];
  if(!Array.isArray(source))return source;
  return source.map((row,i)=>{
   if(!object(row))return row;
   if(!modern)return {...row,measurement_status:'Legacy periods, currencies and formulas unverified',source_record:sourceKey+'['+i+']'};
   const amounts=object(row.amounts)?row.amounts:{},metrics=object(row.measurements)?row.measurements:{},window=row.windows?.current?.income;
   return {...row,quality_score:null,sloan_accruals_pct_assets:metrics.earnings_cash_gap_pct_end_assets,
    cash_conversion_ratio:metrics.cash_conversion_ratio,dsri_beneish:metrics.dsri_reported,gmi_beneish:metrics.gmi_reported,
    average_assets_gap_pct:metrics.cash_flow_accruals_pct_average_assets,reported_net_income:amounts.net_income,
    reported_ocf:amounts.operating_cash_flow,reported_fcf:amounts.free_cash_flow_derived,
    window_start:window?.start_date,window_end:window?.end_date,
    measurement_status:typeof row.status==='string'?row.status:'Unavailable',source_record:'issuer_rows['+i+']'};
  });
 }
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const text=x=>typeof x==='string'&&x.trim()?x:'Unavailable';
 const groups=[
  ['income','Income statement',[['netIncome','Net income'],['revenue','Revenue'],['grossProfit','Gross profit']]],
  ['cash_flow','Cash flow statement',[['operatingCashFlow','Operating cash flow'],['netCashProvidedByOperatingActivities','Operating cash flow (provider alias)'],['capitalExpenditure','Capital expenditure (reported sign)']]],
  ['balance_sheet','Balance sheet',[['totalAssets','Total assets'],['netReceivables','Net receivables']]]
 ];
 function amount(value){
  // Never turn missing, boolean or unsafe parsed numbers into a measured zero.
  return typeof value==='number'&&Number.isFinite(value)&&Math.abs(value)<=Number.MAX_SAFE_INTEGER?String(value):'Unavailable';
 }
 function statementModel(packet){
  if(packet?.measurement_contract!=='earnings-accounting-measurements.v1')return {available:false,issuers:[],message:'Individual statement observations are unavailable in this legacy publication. Inspect the complete received packet.'};
  if(!Array.isArray(packet.issuer_rows))return {available:false,issuers:[],message:'The published issuer population is malformed. Inspect the complete received packet.'};
  const issuers=packet.issuer_rows.map((row,index)=>({row,index})).filter(x=>object(x.row));
  return {available:true,issuers,message:issuers.length+' selectable issuer occurrences; '+(packet.issuer_rows.length-issuers.length)+' malformed occurrences retained in the complete packet. Repeated tickers remain separate source records.'};
 }
 function statementHTML(row,index){
  const base='/issuer_rows/'+index, observations=object(row.statement_observations)?row.statement_observations:{};
  let html='<p class="sub">'+esc(text(row.ticker))+' · '+esc(text(row.name))+' · '+esc(base)+'. Amounts are in each row’s reported currency, without scaling or conversion. Filing and acceptance dates are provider-reported, not verified first availability.</p>';
  if(!object(row.statement_observations))html+='<p>Statement observations are missing or malformed. Inspect the parsed issuer and complete packet.</p>';
  for(const [key,label,metrics]of groups){
   const population=observations[key],pointer=base+'/statement_observations/'+key;
   html+='<h3>'+label+'</h3>';
   if(!Array.isArray(population)){html+='<p>Population unavailable or malformed at '+esc(pointer)+'.</p>';continue;}
   html+='<p class="sub">'+population.length+' received observation occurrences. '+(key==='balance_sheet'?'Balances are reported at an instant; a quarter start is not required.':'A fiscal-quarter label alone does not prove the duration or make these amounts additive.')+'</p>';
   if(!population.length)continue;
   html+='<div class="statement-scroll" role="region" aria-label="'+label+' observations" tabindex="0"><table><thead><tr>';
   for(const heading of ['Period end','Explicit start','Fiscal period','Fiscal year','Currency',...metrics.map(x=>x[1]),'Filing date','Accepted at (reported)','CIK','Duration / basis','Reported issues','Source coordinate'])html+='<th scope="col">'+heading+'</th>';
   html+='</tr></thead><tbody>';
   population.forEach((obs,i)=>{
    const path=pointer+'/'+i;
    if(!object(obs)){html+='<tr><td colspan="'+(11+metrics.length)+'">Malformed observation at '+esc(path)+'; retained in the parsed issuer and complete packet.</td></tr>';return;}
    const identity=object(obs.identity)?obs.identity:{},values=object(obs.values)?obs.values:{};
    const basis=key==='balance_sheet'?'Instant balance':obs.duration_verified===true&&typeof obs.start_date==='string'&&obs.start_date.trim()?'Duration marked verified by producer':'Duration unverified; annual aggregation unavailable';
    const issues=Array.isArray(obs.issues)?(obs.issues.length?obs.issues.map(x=>typeof x==='string'?x:JSON.stringify(x)).join('; '):'None reported'):'Issue list unavailable';
    const source=Number.isSafeInteger(obs.source_row)&&obs.source_row>=0?base+'/acquisitions/'+key+'/response/'+obs.source_row:'provider response coordinate unavailable';
    const cells=[text(obs.date),key==='balance_sheet'?(typeof obs.start_date==='string'?obs.start_date:'Not required for instant'):text(obs.start_date),text(identity.period),text(identity.fiscal_year),text(identity.currency),...metrics.map(([field])=>amount(values[field])),text(identity.filing_date),text(identity.accepted_at),text(identity.cik),basis,issues,path+'; '+source];
    html+='<tr>'+cells.map(x=>'<td>'+esc(x)+'</td>').join('')+'</tr>';
   });
   html+='</tbody></table></div>';
  }
  return html;
 }
 function mount(packet,doc=root.document){
  const select=doc.getElementById('statement-issuer'),status=doc.getElementById('statement-status'),board=doc.getElementById('statements'),detail=doc.getElementById('statement-record'),original=doc.getElementById('statement-original');
  const model=statementModel(packet);let selected=null;
  status.textContent=model.message;select.disabled=!model.available||!model.issuers.length;
  select.innerHTML='<option value="">Choose an issuer occurrence</option>'+model.issuers.map(({row,index})=>'<option value="'+index+'">'+esc(text(row.ticker))+' · '+esc(text(row.name))+' · issuer_rows['+index+']</option>').join('');
  select.value='';detail.hidden=true;detail.open=false;
  board.textContent=model.available?'Choose an issuer above to inspect its reported statements.':'No individual statement view is available.';
  original.textContent='Choose an issuer.';
  select.onchange=()=>{
   selected=model.issuers.find(x=>String(x.index)===select.value)||null;
   detail.open=false;detail.hidden=!selected;original.textContent='Open to inspect the selected parsed issuer record.';
   if(!selected){board.textContent='Choose an issuer above to inspect its reported statements.';return;}
   board.innerHTML=statementHTML(selected.row,selected.index);
  };
  detail.ontoggle=()=>{if(detail.open&&selected)original.textContent=JSON.stringify(selected.row,null,2);};
 }
 const api={rows,statementModel,statementHTML,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHEarningsObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
