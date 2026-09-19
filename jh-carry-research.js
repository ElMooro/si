(function(root){
 'use strict';const CONTRACT='carry-original-research.v1',PREFIX='data/carry-research/';
 const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>v===null||v===undefined||v===''||!Number.isFinite(Number(v))?'—':Number(v).toLocaleString('en-US',{maximumFractionDigits:5});
 const words=v=>esc(v).replaceAll('_',' '),path=k=>typeof k==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(k,label)=>path(k)?`<a href="${esc(path(k))}">${esc(label)}</a>`:'Evidence unavailable';
 const table=(headers,rows)=>`<div class="table-scroll"><table><thead><tr>${headers.map(x=>`<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>'<tr>'+r.map(v=>`<td>${v}</td>`).join('')+'</tr>').join('')}</tbody></table></div>`;
 function history(h,page=0){
  if(!h)return {page:0,pages:1,count:0,html:'No retained history.'};const size=40,pages=Math.max(1,Math.ceil(h.rows.length/size));page=Math.max(0,Math.min(pages-1,page));
  const slice=[...h.rows].reverse().slice(page*size,page*size+size);
  const cols=['date',...Object.keys(h.rows[0]).filter(k=>k!=='date')];return {page,pages,count:h.rows.length,html:table(cols,slice.map(r=>cols.map(k=>esc(typeof r[k]==='object'&&r[k]!==null?JSON.stringify(r[k]):r[k]))))};
 }
 function chart(h,range=260){
  if(!h)return '<p>No retained history.</p>';
  if(h.kind==='splits')return '<p>Reported split ratios: new shares per old share. A point at 5 means five shares replaced one; it is not an investment return.</p>'+chart({...h,kind:'split_ratio_chart',unit:'new shares / old share',rows:h.rows.map(r=>({...r,value_decimal:String(Number(r.numerator_decimal)/Number(r.denominator_decimal))}))},range);
  if(h.kind==='distributions')return '<p>Provider-reported ex-date schedule, including announced events. A point does not establish that cash was paid; inspect each payment date below.</p>'+chart({...h,kind:'distribution_chart'},range);
  const rows=(range?h.rows.slice(-range):h.rows).map(r=>({...r,value:r.value_decimal===null?null:Number(r.value_decimal)})),valid=rows.filter(r=>r.value!==null&&Number.isFinite(r.value));
  if(!valid.length)return '<p>No numeric observations in this range.</p>';
  const W=1050,H=250,pad=60,min=Math.min(...valid.map(r=>r.value)),max=Math.max(...valid.map(r=>r.value)),span=max-min||1,from=Date.parse(rows[0].date),to=Date.parse(rows.at(-1).date),xs=d=>pad+(Date.parse(d)-from)/Math.max(86400000,to-from)*(W-2*pad),ys=v=>H-pad-(v-min)/span*(H-2*pad);
  let d='',prior=null;const maxGap={D:6,W:10,M:35}[h.frequency]||1;
  for(const r of rows){if(r.value===null){prior=null;continue;}const gap=prior?(Date.parse(r.date)-Date.parse(prior.date))/86400000:Infinity;d+=(gap<=maxGap?'L':'M')+xs(r.date).toFixed(2)+','+ys(r.value).toFixed(2)+' ';prior=r;}
  return `<svg class="history-chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(h.id)} dated history"><text x="12" y="25">${number(max)}</text><text x="12" y="${H-pad+4}">${number(min)}</text><text x="${pad}" y="${H-15}">${esc(rows[0].date)}</text><text x="${W-pad}" text-anchor="end" y="${H-15}">${esc(rows.at(-1).date)}</text>${h.frequency==='event'?valid.map(r=>`<circle cx="${xs(r.date).toFixed(2)}" cy="${ys(r.value).toFixed(2)}" r="3" fill="#d5b372"/>`).join(''):''}<path d="${d}" stroke="#d5b372" fill="none" stroke-width="2"/></svg><p class="dim">Linear calendar axis · ${esc(h.unit)}. Missing observations break the line; no interpolation.</p>`;
 }
 async function sha(raw){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',raw))].map(x=>x.toString(16).padStart(2,'0')).join('');}
 async function bytes(fetcher,key,limit){
  if(!path(key))throw Error('Unsupported evidence path');const r=await fetcher(path(key),{cache:'no-store'});if(!r.ok)throw Error('Research artifact unavailable');
  const reader=r.body.getReader(),parts=[];let size=0;try{while(true){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>limit){await reader.cancel();throw Error('Artifact size bound');}parts.push(value);}}finally{reader.releaseLock();}
  const out=new Uint8Array(size);let offset=0;for(const part of parts){out.set(part,offset);offset+=part.length;}return out;
 }
 async function loadSnapshot(id,fetcher=root.fetch.bind(root)){
  if(!/^[a-f0-9]{64}$/.test(id||''))throw Error('Invalid run ID');const key=PREFIX+'runs/'+id+'.json',raw=await bytes(fetcher,key,256*1024);
  if(await sha(raw)!==id)throw Error('Run hash differs');const m=JSON.parse(new TextDecoder().decode(raw)),ref=m.output;
  if(m.contract!=='carry-original-replay.v1'||ref?.key!==PREFIX+'outputs/'+m.output_sha256+'.json'||ref.sha256!==m.output_sha256)throw Error('Run identity differs');
  const output=await bytes(fetcher,ref.key,4*1024*1024);if(output.length!==ref.bytes||await sha(output)!==ref.sha256)throw Error('Research output differs');
  const p=JSON.parse(new TextDecoder().decode(output));boundary(p);if(p.generated_at!==m.generated_at)throw Error('Snapshot clock differs');p.replay={manifest_key:key,output_sha256:ref.sha256,input_key:m.input?.key};return p;
 }
 async function loadHistory(row,fetcher=root.fetch.bind(root)){
  const ref=row.history;if(!ref)return null;if(ref.key!==PREFIX+'histories/'+ref.sha256+'.json')throw Error('History identity differs');
  const raw=await bytes(fetcher,ref.key,16*1024*1024);if(raw.length!==ref.bytes||await sha(raw)!==ref.sha256)throw Error('History bytes differ');
  const h=JSON.parse(new TextDecoder().decode(raw));if(h.contract!=='carry-history.v1'||h.id!==row.id||h.unit!==ref.unit||h.frequency!==ref.frequency||h.kind!==ref.kind||h.rows.length!==ref.observations)throw Error('History contract differs');
  if(h.rows.some((r,i)=>!/^\d{4}-\d{2}-\d{2}$/.test(r.date)||(i&&(r.date<h.rows[i-1].date||(r.date===h.rows[i-1].date&&h.frequency!=='event')))||(Object.hasOwn(r,'value_decimal')&&r.value_decimal!==null&&!Number.isFinite(Number(r.value_decimal)))))throw Error('History rows differ');
  if(h.rows[0].date!==ref.from||h.rows.at(-1).date!==ref.to)throw Error('History endpoints differ');return h;
 }
 const choices=p=>[...Object.values(p.equities),...Object.values(p.measurements),...Object.values(p.comparisons)];
 const selected=(p,id)=>choices(p).find(r=>r.id===id);
 const histories=r=>r?.prices_history?['prices_history','distributions_history','splits_history'].filter(k=>r[k]):r?.history?['history']:[];
 function boundary(p){
  if(p?.contract!==CONTRACT||p.call!==null||p.calls_eligible!==false||p.sizing_eligible!==false||p.execution_eligible!==false||p.portfolio_action!=='WAIT'||p.unwind_overlay?.cohort_fragility!==null||!p.equities||!p.measurements||!p.comparisons||!p.by_class)throw Error('Unsupported carry research contract');
  if(['cross_asset_top','cross_asset_bottom','risk_adjusted_leaders','dislocation_leaders'].some(k=>!Array.isArray(p[k])||p[k].length))throw Error('Unqualified carry rankings');
  for(const r of choices(p)){
   if(typeof r.id!=='string'||typeof r.unit!=='string')throw Error('Measurement identity differs');
   for(const k of histories(r)){const h=r[k];if(h.key!==PREFIX+'histories/'+h.sha256+'.json'||!Number.isInteger(h.observations)||h.observations<1)throw Error('History identity differs');}
  }return p;
 }
 function expectedPriceDate(at){
  const p=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date(at)).map(v=>[v.type,v.value]));
  const d=new Date(Date.UTC(+p.year,+p.month-1,+p.day)-86400000);while([0,6].includes(d.getUTCDay()))d.setUTCDate(d.getUTCDate()-1);return d.toISOString().slice(0,10);
 }
 function status(r,at=Date.now(),pinned=false){
  if(!r)return 'unavailable';const q=r.quality;if(!q)return r.status||'unavailable';if(q.status!=='fresh')return q.status;
  if(pinned)return 'fresh_at_snapshot';const age=at-Date.parse(q.acquired_at);
  if(!Number.isFinite(age)||age<0||age>26*3600000)return 'expired_source';
  if(r.symbol&&r.as_of<expectedPriceDate(at))return 'release_due_unverified';
  const days=Math.floor(at/86400000)-Math.floor(Date.parse(r.as_of)/86400000);
  if(!Number.isFinite(days)||days<0||days>q.max_observation_age_days)return 'stale_observation';return 'fresh';
 }
 const current=(r,at,pinned)=>['fresh','fresh_at_snapshot'].includes(status(r,at,pinned));
 function summary(p,cls='equity',query='',page=0,at=Date.now(),pinned=false){
  const filtered=(p.by_class[cls]||[]).filter(r=>(r.symbol+' '+r.research_id+' '+(r.yield_kind||'')).toLowerCase().includes(query.toLowerCase())),pages=Math.max(1,Math.ceil(filtered.length/25));page=Math.max(0,Math.min(pages-1,page));
  const btn=(id,label)=>selected(p,id)?`<button type="button" class="inspect" data-select="${esc(id)}">${esc(label)}</button>`:esc(label);
  const rows=filtered.slice(page*25,page*25+25).map(r=>{
   const item=selected(p,r.research_id),live=current(item,at,pinned);let value=r.trailing_distribution_yield_pct_decimal??r.rate_pct_decimal??r.yield_pct_decimal??null;
   let measure=cls==='equity'?'Trailing reported distributions / price · %':cls==='fx'?'Reported rate · %':cls==='fixed_income'?(r.yield_kind==='real_yield'?'Real yield · %':'Nominal / index yield · %'):'No verified futures curve';
   const comparison=p.comparisons[r.comparison_id];let notes=cls==='equity'?words(r.income_status||'unavailable'):words(r.rate_kind||r.yield_kind||'unavailable');
   if(comparison){const usable=live&&current(p.measurements.DFF,at,pinned);notes+=`<br>${btn(comparison.id,'Inspect matched-date gap')} · ${usable?number(comparison.value_decimal):'—'} pp · ${words(usable?comparison.status:'source_not_current')}`;}
   if(r.explicit_alternative){const a=r.explicit_alternative;notes+=`<br>Separate ECB EUR series: ${btn(a.research_id,a.as_of)} · ${current(selected(p,a.research_id),at,pinned)?number(a.rate_pct_decimal):'—'}% · ${words(status(selected(p,a.research_id),at,pinned))}`;}
   if(cls==='commodity')notes=esc(r.missing.join(', '));
   return [btn(r.research_id,r.symbol)+(r.legacy_alias&&r.legacy_alias!==r.symbol?`<br><small>Legacy code ${esc(r.legacy_alias)}</small>`:''),`${live?number(value):'—'}<br><small>${esc(measure)}</small>`,esc(item?.as_of),words(status(item,at,pinned)),notes];
  });
  const board=table(['Instrument','Measurement','Observation','Source state','Interpretation / evidence'],rows);
  const fresh=[...Object.values(p.measurements),...Object.values(p.equities)].filter(r=>current(r,at,pinned)).length;
  const evidence=`<p>${esc(p.history_basis)}</p><p>${esc(p.decision_qualification.reason)}</p><p>${link(p.replay.manifest_key,'Immutable replay manifest')} · ${link(p.replay.input_key,'Every retained input reference')}</p><p>Output SHA-256: <code>${esc(p.replay.output_sha256)}</code></p><p><code>python scripts/replay_carry_research.py --run ${esc(p.replay.manifest_key.split('/').at(-1).replace('.json',''))}</code></p><p>Complete preceding packet preserved privately: <code>${esc(p.legacy_preservation?.sha256)}</code>. Its heuristic rankings are not promoted into verified research.</p><details><summary>Unavailable sources and qualification gaps</summary><pre>${esc(JSON.stringify({source_errors:p.source_status_codes,qualification:p.decision_qualification},null,2))}</pre></details>`;
  return {board,evidence,page,pages,count:filtered.length,state:`${pinned?'Archived snapshot':'Current publication'} · ${fresh} source families within their displayed source rules. Research only; no qualified Calls or sizing vote.`,asof:`Compiled ${p.generated_at}. ${pinned?'Freshness labels refer to this snapshot.':'Expired sources are withheld from the current board.'} Each series keeps its own observation date.`};
 }
 function detail(p,id,at,pinned){
  const r=selected(p,id);if(!r)return '<p>No original measurements available for this selection.</p>';
  const refs=[];function walk(v){if(v?.evidence?.key){refs.push(v);return;}if(v&&typeof v==='object')for(const x of Object.values(v))walk(x);}walk(r.originals);
  let text=`<h2>${esc(r.status==='real_nominal_comparison_prohibited'?'Real yield: nominal funding comparison disabled':r.name||id)}</h2><p>Observation ${esc(r.as_of)} · ${words(status(r,at,pinned))}. This panel preserves the selected snapshot, including historical or stale observations.</p>`;
  if(r.symbol){const d=r.trailing_distribution,f=r.funding_reference_comparison;
   text+=table(['Reported item','Value / period'],[
    ['USD price per current share',esc(r.price_decimal)],['Trailing ex-date distributions per current share · USD',esc(d.current_share_distribution_decimal)],
    ['Trailing distribution / price · %',number(d.yield_pct_decimal)],['Window',`${esc(d.window_start_exclusive)} exclusive → ${esc(d.window_end_inclusive)} inclusive`],
    ['Ex-date events / current status',`${number(d.record_count)} / ${words(d.status)}`],['Payment-date window · USD per current share',esc(r.paid_distribution_window.current_share_distribution_decimal)],
    ['Ex-dates unpaid or undated at price date',esc(d.unpaid_or_undated_selected_ex_dates.join(', ')||'None')],
    ['Same-date income-yield minus DFF · pp',number(f.yield_minus_dff_pp_decimal)],['Realized 60-return price volatility · %',number(r.realized_price_volatility['60'].annualized_pct)],
    ['Undated provider-reported TTM ratio · %',number(r.reported_ttm_ratio.percent_decimal)]]);
   text+=`<p>${esc(d.limitation)} ${esc(d.currency_basis)}</p><p>${esc(f.limitation)}</p><p>Corporate actions are reconciled independently using raw distributions, raw closes and split ratios. Ambiguous duplicate ex-dates are retained and cannot qualify the affected window.</p><details><summary>Security identity, reconciliation, income windows and volatility formulas</summary><pre>${esc(JSON.stringify({identity:r.identity,price_adjustment:r.price_adjustment_status,price_mismatches:r.price_adjustment_mismatch_dates,ambiguous_ex_dates:r.ambiguous_distribution_dates,trailing_distribution:d,paid_window:r.paid_distribution_window,reported_ratio:r.reported_ttm_ratio,volatility:r.realized_price_volatility,funding_reference:f,buyback_status:r.buyback_status,coverage:r.history_coverage},null,2))}</pre></details>`;
  }else{text+=`<p class="measurement">${esc(r.value_decimal)} ${esc(r.unit)}</p><p>${esc(r.limitation||r.vintage)}</p>`;
   if(r.left)text+=`<p>Original comparison inputs: <button type="button" data-select="${esc(r.left)}">${esc(r.left)}</button> <button type="button" data-select="${esc(r.right)}">${esc(r.right)}</button>. Latest comparison state: ${words(r.status)}.</p>`;
  }
  if(r.quality)text+=`<p>Acquired ${esc(r.quality.acquired_at)}. Observation age at compilation: ${number(r.quality.observation_age_days)} days. This uses explicit age limits, not a verified release or exchange calendar.</p>`;
  text+=`<p>${histories(r).map(k=>`${link(r[k].key,words(k).replaceAll(' history','')+' · '+r[k].observations+' records')}`).join(' · ')||'No retained history.'}</p>`;
  text+=`<details><summary>Retained original responses and definitions</summary><ul>${refs.map(v=>`<li>${link(v.evidence.key,'Exact retained response')} · <code>${esc(v.url)}</code> · acquired ${esc(v.acquired_at)} · SHA-256 <code>${esc(v.evidence.sha256)}</code></li>`).join('')||'<li>Comparison originals are attached to its input series.</li>'}</ul><pre>${esc(JSON.stringify(r.definition||{},null,2))}</pre></details>`;return text;
 }
 const scenarioFields={
  equity:[['shares','Shares held',0,1e12],['start','Starting price · USD/share',0.000001,1e9],['end','Ending price · USD/share',0,1e9],['distributions','Cash distributions received · USD/share',0,1e9],['funded','Borrowed fraction of initial value',0,1],['rate','Annual borrowing rate · %',-20,100],['days','Calendar days',1,3660,1],['basis','Borrowing day basis',360,365,1],['tax','Tax on distributions · %',0,100],['fees','Total fees · USD',0,1e12]],
  fx:[['principal','Foreign asset principal',0,1e12],['start','Starting FX · funding units / asset unit',0.000001,1e6],['end','Ending FX · funding units / asset unit',0.000001,1e6],['assetRate','Annual asset rate · %',-20,100],['fundingRate','Annual funding rate · %',-20,100],['days','Calendar days',1,3660,1],['assetBasis','Asset day basis',360,365,1],['fundingBasis','Funding day basis',360,365,1],['fees','Fees · funding currency',0,1e12]],
  bond:[['value','Initial dirty market value · USD',0,1e12],['duration','Modified duration · years',0,100],['convexity','Convexity · years²',-1000,10000],['yieldMove','Parallel yield change · bp',-2000,2000],['income','Annual cash income / initial value · %',0,100],['funded','Borrowed fraction of initial value',0,1],['rate','Annual borrowing rate · %',-20,100],['days','Calendar days',1,3660,1],['incomeBasis','Income approximation day basis',360,365,1],['basis','Borrowing day basis',360,365,1],['creditLoss','Additional credit loss over horizon · %',0,100],['fees','Total fees · USD',0,1e12]]
 };
 const scenarioLimits={equity:'Entered cash distributions and terminal prices only. Simple funding interest. No security-price forecast, margin-call model, tax on price gains, reinvestment, or execution slippage beyond entered fees. Borrowed fraction is limited to 0–1; no leveraged-account simulation.',fx:'Unhedged, fully funded foreign asset. Simple interest and fixed fees. No security-price change, compounding, default, cross-currency basis, margin-call, or refinancing model. The FX quote must use funding-currency units per asset-currency unit.',bond:'Second-order parallel-yield approximation, plus pro-rated entered cash-income yield. Not exact bond pricing or coupon scheduling. Entered additional credit loss must not duplicate the yield-move loss. No rolldown, optionality, reinvestment, margin-call or tax model. Larger yield shocks and long horizons can invalidate the approximation.'};
 function scenario(kind,inputs){
  const fields=scenarioFields[kind];if(!fields)throw Error('Choose a supported payoff model');
  if(Object.keys(inputs).length!==fields.length)throw Error('Enter exactly the required assumptions');
  for(const [k,,min,max,step] of fields){const v=inputs[k];if(typeof v!=='number'||!Number.isFinite(v)||v<min||v>max||(step===1&&!Number.isInteger(v)))throw Error('Invalid assumption: '+k);if(k.toLowerCase().includes('basis')&&![360,365].includes(v))throw Error('Choose Actual / 360 or Actual / 365');}
  const x=inputs;let components=[],initial,formula,breakeven=null,currency=kind==='fx'?'funding currency':'USD';
  if(kind==='equity'){
   initial=x.shares*x.start;const price=x.shares*(x.end-x.start),income=x.shares*x.distributions,tax=income*x.tax/100,funding=initial*x.funded*x.rate/100*x.days/x.basis;
   components=[['Price P&L',price],['Gross cash distributions',income],['Distribution tax',-tax],['Funding interest',-funding],['Fees',-x.fees]];
   breakeven=x.shares?x.start-(income-tax-funding-x.fees)/x.shares:null;formula='shares × (end − start) + distributions × shares × (1 − tax/100) − initial value × funded fraction × rate/100 × days/basis − fees';
  }else if(kind==='fx'){
   initial=x.principal*x.start;const assetInterest=x.principal*x.assetRate/100*x.days/x.assetBasis,funding=initial*x.fundingRate/100*x.days/x.fundingBasis;
   if(x.principal+assetInterest<0||initial+funding<0)throw Error('Simple-interest horizon implies negative principal repayment');
   components=[['FX on principal',x.principal*(x.end-x.start)],['Asset interest at ending FX',assetInterest*x.end],['Funding interest',-funding],['Fees',-x.fees]];
   breakeven=x.principal+assetInterest>0?(initial+funding+x.fees)/(x.principal+assetInterest):null;
   formula='foreign principal × (ending FX − starting FX) + foreign interest × ending FX − funding interest − fees';
  }else{
   initial=x.value;const dy=x.yieldMove/10000,price=initial*(-x.duration*dy+.5*x.convexity*dy*dy);
   if(initial+price<0)throw Error('Duration approximation implies a negative terminal price; use exact repricing');
   components=[['Duration price effect',-initial*x.duration*dy],['Convexity correction',initial*.5*x.convexity*dy*dy],['Pro-rated cash income',initial*x.income/100*x.days/x.incomeBasis],['Funding interest',-initial*x.funded*x.rate/100*x.days/x.basis],['Additional credit loss',-initial*x.creditLoss/100],['Fees',-x.fees]];
   formula='value × (−duration × Δyield + 0.5 × convexity × Δyield² + income/100 × days/incomeBasis − funded × fundingRate/100 × days/basis − creditLoss/100) − fees';
  }
  const net=components.reduce((sum,r)=>sum+r[1],0);if(!Number.isFinite(net))throw Error('Scenario numeric bound');
  return {contract:'carry-entered-scenario.v1',kind,assumptions:{...inputs},currency,components,net_pnl:net,initial_value:initial,return_on_initial_value_pct:initial>0?100*net/initial:null,
   break_even_end_price_or_fx:breakeven,formula,limitations:scenarioLimits[kind],forecast:false,position_size:null,execution_eligible:false};
 }
 const api={CONTRACT,PREFIX,esc,number,words,path,link,table,choices,selected,histories,boundary,expectedPriceDate,status,current,summary,detail,history,chart,sha,loadSnapshot,loadHistory,scenarioFields,scenarioLimits,scenario};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.CarryResearch=api;
})(typeof globalThis==='object'?globalThis:this);
