(function(root){
 'use strict';
 const CONTRACT='alpha-research.v1',PREFIX='data/alpha-research/';
 const esc=v=>String(v??'Unavailable').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=v=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('en-US',{maximumFractionDigits:4}):'Unavailable';
 const path=k=>typeof k==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(k)&&!k.includes('..')?'/'+k:null;
 const link=(k,label)=>path(k)?`<a href="${path(k)}">${esc(label)}</a>`:'Unavailable';
 const labels={current_packet:'Current packet',clock_unverified:'Clock unverified',source_model_not_validated:'Source model validation',original_provider_lineage_incomplete:'Original provider lineage',point_in_time_protocol_missing:'Point-in-time evaluation protocol',independent_out_of_sample_validation_missing:'Independent out-of-sample validation',cost_capacity_and_borrow_validation_missing:'Costs, capacity and borrow validation',portfolio_risk_and_limits_not_bound:'Portfolio risk and approved limits',unregistered_or_private_derived_contributor_excluded:'Contributor outside the public research boundary',source_packet_not_current:'Current source packet',no_matching_descriptive_cohort:'Matching descriptive cohort'};
 const human=value=>esc(labels[value]||value);
 const table=(heads,rows)=>`<div class="table-scroll"><table><thead><tr>${heads.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(v=>`<td>${v}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
 function recent(value,now,hours){const age=now-Date.parse(value);return Number.isFinite(age)&&age>=0&&age<=hours*3600000;}
 function assertBoundary(p,contract=CONTRACT){if(p.contract!==contract||p.call!==null||['calls_eligible','sizing_eligible','execution_eligible'].some(k=>p[k]!==false))throw Error('Unqualified research boundary differs');}
 function render(p,now=Date.now(),pinned=false){
  assertBoundary(p);const current=recent(p.generated_at,now,4),ideas=p.research_ideas||[],sources=p.source_feeds||{};
  const id=p.replay?.manifest_key?.match(/^data\/alpha-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];
  const cards=ideas.map(c=>{
   const distributions=c.stats?.distributions||[],scorecards=c.stats?.scorecards||[];
   const usable=pinned||(current&&recent(c.source_generated_at,now,26)&&c.source_status==='current_packet');
   return `<article class="call-card neutral"><h3>${esc(c.subject)}</h3><p class="research-label">RESEARCH IDEA · WAIT / ABSTAIN</p>`+
    `<p>Source direction: <strong>${esc(usable?c.direction:null)}</strong>. Reported conviction: ${num(usable?c.reported_conviction:null)} <span class="muted">(unvalidated source score; not a probability)</span>.</p>`+
    `<p>Source generated ${esc(c.source_generated_at)} · ${human(usable?c.source_status:'stale; retained context')}.</p>`+
    (c.aggregate_withheld?'<p class="warning">Aggregate withheld: an unregistered or private-derived contributor was excluded.</p>':'')+
    '<p><strong>Allocation / stop / target: unavailable.</strong> No portfolio change is authorized.</p>'+
    `<details><summary>Evidence, cohorts and qualification gaps</summary><p>Decision <code>${esc(c.decision_id)}</code></p><p>${link(c.source,'Current source packet')} · the retained input projection is linked in Reproduce below.</p>`+
    table(['Contributing engine','Reported family','Reported signal (definition unverified)'],(c.engines||[]).map(e=>[esc(e.name),esc(e.family),num(e.reported_signal)]))+
    `<p>${distributions.length} distribution and ${scorecards.length} scorecard vocabulary matches. No cohort is selected as the best match, and counts are not pooled.</p>`+
    (distributions.length?table(['Signals / projected row','Horizon days','N (raw)','Median / p25 / p75 (source units)','Return unit','Qualification'],distributions.map(r=>[esc(r.signals.join(', '))+' · '+esc(r.source_row),num(r.horizon_days),num(r.n),[r.median,r.p25,r.p75].map(num).join(' / '),esc(r.return_unit||'undeclared'),esc(r.match_basis)])):'<p>No matching distribution. No prior payoff is invented.</p>')+
    (scorecards.length?table(['Signal / projected row','Scored / quarantined','Hit rate (reported fraction)','Wilson lower / upper (fraction)','Mean return %','Qualification'],scorecards.map(r=>[esc(r.signal_type)+' · '+esc(r.source_row),num(r.n_scored)+' / '+num(r.n_quarantined),num(r.hit_rate),num(r.wilson_lb)+' / '+num(r.wilson_ub),num(r.avg_return_pct),'Descriptive; not strategy validation'])):'<p>No matching scorecard.</p>')+
    '<p>Required before this idea can support sizing: '+(c.qualification_gaps||[]).map(human).join(' · ')+'</p></details></article>';
  }).join('');
  const familyRows=(p.evidence_map?.family_groups||[]).map(r=>[esc(r.family),esc(r.subjects.join(' · ')),'Not established']);
  const f=p.pd_settlement_fails||{};
  return {
   hero:`<p class="research-label">ALPHA COMPASS · REPRODUCIBLE RESEARCH</p><h1>${pinned?'Pinned research snapshot':current?'Research desk':'Research snapshot stale'}</h1><p><strong>WAIT means abstain.</strong> It does not instruct closing or holding an existing position.</p><p>${ideas.length} research ideas · 0 qualified votes · allocation unavailable. Every idea below links to the evidence used and the qualification still required.</p>`,
   ideas:cards||'<p>No registered research ideas are available in this snapshot.</p>',
   regimes:table(['Source / field','Reported label','Packet status'],(p.regime?.sources||[]).map(r=>[link(r.source,r.source)+'<br>'+esc(r.field),esc(r.reported_label),esc(pinned?r.availability.status:recent(r.availability.generated_at,now,26)?r.availability.status:'stale')]))+
    '<p>Labels may describe different horizons and definitions. They remain separate and are not independent votes.</p>',
   dependency:table(['Shared family','Research ideas using family','Statistical independence'],familyRows)+'<p>Underlying series ancestry is incomplete. Family counts are not effective sample sizes.</p>',
   sources:table(['Input','Source generated','Captured','Packet availability','Declared quality','Original provider verified'],Object.entries(sources).map(([name,r])=>[link(r.source,name),esc(r.generated_at),esc(r.acquired_at),esc(pinned?r.status:recent(r.generated_at,now,26)?r.status:'stale'),esc(r.declared_quality),'No']))+
    '<p>Packet transport age has a 26-hour ceiling. A recent packet is not proof of a recent observation or a valid model. Inputs are typed public projections; owner notes and private account data are excluded.</p>',
   fails:table(['Scope','Observation','FTD / FTR / gross, USD bn','Status'],[f,f.ust_ex_tips||{}].map(r=>{const v=(pinned||current)&&r.status==='context_only'?r.display_values:null;return [esc(r.scope_id),esc(r.as_of),v?[v.ftd_bn,v.ftr_bn,v.combined_bn].map(num).join(' / '):'Unavailable',esc(r.status)+' '+esc((r.reasons||[]).join(', '))];}))+
    `<p>${esc(f.note)} Provider originals have not been verified here.</p>`,
   evidence:link(p.replay?.manifest_key,'Immutable run manifest')+(id?` · <a href="/alpha-compass.html?run=${id}">Permanent snapshot link</a>`:'')+
    '<p>Replay checks the retained typed inputs and exact compiler output. It does not certify original provider history or predictive skill.</p>'+link(p.replay?.input_key,'Retained public input projections')+
    '<p>Independent reproduction: <code>python scripts/replay_alpha_research.py</code> using the matching release checkout. Old self-graded history is retained in protected audit storage; it is not a validated performance record.</p>',
   sub:`Published ${p.generated_at} · ${p.method} · ${pinned?'values as published':current?'current snapshot':'stale snapshot'}`
  };
 }
 function scaled(text,decimals){
  if(typeof text!=='string'||!new RegExp('^-?\\d{1,12}(?:\\.\\d{1,'+decimals+'})?$').test(text))throw Error('Enter plain decimal assumptions within the stated precision.');
  const sign=text.startsWith('-')?-1n:1n,[whole,fraction='']=text.replace(/^-/,'').split('.');
  return sign*(BigInt(whole)*10n**BigInt(decimals)+BigInt(fraction.padEnd(decimals,'0')));
 }
 const roundCents=n=>(n<0n?-1n:1n)*((n<0n?-n:n)+50000000n)/100000000n;
 const dollars=n=>{const a=n<0n?-n:n;return (n<0n?'-':'')+(a/100n).toString()+'.'+(a%100n).toString().padStart(2,'0');};
 function scenario(notional,shock,cost,side){
  const n=scaled(notional,2),r=scaled(shock,4),c=scaled(cost,4);
  if(n<=0n||n>100000000000n)throw Error('Notional must be positive and at most USD 1 billion.');
  if(r< -1000000n||r>10000000n)throw Error('Illustrative price move must be between −100% and +1000%.');
  if(c<0n||c>100000000n)throw Error('Entered total costs must be between 0 and 10,000 basis points.');
  if(!['long','short'].includes(side))throw Error('Choose a direct long or direct short exposure.');
  const gross=n*r*100n*(side==='long'?1n:-1n),costs=n*c;
  return {contract:'hypothetical-linear-payoff.v1',assumptions:{notional_usd:notional,price_move_pct:shock,total_cost_bps:cost,direct_side:side},gross_usd:dollars(roundCents(gross)),entered_cost_usd:dollars(roundCents(costs)),after_entered_cost_usd:dollars(roundCents(gross-costs)),
    formula:'direct side × notional × price move / 100 − notional × entered cost bps / 10,000',
    scope:'Hypothetical unleveraged linear price P&L in USD. Not an expected return, account estimate or allocation. FX, borrow, financing, dividends, gaps and market impact are not estimated; short losses are not bounded by initial notional.'};
 }
 function briefMarkup(text){
  return String(text).split('\n').map(line=>{
   const snapshot=line.match(/^\[Retained Alpha snapshot\]\(\/alpha-compass\.html\?run=([a-f0-9]{64})\)$/);
   if(snapshot)return `<p><a href="/alpha-compass.html?run=${snapshot[1]}">Retained Alpha snapshot</a></p>`;
   const heading=line.match(/^(#{1,3}) (.+)$/);if(heading)return `<h3>${esc(heading[2])}</h3>`;
   // Escape first; support only emphasis, code and the exact public snapshot link above.
   let safe=esc(line).replace(/\*\*([^*]+)\*\*/g,'<strong>$1</strong>').replace(/`([^`]+)`/g,'<code>$1</code>');
   return line?`<p>${safe}</p>`:'';
  }).join('');
 }
 async function bytes(fetcher,key,limit=8*1024*1024){
  if(!path(key))throw Error('Unsupported evidence path');const response=await fetcher(path(key),{cache:'no-store'});
  if(!response.ok)throw Error('Research artifact unavailable');const reader=response.body.getReader(),parts=[];let size=0;
  try{while(true){const r=await reader.read();if(r.done)break;size+=r.value.length;if(size>limit){await reader.cancel();throw Error('Research artifact exceeds size bound');}parts.push(r.value);}}finally{reader.releaseLock();}
  const out=new Uint8Array(size);let offset=0;for(const p of parts){out.set(p,offset);offset+=p.length;}return out;
 }
 async function sha(raw){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw))).map(b=>b.toString(16).padStart(2,'0')).join('');}
 async function loadSnapshot(id,fetcher=root.fetch.bind(root),kind='compass'){
  if(!/^[a-f0-9]{64}$/.test(id))throw Error('Invalid snapshot identifier');const key=PREFIX+'runs/'+id+'.json',raw=await bytes(fetcher,key,256*1024);
  if(await sha(raw)!==id)throw Error('Run manifest hash differs');const m=JSON.parse(new TextDecoder().decode(raw)),ref=m.output;
  if(m.contract!=='alpha-replay.v1'||m.kind!==kind||!ref||ref.sha256!==m.output_sha256||ref.key!==PREFIX+'outputs/'+ref.sha256+'.json'||!Number.isInteger(ref.bytes))throw Error('Unsupported Alpha output identity');
  const body=await bytes(fetcher,ref.key);if(body.length!==ref.bytes||await sha(body)!==ref.sha256)throw Error('Snapshot output differs');
  const p=JSON.parse(new TextDecoder().decode(body));assertBoundary(p,kind==='brief'?'alpha-brief-research.v1':CONTRACT);
  if(p.generated_at!==m.generated_at)throw Error('Snapshot clock differs');
  p.replay={manifest_key:key,output_sha256:ref.sha256,input_key:m.input?.key};return p;
 }
 const api={CONTRACT,PREFIX,esc,num,path,recent,render,scenario,briefMarkup,loadSnapshot,sha};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.AlphaResearch=api;
})(typeof globalThis==='object'?globalThis:this);
