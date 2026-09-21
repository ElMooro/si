(function(root){
  'use strict';
  const A=root.JHOptionResearch||(typeof require==='function'?require('./jh-option-research.js'):null);
  const CONTRACT='dollar-original-research.v1',PREFIX='data/dollar-research/',CURRENT='data/dollar-radar.json',MAX=32*1024*1024;
  const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
  const INDICES=['DTWEXBGS','DTWEXAFEGS','DTWEXEMEGS','RTWEXBGS'];
  const FX={DEXUSEU:['EUR','USD','EUR'],DEXUSUK:['GBP','USD','GBP'],DEXJPUS:['JPY','JPY','USD'],DEXCHUS:['CNY','CNY','USD'],DEXCAUS:['CAD','CAD','USD'],DEXMXUS:['MXN','MXN','USD'],DEXKOUS:['KRW','KRW','USD'],DEXSZUS:['CHF','CHF','USD'],DEXINUS:['INR','INR','USD'],DEXBZUS:['BRL','BRL','USD'],DEXUSAL:['AUD','USD','AUD'],DEXSIUS:['SGD','SGD','USD'],DEXTAUS:['TWD','TWD','USD'],DEXSDUS:['SEK','SEK','USD']};
  const CONTEXT=['WALCL','WRESBAL','RRPONTSYD','WTREGEN','DFII10','DGS10','DGS2','IRLTLT01DEM156N','VIXCLS','BAMLH0A0HYM2','DCOILWTICO','SWPT','NFCI','T10YIE'];
  const SERIES=[...INDICES,...Object.keys(FX),...CONTEXT],COMPILERS=['dollar_research_model','dollar_research_catalog','canonical_fred_replay','report_observations','research_brief_model','evidence_store','dollar_research_store'];
  const verified=new WeakSet(),proofs=new WeakMap(),object=v=>!!v&&typeof v==='object'&&!Array.isArray(v),digest=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
  const sameSet=(a,b)=>Array.isArray(a)&&A.same(a.slice().sort(),b.slice().sort()),integer=v=>Number.isSafeInteger(v)&&v>=0;
  const authority=p=>object(p)&&FLAGS.every(k=>p[k]===false),day=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
  const decimal=v=>typeof v==='string'&&v.length<=180&&/^-?\d+(?:\.\d+)?(?:E[+-]?\d{1,3})?$/.test(v)&&Number.isFinite(Number(v));
  const scalar=v=>object(v)&&((v.value===null&&v.exact_value===null)||(decimal(v.exact_value)&&typeof v.value==='number'&&Number.isFinite(v.value)&&Math.abs(v.value-Number(v.exact_value))<=Math.max(5.01e-11,Math.abs(v.value)*2e-15)));
  const point=p=>p===null||(object(p)&&day(p.date)&&integer(p.original_row_index)&&scalar(p));
  const safe=k=>typeof k==='string'&&new RegExp('^'+PREFIX+'(?:(runs|inputs|outputs)/[a-f0-9]{64}\\.json|compilers/[a-f0-9]{64}\\.py)$').test(k);
  function freeze(v){if(v&&typeof v==='object'&&!Object.isFrozen(v)){Object.values(v).forEach(freeze);Object.freeze(v);}return v;}
  function typed(p){
    return p?.contract===CONTRACT&&authority(p)&&['call','score','regime','dollar_pressure'].every(k=>p[k]===null)&&p.portfolio_action==='WAIT'&&p.independent_investment_votes===0&&
      A.clock(p.generated_at)&&A.clock(p.source_generated_at)&&Date.parse(p.source_generated_at)<=Date.parse(p.generated_at)&&object(p.series)&&sameSet(Object.keys(p.series),SERIES)&&
      sameSet(p.indices,INDICES)&&sameSet(p.bilaterals,Object.keys(FX))&&sameSet(p.context_series,CONTEXT)&&p.dependency_graph?.independent_votes===0&&p.benchmark_identity?.benchmark_replication_qualified===false&&
      SERIES.every(sid=>{const r=p.series[sid];if(!authority(r)||r.series_id!==sid||r.source_url!=='https://fred.stlouisfed.org/series/'+sid||!['D','W','M'].includes(r.frequency)||!point(r.latest_observation)||!Array.isArray(r.history)||r.history.length>2000)return false;
        if(r.history.some((v,i)=>!point(v)||v===null||(i&&v.date<=r.history[i-1].date)||v.date>p.source_generated_at.slice(0,10))||new Set(r.history.map(v=>v.original_row_index)).size!==r.history.length)return false;
        if(!A.same(r.history.at(-1)??null,r.latest_observation)||r.history_scope?.displayed_rows!==r.history.length||r.quality?.release_calendar_verified!==false)return false;
        if(r.current_value!==null&&r.current_value!==r.latest_observation?.value)return false;
        if(FX[sid]){const q=r.quote;if(!q||!A.same([q.currency,q.numerator,q.denominator],FX[sid])||!scalar(q.foreign_units_per_usd)||!scalar(q.usd_per_foreign_unit)||q.cnh_substitution_performed!==false)return false;}
        return true;});
  }
  async function load(key,fetcher,signal){
    if(key!==CURRENT&&!safe(key))throw Error('Unapproved Dollar evidence path');
    const response=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});if(!response.ok)throw Error('Recorded Dollar evidence is unavailable');
    if(Number(response.headers?.get('content-length'))>MAX)throw Error('Dollar evidence byte bound');let raw;
    if(response.body?.getReader){const reader=response.body.getReader(),parts=[];let length=0;
      try{for(;;){const part=await reader.read();if(part.done)break;length+=part.value.byteLength;if(length>MAX)throw Error('Dollar evidence byte bound');parts.push(part.value);}}
      catch(error){await reader.cancel();throw error;}
      const joined=new Uint8Array(length);let offset=0;for(const part of parts){joined.set(part,offset);offset+=part.byteLength;}raw=joined.buffer;
    }else raw=await response.arrayBuffer();
    if(!raw.byteLength||raw.byteLength>MAX)throw Error('Dollar evidence byte bound');
    return {raw,text:new TextDecoder('utf-8',{fatal:true}).decode(raw)};
  }
  async function retained(ref,kind,fetcher,signal){
    const ext=kind==='compilers'?'.py':'.json';if(!['inputs','outputs','compilers'].includes(kind)||!digest(ref?.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+ext||!integer(ref.bytes)||!ref.bytes||ref.bytes>MAX)throw Error('Exact Dollar artifact reference required');
    const item=await load(ref.key,fetcher,signal);if(item.raw.byteLength!==ref.bytes||await A.sha(item.raw)!==ref.sha256)throw Error('Retained Dollar bytes differ');
    return kind==='compilers'?item.text:JSON.parse(item.text);
  }
  async function runOutput(ref,fetcher,signal){
    if(!digest(ref?.output_sha256)||!safe(ref?.manifest_key)||!ref.manifest_key.startsWith(PREFIX+'runs/'))throw Error('Exact Dollar run required');
    const item=await load(ref.manifest_key,fetcher,signal),run=JSON.parse(item.text);
    if(ref.manifest_key!==PREFIX+'runs/'+await A.sha(item.raw)+'.json'||run.contract!=='dollar-original-replay.v1'||run.output_sha256!==ref.output_sha256||run.output?.sha256!==ref.output_sha256||!sameSet(Object.keys(run.compilers??{}),COMPILERS))throw Error('Dollar recorded run differs');
    const [output,input]=await Promise.all([retained(run.output,'outputs',fetcher,signal),retained(run.input,'inputs',fetcher,signal)]);
    for(const value of Object.values(run.compilers))await retained(value,'compilers',fetcher,signal);
    if(input.contract!=='dollar-original-inputs.v1'||run.generated_at!==output.generated_at||input.generated_at!==output.generated_at)throw Error('Dollar recorded clocks differ');
    return {output,input,run};
  }
  async function verifyPacket(p,fetcher,signal){
    if(!typed(p))throw Error('Descriptive original Dollar publication required');
    const proof=await runOutput(p.replay,fetcher,signal),{replay,...body}=p;if(!A.same(body,proof.output))throw Error('Dollar publication differs from recorded output');
    freeze(p);freeze(proof);verified.add(p);proofs.set(p,proof);return p;
  }
  async function recordedRun(id,fetcher,signal){
    if(!digest(id))throw Error('Exact recorded Dollar run required');const item=await load(PREFIX+'runs/'+id+'.json',fetcher,signal);
    if(await A.sha(item.raw)!==id)throw Error('Recorded Dollar run bytes differ');const ref={manifest_key:PREFIX+'runs/'+id+'.json',output_sha256:JSON.parse(item.text).output_sha256};
    const proof=await runOutput(ref,fetcher,signal),p={...proof.output,replay:ref};if(!typed(p))throw Error('Descriptive recorded Dollar publication required');
    freeze(p);freeze(proof);verified.add(p);proofs.set(p,proof);return p;
  }
  function selected(p,sid){if(!verified.has(p)||!SERIES.includes(sid))throw Error('Verify and select a recorded Dollar series first');return p.series[sid];}
  function evidence(p,sid){const row=selected(p,sid);return {contract:'dollar-recorded-evidence-export.v1',series_id:sid,run:p.replay,research:row,derived:p.derived,
    dependencies:p.dependency_graph,retained_contexts:p.retained_contexts,retained_predecessors:p.retained_predecessors,recorded_artifacts:proofs.get(p),browser_verification:'Retained artifact hashes and publication binding; original-provider arithmetic is checked on the runner.'};}
  function recordedUrl(p,sid){selected(p,sid);return '/dollar.html?series='+sid+'&run='+p.replay.manifest_key.split('/').pop().slice(0,-5);}
  function review(row,at=Date.now()){if(!row.latest_observation)return 'Source unavailable';if(row.quality.status!=='within_age_ceiling')return row.quality.status.replaceAll('_',' ');
    return A.clock(row.source_review_due_at)&&at<Date.parse(row.source_review_due_at)?'Within recorded age ceilings; release calendar unverified':'Source review overdue; showing dated evidence';}
  const SCALE=100000000n;
  function fixed(value,label,signed=false){if(typeof value!=='string'||!(signed?/^-?\d{1,13}(?:\.\d{1,8})?$/:/^\d{1,13}(?:\.\d{1,8})?$/).test(value))throw Error(label+' requires an explicit decimal with up to 8 places');
    const sign=value.startsWith('-')?-1n:1n,[a,b='']=value.replace(/^-/,'').split('.'),out=sign*(BigInt(a)*SCALE+BigInt(b.padEnd(8,'0')));
    if(out>1000000000000n*SCALE||out< -1000000000000n*SCALE)throw Error(label+' exceeds the calculator bound');return out;}
  function display(value,places){const sign=value<0n?'-':'';value=value<0n?-value:value;const scale=10n**BigInt(places),fraction=(value%scale).toString().padStart(places,'0').replace(/0+$/,'');return sign+(value/scale).toString()+(fraction?'.'+fraction:'');}
  function scenario(p,sid,a){const row=selected(p,sid);if(!FX[sid]||!row.latest_observation||row.latest_observation.exact_value===null)throw Error('Choose a recorded currency observation first');
    const initial=fixed(a.initial,'Signed initial local-currency market value',true),ret=fixed(a.return_pct,'Assumed local asset return',true),entry=fixed(a.entry,'Assumed initial USD per local unit'),future=fixed(a.future,'Assumed future USD per local unit'),cost=fixed(a.cost,'Total assumed USD costs');
    if(initial===0n||entry<=0n||future<=0n||ret< -100n*SCALE)throw Error('Use nonzero local value, positive assumed exchange rates and local asset return of at least -100%');
    const initialUSD=initial*entry,local=initial*ret*entry,fx=initial*(future-entry)*100n*SCALE,interaction=initial*ret*(future-entry);
    const gross=local+fx+interaction,net=gross-cost*100n*SCALE*SCALE;
    return {contract:'dollar-local-asset-scenario.v1',currency:FX[sid][0],reporting_currency:'USD',run:p.replay,series_id:sid,
      context:row.latest_observation,assumptions:{...a},quote_direction:'USD per one '+FX[sid][0],initial_usd:display(initialUSD,16),
      local_return_effect_usd:display(local,26),fx_effect_usd:display(fx,26),interaction_effect_usd:display(interaction,26),
      gross_change_usd:display(gross,26),cost_usd:display(cost,8),net_change_usd:display(net,26),
      formula:'signed initial local value × ((1 + assumed local return / 100) × assumed future USD/local − assumed initial USD/local) − assumed USD costs',
      scope:'A hypothetical unhedged exposure with user-entered rates and constant signed units. Use zero local return for currency cash. No interim flows, hedge instruments, financing, taxes or portfolio recommendation are included.'};}
  const exact=v=>decimal(v)?v:'Unavailable';
  function formatted(value){if(!decimal(value))return 'Unavailable';const n=Number(value);return n!==0&&Math.abs(n)<0.000001?n.toExponential(4):n.toLocaleString('en-US',{maximumFractionDigits:6});}
  function comparisonsView(p,sid){const row=selected(p,sid);return A.table(['Calendar horizon','Baseline target','Actual baseline','Actual current','Level difference','Relative change %','USD strength %'],
    ['week','month','quarter','year'].map(label=>{const c=row.comparisons[label];return c?[label,c.target_date,c.baseline?.date??'Unavailable',c.current?.date??'Unavailable',formatted(c.difference.exact_value),formatted(c.relative_percent.exact_value),FX[sid]?formatted(c.dollar_strength.relative_percent.exact_value):'Not applicable']:[label,'Unavailable','Unavailable','Unavailable','Unavailable','Unavailable','Unavailable'];}),
    'Level differences use '+(row.unit==='Percent'?'percentage points':row.unit)+'. Relative changes are percentages; USD strength uses foreign units per USD. Display rounded to six decimal places; full precision is in the evidence export.');}
  function chart(p,sid){const r=selected(p,sid),points=r.history,numeric=points.filter(p=>p.value!==null);if(numeric.length<2)return '<p>Too few reported values for a chart.</p>';
    const width=980,height=240,pad=40,min=Math.min(...numeric.map(p=>p.value)),max=Math.max(...numeric.map(p=>p.value)),span=max-min||1;
    const first=Date.parse(points[0].date),last=Date.parse(points.at(-1).date),x=p=>pad+(width-2*pad)*(Date.parse(p.date)-first)/(last-first||1),y=p=>height-pad-(height-2*pad)*(p.value-min)/span;
    let path='',open=false;for(const point of points){if(point.value===null){open=false;continue;}path+=(open?'L':'M')+x(point).toFixed(2)+','+y(point).toFixed(2)+' ';open=true;}
    return '<svg class="dr-chart" role="img" aria-label="'+A.esc(r.label+'; '+r.unit+'; current source vintage')+'" viewBox="0 0 '+width+' '+height+'"><path d="'+path+'" fill="none" stroke="#8dd6ed" stroke-width="2"/><text x="6" y="20">'+A.esc(max.toLocaleString('en-US',{maximumFractionDigits:5}))+'</text><text x="6" y="'+(height-18)+'">'+A.esc(min.toLocaleString('en-US',{maximumFractionDigits:5}))+'</text><text x="40" y="'+(height-2)+'">'+points[0].date+'</text><text x="'+(width-130)+'" y="'+(height-2)+'">'+points.at(-1).date+'</text></svg><p>'+A.esc(r.unit)+' · '+points.length+' returned dated rows; missing values break the line. Chart is the current retained vintage, not point-in-time investment history.</p>';}
  const api={CONTRACT,CURRENT,PREFIX,SERIES,FX,typed,load,verifyPacket,recordedRun,selected,evidence,recordedUrl,review,scenario,comparisonsView,chart,exact,formatted};
  root.JHDollarResearch=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
