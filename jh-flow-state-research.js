(function(root){
 'use strict';
 const A=root.JHOptionResearch||(typeof require==='function'?require('./jh-option-research.js'):null);
 const CURRENT='data/cross-asset-flow-state.json',PREFIX='data/flow-state-research/',CONTRACT='cross-asset-flow-research.v1',MAX=32*1024*1024;
 const SOURCES={
  'data/risk-regime.json':['risk-regime-research.v1','data/risk-regime-research/','risk-regime-replay.v1','Market risk observations'],
  'data/capital-inflows.json':['tic-original-research.v1','data/tic-research/','tic-original-replay.v1','Monthly TIC transactions'],
  'data/etf-true-flows.json':['etf-original-research.v1','data/etf-research/','etf-original-replay.v1','Issuer issuance estimates'],
  'data/dollar-radar.json':['dollar-original-research.v1','data/dollar-research/','dollar-original-replay.v1','Dollar and rates'],
  'data/fx-quote-research.json':['fx-original-quote-research.v1','data/fx-quote-research/','fx-original-replay.v1','Currency quotes']
 };
 const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'],verified=new WeakMap();
 const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v),digest=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
 const authority=p=>object(p)&&FLAGS.every(k=>p[k]===false),parentAuthority=p=>object(p)&&FLAGS.slice(0,3).every(k=>p[k]===false)&&p.forecast_qualified!==true;
 const sameSet=(a,b)=>A.same([...a].sort(),[...b].sort());
 function safe(key){return typeof key==='string'&&[PREFIX,...Object.values(SOURCES).map(s=>s[1])].some(p=>key.startsWith(p)&&/^(?:(runs|inputs|outputs)\/[a-f0-9]{64}\.json|compilers\/[a-f0-9]{64}\.py)$/.test(key.slice(p.length)));}
 function freeze(v){if(v&&typeof v==='object'&&!Object.isFrozen(v)){Object.values(v).forEach(freeze);Object.freeze(v);}return v;}
 async function load(key,fetcher,signal){
  if(key!==CURRENT&&!safe(key))throw Error('Unapproved composition evidence path');
  const response=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});
  if(!response.ok)throw Error('Recorded evidence is unavailable');
  if(Number(response.headers?.get('content-length'))>MAX)throw Error('Evidence byte bound');
  let raw;
  if(response.body?.getReader){const reader=response.body.getReader(),chunks=[];let length=0;
   try{for(;;){const v=await reader.read();if(v.done)break;length+=v.value.byteLength;if(length>MAX)throw Error('Evidence byte bound');chunks.push(v.value);}}
   catch(error){await reader.cancel();throw error;}
   const bytes=new Uint8Array(length);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}raw=bytes.buffer;
  }else raw=await response.arrayBuffer();
  if(!raw.byteLength||raw.byteLength>MAX)throw Error('Evidence byte bound');
  return {raw,text:new TextDecoder('utf-8',{fatal:true}).decode(raw)};
 }
 async function artifact(ref,prefix,kind,fetcher,signal){
  const ext=kind==='compilers'?'.py':'.json';
  if(!digest(ref?.sha256)||ref.key!==prefix+kind+'/'+ref.sha256+ext)throw Error('Exact artifact identity required');
  const item=await load(ref.key,fetcher,signal);
  if(await A.sha(item.raw)!==ref.sha256||('bytes' in ref&&(!Number.isSafeInteger(ref.bytes)||ref.bytes!==item.raw.byteLength)))throw Error('Recorded artifact bytes differ');
  return kind==='compilers'?item.text:JSON.parse(item.text);
 }
 async function runOutput(identity,prefix,contract,fetcher,signal){
  if(!digest(identity?.output_sha256)||!safe(identity?.manifest_key)||!identity.manifest_key.startsWith(prefix+'runs/'))throw Error('Exact recorded run required');
  const item=await load(identity.manifest_key,fetcher,signal),run=JSON.parse(item.text);
  if(identity.manifest_key!==prefix+'runs/'+await A.sha(item.raw)+'.json'||run.contract!==contract||run.output_sha256!==identity.output_sha256||run.output?.sha256!==identity.output_sha256)throw Error('Recorded run differs');
  if(!object(run.compilers)||Object.keys(run.compilers).length<1||Object.keys(run.compilers).length>16)throw Error('Recorded compilers required');
  const [output,input]=await Promise.all([artifact(run.output,prefix,'outputs',fetcher,signal),artifact(run.input,prefix,'inputs',fetcher,signal)]);
  for(const ref of Object.values(run.compilers))await artifact(ref,prefix,'compilers',fetcher,signal);
  if(!object(input)||run.generated_at!==output.generated_at)throw Error('Recorded compilation clock differs');
  return {run,input,output};
 }
 function fixed(value){if(typeof value!=='string'||value.length>80||! /^-?\d+(?:\.\d+)?$/.test(value))throw Error('Exact source decimal required');
  const [whole,fraction='']=value.replace(/^-/,'').split('.');return {n:(value.startsWith('-')?-1n:1n)*BigInt(whole+fraction),places:fraction.length};}
 function equalDecimal(a,b){const x=fixed(a),y=fixed(b),p=Math.max(x.places,y.places);return x.n*10n**BigInt(p-x.places)===y.n*10n**BigInt(p-y.places);}
 function formatAmount(value){if(value===null)return 'Unavailable';fixed(value);const [whole,fraction='']=value.split('.'),tail=fraction.replace(/0+$/,'');return whole.replace(/\B(?=(\d{3})+(?!\d))/g,',')+(tail?'.'+tail:'');}
 function sumDecimal(values){const parsed=values.map(fixed),p=Math.max(0,...parsed.map(v=>v.places));return {n:parsed.reduce((s,v)=>s+v.n*10n**BigInt(p-v.places),0n),places:p};}
 function resolve(doc,pointer){if(typeof pointer!=='string'||!pointer.startsWith('/'))throw Error('Recorded source pointer required');let value=doc;
  for(const token of pointer.slice(1).split('/')){const key=token.replaceAll('~1','/').replaceAll('~0','~');if(!value||typeof value!=='object'||!Object.hasOwn(value,key))throw Error('Recorded source pointer unavailable');value=value[key];}return value;}
 function verifyRows(p,parents){
  const etf=parents['data/etf-true-flows.json'],tic=parents['data/capital-inflows.json'];
  if(p.asset_class_rotation.length!==etf.category_rotation.length||p.monthly_transactions.length!==Object.keys(tic.by_asset_class).length*2)throw Error('Measurement inventory differs');
  const membership=new Map();
  p.asset_class_rotation.forEach((row,index)=>{
   const source=etf.category_rotation[index];
   if(!authority(row)||row.category!==source.category||row.unit!=='usd'||row.whole_market_total!==false||row.observed_cash_transfers!==false||
      row.source?.parent!=='data/etf-true-flows.json'||row.source.pointer!=='/category_rotation/'+index||!A.same(row.period,source.period)||
      !['configured_members','covered_members','unavailable_members'].every(k=>A.same(row[k],source[k]))||row.covered_count!==source.covered_members.length||row.configured_count!==source.configured_members.length)throw Error('Category coverage or scope differs');
   const values=source.covered_members.map(t=>{const window=etf.by_etf[t].flow_windows['5d'];if(window.start_date!==source.period.start_date||window.end_date!==source.period.end_date)throw Error('Mixed periods');
    if(!membership.has(t))membership.set(t,[]);membership.get(t).push(source.category);return window.value_decimal;});
   if(!values.length){if(row.value_decimal!==null||row.direction!=='unavailable'||source.value_decimal!==null)throw Error('Missing estimate became a value');}
   else{const total=sumDecimal(values),actual=fixed(row.value_decimal);if(total.n*10n**BigInt(actual.places)!==actual.n*10n**BigInt(total.places)||!equalDecimal(row.value_decimal,source.value_decimal))throw Error('Fund subtotal differs');
    if(row.direction!==(total.n>0n?'net_issuance_estimate':total.n<0n?'net_redemption_estimate':'unchanged_estimate'))throw Error('Estimate direction differs');}
  });
  const overlap=[...membership].sort(([a],[b])=>a.localeCompare(b)).filter(([,v])=>v.length>1).map(([ticker,categories])=>({ticker,categories,additive_across_categories:false}));
  if(!A.same(overlap,p.category_overlap))throw Error('Category overlap differs');
  const seen=new Set();
  for(const row of p.monthly_transactions){const id=row.asset_class+'|'+row.period,source=tic.by_asset_class[row.asset_class],window=source?.[row.period];
   if(seen.has(id)||!['latest','rolling_12mo'].includes(row.period)||!authority(row)||row.unit!=='usd_bn'||!window||row.value_decimal!==window.usd_bn_decimal||row.observation_date!==source.data_asof||row.series_id!==source.series_id||
      !A.same(row.months,window.months)||!A.same(row.missing_months,window.missing_months)||row.month_label_is_release_date!==false||row.price_or_valuation_change!==false||row.source?.parent!=='data/capital-inflows.json'||!A.same(resolve(tic,row.source.pointer),window))throw Error('Monthly transaction scope differs');
   if(row.value_decimal!==null){const bn=fixed(row.value_decimal),million=fixed(window.usd_million_decimal);if(bn.n*1000n*10n**BigInt(million.places)!==million.n*10n**BigInt(bn.places))throw Error('Transaction units differ');}seen.add(id);
  }
 }
 function typed(p){return p?.contract===CONTRACT&&authority(p)&&p.call===null&&p.score===null&&p.regime===null&&p.risk_regime_score===null&&p.independent_investment_votes===0&&p.category_total===null&&p.decision?.status==='abstain'&&
  A.clock(p.generated_at)&&object(p.parents)&&sameSet(Object.keys(p.parents),Object.keys(SOURCES))&&Array.isArray(p.asset_class_rotation)&&p.asset_class_rotation.length<=100&&Array.isArray(p.monthly_transactions)&&p.monthly_transactions.length<=100&&Array.isArray(p.category_overlap)&&
  object(p.hard_assets_and_dollar)&&Object.values(p.hard_assets_and_dollar).every(v=>v===null)&&A.same(p.dark_pool,{accumulation_n:null,distribution_n:null,top_accumulation:[],top_distribution:[]})&&p.quality?.source_original_replay_performed_by_composition===false;}
 async function verifyPacket(p,fetcher,signal){
  if(!typed(p))throw Error('Recorded descriptive composition required');const proof=await runOutput(p.replay,PREFIX,'flow-state-replay.v1',fetcher,signal),{replay,...body}=p;
  if(!A.same(body,proof.output)||!sameSet(Object.keys(proof.run.compilers),['flow_state_model','flow_state_store'])||proof.input.contract!=='flow-state-inputs.v1'||proof.input.generated_at!==p.generated_at)throw Error('Composition differs from recorded assembly');
  const entries=await Promise.all(Object.entries(SOURCES).map(async([key,spec])=>{
   const row=p.parents[key],source=await runOutput(row.replay,spec[1],spec[2],fetcher,signal),out=source.output;
   if(out.contract!==spec[0]||!parentAuthority(out)||!authority(row)||row.public_key!==key||!A.same(row.output,source.run.output)||row.generated_at!==out.generated_at||!A.clock(out.generated_at)||Date.parse(out.generated_at)>Date.parse(p.generated_at))throw Error('Parent identity or clock differs');
   return [key,out];
  }));
  const parents=Object.fromEntries(entries);verifyRows(p,parents);freeze(p);freeze(proof);freeze(parents);verified.set(p,{proof,parents});return p;
 }
 async function recordedRun(id,fetcher,signal){
  if(!digest(id))throw Error('Exact recorded flow run required');const key=PREFIX+'runs/'+id+'.json',item=await load(key,fetcher,signal);
  if(await A.sha(item.raw)!==id)throw Error('Recorded flow run bytes differ');const run=JSON.parse(item.text),ref={manifest_key:key,output_sha256:run.output_sha256};
  const output=await artifact(run.output,PREFIX,'outputs',fetcher,signal);return verifyPacket({...output,replay:ref},fetcher,signal);
 }
 function evidence(p){if(!verified.has(p))throw Error('Verify the recorded composition first');return {contract:'flow-state-evidence-export.v1',publication:p,...verified.get(p),verification:'Exact artifact hashes, parent bindings and displayed arithmetic. Provider-original replay and forecast qualification are not performed by this browser.'};}
 function recordedUrl(p){evidence(p);return '/cross-asset-flow.html?run='+p.replay.manifest_key.split('/').pop().slice(0,-5);}
 const api={CURRENT,PREFIX,CONTRACT,SOURCES,load,typed,verifyPacket,recordedRun,evidence,recordedUrl,verifyRows,equalDecimal,formatAmount};
 root.JHFlowStateResearch=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
