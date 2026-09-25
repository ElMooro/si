(function(root){
  'use strict';
  const PREFIX='data/short-interest-research/',CONTRACT='short-interest-original-research.v1',HASH=/^[a-f0-9]{64}$/;
  const COMPILERS={offexchange_measurements:'b1d994f5a84a17df8eca2559f1de29cfe73297636aa086829025eb5232d64e18',short_interest_measurements:'9002bf37bb778b9737543b3641ff2e6a6d2dbe381b3bf0eaaa4700cf8ecacf63',short_interest_research_model:'11716dc109a04b4b7b34b40d5b5602abe26c518e5dd9708649257f091b0ba831',short_interest_research_store:'5b84503baf1268a4537f16d55f89be903a7ccf24cc64779205cb58d64f1c5dc2'};
  const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  function canonical(v){if(Array.isArray(v))return '['+v.map(canonical).join(',')+']';if(v&&typeof v==='object')return '{'+Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canonical(v[k])).join(',')+'}';return JSON.stringify(v);}
  async function hash(bytes){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',bytes)),n=>n.toString(16).padStart(2,'0')).join('');}
  async function get(fetcher,key,ref){
    if(key!=='data/short-interest.json'&&!new RegExp('^'+PREFIX+'(?:runs|inputs|outputs|records)/[a-f0-9]{64}\\.json$').test(key))throw Error('Invalid research artifact path');
    const response=await fetcher('/'+key,{cache:'no-store',credentials:'same-origin'});
    if(!response.ok)throw Error('Recorded research is unavailable (HTTP '+response.status+')');
    const bytes=new Uint8Array(await response.arrayBuffer());if(bytes.length>8*1024*1024)throw Error('Research artifact exceeds its size limit');
    const digest=await hash(bytes);
    if(ref&&(ref.key!==key||ref.bytes!==bytes.length||ref.sha256!==digest))throw Error('Recorded artifact verification failed');
    if(key!=='data/short-interest.json'&&key.split('/').pop().slice(0,64)!==digest)throw Error('Recorded artifact identity differs');
    return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
  }
  function valid(p){return p&&p.contract===CONTRACT&&FLAGS.every(k=>p[k]===false)&&p.call===null&&p.signal===null&&p.score===null&&p.short_float_pct===null&&p.daily_short_volume_pct===null&&p.price_change_pct===null&&p.decision?.abstain===true&&p.decision?.eligible_votes===0&&Object.keys(p.by_ticker||{}).length===0&&Array.isArray(p.board)&&p.board.length===0;}
  async function load(fetcher,pinned){
    let reference,current=null;
    if(pinned!==null&&pinned!==undefined){if(!HASH.test(pinned))throw Error('Invalid recorded run');reference=PREFIX+'runs/'+pinned+'.json';}
    else{current=await get(fetcher,'data/short-interest.json');if(!valid(current)||!current.replay)throw Error('Native short-interest research is not available yet');reference=current.replay.manifest_key;}
    const run=await get(fetcher,reference);
    if(run.contract!=='short-interest-original-replay.v1'||!HASH.test(run.output_sha256)||JSON.stringify(Object.keys(run.compilers||{}).sort())!==JSON.stringify(Object.keys(COMPILERS).sort()))throw Error('Unsupported recorded calculation');
    for(const [name,digest] of Object.entries(COMPILERS)){const ref=run.compilers[name];if(ref.sha256!==digest||ref.key!==PREFIX+'compilers/'+digest+'.py')throw Error('Unqualified calculation version');}
    const packet=await get(fetcher,run.output.key,run.output);
    if(!valid(packet)||run.output.sha256!==run.output_sha256||packet.generated_at!==run.generated_at)throw Error('Recorded publication differs from its run');
    if(current){const body={...current};delete body.replay;if(current.replay.output_sha256!==run.output_sha256||await hash(new TextEncoder().encode(canonical(body)))!==run.output_sha256)throw Error('Current head differs from its recorded publication');}
    return {packet,run,reference,runId:reference.split('/').pop().slice(0,64)};
  }
  async function record(fetcher,state,symbol,identity){
    const entry=state.packet.symbols.find(v=>v.symbol===symbol);if(!entry)throw Error('Symbol is not in this recorded dataset');
    const issue=identity?entry.issues.find(v=>v.record_id===identity):entry.issues.length===1?entry.issues[0]:null;
    if(!issue)throw Error('Choose the exact reported issue; this symbol has multiple identities');
    const ref=state.packet.record_shards[issue.record_id.slice(0,2)];if(!ref)throw Error('Missing recorded issue bucket');
    const shard=await get(fetcher,ref.key,ref),row=shard.records?.[issue.record_id];
    if(shard.contract!=='short-interest-record-shard.v1'||!row||row.identity?.symbolCode!==symbol||FLAGS.some(k=>row[k]!==false))throw Error('Invalid descriptive issue record');
    const grain=['symbolCode','issueName','issuerServicesGroupExchangeCode','marketClassCode'].map(k=>row.identity[k]);
    if(await hash(new TextEncoder().encode(canonical(grain)))!==issue.record_id)throw Error('Reported issue identity differs');
    return {row,artifact:ref,issue};
  }
  function observation(state,selected,stamp){
    if(!state.packet.dates.includes(stamp))throw Error('Invalid settlement date');
    const fields=state.packet.point_fields,position=fields.indexOf('settlementDate');
    const raw=selected.row.observations.find(v=>v[position]===stamp),population=state.packet.settlement_populations.find(v=>v.settlement_date===stamp);
    if(!raw)return {date:stamp,point:null,source:null,population,missing_reason:'This exact reported issue is absent from the selected settlement population'};
    if(raw.length!==fields.length)throw Error('Incomplete recorded observation');
    const point=Object.fromEntries(fields.map((key,i)=>[key,raw[i]])),source=state.packet.sources[point.source_index];
    if(!source||source.settlement_date!==stamp||source.pass!==1||!Number.isInteger(point.source_row)||point.source_row<0||point.source_row>=source.rows)throw Error('Recorded source locator differs');
    return {date:stamp,point,source,population,missing_reason:null};
  }
  function decimal(value){if(typeof value!=='string'||!/^[-+]?\d{1,18}(?:\.\d{1,8})?$/.test(value.trim()))throw Error('Use a decimal number with at most eight decimal places');const text=value.trim(),negative=text[0]==='-',parts=text.replace(/^[-+]/,'').split('.');return (negative?-1n:1n)*BigInt(parts[0]+(parts[1]||'').padEnd(8,'0'));}
  function exact(value,scale){const negative=value<0n;let text=(negative?-value:value).toString().padStart(scale+1,'0');if(scale)text=text.slice(0,-scale)+'.'+text.slice(-scale);return (negative?'-':'')+text.replace(/(\.\d*?)0+$/,'$1').replace(/\.$/,'');}
  function scenario(a){const q=decimal(a.shares),p=decimal(a.price),shock=decimal(a.shock),cost=decimal(a.cost);if(p<=0n||cost<0n||shock< -100n*10n**8n||shock>1000n*10n**8n)throw Error('Use a positive price, nonnegative costs, and a price change from −100% to 1,000%');return {pnl_usd:exact(q*p*shock-cost*10n**18n,26),signed_notional_usd:exact(q*p,16),assumptions:{...a},forecast:false};}
  function decisionView(){return {by_ticker:{},items:[],rows:[],tracker:[],tickers:{},squeeze_candidates:[],call:null,score:null,short_interest:null,days_to_cover:null,short_float_pct:null,calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false,research_url:'/short-interest-research.html',evidence_note:'Reported settlement positions are descriptive research; they do not qualify a squeeze, covering or sizing recommendation.'};}
  const api={PREFIX,CONTRACT,COMPILERS,load,record,observation,scenario,decisionView,canonical,hash};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHShortInterestResearch=api;
})(typeof globalThis!=='undefined'?globalThis:this);
