(function(root){
  'use strict';
  const PREFIX='data/short-volume-research/', CONTRACT='short-volume-original-research.v1';
  const COMPILERS={offexchange_measurements:'b1d994f5a84a17df8eca2559f1de29cfe73297636aa086829025eb5232d64e18',short_volume_source_index:'9f46b8b8d27e22f206b2407bd7a482cd2a3a2e3fd5fe181f5ac714f1241d0e47',short_volume_measurements:'d87fb1d8a6728467c1de0a65bae9bd978a6db55e7f29b97050126d254e5a3e2a',short_volume_research_model:'735eafe735c09577cbef45095c5b439031a413c8ae62c310b2133c401e6117ce',short_volume_research_store:'f6db72529b80fe7f0211decc879769330e6f36c352cb48f116cb60e4a18bcc98'};
  const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  const HASH=/^[a-f0-9]{64}$/;
  function canonical(value){if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';if(value&&typeof value==='object')return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+canonical(value[k])).join(',')+'}';return JSON.stringify(value);}
  async function hash(bytes){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',bytes)),n=>n.toString(16).padStart(2,'0')).join('');}
  function encoded(value){return new TextEncoder().encode(canonical(value));}
  async function get(fetcher,key,ref){
    if(key!=='data/finra-short.json'&&!new RegExp('^'+PREFIX+'(?:runs|inputs|outputs|records)/[a-f0-9]{64}\\.json$').test(key))throw Error('Invalid research artifact path');
    const response=await fetcher('/'+key,{cache:'no-store',credentials:'same-origin'});
    if(!response.ok)throw Error('Recorded research is unavailable (HTTP '+response.status+')');
    const bytes=new Uint8Array(await response.arrayBuffer());if(bytes.length>8*1024*1024)throw Error('Research artifact exceeds its size limit');
    const digest=await hash(bytes);
    if(ref&&(ref.key!==key||ref.bytes!==bytes.length||ref.sha256!==digest))throw Error('Recorded artifact verification failed');
    if(key!=='data/finra-short.json'&&key.split('/').pop().slice(0,64)!==digest)throw Error('Recorded artifact identity differs');
    return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
  }
  function validPacket(p){return p&&p.contract===CONTRACT&&FLAGS.every(k=>p[k]===false)&&p.call===null&&p.signal===null&&p.score===null&&p.short_interest_shares===null&&p.days_to_cover===null&&p.decision?.abstain===true&&p.decision?.eligible_votes===0&&Array.isArray(p.board)&&p.board.length===0;}
  async function load(fetcher,pinned){
    let reference,current=null;
    if(pinned!==null&&pinned!==undefined){if(!HASH.test(pinned))throw Error('Invalid recorded run');reference=PREFIX+'runs/'+pinned+'.json';}
    else{current=await get(fetcher,'data/finra-short.json');if(!validPacket(current)||!current.replay)throw Error('Native short-volume research is not available yet');reference=current.replay.manifest_key;}
    const run=await get(fetcher,reference);
    if(run.contract!=='short-volume-original-replay.v1'||!HASH.test(run.output_sha256)||JSON.stringify(Object.keys(run.compilers||{}).sort())!==JSON.stringify(Object.keys(COMPILERS).sort()))throw Error('Unsupported recorded calculation');
    for(const [name,digest] of Object.entries(COMPILERS)){const ref=run.compilers[name];if(ref.sha256!==digest||ref.key!==PREFIX+'compilers/'+digest+'.py')throw Error('Unqualified calculation version');}
    const packet=await get(fetcher,run.output.key,run.output);
    if(!validPacket(packet)||run.output.sha256!==run.output_sha256||packet.generated_at!==run.generated_at)throw Error('Recorded publication does not match its run');
    if(current){const body={...current};delete body.replay;if(current.replay.output_sha256!==run.output_sha256||await hash(encoded(body))!==run.output_sha256)throw Error('Current head does not match its recorded publication');}
    return {packet,run,reference,runId:reference.split('/').pop().slice(0,64)};
  }
  async function record(fetcher,state,symbol){
    const entry=state.packet.symbols.find(row=>row.symbol===symbol);if(!entry)throw Error('Symbol is not in this recorded dataset');
    const ref=state.packet.record_shards[entry.bucket];if(!ref)throw Error('Missing recorded symbol bucket');
    const shard=await get(fetcher,ref.key,ref);
    if(shard.contract!=='short-volume-record-shard.v1'||!Object.hasOwn(shard.records||{},symbol))throw Error('Recorded symbol is unavailable');
    const row=shard.records[symbol];if(row.symbol!==symbol||FLAGS.some(k=>row[k]!==false))throw Error('Invalid descriptive record');
    return {row,artifact:ref};
  }
  function decimal(value){if(typeof value!=='string'||!/^[-+]?\d{1,18}(?:\.\d{1,8})?$/.test(value.trim()))throw Error('Use a decimal number with at most eight decimal places');const text=value.trim(),negative=text[0]==='-',parts=text.replace(/^[-+]/,'').split('.');return (negative?-1n:1n)*BigInt(parts[0]+(parts[1]||'').padEnd(8,'0'));}
  function exact(value,scale){const negative=value<0n;let text=(negative?-value:value).toString().padStart(scale+1,'0');if(scale)text=text.slice(0,-scale)+'.'+text.slice(-scale);return (negative?'-':'')+text.replace(/(\.\d*?)0+$/,'$1').replace(/\.$/,'');}
  function scenario(assumptions){
    const q=decimal(assumptions.shares),p=decimal(assumptions.price),shock=decimal(assumptions.shock),cost=decimal(assumptions.cost);
    if(p<=0n||cost<0n||shock< -100n*10n**8n||shock>1000n*10n**8n)throw Error('Use a positive price, nonnegative costs, and a price change from −100% to 1,000%');
    const pnl=q*p*shock-cost*10n**18n;
    return {pnl_usd:exact(pnl,26),signed_notional_usd:exact(q*p,16),assumptions:{...assumptions},forecast:false};
  }

  function shares(value){if(typeof value!=='string'||!/^\d{1,30}(?:\.\d{1,12})?$/.test(value))throw Error('Invalid exact share quantity');const parts=value.split('.');return BigInt(parts[0]+(parts[1]||'').padEnd(12,'0'));}
  function percentage(short,total){
    const s=shares(short),t=shares(total);if(s>t)throw Error('Short volume exceeds reported volume');if(t===0n)return null;
    const numerator=s*100n*10n**12n,q=numerator/t,r=numerator%t;return exact(q+(r*2n>t||(r*2n===t&&q%2n)?1n:0n),12);
  }
  function observation(state,selected,dateIndex){
    if(!Number.isInteger(dateIndex)||dateIndex<0||dateIndex>=61)throw Error('Invalid source date');
    const source=state.packet.daily_sources[dateIndex],date=state.packet.dates[dateIndex];
    if(source.observation_date!==date)throw Error('Source date differs');
    const p=selected.row.points.find(v=>v[0]===dateIndex);
    return {date,date_index:dateIndex,source,point:p||null,short_volume_pct:p?percentage(p[2],p[4]):null,
      missing_reason:!p?'Symbol absent from this source file':shares(p[4])===0n?'Zero reported volume':null};
  }

  function decisionView(){return {names:[],tickers:{},by_ticker:{},sectors:{},squeeze_candidates:[],top_zscore:[],market_composite:{},call:null,score:null,short_interest_shares:null,days_to_cover:null,calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false,research_url:'/short-volume-research.html',evidence_note:'Daily short-sale volume is descriptive trade reporting; it supplies no covering, short-interest or squeeze vote.'};}
  const api={decisionView,percentage,observation,PREFIX,CONTRACT,COMPILERS,load,record,scenario,canonical,hash};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHShortVolumeResearch=api;
})(typeof globalThis!=='undefined'?globalThis:this);
