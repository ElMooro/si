(function(root){
  'use strict';
  const A=root.JHOptionResearch||(typeof require==='function'?require('./jh-option-research.js'):null);
  const CONTRACT='fx-original-quote-research.v1',PREFIX='data/fx-quote-research/',CURRENT='data/fx-quote-research.json',MAX=16*1024*1024;
  const PAIRS=['EUR_USD','USD_JPY','GBP_USD','USD_CHF','USD_CAD','AUD_USD','NZD_USD','USD_NOK','AUD_JPY','EUR_JPY','NZD_JPY','USD_CNH','USD_BRL','USD_MXN','USD_ZAR','USD_KRW','USD_TRY','XAU_USD','XAG_USD'];
  const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'],verified=new WeakSet(),rowsByPacket=new WeakMap();
  const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v),digest=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
  const authority=p=>object(p)&&FLAGS.every(k=>p[k]===false),count=v=>Number.isSafeInteger(v)&&v>=0;
  const decimal=v=>typeof v==='string'&&/^-?\d+(?:\.\d+)?$/.test(v)&&v.length<=260;
  const safe=key=>typeof key==='string'&&key.startsWith(PREFIX)&&/^(runs|outputs|bars)\/[a-f0-9]{64}\.json$/.test(key.slice(PREFIX.length));
  function freeze(v){if((object(v)||Array.isArray(v))&&!Object.isFrozen(v)){Object.values(v).forEach(freeze);Object.freeze(v);}return v;}
  function typed(p){return p?.contract===CONTRACT&&authority(p)&&p.call===null&&p.score===null&&p.portfolio_action==='WAIT'&&p.independent_investment_votes===0&&
    A.clock(p.generated_at)&&object(p.pairs)&&A.same(Object.keys(p.pairs).sort(),PAIRS.slice().sort())&&p.configured_pairs===19&&count(p.returned_rows)&&
    PAIRS.every(pair=>{const f=p.pairs[pair];return f.pair===pair&&f.provider_ticker==='C:'+pair.replace('_','')&&authority(f)&&
      A.clock(f.source_capture_completed_at)&&Date.parse(f.source_capture_completed_at)<=Date.parse(p.generated_at)&&A.clock(f.source_review_due_at)&&
      f.base_code===pair.slice(0,3)&&f.quote_code===pair.slice(4)&&count(f.coverage?.returned_rows)&&f.coverage.returned_rows<=2000&&
      Array.isArray(f.bar_blocks)&&f.bar_blocks.length<=16&&f.coverage.full_calendar_coverage_verified===false&&object(f.comparisons);});}
  async function load(key,fetcher,signal){
    if(key!==CURRENT&&!safe(key))throw Error('Unapproved FX evidence path');
    const response=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});
    if(!response.ok)throw Error('Recorded FX evidence is unavailable');
    if(Number(response.headers?.get('content-length'))>MAX)throw Error('FX response byte bound');
    let raw;
    if(response.body?.getReader){const reader=response.body.getReader(),parts=[];let length=0;
      try{for(;;){const item=await reader.read();if(item.done)break;length+=item.value.byteLength;if(length>MAX)throw Error('FX response byte bound');parts.push(item.value);}}
      catch(error){await reader.cancel();throw error;}
      const joined=new Uint8Array(length);let offset=0;for(const part of parts){joined.set(part,offset);offset+=part.byteLength;}raw=joined.buffer;
    }else raw=await response.arrayBuffer();
    if(!raw.byteLength||raw.byteLength>MAX)throw Error('FX response byte bound');
    return {raw,doc:JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw))};
  }
  async function retained(ref,kind,fetcher,signal){
    if(!['outputs','bars'].includes(kind)||!digest(ref?.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+'.json'||!count(ref.bytes)||!ref.bytes||ref.bytes>MAX)throw Error('Exact FX artifact identity required');
    const item=await load(ref.key,fetcher,signal);if(item.raw.byteLength!==ref.bytes||await A.sha(item.raw)!==ref.sha256)throw Error('FX artifact bytes differ');return item.doc;
  }
  async function runOutput(identity,fetcher,signal){
    if(!safe(identity?.manifest_key)||!identity.manifest_key.startsWith(PREFIX+'runs/')||!digest(identity.output_sha256))throw Error('Exact FX run identity required');
    const item=await load(identity.manifest_key,fetcher,signal),run=item.doc;
    if(identity.manifest_key!==PREFIX+'runs/'+await A.sha(item.raw)+'.json'||run.contract!=='fx-original-replay.v1'||run.output_sha256!==identity.output_sha256||run.output?.sha256!==run.output_sha256)throw Error('FX run differs');
    const output=await retained(run.output,'outputs',fetcher,signal);if(output.generated_at!==run.generated_at)throw Error('FX run clock differs');return output;
  }
  async function verifyPacket(p,fetcher,signal){
    if(!typed(p))throw Error('Descriptive original FX research required');
    const output=await runOutput(p.replay,fetcher,signal),{replay,...body}=p;
    if(!A.same(output,body))throw Error('FX publication differs from recorded output');
    freeze(p);verified.add(p);rowsByPacket.set(p,new Map());return p;
  }
  async function recordedRun(id,fetcher,signal){
    if(!digest(id))throw Error('Exact recorded FX run required');
    const key=PREFIX+'runs/'+id+'.json',item=await load(key,fetcher,signal);
    if(await A.sha(item.raw)!==id)throw Error('Recorded FX run bytes differ');
    const identity={manifest_key:key,output_sha256:item.doc.output_sha256},output=await runOutput(identity,fetcher,signal);
    return verifyPacket({...output,replay:identity},fetcher,signal);
  }
  function selected(p,pair){if(!verified.has(p)||!PAIRS.includes(pair))throw Error('Verify and select a recorded FX pair first');return p.pairs[pair];}
  const endpoint=r=>({ordinal:r.ordinal,window_start_utc:r.window_start_utc,reported_close_decimal:r.values.c.decimal,source:r.source,close_state:r.values.c.state,
    positive_close_usable_as_reported:r.positive_close_usable_as_reported,ohlc_bounds_consistent:r.ohlc_bounds_consistent,quote_observed_at:null,close_observed_at:null,bar_finality_verified:false});
  function rational(v){if(!decimal(v))throw Error('Exact recorded decimal required');const [whole,fraction='']=v.split('.');return [BigInt(whole+fraction),10n**BigInt(fraction.length)];}
  function exactEquals(value,n,d){return object(value)&&typeof value.numerator==='string'&&/^-?\d{1,520}$/.test(value.numerator)&&
    typeof value.denominator==='string'&&/^[1-9]\d{0,519}$/.test(value.denominator)&&BigInt(value.numerator)*d===n*BigInt(value.denominator);}
  function rounded(n,d){const sign=n<0n?-1n:1n;n=n<0n?-n:n;let q=n*1000000000000n/d;const remainder=n*1000000000000n%d;
    if(2n*remainder>d||(2n*remainder===d&&q%2n===1n))q++;return display(sign*q,12);}
  async function records(p,pair,fetcher,signal){
    const f=selected(p,pair),rows=[];
    for(const ref of f.bar_blocks){const block=await retained(ref,'bars',fetcher,signal);
      if(block.contract!=='fx-original-bars.v1'||block.pair!==pair||block.offset!==rows.length||!Array.isArray(block.rows)||!block.rows.length||block.rows.length>128)throw Error('FX bar block identity differs');
      rows.push(...block.rows);}
    if(rows.length!==f.coverage.returned_rows||rows.some((r,i)=>!object(r)||r.ordinal!==i||!object(r.values)||r.bar_finality_verified!==false||r.quote_observed_at!==null||r.close_observed_at!==null||
      !digest(r.source?.original?.sha256)||r.source.original.key!=='audit-private/20260909-originals/fx-research/'+r.source.original.sha256+'.bin'||
      !count(r.source.original.bytes)||!r.source.original.bytes||!/^\/results\/\d+$/.test(r.source.pointer)||!digest(r.source.request_sha256)||!A.clock(r.source.acquired_at)))throw Error('FX returned row inventory differs');
    const order=f.chronological_source_ordinals;
    if(!Array.isArray(order)||order.some(i=>!count(i)||i>=rows.length)||new Set(order).size!==order.length)throw Error('FX chronology index differs');
    if(f.chronology_unambiguous){if(order.length!==rows.length||!order.length||order.some((i,n)=>!A.clock(rows[i].window_start_utc)||(n&&Date.parse(rows[order[n-1]].window_start_utc)>=Date.parse(rows[i].window_start_utc)))||
        !A.same(f.latest_reported_row,endpoint(rows[order.at(-1)])))throw Error('FX dated chronology differs');}
    else if(order.length||f.latest_reported_row!==null)throw Error('Ambiguous FX chronology cannot have a selected latest row');
    for(const n of [1,5,20]){const c=f.comparisons[String(n)];if(!object(c)||!authority(c)||c.requested_row_offset!==n)throw Error('FX comparison identity differs');
      if(!c.available)continue;
      if(!f.chronology_unambiguous||!f.coverage.pagination_complete||order.length<=n)throw Error('Unavailable FX comparison promoted');
      const a=rows[order.at(-1-n)],b=rows[order.at(-1)],window=order.slice(-1-n);
      if(!A.same(c.from,endpoint(a))||!A.same(c.to,endpoint(b))||!a.positive_close_usable_as_reported||!b.positive_close_usable_as_reported||!A.same(c.source_row_ordinals,window)||c.returned_rows_in_window!==n+1)throw Error('FX comparison endpoints differ');
      const [an,ad]=rational(a.values.c.decimal),[bn,bd]=rational(b.values.c.decimal);
      if(an<=0n||bn<=0n||!exactEquals(c.quoted_rate_change_exact,100n*(bn*ad-an*bd),bd*an)||!exactEquals(c.inverse_rate_change_exact,100n*(an*bd-bn*ad),ad*bn))throw Error('FX exact comparison arithmetic differs');
      if(c.quoted_rate_change_decimal!==rounded(100n*(bn*ad-an*bd),bd*an)||c.inverse_rate_change_decimal!==rounded(100n*(an*bd-bn*ad),ad*bn)||
        c.elapsed_calendar_days_decimal!==rounded(BigInt(Date.parse(b.window_start_utc)-Date.parse(a.window_start_utc)),86400000n)||
        c.positive_close_rows_in_window!==window.filter(i=>rows[i].positive_close_usable_as_reported).length||c.unit!=='percent_change_in_reported_quote_close')throw Error('FX displayed comparison or dated span differs');
    }
    rows.forEach(freeze);rowsByPacket.get(p).set(pair,Object.freeze(rows));return rows;
  }
  function evidence(p,pair){selected(p,pair);const rows=rowsByPacket.get(p).get(pair);if(!rows)throw Error('Verify selected FX bar blocks first');return {contract:'fx-evidence-export.v1',pair,run:p.replay,research:p.pairs[pair],rows};}
  function recordedUrl(p,pair){selected(p,pair);return '/fx-research.html?pair='+pair+'&run='+p.replay.manifest_key.split('/').pop().slice(0,-5);}
  function review(f,at=Date.now()){return !A.clock(f?.source_review_due_at)?'Review deadline unknown':at<Date.parse(f.source_review_due_at)?'Within acquisition review window':'Acquisition review overdue';}
  const SCALE=100000000n;
  function fixed(v,label,signed=false){if(typeof v!=='string'||!(signed?/^-?\d{1,13}(?:\.\d{1,8})?$/:/^\d{1,13}(?:\.\d{1,8})?$/).test(v))throw Error(label+' requires a decimal with up to 8 places');
    const sign=v.startsWith('-')?-1n:1n,[a,b='']=v.replace(/^-/, '').split('.'),result=sign*(BigInt(a)*SCALE+BigInt(b.padEnd(8,'0')));
    if(result>1000000000000n*SCALE||result< -1000000000000n*SCALE)throw Error(label+' exceeds the calculator bound');return result;}
  function display(value,places){const sign=value<0n?'-':'';value=value<0n?-value:value;const scale=10n**BigInt(places),fraction=(value%scale).toString().padStart(places,'0').replace(/0+$/,'');return sign+(value/scale).toString()+(fraction?'.'+fraction:'');}
  function scenario(p,pair,assumptions){
    const f=selected(p,pair);evidence(p,pair);
    if(['XAU','XAG'].includes(f.base_code))throw Error('Metal quantity units are unverified; this calculator supports currency pairs only');
    if(!f.latest_reported_row?.positive_close_usable_as_reported)throw Error('A usable recorded comparison context is required');
    const quantity=fixed(assumptions.quantity,'Signed base-currency units',true),entry=fixed(assumptions.entry,'Assumed entry rate'),future=fixed(assumptions.future,'Assumed future rate'),cost=fixed(assumptions.cost,'Total assumed costs');
    if(quantity===0n||entry<=0n||future<=0n)throw Error('Use nonzero signed units and positive assumed rates');
    const gross=quantity*(future-entry),net=gross-cost*SCALE;
    return {calculator_contract:'fx-quote-exposure.v1',arithmetic:'exact_decimal_products_16_places',pair,quote_currency:f.quote_code,
      assumptions:{quantity:assumptions.quantity,entry:assumptions.entry,future:assumptions.future,cost:assumptions.cost},
      assumption_units:{quantity:'signed_'+f.base_code+'_units',entry:f.price_unit,future:f.price_unit,cost:f.quote_code+'_total'},
      gross_quote:display(gross,16),cost_quote:display(cost,8),net_quote:display(net,16),run:p.replay,context:f.latest_reported_row,
      formula:'signed base-currency units × (assumed future quote rate − assumed entry quote rate) − total assumed quote-currency costs',
      scope:'User-entered spot exposure assumptions, not a forecast or position recommendation. Financing, interest, margin, forwards/NDF settlement, taxes and reporting-currency conversion are outside this calculation.'};
  }
  function comparisonsView(p,pair){const f=selected(p,pair);return A.table(['Observed row offset','Start window (UTC)','End window (UTC)','Calendar-day span','Quoted-rate change %','Inverse-rate change %','Coverage'],
    [1,5,20].map(n=>{const c=f.comparisons[String(n)];return [n,c.from?.window_start_utc??'Unavailable',c.to?.window_start_utc??'Unavailable',A.exact(c.elapsed_calendar_days_decimal),
      c.available?A.exact(c.quoted_rate_change_decimal):'Unavailable',c.available?A.exact(c.inverse_rate_change_decimal):'Unavailable',c.available?(c.positive_close_rows_in_window+'/'+c.returned_rows_in_window+' usable closes'):c.reason];}),
    'Quote-close comparisons; window starts do not establish bar finality or close times');}
  function rowsView(p,pair,start=0){const proof=evidence(p,pair),rows=proof.rows.slice(start,start+100);return A.table(['Original row','Window start (UTC)','Reported close','Close state','OHLC consistent','Original location'],
    rows.map(r=>[r.ordinal,r.window_start_utc??'Unknown',A.exact(r.values.c.decimal),r.values.c.state,r.ohlc_bounds_consistent===null?'Unknown':String(r.ohlc_bounds_consistent),'Page '+r.source.page+' '+r.source.pointer]),'Returned source rows '+(rows.length?start+1:0)+'–'+(start+rows.length)+' of '+proof.rows.length);}
  const api={CONTRACT,PREFIX,CURRENT,PAIRS,typed,load,retained,verifyPacket,recordedRun,records,evidence,recordedUrl,review,scenario,comparisonsView,rowsView};
  root.JHFXResearch=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
