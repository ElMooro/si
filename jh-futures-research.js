(function(root){
  'use strict';
  const A=root.JHOptionResearch||(typeof require==='function'?require('./jh-option-research.js'):null);
  const CONTRACT='futures-original-research.v1',PREFIX='data/futures-research/',CURRENT='data/futures-research.json',MAX=16*1024*1024;
  const PRODUCTS={ES:'XCME',NQ:'XCME',CL:'XNYM',GC:'XCEC',SI:'XCEC',HG:'XCEC',NG:'XNYM'};
  const UNITS={ES:['IPNT','index point'],NQ:['IPNT','index point'],CL:['BBL','barrel'],GC:['TRYOZ','troy ounce'],SI:['TRYOZ','troy ounce'],HG:['LBS','pound'],NG:['MMBTU','MMBtu']};
  const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'],verified=new WeakSet(),rowsByPacket=new WeakMap();
  const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v),digest=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
  const authority=p=>object(p)&&FLAGS.every(k=>p[k]===false),count=v=>Number.isSafeInteger(v)&&v>=0;
  const decimal=v=>typeof v==='string'&&/^-?\d+(?:\.\d+)?$/.test(v)&&v.length<=260;
  const safe=key=>typeof key==='string'&&key.startsWith(PREFIX)&&/^(runs|outputs|records)\/[a-f0-9]{64}\.json$/.test(key.slice(PREFIX.length));
  function freeze(v){if((object(v)||Array.isArray(v))&&!Object.isFrozen(v)){Object.values(v).forEach(freeze);Object.freeze(v);}return v;}
  function typed(p){return p?.contract===CONTRACT&&authority(p)&&p.call===null&&p.score===null&&p.portfolio_action==='WAIT'&&p.independent_investment_votes===0&&
    A.clock(p.generated_at)&&A.clock(p.source_capture_completed_at)&&Date.parse(p.source_capture_completed_at)<=Date.parse(p.generated_at)&&object(p.products)&&object(p.datasets)&&
    A.same(Object.keys(p.products).sort(),Object.keys(PRODUCTS).sort())&&Object.keys(p.datasets).length<=42&&Object.entries(p.products).every(([name,f])=>
      f.product_code===name&&f.venue===PRODUCTS[name]&&authority(f)&&Array.isArray(f.contracts)&&f.contracts.length<=3&&f.contracts.every(authority))&&
    Object.values(p.datasets).every(d=>count(d.returned_rows)&&d.returned_rows<=20000&&Array.isArray(d.records)&&d.records.length<=160&&A.clock(d.source_capture_completed_at)&&Date.parse(d.source_capture_completed_at)<=Date.parse(p.generated_at));}
  async function load(key,fetcher,signal){
    if(key!==CURRENT&&!safe(key))throw Error('Unapproved futures evidence path');
    const response=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});
    if(!response.ok)throw Error('Recorded futures evidence is unavailable');
    if(Number(response.headers?.get('content-length'))>MAX)throw Error('futures response byte bound');
    let raw;
    if(response.body?.getReader){const reader=response.body.getReader(),parts=[];let length=0;
      try{for(;;){const item=await reader.read();if(item.done)break;length+=item.value.byteLength;if(length>MAX)throw Error('futures response byte bound');parts.push(item.value);}}
      catch(error){await reader.cancel();throw error;}
      const joined=new Uint8Array(length);let offset=0;for(const part of parts){joined.set(part,offset);offset+=part.byteLength;}raw=joined.buffer;
    }else raw=await response.arrayBuffer();
    if(!raw.byteLength||raw.byteLength>MAX)throw Error('futures response byte bound');
    return {raw,doc:JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw))};
  }
  async function retained(ref,kind,fetcher,signal){
    if(!['outputs','records'].includes(kind)||!digest(ref?.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+'.json'||!count(ref.bytes)||!ref.bytes||ref.bytes>MAX)throw Error('Exact futures artifact identity required');
    const item=await load(ref.key,fetcher,signal);if(item.raw.byteLength!==ref.bytes||await A.sha(item.raw)!==ref.sha256)throw Error('futures artifact bytes differ');return item.doc;
  }
  async function runOutput(identity,fetcher,signal){
    if(!safe(identity?.manifest_key)||!identity.manifest_key.startsWith(PREFIX+'runs/')||!digest(identity.output_sha256))throw Error('Exact futures run identity required');
    const item=await load(identity.manifest_key,fetcher,signal),run=item.doc;
    if(identity.manifest_key!==PREFIX+'runs/'+await A.sha(item.raw)+'.json'||run.contract!=='futures-original-replay.v1'||run.output_sha256!==identity.output_sha256||run.output?.sha256!==run.output_sha256)throw Error('futures run differs');
    const output=await retained(run.output,'outputs',fetcher,signal);if(output.generated_at!==run.generated_at)throw Error('futures run clock differs');return output;
  }
  async function verifyPacket(p,fetcher,signal){
    if(!typed(p))throw Error('Descriptive original futures research required');
    const output=await runOutput(p.replay,fetcher,signal),{replay,...body}=p;
    if(!A.same(output,body))throw Error('futures publication differs from recorded output');
    freeze(p);verified.add(p);rowsByPacket.set(p,new Map());return p;
  }
  async function recordedRun(id,fetcher,signal){
    if(!digest(id))throw Error('Exact recorded futures run required');
    const key=PREFIX+'runs/'+id+'.json',item=await load(key,fetcher,signal);
    if(await A.sha(item.raw)!==id)throw Error('Recorded futures run bytes differ');
    const identity={manifest_key:key,output_sha256:item.doc.output_sha256},output=await runOutput(identity,fetcher,signal);
    return verifyPacket({...output,replay:identity},fetcher,signal);
  }
  function selected(p,product){if(!verified.has(p)||!Object.hasOwn(PRODUCTS,product))throw Error('Verify and select a recorded futures product first');return p.products[product];}
  const date=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
  function rational(v){if(!decimal(v))throw Error('Exact recorded decimal required');const [whole,fraction='']=v.split('.');return [BigInt(whole+fraction),10n**BigInt(fraction.length)];}
  function exactEquals(v,n,d){return object(v)&&typeof v.numerator==='string'&&/^-?\d{1,520}$/.test(v.numerator)&&typeof v.denominator==='string'&&/^[1-9]\d{0,519}$/.test(v.denominator)&&BigInt(v.numerator)*d===n*BigInt(v.denominator);}
  function display(value,places){const sign=value<0n?'-':'';value=value<0n?-value:value;const scale=10n**BigInt(places),fraction=(value%scale).toString().padStart(places,'0').replace(/0+$/,'');return sign+(value/scale).toString()+(fraction?'.'+fraction:'');}
  function rounded(n,d){const sign=n<0n?-1n:1n;n=n<0n?-n:n;let q=n*1000000000000n/d;const rem=n*1000000000000n%d;if(2n*rem>d||(2n*rem===d&&q%2n===1n))q++;return display(sign*q,12);}
  function number(v){return object(v)&&object(v.number)?v.number:{state:v===undefined?'missing':v===null?'null':typeof v==='string'?'string':typeof v==='boolean'?'boolean':'nonnumeric',decimal:null};}
  const value=(r,k)=>number(r.values[k]).decimal;
  function cmp(a,b){const [an,ad]=rational(a),[bn,bd]=rational(b);return an*bd<bn*ad?-1:an*bd>bn*ad?1:0;}
  function source(s){return object(s)&&digest(s.original?.sha256)&&s.original.key==='audit-private/20260909-originals/futures-research/'+s.original.sha256+'.bin'&&count(s.original.bytes)&&s.original.bytes>0&&s.original.bytes<=MAX&&count(s.page)&&s.page>0&&/^\/results\/\d+$/.test(s.pointer)&&digest(s.request_sha256)&&A.clock(s.acquired_at);}
  async function dataset(p,name,fetcher,signal){
    const d=p.datasets[name];if(!d)throw Error('Missing futures dataset');const rows=[];
    for(const ref of d.records){const block=await retained(ref,'records',fetcher,signal);if(block.contract!=='futures-original-records.v1'||block.dataset!==name||block.offset!==rows.length||!Array.isArray(block.rows)||!block.rows.length||block.rows.length>128)throw Error('Futures record inventory differs');rows.push(...block.rows);}
    if(rows.length!==d.returned_rows||rows.some((r,i)=>r.ordinal!==i||!source(r.source)||Date.parse(r.source.acquired_at)>Date.parse(d.source_capture_completed_at)))throw Error('Futures source row identity differs');
    return rows;
  }
  function calendar(f,rows,d){
    const c=f.session_calendar,groups=new Map(),invalid=[];let repeats=0;
    rows.forEach((row,i)=>{const v=row.values;try{
      if(!object(v)||v.product_code!==f.product_code||v.trading_venue!==f.venue||!date(v.session_end_date)||v.session_end_date<d.scope.from||v.session_end_date>d.scope.to||!['open','pre_open','close'].includes(v.event)||!A.clock(v.timestamp))throw Error();
      const days=(Date.parse(new Date(v.timestamp).toISOString().slice(0,10))-Date.parse(v.session_end_date))/86400000;if(days< -7||days>1)throw Error();
      const stamp=new Date(v.timestamp).toISOString().replace('.000Z','+00:00'),key=v.event+'|'+stamp;
      if(!groups.has(v.session_end_date))groups.set(v.session_end_date,new Map());const g=groups.get(v.session_end_date);if(!g.has(key))g.set(key,[]);g.get(key).push(i);
    }catch{invalid.push(i);}});
    if(c.dataset!==f.product_code+':schedules'||c.product_code!==f.product_code||c.venue!==f.venue||c.exchange_calendar_timezone!=='America/Chicago'||c.returned_rows!==rows.length||c.pagination_complete!==d.pagination_complete||!A.same(c.invalid_source_row_ordinals,invalid)||!A.same(Object.keys(c.sessions).sort(),[...groups.keys()].sort()))throw Error('Futures calendar scope differs');
    let qualified=0;
    for(const [day,g] of groups){
      const events=[...g].sort(([a],[b])=>a<b?-1:a>b?1:0).map(([key,ordinals])=>{const [event,timestamp_utc]=key.split('|');return {event,timestamp_utc,source_row_ordinals:ordinals};});
      const opens=events.filter(e=>e.event==='open').map(e=>e.timestamp_utc),closes=events.filter(e=>e.event==='close').map(e=>e.timestamp_utc);
      const chicago=closes.length===1?new Intl.DateTimeFormat('en-CA',{timeZone:'America/Chicago',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(closes[0])):null;
      const eligible=!!(d.pagination_complete&&!invalid.length&&closes.length===1&&opens.length&&chicago===day&&opens.every(t=>Date.parse(t)<Date.parse(closes[0]))),s=c.sessions[day];
      if(!A.same(s.event_identities,events)||!A.same(s.unique_open_times,opens)||!A.same(s.unique_close_times,closes)||s.scheduled_close_qualified!==eligible||s.scheduled_close_utc!==(eligible?closes[0]:null)||s.bar_finality_independently_verified!==false)throw Error('Futures calendar end differs');
      if(eligible)qualified++;repeats+=events.reduce((n,e)=>n+e.source_row_ordinals.length-1,0);
    }
    if(c.qualified_session_closes!==qualified||c.repeated_event_identity_rows!==repeats)throw Error('Futures calendar coverage differs');
  }
  function session(f,r){const day=r.values.session_end_date,s=f.session_calendar.sessions[day],close=s?.scheduled_close_qualified?s.scheduled_close_utc:null,ended=close!==null?Date.parse(close)<=Date.parse(r.source.acquired_at):null;
    return {calendar_dataset:f.schedule_dataset,session_end_date:day,scheduled_close_utc:close,scheduled_session_ended_by_capture:ended,status:ended===true?'scheduled_session_ended':ended===false?'session_still_scheduled_open':'session_calendar_unqualified',bar_finality_independently_verified:false,source_capture_at:r.source.acquired_at};}
  function endpoint(f,r,field){return {ordinal:r.ordinal,ticker:r.values.ticker,session_end_date:r.values.session_end_date,window_start_ns:value(r,'window_start'),field,reported_decimal:value(r,field),source:r.source,session_status:session(f,r)};}
  function usable(r,field){if(!decimal(value(r,field)))return false;if(field==='settlement_price')return true;const fields=['open','high','low','close'];if(!fields.every(k=>decimal(value(r,k))))return true;
    return cmp(value(r,'low'),value(r,'open'))<=0&&cmp(value(r,'low'),value(r,'close'))<=0&&cmp(value(r,'high'),value(r,'open'))>=0&&cmp(value(r,'high'),value(r,'close'))>=0;}
  function verifyComparison(f,c,order,n,field,complete){
    if(!object(c)||!authority(c)||c.requested_row_offset!==n||c.price_field!==field)throw Error('Futures comparison identity differs');
    if(!complete||order.length<=n){if(c.available||c.from!==null||c.to!==null)throw Error('Unavailable futures comparison promoted');return;}
    const a=order.at(-1-n),b=order.at(-1),ok=usable(a,field)&&usable(b,field),days=(Date.parse(b.values.session_end_date)-Date.parse(a.values.session_end_date))/86400000;
    if(c.available!==ok||!A.same(c.from,endpoint(f,a,field))||!A.same(c.to,endpoint(f,b,field))||!A.same(c.source_row_ordinals,order.slice(-1-n).map(r=>r.ordinal))||c.elapsed_calendar_days!==days)throw Error('Futures comparison endpoints differ');
    if(!ok)return;const [an,ad]=rational(value(a,field)),[bn,bd]=rational(value(b,field)),dn=bn*ad-an*bd,dd=ad*bd;
    if(!exactEquals(c.absolute_change_exact,dn,dd)||c.absolute_change_decimal!==rounded(dn,dd))throw Error('Futures absolute arithmetic differs');
    if(an>0n){if(!exactEquals(c.percent_change_exact,100n*dn,bd*an)||c.percent_change_decimal!==rounded(100n*dn,bd*an))throw Error('Futures percentage arithmetic differs');}
    else if(c.percent_change_decimal!==null||c.percent_change_exact!==undefined)throw Error('Nonpositive base cannot supply a percentage return');
  }
  async function records(p,product,fetcher,signal){
    const f=selected(p,product),all={};
    const names=[product+':products',product+':contracts',product+':schedules',...f.contracts.map(c=>c.dataset)];
    for(const name of names)all[name]=await dataset(p,name,fetcher,signal);
    calendar(f,all[product+':schedules'],p.datasets[product+':schedules']);
    const spec=f.specification,meta=all[product+':products'],catalog=all[product+':contracts'];
    if(spec.available){if(meta.length!==1||!A.same(spec.provider_reported,meta[0].values)||!A.same(spec.source,meta[0].source))throw Error('Futures specification source differs');
      const v=meta[0].values,[unit,noun]=UNITS[product],q=number(v.unit_of_measure_qty),qualified=v.trade_currency_code==='USD'&&v.settlement_currency_code==='USD'&&v.unit_of_measure===unit&&v.price_quotation==='U.S. dollars and cents per '+noun&&q.state==='positive';
      if(v.product_code!==product||v.trading_venue!==f.venue||v.date!==p.definition_date||v.type!=='single'||spec.quantity_conversion_qualified!==qualified||spec.usd_value_per_price_unit_per_contract_decimal!==(qualified?q.decimal:null))throw Error('Futures quantity conversion differs');
    }else if(spec.quantity_conversion_qualified)throw Error('Unavailable specification promoted');
    const rows={};
    for(const c of f.contracts){
      const picked=f.contract_selection.selected.find(x=>x.ticker===c.ticker),definition=picked&&catalog[picked.source_row_index];
      if(!definition||!A.same(c.definition,definition.values)||!A.same(c.definition_source,definition.source)||definition.values.ticker!==c.ticker)throw Error('Futures contract definition differs');
      const values=all[c.dataset];rows[c.ticker]=values;let order=[];
      if(c.chronology_unambiguous){order=values.slice().sort((a,b)=>a.values.session_end_date.localeCompare(b.values.session_end_date));
        if(!order.length||order.some((r,i)=>r.values.ticker!==c.ticker||!date(r.values.session_end_date)||r.values.session_end_date<c.definition.first_trade_date||r.values.session_end_date>c.definition.last_trade_date||!/^\d{18,19}$/.test(value(r,'window_start'))||(i&&(r.values.session_end_date<=order[i-1].values.session_end_date||BigInt(value(r,'window_start'))<=BigInt(value(order[i-1],'window_start'))))))throw Error('Futures dated chronology differs');
        const latest=order.at(-1),l=c.latest_reported_row;
        if(l.ordinal!==latest.ordinal||l.ticker!==c.ticker||l.session_end_date!==latest.values.session_end_date||l.window_start_ns!==value(latest,'window_start')||!A.same(l.source,latest.source)||!A.same(l.session_status,session(f,latest)))throw Error('Latest futures row differs');
        for(const field of ['open','high','low','close','settlement_price','volume','dollar_volume','transactions','window_start'])if(!A.same(l.values[field],number(latest.values[field])))throw Error('Latest futures numeric field differs');
      }else if(c.latest_reported_row!==null)throw Error('Ambiguous chronology promoted');
      const ended=order.filter(r=>session(f,r).scheduled_session_ended_by_capture===true);
      if(c.coverage.returned_rows!==values.length||c.coverage.pagination_complete!==p.datasets[c.dataset].pagination_complete||!A.same(c.scheduled_ended_source_ordinals,ended.map(r=>r.ordinal))||c.coverage.scheduled_ended_rows!==ended.length||c.coverage.scheduled_open_rows!==values.filter(r=>session(f,r).scheduled_session_ended_by_capture===false).length||c.coverage.unqualified_calendar_rows!==values.filter(r=>session(f,r).scheduled_session_ended_by_capture===null).length)throw Error('Futures row/calendar coverage differs');
      for(const [group,ordered] of [['comparisons',order],['scheduled_ended_comparisons',ended]])for(const field of ['close','settlement_price'])for(const n of [1,5,20])verifyComparison(f,c[group][field][n],ordered,n,field,c.chronology_unambiguous&&c.coverage.pagination_complete);
    }
    for(const c of f.matched_curves){if(!authority(c))throw Error('Curve authority differs');if(!c.near||!c.far){if(c.available)throw Error('Missing curve endpoints');continue;}
      const ar=rows[c.near_ticker],br=rows[c.far_ticker];if(!ar||!br)throw Error('Curve contracts differ');
      const common=ar.map(r=>r.values.session_end_date).filter(d=>br.some(r=>r.values.session_end_date===d)).sort(),day=common.at(-1),a=ar.find(r=>r.values.session_end_date===day),b=br.find(r=>r.values.session_end_date===day);
      if(!a||!b||!['close','settlement_price'].includes(c.price_field)||!A.same(c.near,endpoint(f,a,c.price_field))||!A.same(c.far,endpoint(f,b,c.price_field))||c.session_end_date!==day||c.common_returned_sessions!==common.length)throw Error('Curve same-session evidence differs');
      const ok=usable(a,c.price_field)&&usable(b,c.price_field);if(c.available!==ok)throw Error('Curve availability differs');if(!ok)continue;
      const [an,ad]=rational(value(a,c.price_field)),[bn,bd]=rational(value(b,c.price_field));if(!exactEquals(c.far_minus_near_exact,bn*ad-an*bd,ad*bd)||c.far_minus_near_decimal!==rounded(bn*ad-an*bd,ad*bd))throw Error('Curve arithmetic differs');
    }
    freeze(all);rowsByPacket.get(p).set(product,{datasets:all,contracts:rows});return rows;
  }
  function evidence(p,product){const f=selected(p,product),r=rowsByPacket.get(p).get(product);if(!r)throw Error('Verify selected futures original record blocks first');return {contract:'futures-evidence-export.v1',product,run:p.replay,research:f,...r};}
  function recordedUrl(p,product,ticker){selected(p,product);return '/futures-research.html?product='+encodeURIComponent(product)+(ticker?'&ticker='+encodeURIComponent(ticker):'')+'&run='+p.replay.manifest_key.split('/').pop().slice(0,-5);}
  const SCALE=100000000n;
  function fixed(v,label,signed=false){if(typeof v!=='string'||!(signed?/^-?\d{1,13}(?:\.\d{1,8})?$/:/^\d{1,13}(?:\.\d{1,8})?$/).test(v))throw Error(label+' requires an explicit decimal with up to 8 places');const sign=v.startsWith('-')?-1n:1n,[a,b='']=v.replace(/^-/,'').split('.'),n=sign*(BigInt(a)*SCALE+BigInt(b.padEnd(8,'0')));if(n>1000000000000n*SCALE||n< -1000000000000n*SCALE)throw Error(label+' exceeds calculator bounds');return n;}
  function scenario(p,product,ticker,assumptions){
    const f=selected(p,product);evidence(p,product);const c=f.contracts.find(v=>v.ticker===ticker),spec=f.specification;
    if(!c?.chronology_unambiguous||!c.latest_reported_row||!spec.quantity_conversion_qualified)throw Error('Verified dated contract and USD quotation conversion required');
    const q=assumptions.quantity;if(typeof q!=='string'||!/^[-]?\d{1,7}$/.test(q)||BigInt(q)===0n||BigInt(q)>1000000n||BigInt(q)< -1000000n)throw Error('Enter a nonzero signed whole contract count, up to 1000000');
    const entry=fixed(assumptions.entry,'Assumed entry price',true),future=fixed(assumptions.future,'Assumed future price',true),cost=fixed(assumptions.cost,'Total USD costs'),[mn,md]=rational(spec.usd_value_per_price_unit_per_contract_decimal),gross=BigInt(q)*(future-entry)*mn,den=SCALE*md,net=gross-cost*md;
    return {calculator_contract:'futures-explicit-exposure.v1',product,ticker,currency:'USD',assumptions:{...assumptions},usd_value_per_price_unit_per_contract_decimal:spec.usd_value_per_price_unit_per_contract_decimal,
      gross_usd:rounded(gross,den),net_usd:rounded(net,den),cost_usd:display(cost,8),gross_exact:{numerator:gross.toString(),denominator:den.toString()},net_exact:{numerator:net.toString(),denominator:den.toString()},run:p.replay,definition:c.definition,specification_source:spec.source,
      formula:'Signed contracts × provider USD value per price unit × (assumed future price − assumed entry price) − total assumed USD costs',
      scope:'Hypothetical price exposure, rounded to 12 decimal places; exact fractions are included in the export. This is not a forecast or sizing advice. Margin calls, financing, daily settlement cash flows, delivery, expiry, rolls, taxes and reporting-currency conversion require separate analysis.'};
  }
  function comparisonsView(p,product,ticker,mode='scheduled_ended_comparisons'){const f=selected(p,product),c=f.contracts.find(v=>v.ticker===ticker);if(!c||!['comparisons','scheduled_ended_comparisons'].includes(mode))throw Error('Select a recorded contract and comparison view');return A.table(['Price field','Row offset','Start session','End session','Calendar days','Absolute price change','Change %','State'],['close','settlement_price'].flatMap(field=>[1,5,20].map(n=>{const v=c[mode][field][n];return [field,n,v.from?.session_end_date??'Unavailable',v.to?.session_end_date??'Unavailable',v.elapsed_calendar_days??'Unavailable',v.available?A.exact(v.absolute_change_decimal):'Unavailable',v.available?A.exact(v.percent_change_decimal):'Unavailable',v.reason??v.percent_change_reason??'Reported endpoints verified'];})),'Separate close and settlement comparisons; offsets count returned rows, not trading days');}
  function rowsView(p,product,ticker,start=0){const f=selected(p,product),all=evidence(p,product).contracts[ticker];if(!all)throw Error('Select a recorded contract');const rows=all.slice(start,start+100);return A.table(['Original row','Session label','Reported close','Reported settlement','Contracts traded','Calendar at capture'],rows.map(r=>[r.ordinal,r.values.session_end_date,A.exact(value(r,'close')),A.exact(value(r,'settlement_price')),A.exact(value(r,'volume')),session(f,r).status]),'Original source rows '+(rows.length?start+1:0)+'–'+(start+rows.length)+' of '+all.length);}
  const api={CONTRACT,PREFIX,CURRENT,PRODUCTS,typed,load,retained,verifyPacket,recordedRun,records,evidence,recordedUrl,scenario,comparisonsView,rowsView,value,number,session};
  root.JHFuturesResearch=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
