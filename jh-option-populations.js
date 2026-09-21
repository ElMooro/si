(function(root){
  'use strict';
  const A=root.JHOptionResearch||(typeof require==='function'?require('./jh-option-research.js'):null);
  const CONTRACT='option-population-desk.v1',PREFIX='data/option-population-research/',CURRENT='data/option-population-research.json',MAX=16*1024*1024;
  const flags=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
  const FIELDS={reported_open_interest:'contracts',gamma_oi_shares:'shares_per_USD_underlying_move',delta_oi_shares:'equivalent_underlying_shares'};
  const packets=new WeakSet(),owners=new WeakMap(),members=new WeakMap(),groupOwners=new WeakMap(),sources=new WeakMap();
  const object=v=>!!v&&typeof v==='object'&&!Array.isArray(v),count=v=>Number.isSafeInteger(v)&&v>=0;
  const digest=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v),decimal=v=>typeof v==='string'&&/^-?\d+(?:\.\d+)?$/.test(v)&&v.length<=260;
  const authority=v=>object(v)&&flags.every(k=>v[k]===false)&&v.call===null&&v.score===null&&v.portfolio_action==='WAIT'&&v.independent_investment_votes===0;
  function freeze(v){if(object(v)||Array.isArray(v)){Object.values(v).forEach(freeze);Object.freeze(v);}return v;}
  const safe=k=>typeof k==='string'&&/^data\/option-population-research\/(?:runs|outputs|chains|groups)\/[a-f0-9]{64}\.json$/.test(k);
  const ref=(r,kind)=>object(r)&&digest(r.sha256)&&r.key===PREFIX+kind+'/'+r.sha256+'.json'&&count(r.bytes)&&r.bytes>0&&r.bytes<=MAX;
  function cell(c,field){
    return object(c)&&c.unit===FIELDS[field]&&(c.value===null||decimal(c.value))&&count(c.included_rows)&&count(c.identity_population_rows)&&
      count(c.excluded_rows)&&c.included_rows+c.excluded_rows===c.identity_population_rows&&
      (c.value!==null)===(c.included_rows>0)&&c.complete_field_coverage===(c.identity_population_rows>0&&c.excluded_rows===0)&&
      c.observation_time===null&&object(c.exclusion_reasons)&&Object.values(c.exclusion_reasons).every(count)&&
      Object.values(c.exclusion_reasons).reduce((a,b)=>a+b,0)===c.excluded_rows;
  }
  function totals(v){
    return object(v)&&object(v.counts)&&object(v.sides)&&['call','put'].every(side=>object(v.sides[side])&&Object.keys(FIELDS).every(f=>cell(v.sides[side][f],f)));
  }
  function typed(p){
    return p?.contract===CONTRACT&&authority(p)&&A.clock(p.generated_at)&&A.clock(p.source_capture_completed_at)&&
      Date.parse(p.generated_at)>=Date.parse(p.source_capture_completed_at)&&Array.isArray(p.universe)&&p.universe.length===10&&
      new Set(p.universe).size===10&&p.universe.every(A.ticker)&&object(p.underlyings)&&A.same(Object.keys(p.underlyings).sort(),p.universe.slice().sort())&&
      p.universe.every(t=>ref(p.underlyings[t].population,'chains')&&ref(p.underlyings[t].source_membership,'groups')&&totals(p.underlyings[t].totals))&&
      p.quality?.independent_oi_greek_clocks_verified===false&&p.quality?.dealer_ownership_observed===false&&A.clock(p.quality.acquisition_review_due_at)&&
      p.observed_dealer_inventory===null&&p.zero_gamma_flip===null&&p.dealer_hedging_flow===null;
  }
  async function load(key,fetcher,signal){
    if(key!==CURRENT&&!safe(key))throw Error('Unapproved population evidence path');
    const response=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});
    if(!response.ok)throw Error('Population evidence request failed');
    if(Number(response.headers?.get('content-length'))>MAX)throw Error('Population response byte bound');
    let raw;
    if(response.body?.getReader){
      const reader=response.body.getReader(),parts=[];let length=0;
      try{for(;;){const part=await reader.read();if(part.done)break;length+=part.value.byteLength;if(length>MAX)throw Error('Population response byte bound');parts.push(part.value);}}
      catch(error){await reader.cancel();throw error;}
      const joined=new Uint8Array(length);let offset=0;for(const part of parts){joined.set(part,offset);offset+=part.byteLength;}raw=joined.buffer;
    }else raw=await response.arrayBuffer();
    if(raw.byteLength>MAX)throw Error('Population response byte bound');
    return {raw,doc:JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw))};
  }
  async function retained(identity,kind,fetcher,signal){
    if(!ref(identity,kind))throw Error('Exact population artifact identity required');
    const value=await load(identity.key,fetcher,signal);
    if(value.raw.byteLength!==identity.bytes||await A.sha(value.raw)!==identity.sha256)throw Error('Population artifact bytes differ');
    return value.doc;
  }
  async function verifyPacket(p,fetcher,signal){
    if(!typed(p))throw Error('Native population research contract required');
    const key=p.replay?.manifest_key;if(!safe(key)||!key.startsWith(PREFIX+'runs/'))throw Error('Population run identity differs');
    const item=await load(key,fetcher,signal),run=item.doc;
    if(key!==PREFIX+'runs/'+await A.sha(item.raw)+'.json'||run.contract!=='option-population-replay.v1'||run.generated_at!==p.generated_at||
      run.output_sha256!==p.replay.output_sha256||run.output?.sha256!==run.output_sha256)throw Error('Population run differs');
    const out=await retained(run.output,'outputs',fetcher,signal),{replay,...body}=p;
    if(!A.same(body,out))throw Error('Population body differs from retained output');
    const sourceKey=p.source_run?.manifest_key;
    if(typeof sourceKey!=='string'||!/^data\/option-flow-research\/runs\/[a-f0-9]{64}\.json$/.test(sourceKey))throw Error('Pinned option source required');
    const source=await A.recordedRun(sourceKey.split('/').pop().slice(0,-5),fetcher,signal);
    if(!A.same(source.replay,p.source_run)||source.generated_at!==p.source_capture_completed_at||
      p.universe.some(t=>!A.same(p.underlyings[t].source_chain,source.chains[t]?.chain)))throw Error('Population source binding differs');
    packets.add(p);sources.set(p,source);return freeze(p);
  }
  async function recordedRun(id,fetcher,signal){
    if(!digest(id))throw Error('Exact recorded population run required');
    const key=PREFIX+'runs/'+id+'.json',item=await load(key,fetcher,signal),r=item.doc;
    if(await A.sha(item.raw)!==id||r.contract!=='option-population-replay.v1'||r.output_sha256!==r.output?.sha256)throw Error('Recorded population run differs');
    return verifyPacket({...await retained(r.output,'outputs',fetcher,signal),replay:{manifest_key:key,output_sha256:r.output_sha256}},fetcher,signal);
  }
  async function chain(p,symbol,fetcher,signal){
    if(!packets.has(p)||!p.universe.includes(symbol))throw Error('Verified population underlying required');
    const item=p.underlyings[symbol],c=await retained(item.population,'chains',fetcher,signal);
    if(c.contract!=='option-population-research.v1'||!authority(c)||c.underlying!==symbol||!A.same(c.source_run,p.source_run)||
      !A.same(c.source_chain,item.source_chain)||!A.same(c.totals,item.totals)||c.group_count!==item.expiry_strike_groups||
      !Array.isArray(c.group_blocks)||c.group_blocks.length>1000||!digest(c.expanded_sha256))throw Error('Population chain differs');
    let n=0;for(const b of c.group_blocks){if(!ref(b.artifact,'groups')||b.offset!==n||!count(b.groups)||b.groups<1||b.groups>100)throw Error('Population group inventory differs');n+=b.groups;}
    if(n!==c.group_count)throw Error('Population group count differs');
    const index=await retained(item.source_membership,'groups',fetcher,signal);
    if(index.contract!=='option-population-membership.v1'||index.underlying!==symbol||!A.same(index.source_run,p.source_run)||
      !A.same(index.source_chain,c.source_chain)||!A.same(index.record_blocks,c.record_blocks)||!Array.isArray(index.groups)||index.groups.length!==c.group_count)throw Error('Population source membership differs');
    const positions=new Set();
    for(const g of index.groups){
      if(!/^\d{4}-\d{2}-\d{2}$/.test(g.expiration_date)||!decimal(g.strike_usd_per_share)||!Array.isArray(g.source_rows)||!g.source_rows.length)throw Error('Population group source identity differs');
      for(const r of g.source_rows){
        const id=r.source_page+':'+r.row_index;
        if(!count(r.source_page)||r.source_page<1||!count(r.row_index)||r.row_index>=250||positions.has(id)||
          typeof r.contract_id!=='string'||!['call','put'].includes(r.contract_type)||!Array.isArray(r.eligible_fields)||r.eligible_fields.some(f=>!Object.hasOwn(FIELDS,f)))throw Error('Population source row differs');
        positions.add(id);
      }
    }
    if(positions.size!==c.totals.counts.identity_eligible_rows)throw Error('Population membership row count differs');
    owners.set(c,p);members.set(c,freeze(index));return freeze(c);
  }
  async function groupBlock(c,index,fetcher,signal){
    if(!owners.has(c)||!count(index)||index>=c.group_blocks.length)throw Error('Verified group block required');
    const ref=c.group_blocks[index],b=await retained(ref.artifact,'groups',fetcher,signal);
    if(b.contract!=='option-population-groups.v1'||b.underlying!==c.underlying||!A.same(b.source_run,c.source_run)||!A.same(b.source_chain,c.source_chain)||
      b.offset!==ref.offset||!Array.isArray(b.groups)||b.groups.length!==ref.groups)throw Error('Population group block differs');
    for(let i=0;i<b.groups.length;i++){
      const g=b.groups[i],m=members.get(c).groups[ref.offset+i];
      if(g.expiration_date!==m.expiration_date||g.strike_usd_per_share!==m.strike_usd_per_share||!totals(g)||g.counts.identity_eligible_rows!==m.source_rows.length)throw Error('Population group membership differs');
      for(const side of ['call','put'])for(const f of Object.keys(FIELDS))if(g.sides[side][f].included_rows!==m.source_rows.filter(r=>r.contract_type===side&&r.eligible_fields.includes(f)).length)throw Error('Population field membership differs');
      groupOwners.set(g,{chain:c,member:m,index:ref.offset+i});freeze(g);
    }
    return b.groups;
  }
  function indices(c,expiry=''){
    if(!owners.has(c))throw Error('Verified population required');
    return members.get(c).groups.flatMap((g,i)=>!expiry||g.expiration_date===expiry?[i]:[]);
  }
  function expiries(c){if(!owners.has(c))throw Error('Verified population required');return [...new Set(members.get(c).groups.map(g=>g.expiration_date))];}
  async function sourceRows(g,fetcher,signal){
    const identity=groupOwners.get(g);if(!identity)throw Error('Verified selected group required');
    const c=identity.chain,p=owners.get(c),source=sources.get(p),chain=await A.chain(source,c.underlying,fetcher,signal),pages=new Map(),out=[];
    for(const entry of identity.member.source_rows){
      const n=chain.record_blocks.findIndex(b=>b.source_page===entry.source_page);
      if(n<0)throw Error('Contributing source page unavailable');
      if(!pages.has(n))pages.set(n,await A.records(chain,n,fetcher,signal));
      const row=pages.get(n).find(r=>r.evidence.row_index===entry.row_index);
      if(!row||row.contract_id!==entry.contract_id||row.contract_type!==entry.contract_type||row.expiration_date!==g.expiration_date||
        !row.identity_eligible||normalize(row.metrics.strike.value)!==normalize(g.strike_usd_per_share))throw Error('Contributing contract differs');
      const fields=[];if(row.metrics.open_interest.value!==null){fields.push('reported_open_interest');for(const k of ['gamma','delta'])if(row.metrics['vendor_'+k].value!==null)fields.push(k+'_oi_shares');}
      if(!A.same(fields,entry.eligible_fields))throw Error('Contributing fields differ');
      out.push({row,fields,url:A.recordedUrl(source,c.underlying)+'&page='+entry.source_page+'&row='+entry.row_index});
    }
    return out;
  }
  const normalize=v=>v.includes('.')?v.replace(/0+$/,'').replace(/\.$/,''):v;
  function display(value){
    if(!decimal(value))return 'Unavailable';
    const negative=value.startsWith('-'),text=negative?value.slice(1):value,[whole,raw='']=text.split('.'),fraction=raw.replace(/0+$/,'');
    if(fraction.length<=6)return A.exact((negative?'-':'')+whole+(fraction?'.'+fraction:''));
    let scaled=BigInt(whole)*1000000n+BigInt(fraction.slice(0,6));if(fraction[6]>='5')scaled++;
    if(scaled===0n)return negative?'-0.000001 < value < 0':'0 < value < 0.000001';
    const tail=(scaled%1000000n).toString().padStart(6,'0').replace(/0+$/,'');
    return '≈ '+A.exact((negative?'-':'')+(scaled/1000000n).toString()+(tail?'.'+tail:''));
  }
  const metric=c=>display(c?.value);
  const coverage=c=>c.included_rows+' / '+c.identity_population_rows+(c.excluded_rows?' · '+c.excluded_rows+' excluded':'');
  function overview(p,at=Date.now()){
    if(!packets.has(p))throw Error('Verified population required');
    return '<p class="or-clock">Source captured '+A.esc(p.source_capture_completed_at)+' · compiled '+A.esc(p.generated_at)+'</p><p>'+
      (at>=Date.parse(p.quality.acquisition_review_due_at)?'Acquisition review overdue. ':'Within acquisition review window. ')+
      'OI and Greek observation dates are not supplied. Compilation does not make those fields current.</p>'+A.table(
      ['Underlying','Returned rows','Valid identities','Call OI · contracts','Put OI · contracts'],p.universe.map(t=>{const x=p.underlyings[t];return ['<button type="button" data-op-pick="'+t+'">'+t+'</button>',x.totals.counts.returned_rows,x.totals.counts.identity_eligible_rows,metric(x.totals.sides.call.reported_open_interest),metric(x.totals.sides.put.reported_open_interest)];}),'Ten declared continuity underlyings',true);
  }
  function summary(c){
    if(!owners.has(c))throw Error('Verified population required');
    return '<h3>'+A.esc(c.underlying)+' · '+A.esc(c.capture_status.replaceAll('_',' '))+'</h3><p>'+c.totals.counts.returned_rows+' captured rows, '+c.totals.counts.identity_eligible_rows+' valid identities, '+c.group_count+' expiry/strike groups.</p>'+A.table(
      ['Population','Metric','Value','Unit','Included / identity rows'],['call','put'].flatMap(side=>Object.keys(FIELDS).map(f=>{const cell=c.totals.sides[side][f];return [side,f.replaceAll('_',' '),metric(cell),cell.unit,coverage(cell)];})),'Captured population coefficients')+
      '<p>Approximate display values use at most six decimal places; source arithmetic remains exact.</p><details><summary>Inspect exact aggregate values</summary>'+A.table(
      ['Population','Metric','Exact value','Unit'],['call','put'].flatMap(side=>Object.keys(FIELDS).map(f=>{const cell=c.totals.sides[side][f];return [side,f.replaceAll('_',' '),A.exact(cell.value),cell.unit];})),'Exact retained aggregate decimals')+'</details>'+
      '<p>These sums describe captured contracts. They do not identify the long or short owners, dealer positions, market support or resistance.</p>';
  }
  function groupTable(groups){return A.table(['Expiry / strike · USD/share','Call OI','Put OI','Call gamma × OI × 100','Put gamma × OI × 100','Gamma rows: calls / puts'],groups.map(g=>{
    const owner=groupOwners.get(g);if(!owner)throw Error('Verified groups required');
    return ['<button type="button" data-op-group="'+owner.index+'">'+A.esc(g.expiration_date)+' / '+A.exact(g.strike_usd_per_share)+'</button>',metric(g.sides.call.reported_open_interest),metric(g.sides.put.reported_open_interest),metric(g.sides.call.gamma_oi_shares),metric(g.sides.put.gamma_oi_shares),coverage(g.sides.call.gamma_oi_shares)+' / '+coverage(g.sides.put.gamma_oi_shares)];
  }),'Captured expiry and strike groups — gamma coefficients in shares per USD underlying move',true);}
  function evidence(items){return items.map(({row,fields,url})=>'<div class="or-evidence"><h3><a href="'+A.esc(url)+'">'+A.esc(row.contract_id)+' — source and expiration payoff</a></h3><p>Contributes: '+A.esc(fields.join(', ')||'No qualified aggregate fields')+'.</p>'+A.rowView(row)+'</div>').join('');}
  function recordedUrl(p,symbol,expiry=''){
    if(!packets.has(p)||!p.universe.includes(symbol))throw Error('Verified population underlying required');
    return '/gex/?underlying='+encodeURIComponent(symbol)+'&run='+p.replay.manifest_key.split('/').pop().slice(0,-5)+(expiry?'&expiry='+encodeURIComponent(expiry):'');
  }
  function captureDate(p){if(!packets.has(p))throw Error('Verified population required');return new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(p.source_capture_completed_at));}
  const api={CONTRACT,PREFIX,CURRENT,typed,load,verifyPacket,recordedRun,chain,groupBlock,indices,expiries,sourceRows,overview,summary,groupTable,evidence,recordedUrl,captureDate,display};
  root.JHOptionPopulations=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
