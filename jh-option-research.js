(function (root) {
  'use strict';
  const CONTRACT='option-flow-original-research.v1',PREFIX='data/option-flow-research/',CURRENT='data/option-flow-research.json',MAX=16*1024*1024;
  const flags=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'];
  const verifiedPackets=new WeakSet(),chainOwners=new WeakMap(),recordOwners=new WeakMap();
  const esc=v=>String(v??'Unavailable').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const object=v=>!!v&&typeof v==='object'&&!Array.isArray(v);
  const ticker=v=>typeof v==='string'&&/^[A-Z][A-Z0-9.]{0,9}$/.test(v);
  const digest=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
  const clock=v=>typeof v==='string'&&/(?:Z|[+-]\d\d:\d\d)$/.test(v)&&Number.isFinite(Date.parse(v));
  const count=v=>Number.isSafeInteger(v)&&v>=0;
  const decimal=v=>typeof v==='string'&&/^-?\d+(?:\.\d+)?$/.test(v)&&v.length<=260;
  const exact=v=>decimal(v)?v.replace(/^(-?\d+)(.*)$/,(_,a,b)=>a.replace(/\B(?=(\d{3})+(?!\d))/g,',')+b):'Unavailable';
  const authority=v=>object(v)&&flags.every(k=>v[k]===false);
  const stable=v=>Array.isArray(v)?v.map(stable):object(v)?Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])])):v;
  const same=(a,b)=>JSON.stringify(stable(a))===JSON.stringify(stable(b));
  function freeze(value){if(value&&typeof value==='object'&&!Object.isFrozen(value)){Object.values(value).forEach(freeze);Object.freeze(value);}return value;}
  async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');}
  const bytes=v=>new TextEncoder().encode(JSON.stringify(stable(v)));
  const safe=key=>typeof key==='string'&&/^data\/option-flow-research\/(?:runs|outputs|chains|rows)\/[a-f0-9]{64}\.json$/.test(key);
  function reference(ref,kind){return object(ref)&&digest(ref.sha256)&&ref.key===PREFIX+kind+'/'+ref.sha256+'.json'&&count(ref.bytes)&&ref.bytes>0&&ref.bytes<=MAX;}
  function typed(p){
    return p?.contract===CONTRACT&&authority(p)&&p.call===null&&p.score===null&&p.portfolio_action==='WAIT'&&p.independent_investment_votes===0&&clock(p.generated_at)&&
      object(p.chains)&&Array.isArray(p.universe?.selected)&&p.universe.selected.length>0&&p.universe.selected.length<=96&&
      new Set(p.universe.selected).size===p.universe.selected.length&&same(Object.keys(p.chains).sort(),p.universe.selected.slice().sort())&&
      p.universe.selected.every(t=>ticker(t)&&authority(p.chains[t])&&reference(p.chains[t].chain,'chains')&&count(p.chains[t].coverage?.returned_rows))&&
      count(p.quality?.returned_rows)&&clock(p.quality.acquisition_review_due_at)&&p.quality.independent_oi_greek_clocks_verified===false;
  }
  async function load(key,fetcher,signal){
    if(key!==CURRENT&&!safe(key))throw Error('Unapproved option evidence path');
    const response=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});
    if(!response.ok)throw Error('Option evidence request failed');
    if(Number(response.headers?.get('content-length'))>MAX)throw Error('Option response byte bound');
    let raw;
    if(response.body?.getReader){
      const reader=response.body.getReader(),parts=[];let length=0;
      try{for(;;){const part=await reader.read();if(part.done)break;length+=part.value.byteLength;if(length>MAX)throw Error('Option response byte bound');parts.push(part.value);}}
      catch(error){await reader.cancel();throw error;}
      const joined=new Uint8Array(length);let offset=0;for(const part of parts){joined.set(part,offset);offset+=part.byteLength;}raw=joined.buffer;
    }else raw=await response.arrayBuffer();
    if(raw.byteLength>MAX)throw Error('Option response byte bound');
    return {raw,doc:JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw))};
  }
  async function retained(ref,kind,fetcher,signal){
    if(!reference(ref,kind))throw Error('Exact option artifact identity required');
    const item=await load(ref.key,fetcher,signal);
    if(item.raw.byteLength!==ref.bytes||await sha(item.raw)!==ref.sha256)throw Error('Option artifact bytes differ');
    return item.doc;
  }
  async function verifyPacket(p,fetcher,signal){
    if(!typed(p))throw Error('Native option research contract required');
    const key=p.replay?.manifest_key;if(!safe(key)||!key.startsWith(PREFIX+'runs/'))throw Error('Option run identity differs');
    const item=await load(key,fetcher,signal),run=item.doc;
    if(key!==PREFIX+'runs/'+await sha(item.raw)+'.json'||run.contract!=='option-flow-replay.v1'||run.generated_at!==p.generated_at||
      run.output_sha256!==p.replay.output_sha256||run.output?.sha256!==run.output_sha256)throw Error('Option run differs');
    const output=await retained(run.output,'outputs',fetcher,signal),{replay,...body}=p;
    if(!same(body,output))throw Error('Current option research differs from retained output');
    verifiedPackets.add(p);return freeze(p);
  }
  async function recordedRun(id,fetcher,signal){
    if(!digest(id))throw Error('Exact recorded option run required');
    const key=PREFIX+'runs/'+id+'.json',item=await load(key,fetcher,signal),run=item.doc;
    if(await sha(item.raw)!==id||run?.contract!=='option-flow-replay.v1'||!clock(run.generated_at)||
      !reference(run.output,'outputs')||run.output_sha256!==run.output.sha256)throw Error('Recorded option run differs');
    const output=await retained(run.output,'outputs',fetcher,signal);
    return verifyPacket({...output,replay:{manifest_key:key,output_sha256:run.output_sha256}},fetcher,signal);
  }
  function recordedUrl(p,symbol){
    if(!verifiedPackets.has(p)||!ticker(symbol)||!p.chains[symbol])throw Error('Verified captured underlying required');
    return '/option-chain-research.html?underlying='+encodeURIComponent(symbol)+'&run='+p.replay.manifest_key.slice((PREFIX+'runs/').length,-5);
  }
  async function chain(p,symbol,fetcher,signal){
    if(!typed(p)||!verifiedPackets.has(p)||!ticker(symbol)||!p.chains[symbol])throw Error('Choose a captured underlying from a verified publication');
    const item=await retained(p.chains[symbol].chain,'chains',fetcher,signal);
    if(item.contract!=='option-research-chain.v1'||item.underlying!==symbol||!authority(item)||!same(item.coverage,p.chains[symbol].coverage)||
      !Array.isArray(item.record_blocks)||item.record_blocks.length>128||item.record_blocks.length!==p.chains[symbol].record_blocks||
      item.record_blocks.some(b=>!reference(b.artifact,'rows')||!Number.isInteger(b.source_page)||b.source_page<1||!count(b.rows)||b.rows>250||!digest(b.expanded_sha256))||
      item.record_blocks.reduce((n,b)=>n+b.rows,0)!==item.coverage.returned_rows)throw Error('Captured chain inventory differs');
    chainOwners.set(item,p);return freeze(item);
  }
  async function unpack(block){
    if(block?.contract!=='option-research-record-block.v1'||!object(block.source)||!object(block.common_record)||!object(block.field_definitions)||
      !Array.isArray(block.rows)||!count(block.row_count)||block.row_count<1||block.row_count>250||block.rows.length!==block.row_count||!digest(block.expanded_sha256)||
      ['metrics','evidence'].some(k=>Object.hasOwn(block.common_record,k)))throw Error('Typed option record block required');
    const states=new Set(['reported','reported_zero','missing','null','invalid_parent','invalid_number','outside_domain','non_integer_quantity','crossed_quote']),positions=new Set(),out=[];
    for(const row of block.rows){
      if(!object(row)||!count(row.source_row)||row.source_row>=250||positions.has(row.source_row)||!object(row.record)||!object(row.cells)||
        Object.keys(row.record).some(k=>k==='metrics'||k==='evidence'||Object.hasOwn(block.common_record,k)))throw Error('Original row identity differs');
      positions.add(row.source_row);const metrics={};
      for(const [name,values] of Object.entries(row.cells)){
        const definition=block.field_definitions[name];
        if(!object(definition)||['state','value','reported_value'].some(k=>Object.hasOwn(definition,k))||!object(values)||!states.has(values.state)||
          !Object.hasOwn(values,'value')||(values.value!==null&&!decimal(values.value))||
          Object.keys(values).some(k=>!['state','value','reported_value'].includes(k)))throw Error('Option metric definition differs');
        const cell={...definition,...values};
        if(cell.state==='reported'||cell.state==='reported_zero'){
          if(!decimal(cell.value)||(Object.hasOwn(cell,'reported_value')&&cell.reported_value!==cell.value))throw Error('Reported metric differs');
          cell.reported_value=cell.value;
        }else if(cell.value!==null)throw Error('Unqualified metric cannot supply a value');
        metrics[name]=cell;
      }
      const restored={...block.common_record,...row.record,metrics,evidence:{...block.source,row_index:row.source_row,row_pointer:'/results/'+row.source_row}};
      if(!authority(restored))throw Error('Option row authority differs');out.push(restored);
    }
    if(await sha(bytes(out))!==block.expanded_sha256)throw Error('Expanded option records differ');
    return out;
  }
  async function records(c,index,fetcher,signal){
    if(c?.contract!=='option-research-chain.v1'||!chainOwners.has(c)||!count(index)||index>=c.record_blocks.length)throw Error('Choose a verified source page');
    const ref=c.record_blocks[index],block=await retained(ref.artifact,'rows',fetcher,signal);
    if(block.expanded_sha256!==ref.expanded_sha256||block.source?.page!==ref.source_page||block.row_count!==ref.rows)throw Error('Selected record page differs');
    const out=await unpack(block);if(out.some(r=>r.underlying!==c.underlying))throw Error('Selected underlying differs');
    out.forEach(r=>{recordOwners.set(r,chainOwners.get(c));freeze(r);});return out;
  }
  function table(headers,rows,caption,firstHtml=false){return '<div class="or-scroll" tabindex="0" role="region" aria-label="'+esc(caption)+'"><table><caption>'+esc(caption)+'</caption><thead><tr>'+headers.map(h=>'<th scope="col">'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(row=>'<tr>'+row.map((v,i)=>'<td>'+(firstHtml&&i===0?v:esc(v))+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';}
  const metric=c=>c?.value!==null&&decimal(c?.value)?exact(c.value):c?.reported_value!==undefined?'Unqualified ('+exact(c.reported_value)+')':String(c?.state||'Unavailable').replaceAll('_',' ');
  function overview(p,at=Date.now()){
    if(!typed(p))throw Error('Native option inventory required');
    const q=p.quality,review=at>=Date.parse(q.acquisition_review_due_at);
    return '<p class="or-clock">Captured '+esc(p.generated_at)+' · '+(review?'acquisition review overdue':'within acquisition review window')+'</p><div class="or-stats">'+[
      [q.selected_underlyings,'underlyings requested'],[q.returned_rows,'returned contracts'],[q.identity_eligible_rows,'valid contract identities'],[q.record_blocks,'traceable record pages']
    ].map(([n,label])=>'<div><strong>'+esc(Number(n).toLocaleString('en-US'))+'</strong><span>'+esc(label)+'</span></div>').join('')+'</div><p>OI, IV and Greek observation times are not supplied. Capture time does not make those fields current. No trade recommendation or position size is issued.</p>';
  }
  function inventory(p,query=''){
    if(!typed(p))throw Error('Native option inventory required');const q=String(query).trim().toUpperCase(),names=p.universe.selected.filter(t=>t.includes(q));
    return '<p>'+names.length+' of '+p.universe.selected.length+' underlyings shown; '+p.universe.deferred.length+' explicitly deferred.</p>'+table(
      ['Underlying','Capture','Returned rows','Identity-valid rows','Source pages'],names.map(t=>{const r=p.chains[t];return ['<button type="button" data-or-pick="'+t+'">'+t+'</button>',r.research_status.replaceAll('_',' '),r.coverage.returned_rows,r.coverage.eligible_identity_rows,r.acquisition.captured_pages];}),'Research watchlist',true);
  }
  function chainView(c){
    const a=c.acquisition,q=c.coverage;
    let html='<h3>'+esc(c.underlying)+' · '+esc(c.research_status.replaceAll('_',' '))+'</h3><p>'+q.returned_rows+' returned rows, '+q.eligible_identity_rows+' with valid contract identities. '+a.compiled_pages+' of '+a.captured_pages+' captured pages reconstructed.</p><p>Acquisition '+esc(a.started_at)+' to '+esc(a.completed_at)+'. '+(a.pagination_complete?'All provider-returned pages captured.':'Pagination incomplete: '+esc(a.stop)+'.')+' This is not an atomic exchange-wide snapshot.</p>';
    const interest=c.reported_open_interest;
    if(interest)html+=table(['Captured population','Reported OI · contracts','Included / population','Observation date'],[['Calls',exact(interest.calls.value),interest.calls.included_rows+' / '+interest.calls.population_rows,'Not supplied'],['Puts',exact(interest.puts.value),interest.puts.included_rows+' / '+interest.puts.population_rows,'Not supplied']],'Open interest in the captured rows');
    html+='<p>Open interest does not identify dealer ownership. Missing or invalid fields remain excluded; true zero remains zero.</p>';
    if(q.field_quality)html+=table(['Field','Valid reported rows','Other field states'],['open_interest','daily_volume','vendor_iv','vendor_gamma','vendor_delta','underlying_price'].map(name=>{
      const states=q.field_quality[name]||{},valid=(states.reported||0)+(states.reported_zero||0);
      return [name.replaceAll('_',' '),valid,Object.entries(states).filter(([k])=>!['reported','reported_zero'].includes(k)).map(([k,v])=>k.replaceAll('_',' ')+': '+v).join('; ')||'None'];
    }),'Field coverage across the returned rows');
    return html;
  }
  function dateGroups(c){return table(['Bar update date · New York','Call volume · contracts','Put volume · contracts','Included call / put rows','Complete market session'],
    (c.daily_bar_update_groups||[]).slice().reverse().map(g=>[g.daily_bar_update_date_new_york,exact(g.calls.value),exact(g.puts.value),g.calls.included_rows+' / '+g.puts.included_rows,'Not verified']), 'Most-recent daily bars, grouped by their own update date');}
  function rowTable(rows,query=''){
    const q=String(query).trim().toUpperCase(),selected=rows.filter(r=>String(r.contract_id||'').toUpperCase().includes(q));
    return '<p>'+selected.length+' of '+rows.length+' rows on this source page. Search applies to this page.</p>'+table(
      ['Contract','Identity','Strike · USD/share','Reported OI','Last-bar volume','Bar update date · NY','Vendor gamma'],selected.map(r=>[
        '<button type="button" data-or-row="'+r.evidence.row_index+'">'+esc(r.contract_id||'Malformed source row')+'</button>',r.identity_eligible?'Valid':r.identity_reasons.join('; '),
        metric(r.metrics.strike),metric(r.metrics.open_interest),metric(r.metrics.daily_volume),r.clocks?.daily_bar_updated?.date_new_york||'Unavailable',metric(r.metrics.vendor_gamma)]),'Original contract rows',true);
  }
  function rowView(r){
    const e=r.evidence;
    return '<h3>'+esc(r.contract_id||'Malformed source row')+'</h3><p>Source page '+e.page+', original row '+e.row_index+' · '+esc(e.row_pointer)+'. Acquired '+esc(r.source_received_at)+'.</p><p class="or-digest">Original SHA-256 '+esc(e.original.sha256)+'</p>'+
      '<details><summary>Inspect every numeric field, unit and source location</summary>'+table(['Field','Qualified value','Reported value','Unit','State','Source field'],Object.entries(r.metrics).map(([name,c])=>[name.replaceAll('_',' '),exact(c.value),exact(c.reported_value),c.unit,c.state,c.source_field]),'Every numeric field and its provenance')+'</details>'+
      '<details><summary>Inspect the original source clocks</summary>'+table(['Provider clock','Reported time','Exact nanoseconds','State'],Object.entries(r.clocks||{}).map(([name,c])=>[name.replaceAll('_',' '),c.value||'Unavailable',c.raw_nanoseconds||'Unavailable',c.state]),'Clocks supplied on this original row')+'</details>'+
      '<p>OI / IV / Greek observation dates: not supplied. Raw provider responses are protected; the retained research records carry their exact digest and row location.</p>';
  }
  const SCALE=100000000n;
  function fixed(v,label){
    if(typeof v!=='string'||!/^\d{1,10}(?:\.\d{1,8})?$/.test(v)||Number(v)>1e9)throw Error(label+' must be nonnegative, at most 1 billion, with up to 8 decimal places');
    const [a,b='']=v.split('.');return BigInt(a)*SCALE+BigInt(b.padEnd(8,'0'));
  }
  function money(v){const sign=v<0n?'-':'';v=v<0n?-v:v;const tail=(v%SCALE).toString().padStart(8,'0').replace(/0+$/,'');return sign+(v/SCALE).toString()+(tail?'.'+tail:'');}
  function scenario(p,r,assumptions){
    if(!typed(p)||!verifiedPackets.has(p)||recordOwners.get(r)!==p||!authority(r)||!r.identity_eligible||!p.chains[r.underlying]||!/^100(?:\.0+)?$/.test(r.metrics?.shares_per_contract?.value)||!['call','put'].includes(r.contract_type))throw Error('Select a verified standard contract from this publication first');
    const raw=String(assumptions.quantity??'');if(!/^-?\d{1,7}$/.test(raw)||Math.abs(Number(raw))>1e6||Number(raw)===0)throw Error('Contracts must be a nonzero whole number up to 1 million; negative means short');
    const quantity=BigInt(raw),strike=fixed(r.metrics.strike.value,'Strike'),premium=fixed(assumptions.premium,'Assumed premium'),spot=fixed(assumptions.spot,'Assumed expiration price'),cost=fixed(assumptions.cost,'Total assumed costs');
    const intrinsic=r.contract_type==='call'?(spot>strike?spot-strike:0n):(strike>spot?strike-spot:0n);
    const premiumCash=-quantity*100n*premium,intrinsicCash=quantity*100n*intrinsic,net=premiumCash+intrinsicCash-cost;
    let loss=null;if(quantity>0n)loss=quantity*100n*premium+cost;else if(r.contract_type==='put')loss=(-quantity)*100n*(strike-premium)+cost;
    if(loss!==null&&loss<0n)loss=0n;
    return {calculator_contract:'option-expiration-payoff.v1',arithmetic:'exact_fixed_point_8_decimal_places',
      assumption_units:{quantity:'signed_contracts',premium:'USD_per_share',spot:'USD_per_share_at_expiration',cost:'USD_total'},
      contract_id:r.contract_id,expiration_date:r.expiration_date,assumptions:{...assumptions},quantity:raw,multiplier:'100',
      premium_cash_usd:money(premiumCash),intrinsic_value_usd:money(intrinsicCash),costs_usd:money(cost),net_usd:money(net),
      maximum_loss_usd:loss===null?null:money(loss),loss_scope:loss===null?'Unbounded loss for an uncovered short call in this expiration model':'Expiration payoff under these assumptions',
      formula:'signed contracts × 100 × (intrinsic value per share − assumed premium per share) − assumed total costs',
      run:p.replay,evidence:r.evidence,
      scope:'User-assumed expiration payoff, not a forecast, live quote, mark-to-market value or size recommendation. Physical exercise, early assignment, financing, taxes and changes in other holdings are outside this calculation.'};
  }
  const api={CONTRACT,PREFIX,CURRENT,esc,exact,ticker,clock,typed,stable,same,sha,bytes,load,retained,verifyPacket,recordedRun,recordedUrl,chain,unpack,records,overview,inventory,chainView,dateGroups,rowTable,rowView,scenario,table,metric};
  root.JHOptionResearch=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
