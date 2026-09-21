(function(root){
  'use strict';
  const A=root.JHOptionResearch||(typeof require==='function'?require('./jh-option-research.js'):null);
  const X=root.JHMassiveCrossAsset||(typeof require==='function'?require('./jh-massive-cross-asset.js'):null);
  const CURRENT='data/massive-research.json',PREFIX='data/massive-research/',CONTRACT='massive-composite-research.v2',V1='massive-composite-research.v1',MAX=16*1024*1024;
  const SOURCES={options:['option-flow-original-research.v1','data/option-flow-research/','option-flow-replay.v1','Captured contracts'],
    populations:['option-population-desk.v1','data/option-population-research/','option-population-replay.v1','Option populations'],
    etf_desk:['etf-desk-original-research.v1','data/etf-desk-research/','etf-desk-replay.v1','ETF desk'],
    fund_flows:['provider-fund-flow-research.v1','data/provider-flow-research/','provider-flow-replay.v1','Dated fund flows'],
    holdings:['etf-holdings-original-research.v1','data/etf-holdings-research/','etf-holdings-replay.v1','Fund constituents'],...X.SOURCES};
  const FLAGS=['forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'],owners=new WeakMap();
  const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v),digest=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
  const ticker=X.identity,authority=v=>object(v)&&FLAGS.every(k=>v[k]===false);
  const safe=key=>typeof key==='string'&&[PREFIX,...Object.values(SOURCES).map(v=>v[1])].some(p=>key.startsWith(p)&&/^(runs|outputs)\/[a-f0-9]{64}\.json$/.test(key.slice(p.length)));
  function freeze(v){if(object(v)||Array.isArray(v)){Object.values(v).forEach(freeze);Object.freeze(v);}return v;}
  async function load(key,fetcher,signal){
    if(key!==CURRENT&&!safe(key))throw Error('Unapproved composite evidence path');
    const response=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});
    if(!response.ok)throw Error('Recorded research is unavailable');
    if(Number(response.headers?.get('content-length'))>MAX)throw Error('Research response byte bound');
    let raw;
    if(response.body?.getReader){const reader=response.body.getReader(),parts=[];let size=0;
      try{for(;;){const item=await reader.read();if(item.done)break;size+=item.value.byteLength;if(size>MAX)throw Error('Research response byte bound');parts.push(item.value);}}
      catch(error){await reader.cancel();throw error;}
      const joined=new Uint8Array(size);let offset=0;for(const part of parts){joined.set(part,offset);offset+=part.byteLength;}raw=joined.buffer;
    }else raw=await response.arrayBuffer();
    if(raw.byteLength>MAX)throw Error('Research response byte bound');
    return {raw,doc:JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw))};
  }
  async function retained(ref,prefix,fetcher,signal){
    if(!digest(ref?.sha256)||ref.key!==prefix+'outputs/'+ref.sha256+'.json'||!Number.isSafeInteger(ref.bytes)||ref.bytes<1||ref.bytes>MAX)throw Error('Exact parent output reference required');
    const item=await load(ref.key,fetcher,signal);
    if(item.raw.byteLength!==ref.bytes||await A.sha(item.raw)!==ref.sha256)throw Error('Parent output bytes differ');
    return item.doc;
  }
  async function runOutput(identity,prefix,contract,fetcher,signal){
    if(!digest(identity?.output_sha256)||!safe(identity.manifest_key)||!identity.manifest_key.startsWith(prefix+'runs/'))throw Error('Exact research run required');
    const item=await load(identity.manifest_key,fetcher,signal),run=item.doc;
    if(identity.manifest_key!==prefix+'runs/'+await A.sha(item.raw)+'.json'||run.contract!==contract||run.output_sha256!==identity.output_sha256||run.output?.sha256!==identity.output_sha256)throw Error('Research run differs');
    const body=await retained(run.output,prefix,fetcher,signal);
    if(body.generated_at!==run.generated_at)throw Error('Research run clock differs');
    return {body,run};
  }
  function sourceKinds(p){return Object.keys(SOURCES).filter(k=>p.contract===CONTRACT||!Object.hasOwn(X.SOURCES,k));}
  function typed(p){return [V1,CONTRACT].includes(p?.contract)&&authority(p)&&p.call===null&&p.score===null&&p.portfolio_action==='WAIT'&&p.independent_investment_votes===0&&A.clock(p.generated_at)&&
    object(p.sources)&&A.same(Object.keys(p.sources).sort(),sourceKinds(p).sort())&&object(p.instruments)&&Object.keys(p.instruments).length<=3000&&
    object(p.dependency_graph?.nodes)&&Object.keys(p.dependency_graph.nodes).length<=(p.contract===CONTRACT?14:12)&&Array.isArray(p.dependency_graph.edges)&&
    p.dependency_graph.statistical_independence_established===false&&p.dependency_graph.independent_investment_votes===0&&
    p.verification?.original_provider_replay_performed_by_composite===false;}
  function verifyGraph(p,parents){
    const edges=new Set(),families=new Map(),member=(family,id)=>{if(!families.has(family))families.set(family,new Set());families.get(family).add(id);};
    for(const[id,out]of parents){
      const kind=p.dependency_graph.nodes[id].kind;
      if(kind==='populations')edges.add(id+'|options:'+out.source_run.output_sha256);
      else if(kind==='etf_desk'){
        edges.add(id+'|fund_flows:'+out.canonical_sources.flows.replay.output_sha256);edges.add(id+'|holdings:'+out.canonical_sources.holdings.replay.output_sha256);
        member('fund_profile',id);if(out.quality?.additional_funds?.length){member('fund_flow',id);member('constituents',id);}
      }else member({options:'option_chain',fund_flows:'fund_flow',holdings:'constituents',fx:'fx_quotes',futures:'futures_prices'}[kind],id);
    }
    const actual=p.dependency_graph.edges.map(e=>e.view+'|'+e.source);
    if(actual.length!==edges.size||!A.same([...actual].sort(),[...edges].sort()))throw Error('Source dependency edges differ');
    const rows=p.dependency_graph.measurement_families,seen=new Set();
    if(!Array.isArray(rows)||rows.length!==families.size)throw Error('Measurement family inventory differs');
    for(const f of rows){if(seen.has(f.family)||!families.has(f.family)||!A.same([...f.nodes].sort(),[...families.get(f.family)].sort())||f.retained_node_count!==f.nodes.length||f.independent_investment_votes!==0)throw Error('Measurement family membership differs');seen.add(f.family);}
  }
  async function verifyPacket(p,fetcher,signal){
    if(!typed(p))throw Error('Recorded descriptive composite required');
    const result=await runOutput(p.replay,PREFIX,p.contract===CONTRACT?'massive-composite-replay.v2':'massive-composite-replay.v1',fetcher,signal),{replay,...body}=p;
    if(!A.same(body,result.body))throw Error('Composite publication differs from retained output');
    const entries=await Promise.all(Object.entries(p.dependency_graph.nodes).map(async([id,node])=>{
      const spec=SOURCES[node.kind];if(!spec||!sourceKinds(p).includes(node.kind))throw Error('Unreviewed parent kind');
      const result=await runOutput(node.replay,spec[1],spec[2],fetcher,signal),out=result.body;
      if(out.contract!==spec[0]||!authority(out)||!A.clock(out.generated_at)||Date.parse(out.generated_at)>Date.parse(p.generated_at)||
        id!==node.kind+':'+node.replay.output_sha256||!A.same(node.output,result.run.output)||node.generated_at!==out.generated_at)throw Error('Parent identity or clock differs');
      return [id,freeze(out)];
    }));
    const parents=new Map(entries),expected=new Set();
    for(const edge of p.dependency_graph.edges)if(!parents.has(edge.view)||!parents.has(edge.source))throw Error('Dependency edge has no recorded parent');
    for(const[kind,row]of Object.entries(p.sources)){
      if(!authority(row))throw Error('Source authority differs');if(row.node===null)continue;
      const out=parents.get(row.node),collection=kind==='options'?'chains':kind==='populations'?'underlyings':'funds';
      if(!out||!row.node.startsWith(kind+':')||row.source_generated_at!==out.generated_at)throw Error('Source row inventory differs');
      if(Object.hasOwn(X.SOURCES,kind)){
        const rows=X.expected(kind,out);if(row.instrument_count!==rows.length)throw Error('Source row inventory differs');
        for(const item of rows)expected.add([item.id,kind,row.node,item.pointer].join('|'));
      }else{
        if(!object(out[collection])||row.instrument_count!==Object.keys(out[collection]).length)throw Error('Source row inventory differs');
        for(const symbol of Object.keys(out[collection])){if(!/^[A-Z][A-Z0-9.-]{0,14}$/.test(symbol))throw Error('Invalid instrument identity');expected.add([symbol,kind,row.node,'/'+collection+'/'+symbol].join('|'));}
      }
    }
    const actual=new Set();
    for(const[symbol,rows]of Object.entries(p.instruments)){
      if(!ticker(symbol)||!Array.isArray(rows)||rows.length>5)throw Error('Invalid instrument references');
      for(const row of rows){const id=[symbol,row.source,row.node,row.pointer].join('|');if(actual.has(id)||!expected.has(id))throw Error('Instrument evidence differs');actual.add(id);}
    }
    if(actual.size!==expected.size)throw Error('Instrument evidence is incomplete');
    if(p.contract===CONTRACT)X.verify(p,parents);
    verifyGraph(p,parents);freeze(p);owners.set(p,parents);return p;
  }
  async function recordedRun(id,fetcher,signal){
    if(!digest(id))throw Error('Exact recorded composite run required');
    const key=PREFIX+'runs/'+id+'.json',item=await load(key,fetcher,signal);
    if(await A.sha(item.raw)!==id||!['massive-composite-replay.v1','massive-composite-replay.v2'].includes(item.doc.contract))throw Error('Recorded composite run differs');
    const body=await retained(item.doc.output,PREFIX,fetcher,signal);
    return verifyPacket({...body,replay:{manifest_key:key,output_sha256:item.doc.output_sha256}},fetcher,signal);
  }
  function requirePacket(p){if(!owners.has(p))throw Error('Verify the retained composite first');}
  function instrument(p,symbol){
    requirePacket(p);if(!ticker(symbol))throw Error('Choose an instrument');
    return(p.instruments[symbol]||[]).map(ref=>{
      const parent=owners.get(p).get(ref.node);let row=parent;
      for(const field of ref.pointer.split('/').slice(1))row=row[field];
      return {reference:ref,parent:row,node:p.dependency_graph.nodes[ref.node]};
    });
  }
  function recordedUrl(p,symbol){requirePacket(p);if(!ticker(symbol))throw Error('Choose an instrument');return '/market-evidence.html?symbol='+encodeURIComponent(symbol)+'&run='+p.replay.manifest_key.split('/').pop().slice(0,-5);}
  function parentUrl(item,symbol){
    const id=item.node.replay.manifest_key.split('/').pop().slice(0,-5);
    if(!ticker(symbol)||!digest(id))throw Error('Exact parent reference required');
    if(Object.hasOwn(X.SOURCES,item.node.kind))return X.parentUrl(item,symbol);
    if(item.node.kind==='options')return '/option-chain-research.html?underlying='+encodeURIComponent(symbol)+'&run='+id;
    if(item.node.kind==='populations')return '/gex/?underlying='+encodeURIComponent(symbol)+'&run='+id;
    if(item.node.kind==='etf_desk')return '/etf.html?fund='+encodeURIComponent(symbol)+'&run='+id;
    return '/'+item.node.output.key;
  }
  function review(row,at=Date.now()){
    if(!row.node)return 'Unavailable / unqualified';
    return A.clock(row.source_review_due_at)?(at>=Date.parse(row.source_review_due_at)?'Source review overdue':'Within source review window'):'Source review deadline unknown';
  }
  function sourcesView(p,at=Date.now()){
    requirePacket(p);
    return A.table(['Research view','Parent publication · UTC','Capture completed · UTC','Acquisition review','Instrument rows'],
      Object.entries(p.sources).map(([kind,row])=>[SOURCES[kind][3],row.source_generated_at??'Unavailable',row.source_capture_completed_at??'See field-specific dates',review(row,at),row.instrument_count??'Unavailable']),
      'Recorded source views — counts are coverage, not independent investment votes');
  }
  function familiesView(p){
    requirePacket(p);return p.dependency_graph.measurement_families.map(f=>'<article class="mr-family"><h3>'+A.esc(f.family.replaceAll('_',' '))+'</h3><p>'+A.esc(f.provider)+'</p><p>'+A.esc(f.meaning)+'</p><p>'+f.nodes.length+' recorded source view'+(f.nodes.length===1?'':'s')+'</p></article>').join('');
  }
  function instrumentView(p,symbol,at=Date.now()){
    return instrument(p,symbol).map(item=>{
      const {reference:r,parent:row,node}=item,source=p.sources[r.source];let detail='';
      if(Object.hasOwn(X.SOURCES,r.source))detail=X.detail(item,p.instrument_identities[symbol]);
      else if(r.source==='options')detail=A.table(['Call open interest · contracts','Put open interest · contracts','OI observation date','Returned contracts'],[[A.exact(row.reported_open_interest?.calls?.value),A.exact(row.reported_open_interest?.puts?.value),row.reported_open_interest?.observed_at??'Unknown',row.coverage.returned_rows]],'Captured outstanding contracts; ownership and trade direction are unknown');
      else if(r.source==='populations')detail='<p>'+row.totals.counts.identity_eligible_rows+' eligible captured identities across '+row.expiry_strike_groups+' expiry/strike groups. Inspect field coverage and the exact contributing contracts.</p>';
      else if(r.source==='fund_flows'||r.source==='etf_desk'){
        const flow=r.source==='etf_desk'?row.flows:row;
        detail=A.table(['Reporting observations','Effective dates','Recorded fund flow · USD','Window status'],Object.entries(flow.aligned_windows||{}).map(([n,w])=>[n,(w.start_date??'?')+' to '+(w.end_date??'?'),A.exact(w.flow_usd_decimal),w.status]),'Dated fund flows — no underlying-security purchases inferred');
      }else detail='<p>Returned constituent identities and source row definitions are retained in the parent output. Raw weights, dates and coverage must be read together.</p>';
      return '<section class="mr-evidence"><h3>'+A.esc(SOURCES[r.source][3])+'</h3><p>'+A.esc(review(source,at))+' · Parent publication '+A.esc(r.source_generated_at)+'</p>'+detail+
        '<p><a href="'+A.esc(parentUrl(item,symbol))+'">'+(r.source==='fx'?'Open this recorded pair and exposure assumptions':r.source==='futures'?'Open this dated contract and exposure assumptions':r.source==='etf_desk'?'Open this recorded fund and hypothetical outcome':r.source==='options'?'Open this recorded chain and contract payoff':r.source==='populations'?'Inspect this recorded population':'Open the exact retained parent output')+'</a></p>'+
        '<details><summary>Exact evidence reference</summary><p>Output SHA-256 <code>'+A.esc(node.output.sha256)+'</code></p><p>JSON pointer <code>'+A.esc(r.pointer)+'</code></p><a href="/'+A.esc(node.replay.manifest_key)+'">Parent calculation run</a></details></section>';
    }).join('')||'<p>No recorded evidence for this instrument in this composition.</p>';
  }
  function sharedView(p){requirePacket(p);return X.sharedView(p);}
  function exportEvidence(p,symbol){requirePacket(p);return {contract:'composite-instrument-evidence.v1',symbol,composition:p.replay,
    instrument_identity:p.instrument_identities?.[symbol]??{asset_class:'provider_security_symbol',symbol},composition_generated_at:p.generated_at,measurements:instrument(p,symbol),verification:p.verification,
    forecast_qualified:false,calls_eligible:false,sizing_eligible:false,execution_eligible:false};}
  const api={CURRENT,PREFIX,CONTRACT,SOURCES,load,retained,typed,verifyPacket,recordedRun,instrument,recordedUrl,parentUrl,review,sourcesView,familiesView,instrumentView,sharedView,exportEvidence};
  root.JHMassiveResearch=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
