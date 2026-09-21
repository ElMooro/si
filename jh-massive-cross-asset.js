(function(root){
  'use strict';
  const A=root.JHOptionResearch||(typeof require==='function'?require('./jh-option-research.js'):null);
  const SOURCES={fx:['fx-original-quote-research.v1','data/fx-quote-research/','fx-original-replay.v1','Currency quote research'],
    futures:['futures-original-research.v1','data/futures-research/','futures-original-replay.v1','Dated futures research']};
  const identity=v=>typeof v==='string'&&(/^[A-Z][A-Z0-9.-]{0,14}$/.test(v)||/^FX:MASSIVE:[A-Z]{3}_[A-Z]{3}$/.test(v)||/^FUTURE:[A-Z][A-Z0-9_]{0,19}:[A-Z][A-Z0-9]{0,11}:[A-Z][A-Z0-9]{0,23}$/.test(v));
  const check=(condition,reason)=>{if(!condition)throw Error(reason);};
  function expected(kind,body){
    const result=[];
    if(kind==='fx'){
      for(const[pair,row]of Object.entries(body.pairs)){
        const id='FX:MASSIVE:'+pair;
        check(identity(id)&&pair===row.pair&&pair===row.base_code+'_'+row.quote_code&&row.provider_ticker==='C:'+row.base_code+row.quote_code,'Currency pair identity differs');
        result.push({id,pointer:'/pairs/'+pair,definition:{asset_class:'currency_quote',provider:'Massive/Polygon',pair,
          base_code:row.base_code,quote_code:row.quote_code,provider_ticker:row.provider_ticker,price_unit:row.price_unit,
          metal_base_quantity_unit_verified:row.metal_base_quantity_unit_verified??null},
          dates:{source_capture_completed_at:row.source_capture_completed_at,source_review_due_at:row.source_review_due_at,
            observation_clock_role:'reported_aggregate_window_start_not_close_time',latest_reported_window_start_utc:row.latest_reported_row?.window_start_utc??null}});
      }
    }else if(kind==='futures'){
      for(const[product,row]of Object.entries(body.products)){
        check(product===row.product_code,'Futures product identity differs');
        row.contracts.forEach((item,index)=>{
          const id='FUTURE:'+row.venue+':'+product+':'+item.ticker,dataset=body.datasets[item.dataset];
          check(identity(id)&&dataset?.scope.product===product&&dataset.scope.ticker===item.ticker&&dataset.scope.kind==='bars','Dated futures identity differs');
          result.push({id,pointer:'/products/'+product+'/contracts/'+index,definition:{asset_class:'dated_future',provider:'Massive',venue:row.venue,
            product,ticker:item.ticker,definition_date:body.definition_date,definition_source:item.definition_source,dataset:item.dataset},
            dates:{source_capture_completed_at:dataset.source_capture_completed_at,source_review_due_at:null,
              observation_clock_role:'reported_session_label_not_synchronized_price',latest_reported_session_end_date:item.latest_reported_row?.session_end_date??null,
              bar_finality_independently_verified:false}});
        });
      }
    }else throw Error('Unreviewed cross-asset parent');
    check(new Set(result.map(r=>r.id)).size===result.length,'Duplicate instrument identity');return result;
  }
  function verify(p,parents){
    check(A.same(Object.keys(p.instrument_identities||{}).sort(),Object.keys(p.instruments).sort()),'Complete instrument identities required');
    const groups=new Map(),cutoff=Date.parse(p.generated_at);
    for(const[id,definition]of Object.entries(p.instrument_identities)){
      if(!id.includes(':'))check(A.same(definition,{asset_class:'provider_security_symbol',symbol:id,
        meaning:'Existing option/fund symbol scope; exchange, share class and economic equivalence are not inferred.'}),'Security identity scope differs');
    }
    for(const kind of Object.keys(SOURCES)){
      const source=p.sources[kind];if(source.node===null)continue;
      const body=parents.get(source.node),rows=expected(kind,body);
      check(A.same(source.instrument_names,rows.map(r=>r.id).sort())&&source.instrument_count===rows.length,'Cross-asset inventory differs');
      check(source.source_capture_completed_at===body.source_capture_completed_at,'Parent acquisition clock differs');
      const records=kind==='fx'?body.pairs:body.datasets,clocks=Object.fromEntries(Object.entries(records).map(([k,r])=>[k,r.source_capture_completed_at]));
      check(A.same(source.measurement_capture_clocks,clocks)&&source.source_definition_date===(body.definition_date??null),'Measurement capture clocks differ');
      const due=rows.map(r=>r.dates.source_review_due_at).filter(v=>v!==null),deadline=due.length?Math.min(...due.map(Date.parse)):null;
      check((deadline===null?source.source_review_due_at===null:Date.parse(source.source_review_due_at)===deadline)&&source.source_review_overdue===(deadline===null?null:cutoff>=deadline),'Source review deadline differs');
      for(const row of rows){
        check(A.same(p.instrument_identities[row.id],row.definition),'Instrument definition differs');
        const refs=p.instruments[row.id];check(refs?.length===1,'Exact instrument reference required');const ref=refs[0];
        check(ref.source===kind&&ref.node===source.node&&ref.pointer===row.pointer&&ref.source_generated_at===body.generated_at,'Instrument source pointer differs');
        for(const[k,value]of Object.entries(row.dates))check(A.same(ref[k],value),'Instrument observation or acquisition date differs');
        check(ref.source_review_overdue===(row.dates.source_review_due_at===null?null:cutoff>=Date.parse(row.dates.source_review_due_at)),'Instrument review status differs');
        const keys=kind==='fx'?['currency:'+row.definition.base_code,'currency:'+row.definition.quote_code]:['futures_product:'+row.definition.venue+':'+row.definition.product];
        for(const key of keys){if(!groups.has(key))groups.set(key,[]);groups.get(key).push(row.id);}
      }
    }
    const actual=p.dependency_graph.shared_exposures;check(Array.isArray(actual)&&actual.length===groups.size,'Shared exposure inventory differs');
    const seen=new Set();
    for(const group of actual){check(!seen.has(group.key)&&groups.has(group.key)&&A.same(group.instruments,groups.get(group.key).sort())&&group.independent_investment_votes===0,'Shared exposure membership differs');seen.add(group.key);}
  }
  function parentUrl(item,id){
    const run=item.node.replay.manifest_key.split('/').pop().slice(0,-5),row=item.parent;
    if(item.node.kind==='fx'){
      check(id==='FX:MASSIVE:'+row.pair,'Currency link identity differs');
      return '/fx-research.html?pair='+encodeURIComponent(row.pair)+'&run='+run;
    }
    const parts=id.split(':');check(parts.length===4&&parts[0]==='FUTURE'&&parts[3]===row.ticker,'Futures link identity differs');
    return '/futures-research.html?product='+encodeURIComponent(parts[2])+'&ticker='+encodeURIComponent(row.ticker)+'&run='+run;
  }
  function detail(item,definition){
    const r=item.reference,row=item.parent;
    if(item.node.kind==='fx')return A.table(['Pair','Quoted price unit','Last reported window start · UTC','Original acquisition · UTC'],
      [[row.pair,row.price_unit,r.latest_reported_window_start_utc??'Unavailable',r.source_capture_completed_at]],'Quote windows are not independently observed close times')+
      '<p>Shared '+A.esc(row.base_code)+' and '+A.esc(row.quote_code)+' currency legs remain related exposures. '+(row.metal_base_quantity_unit_verified===false?'The metal base quantity unit remains unverified. ':'')+'Individual quote finality is not established.</p>';
    return A.table(['Dated contract','Venue / product','Definition date','Latest reported session','Original bar acquisition · UTC'],
      [[row.ticker,definition.venue+' / '+definition.product,definition.definition_date,r.latest_reported_session_end_date??'Unavailable',r.source_capture_completed_at]],'Close and settlement remain separate in the parent contract record')+
      '<p>Contract identity, expiry, quantity conversion and scheduled-session qualifications are retained. A shared product does not establish a continuous roll return, synchronized prices or a hedge ratio.</p>';
  }
  function sharedView(p){
    if(p.contract!=='massive-composite-research.v2')return '<p>This older recorded composition predates the currency and futures identity map.</p>';
    return A.table(['Shared exposure','Recorded instruments'],p.dependency_graph.shared_exposures.map(g=>[g.key,g.instruments.join(', ')]),
      'Shared legs and products — no correlation, hedge ratio or portfolio netting is assumed');
  }
  const api={SOURCES,identity,expected,verify,parentUrl,detail,sharedView};root.JHMassiveCrossAsset=api;if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
