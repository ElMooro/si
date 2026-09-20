(function(root){
  'use strict';
  const PREFIX='data/capital-research/',CONTRACT='capital-evidence-research.v1',LIMIT=64*1024*1024;
  const SCOPES={'SH|NONE':'Shares · non-option','SH|CALL':'Shares · calls','SH|PUT':'Shares · puts','PRN|NONE':'Principal · non-option','PRN|CALL':'Principal · calls','PRN|PUT':'Principal · puts'};
  const canonical=v=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
  const equal=(a,b)=>JSON.stringify(canonical(a))===JSON.stringify(canonical(b));
  const sha=async raw=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');
  async function bytes(response){if(!response.ok)throw Error('Evidence request failed: HTTP '+response.status);const raw=new Uint8Array(await response.arrayBuffer());if(!raw.length||raw.length>LIMIT)throw Error('Complete evidence exceeds the size limit');return raw;}
  async function verified(fetcher,ref,kind){
    if(!ref||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+'.json'||!Number.isSafeInteger(ref.bytes)||ref.bytes<=0||ref.bytes>LIMIT)throw Error('Evidence identity differs');
    const raw=await bytes(await fetcher('/'+ref.key));
    if(await sha(raw)!==ref.sha256||raw.length!==ref.bytes)throw Error('Evidence hash or length differs');
    return JSON.parse(new TextDecoder().decode(raw));
  }
  async function load(fetcher,run){
    let pointer=null,ref;
    if(run){if(!/^[a-f0-9]{64}$/.test(run))throw Error('Invalid permanent snapshot');const raw=await bytes(await fetcher('/'+PREFIX+'runs/'+run+'.json'));ref={key:PREFIX+'runs/'+run+'.json',sha256:run,bytes:raw.length};}
    else{pointer=JSON.parse(new TextDecoder().decode(await bytes(await fetcher('/data/capital-flow.json',{cache:'no-store'}))));if(pointer.contract!==CONTRACT)throw Error('The reproducible CapitalFlow publication is not available yet. The earlier stock-flow score is retired.');ref=pointer.replay;}
    const manifest=await verified(fetcher,ref,'runs'),packet=await verified(fetcher,manifest.output,'outputs');
    if(manifest.contract!=='capital-evidence-replay.v1'||packet.contract!==CONTRACT||!equal(packet.source,manifest.source)||!equal(packet.legacy_contexts,manifest.legacy_contexts))throw Error('Snapshot binding differs');
    if(pointer){const {replay,...value}=pointer;if(!equal(value,packet))throw Error('Current snapshot differs from retained evidence');}
    const funds=Object.values(packet.managers||{});
    if(funds.length!==packet.counts?.managers||funds.reduce((a,v)=>a+v.record_count,0)!==packet.counts.disclosure_comparisons||Object.keys(packet.fund_issuance?.funds||{}).length!==packet.counts.configured_etfs)throw Error('Complete research counts differ');
    if(packet.call!==null||packet.calls_eligible!==false||packet.sizing_eligible!==false||packet.execution_eligible!==false)throw Error('Research authority differs');
    return {packet,manifest,ref};
  }
  function fraction(n,d=1n){n=BigInt(n);d=BigInt(d);if(d<=0n)throw Error('Positive denominator required');let a=n<0n?-n:n,b=d;while(b){const t=a%b;a=b;b=t;}return [n/(a||1n),d/(a||1n)];}
  function dec(v,signed=false){if(typeof v!=='string'||!(signed?/^-?\d{1,40}(?:\.\d{1,12})?$/:/^\d{1,30}(?:\.\d{1,12})?$/).test(v))throw Error('Exact decimal required');const parts=v.split('.');return fraction(v.replace('.',''),10n**BigInt(parts[1]?.length||0));}
  const add=(a,b)=>fraction(a[0]*b[1]+b[0]*a[1],a[1]*b[1]);
  const sub=(a,b)=>fraction(a[0]*b[1]-b[0]*a[1],a[1]*b[1]);
  const mul=(a,b)=>fraction(a[0]*b[0],a[1]*b[1]);
  const div=(a,b)=>{if(!b[0])throw Error('Zero divisor');return fraction(a[0]*b[1]*(b[0]<0n?-1n:1n),a[1]*(b[0]<0n?-b[0]:b[0]));};
  const same=(a,b)=>a[0]===b[0]&&a[1]===b[1];
  function rounded(v,places=2){const neg=v[0]<0n,n=(neg?-v[0]:v[0])*10n**BigInt(places);let q=n/v[1],r=n%v[1];if(r*2n>v[1]||(r*2n===v[1]&&q%2n))q++;const s=q.toString().padStart(places+1,'0');return (neg&&q?'-':'')+(places?s.slice(0,-places)+'.'+s.slice(-places):s);}
  function rational(v,places=2){if(!v||! /^-?\d{1,130}$/.test(v.numerator)||!/^\d{1,130}$/.test(v.denominator))throw Error('Exact rational required');const f=fraction(v.numerator,v.denominator);if(f[0].toString()!==v.numerator||f[1].toString()!==v.denominator||rounded(f,places)!==v.display_decimal)throw Error('Rational display differs');return f;}
  function assertValue(v,expected,places=2){if(!same(rational(v,places),expected))throw Error('Reported value arithmetic differs');}
  function validateManager(doc,packet,fund){
    const entry=packet.managers[fund];
    if(!entry||doc.contract!=='capital-holdings-value-bridge.v1'||doc.fund!==fund||doc.cik!==entry.cik||doc.official_name!==entry.official_name||doc.current_report_period!==entry.current_report_period||doc.prior_report_period!==entry.prior_report_period||doc.current_chain_status!==entry.current_chain_status||doc.prior_chain_status!==entry.prior_chain_status||doc.comparison_available!==entry.comparison_available||!equal(doc.value_reviews,entry.value_reviews)||!equal(doc.source,{native_fund:entry.native_fund,...packet.source.holdings})||!Array.isArray(doc.rows)||doc.rows.length!==entry.record_count||doc.record_count!==entry.record_count)throw Error('Complete manager binding differs');
    if(doc.calls_eligible!==false||doc.sizing_eligible!==false||doc.execution_inferred!==false||doc.corporate_actions_adjusted!==false)throw Error('Disclosure authority differs');
    const seen=new Set(),scopes={},statuses={};
    for(const row of doc.rows){
      const scope=row.identity.quantity_type+'|'+(row.identity.put_call||'NONE');
      if(!Object.hasOwn(SCOPES,scope)||!/^[a-f0-9]{64}$/.test(row.position_id)||seen.has(row.position_id)||(!row.current&&!row.prior))throw Error('Disclosure identity differs');
      seen.add(row.position_id);const g=scopes[scope]||={current:fraction(0),prior:fraction(0),matched:fraction(0),newly:fraction(0),absent:fraction(0),quantity:fraction(0),unit:fraction(0),rounding:fraction(0),other:fraction(0),count:0,decomposed:0};g.count++;
      const a=row.current,b=row.prior,va=a?dec(a.reported_value_usd):fraction(0),vb=b?dec(b.reported_value_usd):fraction(0),qa=a?dec(a.quantity):null,qb=b?dec(b.quantity):null;
      g.current=add(g.current,va);g.prior=add(g.prior,vb);
      const status=!doc.comparison_available?'comparison_unavailable':!b?'newly_present_in_public_disclosure':!a?'not_present_in_current_public_disclosure':'matched_public_identity';
      if(row.status!==status)throw Error('Comparison missingness differs');
      let expectedBridge=!doc.comparison_available?'comparison_unavailable':!a||!b?'identity_not_reported_in_both_periods':scope!=='SH|NONE'?'option_or_principal_scope_not_decomposed':doc.value_reviews.current.length||doc.value_reviews.prior.length?'reported_value_requires_review':!qa[0]||!qb[0]?'zero_quantity_unit_value_unavailable':'exact_reported_value_identity';
      if(row.bridge.status!==expectedBridge)throw Error('Bridge eligibility differs');statuses[expectedBridge]=(statuses[expectedBridge]||0)+1;
      if(doc.comparison_available&&a&&b){assertValue(row.reported_quantity_change,sub(qa,qb),6);assertValue(row.reported_value_change_usd,sub(va,vb));g.matched=add(g.matched,sub(va,vb));}
      else if(row.reported_quantity_change!==null||row.reported_value_change_usd!==null)throw Error('Unavailable change became a number');
      if(!doc.comparison_available)continue;
      if(!b)g.newly=add(g.newly,va);else if(!a)g.absent=add(g.absent,vb);
      if(expectedBridge==='exact_reported_value_identity'){
        const pa=div(va,qa),pb=div(vb,qb),quantity=div(mul(sub(qa,qb),add(pa,pb)),fraction(2)),unit=div(mul(sub(pa,pb),add(qa,qb)),fraction(2));
        assertValue(row.bridge.prior_implied_reported_value_per_share_usd,pb,8);assertValue(row.bridge.current_implied_reported_value_per_share_usd,pa,8);
        assertValue(row.bridge.quantity_term_usd,quantity);assertValue(row.bridge.unit_value_term_usd,unit);assertValue(row.bridge.reported_value_change_usd,sub(va,vb));
        const qd=dec(rounded(quantity),true),ud=dec(rounded(unit),true),residual=sub(sub(va,vb),add(qd,ud));assertValue(row.bridge.display_rounding_residual_usd,residual);
        g.quantity=add(g.quantity,qd);g.unit=add(g.unit,ud);g.rounding=add(g.rounding,residual);g.decomposed++;
      }else if(a&&b)g.other=add(g.other,sub(va,vb));
    }
    const statusKeys=Object.keys(statuses).sort();if(!equal(statusKeys,Object.keys(doc.bridge_status_counts).sort())||statusKeys.some(k=>statuses[k]!==doc.bridge_status_counts[k])||!equal(doc.bridge_status_counts,entry.bridge_status_counts)||!equal(Object.keys(scopes).sort(),Object.keys(doc.scopes).sort()))throw Error('Complete manager coverage differs');
    for(const [scope,g] of Object.entries(scopes)){
      const s=doc.scopes[scope];if(s.identity_count!==g.count||s.decomposed_identity_count!==g.decomposed)throw Error('Scope count differs');
      for(const [key,value] of [['current_reported_value_usd',doc.current_chain_status==='complete_selected_public_chain'?g.current:null],['prior_reported_value_usd',doc.prior_chain_status==='complete_selected_public_chain'?g.prior:null],['change_in_reported_table_value_usd',sub(g.current,g.prior)],['matched_reported_value_change_usd',g.matched],['newly_disclosed_reported_value_usd',g.newly],['absent_prior_reported_value_usd',g.absent],['sum_display_quantity_terms_usd',g.decomposed?g.quantity:null],['sum_display_unit_value_terms_usd',g.decomposed?g.unit:null],['display_rounding_adjustment_usd',g.decomposed?g.rounding:null],['matched_value_change_not_decomposed_usd',g.other]]){
        const available=key==='current_reported_value_usd'||key==='prior_reported_value_usd'||doc.comparison_available;
        if(!available||value===null){if(s[key]!==null)throw Error('Unavailable scope became a number');}else assertValue(s[key],value);
      }
    }
    return doc;
  }
  function rows(doc,scope,status,search,page=0,size=30){const q=(search||'').trim().toUpperCase(),values=doc.rows.filter(r=>(scope==='all'||r.identity.quantity_type+'|'+(r.identity.put_call||'NONE')===scope)&&(status==='all'||r.bridge.status===status)&&(!q||[r.identity.cusip,r.identity.class,...r.issuer_names].some(v=>String(v).toUpperCase().includes(q))));const pages=Math.max(1,Math.ceil(values.length/size)),at=Math.min(Math.max(0,page),pages-1);return {rows:values.slice(at*size,(at+1)*size),count:values.length,pages,page:at};}
  function originalLink(packet,fund,row){const key=packet.source.holdings.native_research.manifest_key,match=/^data\/holdings-research\/runs\/([a-f0-9]{64})\.json$/.exec(key);if(!match||!Object.hasOwn(packet.managers,fund))throw Error('Original source binding differs');return '/holdings-research.html?'+new URLSearchParams({run:match[1],fund,search:row.identity.cusip});}
  function displayDecimal(value){if(value===null||value===undefined)return 'Unavailable';if(typeof value!=='string'||! /^-?\d+(?:\.\d+)?$/.test(value))throw Error('Decimal display requires an exact string');const [a,b]=value.split('.');return a.replace(/\B(?=(\d{3})+(?!\d))/g,',')+(b?'.'+b:'');}
  const api={PREFIX,CONTRACT,SCOPES,verified,load,fraction,dec,add,sub,mul,div,same,rounded,rational,validateManager,rows,originalLink,displayDecimal};
  if(typeof module==='object'&&module.exports)module.exports=api;else root.JHCapitalResearch=api;
})(typeof window==='object'?window:globalThis);
