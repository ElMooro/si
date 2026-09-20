(function(root){
  'use strict';
  const PREFIX='data/holdings-overlap/',CONTRACT='holdings-disclosure-overlap.v1',LIMIT=64*1024*1024;
  const SCOPES={'SH|NONE':'Shares · non-option','SH|CALL':'Shares · calls','SH|PUT':'Shares · puts','PRN|NONE':'Principal · non-option','PRN|CALL':'Principal · calls','PRN|PUT':'Principal · puts'};
  const equal=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
  async function bytes(response){if(!response.ok)throw Error('Evidence request failed: HTTP '+response.status);const raw=new Uint8Array(await response.arrayBuffer());if(!raw.length||raw.length>LIMIT)throw Error('Complete evidence exceeds the size limit');return raw;}
  async function verified(fetcher,ref,kind){
    if(!ref||!/^\w{64}$/.test(ref.sha256)||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+'.json'||!Number.isSafeInteger(ref.bytes)||ref.bytes<=0||ref.bytes>LIMIT)throw Error('Evidence identity differs');
    const raw=await bytes(await fetcher('/'+ref.key));
    const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');
    if(hash!==ref.sha256||raw.length!==ref.bytes)throw Error('Evidence hash or length differs');
    return JSON.parse(new TextDecoder().decode(raw));
  }
  async function load(fetcher,run){
    let pointer=null,ref;
    if(run){if(!/^[a-f0-9]{64}$/.test(run))throw Error('Invalid permanent snapshot');const raw=await bytes(await fetcher('/'+PREFIX+'runs/'+run+'.json'));ref={key:PREFIX+'runs/'+run+'.json',sha256:run,bytes:raw.length};}
    else {pointer=JSON.parse(new TextDecoder().decode(await bytes(await fetcher('/data/smart-money-clusters.json',{cache:'no-store'}))));if(pointer.contract!==CONTRACT)throw Error('Native disclosure overlap is not published yet. Use the original holdings desk while the legacy scoring model is retired.');ref=pointer.replay;}
    const manifest=await verified(fetcher,ref,'runs'),packet=await verified(fetcher,manifest.output,'outputs');
    if(manifest.contract!=='holdings-overlap-replay.v1'||packet.contract!==CONTRACT||!equal(packet.source,manifest.source))throw Error('Snapshot binding differs');
    if(pointer){const {replay,...value}=pointer;if(!equal(value,packet))throw Error('Current snapshot differs from retained evidence');}
    if(Object.keys(packet.funds||{}).length!==packet.counts?.funds_in_roster)throw Error('Complete roster count differs');
    return {packet,manifest,ref};
  }
  function pct(n,d){
    if(!d)return null;
    const num=BigInt(n)*100000000n,den=BigInt(d);let q=num/den;const rem=num%den;
    if(rem*2n>den||(rem*2n===den&&q%2n===1n))q++;
    const s=q.toString().padStart(7,'0');return s.slice(0,-6)+'.'+s.slice(-6);
  }
  function validateScope(doc,packet,period,scope){
    const cohort=packet.cohorts[period],ref=cohort.scopes[scope];
    if(doc.contract!=='holdings-overlap-scope.v1'||doc.report_period!==period||doc.instrument_scope!==scope||!equal(doc.source,packet.source)||!equal(doc.funds,cohort.funds)||!Array.isArray(doc.securities)||doc.securities.length!==doc.security_count||doc.security_count!==ref.security_count||!Array.isArray(doc.pairs)||doc.pairs.length!==ref.pair_count)throw Error('Complete cohort/scope binding differs');
    const sets=Object.fromEntries(doc.funds.map(f=>[f,new Set()])),seen=new Set();let reported=0,positive=0;
    for(const row of doc.securities){
      if(seen.has(row.position_id)||!/^[a-f0-9]{64}$/.test(row.position_id)||!Array.isArray(row.reporters)||row.identity.quantity_type+'|'+(row.identity.put_call||'NONE')!==scope)throw Error('Disclosure identity differs');
      seen.add(row.position_id);const reporters=new Set();let positives=0;
      for(const r of row.reporters){if(!Object.hasOwn(sets,r.fund)||reporters.has(r.fund)||!/^\d{1,30}(?:\.\d{1,12})?$/.test(r.reported_quantity))throw Error('Manager disclosure differs');reporters.add(r.fund);const yes=BigInt(r.reported_quantity.replace('.',''))>0n;if(yes!==r.positive_reported_quantity)throw Error('Reported quantity membership differs');if(yes){sets[r.fund].add(row.position_id);positives++;}}
      if(row.reported_manager_count!==reporters.size||row.positive_quantity_manager_count!==positives)throw Error('Disclosure count differs');reported+=reporters.size;positive+=positives;
    }
    if(reported!==doc.manager_disclosure_count||positive!==doc.positive_manager_disclosure_count||doc.pairs.length!==doc.funds.length*(doc.funds.length-1)/2)throw Error('Complete scope counts differ');
    const pairs=new Set();
    for(const p of doc.pairs){const a=sets[p.fund_a],b=sets[p.fund_b],key=p.fund_a+'|'+p.fund_b;if(!a||!b||p.fund_a>=p.fund_b||pairs.has(key))throw Error('Manager pair differs');pairs.add(key);const shared=[...a].filter(id=>b.has(id)).sort(),union=a.size+b.size-shared.length;if(!equal(shared,p.shared_position_ids)||p.shared_count!==shared.length||p.union_count!==union||p.count_a!==a.size||p.count_b!==b.size||p.jaccard_pct!==pct(shared.length,union)||p.coverage_a_pct!==pct(shared.length,a.size)||p.coverage_b_pct!==pct(shared.length,b.size))throw Error('Overlap arithmetic differs');}
    return doc;
  }
  function pair(doc,a,b){return doc.pairs.find(p=>(p.fund_a===a&&p.fund_b===b)||(p.fund_a===b&&p.fund_b===a))||null;}
  function rows(doc,a,b,mode,search,page=0,size=40){
    const selected=pair(doc,a,b),shared=new Set(selected?.shared_position_ids||[]),q=(search||'').trim().toUpperCase();
    const values=doc.securities.filter(r=>{const positive=r.reporters.filter(v=>v.positive_reported_quantity).map(v=>v.fund);return(mode==='all'||(mode==='shared'&&shared.has(r.position_id))||(mode==='a'&&positive.includes(a))||(mode==='b'&&positive.includes(b))||(mode==='zero'&&r.reporters.some(v=>!v.positive_reported_quantity)))&&(!q||[r.identity.cusip,r.identity.class,...r.issuer_names].some(v=>String(v).toUpperCase().includes(q)));});
    const pages=Math.max(1,Math.ceil(values.length/size)),at=Math.min(Math.max(0,page),pages-1);
    return {rows:values.slice(at*size,(at+1)*size),count:values.length,pages,page:at};
  }
  function originalLink(packet,fund,row){
    const key=packet.source.native_research.manifest_key,match=/^data\/holdings-research\/runs\/([a-f0-9]{64})\.json$/.exec(key);
    if(!match||!Object.hasOwn(packet.funds,fund))throw Error('Original research binding differs');
    return '/holdings-research.html?'+new URLSearchParams({run:match[1],fund,search:row.identity.cusip});
  }
  const api={PREFIX,CONTRACT,SCOPES,verified,load,pct,validateScope,pair,rows,originalLink};
  if(typeof module==='object'&&module.exports)module.exports=api;else root.JHHoldingsOverlap=api;
})(typeof window==='object'?window:globalThis);
