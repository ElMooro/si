/* Provider-reported sovereign observations; no forecast or portfolio authority. */
(function(root){
  'use strict';
  const PATHS={current:'/data/global-sovereign.json',
    archive:'/data/global-sovereign-longhistory.json',universe:'/config/global-sovereign-universe.json'};
  const finite=n=>typeof n==='number'&&Number.isFinite(n);
  const fmt=(n,d=2)=>finite(n)?n.toFixed(d):'Unavailable';
  const text=v=>typeof v==='string'&&v.trim()?v:'Unavailable';
  function clock(value){
    if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/.test(value)||date(value.slice(0,10))===null)return null;
    const n=Date.parse(value);return Number.isFinite(n)?n:null;
  }
  function date(value){
    if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value))return null;
    const n=Date.parse(value+'T00:00:00Z');
    return Number.isFinite(n)&&new Date(n).toISOString().slice(0,10)===value?n:null;
  }
  function countryView(packet,universe,now=Date.now()){
    if(!packet||!Array.isArray(packet.countries)||!universe||universe.contract!=='sovereign-review-universe.v1'||!Array.isArray(universe.countries))throw new Error('Country structure unavailable');
    const at=clock(packet.generated_at);if(at===null||at>now)throw new Error('Invalid publication clock');
    const known=new Map(),found=new Map();
    for(const row of universe.countries){
      if(!row||typeof row.country!=='string'||!row.country.trim()||known.has(row.country)||!/^[-a-z]+$/.test(row.slug)||typeof row.region!=='string')throw new Error('Country universe differs');
      known.set(row.country,row);
    }
    if(!known.size)throw new Error('Empty country universe');
    for(const row of packet.countries){
      if(!row||typeof row.country!=='string'||!row.country.trim()||found.has(row.country))throw new Error('Ambiguous country identity');
      found.set(row.country,row);
    }
    if(packet.n_countries!==found.size)throw new Error('Country count differs');
    const rows=Array.from(new Set([...known.keys(),...found.keys()])).sort((a,b)=>a.localeCompare(b)).map(country=>{
      const ref=known.get(country),source=found.get(country);
      return {country,region:ref?ref.region:text(source.region),source:source||null,
        provider:ref?'https://www.worldgovernmentbonds.com/country/'+ref.slug+'/':null,
        status:!source?'Missing country response':!ref?'Unregistered country; review required':'Provider values; definitions unverified'};
    });
    return {rows,at:packet.generated_at,overdue:now-at>26*3600000,expected:known.size,
      present:Array.from(known.keys()).filter(k=>found.has(k)).length,extras:Array.from(found.keys()).filter(k=>!known.has(k)).length,
      cds:rows.filter(r=>r.source&&finite(r.source.cds_bp)).length,
      yields:rows.filter(r=>r.source&&finite(r.source.yield_10y_pct)).length,
      sourceClockQualified:0,authority:false};
  }
  function historyView(doc,kind){
    const rows=kind==='daily'?doc:doc&&doc.history;
    if(!['daily','archive'].includes(kind)||!Array.isArray(rows))throw new Error('Whole history unavailable');
    const seen=new Set();
    for(const row of rows){
      if(!row||date(row.date)===null||seen.has(row.date))throw new Error('Ambiguous history date');
      seen.add(row.date);
    }
    if(kind==='archive'&&doc.n_points!==rows.length)throw new Error('History count differs');
    const sorted=rows.slice().sort((a,b)=>a.date.localeCompare(b.date));
    return {rows:sorted,first:sorted[0]?.date||null,last:sorted[sorted.length-1]?.date||null,
      fields:Array.from(new Set(['date',...rows.flatMap(r=>Object.keys(r))])),
      generatedAt:kind==='archive'?text(doc.generated_at):null};
  }
  function calendarChanges(current,history){
    const at=clock(current&&current.generated_at),value=current&&current.eurodollar_hub_stress_0_100;
    const result={seven:null,thirty:null};if(at===null||!finite(value))return result;
    const today=new Date(at).toISOString().slice(0,10),byDate=new Map(history.map(r=>[r.date,r]));
    if(byDate.get(today)?.stress!==value)return result; // no mixed projections
    for(const [key,n] of [['seven',7],['thirty',30]]){
      const target=new Date(date(today)-n*86400000).toISOString().slice(0,10),prior=byDate.get(target)?.stress;
      if(finite(prior))result[key]={value:value-prior,from:target,to:today};
    }
    return result;
  }
  function captureStatus(packet,universe){
    const e=packet&&packet.source_evidence;
    if(!e)return 'This publication has no provider-response capture record. Original acquisition is pending its normal schedule.';
    const slugs=new Set(universe.countries.map(r=>r.slug)),coverage=e.coverage,ref=e.manifest;
    const valid=e.contract==='sovereign-source-capture.v1'&&e.configured_countries===slugs.size&&Array.isArray(coverage)&&coverage.length===slugs.size&&new Set(coverage.map(r=>r?.slug)).size===slugs.size&&coverage.every(r=>r&&slugs.has(r.slug))&&ref&&/^[a-f0-9]{64}$/.test(ref.sha256)&&ref.key==='audit-private/20260909-originals/global-sovereign-research/'+ref.sha256+'.bin'&&Number.isInteger(ref.bytes)&&ref.bytes>0&&Number.isInteger(e.requests_attempted)&&e.requests_attempted>=slugs.size&&e.requests_attempted<=slugs.size*2&&Number.isInteger(e.complete_responses_retained)&&e.complete_responses_retained>=0&&e.complete_responses_retained<=e.requests_attempted&&['definitions_verified','observation_clocks_verified','original_source_replay_verified'].every(k=>e[k]===false);
    if(!valid)return 'Provider capture metadata is inconsistent. Source verification remains unavailable.';
    return 'Producer reports '+e.complete_responses_retained+' complete retained responses from '+e.requests_attempted+' requests across '+slugs.size+' countries. Originals remain in protected storage. This browser has not replayed them; definitions, quote clocks and risk interpretations remain unverified.';
  }
  async function load(kind,options={}){
    if(!Object.hasOwn(PATHS,kind))throw new Error('Unknown sovereign source');
    const fetcher=options.fetcher||root.fetch.bind(root),limit=kind==='universe'?65536:32*1024*1024;
    const controller=new AbortController();let timer,reader;
    const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new Error('Sovereign request timed out'));},options.timeout||15000);});
    try{return await Promise.race([deadline,(async()=>{
      const response=await fetcher(PATHS[kind]+'?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});
      if(!response.ok||!response.body?.getReader)throw new Error('Sovereign source unavailable');
      reader=response.body.getReader();const parts=[];let size=0;
      for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;
        if(size>limit)throw new Error('Complete sovereign response exceeds limit');parts.push(value);}
      const bytes=new Uint8Array(size);let offset=0;for(const part of parts){bytes.set(part,offset);offset+=part.byteLength;}
      return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
    })()]);}finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
  }
  function element(doc,tag,value,parent){const el=doc.createElement(tag);if(value!==undefined)el.textContent=value;if(parent)parent.appendChild(el);return el;}
  function table(doc,host,headers,rows){
    host.replaceChildren();const t=element(doc,'table',undefined,host),head=element(doc,'tr',undefined,element(doc,'thead',undefined,t));
    for(const label of headers){const th=element(doc,'th',label,head);th.scope='col';}
    const body=element(doc,'tbody',undefined,t);
    for(const values of rows){const tr=element(doc,'tr',undefined,body);for(const value of values){const td=element(doc,'td',undefined,tr);
      if(value&&typeof value==='object'&&value.tagName)td.appendChild(value);else td.textContent=value;}}
    return t;
  }
  function rawValue(value){return value===null||value===undefined?'Unavailable':typeof value==='object'?JSON.stringify(value):String(value);}
  function historyRender(doc,kind,view){
    const host=doc.getElementById(kind+'-table');
    table(doc,host,view.fields.map(k=>k==='stress'?'Legacy index points':k==='yoy_pct'?'Archived YoY % (unverified)':k),
      view.rows.map(r=>view.fields.map(k=>rawValue(r[k]))));
    doc.getElementById(kind+'-status').textContent=view.rows.length+' complete stored rows · '+(view.first||'no first date')+' to '+(view.last||'no last date')+(view.generatedAt?' · file generated '+view.generatedAt:'');
    const svg=doc.getElementById(kind+'-chart');svg.replaceChildren();
    const pts=view.rows.filter(r=>finite(r.stress));if(!pts.length)return;
    const xmin=date(view.first),xmax=date(view.last),values=pts.map(r=>r.stress),ymin=values.reduce((a,b)=>Math.min(a,b),Infinity),ymax=values.reduce((a,b)=>Math.max(a,b),-Infinity);
    const add=(tag,attrs,label)=>{const n=doc.createElementNS('http://www.w3.org/2000/svg',tag);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);if(label!==undefined)n.textContent=label;svg.appendChild(n);return n;};
    add('title',{},'Unqualified '+kind+' index archive; points show stored dates without interpolating gaps');
    add('line',{x1:50,x2:950,y1:165,y2:165,stroke:'#596583'});
    for(const row of pts){const x=50+(date(row.date)-xmin)/(xmax-xmin||1)*900,y=150-(row.stress-ymin)/(ymax-ymin||1)*120;
      const dot=add('circle',{cx:x,cy:y,r:2,fill:'#8dabea'});const title=doc.createElementNS('http://www.w3.org/2000/svg','title');title.textContent=row.date+': '+row.stress+' legacy index points';dot.appendChild(title);}
    add('text',{x:8,y:28,fill:'#c5d0e7','font-size':12},fmt(ymax,1));add('text',{x:8,y:153,fill:'#c5d0e7','font-size':12},fmt(ymin,1));
    add('text',{x:50,y:187,fill:'#c5d0e7','font-size':12},view.first);add('text',{x:950,y:187,fill:'#c5d0e7','text-anchor':'end','font-size':12},view.last);
  }
  async function mount(doc,loader=load,now=Date.now()){
    let current=null,daily=null,rows=[],sort='country',region='All',query='';
    const say=(id,value)=>{doc.getElementById(id).textContent=value;};
    function changes(){const value=current&&daily?calendarChanges(current,daily.rows):{seven:null,thirty:null};
      for(const [key,id] of [['seven','change-seven'],['thirty','change-thirty']])say(id,value[key]?fmt(value[key].value,1)+' points · '+value[key].from+' → '+value[key].to:'Unavailable: exact matching snapshot dates required');}
    const columns=[['yield_10y_pct','Reported 10Y yield (%)',2],['cds_bp','Reported CDS (bp)',1],['spread_vs_bund_bp','Provider spread field (bp; benchmark unverified)',1],
      ['cb_rate_pct','Reported policy rate (%)',2],['cds_default_prob_pct','Provider default probability (%; unverified)',2]];
    function renderCountries(){
      const filtered=rows.filter(r=>(region==='All'||r.region===region)&&r.country.toLowerCase().includes(query.toLowerCase()));
      filtered.sort((a,b)=>{if(sort==='country')return a.country.localeCompare(b.country);
        const x=a.source&&a.source[sort],y=b.source&&b.source[sort];return finite(x)&&finite(y)?y-x:finite(x)?-1:finite(y)?1:a.country.localeCompare(b.country);});
      table(doc,doc.getElementById('country-table'),['Country / provider page','Region','Coverage',...columns.map(c=>c[1]),'Rating (agency/date unverified)','Provider date text (unverified)'],filtered.map(row=>{
        const label=element(doc,'span',row.country);if(row.provider){const a=element(doc,'a',row.country);a.href=row.provider;label.replaceChildren(a);}
        return [label,row.region,row.status,...columns.map(([k,,d])=>fmt(row.source&&row.source[k],d)),text(row.source&&row.source.rating),text(row.source&&row.source.as_of)];}));
      say('country-filter-status',filtered.length+' of '+rows.length+' review rows shown. Sorting does not rank sovereign creditworthiness.');
    }
    const currentLoad=Promise.resolve().then(()=>loader('current'));
    const primary=(async()=>{try{
      const [packet,universe]=await Promise.all([currentLoad,loader('universe')]);const view=countryView(packet,universe,now);current=packet;rows=view.rows;
      say('publication',packet.generated_at+' · '+(view.overdue?'publication overdue (>26h)':'publication within 26h display window')+' · quote freshness unverified');
      say('source-capture-status',captureStatus(packet,universe));
      say('country-count',view.present+' / '+view.expected);say('cds-count',view.cds+' reported');say('yield-count',view.yields+' reported');say('missing-count',(view.expected-view.present)+' missing · '+view.extras+' unregistered');
      const select=doc.getElementById('region');select.replaceChildren();for(const name of ['All',...new Set(rows.map(r=>r.region))]){const option=element(doc,'option',name,select);option.value=name;}
      select.addEventListener('change',()=>{region=select.value;renderCountries();});
      doc.getElementById('country-search').addEventListener('input',e=>{query=e.target.value;renderCountries();});
      doc.getElementById('sort').addEventListener('change',e=>{sort=e.target.value;renderCountries();});renderCountries();changes();
      const diagnostic=packet; // complete packet: no inherited field disappears from inspection
      doc.getElementById('legacy-fields').textContent=JSON.stringify(diagnostic,null,2);
    }catch(_){current=null;rows=[];doc.getElementById('country-table').replaceChildren();doc.getElementById('legacy-fields').textContent='Unavailable';say('source-capture-status','Provider capture status unavailable.');
      for(const id of ['country-count','cds-count','yield-count','missing-count'])say(id,'Unavailable');say('publication','Country packet or reviewed universe is unavailable or invalid. No current values are shown.');changes();}})();
    const histories=['daily','archive'].map(async kind=>{try{
      let source;if(kind==='daily'){const packet=await currentLoad;source=packet.eurodollar_hub_history;
        if(!Array.isArray(source)||packet.eurodollar_hub_history_n!==source.length)throw new Error('Embedded daily history incomplete');}
      else source=await loader(kind);
      const view=historyView(source,kind);historyRender(doc,kind,view);if(kind==='daily'){daily=view;changes();}}
      catch(_){if(kind==='daily'){daily=null;changes();}doc.getElementById(kind+'-table').replaceChildren();doc.getElementById(kind+'-chart').replaceChildren();say(kind+'-status','Complete history unavailable or invalid. No interpolated or substituted data is shown.');}});
    return Promise.allSettled([primary,...histories]);
  }
  if(typeof module==='object'&&module.exports)module.exports={countryView,historyView,calendarChanges,captureStatus,load,mount,fmt,date,clock};
  if(root.document?.getElementById('sovereign-review'))mount(root.document);
})(typeof window==='undefined'?globalThis:window);
