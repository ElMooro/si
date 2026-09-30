/* jh-reskin-skip */
(function(root){
  'use strict';
  const io=typeof module==='object'&&module.exports?require('./jh-evidence-io.js'):root.JHEvidenceIO;
  const PREFIX='data/term-premium-research/',LIMIT=64*1024*1024,DAY=86400000;
  const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified'];
  const FAMILIES={ACMY:'Fitted yield',ACMTP:'Term premium',ACMRNY:'Risk-neutral yield'};
  const IDS=['D','M'].flatMap(f=>Object.keys(FAMILIES).flatMap(p=>Array.from({length:10},(_,i)=>f+':'+p+String(i+1).padStart(2,'0'))));
  const day=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(s)&&Number.isFinite(Date.parse(s+'T00:00:00Z'))&&new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s?Date.parse(s+'T00:00:00Z'):NaN;
  const clock=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(s)&&Number.isFinite(day(s.slice(0,10)))&&Number.isFinite(Date.parse(s))?Date.parse(s):NaN;
  const recent=(s,now)=>Number.isFinite(clock(s))&&clock(s)<=now&&now-clock(s)<=26*3600000;
  const number=v=>typeof v==='number'&&Number.isFinite(v);
  const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const exact=s=>typeof s==='string'&&/^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(s)&&Number.isFinite(Number(s));
  const same=(a,b)=>JSON.stringify(sort(a))===JSON.stringify(sort(b));
  function sort(v){return Array.isArray(v)?v.map(sort):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,sort(v[k])])):v;}
  async function bytes(url,fetcher,limit=LIMIT,timeoutMs=15000,options={}){
    return io.readComplete(async signal=>{
      const response=await fetcher(url,{cache:'no-store',credentials:'omit',signal});
      if(response?.status!==200||response.headers?.get('Content-Range')!==null){
        try{Promise.resolve(response?.body?.cancel?.()).catch(()=>{});}catch{}
        throw Error('Complete artifact unavailable');
      }
      return response;
    },{...options,limit,timeoutMs});
  }

  async function hash(raw,crypto){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');}
  function identity(ref,category,ext='json'){return ref&&/^[a-f0-9]{64}$/.test(ref.sha256)&&ref.key===PREFIX+category+'/'+ref.sha256+'.'+ext&&Number.isInteger(ref.bytes)&&ref.bytes>0&&ref.bytes<=LIMIT;}
  function qualified(p){return p?.contract==='term-premium-research.v1'&&FLAGS.every(k=>p[k]===false)&&p.call===null&&same(p.decision,{verb:'WAIT',meaning:'abstain'})&&same(p.signals,[])&&same(p.portfolio_consequences,{status:'UNAVAILABLE',target_weights:null})&&
    same(p.dependency_graph?.model_roots,['NYFED:ACM'])&&p.dependency_graph?.independent_votes===0&&p.view?.contract==='term-premium-summary.v1'&&same(Object.keys(p.series||{}).sort(),IDS.slice().sort())&&
    p.source?.source_url==='https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.xls'&&/^[a-f0-9]{64}$/.test(p.source.sha256)&&Number.isInteger(p.source.bytes)&&p.source.bytes>0&&p.source.bytes<=LIMIT&&
    same(Object.keys(p.tables||{}).sort(),['ACM Daily','ACM Monthly'])&&Object.values(p.tables).every(t=>identity(t.complete_table_artifact,'tables'))&&
    /^data\/term-premium-research\/runs\/[a-f0-9]{64}\.json$/.test(p.replay?.manifest_key)&&/^[a-f0-9]{64}$/.test(p.replay?.output_sha256);}
  function view(p,now=Date.now()){
    const valid=qualified(p),current=valid&&recent(p.generated_at,now)&&recent(p.source.acquired_at,now)&&clock(p.source.acquired_at)<=clock(p.generated_at)&&p.acquisition?.status==='acquired';
    const rows=IDS.map(sid=>{
      const row=p?.series?.[sid]||{},[frequency,header]=sid.split(':'),prefix=header.replace(/\d+$/,''),tenor=+header.slice(-2),limit=frequency==='D'?7:70;
      const date=row.current?.observation_date,age=Number.isFinite(day(date))?Math.floor(now/DAY)-Math.floor(day(date)/DAY):null;
      const column=tenor+(prefix==='ACMTP'?10:prefix==='ACMRNY'?20:0),table=frequency==='D'?'ACM Daily':'ACM Monthly';
      const eligible=current&&row.series_id===sid&&row.native_header===header&&row.column===column&&row.table===table&&row.frequency===frequency&&row.tenor_years===tenor&&row.unit==='percent'&&row.model==='Adrian-Crump-Moench'&&
        row.family===({ACMY:'fitted_yield',ACMTP:'term_premium',ACMRNY:'risk_neutral_yield'}[prefix])&&FLAGS.every(k=>row[k]===false)&&row.quality?.status==='within_age_ceiling'&&row.quality?.max_observation_age_days===limit&&row.quality?.max_acquisition_age_seconds===26*3600&&
        age!==null&&age>=0&&age<=limit&&day(date)<=Math.floor(clock(p.source.acquired_at)/DAY)*DAY&&number(row.current?.value)&&exact(row.current?.exact_decimal)&&Number(row.current.exact_decimal)===row.current.value;
      return {sid,frequency,header,prefix,tenor,table,column,label:FAMILIES[prefix],eligible,row,
        status:eligible?'Within age ceiling':row.quality?.status==='within_age_ceiling'?'Expired or invalid':row.quality?.status||'Unavailable',
        date:row.last_observed?.observation_date||'Unavailable',value:eligible?row.current.exact_decimal:null};
    });return {valid,current,rows,available:rows.filter(r=>r.eligible).length};
  }
  async function artifact(ref,category,fetcher=globalThis.fetch,crypto=globalThis.crypto,ext='json',signal){
    if(!identity(ref,category,ext))throw Error('Artifact coordinates invalid');
    const raw=await bytes('/'+ref.key+'?exact=1&nogen=1',fetcher,LIMIT,30000,{expectedBytes:ref.bytes,signal});
    if(raw.length!==ref.bytes||await hash(raw,crypto)!==ref.sha256)throw Error('Artifact bytes differ');
    return ext==='json'?io.decode(raw,LIMIT):raw;
  }
  async function verifyView(packet,fetcher=globalThis.fetch,crypto=globalThis.crypto,signal){
    if(!qualified(packet))throw Error('Native contract unavailable');
    const key=packet.replay.manifest_key,raw=await bytes('/'+key+'?exact=1&nogen=1',fetcher,LIMIT,15000,{signal});
    if(key!==PREFIX+'runs/'+await hash(raw,crypto)+'.json')throw Error('Run identity differs');
    const manifest=io.decode(raw,LIMIT);
    if(manifest.contract!=='term-premium-replay.v1'||manifest.output_sha256!==packet.replay.output_sha256||manifest.generated_at!==packet.generated_at)throw Error('Run binding differs');
    const projected=await artifact(manifest.view,'views',fetcher,crypto,'json',signal),copy={...packet};delete copy.replay;
    if(!same(projected,copy))throw Error('Published view differs');
    for(const name of ['ACM Daily','ACM Monthly'])if(!same(packet.tables[name].complete_table_artifact,manifest.tables[name]))throw Error('Worksheet binding differs');
    return manifest;
  }
  async function verifyTable(packet,name,fetcher=globalThis.fetch,crypto=globalThis.crypto,signal){
    if(!qualified(packet)||!['ACM Daily','ACM Monthly'].includes(name))throw Error('Worksheet unavailable');
    const summary=packet.tables[name],table=await artifact(summary.complete_table_artifact,'tables',fetcher,crypto,'json',signal),copy={...summary};
    delete copy.complete_table_artifact;delete copy.retained_data_rows;delete copy.first_date;delete copy.last_date;
    if(!same(copy,Object.fromEntries(Object.entries(table).filter(([k])=>k!=='rows')))||table.rows.length!==summary.retained_data_rows||table.headers.length!==31||table.rows.some((r,i)=>r.original_row!==i+1||r.cells.length!==31||r.cell_types.length!==31||!Number.isFinite(day(r.observation_date))))throw Error('Whole worksheet projection differs');
    return table;
  }
  async function verifyOriginal(packet,manifest,fetcher=globalThis.fetch,crypto=globalThis.crypto,signal){
    const inputs=await artifact(manifest.input,'inputs',fetcher,crypto,'json',signal),source=await artifact(inputs.source,'sources',fetcher,crypto,'json',signal);
    if(inputs.contract!=='term-premium-inputs.v1'||!same(source,packet.source)||inputs.workbook.sha256!==packet.source.sha256||inputs.workbook.bytes!==packet.source.bytes)throw Error('Acquired original binding differs');
    const raw=await artifact(inputs.workbook,'originals',fetcher,crypto,'xls',signal);return {bytes:raw.length,sha256:inputs.workbook.sha256};
  }
  function controller(verify){let sequence=0,packet=null,evidence=null;return {get:()=>packet,proof:()=>evidence,clear:()=>{sequence++;packet=null;evidence=null;},
    async accept(value,signal){const ticket=++sequence;packet=null;evidence=null;const result=await verify(value,signal);if(ticket!==sequence)return false;packet=value;evidence=result;return true;}};}
  function display(value,unit=''){if(value===null||value===undefined||!exact(String(value)))return 'Unavailable';const n=Number(value),rounded=n.toFixed(4).replace(/0+$/,'').replace(/\.$/,'');return rounded+unit+(Number(rounded)!==n?' (rounded)':'');}
  function table(headers,rows){return '<table><thead><tr>'+headers.map(h=>'<th scope="col">'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+r.map(c=>'<td>'+c+'</td>').join('')+'</tr>').join('')+'</tbody></table>';}
  function chart(points,label){
    const usable=points.filter(p=>number(p.y)&&number(p.x));if(!usable.length)return '<p>No verified numeric points.</p>';
    const xs=usable.map(p=>p.x),ys=usable.map(p=>p.y),minX=Math.min(...xs),maxX=Math.max(...xs),minY=Math.min(0,...ys),maxY=Math.max(0,...ys),span=maxY-minY||1;
    const x=v=>46+(v-minX)/(maxX-minX||1)*632,y=v=>180-(v-minY)/span*146;
    let path='',pen=false;for(const p of points){if(!number(p.y)||!number(p.x)){pen=false;continue;}path+=(pen?' L':' M')+x(p.x).toFixed(2)+' '+y(p.y).toFixed(2);pen=true;}
    return '<svg viewBox="0 0 710 224" role="img" aria-label="'+esc(label)+'"><line x1="46" x2="678" y1="'+y(0)+'" y2="'+y(0)+'" class="term-zero"/>'+[minY,maxY].map(v=>'<text x="4" y="'+y(v)+'">'+v.toFixed(2)+'%</text>').join('')+'<path d="'+path+'" class="term-line"/><text x="46" y="211">'+esc(usable[0].label)+'</text><text x="678" y="211" text-anchor="end">'+esc(usable[usable.length-1].label)+'</text></svg>';
  }
  function mount(doc,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!doc.getElementById('term-research'))return;
    const el=id=>doc.getElementById('term-'+id),gate=controller((p,signal)=>verifyView(p,fetcher,crypto,signal)),requests=new Set();
    let packet=null,manifest=null,tables={},offset=0,historyVisible=false,version=0,activity=0,activityRequest=null,timer=null,paused=false,destroyed=false,ageSignature='';
    function request(){const control=new AbortController();requests.add(control);return {signal:control.signal,abort:()=>control.abort(),done:()=>requests.delete(control)};}
    function cancelActivity(){activity++;activityRequest?.abort();activityRequest=null;el('history-status').textContent='';}
    function cancelAll(){for(const control of requests)control.abort();requests.clear();cancelActivity();}
    function chosen(){return el('frequency').value+':'+el('family').value+String(el('tenor').value).padStart(2,'0');}
    function invalidate(message){
      cancelAll();packet=null;manifest=null;tables={};historyVisible=false;offset=0;ageSignature='';gate.clear();
      for(const id of ['metadata','reading','curve','changes','inventory','history','history-chart','pagination','legacy']){el(id).innerHTML='';el(id).textContent='';}
      el('older').disabled=true;el('newer').disabled=true;el('native').hidden=true;el('legacy').hidden=true;el('status').textContent=message;
    }
    const signature=state=>JSON.stringify(state.rows.map(row=>[row.eligible,row.status]));
    function history(){
      const row=view(packet).rows.find(r=>r.sid===chosen()),data=row&&tables[row.table];
      el('history').innerHTML='';el('history-chart').innerHTML='';el('pagination').textContent='';el('older').disabled=true;el('newer').disabled=true;
      if(!historyVisible||!data)return;
      const rows=data.rows.slice().sort((a,b)=>b.observation_date.localeCompare(a.observation_date)),part=rows.slice(offset,offset+50);
      el('history').innerHTML=table(['Observation date','Original row','Original date cell','Source value (%)','Cell type'],part.map(r=>[esc(r.observation_date),r.original_row,esc(r.cells[0]),esc(r.cells[row.column]??'Missing'),r.cell_types[row.column]]));
      el('pagination').textContent=(offset+1)+'–'+Math.min(offset+50,rows.length)+' of '+rows.length+' original rows';
      el('older').disabled=offset+50>=rows.length;el('newer').disabled=offset===0;
      el('history-chart').innerHTML=chart(rows.slice().reverse().map(r=>({x:day(r.observation_date),y:r.cell_types[row.column]===2?r.cells[row.column]:null,label:r.observation_date})),row.sid+' complete acquired vintage history');
    }
    function render(){
      if(!packet)return;
      const state=view(packet),r=state.rows.find(r=>r.sid===chosen()),row=r.row;
      ageSignature=signature(state);
      el('status').textContent='Retained view matched · '+state.available+'/60 current series within reviewed age ceilings · one model root';
      el('native').hidden=false;
      el('metadata').innerHTML='<span>Observed daily: '+esc(state.rows.find(v=>v.sid==='D:ACMY01').date)+'</span><span>Observed monthly: '+esc(state.rows.find(v=>v.sid==='M:ACMY01').date)+'</span><span>Acquired: '+esc(packet.source.acquired_at)+'</span><span>Published: '+esc(packet.generated_at)+'</span>';
      el('reading').innerHTML='<section class="term-hero"><div><p>'+esc(r.label)+' · '+r.tenor+' years · '+(r.frequency==='D'?'daily':'monthly')+'</p><strong>'+esc(display(r.value,r.eligible?'%':''))+'</strong><p>'+esc(r.date)+' · '+esc(r.status)+'</p></div><div><span class="term-badge">'+esc(r.sid)+'</span><p>Model-implied zero-coupon estimate</p><details><summary>Exact value and definition</summary><p class="term-exact">'+esc(r.value??'Current value unavailable')+'</p><p>'+esc(row.instrument_basis)+'</p><p>Workbook column '+row.column+' · worksheet '+esc(r.table)+' · original row '+esc(row.last_observed?.original_row??'unavailable')+'</p></details></div></section>';
      const curve=state.rows.filter(v=>v.frequency===r.frequency&&v.prefix===r.prefix);
      const dated=curve.every(v=>v.eligible)&&new Set(curve.map(v=>v.date)).size===1;
      el('curve').innerHTML=dated?chart(curve.map(v=>({x:v.tenor,y:Number(v.value),label:v.tenor+'y'})),r.label+' by maturity')+'<p class="term-muted">All ten maturities · same observation date '+esc(r.date)+'</p>':'<p>A complete current curve on one date is unavailable.</p>';
      el('changes').innerHTML=table(['Steps','Change (bp)','Start → end','Calendar days / missing dates'],Object.values(row.current_comparisons||row.historical_comparisons).map(c=>[
        c.numeric_observation_steps,r.eligible?'<details><summary>'+esc(display(c.change_bps.exact_decimal))+'</summary><span class="term-exact">'+esc(c.change_bps.exact_decimal??'Unavailable')+'</span></details>':'Unavailable',
        esc((c.baseline?.observation_date||'Unavailable')+' → '+(c.current?.observation_date||'Unavailable')),esc((c.elapsed_calendar_days??'—')+' / '+(c.missing_observation_dates??'—'))]));
      const query=el('search').value.trim().toLowerCase();
      el('inventory').innerHTML=table(['Series','Component','Frequency','Tenor','Current (%)','Observation','Availability'],state.rows.filter(v=>(v.sid+' '+v.label+' '+(v.frequency==='D'?'daily':'monthly')).toLowerCase().includes(query)).map(v=>[
        '<button type="button" data-series="'+v.sid+'">'+v.sid+'</button>',esc(v.label),v.frequency==='D'?'Daily':'Monthly',v.tenor+'y',esc(display(v.value)),esc(v.date),esc(v.status)]));
      for(const button of el('inventory').querySelectorAll('button[data-series]'))button.onclick=()=>{const value=state.rows.find(v=>v.sid===button.dataset.series);cancelActivity();el('frequency').value=value.frequency;el('family').value=value.prefix;el('tenor').value=String(value.tenor);offset=0;render();};
      history();
    }
    async function refresh(){
      if(paused||destroyed)return;
      const ticket=++version;invalidate('Verifying publication…');
      const work=request();
      try{
        const p=io.decode(await bytes('/data/term-premium.json?exact=1&nogen=1',fetcher,LIMIT,15000,{signal:work.signal}),LIMIT);
        if(ticket!==version)return;
        if(p.contract!=='term-premium-research.v1'){
          if(!p.latest||!Number.isFinite(day(p.latest.date)))throw Error('Feed unavailable');
          el('status').textContent='Legacy snapshot · complete original-workbook publication is pending';el('legacy').hidden=false;
          el('legacy').innerHTML='<h2>Previous published snapshot</h2><p>Observation '+esc(p.latest.date)+' · publication '+esc(p.generated_at||'unknown')+'. Acquisition time and original workbook replay are unavailable; these are historical context.</p>'+table(['Legacy field','Reported value'],Object.entries(p.latest).map(([k,v])=>[esc(k),esc(v)]))+'<p>Current source-qualified measurements and model inventory appear after the first normal native publication.</p>';return;
        }
        if(!await gate.accept(p,work.signal)||ticket!==version)return;
        packet=p;manifest=gate.proof();
        render();
      }catch(_){if(ticket===version&&!paused&&!destroyed)invalidate('Publication unavailable or verification failed. Current readings are withheld.');}
      finally{work.done();}
    }
    el('refresh').onclick=refresh;
    for(const name of ['frequency','family','tenor'])el(name).onchange=()=>{cancelActivity();offset=0;render();};
    el('search').oninput=render;
    el('load-history').onclick=async()=>{
      if(!packet||paused||destroyed)return;cancelActivity();const work=request();activityRequest=work;
      const p=packet,ticket=version,action=activity,name=view(p).rows.find(r=>r.sid===chosen()).table;
      el('history-status').textContent='Verifying the complete worksheet…';
      try{const data=await verifyTable(p,name,fetcher,crypto,work.signal);if(ticket!==version||p!==packet||action!==activity)return;tables[name]=data;historyVisible=true;offset=0;el('history-status').textContent=name+' whole worksheet hash verified · '+data.rows.length+' original rows retained';history();}
      catch(_){if(ticket===version&&action===activity){delete tables[name];historyVisible=false;history();el('history-status').textContent='Worksheet verification failed; history withheld.';}}
      finally{work.done();if(activityRequest===work)activityRequest=null;}
    };
    el('verify-source').onclick=async()=>{
      if(!packet||!manifest||paused||destroyed)return;cancelActivity();const work=request();activityRequest=work;
      const p=packet,ticket=version,action=activity;el('history-status').textContent='Checking the original binary workbook and acquisition receipt…';
      try{const result=await verifyOriginal(p,manifest,fetcher,crypto,work.signal);if(ticket===version&&packet===p&&action===activity)el('history-status').textContent='Original workbook verified · '+result.bytes.toLocaleString()+' bytes · SHA-256 '+result.sha256;}
      catch(_){if(ticket===version&&action===activity)invalidate('Original workbook verification failed. Current readings are withheld.');}
      finally{work.done();if(activityRequest===work)activityRequest=null;}
    };
    el('older').onclick=()=>{offset+=50;history();};el('newer').onclick=()=>{offset=Math.max(0,offset-50);history();};
    function startClock(){if(timer===null&&!paused&&!destroyed)timer=root.setInterval(()=>{if(packet&&signature(view(packet))!==ageSignature)render();},60000);}
    function stopClock(){if(timer!==null)root.clearInterval(timer);timer=null;}
    function suspend(){if(destroyed)return;paused=true;++version;stopClock();invalidate('Page paused. Publication will be reverified on return.');}
    function resume(){if(destroyed||!paused)return;paused=false;startClock();return refresh();}
    function destroy(){if(destroyed)return;suspend();destroyed=true;root.removeEventListener?.('pagehide',suspend);root.removeEventListener?.('pageshow',resume);}
    root.addEventListener?.('pagehide',suspend);root.addEventListener?.('pageshow',resume);
    startClock();refresh();return {refresh,suspend,resume,destroy};
  }
  const api={IDS,qualified,view,identity,bytes,artifact,verifyView,verifyTable,verifyOriginal,controller,display,chart,mount};
  if(typeof module==='object')module.exports=api;else root.JHTermPremium=api;
  if(root.document)mount(root.document);
})(typeof globalThis==='object'?globalThis:this);
