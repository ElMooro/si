/* jh-reskin-skip */
(function(root){
  'use strict';
  const io=typeof module==='object'&&module.exports?require('./jh-evidence-io.js'):root.JHEvidenceIO;
  const PREFIX='data/fifx-vol-research/',LIMIT=64*1024*1024;
  const LABELS={VIXCLS:'VIX reported index',DGS10:'10-year Treasury yield',DEXUSEU:'Euro / US dollar',DEXJPUS:'US dollar / Japanese yen',DEXUSUK:'Sterling / US dollar',DTWEXBGS:'Broad US dollar index','^MOVE':'MOVE reported index','^KS11':'KOSPI','^HSI':'Hang Seng','^N225':'Nikkei 225','^GDAXI':'Provider DAX index','^FTSE':'FTSE 100','^FCHI':'CAC 40','000001.SS':'Shanghai Composite','^BSESN':'Sensex','^BVSP':'Ibovespa','^AXJO':'ASX 200','^VHSI':'HSI Volatility Index'};
  const IDS=Object.keys(LABELS),FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified','publication_eligible'];
  const readable=value=>value==null?'Unavailable':({basis_points:'Basis points',index_points:'Index points',percent_log_return:'Log-return %',within_age_ceiling:'Within freshness limits',identity_mismatch:'Source identity mismatch',http_error:'Provider returned an error',fred_csv:'FRED CSV',fred_api:'FRED observations API',yahoo_chart:'Yahoo Finance',not_applicable:'Not applicable',reviewed_source_identity:'Source identity reviewed'}[value]||String(value).replace(/_/g,' '));
  const number=v=>typeof v==='number'&&Number.isFinite(v);
  const clock=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(s)?Date.parse(s):NaN;
  const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function sort(v){return Array.isArray(v)?v.map(sort):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,sort(v[k])])):v;}
  const same=(a,b)=>JSON.stringify(sort(a))===JSON.stringify(sort(b));
  const research=p=>p&&FLAGS.every(k=>p[k]===false);
  const scalar=s=>s&&number(s.value)&&typeof s.calculated_decimal==='string'&&/^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(s.calculated_decimal)&&Number(s.calculated_decimal)===s.value;
  function display(s){if(!scalar(s))return 'Unavailable';return s.value.toLocaleString('en-US',{maximumFractionDigits:4})+(Number(s.value.toFixed(4))!==s.value?' (rounded)':'');}
  function identity(ref,kind,extension='json'){return ref&&/^[a-f0-9]{64}$/.test(ref.sha256)&&ref.key===PREFIX+kind+'/'+ref.sha256+'.'+extension&&Number.isInteger(ref.bytes)&&ref.bytes>0&&ref.bytes<=LIMIT;}
  async function hash(raw,crypto){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),b=>b.toString(16).padStart(2,'0')).join('');}
  async function bytes(url,fetcher,limit=LIMIT,timeoutMs=45000,options={}){
    return io.readComplete(async signal=>{
      const response=await fetcher(url,{cache:'no-store',credentials:'omit',signal});
      if(response?.status!==200||response.headers?.get('Content-Range')!==null){
        try{Promise.resolve(response?.body?.cancel?.()).catch(()=>{});}catch{}
        throw Error('Complete artifact unavailable');
      }
      return response;
    },{...options,limit,timeoutMs});
  }
  async function artifact(ref,kind,fetcher,crypto,extension='json',signal){
    if(!identity(ref,kind,extension))throw Error('Invalid original coordinates');
    const raw=await bytes('/'+ref.key+'?exact=1&nogen=1',fetcher,LIMIT,45000,{expectedBytes:ref.bytes,signal});
    if(raw.length!==ref.bytes||await hash(raw,crypto)!==ref.sha256)throw Error('Artifact bytes differ');
    return extension==='bin'?raw:io.decode(raw,LIMIT);
  }
  function qualified(p){return p?.contract==='fifx-vol-research.v1'&&research(p)&&same(Object.keys(p.series||{}).sort(),IDS.slice().sort())&&
    IDS.every(s=>research(p.series[s])&&p.series[s].source_id===s&&identity(p.series[s].complete_source_artifact,'series'))&&same(p.decision,{verb:'WAIT',meaning:'abstain'})&&
    p.dependency_graph?.independent_votes===0&&p.dependency_graph.statistical_independence_qualified===false&&
    p.call===null&&p.regime===null&&same(p.signals,[])&&p.portfolio_consequences?.status==='UNAVAILABLE'&&p.portfolio_consequences.target_weights===null;}
  function fresh(p,sid,now=Date.now()){
    const row=p?.series?.[sid],stamp=clock(p?.generated_at),expires=clock(row?.current_expires_at),acquired=clock(row?.receipt?.acquired_at);
    const unit=sid==='DGS10'?'basis_points':['VIXCLS','^MOVE','^VHSI'].includes(sid)?'index_points':'percent_log_return';
    return qualified(p)&&row.quality?.status==='within_age_ceiling'&&row.source_identity?.identity_reviewed===true&&row.specification.measurement_unit===unit&&
      row.current?.valid_intervals===true&&row.current.date===row.latest_reported?.date&&
      (!row.specification.window_changes||row.current.max_interval_days<=7)&&scalar(row.current?.estimate)&&
      Number.isFinite(stamp)&&stamp<=now&&Number.isFinite(expires)&&now<expires&&Number.isFinite(acquired)&&acquired<=stamp&&now-acquired<=26*3600000;
  }
  async function verifyView(p,fetcher=root.fetch,crypto=root.crypto,signal){
    if(!qualified(p))throw Error('Native research contract unavailable');const key=p.replay?.manifest_key;
    if(typeof key!=='string'||!/^data\/fifx-vol-research\/runs\/[a-f0-9]{64}\.json$/.test(key))throw Error('Native run missing');
    const raw=await bytes('/'+key+'?exact=1&nogen=1',fetcher,LIMIT,45000,{signal}),digest=await hash(raw,crypto);
    if(key!==PREFIX+'runs/'+digest+'.json')throw Error('Native run hash differs');const run=io.decode(raw,LIMIT);
    if(run.contract!=='fifx-vol-replay.v1'||run.generated_at!==p.generated_at||run.view?.sha256!==p.replay.view_sha256||!same(Object.keys(run.series||{}).sort(),IDS.slice().sort()))throw Error('Native run binding differs');
    const view=await artifact(run.view,'views',fetcher,crypto,'json',signal);
    if(!same(view,Object.fromEntries(Object.entries(p).filter(([k])=>k!=='replay')))||IDS.some(s=>!same(p.series[s].complete_source_artifact,run.series[s])))throw Error('Published view differs');
    return run;
  }
  async function verifySeries(p,sid,fetcher=root.fetch,crypto=root.crypto,signal){
    if(!qualified(p)||!IDS.includes(sid))throw Error('Source unavailable');const summary=p.series[sid];
    const whole=await artifact(summary.complete_source_artifact,'series',fetcher,crypto,'json',signal);
    if(whole.contract!=='fifx-source-candidate.v1'||!research(whole)||whole.source_id!==sid||whole.generated_at!==p.generated_at||
      whole.original_rows?.length!==summary.retained_original_rows||whole.history?.length!==summary.retained_history_rows)throw Error('Complete source population differs');
    for(const key of ['receipt','specification','source_identity','latest_reported','last_calculated','original_sha256','original_bytes'])if(!same(whole[key],summary[key]))throw Error('Source projection differs');
    if(summary.current!==null&&!same(summary.current,whole.current))throw Error('Current source projection differs');return whole;
  }
  async function verifyOriginal(p,run,sid,fetcher=root.fetch,crypto=root.crypto,signal){
    const source=await verifySeries(p,sid,fetcher,crypto,signal),input=await artifact(run.input,'inputs',fetcher,crypto,'json',signal);
    if(input.contract!=='fifx-vol-inputs.v1'||input.generated_at!==p.generated_at||!same(Object.keys(input.sources||{}).sort(),IDS.slice().sort()))throw Error('Original input binding differs');
    const refs=input.sources[sid];if(!refs.original){if(refs.receipt||source.receipt!==null||source.original_bytes!==null)throw Error('Missing original differs');return {bytes:0,rows:0,missing:true};}
    const raw=await artifact(refs.original,'originals',fetcher,crypto,'bin',signal),receipt=await artifact(refs.receipt,'receipts',fetcher,crypto,'json',signal);
    if(!same(receipt,source.receipt)||receipt.sha256!==refs.original.sha256||receipt.bytes!==raw.length||source.original_sha256!==refs.original.sha256||source.original_bytes!==raw.length)throw Error('Original acquisition receipt differs');
    return {bytes:raw.length,rows:source.original_rows.length,http_status:receipt.http_status,sha256:refs.original.sha256,acquired_at:receipt.acquired_at,missing:false};
  }
  function table(headers,rows){return '<table><thead><tr>'+headers.map(v=>'<th scope="col">'+esc(v)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+r.map(v=>'<td>'+v+'</td>').join('')+'</tr>').join('')+'</tbody></table>';}
  function pairs(rows){return '<dl>'+rows.map(([k,v])=>'<dt>'+esc(k)+'</dt><dd>'+esc(readable(v))+'</dd>').join('')+'</dl>';}
  function stat(label,s,unit){return '<div class="fx-stat"><span>'+esc(label)+'</span><strong>'+esc(display(s))+'</strong><span>'+esc(unit)+'</span><details><summary>Calculation precision</summary><p>'+esc(s?.calculated_decimal??'Unavailable')+'</p></details></div>';}
  function chart(rows,original,unit){
    const points=rows.map(r=>({date:r.date,x:Date.parse(r.date+'T00:00:00Z'),y:original?(typeof r.value==='string'&&r.value.trim()!==''&&r.value!=='.'&&Number.isFinite(Number(r.value))?Number(r.value):null):r.estimate?.value}));
    const valid=points.filter(p=>number(p.x)&&number(p.y));if(!valid.length)return '<p>No verified numeric history.</p>';
    const first=valid[0].x,last=valid.at(-1).x,lo=valid.reduce((m,p)=>Math.min(m,p.y),0),hi=valid.reduce((m,p)=>Math.max(m,p.y),0);
    const X=v=>48+630*(v-first)/(last-first||1),Y=v=>176-142*(v-lo)/(hi-lo||1);let path='',pen=false;
    for(const p of points){if(!number(p.x)||!number(p.y)){pen=false;continue;}path+=(pen?' L':' M')+X(p.x).toFixed(2)+' '+Y(p.y).toFixed(2);pen=true;}
    return '<svg class="fx-chart" viewBox="0 0 710 220" role="img" aria-label="Complete retained history"><line x1="48" x2="678" y1="'+Y(0)+'" y2="'+Y(0)+'"/><path d="'+path+'"/><text x="4" y="20">'+esc(unit)+' · '+hi.toFixed(2)+'</text><text x="4" y="192">'+lo.toFixed(2)+'</text><text x="48" y="212">'+esc(valid[0].date)+'</text><text x="678" y="212" text-anchor="end">'+esc(valid.at(-1).date)+'</text></svg>';
  }
  function sourceIdentityRows(row,sid){
    const identity=row.source_identity,meta=identity?.metadata,definition=identity?.definition?.seriess?.[0],population=identity?.population;
    const date=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(s)&&Number.isFinite(Date.parse(s+'T00:00:00Z'))&&new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s;
    const range=(first,last)=>date(first)&&date(last)&&first<=last?first+' → '+last:null;
    const rows=[['Provider',row.specification.provider],['Identity status',identity?.status],['Provider name',meta?.longName??definition?.title??sid],
      ['Original unit',row.specification.source_unit],['Original response',Number.isInteger(row.receipt?.http_status)?'HTTP '+row.receipt.http_status:'Unavailable'],
      ['Exchange / currency',meta?meta.exchangeName+' / '+meta.currency:null],['Session timezone',identity?.timezone?.name??'Source observation date'],
      ['Pinned timezone database',identity?.timezone?.iana_version??null],['Source roots',row.specification.dependency_roots.join(', ')]];
    if(row.specification.provider==='fred_api'){
      const valid=population&&range(population.requested_start,population.requested_end)&&range(population.realtime_start,population.realtime_end)&&
        Number.isSafeInteger(population.returned_rows)&&population.returned_rows>0&&Number.isSafeInteger(population.reported_rows)&&population.reported_rows>0;
      const complete=valid&&row.receipt?.http_status===200&&population.complete_requested_window===true&&population.offset===0&&population.limit===50000&&
        population.returned_rows===population.reported_rows&&population.returned_rows<=population.limit&&population.returned_rows===row.retained_original_rows;
      rows.push(['Requested observations',valid?range(population.requested_start,population.requested_end):null],
        ['Response realtime window',valid?range(population.realtime_start,population.realtime_end):null],
        ['Returned / reported rows',valid?population.returned_rows.toLocaleString('en-US')+' / '+population.reported_rows.toLocaleString('en-US'):null],
        ['Requested-window completeness',complete?'Complete according to retained response counts':'Unverified'],
        ['Full series history','Unverified'],['Historical first-release availability','Unverified']);
    }
    rows.push(['Official quote-feed parity',row.specification.provider==='yahoo_chart'?'Unverified':'Not applicable'],['Forecast / sizing','Unqualified']);
    return rows;
  }
  function mount(doc,fetcher=root.fetch,crypto=root.crypto){
    if(!doc.getElementById('fx-research'))return;
    const el=id=>doc.getElementById('fx-'+id),requests=new Set();
    let packet=null,run=null,version=0,loaded={},offset=0,activity=0,activityRequest=null,timer=null,paused=false,destroyed=false,ageSignature='';
    function request(){const control=new AbortController();requests.add(control);return {signal:control.signal,abort:()=>control.abort(),done:()=>requests.delete(control)};}
    function cancelActivity(){activity++;activityRequest?.abort();activityRequest=null;el('history-status').textContent='';}
    function cancelAll(){for(const control of requests)control.abort();requests.clear();cancelActivity();}
    const signature=()=>JSON.stringify(IDS.map(sid=>fresh(packet,sid)));
    el('series').innerHTML=IDS.map(s=>'<option value="'+esc(s)+'">'+esc(LABELS[s]+' · '+s)+'</option>').join('');el('series').value='DGS10';
    function clear(message){
      cancelAll();packet=null;run=null;loaded={};offset=0;ageSignature='';
      for(const id of ['metadata','inventory','reading','window','identity','history','chart','pagination','legacy']){el(id).innerHTML='';el(id).textContent='';}
      el('newer').disabled=true;el('older').disabled=true;el('native').hidden=true;el('legacy').hidden=true;el('status').textContent=message;
    }
    function history(){
      const sid=el('series').value,whole=loaded[sid],original=el('kind').value==='original';
      el('history').innerHTML='';el('chart').innerHTML='';el('pagination').textContent='';el('newer').disabled=true;el('older').disabled=true;if(!whole)return;
      const rows=original?whole.original_rows:whole.history,part=rows.slice().reverse().slice(offset,offset+50);
      el('history').innerHTML=original?table(['Original row','Observation date','Original numeric text'],part.map(r=>[r.original_row,esc(r.date),esc(r.value??'Missing')])):
        table(['Observation / window','Estimate','Prior-only z-score','Midrank percentile','Missing rows / max gap','Baseline'],part.map(r=>[esc((r.start_date?r.start_date+' → ':'')+r.date),esc(display(r.estimate)),esc(display(r.baseline?.z_score)),esc(display(r.baseline?.midrank_percentile)),esc(r.change_count?(r.missing_rows_inside_window+' / '+r.max_interval_days+' days'):'Reported index level'),esc(r.baseline?.status)]));
      el('pagination').textContent=(rows.length?offset+1:0)+'–'+Math.min(offset+50,rows.length)+' of '+rows.length+' retained rows';el('newer').disabled=offset===0;el('older').disabled=offset+50>=rows.length;
      el('chart').innerHTML=chart(rows,original,readable(original?whole.specification.source_unit:whole.specification.measurement_unit));
    }
    function render(){
      if(!packet)return;const sid=el('series').value,row=packet.series[sid],available=fresh(packet,sid),current=available?row.current:null;
      const query=el('search').value.trim().toLowerCase(),count=IDS.filter(s=>fresh(packet,s)).length;
      ageSignature=signature();
      el('status').textContent='Retained view matched · '+count+' / 18 current descriptive sources · WAIT: no qualified portfolio action';
      el('metadata').textContent='Published '+packet.generated_at+' · '+packet.quality.original_rows.toLocaleString()+' original rows · '+packet.quality.history_rows.toLocaleString()+' historical estimates';
      el('inventory').innerHTML=table(['Source','Current measurement','Unit','Observation','Acquired','Status'],IDS.filter(s=>(s+' '+LABELS[s]).toLowerCase().includes(query)).map(s=>{
        const r=packet.series[s],ok=fresh(packet,s);return [esc(LABELS[s]+' · '+s),esc(display(ok?r.current?.estimate:null)),esc(readable(r.specification.measurement_unit)),esc(r.latest_reported?.date??'Unavailable'),esc(r.receipt?.acquired_at??'Unavailable'),esc(readable(ok?r.quality.status:r.current?'Expired':r.quality.status))];}));
      el('reading').innerHTML='<div class="fx-reading">'+stat(row.specification.window_changes?'Annualized dispersion (252-step assumption)':'Reported index level',current?.estimate,readable(row.specification.measurement_unit))+stat('Descriptive z-score',current?.baseline?.z_score,'504 preceding estimates')+stat('Midrank percentile',current?.baseline?.midrank_percentile,'percentile, not a probability')+'</div>';
      const last=row.last_calculated;
      el('window').innerHTML=pairs([['Current status',available?row.quality.status:row.current?'Expired':row.quality.status],['Latest source observation',row.latest_reported?.date],['Acquired',row.receipt?.acquired_at],['Current expires',row.current_expires_at],['Release cadence',row.specification.release_cadence],['Last calculated window',last?(last.start_date?last.start_date+' → ':'')+last.date:null],['Window steps',last?.change_count??'Reported index level'],['Calendar span / max gap',last?.change_count?last.elapsed_calendar_days+' / '+last.max_interval_days+' days':null],['Missing interior rows',last?.missing_rows_inside_window??null],['Session evidence',row.session_evidence?.status],['Baseline status',last?.baseline?.status]]);
      el('identity').innerHTML=pairs(sourceIdentityRows(row,sid));
    }
    async function refresh(){
      if(paused||destroyed)return;
      const ticket=++version;clear('Verifying publication…');const work=request();
      try{
        const p=io.decode(await bytes('/data/fifx-vol.json?exact=1&nogen=1',fetcher,2*1024*1024,15000,{signal:work.signal}),2*1024*1024);
        if(ticket!==version)return;
        if(p.contract!=='fifx-vol-research.v1'){
          el('legacy').hidden=false;el('legacy').innerHTML='<div class="fx-panel"><h2>Native publication pending</h2><p>The existing packet was published '+esc(p.generated_at??'at an unknown time')+'. It has not passed the complete source-replay contract. No current measurement or migration recommendation is displayed here.</p><p>The producer keeps its weekday 21:20 UTC schedule.</p></div>';el('status').textContent='Historical predecessor · WAIT: abstain';return;
        }
        const proof=await verifyView(p,fetcher,crypto,work.signal);if(ticket!==version)return;packet=p;run=proof;el('native').hidden=false;render();
      }catch(error){if(ticket===version&&!paused&&!destroyed)clear('Verification unavailable · current measurements withheld. Refresh to try again.');}
      finally{work.done();}
    }
    async function inspect(original){
      if(!packet||paused||destroyed)return;cancelActivity();const work=request();activityRequest=work;
      const ticket=version,action=activity,sid=el('series').value,p=packet,bound=run;el('history-status').textContent='Verifying complete retained evidence…';
      try{
        const whole=await verifySeries(p,sid,fetcher,crypto,work.signal);const source=original?await verifyOriginal(p,bound,sid,fetcher,crypto,work.signal):null;
        if(ticket!==version||action!==activity||p!==packet||sid!==el('series').value)return;loaded={[sid]:whole};offset=0;history();
        el('history-status').textContent=(source?(source.missing?'Original response unavailable':source.bytes.toLocaleString()+' original response bytes and acquisition receipt verified · SHA-256 '+source.sha256+' · acquired '+source.acquired_at):'Complete history hash verified')+' · '+whole.original_rows.length.toLocaleString()+' original rows · '+whole.history.length.toLocaleString()+' historical estimates';
      }catch(error){if(ticket===version&&action===activity&&p===packet&&sid===el('series').value)clear('Source verification failed · current measurements withheld. Refresh to try again.');}
      finally{work.done();if(activityRequest===work)activityRequest=null;}
    }
    el('refresh').addEventListener('click',refresh);el('series').addEventListener('change',()=>{cancelActivity();offset=0;render();history();});
    el('search').addEventListener('input',render);el('load').addEventListener('click',()=>inspect(false));el('original').addEventListener('click',()=>inspect(true));
    el('kind').addEventListener('change',()=>{cancelActivity();offset=0;history();});el('older').addEventListener('click',()=>{offset+=50;history();});el('newer').addEventListener('click',()=>{offset=Math.max(0,offset-50);history();});
    function startClock(){if(timer===null&&!paused&&!destroyed)timer=root.setInterval(()=>{if(packet&&signature()!==ageSignature)render();},30000);}
    function stopClock(){if(timer!==null)root.clearInterval(timer);timer=null;}
    function suspend(){if(destroyed)return;paused=true;++version;stopClock();clear('Page paused. Publication will be reverified on return.');}
    function resume(){if(destroyed||!paused)return;paused=false;startClock();return refresh();}
    function destroy(){if(destroyed)return;suspend();destroyed=true;root.removeEventListener?.('pagehide',suspend);root.removeEventListener?.('pageshow',resume);}
    root.addEventListener?.('pagehide',suspend);root.addEventListener?.('pageshow',resume);
    startClock();refresh();return {refresh,render,suspend,resume,destroy};
  }
  const api={IDS,LABELS,bytes,hash,qualified,fresh,verifyView,verifySeries,verifyOriginal,display,chart,sourceIdentityRows,mount};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;root.JHFIFXResearch=api;
  if(root.document){if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',()=>mount(root.document));else mount(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this);
