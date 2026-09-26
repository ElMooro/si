/* jh-reskin-skip */
(function(root){
  'use strict';
  const PREFIX='data/fifx-vol-research/',LIMIT=64*1024*1024;
  const LABELS={VIXCLS:'VIX reported index',DGS10:'10-year Treasury yield',DEXUSEU:'Euro / US dollar',DEXJPUS:'US dollar / Japanese yen',DEXUSUK:'Sterling / US dollar',DTWEXBGS:'Broad US dollar index','^MOVE':'MOVE reported index','^KS11':'KOSPI','^HSI':'Hang Seng','^N225':'Nikkei 225','^GDAXI':'Provider DAX index','^FTSE':'FTSE 100','^FCHI':'CAC 40','000001.SS':'Shanghai Composite','^BSESN':'Sensex','^BVSP':'Ibovespa','^AXJO':'ASX 200','^VHSI':'HSI Volatility Index'};
  const IDS=Object.keys(LABELS),FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified','publication_eligible'];
  const readable=value=>value==null?'Unavailable':({basis_points:'Basis points',index_points:'Index points',percent_log_return:'Log-return %',within_age_ceiling:'Within freshness limits',identity_mismatch:'Source identity mismatch',http_error:'Provider returned an error',fred_csv:'FRED',yahoo_chart:'Yahoo Finance',not_applicable:'Not applicable',reviewed_source_identity:'Source identity reviewed'}[value]||String(value).replace(/_/g,' '));
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
  async function bytes(url,fetcher,limit=LIMIT,timeout=45000){
    const abort=new AbortController();let timer,stream,expired=false;
    try{return await Promise.race([(async()=>{
      const r=await fetcher(url,{cache:'no-store',credentials:'omit',signal:abort.signal});if(!r.ok)throw Error('Artifact unavailable');
      if(+r.headers.get('Content-Length')>limit)throw Error('Artifact exceeds bound');stream=r.body.getReader();const chunks=[];let size=0;
      while(!expired){const {done,value}=await stream.read();if(done)break;size+=value.length;if(size>limit)throw Error('Artifact exceeds bound');chunks.push(value);}
      if(expired)throw Error('Artifact timed out');const out=new Uint8Array(size);let offset=0;for(const part of chunks){out.set(part,offset);offset+=part.length;}return out;
    })(),new Promise((_,reject)=>{timer=setTimeout(()=>{expired=true;abort.abort();if(stream)Promise.resolve(stream.cancel()).catch(()=>{});reject(Error('Artifact timed out'));},timeout);})]);}
    finally{clearTimeout(timer);if(stream)Promise.resolve(stream.cancel()).catch(()=>{});}
  }
  async function artifact(ref,kind,fetcher,crypto,extension='json'){
    if(!identity(ref,kind,extension))throw Error('Invalid original coordinates');
    const raw=await bytes('/'+ref.key+'?exact=1&nogen=1',fetcher);
    if(raw.length!==ref.bytes||await hash(raw,crypto)!==ref.sha256)throw Error('Artifact bytes differ');
    return extension==='bin'?raw:JSON.parse(new TextDecoder().decode(raw));
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
  async function verifyView(p,fetcher=root.fetch,crypto=root.crypto){
    if(!qualified(p))throw Error('Native research contract unavailable');const key=p.replay?.manifest_key;
    if(typeof key!=='string'||!/^data\/fifx-vol-research\/runs\/[a-f0-9]{64}\.json$/.test(key))throw Error('Native run missing');
    const raw=await bytes('/'+key+'?exact=1&nogen=1',fetcher),digest=await hash(raw,crypto);
    if(key!==PREFIX+'runs/'+digest+'.json')throw Error('Native run hash differs');const run=JSON.parse(new TextDecoder().decode(raw));
    if(run.contract!=='fifx-vol-replay.v1'||run.generated_at!==p.generated_at||run.view?.sha256!==p.replay.view_sha256||!same(Object.keys(run.series||{}).sort(),IDS.slice().sort()))throw Error('Native run binding differs');
    const view=await artifact(run.view,'views',fetcher,crypto);
    if(!same(view,Object.fromEntries(Object.entries(p).filter(([k])=>k!=='replay')))||IDS.some(s=>!same(p.series[s].complete_source_artifact,run.series[s])))throw Error('Published view differs');
    return run;
  }
  async function verifySeries(p,sid,fetcher=root.fetch,crypto=root.crypto){
    if(!qualified(p)||!IDS.includes(sid))throw Error('Source unavailable');const summary=p.series[sid];
    const whole=await artifact(summary.complete_source_artifact,'series',fetcher,crypto);
    if(whole.contract!=='fifx-source-candidate.v1'||!research(whole)||whole.source_id!==sid||whole.generated_at!==p.generated_at||
      whole.original_rows?.length!==summary.retained_original_rows||whole.history?.length!==summary.retained_history_rows)throw Error('Complete source population differs');
    for(const key of ['receipt','specification','source_identity','latest_reported','last_calculated','original_sha256','original_bytes'])if(!same(whole[key],summary[key]))throw Error('Source projection differs');
    if(summary.current!==null&&!same(summary.current,whole.current))throw Error('Current source projection differs');return whole;
  }
  async function verifyOriginal(p,run,sid,fetcher=root.fetch,crypto=root.crypto){
    const source=await verifySeries(p,sid,fetcher,crypto),input=await artifact(run.input,'inputs',fetcher,crypto);
    if(input.contract!=='fifx-vol-inputs.v1'||input.generated_at!==p.generated_at||!same(Object.keys(input.sources||{}).sort(),IDS.slice().sort()))throw Error('Original input binding differs');
    const refs=input.sources[sid];if(!refs.original){if(refs.receipt||source.receipt!==null||source.original_bytes!==null)throw Error('Missing original differs');return {bytes:0,rows:0,missing:true};}
    const raw=await artifact(refs.original,'originals',fetcher,crypto,'bin'),receipt=await artifact(refs.receipt,'receipts',fetcher,crypto);
    if(!same(receipt,source.receipt)||receipt.sha256!==refs.original.sha256||receipt.bytes!==raw.length||source.original_sha256!==refs.original.sha256||source.original_bytes!==raw.length)throw Error('Original acquisition receipt differs');
    return {bytes:raw.length,rows:source.original_rows.length,http_status:receipt.http_status,missing:false};
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
  function mount(doc,fetcher=root.fetch,crypto=root.crypto){
    if(!doc.getElementById('fx-research'))return;
    const el=id=>doc.getElementById('fx-'+id);let packet=null,run=null,version=0,loaded={},offset=0;
    el('series').innerHTML=IDS.map(s=>'<option value="'+esc(s)+'">'+esc(LABELS[s]+' · '+s)+'</option>').join('');el('series').value='DGS10';
    function clear(message){packet=null;run=null;loaded={};offset=0;el('native').hidden=true;el('legacy').hidden=true;el('status').textContent=message;el('history-status').textContent='';el('history').innerHTML='';el('chart').innerHTML='';}
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
      el('status').textContent='Verified retained publication · '+count+' / 18 current descriptive sources · WAIT: no qualified portfolio action';
      el('metadata').textContent='Published '+packet.generated_at+' · '+packet.quality.original_rows.toLocaleString()+' original rows · '+packet.quality.history_rows.toLocaleString()+' historical estimates';
      el('inventory').innerHTML=table(['Source','Current measurement','Unit','Observation','Acquired','Status'],IDS.filter(s=>(s+' '+LABELS[s]).toLowerCase().includes(query)).map(s=>{
        const r=packet.series[s],ok=fresh(packet,s);return [esc(LABELS[s]+' · '+s),esc(display(ok?r.current?.estimate:null)),esc(readable(r.specification.measurement_unit)),esc(r.latest_reported?.date??'Unavailable'),esc(r.receipt?.acquired_at??'Unavailable'),esc(readable(ok?r.quality.status:r.current?'Expired':r.quality.status))];}));
      el('reading').innerHTML='<div class="fx-reading">'+stat(row.specification.window_changes?'Annualized dispersion (252-step assumption)':'Reported index level',current?.estimate,readable(row.specification.measurement_unit))+stat('Descriptive z-score',current?.baseline?.z_score,'504 preceding estimates')+stat('Midrank percentile',current?.baseline?.midrank_percentile,'percentile, not a probability')+'</div>';
      const last=row.last_calculated;
      el('window').innerHTML=pairs([['Current status',available?row.quality.status:row.current?'Expired':row.quality.status],['Latest source observation',row.latest_reported?.date],['Acquired',row.receipt?.acquired_at],['Current expires',row.current_expires_at],['Release cadence',row.specification.release_cadence],['Last calculated window',last?(last.start_date?last.start_date+' → ':'')+last.date:null],['Window steps',last?.change_count??'Reported index level'],['Calendar span / max gap',last?.change_count?last.elapsed_calendar_days+' / '+last.max_interval_days+' days':null],['Missing interior rows',last?.missing_rows_inside_window??null],['Session evidence',row.session_evidence?.status],['Baseline status',last?.baseline?.status]]);
      const meta=row.source_identity?.metadata,definition=row.source_identity?.definition?.seriess?.[0];
      el('identity').innerHTML=pairs([['Provider',row.specification.provider],['Identity status',row.source_identity?.status],['Provider name',meta?.longName??definition?.title??sid],['Original unit',row.specification.source_unit],['Exchange / currency',meta?meta.exchangeName+' / '+meta.currency:null],['Session timezone',row.source_identity?.timezone?.name??'Source observation date'],['Pinned timezone database',row.source_identity?.timezone?.iana_version??null],['Source roots',row.specification.dependency_roots.join(', ')],['Official quote-feed parity',row.specification.provider==='yahoo_chart'?'Unverified':'Official FRED response'],['Forecast / sizing','Unqualified']]);
    }
    async function refresh(){
      const ticket=++version;clear('Verifying publication…');
      try{
        const p=JSON.parse(new TextDecoder().decode(await bytes('/data/fifx-vol.json?exact=1&nogen=1',fetcher,2*1024*1024,15000)));
        if(ticket!==version)return;
        if(p.contract!=='fifx-vol-research.v1'){
          el('legacy').hidden=false;el('legacy').innerHTML='<div class="fx-panel"><h2>Native publication pending</h2><p>The existing packet was published '+esc(p.generated_at??'at an unknown time')+'. It has not passed the complete source-replay contract. No current measurement or migration recommendation is displayed here.</p><p>The producer keeps its weekday 21:20 UTC schedule.</p></div>';el('status').textContent='Historical predecessor · WAIT: abstain';return;
        }
        const proof=await verifyView(p,fetcher,crypto);if(ticket!==version)return;packet=p;run=proof;el('native').hidden=false;render();
      }catch(error){if(ticket===version)clear('Verification unavailable · current measurements withheld. Refresh to try again.');}
    }
    async function inspect(original){
      if(!packet)return;const ticket=version,sid=el('series').value,p=packet,bound=run;el('history-status').textContent='Verifying complete retained evidence…';
      try{
        const whole=await verifySeries(p,sid,fetcher,crypto);const source=original?await verifyOriginal(p,bound,sid,fetcher,crypto):null;
        if(ticket!==version||sid!==el('series').value)return;loaded={[sid]:whole};offset=0;history();
        el('history-status').textContent=(source?(source.missing?'Original response unavailable':source.bytes.toLocaleString()+' original response bytes and acquisition receipt verified'):'Complete history hash verified')+' · '+whole.original_rows.length.toLocaleString()+' original rows · '+whole.history.length.toLocaleString()+' historical estimates';
      }catch(error){if(ticket===version&&sid===el('series').value)clear('Source verification failed · current measurements withheld. Refresh to try again.');}
    }
    el('refresh').addEventListener('click',refresh);el('series').addEventListener('change',()=>{offset=0;el('history-status').textContent='';render();history();});
    el('search').addEventListener('input',render);el('load').addEventListener('click',()=>inspect(false));el('original').addEventListener('click',()=>inspect(true));
    el('kind').addEventListener('change',()=>{offset=0;history();});el('older').addEventListener('click',()=>{offset+=50;history();});el('newer').addEventListener('click',()=>{offset=Math.max(0,offset-50);history();});
    const timer=setInterval(render,30000);refresh();return {refresh,render,destroy(){clearInterval(timer);version++;clear('Closed');}};
  }
  const api={IDS,LABELS,bytes,hash,qualified,fresh,verifyView,verifySeries,verifyOriginal,display,chart,mount};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;root.JHFIFXResearch=api;
  if(root.document){if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',()=>mount(root.document));else mount(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this);
