/* jh-reskin-skip */
(function(root){
  'use strict';
  const PREFIX='data/bond-vol-research/',LIMIT=64*1024*1024,DAY=86400000;
  const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified'];
  const day=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(s)&&Number.isFinite(Date.parse(s+'T00:00:00Z'))&&new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s?Date.parse(s+'T00:00:00Z'):NaN;
  const clock=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(s)&&Number.isFinite(day(s.slice(0,10)))&&Number.isFinite(Date.parse(s))?Date.parse(s):NaN;
  const recent=(s,now)=>Number.isFinite(clock(s))&&clock(s)<=now&&now-clock(s)<=26*3600000;
  const number=v=>typeof v==='number'&&Number.isFinite(v);
  const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const exact=s=>typeof s==='string'&&/^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(s)&&Number.isFinite(Number(s));
  const same=(a,b)=>JSON.stringify(sort(a))===JSON.stringify(sort(b));
  function sort(v){return Array.isArray(v)?v.map(sort):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,sort(v[k])])):v;}
  async function bytes(url,fetcher,limit=LIMIT,timeoutMs=15000){
    const controller=new AbortController();let timer,stream,expired=false;
    try{return await Promise.race([(async()=>{
      const r=await fetcher(url,{cache:'no-store',credentials:'omit',signal:controller.signal});if(!r.ok)throw Error('Artifact unavailable');
      if(+r.headers.get('Content-Length')>limit)throw Error('Artifact exceeds bound');
      stream=r.body.getReader();const chunks=[];let size=0;
      while(!expired){const {done,value}=await stream.read();if(done)break;size+=value.length;if(size>limit)throw Error('Artifact exceeds bound');chunks.push(value);}
      if(expired)throw Error('Artifact timed out');const out=new Uint8Array(size);let offset=0;for(const c of chunks){out.set(c,offset);offset+=c.length;}return out;
    })(),new Promise((_,reject)=>{timer=setTimeout(()=>{expired=true;controller.abort();if(stream)Promise.resolve(stream.cancel()).catch(()=>{});reject(Error('Artifact timed out'));},timeoutMs);})]);}
    finally{clearTimeout(timer);if(stream)Promise.resolve(stream.cancel()).catch(()=>{});}
  }

  async function hash(raw,crypto){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');}
  function identity(ref,category,ext='json'){return ref&&/^[a-f0-9]{64}$/.test(ref.sha256)&&ref.key===PREFIX+category+'/'+ref.sha256+'.'+ext&&Number.isInteger(ref.bytes)&&ref.bytes>0&&ref.bytes<=LIMIT;}
  const LABELS={DGS10:'10-year Treasury',DGS2:'2-year Treasury',DGS30:'30-year Treasury',DGS5:'5-year Treasury',T10Y2Y:'10y − 2y slope',T10Y3M:'10y − 3m slope',BAMLH0A0HYM2:'High-yield OAS',BAMLC0A4CBBB:'BBB corporate OAS',DFII10:'10-year real yield',T10YIE:'10-year breakeven'};
  const IDS=Object.keys(LABELS),ROOTS={DGS10:['H15:nominal'],DGS2:['H15:nominal'],DGS30:['H15:nominal'],DGS5:['H15:nominal'],T10Y2Y:['H15:nominal'],T10Y3M:['H15:nominal'],BAMLH0A0HYM2:['ICE:HY-OAS'],BAMLC0A4CBBB:['ICE:BBB-OAS'],DFII10:['H15:real'],T10YIE:['H15:nominal','H15:real']};
  function scalar(v){return number(v?.value)&&exact(v.calculated_decimal)&&Number(v.calculated_decimal)===v.value;}
  function qualified(p){return p?.contract==='bond-vol-research.v1'&&FLAGS.every(k=>p[k]===false)&&p.publication_eligible===false&&p.call===null&&p.regime===null&&p.composite_z_score===null&&same(p.signals,[])&&same(p.decision,{verb:'WAIT',meaning:'abstain'})&&p.portfolio_consequences?.status==='UNAVAILABLE'&&p.portfolio_consequences.target_weights===null&&
    p.view?.contract==='bond-vol-summary.v1'&&p.view.complete_series_artifacts===10&&p.dependency_graph?.independent_votes===0&&p.dependency_graph.statistical_independence_qualified===false&&same(p.dependency_graph.series_roots,ROOTS)&&same(Object.keys(p.series||{}).sort(),IDS.slice().sort())&&
    IDS.every(s=>identity(p.series[s].complete_history_artifact,'series'))&&identity(p.move?.complete_quote_artifact,'quotes')&&/^data\/bond-vol-research\/runs\/[a-f0-9]{64}\.json$/.test(p.replay?.manifest_key)&&/^[a-f0-9]{64}$/.test(p.replay?.output_sha256);}
  function view(p,now=Date.now()){
    const valid=qualified(p),current=valid&&recent(p.generated_at,now)&&recent(p.source_generated_at,now)&&clock(p.source_generated_at)<=clock(p.generated_at);
    const rows=IDS.map(sid=>{
      const row=p?.series?.[sid]||{},point=row.current,d=row.latest_observation?.date,age=Math.floor(now/DAY)-Math.floor(day(d)/DAY),def=row.source_definition;
      const eligible=current&&row.series_id===sid&&row.definition_reviewed===true&&same(row.dependency_roots,ROOTS[sid])&&FLAGS.every(k=>row[k]===false)&&row.quality?.status==='within_age_ceiling'&&row.quality.max_observation_age_days===7&&row.quality.max_acquisition_age_seconds===26*3600&&
        def?.id===sid&&same([def.units,def.frequency_short,def.frequency,def.seasonal_adjustment],['Percent','D',sid.startsWith('BAML')?'Daily, Close':'Daily','Not Seasonally Adjusted'])&&
        recent(row.acquired_at,now)&&clock(row.acquired_at)<=clock(p.source_generated_at)&&Number.isFinite(age)&&age>=0&&age<=7&&day(d)<=Math.floor(clock(row.acquired_at)/DAY)*DAY&&exact(row.latest_observation?.value)&&
        point?.end_date===d&&Number.isFinite(day(point.start_date))&&day(point.start_date)<day(d)&&point.change_count===30&&point.numeric_observations===31&&point.comparable_interval_ceiling_met===true&&point.max_interval_days<=7&&point.max_interval_days>0&&scalar(point.step_dispersion_bp)&&scalar(point.annualized_assuming_252_steps_bp);
      return {sid,label:LABELS[sid],row,eligible,date:d||'Unavailable',value:eligible?point.step_dispersion_bp:null,status:eligible?'Within age ceiling':row.quality?.status==='within_age_ceiling'?'Expired or invalid':row.quality?.status||'Unavailable'};
    });
    const m=p?.move||{},date=m.current?.session_date,quoteAge=Math.floor(now/DAY)-Math.floor(day(date)/DAY);
    const quote=current&&FLAGS.every(k=>m[k]===false)&&m.status==='quoted_previous_session'&&m.identity_reviewed===true&&m.is_proxy===false&&m.official_feed_parity_verified===false&&m.unit==='index_points'&&recent(m.receipt?.acquired_at,now)&&clock(m.receipt.acquired_at)<=clock(p.generated_at)&&number(m.current?.reported_close)&&m.current.reported_close>=0&&Number.isFinite(quoteAge)&&quoteAge>=0&&quoteAge<=7;
    return {valid,current,rows,available:rows.filter(r=>r.eligible).length,quote:quote?m.current:null};
  }
  async function artifact(ref,category,fetcher=globalThis.fetch,crypto=globalThis.crypto,ext='json'){
    if(!identity(ref,category,ext))throw Error('Artifact coordinates invalid');
    const raw=await bytes('/'+ref.key+'?exact=1&nogen=1',fetcher,LIMIT,30000);
    if(raw.length!==ref.bytes||await hash(raw,crypto)!==ref.sha256)throw Error('Artifact bytes differ');
    return ext==='json'?JSON.parse(new TextDecoder().decode(raw)):raw;
  }
  async function verifyView(p,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!qualified(p))throw Error('Native contract unavailable');
    const key=p.replay.manifest_key,raw=await bytes('/'+key+'?exact=1&nogen=1',fetcher);
    if(key!==PREFIX+'runs/'+await hash(raw,crypto)+'.json')throw Error('Run identity differs');
    const manifest=JSON.parse(new TextDecoder().decode(raw)),copy={...p};delete copy.replay;
    if(manifest.contract!=='bond-vol-replay.v1'||manifest.output_sha256!==p.replay.output_sha256||manifest.generated_at!==p.generated_at)throw Error('Run binding differs');
    if(!same(await artifact(manifest.view,'views',fetcher,crypto),copy))throw Error('Published view differs');
    if(!same(Object.keys(manifest.series||{}).sort(),IDS.slice().sort())||!IDS.every(s=>same(p.series[s].complete_history_artifact,manifest.series[s]))||!same(p.move.complete_quote_artifact,manifest.quote))throw Error('Complete history binding differs');
    return manifest;
  }
  async function verifySeries(p,sid,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!qualified(p)||!IDS.includes(sid))throw Error('Series unavailable');
    const s=p.series[sid],whole=await artifact(s.complete_history_artifact,'series',fetcher,crypto),copy={...s};
    delete copy.complete_history_artifact;delete copy.retained_original_rows;delete copy.retained_rolling_windows;
    if(!same(copy,Object.fromEntries(Object.entries(whole).filter(([k])=>!['original_rows','rolling_history'].includes(k))))||whole.original_rows?.length!==s.retained_original_rows||whole.rolling_history?.length!==s.retained_rolling_windows)throw Error('Complete series projection differs');
    return whole;
  }
  async function verifyOriginal(row,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!IDS.includes(row.series_id)||!same(Object.keys(row.evidence||{}).sort(),['definition','observations']))throw Error('Original references unavailable');
    const docs={};for(const kind of ['definition','observations']){
      const ref=row.evidence[kind];
      if(ref?.captured!==true||ref.provider!=='fred'||!/^data\/evidence\/fred\/[a-f0-9]{64}\/[a-f0-9]{64}\.bin\.gz$/.test(ref.key)||!ref.key.endsWith('/'+ref.sha256+'.bin.gz')||!Number.isInteger(ref.bytes)||ref.bytes<=0||ref.bytes>LIMIT)throw Error('Original coordinates invalid');
      let raw=await bytes('/'+ref.key+'?exact=1&nogen=1',fetcher);
      if(raw[0]===31&&raw[1]===139)raw=await bytes('expanded',async()=>new Response(new Blob([raw]).stream().pipeThrough(new DecompressionStream('gzip'))));
      if(raw.length!==ref.bytes||await hash(raw,crypto)!==ref.sha256)throw Error('Original bytes differ');docs[kind]=JSON.parse(new TextDecoder().decode(raw));
    }
    if(docs.definition.seriess?.length!==1||!same(docs.definition.seriess[0],row.source_definition)||!same(docs.observations.observations,row.original_rows))throw Error('Original definition or observations differ');
    return row.original_rows.length;
  }
  async function verifyQuote(p,manifest,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    const q=await artifact(manifest.quote,'quotes',fetcher,crypto),copy={...p.move};delete copy.complete_quote_artifact;delete copy.retained_quote_rows;
    if(!same(copy,Object.fromEntries(Object.entries(q).filter(([k])=>!['original','history'].includes(k))))||q.history?.length!==p.move.retained_quote_rows)throw Error('Complete quote projection differs');
    const input=await artifact(manifest.input,'inputs',fetcher,crypto);
    if(input.contract!=='bond-vol-inputs.v1'||input.generated_at!==p.generated_at)throw Error('Quote input binding differs');
    if(!input.quote){if(input.quote_receipt||q.original!==null||q.receipt!==null||q.history.length)throw Error('Missing quote differs');return q;}
    const raw=await artifact(input.quote,'originals',fetcher,crypto,'bin'),receipt=await artifact(input.quote_receipt,'receipts',fetcher,crypto);
    if(!same(receipt,q.receipt)||receipt.sha256!==input.quote.sha256||receipt.bytes!==raw.length)throw Error('Quote acquisition receipt differs');
    let original=null;try{original=JSON.parse(new TextDecoder().decode(raw));}catch(_){}
    if(!same(original,q.original))throw Error('Quote original differs');return q;
  }
  function controller(verify){let sequence=0,packet=null,evidence=null;return {get:()=>packet,proof:()=>evidence,clear:()=>{sequence++;packet=null;evidence=null;},async accept(value){const ticket=++sequence;packet=null;evidence=null;const proof=await verify(value);if(ticket!==sequence)return false;packet=value;evidence=proof;return true;}};}
  function display(v){if(!scalar(v))return 'Unavailable';const n=v.value,rounded=n.toFixed(4).replace(/0+$/,'').replace(/\.$/,'');return rounded+(Number(rounded)!==n?' (rounded)':'');}
  function table(headers,rows){return '<table><thead><tr>'+headers.map(h=>'<th scope="col">'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+r.map(c=>'<td>'+c+'</td>').join('')+'</tr>').join('')+'</tbody></table>';}
  function chart(points,label,unit){
    const valid=points.filter(p=>number(p.x)&&number(p.y));if(!valid.length)return '<p>No verified numeric history.</p>';
    const xs=valid.map(p=>p.x),ys=valid.map(p=>p.y),a=Math.min(...xs),b=Math.max(...xs),lo=Math.min(0,...ys),hi=Math.max(0,...ys),x=v=>48+630*(v-a)/(b-a||1),y=v=>176-142*(v-lo)/(hi-lo||1);
    let d='',pen=false;for(const p of points){if(!number(p.x)||!number(p.y)){pen=false;continue;}d+=(pen?' L':' M')+x(p.x).toFixed(2)+' '+y(p.y).toFixed(2);pen=true;}
    return '<svg class="bv-chart" viewBox="0 0 710 220" role="img" aria-label="'+esc(label)+'"><line x1="48" x2="678" y1="'+y(0)+'" y2="'+y(0)+'"/><path d="'+d+'"/><text x="4" y="20">'+esc(unit)+' · '+hi.toFixed(2)+'</text><text x="4" y="192">'+lo.toFixed(2)+'</text><text x="48" y="212">'+esc(valid[0].label)+'</text><text x="678" y="212" text-anchor="end">'+esc(valid.at(-1).label)+'</text></svg>';
  }
  function stat(label,s,unit){return '<div class="bv-stat"><span>'+esc(label)+'</span><strong>'+esc(display(s))+'</strong><span class="bv-unit">'+esc(unit)+'</span><details><summary>Calculation precision</summary><p class="bv-exact">'+esc(s?.calculated_decimal??'Unavailable')+'</p></details></div>';}
  function mount(doc,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!doc.getElementById('bv-research'))return;
    const el=id=>doc.getElementById('bv-'+id),gate=controller(p=>verifyView(p,fetcher,crypto));
    let packet=null,manifest=null,loaded={},offset=0,version=0;
    el('series').innerHTML=IDS.map(s=>'<option value="'+s+'">'+s+' · '+LABELS[s]+'</option>').join('');el('series').value='DGS10';
    function clear(message){packet=null;manifest=null;loaded={};offset=0;gate.clear();el('native').hidden=true;el('legacy').hidden=true;el('status').textContent=message;el('history-status').textContent='';el('quote-status').textContent='';el('quote-evidence').innerHTML='';}
    function history(){
      const sid=el('series').value,whole=loaded[sid],mode=el('history-kind').value;
      el('history').innerHTML='';el('history-chart').innerHTML='';el('pagination').textContent='';el('older').disabled=true;el('newer').disabled=true;
      if(!whole)return;
      const original=mode==='original',all=original?whole.original_rows.map((r,i)=>({...r,original_row:i})):whole.rolling_history;
      const rows=all.slice().sort((a,b)=>(b.date||b.end_date).localeCompare(a.date||a.end_date)),part=rows.slice(offset,offset+50);
      el('history').innerHTML=original?table(['Original row','Observation','Source value (%)','Realtime interval'],part.map(r=>[r.original_row,esc(r.date),esc(r.value),esc(r.realtime_start+' → '+r.realtime_end)])):
        table(['Window dates','Dispersion (bp / step)','Annualized (bp)','Calendar days','Missing rows / max gap'],part.map(r=>[esc(r.start_date+' → '+r.end_date),esc(display(r.step_dispersion_bp)),esc(display(r.annualized_assuming_252_steps_bp)),r.elapsed_calendar_days,esc(r.missing_rows_inside_window+' / '+r.max_interval_days+'d')]));
      el('pagination').textContent=(rows.length?offset+1:0)+'–'+Math.min(offset+50,rows.length)+' of '+rows.length+' retained '+(original?'original rows':'rolling windows');el('older').disabled=offset+50>=rows.length;el('newer').disabled=offset===0;
      el('history-chart').innerHTML=chart(rows.slice().reverse().map(r=>({x:day(r.date||r.end_date),label:r.date||r.end_date,y:original?(exact(r.value)?Number(r.value):null):r.step_dispersion_bp.value})),sid+' full '+mode+' history',original?'percent':'bp / step');
    }
    function render(){
      if(!packet)return;const state=view(packet),selected=state.rows.find(r=>r.sid===el('series').value),r=selected.row,c=selected.eligible?r.current:null,d=selected.eligible?r.current_distribution:null,m=packet.move;
      el('native').hidden=false;el('status').textContent='Immutable publication verified · '+state.available+'/10 current rate measurements within reviewed age ceilings · no qualified portfolio vote';
      el('metadata').innerHTML='<span>Published: '+esc(packet.generated_at)+'</span><span>Canonical source: '+esc(packet.source_generated_at)+'</span><span>Original rows: '+packet.view.complete_original_rows.toLocaleString()+' · windows: '+packet.view.complete_rolling_windows.toLocaleString()+'</span>';
      el('reading').innerHTML='<div class="bv-hero"><div><p>'+esc(selected.sid+' · '+selected.label)+'</p><strong>'+esc(c?display(c.step_dispersion_bp):'Unavailable')+'</strong><p>Basis points per observation step · '+esc(selected.status)+'</p></div><div><span class="bv-badge">Realized changes</span><p>Observed: '+esc(selected.date)+'</p><p>Acquired: '+esc(r.acquired_at||'Unavailable')+'</p><p>Roots: '+esc(r.dependency_roots.join(' + '))+'</p></div></div>'+
        '<div class="bv-stat-grid">'+stat('Annualized dispersion',c?.annualized_assuming_252_steps_bp,'bp · assumes 252 comparable steps')+stat('Historical z-score',d?.z_score,'descriptive · overlapping baseline windows')+stat('Midrank percentile',d?.midrank_percentile,'percent · current window excluded from baseline')+'</div>';
      el('window').innerHTML=table(['Measurement contract','Current window'],[
        ['Window dates',esc(c?c.start_date+' → '+c.end_date:'Unavailable')],['Numeric observations / changes',c?'31 / 30':'Unavailable'],['Calendar days / largest interval',c?c.elapsed_calendar_days+' / '+c.max_interval_days+' days':'Unavailable'],['Missing original rows inside window',c?String(c.missing_rows_inside_window):'Unavailable'],['Prior baseline window endpoints',esc(d?d.first_prior_end_date+' → '+d.last_prior_end_date:'Unavailable')],['Prior windows / availability',esc(d?d.prior_window_count+' / '+d.status:'Unavailable')],['Provider definition',esc(r.source_definition?.title||'Unavailable')],['Provider metadata updated',esc(r.provider_updated_at||'Unavailable')]]);
      el('quote').innerHTML='<h2>MOVE · separate option-implied index</h2><p class="bv-note">'+esc(state.quote?'Quoted previous session · official-feed parity unverified':m.status.replaceAll('_',' ')+' · current index level unavailable')+'</p>'+
        (state.quote?'<strong>'+esc(state.quote.reported_close)+' index points</strong><p>Session '+esc(state.quote.session_date)+'</p>':'')+'<p>Provider instrument name: '+esc(m.provider_name||'Unavailable')+'</p><p class="bv-muted">Acquired '+esc(m.receipt?.acquired_at||'Unavailable')+' · '+m.retained_quote_rows+' retained quote rows. Rate-change dispersion is never substituted for MOVE.</p>';
      const query=el('search').value.trim().toLowerCase();el('inventory').innerHTML=table(['Series / definition','Dispersion (bp / step)','Observation','Acquired','Dependency roots','Availability'],state.rows.filter(v=>(v.sid+' '+v.label).toLowerCase().includes(query)).map(v=>[
        '<button type="button" data-series="'+v.sid+'">'+v.sid+'</button><br>'+esc(v.label),esc(display(v.value)),esc(v.date),esc(v.row.acquired_at||'Unavailable'),esc(v.row.dependency_roots.join(' + ')),esc(v.status)]));
      for(const button of el('inventory').querySelectorAll('button[data-series]'))button.onclick=()=>{el('series').value=button.dataset.series;offset=0;el('history-status').textContent='';render();};history();
    }
    async function refresh(){
      const ticket=++version;clear('Verifying publication…');
      try{const p=JSON.parse(new TextDecoder().decode(await bytes('/data/bond-vol.json?exact=1&nogen=1',fetcher)));if(ticket!==version)return;
        if(p.contract!=='bond-vol-research.v1'){
          if(!Number.isFinite(clock(p.generated_at)))throw Error('No dated predecessor');el('legacy').hidden=false;el('status').textContent='Legacy snapshot · native original-source publication pending';
          el('legacy').innerHTML='<h2>Previous publication preserved</h2><p>Published '+esc(p.generated_at)+'. Its composite and playbook are unqualified. Current source-verified measurements will appear after the normal scheduled engine run.</p><p><a href="/data/bond-vol.json?exact=1&nogen=1">Inspect complete predecessor packet</a> · <a href="/data/bond-vol-history.json?exact=1&nogen=1">Inspect retained legacy history</a></p>';return;
        }
        if(!await gate.accept(p)||ticket!==version)return;packet=p;manifest=gate.proof();render();
      }catch(_){if(ticket===version)clear('Publication unavailable or verification failed. Current readings are withheld.');}
    }
    el('refresh').onclick=refresh;el('series').onchange=()=>{offset=0;el('history-status').textContent='';render();};el('search').oninput=render;el('history-kind').onchange=()=>{offset=0;history();};
    async function load(original){
      if(!packet)return;const p=packet,sid=el('series').value,ticket=version;el('history-status').textContent='Verifying complete retained series'+(original?' and provider originals':'')+'…';
      try{const row=await verifySeries(p,sid,fetcher,crypto);if(original)await verifyOriginal(row,fetcher,crypto);if(ticket!==version||p!==packet)return;loaded[sid]=row;
        if(el('series').value===sid){offset=0;history();el('history-status').textContent=(original?'Original definition and all provider observations verified':'Complete retained series hash verified')+' · '+row.original_rows.length+' original rows · '+row.rolling_history.length+' windows';}
      }catch(_){if(ticket===version){delete loaded[sid];if(original)clear('Original verification failed. Current readings are withheld.');else{history();el('history-status').textContent='History verification failed; requested history withheld.';}}}
    }
    el('load-history').onclick=()=>load(false);el('verify-source').onclick=()=>load(true);
    el('verify-quote').onclick=async()=>{
      if(!packet)return;const p=packet,ticket=version;el('quote-status').textContent='Verifying whole quote response and receipt…';
      try{const q=await verifyQuote(p,manifest,fetcher,crypto);if(ticket!==version||p!==packet)return;
        el('quote-status').textContent='Retained response verified · '+q.history.length+' quote rows · identity status '+q.status;
        el('quote-evidence').innerHTML='<p>Identity checks require both instrument names, ticker, instrument type, daily granularity and timezone. A matching ticker alone does not qualify a quote.</p><p class="bv-exact">Receipt SHA-256: '+esc(q.receipt?.sha256||'No response received')+'</p><p>Session conversion: '+esc(q.timezone?.name||'Unavailable')+' · IANA '+esc(q.timezone?.iana_version||'Unavailable')+'</p><p><a href="/'+p.move.complete_quote_artifact.key+'?exact=1&nogen=1">Complete structured response and all quote rows</a></p>';
      }catch(_){if(ticket===version)clear('Quote original verification failed. Current readings are withheld.');}
    };
    el('older').onclick=()=>{offset+=50;history();};el('newer').onclick=()=>{offset=Math.max(0,offset-50);history();};root.setInterval(()=>{if(packet)render();},60000);refresh();return {refresh};
  }
  const api={IDS,ROOTS,qualified,view,identity,scalar,bytes,artifact,verifyView,verifySeries,verifyOriginal,verifyQuote,controller,display,chart,mount};
  if(typeof module==='object')module.exports=api;else root.JHBondVol=api;if(root.document)mount(root.document);
})(typeof globalThis==='object'?globalThis:this);
