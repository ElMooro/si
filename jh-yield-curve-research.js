/* jh-reskin-skip */
(function(root){
  'use strict';
  const SPECS=typeof module==='object'?require('./jh-yield-curve-definitions.js'):root.JHYieldCurveDefinitions;
  const PREFIX='data/yield-curve-research/',LIMIT=32*1024*1024,DAY=86400000;
  const METRICS={"2s10s":{"coefficients":{"DGS10":100,"DGS2":-100},"divisor":1,"unit":"basis_points"},"3M10Y":{"coefficients":{"DGS10":100,"DGS3MO":-100},"divisor":1,"unit":"basis_points"},"5s30s":{"coefficients":{"DGS30":100,"DGS5":-100},"divisor":1,"unit":"basis_points"},"2s5s":{"coefficients":{"DGS5":100,"DGS2":-100},"divisor":1,"unit":"basis_points"},"10s30s":{"coefficients":{"DGS30":100,"DGS10":-100},"divisor":1,"unit":"basis_points"},"dff_to_10y":{"coefficients":{"DGS10":100,"DFF":-100},"divisor":1,"unit":"basis_points"},"butterfly_2_5_10":{"coefficients":{"DGS5":200,"DGS2":-100,"DGS10":-100},"divisor":2,"unit":"basis_points"},"curvature_2_5_10":{"coefficients":{"DGS5":200,"DGS2":-100,"DGS10":-100},"divisor":1,"unit":"basis_points"},"nominal_mean":{"coefficients":{"DGS1MO":1,"DGS3MO":1,"DGS6MO":1,"DGS1":1,"DGS2":1,"DGS3":1,"DGS5":1,"DGS7":1,"DGS10":1,"DGS20":1,"DGS30":1},"divisor":11,"unit":"percent"},"real_5s30s":{"coefficients":{"DFII30":100,"DFII5":-100},"divisor":1,"unit":"basis_points"},"nominal_real_breakeven_residual_10y":{"coefficients":{"DGS10":100,"DFII10":-100,"T10YIE":-100},"divisor":1,"unit":"basis_points"},"target_band_width":{"coefficients":{"DFEDTARU":100,"DFEDTARL":-100},"divisor":1,"unit":"basis_points"}};
  const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified'];
  const day=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(s)&&Number.isFinite(Date.parse(s+'T00:00:00Z'))&&new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s?Date.parse(s+'T00:00:00Z'):NaN;
  const clock=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(s)&&Number.isFinite(day(s.slice(0,10)))&&Number.isFinite(Date.parse(s))?Date.parse(s):NaN;
  const recent=(s,now)=>Number.isFinite(clock(s))&&clock(s)<=now&&now-clock(s)<=26*3600000;
  const number=v=>typeof v==='number'&&Number.isFinite(v);
  const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const exact=s=>typeof s==='string'&&/^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(s)&&Number.isFinite(Number(s));
  const same=(a,b)=>JSON.stringify(sort(a))===JSON.stringify(sort(b));
  function sort(v){return Array.isArray(v)?v.map(sort):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,sort(v[k])])):v;}
  function identity(ref,category){return ref&&/^[a-f0-9]{64}$/.test(ref.sha256)&&ref.key===PREFIX+category+'/'+ref.sha256+'.json'&&Number.isInteger(ref.bytes)&&ref.bytes>0&&ref.bytes<=LIMIT;}
  function evidence(r){
    if(!r?.evidence||Object.keys(r.evidence).sort().join(',')!=='definition,observations')return false;
    return Object.entries(r.evidence).every(([kind,ref])=>{
      if(ref?.contract!=='source-evidence.v1'||ref.captured!==true||ref.provider!=='fred'||!Number.isInteger(ref.bytes)||ref.bytes<=0||ref.bytes>LIMIT||
        !/^data\/evidence\/fred\/[a-f0-9]{64}\/[a-f0-9]{64}\.bin\.gz$/.test(ref.key)||!/^[a-f0-9]{64}$/.test(ref.sha256)||!ref.key.endsWith('/'+ref.sha256+'.bin.gz'))return false;
      try{const u=new URL(ref.source_url);return u.protocol==='https:'&&u.hostname==='api.stlouisfed.org'&&u.pathname==='/fred/series'+(kind==='observations'?'/observations':'')&&
        u.searchParams.getAll('series_id').join(',')===r.series_id&&![...u.searchParams.keys()].some(k=>/key|token|secret/i.test(k));}catch(_){return false;}
    });
  }
  function qualified(data){return data?.contract==='yield-curve-research.v1'&&FLAGS.every(k=>data[k]===false)&&data.call===null&&data.decision?.verb==='WAIT'&&data.decision?.meaning==='abstain'&&
    data.signals?.length===0&&data.qualified_term_premium_bps===null&&data.portfolio_consequences?.status==='UNAVAILABLE'&&data.portfolio_consequences?.target_weights===null&&FLAGS.every(k=>data.shape?.[k]===false)&&Object.values(data.derived||{}).every(r=>FLAGS.every(k=>r[k]===false))&&same(Object.keys(data.derived||{}).sort(),Object.keys(METRICS).sort())&&data.view?.contract==='yield-curve-summary.v1'&&same(Object.keys(data.series||{}).sort(),Object.keys(SPECS).sort())&&
    /^data\/yield-curve-research\/runs\/[a-f0-9]{64}\.json$/.test(data.replay?.manifest_key)&&/^[a-f0-9]{64}$/.test(data.replay?.output_sha256);}
  function view(data,now=Date.now()){
    const contract=qualified(data),current=contract&&recent(data.generated_at,now)&&recent(data.source_generated_at,now)&&clock(data.source_generated_at)<=clock(data.generated_at);
    const rows=Object.entries(SPECS).map(([sid,spec])=>{
      const r=data?.series?.[sid]||{},d=r.source_definition||{},definition=spec.definition;
      const defined=definition&&same([r.unit,r.frequency,d.frequency,r.seasonal_adjustment],definition)&&d.id===sid&&d.units===r.unit&&d.frequency_short===r.frequency&&d.seasonal_adjustment===r.seasonal_adjustment;
      const age=Number.isFinite(day(r.latest_date))?Math.floor((now-day(r.latest_date))/DAY):null;

      const eligible=current&&defined&&r.definition_reviewed===true&&r.series_id===sid&&FLAGS.every(k=>r[k]===false)&&r.quality?.status==='within_age_ceiling'&&
        recent(r.acquired_at,now)&&clock(r.acquired_at)<=clock(data.generated_at)&&evidence(r)&&identity(r.complete_history_artifact,'series')&&
        age!==null&&age>=0&&age<=7&&number(r.current?.value)&&exact(r.current?.exact_decimal)&&Number(r.current.exact_decimal)===r.current.value;
      return {sid,group:spec.group,label:typeof d.title==='string'?d.title:sid,eligible,quality:eligible?'Within declared age ceiling':r.quality?.status==='within_age_ceiling'?'Expired or invalid':r.quality?.status||'Unavailable',
        value:eligible?r.current.exact_decimal:'Unavailable',unit:defined?r.unit:'Definition unreviewed',date:r.latest_date||'Unavailable',age,row:r};
    });
    return {contract,current,rows,available:rows.filter(r=>r.eligible).length,generated:contract?data.generated_at:null};
  }
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
  async function artifact(ref,category,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!identity(ref,category))throw Error('Artifact coordinates invalid');
    const raw=await bytes('/'+ref.key+'?exact=1&nogen=1',fetcher);
    if(raw.length!==ref.bytes||await hash(raw,crypto)!==ref.sha256)throw Error('Artifact bytes differ');return JSON.parse(new TextDecoder().decode(raw));
  }
  async function verifyView(packet,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!qualified(packet))throw Error('Native contract unavailable');
    const key=packet.replay.manifest_key,raw=await bytes('/'+key+'?exact=1&nogen=1',fetcher);
    if(key!==PREFIX+'runs/'+await hash(raw,crypto)+'.json')throw Error('Run hash differs');
    const manifest=JSON.parse(new TextDecoder().decode(raw));
    if(manifest.contract!=='yield-curve-replay.v1'||manifest.output_sha256!==packet.replay.output_sha256||manifest.generated_at!==packet.generated_at||
      !same(Object.keys(manifest.series||{}).sort(),Object.keys(SPECS).sort()))throw Error('Run binding differs');
    const projection=await artifact(manifest.view,'views',fetcher,crypto),{replay,...body}=packet;
    if(!same(projection,body))throw Error('Current view differs');
    for(const [sid,r] of Object.entries(packet.series))if(!identity(manifest.series[sid],'series')||!same(manifest.series[sid],r.complete_history_artifact))throw Error('Series binding differs');
    return manifest;
  }
  async function verifySeries(row,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!Object.hasOwn(SPECS,row?.series_id))throw Error('Unreviewed series');
    const full=await artifact(row.complete_history_artifact,'series',fetcher,crypto);
    const {complete_history_artifact,retained_original_rows,...summary}=row,{history,...rest}=full;
    if(!Array.isArray(history)||history.length!==retained_original_rows||!same(summary,rest))throw Error('Series projection differs');
    return full;
  }
  function exactDecimal(value){
    if(String(value).length>512)throw Error('Decimal bound');const m=String(value).match(/^([+-]?)(\d*)\.?([0-9]*)(?:[eE]([+-]?\d+))?$/);
    if(!m||!(m[2]+m[3]).length||Math.abs(Number(m[4]||0))>500)throw Error('Invalid decimal');
    let n=(m[2]+m[3]).replace(/^0+/,'')||'0',scale=m[3].length-Number(m[4]||0);if(n==='0')return '0';while(n.endsWith('0')){n=n.slice(0,-1);scale--;}
    return (m[1]==='-'?'-':'')+n+'e'+(-scale);
  }
  async function verifyOriginal(row,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    if(!evidence(row)||!Number.isInteger(row.original_row)||row.original_row<0)throw Error('Original unavailable');
    const docs={};for(const [kind,ref] of Object.entries(row.evidence)){
      let raw=await bytes('/'+ref.key+'?exact=1',fetcher);
      if(raw[0]===31&&raw[1]===139)raw=await bytes('expanded',async()=>new Response(new Blob([raw]).stream().pipeThrough(new DecompressionStream('gzip'))));
      if(raw.length!==ref.bytes||await hash(raw,crypto)!==ref.sha256)throw Error('Original bytes differ');docs[kind]=JSON.parse(new TextDecoder().decode(raw));
    }
    const d=docs.definition.seriess?.length===1?docs.definition.seriess[0]:null,p=docs.observations.observations?.[row.original_row];
    if(!d||!same(d,row.source_definition)||d.id!==row.series_id||!p||p.date!==row.latest_date||exactDecimal(p.value)!==exactDecimal(row.last_observed?.exact_decimal))throw Error('Original definition or value differs');
    return 'Original definition and observation hashes verified; row '+row.original_row+' on '+p.date+'.';
  }
  function comparison(row,horizon){
    const c=row.current_observation_comparisons?.[horizon]||row.current_comparisons?.[horizon],baseline=c?.baseline?.observation_date;
    return c&&c.matched_observation_steps===Number(horizon)&&[1,5,20,60].includes(Number(horizon))&&Number.isFinite(day(c.current_date))&&Number.isFinite(day(baseline))&&day(baseline)<day(c.current_date)&&exact(c.change?.exact_decimal)&&Number(c.change.exact_decimal)===c.change.value?
      display(c.change.exact_decimal)+' '+c.change_unit.replaceAll('_',' ')+' · '+baseline+' → '+c.current_date+' · '+c.elapsed_calendar_days+' calendar days; '+c.unmatched_or_missing_dates+' unmatched dates':'Unavailable';
  }
  function display(value){return exact(value)&&value.length>12?Number(value).toLocaleString('en-US',{maximumFractionDigits:4,useGrouping:false})+' (rounded)':value;}
  function derived(packet,name,now=Date.now()){
    const v=view(packet,now),r=packet?.derived?.[name],spec=METRICS[name];
    if(!v.current||!r||!spec||!same(r.coefficients,spec.coefficients)||r.divisor!==spec.divisor||r.unit!==spec.unit||r.quality?.status!=='within_age_ceiling'||!FLAGS.every(k=>r[k]===false))return null;
    const point=r.current,date=day(point?.observation_date),age=Math.floor(now/DAY)-Math.floor(date/DAY);
    if(!point||!exact(point.measurement?.exact_decimal)||Number(point.measurement.exact_decimal)!==point.measurement.value||!Number.isFinite(age)||age<0||age>7)return null;
    for(const sid of Object.keys(spec.coefficients)){
      const row=v.rows.find(r=>r.sid===sid),leg=point.legs?.[sid];
      if(!row?.eligible||!leg||!Number.isInteger(leg.original_row)||leg.original_row<0||!exact(leg.native_decimal)||date>day(row.date))return null;
    }
    return r;
  }
  function curve(packet,group,now=Date.now()){
    const v=view(packet,now),c=packet?.curves?.[group],expected=Object.keys(SPECS).filter(s=>SPECS[s].group===group);
    if(!v.current||!expected.length||!c?.complete||c.unit!=='percent'||c.interpolated!==false||!same(c.requested_series,expected)||!same(c.points?.map(p=>p.series_id),expected))return [];
    const age=Math.floor(now/DAY)-Math.floor(day(c.observation_date)/DAY);if(!Number.isFinite(age)||age<0||age>7)return [];
    if(c.points.some(p=>!v.rows.find(r=>r.sid===p.series_id)?.eligible||day(c.observation_date)>day(packet.series[p.series_id].latest_date)||p.tenor_months!==SPECS[p.series_id].tenor_months||!exact(p.exact_decimal)||Number(p.exact_decimal)!==p.value||!Number.isInteger(p.original_row)||p.original_row<0))return [];
    return c.points.map(p=>({tenor:p.tenor_months<12?p.tenor_months+'M':p.tenor_months/12+'Y',x:p.tenor_months,y:p.value,date:c.observation_date,exact:p.exact_decimal,sid:p.series_id}));
  }
  function metricsHTML(packet,now=Date.now(),horizon='5'){
    return Object.keys(METRICS).map(name=>{const r=derived(packet,name,now),spec=METRICS[name];
      return '<div class="kv"><div class="k">'+esc(name.replaceAll('_',' '))+'</div><div class="v">'+esc(r?display(r.current.measurement.exact_decimal):'Unavailable')+'</div><small>'+esc(spec.unit.replaceAll('_',' '))+' · '+esc(r?.current?.observation_date||'No current reading')+'</small><p>'+esc(r?comparison(r,horizon):'Unavailable')+'</p><details><summary>Formula and source legs</summary><p>'+esc(JSON.stringify(spec))+'</p><p>Unrounded measurement: '+esc(r?.current?.measurement?.exact_decimal||'Unavailable')+'</p><p>Unrounded change: '+esc(r?.current_comparisons?.[horizon]?.change?.exact_decimal||'Unavailable')+'</p><p>'+esc(r?JSON.stringify(r.current.legs):'Current legs unavailable')+'</p></details></div>';
    }).join('');
  }
  function panelHTML(packet,now=Date.now(),query='',group='',horizon='5'){
    const v=view(packet,now),rows=v.rows.filter(r=>(!group||r.group===group)&&(r.sid+' '+r.label).toLowerCase().includes(query.toLowerCase()));
    return '<p class="yc-status">'+v.available+' / '+v.rows.length+' series within declared age ceilings · '+esc(v.generated||'No native publication')+'</p><p>All changes use matched numeric observations, not calendar periods. Observation age (seven-day ceiling) and acquisition age (26-hour ceiling) are separate. Release-calendar freshness and historical point-in-time validity remain unverified. WAIT means abstain; these measurements provide no position size.</p>'+
      '<div class="yc-scroll" tabindex="0" role="region" aria-label="Yield source measurements"><table><thead><tr><th>Series / definition</th><th>Current / native unit</th><th>Observation</th><th>Availability</th><th>Observation-step change</th><th>Evidence</th></tr></thead><tbody>'+rows.map(r=>'<tr><th scope="row">'+esc(r.sid)+'<small>'+esc(r.label)+'</small></th><td>'+esc(r.value)+'<small>'+esc(r.unit)+'</small></td><td>'+esc(r.date)+'<small>'+esc(r.row.source_definition?.frequency||'Unreviewed period')+'</small></td><td>'+esc(r.quality)+'<small>'+esc(r.age===null?'':r.age+' days from period date')+'</small></td><td>'+esc(r.eligible?comparison(r.row,horizon):'Unavailable')+'</td><td><button type="button" data-series="'+r.sid+'">Inspect</button></td></tr>').join('')+'</tbody></table></div><p>'+rows.length+' series shown; all '+v.rows.length+' remain in the complete packet.</p>';
  }
  function historyHTML(full,page=0){
    const rows=[...full.history].sort((a,b)=>b.observation_date.localeCompare(a.observation_date)),selected=rows.slice(page*50,page*50+50);
    return '<p>Complete retained history: '+rows.length+' rows, including missing and future records. Current retrieved vintage; not a point-in-time backtest.</p><p>Rows '+(rows.length?page*50+1:0)+'–'+Math.min(rows.length,(page+1)*50)+' of '+rows.length+'</p><div class="yc-scroll" tabindex="0"><table><thead><tr><th>Original row</th><th>Observation date</th><th>Native value</th><th>Realtime interval</th></tr></thead><tbody>'+selected.map(r=>'<tr><td>'+r.original_row+'</td><td>'+esc(r.observation_date)+'</td><td>'+esc(r.native_value??'missing')+'</td><td>'+esc(r.realtime_start)+' → '+esc(r.realtime_end)+'</td></tr>').join('')+'</tbody></table></div>';
  }
  function install(win,options={}){
    const doc=win.document,host=doc.getElementById('jh-yield-curve-research');if(!host)return;
    let packet=null,epoch=0,detailEpoch=0,selected=null,historyPage=0;
    host.innerHTML='<h2>Yield Curve source desk</h2><p id="yc-binding" role="status">Waiting for a verified native publication.</p><div class="yc-tools"><label>Find a series <input id="yc-search" type="search" placeholder="Series ID or name"></label><label>Group <select id="yc-group"><option value="">All groups</option>'+[...new Set(Object.values(SPECS).map(s=>s.group))].map(g=>'<option>'+esc(g)+'</option>').join('')+'</select></label><label>Change window <select id="yc-horizon"><option value="1">1 matched observation</option><option value="5" selected>5 matched observations</option><option value="20">20 matched observations</option><option value="60">60 matched observations</option></select></label></div><div id="yc-table"></div><section id="yc-detail" hidden aria-label="Selected source evidence"></section>';
    const binding=doc.getElementById('yc-binding'),table=doc.getElementById('yc-table'),detail=doc.getElementById('yc-detail');
    function render(){table.innerHTML=panelHTML(packet,Date.now(),doc.getElementById('yc-search').value,doc.getElementById('yc-group').value,doc.getElementById('yc-horizon').value);if(options.onrender)options.onrender(packet,view(packet),doc.getElementById('yc-horizon').value);}
    function histories(){doc.getElementById('yc-history').innerHTML=historyHTML(selected,historyPage);doc.getElementById('yc-prev').disabled=historyPage===0;doc.getElementById('yc-next').disabled=(historyPage+1)*50>=selected.history.length;}
    host.addEventListener('input',render);host.addEventListener('change',render);
    host.addEventListener('click',async event=>{
      const button=event.target.closest('button'),sid=button?.dataset.series;
      if(sid&&packet?.series[sid]){
        const mine=epoch,selection=++detailEpoch,row=packet.series[sid];detail.hidden=false;selected=null;
        detail.innerHTML='<h3>'+esc(sid)+' · '+esc(row.source_definition?.title||sid)+'</h3><p>'+esc(row.definition_note||row.source_definition?.notes||'No additional definition note.')+'</p><p>Acquired '+esc(row.acquired_at)+' · provider updated '+esc(row.provider_updated_at)+' · release time unknown</p><p id="yc-evidence-status" role="status">Checking the complete retained series…</p><div id="yc-evidence-body"></div>';
        try{const full=await verifySeries(row,win.fetch.bind(win),win.crypto);if(mine!==epoch||selection!==detailEpoch)return;
          selected=full;historyPage=0;doc.getElementById('yc-evidence-status').textContent='Complete series hash and summary binding verified. Original response verification is separate.';
          doc.getElementById('yc-evidence-body').innerHTML='<p><a href="https://fred.stlouisfed.org/series/'+sid+'">Official definition</a> · <a href="/'+row.complete_history_artifact.key+'?exact=1&nogen=1">Download complete series JSON</a></p><button type="button" id="yc-original">Verify original response</button><p id="yc-original-status" role="status"></p><div id="yc-history"></div><div class="yc-tools"><button id="yc-prev" type="button">Newer 50</button><button id="yc-next" type="button">Older 50</button></div>';histories();
        }catch(_){if(mine===epoch&&selection===detailEpoch)doc.getElementById('yc-evidence-status').textContent='Not verified. The complete series could not be matched to this publication.';}
      }else if(button?.id==='yc-prev'&&selected){historyPage=Math.max(0,historyPage-1);histories();}
      else if(button?.id==='yc-next'&&selected){historyPage=Math.min(Math.ceil(selected.history.length/50)-1,historyPage+1);histories();}
      else if(button?.id==='yc-original'&&selected){const mine=epoch,row=selected,status=doc.getElementById('yc-original-status');button.disabled=true;status.textContent='Checking original definition and observations…';try{const result=await verifyOriginal(row,win.fetch.bind(win),win.crypto);if(mine===epoch&&selected===row)status.textContent=result;}catch(_){if(mine===epoch&&selected===row)status.textContent='Not verified: original bytes or coordinates differ.';}finally{button.disabled=false;}}
    });
    win.JHYieldCurveResearch.accept=async next=>{const mine=++epoch;packet=null;selected=null;detail.hidden=true;render();
      if(next?.contract!=='yield-curve-research.v1'){binding.textContent='Native publication pending. Earlier observations remain in the dated legacy panels.';return;}
      binding.textContent='Checking immutable publication binding…';try{await verifyView(next,win.fetch.bind(win),win.crypto);if(mine!==epoch)return;packet=next;binding.textContent='Immutable publication and all 23 series references verified. Inspect any series for its complete history and original source.';render();}
      catch(_){if(mine===epoch){packet=null;binding.textContent='Publication not verified. Current measurements are withheld.';render();}}};
    render();win.setInterval(render,60000);
  }
  const api={SPECS,METRICS,bytes,derived,curve,metricsHTML,view,qualified,panelHTML,verifyView,verifySeries,verifyOriginal,historyHTML,comparison,install,exactDecimal};
  if(typeof module==='object')module.exports=api;else root.JHYieldCurveResearch=api;
})(typeof window==='object'?window:globalThis);
