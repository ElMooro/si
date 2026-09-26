/* jh-reskin-skip */
(function(root){
  'use strict';
  const SPECS=typeof module==='object'?require('./jh-liquidity-agent-definitions.js'):root.JHLiquidityAgentDefinitions;
  const PREFIX='data/liquidity-agent-research/',LIMIT=32*1024*1024,DAY=86400000;
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
  function qualified(data){return data?.contract==='liquidity-agent-research.v1'&&FLAGS.every(k=>data[k]===false)&&data.call===null&&data.decision?.verb==='WAIT'&&data.decision?.meaning==='abstain'&&
    data.view?.contract==='liquidity-agent-summary.v1'&&same(Object.keys(data.series||{}).sort(),Object.keys(SPECS).sort())&&
    /^data\/liquidity-agent-research\/runs\/[a-f0-9]{64}\.json$/.test(data.replay?.manifest_key)&&/^[a-f0-9]{64}$/.test(data.replay?.output_sha256);}
  function view(data,now=Date.now()){
    const contract=qualified(data),current=contract&&recent(data.generated_at,now)&&recent(data.source_generated_at,now)&&clock(data.source_generated_at)<=clock(data.generated_at);
    const rows=Object.entries(SPECS).map(([sid,spec])=>{
      const r=data?.series?.[sid]||{},d=r.source_definition||{},definition=spec.definition;
      const defined=definition&&same([r.unit,r.frequency,d.frequency,r.seasonal_adjustment],definition)&&d.id===sid&&d.units===r.unit&&d.frequency_short===r.frequency&&d.seasonal_adjustment===r.seasonal_adjustment;
      const age=Number.isFinite(day(r.latest_date))?Math.floor((now-day(r.latest_date))/DAY):null;
      const ageLimits={D:10,W:21,BW:35,M:100,Q:200,SA:370,A:550};
      const eligible=current&&defined&&r.definition_reviewed===true&&r.series_id===sid&&FLAGS.every(k=>r[k]===false)&&r.quality?.status==='within_age_ceiling'&&
        recent(r.acquired_at,now)&&clock(r.acquired_at)<=clock(data.generated_at)&&evidence(r)&&identity(r.complete_history_artifact,'series')&&
        age!==null&&age>=0&&age<=ageLimits[r.frequency]&&number(r.current?.value)&&exact(r.current?.exact_decimal)&&Number(r.current.exact_decimal)===r.current.value;
      return {sid,group:spec.group,label:typeof r.label==='string'?r.label:sid,eligible,quality:eligible?'Within declared age ceiling':r.quality?.status==='within_age_ceiling'?'Expired or invalid':r.quality?.status||'Unavailable',
        value:eligible?r.current.exact_decimal:'Unavailable',unit:defined?r.unit:'Definition unreviewed',date:r.latest_date||'Unavailable',age,row:r};
    });
    return {contract,current,rows,available:rows.filter(r=>r.eligible).length,generated:contract?data.generated_at:null};
  }
  async function bytes(url,fetcher,limit=LIMIT){
    const r=await fetcher(url,{cache:'no-store',credentials:'omit'});if(!r.ok)throw Error('Artifact unavailable');
    if(+r.headers.get('Content-Length')>limit)throw Error('Artifact exceeds bound');
    const reader=r.body.getReader(),chunks=[];let size=0;
    try{while(true){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>limit)throw Error('Artifact exceeds bound');chunks.push(value);}}
    catch(e){await reader.cancel();throw e;}
    const out=new Uint8Array(size);let offset=0;for(const c of chunks){out.set(c,offset);offset+=c.length;}return out;
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
    if(manifest.contract!=='liquidity-agent-replay.v1'||manifest.output_sha256!==packet.replay.output_sha256||manifest.generated_at!==packet.generated_at||
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
  function comparison(row,horizon){const c=row.calendar_comparisons?.[horizon];return c&&c.current_date===row.latest_date&&Number.isFinite(day(c.baseline_date))&&day(c.baseline_date)<=day(c.current_date)&&c.source_unit===row.unit&&exact(c.change_decimal)?c.change_decimal+' '+(c.change_unit||row.unit)+' · '+c.baseline_date+' → '+c.current_date:'Unavailable';}
  function panelHTML(packet,now=Date.now(),query='',group='',horizon='month'){
    const v=view(packet,now),rows=v.rows.filter(r=>(!group||r.group===group)&&(r.sid+' '+r.label).toLowerCase().includes(query.toLowerCase()));
    return '<p class="la-status">'+v.available+' / '+v.rows.length+' series within declared age ceilings · '+esc(v.generated||'No native publication')+'</p><p>Observation age and acquisition age are separate. Release-calendar freshness and historical point-in-time validity remain unverified. WAIT means abstain; these measurements provide no position size.</p>'+
      '<div class="la-scroll" tabindex="0" role="region" aria-label="Liquidity source measurements"><table><thead><tr><th>Series / definition</th><th>Current / native unit</th><th>Observation</th><th>Availability</th><th>Calendar change</th><th>Evidence</th></tr></thead><tbody>'+rows.map(r=>'<tr><th scope="row">'+esc(r.sid)+'<small>'+esc(r.label)+'</small></th><td>'+esc(r.value)+'<small>'+esc(r.unit)+'</small></td><td>'+esc(r.date)+'<small>'+esc(r.row.source_definition?.frequency||'Unreviewed period')+'</small></td><td>'+esc(r.quality)+'<small>'+esc(r.age===null?'':r.age+' days from period date')+'</small></td><td>'+esc(r.eligible?comparison(r.row,horizon):'Unavailable')+'</td><td><button type="button" data-series="'+r.sid+'">Inspect</button></td></tr>').join('')+'</tbody></table></div><p>'+rows.length+' series shown; all '+v.rows.length+' remain in the complete packet.</p>';
  }
  function historyHTML(full,page=0){
    const rows=[...full.history].sort((a,b)=>b.observation_date.localeCompare(a.observation_date)),selected=rows.slice(page*50,page*50+50);
    return '<p>Complete retained history: '+rows.length+' rows, including missing and future records. Current retrieved vintage; not a point-in-time backtest.</p><p>Rows '+(rows.length?page*50+1:0)+'–'+Math.min(rows.length,(page+1)*50)+' of '+rows.length+'</p><div class="la-scroll" tabindex="0"><table><thead><tr><th>Original row</th><th>Observation date</th><th>Native value</th><th>Realtime interval</th></tr></thead><tbody>'+selected.map(r=>'<tr><td>'+r.original_row+'</td><td>'+esc(r.observation_date)+'</td><td>'+esc(r.native_value??'missing')+'</td><td>'+esc(r.realtime_start)+' → '+esc(r.realtime_end)+'</td></tr>').join('')+'</tbody></table></div>';
  }
  function install(win){
    const doc=win.document,host=doc.getElementById('jh-liquidity-agent-research');if(!host)return;
    let packet=null,epoch=0,detailEpoch=0,selected=null,historyPage=0;
    host.innerHTML='<h2>Liquidity source desk</h2><p id="la-binding" role="status">Waiting for a verified native publication.</p><div class="la-tools"><label>Find a series <input id="la-search" type="search" placeholder="Series ID or name"></label><label>Group <select id="la-group"><option value="">All groups</option>'+[...new Set(Object.values(SPECS).map(s=>s.group))].map(g=>'<option>'+esc(g)+'</option>').join('')+'</select></label><label>Change window <select id="la-horizon"><option value="month">Calendar month</option><option value="week">Calendar week</option><option value="quarter">Calendar quarter</option><option value="year">Calendar year</option></select></label></div><div id="la-table"></div><section id="la-detail" hidden aria-label="Selected source evidence"></section>';
    const binding=doc.getElementById('la-binding'),table=doc.getElementById('la-table'),detail=doc.getElementById('la-detail');
    function render(){table.innerHTML=panelHTML(packet,Date.now(),doc.getElementById('la-search').value,doc.getElementById('la-group').value,doc.getElementById('la-horizon').value);}
    function histories(){doc.getElementById('la-history').innerHTML=historyHTML(selected,historyPage);doc.getElementById('la-prev').disabled=historyPage===0;doc.getElementById('la-next').disabled=(historyPage+1)*50>=selected.history.length;}
    host.addEventListener('input',render);host.addEventListener('change',render);
    host.addEventListener('click',async event=>{
      const button=event.target.closest('button'),sid=button?.dataset.series;
      if(sid&&packet?.series[sid]){
        const mine=epoch,selection=++detailEpoch,row=packet.series[sid];detail.hidden=false;selected=null;
        detail.innerHTML='<h3>'+esc(sid)+' · '+esc(row.label)+'</h3><p>'+esc(row.definition_note||row.source_definition?.notes||'No additional definition note.')+'</p><p>Acquired '+esc(row.acquired_at)+' · provider updated '+esc(row.provider_updated_at)+' · release time unknown</p><p id="la-evidence-status" role="status">Checking the complete retained series…</p><div id="la-evidence-body"></div>';
        try{const full=await verifySeries(row,win.fetch.bind(win),win.crypto);if(mine!==epoch||selection!==detailEpoch)return;
          selected=full;historyPage=0;doc.getElementById('la-evidence-status').textContent='Complete series hash and summary binding verified. Original response verification is separate.';
          doc.getElementById('la-evidence-body').innerHTML='<p><a href="https://fred.stlouisfed.org/series/'+sid+'">Official definition</a> · <a href="/'+row.complete_history_artifact.key+'?exact=1&nogen=1">Download complete series JSON</a></p><button type="button" id="la-original">Verify original response</button><p id="la-original-status" role="status"></p><div id="la-history"></div><div class="la-tools"><button id="la-prev" type="button">Newer 50</button><button id="la-next" type="button">Older 50</button></div>';histories();
        }catch(_){if(mine===epoch&&selection===detailEpoch)doc.getElementById('la-evidence-status').textContent='Not verified. The complete series could not be matched to this publication.';}
      }else if(button?.id==='la-prev'&&selected){historyPage=Math.max(0,historyPage-1);histories();}
      else if(button?.id==='la-next'&&selected){historyPage=Math.min(Math.ceil(selected.history.length/50)-1,historyPage+1);histories();}
      else if(button?.id==='la-original'&&selected){const mine=epoch,row=selected,status=doc.getElementById('la-original-status');button.disabled=true;status.textContent='Checking original definition and observations…';try{const result=await verifyOriginal(row,win.fetch.bind(win),win.crypto);if(mine===epoch&&selected===row)status.textContent=result;}catch(_){if(mine===epoch&&selected===row)status.textContent='Not verified: original bytes or coordinates differ.';}finally{button.disabled=false;}}
    });
    win.JHLiquidityAgentResearch.accept=async next=>{const mine=++epoch;packet=null;selected=null;detail.hidden=true;render();
      if(next?.contract!=='liquidity-agent-research.v1'){binding.textContent='Native publication pending. Earlier observations remain in the dated legacy panels below.';return;}
      binding.textContent='Checking immutable publication binding…';try{await verifyView(next,win.fetch.bind(win),win.crypto);if(mine!==epoch)return;packet=next;binding.textContent='Immutable publication and all 73 series references verified. Inspect any series for its complete history and original source.';render();}
      catch(_){if(mine===epoch){packet=null;binding.textContent='Publication not verified. Current measurements are withheld.';render();}}};
    render();win.setInterval(render,60000);
  }
  const api={SPECS,view,qualified,panelHTML,verifyView,verifySeries,verifyOriginal,historyHTML,comparison,install,exactDecimal};
  if(typeof module==='object')module.exports=api;else root.JHLiquidityAgentResearch=api;
})(typeof window==='object'?window:globalThis);
