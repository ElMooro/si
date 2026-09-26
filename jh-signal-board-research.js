/* jh-reskin-skip */
(function(root){
  'use strict';
  const PRIVATE=new Set(['data/pm-decision.json','data/sizing.json']),MAX=4*1024*1024;
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
  const CONTRACT='signal-board-research.v1',PREFIX='data/signal-board-research/',PERMISSIONS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  const stable=v=>JSON.stringify(v&&typeof v==='object'?Array.isArray(v)?v.map(x=>JSON.parse(stable(x))):Object.fromEntries(Object.keys(v).sort().map(k=>[k,JSON.parse(stable(v[k]))])):v);
  function artifact(ref,kind,extension='json'){
    if(!object(ref)||!Number.isSafeInteger(ref.bytes)||ref.bytes<0||ref.bytes>64*1024*1024||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+'.'+extension)throw Error('Exact immutable artifact required');return ref;
  }
  async function retained(ref,kind,api,fetcher,crypto,extension='json'){
    artifact(ref,kind,extension);const raw=await api.bytes('/'+ref.key+'?exact=1&nogen=1',fetcher,64*1024*1024,45000);
    if(raw.length!==ref.bytes||await api.hash(raw,crypto)!==ref.sha256)throw Error('Whole retained artifact differs');return raw;
  }
  async function bindNative(packet,catalog,api,fetcher,crypto){
    if(packet.contract!==CONTRACT)throw Error('Reviewed native contract required');
    const key=packet.replay?.manifest_key;if(typeof key!=='string'||!/^data\/signal-board-research\/runs\/[a-f0-9]{64}\.json$/.test(key))throw Error('Immutable run required');
    const raw=await api.bytes('/'+key+'?exact=1&nogen=1',fetcher,MAX,15000),digest=await api.hash(raw,crypto);
    if(key!==PREFIX+'runs/'+digest+'.json')throw Error('Run hash differs');const run=JSON.parse(new TextDecoder().decode(raw));
    if(run.contract!=='signal-board-replay.v1'||run.qualification!=='b8a507a655bef246f08384504a9e139ed8e254a793ec42dbf7affeecef865054'||run.generated_at!==packet.generated_at||run.view?.sha256!==packet.replay.view_sha256)throw Error('Reviewed run binding differs');
    const [viewRaw,inputRaw]=await Promise.all([retained(run.view,'views',api,fetcher,crypto),retained(run.input,'inputs',api,fetcher,crypto)]);
    const view=JSON.parse(new TextDecoder().decode(viewRaw)),inputs=JSON.parse(new TextDecoder().decode(inputRaw)),{replay,...head}=packet;
    if(stable(head)!==stable(view)||stable(inputs.registry)!==stable(catalog.feeds)||inputs.generated_at!==packet.generated_at||inputs.contract!=='signal-board-inputs.v1')throw Error('Complete bound view or source registry differs');
    const keys=[...new Set(catalog.feeds.map(r=>r.source_key))].sort();
    if(stable(Object.keys(inputs.captures).sort())!==stable(keys)||stable(Object.keys(packet.sources).sort())!==stable(keys))throw Error('Every capture outcome required');
    for(const key of keys){
      const source=packet.sources[key],capture=inputs.captures[key];
      if(!object(source)||!object(capture)||source.source_key!==key||capture.source_key!==key||PERMISSIONS.some(k=>source[k]!==false))throw Error('Unqualified source identity required');
      if(PRIVATE.has(key)){if(source.original!==null||capture.requested!==false||capture.status!=='excluded_private_account_input')throw Error('Private input must be excluded');}
      else if(source.original){artifact(source.original,'originals','bin');if(stable(source.original)!==stable(capture.original))throw Error('Retained source differs from capture');}
    }
    // Binding only: browser does not execute producer Python or replay every source here.
    return {run,inputs,binding_checked:true,complete_source_replay:false,original_provider_verified:false};
  }
  const path=k=>typeof k==='string'&&/^(data|screener)\/[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)?\.json$/.test(k)&&!PRIVATE.has(k)&&!k.startsWith('data/portfolio/');
  function registry(value){
    if(value?.contract!=='signal-board-page-registry.v1'||!Array.isArray(value.feeds)||value.feeds.length!==99||!Array.isArray(value.supplemental)||value.supplemental.length!==17)throw Error('Complete reviewed registry required');
    const names=new Set();for(const row of value.feeds){
      if(!object(row)||typeof row.engine!=='string'||!row.engine||names.has(row.engine)||typeof row.category!=='string'||
        (PRIVATE.has(row.source_key)?row.capture_scope!=='excluded_private_account_input':!path(row.source_key)||row.capture_scope!=='anonymous_public_sidecar_only'))throw Error('Unreviewed source mapping');
      names.add(row.engine);
    }
    if(value.supplemental.some(r=>!path(r.source_key)||typeof r.label!=='string'))throw Error('Unreviewed supplemental source');return value;
  }
  function inventory(packet,catalog){
    registry(catalog);
    if(!object(packet)||!Array.isArray(packet.engines)||packet.engines.length>500||(packet.contract!=null&&packet.contract!==CONTRACT))throw Error('Reviewed inventory shape required');
    const native=packet.contract===CONTRACT;
    if(native&&(packet.engines.length!==99||packet.n_engines!==99||packet.composite_signal!==null||packet.composite_posture!=='WAIT'||packet.decision?.verb!=='WAIT'||PERMISSIONS.some(k=>packet[k]!==false)))throw Error('Complete abstaining native inventory required');
    const byName=new Map(catalog.feeds.map(r=>[r.engine,r])),roots=Object.create(null);catalog.feeds.forEach(r=>roots[r.source_key]=(roots[r.source_key]||0)+1);
    const counts=Object.create(null);packet.engines.forEach(row=>{if(object(row)&&typeof row.engine==='string')counts[row.engine]=(counts[row.engine]||0)+1;});
    return packet.engines.map((raw,index)=>{
      const r=object(raw)?raw:{},source=byName.get(r.engine),matched=!!source&&r.category===source.category&&counts[r.engine]===1;
      if(native&&(!matched||r.source_key!==source.source_key||r.row_id!==index+1||r.signal!==null||PERMISSIONS.some(k=>r[k]!==false)))throw Error('Complete unqualified native row required');
      return {index,raw,name:typeof r.engine==='string'?r.engine:'Unmapped row '+(index+1),category:typeof r.category==='string'?r.category:'Not reported',
        as_of:typeof (native?r.reported_generated_at:r.as_of)==='string'?(native?r.reported_generated_at:r.as_of):'Not reported',source:matched?source:null,
        original:native?packet.sources?.[r.source_key]?.original:null,
        source_views:matched?roots[source.source_key]:null,status:!matched?'Unmapped or duplicate identity':PRIVATE.has(source.source_key)?'Private input excluded':native?String(r.source_status).replaceAll('_',' ')+' · observation age unverified':r.stale===true?'Predecessor marked stale':'Source age unverified',
        calls_eligible:false,sizing_eligible:false};
    });
  }
  function clockStatus(value,now=Date.now()){
    const stamp=typeof value==='string'&&/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value)?Date.parse(value):NaN;
    return !Number.isFinite(stamp)?'Publication date unavailable':stamp>now?'Future publication date — unverified':now-stamp>40*3600000?'Historical snapshot older than 40 hours':'Recent publication; underlying observation freshness unverified';
  }
  function sourceLink(row){
    if(!row.source||!path(row.source.source_key))return esc(row.source?'Private source excluded':'No verified mapping');
    return '<a href="/'+esc(row.source.source_key)+'?exact=1&amp;nogen=1" target="_blank" rel="noopener">Latest: '+esc(row.source.source_key)+'</a>'+(row.source_views>1?'<br><small>'+row.source_views+' views share this key</small>':'')+(row.original?'<br><button type="button" data-sb-source="'+row.index+'">Inspect retained input</button>':'');
  }
  function mount(doc,api=root.JHFIFXResearch,fetcher=root.fetch,crypto=root.crypto){
    if(!doc.getElementById('sb-research')||!api)return null;
    const el=k=>doc.getElementById('sb-'+k);let packet=null,rows=[],version=0,inspection=0;
    function clear(message){packet=null;rows=[];el('content').hidden=true;el('status').textContent=message;el('table').innerHTML='';el('raw').textContent='';el('detail').textContent='';el('fingerprint').textContent='';}
    function render(){
      if(!packet)return;const query=el('search').value.trim().toLowerCase(),cat=el('category').value;
      const filtered=rows.filter(r=>(!cat||r.category===cat)&&(r.name+' '+r.category+' '+(r.source?.source_key||'')).toLowerCase().includes(query));
      el('status').textContent='WAIT · '+(packet.contract===CONTRACT?'bound derived inventory; original-provider ancestry unverified':'unqualified predecessor snapshot')+' · '+clockStatus(packet.generated_at);
      el('count').textContent=filtered.length+' of '+rows.length+' reported rows shown; filtering never changes decision permission.';
      el('table').innerHTML='<table><thead><tr><th scope="col">Engine / category</th><th scope="col">Reported timestamp</th><th scope="col">Evidence status</th><th scope="col">Source packet</th><th scope="col">Original fields</th></tr></thead><tbody>'+filtered.map(r=>'<tr><td><strong>'+esc(r.name)+'</strong><br>'+esc(r.category)+'</td><td>'+esc(r.as_of)+'</td><td>'+esc(r.status)+'</td><td>'+sourceLink(r)+'</td><td><button class="sb-inspect" type="button" data-sb-row="'+r.index+'">Inspect row '+(r.index+1)+'</button></td></tr>').join('')+'</tbody></table>';
    }
    async function refresh(){
      const ticket=++version;clear('Loading the recorded board…');
      try{
        const [raw,reg]=await Promise.all([api.bytes('/data/signal-board.json?exact=1&nogen=1',fetcher,MAX,15000),api.bytes('/assets/signal-board-registry.json',fetcher,100000,15000)]);
        const p=JSON.parse(new TextDecoder().decode(raw)),c=JSON.parse(new TextDecoder().decode(reg)),all=inventory(p,c),digest=await api.hash(raw,crypto);
        if(p.contract===CONTRACT)await bindNative(p,c,api,fetcher,crypto);
        if(ticket!==version)return;packet=p;rows=all;
        const categories=[...new Set(rows.map(r=>r.category))].sort();el('category').innerHTML='<option value="">All categories</option>'+categories.map(v=>'<option value="'+esc(v)+'">'+esc(v)+'</option>').join('');el('category').value='';
        el('metadata').textContent=(p.contract===CONTRACT?'Inventory publication: ':'Predecessor publication: ')+(typeof p.generated_at==='string'?p.generated_at:'Not reported')+(p.n_engines!==rows.length?' · declared row count differs from the actual snapshot':'');
        const unique=new Set(rows.filter(r=>r.source).map(r=>r.source.source_key)).size;
        el('counts').innerHTML=[['Reported rows',rows.length],['Mapped source keys',unique],['Qualified investment votes',0]].map(([label,n])=>'<div class="fx-panel"><span>'+label+'</span><strong>'+n+'</strong></div>').join('');
        el('raw').textContent=new TextDecoder().decode(raw);el('fingerprint').textContent=raw.length.toLocaleString()+' downloaded bytes · SHA-256 '+digest;
        el('extra').innerHTML=c.supplemental.map(r=>'<a href="/'+esc(r.source_key)+'?exact=1&amp;nogen=1" target="_blank" rel="noopener">'+esc(r.label)+'</a>').join('');
        el('detail-title').textContent='Inspect a reported row';el('detail-note').textContent='Every original field remains available. Legacy scores and narratives are unqualified.';
        el('content').hidden=false;render();
      }catch(_){if(ticket===version)clear('Snapshot unavailable or format changed · WAIT. No current recommendation is displayed.');}
    }
    el('table').addEventListener('click',async event=>{
      const input=event.target.closest?.('[data-sb-source]');
      if(input&&packet){
        const row=rows[Number(input.dataset.sbSource)];if(!row?.original)return;const ticket=version,request=++inspection;
        el('detail-title').textContent=row.name+' · retained input';el('detail').textContent='';el('detail-note').textContent='Checking the complete retained bytes…';el('detail-title').scrollIntoView?.({block:'start'});el('detail-title').focus?.({preventScroll:true});
        try{const raw=await retained(row.original,'originals',api,fetcher,crypto,'bin');if(ticket!==version||request!==inspection)return;
          el('detail').textContent=new TextDecoder().decode(raw);el('detail-note').textContent=raw.length.toLocaleString()+' complete bytes · SHA-256 '+row.original.sha256+' · hash checked against this snapshot. Derived engine response; original-provider truth and predictive skill remain unverified.';
        }catch(_){if(ticket===version&&request===inspection){el('detail').textContent='';el('detail-note').textContent='Retained input unavailable or changed. No source evidence is displayed.';}}return;
      }
      const button=event.target.closest?.('[data-sb-row]');if(!button||!packet)return;const index=Number(button.dataset.sbRow),row=rows[index];
      if(!Number.isInteger(index)||!row)return;inspection++;el('detail-title').textContent=row.name;el('detail').textContent=JSON.stringify(row.raw,null,2);el('detail-title').scrollIntoView?.({block:'start'});el('detail-title').focus?.({preventScroll:true});
      el('detail-note').textContent='Unqualified decoded row '+(index+1)+' · reported timestamp '+row.as_of+'. The complete raw snapshot preserves source numeric precision. These fields do not authorize a trade.';
    });
    el('refresh').addEventListener('click',refresh);el('search').addEventListener('input',render);el('category').addEventListener('change',render);
    const timer=setInterval(render,30000);refresh();return {refresh,render,destroy(){clearInterval(timer);version++;clear('Closed');}};
  }
  const api={registry,inventory,clockStatus,sourceLink,mount,artifact,retained,bindNative};if(typeof module!=='undefined'&&module.exports)module.exports=api;root.JHSignalBoardResearch=api;
  if(root.document)mount(root.document);
})(typeof globalThis!=='undefined'?globalThis:this);
