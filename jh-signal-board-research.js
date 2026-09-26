/* jh-reskin-skip */
(function(root){
  'use strict';
  const PRIVATE=new Set(['data/pm-decision.json','data/sizing.json']),MAX=4*1024*1024;
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
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
    if(!object(packet)||!Array.isArray(packet.engines)||packet.engines.length>500||packet.contract!=null)throw Error('Reviewed predecessor shape required; new contracts need separate qualification');
    const byName=new Map(catalog.feeds.map(r=>[r.engine,r])),roots=Object.create(null);catalog.feeds.forEach(r=>roots[r.source_key]=(roots[r.source_key]||0)+1);
    const counts=Object.create(null);packet.engines.forEach(row=>{if(object(row)&&typeof row.engine==='string')counts[row.engine]=(counts[row.engine]||0)+1;});
    return packet.engines.map((raw,index)=>{
      const r=object(raw)?raw:{},source=byName.get(r.engine),matched=!!source&&r.category===source.category&&counts[r.engine]===1;
      return {index,raw,name:typeof r.engine==='string'?r.engine:'Unmapped row '+(index+1),category:typeof r.category==='string'?r.category:'Not reported',
        as_of:typeof r.as_of==='string'?r.as_of:'Not reported',source:matched?source:null,
        source_views:matched?roots[source.source_key]:null,status:!matched?'Unmapped or duplicate identity':PRIVATE.has(source.source_key)?'Private input excluded':r.stale===true?'Predecessor marked stale':'Source age unverified',
        calls_eligible:false,sizing_eligible:false};
    });
  }
  function clockStatus(value,now=Date.now()){
    const stamp=typeof value==='string'&&/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value)?Date.parse(value):NaN;
    return !Number.isFinite(stamp)?'Publication date unavailable':stamp>now?'Future publication date — unverified':now-stamp>40*3600000?'Historical snapshot older than 40 hours':'Recent publication; underlying observation freshness unverified';
  }
  function sourceLink(row){
    if(!row.source||!path(row.source.source_key))return esc(row.source?'Private source excluded':'No verified mapping');
    return '<a href="/'+esc(row.source.source_key)+'?exact=1&amp;nogen=1" target="_blank" rel="noopener">'+esc(row.source.source_key)+'</a>'+(row.source_views>1?'<br><small>'+row.source_views+' views share this key</small>':'');
  }
  function mount(doc,api=root.JHFIFXResearch,fetcher=root.fetch,crypto=root.crypto){
    if(!doc.getElementById('sb-research')||!api)return null;
    const el=k=>doc.getElementById('sb-'+k);let packet=null,rows=[],version=0;
    function clear(message){packet=null;rows=[];el('content').hidden=true;el('status').textContent=message;el('table').innerHTML='';el('raw').textContent='';el('detail').textContent='';el('fingerprint').textContent='';}
    function render(){
      if(!packet)return;const query=el('search').value.trim().toLowerCase(),cat=el('category').value;
      const filtered=rows.filter(r=>(!cat||r.category===cat)&&(r.name+' '+r.category+' '+(r.source?.source_key||'')).toLowerCase().includes(query));
      el('status').textContent='WAIT · unqualified predecessor snapshot · '+clockStatus(packet.generated_at);
      el('count').textContent=filtered.length+' of '+rows.length+' reported rows shown; filtering never changes decision permission.';
      el('table').innerHTML='<table><thead><tr><th scope="col">Engine / category</th><th scope="col">Reported timestamp</th><th scope="col">Evidence status</th><th scope="col">Source packet</th><th scope="col">Original fields</th></tr></thead><tbody>'+filtered.map(r=>'<tr><td><strong>'+esc(r.name)+'</strong><br>'+esc(r.category)+'</td><td>'+esc(r.as_of)+'</td><td>'+esc(r.status)+'</td><td>'+sourceLink(r)+'</td><td><button class="sb-inspect" type="button" data-sb-row="'+r.index+'">Inspect row '+(r.index+1)+'</button></td></tr>').join('')+'</tbody></table>';
    }
    async function refresh(){
      const ticket=++version;clear('Loading the recorded board…');
      try{
        const [raw,reg]=await Promise.all([api.bytes('/data/signal-board.json?exact=1&nogen=1',fetcher,MAX,15000),api.bytes('/assets/signal-board-registry.json',fetcher,100000,15000)]);
        const p=JSON.parse(new TextDecoder().decode(raw)),c=JSON.parse(new TextDecoder().decode(reg)),all=inventory(p,c),digest=await api.hash(raw,crypto);
        if(ticket!==version)return;packet=p;rows=all;
        const categories=[...new Set(rows.map(r=>r.category))].sort();el('category').innerHTML='<option value="">All categories</option>'+categories.map(v=>'<option value="'+esc(v)+'">'+esc(v)+'</option>').join('');el('category').value='';
        el('metadata').textContent='Predecessor publication: '+(typeof p.generated_at==='string'?p.generated_at:'Not reported')+(p.n_engines!==rows.length?' · declared row count differs from the actual snapshot':'');
        const unique=new Set(rows.filter(r=>r.source).map(r=>r.source.source_key)).size;
        el('counts').innerHTML=[['Reported rows',rows.length],['Mapped source keys',unique],['Qualified investment votes',0]].map(([label,n])=>'<div class="fx-panel"><span>'+label+'</span><strong>'+n+'</strong></div>').join('');
        el('raw').textContent=new TextDecoder().decode(raw);el('fingerprint').textContent=raw.length.toLocaleString()+' downloaded bytes · SHA-256 '+digest;
        el('extra').innerHTML=c.supplemental.map(r=>'<a href="/'+esc(r.source_key)+'?exact=1&amp;nogen=1" target="_blank" rel="noopener">'+esc(r.label)+'</a>').join('');
        el('detail-title').textContent='Inspect a reported row';el('detail-note').textContent='Every original field remains available. Legacy scores and narratives are unqualified.';
        el('content').hidden=false;render();
      }catch(_){if(ticket===version)clear('Snapshot unavailable or format changed · WAIT. No current recommendation is displayed.');}
    }
    el('table').addEventListener('click',event=>{
      const button=event.target.closest?.('[data-sb-row]');if(!button||!packet)return;const index=Number(button.dataset.sbRow),row=rows[index];
      if(!Number.isInteger(index)||!row)return;el('detail-title').textContent=row.name;el('detail').textContent=JSON.stringify(row.raw,null,2);
      el('detail-note').textContent='Unqualified decoded row '+(index+1)+' · reported timestamp '+row.as_of+'. The complete raw snapshot preserves source numeric precision. These fields do not authorize a trade.';
    });
    el('refresh').addEventListener('click',refresh);el('search').addEventListener('input',render);el('category').addEventListener('change',render);
    const timer=setInterval(render,30000);refresh();return {refresh,render,destroy(){clearInterval(timer);version++;clear('Closed');}};
  }
  const api={registry,inventory,clockStatus,sourceLink,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;root.JHSignalBoardResearch=api;
  if(root.document)mount(root.document);
})(typeof globalThis!=='undefined'?globalThis:this);
