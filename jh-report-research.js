(function(root){
  'use strict';
  const esc=v=>String(v==null?'':v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const limits={D:10,W:21,BW:35,M:100,Q:200,SA:370,A:550};
  const contract='report-observations.v1';
  function link(key,label){
    return typeof key==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(key)&&!key.includes('..')
      ? '<a target="_blank" rel="noopener" href="/'+esc(key)+'">'+esc(label)+'</a>' : 'Evidence unavailable';
  }
  function status(row,now=Date.now()){
    const d=/^\d{4}-\d{2}-\d{2}$/.test(row.date||'')?Date.parse(row.date+'T00:00:00Z'):NaN;
    const age=Math.floor(now/86400000)-Math.floor(d/86400000), acquired=Date.parse(row.acquired_at);
    if(row.contract!==contract||!Number.isFinite(age)||age<0||row.current_decimal==null||!limits[row.frequency])return 'unavailable';
    if(age>limits[row.frequency])return 'stale observation';
    if(!Number.isFinite(acquired)||acquired>now+5000||now-acquired>26*3600000)return 'stale source';
    return (row.quality||{}).status==='fresh'?'fresh':String((row.quality||{}).status||'unavailable');
  }
  function render(packet,filter='',horizon='month',now=Date.now()){
    if(!packet||packet.contract!==contract)throw new Error('Verified report contract unavailable');
    if(!['week','month','quarter','year'].includes(horizon))horizon='month';
    const catalog=packet.catalog||{}, measures=packet.measurements||{};
    const ids=Object.keys(catalog).filter(s=>[s,catalog[s].display_name,catalog[s].category,(measures[s]||{}).name].join(' ').toLowerCase().includes(filter.toLowerCase()));
    const fresh=Object.values(measures).filter(r=>status(r,now)==='fresh').length;
    const cards=ids.map(sid=>{
      const row=measures[sid], entry=catalog[sid]||{};
      if(!row)return '<article class="research-card"><h3>'+esc(sid)+' · '+esc(entry.display_name)+'</h3><p class="muted">Source unavailable · '+esc((packet.errors||{})[sid]||'not captured')+'</p></article>';
      const change=(row.changes||{})[horizon]||{}, state=status(row,now), evidence=row.evidence||{};
      const diff=change.change_decimal==null?'—':change.change_decimal;
      const dateText=change.baseline_date?'Baseline '+change.baseline_date:'No compatible calendar baseline';
      return '<article class="research-card"><div class="card-heading"><h3>'+esc(row.name||sid)+'</h3><span class="state '+(state==='fresh'?'fresh':'unavailable')+'">'+esc(state)+'</span></div>'+
        '<p class="muted">'+esc(sid)+' · '+esc(entry.category||'')+'</p><p class="measurement">'+esc(row.current_decimal==null?'—':row.current_decimal)+' <span>'+esc(row.unit)+'</span></p>'+
        '<p>Observed <strong>'+esc(row.date)+'</strong> · '+esc(row.definition?.frequency||row.frequency)+' · '+esc(row.seasonal_adjustment)+'</p>'+
        '<p>'+esc(horizon)+' change: <strong>'+esc(diff)+'</strong> '+esc(change.change_unit||row.unit)+'<br><span class="muted">'+esc(dateText)+'</span></p>'+
        '<details><summary>Inspect definition, calculation and originals</summary><p>Current value comes from original observation row '+esc(row.current_row_index)+' (zero based). '+link(evidence.observations?.key,'Original observations')+' · '+link(evidence.definition?.key,'Original definition')+'</p>'+
        '<p>Retrieved '+esc(row.acquired_at)+'. Provider metadata updated '+esc(row.provider_updated_at||'unknown')+'. The original publication time is not established.</p>'+
        '<p>Calendar comparison: '+esc(change.current_date)+' versus '+esc(change.baseline_date||'unavailable')+'. Target '+esc(change.target_date||'unavailable')+'. Current '+esc(change.current_decimal??'—')+', baseline '+esc(change.baseline_decimal??'—')+'.</p>'+
        '<p>Relative change '+esc(change.pct_change==null?'unavailable':change.pct_change+'%')+'; this differs from a percentage-point change. '+esc(change.relative_change_reason||'')+'</p>'+
        '<p>History: '+esc(row.coverage?.returned)+' returned / '+esc(row.coverage?.matching_query_count)+' matching provider rows. Current retrieved vintage; historical as-known-at values are not established.</p>'+
        '<a target="_blank" rel="noopener" href="https://fred.stlouisfed.org/series/'+encodeURIComponent(sid)+'">Official FRED series</a></details></article>';
    }).join('');
    const liq=packet.net_liquidity||{}, legs=Object.entries(liq.components||{});
    const validProxy=legs.length===3&&legs.every(([sid,leg])=>leg.eligible===true&&status(measures[sid]||{},now)==='fresh');
    return '<p class="coverage"><strong>'+fresh+' / '+Object.keys(catalog).length+'</strong> series fresh by observation and acquisition ceilings · '+ids.length+' shown</p>'+
      '<p class="muted">Packet generated '+esc(packet.generated_at)+' · '+link(packet.replay?.manifest_key,'Replay this research snapshot')+'</p>'+
      '<details class="proxy"><summary>Net-liquidity proxy · '+esc(validProxy?liq.net_decimal:'unavailable')+' USD millions</summary><p>'+esc(liq.formula)+'</p><p>'+esc(liq.basis)+'. This is a mixed-date descriptive proxy, not synchronized reserves or an easing signal.</p>'+
      '<ul>'+legs.map(([sid,l])=>'<li>'+esc(sid)+' = '+esc(l.value_decimal??'—')+' '+esc(l.unit)+' · '+esc(l.date)+'</li>').join('')+'</ul></details>'+
      '<div class="research-grid">'+(cards||'<p>No matching series.</p>')+'</div>';
  }
  async function refresh(host,fetcher=root.fetch.bind(root),selection={}){
    try{
      const response=await fetcher('/data/report-measurements.json',{cache:'no-store'});
      if(!response.ok)throw new Error('Source unavailable');
      const packet=await response.json();
      host.innerHTML=render(packet,selection.filter||'',selection.horizon||'month');
      return packet;
    }catch(_){host.innerHTML='<p role="status">Verified research is unavailable. Earlier values have been cleared. No portfolio recommendation is inferred.</p>';return null;}
  }
  if(typeof module!=='undefined'&&module.exports)module.exports={render,status,refresh,link};
  if(root.document){
    const host=root.document.getElementById('report-research'),search=root.document.getElementById('research-search'),horizon=root.document.getElementById('research-horizon');
    if(host&&search&&horizon){
      let packet=null;
      const selection=()=>({filter:search.value,horizon:horizon.value});
      const load=async()=>{packet=await refresh(host,root.fetch.bind(root),selection());};
      const redraw=()=>{if(packet)host.innerHTML=render(packet,search.value,horizon.value);};
      search.addEventListener('input',redraw);horizon.addEventListener('change',redraw);
      load();root.setInterval(load,300000);
    }
  }
})(typeof window!=='undefined'?window:globalThis);
