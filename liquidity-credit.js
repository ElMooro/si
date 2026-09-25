/* reskin-skip: Native liquidity-credit research viewer; no inferred score,
   crisis state, calibrated forecast, or allocation permission. */
(function(root,factory){
  const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;
  if(root&&root.document)api.install(root);
})(typeof window==='object'?window:null,function(){
  'use strict';
  const URL='https://justhodl-dashboard-live.s3.amazonaws.com/data/liquidity-credit-engine.json';
  const RUN=/^data\/lce-research\/runs\/[a-f0-9]{64}\.json$/;
  const EVIDENCE=/^data\/evidence\/fred\/[a-f0-9]{64}\/[a-f0-9]{64}\.bin\.gz$/;
  const AGE={D:10,W:21,BW:35,M:100,Q:200,SA:370,A:550}, DAY=86400000;
  const CATEGORIES={balance_sheet:'Balance sheets',liquidity_facilities:'Loans and swaps',credit_spreads:'Credit spreads',corporate_yields:'Corporate and Treasury yields',lending_standards:'Lending surveys'};
  const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num=x=>typeof x==='number'&&Number.isFinite(x);
  const stamp=x=>typeof x==='string'&&/T.*(?:Z|[+-]\d\d:\d\d)$/.test(x)?Date.parse(x):NaN;
  const recent=(x,now)=>Number.isFinite(stamp(x))&&stamp(x)<=now&&now-stamp(x)<=26*3600000;
  function day(x){
    if(typeof x!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(x))return NaN;
    const t=Date.parse(x+'T00:00:00Z');return Number.isFinite(t)&&new Date(t).toISOString().slice(0,10)===x?t:NaN;
  }
  function eligible(d){
    return d?.contract==='liquidity-credit-research.v1'&&RUN.test(d?.replay?.manifest_key||'')&&
      d.calls_eligible===false&&d.sizing_eligible===false&&d.execution_eligible===false&&d.call===null&&
      d.series&&typeof d.series==='object'&&!Array.isArray(d.series);
  }
  function status(row,d,now){
    if(!row||row.available!==true||row.quality?.status!=='fresh'||!num(row.latest_value))return 'Unavailable';
    if(!recent(d.generated_at,now)||!recent(d.source_generated_at,now)||!recent(row.acquired_at,now))return 'Stale or invalid source clock';
    const t=day(row.latest_date),limit=AGE[row.frequency];
    if(!Number.isFinite(t)||t>now||!limit||Math.floor(now/DAY)-t/DAY>limit)return 'Stale or invalid observation date';
    if(!row._units||row._units!==row.source_definition?.units||row.series_id!==row.source_definition?.id)return 'Definition mismatch';
    if(!['definition','observations'].every(k=>EVIDENCE.test(row.evidence?.[k]?.key||'')&&row.evidence[k].captured===true))return 'Evidence unavailable';
    return 'Within source-age limits';
  }
  function value(n,unit){
    if(!num(n)||typeof unit!=='string'||!unit)return 'Unavailable';
    return n.toLocaleString('en-US',{maximumFractionDigits:6})+' '+unit;
  }
  function period(row){
    const d=row.latest_date;if(!Number.isFinite(day(d)))return 'Observation unavailable';
    if(row.frequency==='M')return d.slice(0,7)+' (monthly period; source date '+d+')';
    if(row.frequency==='Q')return d.slice(0,4)+' Q'+Math.ceil(Number(d.slice(5,7))/3)+' (quarterly period; source date '+d+')';
    return d;
  }
  function comparison(row,name,usable){
    const c=row.calendar_comparisons?.[name];
    if(!usable||!c||!num(c.change)||!Number.isFinite(day(c.baseline_date))||!Number.isFinite(day(c.current_date))||
      c.current_date!==row.latest_date||day(c.baseline_date)>day(c.current_date))return 'Unavailable';
    const expected=row._units==='Percent'?'percentage_points':row._units;
    if(c.change_unit!==expected||c.source_unit!==row._units)return 'Comparison unit mismatch';
    const unit=c.change_unit==='percentage_points'?'percentage points':c.change_unit;
    return (c.change>0?'+':'')+value(c.change,unit)+'; '+c.baseline_date+' \u2192 '+c.current_date+' (target '+(c.target_date||'unavailable')+')';
  }
  function view(d,category='balance_sheet',now=Date.now()){
    if(!eligible(d))return {status:'Unavailable',rows:[],categories:[],fresh:0,total:0,manifest:null};
    const pairs=Object.entries(d.series),categories=[...new Set(pairs.map(([,r])=>r?._category||'other'))];
    const selected=categories.includes(category)?category:categories[0],rows=[];
    let fresh=0;
    for(const [sid,r] of pairs){
      const state=r?.series_id===sid?status(r,d,now):'Definition mismatch',usable=state==='Within source-age limits';fresh+=usable?1:0;
      if((r?._category||'other')!==selected)continue;
      rows.push({sid,state,label:r?._label||sid,value:usable?value(r.latest_value,r._units):'Unavailable',
        observed:period(r||{}),basis:r?.source_definition?.frequency||'Frequency unavailable',acquired:r?.acquired_at||'Unavailable',
        changes:Object.fromEntries(['week','month','quarter','year'].map(k=>[k,comparison(r||{},k,usable)])),
        evidence:['definition','observations'].flatMap(k=>EVIDENCE.test(r?.evidence?.[k]?.key||'')?[{label:k,key:r.evidence[k].key}]:[]),
        history:r?.history||[],statistics:r?.statistics||{},lastObserved:r?.last_observed_value??null});
    }
    return {status:'Research measurements',rows,categories,selected,fresh,total:pairs.length,generated:d.generated_at,manifest:d.replay.manifest_key};
  }
  function panelHTML(d,category,now){
    const v=view(d,category,now);
    const tabs=v.categories.map(c=>'<button type="button" data-lce-category="'+esc(c)+'" aria-pressed="'+(c===v.selected)+'">'+esc(CATEGORIES[c]||c)+'</button>').join('');
    const rows=v.rows.map(r=>'<tr><th scope="row">'+esc(r.sid)+'<small>'+esc(r.label)+'</small></th><td>'+esc(r.value)+'<small>'+esc(r.state)+'</small></td><td>'+esc(r.observed)+'<small>'+esc(r.basis)+'</small><small>Acquired '+esc(r.acquired)+'</small></td><td>'+['week','month','quarter','year'].map(k=>'<div><b>'+k+':</b> '+esc(r.changes[k])+'</div>').join('')+'</td><td>'+r.evidence.map(e=>'<a href="/'+e.key+'">'+esc(e.label)+' original</a>').join('<br>')+'<details><summary>Historical observations and descriptive statistics</summary><pre>'+esc(JSON.stringify({last_observed_value:r.lastObserved,history:r.history,statistics:r.statistics},null,2))+'</pre></details></td></tr>').join('');
    return '<section class="jh-lce-panel"><h3>Liquidity and credit research</h3><p>'+esc(v.status)+' \u00b7 '+v.fresh+'/'+v.total+' within source-age limits'+(v.generated?' \u00b7 generated '+esc(v.generated):'')+'</p><p>WAIT / abstain. A null score or signal is unavailable; it is not zero risk or a NORMAL state. Measurements do not grant a Calls vote, forecast confidence or position size.</p><div class="jh-lce-tabs" aria-label="Research category">'+tabs+'</div><div class="jh-lce-scroll" tabindex="0" role="region" aria-label="Liquidity and credit source measurements"><table><thead><tr><th>Series / official definition</th><th>Current value / native unit</th><th>Observation and acquisition</th><th>Calendar comparisons / actual endpoints</th><th>Source evidence</th></tr></thead><tbody>'+rows+'</tbody></table></div><p>Calendar comparisons use the latest observation at or before the target date. Percent series change in percentage points. Historical statistics use the current retrieved vintage and include the latest observation; they are not a historical point-in-time backtest or a probability. Age ceilings follow the producer (daily 10, weekly 21, monthly 100, quarterly 200 days); the release calendar is unverified.</p><p><a href="'+URL+'">Complete research packet</a>'+(v.manifest?' \u00b7 <a href="/'+v.manifest+'">Retained inputs, code and replay manifest</a>':'')+'</p></section>';
  }
  function install(win){
    const doc=win.document;let data=null,category='balance_sheet',lastMarkup=null;
    function render(){
      const v=view(data,category),panel=doc.getElementById('liquidity-credit-panel');
      const markup=panelHTML(data,category);
      if(panel&&markup!==lastMarkup){panel.innerHTML=markup;lastMarkup=markup;panel.querySelectorAll('[data-lce-category]').forEach(button=>button.addEventListener('click',()=>{category=button.dataset.lceCategory;render();}));}
      if(!win.JUSTHODL_LCE_NO_PILL){
        let pill=doc.querySelector('.jh-lce-pill');if(!pill){pill=doc.createElement('a');pill.className='jh-lce-pill';pill.href='/liquidity.html#liquidity-credit-panel';doc.body.appendChild(pill);}
        pill.textContent=v.status==='Unavailable'?'Credit research unavailable':'Credit research '+v.fresh+'/'+v.total+' \u00b7 no calibrated score';
      }
    }
    async function load(){
      try{const r=await win.fetch(URL+'?t='+Date.now(),{cache:'no-store',credentials:'omit'});if(!r.ok)throw Error('Unavailable');data=await r.json();}catch(e){data=null;}render();
    }
    function init(){
      if(!doc.getElementById('jhLceStyles')){
        const style=doc.createElement('style');style.id='jhLceStyles';style.textContent='.jh-lce-panel{padding:18px;background:#101722;color:#cbd5e1;border:1px solid #334155;border-radius:8px;font:13px/1.6 system-ui}.jh-lce-panel h3{margin:0}.jh-lce-panel a{color:#93c5fd}.jh-lce-tabs{display:flex;flex-wrap:wrap;gap:6px;margin:15px 0}.jh-lce-tabs button{padding:8px 12px;background:#182433;border:1px solid #4b6078;border-radius:5px;color:#cbd5e1;cursor:pointer}.jh-lce-tabs button[aria-pressed=true]{background:#274869;color:white}.jh-lce-scroll{overflow-x:auto}.jh-lce-panel table{width:100%;min-width:950px;border-collapse:collapse}.jh-lce-panel th,.jh-lce-panel td{text-align:left;vertical-align:top;padding:10px;border-bottom:1px solid #334155}.jh-lce-panel th{font-weight:500}.jh-lce-panel tbody th{text-transform:none;letter-spacing:0}.jh-lce-panel tbody th small{font-size:12px}.jh-lce-panel small{display:block;color:#a6b6c9;overflow-wrap:anywhere}.jh-lce-panel td>div{margin-bottom:8px}.jh-lce-panel pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:250px;overflow:auto;max-width:400px}.jh-lce-panel summary{cursor:pointer}.jh-lce-pill{position:fixed;right:12px;bottom:140px;z-index:9997;padding:8px 12px;background:#101722;border:1px solid #506078;border-radius:12px;color:#cbd5e1;font:11px system-ui;text-decoration:none}';doc.head.appendChild(style);
      }
      load();win.setInterval(load,300000);win.setInterval(render,60000);
    }
    if(doc.readyState==='loading')doc.addEventListener('DOMContentLoaded',init);else init();
  }
  return {eligible,status,value,period,comparison,view,panelHTML,install};
});
