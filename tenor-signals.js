/* Treasury research display. Legacy state/score fields never grant authority. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(root&&root.document)api.install(root);
})(typeof window==='object'?window:null,function(){
  'use strict';
  const CONTRACT='treasury-tenor-research.v2';
  const NAMES={nominal_2y:'2-year nominal yields',bill_participation:'Bill participation',nominal_30y:'30-year nominal yields'};
  const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num=(x,unit)=>typeof x==='number'&&Number.isFinite(x)?x.toFixed(2)+' '+unit:'Unavailable';
  function eligible(data,now=Date.now()){
    const t=Date.parse(data?.generated_at);
    return data?.schema_version===CONTRACT&&data?.role==='research_measurements'&&Number.isFinite(t)&&t<=now+300000&&now-t<=48*3600000;
  }
  function view(data,now=Date.now()){
    if(!eligible(data,now))return {status:'UNAVAILABLE',generated_at:null,cards:Object.keys(NAMES).map(k=>({key:k,label:NAMES[k],state:'UNAVAILABLE',rows:[]}))};
    return {status:'RESEARCH ONLY',generated_at:data.generated_at,cards:Object.keys(NAMES).map(key=>{
      const m=data.measurements?.[key]||{},metrics=m.metrics||{},latest=m.latest_auction||{};
      const state=['AVAILABLE','STALE','UNAVAILABLE'].includes(m.state)?m.state:'UNAVAILABLE';
      let rows=[];
      if(state==='AVAILABLE'){
        if(key==='bill_participation'){
          rows=(metrics.tenor_breakdown||[]).map(t=>[t.tenor,
            t.state==='AVAILABLE'?`${t.latest_auction?.auction_date||'Undated'} · indirect share ${num(typeof t.indirect_drop_pts==='number'?-t.indirect_drop_pts:null,'pp vs prior mean')} · bid-to-cover change ${num(t.btc_spike,'ratio points')}`:t.state||'UNAVAILABLE']);
        }else rows=[['Auction / prior',`${latest.auction_date||'Undated'} / ${m.prior_auction?.auction_date||'Undated'}`],
                    ['Quote basis',latest.quote_basis||'Unavailable'],['Yield change',num(metrics.yield_change_bp,'bp')],
                    ['Reopening',latest.reopening===true?'Yes':latest.reopening===false?'No':'Unavailable'],
                    ['Dated DFF comparison',m.fed_funds_observation?`${m.fed_funds_observation.as_of} · ${num(metrics.spread_to_ff_bp,'bp spread')}`:'Unavailable']];
      }
      return {key,label:NAMES[key],state,rows};
    })};
  }
  function panelHTML(data,now){
    const model=view(data,now);
    const cards=model.cards.map(c=>`<article class="jh-tenor-card"><h4>${esc(c.label)}</h4><strong>${esc(c.state)}</strong>${c.rows.map(([a,b])=>`<div class="row"><span>${esc(a)}</span><span>${esc(b)}</span></div>`).join('')}</article>`).join('');
    const run=data?.reproducibility?.key;
    const link=typeof run==='string'&&/^data\/tenor-research\/runs\/[a-f0-9]{64}\.json$/.test(run)&&eligible(data,now)?` · <a href="/${esc(run)}">Retained source/replay manifest</a>`:'';
    return `<section class="jh-tenor-panel"><div class="head"><h3>Treasury research measurements</h3><span>${esc(model.status)}${model.generated_at?' · generated '+esc(model.generated_at):''}</span></div><div class="jh-tenor-cards">${cards}</div><p>Same instrument, remaining term and reopening status. Auction-to-auction yield changes include intervening market moves. Indirect awards include domestic and foreign customers. These observations do not measure Fed policy probabilities, QE or offshore funding conditions and do not authorize a position size.</p><p><a href="/data/auction-tenor-signals.json">Measurement packet</a>${link}</p></section>`;
  }
  function install(win){
    const doc=win.document;
    function render(data){
      const model=view(data);let pill=doc.querySelector('.jh-tenor-pill');
      if(!win.JUSTHODL_TENOR_NO_PILL){
        if(!pill){pill=doc.createElement('a');pill.className='jh-tenor-pill';pill.href='/auctions.html#tenor-research';doc.body.appendChild(pill);}
        pill.textContent='Treasury research · '+(model.status==='UNAVAILABLE'?'unavailable':model.cards.filter(c=>c.state==='AVAILABLE').length+'/3 observed');
        pill.title='Descriptive auction measurements; no macro call or allocation authority';
      }
      const target=doc.getElementById('tenor-signals-panel');if(target)target.innerHTML=panelHTML(data);
    }
    async function load(){
      try{const r=await win.fetch('/data/auction-tenor-signals.json?t='+Date.now());if(!r.ok)throw Error('source unavailable');render(await r.json());}
      catch(e){render(null);}
    }
    function init(){
      if(!doc.getElementById('jhTenorStyles')){
        const s=doc.createElement('style');s.id='jhTenorStyles';
        s.textContent='.jh-tenor-pill{position:fixed;right:12px;bottom:60px;z-index:9998;background:#101722;border:1px solid #506078;border-radius:12px;padding:8px 12px;color:#b8c9dc;font:11px system-ui;text-decoration:none}.jh-tenor-panel{border:1px solid #334155;border-radius:8px;padding:16px;background:#101722;color:#cbd5e1;font:12px system-ui}.jh-tenor-panel .head{display:flex;gap:12px;flex-wrap:wrap;justify-content:space-between}.jh-tenor-panel h3,.jh-tenor-panel h4{margin:4px 0 10px}.jh-tenor-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin:12px 0}.jh-tenor-card{padding:12px;background:#182131;border-radius:6px}.jh-tenor-card strong{color:#a5b4cc}.jh-tenor-card .row{display:flex;justify-content:space-between;gap:12px;margin-top:10px;overflow-wrap:anywhere}.jh-tenor-panel p{line-height:1.6}.jh-tenor-panel a{color:#93c5fd}';
        doc.head.appendChild(s);
      }
      load();win.setInterval(load,5*60*1000);
    }
    if(doc.readyState==='loading')doc.addEventListener('DOMContentLoaded',init);else init();
  }
  return {eligible,view,panelHTML,install};
});
