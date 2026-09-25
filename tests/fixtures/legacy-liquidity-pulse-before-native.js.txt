/* reskin-skip: Explicit FRED units and periods for legacy reported observations.
   This viewer does not qualify the legacy producer's scores or narratives.
   Reviewed metadata: FRED series pages, 2026-09-25. Whole predecessor and
   references: tests/fixtures/liquidity-observation-display-migration.json. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(root&&root.document)api.install(root);
})(typeof window==='object'?window:null,function(){
  'use strict';
  const ENDPOINT='https://justhodl-dashboard-live.s3.amazonaws.com/data/liquidity-pulse.json';
  const DAY=86400000;
  // Ceilings are display-age checks, not a verified release calendar.
  const SPECS={
    WALCL:['Fed total assets','usd_mn','Wednesday level',21],
    WRESBAL:['Reserve balances','usd_mn','Weekly average, ending Wednesday',21],
    WTREGEN:['Treasury General Account','usd_mn','Weekly average, ending Wednesday',21],
    RESPPALGUONNWW:['Treasury notes and bonds held outright','usd_mn','Wednesday level',21],
    RESPPNTEPNWW:['Securities eligible as currency collateral','usd_mn','Wednesday memo level',21],
    OTHL1690:['Loans maturing in 16–90 days','usd_mn','Wednesday level; all reported loans in this maturity bucket',21],
    SWP1690:['Central-bank swaps maturing in 16–90 days','usd_mn','Wednesday level',21],
    BAMLH0A3HYC:['US CCC and lower high-yield OAS','percent','Daily option-adjusted spread',10],
    BAMLHE00EHYIOAS:['Euro high-yield OAS','percent','Daily option-adjusted spread',10],
    BAMLEMHBHYCRPIOAS:['Emerging-market high-yield corporate OAS','percent','Daily option-adjusted spread',10],
    HQMCB10YR:['10-year HQM corporate spot rate','percent','Monthly estimated yield; not an option-adjusted spread',75]
  };
  const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const finite=x=>typeof x==='number'&&Number.isFinite(x);
  function date(value){
    if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value))return NaN;
    const t=Date.parse(value+'T00:00:00Z');
    return Number.isFinite(t)&&new Date(t).toISOString().slice(0,10)===value?t:NaN;
  }
  function format(value,unit){
    if(!finite(value))return 'Unavailable';
    if(unit==='usd_mn')return '$'+(value/1000).toLocaleString('en-US',{minimumFractionDigits:3,maximumFractionDigits:3})+' bn';
    if(unit==='percent')return value.toFixed(2)+'%';
    return 'Unit unverified';
  }
  function view(data,now=Date.now()){
    const generated=typeof data?.generated_at==='string'?Date.parse(data.generated_at):NaN;
    const wrapperOK=Number.isFinite(generated)&&generated<=now&&now-generated<=26*3600000;
    const series=data?.series&&typeof data.series==='object'&&!Array.isArray(data.series)?data.series:{};
    const keys=[...Object.keys(SPECS),...Object.keys(series).filter(k=>!Object.hasOwn(SPECS,k))];
    const rows=keys.map(sid=>{
      const s=series[sid]||{},spec=Object.hasOwn(SPECS,sid)?SPECS[sid]:null;
      const t=date(s.latest_date),age=Number.isFinite(t)?Math.floor((now-t)/DAY):null;
      const monthly=sid==='HQMCB10YR';
      const validDate=Number.isFinite(t)&&t<=now&&(!monthly||s.latest_date.endsWith('-01'));
      const aliases={'usd_mn':'usd_mn','Millions of U.S. Dollars':'usd_mn','percent':'percent','Percent':'percent'};
      const units=[s.unit,s.units].filter(u=>u!=null);
      const conflict=units.some(u=>typeof u!=='string'||!Object.hasOwn(aliases,u)||!spec||aliases[u]!==spec[1]);
      const status=!spec?'Unit unverified':!validDate||!finite(s.latest_value)||conflict?'Unavailable':
        !wrapperOK||age>spec[3]?'Stale reported observation':'Reported observation';
      const show=spec&&validDate&&finite(s.latest_value)&&!conflict;
      return {sid,label:spec?spec[0]:sid,status,value:show?format(s.latest_value,spec[1]):'Unavailable',
        reported:finite(s.latest_value)?String(s.latest_value):'Unavailable',
        nativeUnit:conflict?'Conflicting units':spec?spec[1]==='usd_mn'?'USD millions':'Percent':'Unverified',
        observation:validDate?(monthly?s.latest_date.slice(0,7)+' (monthly period)':s.latest_date):'Unavailable',
        basis:spec?spec[2]:'Definition unverified',age:validDate?age:null,
        source:spec?'https://fred.stlouisfed.org/series/'+sid:null};
    });
    return {generated:Number.isFinite(generated)?data.generated_at:null,status:wrapperOK?'Reported context':'Unavailable or stale packet',rows};
  }
  function panelHTML(data,now){
    const v=view(data,now);
    const rows=v.rows.map(r=>'<tr><td>'+(r.source?'<a href="'+r.source+'">'+esc(r.sid)+'</a>':esc(r.sid))+'<br>'+esc(r.label)+'</td><td>'+esc(r.value)+'<small>'+esc(r.reported)+' '+esc(r.nativeUnit)+' as reported</small></td><td>'+esc(r.observation)+'<small>'+esc(r.basis)+'</small></td><td>'+esc(r.status)+(r.age!==null?'<small>'+r.age+' calendar days from period date</small>':'')+'</td></tr>').join('');
    return '<section class="jh-liq-panel"><h3>Liquidity and credit: reported observations</h3><p>'+esc(v.status)+(v.generated?' · packet generated '+esc(v.generated):'')+'</p><p>Source units and observation periods are explicit. These legacy packet values have not been replayed against retained originals. Display age limits are 21 days for weekly series, 10 for daily and 75 for monthly; they do not establish release-calendar freshness.</p><div class="jh-liq-scroll" tabindex="0" role="region" aria-label="Reported liquidity observations"><table><thead><tr><th>Series / definition</th><th>Value / native unit</th><th>Observation / measurement period</th><th>Availability</th></tr></thead><tbody>'+rows+'</tbody></table></div><p>Composite scores, crisis predictions and return forecasts remain unqualified. Legacy changes lack independently verified comparison endpoints. Nonzero loans or swaps alone do not establish a crisis. These observations grant no Calls vote or position size.</p><p><a href="'+ENDPOINT+'">Complete legacy packet, including unqualified fields</a> · <a href="/liquidity.html#jh-liquidity-research">Original-bound net-liquidity calculation</a></p></section>';
  }
  function install(win){
    const doc=win.document;let packet=null;
    function render(){
      const v=view(packet);const panel=doc.getElementById('liquidity-pulse-panel');if(panel)panel.innerHTML=panelHTML(packet);
      if(!win.JUSTHODL_LIQ_NO_PILL){
        let pill=doc.querySelector('.jh-liq-pill');
        if(!pill){pill=doc.createElement('a');pill.className='jh-liq-pill';pill.href='/liquidity.html#pulse';doc.body.appendChild(pill);}
        pill.textContent='Liquidity observations · '+(v.status==='Reported context'?'research only':'unavailable / stale');
        pill.title='Dated reported values; no calibrated score or allocation authority';
      }
    }
    async function load(){
      try{const response=await win.fetch(ENDPOINT+'?t='+Date.now(),{cache:'no-store',credentials:'omit'});if(!response.ok)throw Error('Unavailable');packet=await response.json();}
      catch(e){packet=null;}render();
    }
    function init(){
      if(!doc.getElementById('jhLiqStyles')){
        const s=doc.createElement('style');s.id='jhLiqStyles';
        s.textContent='.jh-liq-pill{position:fixed;right:12px;bottom:100px;z-index:9997;padding:8px 12px;background:#101722;border:1px solid #506078;border-radius:12px;color:#cbd5e1;font:11px system-ui;text-decoration:none}.jh-liq-panel{padding:18px;border:1px solid #334155;border-radius:8px;background:#101722;color:#cbd5e1;font:13px/1.6 system-ui}.jh-liq-panel h3{margin:0 0 8px}.jh-liq-panel a{color:#93c5fd}.jh-liq-scroll{overflow-x:auto}.jh-liq-panel table{border-collapse:collapse;width:100%;min-width:680px}.jh-liq-panel th,.jh-liq-panel td{text-align:left;padding:10px;border-bottom:1px solid #334155;vertical-align:top}.jh-liq-panel small{display:block;color:#a6b6c9;font-size:11px;overflow-wrap:anywhere}.jh-liq-panel p{max-width:1100px}';doc.head.appendChild(s);
      }
      load();win.setInterval(load,300000);win.setInterval(render,60000);
    }
    if(doc.readyState==='loading')doc.addEventListener('DOMContentLoaded',init);else init();
  }
  return {view,format,panelHTML,install};
});
