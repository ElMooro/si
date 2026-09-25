/* reskin-skip: Explicit FRED units and periods for original-bound and legacy observations.
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
    if(data?.contract)return nativeView(data,now);
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
    if(v.native)return nativePanel(v);
    const rows=v.rows.map(r=>'<tr><td>'+(r.source?'<a href="'+r.source+'">'+esc(r.sid)+'</a>':esc(r.sid))+'<br>'+esc(r.label)+'</td><td>'+esc(r.value)+'<small>'+esc(r.reported)+' '+esc(r.nativeUnit)+' as reported</small></td><td>'+esc(r.observation)+'<small>'+esc(r.basis)+'</small></td><td>'+esc(r.status)+(r.age!==null?'<small>'+r.age+' calendar days from period date</small>':'')+'</td></tr>').join('');
    return '<section class="jh-liq-panel"><h3>Liquidity and credit: reported observations</h3><p>'+esc(v.status)+(v.generated?' · packet generated '+esc(v.generated):'')+'</p><p>Source units and observation periods are explicit. These legacy packet values have not been replayed against retained originals. Display age limits are 21 days for weekly series, 10 for daily and 75 for monthly; they do not establish release-calendar freshness.</p><div class="jh-liq-scroll" tabindex="0" role="region" aria-label="Reported liquidity observations"><table><thead><tr><th>Series / definition</th><th>Value / native unit</th><th>Observation / measurement period</th><th>Availability</th></tr></thead><tbody>'+rows+'</tbody></table></div><p>Composite scores, crisis predictions and return forecasts remain unqualified. Legacy changes lack independently verified comparison endpoints. Nonzero loans or swaps alone do not establish a crisis. These observations grant no Calls vote or position size.</p><p><a href="'+ENDPOINT+'">Complete legacy packet, including unqualified fields</a> · <a href="/liquidity.html#jh-liquidity-research">Original-bound net-liquidity calculation</a></p></section>';
  }
  function install(win){
    const doc=win.document;let packet=null,lastMarkup=null,loadEpoch=0;
    function render(){
      const v=view(packet);const panel=doc.getElementById('liquidity-pulse-panel'),markup=panelHTML(packet);
      if(panel&&markup!==lastMarkup){panel.innerHTML=markup;lastMarkup=markup;}
      if(!win.JUSTHODL_LIQ_NO_PILL){
        let pill=doc.querySelector('.jh-liq-pill');
        if(!pill){pill=doc.createElement('a');pill.className='jh-liq-pill';pill.href='/liquidity.html#pulse';doc.body.appendChild(pill);}
        pill.textContent='Liquidity observations · '+(v.status==='Reported context'||v.current?'research only':'unavailable / stale');
        pill.title='Dated reported values; no calibrated score or allocation authority';
      }
    }
    async function load(){
      const epoch=++loadEpoch;let next=null;
      try{const response=await win.fetch(ENDPOINT+'?t='+Date.now(),{cache:'no-store',credentials:'omit'});if(!response.ok)throw Error('Unavailable');next=await response.json();}
      catch(e){next=null;}if(epoch!==loadEpoch)return;packet=next;render();
    }
    function init(){
      if(!doc.getElementById('jhLiqStyles')){
        const s=doc.createElement('style');s.id='jhLiqStyles';
        s.textContent='.jh-liq-pill{position:fixed;right:12px;bottom:100px;z-index:9997;padding:8px 12px;background:#101722;border:1px solid #506078;border-radius:12px;color:#cbd5e1;font:11px system-ui;text-decoration:none}.jh-liq-panel{padding:18px;border:1px solid #334155;border-radius:8px;background:#101722;color:#cbd5e1;font:13px/1.6 system-ui}.jh-liq-panel h3{margin:0 0 8px}.jh-liq-panel a{color:#93c5fd}.jh-liq-scroll{overflow-x:auto}.jh-liq-panel table{border-collapse:collapse;width:100%;min-width:680px}.jh-liq-panel th,.jh-liq-panel td{text-align:left;padding:10px;border-bottom:1px solid #334155;vertical-align:top}.jh-liq-panel small{display:block;color:#a6b6c9;font-size:11px;overflow-wrap:anywhere}.jh-liq-panel p{max-width:1100px}';doc.head.appendChild(s);
      }
      const panel=doc.getElementById('liquidity-pulse-panel');
      panel?.addEventListener?.('click',async event=>{
        const button=event.target.closest('button[data-pulse-verify]'),sid=button?.dataset.pulseVerify;
        if(!Object.hasOwn(SPECS,sid)||packet?.contract!=='liquidity-pulse-research.v1')return;
        const result=panel.querySelector('[data-pulse-result="'+sid+'"]');if(!result)return;
        button.disabled=true;result.textContent='Checking retained originals…';
        try{result.textContent=await verifyOriginal(packet.series[sid],win.fetch.bind(win),win.crypto);}
        catch(_){result.textContent='Not verified: retained bytes, source definition or observation row did not match.';}
        finally{button.disabled=false;}
      });
      load();win.setInterval(load,300000);win.setInterval(render,60000);
    }
    if(doc.readyState==='loading')doc.addEventListener('DOMContentLoaded',init);else init();
  }
  const aware=s=>typeof s==='string'&&/T.*(?:Z|[+-]\d{2}:\d{2})$/.test(s)&&Number.isFinite(Date.parse(s))?Date.parse(s):NaN;
  const recent=(s,now)=>Number.isFinite(aware(s))&&aware(s)<=now&&now-aware(s)<=26*3600000;
  const decimal=s=>typeof s==='string'&&/^-?\d+(?:\.\d+)?$/.test(s)&&Number.isFinite(Number(s))?Number(s):null;
  function evidence(row,sid){
    const e=row?.evidence;if(!e||Object.keys(e).sort().join(',')!=='definition,observations')return null;
    for(const [kind,ref] of Object.entries(e)){
      if(ref?.contract!=='source-evidence.v1'||ref.captured!==true||ref.provider!=='fred'||
        !/^data\/evidence\/fred\/[a-f0-9]{64}\/[a-f0-9]{64}\.bin\.gz$/.test(ref.key)||
        !/^[a-f0-9]{64}$/.test(ref.sha256)||!ref.key.endsWith('/'+ref.sha256+'.bin.gz'))return null;
      try{const u=new URL(ref.source_url);if(u.protocol!=='https:'||u.hostname!=='api.stlouisfed.org'||
        u.pathname!=='/fred/series'+(kind==='observations'?'/observations':'')||u.searchParams.get('series_id')!==sid||
        [...u.searchParams.keys()].some(k=>/key|token|secret/i.test(k)))return null;}catch(_){return null;}
    }return e;
  }
  function exactDecimal(value){
    if(String(value).length>512)throw Error('Original decimal exceeds bound');
    const m=String(value).trim().match(/^([+-]?)(\d*)\.?([0-9]*)(?:[eE]([+-]?\d+))?$/);
    if(!m||!(m[2]+m[3]).length)throw Error('Invalid original decimal');
    const exponent=Number(m[4]||0);if(!Number.isInteger(exponent)||Math.abs(exponent)>500)throw Error('Original decimal exceeds bound');
    let digits=(m[2]+m[3]).replace(/^0+/,'')||'0',scale=m[3].length-exponent;
    if(digits==='0')return '0';while(digits.endsWith('0')){digits=digits.slice(0,-1);scale--;}
    return (m[1]==='-'?'-':'')+digits+'e'+(-scale);
  }
  async function verifyOriginal(row,fetcher=globalThis.fetch,crypto=globalThis.crypto){
    const ev=evidence(row,row?.series_id),limit=8*1024*1024;
    if(!ev||!Object.hasOwn(SPECS,row.series_id)||!Number.isInteger(row.source_row)||row.source_row<0||!Number.isFinite(date(row.latest_date)))throw Error('Original source coordinates unavailable');
    async function read(ref){
      if(!Number.isInteger(ref.bytes)||ref.bytes<=0||ref.bytes>limit)throw Error('Original response byte bound');
      const response=await fetcher('/'+ref.key+'?exact=1',{cache:'no-store',credentials:'omit'});if(!response.ok)throw Error('Original unavailable');
      let bytes=new Uint8Array(await response.arrayBuffer());if(bytes.length>limit)throw Error('Original exceeds bound');
      if(bytes[0]===31&&bytes[1]===139){
        const reader=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')).getReader(),chunks=[];let total=0;
        while(true){const {done,value}=await reader.read();if(done)break;total+=value.length;if(total>limit){await reader.cancel();throw Error('Expanded original exceeds bound');}chunks.push(value);}
        bytes=new Uint8Array(total);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
      }
      const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
      if(digest!==ref.sha256||bytes.length!==ref.bytes)throw Error('Original bytes differ');
      return JSON.parse(new TextDecoder().decode(bytes));
    }
    const [definition,observations]=await Promise.all([read(ev.definition),read(ev.observations)]);
    const meta=definition.seriess?.length===1?definition.seriess[0]:null,point=observations.observations?.[row.source_row];
    if(!meta||['id','units','frequency','frequency_short','seasonal_adjustment','title'].some(k=>meta[k]!==row.source_definition?.[k])||meta.id!==row.series_id||meta.units!==row.unit||meta.frequency_short!==row.frequency||meta.seasonal_adjustment!==row.seasonal_adjustment)throw Error('Original definition differs');
    if(!point||point.date!==row.latest_date)throw Error('Original observation identity differs');
    if(row.last_observed_value==null){if(!['.', '', null].includes(point.value))throw Error('Original missingness differs');}
    else if(exactDecimal(point.value)!==exactDecimal(row.last_observed_value))throw Error('Original value differs');
    return 'Verified original definition and observation response hashes; row '+row.source_row+' on '+point.date+'. Source evidence only, not forecast validation.';
  }
  function nativeView(data,now=Date.now()){
    const manifest=data?.replay?.manifest_key;
    const contract=data?.contract==='liquidity-pulse-research.v1'&&
      ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified'].every(k=>data[k]===false)&&
      data.call===null&&data.decision?.verb==='WAIT'&&data.decision?.meaning==='abstain'&&
      /^data\/liquidity-pulse-research\/runs\/[a-f0-9]{64}\.json$/.test(manifest)&&/^[a-f0-9]{64}$/.test(data.replay?.output_sha256);
    const current=contract&&recent(data.generated_at,now)&&recent(data.source_generated_at,now)&&aware(data.source_generated_at)<=aware(data.generated_at);
    const rows=Object.entries(SPECS).map(([sid,spec])=>{
      const r=data.series?.[sid]||{},ev=evidence(r,sid),observed=date(r.latest_date);
      const frequency=sid==='HQMCB10YR'?'M':sid.startsWith('BAML')?'D':'W',limit=frequency==='M'?100:spec[3];
      const age=Number.isFinite(observed)?Math.floor((now-observed)/DAY):null;
      const nativeUnit=spec[1]==='usd_mn'?'Millions of U.S. Dollars':'Percent';
      const basis=sid==='HQMCB10YR'?'Monthly':sid==='BAMLEMHBHYCRPIOAS'?'Daily':sid.startsWith('BAML')?'Daily, Close':
        ['WRESBAL','WTREGEN'].includes(sid)?'Weekly, Ending Wednesday':'Weekly, As of Wednesday';
      const identity=contract&&ev&&r.series_id===sid&&r.unit===nativeUnit&&r.frequency===frequency&&
        r.seasonal_adjustment==='Not Seasonally Adjusted'&&r.calls_eligible===false&&r.sizing_eligible===false&&
        r.source_definition?.id===sid&&r.source_definition?.units===nativeUnit&&r.source_definition?.frequency_short===frequency&&r.source_definition?.frequency===basis&&r.source_definition?.seasonal_adjustment==='Not Seasonally Adjusted'&&
        Number.isFinite(observed)&&observed<=aware(data.source_generated_at)&&(!sid.startsWith('HQMC')||r.latest_date.endsWith('-01'));
      const eligible=identity&&current&&recent(r.acquired_at,now)&&aware(r.acquired_at)<=aware(data.source_generated_at)&&
        age<=limit&&r.quality?.status==='fresh'&&finite(r.latest_value)&&decimal(r.latest_value_decimal)===r.latest_value;
      const historical=identity?decimal(r.last_observed_value):null;
      return {sid,label:spec[0],basis:spec[2],status:eligible?'Within source age limits':identity?'Historical / unavailable current reading':'Unavailable: source identity',
        value:eligible?format(r.latest_value,spec[1]):'Unavailable',reported:historical===null?'Unavailable':String(r.last_observed_value),
        nativeUnit,rawDate:identity?r.latest_date:null,observation:identity?(frequency==='M'?r.latest_date.slice(0,7)+' (monthly period)':r.latest_date):'Unavailable',
        source:'https://fred.stlouisfed.org/series/'+sid,acquired:identity?r.acquired_at:null,
        evidence:identity?ev:null,comparisons:identity?r.historical_calendar_comparisons:null,eligible:!!eligible};
    });
    return {native:true,current:rows.every(r=>r.eligible),status:contract?'Original-linked research':'Unavailable: publication contract',
      generated:contract?data.generated_at:null,rows,manifest:contract?manifest:null};
  }
  function nativePanel(v){
    const rows=v.rows.map(r=>{
      const links=r.evidence?'<p><a href="/'+r.evidence.definition.key+'?exact=1">Retained definition</a> · <a href="/'+r.evidence.observations.key+'?exact=1">Complete original observations</a></p><button type="button" data-pulse-verify="'+r.sid+'">Verify original definition and row</button><p role="status" data-pulse-result="'+r.sid+'"></p>':'';
      const comparisons=r.comparisons?'<details><summary>Dated comparisons and original evidence</summary><p>Current retained vintage; historical comparison does not establish publication-time knowledge or a return forecast.</p><table><thead><tr><th>Window</th><th>Target date</th><th>Actual baseline</th><th>Comparison endpoint</th><th>Absolute change / unit</th></tr></thead><tbody>'+['week','month','quarter','year'].map(k=>{
        const c=r.comparisons[k]||{},n=decimal(c.change_decimal),valid=n!==null&&Number.isFinite(date(c.baseline_date))&&Number.isFinite(date(c.current_date))&&c.current_date===r.rawDate&&c.current_date>c.baseline_date&&c.source_unit===r.nativeUnit&&c.change_unit===(r.nativeUnit==='Percent'?'percentage_points':r.nativeUnit);
        return '<tr><td>'+k+'</td><td>'+esc(c.target_date??'Unavailable')+'</td><td>'+esc(c.baseline_date??'Unavailable')+'</td><td>'+esc(c.current_date??'Unavailable')+'</td><td>'+esc(valid?c.change_decimal+' '+c.change_unit:'Unavailable')+'</td></tr>';
      }).join('')+'</tbody></table>'+links+'</details>':links;
      return '<tr><td><a href="'+r.source+'">'+r.sid+'</a><br>'+esc(r.label)+'</td><td>'+esc(r.value)+'<small>Retained observation: '+esc(r.reported)+' '+esc(r.nativeUnit)+'</small></td><td>'+esc(r.observation)+'<small>'+esc(r.basis)+'</small><small>Source acquired '+esc(r.acquired??'unavailable')+'</small></td><td>'+esc(r.status)+comparisons+'</td></tr>';
    }).join('');
    return '<section class="jh-liq-panel"><h3>Liquidity and credit: original-linked observations</h3><p>'+esc(v.status)+(v.generated?' · generated '+esc(v.generated):'')+'</p><p>Weekly monetary values use declared millions, displayed as billions. Credit spreads and monthly yields retain their percent units; changes use percentage points. Observation, source acquisition and generation are separate clocks. Age ceilings are 21 days weekly, 10 daily and 100 monthly, plus 26 hours since acquisition; exact release calendars remain unverified.</p><div class="jh-liq-scroll" tabindex="0" role="region" aria-label="Original-linked liquidity observations"><table><thead><tr><th>Series / definition</th><th>Current / retained observation</th><th>Observation / acquisition</th><th>Availability / evidence</th></tr></thead><tbody>'+rows+'</tbody></table></div><p>Facility balances alone do not establish a crisis or easing. These overlapping FRED sources grant no independent Calls vote, position size or validated forecast.</p>'+(v.manifest?'<p><a href="/'+v.manifest+'?exact=1">Recorded source and calculation run</a> · <a href="'+ENDPOINT+'">Complete packet and original-row histories</a></p>':'')+'</section>';
  }
  return {view,format,panelHTML,install,nativeView,evidence,verifyOriginal,exactDecimal};
});
