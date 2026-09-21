/* Verified ETF context on the active Superchart tab. */
(function(w){
  'use strict';if(w.__jhChartEtfDesk)return;w.__jhChartEtfDesk=true;
  let pack=null,last='',attemptAt=0,epoch=0;const pending=new Map();
  function currentSym(){const on=document.querySelector('#tabs .tab.on[data-id]');return on?w.JHEtfFuse?.bare(on.getAttribute('data-id'))||'':'';}
  function hud(){let el=document.getElementById('etfhud');if(el)return el;const quote=document.getElementById('quote');
    if(!quote?.parentNode)return null;el=document.createElement('div');el.id='etfhud';quote.parentNode.insertBefore(el,quote.nextSibling);return el;}
  function paint(value,error){const el=hud(),A=w.JHEtfDeskResearch;if(!el || !A)return;
    if(error){el.className='on';el.textContent='ETF research unavailable; no raw-provider fallback.';return;}
    if(!value || value.sym!==currentSym())return;
    el.className='on';
    if(value.kind!=='etf'){el.innerHTML='<span>Outside the configured ETF desk.</span><a href="/flow-lookthrough.html">Inspect dated security memberships</a>';return;}
    const f=value.row.fund,p=value.row.packet;
    const html='<span class="pill">ETF RESEARCH</span><span>'+A.esc(value.sym)+'</span><span>Profile '+A.esc(f.profiles.current.effective_date)+
      '</span><span>Holdings '+A.esc(Object.keys(f.holdings.current.effective_dates||{}).join(', ')||'Unavailable')+'</span>'+[1,5,21].map(n=>
      '<span>'+n+' reported observations · USD <b>'+A.exact(A.windowValue(f,n))+'</b></span>').join('')+
      '<span>0 investment votes</span><a href="/etf.html?fund='+encodeURIComponent(value.sym)+'">Inspect sources</a>'+
      '<span>Flow overlays use effective dates retrospectively; they are not intraday prints or buy/sell signals.</span>';
    if(el.innerHTML!==html)el.innerHTML=html;
  }
  function install(value){pack=value;w.jhEtfPack=value;
    w.jhEtfFlowSeries=bars=>value?.kind==='etf' && value.sym===currentSym()?w.JHEtfFuse.alignHist(value.row.flow_hist,bars||w.lastBars||[]):[];
    w.jhEtfFlowMarks=()=>[];
  }
  async function load(sym){
    const F=w.JHEtfFuse;if(!F?.native || !sym)return null;
    if(pending.get(sym)?.epoch===epoch)return pending.get(sym).promise;
    const generation=epoch;
    const task=(async()=>{try{
      const row=await F.of(sym);if(row)row.flow_hist=await F.fullHist(sym,row.packet);
      if(generation!==epoch || sym!==currentSym())return null;
      if(row){row.flow_hist_n=row.flow_hist.length;row.flow_hist_from=row.flow_hist[0]?.d;row.flow_hist_to=row.flow_hist.at(-1)?.d;}
      const value={ok:!!row,kind:row?'etf':'none',sym,row,holders:[],demand:null,derived:null,risk:{},rank:null,at:Date.now(),retrospective:true};
      install(value);paint(value);return value;
    }catch(error){if(generation===epoch && sym===currentSym()){install(null);paint(null,true);}return null;}})();
    pending.set(sym,{epoch:generation,promise:task});try{return await task;}finally{if(pending.get(sym)?.promise===task)pending.delete(sym);}
  }
  w.jhEtfFlowReady=async()=>{const sym=currentSym();if(sym && (!pack || pack.sym!==sym))await load(sym);return [];};
  function tick(){const sym=currentSym();if(!sym){if(last){epoch++;last='';install(null);const el=hud();if(el)el.textContent='Select a chart tab to inspect ETF research.';}return;}
    if(sym!==last){epoch++;last=sym;attemptAt=Date.now();install(null);const el=hud();if(el)el.textContent='Verifying ETF research for '+sym+'…';void load(sym);}
    else if(Date.now()-attemptAt>180000){attemptAt=Date.now();void load(sym);}
    else if(pack)paint(pack);
  }
  function boot(){tick();w.setInterval(tick,800);}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot);else boot();
})(window);
