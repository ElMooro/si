/* jh-fund-chips.js — fleet fundamentals chip strip + plain-English read.
   Usage: include script; add data-jhf="TICKER" to any card element, or call
   JHF.chips(t)/JHF.readline(t) in page render after JHF.ready resolves. */
(function(){
  var S3='https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/';
  function gj(k){return fetch(S3+k+'?_='+Date.now()).then(function(r){return r.ok?r.json():null}).catch(function(){return null});}
  var D={};
  var ready=Promise.all([gj('data/share-flows.json'),gj('data/forensic-screen.json')]).then(function(a){
    D.sf=(a[0]||{}).tickers||{};
    D.fo={};((a[1]||{}).all_results||[]).forEach(function(r){if(r.symbol)D.fo[r.symbol]=r;});
    D.med=(a[1]||{}).sector_valuation_medians||{};
    annotate();return D;});
  function fmtMc(v){return v>=1e12?(v/1e12).toFixed(2)+'T':v>=1e9?(v/1e9).toFixed(1)+'B':(v/1e6).toFixed(0)+'M';}
  function chips(t){
    var f=D.sf[t]||{},o=D.fo[t]||{},b=[];
    if(f.market_cap)b.push('MCAP '+fmtMc(f.market_cap));
    if(f.pe_ttm!=null)b.push('PE '+(+f.pe_ttm).toFixed(1));
    if(f.peg!=null)b.push('PEG '+(+f.peg).toFixed(1));
    if(f.ps_ttm!=null)b.push('P/S '+(+f.ps_ttm).toFixed(1));
    if(f.fcf_yield_pct!=null)b.push('FCF '+(+f.fcf_yield_pct).toFixed(1)+'%');
    if(f.sh_yoy_pct!=null)b.push('SHARES '+(f.sh_yoy_pct>0?'+':'')+f.sh_yoy_pct+'%');
    if(f.buyback_net_yield_pct)b.push('NET BB '+f.buyback_net_yield_pct+'%');
    if(o.strength_grade)b.push('FIN '+o.strength_grade);
    if(o.concern_score!=null&&o.concern_score>=40)b.push('⚑ CONCERN '+o.concern_score);
    if(!b.length)return'';
    return '<div class="jhf-chips" style="font-size:10.5px;color:#8a93a6;font-family:ui-monospace,monospace;margin-top:4px;letter-spacing:.2px">'+b.join(' · ')+'</div>';
  }
  function readline(t){
    var f=D.sf[t]||{},o=D.fo[t]||{},p=[];
    if(o.strength_grade){var g=o.strength_grade[0];
      p.push(g==='A'?'fortress financials ('+o.strength_grade+')':g==='B'?'solid financials ('+o.strength_grade+')':(o.strength_grade==='D'||o.strength_grade==='F')?'weak financials ('+o.strength_grade+')':'middling financials ('+o.strength_grade+')');}
    if(o.industry_pctile!=null)p.push(o.industry_pctile+'th %ile in its sector');
    var m=D.med[o.sector]||{};
    if(o.pe_ttm!=null&&m.pe_ttm!=null)p.push('P/E '+(o.pe_ttm<m.pe_ttm?'below':'above')+' sector median ('+(+o.pe_ttm).toFixed(0)+' vs '+(+m.pe_ttm).toFixed(0)+')');
    if(f.read==='EXTREME_DILUTION')p.push('☠️ death-spiral dilution');
    else if(f.read&&f.read.indexOf('DILUT')>=0)p.push('diluting '+(f.sh_yoy_pct>0?'+':'')+f.sh_yoy_pct+'%/yr');
    else if(f.read==='SHRINKING'||f.read==='BUYBACK_HEAVY')p.push('shrinking share count ('+f.sh_yoy_pct+'%/yr)');
    if(o.m_flag)p.push('⚑ Beneish manipulation-pattern flag');
    if(o.m_deteriorating)p.push('⚑ earnings quality deteriorating q/q');
    if(!p.length)return'';
    return '<div class="jhf-read" style="font-size:11.5px;color:#a8b3c7;margin-top:4px;line-height:1.45">'+p.join(' · ')+' · <a href="/why.html?ticker='+t+'" style="color:#00d4ff">full research ↗</a></div>';
  }
  function annotate(root){
    (root||document).querySelectorAll('[data-jhf]').forEach(function(el){
      if(el.__jhf)return; el.__jhf=1;
      var t=(el.getAttribute('data-jhf')||'').toUpperCase(); if(!t)return;
      var h=chips(t)+(el.hasAttribute('data-jhf-read')?readline(t):'');
      if(h)el.insertAdjacentHTML('beforeend',h);});
  }
  new MutationObserver(function(){if(D.sf)annotate();}).observe(document.documentElement,{childList:true,subtree:true});
  window.JHF={ready:ready,get:function(t){return{sf:D.sf[t],fo:D.fo[t]}},chips:chips,readline:readline,annotate:annotate};
})();
