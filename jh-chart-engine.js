/* JustHodl Chart engine v4 — TradingView desk. Does not touch Chart Pro. */
(function () {
  if (window.__jhChartEngineV4) return;
  window.__jhChartEngineV4 = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var TFS = [["1s","1s","1m","1d"],["1m","1m","1m","5d"],["1h","1h","60m","2y"],["4h","4h","60m","2y"],["12h","12h","60m","2y"],["1d","D","1d","5y"],["2d","2D","1d","5y"],["5d","5D","1d","5y"],["1w","W","1wk","10y"],["2w","2W","1wk","10y"],["1M","M","1mo","10y"],["3M","3M","1d","10y"]];
  var CHG = [["price","Price"],["dod","DoD"],["wow","WoW"],["mom","MoM"],["qoq","QoQ"],["yoy","YoY"],["ytd","YTD"],["fromhigh","From High"],["fromlow","From Low"],["vsspy","vs SPY"]];
  var BARS = { dod:1, wow:5, mom:21, qoq:63, yoy:252 };
  var TABS = ["BTCUSDT","ETHB","PEPEUSDT","CNEQ","PURR","BMNR","ATO"];
  var ISHARES = ["GSG","COMT","EWZS","CMDY","EWZ","LOCK","IAT","IVV","IWM","EEM","LQD","HYG","TLT","IEI"];
  var KINDS = [["candles","Candles"],["hollow","Hollow"],["bars","Bars"],["line","Line"],["area","Area"],["baseline","Baseline"],["heikin","Heikin"],["columns","Columns"]];
  var SCALES = [["0","Linear"],["1","Log"],["2","Percent"],["3","Index"]];
  var TOOLS = [["cursor","+","Cursor"],["trend","/","Trend Alt+T"],["ray","↗","Ray"],["hline","—","Horiz Alt+H"],["vline","|","Vert Alt+V"],["fib","fib","Fib Alt+F"],["rect","▢","Rect"],["channel","//","Channel"],["measure","Δ","Measure"],["text","T","Text"],["brush","~","Brush"]];
  var INDS = [
    {id:"sma20",n:"SMA 20",c:"#26c6da",on:1,k:"sma",p:20},
    {id:"sma50",n:"SMA 50",c:"#2962ff",on:1,k:"sma",p:50},
    {id:"sma200",n:"SMA 200",c:"#ff6d00",on:1,k:"sma",p:200},
    {id:"sma250",n:"SMA 250",c:"#e91e63",on:1,k:"sma",p:250},
    {id:"ema250",n:"EMA 250",c:"#089981",on:1,k:"ema",p:250},
    {id:"vwma20",n:"VWMA 20",c:"#42a5f5",on:0,k:"vwma",p:20},
    {id:"vwap",n:"VWAP",c:"#ab47bc",on:0,k:"vwap"},
    {id:"bb",n:"BB 20",c:"#ab47bc",on:0,k:"bb"},
    {id:"st",n:"Supertrend",c:"#26a69a",on:0,k:"st"},
    {id:"ich",n:"Ichimoku",c:"#5c6bc0",on:0,k:"ich"}
  ];
  var OSC = [
    {id:"rsi",n:"RSI 14",on:0},
    {id:"macd",n:"MACD 12,26,9",on:0},
    {id:"stoch",n:"Stoch 14,3,3",on:0},
    {id:"atr",n:"ATR 14",on:0}
  ];
  var UP="#089981", DN="#f23645", BG="#ffffff", ACC="#2962ff";
  var CUSTOM_KEY="jh-chart-custom-lists", LAY_KEY="jh-chart-v4-layout", ALERT_KEY="jh-chart-alerts", DRAW_KEY="jh-chart-drawings";
  var active="PEPEUSDT", tf="1d", mode="price", kind="candles", scaleMode=0, logScale=false;
  var quotes={}, lists=[], listId="ishares", letter="", filter="", sortCol="sym", sortDir=1;
  var lastBars=[], series=[], spyBars=null, barCache={}, compare=[], mainSeries=null;
  var drawings=[], undo=[], redo=[], tool="cursor", magnet=true, pending=null, objOpen=false, vpOn=false;
  var wtab="list", watchOpen=true, layout=1, gridOn=true, watermark=true;
  var replay={on:false,i:0,speed:1,timer:null,full:[]};
  var alerts=[], news=[], toastT=null;
  var chart, chart2, oscChart, oscSeries=[];

  function bare(s){ s=String(s||""); return s.indexOf(":")>=0?s.split(":").pop():s; }
  function yahooSym(s){
    s=bare(s).toUpperCase();
    if(/USDT$/.test(s)) return s.slice(0,-4)+"-USD";
    if(/BUSD$/.test(s)) return s.slice(0,-4)+"-USD";
    if(/USDC$/.test(s)) return s.slice(0,-4)+"-USD";
    return s;
  }
  function fmt(p){
    if(p==null||!isFinite(p)) return "—";
    var a=Math.abs(p);
    if(a>=1000) return p.toLocaleString(undefined,{maximumFractionDigits:2});
    if(a>=1) return p.toFixed(2);
    if(a>=0.01) return p.toFixed(4);
    return Number(p).toPrecision(6).replace(/0+$/,"").replace(/\.$/,"");
  }
  function toast(m){
    var el=document.getElementById("toast");
    el.textContent=m; el.className="on";
    clearTimeout(toastT); toastT=setTimeout(function(){ el.className=""; }, 2800);
  }
  function uid(){ return "d"+Math.random().toString(36).slice(2,8); }
  function spec(tfId){ for(var i=0;i<TFS.length;i++) if(TFS[i][0]===tfId) return TFS[i]; return TFS[5]; }
  function loadJSON(k, fb){ try{ var x=JSON.parse(localStorage.getItem(k)||""); return x||fb; }catch(e){ return fb; } }
  function saveJSON(k,v){ try{ localStorage.setItem(k, JSON.stringify(v)); }catch(e){} }

  function sma(d,n){ var o=[],s=0; for(var i=0;i<d.length;i++){ s+=d[i].close; if(i>=n)s-=d[i-n].close; if(i>=n-1)o.push({time:d[i].time,value:s/n}); } return o; }
  function ema(d,n){ if(!d.length)return[]; var o=[],k=2/(n+1),p=d[0].close; for(var i=0;i<d.length;i++){ p=d[i].close*k+p*(1-k); if(i>=n-1)o.push({time:d[i].time,value:p}); } return o; }
  function vwma(d,n){ var o=[]; for(var i=0;i<d.length;i++){ if(i+1<n)continue; var pv=0,vv=0,j; for(j=i-n+1;j<=i;j++){ pv+=d[j].close*d[j].volume; vv+=d[j].volume; } if(vv)o.push({time:d[i].time,value:pv/vv}); } return o; }
  function vwap(d){ var o=[],pv=0,vv=0; for(var i=0;i<d.length;i++){ var tp=(d[i].high+d[i].low+d[i].close)/3; pv+=tp*d[i].volume; vv+=d[i].volume; if(vv)o.push({time:d[i].time,value:pv/vv}); } return o; }
  function atr(d,n){ var o=[],tr=[],i; for(i=0;i<d.length;i++){ var prev=i?d[i-1].close:d[i].close; tr.push(Math.max(d[i].high-d[i].low, Math.abs(d[i].high-prev), Math.abs(d[i].low-prev))); } var s=0; for(i=0;i<tr.length;i++){ s+=tr[i]; if(i>=n)s-=tr[i-n]; if(i>=n-1)o.push({time:d[i].time,value:s/n}); } return o; }
  function rsi(d,n){ var o=[],g=0,l=0,i; for(i=1;i<d.length;i++){ var ch=d[i].close-d[i-1].close; var gv=Math.max(ch,0), lv=Math.max(-ch,0); if(i<=n){ g+=gv; l+=lv; if(i===n){ g/=n; l/=n; o.push({time:d[i].time,value:l?100-100/(1+g/l):100}); } } else { g=(g*(n-1)+gv)/n; l=(l*(n-1)+lv)/n; o.push({time:d[i].time,value:l?100-100/(1+g/l):100}); } } return o; }
  function macd(d){ var e12=ema(d,12), e26=ema(d,26), m=[], map={},i; for(i=0;i<e26.length;i++) map[e26[i].time]=e26[i].value; for(i=0;i<e12.length;i++){ if(map[e12[i].time]!=null) m.push({time:e12[i].time,value:e12[i].value-map[e12[i].time]}); } var sig=ema(m.map(function(p){return {time:p.time,close:p.value};}),9), sm={}; for(i=0;i<sig.length;i++) sm[sig[i].time]=sig[i].value; var hist=[]; for(i=0;i<m.length;i++) if(sm[m[i].time]!=null) hist.push({time:m[i].time, macd:m[i].value, signal:sm[m[i].time], hist:m[i].value-sm[m[i].time]}); return hist; }
  function stoch(d,n,k,dper){ k=k||3; dper=dper||3; var kv=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n) continue; var hi=-1e99,lo=1e99; for(j=i-n+1;j<=i;j++){ if(d[j].high>hi)hi=d[j].high; if(d[j].low<lo)lo=d[j].low; } kv.push({time:d[i].time,value:hi===lo?50:((d[i].close-lo)/(hi-lo))*100}); } var ksm=sma(kv.map(function(p){return {time:p.time,close:p.value};}),k); return ksm; }
  function heikin(d){ var o=[],i; for(i=0;i<d.length;i++){ var b=d[i], hc=(b.open+b.high+b.low+b.close)/4, ho=i? (o[i-1].open+o[i-1].close)/2 : (b.open+b.close)/2; o.push({time:b.time,open:ho,high:Math.max(b.high,ho,hc),low:Math.min(b.low,ho,hc),close:hc,volume:b.volume}); } return o; }
  function supertrend(d,n,m){ n=n||10; m=m||3; var a=atr(d,n), o=[],i,map={}; for(i=0;i<a.length;i++) map[a[i].time]=a[i].value; var up=0,dn=0,dir=1; for(i=0;i<d.length;i++){ var at=map[d[i].time]; if(at==null) continue; var mid=(d[i].high+d[i].low)/2; var bu=mid+m*at, bd=mid-m*at; if(i){ if(bd>up) up=bd; else if(d[i-1].close<up) up=bd; if(bu<dn) dn=bu; else if(d[i-1].close>dn) dn=bu; } else { up=bd; dn=bu; } if(d[i].close>dn) dir=1; else if(d[i].close<up) dir=-1; o.push({time:d[i].time,value:dir>0?up:dn, color:dir>0?UP:DN}); } return o; }
  function ichimoku(d){ function mid(len,i){ var hi=-1e99,lo=1e99,j; for(j=Math.max(0,i-len+1);j<=i;j++){ if(d[j].high>hi)hi=d[j].high; if(d[j].low<lo)lo=d[j].low; } return (hi+lo)/2; } var conv=[],base=[],spanA=[],spanB=[],i; for(i=0;i<d.length;i++){ var c=mid(9,i), b=mid(26,i); conv.push({time:d[i].time,value:c}); base.push({time:d[i].time,value:b}); spanA.push({time:d[i].time,value:(c+b)/2}); spanB.push({time:d[i].time,value:mid(52,i)}); } return {conv:conv,base:base,spanA:spanA,spanB:spanB}; }
  function uniq(rows){ var out=[], last=-1; for(var i=0;i<rows.length;i++){ var t=rows[i].time; if(!t||t<=last||!isFinite(rows[i].close)) continue; out.push(rows[i]); last=t; } return out; }
  function toBars(j){
    if(!j) return [];
    var rows=j.bars||j.ohlc||j.results||j.obs||j.points||j.data||(Array.isArray(j)?j:[]);
    var out=[],i;
    for(i=0;i<rows.length;i++){
      var b=rows[i];
      if(Array.isArray(b)){
        var t=+b[0]; if(t>1e12) t=Math.floor(t/1000);
        var c=b[4]!=null?+b[4]:+b[1]; if(!isFinite(c)) continue;
        out.push({time:t,open:+(b[1]||c),high:+(b[2]||c),low:+(b[3]||c),close:c,volume:+(b[5]||0)});
      } else {
        var tm=b.time||b.t||b.date, c2=b.close!=null?b.close:(b.c!=null?b.c:b.value);
        if(c2==null) continue;
        if(typeof tm==="string") tm=Math.floor(Date.parse(tm.length<=10?tm+"T00:00:00Z":tm)/1000);
        if(tm>1e12) tm=Math.floor(tm/1000);
        var c3=+c2; if(!isFinite(c3)) continue;
        out.push({time:+tm,open:+(b.open||b.o||c3),high:+(b.high||b.h||c3),low:+(b.low||b.l||c3),close:c3,volume:+(b.volume||b.v||b.value||0)});
      }
    }
    if(!out.length && j.chart && j.chart.result && j.chart.result[0]){
      var res=j.chart.result[0], q=(res.indicators.quote||[])[0]||{}, ts=res.timestamp||[];
      for(i=0;i<ts.length;i++){ if(q.close[i]==null||!isFinite(+q.close[i])) continue; out.push({time:ts[i],open:+(q.open[i]||q.close[i]),high:+(q.high[i]||q.close[i]),low:+(q.low[i]||q.close[i]),close:+q.close[i],volume:+(q.volume[i]||0)}); }
    }
    if(!out.length && Array.isArray(j.timestamp) && Array.isArray(j.close)){
      for(i=0;i<j.timestamp.length;i++){ if(j.close[i]==null||!isFinite(+j.close[i])) continue; out.push({time:j.timestamp[i],open:+((j.open&&j.open[i])||j.close[i]),high:+((j.high&&j.high[i])||j.close[i]),low:+((j.low&&j.low[i])||j.close[i]),close:+j.close[i],volume:+((j.volume&&j.volume[i])||0)}); }
    }
    return uniq(out);
  }
  async function fetchJson(url){ var r=await fetch(url,{cache:"no-store"}); if(!r.ok) throw new Error(String(r.status)); return r.json(); }
  async function klines(sym, tfId){
    var t=bare(sym), sp=spec(tfId), ys=yahooSym(t);
    var key=t+"|"+tfId;
    if(barCache[key] && barCache[key].length>=8) return barCache[key];
    var urls=[
      PROXY+"/yf-ohlc?symbol="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],
      PROXY+"/yf-ohlc?symbol="+encodeURIComponent(t)+"&range="+sp[3]+"&interval="+sp[2],
      PROXY+"/ohlc?ticker="+encodeURIComponent(t),
      "/data/series/"+encodeURIComponent(ys)+".json"
    ];
    for(var i=0;i<urls.length;i++){
      try{ var d=toBars(await fetchJson(urls[i])); if(d.length>=8){ barCache[key]=d; return d; } }catch(e){}
    }
    return [];
  }
  function computeChange(d,m){
    if(m==="price"||!d.length) return null;
    var closes=d.map(function(b){ return {time:b.time,value:b.close}; });
    if(m==="ytd"){ var yr=new Date().getUTCFullYear(), base=null, out=[]; for(var i=0;i<closes.length;i++){ var y=new Date(closes[i].time*1000).getUTCFullYear(); if(y===yr&&base==null) base=closes[i].value; if(base) out.push({time:closes[i].time,value:(closes[i].value/base-1)*100}); } return out; }
    if(m==="fromhigh"||m==="fromlow"){ var out2=[]; for(var i=0;i<closes.length;i++){ var ext=m==="fromhigh"?-1e99:1e99, t0=closes[i].time-365*86400; for(var j=0;j<=i;j++){ if(closes[j].time<t0) continue; var v=closes[j].value; if(m==="fromhigh"){ if(v>ext)ext=v; } else if(v<ext) ext=v; } if(ext&&isFinite(ext)) out2.push({time:closes[i].time,value:(closes[i].value/ext-1)*100}); } return out2; }
    var n=BARS[m]||1, out3=[]; for(var i=n;i<closes.length;i++){ var then=closes[i-n].value; if(!then) continue; out3.push({time:closes[i].time,value:(closes[i].value/then-1)*100}); } return out3;
  }
  async function vsSpy(d){
    if(!spyBars) spyBars=await klines("SPY","1d");
    var spy=spyBars||[]; if(spy.length<2) return [];
    var j=0, joined=[]; for(var i=0;i<d.length;i++){ var ts=d[i].time; while(j+1<spy.length && Math.abs(spy[j+1].time-ts)<=Math.abs(spy[j].time-ts)) j++; if(Math.abs(spy[j].time-ts)>7*86400) continue; joined.push({time:ts,value:d[i].close,spy:spy[j].close}); }
    if(joined.length<2) return []; var t0=joined[0].value,s0=joined[0].spy; return joined.map(function(p){ return {time:p.time,value:((p.value/t0)/(p.spy/s0)-1)*100}; });
  }

  var LW=window.LightweightCharts, host=document.getElementById("host");
  if(!LW||!host){ var qel=document.getElementById("quote"); if(qel) qel.textContent="Chart library missing"; return; }
  function mkChart(el){
    return LW.createChart(el,{
      autoSize:true,
      layout:{ background:{type:"solid",color:BG}, textColor:"#6a6d78", fontFamily:"IBM Plex Sans,sans-serif", fontSize:11 },
      grid:{ vertLines:{ color: gridOn?"#f0f3fa":"transparent" }, horzLines:{ color: gridOn?"#f0f3fa":"transparent" } },
      rightPriceScale:{ borderColor:"#e0e3eb", scaleMargins:{ top:0.06, bottom:0.18 } },
      timeScale:{ borderColor:"#e0e3eb", timeVisible:true, rightOffset:6 },
      crosshair:{ mode:0 },
      localization:{ priceFormatter:function(p){ return mode==="price"?fmt(p):p.toFixed(2)+"%"; } }
    });
  }
  chart=mkChart(host);
  function wipe(){ series.forEach(function(s){ try{ chart.removeSeries(s); }catch(e){} }); series=[]; mainSeries=null; }

  function addLine(pts, color, w){ if(!pts||!pts.length) return; var s=chart.addLineSeries({ color:color, lineWidth:w||1, lastValueVisible:false, priceLineVisible:false }); s.setData(pts); series.push(s); }
  async function paint(d){
    if(!d||!d.length){ document.getElementById("quote").textContent="No bars for "+active; return; }
    wipe(); lastBars=d;
    chart.applyOptions({ localization:{ priceFormatter:function(p){ return mode==="price"?fmt(p):p.toFixed(2)+"%"; } }, grid:{ vertLines:{color:gridOn?"#f0f3fa":"transparent"}, horzLines:{color:gridOn?"#f0f3fa":"transparent"} } });
    try{ chart.priceScale("right").applyOptions({ mode: scaleMode }); }catch(e){}
    var display = kind==="heikin" ? heikin(d) : d;
    if(mode==="price"){
      var c;
      if(kind==="line"){ c=chart.addLineSeries({color:UP,lineWidth:2}); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
      else if(kind==="area"){ c=chart.addAreaSeries({lineColor:UP,topColor:"rgba(8,153,129,0.28)",bottomColor:"rgba(8,153,129,0.02)"}); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
      else if(kind==="baseline"){ var base=display[0].close; c=chart.addBaselineSeries({ baseValue:{type:"price",price:base}, topLineColor:UP, bottomLineColor:DN }); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
      else if(kind==="columns"){ c=chart.addHistogramSeries({}); c.setData(display.map(function(b){return {time:b.time,value:b.close,color:b.close>=b.open?UP:DN};})); }
      else if(kind==="bars"){ c=chart.addBarSeries({upColor:UP,downColor:DN}); c.setData(display); }
      else { c=chart.addCandlestickSeries({ upColor: kind==="hollow"?BG:UP, downColor:DN, borderUpColor:UP, borderDownColor:DN, wickUpColor:UP, wickDownColor:DN }); c.setData(display); }
      mainSeries=c; series.push(c);
      var v=chart.addHistogramSeries({ priceFormat:{type:"volume"}, priceScaleId:"vol" });
      chart.priceScale("vol").applyOptions({ scaleMargins:{ top:0.82, bottom:0 } });
      v.setData(display.map(function(b){ return {time:b.time,value:b.volume,color:b.close>=b.open?"rgba(8,153,129,.35)":"rgba(242,54,69,.35)"}; }));
      series.push(v);
      INDS.forEach(function(ind){
        if(!ind.on) return;
        if(ind.k==="sma") addLine(sma(d,ind.p), ind.c);
        if(ind.k==="ema") addLine(ema(d,ind.p), ind.c);
        if(ind.k==="vwma") addLine(vwma(d,ind.p), ind.c);
        if(ind.k==="vwap") addLine(vwap(d), ind.c);
        if(ind.k==="bb"){ var bb= (function(){ var m=sma(d,20), up=[],dn=[],i,j; for(i=19;i<d.length;i++){ var ss=0; for(j=0;j<20;j++){ var dv=d[i-j].close-m[i-19].value; ss+=dv*dv; } var sd=Math.sqrt(ss/20); up.push({time:d[i].time,value:m[i-19].value+2*sd}); dn.push({time:d[i].time,value:m[i-19].value-2*sd}); } return {m:m,up:up,dn:dn}; })(); addLine(bb.up,ind.c); addLine(bb.m,ind.c); addLine(bb.dn,ind.c); }
        if(ind.k==="st"){ var st=supertrend(d); var s=chart.addLineSeries({ color:ind.c, lineWidth:2, lastValueVisible:false, priceLineVisible:false }); s.setData(st.map(function(p){return {time:p.time,value:p.value};})); series.push(s); }
        if(ind.k==="ich"){ var ich=ichimoku(d); addLine(ich.conv,"#2962ff"); addLine(ich.base,"#e91e63"); addLine(ich.spanA,"#26a69a"); addLine(ich.spanB,"#ab47bc"); }
      });
      for(var ci=0;ci<compare.length;ci++){
        try{
          var cb=await klines(compare[ci], tf);
          if(cb.length<2||d.length<2) continue;
          var t0=d[0].close, c0=cb[0].close;
          addLine(cb.map(function(b){ return {time:b.time, value: (b.close/c0)*(t0) }; }), ["#f7931a","#7e57c2","#26c6da"][ci%3], 2);
        }catch(e){}
      }
      if(vpOn) drawVP(d);
      else { var cv=document.getElementById("vp"); if(cv){ cv.width=cv.height=0; } }
    } else {
      var pct= mode==="vsspy" ? await vsSpy(d) : computeChange(d, mode);
      var h=chart.addHistogramSeries({ priceFormat:{type:"percent"} });
      h.setData((pct||[]).map(function(p){ return {time:p.time,value:p.value,color:p.value>=0?UP:DN}; }));
      series.push(h); mainSeries=h;
    }
    if(watermark){ /* legend already names the symbol */ }
    chart.timeScale().fitContent();
    paintOsc(d);
    drawSVG();
    var last=d[d.length-1], prev=d[d.length-2]||last;
    var chg=prev.close?(last.close-prev.close)/prev.close:0, up=chg>=0;
    document.getElementById("quote").innerHTML="<b>"+active+" · "+tf+" · "+mode+"</b> <span>O"+fmt(last.open)+" H<span class=up>"+fmt(last.high)+"</span> L<span class=dn>"+fmt(last.low)+"</span> C<span class="+(up?"up":"dn")+">"+fmt(last.close)+"</span></span> <span class="+(up?"up":"dn")+">"+(up?"+":"")+fmt(last.close-last.open)+" ("+(chg*100).toFixed(2)+"%)</span> <span>Vol "+fmtVol(last.volume)+"</span> <span style='margin-left:auto' class=sell>"+fmt(last.close)+" SELL</span> <span class=buy>"+fmt(last.close*1.001)+" BUY</span>";
    var hi=Math.max.apply(null,d.slice(-40).map(function(b){return b.high;}));
    var lo=Math.min.apply(null,d.slice(-40).map(function(b){return b.low;}));
    var yhi=Math.max.apply(null,d.map(function(b){return b.high;}));
    var ylo=Math.min.apply(null,d.map(function(b){return b.low;}));
    var dp=Math.min(100,Math.max(0,(last.close-lo)/(hi-lo||1)*100));
    var yp=Math.min(100,Math.max(0,(last.close-ylo)/(yhi-ylo||1)*100));
    document.getElementById("detail").innerHTML="<div style=font-weight:600>"+active+"</div><div class=px>"+fmt(last.close)+"</div><div class="+(up?"up":"dn")+">"+(up?"+":"")+fmt(last.close-prev.close)+" "+(chg*100).toFixed(2)+"%</div><div style='margin-top:8px;font-size:10px;color:var(--mut)'>DAY RANGE</div><div class=rg><i style=width:"+dp+"%></i><b style=left:"+dp+"%></b></div><div style=display:flex;justify-content:space-between;font-size:10px;font-family:IBM+Plex+Mono,monospace><span>"+fmt(lo)+"</span><span>"+fmt(hi)+"</span></div><div style='margin-top:8px;font-size:10px;color:var(--mut)'>52-WEEK RANGE</div><div class=rg><i style=width:"+yp+"%></i><b style=left:"+yp+"%></b></div><div style=display:flex;justify-content:space-between;font-size:10px;font-family:IBM+Plex+Mono,monospace><span>"+fmt(ylo)+"</span><span>"+fmt(yhi)+"</span></div>";
    checkAlerts(active, last.close);
    countdown(last);
    renderLegend();
  }
  function fmtVol(v){ if(v>=1e12) return (v/1e12).toFixed(2)+"T"; if(v>=1e9) return (v/1e9).toFixed(2)+"B"; if(v>=1e6) return (v/1e6).toFixed(2)+"M"; if(v>=1e3) return (v/1e3).toFixed(1)+"K"; return String(Math.round(v||0)); }
  function paintOsc(d){
    var wrap=document.getElementById("oscwrap");
    var on=OSC.filter(function(o){ return o.on; });
    if(!on.length){ wrap.className=""; if(oscChart){ try{ oscChart.remove(); }catch(e){} oscChart=null; } return; }
    wrap.className="on";
    if(!oscChart) oscChart=LW.createChart(document.getElementById("osc"),{ autoSize:true, layout:{background:{type:"solid",color:BG},textColor:"#6a6d78",fontSize:10}, grid:{vertLines:{color:"#f0f3fa"},horzLines:{color:"#f0f3fa"}}, timeScale:{ visible:false }, rightPriceScale:{ borderColor:"#e0e3eb" } });
    oscSeries.forEach(function(s){ try{ oscChart.removeSeries(s); }catch(e){} }); oscSeries=[];
    on.forEach(function(o){
      if(o.id==="rsi"){ var s=oscChart.addLineSeries({color:ACC,lineWidth:1}); s.setData(rsi(d,14)); oscSeries.push(s); }
      if(o.id==="macd"){ var m=macd(d); var h=oscChart.addHistogramSeries({}); h.setData(m.map(function(p){return {time:p.time,value:p.hist,color:p.hist>=0?UP:DN};})); var l1=oscChart.addLineSeries({color:ACC,lineWidth:1}); l1.setData(m.map(function(p){return {time:p.time,value:p.macd};})); var l2=oscChart.addLineSeries({color:"#ff6d00",lineWidth:1}); l2.setData(m.map(function(p){return {time:p.time,value:p.signal};})); oscSeries.push(h,l1,l2); }
      if(o.id==="stoch"){ var s2=oscChart.addLineSeries({color:"#ab47bc",lineWidth:1}); s2.setData(stoch(d,14,3,3)); oscSeries.push(s2); }
      if(o.id==="atr"){ var s3=oscChart.addLineSeries({color:"#6a6d78",lineWidth:1}); s3.setData(atr(d,14)); oscSeries.push(s3); }
    });
  }
  function drawVP(d){
    var cv=document.getElementById("vp"), box=document.getElementById("chart");
    if(!cv||!box) return;
    var w=80, h=box.clientHeight-8; cv.width=w; cv.height=h; cv.style.width=w+"px"; cv.style.height=h+"px";
    var ctx=cv.getContext("2d"); ctx.clearRect(0,0,w,h);
    var hi=-1e99, lo=1e99,i; for(i=0;i<d.length;i++){ if(d[i].high>hi)hi=d[i].high; if(d[i].low<lo)lo=d[i].low; }
    var bins=24, vol=new Array(bins).fill(0), max=1;
    for(i=0;i<d.length;i++){ var idx=Math.min(bins-1, Math.max(0, Math.floor((d[i].close-lo)/(hi-lo||1)*bins))); vol[idx]+=d[i].volume; if(vol[idx]>max) max=vol[idx]; }
    for(i=0;i<bins;i++){ var y=h-(i+1)*(h/bins); ctx.fillStyle=i>bins/2?"rgba(8,153,129,.35)":"rgba(242,54,69,.35)"; ctx.fillRect(0,y,(vol[i]/max)*w, h/bins-1); }
  }
  async function load(){ try{ var d=await klines(active,tf); if(replay.on) d=d.slice(0, replay.i||d.length); await paint(d); if(layout===2) paint2(); }catch(e){ document.getElementById("quote").textContent="Chart error: "+(e&&e.message||e); } }
  async function paint2(){
    var el=document.getElementById("host2"), wrap=document.getElementById("chart2");
    wrap.style.display="block"; document.getElementById("stage").className="split";
    if(!chart2) chart2=mkChart(el);
    var sym=TABS.filter(function(s){ return s!==active; })[0]||"SPY";
    var d=await klines(sym,tf); if(d.length<2) return;
    try{ var panes=chart2.panes?chart2.panes():[]; }catch(e){}
    try{ chart2.remove(); chart2=mkChart(el); }catch(e){ chart2=mkChart(el); }
    var c=chart2.addCandlestickSeries({upColor:UP,downColor:DN,borderUpColor:UP,borderDownColor:DN,wickUpColor:UP,wickDownColor:DN});
    c.setData(d); chart2.timeScale().fitContent();
  }
  function hide2(){ document.getElementById("chart2").style.display="none"; document.getElementById("stage").className=""; }

  /* drawings */
  function saveDraw(){ var all=loadJSON(DRAW_KEY,{}); all[active]=drawings; saveJSON(DRAW_KEY, all); }
  function loadDraw(){ var all=loadJSON(DRAW_KEY,{}); drawings=all[active]||[]; pending=null; }
  function xy(t,p){ if(!mainSeries) return null; var x=chart.timeScale().timeToCoordinate(t); var y=mainSeries.priceToCoordinate(p); if(x==null||y==null) return null; return {x:x,y:y}; }
  function drawSVG(){
    var svg=document.getElementById("draw"), box=document.getElementById("chart");
    if(!svg||!box) return;
    var w=box.clientWidth, h=box.clientHeight; svg.setAttribute("viewBox","0 0 "+w+" "+h); svg.setAttribute("width",w); svg.setAttribute("height",h);
    var parts=[], i, d;
    for(i=0;i<drawings.length;i++){
      d=drawings[i]; if(d.hide) continue;
      var pts=(d.points||[]).map(function(p){ return xy(p.time,p.price); }).filter(Boolean);
      var c=d.color||ACC;
      if(d.kind==="hline" && pts[0]) parts.push('<line x1="0" x2="'+w+'" y1="'+pts[0].y+'" y2="'+pts[0].y+'" stroke="'+c+'" />');
      else if(d.kind==="vline" && pts[0]) parts.push('<line y1="0" y2="'+h+'" x1="'+pts[0].x+'" x2="'+pts[0].x+'" stroke="'+c+'" />');
      else if((d.kind==="trend"||d.kind==="ray"||d.kind==="measure") && pts.length>=2){ var x2=d.kind==="ray"?w:pts[1].x; parts.push('<line x1="'+pts[0].x+'" y1="'+pts[0].y+'" x2="'+x2+'" y2="'+pts[1].y+'" stroke="'+c+'" stroke-width="1.2"/>'); if(d.kind==="measure"){ var pct=((d.points[1].price-d.points[0].price)/d.points[0].price)*100; parts.push('<text x="'+((pts[0].x+pts[1].x)/2)+'" y="'+((pts[0].y+pts[1].y)/2-6)+'" fill="'+c+'" font-size="11" font-family="IBM Plex Mono">'+pct.toFixed(2)+'%</text>'); } }
      else if(d.kind==="channel" && pts.length>=3){ parts.push('<line x1="'+pts[0].x+'" y1="'+pts[0].y+'" x2="'+pts[1].x+'" y2="'+pts[1].y+'" stroke="'+c+'"/>'); var dy=pts[2].y-pts[0].y; parts.push('<line x1="'+pts[0].x+'" y1="'+(pts[0].y+dy)+'" x2="'+pts[1].x+'" y2="'+(pts[1].y+dy)+'" stroke="'+c+'" stroke-dasharray="4 3"/>'); }
      else if(d.kind==="rect" && pts.length>=2){ var x=Math.min(pts[0].x,pts[1].x), y=Math.min(pts[0].y,pts[1].y); parts.push('<rect x="'+x+'" y="'+y+'" width="'+Math.abs(pts[1].x-pts[0].x)+'" height="'+Math.abs(pts[1].y-pts[0].y)+'" fill="'+c+'22" stroke="'+c+'"/>'); }
      else if(d.kind==="fib" && pts.length>=2){ var lv=[0,0.236,0.382,0.5,0.618,0.786,1]; for(var k=0;k<lv.length;k++){ var yf=pts[0].y+(pts[1].y-pts[0].y)*lv[k]; parts.push('<line x1="0" x2="'+w+'" y1="'+yf+'" y2="'+yf+'" stroke="'+c+'" stroke-opacity=".75" stroke-dasharray="4 3"/>'); parts.push('<text x="8" y="'+(yf-3)+'" fill="'+c+'" font-size="10" font-family="IBM Plex Mono">'+(lv[k]*100).toFixed(1)+'</text>'); } }
      else if(d.kind==="text" && pts[0]) parts.push('<text x="'+pts[0].x+'" y="'+pts[0].y+'" fill="'+c+'" font-size="12">'+(d.label||"note")+'</text>');
      else if(d.kind==="brush" && pts.length){ var pth=pts.map(function(p,i){ return (i?"L":"M")+p.x+" "+p.y; }).join(" "); parts.push('<path d="'+pth+'" fill="none" stroke="'+c+'" stroke-width="1.4"/>'); }
    }
    svg.innerHTML=parts.join("");
    renderObj();
  }
  function onChartClick(ev){
    if(tool==="cursor") return;
    var box=document.getElementById("chart").getBoundingClientRect();
    var x=ev.clientX-box.left, y=ev.clientY-box.top;
    var t=chart.timeScale().coordinateToTime(x); if(t==null||!mainSeries) return;
    var p=mainSeries.coordinateToPrice(y); if(p==null) return;
    p=Number(p);
    if(magnet && lastBars.length){ var best=lastBars[0], bd=1e99,i; for(i=0;i<lastBars.length;i++){ var dd=Math.abs(lastBars[i].time-t); if(dd<bd){ bd=dd; best=lastBars[i]; } } var cand=[best.open,best.high,best.low,best.close]; p=cand.reduce(function(a,b){ return Math.abs(b-p)<Math.abs(a-p)?b:a; }); t=best.time; }
    var pt={time:Number(t),price:p};
    if(!pending){
      var id=uid(); pending={id:id,kind:tool,points:[pt],color:ACC};
      drawings.push(pending); undo.push({op:"add",id:id});
      if(tool==="hline"||tool==="vline"||tool==="text"){ if(tool==="text") pending.label=prompt("Text","note")||"note"; pending=null; tool="cursor"; renderRail(); }
      saveDraw(); drawSVG(); return;
    }
    pending.points.push(pt);
    var need= tool==="channel"?3 : (tool==="brush"? 99 : 2);
    if(pending.points.length>=need && tool!=="brush"){ pending=null; tool="cursor"; renderRail(); }
    saveDraw(); drawSVG();
  }
  function undoDraw(){ var u=undo.pop(); if(!u) return; redo.push(u); drawings=drawings.filter(function(d){ return d.id!==u.id; }); saveDraw(); drawSVG(); }
  function clearDraw(){ undo=undo.concat(drawings.map(function(d){return {op:"add",id:d.id};})); drawings=[]; pending=null; saveDraw(); drawSVG(); }

  /* alerts */
  function loadAlerts(){ alerts=loadJSON(ALERT_KEY,[]); }
  function saveAlerts(){ saveJSON(ALERT_KEY, alerts); renderAlerts(); }
  function addAlert(sym, px){ alerts.push({id:uid(),sym:sym,price:+px,created:Date.now(),fired:false}); saveAlerts(); toast("Alert "+sym+" @ "+fmt(px)); }
  function checkAlerts(sym, px){
    alerts.forEach(function(a){
      if(a.fired||a.sym!==sym) return;
      if(Math.abs(px-a.price)/a.price < 0.002){ a.fired=true; saveAlerts(); toast("ALERT "+sym+" hit "+fmt(a.price)); if(window.Notification&&Notification.permission==="granted") new Notification("JustHodl "+sym, {body:"Price "+fmt(px)}); }
    });
  }
  function renderAlerts(){
    document.getElementById("alerts").innerHTML="<b>ALERTS</b>"+(alerts.length?alerts.map(function(a){ return "<div class=cell><span>"+a.sym+" "+fmt(a.price)+(a.fired?" ✓":"")+"</span><button data-del='"+a.id+"'>×</button></div>"; }).join(""):"<div class=cell>None — click a watchlist row → Alert</div>");
    document.querySelectorAll("#alerts [data-del]").forEach(function(b){ b.onclick=function(){ alerts=alerts.filter(function(a){return a.id!==b.dataset.del;}); saveAlerts(); }; });
  }

  /* UI bars */
  function renderTabs(){
    document.getElementById("tabs").innerHTML="<a class='tab brand' href='/'>JustHodl</a>"+TABS.map(function(s){ var q=quotes[s], up=q&&q.chg>=0; return "<button class='tab "+(s===active?"on":"")+"' data-id='"+s+"'>"+s.replace("USDT","")+(q?" <span class="+(up?"up":"dn")+">"+fmt(q.last)+" "+(up?"+":"")+(q.chg*100).toFixed(2)+"%</span>":"")+" <span data-x='"+s+"'>×</span></button>"; }).join("")+"<button class=tab id=add>+</button><a class='tab pro' href='/chart-pro.html'>Pro</a>";
    document.querySelectorAll(".tab[data-id]").forEach(function(b){ b.onclick=function(e){ if(e.target.dataset.x){ TABS=TABS.filter(function(s){return s!==e.target.dataset.x;}); if(active===e.target.dataset.x) active=TABS[0]||active; renderTabs(); loadDraw(); load(); return; } active=b.dataset.id; loadDraw(); renderTabs(); load(); }; });
    var add=document.getElementById("add"); if(add) add.onclick=function(){ document.getElementById("q").focus(); };
  }
  function renderTf(){
    document.getElementById("tfbar").innerHTML=
      TFS.map(function(t){ return "<button class='"+(t[0]===tf?"on":"")+"' data-tf='"+t[0]+"'>"+t[1]+"</button>"; }).join("")+
      "<span style='width:8px'></span>"+
      KINDS.map(function(k){ return "<button class='"+(k[0]===kind?"on":"")+"' data-k='"+k[0]+"'>"+k[1]+"</button>"; }).join("")+
      SCALES.map(function(s){ return "<button class='"+(+s[0]===scaleMode?"on":"")+"' data-sc='"+s[0]+"'>"+s[1]+"</button>"; }).join("")+
      "<button id=btn-ind>Indicators</button><button id=btn-cmp>Compare</button><button id=btn-rep>Replay</button><button id=btn-shot>Snapshot</button><button id=btn-fs>Full</button><button id=btn-lay>"+(layout===2?"1 pane":"2 panes")+"</button><button id=btn-watch>"+(watchOpen?"Hide list":"Watchlist")+"</button><button id=btn-set>Settings</button><input id=goto type=date title='Go to date'>";
    document.querySelectorAll("#tfbar [data-tf]").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });
    document.querySelectorAll("#tfbar [data-k]").forEach(function(b){ b.onclick=function(){ kind=b.dataset.k; renderTf(); if(lastBars.length) paint(lastBars); }; });
    document.querySelectorAll("#tfbar [data-sc]").forEach(function(b){ b.onclick=function(){ scaleMode=+b.dataset.sc; renderTf(); if(lastBars.length) paint(lastBars); }; });
    document.getElementById("btn-ind").onclick=openInd;
    document.getElementById("btn-cmp").onclick=openCmp;
    document.getElementById("btn-rep").onclick=startReplay;
    document.getElementById("btn-shot").onclick=shot;
    document.getElementById("btn-fs").onclick=function(){ var el=document.getElementById("app"); if(!document.fullscreenElement) el.requestFullscreen(); else document.exitFullscreen(); };
    document.getElementById("btn-lay").onclick=function(){ layout=layout===1?2:1; if(layout===2) paint2(); else hide2(); renderTf(); };
    document.getElementById("btn-watch").onclick=function(){ watchOpen=!watchOpen; document.getElementById("watch").className="watch"+(watchOpen?"":" hide"); renderTf(); };
    document.getElementById("btn-set").onclick=openSet;
    document.getElementById("goto").onchange=function(){ var t=Math.floor(Date.parse(this.value+"T00:00:00Z")/1000); if(!t||!lastBars.length) return; chart.timeScale().setVisibleRange({ from:t-30*86400, to:t+5*86400 }); };
    document.getElementById("chgbar").innerHTML=CHG.map(function(t){ return "<button class='"+(t[0]===mode?"on":"")+"' data-m='"+t[0]+"'>"+t[1]+"</button>"; }).join("");
    document.querySelectorAll("#chgbar [data-m]").forEach(function(b){ b.onclick=function(){ mode=b.dataset.m; renderTf(); if(lastBars.length) paint(lastBars); }; });
  }
  function renderRail(){
    document.getElementById("rail").innerHTML=TOOLS.map(function(x){ return "<button class='"+(tool===x[0]?"on":"")+"' data-t='"+x[0]+"' title='"+x[2]+"'>"+x[1]+"</button>"; }).join("")+
      "<button class='"+(magnet?"on":"")+"' id=mag title=Magnet>M</button><button id=und title=Undo>↶</button><button id=clr title='Clear drawings'>⌫</button><button id=objb title='Object tree'>☰</button><button class='"+(vpOn?"on":"")+"' id=vpb title='Volume profile'>VP</button><button id=csv title='Export CSV'>CSV</button>";
    document.querySelectorAll("#rail [data-t]").forEach(function(b){ b.onclick=function(){ tool=b.dataset.t; pending=null; renderRail(); }; });
    document.getElementById("mag").onclick=function(){ magnet=!magnet; renderRail(); };
    document.getElementById("und").onclick=undoDraw;
    document.getElementById("clr").onclick=clearDraw;
    document.getElementById("objb").onclick=function(){ objOpen=!objOpen; renderObj(); };
    document.getElementById("vpb").onclick=function(){ vpOn=!vpOn; renderRail(); if(lastBars.length) paint(lastBars); };
    document.getElementById("csv").onclick=exportCSV;
  }
  function renderLegend(){
    document.getElementById("legend").innerHTML="<div style='color:#131722;font-weight:500;margin-bottom:4px'>"+active+(compare.length?" + "+compare.join(" "):"")+"</div>"+INDS.map(function(i){ return "<button style=color:"+(i.on?i.c:"#6a6d78")+" data-i='"+i.id+"'>"+i.n+"</button>"; }).join("")+OSC.map(function(o){ return "<button style=color:"+(o.on?ACC:"#6a6d78")+" data-o='"+o.id+"'>"+o.n+"</button>"; }).join("");
    document.querySelectorAll("#legend [data-i]").forEach(function(b){ b.onclick=function(){ var i=INDS.find(function(x){return x.id===b.dataset.i;}); i.on=!i.on; if(lastBars.length) paint(lastBars); }; });
    document.querySelectorAll("#legend [data-o]").forEach(function(b){ b.onclick=function(){ var o=OSC.find(function(x){return x.id===b.dataset.o;}); o.on=!o.on; if(lastBars.length) paint(lastBars); }; });
  }
  function renderObj(){
    var el=document.getElementById("obj");
    el.className=objOpen?"on":"";
    if(!objOpen) return;
    el.innerHTML="<div style=color:var(--mut)>Object tree</div>"+(drawings.length?drawings.map(function(d){ return "<div class=cell><span>"+d.kind+"</span><span><button data-h='"+d.id+"'>"+(d.hide?"show":"hide")+"</button> <button data-r='"+d.id+"'>×</button></span></div>"; }).join(""):"<div>No drawings</div>");
    el.querySelectorAll("[data-h]").forEach(function(b){ b.onclick=function(){ var d=drawings.find(function(x){return x.id===b.dataset.h;}); if(d) d.hide=!d.hide; saveDraw(); drawSVG(); }; });
    el.querySelectorAll("[data-r]").forEach(function(b){ b.onclick=function(){ drawings=drawings.filter(function(x){return x.id!==b.dataset.r;}); saveDraw(); drawSVG(); }; });
  }
  function renderWtabs(){
    document.getElementById("wtabs").innerHTML=["list","news","alerts"].map(function(t){ return "<button class='"+(wtab===t?"on":"")+"' data-w='"+t+"'>"+t+"</button>"; }).join("");
    document.querySelectorAll("#wtabs [data-w]").forEach(function(b){ b.onclick=function(){ wtab=b.dataset.w; document.getElementById("w-list").style.display=wtab==="list"?"":"none"; document.getElementById("news").style.display=wtab==="news"?"":"none"; document.getElementById("alerts").style.display=wtab==="alerts"?"":"none"; document.getElementById("intel").style.display=wtab==="list"?"":"none"; renderWtabs(); if(wtab==="news") renderNews(); if(wtab==="alerts") renderAlerts(); }; });
  }

  function currentSyms(){
    var L=lists.find(function(x){return x.id===listId;})||lists[0]||{symbols:ISHARES};
    var arr=(L.symbols||[]).filter(function(s){ var b=bare(s).toUpperCase(); if(letter&&b.indexOf(letter)!==0) return false; if(filter&&b.indexOf(filter.toUpperCase())<0) return false; return true; });
    arr.sort(function(a,b){
      var qa=quotes[a]||quotes[bare(a)]||{}, qb=quotes[b]||quotes[bare(b)]||{};
      var va=sortCol==="last"?qa.last: sortCol==="chg"?qa.chg: sortCol==="chgv"?qa.chgv: String(a);
      var vb=sortCol==="last"?qb.last: sortCol==="chg"?qb.chg: sortCol==="chgv"?qb.chgv: String(b);
      if(va==null) va=0; if(vb==null) vb=0;
      return va>vb?sortDir: va<vb?-sortDir:0;
    });
    return arr.slice(0,120);
  }
  async function lastPx(sym){
    try{ var d=await klines(sym,"1d"); if(d.length<2) return null; var last=d[d.length-1], prev=d[d.length-2]||last; return {last:last.close, chg:prev.close?(last.close-prev.close)/prev.close:0, chgv:last.close-prev.close, spark:d.slice(-20)}; }catch(e){ return null; }
  }
  function sparkSvg(spark, up){
    if(!spark||spark.length<2) return "";
    var mn=1e99,mx=-1e99,i; for(i=0;i<spark.length;i++){ if(spark[i].close<mn)mn=spark[i].close; if(spark[i].close>mx)mx=spark[i].close; }
    var p=spark.map(function(b,i){ var x=i/(spark.length-1)*40; var y=16-((b.close-mn)/(mx-mn||1))*16; return (i?"L":"M")+x.toFixed(1)+" "+y.toFixed(1); }).join(" ");
    return "<svg class=spark viewBox='0 0 40 16'><path d='"+p+"' fill='none' stroke='"+(up?UP:DN)+"' stroke-width='1.2'/></svg>";
  }
  function renderLetters(){
    document.getElementById("letters").innerHTML="ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("").map(function(L){ return "<button class='"+(letter===L?"on":"")+"' data-l='"+L+"'>"+L+"</button>"; }).join("");
    document.querySelectorAll("#letters [data-l]").forEach(function(b){ b.onclick=function(){ letter=letter===b.dataset.l?"":b.dataset.l; renderLetters(); renderList(); }; });
  }
  function renderList(){
    var sel=document.getElementById("list");
    if(sel&&!sel.dataset.bound){ sel.onchange=function(){ listId=sel.value; renderList(); }; sel.dataset.bound="1"; }
    if(sel){ sel.innerHTML=lists.map(function(l){ return "<option value='"+l.id+"'"+(l.id===listId?" selected":"")+">"+l.name+" ("+(l.n||(l.symbols||[]).length)+")</option>"; }).join(""); }
    document.getElementById("nlists").textContent=lists.length+" lists";
    var box=document.getElementById("wlist");
    var syms=currentSyms();
    box.innerHTML=syms.map(function(s){
      var q=quotes[s]||quotes[bare(s)]; var up=!q||q.chg>=0;
      return "<button class='wrow "+(bare(s)===active?"on":"")+"' data-s='"+s+"'><span>"+s+"</span><span>"+(q?fmt(q.last):"—")+"</span><span class="+(up?"up":"dn")+">"+(q?(q.chg>=0?"+":"")+(q.chg*100).toFixed(2)+"%":"—")+"</span><span class="+(up?"up":"dn")+">"+(q?(q.chgv>=0?"+":"")+fmt(q.chgv):"—")+"</span>"+sparkSvg(q&&q.spark,up)+"</button>";
    }).join("")||"<div style='padding:12px;color:var(--mut)'>No symbols in this filter</div>";
    box.querySelectorAll("[data-s]").forEach(function(b){
      b.onclick=function(){ var s=b.dataset.s; if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); loadDraw(); renderTabs(); load(); };
      b.oncontextmenu=function(e){ e.preventDefault(); openCtx(e.clientX,e.clientY,b.dataset.s); };
    });
    var q=document.getElementById("q");
    if(q&&!q.dataset.bound){
      q.oninput=function(){ filter=q.value; renderHits(); renderList(); };
      q.onkeydown=function(e){ if(e.key==="Enter"&&q.value.trim()){ var s=q.value.trim().toUpperCase(); if(TABS.indexOf(s)<0) TABS.push(s); active=s; q.value=""; filter=""; loadDraw(); renderTabs(); renderList(); load(); } };
      q.dataset.bound="1";
    }
    var paste=document.getElementById("paste");
    if(paste&&!paste.dataset.bound){
      paste.onkeydown=function(e){
        if(e.key!=="Enter") return;
        var syms2=String(paste.value||"").split(/[\s,;]+/).map(function(t){ return t.replace(/^["']|["']$/g,"").trim().toUpperCase(); }).filter(function(t){ return t&&/[A-Z]/.test(t); });
        if(!syms2.length) return;
        var custom=loadJSON(CUSTOM_KEY,[]); var name="Paste "+new Date().toISOString().slice(0,16);
        var hit={id:"custom-"+Date.now(),name:name,symbols:syms2,n:syms2.length,custom:1}; custom.push(hit); saveJSON(CUSTOM_KEY,custom);
        lists=[hit].concat(lists.filter(function(l){ return l.id!==hit.id; })); listId=hit.id; paste.value=""; renderList(); toast("List saved · warehouse lists kept");
      };
      paste.dataset.bound="1";
    }
    document.querySelectorAll("#cols [data-s]").forEach(function(c){ c.onclick=function(){ if(sortCol===c.dataset.s) sortDir*=-1; else { sortCol=c.dataset.s; sortDir=1; } renderList(); }; });
    syms.slice(0,24).forEach(function(s){ if(quotes[s]||quotes[bare(s)]) return; lastPx(s).then(function(px){ if(!px) return; quotes[s]=px; quotes[bare(s)]=px; renderList(); renderTabs(); }); });
  }
  function renderHits(){
    var needle=(document.getElementById("q").value||"").trim().toLowerCase(), box=document.getElementById("hits");
    if(needle.length<2){ box.innerHTML=""; return; }
    var out=[]; for(var i=0;i<lists.length&&out.length<40;i++){ (lists[i].symbols||[]).forEach(function(s){ if(out.length>=40) return; if(String(s).toLowerCase().indexOf(needle)>=0) out.push({s:s,list:lists[i].name}); }); }
    box.innerHTML=out.map(function(h){ return "<button class=hit data-s='"+h.s+"'><span>"+h.s+"</span><span>"+h.list+"</span></button>"; }).join("");
    box.querySelectorAll("[data-s]").forEach(function(b){ b.onclick=function(){ var s=b.dataset.s; if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); document.getElementById("q").value=""; filter=""; loadDraw(); renderTabs(); renderHits(); renderList(); load(); }; });
  }
  async function loadLists(){
    var local=[{id:"ishares",name:"iShares ETFs — BlackRock",symbols:ISHARES,n:ISHARES.length},{id:"tabs",name:"Open tabs",symbols:TABS,n:TABS.length}];
    var custom=loadJSON(CUSTOM_KEY,[]); if(!Array.isArray(custom)) custom=[];
    var copied=[]; try{ var j=await fetchJson("/data/tv-watchlists.json"); var arr=Array.isArray(j)?j:(j.lists||[]); copied=arr.filter(function(l){return l&&l.name&&Array.isArray(l.symbols);}).map(function(l){ return {id:String(l.id||l.name),name:l.name,symbols:l.symbols,n:l.n||l.symbols.length}; }); }catch(e){}
    lists=custom.concat(local, copied);
  }
  async function loadIntel(){
    try{ var j=await fetchJson("/data/jh-internals.json"); var f=j.fields||j;
      document.getElementById("intel").innerHTML="<b>INTERNALS · warehouse</b>"+[["2s10s",f.twos_tens!=null?f.twos_tens+"%":"—"],["LIQ $B",f.liq_proxy_bn!=null?f.liq_proxy_bn:"—"],["NFCI",f.nfci!=null?f.nfci:"—"],["A-D",f.ad_breadth!=null?Number(f.ad_breadth).toFixed(3):"—"],["NH-NL",f.nh_nl!=null?f.nh_nl+" ("+(f.n_new_high||"—")+"H / "+(f.n_new_low||"—")+"L)":"—"]].map(function(x){return "<div class=cell><span>"+x[0]+"</span><span>"+x[1]+"</span></div>";}).join("");
    }catch(e){}
  }
  async function loadNews(){ try{ var j=await fetchJson("/data/finviz-news.json"); news=j.news||[]; renderNews(); }catch(e){} }
  function renderNews(){
    var t=active.replace("USDT","");
    var rows=news.filter(function(n){ return !t || String(n.ticker||"").indexOf(t)>=0 || String(n.title||"").toUpperCase().indexOf(t)>=0; }).slice(0,30);
    if(!rows.length) rows=news.slice(0,20);
    document.getElementById("news").innerHTML="<b>NEWS</b>"+rows.map(function(n){ return "<a class=nitem href='"+(n.url||"#")+"' target=_blank>"+ (n.title||"")+"<span>"+(n.source||"")+" · "+String(n.date||"").slice(0,16)+" · "+(n.ticker||"")+"</span></a>"; }).join("");
  }

  function openInd(){
    var m=document.getElementById("modal"), box=document.getElementById("mbox");
    box.innerHTML="<h3>Indicators</h3><div style=color:var(--mut);font-size:11px;margin-bottom:8px>Templates: <button id=t-tv>TV default</button> <button id=t-tr>Trend</button> <button id=t-os>Oscillators</button> <button id=t-cl>Clean</button></div>"+
      INDS.map(function(i){ return "<label class=indrow><span>"+i.n+"</span><input type=checkbox data-i='"+i.id+"' "+(i.on?"checked":"")+"></label>"; }).join("")+
      OSC.map(function(o){ return "<label class=indrow><span>"+o.n+" · pane</span><input type=checkbox data-o='"+o.id+"' "+(o.on?"checked":"")+"></label>"; }).join("")+
      "<div style='margin-top:10px;text-align:right'><button id=indok>Apply</button></div>";
    m.className="on";
    document.getElementById("t-tv").onclick=function(){ INDS.forEach(function(i){ i.on=/sma|ema250/.test(i.id); }); OSC.forEach(function(o){ o.on=false; }); openInd(); };
    document.getElementById("t-tr").onclick=function(){ INDS.forEach(function(i){ i.on=i.id==="st"||i.id==="ema250"; }); OSC.forEach(function(o){ o.on=false; }); openInd(); };
    document.getElementById("t-os").onclick=function(){ INDS.forEach(function(i){ i.on=false; }); OSC.forEach(function(o){ o.on=true; }); openInd(); };
    document.getElementById("t-cl").onclick=function(){ INDS.forEach(function(i){ i.on=false; }); OSC.forEach(function(o){ o.on=false; }); openInd(); };
    document.getElementById("indok").onclick=function(){
      box.querySelectorAll("[data-i]").forEach(function(c){ INDS.find(function(i){return i.id===c.dataset.i;}).on=c.checked; });
      box.querySelectorAll("[data-o]").forEach(function(c){ OSC.find(function(o){return o.id===c.dataset.o;}).on=c.checked; });
      m.className=""; if(lastBars.length) paint(lastBars);
    };
  }
  function openCmp(){
    var m=document.getElementById("modal"), box=document.getElementById("mbox");
    box.innerHTML="<h3>Compare / overlay</h3><p style=color:var(--mut);font-size:12px>Indexed overlay on the same pane. Does not replace the main series.</p><input id=cmpin placeholder='SPY, QQQ, BTCUSDT' style='width:100%;border:1px solid var(--line);padding:8px;border-radius:4px'><div style='margin-top:10px;text-align:right'><button id=cmpok>Add</button></div><div style=margin-top:8px>"+compare.map(function(s){return "<div class=cell>"+s+" <button data-c='"+s+"'>×</button></div>";}).join("")+"</div>";
    m.className="on";
    document.getElementById("cmpok").onclick=function(){ String(document.getElementById("cmpin").value||"").split(/[\s,]+/).forEach(function(s){ s=s.trim().toUpperCase(); if(s&&compare.indexOf(s)<0) compare.push(s); }); m.className=""; if(lastBars.length) paint(lastBars); };
    box.querySelectorAll("[data-c]").forEach(function(b){ b.onclick=function(){ compare=compare.filter(function(s){return s!==b.dataset.c;}); openCmp(); if(lastBars.length) paint(lastBars); }; });
  }
  function openSet(){
    var m=document.getElementById("modal"), box=document.getElementById("mbox");
    box.innerHTML="<h3>Chart settings</h3><label class=indrow><span>Grid</span><input type=checkbox id=s-grid "+(gridOn?"checked":"")+"></label><label class=indrow><span>Symbol watermark</span><input type=checkbox id=s-wm "+(watermark?"checked":"")+"></label><label class=indrow><span>Magnet</span><input type=checkbox id=s-mag "+(magnet?"checked":"")+"></label><div style='margin-top:10px;text-align:right'><button id=setok>Save</button></div>";
    m.className="on";
    document.getElementById("setok").onclick=function(){ gridOn=document.getElementById("s-grid").checked; watermark=document.getElementById("s-wm").checked; magnet=document.getElementById("s-mag").checked; saveJSON(LAY_KEY,{gridOn:gridOn,watermark:watermark,magnet:magnet,layout:layout,kind:kind,tf:tf}); m.className=""; if(lastBars.length) paint(lastBars); };
  }
  function openCtx(x,y,s){
    var el=document.getElementById("ctx");
    el.style.display="block"; el.style.left=x+"px"; el.style.top=y+"px";
    el.innerHTML="<button data-a=open>Open "+s+"</button><button data-a=cmp>Compare</button><button data-a=al>Alert at last</button><button data-a=tab>Add tab</button>";
    el.querySelectorAll("button").forEach(function(b){
      b.onclick=function(){
        if(b.dataset.a==="open"||b.dataset.a==="tab"){ if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); loadDraw(); renderTabs(); load(); }
        if(b.dataset.a==="cmp"){ if(compare.indexOf(bare(s))<0) compare.push(bare(s)); if(lastBars.length) paint(lastBars); }
        if(b.dataset.a==="al"){ var q=quotes[s]||quotes[bare(s)]; addAlert(bare(s), q?q.last:(lastBars[lastBars.length-1]||{}).close); }
        el.style.display="none";
      };
    });
  }
  document.addEventListener("click", function(e){ var el=document.getElementById("ctx"); if(el && !el.contains(e.target)) el.style.display="none"; var m=document.getElementById("modal"); if(e.target===m) m.className=""; });

  function startReplay(){
    if(!lastBars.length) return;
    replay={on:true,i:Math.max(30, lastBars.length-80), speed:1, timer:null, full:lastBars.slice()};
    document.getElementById("replay").className="on";
    document.getElementById("rp-label").textContent=new Date(replay.full[replay.i].time*1000).toISOString().slice(0,10);
    document.getElementById("rp-play").onclick=function(){ if(replay.timer){ clearInterval(replay.timer); replay.timer=null; this.textContent="Play"; return; } this.textContent="Pause"; var ms=400/replay.speed; replay.timer=setInterval(function(){ replay.i=Math.min(replay.full.length-1, replay.i+1); paint(replay.full.slice(0,replay.i+1)); document.getElementById("rp-label").textContent=new Date(replay.full[replay.i].time*1000).toISOString().slice(0,10); if(replay.i>=replay.full.length-1){ clearInterval(replay.timer); replay.timer=null; document.getElementById("rp-play").textContent="Play"; } }, ms); };
    document.getElementById("rp-step").onclick=function(){ replay.i=Math.min(replay.full.length-1, replay.i+1); paint(replay.full.slice(0,replay.i+1)); };
    document.getElementById("rp-speed").onchange=function(){ replay.speed=+this.value; };
    document.getElementById("rp-date").onchange=function(){ var t=Math.floor(Date.parse(this.value+"T00:00:00Z")/1000); var i=0; while(i<replay.full.length && replay.full[i].time<t) i++; replay.i=i; paint(replay.full.slice(0,replay.i+1)); };
    document.getElementById("rp-exit").onclick=function(){ if(replay.timer) clearInterval(replay.timer); replay.on=false; document.getElementById("replay").className=""; paint(replay.full); };
    paint(replay.full.slice(0,replay.i+1));
  }
  function shot(){
    try{
      var url=chart.takeScreenshot().toDataURL("image/png");
      var a=document.createElement("a"); a.href=url; a.download=active+"-"+tf+".png"; a.click();
    }catch(e){ toast("Snapshot unavailable"); }
  }
  function exportCSV(){
    var rows=["time,open,high,low,close,volume"].concat(lastBars.map(function(b){ return [new Date(b.time*1000).toISOString(),b.open,b.high,b.low,b.close,b.volume].join(","); }));
    var a=document.createElement("a"); a.href=URL.createObjectURL(new Blob([rows.join("\n")],{type:"text/csv"})); a.download=active+".csv"; a.click();
  }
  function countdown(last){
    var el=document.getElementById("cd"); if(!el) return;
    var sec= spec(tf)[0]==="1d"?86400: spec(tf)[0]==="1w"?604800: spec(tf)[0]==="1h"?3600: spec(tf)[0]==="1m"?60:86400;
    var end=last.time+sec, left=end-Math.floor(Date.now()/1000);
    if(left<0) left=0;
    var h=Math.floor(left/3600), m=Math.floor((left%3600)/60), s=left%60;
    el.textContent="bar "+h+":"+String(m).padStart(2,"0")+":"+String(s).padStart(2,"0");
  }

  chart.subscribeCrosshairMove(function(param){
    var hud=document.getElementById("hud");
    if(!param||!param.time||!mainSeries){ hud.style.display="none"; return; }
    var d=param.seriesData.get(mainSeries); if(!d){ hud.style.display="none"; return; }
    var o=d.open!=null?d.open:d.value, h=d.high!=null?d.high:d.value, l=d.low!=null?d.low:d.value, c=d.close!=null?d.close:d.value;
    hud.style.display="block";
    hud.innerHTML=new Date(param.time*1000).toISOString().slice(0,16).replace("T"," ")+"  O "+fmt(o)+" H "+fmt(h)+" L "+fmt(l)+" C "+fmt(c);
  });
  chart.timeScale().subscribeVisibleLogicalRangeChange(function(){ drawSVG(); });
  document.getElementById("chart").addEventListener("click", onChartClick);
  document.getElementById("chart").addEventListener("mousemove", function(ev){
    if(tool!=="brush"||!pending||!(ev.buttons&1)) return;
    onChartClick(ev);
  });

  document.addEventListener("keydown", function(e){
    if(e.target && /input|textarea|select/i.test(e.target.tagName)) {
      if(e.key==="Escape") e.target.blur();
      return;
    }
    if(e.key==="/" && !e.metaKey && !e.ctrlKey){ e.preventDefault(); document.getElementById("q").focus(); }
    if(e.key==="Escape"){ tool="cursor"; pending=null; document.getElementById("modal").className=""; renderRail(); }
    if(e.altKey && (e.key==="t"||e.key==="T")) { tool="trend"; renderRail(); }
    if(e.altKey && (e.key==="h"||e.key==="H")) { tool="hline"; renderRail(); }
    if(e.altKey && (e.key==="f"||e.key==="F")) { tool="fib"; renderRail(); }
    if(e.altKey && (e.key==="v"||e.key==="V")) { tool="vline"; renderRail(); }
    if((e.ctrlKey||e.metaKey) && e.key==="z"){ e.preventDefault(); undoDraw(); }
    if((e.ctrlKey||e.metaKey) && e.key==="s"){ e.preventDefault(); shot(); }
    if(e.key===" "){ e.preventDefault(); if(replay.on) document.getElementById("rp-play").click(); else startReplay(); }
  });

  function clock(){ var el=document.getElementById("clock"); if(el) el.textContent=new Date().toISOString().slice(11,19)+" UTC"; if(lastBars.length) countdown(lastBars[lastBars.length-1]); }
  loadAlerts();
  var lay=loadJSON(LAY_KEY,null); if(lay){ if(lay.gridOn!=null) gridOn=lay.gridOn; if(lay.magnet!=null) magnet=lay.magnet; if(lay.kind) kind=lay.kind; if(lay.tf) tf=lay.tf; }
  loadDraw();
  renderTabs(); renderTf(); renderRail(); renderLetters(); renderWtabs(); renderLegend();
  loadLists().then(renderList);
  loadIntel(); loadNews();
  TABS.forEach(function(s){ lastPx(s).then(function(px){ if(px){ quotes[s]=px; renderTabs(); } }); });
  load();
  clock(); setInterval(clock,1000);
  if(window.Notification && Notification.permission==="default") Notification.requestPermission().catch(function(){});
})();
