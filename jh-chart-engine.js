/* JustHodl Chart engine v5 — TradingView desk. Does not touch Chart Pro. */
(function () {
  if (window.__jhChartEngineV5) return;
  window.__jhChartEngineV5 = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var LIVE = "https://justhodl.ai";
  var TFS = [["1s","1s","1m","1d"],["1m","1m","1m","5d"],["5m","5m","5m","1mo"],["15m","15m","15m","3mo"],["1h","1h","60m","2y"],["4h","4h","60m","2y"],["12h","12h","60m","2y"],["1d","D","1d","5y"],["2d","2D","1d","5y"],["5d","5D","1d","5y"],["1w","W","1wk","10y"],["2w","2W","1wk","10y"],["1M","M","1mo","10y"],["3M","3M","1d","10y"]];
  var CHG = [["price","Price"],["dod","DoD"],["wow","WoW"],["mom","MoM"],["qoq","QoQ"],["yoy","YoY"],["ytd","YTD"],["fromhigh","From High"],["fromlow","From Low"],["vsspy","vs SPY"]];
  var BARS = { dod:1, wow:5, mom:21, qoq:63, yoy:252 };
  var TABS = ["BTCUSDT","ETHB","PEPEUSDT","CNEQ","PURR","BMNR","ATO"];
  var ISHARES = ["GSG","COMT","EWZS","CMDY","EWZ","LOCK","IAT","IVV","IWM","EEM","LQD","HYG","TLT","IEI"];
  var KINDS = [["candles","Candles"],["hollow","Hollow"],["bars","Bars"],["line","Line"],["area","Area"],["baseline","Baseline"],["heikin","Heikin"],["columns","Columns"],["step","Step"],["renko","Renko"],["kagi","Kagi"],["linebreak","Line break"]];
  var SCALES = [["0","Linear"],["1","Log"],["2","Percent"],["3","Index"]];
  var TZS = [["UTC",0],["New York",-4],["London",1],["Tokyo",9],["Hong Kong",8]];
  var COLORS = ["#2962ff","#089981","#f23645","#ff6d00","#ab47bc","#26c6da","#e91e63","#7e57c2","#131722","#f0b429"];
  var GROUPS = [
    {id:"cur",n:"Cursors",g:"+",tools:[["cursor","+","Cursor"],["cross","✚","Crosshair"],["eraser","⌫","Eraser"],["zoom","▣","Zoom"]]},
    {id:"tr",n:"Trend",g:"/",tools:[["trend","/","Trend Alt+T"],["ray","↗","Ray"],["infoline","ℹ","Info line"],["extended","↔","Extended"],["hline","—","Horiz Alt+H"],["hray","→","Horiz ray"],["vline","|","Vert Alt+V"],["crossline","+","Cross line"],["angle","∠","Trend angle"]]},
    {id:"ch",n:"Channels",g:"//",tools:[["channel","//","Parallel"],["disjoint","⫽","Disjoint"],["flattop","⊏","Flat top/bottom"]]},
    {id:"fk",n:"Pitchfork",g:"Ψ",tools:[["pitchfork","Ψ","Andrews"],["schiff","⋔","Schiff"]]},
    {id:"fib",n:"Fib / Gann",g:"Fib",tools:[["fib","Fib","Retracement Alt+F"],["fibext","Ext","Extension"],["fibfan","Fan","Fan"],["fibtz","TZ","Time zones"],["fibarc","Arc","Arcs"],["fibch","Ch","Channel"],["gannfan","G","Gann fan"],["gannbox","□","Gann box"]]},
    {id:"geo",n:"Shapes",g:"▢",tools:[["rect","▢","Rect"],["rrect","▭","Rotated rect"],["ellipse","◯","Ellipse"],["circle","○","Circle"],["triangle","△","Triangle"],["polyline","⌇","Polyline"],["arc","⌒","Arc"]]},
    {id:"ann",n:"Annotate",g:"T",tools:[["text","T","Text"],["callout","💬","Callout"],["pricelbl","P","Price label"],["note","N","Note"],["flag","⚑","Flag"],["arrow","→","Arrow"],["arrowmark","➤","Arrow marker"]]},
    {id:"ms",n:"Measure",g:"Δ",tools:[["measure","Δ","Measure"],["prange","↕","Price range"],["drange","↔","Date range"],["longpos","L","Long position"],["shortpos","S","Short position"]]},
    {id:"br",n:"Brush",g:"~",tools:[["brush","~","Brush"],["highlight","H","Highlighter"]]},
    {id:"pt",n:"Patterns",g:"X",tools:[["xabcd","X","XABCD"],["hs","HS","Head & Shoulders"],["elliott","E","Elliott impulse"]]}
  ];
  var INDS = [
    {id:"sma9",n:"SMA 9",c:"#26c6da",on:0,k:"sma",p:9,cat:"MA"},
    {id:"sma20",n:"SMA 20",c:"#26c6da",on:1,k:"sma",p:20,cat:"MA"},
    {id:"sma50",n:"SMA 50",c:"#2962ff",on:1,k:"sma",p:50,cat:"MA"},
    {id:"sma100",n:"SMA 100",c:"#7e57c2",on:0,k:"sma",p:100,cat:"MA"},
    {id:"sma200",n:"SMA 200",c:"#ff6d00",on:1,k:"sma",p:200,cat:"MA"},
    {id:"sma250",n:"SMA 250",c:"#e91e63",on:1,k:"sma",p:250,cat:"MA"},
    {id:"ema9",n:"EMA 9",c:"#26a69a",on:0,k:"ema",p:9,cat:"MA"},
    {id:"ema21",n:"EMA 21",c:"#42a5f5",on:0,k:"ema",p:21,cat:"MA"},
    {id:"ema50",n:"EMA 50",c:"#5c6bc0",on:0,k:"ema",p:50,cat:"MA"},
    {id:"ema200",n:"EMA 200",c:"#8d6e63",on:0,k:"ema",p:200,cat:"MA"},
    {id:"ema250",n:"EMA 250",c:"#089981",on:1,k:"ema",p:250,cat:"MA"},
    {id:"wma20",n:"WMA 20",c:"#78909c",on:0,k:"wma",p:20,cat:"MA"},
    {id:"hull20",n:"Hull 20",c:"#00897b",on:0,k:"hull",p:20,cat:"MA"},
    {id:"vwma20",n:"VWMA 20",c:"#42a5f5",on:0,k:"vwma",p:20,cat:"MA"},
    {id:"vwap",n:"VWAP",c:"#ab47bc",on:0,k:"vwap",cat:"Volume"},
    {id:"bb",n:"Bollinger 20",c:"#ab47bc",on:0,k:"bb",cat:"Channel"},
    {id:"kc",n:"Keltner 20",c:"#5c6bc0",on:0,k:"kc",cat:"Channel"},
    {id:"dc",n:"Donchian 20",c:"#00897b",on:0,k:"dc",cat:"Channel"},
    {id:"env",n:"Envelopes 20",c:"#ff7043",on:0,k:"env",cat:"Channel"},
    {id:"st",n:"Supertrend",c:"#26a69a",on:0,k:"st",cat:"Trend"},
    {id:"sar",n:"Parabolic SAR",c:"#ef6c00",on:0,k:"sar",cat:"Trend"},
    {id:"ich",n:"Ichimoku",c:"#5c6bc0",on:0,k:"ich",cat:"Trend"},
    {id:"piv",n:"Pivots (classic)",c:"#546e7a",on:0,k:"piv",cat:"Levels"},
    {id:"allig",n:"Alligator",c:"#43a047",on:0,k:"allig",cat:"Trend"},
    {id:"zz",n:"ZigZag 5%",c:"#6d4c41",on:0,k:"zz",cat:"Trend"},
    {id:"linreg",n:"LinReg 20",c:"#3949ab",on:0,k:"linreg",p:20,cat:"Trend"},
    {id:"pc",n:"Prev close",c:"#787b86",on:1,k:"pc",cat:"Levels"}
  ];
  var OSC = [
    {id:"rsi",n:"RSI 14",on:0,cat:"Momentum"},
    {id:"stoch",n:"Stochastic 14,3",on:0,cat:"Momentum"},
    {id:"stochrsi",n:"Stoch RSI",on:0,cat:"Momentum"},
    {id:"macd",n:"MACD 12,26,9",on:0,cat:"Momentum"},
    {id:"cci",n:"CCI 20",on:0,cat:"Momentum"},
    {id:"willr",n:"Williams %R",on:0,cat:"Momentum"},
    {id:"mfi",n:"MFI 14",on:0,cat:"Volume"},
    {id:"obv",n:"OBV",on:0,cat:"Volume"},
    {id:"ad",n:"A/D",on:0,cat:"Volume"},
    {id:"cmf",n:"CMF 20",on:0,cat:"Volume"},
    {id:"atr",n:"ATR 14",on:0,cat:"Volatility"},
    {id:"adx",n:"ADX 14",on:0,cat:"Trend"},
    {id:"ao",n:"Awesome Osc",on:0,cat:"Momentum"},
    {id:"mom",n:"Momentum 10",on:0,cat:"Momentum"},
    {id:"roc",n:"ROC 12",on:0,cat:"Momentum"},
    {id:"aroon",n:"Aroon 25",on:0,cat:"Trend"},
    {id:"uo",n:"Ultimate Osc",on:0,cat:"Momentum"},
    {id:"trix",n:"TRIX 15",on:0,cat:"Momentum"},
    {id:"chaikin",n:"Chaikin Osc",on:0,cat:"Volume"},
    {id:"force",n:"Force Index",on:0,cat:"Volume"},
    {id:"ppo",n:"PPO",on:0,cat:"Momentum"},
    {id:"tsi",n:"TSI",on:0,cat:"Momentum"},
    {id:"dpo",n:"DPO 20",on:0,cat:"Momentum"}
  ];
  var UP="#089981", DN="#f23645", BG="#ffffff", ACC="#2962ff";
  var CUSTOM_KEY="jh-chart-custom-lists", LAY_KEY="jh-chart-v5-layout", ALERT_KEY="jh-chart-alerts", DRAW_KEY="jh-chart-drawings", NOTE_KEY="jh-chart-notes", FLAG_KEY="jh-chart-flags", TPL_KEY="jh-chart-templates";
  var active="PEPEUSDT", tf="1d", mode="price", kind="candles", scaleMode=0;
  var quotes={}, lists=[], listId="ishares", letter="", filter="", sortCol="sym", sortDir=1;
  var lastBars=[], series=[], spyBars=null, barCache={}, compare=[], mainSeries=null;
  var drawings=[], undo=[], redo=[], tool="cursor", magnet=true, pending=null, objOpen=false, vpOn=false;
  var stayTool=false, hideDraw=false, lockDraw=false, drawColor=ACC, drawW=1.2;
  var wtab="list", watchOpen=true, layout=1, gridOn=true, watermark=true, invert=false, hiLo=true, crossMode=0;
  var tzOff=0, tzName="UTC", notes={}, flags={};
  var replay={on:false,i:0,speed:1,timer:null,full:[]};
  var alerts=[], news=[], toastT=null, selDraw=null, paneCharts=[], paneSyms=[];
  var chart, chart2, chart3, chart4, oscChart, oscSeries=[];

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
  function fmtVol(v){ if(v>=1e12) return (v/1e12).toFixed(2)+"T"; if(v>=1e9) return (v/1e9).toFixed(2)+"B"; if(v>=1e6) return (v/1e6).toFixed(2)+"M"; if(v>=1e3) return (v/1e3).toFixed(1)+"K"; return String(Math.round(v||0)); }
  function toast(m){ var el=document.getElementById("toast"); el.textContent=m; el.className="on"; clearTimeout(toastT); toastT=setTimeout(function(){ el.className=""; }, 2800); }
  function uid(){ return "d"+Math.random().toString(36).slice(2,8); }
  function spec(tfId){ for(var i=0;i<TFS.length;i++) if(TFS[i][0]===tfId) return TFS[i]; return TFS[7]; }
  function loadJSON(k, fb){ try{ var x=JSON.parse(localStorage.getItem(k)||""); return x||fb; }catch(e){ return fb; } }
  function saveJSON(k,v){ try{ localStorage.setItem(k, JSON.stringify(v)); }catch(e){} }
  function allTools(){ var a=[]; GROUPS.forEach(function(g){ a=a.concat(g.tools); }); return a; }
  function toolName(k){ var t=allTools().filter(function(x){return x[0]===k;})[0]; return t?t[2]:k; }
  function needPts(k){
    if(/^(hline|vline|hray|text|pricelbl|note|flag|arrowmark)$/.test(k)) return 1;
    if(/^(channel|disjoint|flattop|pitchfork|schiff|triangle)$/.test(k)) return 3;
    if(k==="xabcd") return 5; if(k==="elliott") return 6; if(k==="hs") return 7;
    if(k==="polyline"||k==="brush"||k==="highlight") return 99;
    if(k==="cursor"||k==="cross"||k==="eraser") return 0;
    return 2;
  }

  function sma(d,n){ var o=[],s=0; for(var i=0;i<d.length;i++){ s+=d[i].close; if(i>=n)s-=d[i-n].close; if(i>=n-1)o.push({time:d[i].time,value:s/n}); } return o; }
  function ema(d,n){ if(!d.length)return[]; var o=[],k=2/(n+1),p=d[0].close; for(var i=0;i<d.length;i++){ p=d[i].close*k+p*(1-k); if(i>=n-1)o.push({time:d[i].time,value:p}); } return o; }
  function wma(d,n){ var o=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n)continue; var s=0,w=0; for(j=0;j<n;j++){ var ww=j+1; s+=d[i-n+1+j].close*ww; w+=ww; } o.push({time:d[i].time,value:s/w}); } return o; }
  function hull(d,n){ var n2=Math.max(1,Math.round(n/2)), ns=Math.max(1,Math.round(Math.sqrt(n))); var e1=wma(d,n2), e2=wma(d,n), map={},i; for(i=0;i<e2.length;i++) map[e2[i].time]=e2[i].value; var raw=[]; for(i=0;i<e1.length;i++) if(map[e1[i].time]!=null) raw.push({time:e1[i].time,close:2*e1[i].value-map[e1[i].time]}); return wma(raw,ns); }
  function vwma(d,n){ var o=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n)continue; var pv=0,vv=0; for(j=i-n+1;j<=i;j++){ pv+=d[j].close*d[j].volume; vv+=d[j].volume; } if(vv)o.push({time:d[i].time,value:pv/vv}); } return o; }
  function vwap(d){ var o=[],pv=0,vv=0,i; for(i=0;i<d.length;i++){ var tp=(d[i].high+d[i].low+d[i].close)/3; pv+=tp*d[i].volume; vv+=d[i].volume; if(vv)o.push({time:d[i].time,value:pv/vv}); } return o; }
  function atr(d,n){ var o=[],tr=[],i; for(i=0;i<d.length;i++){ var prev=i?d[i-1].close:d[i].close; tr.push(Math.max(d[i].high-d[i].low, Math.abs(d[i].high-prev), Math.abs(d[i].low-prev))); } var s=0; for(i=0;i<tr.length;i++){ s+=tr[i]; if(i>=n)s-=tr[i-n]; if(i>=n-1)o.push({time:d[i].time,value:s/n}); } return o; }
  function rsi(d,n){ var o=[],g=0,l=0,i; for(i=1;i<d.length;i++){ var ch=d[i].close-d[i-1].close, gv=Math.max(ch,0), lv=Math.max(-ch,0); if(i<=n){ g+=gv; l+=lv; if(i===n){ g/=n; l/=n; o.push({time:d[i].time,value:l?100-100/(1+g/l):100}); } } else { g=(g*(n-1)+gv)/n; l=(l*(n-1)+lv)/n; o.push({time:d[i].time,value:l?100-100/(1+g/l):100}); } } return o; }
  function macd(d){ var e12=ema(d,12), e26=ema(d,26), m=[], map={},i; for(i=0;i<e26.length;i++) map[e26[i].time]=e26[i].value; for(i=0;i<e12.length;i++) if(map[e12[i].time]!=null) m.push({time:e12[i].time,value:e12[i].value-map[e12[i].time]}); var sig=ema(m.map(function(p){return {time:p.time,close:p.value};}),9), sm={}; for(i=0;i<sig.length;i++) sm[sig[i].time]=sig[i].value; var hist=[]; for(i=0;i<m.length;i++) if(sm[m[i].time]!=null) hist.push({time:m[i].time, macd:m[i].value, signal:sm[m[i].time], hist:m[i].value-sm[m[i].time]}); return hist; }
  function stoch(d,n,k,dper){ k=k||3; var kv=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n) continue; var hi=-1e99,lo=1e99; for(j=i-n+1;j<=i;j++){ if(d[j].high>hi)hi=d[j].high; if(d[j].low<lo)lo=d[j].low; } kv.push({time:d[i].time,value:hi===lo?50:((d[i].close-lo)/(hi-lo))*100}); } return sma(kv.map(function(p){return {time:p.time,close:p.value};}),k); }
  function stochRsi(d){ var r=rsi(d,14); return stoch(r.map(function(p){return {time:p.time,high:p.value,low:p.value,close:p.value};}),14,3); }
  function heikin(d){ var o=[],i; for(i=0;i<d.length;i++){ var b=d[i], hc=(b.open+b.high+b.low+b.close)/4, ho=i? (o[i-1].open+o[i-1].close)/2 : (b.open+b.close)/2; o.push({time:b.time,open:ho,high:Math.max(b.high,ho,hc),low:Math.min(b.low,ho,hc),close:hc,volume:b.volume}); } return o; }
  function supertrend(d,n,m){ n=n||10; m=m||3; var a=atr(d,n), o=[],i,map={}; for(i=0;i<a.length;i++) map[a[i].time]=a[i].value; var up=0,dn=0,dir=1; for(i=0;i<d.length;i++){ var at=map[d[i].time]; if(at==null) continue; var mid=(d[i].high+d[i].low)/2, bu=mid+m*at, bd=mid-m*at; if(i){ if(bd>up) up=bd; else if(d[i-1].close<up) up=bd; if(bu<dn) dn=bu; else if(d[i-1].close>dn) dn=bu; } else { up=bd; dn=bu; } if(d[i].close>dn) dir=1; else if(d[i].close<up) dir=-1; o.push({time:d[i].time,value:dir>0?up:dn}); } return o; }
  function ichimoku(d){ function mid(len,i){ var hi=-1e99,lo=1e99,j; for(j=Math.max(0,i-len+1);j<=i;j++){ if(d[j].high>hi)hi=d[j].high; if(d[j].low<lo)lo=d[j].low; } return (hi+lo)/2; } var conv=[],base=[],spanA=[],spanB=[],i; for(i=0;i<d.length;i++){ var c=mid(9,i), b=mid(26,i); conv.push({time:d[i].time,value:c}); base.push({time:d[i].time,value:b}); spanA.push({time:d[i].time,value:(c+b)/2}); spanB.push({time:d[i].time,value:mid(52,i)}); } return {conv:conv,base:base,spanA:spanA,spanB:spanB}; }
  function sar(d,af0,afm){ af0=af0||0.02; afm=afm||0.2; if(d.length<3) return []; var o=[], up=d[1].close>=d[0].close, ep=up?d[1].high:d[1].low, af=af0, s=up?d[0].low:d[0].high, i; o.push({time:d[0].time,value:s}); for(i=1;i<d.length;i++){ s=s+af*(ep-s); if(up){ if(d[i].low<s){ up=false; s=ep; ep=d[i].low; af=af0; } else { if(d[i].high>ep){ ep=d[i].high; af=Math.min(afm,af+af0); } } } else { if(d[i].high>s){ up=true; s=ep; ep=d[i].high; af=af0; } else { if(d[i].low<ep){ ep=d[i].low; af=Math.min(afm,af+af0); } } } o.push({time:d[i].time,value:s}); } return o; }
  function bbands(d,n,k){ n=n||20; k=k||2; var m=sma(d,n), up=[],dn=[],i,j; for(i=n-1;i<d.length;i++){ var ss=0, mv=m[i-(n-1)].value; for(j=0;j<n;j++){ var dv=d[i-j].close-mv; ss+=dv*dv; } var sd=Math.sqrt(ss/n); up.push({time:d[i].time,value:mv+k*sd}); dn.push({time:d[i].time,value:mv-k*sd}); } return {m:m,up:up,dn:dn}; }
  function keltner(d,n){ n=n||20; var m=ema(d,n), a=atr(d,n), map={},i,up=[],dn=[]; for(i=0;i<a.length;i++) map[a[i].time]=a[i].value; for(i=0;i<m.length;i++){ var at=map[m[i].time]; if(at==null) continue; up.push({time:m[i].time,value:m[i].value+2*at}); dn.push({time:m[i].time,value:m[i].value-2*at}); } return {m:m,up:up,dn:dn}; }
  function donchian(d,n){ n=n||20; var up=[],dn=[],mid=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n) continue; var hi=-1e99,lo=1e99; for(j=i-n+1;j<=i;j++){ if(d[j].high>hi)hi=d[j].high; if(d[j].low<lo)lo=d[j].low; } up.push({time:d[i].time,value:hi}); dn.push({time:d[i].time,value:lo}); mid.push({time:d[i].time,value:(hi+lo)/2}); } return {m:mid,up:up,dn:dn}; }
  function envelope(d,n,pct){ n=n||20; pct=pct||0.025; var m=sma(d,n), up=[],dn=[],i; for(i=0;i<m.length;i++){ up.push({time:m[i].time,value:m[i].value*(1+pct)}); dn.push({time:m[i].time,value:m[i].value*(1-pct)}); } return {m:m,up:up,dn:dn}; }
  function pivots(d){ if(d.length<2) return []; var b=d[d.length-2], pp=(b.high+b.low+b.close)/3, r1=2*pp-b.low, s1=2*pp-b.high, r2=pp+(b.high-b.low), s2=pp-(b.high-b.low); var t=d[d.length-1].time; return {pp:{time:t,value:pp},r1:{time:t,value:r1},s1:{time:t,value:s1},r2:{time:t,value:r2},s2:{time:t,value:s2}}; }
  function alligator(d){ function smma(arr,n){ return ema(arr,n); } function med(shift,n){ var o=[],i; for(i=0;i<d.length;i++){ var j=i-shift; if(j<0) continue; o.push({time:d[i].time,close:(d[j].high+d[j].low)/2}); } return sma(o,n); } return {jaw:med(8,13),teeth:med(5,8),lips:med(3,5)}; }
  function zigzag(d,pct){ pct=pct||0.05; var o=[], last=d[0], dir=0, i; o.push({time:d[0].time,value:d[0].close}); for(i=1;i<d.length;i++){ var ch=(d[i].close-last.close)/last.close; if(dir>=0 && ch<=-pct){ o.push({time:d[i].time,value:d[i].close}); last=d[i]; dir=-1; } else if(dir<=0 && ch>=pct){ o.push({time:d[i].time,value:d[i].close}); last=d[i]; dir=1; } } o.push({time:d[d.length-1].time,value:d[d.length-1].close}); return o; }
  function linreg(d,n){ n=n||20; var o=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n) continue; var sx=0,sy=0,sxy=0,sx2=0; for(j=0;j<n;j++){ sx+=j; sy+=d[i-n+1+j].close; sxy+=j*d[i-n+1+j].close; sx2+=j*j; } var den=n*sx2-sx*sx, sl=den?(n*sxy-sx*sy)/den:0, ic=(sy-sl*sx)/n; o.push({time:d[i].time,value:ic+sl*(n-1)}); } return o; }
  function cci(d,n){ n=n||20; var tp=d.map(function(b){return {time:b.time,close:(b.high+b.low+b.close)/3};}), m=sma(tp,n), o=[],i,j,map={}; for(i=0;i<m.length;i++) map[m[i].time]=m[i].value; for(i=n-1;i<d.length;i++){ var mean=map[d[i].time], md=0; for(j=0;j<n;j++) md+=Math.abs(tp[i-n+1+j].close-mean); md/=n; o.push({time:d[i].time,value:md? (tp[i].close-mean)/(0.015*md):0}); } return o; }
  function willr(d,n){ n=n||14; var o=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n) continue; var hi=-1e99,lo=1e99; for(j=i-n+1;j<=i;j++){ if(d[j].high>hi)hi=d[j].high; if(d[j].low<lo)lo=d[j].low; } o.push({time:d[i].time,value:hi===lo?0: -100*(hi-d[i].close)/(hi-lo)}); } return o; }
  function mfi(d,n){ n=n||14; var o=[],pos=[],neg=[],i; for(i=1;i<d.length;i++){ var tp=(d[i].high+d[i].low+d[i].close)/3, ptp=(d[i-1].high+d[i-1].low+d[i-1].close)/3, mf=tp*d[i].volume; pos.push(tp>ptp?mf:0); neg.push(tp<ptp?mf:0); if(i>=n){ var ps=0,ng=0,j; for(j=i-n;j<i;j++){ ps+=pos[j]; ng+=neg[j]; } o.push({time:d[i].time,value:ng?100-100/(1+ps/ng):100}); } } return o; }
  function obv(d){ var o=[],v=0,i; for(i=0;i<d.length;i++){ if(i) v+= d[i].close>=d[i-1].close? d[i].volume : -d[i].volume; o.push({time:d[i].time,value:v}); } return o; }
  function adline(d){ var o=[],v=0,i; for(i=0;i<d.length;i++){ var hl=d[i].high-d[i].low; v+= hl? ((d[i].close-d[i].low)-(d[i].high-d[i].close))/hl*d[i].volume : 0; o.push({time:d[i].time,value:v}); } return o; }
  function cmf(d,n){ n=n||20; var o=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n) continue; var mf=0,vol=0; for(j=i-n+1;j<=i;j++){ var hl=d[j].high-d[j].low; mf+= hl? ((d[j].close-d[j].low)-(d[j].high-d[j].close))/hl*d[j].volume : 0; vol+=d[j].volume; } o.push({time:d[i].time,value:vol?mf/vol:0}); } return o; }
  function adx(d,n){ n=n||14; var tr=[],pdm=[],mdm=[],i; for(i=1;i<d.length;i++){ var up=d[i].high-d[i-1].high, dn=d[i-1].low-d[i].low; pdm.push(up>dn&&up>0?up:0); mdm.push(dn>up&&dn>0?dn:0); tr.push(Math.max(d[i].high-d[i].low, Math.abs(d[i].high-d[i-1].close), Math.abs(d[i].low-d[i-1].close))); } function wild(a,n){ var o=[],s=0,i; for(i=0;i<a.length;i++){ if(i<n){ s+=a[i]; if(i===n-1) o.push(s/n); } else { s=s-(s/n)+a[i]; o.push(s); } } return o; } var str=wild(tr,n), sp=wild(pdm,n), sm=wild(mdm,n), dx=[],o=[]; for(i=0;i<str.length;i++){ var pdi=str[i]?100*sp[i]/str[i]:0, mdi=str[i]?100*sm[i]/str[i]:0, sum=pdi+mdi; dx.push(sum?100*Math.abs(pdi-mdi)/sum:0); } var ad=wild(dx,n); for(i=0;i<ad.length;i++) o.push({time:d[i+n*2-1]&&d[i+n*2-1].time || d[d.length-1].time, value:ad[i]}); return o.filter(function(p){return p.time;}); }
  function ao(d){ var mid=d.map(function(b){return {time:b.time,close:(b.high+b.low)/2};}); var f=sma(mid,5), s=sma(mid,34), map={},i,o=[]; for(i=0;i<s.length;i++) map[s[i].time]=s[i].value; for(i=0;i<f.length;i++) if(map[f[i].time]!=null) o.push({time:f[i].time,value:f[i].value-map[f[i].time]}); return o; }
  function mom(d,n){ n=n||10; var o=[],i; for(i=n;i<d.length;i++) o.push({time:d[i].time,value:d[i].close-d[i-n].close}); return o; }
  function roc(d,n){ n=n||12; var o=[],i; for(i=n;i<d.length;i++) if(d[i-n].close) o.push({time:d[i].time,value:(d[i].close/d[i-n].close-1)*100}); return o; }
  function aroon(d,n){ n=n||25; var up=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n) continue; var hi=-1e99,lo=1e99,ih=i,il=i; for(j=i-n+1;j<=i;j++){ if(d[j].high>=hi){hi=d[j].high;ih=j;} if(d[j].low<=lo){lo=d[j].low;il=j;} } up.push({time:d[i].time,value:100*(n-(i-ih))/n}); } return up; }
  function uo(d){ function avg(bp,tr,n){ var s1=0,s2=0,i; for(i=d.length-n;i<d.length;i++){ if(i<1) continue; s1+=bp[i]; s2+=tr[i]; } return s2?s1/s2:0; } var bp=[],tr=[],i; bp[0]=0; tr[0]=d[0].high-d[0].low; for(i=1;i<d.length;i++){ var mn=Math.min(d[i].low,d[i-1].close); bp.push(d[i].close-mn); tr.push(Math.max(d[i].high,d[i-1].close)-mn); } var o=[],i2; for(i2=28;i2<d.length;i2++){ var slice=d.slice(0,i2+1), b=bp.slice(0,i2+1), t=tr.slice(0,i2+1); var a7=avg(b,t,7), a14=avg(b,t,14), a28=avg(b,t,28); o.push({time:d[i2].time,value:100*(4*a7+2*a14+a28)/7}); } return o; }
  function trix(d,n){ n=n||15; var e1=ema(d,n), e2=ema(e1.map(function(p){return {time:p.time,close:p.value};}),n), e3=ema(e2.map(function(p){return {time:p.time,close:p.value};}),n), o=[],i; for(i=1;i<e3.length;i++) if(e3[i-1].value) o.push({time:e3[i].time,value:(e3[i].value-e3[i-1].value)/e3[i-1].value*100}); return o; }
  function chaikin(d){ var ad=adline(d); var e3=ema(ad.map(function(p){return {time:p.time,close:p.value};}),3), e10=ema(ad.map(function(p){return {time:p.time,close:p.value};}),10), map={},i,o=[]; for(i=0;i<e10.length;i++) map[e10[i].time]=e10[i].value; for(i=0;i<e3.length;i++) if(map[e3[i].time]!=null) o.push({time:e3[i].time,value:e3[i].value-map[e3[i].time]}); return o; }
  function force(d,n){ n=n||13; var raw=[],i; for(i=1;i<d.length;i++) raw.push({time:d[i].time,close:(d[i].close-d[i-1].close)*d[i].volume}); return ema(raw,n); }
  function ppo(d){ var e12=ema(d,12), e26=ema(d,26), map={},i,o=[]; for(i=0;i<e26.length;i++) map[e26[i].time]=e26[i].value; for(i=0;i<e12.length;i++) if(map[e12[i].time]) o.push({time:e12[i].time,value:(e12[i].value-map[e12[i].time])/map[e12[i].time]*100}); return o; }
  function tsi(d){ var mom=[],i; for(i=1;i<d.length;i++) mom.push({time:d[i].time,close:d[i].close-d[i-1].close}); var ds=ema(ema(mom,25),13), abs=ema(ema(mom.map(function(p){return {time:p.time,close:Math.abs(p.close)};}),25),13), map={},o=[]; for(i=0;i<abs.length;i++) map[abs[i].time]=abs[i].value; for(i=0;i<ds.length;i++) if(map[ds[i].time]) o.push({time:ds[i].time,value:100*ds[i].value/map[ds[i].time]}); return o; }
  function dpo(d,n){ n=n||20; var m=sma(d,n), shift=Math.floor(n/2)+1, o=[],i; for(i=0;i<m.length;i++){ var idx=i+(n-1)-shift; if(idx>=0 && idx<d.length) o.push({time:d[idx].time,value:d[idx].close-m[i].value}); } return o; }
  function toRenko(d,pct){ pct=pct||0.01; if(d.length<2) return d; var brick=Math.max(d[d.length-1].close*pct, 1e-12), o=[], last=d[0].close, t0=d[0].time, step=Math.max(1, Math.floor((d[d.length-1].time-d[0].time)/Math.max(d.length,2))); function push(dir){ var open=last, close=last+dir*brick; o.push({time:t0+o.length*step, open:open, close:close, high:Math.max(open,close), low:Math.min(open,close), volume:0}); last=close; } for(var i=1;i<d.length;i++){ var c=d[i].close; while(c-last>=brick) push(1); while(last-c>=brick) push(-1); } return o.length?o:d; }
  function toLineBreak(d,n){ n=n||3; if(d.length<n+1) return d; var o=[d[0]], i; for(i=1;i<d.length;i++){ var slice=o.slice(-n), hi=Math.max.apply(null,slice.map(function(b){return b.high;})), lo=Math.min.apply(null,slice.map(function(b){return b.low;})); if(d[i].close>hi) o.push({time:d[i].time,open:o[o.length-1].close,close:d[i].close,high:d[i].close,low:o[o.length-1].close,volume:d[i].volume}); else if(d[i].close<lo) o.push({time:d[i].time,open:o[o.length-1].close,close:d[i].close,high:o[o.length-1].close,low:d[i].close,volume:d[i].volume}); } return o; }
  function toKagi(d,rev){ rev=rev||0.04; if(d.length<2) return []; var o=[{time:d[0].time,value:d[0].close,color:UP}], last=d[0].close, dir=0, i; for(i=1;i<d.length;i++){ var c=d[i].close, ch=(c-last)/last; if(dir>=0 && ch<=-rev){ o.push({time:d[i].time,value:c,color:DN}); last=c; dir=-1; } else if(dir<=0 && ch>=rev){ o.push({time:d[i].time,value:c,color:UP}); last=c; dir=1; } else if((dir>=0 && c>last) || (dir<=0 && c<last)){ last=c; o[o.length-1]={time:d[i].time,value:c,color:dir>=0?UP:DN}; } } return o; }

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
        out.push({time:+tm,open:+(b.open||b.o||c3),high:+(b.high||b.h||c3),low:+(b.low||b.l||c3),close:c3,volume:+(b.volume||b.v||0)});
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
  function warehouse(path){
    if(/justhodl\.ai$/i.test(location.hostname)) return [path, LIVE+path];
    return [LIVE+path];
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
      "/api/yahoo?ticker="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],
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
      rightPriceScale:{ borderColor:"#e0e3eb", scaleMargins:{ top:0.06, bottom:0.18 }, invertScaledValues:invert },
      timeScale:{ borderColor:"#e0e3eb", timeVisible:true, rightOffset:6 },
      crosshair:{ mode: crossMode },
      localization:{ priceFormatter:function(p){ return mode==="price"?fmt(p):p.toFixed(2)+"%"; } }
    });
  }
  chart=mkChart(host);
  function wipe(){ series.forEach(function(s){ try{ chart.removeSeries(s); }catch(e){} }); series=[]; mainSeries=null; }
  function addLine(pts, color, w){ if(!pts||!pts.length) return; var s=chart.addLineSeries({ color:color, lineWidth:w||1, lastValueVisible:false, priceLineVisible:false }); s.setData(pts); series.push(s); return s; }
  function addPriceLine(px, color, title){ if(!mainSeries||px==null) return; try{ mainSeries.createPriceLine({ price:px, color:color||"#787b86", lineWidth:1, lineStyle:2, axisLabelVisible:true, title:title||"" }); }catch(e){} }

  function displayBars(d){
    if(kind==="heikin") return heikin(d);
    if(kind==="renko") return toRenko(d, 0.01);
    if(kind==="linebreak") return toLineBreak(d, 3);
    return d;
  }
  async function paint(d){
    if(!d||!d.length){ document.getElementById("quote").textContent="No bars for "+active; return; }
    wipe(); lastBars=d;
    chart.applyOptions({
      localization:{ priceFormatter:function(p){ return mode==="price"?fmt(p):p.toFixed(2)+"%"; } },
      grid:{ vertLines:{color:gridOn?"#f0f3fa":"transparent"}, horzLines:{color:gridOn?"#f0f3fa":"transparent"} },
      rightPriceScale:{ invertScaledValues:invert },
      crosshair:{ mode: crossMode }
    });
    try{ chart.priceScale("right").applyOptions({ mode: scaleMode }); }catch(e){}
    var wm=document.getElementById("wm"); if(wm) wm.textContent=watermark?active:"";
    if(mode==="price"){
      var display=displayBars(d), c;
      if(kind==="kagi"){ c=chart.addLineSeries({color:UP,lineWidth:2}); c.setData(toKagi(d)); }
      else if(kind==="line"){ c=chart.addLineSeries({color:UP,lineWidth:2}); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
      else if(kind==="step"){ c=chart.addLineSeries({color:UP,lineWidth:2, lineType:1}); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
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
        if(ind.k==="wma") addLine(wma(d,ind.p), ind.c);
        if(ind.k==="hull") addLine(hull(d,ind.p), ind.c);
        if(ind.k==="vwma") addLine(vwma(d,ind.p), ind.c);
        if(ind.k==="vwap") addLine(vwap(d), ind.c);
        if(ind.k==="linreg") addLine(linreg(d,ind.p), ind.c);
        if(ind.k==="bb"){ var bb=bbands(d); addLine(bb.up,ind.c); addLine(bb.m,ind.c); addLine(bb.dn,ind.c); }
        if(ind.k==="kc"){ var kc=keltner(d); addLine(kc.up,ind.c); addLine(kc.m,ind.c); addLine(kc.dn,ind.c); }
        if(ind.k==="dc"){ var dc=donchian(d); addLine(dc.up,ind.c); addLine(dc.m,ind.c); addLine(dc.dn,ind.c); }
        if(ind.k==="env"){ var en=envelope(d); addLine(en.up,ind.c); addLine(en.m,ind.c); addLine(en.dn,ind.c); }
        if(ind.k==="st") addLine(supertrend(d), ind.c, 2);
        if(ind.k==="sar") addLine(sar(d), ind.c, 1);
        if(ind.k==="ich"){ var ich=ichimoku(d); addLine(ich.conv,"#2962ff"); addLine(ich.base,"#e91e63"); addLine(ich.spanA,"#26a69a"); addLine(ich.spanB,"#ab47bc"); }
        if(ind.k==="piv"){ var pv=pivots(d); if(pv.pp){ addPriceLine(pv.pp.value,"#546e7a","P"); addPriceLine(pv.r1.value,DN,"R1"); addPriceLine(pv.s1.value,UP,"S1"); addPriceLine(pv.r2.value,DN,"R2"); addPriceLine(pv.s2.value,UP,"S2"); } }
        if(ind.k==="allig"){ var ag=alligator(d); addLine(ag.jaw,"#2962ff"); addLine(ag.teeth,"#f23645"); addLine(ag.lips,"#089981"); }
        if(ind.k==="zz") addLine(zigzag(d), ind.c, 2);
        if(ind.k==="pc" && d.length>1) addPriceLine(d[d.length-2].close, "#787b86", "PDC");
      });
      for(var ci=0;ci<compare.length;ci++){
        try{
          var cb=await klines(compare[ci], tf);
          if(cb.length<2||d.length<2) continue;
          var t0=d[0].close, c0=cb[0].close;
          addLine(cb.map(function(b){ return {time:b.time, value: (b.close/c0)*t0 }; }), COLORS[(ci+3)%COLORS.length], 2);
        }catch(e){}
      }
      if(vpOn) drawVP(d); else { var cv=document.getElementById("vp"); if(cv){ cv.width=cv.height=0; } }
      if(hiLo && d.length){ var visHi=-1e99, visLo=1e99, hiT=d[0].time, loT=d[0].time, i2; for(i2=0;i2<d.length;i2++){ if(d[i2].high>visHi){visHi=d[i2].high;hiT=d[i2].time;} if(d[i2].low<visLo){visLo=d[i2].low;loT=d[i2].time;} } addPriceLine(visHi, UP, "H "+fmt(visHi)); addPriceLine(visLo, DN, "L "+fmt(visLo)); }
    } else {
      var pct= mode==="vsspy" ? await vsSpy(d) : computeChange(d, mode);
      var h=chart.addHistogramSeries({ priceFormat:{type:"percent"} });
      h.setData((pct||[]).map(function(p){ return {time:p.time,value:p.value,color:p.value>=0?UP:DN}; }));
      series.push(h); mainSeries=h;
    }
    chart.timeScale().fitContent();
    paintOsc(d);
    drawSVG();
    quoteUI(d);
    checkAlerts(active, d[d.length-1].close);
    countdown(d[d.length-1]);
    renderLegend();
    renderTech(d);
    document.getElementById("stat").textContent=d.length+" bars · "+kind+" · "+(scaleMode?SCALES[scaleMode][1]:"Linear");
  }
  function quoteUI(d){
    var last=d[d.length-1], prev=d[d.length-2]||last;
    var chg=prev.close?(last.close-prev.close)/prev.close:0, up=chg>=0;
    document.getElementById("quote").innerHTML="<b>"+active+" · "+tf+" · "+mode+"</b> <span>O "+fmt(last.open)+" H<span class=up> "+fmt(last.high)+"</span> L<span class=dn> "+fmt(last.low)+"</span> C<span class="+(up?"up":"dn")+"> "+fmt(last.close)+"</span></span> <span class="+(up?"up":"dn")+">"+(up?"+":"")+fmt(last.close-last.open)+" ("+(chg*100).toFixed(2)+"%)</span> <span>Vol "+fmtVol(last.volume)+"</span> <span style='margin-left:auto' class=sell>"+fmt(last.close)+" SELL</span> <span class=buy>"+fmt(last.close*1.001)+" BUY</span>";
    var hi=Math.max.apply(null,d.slice(-40).map(function(b){return b.high;}));
    var lo=Math.min.apply(null,d.slice(-40).map(function(b){return b.low;}));
    var yhi=Math.max.apply(null,d.map(function(b){return b.high;}));
    var ylo=Math.min.apply(null,d.map(function(b){return b.low;}));
    var dp=Math.min(100,Math.max(0,(last.close-lo)/(hi-lo||1)*100));
    var yp=Math.min(100,Math.max(0,(last.close-ylo)/(yhi-ylo||1)*100));
    document.getElementById("detail").innerHTML="<div style=font-weight:600>"+active+"</div><div class=px>"+fmt(last.close)+"</div><div class="+(up?"up":"dn")+">"+(up?"+":"")+fmt(last.close-prev.close)+" "+(chg*100).toFixed(2)+"%</div><div style='margin-top:8px;font-size:10px;color:var(--mut)'>DAY RANGE</div><div class=rg><i style=width:"+dp+"%></i><b style=left:"+dp+"%></b></div><div style=display:flex;justify-content:space-between;font-size:10px;font-family:IBM+Plex+Mono,monospace><span>"+fmt(lo)+"</span><span>"+fmt(hi)+"</span></div><div style='margin-top:8px;font-size:10px;color:var(--mut)'>52-WEEK RANGE</div><div class=rg><i style=width:"+yp+"%></i><b style=left:"+yp+"%></b></div><div style=display:flex;justify-content:space-between;font-size:10px;font-family:IBM+Plex+Mono,monospace><span>"+fmt(ylo)+"</span><span>"+fmt(yhi)+"</span></div>";
  }
  function paintOsc(d){
    var wrap=document.getElementById("oscwrap");
    var on=OSC.filter(function(o){ return o.on; });
    if(!on.length){ wrap.className=""; if(oscChart){ try{ oscChart.remove(); }catch(e){} oscChart=null; } return; }
    wrap.className="on";
    if(!oscChart) oscChart=LW.createChart(document.getElementById("osc"),{ autoSize:true, layout:{background:{type:"solid",color:BG},textColor:"#6a6d78",fontSize:10}, grid:{vertLines:{color:"#f0f3fa"},horzLines:{color:"#f0f3fa"}}, timeScale:{ visible:false }, rightPriceScale:{ borderColor:"#e0e3eb" } });
    oscSeries.forEach(function(s){ try{ oscChart.removeSeries(s); }catch(e){} }); oscSeries=[];
    function addO(fn, color){ var s=oscChart.addLineSeries({color:color||ACC,lineWidth:1}); s.setData(fn); oscSeries.push(s); }
    on.forEach(function(o){
      if(o.id==="rsi") addO(rsi(d,14), ACC);
      if(o.id==="macd"){ var m=macd(d); var h=oscChart.addHistogramSeries({}); h.setData(m.map(function(p){return {time:p.time,value:p.hist,color:p.hist>=0?UP:DN};})); var l1=oscChart.addLineSeries({color:ACC,lineWidth:1}); l1.setData(m.map(function(p){return {time:p.time,value:p.macd};})); var l2=oscChart.addLineSeries({color:"#ff6d00",lineWidth:1}); l2.setData(m.map(function(p){return {time:p.time,value:p.signal};})); oscSeries.push(h,l1,l2); }
      if(o.id==="stoch") addO(stoch(d,14,3,3), "#ab47bc");
      if(o.id==="stochrsi") addO(stochRsi(d), "#7e57c2");
      if(o.id==="atr") addO(atr(d,14), "#6a6d78");
      if(o.id==="cci") addO(cci(d,20), "#26c6da");
      if(o.id==="willr") addO(willr(d,14), "#ef6c00");
      if(o.id==="mfi") addO(mfi(d,14), "#00897b");
      if(o.id==="obv") addO(obv(d), "#5c6bc0");
      if(o.id==="ad") addO(adline(d), "#6d4c41");
      if(o.id==="cmf") addO(cmf(d,20), "#43a047");
      if(o.id==="adx") addO(adx(d,14), "#3949ab");
      if(o.id==="ao"){ var a=ao(d); var h2=oscChart.addHistogramSeries({}); h2.setData(a.map(function(p){return {time:p.time,value:p.value,color:p.value>=0?UP:DN};})); oscSeries.push(h2); }
      if(o.id==="mom") addO(mom(d,10), "#e91e63");
      if(o.id==="roc") addO(roc(d,12), "#ff6d00");
      if(o.id==="aroon") addO(aroon(d,25), "#089981");
      if(o.id==="uo") addO(uo(d), "#2962ff");
      if(o.id==="trix") addO(trix(d,15), "#7e57c2");
      if(o.id==="chaikin") addO(chaikin(d), "#8d6e63");
      if(o.id==="force") addO(force(d,13), "#c62828");
      if(o.id==="ppo") addO(ppo(d), "#00695c");
      if(o.id==="tsi") addO(tsi(d), "#4527a0");
      if(o.id==="dpo") addO(dpo(d,20), "#37474f");
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
  async function load(){ try{ var d=await klines(active,tf); if(replay.on) d=d.slice(0, replay.i||d.length); await paint(d); if(layout>1) paintPanes(); }catch(e){ document.getElementById("quote").textContent="Chart error: "+(e&&e.message||e); } }

  function paneEls(){ return [ ["chart2","host2",1], ["chart3","host3",2], ["chart4","host4",3] ]; }
  function setLayout(n){
    layout=n;
    var stage=document.getElementById("stage");
    stage.className = n===4?"split4": n===2?"split2":"";
    paneEls().forEach(function(p,i){ document.getElementById(p[0]).style.display = (n===2&&i===0)||n===4 ? "block":"none"; });
    if(n>1) paintPanes(); saveLay();
  }
  async function paintPanes(){
    var extras=TABS.filter(function(s){ return s!==active; });
    if(compare.length) extras=extras.concat(compare);
    extras=extras.filter(function(s,i,a){ return a.indexOf(s)===i; });
    var nShow=layout===4?3: layout===2?1:0;
    var refs=[chart2,chart3,chart4];
    for(var i=0;i<3;i++){
      var wrap=document.getElementById(paneEls()[i][0]), el=document.getElementById(paneEls()[i][1]);
      if(i>=nShow){ wrap.style.display="none"; continue; }
      wrap.style.display="block";
      var sym=extras[i]||"SPY";
      try{
        if(refs[i]){ try{ refs[i].remove(); }catch(e){} }
        refs[i]=mkChart(el);
        var d=await klines(sym,tf); if(d.length<2) continue;
        var c=refs[i].addCandlestickSeries({upColor:UP,downColor:DN,borderUpColor:UP,borderDownColor:DN,wickUpColor:UP,wickDownColor:DN});
        c.setData(d); refs[i].timeScale().fitContent();
      }catch(e){}
    }
    chart2=refs[0]; chart3=refs[1]; chart4=refs[2];
  }

  function saveDraw(){ var all=loadJSON(DRAW_KEY,{}); all[active]=drawings; saveJSON(DRAW_KEY, all); }
  function loadDraw(){ var all=loadJSON(DRAW_KEY,{}); drawings=all[active]||[]; pending=null; selDraw=null; }
  function xy(t,p){ if(!mainSeries) return null; var x=chart.timeScale().timeToCoordinate(t); var y=mainSeries.priceToCoordinate(p); if(x==null||y==null) return null; return {x:x,y:y}; }
  function ln(x1,y1,x2,y2,c,w,dash){ return '<line x1="'+x1+'" y1="'+y1+'" x2="'+x2+'" y2="'+y2+'" stroke="'+c+'" stroke-width="'+(w||drawW)+'"'+(dash?' stroke-dasharray="'+dash+'"':'')+' />'; }
  function tx(x,y,c,s){ return '<text x="'+x+'" y="'+y+'" fill="'+c+'" font-size="11" font-family="IBM Plex Mono">'+(s||"")+'</text>'; }
  function ext(p0,p1,w){ var dx=p1.x-p0.x, dy=p1.y-p0.y, len=Math.sqrt(dx*dx+dy*dy)||1; return {x:p0.x+dx/len*w, y:p0.y+dy/len*w}; }

  function drawSVG(){
    var svg=document.getElementById("draw"), box=document.getElementById("chart");
    if(!svg||!box) return;
    var w=box.clientWidth, h=box.clientHeight; svg.setAttribute("viewBox","0 0 "+w+" "+h); svg.setAttribute("width",w); svg.setAttribute("height",h);
    var parts=[], i, d;
    if(hideDraw){ svg.innerHTML=""; renderObj(); return; }
    for(i=0;i<drawings.length;i++){
      d=drawings[i]; if(d.hide) continue;
      var pts=(d.points||[]).map(function(p){ return xy(p.time,p.price); }).filter(Boolean);
      var c=d.color||drawColor, sw=selDraw===d.id?2.4:(d.w||drawW);
      var k=d.kind;
      if(k==="hline" && pts[0]) parts.push(ln(0,pts[0].y,w,pts[0].y,c,sw));
      else if(k==="hray" && pts[0]) parts.push(ln(pts[0].x,pts[0].y,w,pts[0].y,c,sw));
      else if(k==="vline" && pts[0]) parts.push(ln(pts[0].x,0,pts[0].x,h,c,sw));
      else if(k==="crossline" && pts[0]){ parts.push(ln(0,pts[0].y,w,pts[0].y,c,sw,"4 3")); parts.push(ln(pts[0].x,0,pts[0].x,h,c,sw,"4 3")); }
      else if((k==="trend"||k==="ray"||k==="extended"||k==="infoline"||k==="angle"||k==="measure"||k==="arrow") && pts.length>=2){
        var a=pts[0], b=pts[1];
        if(k==="ray"){ var e=ext(a,b,w*2); b={x:e.x,y:e.y}; }
        if(k==="extended"){ var e2=ext(a,b,w*2), e3=ext(b,a,w*2); a=e3; b=e2; }
        parts.push(ln(a.x,a.y,b.x,b.y,c,sw));
        if(k==="arrow"){ var ang=Math.atan2(b.y-a.y,b.x-a.x); parts.push(ln(b.x,b.y,b.x-10*Math.cos(ang-0.4),b.y-10*Math.sin(ang-0.4),c,sw)); parts.push(ln(b.x,b.y,b.x-10*Math.cos(ang+0.4),b.y-10*Math.sin(ang+0.4),c,sw)); }
        if(k==="measure"||k==="infoline"||k==="angle"){
          var pct=d.points[0].price?((d.points[1].price-d.points[0].price)/d.points[0].price)*100:0;
          var dt=d.points[1].time-d.points[0].time;
          var lab=pct.toFixed(2)+"% · "+Math.round(dt/86400)+"d";
          if(k==="angle"){ var ang2=Math.atan2(pts[0].y-pts[1].y, pts[1].x-pts[0].x)*180/Math.PI; lab=ang2.toFixed(1)+"° · "+lab; }
          parts.push(tx((pts[0].x+pts[1].x)/2,(pts[0].y+pts[1].y)/2-8,c,lab));
        }
      }
      else if((k==="channel"||k==="disjoint"||k==="flattop") && pts.length>=3){
        parts.push(ln(pts[0].x,pts[0].y,pts[1].x,pts[1].y,c,sw));
        var dy=k==="flattop"? (pts[2].y-pts[0].y) : (pts[2].y-pts[0].y);
        var dx=k==="disjoint"? (pts[2].x-pts[0].x)*0.15 : 0;
        parts.push(ln(pts[0].x+dx,pts[0].y+dy,pts[1].x+dx,pts[1].y+dy,c,sw,"4 3"));
      }
      else if((k==="pitchfork"||k==="schiff") && pts.length>=3){
        var mx=(pts[1].x+pts[2].x)/2, my=(pts[1].y+pts[2].y)/2;
        if(k==="schiff"){ mx=(pts[0].x+mx)/2; my=(pts[0].y+my)/2; }
        var e=ext(pts[0],{x:mx,y:my},w); parts.push(ln(pts[0].x,pts[0].y,e.x,e.y,c,sw));
        var dx=e.x-pts[0].x, dy=e.y-pts[0].y;
        parts.push(ln(pts[1].x,pts[1].y,pts[1].x+dx,pts[1].y+dy,c,sw,"4 3"));
        parts.push(ln(pts[2].x,pts[2].y,pts[2].x+dx,pts[2].y+dy,c,sw,"4 3"));
      }
      else if(k==="rect" && pts.length>=2){ var x=Math.min(pts[0].x,pts[1].x), y=Math.min(pts[0].y,pts[1].y); parts.push('<rect x="'+x+'" y="'+y+'" width="'+Math.abs(pts[1].x-pts[0].x)+'" height="'+Math.abs(pts[1].y-pts[0].y)+'" fill="'+c+'22" stroke="'+c+'" stroke-width="'+sw+'"/>'); }
      else if(k==="rrect" && pts.length>=2){ parts.push('<polygon points="'+pts[0].x+','+pts[0].y+' '+pts[1].x+','+pts[0].y+' '+pts[1].x+','+pts[1].y+' '+pts[0].x+','+pts[1].y+'" fill="'+c+'18" stroke="'+c+'" transform="rotate(12 '+((pts[0].x+pts[1].x)/2)+' '+((pts[0].y+pts[1].y)/2)+')"/>'); }
      else if((k==="ellipse"||k==="circle") && pts.length>=2){ var rx=Math.abs(pts[1].x-pts[0].x), ry=k==="circle"?rx:Math.abs(pts[1].y-pts[0].y); parts.push('<ellipse cx="'+pts[0].x+'" cy="'+pts[0].y+'" rx="'+rx+'" ry="'+ry+'" fill="'+c+'18" stroke="'+c+'"/>'); }
      else if(k==="arc" && pts.length>=2){ var r=Math.hypot(pts[1].x-pts[0].x,pts[1].y-pts[0].y); parts.push('<path d="M '+(pts[0].x-r)+' '+pts[0].y+' A '+r+' '+r+' 0 0 1 '+(pts[0].x+r)+' '+pts[0].y+'" fill="none" stroke="'+c+'"/>'); }
      else if(k==="triangle" && pts.length>=3){ parts.push('<polygon points="'+pts.map(function(p){return p.x+','+p.y;}).join(' ')+'" fill="'+c+'18" stroke="'+c+'"/>'); }
      else if((k==="polyline"||k==="brush"||k==="highlight"||k==="xabcd"||k==="hs"||k==="elliott") && pts.length){
        var pth=pts.map(function(p,ii){ return (ii?"L":"M")+p.x+" "+p.y; }).join(" ");
        parts.push('<path d="'+pth+'" fill="'+(k==="highlight"?c+"33":"none")+'" stroke="'+c+'" stroke-width="'+(k==="highlight"?10:sw)+'" stroke-linecap="round"/>');
        if(k==="xabcd"){ var lb=["X","A","B","C","D"]; pts.forEach(function(p,ii){ parts.push(tx(p.x+4,p.y-4,c,lb[ii]||"")); }); }
        if(k==="hs"){ var lb2=["LS","H","RS","N","H","N2","T"]; pts.forEach(function(p,ii){ parts.push(tx(p.x+4,p.y-4,c,lb2[ii]||"")); }); }
        if(k==="elliott"){ pts.forEach(function(p,ii){ parts.push(tx(p.x+4,p.y-4,c,String(ii))); }); }
      }
      else if((k==="fib"||k==="fibext"||k==="fibch") && pts.length>=2){
        var lv=k==="fibext"?[0,0.272,0.618,1,1.272,1.618,2.618]:[0,0.236,0.382,0.5,0.618,0.786,1];
        for(var fi=0;fi<lv.length;fi++){ var yf=pts[0].y+(pts[1].y-pts[0].y)*lv[fi]; parts.push(ln(k==="fibch"?pts[0].x:0, yf, k==="fibch"?pts[1].x:w, yf, c, 1, "4 3")); parts.push(tx(8,yf-3,c,(lv[fi]*100).toFixed(1))); }
      }
      else if(k==="fibfan" && pts.length>=2){
        [0,0.382,0.5,0.618,1].forEach(function(lv){ var yf=pts[0].y+(pts[1].y-pts[0].y)*lv; var e=ext(pts[0],{x:pts[1].x,y:yf},w); parts.push(ln(pts[0].x,pts[0].y,e.x,e.y,c,1,"4 3")); });
      }
      else if(k==="fibtz" && pts.length>=2){
        var dt=pts[1].x-pts[0].x; [0,0.382,0.5,0.618,1,1.618,2.618].forEach(function(lv){ var x=pts[0].x+dt*lv; parts.push(ln(x,0,x,h,c,1,"4 3")); });
      }
      else if(k==="fibarc" && pts.length>=2){
        var r0=Math.hypot(pts[1].x-pts[0].x,pts[1].y-pts[0].y); [0.382,0.5,0.618,1].forEach(function(lv){ parts.push('<circle cx="'+pts[0].x+'" cy="'+pts[0].y+'" r="'+(r0*lv)+'" fill="none" stroke="'+c+'" stroke-dasharray="4 3"/>'); });
      }
      else if(k==="gannfan" && pts.length>=2){
        var ratios=[1/8,1/4,1/3,1/2,1,2,3,4,8];
        ratios.forEach(function(r){ var e=ext(pts[0],{x:pts[1].x, y:pts[0].y+(pts[1].y-pts[0].y)*r}, w); parts.push(ln(pts[0].x,pts[0].y,e.x,e.y,c,1,"3 3")); });
      }
      else if(k==="gannbox" && pts.length>=2){ var x=Math.min(pts[0].x,pts[1].x), y=Math.min(pts[0].y,pts[1].y), bw=Math.abs(pts[1].x-pts[0].x), bh=Math.abs(pts[1].y-pts[0].y); parts.push('<rect x="'+x+'" y="'+y+'" width="'+bw+'" height="'+bh+'" fill="none" stroke="'+c+'"/>'); parts.push(ln(x,y,x+bw,y+bh,c,1,"3 3")); parts.push(ln(x+bw,y,x,y+bh,c,1,"3 3")); }
      else if((k==="text"||k==="note"||k==="pricelbl"||k==="flag"||k==="arrowmark"||k==="callout") && pts[0]){
        var lab=d.label|| (k==="pricelbl"?fmt(d.points[0].price):k==="flag"?"⚑":k==="arrowmark"?"➤":"note");
        if(k==="callout" && pts.length>=2){ parts.push(ln(pts[0].x,pts[0].y,pts[1].x,pts[1].y,c,1)); parts.push('<rect x="'+(pts[1].x)+'" y="'+(pts[1].y-16)+'" width="'+(lab.length*7+10)+'" height="20" rx="3" fill="#fff" stroke="'+c+'"/>'); parts.push(tx(pts[1].x+6,pts[1].y-2,c,lab)); }
        else parts.push(tx(pts[0].x,pts[0].y,c,lab));
      }
      else if((k==="prange"||k==="drange") && pts.length>=2){
        if(k==="prange"){ parts.push(ln(pts[0].x,pts[0].y,pts[0].x,pts[1].y,c,sw)); parts.push(tx(pts[0].x+6,(pts[0].y+pts[1].y)/2,c,fmt(Math.abs(d.points[1].price-d.points[0].price)))); }
        else { parts.push(ln(pts[0].x,pts[0].y,pts[1].x,pts[0].y,c,sw)); parts.push(tx((pts[0].x+pts[1].x)/2,pts[0].y-6,c,Math.round((d.points[1].time-d.points[0].time)/86400)+"d")); }
      }
      else if((k==="longpos"||k==="shortpos") && pts.length>=2){
        var entry=pts[0].y, tgt=pts[1].y, stop=entry-(tgt-entry);
        var x0=pts[0].x, x1=Math.max(pts[1].x, x0+80);
        parts.push('<rect x="'+x0+'" y="'+Math.min(entry,tgt)+'" width="'+(x1-x0)+'" height="'+Math.abs(tgt-entry)+'" fill="'+(k==="longpos"?UP:DN)+'22" stroke="'+(k==="longpos"?UP:DN)+'"/>');
        parts.push('<rect x="'+x0+'" y="'+Math.min(entry,stop)+'" width="'+(x1-x0)+'" height="'+Math.abs(stop-entry)+'" fill="'+(k==="longpos"?DN:UP)+'22" stroke="'+(k==="longpos"?DN:UP)+'"/>');
        parts.push(tx(x0+6,entry-4,c,"RR 1.0"));
      }
    }
    svg.innerHTML=parts.join("");
    renderObj();
  }

  function snapPt(t,p){
    if(!magnet || !lastBars.length) return {time:Number(t),price:p};
    var best=lastBars[0], bd=1e99,i;
    for(i=0;i<lastBars.length;i++){ var dd=Math.abs(lastBars[i].time-t); if(dd<bd){ bd=dd; best=lastBars[i]; } }
    var cand=[best.open,best.high,best.low,best.close];
    p=cand.reduce(function(a,b){ return Math.abs(b-p)<Math.abs(a-p)?b:a; });
    return {time:best.time,price:p};
  }
  function nearestDraw(x,y){
    var best=null, bd=24, i, d;
    for(i=0;i<drawings.length;i++){
      d=drawings[i];
      var pts=(d.points||[]).map(function(p){ return xy(p.time,p.price); }).filter(Boolean);
      pts.forEach(function(p){ var dd=Math.hypot(p.x-x,p.y-y); if(dd<bd){ bd=dd; best=d; } });
    }
    return best;
  }
  function finishTool(){ if(!stayTool){ tool="cursor"; renderRail(); } pending=null; }
  function onChartClick(ev){
    if(lockDraw && tool!=="cursor" && tool!=="eraser" && tool!=="zoom") { toast("Drawings locked"); return; }
    var box=document.getElementById("chart").getBoundingClientRect();
    var x=ev.clientX-box.left, y=ev.clientY-box.top;
    if(tool==="cursor"||tool==="cross"){
      var hit=nearestDraw(x,y); selDraw=hit?hit.id:null; renderProps(); drawSVG(); return;
    }
    if(tool==="eraser"){ var er=nearestDraw(x,y); if(er){ undo.push({op:"del",item:er}); drawings=drawings.filter(function(d){return d.id!==er.id;}); saveDraw(); drawSVG(); } return; }
    var t=chart.timeScale().coordinateToTime(x); if(t==null||!mainSeries) return;
    var p=mainSeries.coordinateToPrice(y); if(p==null) return;
    var pt=snapPt(t, Number(p));
    if(tool==="zoom"){
      if(!pending){ pending={id:uid(),kind:"zoom",points:[pt]}; return; }
      var a=pending.points[0].time, b=pt.time; pending=null; tool="cursor"; renderRail();
      try{ chart.timeScale().setVisibleRange({from:Math.min(a,b), to:Math.max(a,b)}); }catch(e){}
      return;
    }
    if(!pending){
      var id=uid(); pending={id:id,kind:tool,points:[pt],color:drawColor,w:drawW};
      if(tool==="text"||tool==="note"||tool==="callout") pending.label=prompt("Text","note")||"note";
      if(tool==="flag") pending.label="⚑";
      if(tool==="pricelbl") pending.label=fmt(pt.price);
      drawings.push(pending); undo.push({op:"add",id:id}); redo=[];
      if(needPts(tool)===1){ finishTool(); }
      saveDraw(); drawSVG(); return;
    }
    pending.points.push(pt);
    var n=needPts(tool);
    if(tool==="polyline" && ev.detail===2){ finishTool(); saveDraw(); drawSVG(); return; }
    if(pending.points.length>=n && n<99){ finishTool(); }
    saveDraw(); drawSVG();
  }

  function undoDraw(){
    var u=undo.pop(); if(!u) return; redo.push(u);
    if(u.op==="add") drawings=drawings.filter(function(d){ return d.id!==u.id; });
    else if(u.op==="del" && u.item) drawings.push(u.item);
    saveDraw(); drawSVG();
  }
  function redoDraw(){
    var u=redo.pop(); if(!u) return; undo.push(u);
    if(u.op==="add"){ /* cannot restore without item; skip */ }
    else if(u.op==="del" && u.item) drawings=drawings.filter(function(d){ return d.id!==u.item.id; });
    saveDraw(); drawSVG();
  }
  function clearDraw(){ if(lockDraw){ toast("Drawings locked"); return; } undo=undo.concat(drawings.map(function(d){return {op:"add",id:d.id};})); drawings=[]; pending=null; selDraw=null; saveDraw(); drawSVG(); }

  function loadAlerts(){ alerts=loadJSON(ALERT_KEY,[]); }
  function saveAlerts(){ saveJSON(ALERT_KEY, alerts); renderAlerts(); }
  function addAlert(sym, px){ alerts.push({id:uid(),sym:sym,price:+px,created:Date.now(),fired:false}); saveAlerts(); toast("Alert "+sym+" @ "+fmt(px)); if(window.Notification&&Notification.permission==="default") Notification.requestPermission().catch(function(){}); }
  function checkAlerts(sym, px){
    alerts.forEach(function(a){
      if(a.fired||a.sym!==sym) return;
      if(Math.abs(px-a.price)/a.price < 0.002){ a.fired=true; saveAlerts(); toast("ALERT "+sym+" hit "+fmt(a.price)); if(window.Notification&&Notification.permission==="granted") new Notification("JustHodl "+sym, {body:"Price "+fmt(px)}); }
    });
  }
  function renderAlerts(){
    document.getElementById("alerts").innerHTML="<b>ALERTS</b>"+(alerts.length?alerts.map(function(a){ return "<div class=cell><span>"+a.sym+" "+fmt(a.price)+(a.fired?" ✓":"")+"</span><button data-del='"+a.id+"'>×</button></div>"; }).join(""):"<div class=cell>None — right-click a row → Alert</div>");
    document.querySelectorAll("#alerts [data-del]").forEach(function(b){ b.onclick=function(){ alerts=alerts.filter(function(a){return a.id!==b.dataset.del;}); saveAlerts(); }; });
  }

  function renderTabs(){
    document.getElementById("tabs").innerHTML="<a class='tab brand' href='/'>JustHodl</a>"+TABS.map(function(s){ var q=quotes[s], up=q&&q.chg>=0; return "<button class='tab "+(s===active?"on":"")+"' data-id='"+s+"'>"+s.replace("USDT","")+(q?" <span class="+(up?"up":"dn")+">"+fmt(q.last)+" "+(up?"+":"")+(q.chg*100).toFixed(2)+"%</span>":"")+" <span data-x='"+s+"'>×</span></button>"; }).join("")+"<button class=tab id=add>+</button><a class='tab pro' href='/chart-pro.html'>Pro</a>";
    document.querySelectorAll(".tab[data-id]").forEach(function(b){ b.onclick=function(e){ if(e.target.dataset.x){ TABS=TABS.filter(function(s){return s!==e.target.dataset.x;}); if(active===e.target.dataset.x) active=TABS[0]||active; renderTabs(); loadDraw(); load(); return; } active=b.dataset.id; loadDraw(); renderTabs(); load(); }; });
    var add=document.getElementById("add"); if(add) add.onclick=function(){ openCmd("sym "); };
  }
  function renderTf(){
    document.getElementById("tfbar").innerHTML=
      TFS.map(function(t){ return "<button class='"+(t[0]===tf?"on":"")+"' data-tf='"+t[0]+"'>"+t[1]+"</button>"; }).join("")+
      "<span style='width:8px'></span>"+
      KINDS.map(function(k){ return "<button class='"+(k[0]===kind?"on":"")+"' data-k='"+k[0]+"'>"+k[1]+"</button>"; }).join("")+
      SCALES.map(function(s){ return "<button class='"+(+s[0]===scaleMode?"on":"")+"' data-sc='"+s[0]+"'>"+s[1]+"</button>"; }).join("")+
      "<button id=btn-ind>Indicators</button><button id=btn-cmp>Compare</button><button id=btn-rep>Replay</button><button id=btn-shot>Snapshot</button><button id=btn-fs>Full</button><button id=btn-lay1 class='"+(layout===1?"on":"")+"'>1</button><button id=btn-lay2 class='"+(layout===2?"on":"")+"'>2</button><button id=btn-lay4 class='"+(layout===4?"on":"")+"'>4</button><button id=btn-watch>"+(watchOpen?"Hide list":"Watchlist")+"</button><button id=btn-set>Settings</button><button id=btn-cmd>⌘K</button><input id=goto type=date title='Go to date'>";
    document.querySelectorAll("#tfbar [data-tf]").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });
    document.querySelectorAll("#tfbar [data-k]").forEach(function(b){ b.onclick=function(){ kind=b.dataset.k; renderTf(); if(lastBars.length) paint(lastBars); }; });
    document.querySelectorAll("#tfbar [data-sc]").forEach(function(b){ b.onclick=function(){ scaleMode=+b.dataset.sc; renderTf(); if(lastBars.length) paint(lastBars); }; });
    document.getElementById("btn-ind").onclick=openInd;
    document.getElementById("btn-cmp").onclick=openCmp;
    document.getElementById("btn-rep").onclick=startReplay;
    document.getElementById("btn-shot").onclick=shot;
    document.getElementById("btn-fs").onclick=function(){ var el=document.getElementById("app"); if(!document.fullscreenElement) el.requestFullscreen(); else document.exitFullscreen(); };
    document.getElementById("btn-lay1").onclick=function(){ setLayout(1); renderTf(); };
    document.getElementById("btn-lay2").onclick=function(){ setLayout(2); renderTf(); };
    document.getElementById("btn-lay4").onclick=function(){ setLayout(4); renderTf(); };
    document.getElementById("btn-watch").onclick=function(){ watchOpen=!watchOpen; document.getElementById("watch").className="watch"+(watchOpen?"":" hide"); renderTf(); };
    document.getElementById("btn-set").onclick=openSet;
    document.getElementById("btn-cmd").onclick=function(){ openCmd(""); };
    document.getElementById("goto").onchange=function(){ var t=Math.floor(Date.parse(this.value+"T00:00:00Z")/1000); if(!t||!lastBars.length) return; chart.timeScale().setVisibleRange({ from:t-30*86400, to:t+5*86400 }); };
    document.getElementById("chgbar").innerHTML=CHG.map(function(t){ return "<button class='"+(t[0]===mode?"on":"")+"' data-m='"+t[0]+"'>"+t[1]+"</button>"; }).join("");
    document.querySelectorAll("#chgbar [data-m]").forEach(function(b){ b.onclick=function(){ mode=b.dataset.m; renderTf(); if(lastBars.length) paint(lastBars); }; });
  }
  function closeFly(){ document.getElementById("fly").className=""; }
  function openFly(btn, group){
    var fly=document.getElementById("fly"), r=btn.getBoundingClientRect();
    fly.className="on";
    fly.style.left=(r.right+4)+"px"; fly.style.top=Math.max(8, r.top-8)+"px";
    fly.innerHTML="<div class=lab>"+group.n+"</div>"+group.tools.map(function(t){ return "<button class='"+(tool===t[0]?"on":"")+"' data-t='"+t[0]+"'>"+t[1]+"  "+t[2]+"</button>"; }).join("");
    fly.querySelectorAll("[data-t]").forEach(function(b){ b.onclick=function(){ tool=b.dataset.t; pending=null; closeFly(); renderRail(); }; });
  }
  function renderRail(){
    var html=GROUPS.map(function(g){ var on=g.tools.some(function(t){return t[0]===tool;}); return "<button class='gbtn "+(on?"on":"")+"' data-g='"+g.id+"' title='"+g.n+"'>"+g.g+"</button>"; }).join("")+
      "<div class=sep></div>"+
      "<button class='gbtn "+(magnet?"on":"")+"' id=mag title=Magnet>M</button>"+
      "<button class='gbtn "+(stayTool?"on":"")+"' id=stay title='Stay in drawing mode'>↻</button>"+
      "<button class='gbtn "+(lockDraw?"on":"")+"' id=lock title='Lock drawings'>🔒</button>"+
      "<button class='gbtn "+(hideDraw?"on":"")+"' id=hide title='Hide drawings'>👁</button>"+
      "<button class=gbtn id=und title='Undo Ctrl+Z'>↶</button>"+
      "<button class=gbtn id=red title='Redo Ctrl+Y'>↷</button>"+
      "<button class=gbtn id=clr title='Clear drawings'>⌫</button>"+
      "<button class='gbtn "+(objOpen?"on":"")+"' id=objb title='Object tree'>☰</button>"+
      "<button class='gbtn "+(vpOn?"on":"")+"' id=vpb title='Volume profile'>VP</button>"+
      "<button class=gbtn id=csv title='Export CSV'>CSV</button>";
    document.getElementById("rail").innerHTML=html;
    document.querySelectorAll("#rail [data-g]").forEach(function(b){
      b.onclick=function(e){ e.stopPropagation(); var g=GROUPS.filter(function(x){return x.id===b.dataset.g;})[0]; openFly(b,g); };
    });
    document.getElementById("mag").onclick=function(){ magnet=!magnet; renderRail(); };
    document.getElementById("stay").onclick=function(){ stayTool=!stayTool; renderRail(); };
    document.getElementById("lock").onclick=function(){ lockDraw=!lockDraw; renderRail(); toast(lockDraw?"Drawings locked":"Drawings unlocked"); };
    document.getElementById("hide").onclick=function(){ hideDraw=!hideDraw; renderRail(); drawSVG(); };
    document.getElementById("und").onclick=undoDraw;
    document.getElementById("red").onclick=redoDraw;
    document.getElementById("clr").onclick=clearDraw;
    document.getElementById("objb").onclick=function(){ objOpen=!objOpen; renderObj(); };
    document.getElementById("vpb").onclick=function(){ vpOn=!vpOn; renderRail(); if(lastBars.length) paint(lastBars); };
    document.getElementById("csv").onclick=exportCSV;
  }
  function renderLegend(){
    document.getElementById("legend").innerHTML="<div style='color:#131722;font-weight:500;margin-bottom:4px'>"+active+(compare.length?" + "+compare.join(" "):"")+"</div>"+INDS.filter(function(i){return i.on;}).map(function(i){ return "<button style=color:"+i.c+" data-i='"+i.id+"'>"+i.n+"</button>"; }).join("")+OSC.filter(function(o){return o.on;}).map(function(o){ return "<button style=color:"+ACC+" data-o='"+o.id+"'>"+o.n+"</button>"; }).join("");
    document.querySelectorAll("#legend [data-i]").forEach(function(b){ b.onclick=function(){ var i=INDS.find(function(x){return x.id===b.dataset.i;}); i.on=!i.on; if(lastBars.length) paint(lastBars); }; });
    document.querySelectorAll("#legend [data-o]").forEach(function(b){ b.onclick=function(){ var o=OSC.find(function(x){return x.id===b.dataset.o;}); o.on=!o.on; if(lastBars.length) paint(lastBars); }; });
  }
  function renderObj(){
    var el=document.getElementById("obj");
    el.className=objOpen?"on":"";
    if(!objOpen) return;
    el.innerHTML="<div style=color:var(--mut)>Object tree · "+drawings.length+"</div>"+(drawings.length?drawings.map(function(d){ return "<div class=cell><span>"+toolName(d.kind)+"</span><span><button data-h='"+d.id+"'>"+(d.hide?"show":"hide")+"</button> <button data-r='"+d.id+"'>×</button></span></div>"; }).join(""):"<div>No drawings</div>");
    el.querySelectorAll("[data-h]").forEach(function(b){ b.onclick=function(){ var d=drawings.find(function(x){return x.id===b.dataset.h;}); if(d) d.hide=!d.hide; saveDraw(); drawSVG(); }; });
    el.querySelectorAll("[data-r]").forEach(function(b){ b.onclick=function(){ drawings=drawings.filter(function(x){return x.id!==b.dataset.r;}); if(selDraw===b.dataset.r) selDraw=null; saveDraw(); drawSVG(); renderProps(); }; });
  }
  function renderProps(){
    var el=document.getElementById("props");
    var d=drawings.find(function(x){return x.id===selDraw;});
    if(!d){ el.className=""; return; }
    el.className="on";
    el.innerHTML="<div style=font-weight:500>"+toolName(d.kind)+"</div><div class=sw>"+COLORS.map(function(c){ return "<i data-c='"+c+"' class='"+(d.color===c?"on":"")+"' style=background:"+c+"></i>"; }).join("")+"</div><div class=cell>Width <input id=dw type=number min=1 max=6 value='"+(d.w||1.2)+"' style='width:48px;border:1px solid var(--line);padding:2px 4px'></div><div class=cell><button id=dalert>Alert on this</button><button id=ddel>Delete</button></div>";
    el.querySelectorAll("[data-c]").forEach(function(i){ i.onclick=function(){ d.color=i.dataset.c; drawColor=d.color; saveDraw(); drawSVG(); renderProps(); }; });
    document.getElementById("dw").onchange=function(){ d.w=+this.value; saveDraw(); drawSVG(); };
    document.getElementById("dalert").onclick=function(){ if(d.points[0]) addAlert(active, d.points[0].price); };
    document.getElementById("ddel").onclick=function(){ drawings=drawings.filter(function(x){return x.id!==d.id;}); selDraw=null; saveDraw(); drawSVG(); renderProps(); };
  }
  function renderWtabs(){
    document.getElementById("wtabs").innerHTML=["list","news","alerts","tech","cal","notes"].map(function(t){ return "<button class='"+(wtab===t?"on":"")+"' data-w='"+t+"'>"+t+"</button>"; }).join("");
    var ids={list:"w-list",news:"news",alerts:"alerts",tech:"tech",cal:"cal",notes:"notes"};
    Object.keys(ids).forEach(function(k){ var el=document.getElementById(ids[k]); if(el) el.style.display = (k==="list"? (wtab==="list"?"":"none") : (wtab===k?"":"none")); });
    document.getElementById("intel").style.display=wtab==="list"?"":"none";
    document.querySelectorAll("#wtabs [data-w]").forEach(function(b){ b.onclick=function(){ wtab=b.dataset.w; renderWtabs(); if(wtab==="news") renderNews(); if(wtab==="alerts") renderAlerts(); if(wtab==="tech"&&lastBars.length) renderTech(lastBars); if(wtab==="cal") renderCal(); if(wtab==="notes") renderNotes(); }; });
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
  function flagDot(s){ var c=flags[s]||flags[bare(s)]; return "<i class=flag style=background:"+(c||"var(--line)")+"></i>"; }
  function renderList(){
    var sel=document.getElementById("list");
    if(sel&&!sel.dataset.bound){ sel.onchange=function(){ listId=sel.value; renderList(); }; sel.dataset.bound="1"; }
    if(sel){ sel.innerHTML=lists.map(function(l){ return "<option value='"+l.id+"'"+(l.id===listId?" selected":"")+">"+l.name+" ("+(l.n||(l.symbols||[]).length)+")</option>"; }).join(""); }
    document.getElementById("nlists").textContent=lists.length+" lists";
    var box=document.getElementById("wlist");
    var syms=currentSyms();
    box.innerHTML=syms.map(function(s){
      var q=quotes[s]||quotes[bare(s)]; var up=!q||q.chg>=0;
      return "<button class='wrow "+(bare(s)===active?"on":"")+"' data-s='"+s+"'>"+flagDot(s)+"<span>"+s+"</span><span>"+(q?fmt(q.last):"—")+"</span><span class="+(up?"up":"dn")+">"+(q?(q.chg>=0?"+":"")+(q.chg*100).toFixed(2)+"%":"—")+"</span><span class="+(up?"up":"dn")+">"+(q?(q.chgv>=0?"+":"")+fmt(q.chgv):"—")+"</span>"+sparkSvg(q&&q.spark,up)+"</button>";
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
    var copied=[];
    var urls=warehouse("/data/tv-watchlists.json");
    for(var i=0;i<urls.length && !copied.length;i++){
      try{ var j=await fetchJson(urls[i]); var arr=Array.isArray(j)?j:(j.lists||[]); copied=arr.filter(function(l){return l&&l.name&&Array.isArray(l.symbols);}).map(function(l){ return {id:String(l.id||l.name),name:l.name,symbols:l.symbols,n:l.n||l.symbols.length}; }); }catch(e){}
    }
    lists=custom.concat(local, copied);
  }
  async function loadIntel(){
    var urls=warehouse("/data/jh-internals.json");
    for(var i=0;i<urls.length;i++){
      try{ var j=await fetchJson(urls[i]); var f=j.fields||j;
        document.getElementById("intel").innerHTML="<b>INTERNALS · warehouse</b>"+[["2s10s",f.twos_tens!=null?f.twos_tens+"%":"—"],["LIQ $B",f.liq_proxy_bn!=null?f.liq_proxy_bn:"—"],["NFCI",f.nfci!=null?f.nfci:"—"],["A-D",f.ad_breadth!=null?Number(f.ad_breadth).toFixed(3):"—"],["NH-NL",f.nh_nl!=null?f.nh_nl+" ("+(f.n_new_high||"—")+"H / "+(f.n_new_low||"—")+"L)":"—"]].map(function(x){return "<div class=cell><span>"+x[0]+"</span><span>"+x[1]+"</span></div>";}).join("");
        return;
      }catch(e){}
    }
  }
  async function loadNews(){
    var urls=warehouse("/data/finviz-news.json");
    for(var i=0;i<urls.length;i++){
      try{ var j=await fetchJson(urls[i]); news=j.news||j.items||(Array.isArray(j)?j:[]); renderNews(); return; }catch(e){}
    }
  }
  function renderNews(){
    var t=active.replace("USDT","");
    var rows=news.filter(function(n){ return !t || String(n.ticker||"").indexOf(t)>=0 || String(n.title||"").toUpperCase().indexOf(t)>=0; }).slice(0,30);
    if(!rows.length) rows=news.slice(0,20);
    document.getElementById("news").innerHTML="<b>NEWS</b>"+(rows.length?rows.map(function(n){ return "<a class=nitem href='"+(n.url||n.link||"#")+"' target=_blank>"+ (n.title||"")+"<span>"+(n.source||"")+" · "+String(n.date||n.published||"").slice(0,16)+" · "+(n.ticker||"")+"</span></a>"; }).join(""):"<div class=cell>No headlines cached</div>");
  }
  function lastOsc(arr){ return arr.length?arr[arr.length-1].value:null; }
  function gauge(label, v, lo, hi){
    var n=v==null?50: Math.min(100, Math.max(0, (v-lo)/(hi-lo||1)*100));
    var tag = n>=66?"buy": n<=33?"sell":"neut";
    var word = tag==="buy"?"Buy": tag==="sell"?"Sell":"Neutral";
    return "<div class=gauge><span>"+label+"</span><div class=bar><i style=left:"+n+"%></i></div><span class="+tag+">"+word+(v==null?"":" "+(typeof v==="number"?v.toFixed(1):v))+"</span></div>";
  }
  function renderTech(d){
    if(!d||d.length<30){ document.getElementById("tech").innerHTML="<b>TECHNICALS</b><div class=cell>Need more bars</div>"; return; }
    var r=lastOsc(rsi(d,14)), st=lastOsc(stoch(d,14,3,3)), m=macd(d), mh=m.length?m[m.length-1].hist:null;
    var s20=lastOsc(sma(d,20)), s50=lastOsc(sma(d,50)), s200=lastOsc(sma(d,200)), px=d[d.length-1].close;
    var ma = s20&&s50&&s200 ? (px>s20&&s20>s50&&s50>s200?90: px<s20&&s20<s50&&s50<s200?10:50) : 50;
    var votes=[r!=null?(r>=55?1:r<=45?-1:0):0, st!=null?(st>=60?1:st<=40?-1:0):0, mh!=null?(mh>=0?1:-1):0, ma>=70?1:ma<=30?-1:0];
    var sum=votes.reduce(function(a,b){return a+b;},0);
    var overall=sum>=2?"buy":sum<=-2?"sell":"neut";
    document.getElementById("tech").innerHTML="<b>TECHNICALS</b><div class=cell><span>Summary</span><span class="+overall+">"+(overall==="buy"?"Buy":overall==="sell"?"Sell":"Neutral")+"</span></div>"+
      gauge("RSI 14", r, 0, 100)+gauge("Stoch", st, 0, 100)+gauge("MACD hist", mh, -Math.abs(mh||1)*2, Math.abs(mh||1)*2)+gauge("MA stack", ma, 0, 100)+
      "<div class=cell style=color:var(--mut);font-size:10px>Local oscillators · not broker advice</div>";
  }
  function renderCal(){
    var rows=news.slice(0,24);
    document.getElementById("cal").innerHTML="<b>CALENDAR · news tape</b>"+(rows.length?rows.map(function(n){ return "<div class=nitem>"+String(n.date||n.published||"").slice(0,16)+" · "+(n.ticker||"")+"<span>"+(n.title||"")+"</span></div>"; }).join(""):"<div class=cell>No dated events in warehouse news</div>");
  }
  function renderNotes(){
    var mine=notes[active]||"";
    document.getElementById("notes").innerHTML="<b>NOTES · "+active+"</b><textarea id=noteb style='width:100%;min-height:90px;border:1px solid var(--line);padding:8px;margin-top:6px;font-family:IBM Plex Sans,sans-serif'>"+mine+"</textarea><div style='text-align:right;margin-top:6px'><button id=notesave>Save</button></div>";
    document.getElementById("notesave").onclick=function(){ notes[active]=document.getElementById("noteb").value; saveJSON(NOTE_KEY, notes); toast("Note saved locally"); };
  }

  function openInd(){
    var m=document.getElementById("modal"), box=document.getElementById("mbox");
    var cats=[]; INDS.forEach(function(i){ if(cats.indexOf(i.cat)<0) cats.push(i.cat); });
    var ocats=[]; OSC.forEach(function(o){ if(ocats.indexOf(o.cat)<0) ocats.push(o.cat); });
    box.innerHTML="<h3>Indicators</h3><input id=indq placeholder='Search RSI, Ichimoku, VWAP…' style='width:100%;border:1px solid var(--line);padding:8px;border-radius:4px;margin-bottom:8px'><div style=color:var(--mut);font-size:11px;margin-bottom:8px>Templates: <button id=t-tv>TV default</button> <button id=t-tr>Trend</button> <button id=t-os>Oscillators</button> <button id=t-vol>Volume</button> <button id=t-ich>Ichimoku</button> <button id=t-cl>Clean</button></div><div id=indbody></div><div style='margin-top:10px;text-align:right'><button id=indok>Apply</button></div>";
    function body(q){
      q=(q||"").toLowerCase();
      var html="";
      cats.forEach(function(c){ var rows=INDS.filter(function(i){ return i.cat===c && (!q || i.n.toLowerCase().indexOf(q)>=0); }); if(!rows.length) return; html+="<div class=icat>OVERLAY · "+c+"</div>"+rows.map(function(i){ return "<label class=indrow><span>"+i.n+"</span><input type=checkbox data-i='"+i.id+"' "+(i.on?"checked":"")+"></label>"; }).join(""); });
      ocats.forEach(function(c){ var rows=OSC.filter(function(o){ return o.cat===c && (!q || o.n.toLowerCase().indexOf(q)>=0); }); if(!rows.length) return; html+="<div class=icat>PANE · "+c+"</div>"+rows.map(function(o){ return "<label class=indrow><span>"+o.n+"</span><input type=checkbox data-o='"+o.id+"' "+(o.on?"checked":"")+"></label>"; }).join(""); });
      document.getElementById("indbody").innerHTML=html||"<div class=cell>No match</div>";
    }
    body(""); m.className="on";
    document.getElementById("indq").oninput=function(){ body(this.value); };
    function setTpl(fn){ fn(); openInd(); }
    document.getElementById("t-tv").onclick=function(){ setTpl(function(){ INDS.forEach(function(i){ i.on=/sma20|sma50|sma200|sma250|ema250|pc/.test(i.id); }); OSC.forEach(function(o){ o.on=false; }); }); };
    document.getElementById("t-tr").onclick=function(){ setTpl(function(){ INDS.forEach(function(i){ i.on=i.k==="st"||i.id==="ema250"||i.k==="sar"; }); OSC.forEach(function(o){ o.on=o.id==="adx"; }); }); };
    document.getElementById("t-os").onclick=function(){ setTpl(function(){ INDS.forEach(function(i){ i.on=false; }); OSC.forEach(function(o){ o.on=/rsi|macd|stoch|atr/.test(o.id); }); }); };
    document.getElementById("t-vol").onclick=function(){ setTpl(function(){ INDS.forEach(function(i){ i.on=i.k==="vwap"||i.k==="vwma"; }); OSC.forEach(function(o){ o.on=/obv|mfi|cmf/.test(o.id); }); vpOn=true; }); };
    document.getElementById("t-ich").onclick=function(){ setTpl(function(){ INDS.forEach(function(i){ i.on=i.k==="ich"; }); OSC.forEach(function(o){ o.on=false; }); }); };
    document.getElementById("t-cl").onclick=function(){ setTpl(function(){ INDS.forEach(function(i){ i.on=false; }); OSC.forEach(function(o){ o.on=false; }); }); };
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
    box.innerHTML="<h3>Chart settings</h3>"+
      "<label class=indrow><span>Grid</span><input type=checkbox id=s-grid "+(gridOn?"checked":"")+"></label>"+
      "<label class=indrow><span>Symbol watermark</span><input type=checkbox id=s-wm "+(watermark?"checked":"")+"></label>"+
      "<label class=indrow><span>Magnet</span><input type=checkbox id=s-mag "+(magnet?"checked":"")+"></label>"+
      "<label class=indrow><span>Invert scale</span><input type=checkbox id=s-inv "+(invert?"checked":"")+"></label>"+
      "<label class=indrow><span>High / low marks</span><input type=checkbox id=s-hl "+(hiLo?"checked":"")+"></label>"+
      "<label class=indrow><span>Crosshair magnet</span><input type=checkbox id=s-xh "+(crossMode===1?"checked":"")+"></label>"+
      "<div class=indrow><span>Timezone</span><select id=s-tz>"+TZS.map(function(z){return "<option value='"+z[1]+"'"+(z[0]===tzName?" selected":"")+">"+z[0]+"</option>";}).join("")+"</select></div>"+
      "<div class=icat>SAVE / LOAD</div><input id=tpln placeholder='Layout name' style='width:100%;border:1px solid var(--line);padding:6px;border-radius:4px'><div style='margin-top:6px'><button id=tplsave>Save layout</button></div><div id=tpls></div>"+
      "<div class=icat>NOT ON THIS DESK</div><div class=cell>Pine editor / community scripts — Chart Pro</div><div class=cell>Broker tickets / server alerts — Chart Pro</div>"+
      "<div style='margin-top:10px;text-align:right'><button id=setok>Save</button></div>";
    m.className="on";
    var tpls=loadJSON(TPL_KEY,{});
    document.getElementById("tpls").innerHTML=Object.keys(tpls).map(function(n){ return "<div class=cell><span>"+n+"</span><span><button data-l='"+n+"'>Load</button> <button data-x='"+n+"'>×</button></span></div>"; }).join("")||"<div class=cell>No saved layouts</div>";
    document.getElementById("tplsave").onclick=function(){ var n=document.getElementById("tpln").value.trim()||("Layout "+new Date().toISOString().slice(0,16)); tpls[n]={kind:kind,tf:tf,scaleMode:scaleMode,inds:INDS.map(function(i){return {id:i.id,on:i.on};}), osc:OSC.map(function(o){return {id:o.id,on:o.on};}), compare:compare.slice(), layout:layout}; saveJSON(TPL_KEY,tpls); toast("Saved "+n); openSet(); };
    document.querySelectorAll("#tpls [data-l]").forEach(function(b){ b.onclick=function(){ var t=tpls[b.dataset.l]; if(!t) return; kind=t.kind||kind; tf=t.tf||tf; scaleMode=t.scaleMode||0; if(t.inds) t.inds.forEach(function(x){ var i=INDS.find(function(i){return i.id===x.id;}); if(i) i.on=x.on; }); if(t.osc) t.osc.forEach(function(x){ var o=OSC.find(function(o){return o.id===x.id;}); if(o) o.on=x.on; }); compare=t.compare||[]; setLayout(t.layout||1); m.className=""; renderTf(); load(); }; });
    document.querySelectorAll("#tpls [data-x]").forEach(function(b){ b.onclick=function(){ delete tpls[b.dataset.x]; saveJSON(TPL_KEY,tpls); openSet(); }; });
    document.getElementById("setok").onclick=function(){
      gridOn=document.getElementById("s-grid").checked; watermark=document.getElementById("s-wm").checked; magnet=document.getElementById("s-mag").checked;
      invert=document.getElementById("s-inv").checked; hiLo=document.getElementById("s-hl").checked; crossMode=document.getElementById("s-xh").checked?1:0;
      var tz=TZS.filter(function(z){ return String(z[1])===document.getElementById("s-tz").value; })[0]||TZS[0];
      tzOff=tz[1]; tzName=tz[0]; document.getElementById("tzlab").textContent=tzName;
      saveLay(); m.className=""; if(lastBars.length) paint(lastBars);
    };
  }
  function saveLay(){ saveJSON(LAY_KEY,{gridOn:gridOn,watermark:watermark,magnet:magnet,layout:layout,kind:kind,tf:tf,invert:invert,hiLo:hiLo,crossMode:crossMode,tzName:tzName,tzOff:tzOff,stayTool:stayTool}); }
  function openCtx(x,y,s){
    var el=document.getElementById("ctx");
    el.style.display="block"; el.style.left=x+"px"; el.style.top=y+"px";
    el.innerHTML="<button data-a=open>Open "+s+"</button><button data-a=cmp>Compare</button><button data-a=al>Alert at last</button><button data-a=tab>Add tab</button><button data-a=flag>Cycle flag</button><button data-a=note>Note</button>";
    el.querySelectorAll("button").forEach(function(b){
      b.onclick=function(){
        if(b.dataset.a==="open"||b.dataset.a==="tab"){ if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); loadDraw(); renderTabs(); load(); }
        if(b.dataset.a==="cmp"){ if(compare.indexOf(bare(s))<0) compare.push(bare(s)); if(lastBars.length) paint(lastBars); }
        if(b.dataset.a==="al"){ var q=quotes[s]||quotes[bare(s)]; addAlert(bare(s), q?q.last:(lastBars[lastBars.length-1]||{}).close); }
        if(b.dataset.a==="flag"){ var cols=["#2962ff","#089981","#f23645","#ff6d00","#ab47bc",""], cur=flags[s]||""; var ix=cols.indexOf(cur); flags[s]=cols[(ix+1)%cols.length]; saveJSON(FLAG_KEY, flags); renderList(); }
        if(b.dataset.a==="note"){ wtab="notes"; active=bare(s); renderWtabs(); renderNotes(); }
        el.style.display="none";
      };
    });
  }

  function cmdItems(q){
    q=(q||"").trim().toLowerCase();
    var out=[], i;
    function push(kind, label, run, extra){ if(!q || label.toLowerCase().indexOf(q)>=0 || (extra||"").toLowerCase().indexOf(q)>=0) out.push({kind:kind,label:label,run:run,extra:extra||""}); }
    if(q.indexOf("sym ")===0) q=q.slice(4);
    TABS.forEach(function(s){ push("tab", s, function(){ active=s; loadDraw(); renderTabs(); load(); }, "open tab"); });
    for(i=0;i<lists.length && out.length<80;i++){ (lists[i].symbols||[]).slice(0,40).forEach(function(s){ push("sym", s, function(){ if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); loadDraw(); renderTabs(); load(); }, lists[i].name); }); }
    INDS.forEach(function(ind){ push("ind", ind.n, function(){ ind.on=!ind.on; if(lastBars.length) paint(lastBars); }, "indicator · "+ind.cat); });
    OSC.forEach(function(o){ push("osc", o.n, function(){ o.on=!o.on; if(lastBars.length) paint(lastBars); }, "oscillator · "+o.cat); });
    allTools().forEach(function(t){ push("draw", t[2], function(){ tool=t[0]; renderRail(); }, "drawing"); });
    [["Indicators",openInd],["Compare",openCmp],["Replay",startReplay],["Snapshot",shot],["Settings",openSet],["CSV",exportCSV],["Fullscreen",function(){ document.getElementById("app").requestFullscreen(); }],["Layout 1",function(){ setLayout(1); renderTf(); }],["Layout 2",function(){ setLayout(2); renderTf(); }],["Layout 4",function(){ setLayout(4); renderTf(); }]].forEach(function(x){ push("cmd", x[0], x[1], "command"); });
    KINDS.forEach(function(k){ push("kind", k[1]+" chart", function(){ kind=k[0]; renderTf(); if(lastBars.length) paint(lastBars); }, "type"); });
    if(q && /^[A-Z0-9:.\-]{1,16}$/i.test(q) && out.length<90){ var s=q.toUpperCase(); push("sym", s, function(){ if(TABS.indexOf(s)<0) TABS.push(s); active=s; loadDraw(); renderTabs(); load(); }, "type to open"); }
    return out.slice(0,80);
  }
  var cmdSel=0, cmdCache=[];
  function renderCmd(q){
    cmdCache=cmdItems(q); if(cmdSel>=cmdCache.length) cmdSel=0;
    document.getElementById("cmdres").innerHTML=cmdCache.map(function(it,i){ return "<button class='"+(i===cmdSel?"on":"")+"' data-i='"+i+"'><b>"+it.label+"</b><span>"+it.kind+(it.extra?" · "+it.extra:"")+"</span></button>"; }).join("")||"<div class=cell style=padding:12px>No matches</div>";
    document.querySelectorAll("#cmdres [data-i]").forEach(function(b){ b.onclick=function(){ runCmd(+b.dataset.i); }; });
  }
  function runCmd(i){ var it=cmdCache[i]; document.getElementById("cmdk").className=""; if(it&&it.run) it.run(); }
  function openCmd(pre){
    document.getElementById("cmdk").className="on";
    var inp=document.getElementById("cmdin"); inp.value=pre||""; inp.focus(); cmdSel=0; renderCmd(inp.value);
  }

  function startReplay(){
    if(!lastBars.length && !(replay.full&&replay.full.length)) return;
    if(!replay.full.length) replay.full=lastBars.slice();
    replay.on=true; replay.i=Math.max(30, replay.full.length-80); replay.speed=replay.speed||1;
    document.getElementById("replay").className="on";
    var scrub=document.getElementById("rp-scrub"); scrub.max=replay.full.length-1; scrub.value=replay.i;
    function show(){ paint(replay.full.slice(0,replay.i+1)); document.getElementById("rp-label").textContent=new Date(replay.full[replay.i].time*1000).toISOString().slice(0,10); scrub.value=replay.i; }
    document.getElementById("rp-play").onclick=function(){
      if(replay.timer){ clearInterval(replay.timer); replay.timer=null; this.textContent="Play"; return; }
      this.textContent="Pause";
      replay.timer=setInterval(function(){ replay.i=Math.min(replay.full.length-1, replay.i+1); show(); if(replay.i>=replay.full.length-1){ clearInterval(replay.timer); replay.timer=null; document.getElementById("rp-play").textContent="Play"; } }, 400/replay.speed);
    };
    document.getElementById("rp-step").onclick=function(){ replay.i=Math.min(replay.full.length-1, replay.i+1); show(); };
    document.getElementById("rp-back").onclick=function(){ replay.i=Math.max(10, replay.i-1); show(); };
    document.getElementById("rp-speed").onchange=function(){ replay.speed=+this.value; };
    document.getElementById("rp-date").onchange=function(){ var t=Math.floor(Date.parse(this.value+"T00:00:00Z")/1000); var i=0; while(i<replay.full.length && replay.full[i].time<t) i++; replay.i=i; show(); };
    scrub.oninput=function(){ replay.i=+this.value; show(); };
    document.getElementById("rp-exit").onclick=function(){ if(replay.timer) clearInterval(replay.timer); replay.on=false; document.getElementById("replay").className=""; paint(replay.full); };
    show();
  }
  function shot(){
    try{ var url=chart.takeScreenshot().toDataURL("image/png"); var a=document.createElement("a"); a.href=url; a.download=active+"-"+tf+".png"; a.click(); }
    catch(e){ toast("Snapshot unavailable"); }
  }
  function exportCSV(){
    var rows=["time,open,high,low,close,volume"].concat(lastBars.map(function(b){ return [new Date(b.time*1000).toISOString(),b.open,b.high,b.low,b.close,b.volume].join(","); }));
    var a=document.createElement("a"); a.href=URL.createObjectURL(new Blob([rows.join("\n")],{type:"text/csv"})); a.download=active+".csv"; a.click();
  }
  function countdown(last){
    var el=document.getElementById("cd"); if(!el) return;
    var id=spec(tf)[0], sec= id==="1d"?86400: id==="1w"?604800: id==="1h"?3600: id==="1m"?60: id==="5m"?300: id==="15m"?900:86400;
    var end=last.time+sec, left=end-Math.floor(Date.now()/1000); if(left<0) left=0;
    var h=Math.floor(left/3600), m=Math.floor((left%3600)/60), s=left%60;
    el.textContent="bar "+h+":"+String(m).padStart(2,"0")+":"+String(s).padStart(2,"0");
  }

  chart.subscribeCrosshairMove(function(param){
    var hud=document.getElementById("hud");
    if(!param||!param.time||!mainSeries){ hud.style.display="none"; return; }
    var d=param.seriesData.get(mainSeries); if(!d){ hud.style.display="none"; return; }
    var o=d.open!=null?d.open:d.value, h=d.high!=null?d.high:d.value, l=d.low!=null?d.low:d.value, c=d.close!=null?d.close:d.value;
    hud.style.display="block";
    hud.innerHTML=new Date((param.time+tzOff*3600)*1000).toISOString().slice(0,16).replace("T"," ")+" "+tzName+"  O "+fmt(o)+" H "+fmt(h)+" L "+fmt(l)+" C "+fmt(c);
  });
  chart.timeScale().subscribeVisibleLogicalRangeChange(function(){ drawSVG(); });
  document.getElementById("chart").addEventListener("click", onChartClick);
  document.getElementById("chart").addEventListener("dblclick", function(ev){ if(tool==="polyline" && pending){ finishTool(); saveDraw(); drawSVG(); } });
  document.getElementById("chart").addEventListener("mousemove", function(ev){
    if((tool!=="brush"&&tool!=="highlight")||!pending||!(ev.buttons&1)) return;
    onChartClick(ev);
  });
  document.addEventListener("mouseup", function(){ if((tool==="brush"||tool==="highlight") && pending) finishTool(); });

  document.addEventListener("click", function(e){
    var el=document.getElementById("ctx"); if(el && !el.contains(e.target)) el.style.display="none";
    var m=document.getElementById("modal"); if(e.target===m) m.className="";
    var fly=document.getElementById("fly"); if(fly && !fly.contains(e.target) && !e.target.closest(".gbtn")) closeFly();
    var cmd=document.getElementById("cmdk"); if(e.target===cmd) cmd.className="";
  });
  document.getElementById("cmdin").addEventListener("input", function(){ cmdSel=0; renderCmd(this.value); });
  document.getElementById("cmdin").addEventListener("keydown", function(e){
    if(e.key==="ArrowDown"){ e.preventDefault(); cmdSel=Math.min(cmdCache.length-1, cmdSel+1); renderCmd(this.value); }
    if(e.key==="ArrowUp"){ e.preventDefault(); cmdSel=Math.max(0, cmdSel-1); renderCmd(this.value); }
    if(e.key==="Enter"){ e.preventDefault(); runCmd(cmdSel); }
    if(e.key==="Escape") document.getElementById("cmdk").className="";
  });

  document.addEventListener("keydown", function(e){
    if(e.target && /input|textarea|select/i.test(e.target.tagName)) {
      if(e.key==="Escape") e.target.blur();
      return;
    }
    if((e.ctrlKey||e.metaKey) && (e.key==="k"||e.key==="K")){ e.preventDefault(); openCmd(""); return; }
    if(e.key==="/" && !e.metaKey && !e.ctrlKey){ e.preventDefault(); openCmd("sym "); }
    if(e.key==="Escape"){ tool="cursor"; pending=null; document.getElementById("modal").className=""; document.getElementById("cmdk").className=""; closeFly(); selDraw=null; renderRail(); renderProps(); }
    if(e.altKey && (e.key==="t"||e.key==="T")) { tool="trend"; renderRail(); }
    if(e.altKey && (e.key==="h"||e.key==="H")) { tool="hline"; renderRail(); }
    if(e.altKey && (e.key==="f"||e.key==="F")) { tool="fib"; renderRail(); }
    if(e.altKey && (e.key==="v"||e.key==="V")) { tool="vline"; renderRail(); }
    if(e.altKey && (e.key==="r"||e.key==="R")) { tool="rect"; renderRail(); }
    if(e.altKey && (e.key==="e"||e.key==="E")) { tool="eraser"; renderRail(); }
    if((e.ctrlKey||e.metaKey) && e.key==="z"){ e.preventDefault(); undoDraw(); }
    if((e.ctrlKey||e.metaKey) && (e.key==="y"||(e.shiftKey&&e.key==="Z"))){ e.preventDefault(); redoDraw(); }
    if((e.ctrlKey||e.metaKey) && e.key==="s"){ e.preventDefault(); shot(); }
    if((e.ctrlKey||e.metaKey) && e.key==="i"){ e.preventDefault(); openInd(); }
    if(e.key==="Delete"||e.key==="Backspace"){ if(selDraw){ drawings=drawings.filter(function(d){return d.id!==selDraw;}); selDraw=null; saveDraw(); drawSVG(); renderProps(); } }
    if(e.key===" "){ e.preventDefault(); if(replay.on) document.getElementById("rp-play").click(); else startReplay(); }
    if(e.key==="+"||e.key==="="){ try{ chart.timeScale().scrollToRealTime(); }catch(err){} }
  });

  function clock(){
    var el=document.getElementById("clock");
    if(el){ var d=new Date(Date.now()+tzOff*3600*1000); el.textContent=d.toISOString().slice(11,19)+" "+tzName; }
    if(lastBars.length) countdown(lastBars[lastBars.length-1]);
  }
  loadAlerts();
  notes=loadJSON(NOTE_KEY,{}); flags=loadJSON(FLAG_KEY,{});
  var lay=loadJSON(LAY_KEY,null); if(lay){ if(lay.gridOn!=null) gridOn=lay.gridOn; if(lay.magnet!=null) magnet=lay.magnet; if(lay.kind) kind=lay.kind; if(lay.tf) tf=lay.tf; if(lay.invert!=null) invert=lay.invert; if(lay.hiLo!=null) hiLo=lay.hiLo; if(lay.crossMode!=null) crossMode=lay.crossMode; if(lay.tzName) tzName=lay.tzName; if(lay.tzOff!=null) tzOff=lay.tzOff; if(lay.stayTool!=null) stayTool=lay.stayTool; if(lay.layout) layout=lay.layout; }
  loadDraw();
  renderTabs(); renderTf(); renderRail(); renderLetters(); renderWtabs(); renderLegend();
  if(layout>1) setLayout(layout);
  loadLists().then(renderList);
  loadIntel(); loadNews();
  TABS.forEach(function(s){ lastPx(s).then(function(px){ if(px){ quotes[s]=px; renderTabs(); } }); });
  load();
  clock(); setInterval(clock,1000);
  if(window.Notification && Notification.permission==="default") Notification.requestPermission().catch(function(){});
})();
