/* JustHodl Chart engine v12.1 — no synth candles; warehouse series first; footer v12 QR. Chart Pro untouched. */
(function () {
  if (window.__jhChartEngineV12) return;
  window.__jhChartEngineV12 = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var LIVE = "https://justhodl.ai";
  var TFS = [["1s","1s","1m","1d"],["1m","1m","1m","5d"],["3m","3m","5m","1mo"],["5m","5m","5m","1mo"],["15m","15m","15m","3mo"],["30m","30m","30m","6mo"],["45m","45m","60m","6mo"],["1h","1h","60m","2y"],["2h","2h","60m","2y"],["4h","4h","60m","2y"],["12h","12h","60m","2y"],["1d","D","1d","5y"],["2d","2D","1d","5y"],["3d","3D","1d","5y"],["5d","5D","1d","5y"],["1w","W","1wk","10y"],["2w","2W","1wk","10y"],["1M","M","1mo","10y"],["3M","3M","1d","10y"]];
  var CHG = [["price","Price"],["dod","DoD"],["wow","WoW"],["mom","MoM"],["qoq","QoQ"],["yoy","YoY"],["ytd","YTD"],["fromhigh","From High"],["fromlow","From Low"],["vsspy","vs SPY"]];
  var BARS = { dod:1, wow:5, mom:21, qoq:63, yoy:252 };
  var TABS = ["SPY","QQQ","IWM","AAPL","MSFT","NVDA","AMZN","META","TSLA","XLE","TLT","GLD"];
  var ISHARES = ["GSG","COMT","EWZS","CMDY","EWZ","LOCK","IAT","IVV","IWM","EEM","LQD","HYG","TLT","IEI"];
  var KINDS = [["candles","Candles"],["hollow","Hollow"],["bars","Bars"],["line","Line"],["area","Area"],["baseline","Baseline"],["heikin","Heikin"],["columns","Columns"],["step","Step"],["hlc","HLC"],["volcandle","Vol candles"],["renko","Renko"],["kagi","Kagi"],["linebreak","Line break"],["pnf","Point & Figure"],["range","Range"]];
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
    {id:"pt",n:"Patterns",g:"X",tools:[["xabcd","X","XABCD"],["hs","HS","Head & Shoulders"],["elliott","E","Elliott impulse"],["gartley","Gart","Gartley"],["abcd","AB","ABCD"]]},
    {id:"ex",n:"Extras",g:"★",tools:[["avwap","VW","Anchored VWAP"],["cyclic","⊙","Cyclic lines"],["path","~","Path"],["curve","∽","Curve"],["buy","B","Buy"],["sell","S","Sell"],["fibwedge","W","Fib wedge"]]}
  ];
  var INDS = [
    {id:"sma9",n:"SMA 9",c:"#26c6da",on:0,k:"sma",p:9,cat:"MA"},
    {id:"sma20",n:"SMA 20",c:"#26c6da",on:0,k:"sma",p:20,cat:"MA"},
    {id:"sma50",n:"SMA 50",c:"#2962ff",on:0,k:"sma",p:50,cat:"MA"},
    {id:"sma100",n:"SMA 100",c:"#7e57c2",on:0,k:"sma",p:100,cat:"MA"},
    {id:"sma200",n:"SMA 200",c:"#ff6d00",on:0,k:"sma",p:200,cat:"MA"},
    {id:"sma250",n:"SMA 250",c:"#e91e63",on:0,k:"sma",p:250,cat:"MA"},
    {id:"ema9",n:"EMA 9",c:"#26a69a",on:0,k:"ema",p:9,cat:"MA"},
    {id:"ema21",n:"EMA 21",c:"#42a5f5",on:0,k:"ema",p:21,cat:"MA"},
    {id:"ema50",n:"EMA 50",c:"#5c6bc0",on:0,k:"ema",p:50,cat:"MA"},
    {id:"ema200",n:"EMA 200",c:"#8d6e63",on:0,k:"ema",p:200,cat:"MA"},
    {id:"ema250",n:"EMA 250",c:"#089981",on:0,k:"ema",p:250,cat:"MA"},
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
    {id:"pc",n:"Prev close",c:"#787b86",on:0,k:"pc",cat:"Levels"},
    {id:"dema20",n:"DEMA 20",c:"#00838f",on:0,k:"dema",p:20,cat:"MA"},
    {id:"tema20",n:"TEMA 20",c:"#6a1b9a",on:0,k:"tema",p:20,cat:"MA"},
    {id:"t3",n:"T3 8",c:"#ef6c00",on:0,k:"t3",p:8,cat:"MA"},
    {id:"mcg",n:"McGinley 14",c:"#2e7d32",on:0,k:"mcg",p:14,cat:"MA"},
    {id:"avwap",n:"Session VWAP",c:"#6a1b9a",on:0,k:"svwap",cat:"Volume"},
    {id:"frac",n:"Fractals",c:"#455a64",on:0,k:"frac",cat:"Trend"},
    {id:"sqz",n:"Squeeze",c:"#c62828",on:0,k:"sqz",cat:"Volatility"},
    {id:"pat",n:"Candle patterns",c:"#37474f",on:0,k:"pat",cat:"Patterns"},
    {id:"ce",n:"Chandelier 22",c:"#00897b",on:0,k:"ce",cat:"Trend"},
    {id:"vwapb",n:"VWAP bands",c:"#6a1b9a",on:0,k:"vwapb",cat:"Volume"},
    {id:"cam",n:"Camarilla pivots",c:"#5d4037",on:0,k:"cam",cat:"Levels"}
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
    {id:"dpo",n:"DPO 20",on:0,cat:"Momentum"},
    {id:"vortex",n:"Vortex 14",on:0,cat:"Trend"},
    {id:"eld",n:"Elder Ray",on:0,cat:"Trend"},
    {id:"kst",n:"Know Sure Thing",on:0,cat:"Momentum"},
    {id:"fisher",n:"Fisher 10",on:0,cat:"Momentum"},
    {id:"rvi",n:"RVI 10",on:0,cat:"Momentum"},
    {id:"cmo",n:"CMO 14",on:0,cat:"Momentum"},
    {id:"mass",n:"Mass Index",on:0,cat:"Volatility"},
    {id:"copp",n:"Coppock",on:0,cat:"Momentum"},
    {id:"cvd",n:"CVD (est.)",on:0,cat:"Volume"},
    {id:"rvol",n:"RVOL 20",on:0,cat:"Volume"}
  ];
  var UP="#089981", DN="#f23645", BG="#ffffff", ACC="#2962ff";
  var CUSTOM_KEY="jh-chart-custom-lists", LAY_KEY="jh-chart-v8-layout", ALERT_KEY="jh-chart-alerts", DRAW_KEY="jh-chart-drawings", NOTE_KEY="jh-chart-notes", FLAG_KEY="jh-chart-flags", TPL_KEY="jh-chart-templates", FAV_KEY="jh-chart-favs", PAPER_KEY="jh-chart-paper";
  var active="SPY", tf="1d", mode="price", kind="candles", scaleMode=0;
  var quotes={}, lists=[], listId="ishares", letter="", filter="", sortCol="sym", sortDir=1;
  var lastBars=[], series=[], spyBars=null, barCache={}, compare=[], mainSeries=null;
  var drawings=[], undo=[], redo=[], tool="cursor", magnet=true, pending=null, objOpen=false, vpOn=true;
  var stayTool=false, hideDraw=false, lockDraw=false, drawColor=ACC, drawW=1.2;
  var wtab="list", wsub="watch", watchOpen=true, layout=1, gridOn=true, watermark=true, invert=false, hiLo=true, crossMode=0;
  var tzOff=0, tzName="UTC", notes={}, flags={}, finCache={}, ssTab="all", ssSel=0, ssRows=[];
  var replay={on:false,i:0,speed:1,timer:null,full:[]};
  var alerts=[], news=[], toastT=null, selDraw=null, paneCharts=[], paneSyms=[];
  var chart, chart2, chart3, chart4, oscSeries=[];
  var lastSource="—", volOn=true, chartOnly=false, clipDraw=null, typeBuf="", typeT=null, favs=[], syncing=false;
  var overlayMap={}, ghostPt=null, dragState=null, patternsOn=true, newsMarks=true, magnetMode=2;
  var dark=false, liveOn=true, liveT=null, dwinOn=false, miniOn=false, leftOn=false, dockTab="";
  var oscCharts=[], lastTest=null, screenFilt="", paper={cash:100000,positions:{},trades:[],realized:0};
  var lastVP={poc:null,vah:null,val:null};
  var tape={prints:[],src:"",vwap:null,buyVol:0,sellVol:0,delta:0,bid:null,ask:null,bidSz:null,askSz:null,filt:"all",sym:"",note:""};
  var tapeT=null;
  var screenRows=[];

  function bare(s){ s=String(s||""); return s.indexOf(":")>=0?s.split(":").pop():s; }
  function yahooSym(s){
    s=bare(s).toUpperCase();
    if(/USDT$/.test(s)) return s.slice(0,-4)+"-USD";
    if(/BUSD$/.test(s)) return s.slice(0,-4)+"-USD";
    if(/USDC$/.test(s)) return s.slice(0,-4)+"-USD";
    return s;
  }
  function resolveSym(raw){
    var s=String(raw||"").trim().toUpperCase(), venue="", ticker=s;
    if(s.indexOf(":")>=0){ venue=s.split(":")[0]; ticker=s.split(":").pop(); }
    if(venue==="INDEX"){
      if(ticker==="SPX"||ticker==="SP500"||ticker==="SPXUSD") ticker="^GSPC";
      else if(ticker==="NDX"||ticker==="NDXUSD") ticker="^NDX";
      else if(ticker==="DJI"||ticker==="DJIUSD") ticker="^DJI";
      else if(ticker==="VIX") ticker="^VIX";
      else if(ticker==="DXY") ticker="DX-Y.NYB";
      else if(ticker==="BTCUSD") ticker="BTCUSDT";
      else ticker="^"+ticker.replace(/^\^/,"");
    }
    if(venue==="CBOE" && ticker==="VIX") ticker="^VIX";
    if(venue==="TVC" && (ticker==="DXY"||ticker==="DXYUSD")) ticker="DX-Y.NYB";
    if(venue==="TVC" && ticker==="SPX") ticker="^GSPC";
    if(ticker==="BTCUSD") ticker="BTCUSDT";
    if(ticker==="ETHUSD") ticker="ETHUSDT";
    var ys=yahooSym(ticker);
    if(/^\^/.test(ticker) || ticker==="DX-Y.NYB") ys=ticker;
    return {raw:s, venue:venue, ticker:ticker, yahoo:ys};
  }
  function nyOffset(){
    try{
      var f=new Intl.DateTimeFormat("en-US",{timeZone:"America/New_York", timeZoneName:"shortOffset"});
      var p=f.formatToParts(new Date()).find(function(x){ return x.type==="timeZoneName"; });
      var m=/GMT([+-]?\d+)/.exec((p&&p.value)||"");
      if(m) return parseInt(m[1],10);
    }catch(e){}
    return -4;
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
  function spec(tfId){ for(var i=0;i<TFS.length;i++) if(TFS[i][0]===tfId) return TFS[i]; return TFS.filter(function(t){return t[0]==="1d";})[0]||TFS[0]; }
  function loadJSON(k, fb){ try{ var x=JSON.parse(localStorage.getItem(k)||""); return x||fb; }catch(e){ return fb; } }
  function saveJSON(k,v){ try{ localStorage.setItem(k, JSON.stringify(v)); }catch(e){} }
  function allTools(){ var a=[]; GROUPS.forEach(function(g){ a=a.concat(g.tools); }); return a; }
  function toolName(k){ var t=allTools().filter(function(x){return x[0]===k;})[0]; return t?t[2]:k; }
  function needPts(k){
    if(/^(hline|vline|hray|text|pricelbl|note|flag|arrowmark|buy|sell|avwap)$/.test(k)) return 1;
    if(/^(channel|disjoint|flattop|pitchfork|schiff|triangle)$/.test(k)) return 3;
    if(k==="xabcd"||k==="gartley") return 5; if(k==="elliott") return 6; if(k==="abcd") return 4; if(k==="hs") return 7;
    if(k==="polyline"||k==="brush"||k==="highlight"||k==="path"||k==="curve") return 99;
    if(k==="cursor"||k==="cross"||k==="eraser") return 0;
    return 2;
  }

  function sma(d,n){ var o=[],s=0; for(var i=0;i<d.length;i++){ s+=d[i].close; if(i>=n)s-=d[i-n].close; if(i>=n-1)o.push({time:d[i].time,value:s/n}); } return o; }
  function ema(d,n){ if(!d.length)return[]; var o=[],k=2/(n+1),p=d[0].close; for(var i=0;i<d.length;i++){ p=d[i].close*k+p*(1-k); if(i>=n-1)o.push({time:d[i].time,value:p}); } return o; }
  function wma(d,n){ var o=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n)continue; var s=0,w=0; for(j=0;j<n;j++){ var ww=j+1; s+=d[i-n+1+j].close*ww; w+=ww; } o.push({time:d[i].time,value:s/w}); } return o; }
  function hull(d,n){ var n2=Math.max(1,Math.round(n/2)), ns=Math.max(1,Math.round(Math.sqrt(n))); var e1=wma(d,n2), e2=wma(d,n), map={},i; for(i=0;i<e2.length;i++) map[e2[i].time]=e2[i].value; var raw=[]; for(i=0;i<e1.length;i++) if(map[e1[i].time]!=null) raw.push({time:e1[i].time,close:2*e1[i].value-map[e1[i].time]}); return wma(raw,ns); }
  function vwma(d,n){ var o=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n)continue; var pv=0,vv=0; for(j=i-n+1;j<=i;j++){ pv+=d[j].close*d[j].volume; vv+=d[j].volume; } if(vv)o.push({time:d[i].time,value:pv/vv}); } return o; }
  function vwap(d){ var o=[],pv=0,vv=0,i; for(i=0;i<d.length;i++){ var tp=(d[i].high+d[i].low+d[i].close)/3; pv+=tp*d[i].volume; vv+=d[i].volume; if(vv)o.push({time:d[i].time,value:pv/vv}); } return o; }
  function twap(d){ var o=[],s=0,i; for(i=0;i<d.length;i++){ s+=(d[i].high+d[i].low+d[i].close)/3; o.push({time:d[i].time,value:s/(i+1)}); } return o; }
  function cvd(d){ var o=[],v=0,i; for(i=0;i<d.length;i++){ var hl=d[i].high-d[i].low; var buy=hl? ((d[i].close-d[i].low)/hl)*d[i].volume : d[i].volume*0.5; v+= buy-(d[i].volume-buy); o.push({time:d[i].time,value:v}); } return o; }
  function rvolSeries(d,n){ n=n||20; var o=[],s=0,i; for(i=0;i<d.length;i++){ s+=d[i].volume; if(i>=n) s-=d[i-n].volume; if(i>=n-1){ var avg=s/n; o.push({time:d[i].time,value:avg?d[i].volume/avg:1}); } } return o; }
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
  function dema(d,n){ var e1=ema(d,n), e2=ema(e1.map(function(p){return {time:p.time,close:p.value};}),n), map={},i,o=[]; for(i=0;i<e2.length;i++) map[e2[i].time]=e2[i].value; for(i=0;i<e1.length;i++) if(map[e1[i].time]!=null) o.push({time:e1[i].time,value:2*e1[i].value-map[e1[i].time]}); return o; }
  function tema(d,n){ var e1=ema(d,n), e2=ema(e1.map(function(p){return {time:p.time,close:p.value};}),n), e3=ema(e2.map(function(p){return {time:p.time,close:p.value};}),n), m2={},m3={},i,o=[]; for(i=0;i<e2.length;i++) m2[e2[i].time]=e2[i].value; for(i=0;i<e3.length;i++) m3[e3[i].time]=e3[i].value; for(i=0;i<e1.length;i++) if(m2[e1[i].time]!=null&&m3[e1[i].time]!=null) o.push({time:e1[i].time,value:3*e1[i].value-3*m2[e1[i].time]+m3[e1[i].time]}); return o; }
  function t3ma(d,n){ return ema(ema(ema(d,n).map(function(p){return {time:p.time,close:p.value};}),n).map(function(p){return {time:p.time,close:p.value};}),n); }
  function mcginley(d,n){ n=n||14; if(!d.length) return []; var o=[], md=d[0].close, i; for(i=0;i<d.length;i++){ var k=n*Math.pow(md/Math.max(d[i].close,1e-12),4); md=md+(d[i].close-md)/Math.max(k,1); if(i>=n-1) o.push({time:d[i].time,value:md}); } return o; }
  function fractals(d){ var up=[],dn=[],i; for(i=2;i<d.length-2;i++){ if(d[i].high>d[i-1].high&&d[i].high>d[i-2].high&&d[i].high>d[i+1].high&&d[i].high>d[i+2].high) up.push({time:d[i].time,value:d[i].high}); if(d[i].low<d[i-1].low&&d[i].low<d[i-2].low&&d[i].low<d[i+1].low&&d[i].low<d[i+2].low) dn.push({time:d[i].time,value:d[i].low}); } return {up:up,dn:dn}; }
  function squeeze(d){ var bb=bbands(d,20,2), kc=keltner(d,20), o=[],i,bm={},km={}; for(i=0;i<bb.up.length;i++) bm[bb.up[i].time]=bb.up[i].value-bb.dn[i].value; for(i=0;i<kc.up.length;i++) km[kc.up[i].time]=kc.up[i].value-kc.dn[i].value; for(i=0;i<d.length;i++){ var t=d[i].time; if(bm[t]!=null&&km[t]!=null) o.push({time:t,value:bm[t]<km[t]?1:0}); } return o; }
  function vortex(d,n){ n=n||14; var o=[],i,j; for(i=n;i<d.length;i++){ var vp=0,vm=0,tr=0; for(j=i-n+1;j<=i;j++){ vp+=Math.abs(d[j].high-d[j-1].low); vm+=Math.abs(d[j].low-d[j-1].high); tr+=Math.max(d[j].high-d[j].low, Math.abs(d[j].high-d[j-1].close), Math.abs(d[j].low-d[j-1].close)); } o.push({time:d[i].time,value:tr?vp/tr:0}); } return o; }
  function elder(d,n){ n=n||13; var e=ema(d,n), map={},i,o=[]; for(i=0;i<e.length;i++) map[e[i].time]=e[i].value; for(i=0;i<d.length;i++) if(map[d[i].time]!=null) o.push({time:d[i].time,value:d[i].high-map[d[i].time]}); return o; }
  function kst(d){ function rc(n){ return roc(d,n); } function sm(arr,n){ return sma(arr.map(function(p){return {time:p.time,close:p.value};}),n); } var r1=sm(rc(10),10), r2=sm(rc(15),10), r3=sm(rc(20),10), r4=sm(rc(30),15), m={},i,o=[]; r2.forEach(function(p){m[p.time]=(m[p.time]||0)+p.value*2;}); r3.forEach(function(p){m[p.time]=(m[p.time]||0)+p.value*3;}); r4.forEach(function(p){m[p.time]=(m[p.time]||0)+p.value*4;}); for(i=0;i<r1.length;i++) if(m[r1[i].time]!=null) o.push({time:r1[i].time,value:r1[i].value+m[r1[i].time]}); return o; }
  function fisher(d,n){ n=n||10; var o=[],i,j; for(i=0;i<d.length;i++){ if(i+1<n) continue; var hi=-1e99,lo=1e99; for(j=i-n+1;j<=i;j++){ if(d[j].high>hi)hi=d[j].high; if(d[j].low<lo)lo=d[j].low; } var v=hi===lo?0:2*((d[i].close-lo)/(hi-lo)-0.5); v=Math.max(-0.999,Math.min(0.999,v)); var prev=o.length?o[o.length-1].raw:0; var val=0.5*Math.log((1+v)/(1-v))+0.5*prev; o.push({time:d[i].time,value:val,raw:val}); } return o; }
  function rvi(d,n){ n=n||10; var o=[],i,j; for(i=n;i<d.length;i++){ var num=0,den=0; for(j=0;j<n;j++){ num+=d[i-j].close-d[i-j].open; den+=d[i-j].high-d[i-j].low; } o.push({time:d[i].time,value:den?num/den:0}); } return o; }
  function cmo(d,n){ n=n||14; var o=[],i; for(i=n;i<d.length;i++){ var up=0,dn=0,j; for(j=0;j<n;j++){ var ch=d[i-j].close-d[i-j-1].close; if(ch>0) up+=ch; else dn-=ch; } o.push({time:d[i].time,value:(up+dn)?100*(up-dn)/(up+dn):0}); } return o; }
  function massIndex(d,n){ n=n||25; var hl=d.map(function(b){return {time:b.time,close:b.high-b.low};}); var e1=ema(hl,9), e2=ema(e1.map(function(p){return {time:p.time,close:p.value};}),9), ratio=[],i,map={}; for(i=0;i<e2.length;i++) map[e2[i].time]=e2[i].value; for(i=0;i<e1.length;i++) if(map[e1[i].time]) ratio.push({time:e1[i].time,value:e1[i].value/map[e1[i].time]}); var o=[],s=0; for(i=0;i<ratio.length;i++){ s+=ratio[i].value; if(i>=n) s-=ratio[i-n].value; if(i>=n-1) o.push({time:ratio[i].time,value:s}); } return o; }
  function coppock(d){ var r14=roc(d,14), r11=roc(d,11), map={},i,sum=[]; for(i=0;i<r11.length;i++) map[r11[i].time]=r11[i].value; for(i=0;i<r14.length;i++) if(map[r14[i].time]!=null) sum.push({time:r14[i].time,close:r14[i].value+map[r14[i].time]}); return wma(sum,10); }
  function chandelier(d,n,m){ n=n||22; m=m||3; var a=atr(d,n), o=[],i,j,am={}; for(i=0;i<a.length;i++) am[a[i].time]=a[i].value; for(i=n;i<d.length;i++){ var hi=-1e99; for(j=i-n+1;j<=i;j++) if(d[j].high>hi) hi=d[j].high; var at=am[d[i].time]; if(at!=null) o.push({time:d[i].time,value:hi-m*at}); } return o; }
  function vwapBands(d){ var mid=vwap(d), a=atr(d,14), map={},i,up=[],dn=[]; for(i=0;i<a.length;i++) map[a[i].time]=a[i].value; for(i=0;i<mid.length;i++){ var at=map[mid[i].time]; if(at==null) continue; up.push({time:mid[i].time,value:mid[i].value+at}); dn.push({time:mid[i].time,value:mid[i].value-at}); } return {m:mid,up:up,dn:dn}; }
  function camarilla(d){ if(d.length<2) return null; var b=d[d.length-2], r=b.high-b.low, t=d[d.length-1].time; return {r3:{time:t,value:b.close+r*1.1/4}, r4:{time:t,value:b.close+r*1.1/2}, s3:{time:t,value:b.close-r*1.1/4}, s4:{time:t,value:b.close-r*1.1/2}, pp:{time:t,value:(b.high+b.low+b.close)/3}}; }
  function pearson(a,b){ var n=Math.min(a.length,b.length); if(n<8) return null; var sa=0,sb=0,i; for(i=0;i<n;i++){ sa+=a[i]; sb+=b[i]; } var ma=sa/n, mb=sb/n, num=0, da=0, db=0; for(i=0;i<n;i++){ var xa=a[i]-ma, xb=b[i]-mb; num+=xa*xb; da+=xa*xa; db+=xb*xb; } var den=Math.sqrt(da*db); return den?num/den:0; }
  function rets(d){ var o=[],i; for(i=1;i<d.length;i++) if(d[i-1].close) o.push(d[i].close/d[i-1].close-1); return o; }
  function maxdd(d){ var peak=-1e99, dd=0, i; for(i=0;i<d.length;i++){ if(d[i].close>peak) peak=d[i].close; var x=peak? d[i].close/peak-1:0; if(x<dd) dd=x; } return dd; }
  function pal(){ return dark?{bg:"#131722",text:"#787b86",grid:"#1e222d",border:"#2a2e39",fg:"#d1d4dc",hud:"rgba(19,23,34,.92)"}:{bg:"#ffffff",text:"#6a6d78",grid:"#f0f3fa",border:"#e0e3eb",fg:"#131722",hud:"rgba(255,255,255,.92)"}; }
  function applyTheme(repaint){
    document.documentElement.setAttribute("data-theme", dark?"dark":"light");
    var meta=document.querySelector('meta[name=theme-color]'); if(meta) meta.setAttribute("content", dark?"#131722":"#ffffff");
    var p=pal(); BG=p.bg;
    var opts={ layout:{background:{type:"solid",color:p.bg},textColor:p.text,fontFamily:"IBM Plex Sans,sans-serif",fontSize:11}, grid:{vertLines:{color:gridOn?p.grid:"transparent"},horzLines:{color:gridOn?p.grid:"transparent"}}, rightPriceScale:{borderColor:p.border,invertScaledValues:invert,visible:true}, leftPriceScale:{visible:leftOn,borderColor:p.border}, timeScale:{borderColor:p.border} };
    [chart,chart2,chart3,chart4].concat(oscCharts).forEach(function(c){ if(c) try{ c.applyOptions(opts); }catch(e){} });
    if(repaint && lastBars.length) paint(lastBars);
    saveLay();
  }
  function candlePatterns(d){
    var out=[], i;
    for(i=2;i<d.length;i++){
      var a=d[i-2], b=d[i-1], c=d[i], body=Math.abs(c.close-c.open), rng=c.high-c.low||1e-12, up=c.close>=c.open;
      if(body/rng<0.12) out.push({time:c.time,n:"Doji",price:c.high});
      else if(up && (c.open-c.low)>2*body && (c.high-c.close)<body*0.6) out.push({time:c.time,n:"Hammer",price:c.low});
      else if(!up && (c.high-c.open)>2*body && (c.close-c.low)<body*0.6) out.push({time:c.time,n:"Shooting star",price:c.high});
      else if(b.close<b.open && c.close>c.open && c.close>=b.open && c.open<=b.close) out.push({time:c.time,n:"Bull engulf",price:c.low});
      else if(b.close>b.open && c.close<c.open && c.close<=b.open && c.open>=b.close) out.push({time:c.time,n:"Bear engulf",price:c.high});
      else if(a.close<b.close && b.close<c.close && c.close>c.open && a.close<a.open) out.push({time:c.time,n:"3 soldiers",price:c.high});
    }
    return out.slice(-36);
  }
  function toPnf(d,box){
    if(d.length<4) return d;
    box=box||Math.max(d[d.length-1].close*0.02,1e-12);
    var cols=[], last=Math.floor(d[0].close/box)*box, dir=0, i;
    cols.push({time:d[0].time,open:last,close:last,high:last,low:last,volume:0});
    for(i=1;i<d.length;i++){
      var c=d[i].close;
      if(dir>=0 && c>=last+box){ last+=box; cols.push({time:d[i].time,open:last-box,close:last,high:last,low:last-box,volume:d[i].volume}); dir=1; }
      else if(dir<=0 && c<=last-box){ last-=box; cols.push({time:d[i].time,open:last+box,close:last,high:last+box,low:last,volume:d[i].volume}); dir=-1; }
      else if(dir>0 && c<=last-3*box){ dir=-1; last-=box; cols.push({time:d[i].time,open:last+box,close:last,high:last+box,low:last,volume:d[i].volume}); }
      else if(dir<0 && c>=last+3*box){ dir=1; last+=box; cols.push({time:d[i].time,open:last-box,close:last,high:last,low:last-box,volume:d[i].volume}); }
    }
    return cols.length?cols:d;
  }
  function toRange(d,pct){
    pct=pct||0.01; if(d.length<2) return d;
    var sz=Math.max(d[d.length-1].close*pct,1e-12), o=[], cur=d[0], i;
    for(i=1;i<d.length;i++){
      cur={time:d[i].time,open:cur.open,high:Math.max(cur.high,d[i].high),low:Math.min(cur.low,d[i].low),close:d[i].close,volume:(cur.volume||0)+(d[i].volume||0)};
      if(Math.abs(cur.close-cur.open)>=sz){ o.push(cur); cur=d[i]; }
    }
    if(o.length) o.push(cur); return o.length?o:d;
  }
  function valAt(arr,t){
    if(!arr||!arr.length) return null;
    var lo=0, hi=arr.length-1, best=arr[0];
    while(lo<=hi){ var m=(lo+hi)>>1; if(arr[m].time===t) return arr[m].value!=null?arr[m].value:arr[m].close; if(arr[m].time<t){ best=arr[m]; lo=m+1; } else hi=m-1; }
    return best.value!=null?best.value:best.close;
  }
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
        out.push({time:+tm,open:+(b.open||b.o||c3),high:+(b.high||b.h||c3),low:+(b.low||b.l||c3),close:c3,volume:+(function(){var cand=b.volume!=null?b.volume:(b.v!=null?b.v:(b.vol!=null?b.vol:(b.Volume!=null?b.Volume:b.value)));var n=+cand;if(!isFinite(n)||n<0)return 0;if(b.volume==null&&b.v==null&&b.vol==null&&b.Volume==null&&n>0&&n<c3*8)return 0;return n;})()});
      }
    }
    if(!out.length && j.chart && j.chart.result && j.chart.result[0]){
      var res=j.chart.result[0], q=(res.indicators.quote||[])[0]||{}, ts=res.timestamp||[];
      for(i=0;i<ts.length;i++){ if(q.close[i]==null||!isFinite(+q.close[i])) continue; out.push({time:ts[i],open:+(q.open[i]||q.close[i]),high:+(q.high[i]||q.close[i]),low:+(q.low[i]||q.close[i]),close:+q.close[i],volume:+(q.volume[i]||0)}); }
    }
    if(!out.length && Array.isArray(j.timestamp) && Array.isArray(j.close)){
      for(i=0;i<j.timestamp.length;i++){ if(j.close[i]==null||!isFinite(+j.close[i])) continue; out.push({time:j.timestamp[i],open:+((j.open&&j.open[i])||j.close[i]),high:+((j.high&&j.high[i])||j.close[i]),low:+((j.low&&j.low[i])||j.close[i]),close:+j.close[i],volume:+((j.volume&&j.volume[i])||0)}); }
    }
    return uniq(sanitizeBars(out));
  }
  function sanitizeBars(rows){
    if(!rows.length) return rows;
    var mids=rows.map(function(b){return b.close;}).sort(function(a,b){return a-b;});
    var med=mids[Math.floor(mids.length/2)]||0;
    if(!med) return rows;
    var cap=med*8, floor=med/20;
    return rows.map(function(b){
      var hi=b.high, lo=b.low;
      if(hi>cap) hi=Math.max(b.close,b.open)*1.08;
      if(lo>0 && lo<floor) lo=Math.min(b.close,b.open)*0.92;
      if(lo<=0) lo=Math.min(b.close,b.open);
      return {time:b.time,open:b.open,high:Math.max(hi,b.close,b.open),low:Math.min(lo,b.close,b.open),close:b.close,volume:b.volume||0};
    });
  }
  function volScore(d){
    var n=0,i; for(i=0;i<d.length;i++) if(d[i].volume>0) n++;
    return n;
  }
  function warehouse(path){
    var file=String(path||"").replace(/^\/data\//,"");
    var urls=["/api/warehouse?file="+encodeURIComponent(file), LIVE+"/data/"+file];
    if(/justhodl\.ai$/i.test(location.hostname)) urls.unshift("/data/"+file);
    return urls;
  }
  async function fetchJson(url){ var r=await fetch(url,{cache:"no-store"}); if(!r.ok) throw new Error(String(r.status)); return r.json(); }
  function synth(sym,n){
    n=n||420;
    var h=2166136261,i; for(i=0;i<String(sym).length;i++) h=Math.imul(h^sym.charCodeAt(i),16777619);
    var rng=h>>>0||1; function rnd(){ rng=(Math.imul(1664525,rng)+1013904223)>>>0; return rng/4294967296; }
    var px=/PEPE/i.test(sym)?0.00000342:/BTC/i.test(sym)?70000:/ETH/i.test(sym)?3500:40+rnd()*80;
    var now=Math.floor(Date.now()/1000)-86400, out=[], sp=spec(tf), step=86400;
    if(sp[0]==="1m"||sp[0]==="1s") step=60;
    else if(sp[0]==="5m") step=300;
    else if(sp[0]==="15m") step=900;
    else if(sp[0]==="30m") step=1800;
    else if(sp[0]==="1h"||sp[0]==="4h"||sp[0]==="12h") step=3600;
    else if(sp[0]==="1w"||sp[0]==="2w") step=604800;
    else if(sp[0]==="1M"||sp[0]==="3M") step=2592000;
    for(i=n;i>=0;i--){
      var o=px; px=Math.max(px*(1+(rnd()-0.48)*0.028),1e-10);
      out.push({time:now-i*step,open:o,high:Math.max(o,px)*1.008,low:Math.min(o,px)*0.992,close:px,volume:1e6*(0.4+rnd())});
    }
    return uniq(out);
  }
  async function klines(sym, tfId){
    var rs=resolveSym(sym), t=rs.ticker, sp=spec(tfId), ys=rs.yahoo;
    var key=t+"|"+tfId, now=Date.now();
    if(barCache[key] && barCache[key].at && now-barCache[key].at<60000 && barCache[key].d && barCache[key].d.length>=8){
      lastSource=barCache[key].src||lastSource; return barCache[key].d;
    }
    var urls=[
      PROXY+"/ohlc?ticker="+encodeURIComponent(t)+"&span="+((/^[0-9]+[sm]$/.test(tfId)||tfId==="1m"||tfId==="3m"||tfId==="5m"||tfId==="15m"||tfId==="30m"||tfId==="45m")?"minute": (/h$/.test(tfId)?"hour":"day")),
      PROXY+"/yf-ohlc?symbol="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],
      PROXY+"/yf-ohlc?symbol="+encodeURIComponent(t)+"&range="+sp[3]+"&interval="+sp[2],
      LIVE+"/data/series/"+encodeURIComponent(ys)+".json",
      LIVE+"/data/series/"+encodeURIComponent(t)+".json",
      "/api/klines?symbol="+encodeURIComponent(t)+"&interval="+encodeURIComponent(sp[0])+"&limit=1000",
      "/api/yahoo?ticker="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],
      LIVE+"/data/series/"+encodeURIComponent(ys)+".json",
      LIVE+"/data/series/"+encodeURIComponent(t)+".json"
    ];
    for(var i=0;i<urls.length;i++){
      try{
        var raw=await fetchJson(urls[i]); var d=toBars(raw);
        if(d.length>8000) d=d.slice(-8000); if(d.length>=8){
          lastSource=(raw&& (raw.warehouse_key||raw.source)) || (urls[i].indexOf("/ohlc")>=0?"warehouse": urls[i].indexOf("/api/klines")===0?"binance": urls[i].indexOf(PROXY)===0?"proxy": "feed");
          var scored=volScore(d);
          if(!raw.warehouse_key && scored<d.length*0.2 && i<urls.length-1) continue;
          barCache[key]={d:d, at:now, src:lastSource};
          return d;
        }
      }catch(e){}
    }
    lastSource="unavailable";
    barCache[key]={d:[], at:now, src:"unavailable"};
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
    spyBars=await klines("SPY", tf);
    var spy=spyBars||[];
    if(spy.length<2) spy=await klines("SPY","1d");
    if(spy.length<2) return [];
    var j=0, joined=[];
    for(var i=0;i<d.length;i++){
      var ts=d[i].time;
      while(j+1<spy.length && Math.abs(spy[j+1].time-ts)<=Math.abs(spy[j].time-ts)) j++;
      if(Math.abs(spy[j].time-ts)>7*86400) continue;
      joined.push({time:ts,value:d[i].close,spy:spy[j].close});
    }
    if(joined.length<2) return [];
    var t0=joined[0].value,s0=joined[0].spy;
    return joined.map(function(p){ return {time:p.time,value:((p.value/t0)/(p.spy/s0)-1)*100}; });
  }

  (function bootLay(){
    var lay=loadJSON(LAY_KEY,null);
    if(lay){ if(lay.gridOn!=null) gridOn=lay.gridOn; if(lay.magnet!=null) magnet=lay.magnet; if(lay.magnetMode!=null) magnetMode=lay.magnetMode; if(lay.kind) kind=lay.kind; if(lay.tf) tf=lay.tf; if(lay.invert!=null) invert=lay.invert; if(lay.hiLo!=null) hiLo=lay.hiLo; if(lay.crossMode!=null) crossMode=lay.crossMode; if(lay.tzName) tzName=lay.tzName; if(lay.tzOff!=null) tzOff=lay.tzOff; if(lay.stayTool!=null) stayTool=lay.stayTool; if(lay.layout) layout=lay.layout; if(lay.dark!=null) dark=lay.dark; if(lay.liveOn!=null) liveOn=lay.liveOn; if(lay.dwinOn!=null) dwinOn=lay.dwinOn; if(lay.miniOn!=null) miniOn=lay.miniOn; if(lay.leftOn!=null) leftOn=lay.leftOn; }
    volOn=true; vpOn=true;
    magnet=magnetMode>0;
    try{
      var h=String(location.hash||"").replace(/^#/,"");
      if(h){ var sp=new URLSearchParams(h); if(sp.get("s")) active=sp.get("s").toUpperCase(); if(sp.get("tf")) tf=sp.get("tf"); if(sp.get("k")) kind=sp.get("k"); if(sp.get("m")) mode=sp.get("m"); if(sp.get("th")==="d") dark=true; if(TABS.indexOf(bare(active))<0) TABS.unshift(bare(active)); }
    }catch(e){}
    document.documentElement.setAttribute("data-theme", dark?"dark":"light");
    BG=pal().bg;
    var ppr=loadJSON(PAPER_KEY,null); if(ppr && typeof ppr.cash==="number") paper=ppr;
  })();

  var LW=window.LightweightCharts, host=document.getElementById("host");
  if(!LW||!host){ var qel=document.getElementById("quote"); if(qel) qel.textContent="Chart library missing"; return; }
  function mkChart(el){
    var p=pal();
    var c=LW.createChart(el,{
      width: Math.max(el.clientWidth||el.parentElement.clientWidth||800, 40),
      height: Math.max(el.clientHeight||el.parentElement.clientHeight||420, 40),
      autoSize:true,
      layout:{ background:{type:"solid",color:p.bg}, textColor:p.text, fontFamily:"IBM Plex Sans,sans-serif", fontSize:11 },
      grid:{ vertLines:{ color: gridOn?p.grid:"transparent" }, horzLines:{ color: gridOn?p.grid:"transparent" } },
      rightPriceScale:{ borderColor:p.border, scaleMargins:{ top:0.06, bottom:0.18 }, invertScaledValues:invert },
      leftPriceScale:{ visible:leftOn, borderColor:p.border },
      timeScale:{ borderColor:p.border, timeVisible:true, rightOffset:6 },
      crosshair:{ mode: crossMode },
      localization:{ priceFormatter:function(p){ return mode==="price"?fmt(p):p.toFixed(2)+"%"; } }
    });
    function fit(){
      var w=el.clientWidth, h=el.clientHeight;
      if(w>20 && h>20) try{ c.resize(w,h); }catch(e){}
    }
    if(typeof ResizeObserver!=="undefined"){
      var ro=new ResizeObserver(fit);
      ro.observe(el);
      if(el.parentElement) ro.observe(el.parentElement);
    }
    setTimeout(fit, 0);
    setTimeout(fit, 250);
    window.addEventListener("resize", fit);
    return c;
  }
  chart=mkChart(host);
  bindSync(chart);
  function bindSync(c){
    if(!c || c.__jhSync) return;
    c.__jhSync=true;
    c.timeScale().subscribeVisibleLogicalRangeChange(function(r){
      if(!r || syncing) return;
      syncing=true;
      [chart, chart2, chart3, chart4].concat(oscCharts).forEach(function(x){
        if(x && x!==c) try{ x.timeScale().setVisibleLogicalRange(r); }catch(e){}
      });
      syncing=false;
    });
  }
  function wipe(){ series.forEach(function(s){ try{ chart.removeSeries(s); }catch(e){} }); series=[]; mainSeries=null; }

  function addLine(pts, color, w){ if(!pts||!pts.length) return; var s=chart.addLineSeries({ color:color, lineWidth:w||1, lastValueVisible:false, priceLineVisible:false }); s.setData(pts); series.push(s); return s; }
  function addPriceLine(px, color, title){ if(!mainSeries||px==null) return; try{ mainSeries.createPriceLine({ price:px, color:color||"#787b86", lineWidth:1, lineStyle:2, axisLabelVisible:true, title:title||"" }); }catch(e){} }

  function displayBars(d){
    if(kind==="heikin") return heikin(d);
    if(kind==="renko") return toRenko(d, 0.01);
    if(kind==="linebreak") return toLineBreak(d, 3);
    if(kind==="pnf") return toPnf(d);
    if(kind==="range") return toRange(d, 0.01);
    return d;
  }
  async function paint(d){
    if(!d||!d.length){ document.getElementById("quote").textContent="No bars for "+active; return; }
    wipe(); lastBars=d; try{window.lastBars=d;window.jhActive=active;window.INDS=INDS;window.OSC=OSC;window.paint=paint;window.lastSource=lastSource;}catch(e){}
    var p=pal();
    chart.applyOptions({
      localization:{ priceFormatter:function(p){ return mode==="price"?fmt(p):p.toFixed(2)+"%"; } },
      layout:{ background:{type:"solid",color:p.bg}, textColor:p.text },
      grid:{ vertLines:{color:gridOn?p.grid:"transparent"}, horzLines:{color:gridOn?p.grid:"transparent"} },
      rightPriceScale:{ invertScaledValues:invert, borderColor:p.border },
      leftPriceScale:{ visible:leftOn, borderColor:p.border },
      timeScale:{ borderColor:p.border },
      crosshair:{ mode: crossMode }
    });
    try{ chart.priceScale("right").applyOptions({ mode: mode==="price"?scaleMode:0 }); }catch(e){}
    var wm=document.getElementById("wm"); if(wm) wm.textContent=watermark?active:"";
    overlayMap={};
    if(mode==="price"){
      var display=displayBars(d), c;
      if(kind==="kagi"){ c=chart.addLineSeries({color:UP,lineWidth:2}); c.setData(toKagi(d)); }
      else if(kind==="line"){ c=chart.addLineSeries({color:UP,lineWidth:2}); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
      else if(kind==="step"){ c=chart.addLineSeries({color:UP,lineWidth:2, lineType:1}); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
      else if(kind==="hlc"){ c=chart.addAreaSeries({lineColor:"#2962ff",topColor:"rgba(41,98,255,.18)",bottomColor:"rgba(41,98,255,.02)"}); c.setData(display.map(function(b){return {time:b.time,value:b.high};})); var lo=chart.addLineSeries({color:DN,lineWidth:1,lastValueVisible:false,priceLineVisible:false}); lo.setData(display.map(function(b){return {time:b.time,value:b.low};})); series.push(lo); }
      else if(kind==="area"){ c=chart.addAreaSeries({lineColor:UP,topColor:"rgba(8,153,129,0.28)",bottomColor:"rgba(8,153,129,0.02)"}); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
      else if(kind==="baseline"){ var base=display[0].close; c=chart.addBaselineSeries({ baseValue:{type:"price",price:base}, topLineColor:UP, bottomLineColor:DN }); c.setData(display.map(function(b){return {time:b.time,value:b.close};})); }
      else if(kind==="columns"){ c=chart.addHistogramSeries({}); c.setData(display.map(function(b){return {time:b.time,value:b.close,color:b.close>=b.open?UP:DN};})); }
      else if(kind==="bars"){ c=chart.addBarSeries({upColor:UP,downColor:DN}); c.setData(display); }
      else { var upC=kind==="hollow"?BG:(kind==="volcandle"?null:UP); var volMed=0, vi0;
        if(kind==="volcandle"){ for(vi0=0;vi0<display.length;vi0++) volMed+=display[vi0].volume; volMed/=display.length||1; }
        c=chart.addCandlestickSeries({ upColor: kind==="hollow"?BG:UP, downColor:DN, borderVisible:true, borderUpColor:UP, borderDownColor:DN, wickVisible:true, wickUpColor:UP, wickDownColor:DN });
        if(kind==="volcandle") c.setData(display.map(function(b){ var hot=b.volume>volMed*1.5; return {time:b.time,open:b.open,high:b.high,low:b.low,close:b.close, color: b.close>=b.open?(hot?"#00695c":UP):(hot?"#b71c1c":DN)}; }));
        else c.setData(display);try{if(window.jhNyVwap && window.INDS && INDS.some(function(i){return i.id==="vwap"&&i.on;})){var vw=window.jhNyVwap(display);if(vw&&vw.length){var vs=chart.addLineSeries({color:"#ff6d00",lineWidth:1,lastValueVisible:true,priceLineVisible:false,title:"VWAP NY"});vs.setData(vw);series.push(vs);}}}catch(e){}
        try{var fn=window.jhTapeRead||window.jhVolEvents;if(fn){var out=fn(display);var mk=out&&out.markers?out.markers:out;if(mk&&mk.length){mk=mk.slice(-3);c.setMarkers(mk)};try{if(window.jhRsReady){window.jhRsReady(display).then(function(rs){if(rs&&rs.length&&c.setMarkers){var cur=c.markers||[];try{c.setMarkers((window.jhTapeRead?((window.jhTapeRead(display).markers)||[]):[]).concat(rs));}catch(e2){}}});}}catch(e){}try{if(window.jhMacroIntel){window.jhMacroIntel(display).then(function(mm){});}}catch(e){}if(out&&out.panel){var q=document.getElementById("quote"); if(q&&!/LIVERMORE/.test(q.textContent)){var n=document.createElement("div");n.style.cssText="flex-basis:100%;font-size:10px;color:var(--mut)";n.textContent=out.panel;q.appendChild(n);}}}}catch(e){}
      }
      mainSeries=c; series.push(c);
      if(volOn){
        var v=chart.addHistogramSeries({ priceFormat:{type:"volume"}, priceScaleId:"vol" });
        chart.priceScale("vol").applyOptions({ scaleMargins:{ top:0.74, bottom:0 } });
        v.setData(display.map(function(b,ix){
          var look=Math.min(20, ix), avg=0, j;
          for(j=Math.max(0,ix-look); j<ix; j++) avg+=display[j].volume;
          avg=look?avg/look:b.volume;
          var r=avg?b.volume/avg:1;
          var a=r>=2?0.78: r>=1.4?0.55: r>=1?0.38:0.18;
          return {time:b.time,value:b.volume,color: (b.close>=b.open?"rgba(8,153,129,":"rgba(242,54,69,")+a+")"};
        }));
        series.push(v);
        var vsma=[], ss=0, vn=20, vi;
        for(vi=0;vi<display.length;vi++){ ss+=display[vi].volume; if(vi>=vn) ss-=display[vi-vn].volume; if(vi>=vn-1) vsma.push({time:display[vi].time,value:ss/vn}); }
        if(vsma.length){ var vl=chart.addLineSeries({ color:"#2962ff", lineWidth:1, priceScaleId:"vol", lastValueVisible:false, priceLineVisible:false }); vl.setData(vsma); series.push(vl); }
      }
      INDS.forEach(function(ind){
        if(!ind.on) return;
        function store(id, pts){ overlayMap[id]=pts; addLine(pts, ind.c); }
        if(ind.k==="sma") store(ind.id, sma(d,ind.p));
        if(ind.k==="ema") store(ind.id, ema(d,ind.p));
        if(ind.k==="wma") store(ind.id, wma(d,ind.p));
        if(ind.k==="hull") store(ind.id, hull(d,ind.p));
        if(ind.k==="vwma") store(ind.id, vwma(d,ind.p));
        if(ind.k==="vwap") store(ind.id, vwap(d));
        if(ind.k==="svwap") store(ind.id, vwap(d));
        if(ind.k==="linreg") store(ind.id, linreg(d,ind.p));
        if(ind.k==="dema") store(ind.id, dema(d,ind.p));
        if(ind.k==="tema") store(ind.id, tema(d,ind.p));
        if(ind.k==="t3") store(ind.id, t3ma(d,ind.p));
        if(ind.k==="mcg") store(ind.id, mcginley(d,ind.p));
        if(ind.k==="bb"){ var bb=bbands(d); store(ind.id, bb.m); addLine(bb.up,ind.c); addLine(bb.dn,ind.c); }
        if(ind.k==="kc"){ var kc=keltner(d); store(ind.id, kc.m); addLine(kc.up,ind.c); addLine(kc.dn,ind.c); }
        if(ind.k==="dc"){ var dc=donchian(d); store(ind.id, dc.m); addLine(dc.up,ind.c); addLine(dc.dn,ind.c); }
        if(ind.k==="env"){ var en=envelope(d); store(ind.id, en.m); addLine(en.up,ind.c); addLine(en.dn,ind.c); }
        if(ind.k==="st") store(ind.id, supertrend(d));
        if(ind.k==="sar") store(ind.id, sar(d));
        if(ind.k==="ich"){ var ich=ichimoku(d); store(ind.id, ich.conv); addLine(ich.base,"#e91e63"); addLine(ich.spanA,"#26a69a"); addLine(ich.spanB,"#ab47bc"); }
        if(ind.k==="piv"){ var pv=pivots(d); if(pv.pp){ addPriceLine(pv.pp.value,"#546e7a","P"); addPriceLine(pv.r1.value,DN,"R1"); addPriceLine(pv.s1.value,UP,"S1"); addPriceLine(pv.r2.value,DN,"R2"); addPriceLine(pv.s2.value,UP,"S2"); } }
        if(ind.k==="allig"){ var ag=alligator(d); store(ind.id, ag.jaw); addLine(ag.teeth,"#f23645"); addLine(ag.lips,"#089981"); }
        if(ind.k==="zz") store(ind.id, zigzag(d));
        if(ind.k==="pc" && d.length>1) addPriceLine(d[d.length-2].close, "#787b86", "PDC");
        if(ind.k==="frac"){ var fr=fractals(d); addLine(fr.up, UP, 1); addLine(fr.dn, DN, 1); }
        if(ind.k==="sqz"){ overlayMap.sqz=squeeze(d); }
        if(ind.k==="ce") store(ind.id, chandelier(d));
        if(ind.k==="vwapb"){ var vb=vwapBands(d); store(ind.id, vb.m); addLine(vb.up,ind.c); addLine(vb.dn,ind.c); }
        if(ind.k==="cam"){ var cm=camarilla(d); if(cm){ addPriceLine(cm.r4.value,DN,"R4"); addPriceLine(cm.r3.value,DN,"R3"); addPriceLine(cm.s3.value,UP,"S3"); addPriceLine(cm.s4.value,UP,"S4"); } }
      });
      for(var ci=0;ci<compare.length;ci++){
        try{
          var cb=await klines(compare[ci], tf);
          if(cb.length<2||d.length<2) continue;
          var joined=[], k=0, j;
          for(j=0;j<d.length;j++){
            while(k+1<cb.length && Math.abs(cb[k+1].time-d[j].time)<=Math.abs(cb[k].time-d[j].time)) k++;
            if(Math.abs(cb[k].time-d[j].time)<7*86400) joined.push({time:d[j].time, a:d[j].close, b:cb[k].close});
          }
          if(joined.length<2) continue;
          var t0=joined[0].a, c0=joined[0].b;
          addLine(joined.map(function(p){ return {time:p.time, value: (p.b/c0)*t0 }; }), COLORS[(ci+3)%COLORS.length], 2);
        }catch(e){}
      }
      if(vpOn) drawVP(d); else { var cv=document.getElementById("vp"); if(cv){ cv.width=cv.height=0; } }
      if(hiLo && d.length){ var visHi=-1e99, visLo=1e99, hiT=d[0].time, loT=d[0].time, i2; for(i2=0;i2<d.length;i2++){ if(d[i2].high>visHi){visHi=d[i2].high;hiT=d[i2].time;} if(d[i2].low<visLo){visLo=d[i2].low;loT=d[i2].time;} } addPriceLine(visHi, UP, "H "+fmt(visHi)); addPriceLine(visLo, DN, "L "+fmt(visLo)); }
      alerts.forEach(function(a){ if(!a.fired && a.sym===active) addPriceLine(a.price, "#ab47bc", "AL"); });
      var pos=paper.positions[active]; if(pos && pos.qty) addPriceLine(pos.avg, ACC, "AVG "+fmt(pos.avg));
      if(lastTest && lastTest.trades){ lastTest.trades.slice(-8).forEach(function(t){ addPriceLine(t.px, t.side==="buy"?UP:DN, t.side==="buy"?"B":"S"); }); }
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
    renderDetail();
    checkAlerts(active, d[d.length-1].close);
    countdown(d[d.length-1]);
    renderLegend();
    renderTech(d);
    paintMini(d);
    writeState();
    var st=document.getElementById("stat");
    var cd=document.getElementById("cd"); if(cd) cd.textContent="v12 QR"; if(st) st.textContent="v12 QR · "+d.length+" bars · Vol "+fmtVol(lastBars.length?lastBars[lastBars.length-1].volume:0)+" · "+tape.prints.length+" prints · "+lastSource;
  }
  function quoteUI(d){
    var last=d[d.length-1], prev=d[d.length-2]||last;
    var chg=prev.close?(last.close-prev.close)/prev.close:0, up=chg>=0, dlt=last.close-prev.close;
    var vwapPts=vwap(d), vw=vwapPts.length?vwapPts[vwapPts.length-1].value:null;
    var tw=twap(d), twv=tw.length?tw[tw.length-1].value:null;
    var vs=0, n=Math.min(20,d.length-1), i4;
    for(i4=d.length-1-n;i4<d.length-1;i4++) if(i4>=0) vs+=d[i4].volume;
    var rvol=n&&vs? last.volume/(vs/n):0;
    var deltaEst=last.high>last.low? ((last.close-last.low)/(last.high-last.low)*2-1)*last.volume : 0;
    var vsPx=vw? (last.close-vw)/vw : 0;
    var heat=rvol>=2?"HOT": rvol>=1.4?"elevated": rvol>=0.8?"normal":"thin";
    var stance=vw==null?"—": last.close>vw?"above VWAP": last.close<vw?"below VWAP":"at VWAP";
    var loc=lastVP.poc==null?"—": last.close>lastVP.vah?"above value": last.close<lastVP.val?"below value":"in value";
    var dltTape=tape.delta;
    document.getElementById("quote").innerHTML="<b class=tick id=qtick title='Search symbol'>▾ "+active+"</b> <span class=last>"+fmt(last.close)+"</span> <span class="+(up?"up":"dn")+">"+(up?"+":"")+fmt(dlt)+" ("+(chg*100).toFixed(2)+"%)</span> <span>"+tf+" · "+mode+"</span> <span>O "+fmt(last.open)+" H<span class=up> "+fmt(last.high)+"</span> L<span class=dn> "+fmt(last.low)+"</span> C<span class="+(up?"up":"dn")+"> "+fmt(last.close)+"</span></span> <span>Vol "+fmtVol(last.volume||0)+"</span> <span title='vs 20-bar average'>RVOL "+(rvol?rvol.toFixed(2)+"x":"—")+" "+heat+"</span> <span>VWAP "+(vw?fmt(vw):"—")+" <span class="+(vsPx>=0?"up":"dn")+">"+(vsPx>=0?"+":"")+(vsPx*100).toFixed(2)+"%</span></span> <span>Δ "+(deltaEst>=0?"+":"")+fmtVol(Math.abs(deltaEst))+"</span>"+(tape.prints.length?" <span title='print tape delta'>QR Δ <span class="+(dltTape>=0?"up":"dn")+">"+(dltTape>=0?"+":"")+fmtVol(Math.abs(dltTape))+"</span></span>":"")+" <span style=color:var(--acc)>"+stance+" · "+loc+"</span> <button type=button id=qfin>Financials</button> <button type=button id=qnote>Notes</button> <button type=button id=qqr>QR</button> <span style='margin-left:auto' class=sell>SELL "+fmt(last.close)+"</span> <span class=buy>BUY "+fmt(last.close*1.001)+"</span>";
    var qt=document.getElementById("qtick"); if(qt) qt.onclick=function(){ openSymSearch(active); };
    var qf=document.getElementById("qfin"); if(qf) qf.onclick=function(){ goSymbol(active,"fin"); };
    var qn=document.getElementById("qnote"); if(qn) qn.onclick=function(){ goSymbol(active,"notes"); };
    var qq=document.getElementById("qqr"); if(qq) qq.onclick=function(){ wsub="watch"; wtab="qr"; renderWtabs(); renderQR(); loadTape(true); };
    var dayHi=last.high, dayLo=last.low;
    var t52=last.time-365*86400, yhi=-1e99, ylo=1e99, i3;
    for(i3=0;i3<d.length;i3++){ if(d[i3].time<t52) continue; if(d[i3].high>yhi) yhi=d[i3].high; if(d[i3].low<ylo) ylo=d[i3].low; }
    if(!isFinite(yhi) || yhi<-1e90){ yhi=Math.max.apply(null,d.map(function(b){return b.high;})); ylo=Math.min.apply(null,d.map(function(b){return b.low;})); }
    var dp=Math.min(100,Math.max(0,(last.close-dayLo)/(dayHi-dayLo||1)*100));
    var yp=Math.min(100,Math.max(0,(last.close-ylo)/(yhi-ylo||1)*100));
    var atr14=atr(d,14), atrv=atr14.length?atr14[atr14.length-1].value:0;
    var vwapBias=vw&&twv? (vw>twv?"size at highs": vw<twv?"size at lows":"even"):"—";
    document.getElementById("detail").innerHTML="<div style=font-weight:600>"+active+"</div><div class=px>"+fmt(last.close)+"</div><div class="+(up?"up":"dn")+">"+(up?"+":"")+fmt(last.close-prev.close)+" "+(chg*100).toFixed(2)+"%</div><div class=cell><span>ATR 14</span><span>"+fmt(atrv)+(last.close? " · "+(100*atrv/last.close).toFixed(2)+"%":"")+"</span></div><div class=cell><span>RVOL 20</span><span>"+(rvol?rvol.toFixed(2)+"x "+heat:"—")+"</span></div><div class=cell><span>VWAP</span><span>"+(vw?fmt(vw)+" "+stance:"—")+"</span></div><div class=cell><span>VWAP vs TWAP</span><span>"+vwapBias+"</span></div><div class=cell><span>POC</span><span>"+(lastVP.poc!=null?fmt(lastVP.poc):"—")+"</span></div><div class=cell><span>Value</span><span>"+loc+"</span></div><div class=cell><span>Δ bar</span><span class="+(deltaEst>=0?"up":"dn")+">"+(deltaEst>=0?"+":"")+fmtVol(Math.abs(deltaEst))+"</span></div><div style='margin-top:8px;font-size:10px;color:var(--mut)'>DAY RANGE</div><div class=rg><i style=width:"+dp+"%></i><b style=left:"+dp+"%></b></div><div style=display:flex;justify-content:space-between;font-size:10px;font-family:IBM+Plex+Mono,monospace><span>"+fmt(dayLo)+"</span><span>"+fmt(dayHi)+"</span></div><div style='margin-top:8px;font-size:10px;color:var(--mut)'>52-WEEK RANGE</div><div class=rg><i style=width:"+yp+"%></i><b style=left:"+yp+"%></b></div><div style=display:flex;justify-content:space-between;font-size:10px;font-family:IBM+Plex+Mono,monospace><span>"+fmt(ylo)+"</span><span>"+fmt(yhi)+"</span></div>";
  }
  function paintOsc(d){
    var wrap=document.getElementById("oscwrap");
    var on=OSC.filter(function(o){ return o.on; }).slice(0,4);
    oscCharts.forEach(function(c){ try{ c.remove(); }catch(e){} });
    oscCharts=[]; oscSeries=[];
    if(!on.length){ wrap.className=""; wrap.innerHTML=""; return; }
    wrap.className="on"; wrap.innerHTML="";
    var p=pal();
    on.forEach(function(o, idx){
      var pane=document.createElement("div"); pane.className="osc"; pane.id="osc"+idx;
      var lab=document.createElement("div"); lab.className="olab"; lab.textContent=o.n;
      pane.appendChild(lab); wrap.appendChild(pane);
      var c=LW.createChart(pane,{ autoSize:true, layout:{background:{type:"solid",color:p.bg},textColor:p.text,fontSize:10}, grid:{vertLines:{color:p.grid},horzLines:{color:p.grid}}, timeScale:{ visible:idx===on.length-1 }, rightPriceScale:{ borderColor:p.border }, handleScroll:false, handleScale:false });
      bindSync(c); oscCharts.push(c);
      function addO(fn, color){ var s=c.addLineSeries({color:color||ACC,lineWidth:1}); s.setData(fn||[]); oscSeries.push(s); }
      function bands(lo,hi){ try{ var s=c.addLineSeries({color:"rgba(120,123,134,.45)",lineWidth:1,lastValueVisible:false,priceLineVisible:false}); s.setData((d||[]).map(function(b){return {time:b.time,value:hi};})); var s2=c.addLineSeries({color:"rgba(120,123,134,.45)",lineWidth:1,lastValueVisible:false,priceLineVisible:false}); s2.setData((d||[]).map(function(b){return {time:b.time,value:lo};})); oscSeries.push(s,s2); }catch(e){} }
      if(o.id==="rsi"){ addO(rsi(d,14), ACC); bands(30,70); }
      else if(o.id==="macd"){ var m=macd(d); var h=c.addHistogramSeries({}); h.setData(m.map(function(p){return {time:p.time,value:p.hist,color:p.hist>=0?UP:DN};})); var l1=c.addLineSeries({color:ACC,lineWidth:1}); l1.setData(m.map(function(p){return {time:p.time,value:p.macd};})); var l2=c.addLineSeries({color:"#ff6d00",lineWidth:1}); l2.setData(m.map(function(p){return {time:p.time,value:p.signal};})); oscSeries.push(h,l1,l2); }
      else if(o.id==="stoch"){ addO(stoch(d,14,3,3), "#ab47bc"); bands(20,80); }
      else if(o.id==="stochrsi") addO(stochRsi(d), "#7e57c2");
      else if(o.id==="atr") addO(atr(d,14), "#6a6d78");
      else if(o.id==="cci"){ addO(cci(d,20), "#26c6da"); bands(-100,100); }
      else if(o.id==="willr"){ addO(willr(d,14), "#ef6c00"); bands(-80,-20); }
      else if(o.id==="mfi"){ addO(mfi(d,14), "#00897b"); bands(20,80); }
      else if(o.id==="obv") addO(obv(d), "#5c6bc0");
      else if(o.id==="ad") addO(adline(d), "#6d4c41");
      else if(o.id==="cmf") addO(cmf(d,20), "#43a047");
      else if(o.id==="adx") addO(adx(d,14), "#3949ab");
      else if(o.id==="ao"){ var a=ao(d); var h2=c.addHistogramSeries({}); h2.setData(a.map(function(p){return {time:p.time,value:p.value,color:p.value>=0?UP:DN};})); oscSeries.push(h2); }
      else if(o.id==="mom") addO(mom(d,10), "#e91e63");
      else if(o.id==="roc") addO(roc(d,12), "#ff6d00");
      else if(o.id==="aroon") addO(aroon(d,25), "#089981");
      else if(o.id==="uo") addO(uo(d), "#2962ff");
      else if(o.id==="trix") addO(trix(d,15), "#7e57c2");
      else if(o.id==="chaikin") addO(chaikin(d), "#8d6e63");
      else if(o.id==="force") addO(force(d,13), "#c62828");
      else if(o.id==="ppo") addO(ppo(d), "#00695c");
      else if(o.id==="tsi") addO(tsi(d), "#4527a0");
      else if(o.id==="dpo") addO(dpo(d,20), "#37474f");
      else if(o.id==="vortex") addO(vortex(d,14), "#00897b");
      else if(o.id==="eld") addO(elder(d,13), "#ef6c00");
      else if(o.id==="kst") addO(kst(d), "#5c6bc0");
      else if(o.id==="fisher") addO(fisher(d,10), "#6a1b9a");
      else if(o.id==="rvi") addO(rvi(d,10), "#00838f");
      else if(o.id==="cmo"){ addO(cmo(d,14), "#ef6c00"); bands(-50,50); }
      else if(o.id==="mass") addO(massIndex(d,25), "#5d4037");
      else if(o.id==="copp") addO(coppock(d), "#1565c0");
      else if(o.id==="cvd"){ var cd=cvd(d); var h3=c.addHistogramSeries({}); h3.setData(cd.map(function(p,i){ var prev=i?cd[i-1].value:0; var step=p.value-prev; return {time:p.time,value:p.value,color:step>=0?UP:DN}; })); oscSeries.push(h3); }
      else if(o.id==="rvol"){ addO(rvolSeries(d,20), "#2962ff"); bands(1,2); }
      if(lastTest && lastTest.equity && lastTest.equity.length && o.id==="macd"){ /* equity lives in test tab */ }
    });
  }
  function drawVP(d){
    var cv=document.getElementById("vp"), box=document.getElementById("chart");
    if(!cv||!box) return;
    var w=96, h=box.clientHeight-8; cv.width=w; cv.height=h; cv.style.width=w+"px"; cv.style.height=h+"px";
    var ctx=cv.getContext("2d"); ctx.clearRect(0,0,w,h);
    var hi=-1e99, lo=1e99,i; for(i=0;i<d.length;i++){ if(d[i].high>hi)hi=d[i].high; if(d[i].low<lo)lo=d[i].low; }
    var bins=48, vol=new Array(bins).fill(0), up=new Array(bins).fill(0), dn=new Array(bins).fill(0), max=1, tot=0;
    if(tape.src==="binance" && tape.prints.length){
      for(i=0;i<tape.prints.length;i++){
        var p=tape.prints[i];
        if(p.px<lo || p.px>hi) continue;
        var idx=Math.min(bins-1, Math.max(0, Math.floor(((p.px-lo)/(hi-lo||1))*bins)));
        vol[idx]+=p.sz; tot+=p.sz;
        if(p.side==="buy") up[idx]+=p.sz; else dn[idx]+=p.sz;
        if(vol[idx]>max) max=vol[idx];
      }
    }
    if(tot<1e-12){
      tot=0; max=1;
      for(i=0;i<d.length;i++){
        var a=Math.min(bins-1, Math.max(0, Math.floor(((d[i].low-lo)/(hi-lo||1))*bins)));
        var b=Math.min(bins-1, Math.max(0, Math.floor(((d[i].high-lo)/(hi-lo||1))*bins)));
        if(b<a){ var tmp=a; a=b; b=tmp; }
        var share=d[i].volume/((b-a)+1), k;
        for(k=a;k<=b;k++){
          vol[k]+=share; tot+=share;
          if(d[i].close>=d[i].open) up[k]+=share; else dn[k]+=share;
          if(vol[k]>max) max=vol[k];
        }
      }
    }
    var poc=0; for(i=1;i<bins;i++) if(vol[i]>vol[poc]) poc=i;
    var va=vol[poc], loB=poc, hiB=poc, target=tot*0.7;
    while(va<target && (loB>0 || hiB<bins-1)){
      var nextLo=loB>0?vol[loB-1]:-1, nextHi=hiB<bins-1?vol[hiB+1]:-1;
      if(nextHi>=nextLo){ hiB++; va+=vol[hiB]; } else { loB--; va+=vol[loB]; }
    }
    for(i=0;i<bins;i++){
      var y=h-(i+1)*(h/bins), bh=h/bins-1, ww=(vol[i]/max)*w;
      var inVa=i>=loB && i<=hiB;
      ctx.fillStyle=up[i]>=dn[i]?(inVa?"rgba(8,153,129,.45)":"rgba(8,153,129,.22)"):(inVa?"rgba(242,54,69,.45)":"rgba(242,54,69,.22)");
      ctx.fillRect(0,y,ww,bh);
    }
    function yAt(bin){ return h-(bin+0.5)*(h/bins); }
    ctx.strokeStyle=ACC; ctx.lineWidth=1.2; ctx.beginPath(); ctx.moveTo(0,yAt(poc)); ctx.lineTo(w,yAt(poc)); ctx.stroke();
    ctx.strokeStyle="#787b86"; ctx.setLineDash([3,3]);
    ctx.beginPath(); ctx.moveTo(0,yAt(hiB)); ctx.lineTo(w,yAt(hiB)); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(0,yAt(loB)); ctx.lineTo(w,yAt(loB)); ctx.stroke();
    ctx.setLineDash([]); ctx.fillStyle=pal().fg; ctx.font="10px IBM Plex Mono";
    ctx.fillText("POC", 4, yAt(poc)-3);
    var pocPx=lo+(poc+0.5)/bins*(hi-lo), vah=lo+(hiB+0.5)/bins*(hi-lo), val=lo+(loB+0.5)/bins*(hi-lo);
    lastVP={poc:pocPx,vah:vah,val:val};
    addPriceLine(pocPx, ACC, "POC"); addPriceLine(vah, "#787b86", "VAH"); addPriceLine(val, "#787b86", "VAL");
  }
  async function loadTape(force){
    if(!force && tape.sym===active && tape.prints.length && Date.now()-(tape.at||0)<1500) { renderQR(); return; }
    var tkr=resolveSym(active).ticker;
    var pack=null;
    async function tryUrl(url){
      var r=await fetch(url,{cache:"no-store"});
      if(!r.ok) return null;
      return r.json();
    }
    var tradeUrls=["/api/trades?symbol="+encodeURIComponent(tkr)+"&limit=500",
      PROXY+"/aggTrades?symbol="+encodeURIComponent(tkr)+"&limit=500"];
    var ui;
    for(ui=0;ui<tradeUrls.length && !pack;ui++){
      try{
        var j=await tryUrl(tradeUrls[ui]);
        if(j && j.trades && j.trades.length) pack=j;
        else if(Array.isArray(j) && j.length) pack=null;
      }catch(e){}
    }
    if(!pack && /USDT$|BUSD$|USDC$/.test(tkr)){
      var hosts=["https://api.binance.com","https://data-api.binance.vision"];
      var hi;
      for(hi=0;hi<hosts.length && !pack;hi++){
        try{
          var raw=await tryUrl(hosts[hi]+"/api/v3/aggTrades?symbol="+encodeURIComponent(tkr)+"&limit=500");
          if(!Array.isArray(raw) || !raw.length) continue;
          var trades=raw.map(function(row){
            return {t:Number(row.T)||0, px:+row.p, sz:+row.q, side:row.m?"sell":"buy", id:String(row.a||row.T)};
          }).filter(function(x){ return x.t && isFinite(x.px) && x.sz>0; });
          var pv=0,vv=0,bv=0,sv=0,i;
          for(i=0;i<trades.length;i++){ pv+=trades[i].px*trades[i].sz; vv+=trades[i].sz; if(trades[i].side==="buy") bv+=trades[i].sz; else sv+=trades[i].sz; }
          var book=null;
          try{ book=await tryUrl(hosts[hi]+"/api/v3/ticker/bookTicker?symbol="+encodeURIComponent(tkr)); }catch(e2){}
          pack={source:"binance", trades:trades, vwap:vv?pv/vv:null, buyVol:bv, sellVol:sv, delta:bv-sv,
            bid:book?+book.bidPrice:null, ask:book?+book.askPrice:null, bidSz:book?+book.bidQty:null, askSz:book?+book.askQty:null, note:""};
        }catch(e3){}
      }
    }
    if(pack){
      tape.prints=pack.trades||[];
      tape.src=pack.source||"";
      tape.vwap=pack.vwap;
      tape.buyVol=pack.buyVol||0;
      tape.sellVol=pack.sellVol||0;
      tape.delta=pack.delta||0;
      tape.bid=pack.bid; tape.ask=pack.ask; tape.bidSz=pack.bidSz; tape.askSz=pack.askSz;
      tape.note=pack.note||"";
      tape.sym=active;
      tape.at=Date.now();
      renderQR();
      if(vpOn && tape.src==="binance" && lastBars.length) drawVP(lastBars);
    } else {
      renderQR();
    }
  }
  function renderQR(){
    fillTape(document.getElementById("qr"));
    /* tape strip above chart disabled */
  }
  function fillTape(el){
    if(!el) return;
    if((!tape.prints || !tape.prints.length) && window.lastBars && lastBars.length){
      var lb=lastBars.slice(-40), i, html="<div class=qrbar><b>QR</b> "+active+" <span>daily warehouse — not SIP ticks</span></div><div class=qrbody>";
      for(i=lb.length-1;i>=0;i--){
        var b=lb[i], up=b.close>=b.open, d=new Date(b.time*1000);
        html+="<div style=display:flex;gap:8px;font-variant-numeric:tabular-nums><span>"+d.toISOString().slice(0,10)+"</span><span style=color:"+(up?"#089981":"#f23645")+">"+b.close.toFixed(2)+"</span><span>"+Math.round(b.volume||0).toLocaleString()+"</span></div>";
      }
      el.innerHTML=html+"</div>"; return;
    }
    var rows=tape.prints.slice().reverse();
    var med=0;
    if(rows.length){
      var sz=rows.map(function(x){return x.sz;}).sort(function(a,b){return a-b;});
      med=sz[Math.floor(sz.length/2)]||0;
    }
    var filt=tape.filt||"all";
    rows=rows.filter(function(x){
      if(filt==="buy") return x.side==="buy";
      if(filt==="sell") return x.side==="sell";
      if(filt==="lg") return med && x.sz>=med*3;
      return true;
    });
    function tms(ms){ var d=new Date(ms+(tzOff||0)*3600*1000); return d.toISOString().slice(11,23); }
    var srcLab=tape.src==="binance"?"Binance prints": tape.src==="yahoo-1m"?"1m recap": (tape.src||"loading");
    if(!tape.prints.length){
      el.innerHTML="<div class=qrbar><b>QR TIME & SALES</b> "+active+" <span>no tick tape</span></div><div class=qrbody style=padding:10px;color:var(--mut)>No public prints for this symbol (warehouse is daily bars; tick tape is crypto/Binance only)</div>";
      return;
    }
    el.innerHTML="<div class=qrbar><b>QR TIME & SALES</b> "+active+" <span>"+tape.prints.length+" prints</span> <span>VWAP "+(tape.vwap!=null?fmt(tape.vwap):"—")+"</span> <span class=up>B "+fmtVol(tape.buyVol)+"</span> <span class=dn>S "+fmtVol(tape.sellVol)+"</span> <span>Δ <span class="+(tape.delta>=0?"up":"dn")+">"+(tape.delta>=0?"+":"")+fmtVol(Math.abs(tape.delta))+"</span></span>"+
      ["all","buy","sell","lg"].map(function(f){ return "<button class='qrf "+(filt===f?"on":"")+"' data-f='"+f+"'>"+(f==="lg"?"LARGE":f.toUpperCase())+"</button>"; }).join("")+
      "</div>"+
      "<div class=qrcols><span>TIME</span><span>PX</span><span>SIZE</span><span>SIDE</span></div>"+
      "<div class=qrbody>"+rows.slice(0,400).map(function(x){
        var lg=med && x.sz>=med*3;
        return "<div class='qrrow "+x.side+(lg?" lg":"")+"'><span>"+tms(x.t)+"</span><span>"+fmt(x.px)+"</span><span>"+fmtVol(x.sz)+"</span><span>"+(x.side==="buy"?"B":"S")+"</span></div>";
      }).join("")+"</div>"+
      "<div class=qrbook><div class=dn>BID "+(tape.bid!=null?fmt(tape.bid):"—")+" × "+(tape.bidSz!=null?fmtVol(tape.bidSz):"—")+"</div><div class=up>ASK "+(tape.ask!=null?fmt(tape.ask):"—")+" × "+(tape.askSz!=null?fmtVol(tape.askSz):"—")+"</div><div class=src>"+srcLab+(tape.note?" · "+tape.note:"")+"</div></div>";
    el.querySelectorAll("[data-f]").forEach(function(b){ b.onclick=function(){ tape.filt=b.dataset.f; renderQR(); }; });
  }
  async function load(){
    try{
      var d=await klines(active,tf);
      if(!d.length){ lastSource="unavailable"; }
      if(replay.on) d=d.slice(0, replay.i||d.length);
      await paint(d);
      if(layout>1) paintPanes();
      loadTape(true);
    }catch(e){
      try{
        var d2=[]; lastSource="unavailable";
        await paint(d2);
        toast("Vendor miss · showing synth tape");
      }catch(e2){
        var qel=document.getElementById("quote");
        if(qel) qel.textContent="Chart error: "+(e&&e.message||e);
      }
    }
  }

  function paneEls(){ return [ ["chart2","host2",1], ["chart3","host3",2], ["chart4","host4",3] ]; }
  function setLayout(n){
    layout=n;
    var stage=document.getElementById("stage");
    stage.className = n===4?"split4": n===2?"split2":"";
    paneEls().forEach(function(p,i){ document.getElementById(p[0]).style.display = (n===2&&i===0)||n===4 ? "block":"none"; });
    if(n>1) paintPanes(); saveLay();
  }
  function zoomChart(dir){
    try{
      var r=chart.timeScale().getVisibleLogicalRange();
      if(!r) return;
      var mid=(r.from+r.to)/2, span=(r.to-r.from)*(dir>0?0.8:1.25);
      chart.timeScale().setVisibleLogicalRange({from:mid-span/2, to:mid+span/2});
    }catch(e){}
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
        bindSync(refs[i]);
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
          var nbar=0, vi; for(vi=0;vi<lastBars.length;vi++) if(lastBars[vi].time>=Math.min(d.points[0].time,d.points[1].time) && lastBars[vi].time<=Math.max(d.points[0].time,d.points[1].time)) nbar++;
          var lab=pct.toFixed(2)+"% · "+fmt(d.points[1].price-d.points[0].price)+" · "+Math.round(dt/86400)+"d · "+nbar+" bars";
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
        var fills=["rgba(41,98,255,.06)","rgba(8,153,129,.08)","rgba(242,54,69,.06)","rgba(255,109,0,.07)","rgba(171,71,188,.07)","rgba(38,198,218,.06)","rgba(41,98,255,.05)"];
        for(var fi=0;fi<lv.length;fi++){
          var yf=pts[0].y+(pts[1].y-pts[0].y)*lv[fi];
          if(fi){ var yprev=pts[0].y+(pts[1].y-pts[0].y)*lv[fi-1]; var x0=k==="fibch"?Math.min(pts[0].x,pts[1].x):0, xw=k==="fibch"?Math.abs(pts[1].x-pts[0].x):w; parts.push('<rect x="'+x0+'" y="'+Math.min(yf,yprev)+'" width="'+xw+'" height="'+Math.abs(yf-yprev)+'" fill="'+(fills[(fi-1)%fills.length])+'" />'); }
          parts.push(ln(k==="fibch"?pts[0].x:0, yf, k==="fibch"?pts[1].x:w, yf, c, 1, "4 3")); parts.push(tx(8,yf-3,c,(lv[fi]*100).toFixed(1)));
        }
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
      else if((k==="gartley"||k==="abcd") && pts.length>=3){
        var pth2=pts.map(function(p,ii){ return (ii?"L":"M")+p.x+" "+p.y; }).join(" ");
        parts.push('<path d="'+pth2+'" fill="'+c+'14" stroke="'+c+'" stroke-width="'+sw+'"/>');
        var lb3=k==="gartley"?["X","A","B","C","D"]:["A","B","C","D"];
        pts.forEach(function(p,ii){ parts.push(tx(p.x+4,p.y-4,c,lb3[ii]||"")); });
      }
      else if(k==="avwap" && pts[0] && lastBars.length){
        var from=d.points[0].time, sl=lastBars.filter(function(b){return b.time>=from;}), av=vwap(sl);
        var path=av.map(function(p,ii){ var q=xy(p.time,p.value); return q?((ii?"L":"M")+q.x+" "+q.y):""; }).join(" ");
        parts.push('<path d="'+path+'" fill="none" stroke="'+c+'" stroke-width="'+sw+'"/>');
        parts.push(tx(pts[0].x+4,pts[0].y-6,c,"AVWAP"));
      }
      else if(k==="cyclic" && pts.length>=2){
        var step=Math.abs(pts[1].x-pts[0].x)||40, xi;
        for(xi=pts[0].x; xi<w; xi+=step) parts.push(ln(xi,0,xi,h,c,1,"3 5"));
      }
      else if((k==="path"||k==="curve") && pts.length){
        var pth3=pts.map(function(p,ii){ return (ii?"L":"M")+p.x+" "+p.y; }).join(" ");
        parts.push('<path d="'+pth3+'" fill="none" stroke="'+c+'" stroke-width="'+sw+'" '+(k==="curve"?'stroke-linejoin="round" stroke-linecap="round"':"")+' />');
      }
      else if((k==="buy"||k==="sell") && pts[0]){
        var fill=k==="buy"?UP:DN;
        parts.push('<polygon points="'+pts[0].x+','+pts[0].y+' '+(pts[0].x-7)+','+(pts[0].y+(k==="buy"?12:-12))+' '+(pts[0].x+7)+','+(pts[0].y+(k==="buy"?12:-12))+'" fill="'+fill+'"/>');
        parts.push(tx(pts[0].x+8,pts[0].y,fill,k.toUpperCase()));
      }
      else if(k==="fibwedge" && pts.length>=2){
        [0.382,0.5,0.618,1,1.618].forEach(function(lv){ var e=ext(pts[0],{x:pts[1].x,y:pts[0].y+(pts[1].y-pts[0].y)*lv}, w); parts.push(ln(pts[0].x,pts[0].y,e.x,e.y,c,1,"4 3")); });
      }
      if(selDraw===d.id){
        pts.forEach(function(p){ parts.push('<circle cx="'+p.x+'" cy="'+p.y+'" r="5" fill="#fff" stroke="'+c+'" stroke-width="2"/>'); });
      }
    }
    if(ghostPt && pending && pending.points.length){
      var g0=xy(pending.points[0].time, pending.points[0].price), g1=xy(ghostPt.time, ghostPt.price);
      if(g0&&g1){ parts.push(ln(g0.x,g0.y,g1.x,g1.y,ACC,1,"5 4")); var gpct=pending.points[0].price?((ghostPt.price-pending.points[0].price)/pending.points[0].price)*100:0; parts.push(tx((g0.x+g1.x)/2,(g0.y+g1.y)/2-8,ACC,gpct.toFixed(2)+"% · "+fmt(ghostPt.price))); }
    }
    var patInd=INDS.find(function(x){return x.id==="pat";});
    if(patInd&&patInd.on&&lastBars.length){
      candlePatterns(lastBars).forEach(function(p){ var q=xy(p.time,p.price); if(q) parts.push(tx(q.x-10,q.y-8,"#546e7a",p.n)); });
    }
    if(newsMarks && news && news.length){
      news.slice(0,12).forEach(function(n){
        var ts=n.date?Math.floor(Date.parse(n.date)/1000):0; if(!ts) return;
        var q=xy(ts, lastBars.length?lastBars[lastBars.length-1].close:0); if(!q) return;
        parts.push(ln(q.x,0,q.x,h,"#ab47bc",1,"2 4"));
      });
    }
    svg.innerHTML=parts.join("");
    renderObj();
  }

  function snapPt(t,p){
    if(!magnetMode || !lastBars.length) return {time:Number(t),price:p};
    var best=lastBars[0], bd=1e99,i;
    for(i=0;i<lastBars.length;i++){ var dd=Math.abs(lastBars[i].time-t); if(dd<bd){ bd=dd; best=lastBars[i]; } }
    if(magnetMode===1) return {time:best.time,price:best.close};
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
      if(ev.shiftKey){
        var t0=chart.timeScale().coordinateToTime(x); var p0=mainSeries && mainSeries.coordinateToPrice(y);
        if(t0!=null && p0!=null){
          if(!pending){ pending={id:uid(),kind:"measure",points:[snapPt(t0,Number(p0))],color:drawColor,w:drawW}; drawings.push(pending); undo.push({op:"add",id:pending.id,item:pending}); }
          else { pending.points.push(snapPt(t0,Number(p0))); finishTool(); saveDraw(); drawSVG(); }
        }
        return;
      }
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
      drawings.push(pending); undo.push({op:"add",id:id,item:pending}); redo=[];
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
    if(u.op==="add" && u.item){ if(!drawings.some(function(d){return d.id===u.id;})) drawings.push(u.item); }
    else if(u.op==="del" && u.item) drawings=drawings.filter(function(d){ return d.id!==u.item.id; });
    saveDraw(); drawSVG();
  }
  function clearDraw(){ if(lockDraw){ toast("Drawings locked"); return; } undo=undo.concat(drawings.map(function(d){return {op:"del",item:d};})); drawings=[]; pending=null; selDraw=null; saveDraw(); drawSVG(); }

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
    document.getElementById("tabs").innerHTML="<a class='tab brand' href='/'>JustHodl</a><input id=symin placeholder='Search  /' value='' autocomplete=off readonly>"+TABS.map(function(s){ var q=quotes[s], up=q&&q.chg>=0; return "<button class='tab "+(s===active?"on":"")+"' data-id='"+s+"'>"+s.replace("USDT","")+(q?" <span class="+(up?"up":"dn")+">"+fmt(q.last)+" "+(up?"+":"")+(q.chg*100).toFixed(2)+"%</span>":"")+" <span data-x='"+s+"'>×</span></button>"; }).join("")+"<button class=tab id=add>+</button><a class='tab pro' href='/chart-pro.html'>Pro</a>";
    document.querySelectorAll(".tab[data-id]").forEach(function(b){ b.onclick=function(e){ if(e.target.dataset.x){ TABS=TABS.filter(function(s){return s!==e.target.dataset.x;}); if(active===e.target.dataset.x) active=TABS[0]||active; renderTabs(); loadDraw(); load(); return; } active=b.dataset.id; loadDraw(); renderTabs(); load(); }; });
    var add=document.getElementById("add"); if(add) add.onclick=function(){ openSymSearch(""); };
    var si=document.getElementById("symin");
    if(si){ si.onclick=function(){ openSymSearch(""); }; }
  }
  function openMenu(btn, html){
    var m=document.getElementById("menu"), r=btn.getBoundingClientRect();
    m.className="menu on"; m.innerHTML=html;
    m.style.left=Math.min(r.left, window.innerWidth-200)+"px"; m.style.top=(r.bottom+4)+"px";
  }
  function closeMenu(){ var m=document.getElementById("menu"); if(m) m.className="menu"; }
  function renderTf(){
    var kindLab=(KINDS.filter(function(k){return k[0]===kind;})[0]||KINDS[0])[1];
    var scLab=(SCALES.filter(function(s){return +s[0]===scaleMode;})[0]||SCALES[0])[1];
    var mdLab=(CHG.filter(function(t){return t[0]===mode;})[0]||CHG[0])[1];
    document.getElementById("tfbar").innerHTML=
      TFS.map(function(t){ return "<button class='"+(t[0]===tf?"on":"")+"' data-tf='"+t[0]+"'>"+t[1]+"</button>"; }).join("")+
      "<span class=sep></span>"+
      "<button class=drop id=btn-kind>"+kindLab+" ▾</button>"+
      "<button class=drop id=btn-sc>"+scLab+" ▾</button>"+
      "<button class=drop id=btn-md>"+mdLab+" ▾</button>"+
      "<span class=sep></span>"+
      "<button id=btn-ind title='Indicators Ctrl+I'>fx</button>"+
      "<button id=btn-cmp title=Compare>Cmp</button>"+
      "<button id=btn-rep title=Replay>Rep</button>"+
      "<button id=btn-al title=Alert>Alrt</button>"+
      "<button id=btn-shot title='Snapshot Ctrl+S'>Cam</button>"+
      "<button id=btn-zm title=Zoom>−</button><button id=btn-zp>+</button>"+
      "<button id=btn-fs title=Fullscreen>⛶</button>"+
      "<button id=btn-lay1 class='"+(layout===1?"on":"")+"'>1</button>"+
      "<button id=btn-lay2 class='"+(layout===2?"on":"")+"'>2</button>"+
      "<button id=btn-lay4 class='"+(layout===4?"on":"")+"'>4</button>"+
      "<button id=btn-vol class='"+(volOn?"on":"")+"'>Vol</button>"+
      "<button id=btn-watch title=Watchlist>List</button>"+
      "<button id=btn-co class='"+(chartOnly?"on":"")+"'>Only</button>"+
      "<button id=btn-theme title=Theme>"+(dark?"Day":"Night")+"</button>"+
      "<button id=btn-live class='"+(liveOn?"on":"")+"'>Live</button>"+
      "<button id=btn-dwin class='"+(dwinOn?"on":"")+"'>Data</button>"+
      "<button id=btn-mini class='"+(miniOn?"on":"")+"'>Nav</button>"+
      "<button id=btn-left class='"+(leftOn?"on":"")+"'>L</button>"+
      "<button id=btn-set>⚙</button>"+
      "<button id=btn-cmd>⌘K</button>"+
      "<input id=goto type=date title='Go to date'>";
    document.querySelectorAll("#tfbar [data-tf]").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });
    document.getElementById("btn-kind").onclick=function(){
      var self=this;
      openMenu(self, "<div class=lab>Chart type</div>"+KINDS.map(function(k){ return "<button class='"+(k[0]===kind?"on":"")+"' data-k='"+k[0]+"'>"+k[1]+"</button>"; }).join(""));
      document.querySelectorAll("#menu [data-k]").forEach(function(b){ b.onclick=function(){ kind=b.dataset.k; closeMenu(); renderTf(); if(lastBars.length) paint(lastBars); }; });
    };
    document.getElementById("btn-sc").onclick=function(){
      openMenu(this, "<div class=lab>Scale</div>"+SCALES.map(function(s){ return "<button class='"+(+s[0]===scaleMode?"on":"")+"' data-sc='"+s[0]+"'>"+s[1]+"</button>"; }).join(""));
      document.querySelectorAll("#menu [data-sc]").forEach(function(b){ b.onclick=function(){ scaleMode=+b.dataset.sc; closeMenu(); renderTf(); if(lastBars.length) paint(lastBars); }; });
    };
    document.getElementById("btn-md").onclick=function(){
      openMenu(this, "<div class=lab>Change</div>"+CHG.map(function(t){ return "<button class='"+(t[0]===mode?"on":"")+"' data-m='"+t[0]+"'>"+t[1]+"</button>"; }).join(""));
      document.querySelectorAll("#menu [data-m]").forEach(function(b){ b.onclick=function(){ mode=b.dataset.m; closeMenu(); renderTf(); if(lastBars.length) paint(lastBars); }; });
    };
    document.getElementById("btn-ind").onclick=openInd;
    document.getElementById("btn-cmp").onclick=openCmp;
    document.getElementById("btn-rep").onclick=startReplay;
    document.getElementById("btn-al").onclick=function(){ var px=lastBars.length?lastBars[lastBars.length-1].close:0; if(px) addAlert(active,px); };
    document.getElementById("btn-shot").onclick=shot;
    document.getElementById("btn-zm").onclick=function(){ zoomChart(-1); };
    document.getElementById("btn-zp").onclick=function(){ zoomChart(1); };
    document.getElementById("btn-fs").onclick=function(){ var el=document.getElementById("app"); if(!document.fullscreenElement) el.requestFullscreen(); else document.exitFullscreen(); };
    document.getElementById("btn-lay1").onclick=function(){ setLayout(1); renderTf(); };
    document.getElementById("btn-lay2").onclick=function(){ setLayout(2); renderTf(); };
    document.getElementById("btn-lay4").onclick=function(){ setLayout(4); renderTf(); };
    document.getElementById("btn-vol").onclick=function(){ volOn=!volOn; renderTf(); if(lastBars.length) paint(lastBars); };
    document.getElementById("btn-watch").onclick=function(){ watchOpen=!watchOpen; document.getElementById("watch").className="watch"+(watchOpen?"":" hide"); renderTf(); };
    document.getElementById("btn-co").onclick=function(){
      chartOnly=!chartOnly;
      document.getElementById("app").classList.toggle("chart-only", chartOnly);
      watchOpen=!chartOnly;
      document.getElementById("watch").className="watch"+(watchOpen?"":" hide");
      renderTf();
    };
    document.getElementById("btn-theme").onclick=function(){ dark=!dark; applyTheme(true); renderTf(); };
    document.getElementById("btn-live").onclick=function(){ liveOn=!liveOn; var el=document.getElementById("livepill"); if(el) el.className=liveOn?"on":""; renderTf(); toast(liveOn?"Live tape on":"Live tape off"); };
    document.getElementById("btn-dwin").onclick=function(){ dwinOn=!dwinOn; var el=document.getElementById("dwin"); if(el) el.className=dwinOn?"on":""; renderTf(); };
    document.getElementById("btn-mini").onclick=function(){ miniOn=!miniOn; var el=document.getElementById("mini"); if(el) el.className=miniOn?"on":""; if(miniOn && lastBars.length) paintMini(lastBars); renderTf(); };
    document.getElementById("btn-left").onclick=function(){ leftOn=!leftOn; applyTheme(true); renderTf(); };
    document.getElementById("btn-set").onclick=openSet;
    document.getElementById("btn-cmd").onclick=function(){ openCmd(""); };
    document.getElementById("goto").onchange=function(){ var t=Math.floor(Date.parse(this.value+"T00:00:00Z")/1000); if(!t||!lastBars.length) return; chart.timeScale().setVisibleRange({ from:t-30*86400, to:t+5*86400 }); };
    renderFavs();
  }
  function renderFavs(){
    var el=document.getElementById("favs"); if(!el) return;
    if(!favs.length){ el.className=""; el.innerHTML=""; return; }
    el.className="on";
    el.innerHTML="<span style=color:var(--mut)>★</span>"+favs.map(function(s){ return "<button data-f='"+s+"'>"+s+"</button>"; }).join("");
    el.querySelectorAll("[data-f]").forEach(function(b){ b.onclick=function(){ var s=b.dataset.f; if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); loadDraw(); renderTabs(); load(); }; });
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
      "<button class='gbtn "+(magnetMode?"on":"")+"' id=mag title='Magnet (cycle)'>"+(magnetMode===2?"M+":magnetMode===1?"M":"M")+"</button>"+
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
      b.onmouseenter=function(){ var g=GROUPS.filter(function(x){return x.id===b.dataset.g;})[0]; clearTimeout(openFly._t); openFly._t=setTimeout(function(){ openFly(b,g); }, 140); };
      b.onmouseleave=function(){ clearTimeout(openFly._t); };
    });
    var fly=document.getElementById("fly");
    if(fly && !fly.dataset.bound){ fly.addEventListener("mouseleave", closeFly); fly.dataset.bound="1"; }
    document.getElementById("mag").onclick=function(){ magnetMode=(magnetMode+1)%3; magnet=magnetMode>0; renderRail(); toast(magnetMode===0?"Magnet off":magnetMode===1?"Magnet: close":"Magnet: OHLC"); };
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
  function renderLegend(atTime){
    var t=atTime, last=lastBars.length?lastBars[lastBars.length-1]:null;
    if(t==null && last) t=last.time;
    function v(id){ var n=valAt(overlayMap[id], t); return n==null?"": " "+fmt(n); }
    document.getElementById("legend").innerHTML="<div style='color:var(--fg);font-weight:500;margin-bottom:4px'>"+active+" · "+tf+(compare.length?" + "+compare.join(" "):"")+"</div>"+INDS.map(function(i){ return "<button style=color:"+i.c+";opacity:"+(i.on?1:.35)+" data-i='"+i.id+"'>"+(i.on?"◉ ":"○ ")+i.n+v(i.id)+"</button>"; }).join("")+OSC.filter(function(o){return o.on;}).map(function(o){ return "<button style=color:"+ACC+" data-o='"+o.id+"'>"+o.n+"</button>"; }).join("");
    document.querySelectorAll("#legend [data-i]").forEach(function(b){ b.onclick=function(e){ var i=INDS.find(function(x){return x.id===b.dataset.i;}); if(e.shiftKey && i.p){ var n=+prompt("Period", i.p); if(n>1){ i.p=n; i.n=i.n.replace(/\d+/, String(n)); } } else i.on=!i.on; if(lastBars.length) paint(lastBars); }; });
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
    var sub=document.getElementById("wsub");
    if(sub){
      sub.innerHTML=["watch","data","info"].map(function(t){ return "<button class='"+(wsub===t?"on":"")+"' data-sub='"+t+"'>"+(t==="watch"?"WATCHLIST":t==="data"?"DATA":"INFO")+"</button>"; }).join("");
      sub.querySelectorAll("[data-sub]").forEach(function(b){ b.onclick=function(){ wsub=b.dataset.sub; if(wsub==="data") wtab="fin"; else if(wsub==="info") wtab="notes"; else if(wtab==="fin"||wtab==="notes") wtab="list"; renderWtabs(); if(wtab==="fin") renderFin(); if(wtab==="notes") renderNotes(); if(wtab==="list") renderList(); renderDetail(); }; });
    }
    var tabs=wsub==="watch"?["list","qr","heat","screen","trade","test","over","season","corr"]: wsub==="data"?["fin","over","tech"]: ["notes","news","alerts","cal"];
    document.getElementById("wtabs").innerHTML=tabs.map(function(t){ return "<button class='"+(wtab===t?"on":"")+"' data-w='"+t+"'>"+t+"</button>"; }).join("");
    var ids={list:"w-list",heat:"heat",news:"news",alerts:"alerts",tech:"tech",cal:"cal",notes:"notes",fin:"fin",screen:"screen",trade:"trade",test:"test",over:"over",season:"season",corr:"corr",qr:"qr"};
    Object.keys(ids).forEach(function(k){ var el=document.getElementById(ids[k]); if(el) el.style.display = (k==="list"? (wtab==="list"?"":"none") : (wtab===k?"":"none")); });
    var intel=document.getElementById("intel"); if(intel) intel.style.display=wtab==="list"?"":"none";
    var det=document.getElementById("detail"); if(det) det.style.display=(wtab==="list"||wsub==="data")?"":"none";
    var qrel=document.getElementById("qr"); if(qrel) qrel.className=wtab==="qr"?"on":"";
    document.querySelectorAll("#wtabs [data-w]").forEach(function(b){ b.onclick=function(){ wtab=b.dataset.w; renderWtabs(); if(wtab==="news") renderNews(); if(wtab==="alerts") renderAlerts(); if(wtab==="tech"&&lastBars.length) renderTech(lastBars); if(wtab==="cal") renderCal(); if(wtab==="notes") renderNotes(); if(wtab==="fin") renderFin(); if(wtab==="heat") renderHeat(); if(wtab==="screen") renderScreen(); if(wtab==="trade") renderTrade(); if(wtab==="test") renderTest(); if(wtab==="over"&&lastBars.length) renderOver(lastBars); if(wtab==="season"&&lastBars.length) renderSeason(lastBars); if(wtab==="corr") renderCorr(); if(wtab==="qr"){ renderQR(); loadTape(true); } }; });
    bindWatchOps();
  }
  function bindWatchOps(){
    var n=document.getElementById("w-new");
    if(n && !n.dataset.bound){ n.onclick=function(){ var name=prompt("New watchlist name","My list"); if(!name) return; var custom=loadJSON(CUSTOM_KEY,[]); var hit={id:"custom-"+Date.now(),name:name,symbols:[active],n:1,custom:1}; custom.unshift(hit); saveJSON(CUSTOM_KEY,custom); lists=[hit].concat(lists.filter(function(l){return l.id!==hit.id;})); listId=hit.id; renderList(); toast("Created "+name); }; n.dataset.bound="1"; }
    var m=document.getElementById("w-menu");
    if(m && !m.dataset.bound){ m.onclick=function(){ openMenu(m, "<div class=lab>LIST</div><button data-a=ren>Rename</button><button data-a=dup>Duplicate</button><button data-a=add>Add "+active+"</button><button data-a=del>Delete list</button>"); document.querySelectorAll("#menu [data-a]").forEach(function(b){ b.onclick=function(){ closeMenu(); listMenu(b.dataset.a); }; }); }; m.dataset.bound="1"; }
    var a=document.getElementById("addsym");
    if(a && !a.dataset.bound){ a.onclick=function(){ openSymSearch("", "add"); }; a.dataset.bound="1"; }
  }
  function listMenu(a){
    var L=lists.find(function(x){return x.id===listId;});
    if(!L) return;
    if(a==="ren"){ var n=prompt("Rename list", L.name); if(!n) return; L.name=n; persistCustom(L); renderList(); }
    if(a==="dup"){ var hit={id:"custom-"+Date.now(),name:L.name+" copy",symbols:(L.symbols||[]).slice(),n:(L.symbols||[]).length,custom:1}; persistCustom(hit); lists=[hit].concat(lists); listId=hit.id; renderList(); }
    if(a==="add"){ addToList(active); }
    if(a==="del"){ if(!L.custom && L.id!=="favorites"){ toast("Warehouse lists stay"); return; } if(!confirm("Delete "+L.name+"?")) return; var custom=loadJSON(CUSTOM_KEY,[]).filter(function(x){return x.id!==L.id;}); saveJSON(CUSTOM_KEY,custom); if(L.id==="favorites"){ favs=[]; saveJSON(FAV_KEY,favs); } lists=lists.filter(function(x){return x.id!==L.id;}); listId=(lists[0]||{}).id; renderList(); }
  }
  function persistCustom(L){
    L.custom=1; L.n=(L.symbols||[]).length;
    var custom=loadJSON(CUSTOM_KEY,[]); if(!Array.isArray(custom)) custom=[];
    var i=custom.findIndex(function(x){return x.id===L.id;});
    if(i>=0) custom[i]=L; else custom.unshift(L);
    saveJSON(CUSTOM_KEY,custom);
  }
  function addToList(s){
    s=bare(s);
    var L=lists.find(function(x){return x.id===listId;});
    if(!L) return;
    if((L.symbols||[]).indexOf(s)>=0){ toast(s+" already on list"); return; }
    L.symbols=L.symbols||[]; L.symbols.unshift(s); persistCustom(L); renderList(); toast("Added "+s+" to "+L.name);
  }
  function noteObj(s){
    var n=notes[bare(s)];
    if(n && typeof n==="object") return n;
    if(typeof n==="string" && n) return {text:n, at:""};
    return {text:"", at:""};
  }
  function classifySym(s){
    s=String(s||"");
    if(/USDT|BTC|ETH|PEPE|SOL|DOGE/i.test(s)) return "crypto";
    if(/^FRED:|^T10|^DGS|^SOFR|^EFFR|^CPI|^GDP/i.test(s)) return "macro";
    if(/USD$|EUR|JPY|GBP|FX:/i.test(s)) return "fx";
    if(/^(GSG|COMT|EWZS|CMDY|IVV|IWM|EEM|LQD|HYG|TLT|IEI|SPY|QQQ|GLD|SLV|XLF|XLK|XLE)$/i.test(bare(s))) return "etf";
    return "stock";
  }
  function goSymbol(s, dest){
    s=bare(s);
    if(!s) return;
    if(TABS.indexOf(s)<0) TABS.push(s);
    active=s; loadDraw(); renderTabs();
    watchOpen=true; document.getElementById("watch").className="watch";
    if(dest==="fin"){ wsub="data"; wtab="fin"; renderWtabs(); renderFin(); }
    else if(dest==="notes"){ wsub="info"; wtab="notes"; renderWtabs(); renderNotes(); }
    else if(dest==="add"){ addToList(s); load(); }
    else { wsub="watch"; wtab="list"; renderWtabs(); renderList(); load(); }
    if(dest!=="add") load();
    closeSymSearch();
  }
  function closeSymSearch(){ var el=document.getElementById("symsearch"); if(el) el.className=""; }
  function openSymSearch(pre, dest){
    var wrap=document.getElementById("symsearch"); if(!wrap) return openCmd(pre||"");
    wrap.className="on";
    var inp=document.getElementById("ssin");
    inp.value=pre&&pre!==active?pre:"";
    inp.focus();
    ssTab="all"; ssSel=0; wrap.dataset.dest=dest||"chart";
    renderSsChips();
    renderSymSearch(inp.value);
    if(!inp.dataset.bound){
      inp.oninput=function(){ ssSel=0; renderSymSearch(inp.value); };
      inp.onkeydown=function(e){
        if(e.key==="Escape"){ closeSymSearch(); return; }
        if(e.key==="ArrowDown"){ e.preventDefault(); ssSel=Math.min(ssRows.length-1, ssSel+1); paintSs(); return; }
        if(e.key==="ArrowUp"){ e.preventDefault(); ssSel=Math.max(0, ssSel-1); paintSs(); return; }
        if(e.key==="Enter"){ e.preventDefault(); var row=ssRows[ssSel]; if(row) goSymbol(row.s, wrap.dataset.dest||"chart"); else if(inp.value.trim()) goSymbol(inp.value.trim().toUpperCase(), wrap.dataset.dest||"chart"); }
      };
      inp.dataset.bound="1";
    }
    if(pre && pre.length>1) yahooSearch(pre);
  }
  function renderSsChips(){
    var chips=["all","stocks","etfs","crypto","fx","macro","lists","notes","fin"];
    document.getElementById("sschips").innerHTML=chips.map(function(c){ return "<button class='"+(ssTab===c?"on":"")+"' data-c='"+c+"'>"+c+"</button>"; }).join("");
    document.querySelectorAll("#sschips [data-c]").forEach(function(b){ b.onclick=function(){ ssTab=b.dataset.c; renderSsChips(); renderSymSearch(document.getElementById("ssin").value); }; });
  }
  function paintSs(){
    document.querySelectorAll("#ssres .ss-hit").forEach(function(el,i){ el.className="ss-hit"+(i===ssSel?" on":""); });
  }
  function renderSymSearch(q){
    q=(q||"").trim();
    var ql=q.toLowerCase();
    var dest=document.getElementById("symsearch").dataset.dest||"chart";
    var rows=[], seen={};
    function push(s, name, extra, type){
      s=String(s); var k=bare(s);
      if(!k||seen[k]) return; seen[k]=1;
      var cls=classifySym(s);
      if(ssTab==="stocks"&&cls!=="stock") return;
      if(ssTab==="etfs"&&cls!=="etf") return;
      if(ssTab==="crypto"&&cls!=="crypto") return;
      if(ssTab==="fx"&&cls!=="fx") return;
      if(ssTab==="macro"&&cls!=="macro") return;
      if(ssTab==="notes" && !noteObj(s).text) return;
      if(ql && String(s).toLowerCase().indexOf(ql)<0 && String(name||"").toLowerCase().indexOf(ql)<0 && String(extra||"").toLowerCase().indexOf(ql)<0) return;
      rows.push({s:k, name:name||"", extra:extra||"", type:type||cls});
    }
    TABS.forEach(function(s){ push(s, s, "open tab", "tab"); });
    lists.forEach(function(L){ (L.symbols||[]).forEach(function(s){ push(s, L.name, L.name, classifySym(s)); }); });
    Object.keys(notes).forEach(function(s){ var n=noteObj(s); if(n.text) push(s, n.text.slice(0,60), "note", "note"); });
    if(q && /^[A-Z0-9:.\-]{1,20}$/i.test(q)) push(q.toUpperCase(), "Type to open", "direct", classifySym(q));
    if(ssTab==="fin") rows = rows.slice();
    ssRows=rows.slice(0,60);
    document.getElementById("ssres").innerHTML=ssRows.map(function(r,i){
      return "<div class='ss-hit"+(i===ssSel?" on":"")+"' data-i='"+i+"'><div><div class=nm>"+r.s+"</div><div class=ds>"+(r.name||r.type)+(r.extra&&r.extra!==r.name?" · "+r.extra:"")+"</div></div><div class=acts>"+
        "<button data-a=chart data-s='"+r.s+"'>Chart</button>"+
        "<button data-a=add data-s='"+r.s+"'>+ List</button>"+
        "<button data-a=notes data-s='"+r.s+"'>Notes</button>"+
        "<button data-a=fin data-s='"+r.s+"'>Fin</button></div></div>";
    }).join("")||"<div class=cell style=padding:14px>No matches — Enter opens "+(q||"ticker")+" anyway</div>";
    document.querySelectorAll("#ssres .ss-hit").forEach(function(el){ el.onclick=function(){ ssSel=+el.dataset.i; }; el.ondblclick=function(){ var r=ssRows[+el.dataset.i]; if(r) goSymbol(r.s, dest); }; });
    document.querySelectorAll("#ssres [data-a]").forEach(function(b){ b.onclick=function(e){ e.stopPropagation(); goSymbol(b.dataset.s, b.dataset.a); }; });
    if(q.length>=2) yahooSearch(q);
  }
  var ssYq="";
  async function yahooSearch(q){
    if(ssYq===q) return; ssYq=q;
    try{
      var r=await fetch("/api/yahoo-search?q="+encodeURIComponent(q));
      var j=await r.json();
      if(ssYq!==q) return;
      (j.quotes||[]).forEach(function(x){
        if(!x.symbol) return;
        var exists=ssRows.some(function(r){ return r.s===bare(x.symbol); });
        if(exists) return;
        ssRows.push({s:bare(x.symbol), name:x.name||"", extra:(x.exchange||"")+" "+(x.type||""), type:(x.type||"stock").toLowerCase()});
      });
      ssRows=ssRows.slice(0,80);
      var box=document.getElementById("ssres"); if(!box) return;
      box.innerHTML=ssRows.map(function(r,i){
        return "<div class='ss-hit"+(i===ssSel?" on":"")+"' data-i='"+i+"'><div><div class=nm>"+r.s+"</div><div class=ds>"+(r.name||r.type)+(r.extra?" · "+r.extra:"")+"</div></div><div class=acts>"+
          "<button data-a=chart data-s='"+r.s+"'>Chart</button>"+
          "<button data-a=add data-s='"+r.s+"'>+ List</button>"+
          "<button data-a=notes data-s='"+r.s+"'>Notes</button>"+
          "<button data-a=fin data-s='"+r.s+"'>Fin</button></div></div>";
      }).join("");
      document.querySelectorAll("#ssres .ss-hit").forEach(function(el){ el.ondblclick=function(){ var r=ssRows[+el.dataset.i]; if(r) goSymbol(r.s, document.getElementById("symsearch").dataset.dest||"chart"); }; });
      document.querySelectorAll("#ssres [data-a]").forEach(function(b){ b.onclick=function(e){ e.stopPropagation(); goSymbol(b.dataset.s, b.dataset.a); }; });
    }catch(e){}
  }
  function numish(x){ if(x==null) return null; if(typeof x==="number") return x; if(typeof x==="object" && x.raw!=null) return x.raw; var n=Number(x); return isFinite(n)?n:null; }
  function fmtBig(n){ if(n==null) return "—"; var a=Math.abs(n); if(a>=1e12) return (n/1e12).toFixed(2)+"T"; if(a>=1e9) return (n/1e9).toFixed(2)+"B"; if(a>=1e6) return (n/1e6).toFixed(2)+"M"; return fmt(n); }
  function renderFin(){
    var el=document.getElementById("fin"); if(!el) return;
    el.innerHTML="<b>FINANCIALS · "+active+"</b><div class=cell>Loading fundamentals…</div>";
    fillFinFromBars(el);
    fetch("/api/yahoo-fund?ticker="+encodeURIComponent(bare(active).replace("USDT",""))).then(function(r){ return r.json(); }).then(function(j){
      if(!j||!j.ok) return;
      finCache[active]=j;
      var p=j.price||{}, sd=j.summaryDetail||{}, ks=j.defaultKeyStatistics||{}, fd=j.financialData||{};
      var rows=[
        ["Name", p.shortName||p.longName||active],
        ["Exchange", p.exchangeName||"—"],
        ["Mkt cap", fmtBig(numish(p.marketCap)||numish(sd.marketCap))],
        ["P/E", fmt(numish(sd.trailingPE)||numish(ks.trailingPE))],
        ["Fwd P/E", fmt(numish(sd.forwardPE)||numish(ks.forwardPE))],
        ["EPS", fmt(numish(ks.trailingEps)||numish(fd.trailingEps))],
        ["Div yld", numish(sd.dividendYield)!=null?(numish(sd.dividendYield)*100).toFixed(2)+"%":"—"],
        ["Beta", fmt(numish(ks.beta))],
        ["52w high", fmt(numish(sd.fiftyTwoWeekHigh)||numish(p.fiftyTwoWeekHigh))],
        ["52w low", fmt(numish(sd.fiftyTwoWeekLow)||numish(p.fiftyTwoWeekLow))],
        ["Avg vol", fmtBig(numish(sd.averageVolume))],
        ["Target", fmt(numish(fd.targetMeanPrice))],
        ["Margin", numish(fd.profitMargins)!=null?(numish(fd.profitMargins)*100).toFixed(1)+"%":"—"]
      ];
      var inc=((j.incomeStatementHistory||{}).incomeStatementHistory)||[];
      var incHtml=inc.slice(0,4).map(function(y){ return "<div class=cell><span>"+String(y.endDate&&y.endDate.fmt||"").slice(0,10)+"</span><span>Rev "+fmtBig(numish(y.totalRevenue))+" · NI "+fmtBig(numish(y.netIncome))+"</span></div>"; }).join("");
      el.innerHTML="<b>FINANCIALS · "+active+"</b><div class=fin-grid>"+rows.map(function(r){ return "<div class=cell><span>"+r[0]+"</span><span>"+r[1]+"</span></div>"; }).join("")+"</div>"+(incHtml?"<b style=display:block;margin-top:8px>INCOME</b>"+incHtml:"")+"<div class=cell style=color:var(--mut);font-size:10px>Yahoo fundamentals · delayed · not advice</div>";
      renderDetail();
    }).catch(function(){});
  }
  function fillFinFromBars(el){
    if(!lastBars.length) return;
    var d=lastBars, last=d[d.length-1];
    var hi=-1e99, lo=1e99, i, vol=0, n=0;
    for(i=Math.max(0,d.length-252);i<d.length;i++){ if(d[i].high>hi) hi=d[i].high; if(d[i].low<lo) lo=d[i].low; vol+=d[i].volume||0; n++; }
    el.innerHTML="<b>FINANCIALS · "+active+"</b>"+
      "<div class=cell><span>Last</span><span>"+fmt(last.close)+"</span></div>"+
      "<div class=cell><span>52w range</span><span>"+fmt(lo)+" – "+fmt(hi)+"</span></div>"+
      "<div class=cell><span>Avg vol</span><span>"+fmtVol(n?vol/n:0)+"</span></div>"+
      "<div class=cell style=color:var(--mut)>Waiting on Yahoo quoteSummary…</div>";
  }
  function renderDetail(){
    var el=document.getElementById("detail"); if(!el) return;
    var q=quotes[active]||quotes[bare(active)]||{};
    var last=lastBars.length?lastBars[lastBars.length-1]:null;
    var px=last?last.close:q.last;
    var ch=q.chg, chgv=q.chgv;
    if(lastBars.length>1){ var prev=lastBars[lastBars.length-2]; ch=(last.close-prev.close)/prev.close; chgv=last.close-prev.close; }
    var hi=-1e99, lo=1e99, i;
    for(i=0;i<lastBars.length;i++){ if(lastBars[i].high>hi) hi=lastBars[i].high; if(lastBars[i].low<lo) lo=lastBars[i].low; }
    var pos=hi>lo&&px!=null? ((px-lo)/(hi-lo))*100 : 50;
    var note=noteObj(active).text;
    var cls=classifySym(active);
    el.innerHTML="<div style='display:flex;justify-content:space-between;align-items:center'><b>"+active+"</b><span style='font-size:10px;color:var(--mut)'>"+cls.toUpperCase()+"</span></div>"+
      "<div class=px "+(ch>=0?"style=color:var(--up)":"style=color:var(--dn)")+">"+(px!=null?fmt(px):"—")+" <span style=font-size:12px>"+(ch!=null?((ch>=0?"+":"")+(ch*100).toFixed(2)+"%"):"")+"</span></div>"+
      "<div class=rg><i style='width:"+pos+"%'></i><b style='left:"+pos+"%'></b></div>"+
      "<div class=cell><span>Range</span><span>"+(isFinite(lo)?fmt(lo):"—")+" – "+(isFinite(hi)?fmt(hi):"—")+"</span></div>"+
      "<div class=cell><span>Δ</span><span>"+(chgv!=null?((chgv>=0?"+":"")+fmt(chgv)):"—")+"</span></div>"+
      "<div style='display:flex;gap:6px;margin-top:8px'><button type=button id=d-fin>Financials</button><button type=button id=d-note>Notes</button><button type=button id=d-add>+ List</button></div>"+
      (note?"<div class=nitem style=margin-top:8px>"+note.slice(0,140)+"</div>":"");
    var df=document.getElementById("d-fin"); if(df) df.onclick=function(){ goSymbol(active,"fin"); };
    var dn=document.getElementById("d-note"); if(dn) dn.onclick=function(){ goSymbol(active,"notes"); };
    var da=document.getElementById("d-add"); if(da) da.onclick=function(){ addToList(active); };
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
    try{
      var d=await klines(sym,"1d"); if(d.length<2) return null;
      var hit=barCache[resolveSym(sym).ticker+"|1d"];
      if(hit && hit.src==="synth") return null;
      var last=d[d.length-1], prev=d[d.length-2]||last;
      return {last:last.close, chg:prev.close?(last.close-prev.close)/prev.close:0, chgv:last.close-prev.close, spark:d.slice(-20)};
    }catch(e){ return null; }
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
      return "<button class='wrow "+(bare(s)===active?"on":"")+"' data-s='"+s+"'>"+flagDot(s)+"<span>"+s+"</span><span>"+(q?fmt(q.last):"—")+"</span><span class="+(up?"up":"dn")+">"+(q?(q.chg>=0?"+":"")+(q.chg*100).toFixed(2)+"%":"—")+"</span><span class="+(up?"up":"dn")+">"+(q?(q.chgv>=0?"+":"")+fmt(q.chgv):"—")+"</span><span>"+(q&&q.spark&&q.spark.length?fmtVol(q.spark[q.spark.length-1].volume):"—")+"</span>"+sparkSvg(q&&q.spark,up)+"</button>";
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
    favs=loadJSON(FAV_KEY,[]); if(!Array.isArray(favs)) favs=[];
    var copied=[];
    var urls=warehouse("/data/tv-watchlists.json");
    for(var i=0;i<urls.length && !copied.length;i++){
      try{ var j=await fetchJson(urls[i]); var arr=Array.isArray(j)?j:(j.lists||[]); copied=arr.filter(function(l){return l&&l.name&&Array.isArray(l.symbols);}).map(function(l){ return {id:String(l.id||l.name),name:l.name,symbols:l.symbols,n:l.n||l.symbols.length}; }); }catch(e){}
    }
    lists=custom.concat(local, copied);
    if(favs.length) lists=[{id:"favorites",name:"Favorites",symbols:favs.slice(),n:favs.length,custom:1}].concat(lists.filter(function(l){ return l.id!=="favorites"; }));
  }
  async function loadIntel(){
    var urls=warehouse("/data/jh-internals.json");
    for(var i=0;i<urls.length;i++){
      try{ var j=await fetchJson(urls[i]); var f=j.fields||j;
        document.getElementById("intel").innerHTML="<b>INTERNALS · warehouse</b>"+[["2s10s",f.twos_tens!=null?f.twos_tens+"%":"—"],["LIQ $B",f.liq_proxy_bn!=null?f.liq_proxy_bn:"—"],["NFCI",f.nfci!=null?f.nfci:"—"],["A-D",f.ad_breadth!=null?Number(f.ad_breadth).toFixed(3)+(f.n_up!=null?" ("+f.n_up+"/"+f.n_down+")":""):"—"],["%>50d",f.pct_above_50!=null?(Number(f.pct_above_50)<=1?(f.pct_above_50*100).toFixed(1):Number(f.pct_above_50).toFixed(1))+"%":"—"],["%>200d",f.pct_above_200!=null?(Number(f.pct_above_200)<=1?(f.pct_above_200*100).toFixed(1):Number(f.pct_above_200).toFixed(1))+"%":"—"],["NH-NL",f.nh_nl!=null?f.nh_nl+" ("+(f.n_new_high||"—")+"H / "+(f.n_new_low||"—")+"L)":"—"]].map(function(x){return "<div class=cell><span>"+x[0]+"</span><span>"+x[1]+"</span></div>";}).join("");
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
    var el=document.getElementById("notes"); if(!el) return;
    var mine=noteObj(active);
    var all=Object.keys(notes).filter(function(s){ return noteObj(s).text; }).sort();
    el.innerHTML="<b>NOTES · "+active+"</b>"+
      "<textarea id=noteb style='width:100%;min-height:110px;border:1px solid var(--line);padding:8px;margin-top:6px;font-family:IBM Plex Sans,sans-serif' placeholder='Write like TV text notes — saved on this device'>"+mine.text+"</textarea>"+
      "<div style='display:flex;justify-content:space-between;align-items:center;margin-top:6px'><span style=color:var(--mut);font-size:10px>"+(mine.at||"unsaved")+"</span><span><button id=notedel>Clear</button> <button id=notesave>Save</button></span></div>"+
      "<b style=display:block;margin-top:12px>ALL NOTES · "+all.length+"</b>"+
      "<input id=noteq placeholder='Search notes' style='width:100%;border:1px solid var(--line);padding:6px;margin:6px 0'>"+
      "<div id=noteall></div>";
    function paintAll(q){
      q=(q||"").toLowerCase();
      var rows=all.filter(function(s){ var t=noteObj(s); return !q || s.toLowerCase().indexOf(q)>=0 || t.text.toLowerCase().indexOf(q)>=0; });
      document.getElementById("noteall").innerHTML=rows.map(function(s){ var t=noteObj(s); return "<button class=note-card data-s='"+s+"'><b>"+s+"</b><span>"+t.text.slice(0,90)+"</span><span>"+(t.at||"")+"</span></button>"; }).join("")||"<div class=cell>No notes yet — save one above</div>";
      document.querySelectorAll("#noteall [data-s]").forEach(function(b){ b.onclick=function(){ goSymbol(b.dataset.s,"notes"); }; });
    }
    paintAll("");
    document.getElementById("noteq").oninput=function(){ paintAll(this.value); };
    document.getElementById("notesave").onclick=function(){
      var t=document.getElementById("noteb").value;
      notes[active]={text:t, at:new Date().toISOString().slice(0,16).replace("T"," ")};
      if(!t) delete notes[active];
      saveJSON(NOTE_KEY, notes); toast(t?"Note saved · "+active:"Note cleared"); renderNotes(); renderDetail();
    };
    document.getElementById("notedel").onclick=function(){ delete notes[active]; saveJSON(NOTE_KEY, notes); renderNotes(); };
  }
  function renderHeat(){
    var el=document.getElementById("heat"); if(!el) return;
    var L=lists.find(function(x){return x.id===listId;})||{symbols:[],name:""};
    var syms=(L.symbols||[]).slice(0,96);
    el.innerHTML="<b>HEATMAP · "+(L.name||"")+"</b><div class=heatgrid>"+syms.map(function(s){
      var q=quotes[s]||quotes[bare(s)]; var ch=q?q.chg*100:0;
      var a=q?Math.min(0.88,0.18+Math.abs(ch)/6):0.12;
      var bg=!q?"var(--chip)": ch>=0? "rgba(8,153,129,"+a+")" : "rgba(242,54,69,"+a+")";
      var col=!q?"var(--mut)":"#fff";
      return "<button class=hcell data-s='"+s+"' style='background:"+bg+";color:"+col+"'>"+bare(s)+"<span>"+(q?(ch>=0?"+":"")+ch.toFixed(1)+"%":"—")+"</span></button>";
    }).join("")+"</div>";
    el.querySelectorAll("[data-s]").forEach(function(b){
      b.onclick=function(){ var s=b.dataset.s; if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); loadDraw(); renderTabs(); load(); };
    });
    syms.slice(0,48).forEach(function(s){ if(quotes[s]||quotes[bare(s)]) return; lastPx(s).then(function(px){ if(!px) return; quotes[s]=px; quotes[bare(s)]=px; if(wtab==="heat") renderHeat(); }); });
  }
  function toggleFav(s){
    s=bare(s);
    var on=favs.indexOf(s)>=0;
    favs=on?favs.filter(function(x){return x!==s;}):favs.concat([s]);
    saveJSON(FAV_KEY,favs);
    lists=lists.filter(function(l){return l.id!=="favorites";});
    if(favs.length) lists=[{id:"favorites",name:"Favorites",symbols:favs.slice(),n:favs.length,custom:1}].concat(lists);
    renderList();
    toast(favs.indexOf(s)>=0?"Starred "+s:"Unstarred "+s);
  }

  function openInd(){
    var m=document.getElementById("modal"), box=document.getElementById("mbox");
    var cats=[]; INDS.forEach(function(i){ if(cats.indexOf(i.cat)<0) cats.push(i.cat); });
    var ocats=[]; OSC.forEach(function(o){ if(ocats.indexOf(o.cat)<0) ocats.push(o.cat); });
    box.innerHTML="<h3>Indicators</h3><input id=indq placeholder='Search RSI, Ichimoku, VWAP…' style='width:100%;border:1px solid var(--line);padding:8px;border-radius:4px;margin-bottom:8px'><div style=color:var(--mut);font-size:11px;margin-bottom:8px>Templates: <button id=t-tv>TV default</button> <button id=t-tr>Trend</button> <button id=t-os>Oscillators</button> <button id=t-vol>Volume</button> <button id=t-ich>Ichimoku</button> <button id=t-rib>MA ribbon</button> <button id=t-cl>Clean</button></div><div id=indbody></div><div style='margin-top:10px;text-align:right'><button id=indok>Apply</button></div>";
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
    document.getElementById("t-rib").onclick=function(){ setTpl(function(){ INDS.forEach(function(i){ i.on=/ema9|ema21|ema50|ema200|sma20|sma50/.test(i.id); }); OSC.forEach(function(o){ o.on=false; }); }); };
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
      "<label class=indrow><span>Dark theme</span><input type=checkbox id=s-dark "+(dark?"checked":"")+"></label>"+
      "<label class=indrow><span>Live tape</span><input type=checkbox id=s-live "+(liveOn?"checked":"")+"></label>"+
      "<label class=indrow><span>Data window</span><input type=checkbox id=s-dwin "+(dwinOn?"checked":"")+"></label>"+
      "<label class=indrow><span>Navigator</span><input type=checkbox id=s-mini "+(miniOn?"checked":"")+"></label>"+
      "<label class=indrow><span>Left scale</span><input type=checkbox id=s-left "+(leftOn?"checked":"")+"></label>"+
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
      dark=document.getElementById("s-dark").checked; liveOn=document.getElementById("s-live").checked;
      dwinOn=document.getElementById("s-dwin").checked; miniOn=document.getElementById("s-mini").checked; leftOn=document.getElementById("s-left").checked;
      var tz=TZS.filter(function(z){ return String(z[1])===document.getElementById("s-tz").value; })[0]||TZS[0];
      tzOff=tz[1]; tzName=tz[0]; document.getElementById("tzlab").textContent=tzName;
      var el=document.getElementById("dwin"); if(el) el.className=dwinOn?"on":"";
      var mn=document.getElementById("mini"); if(mn) mn.className=miniOn?"on":"";
      var lp=document.getElementById("livepill"); if(lp) lp.className=liveOn?"on":"";
      saveLay(); m.className=""; applyTheme(true); renderTf();
    };
  }
  function saveLay(){ saveJSON(LAY_KEY,{gridOn:gridOn,watermark:watermark,magnet:magnet,magnetMode:magnetMode,layout:layout,kind:kind,tf:tf,invert:invert,hiLo:hiLo,crossMode:crossMode,tzName:tzName,tzOff:tzOff,stayTool:stayTool,volOn:volOn,dark:dark,liveOn:liveOn,dwinOn:dwinOn,miniOn:miniOn,leftOn:leftOn}); }
  function openCtx(x,y,s){
    var el=document.getElementById("ctx");
    el.style.display="block"; el.style.left=x+"px"; el.style.top=y+"px";
    el.innerHTML="<button data-a=open>Open chart</button><button data-a=fin>Financials</button><button data-a=note>Notes</button><button data-a=add>Add to list</button><button data-a=cmp>Compare</button><button data-a=al>Alert at last</button><button data-a=tab>Add tab</button><button data-a=fav>Favorite</button><button data-a=flag>Cycle flag</button>";
    el.querySelectorAll("button").forEach(function(b){
      b.onclick=function(){
        if(b.dataset.a==="open"||b.dataset.a==="tab") goSymbol(s,"chart");
        if(b.dataset.a==="fin") goSymbol(s,"fin");
        if(b.dataset.a==="note") goSymbol(s,"notes");
        if(b.dataset.a==="add") addToList(s);
        if(b.dataset.a==="cmp"){ if(compare.indexOf(bare(s))<0) compare.push(bare(s)); if(lastBars.length) paint(lastBars); }
        if(b.dataset.a==="al"){ var q=quotes[s]||quotes[bare(s)]; addAlert(bare(s), q?q.last:(lastBars[lastBars.length-1]||{}).close); }
        if(b.dataset.a==="flag"){ var cols=["#2962ff","#089981","#f23645","#ff6d00","#ab47bc",""], cur=flags[s]||""; var ix=cols.indexOf(cur); flags[s]=cols[(ix+1)%cols.length]; saveJSON(FLAG_KEY, flags); renderList(); }
        if(b.dataset.a==="fav"){ toggleFav(s); }
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
    for(i=0;i<lists.length && out.length<80;i++){
      var arr=lists[i].symbols||[];
      var scan=q?arr:arr.slice(0,16);
      scan.forEach(function(s){ if(out.length>=80) return; push("sym", s, function(){ if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); loadDraw(); renderTabs(); load(); }, lists[i].name); });
    }
    INDS.forEach(function(ind){ push("ind", ind.n, function(){ ind.on=!ind.on; if(lastBars.length) paint(lastBars); }, "indicator · "+ind.cat); });
    OSC.forEach(function(o){ push("osc", o.n, function(){ o.on=!o.on; if(lastBars.length) paint(lastBars); }, "oscillator · "+o.cat); });
    allTools().forEach(function(t){ push("draw", t[2], function(){ tool=t[0]; renderRail(); }, "drawing"); });
    Object.keys(notes).forEach(function(s){ var t=noteObj(s).text; if(t) push("note", s+" · "+t.slice(0,40), function(){ goSymbol(s,"notes"); }, "note"); });
    [["Financials",function(){ goSymbol(active,"fin"); }],["Notes",function(){ goSymbol(active,"notes"); }],["Symbol search",function(){ openSymSearch(""); }],["Indicators",openInd],["Compare",openCmp],["Replay",startReplay],["Snapshot",shot],["Settings",openSet],["CSV",exportCSV],["Fullscreen",function(){ document.getElementById("app").requestFullscreen(); }],["Layout 1",function(){ setLayout(1); renderTf(); }],["Layout 2",function(){ setLayout(2); renderTf(); }],["Layout 4",function(){ setLayout(4); renderTf(); }],["Screener",function(){ wtab="screen"; renderWtabs(); renderScreen(); }],["Paper trade",function(){ wtab="trade"; renderWtabs(); renderTrade(); }],["Strategy",function(){ wtab="test"; renderWtabs(); renderTest(); }],["Overview",function(){ wtab="over"; renderWtabs(); if(lastBars.length) renderOver(lastBars); }],["Hotkeys",openKeys],["Theme",function(){ dark=!dark; applyTheme(true); renderTf(); }]].forEach(function(x){ push("cmd", x[0], x[1], "command"); });
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
  function showType(){
    var el=document.getElementById("typeq"); if(!el) return;
    if(!typeBuf){ el.className=""; el.innerHTML=""; return; }
    el.className="on";
    var items=cmdItems(typeBuf).slice(0,12);
    el.innerHTML="<div class=lab>Type to open</div>"+items.map(function(it,i){ return "<button data-i='"+i+"'><b>"+it.label+"</b><span>"+it.kind+(it.extra?" · "+it.extra:"")+"</span></button>"; }).join("")||"<div class=cell style=padding:8px>No match</div>";
    el.querySelectorAll("[data-i]").forEach(function(b){ b.onclick=function(){ var it=items[+b.dataset.i]; typeBuf=""; showType(); if(it&&it.run) it.run(); }; });
  }
  function openChartCtx(x,y,px){
    var el=document.getElementById("ctx");
    el.style.display="block"; el.style.left=x+"px"; el.style.top=y+"px";
    el.innerHTML="<button data-a=al>Alert at "+fmt(px)+"</button><button data-a=hl>Horizontal at "+fmt(px)+"</button><button data-a=cp>Copy price</button><button data-a=ms>Measure (Shift+click)</button><button data-a=rst>Reset view</button><button data-a=fav>Favorite "+active+"</button>";
    el.querySelectorAll("button").forEach(function(b){
      b.onclick=function(){
        if(b.dataset.a==="al" && px!=null) addAlert(active, px);
        if(b.dataset.a==="hl" && px!=null){
          var id=uid(), d={id:id,kind:"hline",points:[{time:lastBars.length?lastBars[lastBars.length-1].time:0,price:px}],color:drawColor,w:drawW};
          drawings.push(d); undo.push({op:"add",id:id,item:d}); saveDraw(); drawSVG();
        }
        if(b.dataset.a==="cp" && px!=null){ try{ navigator.clipboard.writeText(String(px)); toast("Copied "+fmt(px)); }catch(e){ toast(fmt(px)); } }
        if(b.dataset.a==="ms"){ tool="measure"; renderRail(); }
        if(b.dataset.a==="rst"){ try{ chart.timeScale().fitContent(); }catch(e){} }
        if(b.dataset.a==="fav") toggleFav(active);
        el.style.display="none";
      };
    });
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
    try{
      var url=chart.takeScreenshot().toDataURL("image/png");
      var a=document.createElement("a"); a.href=url; a.download=active+"-"+tf+".png"; a.click();
      if(navigator.clipboard && window.ClipboardItem){
        fetch(url).then(function(r){ return r.blob(); }).then(function(b){ return navigator.clipboard.write([new ClipboardItem({"image/png":b})]); }).then(function(){ toast("Snapshot copied"); }).catch(function(){});
      }
    } catch(e){ toast("Snapshot unavailable"); }
  }
  function exportCSV(){
    var rows=["time,open,high,low,close,volume"].concat(lastBars.map(function(b){ return [new Date(b.time*1000).toISOString(),b.open,b.high,b.low,b.close,b.volume].join(","); }));
    var a=document.createElement("a"); a.href=URL.createObjectURL(new Blob([rows.join("\n")],{type:"text/csv"})); a.download=active+".csv"; a.click();
  }
  function countdown(last){
    var el=document.getElementById("cd"); if(!el) return;
    var id=spec(tf)[0], sec= id==="1d"?86400: id==="1w"?604800: id==="1h"?3600: id==="1m"?60: id==="5m"?300: id==="15m"?900: id==="30m"?1800: id==="4h"?14400:86400;
    var end=last.time+sec, left=end-Math.floor(Date.now()/1000); if(left<0) left=0;
    var h=Math.floor(left/3600), m=Math.floor((left%3600)/60), s=left%60;
    el.textContent="bar "+h+":"+String(m).padStart(2,"0")+":"+String(s).padStart(2,"0");
  }

  chart.subscribeCrosshairMove(function(param){
    var hud=document.getElementById("hud"), ohlc=document.getElementById("ohlc");
    if(!param||!param.time||!mainSeries){ hud.style.display="none"; if(dwinOn){ var dw=document.getElementById("dwin"); if(dw) dw.style.opacity=".55"; } return; }
    var d=param.seriesData.get(mainSeries); if(!d){ hud.style.display="none"; return; }
    var o=d.open!=null?d.open:d.value, h=d.high!=null?d.high:d.value, l=d.low!=null?d.low:d.value, c=d.close!=null?d.close:d.value;
    hud.style.display="block";
    hud.innerHTML=new Date((param.time+tzOff*3600)*1000).toISOString().slice(0,16).replace("T"," ")+" "+tzName+"  O "+fmt(o)+" H "+fmt(h)+" L "+fmt(l)+" C "+fmt(c);
    if(ohlc) ohlc.innerHTML="O "+fmt(o)+"  H <span class=up>"+fmt(h)+"</span>  L <span class=dn>"+fmt(l)+"</span>  C "+fmt(c);
    renderLegend(param.time);
    renderDwin(param);
  });
  chart.timeScale().subscribeVisibleLogicalRangeChange(function(){ drawSVG(); });
  document.getElementById("chart").addEventListener("click", onChartClick);
  document.getElementById("chart").addEventListener("contextmenu", function(ev){
    ev.preventDefault();
    if(!mainSeries) return;
    var box=document.getElementById("chart").getBoundingClientRect();
    var y=ev.clientY-box.top;
    var p=mainSeries.coordinateToPrice(y);
    openChartCtx(ev.clientX, ev.clientY, p==null?null:Number(p));
  });
  document.getElementById("chart").addEventListener("dblclick", function(ev){ if(tool==="polyline" && pending){ finishTool(); saveDraw(); drawSVG(); } });
  function chartXY(ev){
    var box=document.getElementById("chart").getBoundingClientRect();
    return {x:ev.clientX-box.left, y:ev.clientY-box.top, box:box};
  }
  function nearestHandle(x,y){
    if(!selDraw) return null;
    var d=drawings.find(function(z){return z.id===selDraw;}); if(!d) return null;
    var pts=(d.points||[]).map(function(p){ return xy(p.time,p.price); });
    for(var i=0;i<pts.length;i++) if(pts[i] && Math.hypot(pts[i].x-x,pts[i].y-y)<9) return {d:d, idx:i};
    var hit=nearestDraw(x,y); if(hit && hit.id===selDraw) return {d:hit, idx:-1};
    return null;
  }
  document.getElementById("chart").addEventListener("mousedown", function(ev){
    if(ev.button!==0 || !mainSeries) return;
    var p=chartXY(ev);
    if(tool==="cursor" && selDraw){
      var h=nearestHandle(p.x,p.y);
      if(h){ dragState={id:h.d.id, idx:h.idx, sx:p.x, sy:p.y, origin:JSON.parse(JSON.stringify(h.d.points))}; ev.preventDefault(); }
    }
  });
  document.getElementById("chart").addEventListener("mousemove", function(ev){
    var p=chartXY(ev);
    if(dragState && (ev.buttons&1) && mainSeries){
      var t=chart.timeScale().coordinateToTime(p.x); var pr=mainSeries.coordinateToPrice(p.y);
      if(t==null||pr==null) return;
      var d=drawings.find(function(x){return x.id===dragState.id;}); if(!d) return;
      if(dragState.idx>=0) d.points[dragState.idx]=snapPt(t, Number(pr));
      else {
        var t0=chart.timeScale().coordinateToTime(dragState.sx), p0=mainSeries.coordinateToPrice(dragState.sy);
        if(t0==null||p0==null) return;
        var dt=Number(t)-Number(t0), dp=Number(pr)-Number(p0);
        d.points=dragState.origin.map(function(pt){ return {time:pt.time+dt, price:pt.price+dp}; });
      }
      drawSVG(); return;
    }
    if((tool==="brush"||tool==="highlight")&&pending&&(ev.buttons&1)){ onChartClick(ev); return; }
    if(pending && pending.points.length===1 && needPts(tool)>=2 && mainSeries){
      var t2=chart.timeScale().coordinateToTime(p.x); var pr2=mainSeries.coordinateToPrice(p.y);
      if(t2!=null && pr2!=null){ ghostPt=snapPt(t2, Number(pr2)); drawSVG(); }
    }
  });
  document.addEventListener("mouseup", function(){
    if(dragState){ saveDraw(); dragState=null; }
    if((tool==="brush"||tool==="highlight"||tool==="path"||tool==="curve") && pending) finishTool();
    ghostPt=null; drawSVG();
  });

  document.addEventListener("click", function(e){
    var el=document.getElementById("ctx"); if(el && !el.contains(e.target)) el.style.display="none";
    var m=document.getElementById("modal"); if(e.target===m) m.className="";
    var fly=document.getElementById("fly"); if(fly && !fly.contains(e.target) && !e.target.closest(".gbtn")) closeFly();
    var menu=document.getElementById("menu"); if(menu && !menu.contains(e.target) && !e.target.closest(".drop")) closeMenu();
    var cmd=document.getElementById("cmdk"); if(e.target===cmd) cmd.className="";
    var ss=document.getElementById("symsearch"); if(e.target===ss) closeSymSearch();
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
    if(e.key==="/" && !e.metaKey && !e.ctrlKey){ e.preventDefault(); openSymSearch(""); return; }
    if(e.key==="Escape"){ tool="cursor"; pending=null; typeBuf=""; showType(); document.getElementById("modal").className=""; document.getElementById("cmdk").className=""; closeSymSearch(); closeFly(); selDraw=null; renderRail(); renderProps(); return; }
    if(e.altKey && (e.key==="t"||e.key==="T")) { tool="trend"; renderRail(); return; }
    if(e.altKey && (e.key==="h"||e.key==="H")) { tool="hline"; renderRail(); return; }
    if(e.altKey && (e.key==="f"||e.key==="F")) { tool="fib"; renderRail(); return; }
    if(e.altKey && (e.key==="v"||e.key==="V")) { tool="vline"; renderRail(); return; }
    if(e.altKey && (e.key==="r"||e.key==="R")) { tool="rect"; renderRail(); return; }
    if(e.altKey && (e.key==="e"||e.key==="E")) { tool="eraser"; renderRail(); return; }
    if((e.ctrlKey||e.metaKey) && e.key==="z"){ e.preventDefault(); undoDraw(); return; }
    if((e.ctrlKey||e.metaKey) && (e.key==="y"||(e.shiftKey&&e.key==="Z"))){ e.preventDefault(); redoDraw(); return; }
    if((e.ctrlKey||e.metaKey) && e.key==="s"){ e.preventDefault(); shot(); return; }
    if((e.ctrlKey||e.metaKey) && e.key==="i"){ e.preventDefault(); openInd(); return; }
    if((e.ctrlKey||e.metaKey) && e.key==="c"){ if(selDraw){ var d=drawings.find(function(x){return x.id===selDraw;}); if(d){ clipDraw=JSON.parse(JSON.stringify(d)); toast("Copied drawing"); } } return; }
    if((e.ctrlKey||e.metaKey) && e.key==="v"){
      if(clipDraw){ var n=JSON.parse(JSON.stringify(clipDraw)); n.id=uid(); n.points=(n.points||[]).map(function(p){ return {time:p.time+86400, price:p.price}; }); drawings.push(n); undo.push({op:"add",id:n.id,item:n}); saveDraw(); drawSVG(); toast("Pasted drawing"); }
      return;
    }
    if(e.key==="Delete"||e.key==="Backspace"){ if(selDraw){ var gone=drawings.find(function(d){return d.id===selDraw;}); drawings=drawings.filter(function(d){return d.id!==selDraw;}); if(gone) undo.push({op:"del",item:gone}); selDraw=null; saveDraw(); drawSVG(); renderProps(); } return; }
    if(e.key===" "){ e.preventDefault(); if(replay.on) document.getElementById("rp-play").click(); else startReplay(); return; }
    if(e.key==="+"||e.key==="="){ e.preventDefault(); zoomChart(1); return; }
    if(e.key==="-"||e.key==="_"){ e.preventDefault(); zoomChart(-1); return; }
    if(e.key==="0"){ try{ chart.timeScale().fitContent(); }catch(err){} return; }
    if(e.key==="?"||(e.shiftKey && e.key==="/")){ e.preventDefault(); openKeys(); return; }
    if(e.altKey && e.key==="1"){ setLayout(1); renderTf(); return; }
    if(e.altKey && e.key==="2"){ setLayout(2); renderTf(); return; }
    if(e.altKey && e.key==="4"){ setLayout(4); renderTf(); return; }
    if(e.altKey && (e.key==="d"||e.key==="D")){ dark=!dark; applyTheme(true); renderTf(); return; }
    if(!e.ctrlKey && !e.metaKey && !e.altKey && /^[a-zA-Z0-9.:]$/.test(e.key)){
      typeBuf+=e.key.toUpperCase();
      clearTimeout(typeT); typeT=setTimeout(function(){ typeBuf=""; showType(); }, 2800);
      showType();
      return;
    }
    if(e.key==="Enter" && typeBuf){
      var items=cmdItems(typeBuf);
      typeBuf=""; showType();
      if(items[0]&&items[0].run) items[0].run();
    }
  });

  function writeState(){
    try{
      var q="s="+encodeURIComponent(active)+"&tf="+tf+"&k="+kind+"&m="+mode+(dark?"&th=d":"");
      history.replaceState(null,"",location.pathname+location.search+"#"+q);
    }catch(e){}
  }
  function readState(){
    try{
      var h=String(location.hash||"").replace(/^#/,"");
      if(!h) return;
      var sp=new URLSearchParams(h);
      if(sp.get("s")) active=sp.get("s").toUpperCase();
      if(sp.get("tf")) tf=sp.get("tf");
      if(sp.get("k")) kind=sp.get("k");
      if(sp.get("m")) mode=sp.get("m");
      if(sp.get("th")==="d") dark=true;
      if(TABS.indexOf(bare(active))<0) TABS.unshift(bare(active));
    }catch(e){}
  }
  var miniChart=null, miniSeries=null;
  function paintMini(d){
    var el=document.getElementById("mini"); if(!el) return;
    el.className=miniOn?"on":"";
    if(!miniOn) return;
    var p=pal();
    if(!miniChart){
      miniChart=LW.createChart(el,{ autoSize:true, height:52, layout:{background:{type:"solid",color:p.bg},textColor:"transparent",fontSize:1}, grid:{vertLines:{visible:false},horzLines:{visible:false}}, timeScale:{visible:false,borderVisible:false}, rightPriceScale:{visible:false,borderVisible:false}, handleScroll:false, handleScale:false, crosshair:{vertLine:{visible:true,labelVisible:false},horzLine:{visible:false}} });
      miniSeries=miniChart.addAreaSeries({ lineColor:ACC, topColor:"rgba(41,98,255,.25)", bottomColor:"rgba(41,98,255,.02)", lineWidth:1, lastValueVisible:false, priceLineVisible:false });
      miniChart.subscribeClick(function(param){
        if(!param||param.time==null||!lastBars.length) return;
        var t=param.time, span=Math.max(20, Math.floor(lastBars.length*0.12))* (lastBars[1]&&lastBars[0]? lastBars[1].time-lastBars[0].time:86400);
        try{ chart.timeScale().setVisibleRange({from:t-span, to:t+span*0.2}); }catch(e){}
      });
    } else {
      try{ miniChart.applyOptions({ layout:{background:{type:"solid",color:p.bg}} }); }catch(e){}
    }
    if(miniSeries) miniSeries.setData(d.map(function(b){ return {time:b.time,value:b.close}; }));
  }
  function paperFill(side, qty){
    qty=+qty; if(!qty||qty<=0){ toast("Enter quantity"); return; }
    var px=lastBars.length?lastBars[lastBars.length-1].close:0; if(!px) return;
    var pos=paper.positions[active]||{qty:0,avg:0};
    if(side==="buy"){
      var cost=px*qty; if(cost>paper.cash+1e-9){ toast("Insufficient cash"); return; }
      var nq=pos.qty+qty; pos.avg=nq?(pos.avg*pos.qty+cost)/nq:0; pos.qty=nq; paper.cash-=cost;
    } else {
      var sell=Math.min(qty, pos.qty); if(!sell){ toast("No position"); return; }
      paper.cash+=px*sell; paper.realized+=(px-pos.avg)*sell; pos.qty-=sell; if(!pos.qty) pos.avg=0;
    }
    paper.positions[active]=pos;
    paper.trades.unshift({t:Date.now(),sym:active,side:side,qty:qty,px:px});
    paper.trades=paper.trades.slice(0,80);
    saveJSON(PAPER_KEY, paper);
    renderTrade();
    if(lastBars.length) paint(lastBars);
    toast((side==="buy"?"Bought":"Sold")+" "+qty+" "+active+" @ "+fmt(px));
  }
  function paperMtm(){
    var mtm=paper.cash, k;
    for(k in paper.positions){
      var p=paper.positions[k]; if(!p||!p.qty) continue;
      var px=(k===active && lastBars.length)?lastBars[lastBars.length-1].close:(quotes[k]&&quotes[k].last)||p.avg;
      mtm+=p.qty*px;
    }
    return mtm;
  }
  function renderTrade(){
    var el=document.getElementById("trade"); if(!el) return;
    var pos=paper.positions[active]||{qty:0,avg:0};
    var px=lastBars.length?lastBars[lastBars.length-1].close:0;
    var u=(pos.qty&&px)?(px-pos.avg)*pos.qty:0;
    var eq=paperMtm();
    var eqUp=eq>=100000;
    el.innerHTML="<b>PAPER · 100k start</b>"+
      "<div class=cell><span>Cash</span><span>"+fmt(paper.cash)+"</span></div>"+
      "<div class=cell><span>Equity</span><span class="+(eqUp?"up":"dn")+">"+fmt(eq)+" ("+(eqUp?"+":"")+((eq/100000-1)*100).toFixed(2)+"%)</span></div>"+
      "<div class=cell><span>Realized</span><span class="+(paper.realized>=0?"up":"dn")+">"+fmt(paper.realized)+"</span></div>"+
      "<div class=cell><span>"+active+" pos</span><span>"+(pos.qty?pos.qty+" @ "+fmt(pos.avg):"flat")+"</span></div>"+
      "<div class=cell><span>Unrealized</span><span class="+(u>=0?"up":"dn")+">"+fmt(u)+"</span></div>"+
      "<div style='display:flex;gap:8px;align-items:center;margin:10px 0'><button class='tbtn sellx' id=psell>Sell</button><input class=tqty id=pqty type=number min=0 step=any placeholder='qty'><button class='tbtn buyx' id=pbuy>Buy</button></div>"+
      "<div class=sbar><button id=p25>25%</button><button id=p50>50%</button><button id=p100>100%</button><button id=pflat>Flatten</button><button id=preset>Reset</button></div>"+
      "<div class=icat>BLOTTER</div>"+(paper.trades.length?paper.trades.slice(0,12).map(function(t){ return "<div class=cell><span class="+(t.side==="buy"?"up":"dn")+">"+t.side.toUpperCase()+" "+t.sym+"</span><span>"+t.qty+" @ "+fmt(t.px)+"</span></div>"; }).join(""):"<div class=cell>No fills yet</div>");
    function qtyFromPct(pct){ var px=lastBars.length?lastBars[lastBars.length-1].close:0; if(!px) return 0; return Math.floor((paper.cash*pct/px)*10000)/10000; }
    var qel=document.getElementById("pqty");
    document.getElementById("pbuy").onclick=function(){ paperFill("buy", qel.value||qtyFromPct(1)); };
    document.getElementById("psell").onclick=function(){ paperFill("sell", qel.value||(paper.positions[active]&&paper.positions[active].qty)||0); };
    document.getElementById("p25").onclick=function(){ qel.value=qtyFromPct(0.25); };
    document.getElementById("p50").onclick=function(){ qel.value=qtyFromPct(0.5); };
    document.getElementById("p100").onclick=function(){ qel.value=qtyFromPct(1); };
    document.getElementById("pflat").onclick=function(){ var p=paper.positions[active]; if(p&&p.qty) paperFill("sell", p.qty); };
    document.getElementById("preset").onclick=function(){ paper={cash:100000,positions:{},trades:[],realized:0}; saveJSON(PAPER_KEY,paper); renderTrade(); toast("Paper reset"); };
  }
  function runStrat(id, d){
    d=d||lastBars; if(!d||d.length<60) return null;
    var cash=10000, qty=0, entry=0, trades=[], equity=[], fee=0.0005, i;
    var s20=sma(d,20), s50=sma(d,50), s9=ema(d,9), s21=ema(d,21), r=rsi(d,14), m=macd(d), st=supertrend(d), bb=bbands(d,20,2);
    function map(arr){ var o={}; for(var i=0;i<arr.length;i++) o[arr[i].time]=arr[i].value!=null?arr[i].value:arr[i].hist; return o; }
    var m20=map(s20), m50=map(s50), m9=map(s9), m21=map(s21), mr=map(r), mm=map(m.map(function(p){return {time:p.time,value:p.macd};})), ms=map(m.map(function(p){return {time:p.time,value:p.signal};})), mst=map(st), mup=map(bb.up), mdn=map(bb.dn);
    function sig(i){
      var t=d[i].time, p=d[i].close;
      if(id==="sma") return (m20[t]!=null && m50[t]!=null) ? (m20[t]>m50[t]?1:-1) : 0;
      if(id==="ema") return (m9[t]!=null && m21[t]!=null) ? (m9[t]>m21[t]?1:-1) : 0;
      if(id==="rsi"){ if(mr[t]==null) return 0; if(mr[t]<30) return 1; if(mr[t]>70) return -1; return 0; }
      if(id==="macd") return (mm[t]!=null && ms[t]!=null) ? (mm[t]>ms[t]?1:-1) : 0;
      if(id==="st") return mst[t]!=null ? (p>mst[t]?1:-1) : 0;
      if(id==="bb"){ if(mdn[t]==null) return 0; if(p<mdn[t]) return 1; if(p>mup[t]) return -1; return 0; }
      return 0;
    }
    var prev=0;
    for(i=50;i<d.length;i++){
      var s=sig(i), px=d[i].close;
      if(id==="rsi"||id==="bb"){
        if(s>0 && qty===0){ qty=cash*(1-fee)/px; cash=0; entry=px; trades.push({t:d[i].time,side:"buy",px:px}); }
        else if(s<0 && qty>0){ cash=qty*px*(1-fee); trades.push({t:d[i].time,side:"sell",px:px,pnl:(px-entry)/entry}); qty=0; }
      } else {
        if(s!==prev && s!==0){
          if(qty>0 && s<0){ cash=qty*px*(1-fee); trades.push({t:d[i].time,side:"sell",px:px,pnl:(px-entry)/entry}); qty=0; }
          if(qty===0 && s>0){ qty=cash*(1-fee)/px; cash=0; entry=px; trades.push({t:d[i].time,side:"buy",px:px}); }
        }
        if(s) prev=s;
      }
      equity.push({time:d[i].time,value:cash+qty*px});
    }
    if(qty && d.length){ var px=d[d.length-1].close; cash=qty*px*(1-fee); trades.push({t:d[d.length-1].time,side:"sell",px:px,pnl:(px-entry)/entry}); qty=0; equity[equity.length-1]={time:d[d.length-1].time,value:cash}; }
    var wins=trades.filter(function(t){ return t.pnl>0; }).length, n=trades.filter(function(t){ return t.pnl!=null; }).length;
    var gp=0, gl=0; trades.forEach(function(t){ if(t.pnl>0) gp+=t.pnl; else if(t.pnl<0) gl+=-t.pnl; });
    var peak=-1e99, dd=0; equity.forEach(function(p){ if(p.value>peak) peak=p.value; var x=peak?p.value/peak-1:0; if(x<dd) dd=x; });
    var ret=cash/10000-1;
    var bh=d[50]&&d[50].close? d[d.length-1].close/d[50].close-1 : 0;
    return {id:id, trades:trades, equity:equity, stats:{ret:ret, wr:n?wins/n:0, pf:gl?gp/gl:(gp?99:0), n:n, dd:dd, bh:bh, end:cash}};
  }
  function renderTest(){
    var el=document.getElementById("test"); if(!el) return;
    var names=[["sma","SMA 20/50"],["ema","EMA 9/21"],["rsi","RSI 30/70"],["macd","MACD cross"],["st","Supertrend"],["bb","BB mean-rev"]];
    var s=lastTest && lastTest.stats;
    el.innerHTML="<b>STRATEGY TESTER</b><p style=color:var(--mut);font-size:10px>All-in, 5 bps fee, local bars. Not advice.</p><div class=sbar>"+names.map(function(n){ return "<button data-st='"+n[0]+"' class='"+(lastTest&&lastTest.id===n[0]?"on":"")+"'>"+n[1]+"</button>"; }).join("")+"</div>"+
      (s?("<div class=cell><span>Return</span><span class="+(s.ret>=0?"up":"dn")+">"+(s.ret*100).toFixed(2)+"%</span></div>"+
        "<div class=cell><span>Buy & hold</span><span class="+(s.bh>=0?"up":"dn")+">"+(s.bh*100).toFixed(2)+"%</span></div>"+
        "<div class=cell><span>Win rate</span><span>"+(s.wr*100).toFixed(1)+"%</span></div>"+
        "<div class=cell><span>Profit factor</span><span>"+s.pf.toFixed(2)+"</span></div>"+
        "<div class=cell><span>Trades</span><span>"+s.n+"</span></div>"+
        "<div class=cell><span>Max DD</span><span class=dn>"+(s.dd*100).toFixed(2)+"%</span></div>"+
        "<div class=cell><span>End equity</span><span>"+fmt(s.end)+" / 10,000</span></div>"+
        "<div class=icat>LAST FILLS</div>"+lastTest.trades.slice(-8).reverse().map(function(t){ return "<div class=cell><span class="+(t.side==="buy"?"up":"dn")+">"+t.side+"</span><span>"+fmt(t.px)+(t.pnl!=null?" · "+(t.pnl*100).toFixed(2)+"%":"")+"</span></div>"; }).join("")):"<div class=cell>Pick a strategy</div>");
    el.querySelectorAll("[data-st]").forEach(function(b){ b.onclick=function(){ lastTest=runStrat(b.dataset.st, lastBars); renderTest(); if(lastBars.length) paint(lastBars); toast("Backtest "+b.dataset.st+" · "+(lastTest&&lastTest.stats? (lastTest.stats.ret*100).toFixed(1)+"%":"—")); }; });
  }
  function renderScreen(){
    var el=document.getElementById("screen"); if(!el) return;
    var L=lists.find(function(x){return x.id===listId;})||{symbols:[],name:""};
    var rows=(L.symbols||[]).map(function(s){
      var q=quotes[s]||quotes[bare(s)]||{};
      return {s:s, last:q.last, chg:q.chg, rsi:q.rsi, vs50:q.vs50, vs200:q.vs200};
    });
    if(screenFilt==="up") rows=rows.filter(function(r){ return r.chg>0; });
    if(screenFilt==="dn") rows=rows.filter(function(r){ return r.chg<0; });
    if(screenFilt==="os") rows=rows.filter(function(r){ return r.rsi!=null && r.rsi<30; });
    if(screenFilt==="ob") rows=rows.filter(function(r){ return r.rsi!=null && r.rsi>70; });
    if(screenFilt==="a200") rows=rows.filter(function(r){ return r.vs200!=null && r.vs200>0; });
    if(screenFilt==="b200") rows=rows.filter(function(r){ return r.vs200!=null && r.vs200<0; });
    rows.sort(function(a,b){ return (b.chg||0)-(a.chg||0); });
    el.innerHTML="<b>SCREENER · "+(L.name||"")+"</b><div class=sbar>"+[["","All"],["up","Up"],["dn","Down"],["os","RSI<30"],["ob","RSI>70"],["a200",">200"],["b200","<200"]].map(function(x){ return "<button class='"+(screenFilt===x[0]?"on":"")+"' data-f='"+x[0]+"'>"+x[1]+"</button>"; }).join("")+"</div>"+
      "<div class=srow style=color:var(--mut)><span>Sym</span><span>Last</span><span>Chg%</span><span>RSI</span><span>vs200</span></div>"+
      rows.slice(0,80).map(function(r){ var up=r.chg>=0; return "<button class=srow data-s='"+r.s+"'><span>"+r.s+"</span><span>"+(r.last!=null?fmt(r.last):"—")+"</span><span class="+(up?"up":"dn")+">"+(r.chg!=null?((r.chg>=0?"+":"")+(r.chg*100).toFixed(2)+"%"):"—")+"</span><span>"+(r.rsi!=null?r.rsi.toFixed(0):"—")+"</span><span class="+(r.vs200>=0?"up":"dn")+">"+(r.vs200!=null?r.vs200.toFixed(1)+"%":"—")+"</span></button>"; }).join("")||"<div class=cell>No rows</div>";
    el.querySelectorAll("[data-f]").forEach(function(b){ b.onclick=function(){ screenFilt=b.dataset.f; renderScreen(); }; });
    el.querySelectorAll("[data-s]").forEach(function(b){ b.onclick=function(){ var s=b.dataset.s; if(TABS.indexOf(bare(s))<0) TABS.push(bare(s)); active=bare(s); loadDraw(); renderTabs(); load(); }; });
    (L.symbols||[]).slice(0,24).forEach(function(s){
      if(quotes[s] && quotes[s].rsi!=null) return;
      klines(s,"1d").then(function(d){
        if(!d||d.length<30) return;
        var last=d[d.length-1], prev=d[d.length-2]||last;
        var q=quotes[s]||{}; q.last=last.close; q.chg=prev.close?(last.close-prev.close)/prev.close:0; q.rsi=lastOsc(rsi(d,14));
        var s50=lastOsc(sma(d,50)), s200=lastOsc(sma(d,200));
        q.vs50=s50?(last.close/s50-1)*100:null; q.vs200=s200?(last.close/s200-1)*100:null;
        quotes[s]=q; quotes[bare(s)]=q; if(wtab==="screen") renderScreen();
      });
    });
  }
  function renderOver(d){
    var el=document.getElementById("over"); if(!el||!d||!d.length) return;
    var last=d[d.length-1], now=last.time;
    function ret(days){ var t0=now-days*86400, b=d[0]; for(var i=0;i<d.length;i++) if(d[i].time>=t0){ b=d[i]; break; } return b.close?(last.close/b.close-1)*100:null; }
    var r1=ret(21), r3=ret(63), r6=ret(126), r12=ret(252);
    var s20=lastOsc(sma(d,20)), s50=lastOsc(sma(d,50)), s200=lastOsc(sma(d,200));
    var atrv=lastOsc(atr(d,14)), rsi14=lastOsc(rsi(d,14));
    var dd=maxdd(d.filter(function(b){ return b.time>now-365*86400; }));
    var streak=0, i; for(i=d.length-1;i>0;i--){ var up=d[i].close>=d[i-1].close; if(i===d.length-1) streak=up?1:-1; else if((streak>0 && up)||(streak<0 && !up)) streak+=streak>0?1:-1; else break; }
    el.innerHTML="<b>OVERVIEW · "+active+"</b>"+
      "<div class=cell><span>1M</span><span class="+(r1>=0?"up":"dn")+">"+(r1!=null?r1.toFixed(2)+"%":"—")+"</span></div>"+
      "<div class=cell><span>3M</span><span class="+(r3>=0?"up":"dn")+">"+(r3!=null?r3.toFixed(2)+"%":"—")+"</span></div>"+
      "<div class=cell><span>6M</span><span class="+(r6>=0?"up":"dn")+">"+(r6!=null?r6.toFixed(2)+"%":"—")+"</span></div>"+
      "<div class=cell><span>1Y</span><span class="+(r12>=0?"up":"dn")+">"+(r12!=null?r12.toFixed(2)+"%":"—")+"</span></div>"+
      "<div class=cell><span>vs SMA20</span><span>"+(s20?((last.close/s20-1)*100).toFixed(2)+"%":"—")+"</span></div>"+
      "<div class=cell><span>vs SMA50</span><span>"+(s50?((last.close/s50-1)*100).toFixed(2)+"%":"—")+"</span></div>"+
      "<div class=cell><span>vs SMA200</span><span>"+(s200?((last.close/s200-1)*100).toFixed(2)+"%":"—")+"</span></div>"+
      "<div class=cell><span>ATR %</span><span>"+(atrv&&last.close?(100*atrv/last.close).toFixed(2)+"%":"—")+"</span></div>"+
      "<div class=cell><span>RSI 14</span><span>"+(rsi14!=null?rsi14.toFixed(1):"—")+"</span></div>"+
      "<div class=cell><span>1Y max DD</span><span class=dn>"+(dd*100).toFixed(2)+"%</span></div>"+
      "<div class=cell><span>Streak</span><span>"+streak+" bars</span></div>"+
      "<div class=cell><span>Bars</span><span>"+d.length+" · "+lastSource+"</span></div>";
  }
  function renderSeason(d){
    var el=document.getElementById("season"); if(!el||!d||d.length<40) { if(el) el.innerHTML="<b>SEASONALITY</b><div class=cell>Need more bars</div>"; return; }
    var buckets={}, i, names=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    for(i=1;i<d.length;i++){ var m=new Date(d[i].time*1000).getUTCMonth(); var ret=d[i-1].close?(d[i].close/d[i-1].close-1):0; (buckets[m]=buckets[m]||[]).push(ret); }
    var cells="";
    for(i=0;i<12;i++){
      var a=buckets[i]||[], avg=a.length?a.reduce(function(s,x){return s+x;},0)/a.length:0, wr=a.length?a.filter(function(x){return x>0;}).length/a.length:0;
      var bg=avg>=0?"rgba(8,153,129,"+Math.min(0.55,0.12+Math.abs(avg)*8)+")":"rgba(242,54,69,"+Math.min(0.55,0.12+Math.abs(avg)*8)+")";
      cells+="<div class=scell style='background:"+bg+"'><div>"+names[i]+"</div><div>"+(avg*100).toFixed(2)+"%</div><div style=font-size:9px;opacity:.85>"+(wr*100).toFixed(0)+"% wr</div></div>";
    }
    el.innerHTML="<b>SEASONALITY · "+active+"</b><div class=season>"+cells+"</div>";
  }
  async function renderCorr(){
    var el=document.getElementById("corr"); if(!el) return;
    var syms=TABS.slice(0,8); el.innerHTML="<b>CORR · open tabs</b><div class=cell>Computing…</div>";
    var series=[], i, j;
    for(i=0;i<syms.length;i++){ try{ var d=await klines(syms[i], tf==="1d"?tf:"1d"); series.push({s:syms[i], r:rets(d).slice(-120)}); }catch(e){ series.push({s:syms[i], r:[]}); } }
    var html="<b>CORR · "+tf+"</b><div class=corm style='grid-template-columns:64px repeat("+syms.length+",1fr)'><span></span>"+syms.map(function(s){return "<span>"+bare(s).slice(0,5)+"</span>";}).join("");
    for(i=0;i<series.length;i++){
      html+="<span>"+bare(series[i].s).slice(0,6)+"</span>";
      for(j=0;j<series.length;j++){
        var c=i===j?1:pearson(series[i].r, series[j].r);
        var bg=c==null?"var(--chip)": c>=0?"rgba(8,153,129,"+Math.min(0.7,Math.abs(c))+")":"rgba(242,54,69,"+Math.min(0.7,Math.abs(c))+")";
        html+="<span style='background:"+bg+";text-align:center;padding:4px;color:#fff'>"+(c==null?"—":c.toFixed(2))+"</span>";
      }
    }
    html+="</div>"; el.innerHTML=html;
  }
  function openKeys(){
    var m=document.getElementById("modal"), box=document.getElementById("mbox");
    box.innerHTML="<h3>Hotkeys</h3><div class=kgrid>"+
      [["Ctrl/Cmd+K","Command palette"],[" / ","Symbol search"],["Click ticker","Open search"],["Type ticker","Quick open"],["Alt+T / H / F / V / R","Trend / H-line / Fib / Vert / Rect"],["Alt+E","Eraser"],["Ctrl+Z / Y","Undo / Redo"],["Ctrl+S","Snapshot"],["Ctrl+I","Indicators"],["Ctrl+C / V","Copy / paste drawing"],["Delete","Remove selected"],["Space","Replay"],["+ / − / 0","Zoom in / out / fit"],["Alt+1 / 2 / 4","Layouts"],["Shift+click","Measure"],["Shift+legend","Edit MA period"],["Esc","Cursor / close"],["?","This sheet"]].map(function(r){ return "<span>"+r[0]+"</span><span>"+r[1]+"</span>"; }).join("")+
      "</div><div style='margin-top:12px;text-align:right'><button id=kok>Close</button></div>";
    m.className="on"; document.getElementById("kok").onclick=function(){ m.className=""; };
  }
  function renderDock(){
    var tabs=document.getElementById("dtabs"), body=document.getElementById("dockbody"), dock=document.getElementById("dock");
    if(!tabs) return;
    var items=[["","—"],["screen","Screener"],["trade","Paper"],["test","Strategy"],["over","Overview"],["fin","Financials"],["notes","Notes"],["season","Season"],["corr","Corr"],["keys","Hotkeys"]];
    tabs.innerHTML=items.map(function(t){ return "<button class='"+(dockTab===t[0]?"on":"")+"' data-d='"+t[0]+"'>"+t[1]+"</button>"; }).join("");
    dock.className=dockTab?"on":"";
    tabs.querySelectorAll("[data-d]").forEach(function(b){ b.onclick=function(){
      var id=b.dataset.d;
      if(id==="keys"){ openKeys(); return; }
      dockTab=dockTab===id?"":id;
      renderDock();
      if(!dockTab) return;
      if(dockTab==="screen"){ wtab="screen"; renderWtabs(); renderScreen(); body.innerHTML=document.getElementById("screen").innerHTML; }
      else if(dockTab==="trade"){ wtab="trade"; renderWtabs(); renderTrade(); body.innerHTML=document.getElementById("trade").innerHTML; }
      else if(dockTab==="test"){ wtab="test"; renderWtabs(); renderTest(); body.innerHTML=document.getElementById("test").innerHTML; }
      else if(dockTab==="over"&&lastBars.length){ renderOver(lastBars); body.innerHTML=document.getElementById("over").innerHTML; }
      else if(dockTab==="fin"){ wsub="data"; wtab="fin"; renderWtabs(); renderFin(); body.innerHTML="<b>FINANCIALS</b><div class=cell>Opened in the right Data pane</div>"; }
      else if(dockTab==="notes"){ wsub="info"; wtab="notes"; renderWtabs(); renderNotes(); body.innerHTML="<b>NOTES</b><div class=cell>Opened in the right Info pane</div>"; }
      else if(dockTab==="season"&&lastBars.length){ renderSeason(lastBars); body.innerHTML=document.getElementById("season").innerHTML; }
      else if(dockTab==="corr"){ renderCorr(); body.innerHTML="<b>CORR</b><div class=cell>See right panel Corr tab for the matrix</div>"; wtab="corr"; renderWtabs(); renderCorr(); }
    }; });
  }
  function renderDwin(param){
    var el=document.getElementById("dwin"); if(!el) return;
    if(!dwinOn){ el.className=""; return; }
    el.className="on";
    if(!param||!param.time||!mainSeries){ return; }
    var d=param.seriesData.get(mainSeries);
    var o=d&&(d.open!=null?d.open:d.value), h=d&&(d.high!=null?d.high:d.value), l=d&&(d.low!=null?d.low:d.value), c=d&&(d.close!=null?d.close:d.value);
    var t=param.time;
    var rows=[["Time", new Date((t+tzOff*3600)*1000).toISOString().slice(0,16).replace("T"," ")],["Open",fmt(o)],["High",fmt(h)],["Low",fmt(l)],["Close",fmt(c)]];
    INDS.forEach(function(ind){ if(!ind.on) return; var v=valAt(overlayMap[ind.id], t); if(v!=null) rows.push([ind.n, fmt(v)]); });
    var bar=null; for(var i=0;i<lastBars.length;i++) if(lastBars[i].time===t) bar=lastBars[i];
    if(bar) rows.push(["Volume", fmtVol(bar.volume)]);
    el.innerHTML="<b>DATA WINDOW</b>"+rows.map(function(r){ return "<div class=cell><span>"+r[0]+"</span><span>"+r[1]+"</span></div>"; }).join("");
  }
  async function tickLive(){
    if(!liveOn || replay.on) return;
    try{
      var key=resolveSym(active).ticker+"|"+tf;
      if(barCache[key]) delete barCache[key];
      var d=await klines(active, tf);
      if(!d.length) return;
      var a=lastBars[lastBars.length-1], b=d[d.length-1];
      if(!a || a.time!==b.time || a.close!==b.close){ await paint(d); }
      loadTape(false);
    }catch(e){}
  }

  function clock(){
    var el=document.getElementById("clock");
    if(el){ var d=new Date(Date.now()+tzOff*3600*1000); el.textContent=d.toISOString().slice(11,19)+" "+tzName; }
    if(lastBars.length) countdown(lastBars[lastBars.length-1]);
  }
  loadAlerts();
  notes=loadJSON(NOTE_KEY,{}); flags=loadJSON(FLAG_KEY,{}); favs=loadJSON(FAV_KEY,[]); if(!Array.isArray(favs)) favs=[];
  TZS[1][1]=nyOffset();
  loadDraw();
  if(window.innerWidth<720){ watchOpen=false; var w=document.getElementById("watch"); if(w) w.className="watch hide"; }
  applyTheme(false);
  renderTabs(); renderTf(); renderRail(); renderLetters(); renderWtabs(); renderLegend(); renderDock(); renderQR();
  var dwin=document.getElementById("dwin"); if(dwin) dwin.className=dwinOn?"on":"";
  var mn=document.getElementById("mini"); if(mn) mn.className=miniOn?"on":"";
  var lp=document.getElementById("livepill"); if(lp) lp.className=liveOn?"on":"";
  var fab=document.getElementById("listfab");
  if(fab) fab.onclick=function(){ watchOpen=!watchOpen; document.getElementById("watch").className="watch"+(watchOpen?"":" hide"); fab.textContent=watchOpen?"Chart":"List"; };
  var hk=document.getElementById("btn-keys"); if(hk) hk.onclick=openKeys;
  if(layout>1) setLayout(layout);
  loadLists().then(function(){ renderList(); renderDetail(); });
  loadIntel(); loadNews();
  TABS.forEach(function(s){ lastPx(s).then(function(px){ if(px){ quotes[s]=px; renderTabs(); } }); });
  load();
  clock(); setInterval(clock,1000);
  liveT=setInterval(tickLive, 15000);
  tapeT=setInterval(function(){ if(liveOn && !replay.on && /USDT$|BUSD$|USDC$/.test(resolveSym(active).ticker)) loadTape(false); }, 4000);
  if(window.Notification && Notification.permission==="default") Notification.requestPermission().catch(function(){});
})();
