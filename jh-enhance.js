/* jh-enhance.js — JustHodl shared, config-driven chart component.
 * Drop ONE line into any page:
 *   <script src="/jh-enhance.js"
 *           data-feed="data/carry-surface.json"
 *           data-bars="all_assets:symbol:carry_pct"
 *           data-title="Carry by asset (%)" data-sub="…" data-max="20"></script>
 * or for a time-series:
 *   <script ... data-line="khalid_index.signals" data-title="Khalid Index" data-crisis="1"></script>
 *
 * data-bars   = "arrayPath:labelField:valueField"  (dot-paths supported)
 * data-line   = "seriesPath"   (series is [[date,num]] or [{date..,num..}])
 * data-color  = hex (default #eab308)   data-max = N bars (default 20)
 * data-crisis = "1" to shade historical crisis windows on line charts
 * Renders a dark card inserted right after the page's first <h1> (else top of body).
 * No dependencies; safe to include anywhere. */
(function () {
  var ME = document.currentScript;
  var FEED = ME.getAttribute("data-feed");
  if (!FEED) return;
  var BARS = ME.getAttribute("data-bars");
  var LINE = ME.getAttribute("data-line");
  var TITLE = ME.getAttribute("data-title") || "";
  var SUB = ME.getAttribute("data-sub") || "";
  var COLOR = ME.getAttribute("data-color") || "#eab308";
  var MAXN = parseInt(ME.getAttribute("data-max") || "20", 10);
  var CRISIS = ME.getAttribute("data-crisis") === "1";
  var METRICS = ME.getAttribute("data-metrics");
  var PX = "https://justhodl-data-proxy.raafouis.workers.dev";
  var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
  var CRISES = [["1997-07","1998-10","Asian/LTCM"],["2000-03","2002-10","Dot-com"],["2007-08","2009-06","GFC"],["2011-07","2012-07","Euro debt"],["2015-08","2016-02","China/oil"],["2020-02","2020-05","COVID"],["2022-01","2022-10","Rate shock"],["2023-03","2023-05","SVB"]];

  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
  function dig(o, path){ if(!o||!path) return undefined; return path.split(".").reduce(function(a,k){return a==null?undefined:a[k];}, o); }
  function num(v){ var n=typeof v==="number"?v:parseFloat(String(v).replace(/[, %$]/g,"")); return isFinite(n)?n:null; }
  function fmt(v){ var a=Math.abs(v); return a>=1e9?(v/1e9).toFixed(2)+"B":a>=1e6?(v/1e6).toFixed(2)+"M":a>=1e3?(v/1e3).toFixed(1)+"k":a>=1?(Math.round(v*100)/100).toString():(Math.round(v*1000)/1000).toString(); }

  async function getJSON(p){
    var u=[ "https://justhodl.ai/"+p, PX+"/"+p, S3+"/"+p ];
    for(var i=0;i<u.length;i++){ try{ var r=await fetch(u[i]+(u[i].indexOf("?")<0?"?_=":"&_=")+Date.now()); if(r.ok) return await r.json(); }catch(e){} }
    return null;
  }

  function card(){
    var c=document.createElement("section");
    c.id="jhviz";
    c.style.cssText="max-width:1400px;margin:14px auto;padding:14px 16px;background:#121212;border:1px solid #2a2a2a;border-radius:12px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;color:#e5e5e5;position:relative";
    c.innerHTML='<div style="font-weight:600;font-size:14px;color:#e5e5e5">'+esc(TITLE)+'</div>'+(SUB?'<div style="font-family:ui-monospace,Menlo,monospace;font-size:10.5px;color:#777;margin:2px 0 8px">'+esc(SUB)+'</div>':'<div style="margin-bottom:6px"></div>')+'<div id="jhviz-body"></div>';
    return c;
  }
  function mount(c){
    var h=document.querySelector("h1");
    if(h){ var host=h.closest("header,section,div")||h; if(host.parentNode){ host.parentNode.insertBefore(c, host.nextSibling); return; } }
    document.body.insertBefore(c, document.body.firstChild);
  }

  function renderBars(body, items, keepOrder){
    items=items.filter(function(x){return x.v!=null;});
    if(!keepOrder){ items.sort(function(a,b){return Math.abs(b.v)-Math.abs(a.v);}); items=items.slice(0,MAXN); }
    if(!items.length){ body.innerHTML='<div style="color:#777;font-size:11px">No data.</div>'; return; }
    var mx=Math.max.apply(null, items.map(function(i){return Math.abs(i.v);}))||1;
    var anyNeg=items.some(function(i){return i.v<0;});
    var html='<div style="display:flex;flex-direction:column;gap:4px">';
    items.forEach(function(it){
      var w=(Math.abs(it.v)/mx*100).toFixed(1);
      var col=it.v<0?"#ef4444":COLOR;
      html+='<div style="display:flex;align-items:center;gap:10px;font-family:ui-monospace,Menlo,monospace;font-size:11.5px">'
        +'<div style="width:150px;text-align:right;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:#cfcfcf">'+esc(it.label)+'</div>'
        +'<div style="flex:1;background:#0b0b0b;border-radius:4px;overflow:hidden;'+(anyNeg?'display:flex;justify-content:'+(it.v<0?'flex-end':'flex-start'):'')+'"><div style="width:'+w+'%;height:17px;background:linear-gradient(90deg,'+col+'55,'+col+')"></div></div>'
        +'<div style="width:74px;text-align:right;color:#e5e5e5">'+fmt(it.v)+'</div></div>';
    });
    html+='</div><div style="font-family:ui-monospace,Menlo,monospace;font-size:9px;color:#666;margin-top:6px;text-align:right">'+(keepOrder?'live data':'top '+items.length+' by magnitude · live data')+'</div>';
    body.innerHTML=html;
  }

  function renderLine(body, pts){
    // pts: [[date,val]] ascending
    var W=1320,H=420,PL=70,PR=22,PT=24,PB=40,w=W-PL-PR,h=H-PT-PB;
    var vs=pts.map(function(p){return p[1];}); var mn=Math.min.apply(null,vs),mx=Math.max.apply(null,vs); if(mn===mx){mn-=1;mx+=1;} var rg=mx-mn,n=pts.length;
    var X=function(i){return PL+i/(n-1)*w;}, Y=function(v){return PT+h-(v-mn)/rg*h;};
    var line=pts.map(function(p,i){return X(i).toFixed(1)+","+Y(p[1]).toFixed(1);}).join(" ");
    var area=PL+","+(PT+h).toFixed(1)+" "+line+" "+(PL+w).toFixed(1)+","+(PT+h).toFixed(1);
    var sh="";
    if(CRISIS){ CRISES.forEach(function(c){ var ia=pts.findIndex(function(p){return String(p[0]).slice(0,7)>=c[0];}); if(ia<0)return; var ib=pts.findIndex(function(p){return String(p[0]).slice(0,7)>=c[1];}); if(ib<0)ib=n-1; var xa=X(ia),xb=X(ib),bw=Math.max(2,xb-xa); sh+='<rect x="'+xa.toFixed(1)+'" y="'+PT+'" width="'+bw.toFixed(1)+'" height="'+h+'" fill="rgba(239,68,68,0.12)"/><text x="'+(xa+bw/2).toFixed(1)+'" y="'+(PT-5)+'" fill="#f87171" font-size="9" font-weight="700" text-anchor="middle">'+c[2]+'</text>'; }); }
    var xt="",ys={}; pts.forEach(function(p,i){var y=String(p[0]).slice(0,4); if(!(y in ys))ys[y]=i;}); var yl=Object.keys(ys),st=Math.max(1,Math.ceil(yl.length/14));
    yl.forEach(function(y,k){ if(k%st===0){ var px=X(ys[y]); xt+='<line x1="'+px+'" y1="'+PT+'" x2="'+px+'" y2="'+(PT+h)+'" stroke="#1b1b1b"/><text x="'+px+'" y="'+(H-12)+'" fill="#888" font-size="11" text-anchor="middle">'+y+'</text>'; } });
    var yt=""; for(var k=0;k<=5;k++){ var v=mn+rg*k/5,py=Y(v); yt+='<line x1="'+PL+'" y1="'+py.toFixed(1)+'" x2="'+(PL+w)+'" y2="'+py.toFixed(1)+'" stroke="#171717"/><text x="'+(PL-8)+'" y="'+(py+4).toFixed(1)+'" fill="#888" font-size="11" text-anchor="end">'+fmt(v)+'</text>'; }
    body.innerHTML='<svg id="jhsvg" width="100%" viewBox="0 0 '+W+' '+H+'" style="display:block;width:100%;cursor:crosshair"><rect x="'+PL+'" y="'+PT+'" width="'+w+'" height="'+h+'" fill="#0b0b0b"/>'+sh+yt+xt+'<polygon points="'+area+'" fill="'+COLOR+'" fill-opacity="0.09"/><polyline points="'+line+'" fill="none" stroke="'+COLOR+'" stroke-width="1.8"/><line id="jhcx" y1="'+PT+'" y2="'+(PT+h)+'" stroke="'+COLOR+'" stroke-dasharray="3,3" style="display:none"/><circle id="jhdot" r="4" fill="'+COLOR+'" style="display:none"/><rect id="jhov" x="'+PL+'" y="'+PT+'" width="'+w+'" height="'+h+'" fill="transparent"/></svg><div id="jhtip" style="position:absolute;display:none;background:#1c1c1c;border:1px solid '+COLOR+';border-radius:6px;padding:5px 9px;font-family:ui-monospace,Menlo,monospace;font-size:11px;color:#eee;pointer-events:none;white-space:nowrap;z-index:5"></div>';
    var svg=document.getElementById("jhsvg"), ov=document.getElementById("jhov");
    ov.addEventListener("mousemove",function(ev){ var r=svg.getBoundingClientRect(),ratio=W/r.width,sx=(ev.clientX-r.left)*ratio; var i=Math.round((sx-PL)/w*(n-1)); i=Math.max(0,Math.min(n-1,i)); var p=pts[i],px=X(i),py=Y(p[1]); var cx=document.getElementById("jhcx"),dot=document.getElementById("jhdot"),tip=document.getElementById("jhtip"); cx.setAttribute("x1",px);cx.setAttribute("x2",px);cx.style.display="block"; dot.setAttribute("cx",px);dot.setAttribute("cy",py);dot.style.display="block"; var wrap=body; tip.style.display="block"; tip.innerHTML="<b>"+esc(p[0])+"</b> &nbsp; "+fmt(p[1]); tip.style.left=Math.min(wrap.clientWidth-150,Math.max(0,(px/W)*wrap.clientWidth+10))+"px"; tip.style.top=Math.max(0,(py/H)*wrap.clientHeight-10)+"px"; });
    svg.addEventListener("mouseleave",function(){ ["jhcx","jhdot","jhtip"].forEach(function(x){var e=document.getElementById(x);if(e)e.style.display="none";}); });
  }

  function toSeries(raw){
    if(!Array.isArray(raw)||!raw.length) return null;
    if(Array.isArray(raw[0])) return raw.filter(function(p){return p&&p.length>=2&&num(p[1])!=null;}).map(function(p){return [p[0],num(p[1])];});
    // array of objects: find date-ish key + first numeric key
    var e=raw[0]; var dk=["date","Date","asofdate","t","time","period","x"].find(function(k){return k in e;}); if(!dk) dk=Object.keys(e)[0];
    var vk=Object.keys(e).find(function(k){return k!==dk&&num(e[k])!=null;}); if(!vk) return null;
    return raw.map(function(o){return [o[dk], num(o[vk])];}).filter(function(p){return p[1]!=null;});
  }

  function run(data){
    if(!data){ return; }
    var c=card(); mount(c); var body=document.getElementById("jhviz-body");
    try{
      if(LINE){ var pts=toSeries(dig(data,LINE)); if(pts&&pts.length>1){ renderLine(body,pts); return; } body.innerHTML='<div style="color:#777;font-size:11px">No time-series available.</div>'; return; }
      if(METRICS){ var mi=METRICS.split("|").map(function(seg){ var i=seg.indexOf(":"); return {label:seg.slice(i+1), v:num(dig(data,seg.slice(0,i)))}; }); renderBars(body, mi, true); return; }
      if(BARS){ var sp=BARS.split(":"); var arr=dig(data,sp[0]);
      /* DICT-BARS (ops 3524): object-of-numbers -> entries */
      if(arr&&!Array.isArray(arr)&&typeof arr==='object'){var _e=Object.keys(arr).filter(function(k){return typeof arr[k]==='number';}).map(function(k){var o={};o.__k=k;o.__v=arr[k];return o;});if(_e.length>=3){arr=_e;sp[1]=sp[1]||'__k';sp[2]=sp[2]||'__v';}}
      if(!Array.isArray(arr)){ body.innerHTML='<div style="color:#777;font-size:11px">No data.</div>'; return; }
        var items=arr.map(function(o){ return {label:String(dig(o,sp[1])!=null?dig(o,sp[1]):o[sp[1]]), v:num(dig(o,sp[2]))}; });
        renderBars(body,items); return; }
    }catch(e){ body.innerHTML='<div style="color:#777;font-size:11px">chart error</div>'; }
  }

  (async function(){ var d=await getJSON(FEED); run(d); })();
})();
