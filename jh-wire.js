/* jh-wire.js — JustHodl shared LIVE ENGINE FEEDS component.
 * Purpose: surface real engine feeds on the page they belong to, with one line:
 *   <script src="/jh-wire.js" defer
 *     data-feeds="data/fed-nlp.json|justhodl-fed-nlp|FED SPEECH NLP;data/fedwatch.json|justhodl-fedwatch-rate-probability|FEDWATCH RATE PROBABILITIES"></script>
 * Format: FEED|ENGINE|TITLE entries separated by ';'. The full feed path lives in
 * the page source on purpose — the engine-directory audit (exact containment)
 * sees it and flips the engine to WIRED. 100% real data: every card renders only
 * what the feed actually contains, shows the feed's own timestamp, and states
 * failure honestly. No demo values, ever.
 */
(function () {
  var ME = document.currentScript; if (!ME) return;
  var SPEC = ME.getAttribute("data-feeds"); if (!SPEC) return;
  var PX = "https://justhodl-data-proxy.raafouis.workers.dev";
  var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";

  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
  function fmt(v){var n=typeof v==="number"?v:NaN;if(!isFinite(n))return esc(String(v));var a=Math.abs(n);
    return a>=1e12?(n/1e12).toFixed(2)+"T":a>=1e9?(n/1e9).toFixed(2)+"B":a>=1e6?(n/1e6).toFixed(2)+"M":a>=1e3?(n/1e3).toFixed(1)+"k":a>=100?n.toFixed(1):(Math.round(n*10000)/10000).toString();}
  async function getJSON(p){
    var u=["/"+p,"https://justhodl.ai/"+p,PX+"/"+p,S3+"/"+p];
    for(var i=0;i<u.length;i++){try{var r=await fetch(u[i]+(u[i].indexOf("?")<0?"?_=":"&_=")+Date.now());if(r.ok)return await r.json();}catch(e){}}
    return null;
  }
  var TSF=["ts","as_of","asof","updated","updated_at","last_updated","timestamp","generated_at","date","run_ts"];
  function tsOf(o){if(!o||typeof o!=="object")return null;for(var i=0;i<TSF.length;i++){var v=o[TSF[i]];
    if(typeof v==="number"&&v>1e9)return new Date(v>1e12?v:v*1000);
    if(typeof v==="string"&&v.length>=8){var d=new Date(v);if(!isNaN(d))return d;}}return null;}
  function ageBadge(d){if(!d)return '<span class="jw-b jw-na">ts n/a</span>';
    var h=(Date.now()-d.getTime())/36e5,t=h<1?Math.round(h*60)+"m":h<48?h.toFixed(1)+"h":Math.round(h/24)+"d";
    var c=h<=26?"jw-ok":h<=80?"jw-warn":"jw-bad";return '<span class="jw-b '+c+'">'+t+' old</span>';}
  function firstArray(o,depth){if(depth>2||o==null)return null;
    if(Array.isArray(o))return o.length&&typeof o[0]==="object"&&!Array.isArray(o[0])?o:null;
    if(typeof o!=="object")return null;
    var ks=Object.keys(o);for(var i=0;i<ks.length;i++){var r=firstArray(o[ks[i]],depth+1);if(r)return r;}return null;}
  function scalars(o){var out=[];if(!o||typeof o!=="object"||Array.isArray(o))return out;
    var ks=Object.keys(o);for(var i=0;i<ks.length&&out.length<8;i++){var k=ks[i],v=o[k];
      if(TSF.indexOf(k)>=0)continue;
      if(typeof v==="number")out.push([k,fmt(v)]);
      else if(typeof v==="string"&&v.length<=40&&v.length>0)out.push([k,esc(v)]);
      else if(typeof v==="boolean")out.push([k,String(v)]);}
    return out;}
  function tableOf(arr){var rows=arr.slice(0,8),cols=[],c0=rows[0];
    var ks=Object.keys(c0);for(var i=0;i<ks.length&&cols.length<5;i++){var k=ks[i],v=c0[k];
      if(typeof v==="number"||(typeof v==="string"&&v.length<=28))cols.push(k);}
    if(!cols.length)return "";
    var h="<tr>"+cols.map(function(c){return "<th>"+esc(c)+"</th>";}).join("")+"</tr>";
    var b=rows.map(function(r){return "<tr>"+cols.map(function(c){var v=r[c];
      return "<td>"+(typeof v==="number"?fmt(v):esc(String(v==null?"":v)).slice(0,30))+"</td>";}).join("")+"</tr>";}).join("");
    return '<table class="jw-t">'+h+b+"</table><div class='jw-more'>"+(arr.length>8?("+"+(arr.length-8)+" more rows in feed"):"")+"</div>";}
  function summarize(o){if(o==null)return"empty feed";
    if(Array.isArray(o))return"array · "+o.length+" items";
    if(typeof o==="object")return"object · "+Object.keys(o).length+" keys";return esc(String(o));}

  var css='#jh-wire{margin:26px auto;max-width:1180px;padding:0 14px;font-family:var(--jh-mono,ui-monospace,Menlo,monospace)}'+
  '#jh-wire .jw-h{display:flex;align-items:baseline;gap:10px;border-bottom:1px solid var(--jh-line,#2a2416);padding-bottom:6px;margin-bottom:12px}'+
  '#jh-wire .jw-h b{color:var(--jh-amber,#eab308);font-size:12px;letter-spacing:2px}'+
  '#jh-wire .jw-h span{color:var(--jh-dim,#8a8064);font-size:10px}'+
  '#jh-wire .jw-g{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:12px}'+
  '.jw-c{background:var(--jh-panel,#12100a);border:1px solid var(--jh-line,#2a2416);border-radius:6px;padding:10px 12px;min-width:0}'+
  '.jw-c h4{margin:0 0 2px;font-size:11px;letter-spacing:1px;color:var(--jh-ink,#e8e0c8);font-weight:600}'+
  '.jw-c .jw-e{font-size:9px;color:var(--jh-dim,#8a8064);margin-bottom:6px;word-break:break-all}'+
  '.jw-b{font-size:9px;padding:1px 6px;border-radius:3px;border:1px solid var(--jh-line,#2a2416);margin-left:6px;vertical-align:1px}'+
  '.jw-ok{color:var(--jh-green,#4ade80)}.jw-warn{color:var(--jh-amber,#eab308)}.jw-bad{color:var(--jh-red,#f87171)}.jw-na{color:var(--jh-dim,#8a8064)}'+
  '.jw-chips{display:flex;flex-wrap:wrap;gap:6px;margin:4px 0}'+
  '.jw-chip{background:rgba(234,179,8,.06);border:1px solid var(--jh-line,#2a2416);border-radius:4px;padding:2px 7px;font-size:10px;color:var(--jh-ink,#e8e0c8)}'+
  '.jw-chip i{font-style:normal;color:var(--jh-dim,#8a8064);margin-right:5px;font-size:9px}'+
  '.jw-t{width:100%;border-collapse:collapse;margin-top:6px;font-size:10px}'+
  '.jw-t th{text-align:left;color:var(--jh-dim,#8a8064);font-weight:400;border-bottom:1px solid var(--jh-line,#2a2416);padding:2px 6px 2px 0}'+
  '.jw-t td{color:var(--jh-ink,#e8e0c8);padding:2px 6px 2px 0;border-bottom:1px dotted rgba(138,128,100,.18);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:140px}'+
  '.jw-more{font-size:9px;color:var(--jh-dim,#8a8064);margin-top:3px}'+
  '.jw-err{color:var(--jh-red,#f87171);font-size:10px}.jw-sum{color:var(--jh-dim,#8a8064);font-size:10px}';

  var entries=SPEC.split(";").map(function(s){var p=s.split("|");return{feed:(p[0]||"").trim(),eng:(p[1]||"").trim(),title:(p[2]||p[0]||"").trim()};}).filter(function(e){return e.feed;});
  if(!entries.length)return;

  var sec=document.createElement("section");sec.id="jh-wire";
  sec.innerHTML='<style>'+css+'</style><div class="jw-h"><b>LIVE ENGINE FEEDS</b><span>'+entries.length+' engine feed'+(entries.length>1?"s":"")+' wired to this desk · real data, direct from the fleet</span></div><div class="jw-g"></div>';
  ME.parentNode.insertBefore(sec,ME);
  var grid=sec.querySelector(".jw-g");

  entries.forEach(function(en){
    var c=document.createElement("div");c.className="jw-c";c.setAttribute("data-feed",en.feed);
    c.innerHTML='<h4>'+esc(en.title)+'</h4><div class="jw-e">'+esc(en.eng)+' → '+esc(en.feed)+'</div><div class="jw-body jw-sum">loading…</div>';
    grid.appendChild(c);
    getJSON(en.feed).then(function(d){
      var body=c.querySelector(".jw-body");
      if(d==null){body.innerHTML='<span class="jw-err">feed unreachable right now — engine output at '+esc(en.feed)+' did not load</span>';return;}
      var ts=tsOf(d)||(Array.isArray(d)?tsOf(d[0]):null);
      c.querySelector("h4").insertAdjacentHTML("beforeend",ageBadge(ts));
      var html="",ch=scalars(d);
      if(ch.length)html+='<div class="jw-chips">'+ch.map(function(p){return '<span class="jw-chip"><i>'+esc(p[0])+'</i>'+p[1]+'</span>';}).join("")+"</div>";
      var arr=firstArray(d,0);
      if(arr)html+=tableOf(arr);
      if(!html)html='<div class="jw-sum">'+summarize(d)+"</div>";
      body.className="jw-body";body.innerHTML=html;
    });
  });
})();
