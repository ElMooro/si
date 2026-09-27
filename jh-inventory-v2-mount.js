/* Append-only mount for inventory-drawdown v2 + ticker four-state chip.
   No-op if jhInventoryV2 missing. Does not replace the boom board. */
(function(){if(window.__jhInvV2Mount)return;window.__jhInvV2Mount=true;
function esc(s){return String(s==null?"":s).replace(/&/g,String.fromCharCode(38)+"amp;").replace(/</g,String.fromCharCode(38)+"lt;");}
function sign(n,sfx){if(n==null||isNaN(n))return "";return (n>0?"+":"")+(Math.round(n*10)/10)+(sfx||"");}
async function gj(p){try{var r=await fetch("https://justhodl.ai/"+p+"?t="+Date.now(),{cache:"no-store"});if(r.ok)return r.json();}catch(e){}return null;}
function tickerFromUrl(){
  try{
    var u=new URL(location.href);
    var q=u.searchParams.get("symbol")||u.searchParams.get("s")||u.searchParams.get("ticker")||"";
    if(q)return String(q).toUpperCase().trim();
  }catch(e){}
  var hdr=document.getElementById("symbolHeader");
  if(hdr&&hdr.textContent){var t=String(hdr.textContent).toUpperCase().replace(/[^A-Z0-9.]/g,"");if(t&&t!=="TICKER")return t;}
  return "";
}
function ensureCss(){
  if(document.getElementById("jh-inv-v2-css"))return;
  var s=document.createElement("style");
  s.id="jh-inv-v2-css";
  s.textContent=".jh-inv-v2-ticker{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:#a8b3c7;border:1px solid #1c2433;background:#0c1018;border-radius:8px;padding:10px 12px;margin:10px 0 18px}"
    +".jh-inv-v2-ticker b{color:#e1e8f4}"
    +".jh-inv-v2-ticker a{color:#22d3ee;text-decoration:none}"
    +".jh-inv-v2-st{display:inline-block;margin-left:8px;padding:1px 7px;border-radius:4px;font-size:10px;letter-spacing:.4px}"
    +".jh-inv-v2 table{width:100%;border-collapse:collapse;margin-top:8px}"
    +".jh-inv-v2 th,.jh-inv-v2 td{text-align:right;padding:6px 8px;border-bottom:1px solid #1c2433;font:12px ui-monospace,Menlo,monospace}"
    +".jh-inv-v2 th{color:#6b7480;font-size:10px;text-transform:uppercase;cursor:pointer;user-select:none}"
    +".jh-inv-v2 th.on{color:#38bdf8}"
    +".jh-inv-v2 td.l,.jh-inv-v2 th.l{text-align:left}";
  document.head.appendChild(s);
}
async function loadJoined(){
  var pack=await Promise.all([gj("data/inventory-drawdown.json"),gj("data/backlog.json"),gj("data/estimate-revisions.json"),gj("data/earnings-quality.json")]);
  var d=pack[0],api=window.jhInventoryV2;
  if(!d||!api)return null;
  return {d:d,joined:api.joinRows(d.stock_drawdown_board||[],pack[1]||{},pack[2]||{},pack[3]||{}),tape:api.chainTape(d.sector_drawdown||[])};
}
function cmp(a,b,k,dir){
  var av=a[k], bv=b[k];
  if(av==null&&bv==null) return String(a.ticker||"").localeCompare(String(b.ticker||""));
  if(av==null) return 1; if(bv==null) return -1;
  if(typeof av==="number"||typeof bv==="number"){
    av=Number(av); bv=Number(bv);
    if(!isFinite(av)) return 1; if(!isFinite(bv)) return -1;
    return (av-bv)*dir;
  }
  return String(av).localeCompare(String(bv))*dir;
}
function bootDesk(){
  if(!window.jhInventoryV2)return;
  var n=0;var t=setInterval(async function(){
    n++;
    var w=document.getElementById("w");
    if(!w||(!w.querySelector(".sec")&&!w.querySelector("h1"))){if(n>40)clearInterval(t);return;}
    clearInterval(t);
    if(w.querySelector(".jh-inv-v2"))return;
    var pack=await loadJoined();
    if(!pack||!pack.d||!pack.d.sector_drawdown)return;
    var joined=pack.joined,tape=pack.tape;
    var box=document.createElement("div");
    box.className="jh-inv-v2";
    ensureCss();
    var SORT="four_state", DIR=1;
    var cols=[["ticker","Ticker","l"],["four_state","State","l"],["dio_chg_pct","DIO Δ%",""],["rev_growth_yoy","Rev YoY",""],["book","Book","l"],["sloan","Sloan",""]];
    function paintTable(){
      var list=joined.slice().sort(function(a,b){return cmp(a,b,SORT,DIR);});
      var h='<div class="sec">Sector chain tape <span class="n">same FRED I/S rows</span></div>';
      h+='<div class="card">Factory '+(tape.factory_dir||"n/a")+' \u00b7 Store '+(tape.store_dir||"n/a")+' \u00b7 Autos '+(tape.auto_dir||"n/a")+(tape.read?" \u00b7 "+esc(tape.read):"")+'</div>';
      h+='<div class="sec">Four-state overlay <span class="n">Click headers to sort. RM/WIP/FG untagged. Sloan/DSRI from earnings-quality.</span></div>';
      h+='<table><thead><tr>';
      cols.forEach(function(c){h+='<th class="'+(c[2]||"")+(SORT===c[0]?" on":"")+'" data-k="'+c[0]+'">'+c[1]+(SORT===c[0]?(DIR>0?" \u2191":" \u2193"):"")+"</th>";});
      h+="</tr></thead><tbody>";
      list.forEach(function(r){
        h+='<tr><td class="l"><a href="/ticker.html?symbol='+encodeURIComponent(r.ticker)+'" style="color:#fff">'+esc(r.ticker)+"</a></td>";
        h+='<td class="l">'+esc(r.four_state||"")+"</td>";
        h+="<td>"+esc(sign(r.dio_chg_pct,"%"))+"</td>";
        h+="<td>"+esc(sign(r.rev_growth_yoy,"%"))+"</td>";
        h+='<td class="l">'+esc(r.book||"")+"</td>";
        h+="<td>"+(r.sloan==null?"":r.sloan.toFixed(1))+"</td></tr>";
      });
      h+='</tbody></table><div class="foot">Composition columns stay blank. Data: inventory-drawdown + backlog + estimate-revisions + earnings-quality. Click headers to sort asc/desc. Nulls last.</div>';
      box.innerHTML=h;
      box.querySelectorAll("th[data-k]").forEach(function(th){
        th.onclick=function(){if(SORT===th.dataset.k)DIR=-DIR;else{SORT=th.dataset.k;DIR=th.dataset.k==="ticker"||th.dataset.k==="four_state"||th.dataset.k==="book"?1:-1;}paintTable();};
      });
    }
    paintTable();
    var foot=w.querySelector(".foot");
    if(foot)w.insertBefore(box,foot);else w.appendChild(box);
  },250);
}
function paintTicker(sym, pack){
  if(!sym||!pack)return;
  ensureCss();
  var row=null;
  for(var i=0;i<pack.joined.length;i++){
    if(String(pack.joined[i].ticker||"").toUpperCase()===sym){row=pack.joined[i];break;}
  }
  var box=document.querySelector(".jh-inv-v2-ticker");
  if(!box){
    box=document.createElement("div");
    box.className="jh-inv-v2-ticker";
    var hero=document.querySelector(".hero");
    if(hero&&hero.parentNode)hero.parentNode.insertBefore(box,hero.nextSibling);
    else {
      var host=document.getElementById("jh-inv-v2-slot")||document.querySelector(".wrap")||document.body;
      if(host.firstChild)host.insertBefore(box,host.firstChild);else host.appendChild(box);
    }
  }
  box.setAttribute("data-ticker",sym);
  if(!row){
    box.innerHTML='<div><b>'+esc(sym)+'</b> has no DIO row on the inventory board. <a href="/inventory-drawdown.html">Inventory desk</a></div>';
    return;
  }
  var st=row.four_state||"";
  var color=st==="SHORTAGE"?"#34d399":st==="COMMODITY"?"#fbbf24":st==="DESTOCK"?"#f87171":st==="STUFFED"?"#f87171":st==="RESTOCK"?"#38bdf8":"#9aa5b1";
  var bits=['<b>'+esc(sym)+'</b> <span class="jh-inv-v2-st" style="color:'+color+';border:1px solid '+color+'">'+esc(st||"UNTAGGED")+'</span>'];
  if(row.dio_chg_pct!=null)bits.push("DIO "+esc(sign(row.dio_chg_pct,"%")));
  if(row.rev_growth_yoy!=null)bits.push("rev "+esc(sign(row.rev_growth_yoy,"%")));
  if(row.book)bits.push("book "+esc(row.book));
  if(row.sloan!=null)bits.push("sloan "+row.sloan.toFixed(1));
  if(row.four_why)bits.push(esc(row.four_why));
  box.innerHTML='<div>'+bits.join(" \u00b7 ")+'</div><div style="margin-top:4px"><a href="/inventory-drawdown.html">Inventory desk</a> \u00b7 RM/WIP/FG untagged</div>';
}
async function bootTicker(){
  if(!window.jhInventoryV2)return;
  if(document.getElementById("w")&&location.pathname.indexOf("inventory-drawdown")>=0)return;
  var pack=null;
  var last="";
  async function tick(){
    var t=tickerFromUrl();
    if(!t||t===last)return;
    if(!pack)pack=await loadJoined();
    if(!pack)return;
    last=t;
    paintTicker(t,pack);
  }
  await tick();
  setInterval(tick,900);
}
function boot(){bootDesk();bootTicker();}
if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",boot);else boot();
})();
