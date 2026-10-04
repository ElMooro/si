/* Watchlist correctness v2. Saved members stay in saved order; quotes disclose
   daily aggregate dates/source/uncertainty. Chart Pro import is preview-only. */
(function () {
  if (window.__jhTvWatch) return;
  window.__jhTvWatch = 1;
  var quotes = Object.create(null), qSet = Object.create(null), inflight = 0, wait = [], lock = 0;
  var resolver = null, flagStore = Object.create(null), sort = null, sortDir = 1;
  var TTL = 300000, BACKOFF = 30000, TIMEOUT = 10000, generation = 0;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var css = document.createElement("style");
  css.id = "jh-tvwatch-css";
  css.textContent = [
    "#watch{font-family:-apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,Ubuntu,sans-serif}",
    "#nlists,.listbtn .n{display:none!important}",
    "#w-list{background:#131722}",
    "#menu.ss-watch-menu{z-index:49}",
    "#cols{display:grid;grid-template-columns:3px 8px 22px minmax(0,1fr) 62px 46px 52px!important;gap:0 6px;padding:4px 10px 3px;color:#787b86;font-size:11px;letter-spacing:.02em}",
    "#cols span{cursor:pointer}",
    "#wlist .wrow{display:grid;grid-template-columns:3px 8px 22px minmax(0,1fr) 62px 46px 52px!important;gap:0 6px;align-items:center;height:32px;padding:0 10px 0 0;border:0;border-radius:0;background:transparent;color:#d1d4dc;font-size:13px;position:relative;width:100%;text-align:left}",
    "#wlist .wrow:hover{background:#2a2e39}",
    "#wlist .wrow.on{background:#2a2e39}",
    "#wlist .wrow{height:auto;min-height:54px;grid-template-rows:30px auto;padding:0 10px 4px!important}",
    "#wlist .tvflag{grid-column:2;grid-row:1}#wlist .tvlogo{grid-column:3;grid-row:1}#wlist .wsym{grid-column:4;grid-row:1;text-align:left}#wlist .px{grid-column:5;grid-row:1}#wlist .chg:nth-child(6){grid-column:6;grid-row:1}#wlist .chg:nth-child(7){grid-column:7;grid-row:1}",
    "#wlist .qe{grid-column:4 / -1;grid-row:2;color:#9aa1ad;font-size:10px;white-space:normal;overflow-wrap:anywhere;line-height:1.3}",
    "#w-import{width:auto!important;min-width:88px;height:auto!important;min-height:26px;white-space:nowrap;font-size:11px}",
    "#w-import-status{font-size:12px;padding:6px 10px;overflow-wrap:anywhere}",
    "#watch .wquote-tools{display:flex;gap:6px;align-items:center;padding:4px 10px;color:#9aa1ad;font-size:10px}",
    "#watch .wquote-tools button{flex:none;color:#2962ff;padding:4px}",
    "#wlist .wrow .wacc{width:3px;height:32px;background:transparent!important;position:absolute;left:0;top:0}",
    "#wlist .wrow.on .wacc{background:#2962ff!important}",
    "#wlist .wsym{font-weight:400;color:#d1d4dc;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:0}",
    "#wlist .px,#wlist .chg{text-align:right;font-variant-numeric:tabular-nums;font-size:13px}",
    "#wlist .up{color:#089981}#wlist .dn{color:#f23645}",
    "#wlist .tvlogo{width:22px;height:22px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:10px;font-weight:600;color:#fff;background:#363a45}",
    "#wlist .tvflag{width:8px;height:8px;border-radius:50%;background:transparent;justify-self:center}",
    "#wlist .wsec{display:flex;align-items:center;height:28px;padding:0 12px;color:#787b86;font-size:11px;letter-spacing:.08em;text-transform:uppercase;background:#1e222d}",
    "#wlist .wsec span{margin-left:auto}",
    "#wlist .flash-up{animation:jhup .45s}#wlist .flash-dn{animation:jhdn .45s}",
    "@keyframes jhup{from{background:rgba(8,153,129,.35)}to{background:transparent}}",
    "@keyframes jhdn{from{background:rgba(242,54,69,.35)}to{background:transparent}}",
    ".listbtn .tvflag{width:10px;height:10px;border-radius:2px;flex:none;background:#787b86}",
    ".addsym{border:0!important;text-align:left!important;color:#2962ff!important;font-size:13px!important;padding:8px 12px!important}",
    "#watch .wtitle{height:0;padding:0;border:0;overflow:visible}",
    "#watch .wtitle b{display:none}",
    "#watch .wtitle .wops{position:absolute;right:6px;top:4px;z-index:5}",
    "#watch .whead{height:38px;padding:4px 8px}",
    "#watch .wtitle{height:auto;min-height:32px;overflow:visible;flex:none}#watch .wtitle .wops{position:static;flex-wrap:wrap;margin-left:auto}",
    "#listres .ld-row{display:flex;align-items:center;gap:8px}",
    "#listres .ld-flag{width:10px;height:10px;border-radius:2px;flex:none}"
  ].join("");
  document.documentElement.appendChild(css);

  function bare(s) { s = String(s || ""); return s.indexOf(":") >= 0 ? s.split(":").pop() : s; }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      if (c === "&") return "&" + "amp;";
      if (c === "<") return "&" + "lt;";
      if (c === ">") return "&" + "gt;";
      if (c === "\"") return "&" + "quot;";
      return "&" + "#39;";
    });
  }
  function hue(s) { var h = 0, i; s = bare(s); for (i = 0; i < s.length; i++) h = (h * 33 + s.charCodeAt(i)) >>> 0; return h % 360; }
  function num(n) {
    if (typeof n !== "number" || !isFinite(n)) return "—";
    var a = Math.abs(n), d = a >= 100 ? 2 : a >= 1 ? 2 : a >= 0.01 ? 4 : 6;
    return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: d });
  }
  function activeSym() {
    if(window.jhWatchlistActive)return window.jhWatchlistActive();
    var el = document.getElementById("symin");
    return el ? String(el.value || "").trim() : "";
  }
  function findList(id) {
    try {
      var raw=localStorage.getItem("jh-chart-custom-lists"), custom=raw===null?[]:JSON.parse(raw);
      if(!Array.isArray(custom))return null;
      if(id==="favorites"){
        var f=localStorage.getItem("jh-chart-favs"), fav=f===null?[]:JSON.parse(f);
        return Array.isArray(fav)?{id:id,name:"Favorites",symbols:fav,color:"#fdd835"}:null;
      }
      var saved=custom.filter(function(l){return l&&String(l.id)===String(id);});
      if(saved.length>1)return null;
      if(saved.length)return saved[0];
      return window.jhWatchlistNativeList ? window.jhWatchlistNativeList(id) : null;
    } catch(error) { return null; }
  }
  function openSym(s) {
    var q = document.getElementById("q");
    if (!q) { if (window.jhOpenSymbol) window.jhOpenSymbol(s); return; }
    q.value = s;
    q.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
  }
  function status(q) {
    if(!q || q.last==null)return "Unavailable · "+(q&&q.reason||"awaiting daily aggregate");
    var age=Math.max(0,Math.floor((Date.now()-q.time*1000)/86400000));
    return q.date+(q.previousTime?" vs "+new Date(q.previousTime*1000).toISOString().slice(0,10):"")+" · "+q.source+" · "+age+"d since bar timestamp · venue/currency and completion unverified"+
      (Date.now()-q.fetched>=TTL?" · cache expired":"")+(q.reason?" · "+q.reason:"");
  }
  function rowHtml(s, on) {
    if(!quotes[s] || quotes[s].last==null&&!quotes[s].retryAt){var identity=resolve(s,resolver);if(!identity.ticker)quotes[s]={reason:identity.reason};}
    var q=quotes[s], change=q && q.chg!=null, cls=change?(q.chg>=0?"up":"dn"):"";
    var flag=flagStore[s]||"", b=bare(s), label=status(q);
    return "<button type=button class='wrow tv"+(on?" on":"")+"' data-s='"+esc(s)+"' title='"+esc(s+" · "+label)+"'>"+
      "<i class=wacc></i><i class=tvflag"+(flag?" style='background:"+esc(flag)+"'":"")+"></i>"+
      "<i class=tvlogo style='background:hsl("+hue(s)+",42%,42%)'>"+esc((b||"?").slice(0,1))+"</i>"+
      "<span class=wsym>"+esc(s)+"</span><span class='px "+cls+"'>"+num(q&&q.last)+"</span>"+
      "<span class='chg "+cls+"'>"+(change?(q.chgv>=0?"+":"")+num(q.chgv):"—")+"</span>"+
      "<span class='chg "+cls+"'>"+(change?(q.chg>=0?"+":"")+(q.chg*100).toFixed(2)+"%":"—")+"</span>"+
      "<small class=qe>"+esc(label)+"</small></button>";
  }
  function resolve(s, rules) {
    if(typeof s!=="string" || !s.trim())return {reason:"invalid instrument identity"};
    var t=s.trim().toUpperCase();
    if(!rules)return {reason:"instrument resolver unavailable"};
    if((rules.licensed_econ_skip||[]).indexOf(t)>=0)return {reason:"licensed economic series"};
    var exact=rules.exact&&rules.exact[t], prefix=t.split(":")[0];
    if(window.JHChartCatalog && window.JHChartCatalog.lookupSym && /^FRED:/.test(window.JHChartCatalog.lookupSym(t)||""))return {reason:"economic series identity requires the series endpoint"};
    if(exact)return {reason:"resolver maps to "+exact.id+"; daily equity endpoint cannot verify that instrument"};
    if(t.indexOf(":")>=0){
      var rule=rules.prefix&&rules.prefix[prefix];
      return {reason:rule?"qualified "+rule.engine+" instrument; endpoint cannot verify venue or series identity":"unresolved namespace "+prefix};
    }
    if(!window.jhWatchlistResolve)return {reason:"native instrument resolver unavailable"};
    if(window.jhWatchlistResolve){
      var native=window.jhWatchlistResolve(t);
      if(!native||native.engine!=="equity"||native.ticker!==t||native.yahoo!==t)return {reason:"existing resolver identifies an alias or non-equity instrument; endpoint identity unverified"};
    }
    if(/^[A-Z]{6}$/.test(t))return {reason:"ambiguous currency, metal or equity identity"};
    if(/^[A-Z]{1,3}[FGHJKMNQUVXZ][0-9]{1,2}$/.test(t))return {reason:"possible futures contract identity unverified"};
    var currencies=["USD","EUR","GBP","JPY","CHF","CAD","AUD","NZD","CNY","CNH","HKD","SEK","NOK","DKK","MXN","ZAR","SGD"];
    if(t.length===6&&currencies.indexOf(t.slice(0,3))>=0&&currencies.indexOf(t.slice(3))>=0)return {reason:"currency pair identity unverified"};
    if(!/^[A-Z][A-Z0-9.\-]{0,11}$/.test(t))return {reason:"unsupported instrument identity"};
    if(/(?:USDT|USDC|BUSD|-USD)$/.test(t)||/^(BTCUSD|ETHUSD)$/.test(t))return {reason:"crypto currency/provider identity unverified"};
    return {ticker:t};
  }
  function packet(j,ticker,now) {
    if(!j||typeof j!=="object"||j.ticker!==ticker)return {reason:"response instrument identity missing or conflicting"};
    if(j.span!=="day"||j.mult!==1)return {reason:"daily interval identity missing or conflicting"};
    if(typeof j.source!=="string"||!j.source.trim())return {reason:"response source unavailable"};
    if(!Array.isArray(j.bars)||!j.bars.length)return {reason:"no daily aggregates"};
    var prevTime=-Infinity;
    for(var i=0;i<j.bars.length;i++){
      var b=j.bars[i];
      if(!b||!Number.isSafeInteger(b.time)||b.time<=0||b.time<=prevTime||b.time*1000>now || b.time*1000>8640000000000000)return {reason:"invalid or unordered bar timestamps"};
      if(typeof b.close!=="number"||!Number.isFinite(b.close))return {reason:"invalid aggregate close"};
      prevTime=b.time;
    }
    var last=j.bars[j.bars.length-1], prev=j.bars[j.bars.length-2];
    var delta=prev?last.close-prev.close:null, change=prev&&prev.close!==0?delta/prev.close:null;
    if(delta!==null&&!Number.isFinite(delta)||change!==null&&(!Number.isFinite(change)||!Number.isFinite(change*100)))return {reason:"aggregate change overflow"};
    return {last:last.close,chgv:change===null?null:delta,chg:change,time:last.time,
      previousTime:prev?prev.time:null,date:new Date(last.time*1000).toISOString().slice(0,10),
      source:j.source,fetched:now,completion:"unverified",reason:prev?null:"previous aggregate unavailable"};
  }
  window.jhWatchlistQuote={resolve:resolve,packet:packet,get:function(s){return quotes[s]&&quotes[s].last!=null?quotes[s]:null;}};
  function visible(s) {
    var box=document.getElementById("wlist");
    if(!box)return false;
    return Array.from(box.querySelectorAll(".wrow")).some(function(row){return row.dataset.s===s;});
  }
  function pump() {
    while(inflight<3 && wait.length){
      var item=wait.shift(), s=item.symbol;
      if(item.generation!==generation||!visible(s)){delete qSet[s];continue;}
      inflight++;
      (function(item){
        var s=item.symbol, ctrl=new AbortController(), timer;
        var deadline=new Promise(function(_,reject){timer=setTimeout(function(){ctrl.abort();reject(Error("request timeout"));},TIMEOUT);});
        var request=Promise.resolve().then(function(){
          return fetch(PROXY+"/ohlc?ticker="+encodeURIComponent(item.ticker)+"&span=day&mult=1&days=6",{signal:ctrl.signal});
        }).then(function(r){if(!r.ok)throw Error("HTTP "+r.status);return r.json();});
        Promise.race([request,deadline]).then(function(j){
          var next=packet(j,item.ticker,Date.now());
          if(next.last==null)throw Error(next.reason);
          quotes[s]=next;
        }).catch(function(error){
          var old=quotes[s]||{};
          quotes[s]=Object.assign({},old,{reason:error.message,retryAt:Date.now()+BACKOFF});
        }).finally(function(){
          clearTimeout(timer);delete qSet[s];inflight--;
          if(visible(s))paint(true);
          pump();
        });
      })(item);
    }
  }
  function want(s) {
    if(qSet[s])return;
    var hit=quotes[s], now=Date.now();
    if(hit&&((hit.last!=null&&now-hit.fetched<TTL)||(hit.retryAt&&now<hit.retryAt)))return;
    var resolved=resolve(s,resolver);
    if(!resolved.ticker){quotes[s]={reason:resolved.reason};return;}
    qSet[s]=1;wait.push({symbol:s,ticker:resolved.ticker,generation:generation});pump();
  }
  function paint(force) {
    if (lock) return;
    var sel = document.getElementById("list"), box = document.getElementById("wlist");
    if (!sel || !box) return;
    var qf = document.getElementById("q");
    var filter=qf?qf.value.trim().toUpperCase():"";
    var letter=document.querySelector("#letters button.on");
    var L = findList(sel.value);
    if(!L||!Array.isArray(L.symbols)||!L.symbols.every(function(s){return typeof s==="string";})){
      box.dataset.sig="";var message="Saved list unavailable; original storage retained";if(box.textContent!==message)box.textContent=message;return;
    }
    flagsFromStore();
    var symbols=L.symbols.filter(function(s){return !filter&&!letter || s.indexOf("###")!==0 && (!filter||s.toUpperCase().indexOf(filter)>=0)&&(!letter||bare(s).indexOf(letter.textContent)===0);});
    if(sort && !symbols.some(function(s){return s.indexOf("###")===0;}))symbols=symbols.slice().sort(function(a,b){
      var va=sort==="sym"?a:quotes[a]&&quotes[a][sort], vb=sort==="sym"?b:quotes[b]&&quotes[b][sort];
      if(va==null)return vb==null?0:1;if(vb==null)return -1;return (va>vb?1:va<vb?-1:0)*sortDir;
    });
    var cur = activeSym();
    var sig=JSON.stringify([sel.value,symbols,cur,flagStore,sort,sortDir]);
    if (!force && box.dataset.sig === sig && (box.querySelector(".wrow.tv")||!symbols.length)) return;
    var membership=JSON.stringify([sel.value,symbols]);
    if(box.dataset.membership!==membership){generation++;box.dataset.membership=membership;}
    var focused=document.activeElement&&document.activeElement.closest("#wlist .wrow");
    var focusSymbol=focused&&focused.dataset.s, scroll=box.scrollTop;
    lock = 1;
    var html = [], i, s, sec = 0;
    for (i = 0; i < symbols.length; i++) {
      s = symbols[i];
      if (String(s).indexOf("###") === 0) {
        html.push("<div class=wsec>" + esc(String(s).replace(/^#+/, "")) + "</div>");
        sec++;
        continue;
      }
      html.push(rowHtml(s, s === cur));
    }
    box.dataset.sig = sig;
    box.innerHTML = html.join("");
    box.querySelectorAll(".wrow").forEach(function (b) {
      b.onclick = function () { openSym(b.getAttribute("data-s")); };
      b.oncontextmenu=function(e){
        if(window.jhWatchlistContext){e.preventDefault();window.jhWatchlistContext(e.clientX,e.clientY,b.dataset.s);}
      };

    });
    var btn = document.getElementById("listbtn");
    if (btn && !btn.querySelector(".tvflag")) {
      btn.insertAdjacentHTML("afterbegin", "<i class=tvflag></i>");
    }
    if (btn) {
      var dot = btn.querySelector(".tvflag");
      if (dot) dot.style.background = L.color && String(L.color).indexOf("#") === 0 ? L.color : "#787b86";
    }
    var cols = document.getElementById("cols");
    if (cols && !cols.dataset.tv) { cols.dataset.tv = "1"; cols.insertAdjacentHTML("afterbegin", "<span></span><span></span>"); }
    box.scrollTop=scroll;
    if(focusSymbol)Array.from(box.querySelectorAll(".wrow")).some(function(row){if(row.dataset.s!==focusSymbol)return false;row.focus({preventScroll:true});return true;});
    lock = 0;
    bindHeaders();
    see();
  }
  function see() {
    var box = document.getElementById("wlist"); if (!box || !window.IntersectionObserver) return;
    if (see.io) see.io.disconnect();
    see.io = new IntersectionObserver(function (ents) {
      ents.forEach(function (en) { if (en.isIntersecting) want(en.target.getAttribute("data-s")); });
    }, { root: box, rootMargin: "80px" });
    box.querySelectorAll(".wrow").forEach(function (n) { see.io.observe(n); });
  }
  function flagsFromStore() {
    try{
      var raw=localStorage.getItem("jh-chart-flags"), flags=raw===null?{}:JSON.parse(raw);
      if(!flags||typeof flags!=="object"||Array.isArray(flags))return;
      flagStore=Object.create(null);
      Object.keys(flags).forEach(function(k){if(typeof flags[k]==="string"&&/^#[0-9a-f]{6}$/i.test(flags[k]))flagStore[k]=flags[k];});
    }catch(error){flagStore=Object.create(null);}
  }
  function loadResolver() {
    var ctrl=new AbortController(), timer=setTimeout(function(){ctrl.abort();},TIMEOUT);
    var deadline=new Promise(function(_,reject){clearTimeout(timer);timer=setTimeout(function(){ctrl.abort();reject(Error("resolver timeout"));},TIMEOUT);});
    return Promise.race([fetch("/data/tv-symbol-resolver.json",{signal:ctrl.signal}).then(function(r){if(!r.ok)throw Error("resolver unavailable");return r.json();}),deadline])
      .then(function(r){if(r&&typeof r==="object"&&r.prefix&&r.exact){resolver=r;paint(true);}})
      .catch(function(){resolver=null;paint(true);}).finally(function(){clearTimeout(timer);});
  }
  function refresh() {
    var now=Date.now();
    Object.keys(quotes).forEach(function(s){var q=quotes[s];if(q.last!=null&&now-q.fetched>=TTL)q.fetched=0;});
    paint(true);
  }
  function bindHeaders(){
    var cols=document.getElementById("cols");
    if(cols)cols.querySelectorAll("[data-s]").forEach(function(header){
      header.setAttribute("role","columnheader");header.tabIndex=0;
      header.setAttribute("aria-sort",sort===header.dataset.s?(sortDir===1?"ascending":"descending"):"none");
      header.onclick=function(){if(sort===header.dataset.s)sortDir*=-1;else{sort=header.dataset.s;sortDir=1;}document.getElementById("wlist").dataset.sig="";paint(true);};
      header.onkeydown=function(e){if(e.key==="Enter"||e.key===" "){e.preventDefault();e.stopPropagation();header.click();}};
    });
  }
  function hook() {
    var box = document.getElementById("wlist");
    if (!box) return false;
    if (!box.dataset.tvw) {
      box.dataset.tvw = "1";
      try {
        if (!localStorage.getItem("jh-tv-list-sized")) {
          var h = Math.max(220, Math.min(560, Math.round((window.innerHeight || 800) * 0.46)));
          box.style.height = h + "px";
          document.documentElement.style.setProperty("--list-h", h + "px");
          localStorage.setItem("jh-tv-list-sized", "1");
        }
      } catch (e) {}
      new MutationObserver(function () { paint(); }).observe(box, { childList: true });
      var res = document.getElementById("listres");
      if (res) new MutationObserver(function () {
        if (lock) return;
        res.querySelectorAll(".ld-row").forEach(function (row) {
          if (row.querySelector(".ld-flag")) return;
          var id = row.getAttribute("data-id");
          var L = findList(id);
          var c = L && L.color && String(L.color).charAt(0) === "#" ? L.color : "#787b86";
          row.insertAdjacentHTML("afterbegin", "<i class=ld-flag style='background:" + c + "'></i>");
        });
      }).observe(res, { childList: true });
    }
    bindHeaders();
    if(!document.getElementById("w-quote-refresh")){
      var tools=document.createElement("div");tools.className="wquote-tools";
      tools.innerHTML="<span>Daily aggregate close · Δ previous bar · completion unverified</span><button type=button id=w-quote-refresh>Refresh</button>";
      var list=document.getElementById("w-list");if(list)list.insertBefore(tools,list.firstChild);
      document.getElementById("w-quote-refresh").onclick=function(){if(!resolver)loadResolver();refresh();};
      var headers=document.querySelectorAll("#cols [data-s]");headers.forEach(function(h){if(h.dataset.s==="last")h.textContent="Close";if(h.dataset.s==="chg")h.textContent="Δ%";if(h.dataset.s==="chgv")h.textContent="Δ";});
    }
    paint();
    return true;
  }
  document.addEventListener("keydown", function (e) {
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
    var tag = (e.target && e.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || !e.target.closest || !e.target.closest("#wlist .wrow")) return;
    var w = document.getElementById("watch");
    if (!w || !w.classList.contains("is-open")) return;
    var rows = [].slice.call(document.querySelectorAll("#wlist .wrow"));
    if (!rows.length) return;
    var i = 0, k; for (k = 0; k < rows.length; k++) if (rows[k].classList.contains("on")) i = k;
    i = e.key === "ArrowDown" ? Math.min(rows.length - 1, i + 1) : Math.max(0, i - 1);
    e.preventDefault();
    rows[i].focus({preventScroll:true});
    rows[i].scrollIntoView({ block: "nearest" });
    openSym(rows[i].getAttribute("data-s"));
  });
  window.addEventListener("storage",function(e){if(["jh-chart-custom-lists","jh-chart-favs","jh-chart-flags"].indexOf(e.key)>=0)paint(true);});
  document.addEventListener("visibilitychange",function(){if(!document.hidden)refresh();});
  loadResolver();
  var n = 0, t = setInterval(function () { if (hook() || ++n > 40) clearInterval(t); }, 250);
})();
