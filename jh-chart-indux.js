/* jh-reskin-skip */
/* Supercharts indicator chrome: list, star, eye, settings, remove, pane + watch resize. */
(function () {
  if (window.__jhIndux) return;
  window.__jhIndux = true;
  var FAV_KEY = "jh-chart-ind-favs";
  var favs = (function () {
    try {
      var x = JSON.parse(localStorage.getItem(FAV_KEY) || "null");
      if (Array.isArray(x) && x.length) return x;
    } catch (e) {}
    return ["sma20", "sma50", "sma200", "ema9", "bb", "avgdev", "vwap", "rsi", "macd", "adx", "hv", "bbp", "vol", "voltape", "vsa", "livermore", "wyckoff", "gdx", "bbw", "bbsqz", "keylv", "pvwap", "ddown", "alpha", "rngpos", "earn"];
  })();
  function saveFav() {
    try { localStorage.setItem(FAV_KEY, JSON.stringify(favs)); } catch (e) {}
  }
  function isFav(id) { return favs.indexOf(id) >= 0; }
  function star(id) {
    favs = isFav(id) ? favs.filter(function (x) { return x !== id; }) : favs.concat([id]);
    saveFav();
  }

  var css = document.createElement("style");
  css.id = "jh-indux-css";
  css.textContent = [
    "#legend{pointer-events:none;max-width:min(560px,72%);z-index:8;left:10px;top:6px;font-family:IBM Plex Sans,system-ui,sans-serif}",
    "#legend .leg-sym{pointer-events:auto;color:#d1d4dc;font-weight:600;font-size:13px;margin-bottom:2px;display:flex;align-items:center;gap:4px}",
    "#legend .leg-dia{width:18px;height:18px;color:#787b86;border-radius:3px;font-size:12px;line-height:18px}",
    "#legend .leg-dia:hover{background:#2a2e39;color:#d1d4dc}",
    "#legend .leg-row{pointer-events:auto;display:flex;align-items:center;gap:6px;padding:1px 4px 1px 2px;margin:0;border-radius:2px;font-size:11px;line-height:18px;cursor:default;width:fit-content}",
    "#legend .leg-row:hover{background:rgba(41,98,255,.12)}",
    "#legend .leg-row.dim{opacity:.4}",
    "#legend .leg-sw{width:8px;height:8px;border-radius:1px;flex:none}",
    "#legend .leg-n{font-weight:500}",
    "#legend .leg-v{color:#787b86;font-family:IBM Plex Mono,monospace;font-size:11px}",
    "#legend .leg-ops,#oscwrap .leg-ops{display:inline-flex;gap:2px;margin-left:4px;opacity:0}",
    "#legend .leg-row:hover .leg-ops,#oscwrap .osc-head:hover .leg-ops{opacity:1}",
    "#legend .leg-ops button,#oscwrap .leg-ops button{display:inline-flex;align-items:center;justify-content:center;width:18px;height:18px;padding:0;border-radius:2px;color:#787b86;font-size:12px;line-height:18px}",
    "#legend .leg-ops button:hover,#oscwrap .leg-ops button:hover{background:#2a2e39;color:#d1d4dc}",
    "#inddlg,#indset{display:none;position:fixed;inset:0;z-index:70;background:rgba(0,0,0,.5);align-items:flex-start;justify-content:center;padding-top:6vh}",
    "#inddlg.on,#indset.on{display:flex}",
    "#inddlg .box{width:min(760px,94vw);height:min(72vh,560px);background:#1e222d;border:1px solid #2a2e39;border-radius:8px;display:flex;overflow:hidden;box-shadow:0 18px 50px rgba(0,0,0,.45);color:#d1d4dc}",
    "#inddlg .nav{width:150px;background:#131722;border-right:1px solid #2a2e39;padding:10px 0;flex:none;overflow:auto}",
    "#inddlg .nav button{display:block;width:100%;text-align:left;padding:8px 14px;font-size:13px;color:#787b86}",
    "#inddlg .nav button.on,#inddlg .nav button:hover{background:#2a2e39;color:#d1d4dc}",
    "#inddlg .maincol{flex:1;min-width:0;display:flex;flex-direction:column}",
    "#inddlg .hd{display:flex;align-items:center;gap:8px;padding:0 12px;height:44px;border-bottom:1px solid #2a2e39}",
    "#inddlg .hd input{flex:1;min-width:0;background:transparent;color:#d1d4dc;font-size:14px;outline:none}",
    "#inddlg .hd .x{width:28px;height:28px;color:#787b86}",
    "#inddlg .list{flex:1;overflow:auto}",
    "#inddlg .icat{padding:10px 16px 4px;font-size:11px;letter-spacing:.08em;color:#2962ff}",
    "#inddlg .irow{display:flex;align-items:center;gap:10px;padding:8px 16px;width:100%;text-align:left;border-bottom:1px solid #2a2e39}",
    "#inddlg .irow:hover{background:#2a2e39}",
    "#inddlg .irow.on{background:rgba(41,98,255,.12)}",
    "#inddlg .irow b{display:block;font-size:13px;font-weight:500;color:#d1d4dc}",
    "#inddlg .irow span{display:block;font-size:11px;color:#787b86}",
    "#inddlg .star{margin-left:auto;width:28px;height:28px;flex:none;color:#787b86;font-size:16px}",
    "#inddlg .star.on{color:#f0b429}",
    "#inddlg .qhelp{width:22px;height:22px;flex:none;border-radius:50%;border:1px solid #434651;color:#787b86;font-size:11px;font-weight:700;margin-left:4px}",
    "#inddlg .qhelp:hover{border-color:#2962ff;color:#d1d4dc;background:#2a2e39}",
    "#indhelp{display:none;position:fixed;inset:0;z-index:80;background:rgba(0,0,0,.55);align-items:flex-start;justify-content:center;padding-top:8vh}",
    "#indhelp.on{display:flex}",
    "#indhelp .box{width:min(520px,94vw);max-height:min(78vh,640px);background:#1e222d;border:1px solid #2a2e39;border-radius:8px;color:#d1d4dc;box-shadow:0 18px 50px rgba(0,0,0,.5);overflow:hidden;display:flex;flex-direction:column}",
    "#indhelp .sh{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;padding:14px 16px;border-bottom:1px solid #2a2e39}",
    "#indhelp .sh b{display:block;font-size:15px;font-weight:600}",
    "#indhelp .sh .tag{display:block;margin-top:3px;font-size:11px;color:#787b86;letter-spacing:.04em}",
    "#indhelp .sh .x{width:28px;height:28px;color:#787b86;flex:none}",
    "#indhelp .hb{padding:14px 16px 18px;overflow:auto;flex:1}",
    "#indhelp h5{margin:14px 0 4px;font-size:11px;letter-spacing:.08em;color:#2962ff;font-weight:600}",
    "#indhelp h5:first-child{margin-top:0}",
    "#indhelp p{margin:0;font-size:13px;line-height:1.45;color:#d1d4dc}",
    "#indhelp .caveat{margin-top:16px;padding:10px 12px;background:#131722;border:1px solid #2a2e39;border-radius:6px;font-size:12px;color:#787b86;line-height:1.4}",
    "#indhelp .evlist{margin:8px 0 0;display:flex;flex-direction:column;gap:4px}",
    "#indhelp .evlist button,#indset .evlist button{display:flex;align-items:center;gap:8px;width:100%;text-align:left;padding:7px 8px;border-radius:4px;color:#d1d4dc;font-size:13px}",
    "#indhelp .evlist button:hover,#indset .evlist button:hover{background:#2a2e39}",
    "#indhelp .evlist b,#indset .evlist b{font-family:IBM Plex Mono,monospace;font-size:12px;width:52px;flex:none}",
    "#indhelp .evlist span,#indset .evlist span{flex:1;color:#787b86;font-size:12px}",
    "#indhelp .evlist .qhelp,#indset .evlist .qhelp{margin-left:auto}",
    "#indset .snote{font-size:12px;color:#787b86;line-height:1.4;margin-bottom:8px}",
    "#indset .box{width:min(480px,94vw);background:#1e222d;border:1px solid #2a2e39;border-radius:8px;color:#d1d4dc;box-shadow:0 18px 50px rgba(0,0,0,.45);overflow:hidden}",
    "#indset .sh{display:flex;align-items:center;justify-content:space-between;padding:12px 14px;border-bottom:1px solid #2a2e39;font-weight:600}",
    "#indset .stabs{display:flex;border-bottom:1px solid #2a2e39}",
    "#indset .stabs button{padding:10px 16px;color:#787b86;font-size:13px}",
    "#indset .stabs button.on{color:#2962ff;border-bottom:2px solid #2962ff}",
    "#indset .sbody{padding:14px;min-height:180px}",
    "#indset .srow{display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid #2a2e39;font-size:13px}",
    "#indset .srow input,#indset .srow select{background:#131722;border:1px solid #2a2e39;color:#d1d4dc;padding:6px 8px;border-radius:4px;min-width:96px}",
    "#indset .sfoot{display:flex;justify-content:flex-end;gap:8px;padding:12px 14px;border-top:1px solid #2a2e39}",
    "#indset .sfoot .ok{background:#2962ff;color:#fff;padding:7px 16px;border-radius:4px;font-weight:600}",
    "#indset .sfoot .cancel{padding:7px 16px;color:#787b86}",
    "#oscwrap.on{display:flex;flex-direction:column;flex:none}",
    "#oscwrap .osc{display:flex;flex-direction:column;min-height:72px;height:118px;border-top:0;position:relative}",
    "#oscwrap .osc-head{display:flex;align-items:center;gap:8px;height:22px;padding:0 8px;flex:none;font-size:11px;color:#d1d4dc;z-index:6;pointer-events:auto}",
    "#oscwrap .osc-n{font-weight:600}",
    "#oscwrap .osc-v{color:#787b86;font-family:IBM Plex Mono,monospace}",
    "#oscwrap .osc-host{flex:1;min-height:0;position:relative}",
    ".pane-split{height:8px;cursor:ns-resize;background:transparent;flex:none;position:relative;z-index:9}",
    ".pane-split::after{content:'';position:absolute;left:50%;top:2px;width:40px;height:4px;margin-left:-20px;border-radius:2px;background:#2a2e39}",
    ".pane-split:hover,.pane-split.drag{background:rgba(41,98,255,.18)}",
    ".pane-split:hover::after,.pane-split.drag::after{background:#2962ff}",
    ".w-split{position:absolute;left:0;top:0;bottom:0;width:8px;cursor:ew-resize;z-index:12}",
    ".w-split:hover,.w-split.drag{background:rgba(41,98,255,.18)}",
    ".watch,#watch{max-width:none}",
    "#quote .sell,#quote .buy,#quote,#desk-intel,#intel{display:none!important}",
  ].join("");
  document.documentElement.appendChild(css);

  function el(id, html) {
    var n = document.getElementById(id);
    if (!n) {
      n = document.createElement("div");
      n.id = id;
      document.body.appendChild(n);
    }
    if (html != null) n.innerHTML = html;
    return n;
  }

  function allStudies() {
    var a = [];
    a.push({ item: { id: "vol", n: "Volume", on: !!window.volOn, cat: "Volume" }, osc: false, id: "vol", n: "Volume", cat: "Volume" });
    (window.INDS || []).forEach(function (i) { a.push({ item: i, osc: false, id: i.id, n: i.n, cat: i.cat || "Overlay" }); });
    (window.OSC || []).forEach(function (o) { a.push({ item: o, osc: true, id: o.id, n: o.n, cat: o.cat || "Oscillator" }); });
    return a;
  }

  function paintNow() {
    if (window.lastBars && window.lastBars.length && window.paint) window.paint(window.lastBars);
    try { if (window.jhSaveLay) window.jhSaveLay(); } catch (e) {}
  }

  window.jhInduxLegend = function (ctx) {
    var INDS = ctx.INDS, last = ctx.lastBars && ctx.lastBars.length ? ctx.lastBars[ctx.lastBars.length - 1] : null;
    var t = ctx.atTime != null ? ctx.atTime : (last && last.time);
    var host = document.getElementById("legend");
    if (!host) return;
    function val(id) {
      var n = ctx.valAt(ctx.overlayMap[id], t);
      return n == null ? "" : ctx.fmt(n);
    }
    var tfLab = ctx.spec(ctx.tf)[1] || ctx.tf;
    var html = "<div class=leg-sym><button type=button class=leg-dia title='Chart / company info'>◆</button> " + ctx.active + " · " + tfLab + "</div>";
    INDS.filter(function (i) { return i.on; }).forEach(function (i) {
      html += "<div class='leg-row" + (i.hide ? " dim" : "") + "' data-kind=ov data-id='" + i.id + "'>" +
        "<i class=leg-sw style=background:" + (i.c || ctx.ACC) + "></i>" +
        "<span class=leg-n style=color:" + (i.c || ctx.ACC) + ">" + i.n + "</span>" +
        "<span class=leg-v>" + val(i.id) + "</span>" +
        "<span class=leg-ops>" +
          "<button type=button data-act=help title='What is this'>?</button>" +
          "<button type=button data-act=eye title=Visibility>" + (i.hide ? "○" : "◉") + "</button>" +
          "<button type=button data-act=set title=Settings>⚙</button>" +
          "<button type=button data-act=x title=Remove>×</button></span></div>";
    });
    if (ctx.volOn) {
      html += "<div class='leg-row' data-kind=vol>" +
        "<i class=leg-sw style=background:" + ctx.UP + "></i>" +
        "<span class=leg-n style=color:" + ctx.UP + ">Volume</span>" +
        "<span class=leg-v>" + (last ? ctx.fmtVol(last.volume) : "") + "</span>" +
        "<span class=leg-ops>" +
          "<button type=button data-act=eye title=Visibility>◉</button>" +
          "<button type=button data-act=x title=Remove>×</button></span></div>";
    }
    host.innerHTML = html;
    var dia = host.querySelector(".leg-dia");
    if (dia) dia.onclick = function (e) { e.stopPropagation(); if (window.jhChartMenu) window.jhChartMenu(dia); };
    host.querySelectorAll(".leg-row").forEach(function (row) {
      row.onclick = function (e) {
        var btn = e.target.closest("[data-act]");
        var act = btn ? btn.getAttribute("data-act") : "set";
        if (row.getAttribute("data-kind") === "vol") {
          if (act === "x" || act === "eye") ctx.setVol(false);
          return;
        }
        var i = INDS.find(function (x) { return x.id === row.getAttribute("data-id"); });
        if (!i) return;
        if (act === "eye") { i.hide = !i.hide; paintNow(); }
        else if (act === "help") { if (window.jhInduxHelp) window.jhInduxHelp(i.id); }
        else if (act === "x") { i.on = false; i.hide = false; paintNow(); }
        else window.jhInduxSet(i, false, ctx);
      };
    });
  };

  var dlgTab = "tech";
  window.jhInduxOpen = function () {
    var d = el("inddlg");
    d.className = "on";
    d.innerHTML = "<div class=box>" +
      "<div class=nav>" +
        "<button type=button data-tab=fav>Favorites</button>" +
        "<button type=button data-tab=tech>Technicals</button>" +
        "<button type=button data-tab=osc>Oscillators</button>" +
      "</div>" +
      "<div class=maincol>" +
        "<div class=hd><input id=indq2 placeholder=Search autocomplete=off><button type=button class=x id=indx2>×</button></div>" +
        "<div class=list id=indlist></div>" +
      "</div></div>";
    function draw() {
      d.querySelectorAll("[data-tab]").forEach(function (b) {
        b.className = b.getAttribute("data-tab") === dlgTab ? "on" : "";
      });
      var q = ((document.getElementById("indq2") && document.getElementById("indq2").value) || "").toLowerCase();
      var rows = allStudies().filter(function (s) {
        if (dlgTab === "fav" && !isFav(s.id)) return false;
        if (dlgTab === "osc" && !s.osc) return false;
        if (dlgTab === "tech" && s.osc) return false;
        var keys = (s.n + " " + s.cat + " " + s.id).toLowerCase();
        if (s.id === "voltape") keys += " capitulation huge buy confirmed breakout effort vs results selling climax buying climax tape volume stopping absorption hidden bottom top accumulation reversal eoa eod";
        if (s.id === "vsa") keys += " vsa volume spread analysis no demand no supply stopping absorption hidden buying selling test trap shakeout tape";
        if (s.id === "livermore") keys += " pivot livermore trend reversal bottom top rev-up rev-dn cycle";
        if (s.id === "wyckoff") keys += " wyckoff climax sos sow accumulation distribution eoa eod phase";
        if (s.id === "accum") keys += " accumulation eoa sos sc bottom";
        if (s.id === "distrib") keys += " distribution eod sow bc top";
        if (s.id === "bb" || s.id === "bbw" || s.id === "bbsqz" || s.id === "bbp") keys += " bollinger band squeeze width average percent %b";
        if (s.id === "rsi") keys += " overbought oversold wilder";
        if (s.id === "adx") keys += " dmi directional +di -di adx trend";
        if (s.id === "avgdev") keys += " average deviation standard bloomberg sigma mean";
        if (s.id === "gdx") keys += " golden death cross sma 50 200";
        if (s.id === "hv") keys += " historical volatility realized vol";
        if (s.id === "beta" || s.id === "rsline" || s.id === "corrspy") keys += " spy relative strength beta correlation vs";
        if (s.id === "fibauto" || s.id === "fibpiv") keys += " fibonacci retracement pivot";
        if (s.id === "gmma" || s.id === "ribbon") keys += " guppy ribbon multiple moving average ema";
        if (s.id === "kama") keys += " kaufman adaptive ama";
        if (s.id === "seb") keys += " standard error linear regression bands";
        if (s.id === "keylv") keys += " pdh pdl pdc pwh pwl pmh pml previous day week month high low close session levels";
        if (s.id === "gaps") keys += " unfilled gap up down support resistance";
        if (s.id === "pvwap") keys += " ytd monthly weekly period vwap volume weighted year to date";
        if (s.id === "athln") keys += " all time high ath peak avwap anchored from high";
        if (s.id === "htfma") keys += " weekly sma 10 40 higher timeframe htf guppy bias";
        if (s.id === "struct") keys += " market structure hh hl lh ll swing fractal";
        if (s.id === "rsidiv") keys += " rsi macd divergence regular hidden bullish bearish";
        if (s.id === "svwaps") keys += " swing anchored vwap last high low fractal";
        if (s.id === "earn") keys += " earnings beat miss fomc witching rebalance auction events calendar eps surprise";
        if (s.id === "htrsi") keys += " weekly rsi higher timeframe htf";
        if (s.id === "ddown") keys += " drawdown from ath underwater maxdd risk";
        if (s.id === "alpha") keys += " jensen residual alpha vs spy beta adjusted excess";
        if (s.id === "rngpos") keys += " range position 52 week percentile high low";
        if (q && keys.indexOf(q) < 0) return false;
        return true;
      });
      var cats = [];
      rows.forEach(function (s) { if (cats.indexOf(s.cat) < 0) cats.push(s.cat); });
      var html = "";
      if (dlgTab === "fav" && !rows.length) html = "<div class=irow><span>Star studies in Technicals or Oscillators.</span></div>";
      cats.forEach(function (c) {
        html += "<div class=icat>" + c.toUpperCase() + "</div>";
        rows.filter(function (s) { return s.cat === c; }).forEach(function (s) {
          html += "<button type=button class='irow" + (s.item.on ? " on" : "") + "' data-add='" + s.id + "' data-osc='" + (s.osc ? "1" : "0") + "'>" +
            "<div><b>" + s.n + "</b><span>" + (
              s.id === "voltape" ? "Bottom · Top · EOA · REV · Capit · SC/BC · SV · ABS · " :
              s.id === "vsa" ? "ND · NS · SV · ABS · HB · HS · TEST · TRAP · SHK · " :
              s.id === "livermore" ? "BOTTOM · TOP · REV-UP/DN · " :
              s.id === "wyckoff" ? "SC/CAPIT · SOS/EOA · SOW/EOD · BC · " :
              s.id === "accum" ? "BOTTOM · EOA · SC/CAPIT · " :
              s.id === "distrib" ? "TOP · EOD · BC · " :
              s.id === "vol" ? "Histogram · " :
              s.id === "bb" ? "Avg + upper/lower · " :
              s.id === "bbw" ? "Width % + squeeze · " :
              s.id === "bbsqz" ? "TTM squeeze momentum · " :
              s.id === "bbp" ? "%B 0–100 · " :
              s.id === "avgdev" ? "Mean ±1σ ±2σ · " :
              s.id === "adx" ? "+DI −DI ADX · " :
              s.id === "gdx" ? "SMA 50 / 200 · " :
              s.id === "hv" ? "Close-to-close ann. · " :
              s.id === "beta" ? "60-bar vs SPY · " :
              s.id === "rsline" ? "Rebased vs SPY · " :
              s.id === "gmma" ? "Short 3–15 / Long 30–60 · " :
              s.id === "fibauto" ? "Swing 0–100% · " :
              s.id === "volosc" ? "Vol SMA 5−20 · " :
              s.id === "keylv" ? "PDH PDL PDC · PWH PWL · PMH PML · " :
              s.id === "gaps" ? "Unfilled gap up/down · " :
              s.id === "pvwap" ? "YTD + monthly + weekly · " :
              s.id === "athln" ? "ATH line + AVWAP from high · " :
              s.id === "htfma" ? "Weekly SMA 10 / 40 · " :
              s.id === "struct" ? "HH HL LH LL swings · " :
              s.id === "rsidiv" ? "RSI + MACD regular/hidden · " :
              s.id === "svwaps" ? "AVWAP from last swing H/L · " :
              s.id === "earn" ? "BEAT/MISS · FOMC · EPS · " :
              s.id === "ddown" ? "% from ATH · " :
              s.id === "alpha" ? "Residual vs SPY · " :
              s.id === "rngpos" ? "Close in 52w range · " :
              s.id === "htrsi" ? "Weekly RSI 14 on daily · " :
              s.osc ? "New pane · " : "Overlay · "
            ) + s.cat + "</span></div>" +
            "<span class='qhelp' data-help='" + s.id + "' title='What is this'>?</span>" +
            "<span class='star" + (isFav(s.id) ? " on" : "") + "' data-star='" + s.id + "'>" + (isFav(s.id) ? "★" : "☆") + "</span></button>";
        });
      });
      document.getElementById("indlist").innerHTML = html || "<div class=irow><span>No match</span></div>";
      document.querySelectorAll("#indlist [data-add]").forEach(function (b) {
        b.onclick = function (e) {
          if (e.target.getAttribute("data-star") != null || e.target.closest("[data-star]")) return;
          if (e.target.getAttribute("data-help") != null || e.target.closest("[data-help]")) return;
          var osc = b.getAttribute("data-osc") === "1";
          var id = b.getAttribute("data-add");
          if (id === "vol" || id === "voltape") {
            if (window.jhSetVol) window.jhSetVol(true);
            else { window.volOn = true; paintNow(); }
            if (id === "vol") { draw(); return; }
          }
          var item = (osc ? window.OSC : window.INDS).find(function (x) { return x.id === id; });
          if (!item) return;
          item.on = true; item.hide = false;
          paintNow();
          draw();
        };
      });
      document.querySelectorAll("#indlist [data-help]").forEach(function (b) {
        b.onclick = function (e) {
          e.stopPropagation();
          if (window.jhInduxHelp) window.jhInduxHelp(b.getAttribute("data-help"));
        };
      });
      document.querySelectorAll("#indlist [data-star]").forEach(function (b) {
        b.onclick = function (e) {
          e.stopPropagation();
          star(b.getAttribute("data-star"));
          draw();
        };
      });
    }
    d.querySelectorAll("[data-tab]").forEach(function (b) {
      b.onclick = function () { dlgTab = b.getAttribute("data-tab"); draw(); };
    });
    document.getElementById("indq2").oninput = draw;
    document.getElementById("indx2").onclick = function () { d.className = ""; };
    d.onclick = function (e) { if (e.target === d) d.className = ""; };
    document.getElementById("indq2").focus();
    draw();
  };

  window.jhInduxSet = function (item, isOsc, ctx) {
    if (!item) return;
    var d = el("indset");
    var tab = "in";
    d.className = "on";
    function draw() {
      d.innerHTML = "<div class=box><div class=sh><span>" + item.n + "</span><button type=button id=setx>×</button></div>" +
        "<div class=stabs>" +
          "<button type=button data-t=in class='" + (tab === "in" ? "on" : "") + "'>Inputs</button>" +
          "<button type=button data-t=st class='" + (tab === "st" ? "on" : "") + "'>Style</button>" +
          "<button type=button data-t=vi class='" + (tab === "vi" ? "on" : "") + "'>Visibility</button>" +
        "</div><div class=sbody id=setbody></div>" +
        "<div class=sfoot><button type=button class=cancel id=setc>Cancel</button><button type=button class=ok id=setok>Ok</button></div></div>";
      var body = document.getElementById("setbody");
      if (tab === "in") {
        var extra = "";
        if (item.id === "rsi" || item.id === "stoch" || item.id === "stochrsi" || item.id === "mfi" || item.id === "bbp" || item.id === "dem" || item.id === "rngpos" || item.id === "htrsi") {
          extra = "<div class=srow><span>Overbought</span><input id=sob type=number min=50 max=99 value='" + (item.ob != null ? item.ob : 70) + "'></div>" +
            "<div class=srow><span>Oversold</span><input id=sos type=number min=1 max=50 value='" + (item.os != null ? item.os : 30) + "'></div>";
        }
        if (item.id === "macd") {
          extra = "<div class=srow><span>Fast EMA</span><input id=sp type=number min=2 max=50 value='" + (item.p || 12) + "'></div>" +
            "<div class=srow><span>Slow EMA</span><input id=sp2 type=number min=2 max=80 value='" + (item.p2 || 26) + "'></div>" +
            "<div class=srow><span>Signal</span><input id=sp3 type=number min=2 max=40 value='" + (item.p3 || 9) + "'></div>";
        } else if (item.k === "bb" || item.k === "avgdev" || item.k === "seb" || item.k === "atrb" || item.id === "bbw" || item.id === "bbsqz" || item.id === "bbp") {
          extra += "<div class=srow><span>StdDev</span><input id=smult type=number min=0.5 max=5 step=0.1 value='" + (item.mult || 2) + "'></div>";
        }
        body.innerHTML =
          (item.id === "macd" ? extra :
            "<div class=srow><span>Length</span><input id=sp type=number min=1 max=500 value='" + (item.p || (isOsc ? 14 : 20)) + "'></div>" + extra) +
          "<div class=srow><span>Source</span><select id=ssrc><option>close</option><option>open</option><option>hl2</option><option>hlc3</option></select></div>";
      } else if (tab === "st") {
        body.innerHTML =
          "<div class=srow><span>Color</span><input id=sc type=color value='" + ((item.c && item.c[0] === "#") ? item.c : "#2962ff") + "'></div>" +
          "<div class=srow><span>Line width</span><input id=sw type=number min=1 max=4 value='" + (item.w || 1) + "'></div>" +
          "<div class=srow><span>Visible</span><input id=svis type=checkbox " + (item.hide ? "" : "checked") + "></div>";
      } else {
        body.innerHTML = "<div class=srow><span>Visible on all intervals</span><input id=sall type=checkbox checked></div>";
      }
      d.querySelectorAll("[data-t]").forEach(function (b) {
        b.onclick = function () { tab = b.getAttribute("data-t"); draw(); };
      });
      document.getElementById("setx").onclick = document.getElementById("setc").onclick = function () { d.className = ""; };
      document.getElementById("setok").onclick = function () {
        var p = document.getElementById("sp");
        var p2 = document.getElementById("sp2");
        var p3 = document.getElementById("sp3");
        var ob = document.getElementById("sob");
        var os = document.getElementById("sos");
        var mu = document.getElementById("smult");
        var c = document.getElementById("sc");
        var w = document.getElementById("sw");
        var vis = document.getElementById("svis");
        if (p) {
          var n = +p.value;
          if (n > 1) {
            item.p = n;
            if (item.id !== "macd") item.n = String(item.n).replace(/\d+/, String(n));
          }
        }
        if (p2) item.p2 = +p2.value || item.p2;
        if (p3) item.p3 = +p3.value || item.p3;
        if (item.id === "macd" && item.p && item.p2 && item.p3) item.n = "MACD " + item.p + "," + item.p2 + "," + item.p3;
        if (ob) item.ob = +ob.value;
        if (os) item.os = +os.value;
        if (mu) item.mult = +mu.value || 2;
        if (c) item.c = c.value;
        if (w) item.w = +w.value || 1;
        if (vis) item.hide = !vis.checked;
        d.className = "";
        paintNow();
      };
    }
    d.onclick = function (e) { if (e.target === d) d.className = ""; };
    draw();
  };

  function bindWatch() {
    var w = document.getElementById("watch");
    if (!w || w.querySelector(".w-split")) return;
    var hs = document.createElement("div");
    hs.className = "w-split";
    w.insertBefore(hs, w.firstChild);
    hs.onmousedown = function (e) {
      e.preventDefault();
      hs.classList.add("drag");
      var x0 = e.clientX, w0 = w.getBoundingClientRect().width;
      function mv(ev) {
        var nw = Math.max(180, Math.min(560, w0 - (ev.clientX - x0)));
        w.style.width = nw + "px";
        w.style.minWidth = nw + "px";
        w.style.maxWidth = nw + "px";
      }
      function up() {
        hs.classList.remove("drag");
        document.removeEventListener("mousemove", mv);
        document.removeEventListener("mouseup", up);
      }
      document.addEventListener("mousemove", mv);
      document.addEventListener("mouseup", up);
    };
  }

  window.jhInduxBindOsc = function (wrap) {
    if (!wrap) return;
    wrap.querySelectorAll(".pane-split").forEach(function (sp) {
      if (sp.dataset.bound) return;
      sp.dataset.bound = "1";
      sp.onmousedown = function (e) {
        e.preventDefault();
        sp.classList.add("drag");
        var pane = sp.nextElementSibling;
        if (!pane || !pane.classList.contains("osc")) return;
        var y0 = e.clientY, h0 = pane.getBoundingClientRect().height;
        var id = pane.getAttribute("data-oid");
        function mv(ev) {
          var nh = Math.max(72, Math.min(420, h0 - (ev.clientY - y0)));
          pane.style.height = nh + "px";
          var o = (window.OSC || []).find(function (x) { return x.id === id; });
          if (o) o.h = nh;
        }
        function up() {
          sp.classList.remove("drag");
          document.removeEventListener("mousemove", mv);
          document.removeEventListener("mouseup", up);
        }
        document.addEventListener("mousemove", mv);
        document.addEventListener("mouseup", up);
      };
    });
  };

  window.jhInduxHelp = function (id) {
    id = String(id || "");
    var TAPE = {
      capit: ["CAPIT · Capitulation", "Volume", "Wide-range down bar, close in the lower ~20% of the range, volume ≥1.75× the 20-bar average. Supply is dumping into the close.", "Not a buy by itself. It is the first evidence that a selling climax *may* be forming. Wait for a reversal bar or a spring before treating it as demand.", "Wyckoff selling climax / VSA stopping volume cousin. Bloomberg tape: climactic print on the down tape."],
      hugebuy: ["HUGE · Huge buying", "Volume", "Up bar, close in the upper ~72% of the range, volume ≥1.55× 20-bar average. Aggressive demand lifting offers.", "Strength, not a guarantee of continuation. If the next bar gives it back on equal volume, it was a bull trap.", "Effort with result. The opposite of effort-vs-result."],
      sc: ["SC · Selling climax", "Volume", "Down close, elevated volume (≥1.45×), close still in the lower 38% — panic but not a full capitulation wipe.", "Classic Wyckoff SC. Often followed by an automatic rally (AR). The low of this bar is a candidate spring line.", "Mark the low. Next test of that low on lighter volume is the trade."],
      bc: ["BC · Buying climax", "Volume", "Up close, elevated volume, close in the upper 62%+. Late demand chasing highs.", "Distribution risk. The high of this bar is a candidate upthrust line. Do not buy strength here without a higher-timeframe bias.", "Wyckoff BC. Opposite of SC."],
      sv: ["SV · Stopping volume", "Volume", "Heavy volume down bar that *closes up in the range* (≥55%). Selling came in and was absorbed.", "Demand is present under the close. A follow-through up bar confirms; a next-day dump means absorption failed.", "VSA stopping volume. One of the highest-quality tape tells."],
      abs: ["ABS · Absorption", "Volume", "High volume, small body (≤34% of range), tight spread. Large size transacted without price going anywhere.", "Someone is taking the other side of the crowd. Direction is given by the next range expansion, not this bar.", "Professional absorption. Combine with location (at PDH/VAH vs PDL/VAL)."],
      hb: ["HB · Hidden buying", "Volume", "Down bar that still closes in the upper 62% on ≥1.28× volume. Offers were lifted into a red print.", "Demand is underneath. Often precedes a reversal if it prints at support (PDL, VWAP, weekly SMA).", "VSA upthrust-of-demand on a down close."],
      hs: ["HS · Hidden selling", "Volume", "Up bar that closes in the lower 38% on ≥1.28× volume. Bids were hit into a green print.", "Supply is overhead. Dangerous late in a rally, especially at PDH / weekly high.", "VSA up-bar close-off-highs."],
      breakout: ["BO · Confirmed breakout", "Volume", "Close above the prior 20-bar high on ≥1.22× volume, previous close was still inside.", "Confirmed range escape. Failure is a close back inside the 20-bar high on rising volume.", "Donchian break with volume confirmation."],
      evr: ["EvR · Effort vs result", "Volume", "≥1.35× volume but a small body (≤40% of range). A lot of effort, little progress.", "Trend is tiring. At highs it is distribution; at lows it can be absorption. Let location decide.", "Wyckoff effort vs result. The tape's 'warning' print."]
    };
    var STUDY = {
      voltape: ["Volume Tape", "Volume", "Labels climactic volume events on the volume pane: capitulation, huge buying, selling/buying climax, stopping volume, absorption, hidden buying/selling, confirmed breakout, effort vs result.", "One event per bar, highest-priority tag wins. Uses 20-bar local RVOL so recent climaxes still print on a ~200-bar window. Hover a tag or tap ? for the event card.", "Bloomberg-local volume tape, not SIP ticks. Daily warehouse bars, not time-and-sales."],
      vol: ["Volume", "Volume", "Histogram of bar volume, colored by close vs prior close, opacity by 20-bar relative volume. Gold overlay is Vol MA 20.", "RVOL ≥1.6 is elevated; ≥2.5 is climactic. Combine with Volume Tape for named events.", "Standard Bloomberg volume pane."],
      keylv: ["Key Levels", "Levels", "Previous session high/low/close (PDH/PDL/PDC), previous week (PWH/PWL), previous month (PMH/PML), plus developing current-week (CWH/CWL) and current-month (CMH/CML) once that period has bars. NY calendar.", "Desks fade and break these. A close through PDH on huge volume is a different trade than a wick through it. Developing CWH is *this week's* high so far — the active magnet after Monday.", "Bloomberg GIP session levels / floor-trader references."],
      gaps: ["Unfilled Gaps", "Levels", "Gap-up (open above prior high) and gap-down (open below prior low) that have not traded back through the origin. Last 6 unfilled, ≥0.05% of price.", "Unfilled gap-up = leftover demand / support at the prior high. Unfilled gap-down = leftover supply. A later bar's low through a gap-up origin fills it.", "Classic gap-fill map. Earnings and weekend gaps dominate on daily."],
      pvwap: ["Period VWAP", "Volume", "Three session-reset VWAPs: YTD (gold, thick), calendar month (purple), calendar week (cyan). Typical price × volume, NY calendar reset.", "Price above YTD VWAP = average buyer this year is in the money. A close back under YTD VWAP after a long stay above is a regime change, not noise.", "Bloomberg custom VWAP. Chart VWAP (cumulative from first loaded bar) is a different study."],
      athln: ["All-Time High", "Levels", "Running peak of highs, a horizontal at the current ATH, and anchored VWAP from the bar that made that high.", "Drawdown vs ATH is the pain gauge (see Drawdown pane). AVWAP from the high is the volume-weighted path since the peak — reclaiming it is often the first sign of repair.", "Bloomberg GPDD companion."],
      htfma: ["Weekly SMA 10/40", "MA", "Calendar-week bars, SMA 10 and SMA 40, stepped back onto this chart. Institutional swing bias (≈50/200 on daily but Friday-close based).", "Weekly 10 above weekly 40 = higher-timeframe uptrend. Daily noise under a rising weekly 10 is a dip, not a breakdown, until the week closes through it.", "Guppy long-term / fund SMA overlay."],
      struct: ["Market Structure", "Trend", "Confirmed fractal swings labeled HH / HL / LH / LL.", "Uptrend = HH + HL. First LH after a run of HHs is the warning; first LL is the break. Do not mix with unconfirmed 1-bar spikes.", "Market-structure map used with ZigZag and Livermore pivots."],
      rsidiv: ["Divergence", "Momentum", "Regular and hidden RSI divergence, plus MACD-histogram divergence, at fractal swing highs/lows. DIV↑/DIV↓ = regular, hDIV = hidden, mDIV = MACD.", "Regular bullish (price LL, RSI HL) at support is the highest-quality reversal tell this chart will print. Hidden divergence is continuation. Require location (key level, VWAP) — divergence in the middle of a range is noise.", "Standard RSI/MACD divergence. Not a standalone signal."],
      svwaps: ["Swing VWAP", "Volume", "Anchored VWAP from the last confirmed swing high (red) and last swing low (green).", "AVWAP from the last low is the volume-weighted cost of the current rally. Holding it = dip-buyers in control. AVWAP from the last high is the cost of the current decline.", "Anchored VWAP from structure, not from an arbitrary click."],
      earn: ["Earnings / Events", "Events", "Pins from the live JustHodl catalyst calendar and earnings tracker: BEAT / MISS on reported EPS, EPS for upcoming prints, plus market events FOMC, options witching, rebalance, auction.", "Coverage is the warehouse universe (watchlist + recent filers), not every ticker. SPY still gets FOMC/witching. Surprise % is Benzinga via the tracker — a BEAT that sells off is labeled BEAT, not a buy.", "Real calendar only. Empty on a name means the warehouse has no dated event, not a bug."],
      ddown: ["Drawdown", "Risk", "Percent from the running all-time (or series) high. Underwater histogram, 0% at highs.", "SPY at −2% is a dip. A name at −35% with Range Position still falling is not. Pair with Alpha vs SPY so you know whether the pain is idiosyncratic.", "Bloomberg GPDD."],
      alpha: ["Alpha vs SPY", "Stats", "Cumulative residual after rolling 60-bar beta vs SPY, indexed to 100. Above 100 = beating the hedge after beta.", "This is not annualized Jensen alpha; it is the path of leftover return. SPY vs SPY stays at 100. A stock with RS rising but alpha flat is just high-beta.", "Residual / beta-adjusted relative strength."],
      rngpos: ["Range Position", "Stats", "Close as % of the trailing 252-bar (≈52-week) high–low. 100 = at the high, 0 = at the low. Default bands 80 / 20.", "90%+ is extended, not 'strong' by itself. Mean-reversion setups live at the extremes; trend-following setups live on the push through 80 with volume.", "Bloomberg 52-week range position."],
      htrsi: ["Weekly RSI", "Momentum", "RSI 14 computed on calendar-week bars, then stepped onto this chart. Slow, fund-level momentum.", "Weekly RSI still <50 while daily RSI is 70 is a rally in a downtrend. Weekly RSI reclaiming 50 is often the real turn.", "Higher-timeframe RSI overlay as a pane."],
      rsi: ["RSI 14", "Momentum", "Wilder RSI. Default 70/30 bands, 50 midline. Length and OB/OS are editable in Settings.", "Overbought is not sell. In a weekly uptrend, RSI can sit 60–80 for months. Use with Divergence and Key Levels.", "Wilder 1978. Smoothed, not cutler."],
      macd: ["MACD", "Momentum", "12/26/9 EMA MACD, 4-color histogram (up/down × rising/falling), zero line, signal.", "Histogram shrinking toward zero while price makes a new high is the MACD half of Divergence. Crosses at the zero line carry more weight than crosses at +2%.", "Standard MACD. Fast/slow/signal editable."],
      bb: ["Bollinger Bands", "Channel", "SMA middle (BB Average, thicker) ± StdDev × multiplier. Default 20, 2.", "The middle band is the mean. Width compression is BB Width / Squeeze. %B says where close sits inside the envelope.", "Bollinger 1980s. Pair with BB Width and BB Squeeze."],
      bbw: ["BB Width", "Volatility", "100 × (upper−lower) / middle. Gold = tightening, green = squeeze zone (15th percentile).", "Low width is fuel, not a direction. Direction comes from the squeeze-release bar.", "Volatility regime."],
      bbsqz: ["BB Squeeze", "Volatility", "TTM squeeze: Bollinger inside Keltner, with linear-regression momentum histogram. Dots = squeeze on.", "Squeeze on + momentum flipping through zero is the release. Do not anticipate the side.", "TTM Squeeze (John Carter)."],
      bbp: ["Bollinger %B", "Volatility", "Close as 0–100 inside the Bollinger envelope. 100 = at upper band.", "Walks above 80 in strong trends. Failure to reach 80 on a new price high is divergence inside the band.", "Bollinger %B."],
      vwap: ["VWAP", "Volume", "Cumulative typical-price VWAP from the first loaded bar of this series.", "On daily this is 'chart VWAP', not the NY session. Use Period VWAP for YTD/M/W and Session VWAP on intraday.", "Volume-weighted average price."],
      avwap: ["Session VWAP", "Volume", "VWAP reset each NY calendar day. Meaningful on intraday; on daily it hugs typical price.", "Intraday: the session's cost basis. Daily: prefer Period VWAP.", "NY session VWAP."],
      adx: ["DMI / ADX 14", "Trend", "Wilder +DI, −DI, ADX. ADX 20 line. ADX is 0–100.", "ADX rising above 20 = trend. +DI above −DI = upside. ADX high + DI flip = trend exhaustion risk, not a new trend.", "Wilder DMI. Smoothed as an average, not a sum."],
      avgdev: ["Avg & Deviation", "Stats", "Mean with ±1σ and ±2σ bands.", "Same family as Bollinger; the ±1σ is the value area of close. Mean-reversion at ±2σ, trend when close holds outside ±1σ.", "Bloomberg average & deviation."],
      gdx: ["Golden / Death Cross", "Trend", "SMA 50 / SMA 200 with GOLDEN / DEATH markers on cross.", "Lagging by design. The cross confirms a trend that started weeks earlier. Use Weekly SMA 10/40 for a faster HTF read.", "The classic 50/200."],
      hv: ["Hist Vol 20", "Volatility", "Close-to-close log-return standard deviation, annualized √252, in percent.", "Realized vol. Compare to ATR % (range vol). A vol crush with BB Width low is the squeeze setup.", "Historical / realized volatility."],
      beta: ["Beta vs SPY", "Stats", "Rolling 60-bar slope of this name's returns on SPY returns.", "Beta 1 on SPY is tautology. On a stock, rising beta into a rally means the move is market, not alpha (see Alpha vs SPY).", "OLS beta."],
      rsline: ["RS vs SPY", "Stats", "Price relative to SPY, rebased to 100 at the first aligned bar.", "Rising RS = outperforming. Does not adjust for beta — a 2-beta name will look strong in a bull. Use Alpha for the residual.", "Relative strength line."]
    };
    var pack = TAPE[id] || STUDY[id];
    var title, tag, what, how, cave;
    if (pack) {
      title = pack[0]; tag = pack[1]; what = pack[2]; how = pack[3]; cave = pack[4];
    } else {
      var item = (window.INDS || []).concat(window.OSC || []).find(function (x) { return x.id === id; });
      title = item ? item.n : id;
      tag = item ? (item.cat || "Study") : "Study";
      what = "A chart study on this engine. Open Settings on the legend chip to change length, bands and color.";
      how = "Add it from Indicators, then use eye / settings / remove on the legend like TradingView.";
      cave = "Computed from the bars on this chart. No broker advice.";
    }
    var d = el("indhelp");
    d.className = "on";
    var evHtml = "";
    if (id === "voltape") {
      evHtml = "<h5>EVENTS ON THE PANE</h5><div class=evlist>" +
        Object.keys(TAPE).map(function (k) {
          return "<button type=button data-kind='" + k + "'><b>" + TAPE[k][0].split("·")[0].trim() + "</b><span>" + TAPE[k][0].split("·")[1].trim() + "</span><span class=qhelp>?</span></button>";
        }).join("") + "</div>";
    }
    d.innerHTML = "<div class=box><div class=sh><div><b>" + title + "</b><span class=tag>" + tag.toUpperCase() + "</span></div><button type=button class=x id=helpx>×</button></div>" +
      "<div class=hb>" +
        "<h5>WHAT IT IS</h5><p>" + what + "</p>" +
        "<h5>HOW TO READ IT</h5><p>" + how + "</p>" +
        evHtml +
        "<div class=caveat>" + cave + "</div>" +
      "</div></div>";
    document.getElementById("helpx").onclick = function () { d.className = ""; };
    d.onclick = function (e) { if (e.target === d) d.className = ""; };
    d.querySelectorAll("[data-kind]").forEach(function (b) {
      b.onclick = function (e) { e.stopPropagation(); window.jhInduxHelp(b.getAttribute("data-kind")); };
    });
  };

  window.addEventListener("keydown", function (e) {
    if (e.key === "Escape") {
      var a = document.getElementById("inddlg"); if (a) a.className = "";
      var b = document.getElementById("indset"); if (b) b.className = "";
      var c = document.getElementById("indhelp"); if (c) c.className = "";
    }
  });

  function boot() { bindWatch(); }
  if (document.readyState === "complete") boot();
  else window.addEventListener("load", boot);
  setTimeout(boot, 600);
})();
