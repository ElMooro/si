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
    return ["sma20", "sma50", "sma200", "ema9", "bb", "vwap", "rsi", "macd", "vol", "livermore", "wyckoff", "bbw"];
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
        if (q && s.n.toLowerCase().indexOf(q) < 0 && s.cat.toLowerCase().indexOf(q) < 0 && s.id.indexOf(q) < 0) return false;
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
            "<div><b>" + s.n + "</b><span>" + (s.id === "vol" ? "Histogram · " : s.osc ? "New pane · " : "Overlay · ") + s.cat + "</span></div>" +
            "<span class='star" + (isFav(s.id) ? " on" : "") + "' data-star='" + s.id + "'>" + (isFav(s.id) ? "★" : "☆") + "</span></button>";
        });
      });
      document.getElementById("indlist").innerHTML = html || "<div class=irow><span>No match</span></div>";
      document.querySelectorAll("#indlist [data-add]").forEach(function (b) {
        b.onclick = function (e) {
          if (e.target.getAttribute("data-star") != null || e.target.closest("[data-star]")) return;
          var osc = b.getAttribute("data-osc") === "1";
          var id = b.getAttribute("data-add");
          if (id === "vol") {
            if (window.jhSetVol) window.jhSetVol(true);
            else { window.volOn = true; paintNow(); }
            draw();
            return;
          }
          var item = (osc ? window.OSC : window.INDS).find(function (x) { return x.id === id; });
          if (!item) return;
          item.on = true; item.hide = false;
          paintNow();
          draw();
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
        body.innerHTML =
          "<div class=srow><span>Length</span><input id=sp type=number min=1 max=500 value='" + (item.p || (isOsc ? 14 : 20)) + "'></div>" +
          "<div class=srow><span>Source</span><select id=ssrc><option>close</option><option>open</option><option>hl2</option><option>hlc3</option></select></div>" +
          "<div class=srow><span>Offset</span><input id=soff type=number value='0'></div>";
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
        var c = document.getElementById("sc");
        var w = document.getElementById("sw");
        var vis = document.getElementById("svis");
        if (p) {
          var n = +p.value;
          if (n > 1) {
            item.p = n;
            item.n = String(item.n).replace(/\d+/, String(n));
          }
        }
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

  window.addEventListener("keydown", function (e) {
    if (e.key === "Escape") {
      var a = document.getElementById("inddlg"); if (a) a.className = "";
      var b = document.getElementById("indset"); if (b) b.className = "";
    }
  });

  function boot() { bindWatch(); }
  if (document.readyState === "complete") boot();
  else window.addEventListener("load", boot);
  setTimeout(boot, 600);
})();
