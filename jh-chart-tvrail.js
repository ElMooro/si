/* jh-reskin-skip */
/* Supercharts rail: TV watchlist, icon strip, pane grips, Chart-menu, Chart Pro overlays. */
(function () {
  if (window.__jhTvRail) return;
  window.__jhTvRail = true;

  var PIN_KEY = "jh-chart-watch-pin";
  var WKEY = "jh-chart-watch-w";
  var LKEY = "jh-chart-list-h";
  var pinned = false;
  try { pinned = localStorage.getItem(PIN_KEY) === "1"; } catch (e) {}
  var hideT = null;
  var watchW = 320;
  var listH = 240;
  try {
    var ww = parseInt(localStorage.getItem(WKEY) || "320", 10);
    if (isFinite(ww)) watchW = Math.max(220, Math.min(720, ww));
    var lh = parseInt(localStorage.getItem(LKEY) || "240", 10);
    if (isFinite(lh)) listH = Math.max(96, Math.min(640, lh));
  } catch (e) {}

  var css = document.createElement("style");
  css.id = "jh-tvrail-css";
  css.textContent = [
    ":root{--watch-w:" + watchW + "px;--list-h:" + listH + "px}",
    "#rrail{flex:none;width:40px;background:#1e222d;border-left:1px solid #2a2e39;display:flex;flex-direction:column;align-items:center;padding:6px 0;gap:2px;z-index:24;position:relative}",
    "#rrail button{width:32px;height:32px;border-radius:4px;color:#787b86;font-size:15px;display:inline-flex;align-items:center;justify-content:center}",
    "#rrail button:hover,#rrail button.on{background:#2a2e39;color:#d1d4dc}",
    "#rrail button.on{color:#2962ff}",
    "#watch{position:relative;display:flex;flex-direction:column;background:#131722;border-left:1px solid #2a2e39}",
    "#watch.is-collapsed{width:0!important;min-width:0!important;max-width:0!important;border:0!important;overflow:hidden;padding:0;opacity:0;pointer-events:none}",
    "#watch.is-open{width:var(--watch-w,320px)!important;min-width:220px!important;max-width:none!important;opacity:1;pointer-events:auto;box-shadow:-8px 0 24px rgba(0,0,0,.35)}",
    "#watch.is-pinned{box-shadow:none}",
    "#watch .wtitle{display:flex;align-items:center;height:36px;padding:0 8px 0 12px;border-bottom:1px solid #2a2e39;flex:none;gap:8px}",
    "#watch .wtitle b{flex:1;font-size:13px;font-weight:600;color:#d1d4dc;letter-spacing:.01em}",
    "#watch .whead{gap:4px;padding:6px 8px;flex:none}",
    "#watch .whead select{flex:1;min-width:0;background:#131722;border:1px solid #2a2e39;border-radius:4px;height:26px;padding:0 6px;font-size:12px;color:#d1d4dc}",
    "#watch .wsub{display:flex;border-bottom:1px solid #2a2e39;font-size:12px;font-family:IBM Plex Sans,system-ui,sans-serif;flex:none;letter-spacing:0}",
    "#watch .wsub button{flex:1;padding:8px 4px;color:#787b86;letter-spacing:0;font-weight:500}",
    "#watch .wsub button.on{color:#d1d4dc;border-bottom:2px solid #2962ff;font-weight:600}",
    "#wtabs{display:none}",
    "#w-list{flex:none;height:var(--list-h,240px);min-height:96px;overflow:hidden;display:flex;flex-direction:column}",
    "#wlist{flex:1;overflow:auto;min-height:0}",
    "#wd-split{height:8px;cursor:ns-resize;flex:none;position:relative;background:transparent;z-index:8}",
    "#wd-split::after{content:'';position:absolute;left:50%;top:2px;width:40px;height:4px;margin-left:-20px;border-radius:2px;background:#2a2e39}",
    "#wd-split:hover,#wd-split.drag{background:rgba(41,98,255,.18)}",
    "#wd-split:hover::after,#wd-split.drag::after{background:#2962ff}",
    ".w-split{position:absolute;left:0;top:0;bottom:0;width:8px;cursor:ew-resize;z-index:14;background:transparent}",
    ".w-split::after{content:'';position:absolute;left:2px;top:50%;width:4px;height:40px;margin-top:-20px;border-radius:2px;background:#2a2e39}",
    ".w-split:hover,.w-split.drag{background:rgba(41,98,255,.18)}",
    ".w-split:hover::after,.w-split.drag::after{background:#2962ff}",
    "#w-stack{flex:1;min-height:140px;overflow:auto;display:flex;flex-direction:column}",
    "#w-stack>#detail,#w-stack>#fin,#w-stack>#over,#w-stack>#tech,#w-stack>#season,#w-stack>#notes,#w-stack>#alerts,#w-stack>#cal,#w-stack>#news{flex:none;max-height:none;overflow:visible;border-top:1px solid #2a2e39;padding:10px 14px}",
    "#detail.tvcard{border-top:0;padding:12px 14px}",
    "#detail.tvcard .nm{font-size:13px;font-weight:600;color:#d1d4dc}",
    "#detail.tvcard .ex{font-size:11px;color:#787b86}",
    "#detail.tvcard .px{font-size:22px;font-weight:600;font-family:IBM Plex Mono,monospace;margin:4px 0 2px}",
    "#detail.tvcard .rg-lab{display:flex;justify-content:space-between;font-size:10px;color:#787b86;margin-top:8px}",
    "#detail.tvcard .news{margin-top:10px;padding:8px 10px;background:#1e222d;border-radius:6px;font-size:12px;line-height:1.35;color:#d1d4dc}",
    "#detail.tvcard .jumps{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}",
    "#detail.tvcard .jumps button{padding:4px 8px;border:1px solid #2a2e39;border-radius:4px;font-size:11px;color:#787b86}",
    "#detail.tvcard .jumps button:hover{border-color:#2962ff;color:#d1d4dc}",
    ".cols,.wrow{grid-template-columns:4px 1fr 64px 54px 58px!important;padding:5px 10px;align-items:center}",
    ".wrow{position:relative;width:100%;text-align:left;border-left:0}",
    ".wrow .wacc{width:4px;height:100%;position:absolute;left:0;top:0;background:#2a2e39}",
    ".wrow.on{background:#2a2e39}",
    ".wrow.on .wacc{background:#2962ff}",
    ".wrow .wsym{font-weight:600;color:#d1d4dc;padding-left:8px}",
    ".wrow span:nth-child(n+3){text-align:right;font-variant-numeric:tabular-nums}",
    ".wrow .w-cmp{position:absolute;right:4px;top:50%;transform:translateY(-50%);opacity:0;width:18px;height:18px;border-radius:3px;color:#787b86;font-size:12px}",
    ".wrow:hover .w-cmp,.wrow .w-cmp.on{opacity:1}",
    ".wrow:hover .w-cmp,.wrow .w-cmp.on{color:#2962ff;background:#131722}",
    ".addsym{margin:4px 10px 8px;padding:6px;border:0;color:#2962ff;font-size:12px;text-align:left;width:auto}",
    ".letters{padding:4px 8px;gap:1px}",
    ".letters button{width:16px;height:16px;font-size:9px}",
    "#w-chartm,#w-pin,#w-searchbtn,#w-close{width:26px;height:26px;border-radius:4px;color:#787b86;flex:none}",
    "#w-chartm:hover,#w-pin:hover,#w-searchbtn:hover,#w-close:hover,#w-pin.on{background:#2a2e39;color:#d1d4dc}",
    "#w-pin.on{color:#2962ff}",
    "#chartm{display:none;position:fixed;z-index:80;width:220px;background:#1e222d;border:1px solid #2a2e39;border-radius:6px;box-shadow:0 16px 40px rgba(0,0,0,.5);padding:6px 0;color:#d1d4dc;font-size:13px}",
    "#chartm.on{display:block}",
    "#chartm .lab{padding:8px 14px 4px;font-size:10px;letter-spacing:.1em;color:#787b86}",
    "#chartm button{display:block;width:100%;text-align:left;padding:7px 16px;color:#d1d4dc}",
    "#chartm button:hover{background:#2a2e39}",
    "#tfbar .wsico{width:34px;height:34px;padding:0;display:inline-flex;align-items:center;justify-content:center;color:#787b86;border-radius:4px}",
    "#tfbar .wsico:hover,#tfbar .wsico.on{background:#2a2e39;color:#d1d4dc}",
    "#tfbar .chg{height:32px;padding:0 9px;font-size:13px;letter-spacing:.02em;color:#787b86;border-radius:4px}",
    "#tfbar .chg.on{background:#7c5cea;color:#fff}",
    "#tfbar .chg:hover{color:#d1d4dc}",
    "#tabbar,.tabs{height:48px!important;min-height:48px!important}",
    "#tfbar{height:48px!important;min-height:48px!important}",
    ".listbtn{flex:1;min-width:0;display:flex;align-items:center;gap:6px;height:32px;padding:0 8px;text-align:left;background:#131722;border:1px solid #2a2e39;border-radius:4px;font-size:13px;color:#d1d4dc}",
    ".listbtn .n{color:#787b86;flex:none}",
    "#listdrop{display:none;position:fixed;z-index:85;max-height:60vh;background:#1e222d;border:1px solid #2a2e39;border-radius:8px;box-shadow:0 16px 40px rgba(0,0,0,.5);flex-direction:column;overflow:hidden}",
    "#listdrop.on{display:flex}",
    "#symsearch .sbox{background:#1e222d;border-color:#2a2e39;color:#d1d4dc}",
    "#symsearch .shd b{color:#d1d4dc}",
    "#symsearch .ssearch{background:#131722;border-color:#2a2e39}",
    "#symsearch .ss-hit .nm{color:#4c9bff}",
    ".sschips button.on{background:#d1d4dc;color:#131722;border-color:#d1d4dc}",
    ".letters{flex-wrap:nowrap!important;overflow-x:auto}",
    ".filt.wq{border-bottom:1px solid #2a2e39}",
    "#cmpchips{position:absolute;left:10px;top:auto;bottom:36px;z-index:7;display:flex;flex-wrap:wrap;gap:4px;max-width:70%;pointer-events:none}",
    "#cmpchips .chip{pointer-events:auto;display:inline-flex;align-items:center;gap:6px;background:#1e222d;border:1px solid #2a2e39;border-radius:4px;padding:2px 6px 2px 8px;font:11px IBM Plex Sans,system-ui;color:#d1d4dc}",
    "#cmpchips .chip i{width:8px;height:8px;border-radius:1px}",
    "#cmpchips .chip button{width:16px;height:16px;color:#787b86}",
    "#ws-overlay{display:none;position:fixed;inset:0;z-index:90;background:rgba(0,0,0,.55);padding:3vh 3vw}",
    "#ws-overlay.on{display:flex;flex-direction:column}",
    "#ws-overlay .wsbox{flex:1;background:#131722;border:1px solid #2a2e39;border-radius:8px;overflow:hidden;display:flex;flex-direction:column;min-height:0}",
    "#ws-overlay .wshd{display:flex;align-items:center;gap:10px;height:40px;padding:0 12px;background:#1e222d;border-bottom:1px solid #2a2e39;color:#d1d4dc;font-weight:600}",
    "#ws-overlay .wshd a{margin-left:auto;color:#787b86;font-size:12px;font-weight:500}",
    "#ws-overlay .wshd .x{width:32px;height:32px;color:#787b86;font-size:20px}",
    "#ws-overlay iframe,#ws-overlay #ws-corr{flex:1;border:0;width:100%;min-height:0;background:#131722}",
    "#symsearch .sbox{background:#1e222d;border-color:#2a2e39}",
    "#symsearch input{font-size:15px}",
    ".pane-split{height:8px!important;cursor:ns-resize;background:transparent;flex:none;position:relative;z-index:9}",
    ".pane-split::after{content:'';position:absolute;left:50%;top:2px;width:40px;height:4px;margin-left:-20px;border-radius:2px;background:#2a2e39}",
    ".pane-split:hover,.pane-split.drag{background:rgba(41,98,255,.18)}",
    ".pane-split:hover::after,.pane-split.drag::after{background:#2962ff}",
    ".leg-dia{width:18px;height:18px;margin-right:4px;color:#787b86;border-radius:3px;font-size:12px;pointer-events:auto}",
    ".leg-dia:hover{background:#2a2e39;color:#d1d4dc}",
    "#legend .leg-sym{display:flex;align-items:center;gap:4px}",
    "@media(max-width:720px){#rrail{display:none!important}}"
  ].join("");
  document.documentElement.appendChild(css);
  document.documentElement.style.setProperty("--watch-w", watchW + "px");
  document.documentElement.style.setProperty("--list-h", listH + "px");

  function el(id, html, tag) {
    var n = document.getElementById(id);
    if (!n) {
      n = document.createElement(tag || "div");
      n.id = id;
      document.body.appendChild(n);
    }
    if (html != null) n.innerHTML = html;
    return n;
  }

  function watch() { return document.getElementById("watch"); }

  function setWatchW(px) {
    watchW = Math.max(220, Math.min(720, px | 0));
    document.documentElement.style.setProperty("--watch-w", watchW + "px");
    try { localStorage.setItem(WKEY, String(watchW)); } catch (e) {}
    var w = watch();
    if (w && w.classList.contains("is-open")) {
      w.style.setProperty("width", watchW + "px", "important");
      w.style.setProperty("min-width", watchW + "px", "important");
    }
  }

  function setListH(px) {
    listH = Math.max(96, Math.min(640, px | 0));
    document.documentElement.style.setProperty("--list-h", listH + "px");
    try { localStorage.setItem(LKEY, String(listH)); } catch (e) {}
    var box = document.getElementById("w-list");
    if (box) box.style.height = listH + "px";
  }

  function applyWatch(open) {
    var w = watch();
    if (!w) return;
    w.classList.remove("hide");
    if (open || pinned) {
      w.classList.add("is-open");
      w.classList.remove("is-collapsed");
      w.style.setProperty("width", watchW + "px", "important");
      w.style.setProperty("min-width", "220px", "important");
    } else {
      w.classList.add("is-collapsed");
      w.classList.remove("is-open");
      w.style.setProperty("width", "0px", "important");
      w.style.setProperty("min-width", "0px", "important");
    }
    w.classList.toggle("is-pinned", pinned);
    var p = document.getElementById("w-pin");
    if (p) p.className = pinned ? "on" : "";
    document.querySelectorAll("#rrail [data-rail]").forEach(function (b) {
      b.classList.toggle("on", (open || pinned) && b.getAttribute("data-rail") === "watch");
    });
  }

  window.jhWatchSet = function (open) {
    applyWatch(!!open);
  };
  window.jhWatchPin = pin;
  window.jhWatchUnpin = unpin;

  function pin(on) {
    pinned = on !== false;
    try { localStorage.setItem(PIN_KEY, pinned ? "1" : "0"); } catch (e) {}
    applyWatch(true);
  }

  function unpin() {
    pinned = false;
    try { localStorage.setItem(PIN_KEY, "0"); } catch (e) {}
    applyWatch(false);
  }

  window.jhOpenWorkspace = function (kind) {
    var ov = el("ws-overlay");
    var title = kind === "macro" ? "Macro & Economic Data" : kind === "heat" ? "Universe Heatmap" : kind === "corr" ? "Correlation Matrix · ALERT" : "Alert Center";
    var href = kind === "macro" ? "/macro-economic-data.html" : kind === "heat" ? "/universe-heatmap.html" : kind === "corr" ? "/correlation.html" : "/alerts.html";
    ov.className = "on";
    ov.innerHTML = "<div class=wsbox><div class=wshd><span>" + title + "</span><a href='" + href + "' target=_blank rel=noopener>Open page ↗</a><button type=button class=x id=wsx>×</button></div>" +
      (kind === "corr" ? "<div id=ws-corr></div>" : "<iframe src='" + href + "' title='" + title + "'></iframe>") +
      "</div>";
    document.getElementById("wsx").onclick = function () { ov.className = ""; };
    ov.onclick = function (e) { if (e.target === ov) ov.className = ""; };
    if (kind === "corr") {
      var host = document.getElementById("ws-corr");
      host.style.padding = "16px";
      host.style.overflow = "auto";
      host.innerHTML = "<div style='padding:16px;color:#787b86'>Computing watchlist correlations…</div>";
      if (window.jhRenderCorr) {
        Promise.resolve(window.jhRenderCorr()).then(function () {
          var src = document.getElementById("corr");
          if (src) host.innerHTML = src.innerHTML;
        });
      } else {
        host.innerHTML = "<iframe src='/correlation.html' style='width:100%;height:100%;border:0'></iframe>";
      }
    }
  };

  window.jhTvChips = function (list, colors) {
    var host = document.getElementById("cmpchips");
    if (!host) return;
    list = list || [];
    colors = colors || ["#2962ff", "#089981", "#f23645", "#ff6d00", "#ab47bc"];
    host.innerHTML = list.map(function (s, i) {
      return "<span class=chip><i style=background:" + colors[(i + 3) % colors.length] + "></i>" + s +
        "<button type=button data-x='" + s + "' title='Remove overlay'>×</button></span>";
    }).join("");
    host.querySelectorAll("[data-x]").forEach(function (b) {
      b.onclick = function () { if (window.jhDelCompare) window.jhDelCompare(b.getAttribute("data-x")); };
    });
  };

  function chartMenu(at) {
    var m = el("chartm");
    m.className = "on";
    m.innerHTML =
      "<button data-d=chart>Chart</button>" +
      "<div class=lab>FUNDAMENTALS</div>" +
      "<button data-d=fin>Financials</button>" +
      "<button data-d=over>Overview</button>" +
      "<div class=lab>ANALYSIS</div>" +
      "<button data-d=tech>Technicals</button>" +
      "<button data-d=season>Seasonals</button>" +
      "<button data-d=news>News</button>" +
      "<button data-d=notes>Notes</button>" +
      "<div class=lab>ASSETS</div>" +
      "<button data-d=qr>Tape / QR</button>" +
      "<button data-d=heat>Heatmap</button>" +
      "<button data-d=corr>Correlation</button>";
    var r = at.getBoundingClientRect();
    m.style.left = Math.max(8, Math.min(window.innerWidth - 230, r.left - 40)) + "px";
    m.style.top = (r.bottom + 4) + "px";
    m.querySelectorAll("[data-d]").forEach(function (b) {
      b.onclick = function () {
        m.className = "";
        var d = b.getAttribute("data-d");
        pin(true);
        if (window.jhShowInfo) window.jhShowInfo(d);
      };
    });
  }
  window.jhChartMenu = chartMenu;

  function bindSplit(node, axis, onMove, onDbl) {
    if (!node || node.dataset.bound) return;
    node.dataset.bound = "1";
    node.onmousedown = function (e) {
      if (e.button !== 0) return;
      e.preventDefault();
      e.stopPropagation();
      node.classList.add("drag");
      pin(true);
      var x0 = e.clientX, y0 = e.clientY;
      function mv(ev) { onMove(ev, x0, y0); }
      function up() {
        node.classList.remove("drag");
        document.removeEventListener("mousemove", mv);
        document.removeEventListener("mouseup", up);
      }
      document.addEventListener("mousemove", mv);
      document.addEventListener("mouseup", up);
    };
    node.ondblclick = function (e) {
      e.preventDefault();
      if (onDbl) onDbl();
    };
  }

  function bindResizers() {
    var w = watch();
    if (!w) return;
    var hs = document.getElementById("w-split") || w.querySelector(".w-split");
    if (!hs) {
      hs = document.createElement("div");
      hs.id = "w-split";
      hs.className = "w-split";
      hs.title = "Drag to resize watchlist";
      w.insertBefore(hs, w.firstChild);
    }
    bindSplit(hs, "ew", function (ev, x0) {
      setWatchW(watchW - (ev.clientX - x0));
      x0 = ev.clientX;
      hs._x0 = x0;
    }, function () { setWatchW(320); });
    hs.onmousedown = function (e) {
      if (e.button !== 0) return;
      e.preventDefault();
      e.stopPropagation();
      hs.classList.add("drag");
      pin(true);
      var x0 = e.clientX, w0 = watchW;
      function mv(ev) { setWatchW(w0 - (ev.clientX - x0)); }
      function up() {
        hs.classList.remove("drag");
        document.removeEventListener("mousemove", mv);
        document.removeEventListener("mouseup", up);
      }
      document.addEventListener("mousemove", mv);
      document.addEventListener("mouseup", up);
    };

    var vs = document.getElementById("wd-split");
    if (vs) {
      vs.onmousedown = function (e) {
        if (e.button !== 0) return;
        e.preventDefault();
        e.stopPropagation();
        vs.classList.add("drag");
        pin(true);
        var y0 = e.clientY, h0 = listH;
        function mv(ev) { setListH(h0 + (ev.clientY - y0)); }
        function up() {
          vs.classList.remove("drag");
          document.removeEventListener("mousemove", mv);
          document.removeEventListener("mouseup", up);
        }
        document.addEventListener("mousemove", mv);
        document.addEventListener("mouseup", up);
      };
      vs.ondblclick = function () { setListH(240); };
    }
  }

  function boot() {
    var row = document.querySelector("#app .row") || document.getElementById("app");
    var old = document.getElementById("wedge");
    if (old) old.remove();
    if (row && !document.getElementById("rrail")) {
      var rr = document.createElement("aside");
      rr.id = "rrail";
      rr.setAttribute("aria-label", "Panels");
      rr.innerHTML =
        "<button type=button data-rail=watch title='Watchlist'>☰</button>" +
        "<button type=button data-rail=details title='Symbol details'>◆</button>" +
        "<button type=button data-rail=news title='News'>N</button>" +
        "<button type=button data-rail=alerts title='Alerts'>🔔</button>";
      row.appendChild(rr);
    }
    if (!document.getElementById("ws-overlay")) el("ws-overlay");
    if (!document.getElementById("chartm")) el("chartm");
    var chart = document.getElementById("chart");
    if (chart && !document.getElementById("cmpchips")) {
      var chips = document.createElement("div");
      chips.id = "cmpchips";
      chart.appendChild(chips);
    }
    var w = watch();
    if (w) {
      if (!w.querySelector(".wtitle")) {
        var title = document.createElement("div");
        title.className = "wtitle";
        title.innerHTML = "<b>Watchlist</b><div class=wops></div>";
        var head = w.querySelector(".whead");
        w.insertBefore(title, head || w.firstChild);
      }
      var ops = (w.querySelector(".wtitle .wops")) || w.querySelector(".wops") || w.querySelector(".whead");
      function ensureBtn(id, title, text) {
        var b = document.getElementById(id);
        if (b) return b;
        b = document.createElement("button");
        b.type = "button"; b.id = id; b.title = title; b.textContent = text;
        ops.appendChild(b);
        return b;
      }
      ensureBtn("w-searchbtn", "Search symbol", "⌕");
      ensureBtn("w-chartm", "Chart / company info", "◆");
      ensureBtn("w-pin", "Pin watchlist", "📌");
      ensureBtn("w-close", "Close watchlist", "×");
      var det = document.getElementById("detail");
      if (det) det.classList.add("tvcard");
      if (!document.getElementById("w-stack")) {
        var stack = document.createElement("div");
        stack.id = "w-stack";
        var ids = ["detail", "fin", "over", "tech", "season", "notes", "alerts", "cal", "news", "heat", "screen", "trade", "test", "corr", "qr"];
        ids.forEach(function (id) {
          var n = document.getElementById(id);
          if (n) stack.appendChild(n);
        });
        w.appendChild(stack);
      }
      if (!document.getElementById("wd-split")) {
        var vs = document.createElement("div");
        vs.id = "wd-split";
        vs.title = "Drag to resize list / details";
        var list = document.getElementById("w-list");
        if (list && list.parentNode === w) w.insertBefore(vs, list.nextSibling);
        else w.insertBefore(vs, document.getElementById("w-stack"));
      }
      var box = document.getElementById("w-list");
      if (box) box.style.height = listH + "px";
    }
    applyWatch(pinned);
    bindResizers();

    function enter() { clearTimeout(hideT); applyWatch(true); }
    function leave() {
      clearTimeout(hideT);
      hideT = setTimeout(function () {
        var ld = document.getElementById("listdrop");
        var ss = document.getElementById("symsearch");
        if (ld && ld.classList.contains("on")) return;
        if (ss && ss.classList.contains("on")) return;
        if (!pinned) applyWatch(false);
      }, 380);
    }
    var rr = document.getElementById("rrail");
    if (rr) {
      rr.onmouseenter = enter;
      rr.onmouseleave = leave;
      rr.querySelectorAll("[data-rail]").forEach(function (b) {
        b.onclick = function (e) {
          e.stopPropagation();
          var kind = b.getAttribute("data-rail");
          pin(true);
          if (kind === "watch") {
            if (window.jhShowInfo) window.jhShowInfo("chart");
          } else if (window.jhShowInfo) window.jhShowInfo(kind === "details" ? "fin" : kind);
        };
      });
    }
    if (w) {
      w.onmouseenter = enter;
      w.onmouseleave = leave;
    }
    var pinb = document.getElementById("w-pin");
    if (pinb) pinb.onclick = function (e) { e.stopPropagation(); if (pinned) unpin(); else pin(true); };
    var xb = document.getElementById("w-close");
    if (xb) xb.onclick = function (e) { e.stopPropagation(); unpin(); };
    var cmb = document.getElementById("w-chartm");
    if (cmb) cmb.onclick = function (e) { e.stopPropagation(); chartMenu(cmb); };
    var sb = document.getElementById("w-searchbtn");
    if (sb) sb.onclick = function () {
      pin(true);
      if (window.jhOpenSearch) window.jhOpenSearch();
      else { var q = document.getElementById("q"); if (q) q.focus(); }
    };
    document.addEventListener("click", function (e) {
      var m = document.getElementById("chartm");
      if (m && m.className === "on" && !m.contains(e.target) && e.target.id !== "w-chartm" && !(e.target.classList && e.target.classList.contains("leg-dia"))) m.className = "";
    });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        var ov = document.getElementById("ws-overlay");
        if (ov) ov.className = "";
        var m = document.getElementById("chartm");
        if (m) m.className = "";
      }
    });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
  window.addEventListener("load", boot);
})();
