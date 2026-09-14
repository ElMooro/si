/* jh-reskin-skip */
/* Supercharts rail: hover watchlist, Chart-menu company info, Chart Pro workspace overlays, compare chips. */
(function () {
  if (window.__jhTvRail) return;
  window.__jhTvRail = true;

  var PIN_KEY = "jh-chart-watch-pin";
  var pinned = false;
  try { pinned = localStorage.getItem(PIN_KEY) === "1"; } catch (e) {}
  var hideT = null;
  var infoDest = "card";

  var css = document.createElement("style");
  css.id = "jh-tvrail-css";
  css.textContent = [
    "#wedge{position:absolute;right:0;top:76px;bottom:28px;width:18px;z-index:22;display:flex;align-items:center;justify-content:center;cursor:pointer;background:transparent;color:#787b86}",
    "#wedge:hover,#wedge.on{background:rgba(41,98,255,.16);color:#d1d4dc}",
    "#wedge b{writing-mode:vertical-rl;transform:rotate(180deg);font:600 10px IBM Plex Sans,system-ui,sans-serif;letter-spacing:.14em}",
    "#watch.is-collapsed{width:0!important;min-width:0!important;max-width:0!important;border:0!important;overflow:hidden;padding:0;opacity:0;pointer-events:none}",
    "#watch.is-open{width:var(--watch-w,320px);min-width:220px;max-width:none;opacity:1;pointer-events:auto;box-shadow:-8px 0 24px rgba(0,0,0,.35)}",
    "#watch.is-pinned{box-shadow:none}",
    "#watch .whead{gap:4px}",
    "#watch .wsearch{flex:1;min-width:0;display:flex;align-items:center;gap:6px;background:#131722;border:1px solid #2a2e39;border-radius:4px;padding:0 8px;height:28px}",
    "#watch .wsearch input{flex:1;min-width:0;font-size:12px;background:transparent;color:#d1d4dc;outline:none}",
    "#w-chartm,#w-pin,#w-searchbtn{width:26px;height:26px;border-radius:4px;color:#787b86;flex:none}",
    "#w-chartm:hover,#w-pin:hover,#w-searchbtn:hover,#w-pin.on{background:#2a2e39;color:#d1d4dc}",
    "#w-pin.on{color:#2962ff}",
    "#chartm{display:none;position:fixed;z-index:80;width:220px;background:#1e222d;border:1px solid #2a2e39;border-radius:6px;box-shadow:0 16px 40px rgba(0,0,0,.5);padding:6px 0;color:#d1d4dc;font-size:13px}",
    "#chartm.on{display:block}",
    "#chartm .lab{padding:8px 14px 4px;font-size:10px;letter-spacing:.1em;color:#787b86}",
    "#chartm button{display:block;width:100%;text-align:left;padding:7px 16px;color:#d1d4dc}",
    "#chartm button:hover{background:#2a2e39}",
    "#tfbar .wsico{width:28px;height:28px;padding:0;display:inline-flex;align-items:center;justify-content:center;color:#787b86;border-radius:4px}",
    "#tfbar .wsico:hover,#tfbar .wsico.on{background:#2a2e39;color:#d1d4dc}",
    "#tfbar .chg{height:26px;padding:0 7px;font-size:11px;letter-spacing:.02em;color:#787b86;border-radius:4px}",
    "#tfbar .chg.on{background:#7c5cea;color:#fff}",
    "#tfbar .chg:hover{color:#d1d4dc}",
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
    "#detail.tvcard{border-top:1px solid #2a2e39;padding:12px 14px;overflow:auto;flex:1;min-height:160px}",
    "#detail.tvcard .nm{font-size:13px;font-weight:600;color:#d1d4dc}",
    "#detail.tvcard .ex{font-size:11px;color:#787b86}",
    "#detail.tvcard .px{font-size:22px;font-weight:600;font-family:IBM Plex Mono,monospace;margin:4px 0 2px}",
    "#detail.tvcard .rg-lab{display:flex;justify-content:space-between;font-size:10px;color:#787b86;margin-top:8px}",
    "#detail.tvcard .news{margin-top:10px;padding:8px 10px;background:#1e222d;border-radius:6px;font-size:12px;line-height:1.35;color:#d1d4dc}",
    "#detail.tvcard .jumps{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}",
    "#detail.tvcard .jumps button{padding:4px 8px;border:1px solid #2a2e39;border-radius:4px;font-size:11px;color:#787b86}",
    "#detail.tvcard .jumps button:hover{border-color:#2962ff;color:#d1d4dc}",
    ".wrow .w-cmp{opacity:0;width:18px;height:18px;border-radius:3px;color:#787b86;font-size:12px;margin-left:2px}",
    ".wrow:hover .w-cmp,.wrow .w-cmp.on{opacity:1}",
    ".wrow .w-cmp:hover,.wrow .w-cmp.on{color:#2962ff;background:#2a2e39}",
    "#symsearch .sbox{background:#1e222d;border-color:#2a2e39}",
    "#symsearch input{font-size:15px}",
    "#fin,#tech,#news,#season,#over,#corr,#notes,#alerts,#cal{max-height:none;flex:1;overflow:auto}"
  ].join("");
  document.documentElement.appendChild(css);

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

  function applyWatch(open) {
    var w = watch();
    if (!w) return;
    w.classList.remove("hide");
    if (open || pinned) {
      w.classList.add("is-open");
      w.classList.remove("is-collapsed");
    } else {
      w.classList.add("is-collapsed");
      w.classList.remove("is-open");
    }
    w.classList.toggle("is-pinned", pinned);
    var wg = document.getElementById("wedge");
    if (wg) wg.className = (open || pinned) ? "on" : "";
    var p = document.getElementById("w-pin");
    if (p) p.className = pinned ? "on" : "";
  }

  window.jhWatchSet = function (open) {
    applyWatch(!!open);
  };

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
    m.style.left = Math.max(8, r.left - 160) + "px";
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

  function boot() {
    var row = document.querySelector("#app .row") || document.getElementById("app");
    if (row && !document.getElementById("wedge")) {
      var wg = document.createElement("div");
      wg.id = "wedge";
      wg.innerHTML = "<b>WATCHLIST</b>";
      row.appendChild(wg);
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
      var head = w.querySelector(".whead");
      if (head && !document.getElementById("w-pin")) {
        var ops = head.querySelector(".wops") || head;
        var sb = document.createElement("button");
        sb.type = "button"; sb.id = "w-searchbtn"; sb.title = "Search symbol"; sb.textContent = "⌕";
        var cm = document.createElement("button");
        cm.type = "button"; cm.id = "w-chartm"; cm.title = "Chart / company info"; cm.textContent = "◆";
        var pn = document.createElement("button");
        pn.type = "button"; pn.id = "w-pin"; pn.title = "Keep watchlist open"; pn.textContent = "📌";
        ops.appendChild(sb); ops.appendChild(cm); ops.appendChild(pn);
      }
      var det = document.getElementById("detail");
      if (det) det.classList.add("tvcard");
    }
    applyWatch(pinned);

    function enter() { clearTimeout(hideT); applyWatch(true); }
    function leave() {
      clearTimeout(hideT);
      hideT = setTimeout(function () { if (!pinned) applyWatch(false); }, 380);
    }
    var wg = document.getElementById("wedge");
    if (wg) {
      wg.onmouseenter = enter;
      wg.onmouseleave = leave;
      wg.onclick = function () { pin(!pinned); };
    }
    if (w) {
      w.onmouseenter = enter;
      w.onmouseleave = leave;
    }
    var pinb = document.getElementById("w-pin");
    if (pinb) pinb.onclick = function () { if (pinned) unpin(); else pin(true); };
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
      if (m && m.className === "on" && !m.contains(e.target) && e.target.id !== "w-chartm") m.className = "";
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
