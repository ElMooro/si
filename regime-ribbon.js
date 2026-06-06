/* regime-ribbon.js — a thin, always-current bond-vol regime ribbon.
 *
 * Usage (one line, anywhere):
 *   <script src="/regime-ribbon.js"></script>
 *   <script>RegimeRibbon.mount();</script>           // fixed bar at top of <body>
 *   RegimeRibbon.mount({ target: 'el-id' });           // or into a specific element
 *   RegimeRibbon.mount({ position: 'inline' });         // inline (not fixed)
 *
 * Pulls data/bond-vol.json via the data proxy and shows the regime, composite z
 * + percentile, risk posture, term-structure lead, and trend. Click → bond-vol page.
 */
(function () {
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var COLORS = {
    CRISIS:       { c: "#ff5577", bg: "rgba(255,85,119,0.14)", label: "CRISIS" },
    ELEVATED:     { c: "#ff9f43", bg: "rgba(255,159,67,0.14)", label: "ELEVATED" },
    NORMAL:       { c: "#fbbf24", bg: "rgba(251,191,36,0.10)", label: "NORMAL" },
    BOND_VOL_LOW: { c: "#22d3ee", bg: "rgba(34,211,238,0.10)", label: "VOL LOW" },
    DATA_UNAVAILABLE: { c: "#6f7b91", bg: "rgba(111,123,145,0.10)", label: "—" },
  };

  function injectCSS() {
    if (document.getElementById("rr-css")) return;
    var s = document.createElement("style");
    s.id = "rr-css";
    s.textContent = [
      ".rr-bar{display:flex;align-items:center;gap:14px;padding:7px 16px;font-family:'IBM Plex Mono',ui-monospace,Menlo,monospace;font-size:11.5px;border-bottom:1px solid rgba(255,255,255,0.06);cursor:pointer;flex-wrap:wrap;background:#0c1018}",
      ".rr-bar.rr-fixed{position:fixed;top:0;left:0;right:0;z-index:9000}",
      ".rr-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;animation:rrpulse 2.4s infinite}",
      "@keyframes rrpulse{0%,100%{opacity:1}50%{opacity:0.4}}",
      ".rr-seg{display:flex;align-items:center;gap:6px;white-space:nowrap}",
      ".rr-k{color:#6f7b91;text-transform:uppercase;letter-spacing:.5px;font-size:9.5px}",
      ".rr-v{color:#e1e8f4;font-weight:700}",
      ".rr-regime{font-weight:800;letter-spacing:.5px}",
      ".rr-sep{width:1px;height:14px;background:rgba(255,255,255,0.08)}",
      ".rr-more{margin-left:auto;color:#6f7b91;font-size:9.5px}",
      ".rr-bar:hover .rr-more{color:#22d3ee}",
      "@media(max-width:680px){.rr-seg.rr-opt{display:none}}",
    ].join("");
    document.head.appendChild(s);
  }

  function fmtZ(z) { return z == null ? "—" : (z >= 0 ? "+" : "") + (+z).toFixed(2) + "σ"; }
  function trendArrow(t) {
    if (!t || t.wow == null) return "";
    if (t.wow > 0.15) return "↑ rising";
    if (t.wow < -0.15) return "↓ easing";
    return "→ flat";
  }

  function render(host, d, opts) {
    var reg = (d && d.regime || "DATA_UNAVAILABLE").toUpperCase();
    var col = COLORS[reg] || COLORS.DATA_UNAVAILABLE;
    var ts = (d && d.term_structure && d.term_structure.signal) ? d.term_structure.signal.split(" - ")[0] : null;
    var posture = d && d.risk_posture;
    var pct = d && d.composite_percentile;
    var bar = document.createElement("div");
    bar.className = "rr-bar" + (opts.position === "fixed" ? " rr-fixed" : "");
    bar.style.background = col.bg;
    bar.title = "Bond-vol regime — click for the full gauge";
    var plumb = d && d.funding_plumbing;
    var plumbCol = { STRESS: "#ff5577", FRAGILE: "#ff9f43", TIGHTENING: "#fbbf24", AMPLE: "#26ffaf" };
    var plumbSeg = "";
    if (plumb && plumb.regime) {
      var pc = plumbCol[plumb.regime] || "#6f7b91";
      var bsd = plumb.balance_sheet_direction;
      var bsdTxt = bsd ? " (" + bsd + (plumb.qt_ended_not_qe ? " · QT≠QE" : "") + ")" : "";
      plumbSeg = '<span class="rr-sep"></span><span class="rr-seg' + (opts.compact ? '' : ' rr-opt') +
        '"><span class="rr-k">Plumbing</span><span class="rr-v" style="color:' + pc + '">' + esc(plumb.regime) + esc(bsdTxt) + '</span></span>';
    }
    bar.innerHTML =
      '<span class="rr-dot" style="background:' + col.c + ';box-shadow:0 0 7px ' + col.c + '"></span>' +
      '<span class="rr-seg"><span class="rr-k">Bond Vol</span><span class="rr-regime" style="color:' + col.c + '">' + col.label + '</span></span>' +
      '<span class="rr-sep"></span>' +
      '<span class="rr-seg"><span class="rr-k">z</span><span class="rr-v">' + fmtZ(d && d.composite_z_score) + (pct != null ? ' · ' + pct + 'p' : '') + '</span></span>' +
      plumbSeg +
      (opts.compact ? '' : (
        (posture ? '<span class="rr-sep"></span><span class="rr-seg rr-opt"><span class="rr-k">Posture</span><span class="rr-v" style="color:' + col.c + '">' + esc(posture) + '</span></span>' : '') +
        (ts ? '<span class="rr-sep rr-opt"></span><span class="rr-seg rr-opt"><span class="rr-k">Lead</span><span class="rr-v">' + esc(ts) + '</span></span>' : '') +
        (d && d.trend && trendArrow(d.trend) ? '<span class="rr-sep rr-opt"></span><span class="rr-seg rr-opt"><span class="rr-k">Trend</span><span class="rr-v">' + trendArrow(d.trend) + '</span></span>' : '') +
        '<span class="rr-more">bond-vol gauge →</span>'));
    if (opts.compact) { bar.style.border = "none"; bar.style.padding = "0 8px"; bar.style.borderRadius = "6px"; bar.title = "Bond-vol regime — click for the full gauge"; }
    bar.onclick = function () { window.location.href = "/bond-vol.html"; };
    host.appendChild(bar);
    // push body down if fixed so it doesn't cover content
    if (opts.position === "fixed") {
      requestAnimationFrame(function () {
        document.body.style.paddingTop = (bar.offsetHeight || 34) + "px";
      });
    }
  }

  function esc(s) { return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }

  function mount(opts) {
    opts = opts || {};
    injectCSS();
    var host;
    if (opts.target) { host = document.getElementById(opts.target); if (!host) return; }
    else { host = document.body; }
    var pos = opts.position || (opts.target ? "inline" : "fixed");
    fetch(PROXY + "/data/bond-vol.json?t=" + Date.now())
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (opts.target) host.innerHTML = "";
        else {
          // prepend to body
          var tmp = document.createElement("div");
          render(tmp, d, { position: pos });
          if (tmp.firstChild) document.body.insertBefore(tmp.firstChild, document.body.firstChild);
          if (pos === "fixed") {
            var b = document.querySelector(".rr-bar.rr-fixed");
            requestAnimationFrame(function () { document.body.style.paddingTop = ((b && b.offsetHeight) || 34) + "px"; });
          }
          return;
        }
        render(host, d, { position: pos, compact: opts.compact });
      })
      .catch(function () {});
  }

  window.RegimeRibbon = { mount: mount };
})();
