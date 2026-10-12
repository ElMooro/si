/* Bollinger safety and BTC volume. No engine edit.
   alignBeforePaint: scale the coin side before paint reads the bars.
   jhBbFrame: arm Bollinger once, then open on the last 160 bars so the band is readable. */
(function () {
  if (typeof window === "undefined" || window.__jhBbFix) return;
  window.__jhBbFix = 1;
  /* One visit: drop every preloaded study. Volume stays. Later choices still save. */
  try {
    if (localStorage.getItem("jh-only-volume-v1") !== "1") {
      var raw = localStorage.getItem("jh-chart-v12-tv-layout");
      if (raw) {
        var lay = JSON.parse(raw);
        if (lay && lay.inds) lay.inds.forEach(function (i) { if (i) i.on = false; });
        if (lay && lay.osc) lay.osc.forEach(function (o) { if (o) o.on = false; });
        if (lay) lay.volOn = true;
        localStorage.setItem("jh-chart-v12-tv-layout", JSON.stringify(lay));
      }
      localStorage.setItem("jh-desk-pack-v2", JSON.stringify({ avwap: 0, vp: 0, ref: 0 }));
      localStorage.setItem("jh-bb-armed", "1");
      localStorage.setItem("jh-only-volume-v1", "1");
    }
  } catch (eClean) {}
  var css = document.createElement("style");
  css.id = "jh-legend-x";
  css.textContent = "#legend .leg-ops{opacity:1!important}#legend .leg-ops button,#legend .leg-sym [data-act=symx]{display:inline-flex!important;align-items:center;justify-content:center;width:18px;height:18px;margin-left:4px;border-radius:2px;color:#d1d4dc;font:700 14px/16px IBM Plex Sans,sans-serif;pointer-events:auto;background:transparent}#legend .leg-ops button:hover,#legend .leg-sym [data-act=symx]:hover{background:#2a2e39;color:#fff}#legend .leg-row{position:relative;z-index:2}#legend .leg-sym{position:relative;z-index:3}";
  (document.head || document.documentElement).appendChild(css);
  function mountSymX() {
    var host = document.getElementById("legend");
    if (!host) return;
    var sym = host.querySelector(".leg-sym");
    if (!sym) return;
    var hidden = false;
    try { hidden = localStorage.getItem("jh-legend-sym") === "0"; } catch (e) {}
    if (hidden) { sym.style.display = "none"; var wm = document.getElementById("wm"); if (wm) wm.textContent = ""; return; }
    if (sym.querySelector("[data-act=symx]")) return;
    var x = document.createElement("button");
    x.type = "button";
    x.setAttribute("data-act", "symx");
    x.title = "Remove symbol and ticker";
    x.setAttribute("aria-label", "Remove symbol and ticker");
    x.textContent = "\u00d7";
    x.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      try { localStorage.setItem("jh-legend-sym", "0"); } catch (err) {}
      sym.style.display = "none";
      var mark = document.getElementById("wm");
      if (mark) mark.textContent = "";
    });
    sym.appendChild(x);
  }
  document.addEventListener("click", function (e) {
    var btn = e.target && e.target.closest ? e.target.closest("#legend [data-act=x]") : null;
    if (!btn) return;
    var row = btn.closest(".leg-row");
    if (!row || row.getAttribute("data-kind") !== "ov") return;
    var id = row.getAttribute("data-id");
    var inds = window.INDS || [];
    var item = null, i;
    for (i = 0; i < inds.length; i++) if (inds[i] && inds[i].id === id) item = inds[i];
    if (!item) return;
    e.preventDefault();
    e.stopPropagation();
    item.on = false;
    item.hide = false;
    if (id === "vprof" || id === "reflevels" || id === "avwap" || /vwap/i.test(id)) {
      try {
        var st = JSON.parse(localStorage.getItem("jh-desk-pack-v2") || "{}") || {};
        if (id === "vprof") st.vp = 0;
        else if (id === "reflevels") st.ref = 0;
        else st.avwap = 0;
        localStorage.setItem("jh-desk-pack-v2", JSON.stringify(st));
      } catch (err) {}
    }
    try { if (window.jhSaveLay) window.jhSaveLay(); } catch (err2) {}
    try { if (window.paint && window.lastBars && window.lastBars.length) window.paint(window.lastBars); } catch (err3) {}
  }, true);
  function finite(v) { return typeof v === "number" && isFinite(v); }
  function isBtc() {
    var el = document.getElementById("symin");
    var s = String(window.jhActive || window.active || (el && el.value) || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
    return s.indexOf("BTC") === 0 || s.indexOf("XBTC") >= 0;
  }
  function align(d) {
    if (!d || d.length < 40 || d.__jhVol || !isBtc()) return;
    function med(arr) {
      if (!arr || arr.length < 8) return null;
      var s = arr.slice().sort(function (a, c) { return a - c; });
      return s[s.length >> 1];
    }
    function ratios(i0, n) {
      var s = [], k, row, q;
      for (k = i0; k < i0 + n && k < d.length; k++) {
        row = d[k];
        if (row && row.close > 0 && row.volume > 0) {
          q = row.volume / row.close;
          if (finite(q)) s.push(q);
        }
      }
      return med(s);
    }
    function vols(i0, n) {
      var s = [], k, row;
      for (k = i0; k < i0 + n && k < d.length; k++) {
        row = d[k];
        if (row && row.volume > 0) s.push(row.volume);
      }
      return med(s);
    }
    var dollar = [], i, b, q, cut = -1, before, after, pre, post, factor;
    for (i = 0; i < d.length; i++) {
      b = d[i];
      q = b && b.close > 0 && b.volume > 0 ? b.volume / b.close : 0;
      dollar[i] = q >= 100;
    }
    for (i = 30; i < d.length - 20; i++) {
      before = ratios(i - 30, 30);
      after = ratios(i, 20);
      if (before > 1000 && after != null && after < 100) { cut = i; break; }
    }
    if (cut >= 0) {
      for (i = cut; i < d.length; i++) {
        b = d[i];
        if (dollar[i] || !b || !(b.close > 0) || !(b.volume > 0)) continue;
        b.volume = b.volume * b.close;
      }
      pre = vols(Math.max(0, cut - 40), Math.min(40, cut));
      post = vols(cut, 40);
      if (pre > 0 && post > 0 && pre > post * 3) {
        factor = post / pre;
        for (i = 0; i < d.length; i++) {
          if (!dollar[i] || !(d[i].volume > 0)) continue;
          d[i].volume = d[i].volume * factor;
        }
      }
    }
    d.__jhVol = 1;
  }
  function sane(data) {
    if (!data || !data.length) return data || [];
    var out = [], i, p, last = null;
    for (i = 0; i < data.length; i++) {
      p = data[i];
      if (!p || typeof p.time !== "number" || !finite(p.time)) continue;
      if (p.value != null && !finite(p.value)) continue;
      if (last != null && !(p.time > last)) continue;
      out.push(p);
      last = p.time;
    }
    return out;
  }
  function guardSeries(s) {
    if (!s || !s.setData || s.setData.__jh) return;
    var sd = s.setData.bind(s);
    function setData(data) {
      try { return sd(data); }
      catch (e) { try { return sd(sane(data)); } catch (e2) {} }
    }
    setData.__jh = 1;
    s.setData = setData;
  }
  function quietTitle(args) {
    if (!args || !args.length || !args[0] || typeof args[0] !== "object" || typeof args[0].title !== "string") return args;
    var title = args[0].title;
    var sym = String(window.jhActive || window.active || "").toUpperCase();
    var up = title.toUpperCase();
    if (!(/vwap/i.test(title) || (sym && (up === sym || up === sym + " H")))) return args;
    var copy = Array.prototype.slice.call(args);
    copy[0] = Object.assign({}, args[0], { title: "" });
    return copy;
  }
  function hook(chart) {
    if (!chart || chart.__jhBbHook) return false;
    chart.__jhBbHook = 1;
    ["addLineSeries", "addAreaSeries", "addBaselineSeries", "addHistogramSeries", "addCandlestickSeries", "addBarSeries"].forEach(function (name) {
      if (!chart[name] || chart[name].__jhQuiet) return;
      var fn = chart[name].bind(chart);
      var wrapped = function () {
        var s = fn.apply(chart, quietTitle(arguments));
        guardSeries(s);
        return s;
      };
      wrapped.__jhQuiet = 1;
      chart[name] = wrapped;
    });
    return true;
  }
  function wrapCreate() {
    var LC = window.LightweightCharts;
    if (!LC || !LC.createChart || LC.createChart.__jhQuiet) return;
    var orig = LC.createChart.bind(LC);
    var wrapped = function () {
      var chart = orig.apply(LC, arguments);
      try { hook(chart); } catch (e) {}
      return chart;
    };
    wrapped.__jhQuiet = 1;
    LC.createChart = wrapped;
  }
  var framedKey = "";
  function frame(chart) {
    var bars = window.lastBars;
    var key = String(window.jhActive || "") + "|" + String(window.tf || "");
    if (!chart || !chart.timeScale || !bars || bars.length < 40 || framedKey === key) return;
    framedKey = key;
    try {
      chart.timeScale().setVisibleLogicalRange({
        from: Math.max(-0.5, bars.length - 160),
        to: bars.length + 5
      });
    } catch (e) {}
  }
  var seen = null;
  setInterval(function () {
    try { wrapCreate(); } catch (eW) {}
    try { mountSymX(); } catch (eM) {}
    if (window.jhInduxLegend && !window.jhInduxLegend.__symx) {
      var origLeg = window.jhInduxLegend;
      var wrappedLeg = function () {
        var out = origLeg.apply(this, arguments);
        try { mountSymX(); } catch (eL) {}
        return out;
      };
      wrappedLeg.__symx = 1;
      window.jhInduxLegend = wrappedLeg;
    }
    var chart = window.jhDeskChart || window.chart;
    hook(chart);
    var bars = window.lastBars;
    if (!bars || bars === seen || !window.paint) return;
    try { align(bars); } catch (e) {}
    seen = bars;
    var painted = null;
    try { painted = window.paint(bars); } catch (e2) {}
    if (painted && painted.then) painted.then(function () { frame(window.jhDeskChart || window.chart); }, function () {});
    else frame(chart);
  }, 250);
})();
