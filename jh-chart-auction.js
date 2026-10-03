/* Desk levels from bars already on the chart. Bar volume, not footprint. */
(function () {
  function nyParts(ts) {
    var s = new Date(ts * 1000).toLocaleString("en-US", { timeZone: "America/New_York", hour12: false });
    var p = s.match(/(\d+)\/(\d+)\/(\d+),\s*(\d+):(\d+)/);
    if (!p) return null;
    return { mon: +p[1], day: +p[2], year: +p[3], hh: +p[4], mm: +p[5] };
  }
  function sessionKey(ts) {
    var p = nyParts(ts);
    return p ? p.year + "-" + p.mon + "-" + p.day : "";
  }
  var KEY = "jh-desk-pack";
  var state = { avwap: 1, vp: 1, ref: 1 };
  try { var saved = JSON.parse(localStorage.getItem(KEY) || ""); if (saved && typeof saved === "object") state = saved; } catch (e) {}
  function save() { try { localStorage.setItem(KEY, JSON.stringify(state)); } catch (e) {} }
  function ensure() {
    var INDS = window.INDS;
    if (!INDS || !INDS.push) return;
    [["avwap", "Anchored VWAP"], ["vprof", "Volume profile"], ["reflevels", "PDH / PWH / OR"]].forEach(function (pair) {
      if (!INDS.some(function (i) { return i && i.id === pair[0]; })) INDS.push({ id: pair[0], n: pair[1], on: state[pair[0] === "vprof" ? "vp" : pair[0] === "reflevels" ? "ref" : "avwap"] ? 1 : 0, cat: "Volume", c: "#C9942E" });
    });
  }
  function studyOn(id) {
    var INDS = window.INDS, row = INDS && INDS.filter(function (i) { return i && i.id === id; })[0];
    if (!row) return id === "avwap" ? state.avwap : id === "vprof" ? state.vp : state.ref;
    return row.on === 1 || row.on === true;
  }
  function isMain(el) {
    if (!el || !el.id) return false;
    return el.id === "host" || el.id === "chart";
  }
  function bind(chart) {
    if (!chart || chart.__desk) return;
    chart.__desk = 1;
    window.jhDeskChart = chart;
    ["addCandlestickSeries", "addBarSeries", "addBaselineSeries"].forEach(function (name) {
      if (!chart[name] || chart[name].__desk) return;
      var fn = chart[name];
      chart[name] = function () { var s = fn.apply(chart, arguments); window.jhDeskSeries = s; return s; };
      chart[name].__desk = 1;
    });
    try { chart.timeScale().subscribeVisibleLogicalRangeChange(draw); } catch (e) {}
  }
  function wrapCreate() {
    var LC = window.LightweightCharts;
    if (!LC || !LC.createChart || LC.createChart.__desk) return;
    var orig = LC.createChart;
    LC.createChart = function (el) {
      var chart = orig.apply(this, arguments);
      if (isMain(el)) bind(chart);
      return chart;
    };
    LC.createChart.__desk = true;
  }
  wrapCreate();
  function bars() { return window.lastBars || []; }
  function visible(chart, d) {
    var range = null;
    try { range = chart.timeScale().getVisibleRange(); } catch (e) {}
    if (!range || range.from == null) return d.slice(-160);
    var out = d.filter(function (b) { return b.time >= range.from && b.time <= range.to; });
    return out.length ? out : d.slice(-160);
  }
  function avwap(d, from) {
    var pv = 0, vv = 0, pv2 = 0, out = [], i;
    for (i = from; i < d.length; i++) {
      var b = d[i], tp = (b.high + b.low + b.close) / 3, v = b.volume || 1;
      pv += tp * v; vv += v; pv2 += tp * tp * v;
      var mean = pv / vv, sd = Math.sqrt(Math.max(0, pv2 / vv - mean * mean));
      out.push({ time: b.time, v: mean, u1: mean + sd, l1: mean - sd, u2: mean + 2 * sd, l2: mean - 2 * sd });
    }
    return out;
  }
  function profile(d) {
    var lo = Infinity, hi = -Infinity, i, total = 0;
    for (i = 0; i < d.length; i++) { if (d[i].low < lo) lo = d[i].low; if (d[i].high > hi) hi = d[i].high; }
    if (!(hi > lo)) return null;
    var n = 28, bins = [], span = hi - lo;
    for (i = 0; i < n; i++) bins[i] = 0;
    for (i = 0; i < d.length; i++) {
      var b = d[i], vol = b.volume || 0; if (vol <= 0) continue;
      var a = Math.max(0, Math.min(n - 1, Math.floor((b.low - lo) / span * n)));
      var c = Math.max(a, Math.min(n - 1, Math.floor((b.high - lo) / span * n)));
      var share = vol / (c - a + 1), k;
      for (k = a; k <= c; k++) bins[k] += share;
      total += vol;
    }
    if (total <= 0) return null;
    var poc = 0;
    for (i = 1; i < n; i++) if (bins[i] > bins[poc]) poc = i;
    var acc = bins[poc], loB = poc, hiB = poc;
    while (acc < total * 0.7 && (loB > 0 || hiB < n - 1)) {
      var left = loB > 0 ? bins[loB - 1] : -1, right = hiB < n - 1 ? bins[hiB + 1] : -1;
      if (right >= left) { hiB++; acc += bins[hiB]; } else { loB--; acc += bins[loB]; }
    }
    return { lo: lo, hi: hi, bins: bins, poc: poc, vaLo: loB, vaHi: hiB, max: Math.max.apply(null, bins) };
  }
  function refs(d) {
    if (d.length < 2) return {};
    var daily = d[d.length - 1].time - d[0].time > 20 * 86400 || (d.length >= 2 && d[1].time - d[0].time >= 20 * 3600);
    var out = {};
    if (daily) {
      var prev = d[d.length - 2];
      out.pdh = prev.high; out.pdl = prev.low; out.pdc = prev.close;
      var wk = d.slice(-6, -1);
      if (wk.length) { out.pwh = Math.max.apply(null, wk.map(function (b) { return b.high; })); out.pwl = Math.min.apply(null, wk.map(function (b) { return b.low; })); }
      return out;
    }
    var days = {}, order = [], i;
    for (i = 0; i < d.length; i++) {
      var k = sessionKey(d[i].time); if (!k) continue;
      if (!days[k]) { days[k] = { high: -Infinity, low: Infinity, close: d[i].close, bars: [] }; order.push(k); }
      var g = days[k];
      if (d[i].high > g.high) g.high = d[i].high;
      if (d[i].low < g.low) g.low = d[i].low;
      g.close = d[i].close; g.bars.push(d[i]);
    }
    if (order.length >= 2) { var p = days[order[order.length - 2]]; out.pdh = p.high; out.pdl = p.low; out.pdc = p.close; }
    if (order.length >= 7) {
      var slice = order.slice(-7, -1), hi = -Infinity, lo = Infinity;
      slice.forEach(function (k) { if (days[k].high > hi) hi = days[k].high; if (days[k].low < lo) lo = days[k].low; });
      out.pwh = hi; out.pwl = lo;
    }
    var today = days[order[order.length - 1]];
    if (today) {
      var orh = -Infinity, orl = Infinity, n = 0;
      today.bars.forEach(function (b) {
        var p = nyParts(b.time); if (!p) return;
        var m = p.hh * 60 + p.mm;
        if (m >= 570 && m < 600) { if (b.high > orh) orh = b.high; if (b.low < orl) orl = b.low; n++; }
      });
      if (n) { out.orh = orh; out.orl = orl; }
    }
    return out;
  }
  function px(series, price) { try { return series.priceToCoordinate(price); } catch (e) { return null; } }
  function line(host, y, color, label, dash) {
    if (y == null || !isFinite(y)) return;
    var row = document.createElement("div");
    row.style.cssText = "position:absolute;left:0;right:58px;height:0;border-top:1px " + (dash ? "dashed" : "solid") + " " + color + ";top:" + y + "px;pointer-events:none";
    var lab = document.createElement("div");
    lab.textContent = label;
    lab.style.cssText = "position:absolute;right:60px;top:" + (y - 11) + "px;font:10px IBM Plex Mono,monospace;color:" + color + ";pointer-events:none";
    host.appendChild(row); host.appendChild(lab);
  }
  function draw() {
    wrapCreate();
    ensure();
    var chart = window.jhDeskChart || window.chart, series = window.jhDeskSeries || window.mainSeries, d = bars();
    if (!chart || !series || !d.length) return;
    var el = null;
    try { el = chart.chartElement(); } catch (e) {}
    if (!el) return;
    var host = el.querySelector(".jh-desk");
    if (!host) {
      host = document.createElement("div");
      host.className = "jh-desk";
      host.style.cssText = "position:absolute;inset:0;z-index:4;pointer-events:none";
      el.appendChild(host);
      var bar = document.createElement("div");
      bar.style.cssText = "position:absolute;top:6px;left:8px;display:flex;gap:4px;pointer-events:auto";
      [["avwap", "AVWAP"], ["vp", "VP"], ["ref", "PDH/OR"]].forEach(function (pair) {
        var b = document.createElement("button");
        b.type = "button"; b.textContent = pair[1]; b.dataset.k = pair[0];
        b.style.cssText = "border:1px solid var(--line,#3a3428);background:#12110C;color:#d1d4dc;font:10px sans-serif;padding:2px 6px;border-radius:3px;cursor:pointer";
        b.addEventListener("click", function () {
          state[pair[0]] = state[pair[0]] ? 0 : 1; save();
          var id = pair[0] === "vp" ? "vprof" : pair[0] === "ref" ? "reflevels" : "avwap";
          var INDS = window.INDS, row = INDS && INDS.filter(function (i) { return i && i.id === id; })[0];
          if (row) row.on = state[pair[0]];
          draw();
        });
        bar.appendChild(b);
      });
      host.appendChild(bar);
    }
    var keep = host.querySelector("div");
    host.replaceChildren(keep);
    [["avwap", "AVWAP"], ["vp", "VP"], ["ref", "PDH/OR"]].forEach(function (pair) {
      var b = keep.querySelector("[data-k='" + pair[0] + "']");
      if (b) b.style.color = (pair[0] === "vp" ? studyOn("vprof") : pair[0] === "ref" ? studyOn("reflevels") : studyOn("avwap")) ? "#C9942E" : "#787b86";
    });
    var vis = visible(chart, d);
    if (studyOn("avwap") && vis.length > 3) {
      var anchor = 0, low = Infinity, i;
      for (i = 0; i < vis.length - 2; i++) if (vis[i].low < low) { low = vis[i].low; anchor = i; }
      var from = d.indexOf(vis[anchor]); if (from < 0) from = Math.max(0, d.length - vis.length);
      var path = avwap(d, from), last = path[path.length - 1];
      if (last) {
        line(host, px(series, last.v), "#C9942E", "AVWAP", false);
        line(host, px(series, last.u1), "#C9942E", "+1s", true);
        line(host, px(series, last.l1), "#C9942E", "-1s", true);
        line(host, px(series, last.u2), "#8a836f", "+2s", true);
        line(host, px(series, last.l2), "#8a836f", "-2s", true);
      }
    }
    if (studyOn("vprof")) {
      var prof = profile(vis);
      if (prof) {
        var span = prof.hi - prof.lo, w = 56;
        prof.bins.forEach(function (v, i) {
          var price = prof.lo + (i + 0.5) / prof.bins.length * span;
          var y = px(series, price); if (y == null) return;
          var bar = document.createElement("div");
          var hot = i === prof.poc, va = i >= prof.vaLo && i <= prof.vaHi;
          bar.style.cssText = "position:absolute;right:58px;height:3px;width:" + Math.max(2, v / prof.max * w) + "px;background:" + (hot ? "#C9942E" : va ? "rgba(201,148,46,.45)" : "rgba(181,173,153,.25)") + ";top:" + y + "px;pointer-events:none";
          host.appendChild(bar);
        });
        line(host, px(series, prof.lo + (prof.poc + 0.5) / prof.bins.length * span), "#C9942E", "POC", false);
        line(host, px(series, prof.lo + (prof.vaHi + 1) / prof.bins.length * span), "#E07A6A", "VAH", true);
        line(host, px(series, prof.lo + prof.vaLo / prof.bins.length * span), "#7d9a78", "VAL", true);
        var note = document.createElement("div");
        note.textContent = "Bar volume, not footprint";
        note.style.cssText = "position:absolute;right:60px;bottom:18px;font:9px sans-serif;color:#787b86;pointer-events:none";
        host.appendChild(note);
      }
    }
    if (studyOn("reflevels")) {
      var r = refs(d);
      line(host, px(series, r.pdh), "#E07A6A", "PDH", false);
      line(host, px(series, r.pdl), "#7d9a78", "PDL", false);
      line(host, px(series, r.pdc), "#8a9bb0", "PDC", true);
      line(host, px(series, r.pwh), "#E07A6A", "PWH", true);
      line(host, px(series, r.pwl), "#7d9a78", "PWL", true);
      line(host, px(series, r.orh), "#C9942E", "ORH", false);
      line(host, px(series, r.orl), "#C9942E", "ORL", false);
    }
  }
  setInterval(function () { try { draw(); } catch (e) {} }, 800);
  window.jhDeskDraw = draw;
})();
