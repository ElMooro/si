/* jh-chart-tf-fix.js -- TF + % modes + vs SPY. No watchlist deletes. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhTfFix) return;
  window.__jhTfFix = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  function useNative() {
    if (!window.State) return;
    if (State.chartEngine === "native") return;
    State.chartEngine = "native";
    document.querySelectorAll(".ceng-btn[data-engine]").forEach(function (x) {
      x.classList.toggle("active", x.dataset.engine === "native");
    });
  }
  function reload() {
    if (!window.State || !window.ChartController) return;
    var t = State.activeTicker;
    State.activeTicker = "__r";
    ChartController.loadTicker(t);
  }
  function dayKey(t) {
    if (t == null) return "";
    if (typeof t === "string") return t.slice(0, 10);
    var n = +t;
    if (n > 1e12) n = n / 1000;
    if (n > 1e9 && n < 1e12) return new Date(n * 1000).toISOString().slice(0, 10);
    if (n > 20000 && n < 80000) return new Date((n - 25569) * 86400 * 1000).toISOString().slice(0, 10);
    return String(t).slice(0, 10);
  }
  function medSpacing(series) {
    var spacings = [];
    for (var k = 1; k < series.length; k++) spacings.push(series[k].time - series[k - 1].time);
    spacings.sort(function (a, b) { return a - b; });
    var med = spacings.length ? spacings[Math.floor(spacings.length / 2)] : 86400;
    return med < 1 ? 86400 : med;
  }
  function winBars(series, calendarDays) {
    return Math.max(2, Math.round((calendarDays * 86400) / medSpacing(series)));
  }
  function parseBars(d) {
    var bars = (d && (d.bars || d.results || d.data)) || [];
    return bars.map(function (b) {
      var val = b.close != null ? b.close : b.c != null ? b.c : b.value;
      var tm = b.time != null ? b.time : b.t != null ? b.t : b.date;
      return { time: tm, value: +val };
    }).filter(function (p) { return p.value && p.time != null; });
  }
  function patchChange() {
    if (!window.NativeChart || NativeChart.__jhChangePatched) return !!window.NativeChart;
    NativeChart.__jhChangePatched = true;
    NativeChart.CHANGE_LABELS = NativeChart.CHANGE_LABELS || {};
    NativeChart.CHANGE_LABELS.fromhigh = "Drawdown from 52w high";
    NativeChart.CHANGE_LABELS.fromlow = "Rally from 52w low";
    NativeChart.CHANGE_LABELS.vsspy = "Relative strength vs SPY";
    NativeChart.fetchSpySeries = async function () {
      if (NativeChart._spyCache && NativeChart._spyCache.length) return NativeChart._spyCache;
      var urls = [
        PROXY + "/ohlc?ticker=SPY&mult=1&span=day&days=2200",
        "/ohlc?ticker=SPY&mult=1&span=day&days=2200",
        PROXY + "/yahoo?symbol=SPY"
      ];
      for (var i = 0; i < urls.length; i++) {
        try {
          var r = await fetch(urls[i], { cache: "no-store" });
          if (!r.ok) continue;
          var rows = parseBars(await r.json());
          if (rows.length > 10) { NativeChart._spyCache = rows; return rows; }
        } catch (e) {}
      }
      NativeChart._spyCache = null;
      return [];
    };
    NativeChart.computeVsSpy = function (series, spy) {
      if (!series || !series.length || !spy || !spy.length) return [];
      var spyMap = {};
      spy.forEach(function (p) { spyMap[dayKey(p.time)] = p.value; });
      var aligned = [];
      series.forEach(function (p) {
        var k = dayKey(p.time);
        if (k && spyMap[k] && p.value) aligned.push({ time: p.time, value: p.value, spy: spyMap[k] });
      });
      if (aligned.length < 2) return [];
      var t0 = aligned[0].value, s0 = aligned[0].spy;
      if (!t0 || !s0) return [];
      return aligned.map(function (p) {
        return { time: p.time, value: ((p.value / t0) / (p.spy / s0) - 1) * 100 };
      });
    };
    NativeChart.computeChange = function (series, mode) {
      if (!series || series.length < 2) return [];
      if (mode === "ytd") {
        var out = [], yearStart = {};
        for (var i = 0; i < series.length; i++) {
          var p = series[i];
          var yr = new Date((+p.time > 1e12 ? +p.time : +p.time * 1000)).getUTCFullYear();
          if (yearStart[yr] == null) yearStart[yr] = p.value;
          if (yearStart[yr]) out.push({ time: p.time, value: (p.value / yearStart[yr] - 1) * 100 });
        }
        return out;
      }
      if (mode === "fromhigh" || mode === "fromlow") {
        var w = winBars(series, 365);
        var outH = [];
        for (var i2 = 0; i2 < series.length; i2++) {
          var ext = mode === "fromhigh" ? -Infinity : Infinity;
          var from = Math.max(0, i2 - w + 1);
          for (var j = from; j <= i2; j++) {
            var v = series[j].value;
            if (mode === "fromhigh") { if (v > ext) ext = v; } else { if (v < ext) ext = v; }
          }
          if (ext && isFinite(ext)) outH.push({ time: series[i2].time, value: (series[i2].value / ext - 1) * 100 });
        }
        return outH;
      }
      var days = { dod: 1, wow: 7, mom: 30.44, qoq: 91.31, yoy: 365 }[mode];
      if (!days) return [];
      var n = mode === "dod" ? 1 : Math.max(1, Math.round((days * 86400) / medSpacing(series)));
      var out3 = [];
      for (var i3 = n; i3 < series.length; i3++) {
        var then = series[i3 - n].value, now = series[i3].value;
        if (!then) continue;
        out3.push({ time: series[i3].time, value: (now / then - 1) * 100 });
      }
      return out3;
    };
    return true;
  }
  function addFromLow() {
    var fh = document.querySelector('.chg-btn[data-chg="fromhigh"]');
    if (!fh || document.querySelector('.chg-btn[data-chg="fromlow"]')) return;
    var b = document.createElement("button");
    b.className = "chg-btn";
    b.setAttribute("data-chg", "fromlow");
    b.title = "% above the 52-week low";
    b.textContent = "From Low";
    fh.after(b);
    b.addEventListener("click", function () {
      document.querySelectorAll(".chg-btn").forEach(function (x) { x.classList.remove("active"); });
      b.classList.add("active");
      if (window.State) { State.changeMode = "fromlow"; useNative(); reload(); }
    });
  }
  function wire() {
    patchChange();
    addFromLow();
    document.querySelectorAll(".chg-btn").forEach(function (b) {
      if (b.dataset.jhChg) return;
      b.dataset.jhChg = "1";
      b.addEventListener("click", function () {
        if (b.dataset.chg && b.dataset.chg !== "price") useNative();
      }, true);
    });
  }
  var n = 0;
  var t = setInterval(function () {
    n++;
    if (patchChange() || n > 20) { wire(); clearInterval(t); }
  }, 300);
})();
