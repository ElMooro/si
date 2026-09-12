/* jh-chart-tf-fix.js */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhTfFix2) return;
  window.__jhTfFix2 = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function toSec(t) {
    if (t == null) return null;
    if (typeof t === "string") {
      var ms = Date.parse(t.length <= 10 ? t + "T00:00:00Z" : t);
      return isNaN(ms) ? null : ms / 1000;
    }
    var n = +t;
    if (n > 1e12) return n / 1000;
    if (n > 1e9) return n;
    return n;
  }
  function parseBars(d) {
    var bars = (d && (d.bars || d.ohlc || d.results || d.data)) || [];
    return bars.map(function (b) {
      if (Array.isArray(b)) return { time: b[0], sec: toSec(b[0]), value: +b[4] };
      var val = b.close != null ? b.close : b.c != null ? b.c : b.value;
      var tm = b.time != null ? b.time : b.t != null ? b.t : b.date;
      return { time: tm, sec: toSec(tm), value: +val };
    }).filter(function (p) { return p.value && p.sec; });
  }
  function patch() {
    if (!window.NativeChart) return false;
    NativeChart.CHANGE_LABELS = NativeChart.CHANGE_LABELS || {};
    NativeChart.CHANGE_LABELS.fromlow = "Rally from 52w low";
    NativeChart.CHANGE_PERIODS = NativeChart.CHANGE_PERIODS || {};
    NativeChart.CHANGE_PERIODS.fromlow = 365;
    NativeChart.CHANGE_PERIODS.fromhigh = 365;
    NativeChart.CHANGE_PERIODS.vsspy = 365;
    NativeChart.fetchSpySeries = async function () {
      try {
        if (window.SymDir && typeof SymDir.series === "function") {
          var d = await SymDir.series("SPY");
          var rows = parseBars(d);
          if (rows.length > 10) return rows;
        }
      } catch (e) {}
      var urls = [
        PROXY + "/ohlc?ticker=SPY&mult=1&span=day&days=2200",
        PROXY + "/yf-ohlc?symbol=SPY&range=5y"
      ];
      for (var i = 0; i < urls.length; i++) {
        try {
          var r = await fetch(urls[i], { cache: "no-store" });
          if (!r.ok) continue;
          var rows2 = parseBars(await r.json());
          if (rows2.length > 10) return rows2;
        } catch (e) {}
      }
      return [];
    };
    NativeChart.computeVsSpy = function (series, spy) {
      if (!series || !spy || series.length < 2 || spy.length < 2) return [];
      var out = [], j = 0;
      for (var i = 0; i < series.length; i++) {
        var ts = toSec(series[i].time);
        if (!ts || !series[i].value) continue;
        while (j + 1 < spy.length && Math.abs(spy[j + 1].sec - ts) <= Math.abs(spy[j].sec - ts)) j++;
        if (Math.abs(spy[j].sec - ts) > 7 * 86400) continue;
        out.push({ time: series[i].time, value: series[i].value, spy: spy[j].value });
      }
      if (out.length < 2) return [];
      var t0 = out[0].value, s0 = out[0].spy;
      return out.map(function (p) { return { time: p.time, value: ((p.value / t0) / (p.spy / s0) - 1) * 100 }; });
    };
    var prev = NativeChart.computeChange.bind(NativeChart);
    NativeChart.computeChange = function (series, mode) {
      if (mode === "fromlow" || mode === "fromhigh") {
        if (!series || series.length < 2) return [];
        var med = 86400;
        try {
          var sp = [];
          for (var k = 1; k < series.length; k++) sp.push(toSec(series[k].time) - toSec(series[k - 1].time));
          sp.sort(function (a, b) { return a - b; });
          if (sp.length) med = sp[Math.floor(sp.length / 2)] || 86400;
        } catch (e) {}
        var w = Math.max(20, Math.round((365 * 86400) / med));
        var outH = [];
        for (var i2 = 0; i2 < series.length; i2++) {
          var ext = mode === "fromhigh" ? -Infinity : Infinity;
          for (var j = Math.max(0, i2 - w + 1); j <= i2; j++) {
            var v = series[j].value;
            if (mode === "fromhigh") { if (v > ext) ext = v; } else if (v < ext) ext = v;
          }
          if (ext && isFinite(ext)) outH.push({ time: series[i2].time, value: (series[i2].value / ext - 1) * 100 });
        }
        return outH;
      }
      return prev(series, mode);
    };
    return true;
  }
  function forceDaily() {
    if (!window.State) return;
    State.chartEngine = "native";
    State.tf = { mult: 1, span: "day", days: 1825 };
  }
  function addFromLow() {
    var fh = document.querySelector('.chg-btn[data-chg="fromhigh"]');
    if (!fh || document.querySelector('.chg-btn[data-chg="fromlow"]')) return;
    var b = document.createElement("button");
    b.className = "chg-btn";
    b.setAttribute("data-chg", "fromlow");
    b.textContent = "From Low";
    fh.after(b);
    b.addEventListener("click", function (e) {
      e.preventDefault();
      document.querySelectorAll(".chg-btn").forEach(function (x) { x.classList.remove("active"); });
      b.classList.add("active");
      forceDaily();
      State.changeMode = "fromlow";
      var t = State.activeTicker; State.activeTicker = "__r";
      if (window.ChartController) ChartController.loadTicker(t);
    });
  }
  document.addEventListener("click", function (e) {
    var b = e.target && e.target.closest && e.target.closest(".chg-btn");
    if (!b || !b.dataset.chg || b.dataset.chg === "price") return;
    forceDaily();
  }, true);
  var n = 0;
  var id = setInterval(function () {
    n++;
    if (patch()) addFromLow();
    if (n > 30) clearInterval(id);
  }, 300);
})();
