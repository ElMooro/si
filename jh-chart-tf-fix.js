/* jh-chart-tf-fix.js -- % modes, 52w high/low, vs SPY join. No watchlist deletes. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhTfFix) return;
  window.__jhTfFix = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function useNative() {
    if (!window.State || State.chartEngine === "native") return;
    State.chartEngine = "native";
    document.querySelectorAll(".ceng-btn[data-engine]").forEach(function (x) {
      x.classList.toggle("active", x.dataset.engine === "native");
    });
  }
  function reload() {
    if (!window.State || !window.ChartController) return;
    var t = State.activeTicker; State.activeTicker = "__r"; ChartController.loadTicker(t);
  }
  function toSec(t) {
    if (t == null) return null;
    if (typeof t === "string") {
      var ms = Date.parse(t.length === 10 ? t + "T00:00:00Z" : t);
      return isNaN(ms) ? null : ms / 1000;
    }
    var n = +t;
    if (n > 1e12) return n / 1000;
    if (n > 1e9) return n;
    return n;
  }
  function parseBars(d) {
    var bars = (d && (d.bars || d.results || d.data || d.quotes)) || [];
    return bars.map(function (b) {
      var val = b.close != null ? b.close : b.c != null ? b.c : b.value != null ? b.value : b.adjClose;
      var tm = b.time != null ? b.time : b.t != null ? b.t : b.date || b.timestamp;
      return { time: tm, sec: toSec(tm), value: +val };
    }).filter(function (p) { return p.value && p.sec; }).sort(function (a, b) { return a.sec - b.sec; });
  }
  function medSpacing(series) {
    var s = [];
    for (var k = 1; k < series.length; k++) s.push(toSec(series[k].time) - toSec(series[k - 1].time));
    s.sort(function (a, b) { return a - b; });
    return s.length && s[Math.floor(s.length / 2)] > 0 ? s[Math.floor(s.length / 2)] : 86400;
  }
  function patch() {
    if (!window.NativeChart) return false;
    NativeChart.CHANGE_LABELS = NativeChart.CHANGE_LABELS || {};
    NativeChart.CHANGE_LABELS.fromlow = "Rally from 52w low";
    NativeChart.CHANGE_LABELS.vsspy = "Relative strength vs SPY";
    NativeChart.fetchSpySeries = async function () {
      var tf = (window.State && State.tf) || { mult: 1, span: "day", days: 1825 };
      var urls = [
        PROXY + "/ohlc?ticker=SPY&mult=" + (tf.mult || 1) + "&span=" + (tf.span || "day") + "&days=" + Math.max(400, tf.days || 400),
        PROXY + "/ohlc?ticker=SPY&mult=1&span=day&days=2200",
        PROXY + "/yf-ohlc?symbol=SPY&range=5y"
      ];
      for (var i = 0; i < urls.length; i++) {
        try {
          var r = await fetch(urls[i], { cache: "no-store" });
          if (!r.ok) continue;
          var rows = parseBars(await r.json());
          if (rows.length > 10) return rows;
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
        if (Math.abs(spy[j].sec - ts) > 5 * 86400) continue;
        out.push({ time: series[i].time, value: series[i].value, spy: spy[j].value });
      }
      if (out.length < 2) return [];
      var t0 = out[0].value, s0 = out[0].spy;
      return out.map(function (p) { return { time: p.time, value: ((p.value / t0) / (p.spy / s0) - 1) * 100 }; });
    };
    NativeChart.computeChange = function (series, mode) {
      if (!series || series.length < 2) return [];
      if (mode === "ytd") {
        var out = [], yearStart = {};
        series.forEach(function (p) {
          var yr = new Date(toSec(p.time) * 1000).getUTCFullYear();
          if (yearStart[yr] == null) yearStart[yr] = p.value;
          if (yearStart[yr]) out.push({ time: p.time, value: (p.value / yearStart[yr] - 1) * 100 });
        });
        return out;
      }
      if (mode === "fromhigh" || mode === "fromlow") {
        var w = Math.max(2, Math.round((365 * 86400) / medSpacing(series)));
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
      var days = { dod: 1, wow: 7, mom: 30.44, qoq: 91.31, yoy: 365 }[mode];
      if (!days) return [];
      var n = mode === "dod" ? 1 : Math.max(1, Math.round((days * 86400) / medSpacing(series)));
      var out3 = [];
      for (var i3 = n; i3 < series.length; i3++) {
        if (!series[i3 - n].value) continue;
        out3.push({ time: series[i3].time, value: (series[i3].value / series[i3 - n].value - 1) * 100 });
      }
      return out3;
    };
    return true;
  }
  function addFromLow() {
    var fh = document.querySelector('.chg-btn[data-chg="fromhigh"]');
    if (!fh || document.querySelector('.chg-btn[data-chg="fromlow"]')) return;
    var b = document.createElement("button");
    b.className = "chg-btn"; b.setAttribute("data-chg", "fromlow");
    b.title = "% above the 52-week low"; b.textContent = "From Low";
    fh.after(b);
    b.addEventListener("click", function () {
      document.querySelectorAll(".chg-btn").forEach(function (x) { x.classList.remove("active"); });
      b.classList.add("active");
      if (window.State) { State.changeMode = "fromlow"; useNative(); reload(); }
    });
  }
  var n = 0;
  var id = setInterval(function () {
    n++;
    if (patch() || n > 25) { addFromLow(); clearInterval(id); }
  }, 250);
})();
