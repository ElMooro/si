/* jh-chart-tf-fix.js -- includes vs SPY nearest-session join */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhTfFixVsspy) return;
  window.__jhTfFixVsspy = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  function useNative() {
    if (!window.State || State.chartEngine === "native") return;
    State.chartEngine = "native";
    document.querySelectorAll(".ceng-btn[data-engine]").forEach(function (x) {
      x.classList.toggle("active", x.dataset.engine === "native");
    });
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
    if (n > 20000 && n < 80000) return (n - 25569) * 86400;
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
  function nearestJoin(series, spy) {
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
    if (!t0 || !s0) return [];
    return out.map(function (p) {
      return { time: p.time, value: ((p.value / t0) / (p.spy / s0) - 1) * 100 };
    });
  }
  function patch() {
    if (!window.NativeChart) return false;
    NativeChart.fetchSpySeries = async function () {
      var tf = (window.State && State.tf) || { mult: 1, span: "day", days: 1825 };
      var urls = [
        PROXY + "/ohlc?ticker=SPY&mult=" + (tf.mult || 1) + "&span=" + (tf.span || "day") + "&days=" + (tf.days || 1825),
        PROXY + "/ohlc?ticker=SPY&mult=1&span=day&days=2200",
        PROXY + "/yf-ohlc?symbol=SPY&range=5y",
        PROXY + "/yahoo?symbol=SPY"
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
    NativeChart.computeVsSpy = function (series, spy) { return nearestJoin(series, spy); };
    return true;
  }
  document.querySelectorAll(".chg-btn[data-chg=\"vsspy\"]").forEach(function (b) {
    b.addEventListener("click", function () { useNative(); }, true);
  });
  var n = 0;
  var id = setInterval(function () {
    if (patch() || ++n > 25) clearInterval(id);
  }, 250);
})();
