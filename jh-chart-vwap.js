/* NYSE session VWAP on lastBars. America/New_York, not UTC day. */
(function () {
  function bindDesk(el, chart) {
    if (!el || (el.id !== "host" && el.id !== "chart") || !chart || chart.__desk) return;
    chart.__desk = 1;
    window.jhDeskChart = chart;
    ["addCandlestickSeries", "addBarSeries", "addBaselineSeries"].forEach(function (name) {
      if (!chart[name] || chart[name].__desk) return;
      var fn = chart[name];
      chart[name] = function () { var s = fn.apply(chart, arguments); window.jhDeskSeries = s; return s; };
      chart[name].__desk = 1;
    });
  }
  var LC = window.LightweightCharts;
  if (LC && LC.createChart && !LC.createChart.__desk) {
    var orig = LC.createChart;
    LC.createChart = function (el) {
      var chart = orig.apply(this, arguments);
      bindDesk(el, chart);
      return chart;
    };
    LC.createChart.__desk = true;
  }
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
  function inRth(ts) {
    var p = nyParts(ts);
    if (!p) return true;
    var m = p.hh * 60 + p.mm;
    return m >= 9 * 60 + 30 && m < 16 * 60;
  }
  window.jhNyVwap = function (d) {
    if (!d || !d.length) return [];
    var out = [], pv = 0, vv = 0, day = null, i;
    var daily = true;
    if (d.length >= 2 && d[d.length - 1].time - d[0].time < 20 * 86400) daily = false;
    for (i = 0; i < d.length; i++) {
      var k = sessionKey(d[i].time);
      if (k !== day) { day = k; pv = 0; vv = 0; }
      if (!daily && !inRth(d[i].time)) continue;
      var typ = (d[i].high + d[i].low + d[i].close) / 3;
      pv += typ * (d[i].volume || 0);
      vv += d[i].volume || 0;
      if (vv > 0) out.push({ time: d[i].time, value: pv / vv });
    }
    return out;
  };
  document.write('<script src="/jh-chart-auction.js?v=20261002desk"><\/script>');
})();
