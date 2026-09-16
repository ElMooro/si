/* Candle + volume quality layer. Tick-round lives in the engine (window.jhQx). */
(function () {
  function sanitize(d) {
    if (!d || !d.length) return [];
    var out = [], i, drop = 0, volZero = 0;
    for (i = 0; i < d.length; i++) {
      var b = d[i];
      var o = +b.open, h = +b.high, l = +b.low, c = +b.close, v = +b.volume;
      if (!isFinite(c) || !isFinite(o) || !b.time) { drop++; continue; }
      if (!isFinite(h)) h = Math.max(o, c);
      if (!isFinite(l)) l = Math.min(o, c);
      h = Math.max(h, o, c);
      l = Math.min(l, o, c);
      if (l <= 0 && c > 0) l = Math.min(o, c);
      if (!isFinite(v) || v < 0) v = 0;
      if (v === 0) volZero++;
      if (i && b.time <= d[i - 1].time) { drop++; continue; }
      out.push({ time: b.time, open: o, high: h, low: l, close: c, volume: v });
    }
    return { bars: out, drop: drop, volZero: volZero, n: out.length };
  }

  function sessionVwap(d) {
    var out = [], pv = 0, vv = 0, day = null, i;
    for (i = 0; i < d.length; i++) {
      var dt = new Date(d[i].time * 1000);
      var key = dt.getUTCFullYear() + "-" + dt.getUTCMonth() + "-" + dt.getUTCDate();
      if (day !== key) { day = key; pv = 0; vv = 0; }
      var typ = (d[i].high + d[i].low + d[i].close) / 3;
      pv += typ * (d[i].volume || 0);
      vv += d[i].volume || 0;
      if (vv > 0) out.push({ time: d[i].time, value: pv / vv });
    }
    return out;
  }

  function qualityReport(s) {
    var el = document.getElementById("stat");
    if (!el || !s) return;
    var q = s.n ? (100 * (s.n - s.volZero) / s.n).toFixed(0) : "0";
    var extra = " · vol-cover " + q + "%";
    if (s.drop) extra += " · dropped " + s.drop;
    if (el.textContent.indexOf("vol-cover") < 0) el.textContent += extra;
  }

  function tick() {
    var raw = window.lastBars;
    if (!raw || !raw.length) return;
    var s = sanitize(raw);
    if (window.jhQx && typeof window.jhQx.roundBars === "function") {
      s.bars = window.jhQx.roundBars(s.bars);
    }
    window.jhQuality = s;
    qualityReport(s);
    window.jhSessionVwap = sessionVwap(s.bars);
  }

  setInterval(tick, 2000);
  window.addEventListener("load", function () { setTimeout(tick, 800); });
})();
