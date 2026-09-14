/* Volume Tape — Bloomberg-style effort/result events on the volume pane. */
(function () {
  function avg(a) { var s = 0, i; for (i = 0; i < a.length; i++) s += a[i]; return a.length ? s / a.length : 0; }
  function hh(d, i, n) {
    var h = -1e99, j, a = Math.max(0, i - n);
    for (j = a; j < i; j++) if (d[j].high > h) h = d[j].high;
    return h;
  }
  function ll(d, i, n) {
    var l = 1e99, j, a = Math.max(0, i - n);
    for (j = a; j < i; j++) if (d[j].low < l) l = d[j].low;
    return l;
  }
  function classify(d) {
    if (!d || d.length < 55) return [];
    var out = [], i;
    for (i = 50; i < d.length; i++) {
      var win = d.slice(i - 50, i);
      var vAvg = avg(win.map(function (b) { return b.volume || 0; }));
      var sprAvg = avg(win.map(function (b) { return b.close ? (b.high - b.low) / b.close : 0; }));
      var b = d[i], v = b.volume || 0;
      var spr = b.close ? (b.high - b.low) / b.close : 0;
      var rng = b.high - b.low || 1e-12;
      var body = Math.abs(b.close - b.open);
      var closeLoc = (b.close - b.low) / rng;
      var rvol = vAvg ? v / vAvg : 0;
      var down = b.close < b.open, up = b.close > b.open;
      var prev = d[i - 1];
      var kind = null, label = null, color = "#787b86";
      var donHi = hh(d, i, 20), donLo = ll(d, i, 20);
      if (rvol >= 3.0 && down && closeLoc <= 0.22 && spr >= sprAvg * 1.25) {
        kind = "capit"; label = "CAPIT"; color = "#f23645";
      } else if (rvol >= 2.5 && up && closeLoc >= 0.72) {
        kind = "hugebuy"; label = "HUGE"; color = "#089981";
      } else if (rvol >= 2.2 && down && closeLoc <= 0.35 && spr >= sprAvg * 1.15) {
        kind = "sc"; label = "SC"; color = "#ef5350";
      } else if (rvol >= 2.2 && up && closeLoc >= 0.65 && spr >= sprAvg * 1.15) {
        kind = "bc"; label = "BC"; color = "#26a69a";
      } else if (up && b.close > donHi && rvol >= 1.55 && prev && prev.close <= donHi) {
        kind = "breakout"; label = "BO"; color = "#2962ff";
      } else if (rvol >= 1.8 && body / rng <= 0.38 && spr >= sprAvg * 0.9) {
        kind = "evr"; label = "EvR"; color = "#f0b429";
      }
      if (kind) out.push({ time: b.time, kind: kind, label: label, color: color, vol: v, rvol: rvol, i: i });
    }
    var seen = {}, keep = [];
    for (i = out.length - 1; i >= 0; i--) {
      if (seen[out[i].time]) continue;
      seen[out[i].time] = 1;
      keep.push(out[i]);
    }
    keep.reverse();
    return keep;
  }
  window.jhVolumeTape = function (d) {
    var ev = classify(d);
    return {
      events: ev,
      markers: ev.map(function (e) {
        var below = e.kind === "capit" || e.kind === "sc" || e.kind === "evr";
        return { time: e.time, position: below ? "belowBar" : "aboveBar", color: e.color, shape: below ? "arrowDown" : "arrowUp", text: e.label };
      })
    };
  };
  window.jhVolEvents = function (d) {
    var t = window.jhVolumeTape(d);
    return t && t.markers ? t.markers : [];
  };
  window.jhVolEventTable = classify;
})();
