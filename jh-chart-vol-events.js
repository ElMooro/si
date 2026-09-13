/* Volume events. 50-bar RVOL so crash weeks still print vs the quiet base. */
(function () {
  function avg(a) { var s = 0, i; for (i = 0; i < a.length; i++) s += a[i]; return a.length ? s / a.length : 0; }
  function events(d) {
    if (!d || d.length < 55) return [];
    var out = [], i;
    for (i = 50; i < d.length; i++) {
      var win = d.slice(i - 50, i);
      var vAvg = avg(win.map(function (b) { return b.volume || 0; }));
      var sprAvg = avg(win.map(function (b) { return b.close ? (b.high - b.low) / b.close : 0; }));
      var b = d[i], v = b.volume || 0;
      var spr = b.close ? (b.high - b.low) / b.close : 0;
      var rng = b.high - b.low;
      var clv = rng ? ((b.close - b.low) - (b.high - b.close)) / rng : 0;
      var rvol = vAvg ? v / vAvg : 0;
      var down = b.close < b.open, up = b.close > b.open;
      var tag = null, pos = "aboveBar", color = "#787b86", shape = "circle";
      if (rvol >= 2.2 && down && clv <= -0.3 && spr >= sprAvg * 1.2) {
        tag = (rvol >= 3.2 && clv <= -0.45) ? "CAPIT" : "SC";
        pos = "belowBar"; color = "#f23645"; shape = "arrowDown";
      } else if (rvol >= 2.2 && up && clv >= 0.3 && spr >= sprAvg * 1.2) {
        tag = (rvol >= 3.2 && clv >= 0.45) ? "BC+" : "BC";
        pos = "aboveBar"; color = "#089981"; shape = "arrowUp";
      } else if (rvol >= 1.8 && down && clv <= -0.15) {
        tag = "DIST"; pos = "aboveBar"; color = "#ab47bc"; shape = "square";
      } else if (rvol <= 0.55) {
        var prior = out.filter(function (e) { return e._i >= i - 18 && (e.text === "SC" || e.text === "CAPIT"); });
        if (prior.length && Math.abs(b.low - d[prior[prior.length - 1]._i].low) / b.close < 0.025) {
          tag = "NS"; pos = "belowBar"; color = "#2962ff"; shape = "circle";
        } else if (rvol <= 0.45 && spr < sprAvg * 0.75 && !down) {
          tag = "ACC"; pos = "belowBar"; color = "#089981"; shape = "circle";
        }
      }
      if (tag) out.push({ time: b.time, position: pos, color: color, shape: shape, text: tag, _i: i, rvol: rvol });
    }
    return out;
  }
  window.jhVolEvents = function (d) {
    return events(d).map(function (e) {
      return { time: e.time, position: e.position, color: e.color, shape: e.shape, text: e.text };
    });
  };
  window.jhVolEventTable = events;
})();
