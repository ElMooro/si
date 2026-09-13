/* Wyckoff / tape volume events on lastBars. No invented prints. */
(function () {
  function avg(arr) {
    var s = 0, i;
    for (i = 0; i < arr.length; i++) s += arr[i];
    return arr.length ? s / arr.length : 0;
  }

  function events(d) {
    if (!d || d.length < 25) return [];
    var out = [], i;
    for (i = 20; i < d.length; i++) {
      var win = d.slice(i - 20, i);
      var vAvg = avg(win.map(function (b) { return b.volume || 0; }));
      var sprAvg = avg(win.map(function (b) {
        return b.close ? (b.high - b.low) / b.close : 0;
      }));
      var b = d[i];
      var v = b.volume || 0;
      var spr = b.close ? (b.high - b.low) / b.close : 0;
      var rng = b.high - b.low;
      var clv = rng ? ((b.close - b.low) - (b.high - b.close)) / rng : 0;
      var rvol = vAvg ? v / vAvg : 0;
      var down = b.close < b.open;
      var up = b.close > b.open;
      var tag = null, pos = "aboveBar", color = "#787b86", shape = "circle";

      if (rvol >= 2.5 && down && clv <= -0.35 && spr >= sprAvg * 1.3) {
        tag = rvol >= 4 ? "SC" : "SELL CLX";
        pos = "belowBar"; color = "#f23645"; shape = "arrowDown";
      } else if (rvol >= 2.5 && up && clv >= 0.35 && spr >= sprAvg * 1.3) {
        tag = rvol >= 4 ? "BC" : "BUY CLX";
        pos = "aboveBar"; color = "#089981"; shape = "arrowUp";
      } else if (rvol >= 2 && down && clv <= -0.2) {
        tag = "DIST"; pos = "aboveBar"; color = "#ab47bc"; shape = "square";
      } else if (rvol <= 0.55 && i >= 22) {
        var prior = out.filter(function (e) { return e._i >= i - 15 && (e.text === "SC" || e.text === "SELL CLX"); });
        if (prior.length && Math.abs(b.low - d[prior[prior.length - 1]._i].low) / b.close < 0.02) {
          tag = "NS"; pos = "belowBar"; color = "#2962ff"; shape = "circle";
        } else if (rvol <= 0.45 && spr < sprAvg * 0.7 && !down) {
          tag = "ACC"; pos = "belowBar"; color = "#089981"; shape = "circle";
        }
      }
      if (tag === "SC" && rvol >= 3.5 && clv <= -0.5) tag = "CAPIT";
      if (tag) out.push({ time: b.time, position: pos, color: color, shape: shape, text: tag, _i: i, rvol: rvol });
    }
    return out;
  }

  window.jhVolEvents = function (d) {
    return events(d).map(function (e) {
      return { time: e.time, position: e.position, color: e.color, shape: e.shape, text: e.text };
    });
  };

  window.jhVolEventTable = function (d) {
    return events(d).slice(-8);
  };
})();
