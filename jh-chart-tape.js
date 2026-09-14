/* Bloomberg-grade tape: Livermore + Wyckoff consume the 100% gold-list table only.
   No 4-bar PH/PL wallpaper, no SPRING/UTAD/AR/ST — those failed the S&P 1980–now bar. */
(function () {
  function mean(d, i, n, key) {
    var a = Math.max(0, i - n), s = 0, j, c = 0;
    for (j = a; j < i; j++) { s += d[j][key] || 0; c++; }
    return c ? s / c : 0;
  }
  function rngMean(d, i, n) {
    var a = Math.max(0, i - n), s = 0, j, c = 0;
    for (j = a; j < i; j++) { s += (d[j].high - d[j].low); c++; }
    return c ? s / c : 0;
  }
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
  function tag(t, pos, color, shape, text) {
    return { time: t, position: pos, color: color, shape: shape, text: text };
  }
  function metaBar(d, i) {
    var b = d[i], rng = b.high - b.low || 1e-12;
    var vAvg = mean(d, i, 20, "volume");
    return {
      rvol: vAvg ? (b.volume || 0) / vAvg : 0,
      closeLoc: (b.close - b.low) / rng,
      rng: rng,
      body: Math.abs(b.close - b.open),
      spr: b.close ? rng / Math.abs(b.close) : 0,
      up: b.close > b.open,
      down: b.close < b.open,
      vAvg: vAvg
    };
  }
  function tableRows(d) {
    return (window.jhVolEventTable && window.jhVolEventTable(d)) || [];
  }
  function fromTable(d, map) {
    var rows = tableRows(d), out = [], i, e, spec;
    for (i = 0; i < rows.length; i++) {
      e = rows[i];
      spec = map[e.kind];
      if (!spec) continue;
      out.push(tag(e.time, spec.pos, spec.color || e.color, spec.shape, spec.text || e.label));
    }
    return out;
  }

  /* Jesse Livermore: pivotal cycle points + confirmed reverse of the last pivotal point. */
  function livermore(d) {
    var marks = fromTable(d, {
      bottom: { pos: "belowBar", color: "#089981", shape: "arrowUp", text: "BOTTOM" },
      top: { pos: "aboveBar", color: "#f23645", shape: "arrowDown", text: "TOP" },
      revup: { pos: "belowBar", color: "#089981", shape: "arrowUp", text: "REV-UP" },
      revdn: { pos: "aboveBar", color: "#f23645", shape: "arrowDown", text: "REV-DN" }
    });
    var trend = 0, i;
    for (i = 0; i < marks.length; i++) {
      if (marks[i].text === "REV-UP" || marks[i].text === "BOTTOM") trend = 1;
      if (marks[i].text === "REV-DN" || marks[i].text === "TOP") trend = -1;
    }
    var note = trend === 1 ? "uptrend from last cycle low" : trend === -1 ? "downtrend from last cycle high" : "no confirmed cycle turn";
    return {
      trend: trend === 1 ? "UPTREND" : trend === -1 ? "DOWNTREND" : "TEST",
      note: note,
      markers: marks
    };
  }

  /* Wyckoff: climax + SOS/SOW that hit the gold list. No Spring/UTAD wallpaper. */
  function wyckoffScan(d) {
    return fromTable(d, {
      capit: { pos: "belowBar", color: "#f23645", shape: "arrowDown", text: "CAPIT" },
      sc: { pos: "belowBar", color: "#ef5350", shape: "arrowDown", text: "SC" },
      bc: { pos: "aboveBar", color: "#26a69a", shape: "arrowUp", text: "BC" },
      eoa: { pos: "aboveBar", color: "#089981", shape: "arrowUp", text: "SOS" },
      eod: { pos: "belowBar", color: "#ab47bc", shape: "arrowDown", text: "SOW" }
    });
  }

  /* VSA — Tom Williams tape: demand/supply, stopping, absorption, tests, traps. */
  function vsaScan(d) {
    var out = [], i, lastTag = -9, lastKind = "";
    if (d.length < 25) return out;
    for (i = 20; i < d.length; i++) {
      var b = d[i], m = metaBar(d, i);
      var v1 = d[i - 1].volume || 0, v2 = d[i - 2].volume || 0;
      var less2 = (b.volume || 0) < v1 && (b.volume || 0) < v2;
      var rAvg = rngMean(d, i, 20);
      var rise = b.close > d[i - 5].close, fall = b.close < d[i - 5].close;
      var donHi = hh(d, i, 20), donLo = ll(d, i, 20);
      var kind = null, label = null, color = "#ff9800", pos = "aboveBar", shape = "circle";

      if (m.down && m.rvol >= 1.5 && m.closeLoc >= 0.55) {
        kind = "sv"; label = "SV"; color = "#089981"; pos = "belowBar"; shape = "arrowUp";
      } else if (m.rvol >= 1.55 && m.body / m.rng <= 0.34 && m.rng <= rAvg * 1.05) {
        kind = "abs"; label = "ABS"; color = "#ff9800"; pos = m.up ? "aboveBar" : "belowBar"; shape = "square";
      } else if (m.down && m.rvol >= 1.28 && m.closeLoc >= 0.62) {
        kind = "hb"; label = "HB"; color = "#089981"; pos = "belowBar"; shape = "arrowUp";
      } else if (m.up && m.rvol >= 1.28 && m.closeLoc <= 0.38) {
        kind = "hs"; label = "HS"; color = "#f23645"; pos = "aboveBar"; shape = "arrowDown";
      } else if (b.high > donHi && m.closeLoc <= 0.35 && m.rng >= rAvg * 1.05) {
        kind = "trap"; label = "TRAP"; color = "#f23645"; pos = "aboveBar"; shape = "arrowDown";
      } else if (b.low < donLo && m.closeLoc >= 0.65 && m.rng >= rAvg * 0.9) {
        kind = "shk"; label = "SHK"; color = "#089981"; pos = "belowBar"; shape = "arrowUp";
      } else if (fall && m.down && less2 && m.closeLoc >= 0.45 && m.rng <= rAvg * 1.15) {
        kind = "test"; label = "TEST"; color = "#2962ff"; pos = "belowBar"; shape = "circle";
      } else if (m.up && less2 && rise && m.rng <= rAvg * 1.1) {
        kind = "nd"; label = "ND"; color = "#ef9a9a"; pos = "aboveBar"; shape = "circle";
      } else if (m.down && less2 && fall && m.rng <= rAvg * 1.1) {
        kind = "ns"; label = "NS"; color = "#80cbc4"; pos = "belowBar"; shape = "circle";
      }
      if (!kind) continue;
      if ((kind === "nd" || kind === "ns" || kind === "test") && i - lastTag < 4 && (lastKind === kind || lastKind === "nd" || lastKind === "ns")) continue;
      lastTag = i; lastKind = kind;
      out.push(tag(b.time, pos, color, shape, label));
    }
    return out;
  }

  function effort(d) {
    var out = [], i;
    for (i = 10; i < d.length; i++) {
      var vA = 0, pA = 0, j;
      for (j = i - 10; j < i; j++) { vA += d[j].volume || 0; pA += Math.abs(d[j].close - d[j].open); }
      vA /= 10; pA /= 10;
      var body = Math.abs(d[i].close - d[i].open);
      var vol = d[i].volume || 0;
      if (vA && vol > vA * 1.65 && pA && body < pA * 0.55) {
        out.push(tag(d[i].time, d[i].close >= d[i].open ? "aboveBar" : "belowBar", "#f0b429", "circle",
          d[i].close >= d[i].open ? "E↑noR" : "E↓noR"));
      }
    }
    return out;
  }

  function accumDistrib(d) {
    var accum = fromTable(d, {
      bottom: { pos: "belowBar", color: "#089981", shape: "arrowUp", text: "BOTTOM" },
      eoa: { pos: "aboveBar", color: "#2962ff", shape: "arrowUp", text: "EOA" },
      capit: { pos: "belowBar", color: "#f23645", shape: "arrowDown", text: "CAPIT" },
      sc: { pos: "belowBar", color: "#ef5350", shape: "arrowDown", text: "SC" }
    });
    var distrib = fromTable(d, {
      top: { pos: "aboveBar", color: "#f23645", shape: "arrowDown", text: "TOP" },
      eod: { pos: "belowBar", color: "#ab47bc", shape: "arrowDown", text: "EOD" },
      bc: { pos: "aboveBar", color: "#26a69a", shape: "arrowUp", text: "BC" }
    });
    return { accum: accum, distrib: distrib };
  }

  function keepLast(arr, n) {
    if (!arr || arr.length <= n) return arr || [];
    return arr.slice(-n);
  }

  window.jhTapeRead = function (d) {
    if (!d || d.length < 40) {
      return { markers: [], panel: "tape: short", livermore: { markers: [] }, wyckoff: { markers: [] }, accum: { markers: [] }, distrib: { markers: [] }, vsa: { markers: [] }, tape: { markers: [] } };
    }
    var lv = livermore(d);
    var wy = wyckoffScan(d);
    var vs = vsaScan(d);
    var ef = effort(d);
    var ad = accumDistrib(d);
    var wyMarks = wy;
    var vsaMarks = keepLast(vs.concat(ef), 90);
    var all = lv.markers.concat(wyMarks).concat(vsaMarks);
    var panel = "LIVERMORE " + lv.trend + " · " + lv.note + " | WYCKOFF SC/CAPIT · SOS/EOA · SOW/EOD · BC | cycle BOTTOM/TOP/REV";
    return {
      markers: all,
      panel: panel,
      livermore: { trend: lv.trend, note: lv.note, markers: lv.markers },
      wyckoff: { markers: wyMarks },
      accum: { markers: ad.accum },
      distrib: { markers: ad.distrib },
      vsa: { markers: vsaMarks },
      tape: { markers: all }
    };
  };
  window.__jhTapeReadRaw = window.jhTapeRead;
})();
