/* Climax / bottom / accumulation marks for whatever symbol is on the chart.
   Daily history uses the gold-list scans (SC, confirmed cycle low, end of
   accumulation). The bottom harvest, when it has this ticker, snaps its
   printed sc_date / st_date / trigger_date onto those same labels. */
(function () {
  var board = null;
  var inflight = null;
  var repainted = false;
  var dailySaved = {};

  function pad(n) { return n < 10 ? "0" + n : String(n); }
  function ymd(ts) {
    var dt = new Date(ts * 1000);
    return dt.getUTCFullYear() + "-" + pad(dt.getUTCMonth() + 1) + "-" + pad(dt.getUTCDate());
  }
  function bare(sym) {
    var s = String(sym || "").toUpperCase().trim();
    if (!s) return "";
    if (s.indexOf(":") >= 0) s = s.split(":").pop();
    s = s.replace(/\s+/g, "");
    return s;
  }
  function keysOf(sym) {
    var b = bare(sym);
    var out = {};
    if (!b) return [];
    out[b] = 1;
    out[b.replace(/USDT$/, "")] = 1;
    out[b.replace(/-USD$/, "")] = 1;
    out[b.replace(/USD$/, "")] = 1;
    if (b.indexOf("-") < 0 && b.slice(-3) !== "USD") out[b + "-USD"] = 1;
    return Object.keys(out).filter(Boolean);
  }
  function tag(t, pos, color, shape, text) {
    return { time: t, position: pos, color: color, shape: shape, text: text };
  }
  function medGap(d) {
    if (!d || d.length < 3) return 0;
    var g = [], i, n = Math.min(d.length - 1, 80);
    for (i = d.length - n; i < d.length; i++) if (i > 0 && d[i].time > d[i - 1].time) g.push(d[i].time - d[i - 1].time);
    if (!g.length) return 0;
    g.sort(function (a, b) { return a - b; });
    return g[Math.floor(g.length / 2)] || 0;
  }
  function isDaily(d, tf) {
    if (tf === "1d") return true;
    var g = medGap(d);
    return g >= 18 * 3600 && g <= 4 * 86400;
  }
  function realKind(kind) {
    return !kind || kind === "candles" || kind === "hollow" || kind === "volcandle";
  }
  function snapTime(d, dateStr) {
    if (!dateStr || !d || !d.length) return null;
    var target = Date.parse(String(dateStr).slice(0, 10) + "T00:00:00Z") / 1000;
    if (!isFinite(target)) return null;
    var best = null, bestAbs = 1e15, i, step = medGap(d) || 86400;
    for (i = 0; i < d.length; i++) {
      var ad = Math.abs(d[i].time - target);
      if (ad < bestAbs) { bestAbs = ad; best = d[i].time; }
    }
    if (best == null || bestAbs > Math.max(step * 1.6, 8 * 86400)) return null;
    return best;
  }
  function indexBoard(j) {
    var map = {}, rows = (j && (j.board || j.board_all)) || [], i, r, ks, k;
    for (i = 0; i < rows.length; i++) {
      r = rows[i];
      if (!r || !r.ticker) continue;
      ks = keysOf(r.ticker);
      for (k = 0; k < ks.length; k++) {
        if (!map[ks[k]]) map[ks[k]] = r;
      }
    }
    return map;
  }
  function rowFor(sym) {
    if (!board) return null;
    var ks = keysOf(sym), i;
    for (i = 0; i < ks.length; i++) if (board[ks[i]]) return board[ks[i]];
    return null;
  }
  function ensureBoard() {
    if (board || inflight) return;
    if (typeof fetch !== "function") return;
    inflight = fetch("/data/bottom.json", { cache: "no-store" }).then(function (r) {
      if (!r.ok) throw new Error(String(r.status));
      return r.json();
    }).then(function (j) {
      board = indexBoard(j);
      inflight = null;
      if (!repainted && window.paint && window.lastBars && window.lastBars.length) {
        repainted = true;
        try { window.paint(window.lastBars); } catch (e) {}
      }
    }).catch(function () { inflight = null; });
  }
  function fromScan(d) {
    var table = window.jhVolEventTable;
    if (typeof table !== "function") return [];
    var rows = table(d) || [];
    var sc = [], bot = [], eoa = [], i, e, k;
    for (i = 0; i < rows.length; i++) {
      e = rows[i];
      if (!e || e.time == null) continue;
      if (e.kind === "sc") sc.push(e);
      else if (e.kind === "bottom") bot.push(e);
      else if (e.kind === "eoa") eoa.push(e);
    }
    var out = [], used = {};
    for (i = 0; i < bot.length; i++) {
      var b = bot[i], best = null;
      for (k = 0; k < sc.length; k++) {
        var s = sc[k];
        if (s.i == null || b.i == null) continue;
        if (s.i >= b.i || s.time === b.time) continue;
        if (b.i - s.i > 80) continue;
        if (!best || (s.score || 0) > (best.score || 0)) best = s;
      }
      /* Same bar as the low: one label, BOTTOM. The climax still prints when it was earlier. */
      if (best && best.time !== b.time) {
        used[best.i] = 1;
        out.push(tag(best.time, "belowBar", "#ef5350", "arrowDown", "CLIMAX"));
      }
      out.push(tag(b.time, "belowBar", "#089981", "arrowUp", "BOTTOM"));
    }
    for (i = 0; i < eoa.length; i++) {
      out.push(tag(eoa[i].time, "aboveBar", "#2962ff", "arrowUp", "ACCUM"));
    }
    if (d.length && !bot.length) {
      var last = d.length - 1, live = null;
      for (k = 0; k < sc.length; k++) {
        s = sc[k];
        if (s.i == null || last - s.i > 40 || used[s.i]) continue;
        if ((s.score || 0) < 4) continue;
        if (!live || (s.score || 0) > (live.score || 0)) live = s;
      }
      if (live) out.push(tag(live.time, "belowBar", "#ef5350", "arrowDown", "CLIMAX"));
    } else if (d.length) {
      var lastI = 0;
      for (i = 0; i < d.length; i++) lastI = i;
      var live2 = null;
      for (k = 0; k < sc.length; k++) {
        s = sc[k];
        if (s.i == null || used[s.i]) continue;
        if (lastI - s.i > 40) continue;
        if ((s.score || 0) < 4) continue;
        var after = false;
        for (i = 0; i < bot.length; i++) if (bot[i].i > s.i) after = true;
        if (after) continue;
        if (!live2 || (s.score || 0) > (live2.score || 0)) live2 = s;
      }
      if (live2) out.push(tag(live2.time, "belowBar", "#ef5350", "arrowDown", "CLIMAX"));
    }
    return out;
  }
  function fromHarvest(d, sym) {
    var r = rowFor(sym);
    if (!r) return [];
    var out = [];
    function put(date, text, pos, color, shape) {
      var t = snapTime(d, date);
      if (t == null) return;
      out.push(tag(t, pos, color, shape, text));
    }
    put(r.sc_date, "CLIMAX", "belowBar", "#ef5350", "arrowDown");
    put(r.st_date, "BOTTOM", "belowBar", "#089981", "arrowUp");
    if (r.trigger_date) put(r.trigger_date, "ACCUM", "aboveBar", "#2962ff", "arrowUp");
    return out;
  }
  function project(saved, d) {
    if (!saved || !saved.length || !d || !d.length) return [];
    var out = [], i;
    for (i = 0; i < saved.length; i++) {
      var t = snapTime(d, ymd(saved[i].time));
      if (t == null) continue;
      var m = saved[i];
      out.push(tag(t, m.position, m.color, m.shape, m.text));
    }
    return out;
  }

  window.jhCampaignMarks = function (d, sym, tf, kind) {
    ensureBoard();
    if (!d || d.length < 8) return [];
    var key = bare(sym) || String(sym || "");
    var marks = [];
    if (realKind(kind) && isDaily(d, tf)) {
      marks = fromScan(d);
      if (marks.length) dailySaved[key] = marks;
    } else if (dailySaved[key]) {
      marks = project(dailySaved[key], d);
    }
    var hv = fromHarvest(d, sym);
    return hv.concat(marks);
  };
  window.__jhCampaignBoardIndex = indexBoard;
})();
