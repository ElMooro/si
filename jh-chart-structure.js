(function () {
  if (window.jhStructureMarks) return;
  var K = "jhStruct.v2";
  var baseOn = { t2: 1, b2: 1, ac: 1, dist: 1, spr: 1, ut: 1, thr: 1, sc: 1, bc: 1 };
  var on = { t2: 1, b2: 1, ac: 1, dist: 1, spr: 1, ut: 1, thr: 1, sc: 1, bc: 1 };
  try {
    var saved = JSON.parse(localStorage.getItem(K) || "null");
    if (saved && typeof saved === "object") {
      Object.keys(baseOn).forEach(function (k) {
        if (saved[k] === 0 || saved[k] === 1) on[k] = saved[k];
      });
    }
  } catch (e) {}
  function save() { try { localStorage.setItem(K, JSON.stringify(on)); } catch (e) {} }
  function daily(tf, d) {
    var t = String(tf || "").toLowerCase();
    if (t === "1d" || t === "d" || t === "1w" || t === "w" || t === "1wk") return true;
    if (!d || d.length < 3) return false;
    return d[d.length - 1].time - d[d.length - 2].time >= 18 * 3600;
  }
  function candleKind(kind) {
    return !kind || kind === "candles" || kind === "hollow" || kind === "volcandle";
  }
  function vol(b) { var v = b && b.volume; return v > 0 ? v : 0; }
  function pivots(d, wing, last) {
    var hi = [], lo = [], i, k, ok;
    for (i = wing; i <= last - wing; i++) {
      ok = true;
      for (k = 1; k <= wing; k++) if (d[i].high < d[i - k].high || d[i].high < d[i + k].high) ok = false;
      if (ok) hi.push(i);
      ok = true;
      for (k = 1; k <= wing; k++) if (d[i].low > d[i - k].low || d[i].low > d[i + k].low) ok = false;
      if (ok) lo.push(i);
    }
    return { hi: hi, lo: lo };
  }
  function mark(b, pos, color, shape, text) {
    return { time: b.time, position: pos, color: color, shape: shape, text: text };
  }
  function edge(d, a, b) {
    var highs = [], lows = [], rv = 0, rc = 0, j;
    for (j = a; j <= b; j++) {
      highs.push(d[j].high);
      lows.push(d[j].low);
      if (vol(d[j])) { rv += vol(d[j]); rc++; }
    }
    highs.sort(function (x, y) { return x - y; });
    lows.sort(function (x, y) { return x - y; });
    return {
      rh: highs[Math.max(0, highs.length - 2)],
      rl: lows[Math.min(lows.length - 1, 1)],
      avg: rc ? rv / rc : 0
    };
  }
  function ranges(d, last) {
    var found = [], end, len, start, j, box, mid, width, prior, fell, rose, score, local, inside;
    for (end = last - 3; end >= 36; end -= 2) {
      if (found.length >= 2) break;
      local = null;
      for (len = 12; len <= 36; len += 2) {
        start = end - len;
        if (start < 10) continue;
        box = edge(d, start, end);
        mid = (box.rh + box.rl) / 2;
        if (!(mid > 0) || !(box.rh > box.rl)) continue;
        width = (box.rh - box.rl) / mid;
        if (width > 0.11 || width < 0.012) continue;
        inside = 0;
        for (j = start; j <= end; j++) if (d[j].close <= box.rh && d[j].close >= box.rl) inside++;
        if (inside / (end - start + 1) < 0.7) continue;
        prior = d[Math.max(0, start - 16)].close;
        fell = prior > d[start].close * 1.06;
        rose = d[start].close > prior * 1.06;
        if (fell === rose) continue;
        score = (0.11 - width) * 100 + len * 0.2 + end * 0.01;
        if (!local || score > local.score) local = { start: start, end: end, rh: box.rh, rl: box.rl, avg: box.avg, fell: fell, rose: rose, score: score };
      }
      if (!local) continue;
      var hit = false;
      for (j = 0; j < found.length; j++) if (!(local.end < found[j].start || local.start > found[j].end)) hit = true;
      if (!hit) found.push(local);
    }
    return found;
  }
  function build(d) {
    var out = [], used = {}, last = d.length - 2, wing = d.length > 400 ? 5 : 3;
    if (last < 40) return out;
    var p = pivots(d, wing, last), i, a, b, lo, hi, j, n, s;
    function take(bar, token) {
      if (!bar || used[bar.time]) return false;
      used[bar.time] = token;
      return true;
    }
    if (on.t2) {
      n = 0;
      for (i = p.hi.length - 1; i > 0 && n < 3; i--) {
        a = p.hi[i - 1]; b = p.hi[i];
        if (b - a < 8 || b - a > 70) continue;
        if (Math.abs(d[a].high - d[b].high) / d[a].high > 0.025) continue;
        lo = d[a].low;
        for (j = a; j <= b; j++) if (d[j].low < lo) lo = d[j].low;
        if ((Math.min(d[a].high, d[b].high) - lo) / d[a].high < 0.04) continue;
        if (d[b].close > d[a].high * 1.004) continue;
        if (!take(d[b], "D-TOP")) continue;
        out.push(mark(d[b], "aboveBar", "#F0B429", "arrowDown", "D-TOP"));
        n++;
      }
    }
    if (on.b2) {
      n = 0;
      for (i = p.lo.length - 1; i > 0 && n < 3; i--) {
        a = p.lo[i - 1]; b = p.lo[i];
        if (b - a < 8 || b - a > 70) continue;
        if (Math.abs(d[a].low - d[b].low) / d[a].low > 0.025) continue;
        hi = d[a].high;
        for (j = a; j <= b; j++) if (d[j].high > hi) hi = d[j].high;
        if ((hi - Math.max(d[a].low, d[b].low)) / d[a].low < 0.04) continue;
        if (d[b].close < d[a].low * 0.996) continue;
        if (!take(d[b], "DB")) continue;
        out.push(mark(d[b], "belowBar", "#7EB8E8", "arrowUp", "DB"));
        n++;
      }
    }
    var rs = ranges(d, last), r, bar;
    for (r = 0; r < rs.length; r++) {
      var rg = rs[r];
      if (rg.fell && on.ac && take(d[rg.end], "ACCUM")) out.push(mark(d[rg.end], "belowBar", "#6fce8a", "circle", "ACCUM"));
      if (rg.rose && on.dist && take(d[rg.end], "DIST")) out.push(mark(d[rg.end], "aboveBar", "#F0B429", "circle", "DIST"));
      for (j = rg.start; j <= last; j++) {
        bar = d[j];
        if (on.spr && rg.fell && bar.low < rg.rl - Math.max(rg.rl * 0.004, (rg.rh - rg.rl) * 0.18) && bar.close >= rg.rl && bar.close <= rg.rh && take(bar, "SPRING")) out.push(mark(bar, "belowBar", "#6fce8a", "arrowUp", "SPRING"));
        if (on.ut && rg.rose && bar.high > rg.rh + Math.max(rg.rh * 0.004, (rg.rh - rg.rl) * 0.18) && bar.close <= rg.rh && bar.close >= rg.rl && take(bar, "UT")) out.push(mark(bar, "aboveBar", "#E07A6A", "arrowDown", "UT"));
        if (on.thr && rg.avg && vol(bar) >= rg.avg * 1.35) {
          if (bar.close > rg.rh && take(bar, "SOS")) out.push(mark(bar, "aboveBar", "#FFD45E", "circle", "SOS"));
          if (bar.close < rg.rl && take(bar, "SOW")) out.push(mark(bar, "belowBar", "#FFD45E", "circle", "SOW"));
        }
      }
    }
    if (on.sc || on.bc) {
      var med = [];
      for (i = Math.max(1, last - 80); i <= last; i++) med.push(d[i].high - d[i].low);
      med.sort(function (x, y) { return x - y; });
      var width = med.length ? med[Math.floor(med.length / 2)] : 0;
      n = 0;
      for (i = last; i > 20 && n < 4; i--) {
        var c = d[i], w = c.high - c.low, v20 = 0, c20 = 0;
        for (s = i - 20; s < i; s++) if (vol(d[s])) { v20 += vol(d[s]); c20++; }
        var avg = c20 ? v20 / c20 : 0;
        if (!(w > width * 1.8 && avg && vol(c) >= avg * 2)) continue;
        var offLow = w > 0 && (c.close - c.low) / w >= 0.4 && c.close < c.open && c.low < d[i - 12].low;
        var offHigh = w > 0 && (c.high - c.close) / w >= 0.4 && c.close > c.open && c.high > d[i - 12].high;
        if (on.sc && offLow && take(c, "SC")) { out.push(mark(c, "belowBar", "#E07A6A", "arrowUp", "SC")); n++; }
        else if (on.bc && offHigh && take(c, "BC")) { out.push(mark(c, "aboveBar", "#C9942E", "arrowDown", "BC")); n++; }
      }
    }
    out.sort(function (x, y) { return x.time - y.time; });
    return out;
  }
  window.jhStructureMarks = build;
  function install() {
    var cur = window.jhDistributionMarks;
    if (cur && cur.__struct) return true;
    var prev = cur;
    function wrapped(display, tf, kind) {
      var base = [];
      try { base = (prev && prev(display, tf, kind)) || []; } catch (e) { base = []; }
      if (!display || display.length < 40 || !daily(tf, display) || !candleKind(kind)) return base;
      var extra = [];
      try { extra = build(display); } catch (e2) { extra = []; }
      var seen = {}, ix, row, out = base.slice();
      for (ix = 0; ix < base.length; ix++) seen[base[ix].time + "|" + base[ix].text] = 1;
      for (ix = 0; ix < extra.length; ix++) {
        row = extra[ix];
        if (seen[row.time + "|" + row.text]) continue;
        seen[row.time + "|" + row.text] = 1;
        out.push(row);
      }
      out.sort(function (a, b) { return a.time - b.time; });
      return out;
    }
    wrapped.__struct = 1;
    window.jhDistributionMarks = wrapped;
    return true;
  }
  function kick() {
    if (window.paint && window.lastBars && window.lastBars.length) {
      try { window.paint(window.lastBars); } catch (e) {}
    }
  }
  install();
  var ntry = 0;
  var iv = setInterval(function () {
    var cur = window.jhDistributionMarks;
    if (!cur || !cur.__struct) install();
    ntry++;
    if (ntry === 2 || ntry === 6) kick();
    if (ntry > 16) clearInterval(iv);
  }, 400);
  function legend() {
    if (document.getElementById("jh-struct")) return;
    var host = document.getElementById("host");
    if (!host || !host.parentNode) return;
    var bar = document.createElement("div");
    bar.id = "jh-struct";
    bar.style.cssText = "position:absolute;z-index:20;left:8px;bottom:32px;display:flex;flex-wrap:wrap;gap:3px;max-width:78%;padding:3px 4px;background:#12110Cee;border:1px solid #2B2820;font:11px monospace;color:#b5ad99";
    var cap = document.createElement("span");
    cap.textContent = "STRUCT";
    cap.style.cssText = "padding:0 4px;letter-spacing:.04em";
    bar.appendChild(cap);
    [["t2", "2T"], ["b2", "2B"], ["ac", "AC"], ["dist", "DIST"], ["spr", "SPR"], ["ut", "UT"], ["thr", "THR"], ["sc", "SC"], ["bc", "BC"]].forEach(function (pair) {
      var b = document.createElement("button");
      b.type = "button";
      b.textContent = pair[1];
      b.style.cssText = "border:1px solid #2B2820;background:#12110C;color:#b5ad99;border-radius:3px;padding:2px 6px;cursor:pointer";
      function paint() { b.style.opacity = on[pair[0]] ? "1" : "0.35"; }
      paint();
      b.onclick = function () {
        on[pair[0]] = on[pair[0]] ? 0 : 1;
        save();
        paint();
        kick();
      };
      bar.appendChild(b);
    });
    host.parentNode.appendChild(bar);
    window.jhStructure = { on: on };
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", legend);
  else setTimeout(legend, 0);
})();
