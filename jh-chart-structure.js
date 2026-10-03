/* Structure marks. Confirmed after the bar. Not a forecast.
   Paints through jhDistributionMarks, which the engine already concatenates. */
(function () {
  if (window.jhStructureMarks) return;
  var K = "jhStruct.v1";
  var on = { t2: 1, b2: 1, ac: 1, dist: 1, spr: 1, ut: 1, thr: 1, sc: 1, bc: 1 };
  try { var saved = JSON.parse(localStorage.getItem(K) || "null"); if (saved) on = saved; } catch (e) {}
  function save() { try { localStorage.setItem(K, JSON.stringify(on)); } catch (e) {} }
  function daily(tf, d) {
    var t = String(tf || "").toLowerCase();
    if (t === "1d" || t === "d" || t === "1w" || t === "w" || t === "1wk") return true;
    if (!d || d.length < 3) return false;
    return d[d.length - 1].time - d[d.length - 2].time >= 18 * 3600;
  }
  function vol(b) { var v = b && b.volume; return v > 0 ? v : 0; }
  function pivots(d, wing) {
    var hi = [], lo = [], i, k, ok;
    for (i = wing; i < d.length - wing - 1; i++) {
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
  function build(d) {
    var out = [], used = {}, wing = d.length > 400 ? 5 : 3;
    var p = pivots(d, wing), i, a, b, lo, hi, j, n, s;
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
        if (!take(d[b], "2T")) continue;
        out.push(mark(d[b], "aboveBar", "#e0b04a", "arrowDown", "2T"));
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
        if (!take(d[b], "2B")) continue;
        out.push(mark(d[b], "belowBar", "#7eb8e8", "arrowUp", "2B"));
        n++;
      }
    }
    n = 0;
    for (i = 40; i < d.length - 6 && n < 4; i += 8) {
      var start = i - 18, rh = d[start].high, rl = d[start].low, rv = 0, rc = 0;
      for (j = start; j <= i; j++) {
        if (d[j].high > rh) rh = d[j].high;
        if (d[j].low < rl) rl = d[j].low;
        if (vol(d[j])) { rv += vol(d[j]); rc++; }
      }
      var mid = (rh + rl) / 2;
      if (!(mid > 0 && (rh - rl) / mid < 0.14)) continue;
      var prior = d[Math.max(0, start - 20)].close;
      var fell = prior > 0 && d[start].close < prior * 0.94;
      var rose = prior > 0 && d[start].close > prior * 1.06;
      var avg = rc ? rv / rc : 0;
      if (fell && on.ac && take(d[i], "AC")) { out.push(mark(d[i], "belowBar", "#3d9a78", "circle", "AC")); n++; }
      if (rose && on.dist && take(d[i], "DIST")) { out.push(mark(d[i], "aboveBar", "#c9942e", "circle", "DIST")); n++; }
      var end = Math.min(d.length - 1, i + 16);
      for (j = i + 1; j <= end; j++) {
        var bar = d[j];
        if (on.spr && fell && bar.low < rl && bar.close >= rl && take(bar, "SPR")) out.push(mark(bar, "belowBar", "#8fd6c4", "arrowUp", "SPR"));
        if (on.ut && rose && bar.high > rh && bar.close <= rh && take(bar, "UT")) out.push(mark(bar, "aboveBar", "#e07a5f", "arrowDown", "UT"));
        if (on.thr && avg && vol(bar) > avg * 1.15) {
          if (fell && bar.close > rh && take(bar, "THR")) out.push(mark(bar, "aboveBar", "#f2cc8f", "circle", "THR"));
          if (rose && bar.close < rl && take(bar, "THR")) out.push(mark(bar, "belowBar", "#f2cc8f", "circle", "THR"));
        }
      }
    }
    if (on.sc || on.bc) {
      var med = [];
      for (i = Math.max(1, d.length - 80); i < d.length - 1; i++) med.push(d[i].high - d[i].low);
      med.sort(function (x, y) { return x - y; });
      var width = med.length ? med[Math.floor(med.length / 2)] : 0;
      n = 0;
      for (i = d.length - 2; i > 20 && n < 4; i--) {
        var c = d[i], w = c.high - c.low, v20 = 0, c20 = 0;
        for (s = i - 20; s < i; s++) if (vol(d[s])) { v20 += vol(d[s]); c20++; }
        var base = c20 ? v20 / c20 : 0;
        if (!(w > width * 1.8 && base && vol(c) >= base * 2)) continue;
        var offLow = w > 0 && (c.close - c.low) / w > 0.45 && c.close < c.open && c.close < d[i - 12].close;
        var offHigh = w > 0 && (c.high - c.close) / w > 0.45 && c.close > c.open && c.close > d[i - 12].close;
        if (on.sc && offLow && take(c, "SC")) { out.push(mark(c, "belowBar", "#d64b4b", "arrowUp", "SC")); n++; }
        else if (on.bc && offHigh && take(c, "BC")) { out.push(mark(c, "aboveBar", "#2a9d8f", "arrowDown", "BC")); n++; }
      }
    }
    out.sort(function (x, y) { return x.time - y.time; });
    return out;
  }
  window.jhStructureMarks = build;
  function install() {
    var cur = window.jhDistributionMarks;
    if (cur && cur.__struct) return;
    var prev = cur;
    function wrapped(display, tf, kind) {
      var base = [];
      try { base = (prev && prev(display, tf, kind)) || []; } catch (e) { base = []; }
      if (!display || display.length < 40 || !daily(tf, display)) return base;
      var extra = [];
      try { extra = build(display); } catch (e2) { extra = []; }
      var seen = {}, ix, out = [];
      for (ix = 0; ix < base.length; ix++) { seen[base[ix].time] = 1; out.push(base[ix]); }
      for (ix = 0; ix < extra.length; ix++) if (!seen[extra[ix].time]) out.push(extra[ix]);
      out.sort(function (a, b) { return a.time - b.time; });
      return out;
    }
    wrapped.__struct = 1;
    window.jhDistributionMarks = wrapped;
  }
  install();
  function legend() {
    if (document.getElementById("jh-struct")) return;
    var host = document.getElementById("host");
    if (!host || !host.parentNode) return;
    var bar = document.createElement("div");
    bar.id = "jh-struct";
    bar.style.cssText = "display:flex;flex-wrap:wrap;gap:4px;padding:4px 8px;font:11px IBM Plex Mono,monospace;color:#c8cdd8";
    [["t2", "2T"], ["b2", "2B"], ["ac", "AC"], ["dist", "DIST"], ["spr", "SPR"], ["ut", "UT"], ["thr", "THR"], ["sc", "SC"], ["bc", "BC"]].forEach(function (pair) {
      var b = document.createElement("button");
      b.type = "button";
      b.textContent = pair[1];
      b.style.cssText = "border:1px solid #3a4254;background:#1c2230;color:#d5d8e0;border-radius:3px;padding:2px 6px;cursor:pointer";
      function paint() { b.style.opacity = on[pair[0]] ? "1" : "0.4"; }
      paint();
      b.onclick = function () {
        on[pair[0]] = on[pair[0]] ? 0 : 1;
        save();
        paint();
        if (window.paint && window.lastBars) { try { window.paint(window.lastBars); } catch (e) {} }
      };
      bar.appendChild(b);
    });
    host.parentNode.insertBefore(bar, host);
    window.jhStructure = { on: on };
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", legend);
  else setTimeout(legend, 0);
})();
