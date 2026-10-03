/* Structure marks. Confirmed after the bar. Not a forecast.
   Paints through jhDistributionMarks, which the engine already concatenates. */
(function () {
  if (window.jhStructure) return;
  var K = "jhStruct.v1";
  var on = { t2: 1, b2: 1, ac: 1, dist: 1, spr: 1, ut: 1, thr: 1, sc: 1 };
  try { var saved = JSON.parse(localStorage.getItem(K) || "null"); if (saved) on = saved; } catch (e) {}
  function save() { try { localStorage.setItem(K, JSON.stringify(on)); } catch (e) {} }
  function daily(tf) { return tf === "D" || tf === "1D" || tf === "W" || tf === "1W"; }
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
    var p = pivots(d, wing), i, a, b, lo, hi, j, n;
    function take(bar, token) { if (used[bar.time]) return; used[bar.time] = token; }
    if (on.t2) {
      n = 0;
      for (i = p.hi.length - 1; i > 0 && n < 2; i--) {
        a = p.hi[i - 1]; b = p.hi[i];
        if (b - a < 8 || b - a > 70) continue;
        if (Math.abs(d[a].high - d[b].high) / d[a].high > 0.025) continue;
        lo = d[a].low;
        for (j = a; j <= b; j++) if (d[j].low < lo) lo = d[j].low;
        if ((Math.min(d[a].high, d[b].high) - lo) / d[a].high < 0.04) continue;
        take(d[b], "2T");
        out.push(mark(d[b], "aboveBar", "#e0b04a", "arrowDown", "2T"));
        n++;
      }
    }
    if (on.b2) {
      n = 0;
      for (i = p.lo.length - 1; i > 0 && n < 2; i--) {
        a = p.lo[i - 1]; b = p.lo[i];
        if (b - a < 8 || b - a > 70) continue;
        if (Math.abs(d[a].low - d[b].low) / d[a].low > 0.025) continue;
        hi = d[a].high;
        for (j = a; j <= b; j++) if (d[j].high > hi) hi = d[j].high;
        if ((hi - Math.max(d[a].low, d[b].low)) / d[a].low < 0.04) continue;
        if (used[d[b].time]) continue;
        take(d[b], "2B");
        out.push(mark(d[b], "belowBar", "#7eb8e8", "arrowUp", "2B"));
        n++;
      }
    }
    var end = d.length - 2, start = Math.max(wing, end - 28);
    var rh = d[start].high, rl = d[start].low, rv = 0, rc = 0;
    for (i = start; i <= end; i++) {
      if (d[i].high > rh) rh = d[i].high;
      if (d[i].low < rl) rl = d[i].low;
      if (vol(d[i])) { rv += vol(d[i]); rc++; }
    }
    var mid = (rh + rl) / 2, tight = mid > 0 && (rh - rl) / mid < 0.14;
    var prior = d[Math.max(0, start - 20)].close;
    var rose = prior > 0 && d[start].close > prior * 1.06;
    var fell = prior > 0 && d[start].close < prior * 0.94;
    var avg = rc ? rv / rc : 0;
    if (tight && fell && on.ac && !used[d[end].time]) {
      take(d[end], "AC");
      out.push(mark(d[end], "belowBar", "#3d9a78", "circle", "AC"));
    }
    if (tight && rose && on.dist && !used[d[end].time]) {
      take(d[end], "DIST");
      out.push(mark(d[end], "aboveBar", "#c9942e", "circle", "DIST"));
    }
    if (tight) {
      for (i = end; i > start; i--) {
        var bar = d[i], range = bar.high - bar.low;
        if (on.spr && fell && bar.low < rl && bar.close >= rl && !used[bar.time]) {
          take(bar, "SPR");
          out.push(mark(bar, "belowBar", "#8fd6c4", "arrowUp", "SPR"));
        }
        if (on.ut && rose && bar.high > rh && bar.close <= rh && !used[bar.time]) {
          take(bar, "UT");
          out.push(mark(bar, "aboveBar", "#e07a5f", "arrowDown", "UT"));
        }
        if (on.thr && avg && vol(bar) > avg * 1.15 && !used[bar.time]) {
          if (fell && bar.close > rh) { take(bar, "THR"); out.push(mark(bar, "aboveBar", "#f2cc8f", "circle", "THR")); }
          if (rose && bar.close < rl) { take(bar, "THR"); out.push(mark(bar, "belowBar", "#f2cc8f", "circle", "THR")); }
        }
      }
    }
    if (on.sc || on.bc) {
      var med = [], s;
      for (i = Math.max(1, d.length - 60); i < d.length - 1; i++) med.push(d[i].high - d[i].low);
      med.sort(function (x, y) { return x - y; });
      var width = med.length ? med[Math.floor(med.length / 2)] : 0;
      n = 0;
      for (i = d.length - 2; i > 20 && n < 3; i--) {
        var c = d[i], w = c.high - c.low, v20 = 0, c20 = 0;
        for (s = i - 20; s < i; s++) if (vol(d[s])) { v20 += vol(d[s]); c20++; }
        var base = c20 ? v20 / c20 : 0;
        if (!(w > width * 1.8 && base && vol(c) >= base * 2)) continue;
        var offLow = w > 0 && (c.close - c.low) / w > 0.45 && c.close < c.open && c.close < d[i - 12].close;
        var offHigh = w > 0 && (c.high - c.close) / w > 0.45 && c.close > c.open && c.close > d[i - 12].close;
        if (on.sc && offLow && !used[c.time]) { take(c, "SC"); out.push(mark(c, "belowBar", "#d64b4b", "arrowUp", "SC")); n++; }
        else if (on.bc && offHigh && !used[c.time]) { take(c, "BC"); out.push(mark(c, "aboveBar", "#2a9d8f", "arrowDown", "BC")); n++; }
      }
    }
    out.sort(function (x, y) { return x.time - y.time; });
    return out;
  }
  var prev = window.jhDistributionMarks;
  window.jhDistributionMarks = function (display, tf, kind) {
    var base = [];
    try { base = (prev && prev(display, tf, kind)) || []; } catch (e) { base = []; }
    if (!display || display.length < 40 || !daily(tf)) return base;
    var extra = [];
    try { extra = build(display); } catch (e) { extra = []; }
    var seen = {}, i, out = [];
    for (i = 0; i < base.length; i++) { seen[base[i].time] = 1; out.push(base[i]); }
    for (i = 0; i < extra.length; i++) if (!seen[extra[i].time]) out.push(extra[i]);
    out.sort(function (a, b) { return a.time - b.time; });
    return out;
  };
  function legend() {
    if (document.getElementById("jh-struct")) return;
    var host = document.getElementById("host");
    if (!host || !host.parentNode) return;
    var bar = document.createElement("div");
    bar.id = "jh-struct";
    bar.style.cssText = "display:flex;flex-wrap:wrap;gap:4px;padding:4px 8px;font:11px IBM Plex Mono,monospace;color:#c8cdd8";
    [["t2", "2T"], ["b2", "2B"], ["ac", "AC"], ["dist", "DIST"], ["spr", "SPR"], ["ut", "UT"], ["thr", "THR"], ["sc", "SC/BC"]].forEach(function (pair) {
      var b = document.createElement("button");
      b.type = "button";
      b.textContent = pair[1];
      b.style.cssText = "border:1px solid #3a4254;background:#1c2230;color:#d5d8e0;border-radius:3px;padding:2px 6px;cursor:pointer";
      function paint() { b.style.opacity = on[pair[0]] ? "1" : "0.4"; }
      paint();
      b.onclick = function () {
        on[pair[0]] = on[pair[0]] ? 0 : 1;
        save(); paint();
        if (window.paint) window.paint();
      };
      bar.appendChild(b);
    });
    host.parentNode.insertBefore(bar, host);
    window.jhStructure = { on: on };
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", legend);
  else legend();
})();
