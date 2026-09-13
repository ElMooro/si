/* Beta + relative strength vs SPY. Warehouse bars. */
(function () {
  var spyCache = null;
  function align(a, b) {
    var map = {}, i, out = [];
    for (i = 0; i < b.length; i++) map[b[i].time] = b[i].close;
    for (i = 1; i < a.length; i++) {
      var t = a[i].time, t0 = a[i - 1].time;
      if (map[t] == null || map[t0] == null || !a[i - 1].close) continue;
      var rs = a[i].close / map[t];
      var ra = a[i].close / a[i - 1].close - 1;
      var rb = map[t] / map[t0] - 1;
      out.push({ time: t, rs: rs, ra: ra, rb: rb, close: a[i].close });
    }
    return out;
  }
  function betaParts(rows) {
    var upA = 0, upB = 0, dnA = 0, dnB = 0, i;
    for (i = 0; i < rows.length; i++) {
      if (rows[i].rb > 0) { upA += rows[i].ra; upB += rows[i].rb; }
      else if (rows[i].rb < 0) { dnA += rows[i].ra; dnB += rows[i].rb; }
    }
    return {
      up: upB ? upA / upB : null,
      down: dnB ? dnA / dnB : null,
      n: rows.length
    };
  }
  function extrema(rows) {
    if (!rows.length) return {};
    var lo = rows[0], hi = rows[0], i;
    for (i = 1; i < rows.length; i++) {
      if (rows[i].rs < lo.rs) lo = rows[i];
      if (rows[i].rs > hi.rs) hi = rows[i];
    }
    var now = rows[rows.length - 1];
    var pctFromLo = lo.rs ? (now.rs / lo.rs - 1) * 100 : 0;
    var pctFromHi = hi.rs ? (now.rs / hi.rs - 1) * 100 : 0;
    return { lo: lo, hi: hi, now: now, pctFromLo: pctFromLo, pctFromHi: pctFromHi };
  }
  function paintPanel(txt) {
    var q = document.getElementById("quote");
    if (!q) return;
    var id = "jh-rs-line";
    var n = document.getElementById(id);
    if (!n) {
      n = document.createElement("div");
      n.id = id;
      n.style.cssText = "flex-basis:100%;font-size:10px;color:var(--mut)";
      q.appendChild(n);
    }
    n.textContent = txt;
  }
  window.jhRsMarkers = function (d, spy) {
    if (!d || !spy || d.length < 30) return [];
    var rows = align(d, spy);
    if (rows.length < 20) return [];
    var win = rows.slice(-252);
    var b = betaParts(win);
    var x = extrema(win);
    var mk = [];
    if (x.lo) mk.push({ time: x.lo.time, position: "belowBar", color: "#089981", shape: "circle", text: "RS LO" });
    if (x.hi) mk.push({ time: x.hi.time, position: "aboveBar", color: "#f23645", shape: "circle", text: "RS HI" });
    var up = b.up == null ? "—" : b.up.toFixed(2);
    var dn = b.down == null ? "—" : b.down.toFixed(2);
    var nowRs = x.now ? ((x.now.rs / (x.lo && x.lo.rs ? x.lo.rs : x.now.rs) - 1) * 100).toFixed(1) : "—";
    paintPanel(
      "vs SPY 1y  βup " + up + "  βdn " + dn +
      "  | RS now vs 1y low " + (x.pctFromLo != null ? x.pctFromLo.toFixed(1) + "%" : "—") +
      "  vs 1y high " + (x.pctFromHi != null ? x.pctFromHi.toFixed(1) + "%" : "—")
    );
    window.jhRsLast = { beta: b, ext: x };
    return mk;
  };
  async function loadSpy() {
    if (spyCache && Date.now() - spyCache.at < 300000) return spyCache.d;
    try {
      var r = await fetch("https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=SPY", { cache: "no-store" });
      var j = await r.json();
      var bars = (j.bars || []).map(function (b) {
        var t = b.time; if (t > 1e12) t = Math.floor(t / 1000);
        return { time: t, close: +b.close, open: +b.open, high: +b.high, low: +b.low, volume: +(b.volume || b.value || 0) };
      });
      spyCache = { d: bars, at: Date.now() };
      return bars;
    } catch (e) { return []; }
  }
  window.jhRsReady = function (d) {
    return loadSpy().then(function (spy) { return window.jhRsMarkers(d, spy); });
  };
})();
