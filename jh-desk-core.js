/* jh-desk-core.js — shared institutional desk primitives (quotes, OHLC, FRED, feeds). */
(function (w) {
  "use strict";
  if (w.JHDesk) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
  var LIVE = "https://justhodl.ai";
  var ohlcCache = {};
  var fredCache = {};

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return ({ "&": "&" + "amp;", "<": "&" + "lt;", ">": "&" + "gt;", '"': "&" + "quot;", "'": "&#39;" })[c];
    });
  }
  function num(v) { var n = +v; return isFinite(n) ? n : null; }
  function round(v, d) { return v == null ? null : Math.round(v * Math.pow(10, d)) / Math.pow(10, d); }
  function cls(v) { return v == null || !isFinite(v) || Math.abs(v) < 1e-9 ? "flat" : v > 0 ? "up" : "dn"; }
  function fmtPct(v, d) {
    if (v == null || !isFinite(v)) return "—";
    d = d == null ? 2 : d;
    return (v > 0 ? "+" : "") + v.toFixed(d) + "%";
  }
  function fmtPx(v, d) {
    if (v == null || !isFinite(v)) return "—";
    d = d == null ? (Math.abs(v) >= 100 ? 2 : Math.abs(v) >= 1 ? 2 : 4) : d;
    return v.toFixed(d);
  }
  function fmtBp(v) {
    if (v == null || !isFinite(v)) return "—";
    return (v > 0 ? "+" : "") + v.toFixed(1) + "bp";
  }
  function fmtUsd(v) {
    if (v == null || !isFinite(v)) return "—";
    var a = Math.abs(v);
    if (a >= 1e12) return (v / 1e12).toFixed(2) + "T";
    if (a >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return (v / 1e6).toFixed(1) + "M";
    if (a >= 1e3) return (v / 1e3).toFixed(1) + "k";
    return v.toFixed(0);
  }
  async function firstOk(urls) {
    for (var i = 0; i < urls.length; i++) {
      try {
        var r = await fetch(urls[i], { cache: "no-store" });
        if (r.ok) return await r.json();
      } catch (e) {}
    }
    return null;
  }
  function feed(key) {
    var k = key.replace(/^\//, "");
    return firstOk(["/" + k, LIVE + "/" + k, PROXY + "/" + k, S3 + "/" + k]);
  }
  async function quotes(tickers) {
    var out = {};
    var uniq = [];
    var seen = {};
    (tickers || []).forEach(function (t) {
      t = String(t || "").toUpperCase();
      if (!t || seen[t] || /USD$|-/.test(t)) return;
      seen[t] = 1;
      uniq.push(t);
    });
    for (var i = 0; i < uniq.length; i += 50) {
      var chunk = uniq.slice(i, i + 50);
      var d = await firstOk([
        PROXY + "/quotes?tickers=" + encodeURIComponent(chunk.join(",")),
        LIVE.replace("https://justhodl.ai", PROXY) + "/quotes?tickers=" + encodeURIComponent(chunk.join(","))
      ]);
      var map = (d && d.tickers) || d || {};
      chunk.forEach(function (t) {
        var q = map[t];
        if (q) out[t] = q;
      });
    }
    return out;
  }
  async function ohlc(symbol, range) {
    range = range || "6mo";
    var key = symbol + "|" + range;
    if (ohlcCache[key]) return ohlcCache[key];
    var d = await firstOk([
      PROXY + "/yf-ohlc?symbol=" + encodeURIComponent(symbol) + "&range=" + range + "&interval=1d",
      "/yf-ohlc?symbol=" + encodeURIComponent(symbol) + "&range=" + range + "&interval=1d"
    ]);
    var bars = (d && d.bars) || [];
    ohlcCache[key] = bars;
    return bars;
  }
  async function fred(series, obs) {
    obs = obs || 280;
    var key = series + "|" + obs;
    if (fredCache[key]) return fredCache[key];
    var d = await firstOk([
      PROXY + "/fred?series=" + encodeURIComponent(series) + "&obs=" + obs
    ]);
    var bars = (d && d.bars) || [];
    fredCache[key] = bars;
    return bars;
  }
  function pool(items, limit, fn) {
    var i = 0, out = new Array(items.length);
    function next() {
      if (i >= items.length) return Promise.resolve();
      var idx = i++;
      return Promise.resolve(fn(items[idx], idx)).then(function (v) { out[idx] = v; return next(); });
    }
    var workers = [];
    for (var k = 0; k < Math.min(limit, items.length); k++) workers.push(next());
    return Promise.all(workers).then(function () { return out; });
  }
  function retN(closes, n) {
    if (!closes || closes.length <= n) return null;
    var a = closes[closes.length - 1 - n], b = closes[closes.length - 1];
    if (!a || !b) return null;
    return round(((b - a) / a) * 100, 2);
  }
  function closesOf(bars) {
    return (bars || []).map(function (b) { return num(b.close != null ? b.close : b.c != null ? b.c : b.value); }).filter(function (v) { return v != null; });
  }
  function horizons(closes) {
    return { d: retN(closes, 1), w: retN(closes, 5), m: retN(closes, 21), q: retN(closes, 63) };
  }
  function vsSpy(h, spy) {
    if (!h || !spy) return { d: null, w: null, m: null, q: null };
    function d(a, b) { return a == null || b == null ? null : round(a - b, 2); }
    return { d: d(h.d, spy.d), w: d(h.w, spy.w), m: d(h.m, spy.m), q: d(h.q, spy.q) };
  }
  function cmf(bars, n) {
    n = n || 20;
    var sl = (bars || []).slice(-n);
    var nume = 0, den = 0;
    sl.forEach(function (b) {
      var h = num(b.high != null ? b.high : b.h), l = num(b.low != null ? b.low : b.l);
      var c = num(b.close != null ? b.close : b.c), v = num(b.value != null ? b.value : b.v) || 0;
      if (h == null || l == null || c == null || h === l) return;
      nume += (((c - l) - (h - c)) / (h - l)) * v;
      den += v;
    });
    return den ? round(nume / den, 4) : null;
  }
  function adPhase(bars, h) {
    var flow = cmf(bars, 20);
    var r = h && h.m;
    if (flow == null) return { phase: "—", cmf: null };
    var phase = "NEUTRAL";
    if (flow > 0.06 && (r == null || r > 0)) phase = "ACCUMULATION";
    else if (flow < -0.06 && (r == null || r < 0)) phase = "DISTRIBUTION";
    else if (flow > 0.06 && r != null && r < 0) phase = "ABSORPTION";
    else if (flow < -0.06 && r != null && r > 0) phase = "HIDDEN SELLING";
    return { phase: phase, cmf: flow };
  }
  function pattern(bars) {
    if (!bars || bars.length < 40) return "—";
    var c = closesOf(bars);
    if (c.length < 40) return "—";
    var last = c[c.length - 1];
    var hi20 = -Infinity, lo20 = Infinity, hi40 = -Infinity, lo40 = Infinity;
    var i, b, h, l;
    for (i = Math.max(0, bars.length - 40); i < bars.length; i++) {
      b = bars[i];
      h = num(b.high != null ? b.high : b.h); l = num(b.low != null ? b.low : b.l);
      if (h == null) h = num(b.close != null ? b.close : b.c);
      if (l == null) l = h;
      if (i >= bars.length - 20) { if (h > hi20) hi20 = h; if (l < lo20) lo20 = l; }
      if (h > hi40) hi40 = h; if (l < lo40) lo40 = l;
    }
    function ma(n) {
      var s = 0, k, start = c.length - n;
      for (k = start; k < c.length; k++) s += c[k];
      return s / n;
    }
    var ma10 = ma(10), ma40 = ma(40);
    var rng = (hi20 - lo20) / last;
    var lows = [];
    for (i = bars.length - 40; i < bars.length; i++) {
      b = bars[i]; l = num(b.low != null ? b.low : b.l);
      if (l != null) lows.push(l);
    }
    lows.sort(function (a, z) { return a - z; });
    var bounce = last > 0 && lows.length ? (last - lows[0]) / lows[0] : 0;
    var lastPx = last;
    if (rng < 0.035) return "COIL";
    if (ma10 > ma40 && c[c.length - 15] <= ma(40)) return "GOLDEN CROSS";
    if (lastPx >= hi20 * 0.999 && lastPx >= c[c.length - 6]) return "BREAKOUT";
    if (lastPx <= lo20 * 1.001 && lastPx <= c[c.length - 6]) return "BREAKDOWN";
    if (bounce >= 0.04 && lows.length >= 2 && (lows[1] - lows[0]) / last < 0.015) return "DOUBLE BOTTOM";
    if (ma10 > ma40) return "UPTREND";
    if (ma10 < ma40) return "DOWNTREND";
    return "RANGE";
  }
  function spark(vals, w, h) {
    vals = (vals || []).filter(function (v) { return v != null && isFinite(v); });
    if (vals.length < 2) return "";
    w = w || 72; h = h || 22;
    var mn = Math.min.apply(null, vals), mx = Math.max.apply(null, vals), span = Math.max(mx - mn, 1e-9);
    var last = vals[vals.length - 1], first = vals[0];
    var col = last >= first ? "#26ffaf" : "#ff5577";
    var d = vals.map(function (v, i) {
      var x = 1 + i * (w - 2) / (vals.length - 1);
      var y = h - 2 - ((v - mn) / span) * (h - 4);
      return (i ? "L" : "M") + x.toFixed(1) + "," + y.toFixed(1);
    }).join(" ");
    return '<svg class="jh-spark" viewBox="0 0 ' + w + ' ' + h + '" preserveAspectRatio="none"><path d="' + d + '" fill="none" stroke="' + col + '" stroke-width="1.4"/></svg>';
  }
  function sortRows(rows, key, dir) {
    dir = dir || -1;
    return rows.slice().sort(function (a, b) {
      var av = a[key], bv = b[key];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "string") return dir * (av < bv ? 1 : av > bv ? -1 : 0);
      return dir * ((bv - av) || 0);
    });
  }

  w.JHDesk = {
    PROXY: PROXY, S3: S3, LIVE: LIVE,
    esc: esc, num: num, round: round, cls: cls,
    fmtPct: fmtPct, fmtPx: fmtPx, fmtBp: fmtBp, fmtUsd: fmtUsd,
    feed: feed, quotes: quotes, ohlc: ohlc, fred: fred, pool: pool,
    retN: retN, closesOf: closesOf, horizons: horizons, vsSpy: vsSpy,
    cmf: cmf, adPhase: adPhase, pattern: pattern, spark: spark, sortRows: sortRows
  };
})(window);
