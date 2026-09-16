/* jh-etf-fuse.js — single client for Massive ETF Global (flows + profiles + constituents).
   Warehouse first (data/etf-desk.json), live /poly/etf as fill. Fail-soft. Never fabricate. */
(function (w) {
  "use strict";
  if (w.JHEtfFuse) return;
  var LIVE = "https://justhodl.ai";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
  var deskCache = null, deskP = null, idxCache = null, liveCache = {}, derCache = null, derP = null;
  var invCache = null, cenCache = null, cenP = null, histCache = {};

  function firstOk(urls) {
    var i = 0;
    function next() {
      if (i >= urls.length) return Promise.resolve(null);
      var u = urls[i++];
      return fetch(u, { cache: "no-store" }).then(function (r) {
        if (r.ok) return r.json();
        return next();
      }).catch(next);
    }
    return next();
  }
  function num(v) {
    if (v == null || v === "") return null;
    if (typeof v === "object" && v.raw != null) v = v.raw;
    var n = Number(v);
    return isFinite(n) ? n : null;
  }
  function bare(s) {
    s = String(s || "").trim().toUpperCase();
    if (s.indexOf(":") >= 0) s = s.split(":").pop();
    return s.replace(/[^A-Z0-9.\-]/g, "");
  }
  function fmtUsd(v) {
    v = num(v);
    if (v == null) return "—";
    var a = Math.abs(v), s = v < 0 ? "−" : "";
    if (a >= 1e12) return s + (a / 1e12).toFixed(2) + "T";
    if (a >= 1e9) return s + (a / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return s + (a / 1e6).toFixed(1) + "M";
    if (a >= 1e3) return s + (a / 1e3).toFixed(0) + "k";
    return s + a.toFixed(0);
  }
  function fmtEr(v) {
    v = num(v);
    if (v == null) return "—";
    if (v >= 1.5) v = v / 100;
    else if (v <= 0.005) v = v * 100;
    return v.toFixed(3) + "%";
  }
  function wgt(v) {
    v = num(v);
    if (v == null) return "—";
    return (v < 1 && v > -1 ? v * 100 : v).toFixed(2) + "%";
  }
  function polyRows(j) {
    if (!j) return [];
    if (Array.isArray(j.results)) return j.results;
    if (j.results && Array.isArray(j.results.values)) return j.results.values;
    if (Array.isArray(j)) return j;
    return [];
  }

  function desk() {
    if (deskCache) return Promise.resolve(deskCache);
    if (deskP) return deskP;
    deskP = firstOk([
      "/data/etf-desk.json?t=" + Date.now(),
      LIVE + "/data/etf-desk.json?t=" + Date.now(),
      PROXY + "/data/etf-desk.json?t=" + Date.now(),
      S3 + "/data/etf-desk.json"
    ]).then(function (j) {
      deskCache = j && typeof j === "object" ? j : { by_etf: {}, status: "EMPTY" };
      return deskCache;
    });
    return deskP;
  }
  function holdingsIndex() {
    if (idxCache) return Promise.resolve(idxCache);
    return firstOk([
      "/data/etf-holdings-index.json?t=" + Date.now(),
      LIVE + "/data/etf-holdings-index.json?t=" + Date.now(),
      PROXY + "/data/etf-holdings-index.json?t=" + Date.now(),
      S3 + "/data/etf-holdings-index.json"
    ]).then(function (j) {
      idxCache = j && typeof j === "object" ? j : { by_stock: {} };
      return idxCache;
    }).catch(function () {
      idxCache = { by_stock: {} };
      return idxCache;
    });
  }
  function derived() {
    if (derCache) return Promise.resolve(derCache);
    if (derP) return derP;
    derP = firstOk([
      "/data/etf-derived.json?t=" + Date.now(),
      LIVE + "/data/etf-derived.json?t=" + Date.now(),
      PROXY + "/data/etf-derived.json?t=" + Date.now(),
      S3 + "/data/etf-derived.json"
    ]).then(function (j) {
      derCache = j && typeof j === "object" ? j : { by_ticker: {}, status: "EMPTY" };
      return derCache;
    }).catch(function () {
      derCache = { by_ticker: {}, status: "EMPTY" };
      return derCache;
    });
    return derP;
  }
  function ofDerived(ticker) {
    var t = bare(ticker);
    return derived().then(function (d) { return ((d && d.by_ticker) || {})[t] || null; });
  }
  function live(ticker) {
    var t = bare(ticker);
    if (!t) return Promise.resolve(null);
    if (liveCache[t] && (Date.now() - liveCache[t].at) < 120000) return Promise.resolve(liveCache[t].j);
    return fetch(PROXY + "/poly/etf?ticker=" + encodeURIComponent(t), { cache: "default" })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        liveCache[t] = { at: Date.now(), j: j };
        return j;
      })
      .catch(function () { return null; });
  }
  function of(ticker) {
    var t = bare(ticker);
    return desk().then(function (d) { return (d.by_etf || {})[t] || null; });
  }
  function reverseFromDesk(d, ticker) {
    var t = bare(ticker), out = [];
    var by = (d && d.by_etf) || {};
    Object.keys(by).forEach(function (etf) {
      (by[etf].top || []).forEach(function (h) {
        if (bare(h.t) === t) {
          out.push({
            etf: etf, w: num(h.w), n: h.n,
            flow_1d: by[etf].flow_1d, flow_5d: by[etf].flow_5d,
            flow_label: by[etf].flow_label, aum: by[etf].aum, name: by[etf].name
          });
        }
      });
    });
    out.sort(function (a, b) { return (b.w || 0) - (a.w || 0); });
    return out;
  }
  function reverse(ticker) {
    var t = bare(ticker);
    return Promise.all([desk(), holdingsIndex()]).then(function (pack) {
      var idx = ((pack[1] || {}).by_stock || {})[t];
      if (idx && idx.length) return idx.slice().sort(function (a, b) { return (b.w || 0) - (a.w || 0); });
      return reverseFromDesk(pack[0], t);
    });
  }
  function impliedDemand(holders) {
    if (!holders || !holders.length) return null;
    var usd = 0, n = 0;
    holders.forEach(function (h) {
      var f = num(h.flow_1d), ww = num(h.w);
      if (f == null || ww == null) return;
      usd += f * (ww < 1 && ww > -1 ? ww : ww / 100);
      n++;
    });
    return n ? usd : null;
  }
  function isFund(row, liveJ) {
    if (row && (row.ok && (row.ok.flows || row.ok.profiles))) return true;
    if (row && (row.aum || row.flow_1d != null || (row.top && row.top.length))) return true;
    var prof = polyRows(liveJ && liveJ.profile)[0];
    var flows = polyRows(liveJ && liveJ.flows);
    return !!(prof && (prof.aum || prof.issuer || prof.asset_class)) || flows.length > 0;
  }
  function ymd(t) {
    var d = new Date((Number(t) || 0) * 1000);
    if (!isFinite(d.getTime())) return "";
    return d.toISOString().slice(0, 10);
  }
  function barStep(bars) {
    if (!bars || bars.length < 2) return 86400;
    var gaps = [], i, n = Math.min(bars.length - 1, 80);
    for (i = bars.length - n; i < bars.length; i++) if (i > 0) gaps.push(bars[i].time - bars[i - 1].time);
    if (!gaps.length) return 86400;
    gaps.sort(function (a, b) { return a - b; });
    return gaps[Math.floor(gaps.length / 2)] || 86400;
  }
  function isIntraBars(bars) {
    var step = barStep(bars);
    if (step < 20 * 3600) return true;
    var n = Math.min(bars.length - 1, 40), small = 0, i;
    for (i = bars.length - n; i < bars.length; i++) {
      if (i > 0 && bars[i].time - bars[i - 1].time < 20 * 3600) small++;
    }
    return n > 0 && small >= n * 0.35;
  }
  function compactToRows(j) {
    if (!j) return [];
    if (Array.isArray(j.d) && Array.isArray(j.f)) {
      var out = [], i;
      for (i = 0; i < j.d.length; i++) out.push({ d: j.d[i], f: num(j.f[i]), n: Array.isArray(j.n) ? num(j.n[i]) : null });
      return out;
    }
    if (Array.isArray(j.rows)) return j.rows.map(function (r) {
      return { d: r.d || r.processed_date || r.effective_date, f: num(r.f != null ? r.f : r.fund_flow), n: num(r.n != null ? r.n : r.nav) };
    });
    var flows = polyRows(j.flows || j);
    return flows.map(function (r) {
      return { d: r.processed_date || r.effective_date || r.d, f: num(r.fund_flow != null ? r.fund_flow : r.f), n: num(r.nav != null ? r.nav : r.n), s: num(r.shares_outstanding) };
    });
  }
  function mergeHist(a, b) {
    var map = {}, order = [];
    function add(h) {
      if (!h || !h.d) return;
      var d = String(h.d).slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return;
      var f = num(h.f);
      var cur = map[d];
      if (!cur) { map[d] = { d: d, f: f, n: num(h.n), s: num(h.s) }; order.push(d); }
      else {
        if (f != null) cur.f = f;
        if (h.n != null && cur.n == null) cur.n = num(h.n);
        if (h.s != null && cur.s == null) cur.s = num(h.s);
      }
    }
    (a || []).forEach(add);
    (b || []).forEach(add);
    order.sort();
    return order.map(function (d) { return map[d]; });
  }
  function histFromLive(liveJ) {
    return compactToRows(liveJ && liveJ.flows ? { flows: liveJ.flows } : liveJ);
  }
  function histFrom(row, liveJ) {
    return mergeHist(row && row.flow_hist, histFromLive(liveJ));
  }
  function alignHist(hist, bars) {
    /* Map official daily fund_flow onto whatever tick the chart pulled.
       Daily: 1:1 on UTC session date. Weekly/monthly/multi-day: SUM the
       prints whose dates fall in [bar.time, nextBar.time). Intraday: the
       day's total sits on the last bar of that session so the histogram
       is one accurate print per day, not a fake per-minute flow. */
    if (!hist || !hist.length || !bars || !bars.length) return [];
    var byDay = {}, i;
    hist.forEach(function (h) {
      if (!h || !h.d) return;
      var d = String(h.d).slice(0, 10);
      var f = num(h.f);
      if (!d || f == null) return;
      byDay[d] = (byDay[d] == null ? 0 : byDay[d]) + f;
    });
    var step = barStep(bars);
    var intra = isIntraBars(bars);
    var out = [];
    if (intra) {
      var lastOf = {};
      for (i = 0; i < bars.length; i++) lastOf[ymd(bars[i].time)] = i;
      Object.keys(lastOf).forEach(function (d) {
        if (byDay[d] == null) return;
        var b = bars[lastOf[d]];
        out.push({ time: b.time, value: byDay[d] / 1e9, raw: byDay[d], d: d, n: 1 });
      });
      out.sort(function (a, b) { return a.time - b.time; });
      return out;
    }
    for (i = 0; i < bars.length; i++) {
      var t0 = bars[i].time;
      var t1 = i + 1 < bars.length ? bars[i + 1].time : t0 + Math.max(step, 86400);
      var d0 = ymd(t0), d1 = ymd(t1);
      if (!d0) continue;
      if (!d1 || d1 <= d0) d1 = ymd(t0 + Math.max(step, 86400));
      var sum = 0, n = 0, d;
      for (d in byDay) {
        if (d >= d0 && d < d1) { sum += byDay[d]; n++; }
      }
      if (!n) continue;
      out.push({ time: t0, value: sum / 1e9, raw: sum, d: d0, n: n });
    }
    return out;
  }
  function fullHist(ticker) {
    var t = bare(ticker);
    if (!t) return Promise.resolve([]);
    if (histCache[t] && histCache[t].rows && histCache[t].rows.length) return Promise.resolve(histCache[t].rows);
    if (histCache[t] && histCache[t].p) return histCache[t].p;
    histCache[t] = histCache[t] || {};
    histCache[t].p = firstOk([
      PROXY + "/poly/etf-flow-hist?ticker=" + encodeURIComponent(t),
      "/data/etf-flow-hist/" + t + ".json?t=" + Date.now(),
      LIVE + "/data/etf-flow-hist/" + t + ".json?t=" + Date.now(),
      PROXY + "/data/etf-flow-hist/" + t + ".json?t=" + Date.now(),
      S3 + "/data/etf-flow-hist/" + t + ".json"
    ]).then(function (j) {
      if (!j) { delete histCache[t].p; return []; }
      var rows = compactToRows(j);
      histCache[t].rows = rows;
      histCache[t].meta = { n: rows.length, from: rows[0] && rows[0].d, to: rows.length ? rows[rows.length - 1].d : null, source: (j && (j.source || j.engine)) || "etf-global" };
      return rows;
    }).catch(function () {
      delete histCache[t].p;
      return [];
    });
    return histCache[t].p;
  }
  function markers(hist, bars) {
    var pts = alignHist(hist, bars);
    var out = [];
    pts.forEach(function (p) {
      if (p.raw == null) return;
      var a = Math.abs(p.raw);
      if (a < 1.5e8) return;
      var bn = p.raw / 1e9;
      var heavy = a >= 1e9;
      out.push({
        time: p.time,
        position: p.raw > 0 ? "belowBar" : "aboveBar",
        color: p.raw > 0 ? "#089981" : "#f23645",
        shape: p.raw > 0 ? "arrowUp" : "arrowDown",
        text: (p.raw > 0 ? "IN " : "OUT ") + (heavy ? Math.abs(bn).toFixed(1) + "B" : (a / 1e6).toFixed(0) + "M")
      });
    });
    return out.slice(-18);
  }
  function mergeLive(row, liveJ) {
    var flows = polyRows(liveJ && liveJ.flows);
    var prof = polyRows(liveJ && liveJ.profile)[0] || {};
    var holds = polyRows(liveJ && liveJ.holdings);
    var latest = flows[0] || {};
    var out = row ? Object.assign({}, row) : {};
    if (!out.name) out.name = prof.description || prof.fund_name || out.name;
    if (!out.issuer) out.issuer = prof.issuer || prof.advisor;
    if (out.aum == null) out.aum = num(prof.aum);
    if (out.er == null) out.er = num(prof.net_expense_ratio) || num(prof.expense_ratio);
    if (out.nav == null) out.nav = num(latest.nav);
    if (out.shares == null) out.shares = num(latest.shares_outstanding);
    if (out.asset_class == null) out.asset_class = prof.asset_class;
    if (out.benchmark == null) out.benchmark = prof.primary_benchmark;
    if (out.flow_1d == null) out.flow_1d = num(latest.fund_flow);
    if ((!out.top || !out.top.length) && holds.length) {
      out.top = holds.slice(0, 12).map(function (h) {
        return { t: h.constituent_ticker, n: h.constituent_name, w: num(h.weight), mv: num(h.market_value) };
      });
    }
    out.flow_hist = mergeHist(out.flow_hist, histFromLive(liveJ));
    if (out.flow_hist && out.flow_hist.length) {
      out.flow_hist_n = out.flow_hist.length;
      out.flow_hist_from = out.flow_hist[0] && out.flow_hist[0].d;
      out.flow_hist_to = out.flow_hist[out.flow_hist.length - 1] && out.flow_hist[out.flow_hist.length - 1].d;
    }
    return out;
  }

  function invertHoldings(idx) {
    if (invCache) return invCache;
    var by = {};
    var src = (idx && (idx.by_stock || idx.tickers)) || {};
    Object.keys(src).forEach(function (stock) {
      var arr = src[stock];
      if (!Array.isArray(arr)) return;
      var tk = bare(stock);
      arr.forEach(function (h) {
        var etf = bare(h.etf);
        if (!etf) return;
        if (!by[etf]) by[etf] = [];
        by[etf].push({ t: tk, n: h.n || h.name || "", w: num(h.w), mv: num(h.mv), sh: num(h.sh) });
      });
    });
    Object.keys(by).forEach(function (etf) {
      by[etf].sort(function (a, b) { return (b.w || 0) - (a.w || 0); });
    });
    invCache = by;
    return by;
  }

  function constituents(ticker) {
    var t = bare(ticker);
    if (!t) return Promise.resolve({ ticker: t, n: 0, rows: [], holdings_n: null, complete: null });
    return Promise.all([desk(), holdingsIndex(), live(t)]).then(function (pack) {
      var row = ((pack[0] && pack[0].by_etf) || {})[t] || {};
      var byEtf = invertHoldings(pack[1] || {});
      var liveJ = pack[2];
      var map = {};
      function add(h) {
        var tk = bare(h.t || h.constituent_ticker || h.ticker);
        if (!tk) return;
        var w = num(h.w != null ? h.w : h.weight);
        var mv = num(h.mv != null ? h.mv : h.market_value);
        var sh = num(h.sh != null ? h.sh : h.shares);
        var n = h.n || h.constituent_name || h.name || "";
        var rk = h.rank || h.constituent_rank;
        if (!map[tk]) map[tk] = { t: tk, n: n, w: w, mv: mv, sh: sh, rank: rk };
        else {
          if (!map[tk].n && n) map[tk].n = n;
          if (map[tk].w == null && w != null) map[tk].w = w;
          if (map[tk].mv == null && mv != null) map[tk].mv = mv;
          if (map[tk].sh == null && sh != null) map[tk].sh = sh;
          if (map[tk].rank == null && rk != null) map[tk].rank = rk;
        }
      }
      (byEtf[t] || []).forEach(add);
      (row.top || []).forEach(add);
      polyRows(liveJ && liveJ.holdings).forEach(add);
      var out = Object.keys(map).map(function (k) { return map[k]; });
      out.sort(function (a, b) { return (b.w || 0) - (a.w || 0); });
      out.forEach(function (h, i) { if (h.rank == null) h.rank = i + 1; });
      return {
        ticker: t,
        n: out.length,
        holdings_n: row.holdings_n != null ? row.holdings_n : out.length,
        complete: row.holdings_complete,
        rows: out,
        source: out.length ? "ETF Global constituents · holdings-index + desk top + live fill" : "empty"
      };
    });
  }

  function census() {
    if (cenCache) return Promise.resolve(cenCache);
    if (cenP) return cenP;
    cenP = firstOk([
      "/data/etf-census-matrix.json?t=" + Date.now(),
      LIVE + "/data/etf-census-matrix.json?t=" + Date.now(),
      PROXY + "/data/etf-census-matrix.json?t=" + Date.now()
    ]).then(function (j) {
      cenCache = j && typeof j === "object" ? j : { tickers: [], cols: {}, n: 0 };
      return cenCache;
    }).catch(function () {
      cenCache = { tickers: [], cols: {}, n: 0 };
      return cenCache;
    });
    return cenP;
  }

  function rankVs(ticker, mx) {
    mx = mx || {};
    var t = bare(ticker);
    var tickers = mx.tickers || [];
    var cols = mx.cols || {};
    function colAt(name, idx) {
      var arr = cols[name] || [];
      if (idx < 0 || idx >= arr.length) return null;
      var v = arr[idx];
      if (v == null || v === "") return null;
      v = Number(v);
      return isFinite(v) ? v : null;
    }
    function rankOf(name) {
      var arr = cols[name] || [];
      var scored = [];
      for (var j = 0; j < tickers.length; j++) {
        var v = colAt(name, j);
        if (v == null) continue;
        scored.push({ t: tickers[j], v: v });
      }
      scored.sort(function (a, b) { return b.v - a.v; });
      for (var k = 0; k < scored.length; k++) {
        if (scored[k].t === t) return { rank: k + 1, n: scored.length, v: scored[k].v };
      }
      return { rank: null, n: scored.length, v: null };
    }
    var spyI = tickers.indexOf("SPY");
    var meI = tickers.indexOf(t);
    var windows = [
      { k: "d", label: "1D", col: "f_return_1d_pct", rel: false },
      { k: "w", label: "1W", col: "f_return_5d_pct", rel: false },
      { k: "m", label: "1M", col: "f_return_20d_pct", rel: false },
      { k: "q", label: "3M", col: "rs_13w_pct", rel: true }
    ];
    var out = { ticker: t, n_universe: tickers.length, as_of: mx.generated_at || mx.as_of, version: mx.version, in_universe: meI >= 0, windows: {} };
    windows.forEach(function (w) {
      var rk = rankOf(w.col);
      var spyV = colAt(w.col, spyI);
      var vs = null;
      if (rk.v != null) vs = w.rel ? rk.v : (spyV != null ? rk.v - spyV : null);
      out.windows[w.k] = {
        label: w.label,
        etf: rk.v,
        spy: w.rel ? 0 : spyV,
        vs: vs,
        rank: rk.rank,
        n: rk.n,
        already_vs_spy: w.rel
      };
    });
    out.beta = colAt("beta_spy", meI);
    out.corr = colAt("corr_spy_52w", meI);
    out.rs13 = colAt("rs_13w_pct", meI);
    return out;
  }

  w.JHEtfFuse = {
    desk: desk, live: live, of: of, reverse: reverse, reverseFromDesk: reverseFromDesk,
    impliedDemand: impliedDemand, isFund: isFund, histFrom: histFrom, alignHist: alignHist,
    fullHist: fullHist, mergeHist: mergeHist, compactToRows: compactToRows,
    markers: markers, mergeLive: mergeLive, holdingsIndex: holdingsIndex,
    derived: derived, ofDerived: ofDerived,
    constituents: constituents, census: census, rankVs: rankVs, invertHoldings: invertHoldings,
    fmtUsd: fmtUsd, fmtEr: fmtEr, wgt: wgt, num: num, bare: bare, polyRows: polyRows, PROXY: PROXY
  };
})(window);
