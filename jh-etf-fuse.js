/* jh-etf-fuse.js — single client for Massive ETF Global (flows + profiles + constituents).
   Warehouse first (data/etf-desk.json), live /poly/etf as fill. Fail-soft. Never fabricate. */
(function (w) {
  "use strict";
  if (w.JHEtfFuse) return;
  var LIVE = "https://justhodl.ai";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
  var deskCache = null, deskP = null, idxCache = null, liveCache = {}, derCache = null, derP = null;

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
      PROXY + "/data/etf-holdings-index.json?t=" + Date.now()
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
  function histFrom(row, liveJ) {
    if (row && row.flow_hist && row.flow_hist.length) return row.flow_hist;
    var flows = polyRows(liveJ && liveJ.flows);
    return flows.map(function (r) {
      return { d: r.processed_date || r.effective_date, f: num(r.fund_flow), n: num(r.nav), s: num(r.shares_outstanding) };
    });
  }
  function alignHist(hist, bars) {
    if (!hist || !hist.length || !bars || !bars.length) return [];
    var map = {};
    hist.forEach(function (h) {
      if (!h || !h.d) return;
      map[String(h.d).slice(0, 10)] = num(h.f);
    });
    return bars.map(function (b) {
      var dt = new Date((b.time || 0) * 1000).toISOString().slice(0, 10);
      var v = map[dt];
      return { time: b.time, value: v == null ? 0 : v / 1e9, raw: v };
    }).filter(function (p, i, a) {
      if (p.raw != null) return true;
      for (var j = 0; j < a.length; j++) if (a[j].raw != null) return true;
      return false;
    });
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
    if (!out.flow_hist || !out.flow_hist.length) out.flow_hist = histFrom(out, liveJ);
    return out;
  }

  w.JHEtfFuse = {
    desk: desk, live: live, of: of, reverse: reverse, reverseFromDesk: reverseFromDesk,
    impliedDemand: impliedDemand, isFund: isFund, histFrom: histFrom, alignHist: alignHist,
    markers: markers, mergeLive: mergeLive, holdingsIndex: holdingsIndex,
    derived: derived, ofDerived: ofDerived,
    fmtUsd: fmtUsd, fmtEr: fmtEr, wgt: wgt, num: num, bare: bare, polyRows: polyRows, PROXY: PROXY
  };
})(window);
