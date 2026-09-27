/* jh-growth-engines.js — derived growth desks from existing JustHodl feeds.
   Billings + coverage years from backlog.json + forward-orders.json.
   Does not invent RPO timing bands when the filing does not tag them. */
(function (root) {
  if (root.__jhGrowthEnginesV1) return;
  root.__jhGrowthEnginesV1 = true;

  function up(t) { return String(t == null ? "" : t).toUpperCase().trim(); }
  function num(v) {
    if (v == null || v === "") return null;
    var n = Number(v);
    return isFinite(n) ? n : null;
  }

  function latestRev(hist) {
    if (!Array.isArray(hist) || !hist.length) return null;
    var copy = hist.slice().sort(function (a, b) {
      return String(b.end || "").localeCompare(String(a.end || ""));
    });
    return num(copy[0] && copy[0].value);
  }

  function priorDeferred(deferred, yoy) {
    var d = num(deferred), y = num(yoy);
    if (d == null || y == null || y <= -100) return null;
    return d / (1 + y / 100);
  }

  function impliedBillings(revTtm, deferred, deferredYoy) {
    var rev = num(revTtm), d = num(deferred), y = num(deferredYoy);
    if (rev == null || d == null || y == null || y <= -100) return null;
    return rev + (d - priorDeferred(d, y));
  }

  function coverageYears(rpo, revTtm) {
    var b = num(rpo), r = num(revTtm);
    if (b == null || r == null || r <= 0) return null;
    return b / r;
  }

  function foList(doc) {
    if (!doc) return [];
    if (Array.isArray(doc.all_results)) return doc.all_results;
    if (Array.isArray(doc.results)) return doc.results;
    return [];
  }

  function buildBillingsRows(backlog, fo) {
    var radar = (backlog && backlog.by_ticker) || {};
    var byFo = {};
    foList(fo).forEach(function (row) {
      var t = up(row.ticker);
      if (t) byFo[t] = row;
    });
    var tickers = {};
    Object.keys(radar).forEach(function (k) { tickers[up(k)] = true; });
    Object.keys(byFo).forEach(function (k) { tickers[k] = true; });
    var rows = [];
    Object.keys(tickers).forEach(function (t) {
      var r = radar[t] || {};
      var f = byFo[t] || {};
      var d = f.data || {};
      var rev = latestRev(d.revenue_history);
      var rpo = num(d.rpo_latest_usd != null ? d.rpo_latest_usd : r.rpo);
      var def = num(r.deferred_rev);
      var defY = num(r.deferred_yoy);
      var bill = impliedBillings(rev, def, defY);
      var cov = coverageYears(rpo, rev);
      var btb = num(d.book_to_bill_spread_pct);
      if (btb == null && r.rpo_minus_rev_growth != null) btb = num(r.rpo_minus_rev_growth);
      rows.push({
        ticker: t,
        name: f.name || t,
        sector: f.sector || r.sector || r.group || "",
        rpo: rpo,
        rev_ttm: rev,
        deferred: def,
        deferred_yoy: defY,
        billings: bill,
        billings_vs_rev: (bill != null && rev) ? (bill / rev - 1) * 100 : null,
        coverage_years: cov,
        book_to_bill: btb,
        rpo_yoy: num(d.rpo_growth_yoy_pct != null ? d.rpo_growth_yoy_pct : r.rpo_yoy),
        rev_yoy: num(r.rev_yoy),
        accelerating: !!(r.demand_accelerating || r.deferred_accelerating),
        src: rpo != null ? (d.rpo_tag || r.rpo_tag || "rpo") : (def != null ? "deferred" : "")
      });
    });
    return rows;
  }

  var api = {
    num: num,
    latestRev: latestRev,
    priorDeferred: priorDeferred,
    impliedBillings: impliedBillings,
    coverageYears: coverageYears,
    buildBillingsRows: buildBillingsRows
  };
  root.jhGrowthEngines = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
