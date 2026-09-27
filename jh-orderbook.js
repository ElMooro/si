/* jh-orderbook.js — company order-book desk (SEC RPO / backlog / deferred + price).
   Book QoQ/YoY come from the same series as the chosen book level.
   Book MoM is only printed when two observations are 20–40 calendar days apart.
   Quarterly 10-Q prints are not interpolated into a monthly %.
   Price MoM/QoQ/YoY are ret_1m/ret_3m/ret_12m from momentum-scanner when present. */
(function (root) {
  if (root.__jhOrderbookV2) return;
  root.__jhOrderbookV2 = true;

  var MAX_AGE_DAYS = 550;
  var MOM_MIN = 20;
  var MOM_MAX = 40;
  var QOQ_MIN = 70;
  var QOQ_MAX = 120;
  var YOY_MIN = 330;
  var YOY_MAX = 400;
  var IMPLAUSIBLE_PCT = 150;

  function finite(n) { return typeof n === "number" && isFinite(n); }
  function parseDay(v) {
    if (!v) return null;
    var s = String(v).slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
    var t = Date.parse(s + "T00:00:00Z");
    return isFinite(t) ? t : null;
  }
  function ageDays(asof, now) {
    var t = parseDay(asof);
    if (t == null) return null;
    var n = now == null ? Date.now() : (now instanceof Date ? now.getTime() : Number(now));
    if (!isFinite(n)) n = Date.now();
    return (n - t) / 86400000;
  }
  function pctChange(newer, older) {
    if (!finite(newer) || !finite(older) || older === 0) return null;
    return (newer - older) / Math.abs(older) * 100;
  }
  function round1(n) { return finite(n) ? Math.round(n * 10) / 10 : null; }

  function historyPoints(hist) {
    var out = [], i, p, t, v;
    if (!Array.isArray(hist)) return out;
    for (i = 0; i < hist.length; i++) {
      p = hist[i] || {};
      t = parseDay(p.end || p.asof || p.date || p.as_of);
      v = p.value != null ? p.value : (p.backlog_usd != null ? p.backlog_usd : p.rpo);
      if (t != null && finite(v) && v > 0) {
        out.push({
          t: t,
          asof: String(p.end || p.asof || p.date || p.as_of).slice(0, 10),
          value: v,
          form: p.form || p.src || "",
          fp: p.fp || p.period || ""
        });
      }
    }
    out.sort(function (a, b) { return b.t - a.t; });
    var seen = {}, dedup = [];
    for (i = 0; i < out.length; i++) {
      if (seen[out[i].asof]) continue;
      seen[out[i].asof] = 1;
      dedup.push(out[i]);
    }
    return dedup;
  }

  function changeFromHistory(points, newer, minD, maxD, preferFp) {
    var i, days, hit, best = null;
    if (!newer || !finite(newer.value)) return null;
    for (i = 0; i < points.length; i++) {
      if (points[i].asof === newer.asof) continue;
      days = (newer.t - points[i].t) / 86400000;
      if (days >= minD && days <= maxD) {
        hit = { pct: round1(pctChange(newer.value, points[i].value)), prior: points[i].value, prior_asof: points[i].asof, days: Math.round(days), fp: points[i].fp || "" };
        if (preferFp && newer.fp && points[i].fp && String(newer.fp) === String(points[i].fp)) return hit;
        if (!best) best = hit;
      }
    }
    return best;
  }

  function computeChanges(hist, latestValue, latestAsof, latestFp) {
    var pts = historyPoints(hist);
    var t = parseDay(latestAsof);
    var newer = (t != null && finite(latestValue))
      ? { t: t, asof: String(latestAsof).slice(0, 10), value: latestValue, fp: latestFp || "" }
      : pts[0];
    if (!newer) return { mom: null, qoq: null, yoy: null, mom_meta: null, qoq_meta: null, yoy_meta: null };
    var mom = changeFromHistory(pts, newer, MOM_MIN, MOM_MAX, false);
    var qoq = changeFromHistory(pts, newer, QOQ_MIN, QOQ_MAX, false);
    var yoy = changeFromHistory(pts, newer, YOY_MIN, YOY_MAX, true);
    return {
      mom: mom ? mom.pct : null,
      qoq: qoq ? qoq.pct : null,
      yoy: yoy ? yoy.pct : null,
      mom_meta: mom,
      qoq_meta: qoq,
      yoy_meta: yoy
    };
  }

  function plausibleVsXbrl(minedUsd, xbrlUsd) {
    if (!finite(minedUsd) || minedUsd <= 0) return false;
    if (!finite(xbrlUsd) || xbrlUsd <= 0) return true;
    if (minedUsd < 0.05 * xbrlUsd) return false;
    if (minedUsd > 20 * xbrlUsd) return false;
    return true;
  }

  function chooseBook(xbrl, mined, now, fo) {
    xbrl = xbrl || {};
    mined = mined || {};
    fo = fo || {};
    var fd = fo.data || {};
    var xAge = ageDays(xbrl.rpo_asof, now);
    var dAge = ageDays(xbrl.deferred_asof, now);
    var mAge = ageDays(mined.asof, now);
    var fAge = ageDays(fd.rpo_as_of, now);
    var refUsd = finite(fd.rpo_latest_usd) && fd.rpo_latest_usd > 0 ? fd.rpo_latest_usd : xbrl.rpo;
    var xOk = finite(xbrl.rpo) && xbrl.rpo > 0 && xAge != null && xAge <= MAX_AGE_DAYS;
    var fOk = finite(fd.rpo_latest_usd) && fd.rpo_latest_usd > 0 && fAge != null && fAge <= MAX_AGE_DAYS;
    var dOk = finite(xbrl.deferred_rev) && xbrl.deferred_rev > 0 && dAge != null && dAge <= MAX_AGE_DAYS;
    var mOk = mined.status === "MINED" && finite(mined.backlog_usd) && mined.backlog_usd > 0 && mAge != null && mAge <= MAX_AGE_DAYS && plausibleVsXbrl(mined.backlog_usd, refUsd);

    if (fOk) {
      return {
        kind: "rpo",
        usd: fd.rpo_latest_usd,
        asof: fd.rpo_as_of,
        tag: fd.rpo_tag || "RevenueRemainingPerformanceObligation",
        form: "10-Q",
        fp: (fd.rpo_history && fd.rpo_history[0] && fd.rpo_history[0].fp) || "",
        qoq_given: null,
        yoy_given: finite(fd.rpo_growth_yoy_pct) ? fd.rpo_growth_yoy_pct : null,
        src: "forward"
      };
    }
    if (xOk) {
      return {
        kind: "rpo",
        usd: xbrl.rpo,
        asof: xbrl.rpo_asof,
        tag: xbrl.rpo_tag || "RevenueRemainingPerformanceObligation",
        form: xbrl.rpo_form || "10-Q",
        fp: "",
        qoq_given: finite(xbrl.rpo_qoq) ? xbrl.rpo_qoq : null,
        yoy_given: finite(xbrl.rpo_yoy) ? xbrl.rpo_yoy : null,
        src: "xbrl"
      };
    }
    if (mOk) {
      return {
        kind: "backlog",
        usd: mined.backlog_usd,
        asof: mined.asof,
        tag: "MD&A backlog (mined)",
        form: mined.src || "",
        fp: "",
        qoq_given: finite(mined.backlog_qoq_pct) ? mined.backlog_qoq_pct : null,
        yoy_given: finite(mined.backlog_yoy_pct) ? mined.backlog_yoy_pct : null,
        src: "mined"
      };
    }
    if (dOk) {
      return {
        kind: "deferred",
        usd: xbrl.deferred_rev,
        asof: xbrl.deferred_asof,
        tag: "DeferredRevenue",
        form: xbrl.deferred_filed || "",
        fp: "",
        qoq_given: finite(xbrl.deferred_qoq) ? xbrl.deferred_qoq : null,
        yoy_given: finite(xbrl.deferred_yoy) ? xbrl.deferred_yoy : null,
        src: "deferred"
      };
    }
    return null;
  }

  function rowFromParts(ticker, xbrl, mined, fo, now) {
    ticker = String(ticker || "").toUpperCase();
    xbrl = xbrl || {};
    mined = mined || {};
    fo = fo || {};
    var book = chooseBook(xbrl, mined, now, fo);
    if (!book) return null;
    var hist = (fo.data && fo.data.rpo_history) || [];
    var ch = computeChanges(hist, book.usd, book.asof, book.fp);
    var qoq = ch.qoq != null ? ch.qoq : book.qoq_given;
    var yoy = ch.yoy != null ? ch.yoy : book.yoy_given;
    var mom = ch.mom;
    var warn = [];
    if (finite(qoq) && Math.abs(qoq) >= IMPLAUSIBLE_PCT) warn.push("qoq");
    if (finite(yoy) && Math.abs(yoy) >= IMPLAUSIBLE_PCT) warn.push("yoy");
    return {
      ticker: ticker,
      name: fo.name || xbrl.name || ticker,
      sector: fo.sector || xbrl.sector || xbrl.group || "",
      cap: xbrl.cap_bucket || "",
      book_usd: book.usd,
      book_kind: book.kind,
      book_src: book.src,
      asof: String(book.asof || "").slice(0, 10),
      tag: book.tag,
      mom: mom,
      qoq: finite(qoq) ? round1(qoq) : null,
      yoy: finite(yoy) ? round1(yoy) : null,
      mom_reason: mom == null ? "10-Q cadence, not monthly" : null,
      price_mom: null,
      price_qoq: null,
      price_yoy: null,
      last_close: null,
      rev_yoy: finite(xbrl.rev_yoy) ? xbrl.rev_yoy : null,
      ev_to_rpo: finite(xbrl.ev_to_rpo) ? xbrl.ev_to_rpo : null,
      accelerating: !!xbrl.demand_accelerating,
      warn: warn,
      filing: book.form
    };
  }

  function buildRows(backlog, mined, forward, now) {
    backlog = backlog || {};
    mined = mined || {};
    forward = forward || {};
    var byX = backlog.by_ticker || {};
    var byM = mined.by_ticker || {};
    var byF = {};
    (forward.all_results || forward.top_25_by_score || []).forEach(function (r) {
      if (r && r.ticker) byF[String(r.ticker).toUpperCase()] = r;
    });
    var tickers = {}, k;
    for (k in byX) tickers[k] = 1;
    for (k in byM) tickers[k] = 1;
    for (k in byF) tickers[k] = 1;
    var rows = [], t, row;
    for (t in tickers) {
      row = rowFromParts(t, byX[t], byM[t], byF[t], now);
      if (row) rows.push(row);
    }
    return rows;
  }

  function attachPrices(rows, momDoc) {
    var map = {}, rankings = (momDoc && momDoc.rankings) || {}, k;
    Object.keys(rankings).forEach(function (key) {
      (rankings[key] || []).forEach(function (r) {
        if (!r || !r.ticker) return;
        map[String(r.ticker).toUpperCase()] = r;
      });
    });
    (rows || []).forEach(function (row) {
      var p = map[row.ticker];
      if (!p) return;
      row.price_mom = finite(p.ret_1m) ? round1(p.ret_1m) : null;
      row.price_qoq = finite(p.ret_3m) ? round1(p.ret_3m) : null;
      row.price_yoy = finite(p.ret_12m) ? round1(p.ret_12m) : null;
      row.last_close = finite(p.last_close) ? p.last_close : null;
    });
    return rows;
  }

  function priceBookRows(momDoc) {
    var map = {}, rankings = (momDoc && momDoc.rankings) || {};
    Object.keys(rankings).forEach(function (key) {
      (rankings[key] || []).forEach(function (r) {
        if (!r || !r.ticker) return;
        var t = String(r.ticker).toUpperCase();
        if (map[t]) return;
        map[t] = {
          ticker: t,
          name: r.name || t,
          sector: r.sector || "",
          last_close: finite(r.last_close) ? r.last_close : null,
          price_mom: finite(r.ret_1m) ? round1(r.ret_1m) : null,
          price_qoq: finite(r.ret_3m) ? round1(r.ret_3m) : null,
          price_yoy: finite(r.ret_12m) ? round1(r.ret_12m) : null,
          ret_6m: finite(r.ret_6m) ? round1(r.ret_6m) : null,
          composite: finite(r.composite_score) ? r.composite_score : null
        };
      });
    });
    return Object.keys(map).map(function (k) { return map[k]; });
  }

  function sortRows(rows, key, dir) {
    var out = (rows || []).slice();
    var sign = dir === "asc" ? 1 : -1;
    var numeric = { book_usd: 1, mom: 1, qoq: 1, yoy: 1, ev_to_rpo: 1, price_mom: 1, price_qoq: 1, price_yoy: 1, last_close: 1, ret_6m: 1, composite: 1 };
    out.sort(function (a, b) {
      var av = a[key], bv = b[key];
      var aNull = av == null || av === "";
      var bNull = bv == null || bv === "";
      if (aNull && bNull) return String(a.ticker).localeCompare(String(b.ticker));
      if (aNull) return 1;
      if (bNull) return -1;
      if (numeric[key]) {
        var an = Number(av), bn = Number(bv);
        if (an === bn) return String(a.ticker).localeCompare(String(b.ticker));
        return an > bn ? sign : -sign;
      }
      var cmp = String(av).localeCompare(String(bv));
      return cmp === 0 ? String(a.ticker).localeCompare(String(b.ticker)) : cmp * sign;
    });
    return out;
  }

  var api = {
    MAX_AGE_DAYS: MAX_AGE_DAYS,
    parseDay: parseDay,
    ageDays: ageDays,
    pctChange: pctChange,
    historyPoints: historyPoints,
    computeChanges: computeChanges,
    plausibleVsXbrl: plausibleVsXbrl,
    chooseBook: chooseBook,
    rowFromParts: rowFromParts,
    buildRows: buildRows,
    attachPrices: attachPrices,
    priceBookRows: priceBookRows,
    sortRows: sortRows
  };
  root.jhOrderbook = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
