/* jh-growth-stack.js — six growth-lead desks from live JustHodl feeds.
   Does not invent 8-K guidance, RPO timing bands, NRR, or job-posting panels
   when those tags are absent. */
(function (root) {
  if (root.__jhGrowthStackV1) return;
  root.__jhGrowthStackV1 = true;

  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
  var STATE = { ready: false, billings: {}, street: {}, quality: {}, hiring: {}, fo: {}, radar: {} };

  function up(t) { return String(t == null ? "" : t).toUpperCase().trim(); }
  function num(v) { var n = Number(v); return isFinite(n) ? n : null; }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) {
      if (c === "&") return "\u0026amp;";
      if (c === "<") return "\u0026lt;";
      if (c === ">") return "\u0026gt;";
      return "\u0026quot;";
    });
  }
  function bn(v) {
    if (v == null || !isFinite(v)) return "";
    var a = Math.abs(v);
    if (a >= 1e12) return "$" + (v / 1e12).toFixed(2) + "T";
    if (a >= 1e9) return "$" + (v / 1e9).toFixed(1) + "B";
    if (a >= 1e6) return "$" + (v / 1e6).toFixed(0) + "M";
    return "$" + Math.round(v).toLocaleString("en-US");
  }
  function pct(v, d) {
    if (v == null || !isFinite(v)) return "";
    d = d == null ? 1 : d;
    return (v >= 0 ? "+" : "") + Number(v).toFixed(d) + "%";
  }

  function ttmFromHistory(hist) {
    if (!Array.isArray(hist) || !hist.length) return null;
    var rows = hist.filter(function (h) { return h && isFinite(Number(h.value)); })
      .slice().sort(function (a, b) { return String(a.end || "").localeCompare(String(b.end || "")); });
    if (!rows.length) return null;
    var last = rows[rows.length - 1];
    if (rows.length >= 4) {
      var tail = rows.slice(-4);
      var ta = Date.parse(tail[2].end);
      var tb = Date.parse(tail[3].end);
      var gap = (isFinite(ta) && isFinite(tb)) ? (tb - ta) / 864e5 : 9999;
      if (gap > 50 && gap < 160) {
        var s = 0;
        tail.forEach(function (h) { s += Number(h.value); });
        return s;
      }
    }
    return Number(last.value);
  }

  function foList(doc) {
    if (!doc) return [];
    if (Array.isArray(doc.all_results)) return doc.all_results;
    if (Array.isArray(doc.top_25_by_score)) return doc.top_25_by_score;
    return [];
  }

  function loadFromDocs(backlog, fo, revisions, quality, hiring) {
    var radar = {};
    var src = (backlog && backlog.by_ticker) || {};
    Object.keys(src).forEach(function (k) { radar[up(k)] = src[k]; });
    var foIdx = {};
    foList(fo).forEach(function (row) {
      var t = up(row.ticker);
      if (t) foIdx[t] = row;
    });
    var billings = {};
    var names = {};
    Object.keys(radar).forEach(function (t) { names[t] = 1; });
    Object.keys(foIdx).forEach(function (t) { names[t] = 1; });
    Object.keys(names).forEach(function (t) {
      var r = radar[t] || {};
      var f = foIdx[t] || {};
      var d = f.data || {};
      var ttm = ttmFromHistory(d.revenue_history);
      var rpo = num(d.rpo_latest_usd) != null ? num(d.rpo_latest_usd) : num(r.rpo);
      var deferred = num(r.deferred_rev);
      var coverage = (rpo != null && ttm && ttm > 0) ? rpo / ttm : null;
      var defYoy = num(r.deferred_yoy);
      var billingsEst = null;
      if (ttm != null && defYoy != null && deferred != null && defYoy > -100) {
        billingsEst = ttm + (deferred - (deferred / (1 + defYoy / 100)));
      } else if (ttm != null) {
        billingsEst = ttm;
      }
      billings[t] = {
        ticker: t,
        sector: r.sector || f.sector || "",
        rpo: rpo,
        deferred: deferred,
        ttm_rev: ttm,
        billings: billingsEst,
        coverage_yrs: coverage,
        deferred_yoy: defYoy,
        deferred_qoq: num(r.deferred_qoq),
        rev_yoy: num(r.rev_yoy) != null ? num(r.rev_yoy) : num(d.rpo_growth_yoy_pct),
        btb: num(d.book_to_bill_spread_pct),
        rpo_yoy: num(r.rpo_yoy) != null ? num(r.rpo_yoy) : num(d.rpo_growth_yoy_pct),
        rpo_qoq: num(r.rpo_qoq),
        accelerating: !!(r.demand_accelerating || r.deferred_accelerating),
        asof: d.rpo_as_of || r.rpo_asof || r.deferred_asof || "",
        src: rpo != null && d.rpo_latest_usd != null ? "forward" : (r.rpo ? "xbrl" : "deferred")
      };
    });
    var street = {};
    var byT = (revisions && revisions.by_ticker) || {};
    Object.keys(byT).forEach(function (k) {
      var row = byT[k] || {};
      var t = up(row.ticker || k);
      street[t] = {
        ticker: t,
        name: row.company || t,
        current_eps: num(row.current_eps_est),
        baseline_eps: num(row.baseline_eps_est),
        scheduled_eps: num(row.scheduled_eps_est),
        eps_rev_pct: num(row.eps_rev_pct),
        rev_rev_pct: num(row.rev_rev_pct),
        fwd_eps_growth_pct: num(row.fwd_eps_growth_pct),
        direction: row.direction || "",
        earnings_date: row.earnings_date || "",
        n_analysts: num(row.n_analysts),
        dispersion_pct: num(row.dispersion_pct),
        estimate_strength: num(row.estimate_strength)
      };
    });
    var qualityIdx = {};
    ((quality && quality.all_ranked) || (quality && quality.top_20_high_quality) || []).forEach(function (row) {
      var t = up(row.ticker);
      if (!t) return;
      var ocf = num(row.ttm_ocf_usd);
      var ni = num(row.ttm_ni_usd);
      qualityIdx[t] = {
        ticker: t,
        name: row.name || t,
        ni: ni,
        ocf: ocf,
        fcf: num(row.ttm_fcf_usd),
        sloan: num(row.sloan_accruals_pct_assets),
        cash_conv: num(row.cash_conversion_ratio) != null ? num(row.cash_conversion_ratio) : (ni && ni !== 0 && ocf != null ? ocf / ni : null),
        dsri: num(row.dsri_beneish),
        gmi: num(row.gmi_beneish),
        accruals_yoy: num(row.accruals_change_yoy_pct),
        quality_score: num(row.quality_score)
      };
    });
    var hiringIdx = {};
    function addHire(row) {
      if (!row) return;
      var t = up(row.symbol || row.ticker);
      if (!t) return;
      hiringIdx[t] = {
        ticker: t,
        name: row.name || t,
        sector: row.sector || "",
        state: row.state || "",
        score: num(row.expansion_score),
        headcount: num(row.headcount_latest),
        yoy: num(row.headcount_yoy_pct),
        accel: num(row.headcount_accel_pp),
        rpe: num(row.revenue_per_employee),
        rpe_trend: num(row.revenue_per_employee_trend_pct),
        inflection: !!row.inflection,
        notes: row.notes || []
      };
    }
    ((hiring && hiring.top_50) || []).forEach(addHire);
    ((hiring && hiring.expansion_inflections) || []).forEach(addHire);
    ((hiring && hiring.double_confirmed) || []).forEach(addHire);
    STATE = { ready: true, billings: billings, street: street, quality: qualityIdx, hiring: hiringIdx, fo: foIdx, radar: radar };
    return STATE;
  }

  function lookup(ticker) {
    var t = up(ticker);
    if (!t) return null;
    var b = STATE.billings[t] || null;
    var s = STATE.street[t] || null;
    var q = STATE.quality[t] || null;
    var h = STATE.hiring[t] || null;
    var f = STATE.fo[t] || null;
    if (!b && !s && !q && !h && !f) return null;
    return { ticker: t, billings: b, street: s, quality: q, hiring: h, fo_score: f && f.composite != null ? f.composite : null, btb: b ? b.btb : null };
  }

  function list(kind) {
    if (kind === "billings") return Object.keys(STATE.billings).map(function (k) { return STATE.billings[k]; });
    if (kind === "street") return Object.keys(STATE.street).map(function (k) { return STATE.street[k]; });
    if (kind === "quality") return Object.keys(STATE.quality).map(function (k) { return STATE.quality[k]; });
    if (kind === "hiring") return Object.keys(STATE.hiring).map(function (k) { return STATE.hiring[k]; });
    return [];
  }

  async function pull(path) {
    var urls = [path + "?t=" + Date.now(), PROXY + path + "?t=" + Date.now(), S3 + path + "?t=" + Date.now()];
    for (var i = 0; i < urls.length; i++) {
      try {
        var r = await fetch(urls[i], { cache: "no-store" });
        if (r.ok) return await r.json();
      } catch (e) {}
    }
    return null;
  }

  async function boot() {
    var pack = await Promise.all([
      pull("/data/backlog.json"),
      pull("/data/forward-orders.json"),
      pull("/data/estimate-revisions.json"),
      pull("/data/earnings-quality.json"),
      pull("/data/hiring-velocity.json")
    ]);
    loadFromDocs(pack[0] || {}, pack[1] || {}, pack[2] || {}, pack[3] || {}, pack[4] || {});
    return STATE;
  }

  var api = {
    loadFromDocs: loadFromDocs,
    lookup: lookup,
    list: list,
    boot: boot,
    ttmFromHistory: ttmFromHistory,
    esc: esc,
    bn: bn,
    pct: pct,
    state: function () { return STATE; }
  };
  root.jhGrowthStack = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
