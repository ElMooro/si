/*
 * jh-credit-board.js - the Credit Before Equity board (credit-before-equity.html).
 *
 * Reads data/credit-before-equity.json (contract credit-before-equity.v2) and draws: the evidence status, the industry heat
 * map of spread changes, the two ranked lists (widening most / widening least), the leads, the sortable table of every
 * company, the company CDS table, and the previous 26-name model board one click away.
 *
 * jh-reskin-skip: on this board a colour is a measurement. The site build's re-skin (scripts/reskin_site.py) turns every
 * cool hue into amber; red for a widening spread and green for a tightening one must stay what they are. Every data colour
 * lives in this file and nowhere else; the page's own chrome is written in the site's warm palette.
 */
(function (root) {
  "use strict";

  var PALETTES = {
    rg: { name: "Red and green", bad: "#f23645", good: "#089981", mid: "#363a45", none: "#20242e", badText: "#f7525f", goodText: "#2bb89f" },
    cb: { name: "Orange and blue", bad: "#e8623a", good: "#3b82f6", mid: "#363a45", none: "#20242e", badText: "#f08c5a", goodText: "#6aa8ff" }
  };
  var STORE = "jh-credit-board.v1";
  var WINDOWS = ["w1", "m1", "m3", "ytd"];
  var WIN_COL = { w1: "d_w1", m1: "d_m1", m3: "d_m3", ytd: "d_ytd" };
  var WIN_SHORT = { w1: "1W", m1: "1M", m3: "3M", ytd: "YTD" };
  var BOOK_SHORT = { ig: "IG", hy: "HY", em: "EM" };
  var MEASURES = [["bp", "Change, bp"], ["pct", "Change, %"], ["rel", "Against the market"], ["level", "Spread level"]];
  var LEADS = ["CREDIT_LEADS_DOWN", "CREDIT_LEADS_UP"];
  var DATA_URL = "https://justhodl.ai/data/credit-before-equity.json";

  // ------------------------------------------------------------------------------------------------ small things
  function esc(s) {
    return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function isCount(v) { return typeof v === "number" && isFinite(v) && v >= 0 && Math.floor(v) === v; }
  function isObj(v) { return !!v && typeof v === "object" && !Array.isArray(v); }
  function clamp(v, lo, hi) { return v < lo ? lo : v > hi ? hi : v; }
  function minus(s) { return s.replace("-", "−"); }
  /* A figure rounded for reading: the sign of the ROUNDED value, so -0.04 is "0.0" and never "-0.0". */
  function fixed(v, d) {
    var r = Number(v).toFixed(d);
    if (Number(r) === 0) r = (0).toFixed(d);
    return r;
  }
  function bp(v, signed) {
    if (!isNum(v)) return "–";
    var d = Math.abs(v) >= 100 ? 0 : 1, r = fixed(v, d);
    if (Math.abs(Number(r)) >= 1000) r = Number(r).toLocaleString("en-US", { maximumFractionDigits: 0 });
    return minus((signed && Number(fixed(v, d)) > 0 ? "+" : "") + r);
  }
  function pct(v, signed) {
    if (!isNum(v)) return "–";
    var r = fixed(v, 1);
    return minus((signed && Number(r) > 0 ? "+" : "") + r) + "%";
  }
  function money(v) {
    if (!isNum(v)) return "–";
    if (v >= 1e9) return "$" + fixed(v / 1e9, 1) + "B";
    if (v >= 1e6) return "$" + fixed(v / 1e6, 0) + "M";
    return "$" + fixed(v / 1e3, 0) + "K";
  }
  function count(v) { return isCount(v) ? v.toLocaleString("en-US") : "Unavailable"; }
  function day(s) {
    if (typeof s !== "string" || s.length < 10) return "Unavailable";
    var m = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][Number(s.slice(5, 7)) - 1];
    return m ? m + " " + Number(s.slice(8, 10)) + ", " + s.slice(0, 4) : "Unavailable";
  }
  function stamp(s) {
    if (typeof s !== "string" || s.length < 16) return "Unavailable";
    return day(s) + " " + s.slice(11, 16) + " UTC";
  }

  // ------------------------------------------------------------------------------------------------ the document
  function isV2(doc) { return isObj(doc) && doc.contract === "credit-before-equity.v2" && isObj(doc.issuers) && Array.isArray(doc.issuers.rows); }

  /* Every company of the board as an object; the columnar table is turned once and kept on the document. */
  function table(doc) {
    if (!isV2(doc)) return [];
    if (doc.__table) return doc.__table;
    var cols = doc.issuers.cols || [], out = [];
    doc.issuers.rows.forEach(function (r) {
      if (!Array.isArray(r) || r.length !== cols.length) return;
      var o = {};
      for (var i = 0; i < cols.length; i++) o[cols[i]] = r[i];
      out.push(o);
    });
    Object.defineProperty(doc, "__table", { value: out, enumerable: false });
    return out;
  }

  function windowList(doc) {
    var w = (isObj(doc) && isObj(doc.windows)) ? doc.windows : {};
    return WINDOWS.map(function (k) {
      var m = isObj(w[k]) ? w[k] : {};
      return { key: k, short: WIN_SHORT[k], words: typeof m.words === "string" ? m.words : WIN_SHORT[k], ref: typeof m.ref === "string" ? m.ref : null,
               days: isNum(m.days) ? m.days : null, source: typeof m.source === "string" ? m.source : null, why: typeof m.why === "string" ? m.why : null,
               n: isCount(m.n_companies) ? m.n_companies : null, ok: typeof m.ref === "string" };
    });
  }

  /* What the page can and cannot stand behind. It reconciles the document with itself; it never claims the sources are fresh. */
  function evidence(doc) {
    if (!isObj(doc)) return { state: "UNAVAILABLE", headline: "The published document could not be read.", checks: [], notes: [] };
    var names = Array.isArray(doc.names) ? doc.names.filter(isObj) : null;
    var checks = [], notes = [];
    function check(label, ok, detail) { checks.push({ label: label, ok: ok, detail: detail }); }
    if (!names) {
      return { state: "UNAVAILABLE", headline: "The document carries no rows.", checks: [], notes: [] };
    }
    var leadRows = names.filter(function (x) { return LEADS.indexOf(x.signal) >= 0; });
    var waiting = names.filter(function (x) { return x.signal === "INSUFFICIENT_HISTORY"; });
    check("Listed names counted", isCount(doc.n_names) ? doc.n_names === names.length : null, isCount(doc.n_names) ? doc.n_names + " declared, " + names.length + " rows" : "count not published");
    check("Leads counted", isCount(doc.n_leads) ? doc.n_leads === leadRows.length : null, isCount(doc.n_leads) ? doc.n_leads + " declared, " + leadRows.length + " lead rows" : "count not published");
    var preview = Array.isArray(doc.leads) ? doc.leads : null;
    var same = preview && preview.length === Math.min(20, leadRows.length) && preview.every(function (x, i) { return isObj(x) && x.ticker === leadRows[i].ticker && x.signal === leadRows[i].signal; });
    check("Lead preview is the first 20 lead rows", preview ? !!same : null, preview ? preview.length + " in the preview" : "preview not published");
    check("Names awaiting a credit reading counted", isCount(doc.n_awaiting_history) ? doc.n_awaiting_history === waiting.length : null,
          isCount(doc.n_awaiting_history) ? doc.n_awaiting_history + " declared, " + waiting.length + " rows" : "count not published");
    if (isV2(doc)) {
      var t = table(doc), cov = isObj(doc.coverage) ? doc.coverage : {};
      check("Every company row is whole", t.length === doc.issuers.rows.length, t.length + " of " + doc.issuers.rows.length + " rows as wide as the header");
      check("Companies counted", isCount(cov.companies) ? cov.companies === t.length : null, isCount(cov.companies) ? cov.companies + " declared, " + t.length + " rows" : "count not published");
      var listed = t.filter(function (x) { return !!x.ticker; }).length;
      check("Listed companies counted", isCount(cov.listed) ? cov.listed === listed : null, isCount(cov.listed) ? cov.listed + " declared, " + listed + " rows with a ticker" : "count not published");
      var all = isObj(doc.industries) && Array.isArray(doc.industries.all) ? doc.industries.all : null;
      var inInd = all ? all.reduce(function (s, i) { return s + (isCount(i.n) ? i.n : 0); }, 0) : null;
      check("Industries hold every company", all ? inInd === t.length : null, all ? inInd + " in " + all.length + " industries, " + t.length + " companies" : "industries not published");
      windowList(doc).forEach(function (w) { if (!w.ok) notes.push(w.words + ": not available yet. " + (w.why || "")); });
    } else {
      notes.push("This is the engine's first document (26 names, a structural model). The board of every company appears when the 2.0 engine has published.");
    }
    var degraded = Array.isArray(doc.degraded) ? doc.degraded.filter(function (x) { return typeof x === "string" && x.trim(); }) : null;
    if (degraded === null) notes.push("The engine did not say whether the run was degraded.");
    var failed = checks.filter(function (c) { return c.ok === false; }).length, unknown = checks.filter(function (c) { return c.ok === null; }).length;
    var state = failed ? "MISMATCH" : (unknown || degraded === null) ? "INCOMPLETE" : (degraded.length ? "DEGRADED" : "RECONCILED");
    var headline = { MISMATCH: "The document does not agree with itself: " + failed + " of " + checks.length + " checks failed. Read the figures below with that in mind.",
                     INCOMPLETE: "The document is incomplete: " + unknown + " of " + checks.length + " checks could not be made.",
                     DEGRADED: "The counts reconcile; the engine reports a degraded run.",
                     RECONCILED: "The counts reconcile with the rows (" + checks.length + " checks)." }[state];
    return { state: state, headline: headline, checks: checks, notes: notes, degraded: degraded || [] };
  }

  // ------------------------------------------------------------------------------------------------ arithmetic
  function weightedMedian(pairs) {
    var p = pairs.filter(function (x) { return isNum(x[0]); }).map(function (x) { return [x[0], Math.max(isNum(x[1]) ? x[1] : 0, 0)]; }).sort(function (a, b) { return a[0] - b[0]; });
    if (!p.length) return null;
    var total = p.reduce(function (s, x) { return s + x[1]; }, 0), run = 0, i;
    if (!(total > 0)) return p.length % 2 ? p[(p.length - 1) / 2][0] : (p[p.length / 2 - 1][0] + p[p.length / 2][0]) / 2;
    for (i = 0; i < p.length; i++) { run += p[i][1]; if (run >= total / 2) return p[i][0]; }
    return p[p.length - 1][0];
  }

  /* The market's own change: the middle company by market value held, in the chosen book. */
  function marketChange(doc, win, book) {
    var col = WIN_COL[win];
    return weightedMedian(table(doc).filter(function (r) { return !book || book === "all" || r.book === book; }).map(function (r) { return [r[col], r.value]; }));
  }

  /* The heat map's rows for one window, book and measure. v is the figure that is coloured and ranked. */
  function industryView(doc, opt) {
    if (!isV2(doc) || !isObj(doc.industries)) return [];
    var rows = doc.industries[opt.book && opt.book !== "all" ? opt.book : "all"];
    if (!Array.isArray(rows)) return [];
    var market = opt.measure === "rel" ? marketChange(doc, opt.win, opt.book) : null;
    return rows.filter(isObj).map(function (r) {
      var w = isObj(r.win) && isObj(r.win[opt.win]) ? r.win[opt.win] : null;
      var o = { industry: r.industry, n: r.n, n_listed: r.n_listed, value: r.value, spread_bp: r.spread_bp, n_distressed: r.n_distressed,
                d_bp: w ? w.d_bp : null, d_pct: w ? w.d_pct : null, from_bp: w ? w.from_bp : null, n_win: w ? w.n : null, wider: w ? w.wider : null, tighter: w ? w.tighter : null,
                widest: w ? w.widest : null, tightest: w ? w.tightest : null };
      o.v = opt.measure === "level" ? o.spread_bp : opt.measure === "pct" ? o.d_pct : opt.measure === "rel" ? (isNum(o.d_bp) && isNum(market) ? Math.round((o.d_bp - market) * 10) / 10 : null) : o.d_bp;
      return o;
    });
  }

  /* The figure at which the colour is full: the largest move on the map, never under a floor, so a quiet week is not painted loud. */
  function scaleOf(view, measure) {
    var vals = view.map(function (r) { return r.v; }).filter(isNum).map(Math.abs);
    if (measure === "level") return { lo: Math.min.apply(null, vals.concat([Infinity])), hi: Math.max.apply(null, vals.concat([0])) };
    var floor = measure === "pct" ? 5 : 10;
    return { lo: 0, hi: Math.max(floor, vals.length ? Math.max.apply(null, vals) : 0) };
  }
  function mix(a, b, t) {
    function ch(h, i) { return parseInt(h.slice(1 + 2 * i, 3 + 2 * i), 16); }
    var out = "#", i, v;
    for (i = 0; i < 3; i++) { v = Math.round(ch(a, i) + (ch(b, i) - ch(a, i)) * t); out += (v < 16 ? "0" : "") + v.toString(16); }
    return out;
  }
  /* A wider spread is trouble (bad colour), a tighter one safety (good colour); a level runs one hue from quiet to loud. */
  function colour(v, scale, measure, palette) {
    var p = PALETTES[palette] || PALETTES.rg;
    if (!isNum(v)) return p.none;
    if (measure === "level") {
      var span = scale.hi - scale.lo;
      return mix(p.mid, p.bad, span > 0 ? clamp((v - scale.lo) / span, 0, 1) * 0.9 + 0.1 : 0.5);
    }
    var t = clamp(Math.abs(v) / (scale.hi || 1), 0, 1);
    return mix(p.mid, v > 0 ? p.bad : p.good, v === 0 ? 0 : 0.18 + 0.82 * t);
  }
  function textColour(v, palette) {
    var p = PALETTES[palette] || PALETTES.rg;
    return !isNum(v) || v === 0 ? "" : v > 0 ? p.badText : p.goodText;
  }

  function squarify(nodes, x, y, w, h) {
    var list = nodes.filter(function (n) { return n.v > 0; }).sort(function (a, b) { return b.v - a.v; });
    var total = list.reduce(function (s, n) { return s + n.v; }, 0), out = [], i = 0;
    if (!list.length || !(w > 0) || !(h > 0)) return out;
    var k = w * h / total;
    while (i < list.length) {
      var side = Math.min(w, h), row = [], sum = 0, best = Infinity;
      while (i < list.length) {
        var a = list[i].v * k, s2 = sum + a, mx = row.length ? row[0] : a;
        var worst = Math.max(side * side * mx / (s2 * s2), s2 * s2 / (side * side * a));
        if (row.length && worst > best) break;
        row.push(a); sum = s2; best = worst; i++;
      }
      var thick = sum / side, off = 0, start = i - row.length, j, len;
      for (j = 0; j < row.length; j++) {
        len = row[j] / thick;
        if (w >= h) out.push({ node: list[start + j], x: x, y: y + off, w: thick, h: len });
        else out.push({ node: list[start + j], x: x + off, y: y, w: len, h: thick });
        off += len;
      }
      if (w >= h) { x += thick; w -= thick; } else { y += thick; h -= thick; }
    }
    return out;
  }

  /* Sorted copy: by one column, ascending or descending; a row with no figure goes last either way; ties keep their order. */
  function sortRows(rows, key, dir) {
    var sign = dir === "asc" ? 1 : -1;
    return rows.map(function (r, i) { return [r, i]; }).sort(function (a, b) {
      var x = a[0][key], y = b[0][key], nx = x == null || x === "", ny = y == null || y === "";
      if (nx || ny) return nx && ny ? a[1] - b[1] : nx ? 1 : -1;
      if (typeof x === "string" || typeof y === "string") { var c = String(x).localeCompare(String(y)); return c ? sign * c : a[1] - b[1]; }
      return x === y ? a[1] - b[1] : sign * (x - y);
    }).map(function (p) { return p[0]; });
  }

  function filterRows(rows, f) {
    var q = (f.q || "").trim().toLowerCase();
    return rows.filter(function (r) {
      if (f.industry && r.industry !== f.industry) return false;
      if (f.book && f.book !== "all" && r.book !== f.book) return false;
      if (f.listed && !r.ticker) return false;
      if (f.signal === "leads" && LEADS.indexOf(r.signal) < 0) return false;
      if (f.signal === "cds" && !isNum(r.cds_bp)) return false;
      if (f.signal === "distressed" && !r.distressed) return false;
      if (q && String(r.name || "").toLowerCase().indexOf(q) < 0 && String(r.ticker || "").toLowerCase().indexOf(q) < 0) return false;
      return true;
    });
  }

  /* The table's rows carry d (the change over the chosen window) so one sort key follows the window. */
  function withWindow(rows, win) {
    var col = WIN_COL[win];
    return rows.map(function (r) { var o = Object.create(r); o.d = r[col]; return o; });
  }

  function industriesIn(rows) {
    var seen = {};
    rows.forEach(function (r) { if (r.industry) seen[r.industry] = (seen[r.industry] || 0) + 1; });
    return Object.keys(seen).sort().map(function (k) { return [k, seen[k]]; });
  }

  // ------------------------------------------------------------------------------------------------ drawing
  var COLUMNS = [
    ["name", "Company", "l"], ["industry", "Industry", "l"], ["book", "Book", "l"], ["n_bonds", "Bonds", "r"], ["value", "Held", "r"], ["spread_bp", "Spread", "r"], ["d", "Δ", "r"],
    ["x_m1", "Past book", "r"], ["pct_m1", "Δ %", "r"], ["d_ytd", "YTD Δ", "r"], ["cds_bp", "CDS", "r"], ["cds_d1m", "CDS 1M", "r"], ["px_m1", "Stock", "r"],
    ["direction", "Credit", "l"], ["signal", "Signal", "l"]
  ];
  var CDS_COLUMNS = [
    ["name", "Reference entity", "l"], ["ticker", "Ticker", "l"], ["sector", "Sector", "l"], ["region", "Region", "l"], ["spread_bp", "Level", "r"], ["d1d", "1D", "r"], ["d1w", "1W", "r"],
    ["d1m", "1M", "r"], ["d3m", "3M", "r"], ["rank_1y", "1Y rank", "r"], ["last_date", "Last print", "l"], ["trades_30d", "Trades 30d", "r"], ["px_1m", "Stock 1M", "r"]
  ];
  var SIGNAL_WORDS = { CREDIT_LEADS_DOWN: "Credit leads down", CREDIT_LEADS_UP: "Credit leads up", NONE: "No lead", INSUFFICIENT_HISTORY: "No reading" };
  var DIR_WORDS = { DETERIORATING: "Worse", IMPROVING: "Better", FLAT: "Flat" };

  function seg(name, options, value, label) {
    return '<div class="cb-ctl"><span class="cb-ctl-l">' + esc(label) + '</span><div class="cb-seg" role="group" aria-label="' + esc(label) + '">' + options.map(function (o) {
      return '<button type="button" data-' + name + '="' + esc(o[0]) + '" aria-pressed="' + (o[0] === value ? "true" : "false") + '"' + (o[2] ? ' disabled title="' + esc(o[2]) + '"' : "") + ">" + esc(o[1]) + "</button>";
    }).join("") + "</div></div>";
  }

  function evidenceHtml(doc) {
    var e = evidence(doc), h = '<h2 id="evidence-title">Evidence status</h2><p><b class="cb-state cb-state-' + e.state.toLowerCase() + '">' + esc(e.state) + "</b> " + esc(e.headline) + "</p>";
    if (e.checks.length) {
      h += '<details' + (e.state === "RECONCILED" ? "" : " open") + '><summary data-jh-title="The checks">The ' + e.checks.length + ' checks</summary><ul class="cb-checks">' + e.checks.map(function (c) {
        return "<li><span class=\"cb-mark\">" + (c.ok === true ? "ok" : c.ok === false ? "MISMATCH" : "unknown") + "</span> " + esc(c.label) + " — " + esc(c.detail) + "</li>";
      }).join("") + "</ul></details>";
    }
    (e.degraded || []).forEach(function (d) { h += '<p class="cb-warn">Degraded: ' + esc(d) + "</p>"; });
    e.notes.forEach(function (n) { h += '<p class="cb-note">' + esc(n) + "</p>"; });
    h += '<p class="cb-note">This page reconciles the document with itself. It does not verify that the bond, CDS and price sources are fresh; the dates below are the engine\'s own.</p>';
    return h;
  }

  function heroHtml(doc, state) {
    if (!isV2(doc)) return "";
    var cov = isObj(doc.coverage) ? doc.coverage : {}, names = doc.names || [];
    var down = names.filter(function (x) { return x.signal === "CREDIT_LEADS_DOWN"; }).length, up = names.filter(function (x) { return x.signal === "CREDIT_LEADS_UP"; }).length;
    var view = industryView(doc, { win: state.win, book: state.book, measure: "bp" }).filter(function (r) { return isNum(r.d_bp); });
    var wide = view.filter(function (r) { return r.d_bp > 0; }).length;
    var w = windowList(doc).filter(function (x) { return x.key === state.win; })[0];
    function tile(label, big, sub) { return '<div class="cb-tile-h"><div class="cb-lbl">' + esc(label) + '</div><div class="cb-big">' + big + '</div><div class="cb-sub">' + sub + "</div></div>"; }
    return '<div class="cb-hero">' +
      tile("Companies with bonds read", esc(count(cov.companies)), esc(count(cov.listed)) + " with a listed stock · " + esc(count(cov.unlisted)) + " credit only") +
      tile("Bonds with a spread", esc(count(cov.bonds_with_spread)), esc(money(cov.value_held)) + " held by the scanned funds") +
      tile("Leads", esc(String(down + up)), esc(String(down)) + " credit worse, stock not down · " + esc(String(up)) + " credit better, stock not up") +
      tile("Industries wider, " + (w ? w.short : ""), view.length ? esc(wide + " of " + view.length) : "Unavailable", w && w.ok ? "since " + esc(day(w.ref)) : "no reference day yet") +
      tile("Traded CDS levels", esc(count(cov.cds_priced)), esc(count(cov.cds_priced_tied)) + " tied to a listed stock · " + esc(count(cov.cds_no_level)) + " trade with no level") +
      "</div>" +
      '<p class="cb-dates">Bond holdings of <b>' + esc(day(doc.as_of)) + "</b> · Treasury curve of " + esc(day((doc.curve || {}).as_of)) + " · stocks to " + esc(day(doc.stock_as_of)) +
      " · CDS of " + esc(day((doc.cds || {}).as_of)) + " · published " + esc(stamp(doc.generated_at)) + "</p>";
  }

  function controlsHtml(doc, state) {
    var wins = windowList(doc).map(function (w) { return [w.key, w.short, w.ok ? "" : (w.why || "Not available yet")]; });
    var books = [["all", "All"], ["ig", "Investment grade"], ["hy", "High yield"], ["em", "Emerging"]];
    return seg("win", wins, state.win, "Since") + seg("book", books, state.book, "Book") + seg("measure", MEASURES, state.measure, "Colour by") +
      seg("palette", [["rg", "Red / green"], ["cb", "Orange / blue"]], state.palette, "Colours");
  }

  function measureText(r, measure) {
    return measure === "level" ? bp(r.v) + " bp" : measure === "pct" ? pct(r.v, true) : bp(r.v, true) + " bp";
  }

  function mapHtml(doc, state, width) {
    var view = industryView(doc, state), scale = scaleOf(view, state.measure), W = Math.max(280, Math.floor(width)), H = W < 560 ? 520 : 440;
    var nodes = view.filter(function (r) { return r.value > 0; }).map(function (r) { return { v: r.value, r: r }; });
    if (!nodes.length) return { html: '<p class="cb-empty">No industry has a spread for this book.</p>', view: view, scale: scale };
    var rects = squarify(nodes, 0, 0, W, H);
    var html = '<div class="cb-map" style="height:' + H + 'px">' + rects.map(function (b) {
      var r = b.node.r, big = b.w > 110 && b.h > 58, mid = b.w > 46 && b.h > 30, small = b.w < 110 || b.h < 50, c = colour(r.v, scale, state.measure, state.palette);
      var tip = r.industry + ": " + (isNum(r.v) ? measureText(r, state.measure) : "no reading") + " · spread " + bp(r.spread_bp) + " bp · " + r.n + " companies · " + money(r.value) + " held";
      var bw = Math.max(b.w - 2, 1), bh = Math.max(b.h - 2, 1), bx = Math.max(Math.min(b.x, W - bw - 2), 0), by = Math.max(Math.min(b.y, H - bh - 2), 0);      // a sliver is a pixel wide and stays inside the map
      return '<button type="button" class="cb-box' + (small ? " cb-sm" : "") + (mid ? "" : " cb-tiny") + (state.industry === r.industry ? " cb-on" : "") + '" data-ind="' + esc(r.industry) + '" title="' + esc(tip) + '" style="left:' + bx.toFixed(1) + "px;top:" + by.toFixed(1) +
        "px;width:" + bw.toFixed(1) + "px;height:" + bh.toFixed(1) + "px;background:" + c + '">' +
        (mid ? '<span class="cb-box-n">' + esc(r.industry) + "</span>" : "") +
        (mid ? '<span class="cb-box-v">' + esc(isNum(r.v) ? measureText(r, state.measure) : "–") + "</span>" : "") +
        (big ? '<span class="cb-box-s">' + esc(bp(r.spread_bp)) + " bp · " + esc(String(r.n)) + " cos</span>" : "") + "</button>";
    }).join("") + "</div>";
    var p = PALETTES[state.palette] || PALETTES.rg;
    var ends = state.measure === "level" ? [bp(scale.lo) + " bp", "", bp(scale.hi) + " bp", "Tighter spread (safer)", "Wider spread (riskier)"] :
      [(state.measure === "pct" ? pct(-scale.hi, true) : bp(-scale.hi, true) + " bp"), "0", (state.measure === "pct" ? pct(scale.hi, true) : bp(scale.hi, true) + " bp"), "Tightening (safety)", "Widening (trouble)"];
    var grad = state.measure === "level" ? "linear-gradient(90deg," + mix(p.mid, p.bad, 0.1) + "," + p.bad + ")" : "linear-gradient(90deg," + p.good + "," + p.mid + "," + p.bad + ")";
    html += '<div class="cb-legend" data-jh-title="Legend"><span>' + esc(ends[3]) + '</span><span class="cb-legend-n">' + esc(ends[0]) + '</span><i style="background:' + grad + '"></i><span class="cb-legend-n">' + esc(ends[2]) +
      "</span><span>" + esc(ends[4]) + '</span><span class="cb-legend-size">Tile size: market value of the industry\'s bonds held by the scanned funds</span></div>';
    return { html: html, view: view, scale: scale };
  }

  function rankHtml(view, state, scale) {
    var have = view.filter(function (r) { return isNum(r.v); });
    if (!have.length) return "";
    var level = state.measure === "level";
    var top = sortRows(have, "v", "desc").slice(0, 6), bottom = sortRows(have, "v", "asc").slice(0, 6);
    function list(title, sub, rows) {
      return '<div class="cb-rank"><h3>' + esc(title) + '</h3><p class="cb-note">' + esc(sub) + "</p>" + rows.map(function (r, i) {
        var share = level ? clamp((r.v - scale.lo) / ((scale.hi - scale.lo) || 1), 0.02, 1) : clamp(Math.abs(r.v) / (scale.hi || 1), 0.02, 1);
        return '<button type="button" class="cb-rank-row" data-ind="' + esc(r.industry) + '"><span class="cb-rank-i">' + (i + 1) + '</span><span class="cb-rank-n">' + esc(r.industry) +
          '</span><span class="cb-rank-bar"><i style="width:' + (share * 100).toFixed(0) + "%;background:" + colour(r.v, scale, state.measure, state.palette) + '"></i></span><span class="cb-rank-v" style="color:' +
          (level ? "" : textColour(r.v, state.palette)) + '">' + esc(measureText(r, state.measure)) + '</span><span class="cb-rank-s">' + esc(bp(r.spread_bp)) + " bp · " + esc(String(r.n)) + "</span></button>";
      }).join("") + "</div>";
    }
    return list(level ? "Widest spreads — trouble" : "Widening most — trouble", level ? "The industries the bond market charges most." : "The industries whose spreads moved out the most.", top) +
      list(level ? "Tightest spreads — safety" : "Widening least or tightening — safety", level ? "The industries the bond market charges least." : "The industries whose spreads held or came in.", bottom);
  }

  function detailHtml(doc, state, view) {
    var r = view.filter(function (x) { return x.industry === state.industry; })[0];
    if (!r) return "";
    var rows = withWindow(filterRows(table(doc), { industry: r.industry, book: state.book }), state.win).filter(function (x) { return isNum(x.d); });
    var widest = sortRows(rows, "d", "desc").slice(0, 6), tightest = sortRows(rows, "d", "asc").slice(0, 6);
    function li(x) {
      return "<li><b>" + esc(x.ticker || "") + "</b> " + esc(x.name) + ' <span style="color:' + textColour(x.d, state.palette) + '">' + esc(bp(x.d, true)) + " bp</span> <span class=\"cb-dim\">to " + esc(bp(x.spread_bp)) + " bp</span></li>";
    }
    return '<div class="cb-detail" data-jh-title="Industry detail"><div class="cb-detail-h"><h3>' + esc(r.industry) + '</h3><button type="button" data-show-ind="' + esc(r.industry) +
      '">Show its ' + esc(String(r.n)) + ' companies in the table</button><button type="button" data-ind="">Close</button></div>' +
      '<p class="cb-note">Middle company\'s change ' + esc(bp(r.d_bp, true)) + " bp (" + esc(pct(r.d_pct, true)) + ") from " + esc(bp(r.from_bp)) + " bp · spread now " + esc(bp(r.spread_bp)) + " bp · " +
      esc(String(r.wider == null ? "–" : r.wider)) + " wider by 5 bp or more, " + esc(String(r.tighter == null ? "–" : r.tighter)) + " tighter · " + esc(String(r.n_listed)) + " of " + esc(String(r.n)) +
      " with a listed stock · " + esc(String(r.n_distressed || 0)) + " distressed (1,000 bp or more) · " + esc(money(r.value)) + ' held</p><div class="cb-detail-cols"><div><h4>Widened most</h4><ol>' +
      widest.map(li).join("") + "</ol></div><div><h4>Tightened most</h4><ol>" + tightest.map(li).join("") + "</ol></div></div></div>";
  }

  function leadsHtml(doc, state) {
    var names = (doc.names || []).filter(isObj), ref = (windowList(doc).filter(function (w) { return w.key === (doc.primary || "m1"); })[0] || {}).ref;
    function card(x) {
      var basis = x.signal_basis === "traded CDS";
      var credit = basis ? "CDS " + bp(x.cds_bp) + " bp, " + bp(x.cds_d1m, true) + " bp in a month" :
        "Spread " + bp(x.spread_bp) + " bp, " + bp(x.d_spread_bp, true) + " bp since " + day(ref) + " (" + bp(x.x_spread_bp, true) + " past its book; the bar was " + bp(x.bar_bp) + ")";
      var stock = "Stock " + pct(x.d_price_pct, true) + (x.equity_state === "opposite" ? " — moving the other way" : " — has not followed");
      var cds = !basis && isNum(x.cds_bp) ? " · CDS " + bp(x.cds_bp) + " bp" + (isNum(x.cds_d1m) ? " (" + bp(x.cds_d1m, true) + " in a month" + (x.cds_says ? ", " + x.cds_says : "") + ")" : "") : "";
      return '<div class="cb-lead"><div class="cb-lead-h"><b>' + esc(x.ticker) + "</b> " + esc(x.name) + ' <span class="cb-dim">' + esc(x.industry || "") + (x.book ? " · " + esc(BOOK_SHORT[x.book] || x.book) : "") +
        (isCount(x.n_matched) ? " · " + x.n_matched + (x.n_matched === 1 ? " bond" : " bonds") : "") + '</span></div><div class="cb-lead-b">' + esc(credit) + esc(cds) + '</div><div class="cb-lead-b">' + esc(stock) + "</div></div>";
    }
    function col(sig, title, sub) {
      var rows = names.filter(function (x) { return x.signal === sig; }), shown = state.allLeads ? rows : rows.slice(0, 8);
      return '<div class="cb-leadcol"><h3>' + esc(title) + " (" + rows.length + ')</h3><p class="cb-note">' + esc(sub) + "</p>" + (rows.length ? shown.map(card).join("") : '<p class="cb-empty">None on this run.</p>') +
        (rows.length > shown.length ? '<button type="button" class="cb-more" data-all-leads="1">Show all ' + rows.length + "</button>" : "") + "</div>";
    }
    return col("CREDIT_LEADS_DOWN", "Credit worse, stock has not fallen", "The bond market marked the company down past its book; the stock is not down 3% or more.") +
      col("CREDIT_LEADS_UP", "Credit better, stock has not risen", "The bond market marked the company up past its book; the stock is not up 3% or more.");
  }

  function cell(col, r, state) {
    var k = col[0], v = r[k];
    if (k === "name") return '<td class="l"><b>' + esc(r.ticker || "") + "</b>" + (r.ticker ? " " : "") + esc(r.name) + (r.ticker ? "" : ' <span class="cb-dim">no listed stock</span>') + (r.distressed ? ' <span class="cb-tag">distressed</span>' : "") + "</td>";
    if (k === "industry") return '<td class="l"><button type="button" class="cb-link" data-show-ind="' + esc(v || "") + '">' + esc(v || "–") + "</button></td>";
    if (k === "book") return '<td class="l">' + esc(BOOK_SHORT[v] || v || "–") + "</td>";
    if (k === "n_bonds") return '<td class="r">' + esc(isNum(v) ? String(v) : "–") + "</td>";
    if (k === "value") return '<td class="r">' + esc(money(v)) + "</td>";
    if (k === "spread_bp" || k === "cds_bp") return '<td class="r">' + esc(bp(v)) + "</td>";
    if (k === "d" || k === "x_m1" || k === "d_ytd" || k === "cds_d1m") return '<td class="r" style="color:' + textColour(v, state.palette) + '">' + esc(bp(v, true)) + "</td>";
    if (k === "pct_m1") return '<td class="r" style="color:' + textColour(v, state.palette) + '">' + esc(pct(v, true)) + "</td>";
    if (k === "px_m1") return '<td class="r" style="color:' + textColour(isNum(v) ? -v : null, state.palette) + '">' + esc(pct(v, true)) + "</td>";
    if (k === "direction") return '<td class="l" style="color:' + textColour(v === "DETERIORATING" ? 1 : v === "IMPROVING" ? -1 : null, state.palette) + '">' + esc(DIR_WORDS[v] || "–") + "</td>";
    if (k === "signal") return '<td class="l">' + (LEADS.indexOf(v) >= 0 ? '<b style="color:' + textColour(v === "CREDIT_LEADS_DOWN" ? 1 : -1, state.palette) + '">' + esc(SIGNAL_WORDS[v]) + "</b>" : esc(r.ticker ? (SIGNAL_WORDS[v] || "–") : "–")) + "</td>";
    return '<td class="l">' + esc(v == null ? "–" : String(v)) + "</td>";
  }

  function headHtml(cols, sort, attr, relabel) {
    return "<thead><tr>" + cols.map(function (c) {
      var on = sort.key === c[0], label = relabel && relabel[c[0]] ? relabel[c[0]] : c[1];
      return '<th class="' + c[2] + '" aria-sort="' + (on ? (sort.dir === "asc" ? "ascending" : "descending") : "none") + '"><button type="button" data-' + attr + '="' + esc(c[0]) + '">' + esc(label) +
        (on ? (sort.dir === "asc" ? " ▲" : " ▼") : "") + "</button></th>";
    }).join("") + "</tr></thead>";
  }

  function tableHtml(doc, state) {
    var all = withWindow(table(doc), state.win);
    var rows = filterRows(all, { q: state.q, industry: state.industry, book: state.book, listed: state.listed, signal: state.signal });
    var w = windowList(doc).filter(function (x) { return x.key === state.win; })[0] || {};
    var relabel = { d: "Δ " + (w.short || ""), px_m1: "Stock 1M" };
    var inds = industriesIn(all);
    var ctl = '<div class="cb-tctl" data-jh-title="Table controls">' +
      '<label>Search <input type="search" data-q value="' + esc(state.q) + '" placeholder="Company or ticker" autocomplete="off"></label>' +
      '<label>Industry <select data-industry><option value="">All industries</option>' + inds.map(function (i) { return '<option value="' + esc(i[0]) + '"' + (i[0] === state.industry ? " selected" : "") + ">" + esc(i[0]) + " (" + i[1] + ")</option>"; }).join("") + "</select></label>" +
      '<label>Sort by <select data-sortsel>' + COLUMNS.map(function (c) { return '<option value="' + esc(c[0]) + '"' + (c[0] === state.sort.key ? " selected" : "") + ">" + esc(relabel[c[0]] || c[1]) + "</option>"; }).join("") + "</select></label>" +
      '<button type="button" class="cb-dir" data-dir aria-label="Sort direction">' + (state.sort.dir === "asc" ? "Ascending ▲" : "Descending ▼") + "</button>" +
      '<label>Show <select data-signal>' + [["", "Every company"], ["leads", "Leads only"], ["cds", "With a traded CDS"], ["distressed", "Distressed"]].map(function (o) {
        return '<option value="' + o[0] + '"' + (o[0] === state.signal ? " selected" : "") + ">" + o[1] + "</option>"; }).join("") + "</select></label>" +
      '<label class="cb-chk"><input type="checkbox" data-listed' + (state.listed ? " checked" : "") + "> Listed stocks only</label>" +
      '<label class="cb-chk"><input type="checkbox" data-group' + (state.group ? " checked" : "") + "> Group by industry</label></div>";
    var body = "", shown = 0, total = rows.length;
    if (state.group) {
      var meta = {};
      industryView(doc, { win: state.win, book: state.book, measure: "bp" }).forEach(function (r) { meta[r.industry] = r; });
      var groups = {};
      rows.forEach(function (r) { (groups[r.industry || "Unclassified"] = groups[r.industry || "Unclassified"] || []).push(r); });
      var order = Object.keys(groups).map(function (k) { return { industry: k, d: (meta[k] || {}).d_bp, n: groups[k].length }; });
      order = state.sort.key === "industry" ? sortRows(order, "industry", state.sort.dir) : sortRows(order, "d", state.sort.dir);
      order.forEach(function (g) {
        var m = meta[g.industry] || {}, inner = sortRows(groups[g.industry], state.sort.key === "industry" ? "d" : state.sort.key, state.sort.key === "industry" ? "desc" : state.sort.dir);
        var cut = state.open[g.industry] ? inner : inner.slice(0, 8);
        body += '<tr class="cb-grp"><td colspan="' + COLUMNS.length + '"><b>' + esc(g.industry) + "</b> · " + g.n + " companies · middle change <span style=\"color:" + textColour(m.d_bp, state.palette) + '">' +
          esc(bp(m.d_bp, true)) + " bp</span> · spread " + esc(bp(m.spread_bp)) + " bp" + (inner.length > cut.length ? ' <button type="button" class="cb-link" data-open="' + esc(g.industry) + '">show all ' + inner.length + "</button>" : "") + "</td></tr>";
        cut.forEach(function (r) { body += "<tr>" + COLUMNS.map(function (c) { return cell(c, r, state); }).join("") + "</tr>"; shown++; });
      });
    } else {
      var sorted = sortRows(rows, state.sort.key, state.sort.dir), page = sorted.slice(0, state.limit);
      page.forEach(function (r) { body += "<tr>" + COLUMNS.map(function (c) { return cell(c, r, state); }).join("") + "</tr>"; });
      shown = page.length;
    }
    if (!total) body = '<tr><td colspan="' + COLUMNS.length + '" class="cb-empty">No company matches.</td></tr>';
    var foot = '<p class="cb-note" data-jh-title="Rows shown">' + shown.toLocaleString("en-US") + " of " + total.toLocaleString("en-US") + " companies shown" + (total < all.length ? " (" + all.length.toLocaleString("en-US") + " on the board)" : "") +
      (w.ok ? " · Δ is the spread change since " + esc(day(w.ref)) + ", in basis points; Past book, Δ % and Stock are for the month" : "") + ".</p>" +
      (!state.group && shown < total ? '<div class="cb-pager"><button type="button" data-more="200">Show 200 more</button><button type="button" data-more="all">Show all ' + total.toLocaleString("en-US") + "</button></div>" : "");
    return ctl + '<div class="cb-scroll" tabindex="0" role="region" aria-label="Every company; scroll sideways for all columns"><table class="cb-table" data-jh-nosort>' + headHtml(COLUMNS, state.sort, "sort", relabel) + "<tbody>" + body + "</tbody></table></div>" + foot;
  }

  function cdsHtml(doc, state) {
    var c = isObj(doc.cds) ? doc.cds : {}, priced = Array.isArray(c.priced) ? c.priced.filter(isObj) : [], bare = Array.isArray(c.no_level) ? c.no_level.filter(isObj) : [];
    var sectors = {};
    priced.forEach(function (x) { if (x.sector) sectors[x.sector] = 1; });
    var rows = priced.filter(function (x) { return (!state.cdsSector || x.sector === state.cdsSector) && (!state.cdsLiquid || x.liquid) && (!state.cdsListed || x.ticker); });
    rows = sortRows(rows, state.cdsSort.key, state.cdsSort.dir);
    var nRows = rows.length;
    if (!state.cdsAll) rows = rows.slice(0, 40);
    var ctl = '<div class="cb-tctl" data-jh-title="CDS controls"><label>Sector <select data-cds-sector><option value="">All sectors</option>' + Object.keys(sectors).sort().map(function (s) {
      return '<option value="' + esc(s) + '"' + (s === state.cdsSector ? " selected" : "") + ">" + esc(s) + "</option>"; }).join("") + "</select></label>" +
      '<label>Sort by <select data-cds-sortsel>' + CDS_COLUMNS.map(function (k) { return '<option value="' + k[0] + '"' + (k[0] === state.cdsSort.key ? " selected" : "") + ">" + esc(k[1]) + "</option>"; }).join("") + "</select></label>" +
      '<button type="button" class="cb-dir" data-cds-dir aria-label="CDS sort direction">' + (state.cdsSort.dir === "asc" ? "Ascending ▲" : "Descending ▼") + "</button>" +
      '<label class="cb-chk"><input type="checkbox" data-cds-liquid' + (state.cdsLiquid ? " checked" : "") + "> Liquid names only</label>" +
      '<label class="cb-chk"><input type="checkbox" data-cds-listed' + (state.cdsListed ? " checked" : "") + "> Listed stocks only</label></div>";
    var body = rows.map(function (x) {
      return "<tr>" + CDS_COLUMNS.map(function (k) {
        var v = x[k[0]];
        if (k[0] === "name") return '<td class="l">' + esc(x.name) + (x.liquid ? "" : ' <span class="cb-dim">thin</span>') + "</td>";
        if (k[0] === "ticker") return '<td class="l"><b>' + esc(v || "") + "</b>" + (v ? "" : '<span class="cb-dim">–</span>') + "</td>";
        if (k[0] === "spread_bp") return '<td class="r">' + (x.believable === false ? esc(isNum(x.points_upfront) ? fixed(x.points_upfront, 1) + " pts upfront" : "quoted in points") : esc(bp(v)) + " bp") + "</td>";
        if (k[0] === "d1d" || k[0] === "d1w" || k[0] === "d1m" || k[0] === "d3m") return '<td class="r" style="color:' + textColour(v, state.palette) + '">' + esc(bp(v, true)) + "</td>";
        if (k[0] === "rank_1y") return '<td class="r">' + esc(isNum(v) ? fixed(v, 0) + "%" : "–") + "</td>";
        if (k[0] === "px_1m") return '<td class="r" style="color:' + textColour(isNum(v) ? -v : null, state.palette) + '">' + esc(pct(v, true)) + "</td>";
        if (k[0] === "last_date") return '<td class="l">' + esc(typeof v === "string" ? v : "–") + "</td>";
        if (k[0] === "trades_30d") return '<td class="r">' + esc(isNum(v) ? String(v) : "–") + "</td>";
        return '<td class="l">' + esc(v == null ? "–" : String(v)) + "</td>";
      }).join("") + "</tr>";
    }).join("") || '<tr><td colspan="' + CDS_COLUMNS.length + '" class="cb-empty">No reference entity matches.</td></tr>';
    var note = (nRows > rows.length ? '<div class="cb-pager"><button type="button" data-cds-all="1">Show all ' + nRows + "</button></div>" : "") +
      '<p class="cb-note">' + rows.length + " of " + priced.length + " reference entities with a level shown. Levels and changes are the CDS desk's, from DTCC's public prints, in basis points a year; 1Y rank is where today sits in the year's range. " +
      "A level past 5,000 bp is a points-upfront quote and is shown in points. CDS of " + esc(day(c.as_of)) + ".</p>";
    var more = bare.length ? '<details class="cb-bare"><summary data-jh-title="CDS without a level">' + bare.length + " more names trade with no level the desk can print</summary><p class=\"cb-note\">" + bare.map(function (x) {
      return esc(x.name) + (x.ticker ? " (" + esc(x.ticker) + ")" : "") + (x.status === "dormant" ? " – dormant" : ""); }).join(" · ") + "</p></details>" : "";
    return ctl + '<div class="cb-scroll" tabindex="0" role="region" aria-label="Company CDS; scroll sideways for all columns"><table class="cb-table" data-jh-nosort>' + headHtml(CDS_COLUMNS, state.cdsSort, "cds-sort") + "<tbody>" + body + "</tbody></table></div>" + note + more;
  }

  /* The first engine's board, as it was: the structural model's 26 names, each day against the observation before it. */
  function previousHtml(doc) {
    var names = (isObj(doc) && Array.isArray(doc.names) ? doc.names : []).filter(function (x) { return isObj(x) && (isNum(x.distance_to_default) || isNum(x.synthetic_cds_bp)); });
    var v2 = isV2(doc);
    function num(v, d) { return isNum(v) ? minus(fixed(v, d)) : "Unavailable"; }
    var body = names.map(function (x) {
      var px = v2 ? x.model_d_price_pct : x.d_price_pct, dir = v2 ? x.model_credit_direction : x.credit_direction, sig = v2 ? x.model_signal : x.signal;
      return "<tr><td class=\"l\"><b>" + esc(x.ticker) + '</b></td><td class="l">' + esc(x.name || "Unavailable") + '</td><td class="r">' + esc(num(x.distance_to_default, 2)) + '</td><td class="r">' + esc(num(x.d_distance_to_default, 3)) +
        '</td><td class="r">' + esc(num(x.synthetic_cds_bp, 1)) + '</td><td class="r">' + esc(num(x.d_synthetic_cds_bp, 1)) + '</td><td class="r">' + esc(isNum(px) ? pct(px, true) : "Unavailable") + '</td><td class="l">' +
        esc(typeof dir === "string" ? dir : "–") + '</td><td class="l">' + esc(typeof sig === "string" ? sig : "Unavailable") + "</td></tr>";
    }).join("") || '<tr><td colspan="9" class="cb-empty">The model rows are not in this document.</td></tr>';
    return '<p class="cb-note">The board as it was before it was widened: ' + names.length + " hand-listed issuers, credit from a structural default model (distance to default and a synthetic CDS computed from each company's own stock and debt), " +
      'each day compared with the observation before it. A model, not a market price.</p><div class="cb-scroll" tabindex="0" role="region" aria-label="The previous board"><table class="cb-table"><thead><tr><th class="l">Ticker</th><th class="l">Name</th>' +
      '<th class="r">DD</th><th class="r">ΔDD</th><th class="r">Model CDS bp</th><th class="r">ΔCDS</th><th class="r">ΔPrice</th><th class="l">Credit</th><th class="l">Signal</th></tr></thead><tbody>' + body + "</tbody></table></div>";
  }

  function methodHtml(doc) {
    var rules = isObj(doc.rules) ? doc.rules : {}, order = ["spread", "company", "change", "industry", "credit_moved", "stock_has_not_followed", "cds", "left_out"];
    var words = { spread: "Spread", company: "Company", change: "Change", industry: "Industry", credit_moved: "Credit moved", stock_has_not_followed: "Stock has not followed", cds: "Traded CDS", left_out: "Left out" };
    var h = "<p>" + esc(typeof doc.method === "string" ? doc.method : "Method unavailable.") + "</p><dl class=\"cb-rules\">" + order.filter(function (k) { return typeof rules[k] === "string"; }).map(function (k) {
      return "<dt>" + esc(words[k]) + "</dt><dd>" + esc(rules[k]) + "</dd>"; }).join("") + "</dl>";
    var wins = windowList(doc);
    h += '<p class="cb-note">Reference days: ' + wins.map(function (w) { return esc(w.short) + " – " + (w.ok ? esc(day(w.ref)) + " (" + w.days + " days; " + esc(w.source || "") + ")" : "not available yet"); }).join(" · ") + "</p>";
    return h;
  }

  function coverageHtml(doc) {
    var cov = isObj(doc.coverage) ? doc.coverage : {}, m = isObj(doc.match) ? doc.match : {}, left = isObj(cov.bonds_left_out) ? cov.bonds_left_out : {}, ties = isObj(cov.ties) ? cov.ties : {};
    var h = "<p>" + esc(count(cov.bonds_read)) + " US-dollar corporate bonds read, " + esc(count(cov.bonds_with_spread)) + " with a spread. Left out: " + Object.keys(left).map(function (k) { return esc(k) + " " + esc(count(left[k])); }).join("; ") + ".</p>" +
      "<p>" + esc(count(cov.companies)) + " companies; " + esc(count(cov.listed)) + " tied to a listed stock, holding " + esc(isNum(cov.value_tied_pct) ? fixed(cov.value_tied_pct, 1) + "%" : "Unavailable") +
      " of the market value read. How issuer names were tied: " + Object.keys(ties).map(function (k) { return esc(k) + " " + esc(count(ties[k])); }).join(", ") + ".</p>";
    var rule = Array.isArray(m.by_rule) ? m.by_rule : [], un = Array.isArray(m.largest_untied) ? m.largest_untied : [];
    if (rule.length) h += '<details><summary data-jh-title="Ties made by a rule">' + rule.length + ' ties made by a naming rule (each one is listed)</summary><p class="cb-note">' + rule.map(function (t) {
      return esc(t.issuer) + " → " + esc(t.ticker) + " " + esc(t.company) + " (" + esc(t.how) + ")"; }).join(" · ") + "</p></details>";
    if (un.length) h += '<details><summary data-jh-title="Largest issuers with no listed stock">The ' + un.length + ' largest issuers with no listed stock tied</summary><p class="cb-note">' + un.map(function (t) {
      return esc(t.issuer) + " " + esc(money(t.value)); }).join(" · ") + "</p></details>";
    return h;
  }

  // ------------------------------------------------------------------------------------------------ the page
  function load(store) {
    try { var s = JSON.parse(store.getItem(STORE) || "{}"); return isObj(s) ? s : {}; } catch (e) { return {}; }
  }
  function save(store, state) {
    try { store.setItem(STORE, JSON.stringify({ win: state.win, book: state.book, measure: state.measure, palette: state.palette })); } catch (e) { /* a private window: the choice lasts the visit */ }
  }

  function firstState(doc, kept) {
    var wins = windowList(doc), ok = function (k) { return wins.some(function (w) { return w.key === k && w.ok; }); };
    var win = ok(kept.win) ? kept.win : ok(doc.primary) ? doc.primary : (wins.filter(function (w) { return w.ok; })[0] || {}).key || "m1";
    return { win: win, book: ["all", "ig", "hy", "em"].indexOf(kept.book) >= 0 ? kept.book : "all", measure: MEASURES.some(function (m) { return m[0] === kept.measure; }) ? kept.measure : "bp",
             palette: PALETTES[kept.palette] ? kept.palette : "rg", industry: "", q: "", listed: false, signal: "", group: false, open: {}, limit: 100, sort: { key: "d", dir: "desc" },
             cdsSort: { key: "d1m", dir: "desc" }, cdsSector: "", cdsLiquid: false, cdsListed: false, cdsAll: false, allLeads: false, view: "new" };
  }

  function mount(host, doc, opt) {
    opt = opt || {};
    var D = host.ownerDocument, store = opt.store || (function () { try { return root.localStorage; } catch (e) { return null; } })() || { getItem: function () { return null; }, setItem: function () {} };
    function $(id) { return host.querySelector("#" + id); }
    var v2 = isV2(doc), state = firstState(doc || {}, load(store));
    if (!v2) state.view = "previous";
    $("evidence").innerHTML = evidenceHtml(doc);

    function drawMap() {
      var box = $("map"), m = mapHtml(doc, state, box.clientWidth || host.clientWidth || 900);
      box.innerHTML = m.html;
      $("rank").innerHTML = rankHtml(m.view, state, m.scale);
      $("mapdetail").innerHTML = detailHtml(doc, state, m.view);
    }
    function draw(what) {
      var toggle = $("viewtoggle");
      toggle.textContent = state.view === "new" ? "Previous view" : "Back to the full board";
      toggle.setAttribute("aria-pressed", state.view === "previous" ? "true" : "false");
      toggle.hidden = !v2;
      $("newview").hidden = state.view !== "new";
      $("prevview").hidden = state.view !== "previous";
      if (state.view === "previous") { $("prev").innerHTML = previousHtml(doc); return; }
      if (!what || what === "all") {
        $("hero").innerHTML = heroHtml(doc, state);
        $("controls").innerHTML = controlsHtml(doc, state);
        $("leads").innerHTML = leadsHtml(doc, state);
        $("cds").innerHTML = cdsHtml(doc, state);
        $("method").innerHTML = methodHtml(doc);
        $("coverage").innerHTML = coverageHtml(doc);
        $("gaps").innerHTML = (Array.isArray(doc.gaps) ? doc.gaps : []).filter(function (g) { return typeof g === "string"; }).map(function (g) { return "<p>" + esc(g) + "</p>"; }).join("") || "<p>None declared.</p>";
      }
      if (!what || what === "all" || what === "map") drawMap();
      if (!what || what === "all" || what === "map" || what === "table") $("table").innerHTML = tableHtml(doc, state);
      if (what === "cds") $("cds").innerHTML = cdsHtml(doc, state);
      if (what === "leads") $("leads").innerHTML = leadsHtml(doc, state);
    }

    function flip(sort, key) {
      if (sort.key === key) sort.dir = sort.dir === "asc" ? "desc" : "asc";
      else { sort.key = key; sort.dir = (key === "name" || key === "industry" || key === "sector" || key === "ticker" || key === "region") ? "asc" : "desc"; }
    }
    host.addEventListener("click", function (ev) {
      var t = ev.target.closest ? ev.target.closest("button") : null;
      if (!t || !host.contains(t)) return;
      var d = t.dataset;
      if (t.id === "viewtoggle") { state.view = state.view === "new" ? "previous" : "new"; draw("all"); return; }
      if (d.win) { state.win = d.win; save(store, state); draw("all"); return; }
      if (d.book) { state.book = d.book; state.industry = ""; save(store, state); draw("all"); return; }
      if (d.measure) { state.measure = d.measure; save(store, state); draw("all"); return; }
      if (d.palette) { state.palette = d.palette; save(store, state); draw("all"); return; }
      if ("showInd" in d) { state.industry = d.showInd; state.group = false; state.limit = 100; draw("map"); var tb = $("table"); if (tb && tb.scrollIntoView) tb.scrollIntoView({ block: "start" }); return; }
      if ("ind" in d) { state.industry = state.industry === d.ind ? "" : d.ind; draw("map"); return; }
      if (d.sort) { flip(state.sort, d.sort); draw("table"); return; }
      if ("dir" in d) { state.sort.dir = state.sort.dir === "asc" ? "desc" : "asc"; draw("table"); return; }
      if (d.more) { state.limit = d.more === "all" ? 1e9 : state.limit + 200; draw("table"); return; }
      if (d.open) { state.open[d.open] = true; draw("table"); return; }
      if (d.cdsSort) { flip(state.cdsSort, d.cdsSort); draw("cds"); return; }
      if ("cdsDir" in d) { state.cdsSort.dir = state.cdsSort.dir === "asc" ? "desc" : "asc"; draw("cds"); return; }
      if (d.allLeads) { state.allLeads = true; draw("leads"); return; }
      if (d.cdsAll) { state.cdsAll = true; draw("cds"); return; }
    });
    host.addEventListener("change", function (ev) {
      var t = ev.target, d = t.dataset || {};
      if ("industry" in d) { state.industry = t.value; state.limit = 100; draw("map"); }
      else if ("sortsel" in d) { state.sort.key = t.value; draw("table"); }
      else if ("signal" in d) { state.signal = t.value; state.limit = 100; draw("table"); }
      else if ("listed" in d) { state.listed = t.checked; draw("table"); }
      else if ("group" in d) { state.group = t.checked; state.open = {}; draw("table"); }
      else if ("cdsSector" in d) { state.cdsSector = t.value; draw("cds"); }
      else if ("cdsSortsel" in d) { state.cdsSort.key = t.value; draw("cds"); }
      else if ("cdsLiquid" in d) { state.cdsLiquid = t.checked; draw("cds"); }
      else if ("cdsListed" in d) { state.cdsListed = t.checked; draw("cds"); }
    });
    var typing = null;
    host.addEventListener("input", function (ev) {
      var t = ev.target;
      if (!t.dataset || !("q" in t.dataset)) return;
      state.q = t.value;
      clearTimeout(typing);
      typing = setTimeout(function () {
        var at = t.selectionStart;
        state.limit = 100;
        draw("table");
        var again = host.querySelector("input[data-q]");
        if (again) { again.focus(); try { again.setSelectionRange(at, at); } catch (e) { /* not every input type keeps a caret */ } }
      }, 180);
    });
    var sizing = null, last = 0;
    if (root.addEventListener) root.addEventListener("resize", function () {
      clearTimeout(sizing);
      sizing = setTimeout(function () { var w = $("map") ? $("map").clientWidth : 0; if (state.view === "new" && v2 && w && Math.abs(w - last) > 8) { last = w; drawMap(); } }, 150);
    });
    draw("all");
    last = $("map") ? $("map").clientWidth : 0;
    var foot = $("foot");
    if (foot) foot.textContent = "Feed data/credit-before-equity.json · " + (typeof (doc || {}).attribution === "string" ? doc.attribution : "Attribution unavailable") + " · v" + (typeof (doc || {}).version === "string" ? doc.version : "?");
    return { state: state, draw: draw };
  }

  function start(host) {
    var D = host.ownerDocument;
    function fail(why) {
      host.querySelector("#evidence").innerHTML = '<h2 id="evidence-title">Evidence status</h2><p><b class="cb-state cb-state-unavailable">UNAVAILABLE</b> The published document could not be loaded (' + esc(why) +
        "). Nothing is shown rather than something stale.</p>";
      host.querySelector("#newview").hidden = true;
    }
    return root.fetch(DATA_URL + "?t=" + Date.now()).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (doc) { return mount(host, doc); }, function (e) { fail(String(e && e.message || e)); });
  }

  root.JHCreditBoard = { PALETTES: PALETTES, WINDOWS: WINDOWS, COLUMNS: COLUMNS, CDS_COLUMNS: CDS_COLUMNS, esc: esc, bp: bp, pct: pct, money: money, day: day, fixed: fixed, isV2: isV2, table: table,
    windowList: windowList, evidence: evidence, weightedMedian: weightedMedian, marketChange: marketChange, industryView: industryView, scaleOf: scaleOf, colour: colour, textColour: textColour, mix: mix,
    squarify: squarify, sortRows: sortRows, filterRows: filterRows, withWindow: withWindow, industriesIn: industriesIn, evidenceHtml: evidenceHtml, heroHtml: heroHtml, controlsHtml: controlsHtml,
    mapHtml: mapHtml, rankHtml: rankHtml, detailHtml: detailHtml, leadsHtml: leadsHtml, tableHtml: tableHtml, cdsHtml: cdsHtml, previousHtml: previousHtml, methodHtml: methodHtml,
    coverageHtml: coverageHtml, firstState: firstState, mount: mount, start: start };
})(typeof window !== "undefined" ? window : globalThis);
