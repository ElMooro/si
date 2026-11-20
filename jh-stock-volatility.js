/* jh-stock-volatility.js - the stock volatility desk's client (Khalid 2026-11-19: "create an engine that would measure
 * volatility. Most and least volatile stocks. Also it would measure the price spreads for all stocks and rank them based
 * on that. Price spread last day, week, month, quarter and create an icon for it in chart.html").
 *
 * Reads data/stock-volatility.json (justhodl-stock-volatility, daily 04:06 UTC, a reader of the warehouse). Nothing is
 * computed from a provider here and nothing is estimated: every figure is the artifact's, a missing one is a dash. The
 * only arithmetic is a difference of two of its figures (volatility now minus 21 sessions ago, high minus low) and the
 * position of a row in the list on screen.
 *
 * The first half is pure (unpacking, formatters, the comparator, the column registry, filters, the chart's geometry,
 * CSV) and runs under node for the tests; the second half paints the DOM. */
(function (w) {
  "use strict";
  if (w.JHStockVol) return;
  var KEY = "data/stock-volatility.json", CONTRACT = "stock-volatility.v1", RENDER_CAP = 200, REFRESH_MS = 15 * 60 * 1000;
  var MINUS = "−", DASH = "—", TIMES = "×", MID = "·";
  var WINDOWS = [["d", "Day", "the last session", 1], ["w", "Week", "the last 5 sessions", 5], ["m", "Month", "the last 21 sessions", 21], ["q", "Quarter", "the last 63 sessions", 63]];
  var MEASURE = { hv21: "hv21", hv63: "hv63", sp_d: "sp_d", sp_w: "sp_w", sp_m: "sp_m", sp_q: "sp_q" };
  var WHY = { pinned: ["Pinned", "amber", "the last 21 sessions fit inside 3.5% from low to high: the tape of a company waiting for a takeover to close. Measured, not ranked."],
    thin: ["Thin", "mute", "missed more than 3 of the last 63 sessions: its windows cover more days than they say. Measured, not ranked."],
    jump: ["Jump", "dn", "a one-session move beyond 3-to-1 that no split explains, inside the last 63 sessions. Measured, not ranked."],
    print: ["Print", "dn", "a high or a low more than 3-to-1 away from every close around it, inside the last 63 sessions: one print no trade beside it supports. Measured, not ranked."] };

  // ------------------------------------------------------------------------------------------------------- basics
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return ({ "&": "&" + "amp;", "<": "&" + "lt;", ">": "&" + "gt;", '"': "&" + "quot;", "'": "&#39;" })[c];
    });
  }
  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function fnum(v, d) {
    if (!isNum(v)) return DASH;
    var s = Math.abs(v).toFixed(d == null ? 2 : d);
    return (v < 0 && Number(s) !== 0 ? MINUS : "") + s;
  }
  function fint(v) { return isNum(v) ? String(Math.round(v)).replace(/\B(?=(\d{3})+(?!\d))/g, ",") : DASH; }
  /* a level in percent: no sign */
  function flev(v, d) { return isNum(v) ? v.toFixed(d == null ? 1 : d) + "%" : DASH; }
  /* a change in percent or in points: signed */
  function fpct(v, d, unit) {
    if (!isNum(v)) return DASH;
    var s = Math.abs(v).toFixed(d == null ? 1 : d);
    return (Number(s) === 0 ? "" : v > 0 ? "+" : MINUS) + s + (unit == null ? "%" : unit);
  }
  function fusd(v) {
    if (!isNum(v)) return DASH;
    var a = Math.abs(v), s = a >= 1e12 ? (a / 1e12).toFixed(2) + "T" : a >= 1e9 ? (a / 1e9).toFixed(a >= 1e11 ? 0 : 1) + "B" : a >= 1e6 ? (a / 1e6).toFixed(a >= 1e8 ? 0 : 1) + "M" : a >= 1e3 ? (a / 1e3).toFixed(0) + "K" : a.toFixed(0);
    return (v < 0 ? MINUS : "") + "$" + s;
  }
  function fpx(v) { return isNum(v) ? (Math.abs(v) >= 1000 ? v.toFixed(0) : Math.abs(v) >= 1 ? v.toFixed(2) : v.toFixed(4)) : DASH; }
  /* a dollar distance between two prices: cents, and finer only under a cent */
  function fgap(v) { return isNum(v) ? "$" + (Math.abs(v) >= 1000 ? v.toFixed(0) : Math.abs(v) >= 0.01 ? v.toFixed(2) : v.toFixed(4)) : DASH; }
  /* a size floor: "$1B", "$300M" */
  function ffloor(v) { return isNum(v) ? "$" + (v >= 1e9 ? +(v / 1e9).toFixed(1) + "B" : +(v / 1e6).toFixed(0) + "M") : DASH; }
  function tone(v) { return !isNum(v) || Math.abs(v) < 1e-9 ? "flat" : v > 0 ? "up" : "dn"; }

  // ---------------------------------------------------------------------------------------------------- the feed
  /* The artifact holds its table as columns + arrays; a row here is an object with the same keys. */
  function unpack(doc) {
    var cols = (doc && doc.cols) || [], out = [];
    ((doc && doc.rows) || []).forEach(function (a) {
      var r = {};
      for (var i = 0; i < cols.length; i++) r[cols[i]] = a[i] == null ? null : a[i];
      r._aside = r.aside ? String(r.aside).split(",") : [];
      out.push(r);
    });
    return out;
  }
  function index(rows) { var by = {}; rows.forEach(function (r) { by[r.t] = r; }); return by; }
  function isAside(r, measure) { return !!measure && r._aside.indexOf(measure) >= 0; }
  function hvd(r) { return isNum(r.hv21) && isNum(r.hv21p) ? r.hv21 - r.hv21p : null; }

  // --------------------------------------------------------------------------------------------------- comparator
  function blank(v) { return v == null || v === "" || (typeof v === "number" && !isFinite(v)); }
  /* dir -1 = largest first, +1 = smallest first. A blank is last either way: it is "no reading", not a small one. */
  function compare(a, b, dir) {
    var an = blank(a), bn = blank(b);
    if (an || bn) return an && bn ? 0 : an ? 1 : -1;
    var c;
    if (typeof a === "number" && typeof b === "number") c = a < b ? -1 : a > b ? 1 : 0;
    else c = String(a).localeCompare(String(b), "en", { numeric: true, sensitivity: "base" });
    return dir < 0 ? -c : c;
  }
  function sortRows(rows, col, dir) {
    if (!col) return rows.slice();
    return rows.map(function (r, i) { return [col.get(r), i, r]; })
      .sort(function (x, y) { return compare(x[0], y[0], dir) || x[1] - y[1]; })
      .map(function (x) { return x[2]; });
  }
  /* A header click: the same column flips; a new column starts largest-first for numbers and A-Z for text. */
  function nextSort(cur, col) {
    if (cur && cur.k === col.id) return { k: col.id, d: -cur.d };
    return { k: col.id, d: col.type === "s" ? 1 : -1 };
  }

  // ------------------------------------------------------------------------------------------------------ cells
  function td(html, cls, title) {
    if (html === DASH) cls = (cls ? cls + " " : "") + "na";
    return "<td" + (cls ? ' class="' + cls + '"' : "") + (title ? ' title="' + esc(title) + '"' : "") + ">" + html + "</td>";
  }
  function chip(label, cls, title) { return '<span class="sv-chip ' + (cls || "") + '"' + (title ? ' title="' + esc(title) + '"' : "") + ">" + esc(label) + "</span>"; }
  function tk(t) { return '<a class="tk" href="/chart.html?s=' + encodeURIComponent(t) + '" target="_top" data-t="' + esc(t) + '">' + esc(t) + "</a>"; }
  /* 0-100, where a reading stands in the stock's own year */
  function bar(v, title) {
    if (!isNum(v)) return DASH;
    return '<span class="sv-bar" title="' + esc(title || "") + '"><i style="left:' + Math.max(0, Math.min(100, v)).toFixed(1) + '%"></i></span>' + v.toFixed(0);
  }
  /* the window's low and high with the last close marked between them */
  function range(r, k) {
    var lo = r["lo_" + k], hi = r["hi_" + k], pos = r["pos_" + k];
    if (!isNum(lo) || !isNum(hi)) return DASH;
    return '<span class="sv-rng" title="low ' + fpx(lo) + ", high " + fpx(hi) + ", spread " + fgap(hi - lo) + (isNum(pos) ? ", the last close sits " + pos.toFixed(0) + "% of the way up" : "") + '">' +
      '<em>' + fpx(lo) + '</em><span class="sv-track">' + (isNum(pos) ? '<i style="left:' + Math.max(0, Math.min(100, pos)).toFixed(1) + '%"></i>' : "") + "</span><em>" + fpx(hi) + "</em></span>";
  }
  function flag(r) {
    var out = [];
    if (r.why && WHY[r.why]) {
      var label = WHY[r.why][0] + (r.why === "thin" && isNum(r.miss) ? " " + r.miss + " missed" : "");
      out.push(chip(label, WHY[r.why][1], WHY[r.why][2]));
    }
    if (r.jd) out.push(chip("jump " + r.jd + " " + TIMES + fnum(r.jr, 2), r.why === "jump" ? "dn" : "mute", "a one-session close-to-close move beyond 3-to-1 that no split explains, " + r.ja + " sessions ago" + (r.why === "jump" ? "" : "; older than every ranked window, so the row is ranked")));
    if (r.pd) out.push(chip((r.pr >= 1 ? "high " : "low ") + r.pd + " " + TIMES + fnum(r.pr, r.pr >= 0.01 ? 2 : 4), r.why === "print" ? "dn" : "mute", "that session's " + (r.pr >= 1 ? "high" : "low") + " stood " + TIMES + fnum(r.pr, r.pr >= 0.01 ? 2 : 4) +
      " from the nearest close around it, " + r.pa + " sessions ago" + (r.why === "print" ? "" : "; older than every ranked window, so the row is ranked")));
    return out.length ? out.join("") : DASH;
  }
  function flagText(r) { return [r.why || "", r.jd ? "jump " + r.jd + " x" + r.jr : "", r.pd ? "print " + r.pd + " x" + r.pr : ""].filter(Boolean).join("; ") || null; }

  // -------------------------------------------------------------------------------------------- the column registry
  var COLS = {};
  function col(id, t, tip, type, get, cell, measure) { return { id: id, t: t, tip: tip, type: type, get: get, cell: cell, measure: measure || null }; }
  function level(id, t, tip, measure, d) {
    return col(id, t, tip, "n", function (r) { return r[id]; }, function (r) { return td(flev(r[id], d), isAside(r, measure) ? "aside" : "", isAside(r, measure) ? "measured, not ranked: " + (r.why || "") : ""); }, measure);
  }
  [
    col("t", "Ticker", "opens the symbol in the chart", "s", function (r) { return r.t; }, function (r) { return td(tk(r.t) + (r.name ? '<span class="nm2">' + esc(r.name) + "</span>" : ""), "l"); }),
    col("name", "Name", "", "s", function (r) { return r.name; }, function (r) { return td(r.name ? '<span class="nm">' + esc(r.name) + "</span>" : DASH, "l"); }),
    col("sector", "Sector", "", "s", function (r) { return r.sector; }, function (r) { return td(r.sector ? esc(r.sector) : DASH, "l"); }),
    col("industry", "Industry", "", "s", function (r) { return r.industry; }, function (r) { return td(r.industry ? '<span class="nm wide">' + esc(r.industry) + "</span>" : DASH, "l"); }),
    col("mcap", "Mkt cap", "market value, the factor book's", "n", function (r) { return r.mcap; }, function (r) { return td(fusd(r.mcap)); }),
    col("adv", "$ vol 20d", "average dollar volume of the last 20 sessions", "n", function (r) { return r.adv; }, function (r) { return td(fusd(r.adv)); }),
    col("px", "Last", "the last close", "n", function (r) { return r.px; }, function (r) { return td(fpx(r.px)); }),
    col("chg", "1d %", "the last session's close-to-close change", "n", function (r) { return r.chg; }, function (r) { return td(fpct(r.chg, 2), tone(r.chg)); }),
    level("hv21", "Vol 21", "realized volatility of the last 21 sessions, close to close, percent a year", "hv21"),
    level("hv63", "Vol 63", "realized volatility of the last 63 sessions, percent a year", "hv63"),
    level("hv252", "Vol 252", "realized volatility of the last 252 sessions, percent a year", null),
    level("yz21", "YZ 21", "Yang-Zhang volatility of the last 21 sessions: overnight gap + open-to-close + intraday range, percent a year", "hv21"),
    level("atr", "ATR %", "the chart's ATR %: average true range of 14 sessions over the last close", null, 2),
    col("hvp", "In its year", "where today's Vol 21 stands among the stock's own last 252 readings, 0 to 100", "n", function (r) { return r.hvp; }, function (r) { return td(bar(r.hvp, "more volatile than on " + (isNum(r.hvp) ? r.hvp.toFixed(0) : "") + "% of the last 252 sessions"), "sv-barc"); }),
    col("hv21p", "21 ago", "Vol 21 as it stood 21 sessions ago", "n", function (r) { return r.hv21p; }, function (r) { return td(flev(r.hv21p)); }),
    col("hvd", "Change", "Vol 21 now minus Vol 21 twenty-one sessions ago, in points", "n", hvd, function (r) { var v = hvd(r); return td(fpct(v, 1, " pt"), isNum(v) ? (v > 0 ? "amber" : "") : ""); }, "hv21"),
    col("mx", "Largest move", "the largest one-session close-to-close move of the last 63 sessions and its date; sorts by its size", "n", function (r) { return isNum(r.mx) ? Math.abs(r.mx) : null; }, function (r) {
      return td(isNum(r.mx) ? '<span class="' + tone(r.mx) + '">' + fpct(r.mx, 1) + '</span> <span class="dim">' + esc(r.mxd || "") + "</span>" : DASH); }),
    col("flag", "Set aside", "why a row is measured but not ranked, and any unexplained jump or unsupported print in its bars", "s", function (r) { return flagText(r); }, function (r) { return td(flag(r), "l"); }),
    col("n", "Sessions", "sessions of the stock's own continuous history that were read", "n", function (r) { return r.n; }, function (r) { return td(fint(r.n)); }),
    col("rk_hv21", "# Vol 21", "rank by Vol 21 inside its kind, 1 = most volatile", "n", function (r) { return r.rk_hv21; }, function (r) { return td(fint(r.rk_hv21)); }),
    col("rk_hv63", "# Vol 63", "rank by Vol 63 inside its kind, 1 = most volatile", "n", function (r) { return r.rk_hv63; }, function (r) { return td(fint(r.rk_hv63)); })
  ].forEach(function (c) { COLS[c.id] = c; });
  WINDOWS.forEach(function (x) {
    var k = x[0], name = x[1], what = x[2];
    COLS["sp_" + k] = level("sp_" + k, name, "price spread of " + what + ": how far the highest high stood above the lowest low", "sp_" + k, 2);
    COLS["own_" + k] = col("own_" + k, name + " in its year", "where that spread stands among the stock's own last 252 windows of the same length, 0 to 100", "n", function (r) { return r["own_" + k]; },
      function (r) { return td(bar(r["own_" + k], "wider than " + (isNum(r["own_" + k]) ? r["own_" + k].toFixed(0) : "") + "% of its own last 252 " + name.toLowerCase() + " windows"), "sv-barc"); });
    COLS["rng_" + k] = col("rng_" + k, name + ": low " + MID + " close " + MID + " high", "the lowest low and highest high of " + what + ", the last close marked between them; sorts by where the close sits", "n",
      function (r) { return r["pos_" + k]; }, function (r) { return td(range(r, k), "sv-rngc"); });
    COLS["usd_" + k] = col("usd_" + k, name + " $", "high minus low of " + what + ", in dollars", "n", function (r) { return isNum(r["hi_" + k]) && isNum(r["lo_" + k]) ? r["hi_" + k] - r["lo_" + k] : null; },
      function (r) { var h = r["hi_" + k], l = r["lo_" + k]; return td(isNum(h) && isNum(l) ? fgap(h - l) : DASH); });
    COLS["rk_" + k] = col("rk_" + k, "# " + name, "rank by the " + name.toLowerCase() + "'s spread inside its kind, 1 = widest", "n", function (r) { return r["rk_" + k]; }, function (r) { return td(fint(r["rk_" + k])); });
    ["hi_", "lo_", "pos_"].forEach(function (p) { COLS[p + k] = col(p + k, p + k, "", "n", function (r) { return r[p + k]; }, function (r) { return td(fnum(r[p + k])); }); });
  });
  COLS.flag.csv = flagText;
  COLS.mx.csv = function (r) { return r.mx; };

  var VIEWS = [
    ["overview", "Overview", ["t", "name", "hv21", "hv63", "hvp", "sp_d", "sp_w", "sp_m", "sp_q", "mcap", "px", "chg", "flag"]],
    ["vol", "Volatility", ["t", "name", "hv21", "hv63", "hv252", "yz21", "atr", "hvp", "hv21p", "hvd", "mx", "sector", "mcap", "px", "flag"]],
    ["spread", "Price spread", ["t", "name", "sp_d", "own_d", "sp_w", "own_w", "sp_m", "own_m", "sp_q", "own_q", "px", "flag"]],
    ["range", "Low " + MID + " high", ["t", "px", "chg", "rng_d", "usd_d", "rng_w", "usd_w", "rng_m", "usd_m", "rng_q", "usd_q"]],
    ["ranks", "Ranks", ["t", "name", "rk_hv21", "rk_hv63", "rk_d", "rk_w", "rk_m", "rk_q", "sector", "industry", "mcap", "adv", "n", "flag"]]
  ];
  var CSV_COLS = ["t", "name", "sector", "industry", "mcap", "adv", "px", "chg", "hv21", "hv63", "hv252", "yz21", "atr", "hvp", "hv21p", "hvd", "mx",
    "sp_d", "lo_d", "hi_d", "pos_d", "own_d", "sp_w", "lo_w", "hi_w", "pos_w", "own_w", "sp_m", "lo_m", "hi_m", "pos_m", "own_m", "sp_q", "lo_q", "hi_q", "pos_q", "own_q",
    "rk_hv21", "rk_hv63", "rk_d", "rk_w", "rk_m", "rk_q", "n", "flag"];
  /* board id, chip label, the column it is ranked by, direction on screen, its columns */
  var VOLC = ["t", "name", "hv21", "hv63", "hv252", "hvp", "hv21p", "hvd", "mx", "sector", "mcap", "px", "chg"];
  function spreadCols(k) { return ["t", "name", "sp_" + k, "rng_" + k, "usd_" + k, "own_" + k, "hv21", "sector", "mcap", "px", "chg"]; }
  var BOARDS = [
    ["most_volatile", "Most volatile", "hv21", -1, VOLC], ["least_volatile", "Least volatile", "hv21", 1, VOLC],
    ["vol_rising", "Volatility rising", "hvd", -1, VOLC], ["vol_falling", "Volatility falling", "hvd", 1, VOLC],
    ["widest_d", "Widest day", "sp_d", -1, spreadCols("d")], ["widest_w", "Widest week", "sp_w", -1, spreadCols("w")], ["widest_m", "Widest month", "sp_m", -1, spreadCols("m")], ["widest_q", "Widest quarter", "sp_q", -1, spreadCols("q")],
    ["narrowest_d", "Narrowest day", "sp_d", 1, spreadCols("d")], ["narrowest_w", "Narrowest week", "sp_w", 1, spreadCols("w")], ["narrowest_m", "Narrowest month", "sp_m", 1, spreadCols("m")], ["narrowest_q", "Narrowest quarter", "sp_q", 1, spreadCols("q")],
    ["pinned", "Pinned to a deal price", "mcap", -1, ["t", "name", "mcap", "sp_m", "rng_m", "sp_q", "hv21", "hv252", "mx", "sector", "px"]]
  ];
  var SIZES = [["all", "All sizes", 0], ["300m", "$300M+", 3e8], ["2b", "$2B+", 2e9], ["10b", "$10B+", 1e10], ["200b", "$200B+", 2e11]];
  var SHOWS = [["free", "Trading freely"], ["pinned", "Pinned"], ["aside", "Jump, print or thin"], ["all", "All"]];

  function defs(ids) { return ids.map(function (id) { return COLS[id]; }).filter(Boolean); }
  function viewCols(id) { for (var i = 0; i < VIEWS.length; i++) if (VIEWS[i][0] === id) return VIEWS[i][2]; return VIEWS[0][2]; }
  function boardDef(id) { for (var i = 0; i < BOARDS.length; i++) if (BOARDS[i][0] === id) return BOARDS[i]; return BOARDS[0]; }
  function sizeFloor(id) { for (var i = 0; i < SIZES.length; i++) if (SIZES[i][0] === id) return SIZES[i][2]; return 0; }
  function showOf(r) { return r.why === "pinned" ? "pinned" : r.why ? "aside" : "free"; }

  // ------------------------------------------------------------------------------------------------------ filters
  function rowFilter(st) {
    var q = String(st.q || "").trim().toLowerCase(), floor = sizeFloor(st.size), kind = st.kind === "etf" ? "etf" : "stock", show = st.show || "free";
    return function (r) {
      if (r.kind !== kind) return false;
      if (floor && !((r.mcap || 0) >= floor)) return false;
      if (st.sector && (r.sector || "Unclassified") !== st.sector) return false;
      if (show !== "all" && showOf(r) !== show) return false;
      if (q && !(String(r.t).toLowerCase().indexOf(q) === 0 || String(r.name || "").toLowerCase().indexOf(q) >= 0 || String(r.industry || "").toLowerCase().indexOf(q) >= 0)) return false;
      return true;
    };
  }
  function counts(rows, st) {
    var base = { kind: st.kind, size: st.size, sector: st.sector, q: st.q, show: "all" }, f = rowFilter(base), out = { free: 0, pinned: 0, aside: 0, all: 0 };
    rows.forEach(function (r) { if (f(r)) { out.all++; out[showOf(r)]++; } });
    return out;
  }
  function boardRows(doc, by, id) {
    var list = id === "pinned" ? (doc.pinned || []) : ((doc.boards || {})[id] || []);
    return list.map(function (t) { return by[t]; }).filter(Boolean);
  }
  function csvOf(cols, rows) {
    function q(v) { v = v == null ? "" : String(v); return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; }
    return [cols.map(function (c) { return q(c.id); }).join(",")].concat(rows.map(function (r) { return cols.map(function (c) { var v = c.csv ? c.csv(r) : c.get(r); return q(isNum(v) ? String(Math.round(v * 10000) / 10000) : v); }).join(","); })).join("\n");
  }

  // ----------------------------------------------------------------------------------------------------- verdict
  function share(a, b) { return b ? Math.round(a / b * 100) : null; }
  function verdictLine(doc) {
    var m = doc.market.stock, med = m.med, moving = m.rising + m.falling, bm = doc.market.benchmark || {};
    var dir = isNum(med.hv21) && isNum(med.hv21p) ? (med.hv21 > med.hv21p * 1.05 ? "up from" : med.hv21 < med.hv21p * 0.95 ? "down from" : "about the same as") : null;
    return "As of " + doc.as_of + " the median listed stock is moving at " + flev(med.hv21) + " a year on its last 21 sessions" + (dir ? ", " + dir + " " + flev(med.hv21p) + " a month earlier" : "") +
      "; " + (moving ? share(m.rising, moving) + "% of stocks are more volatile than they were 21 sessions ago" : "") + ". Its median price spread is " + flev(med.sp_d, 2) + " for the day, " + flev(med.sp_w, 1) +
      " for the week, " + flev(med.sp_m, 1) + " for the month and " + flev(med.sp_q, 1) + " for the quarter." + (isNum(bm.hv21) ? " The S&P 500 fund (SPY) reads " + flev(bm.hv21) + "." : "");
  }

  // ------------------------------------------------------------------------------------------- the year's chart
  /* Nice round ticks covering [lo, hi]: at most `n` of them. */
  function ticks(lo, hi, n) {
    var span = Math.max(hi - lo, 1e-9), raw = span / Math.max(1, n), mag = Math.pow(10, Math.floor(Math.log(raw) / Math.LN10)), f = raw / mag;
    var step = (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * mag, out = [];
    for (var v = Math.ceil(lo / step) * step; v <= hi + step * 1e-6; v += step) out.push(Math.round(v / step) * step);
    return out;
  }
  /* The geometry of the market's year: one line (the median) inside a band (the middle half of all stocks), or the
   * median day spread alone. Pure: returns the SVG markup and the points the hover layer reads. */
  function chartOf(history, mode, W, H) {
    var pts = (history || []).filter(function (h) { return isNum(mode === "spread" ? h.sp_d : h.med); });
    if (pts.length < 2) return null;
    var L = 44, R = 62, T = 12, B = 24, band = mode !== "spread";
    var val = function (h) { return mode === "spread" ? h.sp_d : h.med; };
    var lo = Infinity, hi = -Infinity;
    pts.forEach(function (h) { lo = Math.min(lo, band && isNum(h.p25) ? h.p25 : val(h)); hi = Math.max(hi, band && isNum(h.p75) ? h.p75 : val(h)); });
    var pad = (hi - lo) * 0.06 || 1; lo = Math.max(0, lo - pad); hi = hi + pad;
    var tk = ticks(lo, hi, 4); if (tk.length) { lo = Math.min(lo, tk[0]); hi = Math.max(hi, tk[tk.length - 1]); }
    var x = function (i) { return L + i * (W - L - R) / (pts.length - 1); }, y = function (v) { return T + (hi - v) / (hi - lo) * (H - T - B); };
    var line = pts.map(function (h, i) { return (i ? "L" : "M") + x(i).toFixed(1) + "," + y(val(h)).toFixed(1); }).join("");
    var area = "";
    if (band) {
      var up = pts.map(function (h, i) { return (i ? "L" : "M") + x(i).toFixed(1) + "," + y(isNum(h.p75) ? h.p75 : h.med).toFixed(1); }).join("");
      var dn = pts.slice().reverse().map(function (h, j) { return "L" + x(pts.length - 1 - j).toFixed(1) + "," + y(isNum(h.p25) ? h.p25 : h.med).toFixed(1); }).join("");
      area = '<path class="sv-band" d="' + up + dn + 'Z"/>';
    }
    var grid = tk.map(function (v) { return '<line class="sv-grid" x1="' + L + '" x2="' + (W - R) + '" y1="' + y(v).toFixed(1) + '" y2="' + y(v).toFixed(1) + '"/><text class="sv-ax" x="' + (L - 6) + '" y="' + (y(v) + 3.5).toFixed(1) + '" text-anchor="end">' + (mode === "spread" ? v.toFixed(v % 1 ? 1 : 0) : v.toFixed(0)) + "%</text>"; }).join("");
    var months = [], seen = "";
    pts.forEach(function (h, i) { var m = h.date.slice(0, 7); if (m !== seen) { seen = m; if (i > 6 && i < pts.length - 6) months.push(i); } });
    var every = Math.ceil(months.length / 6), MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    var xl = months.filter(function (_, j) { return j % every === 0; }).map(function (i) { var d = pts[i].date; return '<text class="sv-ax" x="' + x(i).toFixed(1) + '" y="' + (H - 7) + '" text-anchor="middle">' + MON[+d.slice(5, 7) - 1] + (d.slice(5, 7) === "01" ? " " + d.slice(0, 4) : "") + "</text>"; }).join("");
    var last = pts[pts.length - 1];
    var end = '<circle class="sv-dot" cx="' + x(pts.length - 1).toFixed(1) + '" cy="' + y(val(last)).toFixed(1) + '" r="3.5"/><text class="sv-end" x="' + (x(pts.length - 1) + 8).toFixed(1) + '" y="' + (y(val(last)) + 4).toFixed(1) + '">' + flev(val(last), mode === "spread" ? 2 : 1) + "</text>";
    return { svg: grid + area + '<path class="sv-line" d="' + line + '"/>' + xl + end, pts: pts, x: x, y: y, val: val, L: L, R: R, T: T, B: B, W: W, H: H, lo: lo, hi: hi, band: band };
  }

  var api = { KEY: KEY, CONTRACT: CONTRACT, COLS: COLS, VIEWS: VIEWS, BOARDS: BOARDS, SIZES: SIZES, SHOWS: SHOWS, CSV_COLS: CSV_COLS, WINDOWS: WINDOWS,
    unpack: unpack, index: index, isAside: isAside, hvd: hvd, compare: compare, sortRows: sortRows, nextSort: nextSort, rowFilter: rowFilter, counts: counts, boardRows: boardRows,
    boardDef: boardDef, viewCols: viewCols, defs: defs, csvOf: csvOf, verdictLine: verdictLine, chartOf: chartOf, ticks: ticks, showOf: showOf,
    fmt: { num: fnum, int: fint, lev: flev, pct: fpct, usd: fusd, px: fpx, gap: fgap, floor: ffloor }, flagText: flagText };
  w.JHStockVol = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  if (!w.document || !w.document.getElementById) return;

  // ================================================================================================= the page
  var doc = null, ROWS = [], BY = {}, D = w.JHDesk, $ = function (id) { return w.document.getElementById(id); };
  var S = { kind: "stock", size: "all", sector: "", q: "", view: "overview", show: "free", board: "most_volatile", chart: "vol", all: false, sort: { rows: { k: "hv21", d: -1 }, board: null, sec: null, size: null } };
  var HASH_KEYS = ["kind", "size", "sector", "q", "view", "show", "board", "chart"], DEFAULTS = { kind: "stock", size: "all", view: "overview", show: "free", board: "most_volatile", chart: "vol" };

  function readHash() {
    var h = String(w.location.hash || "").replace(/^#/, "");
    if (!h || h.indexOf("=") < 0) return;
    h.split("&").forEach(function (kv) {
      var i = kv.indexOf("="), k = kv.slice(0, i), v = decodeURIComponent(kv.slice(i + 1));
      if (HASH_KEYS.indexOf(k) >= 0) S[k] = v;
      else if (k.indexOf("s_") === 0 && S.sort.hasOwnProperty(k.slice(2))) { var p = v.split(":"); S.sort[k.slice(2)] = { k: p[0], d: p[1] === "asc" ? 1 : -1 }; }
    });
  }
  function writeHash() {
    var out = [];
    HASH_KEYS.forEach(function (k) { var v = S[k]; if (v && DEFAULTS[k] !== v) out.push(k + "=" + encodeURIComponent(v)); });
    var bd = boardDef(S.board);
    Object.keys(S.sort).forEach(function (t) { var s = S.sort[t]; if (s && !(t === "rows" && s.k === "hv21" && s.d === -1) && !(t === "board" && s.k === bd[2] && s.d === bd[3])) out.push("s_" + t + "=" + encodeURIComponent(s.k + ":" + (s.d > 0 ? "asc" : "desc"))); });
    try { w.history.replaceState(null, "", out.length ? "#" + out.join("&") : w.location.pathname + w.location.search); } catch (e) { /* a sandboxed frame: the state stays in memory */ }
  }

  /* a cell with its column's class, so a narrow screen can drop a column */
  function cellOf(c, r) {
    var h = c.cell(r);
    return h.indexOf('<td class="') === 0 ? '<td class="c-' + c.id + " " + h.slice(11) : '<td class="c-' + c.id + '"' + h.slice(3);
  }
  function head(table, cols, numbered) {
    var s = S.sort[table];
    return "<thead><tr>" + (numbered ? '<th class="sv-pos" title="position in the list on screen">#</th>' : "") + cols.map(function (c) {
      var on = s && s.k === c.id;
      return '<th scope="col" tabindex="0" data-sort="' + c.id + '" class="c-' + c.id + (c.type === "s" ? " txt" : "") + '"' + (on ? ' aria-sort="' + (s.d > 0 ? "ascending" : "descending") + '"' : "") + (c.tip ? ' title="' + esc(c.tip) + '"' : "") + ">" + esc(c.t) + "</th>";
    }).join("") + "</tr></thead>";
  }
  function paint(table, hostId, cols, rows, empty, cap, numbered) {
    var s = S.sort[table], c = null;
    if (s) cols.forEach(function (x) { if (x.id === s.k) c = x; });
    if (s && !c && COLS[s.k]) c = COLS[s.k];
    var sorted = c ? sortRows(rows, c, s.d) : rows, shown = cap ? sorted.slice(0, cap) : sorted;
    $(hostId).innerHTML = '<table class="jh-tbl sv-tbl">' + head(table, cols, numbered) + "<tbody>" +
      (shown.map(function (r, i) { return "<tr>" + (numbered ? '<td class="sv-pos">' + (i + 1) + "</td>" : "") + cols.map(function (x) { return cellOf(x, r); }).join("") + "</tr>"; }).join("") ||
        '<tr><td class="jh-load" colspan="' + (cols.length + (numbered ? 1 : 0)) + '">' + esc(empty) + "</td></tr>") + "</tbody></table>";
    Array.prototype.forEach.call($(hostId).querySelectorAll("th[data-sort]"), function (th) {
      function go() {
        var id = th.getAttribute("data-sort"), cdef = null;
        cols.forEach(function (x) { if (x.id === id) cdef = x; });
        if (!cdef) return;
        S.sort[table] = nextSort(S.sort[table], cdef);
        draw(table);
      }
      th.onclick = go;
      th.onkeydown = function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(); } };
    });
    return sorted;
  }
  function chips(hostId, items, cur, on) {
    $(hostId).innerHTML = items.map(function (it) {
      return '<button type="button" class="jh-chip' + (it[0] === cur ? " on" : "") + '" data-v="' + esc(it[0]) + '" aria-pressed="' + (it[0] === cur) + '"' + (it[2] ? ' title="' + esc(it[2]) + '"' : "") + ">" + esc(it[1]) + "</button>";
    }).join("");
    Array.prototype.forEach.call($(hostId).querySelectorAll("[data-v]"), function (b) { b.onclick = function () { on(b.getAttribute("data-v")); }; });
  }
  function kpi(label, value, sub) { return '<div class="jh-kpi"><div class="l">' + label + '</div><div class="v">' + value + '</div><div class="s">' + sub + "</div></div>"; }
  function jump(id, text) { return '<button type="button" class="sv-jump" data-board="' + id + '">' + text + "</button>"; }
  function names(id, key, n, d) {
    return boardRows(doc, BY, id).slice(0, n).map(function (r) { return tk(r.t) + ' <span class="dim">' + flev(r[key], d) + "</span>"; }).join(" " + MID + " ") || DASH;
  }

  function drawVerdict() {
    var m = doc.market.stock, med = m.med, u = doc.universe, bm = doc.market.benchmark || {}, moving = m.rising + m.falling, wide = m.expanding + m.contracting;
    var top = boardRows(doc, BY, "most_volatile")[0], calm = boardRows(doc, BY, "least_volatile")[0], floor = ffloor(doc.board_rule.min_mcap);
    $("verdict-line").textContent = verdictLine(doc);
    $("verdict-meta").textContent = fint(u.scanned_stock) + " stocks and " + fint(u.scanned_etf) + " ETFs measured " + MID + " session of " + doc.as_of;
    $("kpis").innerHTML = [
      kpi("Median stock volatility", flev(med.hv21) + " <small>a year, 21 sessions</small>", flev(med.hv21p) + " 21 sessions ago " + MID + " " + flev(med.hv63) + " on 63 " + MID + " " + flev(med.hv252) + " on 252"),
      kpi("More volatile than a month ago", (moving ? share(m.rising, moving) + "%" : DASH) + " <small>of stocks</small>", fint(m.rising) + " rising, " + fint(m.falling) + " falling " + MID + " " + (wide ? share(m.expanding, wide) : DASH) + "% above their own 63-session reading"),
      kpi("S&amp;P 500 fund (SPY)", flev(bm.hv21) + " <small>a year</small>", flev(bm.hv21p) + " 21 sessions ago " + MID + (isNum(bm.hvp) ? " above " + bm.hvp.toFixed(0) + "% of its own last 252 readings" : " no year of readings")),
      kpi("Median price spread", flev(med.sp_d, 2) + " <small>the day</small>", "week " + flev(med.sp_w, 1) + " " + MID + " month " + flev(med.sp_m, 1) + " " + MID + " quarter " + flev(med.sp_q, 1)),
      kpi(jump("most_volatile", "Most volatile") + " <small>" + floor + "+</small>", top ? tk(top.t) + " <small>" + flev(top.hv21) + "</small>" : DASH, names("most_volatile", "hv21", 5).split(" " + MID + " ").slice(1).join(" " + MID + " ")),
      kpi(jump("least_volatile", "Least volatile") + " <small>" + floor + "+</small>", calm ? tk(calm.t) + " <small>" + flev(calm.hv21) + "</small>" : DASH, names("least_volatile", "hv21", 5).split(" " + MID + " ").slice(1).join(" " + MID + " ")),
      kpi(jump("widest_d", "Widest day") + " " + MID + " " + jump("widest_q", "quarter"), names("widest_d", "sp_d", 1, 1), "quarter: " + names("widest_q", "sp_q", 3, 0)),
      kpi(jump("pinned", "Pinned to a deal price"), fint((doc.pinned || []).length) + " <small>stocks</small>", "measured, not ranked " + MID + " " + fint((u.set_aside.stock || 0) - (u.pinned.stock || 0)) + " more set aside (jump, print or thin) " + MID + " " +
        fint((u.reclassified || []).length) + " notes and preferreds left out")
    ].join("");
    Array.prototype.forEach.call($("kpis").querySelectorAll("[data-board]"), function (x) {
      x.onclick = function () { S.board = x.getAttribute("data-board"); S.sort.board = null; draw("board"); $("boards").scrollIntoView({ behavior: "smooth", block: "start" }); };
    });
    var ch = doc.changes || {};
    function list(xs) { return xs.slice(0, 12).map(tk).join(" "); }
    $("changes").innerHTML = !ch.prev_as_of ? "" : "Since " + esc(ch.prev_as_of) + ": " + ["most_volatile", "least_volatile"].map(function (b) {
      var c = ch[b] || {}; if (!(c["new"] || []).length && !(c.gone || []).length) return "";
      return "<b>" + (b === "most_volatile" ? "most volatile" : "least volatile") + "</b> " + ((c["new"] || []).length ? "new " + list(c["new"]) : "") + ((c.gone || []).length ? ' <span class="dim">left ' + c.gone.slice(0, 12).map(esc).join(" ") + "</span>" : "");
    }).filter(Boolean).join(" " + MID + " ") || "Since " + esc(ch.prev_as_of) + ": the two volatility boards hold the same names.";
  }

  var chartState = null;
  function drawChart() {
    var host = $("hist"), W = Math.max(320, Math.min(900, host.clientWidth || 720)), H = 240, mode = S.chart === "spread" ? "spread" : "vol";
    chips("hist-mode", [["vol", "Volatility"], ["spread", "Day spread"]], mode, function (v) { S.chart = v; draw("chart"); });
    var g = chartOf(doc.history, mode, W, H);
    if (!g) { host.innerHTML = '<p class="jh-load">The market' + "'s own history is not in this artifact yet.</p>"; return; }
    host.innerHTML = '<svg class="sv-chart" viewBox="0 0 ' + W + " " + H + '" width="' + W + '" height="' + H + '" role="img" tabindex="0" aria-label="' +
      esc(mode === "spread" ? "Median one-session price spread of listed stocks over the last 252 sessions" : "Median 21-session volatility of listed stocks over the last 252 sessions, with the middle half of all stocks as a band") + '">' +
      g.svg + '<line class="sv-cross" id="hist-x" y1="' + g.T + '" y2="' + (H - g.B) + '" x1="-9" x2="-9"/><circle class="sv-dot" id="hist-dot" r="3.5" cx="-9" cy="-9"/></svg><div class="sv-tip" id="hist-tip" hidden></div>';
    $("hist-key").innerHTML = mode === "spread" ? '<span><i class="k-line"></i>median one-session spread of all listed stocks</span>' :
      '<span><i class="k-line"></i>median stock</span><span><i class="k-band"></i>the middle half of all stocks (25th to 75th percentile)</span>';
    chartState = g;
    var svg = host.querySelector("svg"), tip = $("hist-tip");
    function show(i) {
      i = Math.max(0, Math.min(g.pts.length - 1, i));
      var h = g.pts[i], cx = g.x(i), cy = g.y(g.val(h));
      $("hist-x").setAttribute("x1", cx); $("hist-x").setAttribute("x2", cx);
      $("hist-dot").setAttribute("cx", cx); $("hist-dot").setAttribute("cy", cy);
      tip.textContent = "";
      function row(label, value, strong) { var d = w.document.createElement("div"); var b = w.document.createElement(strong ? "b" : "span"); b.textContent = value; d.appendChild(b); d.appendChild(w.document.createTextNode(" " + label)); tip.appendChild(d); }
      var dt = w.document.createElement("div"); dt.className = "d"; dt.textContent = h.date; tip.appendChild(dt);
      if (mode === "spread") row("median day spread", flev(h.sp_d, 2), true);
      else { row("median stock, 21-session volatility", flev(h.med), true); row("middle half of stocks", flev(h.p25) + " to " + flev(h.p75)); row("median day spread", flev(h.sp_d, 2)); }
      row("stocks counted", fint(h.n));
      tip.hidden = false;
      var left = cx + 12; if (left + 230 > W) left = cx - 242;
      tip.style.left = Math.max(0, left) + "px"; tip.style.top = Math.max(0, cy - 70) + "px";
      svg.setAttribute("data-i", i);
    }
    function hide() { tip.hidden = true; $("hist-x").setAttribute("x1", -9); $("hist-x").setAttribute("x2", -9); $("hist-dot").setAttribute("cx", -9); }
    function at(e) {
      var b = svg.getBoundingClientRect(), px = (e.clientX - b.left) * (W / Math.max(1, b.width));
      show(Math.round((px - g.L) / ((W - g.L - g.R) / (g.pts.length - 1))));
    }
    svg.addEventListener("pointermove", at); svg.addEventListener("pointerdown", at); svg.addEventListener("pointerleave", hide);
    svg.addEventListener("focus", function () { show(g.pts.length - 1); }); svg.addEventListener("blur", hide);
    svg.addEventListener("keydown", function (e) {
      var i = +(svg.getAttribute("data-i") || g.pts.length - 1);
      if (e.key === "ArrowLeft") { e.preventDefault(); show(i - (e.shiftKey ? 21 : 1)); } else if (e.key === "ArrowRight") { e.preventDefault(); show(i + (e.shiftKey ? 21 : 1)); }
    });
    var every = (doc.history || []).filter(function (_, i, a) { return i % 21 === 0 || i === a.length - 1; });
    $("hist-table").innerHTML = '<table class="jh-tbl sv-tbl"><thead><tr><th class="txt">Session</th><th>Stocks</th><th>25th pct</th><th>Median vol 21</th><th>75th pct</th><th>Median day spread</th></tr></thead><tbody>' +
      every.slice().reverse().map(function (h) { return "<tr>" + td(esc(h.date), "l") + td(fint(h.n)) + td(flev(h.p25)) + td(flev(h.med)) + td(flev(h.p75)) + td(flev(h.sp_d, 2)) + "</tr>"; }).join("") + "</tbody></table>";
  }

  function drawBands() {
    var b = doc.bands || [], n = 0, cap = 0;
    b.forEach(function (x) { n += x.n; cap += x.mcap || 0; });
    $("bands").innerHTML = '<table class="jh-tbl sv-tbl sv-bands"><thead><tr><th class="txt">Volatility, 21 sessions</th><th>Stocks</th><th class="txt">Share of stocks</th><th>Market value</th><th class="txt">Share of value</th></tr></thead><tbody>' +
      b.map(function (x) {
        var a = n ? x.n / n * 100 : 0, c = cap ? (x.mcap || 0) / cap * 100 : 0;
        return "<tr>" + td(esc(x.band), "l") + td(fint(x.n)) + td('<span class="sv-share"><i style="width:' + a.toFixed(1) + '%"></i></span>' + a.toFixed(0) + "%", "l") + td(fusd(x.mcap)) +
          td('<span class="sv-share"><i style="width:' + c.toFixed(1) + '%"></i></span>' + c.toFixed(0) + "%", "l") + "</tr>";
      }).join("") + '<tr class="sv-total">' + td("All ranked stocks", "l") + td(fint(n)) + td("", "l") + td(fusd(cap)) + td("", "l") + "</tr></tbody></table>";
  }

  function drawBoard() {
    var def = boardDef(S.board), rows = boardRows(doc, BY, def[0]), floor = ffloor(doc.board_rule.min_mcap);
    chips("board-chips", BOARDS.map(function (b) { return [b[0], b[1] + (b[0] === "pinned" ? " " + (doc.pinned || []).length : "")]; }), def[0], function (v) { S.board = v; S.sort.board = null; draw("board"); });
    if (!S.sort.board) S.sort.board = { k: def[2], d: def[3] };
    $("board-meta").textContent = def[0] === "pinned" ? rows.length + " stocks whose last 21 sessions fit inside " + doc.windows.pin_spread_m + "% " + MID + " measured, not ranked" :
      "the " + rows.length + " at this end among " + fint((doc.board_rule.population || {})[MEASURE[def[2]] || "hv21"]) + " ranked common stocks worth " + floor + " or more";
    paint("board", "board", defs(def[4]), rows, "No stock is on this board in this artifact.", 0, true);
  }

  function drawRows() {
    var cnt = counts(ROWS, S);
    chips("rows-kind", [["stock", "Stocks " + fint(doc.universe.scanned_stock)], ["etf", "ETFs " + fint(doc.universe.scanned_etf)]], S.kind, function (v) { S.kind = v; S.sector = ""; S.all = false; if (v === "etf" && S.show === "pinned") S.show = "free"; draw("rows"); });
    chips("rows-size", SIZES.map(function (x) { return [x[0], x[1]]; }), S.size, function (v) { S.size = v; S.all = false; draw("rows"); });
    chips("rows-view", VIEWS.map(function (v) { return [v[0], v[1]]; }), S.view, function (v) { S.view = v; draw("rows"); });
    chips("rows-show", SHOWS.map(function (x) { return [x[0], x[1] + " " + fint(cnt[x[0]]), x[0] === "free" ? "rows that are ranked: not pinned, not thin, no unexplained jump or unsupported print inside the last 63 sessions" : ""]; }), S.show, function (v) { S.show = v; S.all = false; draw("rows"); });
    var sectors = [], seen = {};
    ROWS.forEach(function (r) { var s = r.sector || "Unclassified"; if (r.kind === S.kind && !seen[s]) { seen[s] = 1; sectors.push(s); } });
    sectors.sort();
    var sel = $("rows-sector");
    sel.innerHTML = '<option value="">All sectors</option>' + sectors.map(function (v) { return '<option value="' + esc(v) + '"' + (v === S.sector ? " selected" : "") + ">" + esc(v) + "</option>"; }).join("");
    sel.disabled = S.kind === "etf" && sectors.length < 2;
    var rows = ROWS.filter(rowFilter(S)), cols = defs(viewCols(S.view));
    var sorted = paint("rows", "rows", cols, rows, "No row matches these filters.", S.all ? 0 : RENDER_CAP, true), s = S.sort.rows, sc = s && COLS[s.k];
    $("rows-meta").textContent = fint(rows.length) + " of " + fint(cnt.all) + " " + (S.kind === "etf" ? "ETFs" : "stocks") + (sc ? " " + MID + " ranked by " + sc.t + ", " + (s.d < 0 ? "largest first" : "smallest first") : "");
    var more = $("rows-more");
    more.hidden = S.all || sorted.length <= RENDER_CAP;
    more.textContent = "Show all " + fint(sorted.length) + " rows (" + RENDER_CAP + " shown)";
    api._last = { rows: sorted, cols: defs(CSV_COLS) };
  }

  var GCOLS = {};
  function gcol(id, t, tip, get, cell) { GCOLS[id] = { id: id, t: t, tip: tip, type: id === "name" ? "s" : "n", get: get, cell: cell }; }
  gcol("name", "Group", "", function (g) { return g.sector || g.label; }, function (g) { return td(esc(g.sector || g.label), "l"); });
  gcol("n", "Stocks", "", function (g) { return g.n; }, function (g) { return td(fint(g.n)); });
  gcol("mcap", "Market value", "", function (g) { return g.mcap; }, function (g) { return td(fusd(g.mcap)); });
  [["hv21", "Vol 21"], ["hv21p", "21 ago"], ["hv63", "Vol 63"], ["hv252", "Vol 252"], ["yz21", "YZ 21"], ["atr", "ATR %"], ["sp_d", "Day"], ["sp_w", "Week"], ["sp_m", "Month"], ["sp_q", "Quarter"]].forEach(function (x) {
    gcol(x[0], x[1], "the group's median", function (g) { return g.med[x[0]]; }, function (g) { return td(flev(g.med[x[0]], x[0] === "atr" || x[0] === "sp_d" ? 2 : 1)); });
  });
  gcol("rising", "Rising", "share of the group's stocks whose Vol 21 is above its reading 21 sessions ago", function (g) { var m = g.rising + g.falling; return m ? g.rising / m * 100 : null; },
    function (g) { var m = g.rising + g.falling; return td(m ? Math.round(g.rising / m * 100) + '% <span class="dim">' + fint(g.rising) + " of " + fint(m) + "</span>" : DASH); });
  var GLIST = ["name", "n", "mcap", "hv21", "hv21p", "hv63", "hv252", "atr", "sp_d", "sp_w", "sp_m", "sp_q", "rising"];
  function drawGroups() {
    function one(table, host, rows) {
      var cols = GLIST.map(function (id) { return GCOLS[id]; }), s = S.sort[table], c = s ? GCOLS[s.k] : null, sorted = c ? sortRows(rows, c, s.d) : rows;
      $(host).innerHTML = '<table class="jh-tbl sv-tbl">' + head(table, cols, false) + "<tbody>" + sorted.map(function (g) { return "<tr>" + cols.map(function (x) { return x.cell(g); }).join("") + "</tr>"; }).join("") + "</tbody></table>";
      Array.prototype.forEach.call($(host).querySelectorAll("th[data-sort]"), function (th) {
        function go() { S.sort[table] = nextSort(S.sort[table], GCOLS[th.getAttribute("data-sort")]); draw("groups"); }
        th.onclick = go; th.onkeydown = function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(); } };
      });
    }
    one("sec", "sectors", doc.by_sector || []);
    one("size", "sizes", doc.by_size || []);
  }

  var RULE_NAME = { "hv21 / hv63 / hv252": "Vol 21 / 63 / 252", "yz21": "YZ 21", "atr": "ATR %", "hvp": "In its year (volatility)", "hv21p": "21 ago, Change", "mx / mxd": "Largest move",
    "sp_d / sp_w / sp_m / sp_q": "Day / Week / Month / Quarter", "hi / lo": "Low and high", "pos": "Close inside the spread", "own": "In its year (spread)", "rk_*": "# ranks", "aside / why": "Set aside",
    "pinned": "Pinned", "jd / jr / ja": "Jump", "pd / pr / pa": "Print", "miss": "Thin", "boards": "The two ends", "bands": "Volatility bands", "universe": "Universe" };
  function drawRules() {
    var d = doc.definitions || {};
    $("rules").innerHTML = "<dl>" + Object.keys(d).map(function (k) { return "<dt>" + esc(RULE_NAME[k] || k) + "</dt><dd>" + esc(d[k]) + "</dd>"; }).join("") + "</dl>";
    $("limits").innerHTML = (doc.limitations || []).map(function (x) { return "<li>" + esc(x) + "</li>"; }).join("");
  }
  function drawSources() {
    var u = doc.universe, rc = u.reclassified || [];
    $("sources").innerHTML = '<table class="jh-tbl sv-tbl sv-src"><thead><tr><th class="txt">Engine</th><th class="txt">Reads</th><th class="txt">What for</th><th class="txt">Runs</th></tr></thead><tbody>' +
      (doc.sources || []).map(function (s) { return "<tr>" + td(esc(s.engine), "l") + td(esc(s.key), "l") + td(esc(s.role), "l wrap") + td(esc(s.schedule), "l") + "</tr>"; }).join("") + "</tbody></table>";
    var out = Object.keys(u.left_out || {}).map(function (k) { return fint(u.left_out[k]) + " " + esc(k); }).join(" " + MID + " ");
    $("ops").innerHTML = "<p><b>" + esc(doc.engine) + " " + esc(doc.version) + "</b> " + MID + " contract " + esc(doc.contract) + " " + MID + " generated " + esc(doc.generated_at) + " in " + esc(doc.run_seconds) + " s " + MID + " " + fint(doc.n_errors) + " errors " + MID + " " +
      esc((doc.trigger || {}).kind || "") + "</p>" +
      "<p><b>Universe</b> " + fint(u.book_rows) + " factor-book rows " + MID + " " + fint(u.eligible) + " eligible " + MID + " " + fint(u.scanned) + " measured (" + fint(u.scanned_stock) + " stocks, " + fint(u.scanned_etf) + " ETFs) " + MID + " " +
      fint(u.short_history) + " too young " + MID + " " + fint(u.not_trading) + " not trading " + MID + " bank from " + esc(u.bank_first_session) + ", " + fint(u.bank_sessions) + " sessions</p>" +
      "<p><b>Left out</b> " + out + "</p>" +
      "<p><b>Set aside, stocks</b> " + fint(u.pinned.stock) + " pinned " + MID + " " + fint(u.thin.stock) + " thin " + MID + " " + fint(u.flagged_jump.stock) + " with a flagged jump in their bars " + MID + " " + fint((u.flagged_print || {}).stock || 0) + " with a flagged print " + MID + " ranked for Vol 21: " + fint(u.ranked.stock.hv21) + "</p>" +
      (rc.length ? "<p><b>Notes, preferred shares and warrants the factor book lists as common stock</b> (" + rc.length + ", left out) " + rc.map(function (x) { return '<span title="' + esc(x.why) + '">' + esc(x.t) + "</span>"; }).join(" ") + "</p>" : "") +
      ((doc.errors || []).length ? "<p><b>Errors</b> " + esc(JSON.stringify(doc.errors.slice(0, 8))) + "</p>" : "");
  }

  function stamp() {
    $("stamp").innerHTML = "session of <b>" + esc(doc.as_of) + "</b><br>generated " + esc(String(doc.generated_at).replace("T", " ").replace("+00:00", " UTC")) + "<br>" + esc(doc.engine) + " " + esc(doc.version);
  }
  function draw(what) {
    if (!doc) return;
    if (!what || what === "verdict") drawVerdict();
    if (!what || what === "chart") drawChart();
    if (!what) drawBands();
    if (!what || what === "board") drawBoard();
    if (!what || what === "rows") drawRows();
    if (!what || what === "groups" || what === "sec" || what === "size") drawGroups();
    if (!what) { drawRules(); drawSources(); }
    writeHash();
  }
  function fail(msg) { $("verdict-line").textContent = msg; $("rows").innerHTML = '<p class="jh-load">' + esc(msg) + "</p>"; }
  function load() {
    return D.feed(KEY).then(function (d) {
      if (!d || d.contract !== CONTRACT) { if (!doc) fail("data/stock-volatility.json is not published yet - justhodl-stock-volatility runs daily at 04:06 UTC, after the Polygon EOD bank folds the session."); return; }
      if (doc && doc.generated_at === d.generated_at) return;
      doc = d; ROWS = unpack(d); BY = index(ROWS);
      stamp();
      draw();
    }).catch(function (e) { if (!doc) fail(String(e && e.message || e)); });
  }
  function download() {
    var last = api._last; if (!last) return;
    var a = w.document.createElement("a");
    a.href = URL.createObjectURL(new Blob([csvOf(last.cols, last.rows)], { type: "text/csv" }));
    a.download = "stock-volatility-" + doc.as_of + "-" + S.kind + ".csv";
    w.document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 2000);
  }
  function wire() {
    var q = $("rows-q"), sel = $("rows-sector"), t = null;
    q.value = S.q;
    q.oninput = function () { clearTimeout(t); t = setTimeout(function () { S.q = q.value; S.all = false; draw("rows"); }, 120); };
    sel.onchange = function () { S.sector = sel.value; S.all = false; draw("rows"); };
    $("rows-more").onclick = function () { S.all = true; draw("rows"); };
    $("rows-reset").onclick = function () { S.size = "all"; S.sector = ""; S.q = ""; S.show = "free"; S.all = false; q.value = ""; draw("rows"); };
    $("rows-csv").onclick = download;
    var rz = null;
    w.addEventListener("resize", function () { clearTimeout(rz); rz = setTimeout(function () { if (doc) drawChart(); }, 150); });
    w.document.addEventListener("click", function (e) {
      var a = e.target && e.target.closest ? e.target.closest("a.tk") : null;
      if (!a || w.top === w) return;
      try {                                                        // inside the chart's workspace: load the symbol in the chart behind it
        if (typeof w.top.jhOpenSymbol === "function") {
          e.preventDefault();
          w.top.jhOpenSymbol(a.getAttribute("data-t"));
          var x = w.top.document.getElementById("wsx");
          if (x) x.click();
        }
      } catch (err) { /* another origin: the link's own target=_top navigation stands */ }
    });
  }
  function boot() {
    if (!D || !$("kpis")) return;
    readHash();
    wire();
    load();
    setInterval(load, REFRESH_MS);
  }
  if (w.document.readyState === "loading") w.document.addEventListener("DOMContentLoaded", boot); else boot();
})(typeof window !== "undefined" ? window : globalThis);
