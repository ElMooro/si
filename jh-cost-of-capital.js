/* jh-cost-of-capital.js - the Cost of capital desk (cost-of-capital.html).   jh-reskin-skip
 *
 * Reads what justhodl-damodaran publishes and computes nothing of its own:
 *   data/equity-risk-premium.json     the implied premium today (five measures), monthly since 2008, annual since 1960,
 *                                     annual returns since 1928 and the premiums the engine worked from them
 *   data/country-risk-premiums.json   equity and country risk premium by country, and the frontier markets
 *   data/industry-benchmarks.json     cost of capital, margins, returns and multiples by industry and region,
 *                                     and the list of every table (data/warm/damodaran/tables/<id>.json)
 * The data is Aswath Damodaran's (Damodaran Online, NYU Stern); every block says so and links his file.
 * Every list is whole, sorts both ways on every column and saves as a file. Colours that carry data live here.
 * Khalid 2026-11-27: "add whatever my system needs".
 */
(function (root, factory) {
  var api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.JHCostOfCapital = api;
})(typeof window !== "undefined" ? window : null, function () {
  "use strict";
  var SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181"];       // five steps of one validated palette, in fixed order
  var C = { ink: "#e6eaf2", mute: "#a8b3c7", dim: "#6f7b91", line: "#1c2433", grid: "#182030" };
  var MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  var BENCH = { firms: "int", beta: "num2", pe_trailing: "x1", pe_forward: "x1", pbv: "x1", ps: "x1", ev_sales: "x1", ev_ebitda: "x1", ev_ebit: "x1" };
  var BASE = "/";

  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function pct(v, d) { return isNum(v) ? (v * 100).toFixed(d == null ? 2 : d) + "%" : "–"; }
  function group(n) { return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ","); }
  function raw(v) {
    if (!isNum(v)) return v == null || v === "" ? "–" : String(v);
    if (Math.abs(v) >= 1000) return group(Math.round(v));
    var s = v.toFixed(4).replace(/0+$/, "").replace(/\.$/, "");
    return s === "-0" ? "0" : s;
  }
  function day(s) { var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(s || "")); return m ? MONTHS[+m[2] - 1] + " " + (+m[3]) + ", " + m[1] : "–"; }
  function month(s) { var m = /^(\d{4})-(\d{2})/.exec(String(s || "")); return m ? MONTHS[+m[2] - 1] + " " + m[1] : "–"; }
  /* One cell, by the kind of its column. */
  function cell(kind, v) {
    if (v == null || v === "") return "–";
    if (kind === "pct") return pct(v, 2);
    if (kind === "int") return isNum(v) ? group(Math.round(v)) : String(v);
    if (kind === "num2") return isNum(v) ? v.toFixed(2) : String(v);
    if (kind === "x1") return isNum(v) ? v.toFixed(1) : String(v);
    if (kind === "usd0") return isNum(v) ? "$" + group(Math.round(v)) : String(v);
    if (kind === "year") return String(v);
    if (kind === "month") return month(v);
    if (kind === "text") return String(v);
    return raw(v);
  }
  /* Rows by one column, either way; a row without the figure is last both ways; ties keep their order. */
  function sorted(rows, j, dir) {
    var sign = dir === "asc" ? 1 : -1;
    return rows.map(function (r, i) { return [r, i]; }).sort(function (a, b) {
      var x = a[0][j], y = b[0][j], nx = x == null || x === "", ny = y == null || y === "";
      if (nx || ny) return nx && ny ? a[1] - b[1] : nx ? 1 : -1;
      var c = typeof x === "number" && typeof y === "number" ? x - y : String(x).localeCompare(String(y), "en", { numeric: true, sensitivity: "base" });
      return c ? sign * c : a[1] - b[1];
    }).map(function (p) { return p[0]; });
  }
  function filtered(rows, q) {
    q = String(q || "").trim().toLowerCase();
    if (!q) return rows;
    return rows.filter(function (r) { return r.some(function (v) { return typeof v === "string" && v.toLowerCase().indexOf(q) >= 0; }); });
  }
  function csv(cols, rows) {
    function q(v) { v = v == null ? "" : String(v); return /[",\r\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; }
    return [cols.map(function (c) { return q(c.label); }).join(",")].concat(rows.map(function (r) { return r.map(q).join(","); })).join("\r\n") + "\r\n";
  }
  function shown(g) { var rows = filtered(g.rows, g.q); return g.sort ? sorted(rows, g.sort.j, g.sort.dir) : rows; }
  /* A whole list: search, count, save, and a heading on every column that sorts it both ways. */
  function gridHtml(g) {
    var rows = shown(g), text0 = g.cols[0].kind === "text" || !g.cols[0].kind;
    var h = '<div class="cc-tools">' + (text0 ? '<input type="search" data-cc-q="' + esc(g.id) + '" value="' + esc(g.q || "") + '" placeholder="Search ' + esc(g.what || "the list") + '" aria-label="Search ' + esc(g.what || "the list") + '">' : "") +
      '<span class="cc-count" data-cc-count="' + esc(g.id) + '">' + group(rows.length) + (rows.length !== g.rows.length ? " of " + group(g.rows.length) : "") + " " + esc(g.unit || "rows") + "</span>" +
      '<button type="button" class="cc-btn" data-cc-csv="' + esc(g.id) + '">Download as a file</button></div>';
    h += '<div class="cc-scroll" tabindex="0"><table class="cc-t"><thead><tr>' + g.cols.map(function (c, j) {
      var on = g.sort && g.sort.j === j, left = !c.kind || c.kind === "text";
      return '<th class="' + (left ? "l" : "r") + '" aria-sort="' + (on ? (g.sort.dir === "asc" ? "ascending" : "descending") : "none") + '"><button type="button" data-cc-sort="' + esc(g.id) + '" data-j="' + j + '">' +
        esc(c.label) + (on ? (g.sort.dir === "asc" ? " ▲" : " ▼") : "") + "</button></th>";
    }).join("") + "</tr></thead><tbody>" + rows.map(function (r) {
      return "<tr>" + g.cols.map(function (c, j) { var left = !c.kind || c.kind === "text"; return '<td class="' + (left ? "l" : "r") + '">' + esc(cell(c.kind || (j ? "raw" : "text"), r[j])) + "</td>"; }).join("") + "</tr>";
    }).join("") + (rows.length ? "" : '<tr><td class="l cc-none" colspan="' + g.cols.length + '">Nothing matches.</td></tr>') + "</tbody></table></div>";
    return h;
  }

  /* ---------------------------------------------------------------------------------------------- the premium */
  function tiles(P) {
    var us = P.us || {}, a = us.a_month_before, y = us.a_year_before;
    function tile(big, label, sub) { return '<div class="cc-tile"><div class="cc-lbl">' + esc(label) + '</div><div class="cc-big">' + esc(big) + '</div><div class="cc-sub">' + esc(sub) + "</div></div>"; }
    function chg(b) { if (!b || !isNum(b.pct) || !isNum(us.premium_pct)) return "–"; var d = us.premium_pct - b.pct; return (d > 0 ? "+" : d < 0 ? "−" : "") + Math.abs(d).toFixed(2) + " pts"; }
    return '<div class="cc-tiles">' +
      tile(isNum(us.premium_pct) ? us.premium_pct.toFixed(2) + "%" : "–", "Implied equity risk premium", "start of " + month(us.as_of) + " · over the 10-year Treasury") +
      tile(a ? a.pct.toFixed(2) + "%" : "–", "A month before", chg(a) + " since " + (a ? month(a.date) : "–")) +
      tile(y ? y.pct.toFixed(2) + "%" : "–", "A year before", chg(y) + " since " + (y ? month(y.date) : "–")) +
      tile(isNum(us.tbond_pct) ? us.tbond_pct.toFixed(2) + "%" : "–", "10-year Treasury rate", "the rate the premium sits on") +
      tile(isNum(us.expected_return_pct) ? us.expected_return_pct.toFixed(2) + "%" : "–", "Expected return on stocks", "Treasury rate plus premium") +
      tile(isNum(us.sp500) ? group(Math.round(us.sp500)) : "–", "S&P 500", "the level the premium is read from") + "</div>";
  }
  function measuresHtml(P) {
    var us = P.us || {}, hp = P.home_page || {};
    var h = '<table class="cc-t cc-small"><thead><tr><th class="l">Measure</th><th class="l">His column</th><th class="r">Premium</th><th class="r">His page says</th></tr></thead><tbody>';
    (us.measures || []).forEach(function (m, i) {
      var page = (hp.page_values_pct || [])[i];
      h += "<tr><td class=\"l\">" + (m.key === us.measure ? "<b>" + esc(m.label) + "</b> · the one the engines use" : esc(m.label)) + '</td><td class="l cc-dim">' + esc(m.his_heading) + '</td><td class="r">' + m.pct.toFixed(2) + '%</td><td class="r">' +
        (isNum(page) && hp.agree !== null ? page.toFixed(2) + "%" : "–") + "</td></tr>";
    });
    return h + "</tbody></table>" + '<p class="cc-note">' + (hp.agree === true ? "The file's newest row is his home page's five figures for " + esc(day(hp.page_date)) + "."
      : hp.agree === false ? "His home page and his file disagree for " + esc(day(hp.page_date)) + ": the file is used."
        : "His home page is of " + esc(day(hp.page_date)) + " and his file of " + esc(day(hp.file_date)) + ": not compared.") + "</p>";
  }
  /* Lines to draw: [{key, label, color, pts: [[ms, fraction], ...]}], in the palette's fixed order. */
  function monthlySeries(P) {
    var mo = P.monthly || {}, keys = (mo.cols || []).map(function (c) { return c.key; }), mc = mo.measure_columns || {};
    return ((P.us || {}).measures || []).map(function (m, i) {
      var j = keys.indexOf(mc[m.key]);
      return { key: m.key, label: m.label, color: SERIES[i % SERIES.length], pts: j < 0 ? [] : (mo.rows || []).map(function (r) { return [Date.parse(r[0] + "T00:00:00Z"), r[j]]; }).filter(function (p) { return isNum(p[1]); }) };
    });
  }
  function annualSeries(P) {
    var an = P.annual || {}, keys = (an.cols || []).map(function (c) { return c.key; });
    var want = [["implied_premium_fcfe_with_sustainable_payout", "Cash flows, sustainable payout"], ["implied_erp_fcfe", "Cash flows (dividends and buybacks)"], ["implied_premium_ddm", "Dividends alone"]];
    return want.map(function (w, i) {
      var j = keys.indexOf(w[0]);
      return { key: w[0], label: w[1], color: SERIES[i], pts: j < 0 ? [] : (an.rows || []).map(function (r) { return [Date.UTC(r[0], 11, 31), r[j]]; }).filter(function (p) { return isNum(p[1]); }) };
    }).filter(function (s) { return s.pts.length; });
  }
  /* One axis, thin lines, each line's last value written at its end in ink beside a dot of its colour. */
  function chartSvg(series, w, h) {
    var L = 46, R = 74, T = 12, B = 26, all = [];
    series.forEach(function (s) { s.pts.forEach(function (p) { all.push(p); }); });
    if (!all.length) return { svg: '<p class="cc-none">No history to draw.</p>', x: null };
    var x0 = Math.min.apply(null, all.map(function (p) { return p[0]; })), x1 = Math.max.apply(null, all.map(function (p) { return p[0]; }));
    var lo = Math.floor(Math.min.apply(null, all.map(function (p) { return p[1]; })) * 100), hi = Math.ceil(Math.max.apply(null, all.map(function (p) { return p[1]; })) * 100);
    if (hi === lo) hi = lo + 1;
    function X(t) { return L + (w - L - R) * (x1 === x0 ? 0.5 : (t - x0) / (x1 - x0)); }
    function Y(v) { return T + (h - T - B) * (1 - (v * 100 - lo) / (hi - lo)); }
    var step = hi - lo > 8 ? 2 : 1, g = "";
    for (var v = lo; v <= hi; v += step) g += '<line x1="' + L + '" x2="' + (w - R) + '" y1="' + Y(v / 100).toFixed(1) + '" y2="' + Y(v / 100).toFixed(1) + '" stroke="' + C.grid + '"/><text x="' + (L - 8) + '" y="' + (Y(v / 100) + 4).toFixed(1) + '" text-anchor="end" fill="' + C.dim + '" font-size="11">' + v + "%</text>";
    var y0 = new Date(x0).getUTCFullYear(), y1 = new Date(x1).getUTCFullYear(), every = Math.max(1, Math.ceil((y1 - y0) / Math.max(2, Math.floor((w - L - R) / 70))));
    for (var yr = y0 + 1; yr <= y1; yr++) if ((yr - y0) % every === 0 || every === 1) { var tx = X(Date.UTC(yr, 0, 1)); if (tx > L + 14 && tx < w - R - 8) g += '<text x="' + tx.toFixed(1) + '" y="' + (h - 8) + '" text-anchor="middle" fill="' + C.dim + '" font-size="11">' + yr + "</text>"; }
    var ends = [];
    series.forEach(function (s) {
      if (!s.pts.length) return;
      var d = "", prev = null;
      s.pts.forEach(function (p) { d += (prev === null || p[0] - prev > 75 * 86400000 * (s.pts.length > 150 ? 1 : 6) ? "M" : "L") + X(p[0]).toFixed(1) + " " + Y(p[1]).toFixed(1); prev = p[0]; });
      g += '<path d="' + d + '" fill="none" stroke="' + s.color + '" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>';
      var last = s.pts[s.pts.length - 1];
      ends.push({ y: Y(last[1]), x: X(last[0]), text: (last[1] * 100).toFixed(2) + "%", color: s.color });
    });
    ends.sort(function (a, b) { return a.y - b.y; });
    for (var i = 1; i < ends.length; i++) if (ends[i].y - ends[i - 1].y < 14) ends[i].ly = (ends[i - 1].ly || ends[i - 1].y) + 14;
    ends.forEach(function (e) { g += '<circle cx="' + e.x.toFixed(1) + '" cy="' + e.y.toFixed(1) + '" r="4" fill="' + e.color + '" stroke="#0f131a" stroke-width="2"/><text x="' + (e.x + 9).toFixed(1) + '" y="' + ((e.ly || e.y) + 4).toFixed(1) + '" fill="' + C.ink + '" font-size="12">' + e.text + "</text>"; });
    return { svg: '<svg class="cc-svg" width="' + w + '" height="' + h + '" viewBox="0 0 ' + w + " " + h + '" role="img" aria-label="Implied equity risk premium over time">' + g +
      '<line class="cc-cross" x1="0" x2="0" y1="' + T + '" y2="' + (h - B) + '" stroke="' + C.mute + '" stroke-dasharray="3 3" visibility="hidden"/></svg>', x: { x0: x0, x1: x1, L: L, R: R, w: w } };
  }
  /* The reading under the pointer: for each line, its point nearest that day (within a quarter for months, two years for years). */
  function nearest(series, t) {
    return series.map(function (s) {
      var best = null;
      s.pts.forEach(function (p) { if (best === null || Math.abs(p[0] - t) < Math.abs(best[0] - t)) best = p; });
      return best && Math.abs(best[0] - t) < (s.pts.length > 150 ? 100 : 800) * 86400000 ? { label: s.label, color: s.color, t: best[0], v: best[1] } : null;
    }).filter(Boolean);
  }
  function legendHtml(series, off) {
    return '<div class="cc-legend">' + series.map(function (s) { return '<button type="button" class="cc-key' + (off[s.key] ? " off" : "") + '" data-cc-line="' + esc(s.key) + '" aria-pressed="' + (off[s.key] ? "false" : "true") + '"><i style="background:' + s.color + '"></i>' + esc(s.label) + "</button>"; }).join("") + "</div>";
  }

  /* ------------------------------------------------------------------------------------------------ the lists */
  function countryGrid(K, st) {
    return { id: "countries", what: "countries", unit: "countries", q: st.q, sort: st.sort, rows: K.rows || [],
      cols: (K.cols || []).map(function (c, j) { return { key: c.key, label: c.label, kind: j < 3 ? "text" : "pct" }; }) };
  }
  function frontierGrid(K, st) {
    var f = K.frontier || { cols: [], rows: [] };
    return { id: "frontier", what: "frontier markets", unit: "markets", q: st.q, sort: st.sort, rows: f.rows || [],
      cols: (f.cols || []).map(function (c, j) { return { key: c.key, label: c.label, kind: !j ? "text" : /score/i.test(c.label) ? "num2" : "pct" }; }) };
  }
  function benchGrid(I, region, st) {
    var b = (I.benchmarks || {})[region] || { cols: [], rows: [] };
    return { id: "industries", what: "industries", unit: "industries", q: st.q, sort: st.sort, rows: b.rows || [],
      cols: (b.cols || []).map(function (c, j) { return { key: c.key, label: c.label, kind: !j ? "text" : BENCH[c.key] || "pct" }; }) };
  }
  function tableGrid(Tb, st) {
    return { id: "table", what: "the table", unit: "rows", q: st.q, sort: st.sort, rows: Tb.rows || [],
      cols: (Tb.cols || []).map(function (c, j) { return { key: c.key, label: c.label, kind: !j ? "text" : /^number of firms$/i.test(c.label) ? "int" : "raw" }; }) };
  }
  function returnsGrid(P, st) {
    var r = P.returns || { cols: [], rows: [] };
    return { id: "returns", unit: "years", q: "", sort: st.sort, rows: r.rows || [],
      cols: (r.cols || []).map(function (c, j) { return { key: c.key, label: c.label, kind: !j ? "year" : /^value of/i.test(c.label) ? "usd0" : "pct" }; }) };
  }
  function monthlyGrid(P, st) {
    var mo = P.monthly || { cols: [], rows: [] };
    return { id: "monthly", unit: "months", q: "", sort: st.sort, rows: mo.rows || [],
      cols: (mo.cols || []).map(function (c, j) { return { key: c.key, label: c.label, kind: !j ? "month" : /^erp|rate|growth|return/i.test(c.label) ? "pct" : /notes/i.test(c.label) ? "text" : "raw" }; }) };
  }
  function annualGrid(P, st) {
    var an = P.annual || { cols: [], rows: [] };
    return { id: "annual", unit: "years", q: "", sort: st.sort, rows: an.rows || [],
      cols: (an.cols || []).map(function (c, j) { return { key: c.key, label: c.label, kind: !j ? "year" : /\/riskfree/i.test(c.label) ? "raw" : /yield|change|rate|bond-bill|growth|premium|erp/i.test(c.label) ? "pct" : "raw" }; }) };
  }
  function workedHtml(P) {
    var w = (P.returns || {}).premiums_worked_here || [];
    if (!w.length) return "";
    var h = '<table class="cc-t cc-small"><thead><tr><th class="l">Years</th><th class="r">Stocks, average year</th><th class="r">Stocks, compounded</th><th class="r">Over bills, average</th><th class="r">Over bills, compounded</th><th class="r">Over bonds, average</th><th class="r">Over bonds, compounded</th></tr></thead><tbody>';
    w.forEach(function (x) {
      h += '<tr><td class="l">' + x.from + " to " + x.to + " (" + x.years + ')</td><td class="r">' + pct(x.stocks.arithmetic) + '</td><td class="r">' + pct(x.stocks.compounded) + '</td><td class="r">' + pct(x.stocks_minus_bills.arithmetic) +
        '</td><td class="r">' + pct(x.stocks_minus_bills.compounded) + '</td><td class="r">' + pct(x.stocks_minus_bonds.arithmetic) + '</td><td class="r">' + pct(x.stocks_minus_bonds.compounded) + "</td></tr>";
    });
    return h + "</tbody></table>";
  }
  function sourceHtml(P, K, I) {
    var files = [].concat((P && P.files) || [], (K && K.files) || []);
    var h = '<p class="cc-note">' + esc((P || K || I || {}).attribution || "") + " He shares it with no strings attached; an acknowledgement is all he mentions, and it is given here.</p>" +
      '<p class="cc-note">' + esc((I || P || {}).caution || "") + "</p>";
    if (P && (P.monthly || {}).months_his_file_skips && P.monthly.months_his_file_skips.length) h += '<p class="cc-note">Months his monthly file has no row for: ' + esc(P.monthly.months_his_file_skips.map(month).join(", ")) + ".</p>";
    h += '<table class="cc-t cc-small"><thead><tr><th class="l">His file</th><th class="l">Last changed on his site</th><th class="r">Size</th></tr></thead><tbody>' + files.map(function (f) {
      return '<tr><td class="l">' + (f.url ? '<a href="' + esc(f.url) + '" rel="noopener" target="_blank">' + esc(f.file) + "</a>" : esc(f.file)) + '</td><td class="l">' + esc(f.last_modified || "–") + '</td><td class="r">' + (isNum(f.bytes) ? group(f.bytes) + " bytes" : "–") + "</td></tr>";
    }).join("") + "</tbody></table>";
    return h;
  }

  /* --------------------------------------------------------------------------------------------------- mount */
  function mount(win) {
    var D = win.document, $ = function (id) { return D.getElementById(id); };
    var docs = { P: null, K: null, I: null, T: null }, grids = {};
    var st = { view: "monthly", off: {}, region: "US", topic: null, tregion: "US",
      countries: { q: "", sort: null }, frontier: { q: "", sort: null }, industries: { q: "", sort: null }, table: { q: "", sort: null }, returns: { sort: { j: 0, dir: "desc" } },
      monthly: { sort: { j: 0, dir: "desc" } }, annual: { sort: { j: 0, dir: "desc" } } };
    var lines = [];
    function get(path) { return win.fetch(BASE + path, { cache: "no-cache" }).then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); }); }
    function put(id, html) { var el = $(id); if (el) el.innerHTML = html; }
    function none(what) { return '<p class="cc-none">' + esc(what) + " has not been published yet. The engine runs once a day at 23:17 UTC.</p>"; }
    function drawGrid(id, g) { grids[g.id] = g; put(id, gridHtml(g)); }
    function drawChart() {
      var P = docs.P, box = $("cc-chart");
      if (!P || !box) return;
      var all = st.view === "monthly" ? monthlySeries(P) : annualSeries(P);
      lines = all.filter(function (s) { return !st.off[s.key]; });
      var w = Math.max(320, box.clientWidth || 900), out = chartSvg(lines, w, 320);
      box.innerHTML = out.svg + '<div class="cc-tip" hidden></div>';
      box._x = out.x;
      put("cc-legend", legendHtml(all, st.off));
      put("cc-views", ["monthly", "annual"].map(function (v) { return '<button type="button" class="cc-chip' + (st.view === v ? " on" : "") + '" data-cc-view="' + v + '" aria-pressed="' + (st.view === v) + '">' + (v === "monthly" ? "Each month since " + month((P.monthly || {}).from) : "Each year end since " + ((P.annual || {}).from || "1960")) + "</button>"; }).join(""));
    }
    function hover(ev) {
      var box = $("cc-chart"), x = box && box._x, tip = box && box.querySelector(".cc-tip"), cross = box && box.querySelector(".cc-cross");
      if (!x || !tip || !cross) return;
      var rect = box.getBoundingClientRect(), px = (ev.touches ? ev.touches[0].clientX : ev.clientX) - rect.left;
      if (px < x.L || px > x.w - x.R) { tip.hidden = true; cross.setAttribute("visibility", "hidden"); return; }
      var t = x.x0 + (x.x1 - x.x0) * (px - x.L) / (x.w - x.L - x.R), near = nearest(lines, t);
      if (!near.length) { tip.hidden = true; return; }
      var at = near[0].t, cx = x.L + (x.w - x.L - x.R) * (x.x1 === x.x0 ? 0.5 : (at - x.x0) / (x.x1 - x.x0));
      cross.setAttribute("x1", cx); cross.setAttribute("x2", cx); cross.setAttribute("visibility", "visible");
      var d = new Date(at);
      tip.innerHTML = "<b>" + (st.view === "monthly" ? MONTHS[d.getUTCMonth()] + " " + d.getUTCFullYear() : "End of " + d.getUTCFullYear()) + "</b>" + near.map(function (n) { return '<div><i style="background:' + n.color + '"></i>' + esc(n.label) + "<span>" + (n.v * 100).toFixed(2) + "%</span></div>"; }).join("");
      tip.hidden = false;
      tip.style.left = Math.max(8, cx > x.w / 2 ? cx - 252 : cx + 14) + "px";            // beside the line it reads, on the side with room
    }
    function drawPremium() {
      var P = docs.P;
      if (!P) { put("cc-premium", none("The premium")); return; }
      put("cc-premium", tiles(P));
      put("cc-measures", measuresHtml(P));
      put("cc-asof", "Start of " + esc(month((P.us || {}).as_of)) + " · " + esc(P.source || ""));
      drawChart();
      drawGrid("cc-monthly", monthlyGrid(P, st.monthly));
      drawGrid("cc-annual", annualGrid(P, st.annual));
      put("cc-worked", workedHtml(P));
      drawGrid("cc-returns", returnsGrid(P, st.returns));
    }
    function drawCountries() {
      var K = docs.K;
      if (!K) { put("cc-countries", none("The country table")); return; }
      put("cc-country-facts", '<div class="cc-tiles">' + [["Mature market premium", pct(K.mature_market_premium), "a country rated Aaa"], ["United States", pct(K.us_premium), "rated below Aaa since May 2025"],
        ["Countries", group(K.n || 0), "rated by Moody's" + ((K.frontier || {}).rows ? " · " + K.frontier.rows.length + " frontier markets below" : "")], ["As of", day(K.as_of), "his file " + (K.file || "")]].map(function (t) {
        return '<div class="cc-tile"><div class="cc-lbl">' + esc(t[0]) + '</div><div class="cc-big">' + esc(t[1]) + '</div><div class="cc-sub">' + esc(t[2]) + "</div></div>"; }).join("") + "</div>");
      drawGrid("cc-countries", countryGrid(K, st.countries));
      if ((K.frontier || {}).rows) drawGrid("cc-frontier", frontierGrid(K, st.frontier));
    }
    function drawIndustries() {
      var I = docs.I;
      if (!I) { put("cc-industries", none("The industry table")); put("cc-table", ""); return; }
      put("cc-regions", (I.regions || []).map(function (g) { return '<button type="button" class="cc-chip' + (st.region === g.code ? " on" : "") + '" data-cc-region="' + esc(g.code) + '" aria-pressed="' + (st.region === g.code) + '">' + esc(g.label) + "</button>"; }).join(""));
      put("cc-ind-asof", "His update of " + esc(day(I.as_of)) + " · " + (I.n_tables || 0) + " tables in " + (I.regions || []).length + " regions");
      drawGrid("cc-industries", benchGrid(I, st.region, st.industries));
      var topics = {}, order = [];
      (I.tables || []).forEach(function (t) { if (!topics[t.topic]) { topics[t.topic] = t.title || t.topic; order.push(t.topic); } });
      if (!st.topic || !topics[st.topic]) st.topic = topics.wacc ? "wacc" : order[0];
      put("cc-pick", '<label>Table <select data-cc-topic>' + order.map(function (k) { return '<option value="' + esc(k) + '"' + (k === st.topic ? " selected" : "") + ">" + esc(topics[k]) + "</option>"; }).join("") + "</select></label>" +
        '<label>Region <select data-cc-tregion>' + (I.regions || []).map(function (g) { return '<option value="' + esc(g.code) + '"' + (g.code === st.tregion ? " selected" : "") + ">" + esc(g.label) + "</option>"; }).join("") + "</select></label>");
      loadTable();
    }
    function loadTable() {
      var I = docs.I, t = (I.tables || []).filter(function (x) { return x.topic === st.topic && x.region === st.tregion; })[0];
      if (!t) { put("cc-table-head", ""); put("cc-table", '<p class="cc-none">He publishes no such table for this region.</p>'); return; }
      put("cc-table", '<p class="cc-none">Loading the table.</p>');
      get(t.key).then(function (Tb) {
        docs.T = Tb; st.table = { q: "", sort: null };
        put("cc-table-head", "<b>" + esc(Tb.title || t.topic) + "</b> · " + esc(Tb.region_label || "") + " · his update of " + esc(day(Tb.updated)) + " · " +
          ((Tb.file || {}).url ? '<a href="' + esc(Tb.file.url) + '" rel="noopener" target="_blank">' + esc(Tb.file.file) + "</a>" : esc((Tb.file || {}).file || "")) + " · figures as his file holds them (0.0935 is 9.35%)");
        drawGrid("cc-table", tableGrid(Tb, st.table));
      }, function () { put("cc-table", '<p class="cc-none">The table could not be loaded.</p>'); });
    }
    function redraw(id) {
      if (id === "countries") drawGrid("cc-countries", countryGrid(docs.K, st.countries));
      else if (id === "frontier") drawGrid("cc-frontier", frontierGrid(docs.K, st.frontier));
      else if (id === "industries") drawGrid("cc-industries", benchGrid(docs.I, st.region, st.industries));
      else if (id === "table") drawGrid("cc-table", tableGrid(docs.T, st.table));
      else if (id === "returns") drawGrid("cc-returns", returnsGrid(docs.P, st.returns));
      else if (id === "monthly") drawGrid("cc-monthly", monthlyGrid(docs.P, st.monthly));
      else if (id === "annual") drawGrid("cc-annual", annualGrid(docs.P, st.annual));
    }
    var host = $("cc-host") || D.body;
    host.addEventListener("click", function (ev) {
      var b = ev.target.closest ? ev.target.closest("button") : null;
      if (!b) return;
      var d = b.dataset, id;
      if (d.ccSort) {
        id = d.ccSort; var s = st[id], j = +d.j, g = grids[id];
        var first = g && g.cols[j] && (!g.cols[j].kind || g.cols[j].kind === "text" || g.cols[j].kind === "year" || g.cols[j].kind === "month") ? "asc" : "desc";
        s.sort = s.sort && s.sort.j === j ? { j: j, dir: s.sort.dir === "asc" ? "desc" : "asc" } : { j: j, dir: first };
        redraw(id);
        var again = host.querySelector('button[data-cc-sort="' + id + '"][data-j="' + j + '"]'); if (again && again.focus) again.focus();
      } else if (d.ccCsv) {
        var gg = grids[d.ccCsv]; if (!gg) return;
        var a = D.createElement("a"), url = win.URL.createObjectURL(new win.Blob([csv(gg.cols, shown(gg))], { type: "text/csv;charset=utf-8" }));
        a.href = url; a.download = "damodaran-" + d.ccCsv + (d.ccCsv === "industries" ? "-" + st.region.toLowerCase() : d.ccCsv === "table" && docs.T ? "-" + docs.T.id : "") + ".csv"; a.style.display = "none";
        host.appendChild(a); a.click(); host.removeChild(a); win.setTimeout(function () { win.URL.revokeObjectURL(url); }, 4000);
      } else if (d.ccView) { st.view = d.ccView; st.off = {}; drawChart(); }
      else if (d.ccLine) { st.off[d.ccLine] = !st.off[d.ccLine]; drawChart(); }
      else if (d.ccRegion) { st.region = d.ccRegion; st.industries = { q: st.industries.q, sort: st.industries.sort }; drawIndustriesKeep(); }
    });
    function drawIndustriesKeep() {
      var I = docs.I;
      put("cc-regions", (I.regions || []).map(function (g) { return '<button type="button" class="cc-chip' + (st.region === g.code ? " on" : "") + '" data-cc-region="' + esc(g.code) + '" aria-pressed="' + (st.region === g.code) + '">' + esc(g.label) + "</button>"; }).join(""));
      drawGrid("cc-industries", benchGrid(I, st.region, st.industries));
    }
    host.addEventListener("input", function (ev) {
      var id = ev.target && ev.target.dataset ? ev.target.dataset.ccQ : null;
      if (!id) return;
      st[id].q = ev.target.value;
      var g = grids[id]; g.q = st[id].q;
      var rows = shown(g), box = ev.target.closest(".cc-grid") || host, body = box.querySelector("tbody"), count = box.querySelector('[data-cc-count="' + id + '"]');
      var tmp = D.createElement("div"); tmp.innerHTML = gridHtml(g);                       // the search box keeps its focus: only the rows and the count change
      if (body) body.innerHTML = tmp.querySelector("tbody").innerHTML;
      if (count) count.textContent = tmp.querySelector(".cc-count").textContent;
    });
    host.addEventListener("change", function (ev) {
      var d = ev.target && ev.target.dataset;
      if (d && "ccTopic" in d) { st.topic = ev.target.value; loadTable(); }
      else if (d && "ccTregion" in d) { st.tregion = ev.target.value; loadTable(); }
    });
    var chart = $("cc-chart");
    if (chart) { chart.addEventListener("mousemove", hover); chart.addEventListener("touchmove", hover, { passive: true }); chart.addEventListener("mouseleave", function () { var t = chart.querySelector(".cc-tip"), c = chart.querySelector(".cc-cross"); if (t) t.hidden = true; if (c) c.setAttribute("visibility", "hidden"); }); }
    var sizing = null;
    win.addEventListener("resize", function () { win.clearTimeout(sizing); sizing = win.setTimeout(drawChart, 150); });
    var jobs = [get("data/equity-risk-premium.json").then(function (j) { docs.P = j; }, function () {}).then(drawPremium),
      get("data/country-risk-premiums.json").then(function (j) { docs.K = j; }, function () {}).then(drawCountries),
      get("data/industry-benchmarks.json").then(function (j) { docs.I = j; }, function () {}).then(drawIndustries)];
    return win.Promise.all(jobs).then(function () { put("cc-source", sourceHtml(docs.P, docs.K, docs.I)); return { docs: docs, state: st, grids: grids }; });
  }

  return { SERIES: SERIES, esc: esc, pct: pct, raw: raw, day: day, month: month, cell: cell, sorted: sorted, filtered: filtered, csv: csv, shown: shown, gridHtml: gridHtml, tiles: tiles,
    measuresHtml: measuresHtml, monthlySeries: monthlySeries, annualSeries: annualSeries, chartSvg: chartSvg, nearest: nearest, legendHtml: legendHtml, countryGrid: countryGrid,
    frontierGrid: frontierGrid, benchGrid: benchGrid, tableGrid: tableGrid, returnsGrid: returnsGrid, monthlyGrid: monthlyGrid, annualGrid: annualGrid, workedHtml: workedHtml, sourceHtml: sourceHtml, mount: mount };
});
