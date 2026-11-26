/* jh-money-map.js - Money flow: where money is flowing in and out, for every ETF in the warehouse.
 *
 * Khalid 2026-11-25: "Enhance that engine and tracks what etfs and sectors is the money flowing out and flowing into and
 * covers as many asset classes and etfs as possible. And add a button on charts.html for it call it flow."
 *
 * Reads what justhodl-global-flow-desk publishes beside its research view (flow_map 3.0.0):
 *   /data/global-flow-map.json            totals, groups (asset classes, sectors, countries ...), boards, quality, method
 *   /data/global-flow-map-funds.json      every fund: 34 columns
 *   /data/global-flow-map-history.json    260 days of each group, a day at a time
 *   /data/global-flow-map/funds/<A>.json  each fund's own 130 days, for its sheet
 * It measures nothing: every dollar on the page is the document's. One thing is computed here, from figures the documents
 * carry: audit() adds the funds up again and holds every group row and total to them; the page prints the result.
 *
 * The research view that was this page (jh-flow-research*.js) is untouched and one click away (Research view).
 *
 * Pure functions are exported for node tests (module.exports) and the browser (window.JHMoneyMap).
 *
 * jh-reskin-skip: on this page a colour is a measurement (money in, money out). The site build's re-skin
 * (scripts/reskin_site.py) turns cool hues into the site's amber; every colour that carries meaning is therefore defined in
 * this file, which the re-skin leaves alone. The frame uses the site's own palette.
 */
(function (root, factory) {
  var api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.JHMoneyMap = api;
})(typeof window !== "undefined" ? window : null, function (root) {
  "use strict";

  var VERSION = "1.0.0";
  var DOCS = { main: "/data/global-flow-map.json", funds: "/data/global-flow-map-funds.json", history: "/data/global-flow-map-history.json", shard: "/data/global-flow-map/funds/" };
  // full: the flow, as a percentage of assets, that gets the full colour on the rotation matrix (a day moves less than a year)
  var WINDOWS = [
    { id: "d1", label: "1D", word: "the last reporting day", full: 0.4 },
    { id: "w1", label: "1W", word: "the last five reporting days", full: 1.2 },
    { id: "m1", label: "1M", word: "the last 21 reporting days", full: 3 },
    { id: "m3", label: "3M", word: "the last 63 reporting days", full: 10 },
    { id: "ytd", label: "YTD", word: "this year", full: 35 },
    { id: "y1", label: "1Y", word: "the last 252 reporting days", full: 40 }
  ];
  var WIN = {};
  WINDOWS.forEach(function (w) { WIN[w.id] = w; });
  // the same two measured palettes as the Risk Map (2026-11-24): each pair passes the colour-blind separation check
  var PALETTES = {
    rg: { name: "Red and green", neg: "#f23645", pos: "#089981", mid: "#363a45", none: "#20242e", up: "#2bb89f", dn: "#f7525f" },
    cb: { name: "Orange and blue", neg: "#e8623a", pos: "#3b82f6", mid: "#363a45", none: "#20242e", up: "#6aa8ff", dn: "#f08c5a" }
  };
  var LEV = "Leveraged & inverse";
  var NO_CLASS = "No class given";
  // every table of the document, in the order the page shows it; pick: which funds stand behind a row
  var KINDS = [
    { id: "segments", title: "Asset classes", sub: "Every fund is in exactly one row, so the rows add up to the total.", open: 99,
      pick: function (f, id) { return f.seg === id; } },
    { id: "classes", title: "The provider's own six classes", sub: "Unleveraged funds by the class ETF Global gives them. Crypto funds are in its Currency class.", open: 99,
      pick: function (f, id) { return f.seg !== LEV && (f.ac || NO_CLASS) === id; } },
    { id: "sectors", title: "Sectors \u00b7 sector funds", sub: "Funds that hold one sector, by sector.", open: 99,
      pick: function (f, id) { return f.seg !== LEV && f.sec === id; } },
    { id: "sectors_through", title: "Sectors \u00b7 through every equity fund", sub: "Each unleveraged equity fund's flow split by the weights it holds in each sector, so money into an S&P 500 fund counts here too.", open: 99,
      pick: function (f, id) { return f.seg !== LEV && f.sec === id; }, through: true },
    { id: "countries", title: "Countries", sub: "Equity funds with 60% or more in one country other than the United States.", open: 12,
      pick: function (f, id) { return f.seg !== LEV && f.cty === id; } },
    { id: "regions", title: "Regions", sub: "Unleveraged equity funds by the provider's region.", open: 10,
      pick: function (f, id) { return f.seg !== LEV && (f.ac === "Equity" || f.ac === "Real Estate") && f.reg === id; } },
    { id: "styles", title: "Equity styles", sub: "Unleveraged equity funds outside the sector funds, by the provider's focus.", open: 10,
      pick: function (f, id) { return f.seg !== LEV && f.seg.indexOf("Equity \u00b7") === 0 && f.seg !== "Equity \u00b7 sector funds" && f.sub === id; } },
    { id: "bonds", title: "Bonds", sub: "Unleveraged bond funds by kind.", open: 99,
      pick: function (f, id) { return f.seg === id; } },
    { id: "bond_maturity", title: "Bonds by maturity", sub: "By the maturities the fund holds.", open: 99,
      pick: function (f, id) { return f.seg !== LEV && f.ac === "Fixed Income" && f.mat === id; } },
    { id: "commodities", title: "Commodities", sub: "Unleveraged commodity funds by what they hold.", open: 10,
      pick: function (f, id) { return f.seg !== LEV && f.ac === "Commodities" && f.sub === id; } },
    { id: "crypto", title: "Crypto", sub: "Spot and futures crypto funds.", open: 99,
      pick: function (f, id) { return f.seg === "Crypto" && f.sub === id; } },
    { id: "leveraged", title: "Leveraged & inverse", sub: "Bets for and against, kept apart from everything above so a bet against the market is never netted against a purchase.", open: 99,
      pick: function (f, id) { return f.seg === LEV && f.sub === id; } },
    { id: "issuers", title: "Issuers", sub: "The sixty issuers taking in or losing the most this week, all their funds together.", open: 10,
      pick: function (f, id) { return f.issuer === id; } },
    { id: "categories", title: "The provider's categories", sub: "Class and category.", open: 10,
      pick: function (f, id) { return f.seg !== LEV && f.ac && f.cat && (f.ac + " \u00b7 " + f.cat) === id; } },
    { id: "focus", title: "The provider's focus groups", sub: "Class, category and focus, where two or more funds share them.", open: 10,
      pick: function (f, id) { return f.seg !== LEV && f.cat && f.foc && (f.ac + " \u00b7 " + f.cat + " \u00b7 " + f.foc) === id; } }
  ];
  var KIND = {};
  KINDS.forEach(function (k) { KIND[k.id] = k; });
  var COLS = {            // the funds document's 34 columns: label, kind (text | usd | pct | num | int | date), note
    t: ["Ticker", "text"], name: ["Fund", "text"], issuer: ["Issuer", "text"], typ: ["Type", "text"], seg: ["Asset class", "text"], ac: ["Provider class", "text"],
    cat: ["Category", "text"], foc: ["Focus", "text"], reg: ["Region", "text"], sec: ["Sector", "text"], cty: ["Country", "text"], mat: ["Bond maturity", "text"],
    sub: ["Group", "text"], lev: ["Leverage", "num"], assets: ["Assets", "usd0"], nav: ["NAV", "num"], shares: ["Shares", "int"], newest: ["Newest row", "date"],
    behind: ["Days behind", "int"], d1: ["1D", "usd"], w1: ["1W", "usd"], m1: ["1M", "usd"], m3: ["3M", "usd"], ytd: ["YTD", "usd"], y1: ["1Y", "usd"],
    p_w1: ["1W % of assets", "pct"], p_m1: ["1M %", "pct"], p_m3: ["3M %", "pct"], prior_w1: ["Week before", "usd"], z5: ["Week vs own year", "z"],
    streak: ["Days in a row", "streak"], chk: ["Issuer's count", "chk"], flags: ["Notes", "flags"], inc: ["Started", "date"]
  };
  var TABLE_COLS = ["t", "name", "seg", "issuer", "assets", "d1", "w1", "m1", "m3", "ytd", "y1", "p_w1", "p_m1", "streak", "z5", "flags"];
  var MINUS = "\u2212";

  // ------------------------------------------------------------------------------------------------ small things
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; });
  }
  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function usd(v, signed) {
    if (!isNum(v)) return "n/a";
    var a = Math.abs(v), s = v < 0 ? MINUS : (signed && v > 0 ? "+" : "");
    var body = a >= 1e12 ? (a / 1e12).toFixed(2) + "T" : a >= 1e9 ? (a / 1e9).toFixed(a >= 1e11 ? 0 : a >= 1e10 ? 1 : 2) + "B" : a >= 1e6 ? (a / 1e6).toFixed(a >= 1e8 ? 0 : 1) + "M"
      : a >= 1e3 ? (a / 1e3).toFixed(0) + "K" : a.toFixed(0);
    return s + "$" + body;
  }
  function flow(v) { return usd(v, true); }
  function pct(v, digits) {
    if (!isNum(v)) return "n/a";
    var a = Math.abs(v), d = digits == null ? (a >= 100 ? 0 : a >= 10 ? 1 : 2) : digits;
    return (v < 0 ? MINUS : v > 0 ? "+" : "") + a.toFixed(d) + "%";
  }
  function int(v) { return isNum(v) ? Math.round(v).toLocaleString("en-US") : "n/a"; }
  function dayName(iso) {
    if (!/^\d{4}-\d{2}-\d{2}/.test(String(iso))) return String(iso || "");
    var d = new Date(String(iso).slice(0, 10) + "T12:00:00Z");
    return ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"][d.getUTCDay()] + " " + d.getUTCDate() + " " + ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][d.getUTCMonth()] + " " + d.getUTCFullYear();
  }
  function sign(v) { return !isNum(v) || v === 0 ? "flat" : v > 0 ? "pos" : "neg"; }

  // ------------------------------------------------------------------------------------------------ the documents
  function fundObjects(fdoc) {
    var cols = (fdoc && fdoc.cols) || [];
    return ((fdoc && fdoc.rows) || []).map(function (r) {
      var o = {};
      for (var i = 0; i < cols.length; i++) o[cols[i]] = r[i];
      return o;
    });
  }
  function isCurrent(f, doc) { return f.behind <= ((doc && doc.quality && doc.quality.late_rule_days) || 3); }
  function rowsOf(doc, kind, win, measure) {
    var rows = ((doc.groups || {})[kind] || []).map(function (g) {
      var w = (g.w || {})[win] || {};
      return { id: g.id, n: g.n, assets: g.assets, net: w.net, "in": w["in"], out: w.out, n_in: w.n_in, n_out: w.n_out, pct: w.pct, prior: w.prior, top_in: w.top_in || [], top_out: w.top_out || [], w: g.w };
    });
    var key = measure === "pct" ? "pct" : "net";
    rows.sort(function (a, b) { return (isNum(b[key]) ? b[key] : -Infinity) - (isNum(a[key]) ? a[key] : -Infinity) || (a.id < b.id ? -1 : 1); });
    return rows;
  }
  function members(funds, doc, kind, id) {
    var k = KIND[kind];
    return funds.filter(function (f) { return isCurrent(f, doc) && k.pick(f, id); });
  }
  function headline(doc, win) {
    var t = (((doc.totals || {}).plain || {}).w || {})[win] || {}, lev = (((doc.totals || {}).leveraged || {}).w || {})[win] || {};
    var seg = rowsOf(doc, "segments", win).filter(function (r) { return r.id !== LEV && isNum(r.net); });
    return { net: t.net, "in": t["in"], out: t.out, n_in: t.n_in, n_out: t.n_out, n: (doc.totals.plain || {}).n, assets: (doc.totals.plain || {}).assets,
      lev_net: lev.net, lev_n: (doc.totals.leveraged || {}).n, top: seg[0] || null, bottom: seg.length ? seg[seg.length - 1] : null };
  }
  function reading(doc, win) {
    var h = headline(doc, win), w = WIN[win];
    if (!isNum(h.net)) return "";
    var s = "Over " + w.word + " a net " + usd(Math.abs(h.net)) + (h.net >= 0 ? " went into " : " came out of ") + int(h.n) + " unleveraged ETFs: " + usd(h["in"]) + " into the " + int(h.n_in)
      + " that took money in, " + usd(Math.abs(h.out)) + " out of the " + int(h.n_out) + " that lost it.";
    function tops(r, key) { return r[key].length ? " (" + r[key].map(function (x) { return x[0]; }).join(", ") + ")" : ""; }
    if (h.top && h.top.net > 0) s += " Most in: " + h.top.id + " " + flow(h.top.net) + tops(h.top, "top_in") + ".";
    if (h.bottom && h.bottom.net < 0) s += " Most out: " + h.bottom.id + " " + flow(h.bottom.net) + tops(h.bottom, "top_out") + ".";
    return s;
  }
  // The funds added up again: every asset-class row and the totals, in every window, against the funds document.
  function audit(doc, funds) {
    var out = { rows: 0, wrong: [], funds: funds.length, current: 0, ok: false };
    if (!doc || !funds.length || !(doc.groups || {}).segments) return out;
    var cur = funds.filter(function (f) { return isCurrent(f, doc); });
    out.current = cur.length;
    WINDOWS.forEach(function (w) {
      var by = {}, all = 0, plain = 0;
      cur.forEach(function (f) {
        var v = f[w.id] || 0;
        by[f.seg] = (by[f.seg] || 0) + v;
        all += v;
        if (f.seg !== LEV) plain += v;
      });
      doc.groups.segments.forEach(function (g) {
        out.rows++;
        var said = ((g.w || {})[w.id] || {}).net || 0;
        if (Math.abs(said - (by[g.id] || 0)) > Math.max(5, 1e-9 * Math.abs(said))) out.wrong.push([g.id, w.id, said, by[g.id] || 0]);
      });
      [["all", all], ["plain", plain]].forEach(function (p) {
        out.rows++;
        var said = ((((doc.totals || {})[p[0]] || {}).w || {})[w.id] || {}).net || 0;
        if (Math.abs(said - p[1]) > Math.max(5, 1e-9 * Math.abs(said))) out.wrong.push(["total " + p[0], w.id, said, p[1]]);
      });
    });
    if (cur.length !== (doc.quality || {}).funds_current) out.wrong.push(["current funds", "", (doc.quality || {}).funds_current, cur.length]);
    out.ok = !out.wrong.length;
    return out;
  }

  // ------------------------------------------------------------------------------------------------ colour
  function hexRgb(h) { return [parseInt(h.slice(1, 3), 16), parseInt(h.slice(3, 5), 16), parseInt(h.slice(5, 7), 16)]; }
  // a flow as a share of assets -> a cell colour: the neutral tone at zero, the full colour at +/- full
  function heat(p, full, pal) {
    if (!isNum(p)) return pal.none;
    var t = Math.max(-1, Math.min(1, p / full)), a = hexRgb(pal.mid), b = hexRgb(t >= 0 ? pal.pos : pal.neg), k = Math.pow(Math.abs(t), 0.6);
    return "rgb(" + [0, 1, 2].map(function (i) { return Math.round(a[i] + (b[i] - a[i]) * k); }).join(",") + ")";
  }

  // where zero sits on a row of bars and what one unit is worth, in percent of the track: one scale for both sides
  function axis(values) {
    var hi = 0, lo = 0;
    values.forEach(function (v) { if (isNum(v)) { if (v > hi) hi = v; if (-v > lo) lo = -v; } });
    if (!hi && !lo) return { zero: 50, unit: 0 };
    var zero = Math.max(lo ? 6 : 0.5, Math.min(hi ? 94 : 99.5, 100 * lo / (hi + lo)));
    return { zero: zero, unit: Math.min(hi ? (100 - zero) / hi : Infinity, lo ? zero / lo : Infinity) };
  }

  // ------------------------------------------------------------------------------------------------ the fund table
  function searchFunds(funds, q) {
    q = String(q || "").trim().toUpperCase();
    if (!q) return funds;
    var exact = [], starts = [], rest = [];
    funds.forEach(function (f) {
      if (f.t === q) exact.push(f);
      else if (f.t.indexOf(q) === 0) starts.push(f);
      else if (String(f.name || "").toUpperCase().indexOf(q) >= 0 || String(f.issuer || "").toUpperCase().indexOf(q) >= 0) rest.push(f);
    });
    return exact.concat(starts, rest);
  }
  function sortFunds(funds, col, dir) {
    var text = COLS[col] && (COLS[col][1] === "text" || COLS[col][1] === "date"), m = dir === "asc" ? 1 : -1;
    function key(f) { var v = f[col]; return col === "flags" ? (v || []).length : v; }
    return funds.slice().sort(function (a, b) {
      var x = key(a), y = key(b), nx = x == null || x === "", ny = y == null || y === "";
      if (nx || ny) return nx && ny ? (a.t < b.t ? -1 : 1) : nx ? 1 : -1;            // an empty cell is last either way
      if (text) return m * String(x).localeCompare(String(y)) || (a.t < b.t ? -1 : 1);
      return m * (x - y) || (a.t < b.t ? -1 : 1);
    });
  }
  function csvOf(funds, cols) {
    function cell(v) {
      if (v == null) return "";
      if (Array.isArray(v)) v = v.join("; ");
      v = String(v);
      return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
    }
    return [cols.join(",")].concat(funds.map(function (f) { return cols.map(function (c) { return cell(f[c]); }).join(","); })).join("\n") + "\n";
  }
  function cellText(col, v) {
    var k = (COLS[col] || [])[1];
    if (v == null || v === "") return "";
    if (k === "usd") return flow(v);
    if (k === "usd0") return usd(v);
    if (k === "pct") return pct(v);
    if (k === "int") return int(v);
    if (k === "num") return isNum(v) ? (Math.abs(v) >= 1000 ? v.toFixed(0) : v.toFixed(2)) : String(v);
    if (k === "z") return isNum(v) ? (v > 0 ? "+" : v < 0 ? MINUS : "") + Math.abs(v).toFixed(1) + "\u03c3" : "";
    if (k === "streak") return !v ? "" : Math.abs(v) + (v > 0 ? " in" : " out");
    if (k === "chk") return v === 1 ? "matches (next day)" : v === 0 ? "matches (same day)" : v === -1 ? "differs" : "";
    if (k === "flags") return (v || []).map(function (x) { return x.slice(5); }).join("; ");
    return String(v);
  }

  // ------------------------------------------------------------------------------------------------ history
  function seriesOf(hist, key, span, cumulative) {
    var all = ((hist || {}).series || {})[key] || [], dates = (hist || {}).dates || [];
    var n = Math.min(span || all.length, all.length), vals = all.slice(all.length - n), ds = dates.slice(dates.length - n), run = 0;
    if (cumulative) vals = vals.map(function (v) { run += v || 0; return run; });
    return { dates: ds, values: vals, sum: all.slice(all.length - n).reduce(function (a, b) { return a + (b || 0); }, 0) };
  }
  function niceMax(v) {
    if (!(v > 0)) return 1;
    var p = Math.pow(10, Math.floor(Math.log(v) / Math.LN10)), m = v / p;
    return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 2.5 ? 2.5 : m <= 5 ? 5 : 10) * p;
  }
  // bars for a day at a time, a line for the running total; one axis either way
  function chartSvg(s, pal, opt) {
    opt = opt || {};
    var W = 1000, H = opt.h || 250, L = 62, R = 10, T = 12, B = 24, n = s.values.length;
    if (!n) return '<div class="mm-empty">No history for this row.</div>';
    var hi = Math.max.apply(null, s.values.map(function (v) { return v || 0; }).concat([0])), lo = Math.min.apply(null, s.values.map(function (v) { return v || 0; }).concat([0]));
    var top = niceMax(Math.max(hi, -lo)), y0 = lo < 0 ? -top : 0, y1 = hi > 0 ? top : 0;
    if (y0 === y1) y1 = 1;
    function Y(v) { return T + (H - T - B) * (1 - (v - y0) / (y1 - y0)); }
    function X(i) { return L + (W - L - R) * (i + 0.5) / n; }
    var out = ['<svg viewBox="0 0 ' + W + " " + H + '" class="mm-chart" role="img" aria-label="' + esc(opt.label || "flow history") + '" data-n="' + n + '">'];
    [y1, y1 / 2, 0, y0 / 2, y0].filter(function (v, i, a) { return a.indexOf(v) === i; }).forEach(function (v) {
      out.push('<line x1="' + L + '" x2="' + (W - R) + '" y1="' + Y(v).toFixed(1) + '" y2="' + Y(v).toFixed(1) + '" class="' + (v === 0 ? "mm-axis0" : "mm-grid") + '"/>'
        + '<text x="' + (L - 6) + '" y="' + (Y(v) + 4).toFixed(1) + '" text-anchor="end" class="mm-tick">' + esc(usd(v, true)) + "</text>");
    });
    var last = "";
    s.dates.forEach(function (d, i) {
      var m = d.slice(0, 7);
      if (m !== last && i > 0 && n / 12 < 40) out.push('<text x="' + X(i).toFixed(1) + '" y="' + (H - 7) + '" text-anchor="middle" class="mm-tick">' + ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][+d.slice(5, 7) - 1] + "</text>");
      last = m;
    });
    if (opt.cumulative) {
      var pts = s.values.map(function (v, i) { return X(i).toFixed(1) + "," + Y(v || 0).toFixed(1); });
      var end = s.values[n - 1] || 0;
      out.push('<polygon points="' + X(0).toFixed(1) + "," + Y(0).toFixed(1) + " " + pts.join(" ") + " " + X(n - 1).toFixed(1) + "," + Y(0).toFixed(1) + '" fill="' + (end >= 0 ? pal.pos : pal.neg) + '" opacity=".16"/>'
        + '<polyline points="' + pts.join(" ") + '" fill="none" stroke="' + (end >= 0 ? pal.up : pal.dn) + '" stroke-width="2" stroke-linejoin="round"/>');
    } else {
      var bw = Math.max(1, (W - L - R) / n - (n > 140 ? 0.6 : 1.5));
      s.values.forEach(function (v, i) {
        if (!v) return;
        var a = Y(Math.max(v, 0)), b = Y(Math.min(v, 0));
        out.push('<rect x="' + (X(i) - bw / 2).toFixed(1) + '" y="' + a.toFixed(1) + '" width="' + bw.toFixed(1) + '" height="' + Math.max(0.8, b - a).toFixed(1) + '" fill="' + (v > 0 ? pal.pos : pal.neg) + '"/>');
      });
    }
    out.push('<line class="mm-cross" x1="0" x2="0" y1="' + T + '" y2="' + (H - B) + '" style="display:none"/><rect class="mm-hit" x="' + L + '" y="' + T + '" width="' + (W - L - R) + '" height="' + (H - T - B) + '" fill="transparent"/></svg>');
    return out.join("");
  }

  var api = { VERSION: VERSION, DOCS: DOCS, WINDOWS: WINDOWS, PALETTES: PALETTES, KINDS: KINDS, COLS: COLS, TABLE_COLS: TABLE_COLS, LEV: LEV,
    esc: esc, usd: usd, flow: flow, pct: pct, int: int, dayName: dayName, fundObjects: fundObjects, isCurrent: isCurrent, rowsOf: rowsOf, members: members, headline: headline,
    reading: reading, audit: audit, heat: heat, axis: axis, searchFunds: searchFunds, sortFunds: sortFunds, csvOf: csvOf, cellText: cellText, seriesOf: seriesOf, niceMax: niceMax, chartSvg: chartSvg };
  if (!root || !root.document) return api;

  // ================================================================================================ the page
  var doc = null, fdoc = null, funds = [], byT = {}, hist = null, shards = {}, el = null;
  var st = { win: "w1", measure: "usd", pal: "rg", open: {}, q: "", fseg: "", fsec: "", fcty: "", fiss: "", late: false, sort: "assets", dir: "desc", page: 0, allcols: false,
    hkey: "total:plain", hspan: 130, hcum: false, board: "usd", boardN: 15, view: "map" };
  try { var saved = JSON.parse(root.localStorage.getItem("jh-money-map") || "{}"); if (PALETTES[saved.pal]) st.pal = saved.pal; if (WIN[saved.win]) st.win = saved.win; } catch (e) { /* a private window keeps nothing */ }
  function remember() { try { root.localStorage.setItem("jh-money-map", JSON.stringify({ pal: st.pal, win: st.win })); } catch (e) { /* nothing to keep it in */ } }
  function pal() { return PALETTES[st.pal]; }
  var embedded = false;
  try { embedded = root.self !== root.top; } catch (e) { embedded = true; }

  var CSS = [
    ".mm{--mm-bg:#0C0B09;--mm-panel:#17150E;--mm-panel2:#1d1a12;--mm-line:#2B2820;--mm-line2:#3A3628;--mm-ink:#e8e2d4;--mm-ink2:#b5ad99;--mm-ink3:#8a836f;--mm-acc:#F0B429;",
    "background:var(--mm-bg);color:var(--mm-ink);font:14px/1.45 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:12px 14px 40px;max-width:1900px;margin:0 auto}",
    ".mm *{box-sizing:border-box}.mm td,.mm th{border-bottom:0;min-width:0;vertical-align:middle}.mm button,.mm select,.mm input{font:inherit;min-height:0;min-width:0;border-radius:6px;border:1px solid var(--mm-line2);background:var(--mm-panel);color:var(--mm-ink);padding:5px 10px}",
    ".mm button{cursor:pointer}.mm a{color:var(--mm-acc)}.mm h1{font-size:21px;margin:0;color:#fff;font-weight:700;line-height:1.2}.mm h1 span{font-size:13px;color:var(--mm-ink3);font-weight:500;margin-left:8px}",
    ".mm h2{font-size:16px;margin:0;color:#fff;font-weight:650}.mm-top{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:8px}",
    ".mm-seg{display:inline-flex;border:1px solid var(--mm-line2);border-radius:7px;overflow:hidden}.mm-seg button{border:0;border-radius:0;background:none;color:var(--mm-ink2);padding:6px 11px;font-weight:550}",
    ".mm-seg button.on{background:var(--mm-line2);color:#fff}.mm-seg button+button{border-left:1px solid var(--mm-line)}.mm-right{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap;align-items:center}",
    ".mm-asof{color:var(--mm-ink2);font-size:12.5px;margin:0 0 10px}.mm-asof b{color:var(--mm-ink)}",
    ".mm-bar0{position:sticky;top:0;z-index:20;background:var(--mm-bg);padding:7px 0;display:flex;gap:8px;flex-wrap:wrap;align-items:center;border-bottom:1px solid var(--mm-line);margin-bottom:10px}",
    ".mm-jump{display:flex;gap:4px;flex-wrap:wrap;margin:0 0 12px}.mm-jump a{font-size:12px;color:var(--mm-ink2);text-decoration:none;border:1px solid var(--mm-line);border-radius:999px;padding:2px 9px}.mm-jump a:hover{color:#fff;border-color:var(--mm-line2)}",
    ".mm-tiles{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-bottom:8px}.mm-tile{background:var(--mm-panel);border:1px solid var(--mm-line);border-radius:8px;padding:10px 12px;min-width:0}",
    ".mm-k{font-size:10.5px;text-transform:uppercase;letter-spacing:.06em;color:var(--mm-ink3);font-weight:650}.mm-v{font-size:24px;font-weight:720;letter-spacing:-.01em;line-height:1.2;color:#fff;white-space:nowrap}",
    ".mm-v.mm-vt{font-size:15.5px;white-space:normal;line-height:1.25;min-height:29px;display:flex;align-items:center}.mm-s{font-size:12px;color:var(--mm-ink2);overflow:hidden;text-overflow:ellipsis}.mm-read{background:var(--mm-panel);border:1px solid var(--mm-line);border-left:3px solid var(--mm-acc);border-radius:8px;padding:9px 12px;margin-bottom:14px;color:var(--mm-ink);font-size:13.5px}",
    ".mm-card{background:var(--mm-panel);border:1px solid var(--mm-line);border-radius:8px;padding:12px 14px;margin-bottom:12px;min-width:0}.mm-card,.mm-two{scroll-margin-top:58px}.mm-card>header{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin-bottom:8px}",
    ".mm-card>header p{margin:0;color:var(--mm-ink3);font-size:12px;flex:1 1 260px}.mm-two{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:12px}.mm-two>.mm-card{margin-bottom:0}.mm-gap{height:12px}.mm-in{gap:26px}.mm h3{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--mm-ink3);margin:0 0 4px;font-weight:650}",
    ".mm-rows{display:grid;grid-template-columns:minmax(120px,230px) minmax(120px,1fr) 86px 70px;column-gap:10px;row-gap:1px;align-items:center}",
    ".mm-row{display:contents;cursor:pointer}.mm-row>*{padding:3px 0;border-bottom:1px solid transparent}.mm-row:hover>*{background:var(--mm-panel2)}.mm-name{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:13px}",
    ".mm-name small{color:var(--mm-ink3);font-size:11px;margin-left:5px}.mm-track{position:relative;height:18px}.mm-track:before{content:'';position:absolute;left:var(--z,50%);top:0;bottom:0;width:1px;background:var(--mm-line2)}",
    ".mm-track i{position:absolute;top:3px;height:12px;border-radius:0 3px 3px 0;left:var(--z,50%)}.mm-track i.neg{left:auto;right:calc(100% - var(--z,50%));border-radius:3px 0 0 3px}",
    ".mm-num{text-align:right;font-variant-numeric:tabular-nums;font-size:13px;white-space:nowrap}.mm-num.sub{color:var(--mm-ink3);font-size:12px}.mm-pos{color:var(--mm-up)}.mm-neg{color:var(--mm-dn)}",
    ".mm-more{margin-top:8px;font-size:12px;color:var(--mm-ink2)}.mm-more button{font-size:12px;padding:3px 9px}",
    ".mm-matrix{width:100%;border-collapse:separate;border-spacing:2px;font-size:12px}.mm-matrix th{font-weight:600;color:var(--mm-ink3);font-size:11px;text-align:center;padding:2px 4px}.mm-matrix th:first-child,.mm-matrix td:first-child{text-align:left;white-space:nowrap;max-width:230px;overflow:hidden;text-overflow:ellipsis;color:var(--mm-ink);font-size:12.5px;padding-right:8px}",
    ".mm-matrix td.c{text-align:center;border-radius:4px;padding:5px 4px;font-variant-numeric:tabular-nums;color:#fff;cursor:pointer;min-width:62px;white-space:nowrap}.mm-matrix td.c small{display:block;font-size:10px;opacity:.82}",
    ".mm-scroll{overflow-x:auto;max-width:100%}.mm-board{display:grid;grid-template-columns:22px 62px minmax(0,1fr) minmax(60px,34%) 76px 62px;column-gap:8px;align-items:center}.mm-brow{display:contents;cursor:pointer}.mm-brow>*{padding:3px 0}.mm-brow:hover>*{background:var(--mm-panel2)}",
    ".mm-rank{color:var(--mm-ink3);font-size:11px;text-align:right}.mm-tk{font-weight:700;color:#fff;font-size:13px}.mm-tk em{font-style:normal;color:var(--mm-acc);margin-left:3px;font-size:11px}.mm-fn{color:var(--mm-ink2);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}",
    ".mm-one{position:relative;height:14px}.mm-one i{position:absolute;left:0;top:1px;height:12px;border-radius:0 3px 3px 0}",
    ".mm-tablewrap{overflow:auto;max-width:100%;border:1px solid var(--mm-line);border-radius:6px}.mm-table{border-collapse:collapse;width:100%;font-size:12.5px}.mm-table th,.mm-table td{padding:5px 8px;border-bottom:1px solid var(--mm-line);white-space:nowrap;min-width:0;text-align:right;vertical-align:middle}",
    ".mm-table th{position:sticky;top:0;background:var(--mm-panel2);color:var(--mm-ink2);font-weight:600;font-size:11.5px;cursor:pointer;user-select:none}.mm-table th.on{color:#fff}.mm-table td.l,.mm-table th.l{text-align:left}.mm-table td.nm{max-width:300px;overflow:hidden;text-overflow:ellipsis}",
    ".mm-table tr.r{cursor:pointer}.mm-table tr.r:hover td{background:var(--mm-panel2)}.mm-table tr.late td{color:var(--mm-ink3)}.mm-filters{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px}.mm-filters input[type=search]{width:240px}",
    ".mm-filters select{max-width:210px}.mm-pager{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:8px;font-size:12.5px;color:var(--mm-ink2)}",
    ".mm-chart{width:100%;height:auto;display:block}.mm-grid{stroke:var(--mm-line);stroke-width:1}.mm-axis0{stroke:var(--mm-line2);stroke-width:1.2}.mm-tick{fill:var(--mm-ink3);font-size:12px}.mm-cross{stroke:var(--mm-ink3);stroke-width:1;stroke-dasharray:3 3}",
    ".mm-empty{color:var(--mm-ink3);padding:14px 0}.mm-tip{position:fixed;z-index:60;pointer-events:none;background:#000;border:1px solid var(--mm-line2);border-radius:7px;padding:8px 10px;font-size:12.5px;color:var(--mm-ink);max-width:340px;box-shadow:0 6px 24px rgba(0,0,0,.5);display:none}",
    ".mm-tip b{color:#fff}.mm-tip table{border-collapse:collapse;margin-top:4px}.mm-tip td{padding:1px 0;min-width:0;border:0}.mm-tip td+td{text-align:right;padding-left:14px;font-variant-numeric:tabular-nums}",
    ".mm-modal{position:fixed;inset:0;z-index:80;background:rgba(0,0,0,.66);display:none;align-items:flex-start;justify-content:center;padding:4vh 12px;overflow:auto}.mm-modal.on{display:flex}",
    ".mm-sheet{background:var(--mm-bg);border:1px solid var(--mm-line2);border-radius:10px;width:min(1180px,100%);padding:14px 16px 18px;position:relative}.mm-sheet>button.x{position:absolute;right:10px;top:10px}",
    ".mm-facts{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:6px;margin:10px 0}.mm-facts .mm-tile{padding:7px 9px}.mm-facts .mm-v{font-size:17px}",
    ".mm-kv{display:grid;grid-template-columns:max-content minmax(0,1fr);gap:2px 14px;font-size:12.5px}.mm-kv dt{color:var(--mm-ink3)}.mm-kv dd{margin:0;overflow-wrap:anywhere}",
    ".mm-note{color:var(--mm-ink2);font-size:12.5px;margin:6px 0}.mm-list{margin:4px 0 0;padding:0;list-style:none;font-size:12.5px;color:var(--mm-ink2);columns:2;column-gap:22px}.mm-list li{break-inside:avoid;padding:1px 0}.mm-list b{color:var(--mm-ink)}",
    ".mm-ok{color:var(--mm-up)}.mm-bad{color:var(--mm-dn);font-weight:650}.mm-json{font:11.5px/1.45 ui-monospace,Menlo,monospace;white-space:pre-wrap;overflow-wrap:anywhere;max-height:420px;overflow:auto;background:#000;border:1px solid var(--mm-line);border-radius:6px;padding:8px;color:var(--mm-ink2)}",
    ".mm details>summary{cursor:pointer;color:var(--mm-ink2);font-size:12.5px;margin-top:8px}.mm-research .mm-map-only{display:none}.mm-embed h1{display:none}",
    "@media(max-width:1100px){.mm-tiles{grid-template-columns:repeat(3,minmax(0,1fr))}.mm-two{grid-template-columns:minmax(0,1fr)}.mm-two>.mm-card{margin-bottom:12px}.mm-facts{grid-template-columns:repeat(3,minmax(0,1fr))}}",
    "@media(max-width:640px){.mm{padding:8px 8px 30px}.mm-tiles{grid-template-columns:repeat(2,minmax(0,1fr))}.mm-v{font-size:19px}.mm-rows{grid-template-columns:minmax(90px,38%) minmax(60px,1fr) 70px}.mm-rows .mm-num.sub{display:none}",
    ".mm-board{grid-template-columns:18px 52px minmax(0,1fr) 70px}.mm-board .mm-one,.mm-board .mm-num.sub{display:none}.mm-list{columns:1}.mm-filters input[type=search]{width:100%}.mm-bar0{position:static}.mm-top{margin-top:26px}.mm-matrix th:first-child,.mm-matrix td:first-child{max-width:128px;font-size:11.5px}.mm-kv{grid-template-columns:minmax(0,1fr)}.mm-kv dt{margin-top:5px}.mm-sheet{padding:12px 10px}.mm-modal{padding:0}.mm-facts{grid-template-columns:repeat(2,minmax(0,1fr))}.mm-matrix td.c{min-width:48px;padding:4px 2px}.mm-matrix td.c small{display:none}}"
  ].join("");

  function setPalette() {
    var p = pal();
    ["pos", "neg", "up", "dn", "mid", "none"].forEach(function (k) { el.style.setProperty("--mm-" + k, p[k]); });
  }
  function num(v, cls) { return '<span class="mm-num ' + (cls || "") + " mm-" + sign(v) + '">' + esc(flow(v)) + "</span>"; }
  function flagged(t) {
    var f = byT[t], w = doc && doc.windows.filter(function (x) { return x.key === st.win; })[0];
    if (!f || !w || !(f.flags || []).length) return "";
    var hit = f.flags.filter(function (x) { return x.slice(0, 10) >= w.start; });
    return hit.length ? '<em title="' + esc(hit.map(function (x) { return x.slice(5); }).join("; ")) + '">\u2691</em>' : "";
  }

  // ---- sections
  function tiles() {
    var h = headline(doc, st.win), q = doc.quality || {};
    function tile(k, v, s, cls) { return '<div class="mm-tile"><div class="mm-k">' + esc(k) + '</div><div class="mm-v ' + (cls || "") + '">' + v + '</div><div class="mm-s">' + s + "</div></div>"; }
    return '<div class="mm-tiles">'
      + tile("Net into ETFs \u00b7 " + WIN[st.win].label, esc(flow(h.net)), int(h.n) + " unleveraged funds, " + esc(usd(h.assets)), "mm-" + sign(h.net))
      + tile("Money in", esc(usd(h["in"])), "into " + int(h.n_in) + " funds", "mm-pos")
      + tile("Money out", esc(usd(Math.abs(h.out))), "out of " + int(h.n_out) + " funds", "mm-neg")
      + tile("Most in", h.top ? esc(h.top.id) : "n/a", h.top ? esc(flow(h.top.net)) + (h.top.top_in.length ? " \u00b7 " + esc(h.top.top_in.map(function (x) { return x[0]; }).join(", ")) : "") : "", "mm-vt")
      + tile("Most out", h.bottom && h.bottom.net < 0 ? esc(h.bottom.id) : "none", h.bottom && h.bottom.net < 0 ? esc(flow(h.bottom.net)) + (h.bottom.top_out.length ? " \u00b7 " + esc(h.bottom.top_out.map(function (x) { return x[0]; }).join(", ")) : "") : "every class took money in", "mm-vt")
      + "</div>" + '<div class="mm-read" id="mm-reading">' + esc(reading(doc, st.win)) + (isNum(h.lev_net) ? " Leveraged and inverse funds, counted apart: " + esc(flow(h.lev_net)) + " across " + int(h.lev_n) + " funds." : "")
      + (q.funds_late ? " " + int(q.funds_late) + " funds (" + esc(usd(q.assets_late_usd)) + ") have no recent row and are left out." : "") + "</div>";
  }
  function bars(kind) {
    var k = KIND[kind], rows = rowsOf(doc, kind, st.win, st.measure), key = st.measure === "pct" ? "pct" : "net";
    if (!rows.length) return "";
    var ax = axis(rows.map(function (r) { return r[key]; }));
    var shown = st.open[kind] ? rows.length : Math.min(rows.length, k.open), list = rows;
    if (shown < rows.length) {                       // the most in and the most out, with the middle folded
      var a = Math.ceil(shown / 2);
      list = rows.slice(0, a).concat(rows.slice(rows.length - (shown - a)));
    }
    var html = ['<div class="mm-rows" style="--z:' + ax.zero.toFixed(2) + '%">'];
    list.forEach(function (r) {
      var v = r[key], w = isNum(v) && v ? Math.max(0.4, Math.abs(v) * ax.unit) : 0;
      html.push('<div class="mm-row" data-g="' + esc(kind + "|" + r.id) + '" role="button" tabindex="0"><div class="mm-name" title="' + esc(r.id) + '">' + esc(r.id) + "<small>" + int(r.n) + "</small></div>"
        + '<div class="mm-track">' + (w ? '<i class="' + (v < 0 ? "neg" : "pos") + '" style="width:' + w.toFixed(2) + "%;background:" + (v < 0 ? pal().neg : pal().pos) + '"></i>' : "") + "</div>"
        + (st.measure === "pct" ? '<span class="mm-num mm-' + sign(r.pct) + '">' + esc(pct(r.pct)) + "</span>" + num(r.net, "sub") : num(r.net) + '<span class="mm-num sub mm-' + sign(r.pct) + '">' + esc(pct(r.pct)) + "</span>") + "</div>");
    });
    html.push("</div>");
    if (rows.length > k.open) html.push('<div class="mm-more"><button data-open="' + esc(kind) + '">' + (st.open[kind] ? "Show the most in and the most out only" : "Show all " + rows.length) + "</button> "
      + (st.open[kind] ? "" : "the " + Math.ceil(shown / 2) + " with the most in and the " + (shown - Math.ceil(shown / 2)) + " with the most out are shown") + "</div>");
    return html.join("");
  }
  function matrix(kind) {
    var rows = rowsOf(doc, kind, st.win, st.measure), p = pal();
    var html = ['<div class="mm-scroll"><table class="mm-matrix"><thead><tr><th></th>' + WINDOWS.map(function (w) { return "<th>" + w.label + "</th>"; }).join("") + "</tr></thead><tbody>"];
    rows.forEach(function (r) {
      html.push("<tr><td title=\"" + esc(r.id) + '">' + esc(r.id) + "</td>" + WINDOWS.map(function (w) {
        var x = (r.w || {})[w.id] || {};
        return '<td class="c" data-g="' + esc(kind + "|" + r.id) + '" data-w="' + w.id + '" style="background:' + heat(x.pct, w.full, p) + '">' + esc(pct(x.pct)) + "<small>" + esc(flow(x.net)) + "</small></td>";
      }).join("") + "</tr>");
    });
    html.push("</tbody></table></div>");
    return html.join("");
  }
  function card(id, title, sub, body, extra) {
    return '<section class="mm-card" id="mm-' + id + '"><header><h2>' + esc(title) + "</h2><p>" + esc(sub || "") + "</p>" + (extra || "") + "</header>" + body + "</section>";
  }
  function groupCard(kind) {
    var k = KIND[kind];
    if (!((doc.groups || {})[kind] || []).length) return "";
    return card(kind, k.title, k.sub, bars(kind));
  }
  function board(side) {
    var b = (doc.boards || {})[st.win] || {}, list = b[side + "_" + st.board] || [], n = Math.min(st.boardN, list.length), by = st.board === "pct" ? 2 : 1;
    if (!list.length) return '<div class="mm-empty">No fund ' + (side === "in" ? "took money in" : "lost money") + ".</div>";
    var max = Math.max.apply(null, list.map(function (x) { return Math.abs(x[by] || 0); }).concat([1e-9]));
    var html = ['<div class="mm-board">'];
    list.slice(0, n).forEach(function (x, i) {
      var f = byT[x[0]] || {};
      html.push('<div class="mm-brow" data-f="' + esc(x[0]) + '" role="button" tabindex="0"><span class="mm-rank">' + (i + 1) + '</span><span class="mm-tk">' + esc(x[0]) + flagged(x[0]) + '</span><span class="mm-fn" title="' + esc(f.name || "") + '">'
        + esc(f.name || "") + (f.seg ? " \u00b7 " + esc(f.seg) : "") + '</span><span class="mm-one"><i style="width:' + Math.max(1, 100 * Math.abs(x[by] || 0) / max).toFixed(1) + "%;background:" + (side === "in" ? pal().pos : pal().neg) + '"></i></span>'
        + (st.board === "pct" ? '<span class="mm-num mm-' + sign(x[2]) + '">' + esc(pct(x[2])) + "</span>" + num(x[1], "sub") : num(x[1]) + '<span class="mm-num sub mm-' + sign(x[2]) + '">' + esc(pct(x[2])) + "</span>") + "</div>");
    });
    html.push("</div>");
    html.push('<div class="mm-more">' + (list.length > 15 ? '<button data-boardn="' + (st.boardN > 15 ? 15 : 40) + '">' + (st.boardN > 15 ? "Show 15" : "Show all " + list.length) + "</button> " : "")
      + '<button data-sortfunds="' + (side === "in" ? "desc" : "asc") + '">Every fund, ' + (side === "in" ? "most in first" : "most out first") + "</button></div>");
    return html.join("");
  }
  function boards() {
    var seg = '<div class="mm-seg" role="group" aria-label="Rank by"><button data-board="usd" class="' + (st.board === "usd" ? "on" : "") + '">By dollars</button><button data-board="pct" class="' + (st.board === "pct" ? "on" : "") + '">By % of the fund</button></div>';
    return card("etfs", "ETFs taking in and losing the most money", "Over " + WIN[st.win].word + (st.board === "pct" ? ", against the fund's own assets (funds of $50M and more)." : ".")
      + " \u2691 marks a fund with a split, a round trip or a jump in the window: open the fund to read the note.",
      '<div class="mm-two mm-in"><div><h3>Money in</h3>' + board("in") + "</div><div><h3>Money out</h3>" + board("out") + "</div></div>", seg);
  }
  function turned() {
    var t = doc.turned || {};
    function side(list, word) {
      if (!(list || []).length) return '<div class="mm-empty">None.</div>';
      return '<div class="mm-board" style="grid-template-columns:62px minmax(0,1fr) 84px 110px">' + list.map(function (x) {
        var f = byT[x[0]] || {};
        return '<div class="mm-brow" data-f="' + esc(x[0]) + '" role="button" tabindex="0"><span class="mm-tk">' + esc(x[0]) + '</span><span class="mm-fn">' + esc(f.name || "") + "</span>" + num(x[1]) + '<span class="mm-num sub">' + word + " " + esc(flow(x[2])) + "</span></div>";
      }).join("") + "</div>";
    }
    return '<div class="mm-two" id="mm-turned">' + card("toin", "Turned to money in this week", "Funds of $100M and more whose last five days are in after five days out, largest swing first.", side(t.to_in, "was"))
      + card("toout", "Turned to money out this week", "In the five days before, money was coming in.", side(t.to_out, "was")) + '</div><div class="mm-gap"></div>';
  }
  function historyCard() {
    if (!hist) return card("history", "Day by day", "Loading the history document.", '<div class="mm-empty">Loading.</div>');
    var keys = Object.keys(hist.series || {}), groups = {};
    keys.forEach(function (k) { var i = k.indexOf(":"); (groups[k.slice(0, i)] = groups[k.slice(0, i)] || []).push(k); });
    var names = { total: "Totals", segments: "Asset classes", classes: "Provider classes", sectors: "Sector funds", sectors_through: "Sectors through every equity fund", bonds: "Bonds" };
    var sel = '<select id="mm-hkey" aria-label="Row">' + ["total", "segments", "sectors", "sectors_through", "bonds", "classes"].filter(function (g) { return groups[g]; }).map(function (g) {
      return '<optgroup label="' + esc(names[g] || g) + '">' + groups[g].map(function (k) {
        var label = k === "total:plain" ? "Every unleveraged ETF" : k === "total:leveraged" ? "Leveraged & inverse funds" : k.slice(k.indexOf(":") + 1);
        return '<option value="' + esc(k) + '"' + (k === st.hkey ? " selected" : "") + ">" + esc(label) + "</option>";
      }).join("") + "</optgroup>";
    }).join("") + "</select>";
    var s = seriesOf(hist, st.hkey, st.hspan, st.hcum);
    var ctl = sel + ' <div class="mm-seg">' + [[65, "3M"], [130, "6M"], [260, "1Y"]].map(function (x) { return '<button data-hspan="' + x[0] + '" class="' + (st.hspan === x[0] ? "on" : "") + '">' + x[1] + "</button>"; }).join("")
      + '</div> <div class="mm-seg"><button data-hcum="0" class="' + (!st.hcum ? "on" : "") + '">A day at a time</button><button data-hcum="1" class="' + (st.hcum ? "on" : "") + '">Running total</button></div>';
    return card("history", "Day by day", s.dates.length ? s.dates[0] + " to " + s.dates[s.dates.length - 1] + ": " + flow(s.sum) + " in all. Each fund's flow summed for the day; today's funds throughout." : "", '<div id="mm-hchart">'
      + chartSvg(s, pal(), { cumulative: st.hcum, label: st.hkey }) + "</div>", ctl);
  }
  function filtered() {
    var list = funds.filter(function (f) {
      return (st.late || isCurrent(f, doc)) && (!st.fseg || f.seg === st.fseg) && (!st.fsec || f.sec === st.fsec) && (!st.fcty || f.cty === st.fcty) && (!st.fiss || f.issuer === st.fiss);
    });
    return st.q ? searchFunds(sortFunds(list, st.sort, st.dir), st.q) : sortFunds(list, st.sort, st.dir);
  }
  function fundRows(list, cols, from, to) {
    return list.slice(from, to).map(function (f) {
      return '<tr class="r' + (isCurrent(f, doc) ? "" : " late") + '" data-f="' + esc(f.t) + '">' + cols.map(function (c) {
        var k = COLS[c][1], v = f[c], cls = (k === "text" || k === "date" || k === "flags" || k === "chk" ? "l" : "") + (c === "name" ? " nm" : "") + (k === "usd" || k === "pct" || k === "z" || k === "streak" ? " mm-" + sign(v) : "");
        return '<td class="' + cls + '"' + (c === "name" ? ' title="' + esc(v) + '"' : "") + ">" + (c === "t" ? "<b>" + esc(v) + "</b>" + (isCurrent(f, doc) ? "" : " <small>late</small>") : esc(cellText(c, v))) + "</td>";
      }).join("") + "</tr>";
    }).join("");
  }
  function tableHtml(list, cols, from, to, sort, dir) {
    sort = sort || st.sort; dir = dir || st.dir;
    return '<div class="mm-tablewrap"><table class="mm-table"><thead><tr>' + cols.map(function (c) {
      var k = COLS[c][1];
      return '<th data-sort="' + c + '" class="' + (k === "text" || k === "date" || k === "flags" || k === "chk" ? "l " : "") + (sort === c ? "on" : "") + '" title="Sort by ' + esc(COLS[c][0]) + '">' + esc(COLS[c][0]) + (sort === c ? (dir === "asc" ? " \u25b2" : " \u25bc") : "") + "</th>";
    }).join("") + "</tr></thead><tbody>" + fundRows(list, cols, from, to) + "</tbody></table></div>";
  }
  function uniq(key) {
    var seen = {};
    funds.forEach(function (f) { if (f[key]) seen[f[key]] = (seen[f[key]] || 0) + 1; });
    return Object.keys(seen).sort();
  }
  function fundsCard() {
    if (!fdoc) return card("funds", "Every fund", "Loading the funds document.", '<div class="mm-empty">Loading every fund.</div>');
    var list = filtered(), per = 50, pages = Math.max(1, Math.ceil(list.length / per));
    st.page = Math.min(st.page, pages - 1);
    var cols = st.allcols ? fdoc.cols : TABLE_COLS;
    function opt(key, label, val) { return '<select data-filter="' + key + '" aria-label="' + label + '"><option value="">' + label + ": all</option>" + uniq(key === "fseg" ? "seg" : key === "fsec" ? "sec" : key === "fcty" ? "cty" : "issuer").map(function (x) { return '<option' + (x === val ? " selected" : "") + ">" + esc(x) + "</option>"; }).join("") + "</select>"; }
    var filters = '<div class="mm-filters"><input type="search" id="mm-q" placeholder="Ticker, fund or issuer" value="' + esc(st.q) + '" aria-label="Search funds">' + opt("fseg", "Asset class", st.fseg) + opt("fsec", "Sector", st.fsec) + opt("fcty", "Country", st.fcty) + opt("fiss", "Issuer", st.fiss)
      + '<label><input type="checkbox" id="mm-late"' + (st.late ? " checked" : "") + "> late funds too</label>" + '<button data-allcols="1">' + (st.allcols ? "Fewer columns" : "All " + fdoc.cols.length + " columns") + '</button><button data-csv="1">Download CSV (' + int(list.length) + ")</button></div>";
    var pager = '<div class="mm-pager"><button data-page="-1"' + (st.page ? "" : " disabled") + '>Previous</button><span>' + int(list.length) + " funds \u00b7 page " + (st.page + 1) + " of " + pages + '</span><button data-page="1"' + (st.page < pages - 1 ? "" : " disabled") + ">Next</button><span>Click a column to sort it, again to turn it round. Click a fund for its own days.</span></div>";
    return card("funds", "Every fund", int((doc.quality || {}).funds_with_rows) + " funds in the warehouse; " + int((doc.quality || {}).funds_current) + " are current and counted above.", filters + '<div id="mm-ftable">' + tableHtml(list, cols, st.page * per, st.page * per + per) + "</div>" + pager);
  }
  function li(items) { return '<ul class="mm-list">' + items.map(function (x) { return "<li>" + x + "</li>"; }).join("") + "</ul>"; }
  function notes() {
    var q = doc.quality || {}, a = audit(doc, funds), sp = q.splits || {}, rt = q.round_trips || {}, pf = q.provider_figure || {}, ic = q.issuer_check || {}, mp = q.misprints_dropped || {};
    var html = [];
    html.push('<p class="mm-note">' + (fdoc ? (a.ok ? '<span class="mm-ok">Added up again on this page:</span> the ' + int(a.current) + " current funds give every asset-class row and both totals in all six windows (" + a.rows + " figures)."
      : '<span class="mm-bad">This page added the funds up again and ' + a.wrong.length + " figures differ from the document:</span> " + esc(a.wrong.slice(0, 4).map(function (x) { return x[0] + " " + x[1] + " " + flow(x[2]) + " against " + flow(x[3]); }).join("; "))) : "The funds document is loading; the page adds the funds up again when it arrives.") + "</p>");
    html.push('<dl class="mm-kv"><dt>Orders through</dt><dd>' + esc(dayName(doc.orders_through)) + " (" + int(q.newest_day_age_days) + " days before this was built). The provider posts a day's orders the day after.</dd>"
      + "<dt>Built</dt><dd>" + esc(String(doc.generated_at).replace("T", " ").slice(0, 16)) + " UTC by " + esc(doc.engine) + " " + esc(doc.version) + " (" + esc(doc.contract) + ")</dd>"
      + "<dt>Funds</dt><dd>" + int(q.funds_with_rows) + " with rows; " + int(q.funds_current) + " current (" + esc(usd(q.assets_current_usd)) + "); " + int(q.funds_late) + " late (" + esc(usd(q.assets_late_usd)) + ", more than " + int(q.late_rule_days) + " reporting days behind); "
      + int(q.funds_no_rows) + " with no usable row; " + int(q.funds_with_profile) + " with the provider's profile; " + int(q.funds_unclassified) + " it has not classed</dd>"
      + "<dt>Days</dt><dd>" + int(q.reporting_days) + " reporting days kept, " + esc(q.first_day) + " to " + esc(q.newest_day) + "; whole-market months read: " + esc((q.by_date_months || []).join(", ")) + "</dd>"
      + "<dt>Windows</dt><dd>" + doc.windows.map(function (w) { return esc(w.label) + " " + esc(w.start) + " to " + esc(w.end) + " (" + w.n + " days" + (w.complete ? "" : ", not a full window") + ")"; }).join("; ") + "</dd></dl>");
    html.push("<details open><summary>Largest funds left out as late (" + (q.late_largest || []).length + " shown)</summary>" + li((q.late_largest || []).map(function (x) { return "<b>" + esc(x[0]) + "</b> newest row " + esc(x[1]) + ", " + esc(usd(x[2])); })) + "</details>");
    html.push("<details open><summary>Share splits taken out: " + int(sp.in_history) + " in the kept history (" + int(sp.on_record) + " on the warehouse's record), " + esc(usd(sp.usd_not_money)) + " the provider printed as flow that is not money; "
      + int(sp.recorded_but_not_in_the_counts) + " recorded splits never show in the counts</summary>" + li((sp.largest || []).map(function (x) { return "<b>" + esc(x[0]) + "</b> " + esc(x[1]) + " " + esc(x[2].replace("split ", "")) + ", printed as " + esc(usd(x[3])); }))
      + '<p class="mm-note">In the last 63 days: ' + esc((q.splits_taken_out || []).map(function (x) { return x[0] + " " + x[1].slice(5) + " " + x[2].replace("split ", ""); }).join(", ")) + "</p></details>");
    html.push("<details open><summary>Round trips netted: " + int(rt.last_63_days) + " in the last 63 days, " + esc(usd(rt.usd_each_way_last_63_days)) + " each way (" + int(rt.in_history) + " in the kept history)</summary>"
      + li((rt.largest_last_63_days || []).map(function (x) { return "<b>" + esc(x[0]) + "</b> " + esc(x[1]) + " " + esc(usd(x[2])) + " each way: " + esc(x[3]); })) + "</details>");
    html.push("<details open><summary>Jumps that stayed and are counted as money, last 21 days (" + (q.jumps_last_21_days || []).length + " largest)</summary>" + li((q.jumps_last_21_days || []).map(function (x) { return "<b>" + esc(x[0]) + "</b> " + esc(x[1]) + " " + esc(flow(x[2])) + ": " + esc(x[3]); })) + "</details>");
    html.push("<details><summary>Rows dropped as misprints: " + int(mp.in_history) + " in the kept history</summary>" + li((mp.last_63_days || []).map(function (x) { return "<b>" + esc(x[0]) + "</b> " + esc(x[1]) + ": " + esc(x[2]); })) + "</details>");
    html.push("<details><summary>The provider's own figure beside the share-count path: " + int(pf.rows_equal) + " of " + int(pf.rows_compared) + " untouched rows equal in the last 63 days; " + int(pf.rows_differing) + " differ (" + int(pf.differing_after_a_missing_day)
      + " after a missing day), " + esc(usd(pf.usd_apart)) + " apart</summary>" + li((pf.largest || []).map(function (x) { return "<b>" + esc(x[0]) + "</b> " + esc(x[1]) + ": provider " + esc(flow(x[2])) + ", counted " + esc(flow(x[3])) + (x[4] ? " (after a missing day)" : ""); })) + "</details>");
    html.push("<details><summary>The issuers' own share counts: " + int(ic.funds) + " funds checked, " + int(ic.next_day) + " match the issuer's next-day file, " + int(ic.same_day) + " its same-day file, " + int(ic.too_few_days) + " had too few telling days, "
      + (ic.no_match || []).length + ' match neither</summary><p class="mm-note">' + esc((ic.no_match || []).join(" ")) + "</p></details>");
    html.push("<details><summary>Five-day flows over a quarter of a fund's assets (" + (q.unusual_week || []).length + ")</summary>" + li((q.unusual_week || []).map(function (x) { return "<b>" + esc(x[0]) + "</b> " + esc(flow(x[1])) + ", " + esc(pct(x[2])); })) + "</details>");
    html.push('<details open><summary>How a figure is made</summary><dl class="mm-kv">' + Object.keys(doc.method || {}).map(function (k) { return "<dt>" + esc(k.replace(/_/g, " ")) + "</dt><dd>" + esc(doc.method[k]) + "</dd>"; }).join("") + "</dl></details>");
    html.push('<details><summary>Where it comes from</summary><dl class="mm-kv">' + Object.keys(doc.sources || {}).map(function (k) { return "<dt>" + esc(k.replace(/_/g, " ")) + "</dt><dd>" + esc(doc.sources[k]) + "</dd>"; }).join("")
      + Object.keys(doc.documents || {}).map(function (k) { return "<dt>document: " + esc(k.replace(/_/g, " ")) + "</dt><dd>" + (doc.documents[k].indexOf("<") < 0 ? '<a href="/' + esc(doc.documents[k]) + '">' + esc(doc.documents[k]) + "</a>" : esc(doc.documents[k])) + "</dd>"; }).join("")
      + (fdoc ? Object.keys(fdoc.units || {}).map(function (k) { return "<dt>fund column " + esc(k) + "</dt><dd>" + esc(fdoc.units[k]) + "</dd>"; }).join("") : "") + "</dl></details>");
    html.push('<details id="mm-whole"><summary>The whole document, as published</summary><div class="mm-json" id="mm-json"></div></details>');
    return card("notes", "What was done to the data", "Nothing is estimated. What the provider's rows needed is counted here, fund by fund.", html.join(""));
  }

  function render() {
    if (!doc) return;
    setPalette();
    var w = doc.windows.filter(function (x) { return x.key === st.win; })[0] || {};
    var jump = [["segments", "Asset classes"], ["matrix", "Rotation"], ["sectors", "Sectors"], ["etfs", "ETFs in and out"], ["countries", "Countries"], ["bonds", "Bonds"], ["commodities", "Commodities"], ["crypto", "Crypto"],
      ["leveraged", "Leveraged"], ["issuers", "Issuers"], ["turned", "Turned"], ["history", "Day by day"], ["funds", "Every fund"], ["notes", "Data notes"]];
    var html = ['<div class="mm-bar0 mm-map-only"><div class="mm-seg" role="group" aria-label="Window">' + WINDOWS.map(function (x) { return '<button data-win="' + x.id + '" class="' + (st.win === x.id ? "on" : "") + '">' + x.label + "</button>"; }).join("") + "</div>"
      + '<div class="mm-seg" role="group" aria-label="Measure"><button data-measure="usd" class="' + (st.measure === "usd" ? "on" : "") + '">Dollars</button><button data-measure="pct" class="' + (st.measure === "pct" ? "on" : "") + '">% of assets</button></div>'
      + '<span class="mm-asof" style="margin:0"><b>' + esc(w.label || "") + "</b> " + esc(dayName(w.start)) + (w.start !== w.end ? " to " + esc(dayName(w.end)) : "") + (w.complete === false ? " (not a full window)" : "") + "</span>"
      + '<div class="mm-right"><select id="mm-pal" aria-label="Colours">' + Object.keys(PALETTES).map(function (k) { return '<option value="' + k + '"' + (k === st.pal ? " selected" : "") + ">" + esc(PALETTES[k].name) + "</option>"; }).join("") + "</select></div></div>"];
    html.push('<div class="mm-map-only"><nav class="mm-jump" aria-label="On this page">' + jump.map(function (j) { return '<a href="#mm-' + j[0] + '">' + j[1] + "</a>"; }).join("") + "</nav>");
    html.push(tiles());
    html.push(groupCard("segments"));
    html.push(card("matrix", "Rotation \u00b7 asset classes across every window", "Each cell is the flow as a percentage of the assets the group had before the window; the dollars are under it. The colour is that percentage against a full-colour mark of "
      + WINDOWS.map(function (x) { return x.label + " " + x.full + "%"; }).join(", ") + ".", matrix("segments")));
    html.push(groupCard("classes"));
    html.push('<div class="mm-two">' + groupCard("sectors") + groupCard("sectors_through") + '</div><div class="mm-gap"></div>');
    html.push(card("sectormatrix", "Rotation \u00b7 sectors through every equity fund", "The same reading for the eleven sectors, money into broad funds counted by the weights they hold.", matrix("sectors_through")));
    html.push(boards());
    html.push('<div class="mm-two">' + groupCard("countries") + groupCard("regions") + '</div><div class="mm-gap"></div>');
    html.push('<div class="mm-two">' + groupCard("bonds") + groupCard("bond_maturity") + '</div><div class="mm-gap"></div>');
    html.push('<div class="mm-two">' + groupCard("commodities") + groupCard("crypto") + '</div><div class="mm-gap"></div>');
    html.push('<div class="mm-two">' + groupCard("leveraged") + groupCard("styles") + '</div><div class="mm-gap"></div>');
    html.push('<div class="mm-two">' + groupCard("issuers") + groupCard("categories") + '</div><div class="mm-gap"></div>');
    html.push(groupCard("focus"));
    html.push(turned());
    html.push(historyCard());
    html.push(fundsCard());
    html.push(notes());
    html.push("</div>");
    el.querySelector("#mm-body").innerHTML = html.join("");
  }
  function shell() {
    el.innerHTML = '<div class="mm-top"><h1>Money flow<span>where money is flowing in and out of ETFs</span></h1><div class="mm-right"><div class="mm-seg" role="group" aria-label="View"><button data-view="map" class="on">Money map</button>'
      + '<button data-view="research">Research view</button></div></div></div><p class="mm-asof mm-map-only" id="mm-asof">Loading the money map.</p><div id="mm-body"></div>'
      + '<div class="mm-tip" id="mm-tip"></div><div class="mm-modal" id="mm-modal" role="dialog" aria-modal="true"><div class="mm-sheet"><button class="x" data-close="1" aria-label="Close">Close</button><div id="mm-sheet"></div></div></div>';
  }
  function asof() {
    var q = doc.quality || {};
    el.querySelector("#mm-asof").innerHTML = "Orders through <b>" + esc(dayName(doc.orders_through)) + "</b> \u00b7 <b>" + int(q.funds_current) + "</b> ETFs \u00b7 <b>" + esc(usd(q.assets_current_usd)) + "</b> \u00b7 built "
      + esc(String(doc.generated_at).replace("T", " ").slice(0, 16)) + " UTC \u00b7 the provider posts a day's orders the day after";
  }

  // ---- sheets
  function openModal(html) { el.querySelector("#mm-sheet").innerHTML = html; el.querySelector("#mm-modal").classList.add("on"); }
  function closeModal() { el.querySelector("#mm-modal").classList.remove("on"); }
  var SHEET_COLS = ["t", "name", "issuer", "assets", "d1", "w1", "m1", "m3", "ytd", "y1", "p_w1", "p_m1", "streak", "flags"];
  function sheetList() {
    var g = st.gs, list = sortFunds(members(funds, doc, g.kind, g.id), g.sort, g.dir);
    return '<h2 style="margin:12px 0 6px">' + int(list.length) + " funds \u00b7 click a column to sort it, again to turn it round</h2>" + '<div style="max-height:52vh;overflow:auto">' + tableHtml(list, SHEET_COLS, 0, list.length, g.sort, g.dir) + "</div>";
  }
  function bind(svg, series, kind) { if (svg) svg.__series = { dates: series.dates, values: series.values, kind: kind || "flow" }; }
  function groupSheet(kind, id) {
    var k = KIND[kind], g = ((doc.groups || {})[kind] || []).filter(function (x) { return x.id === id; })[0];
    if (!g) return;
    st.gs = { kind: kind, id: id, sort: st.win, dir: "desc" };
    var facts = WINDOWS.map(function (w) {
      var x = (g.w || {})[w.id] || {};
      return '<div class="mm-tile"><div class="mm-k">' + w.label + '</div><div class="mm-v mm-' + sign(x.net) + '">' + esc(flow(x.net)) + '</div><div class="mm-s">' + esc(pct(x.pct)) + " \u00b7 in " + esc(usd(x["in"])) + " (" + int(x.n_in) + ") \u00b7 out " + esc(usd(Math.abs(x.out || 0))) + " (" + int(x.n_out) + ")</div></div>";
    }).join("");
    var hk = (kind === "segments" || kind === "classes" || kind === "sectors" || kind === "sectors_through" || kind === "bonds") ? kind + ":" + id : null;
    var series = hk && hist && (hist.series || {})[hk] ? seriesOf(hist, hk, 130, false) : null;
    openModal("<h2>" + esc(id) + ' <span style="color:var(--mm-ink3);font-weight:500;font-size:13px">' + esc(k.title) + " \u00b7 " + int(g.n) + " funds \u00b7 " + esc(usd(g.assets)) + '</span></h2><div class="mm-facts">' + facts + "</div>"
      + (series ? '<h2 style="margin:12px 0 4px">Day by day, last 130 reporting days</h2><div id="mm-gchart">' + chartSvg(series, pal(), { h: 190, label: id }) + "</div>" : "")
      + (k.through ? '<p class="mm-note">This row splits every equity fund\'s flow by the weight it holds in the sector. The funds listed below are the sector\'s own funds; broad funds such as VOO count in the row by their weight.</p>' : "")
      + '<div id="mm-glist">' + (fdoc ? sheetList() : '<div class="mm-empty">Loading the funds.</div>') + "</div>");
    if (series) bind(el.querySelector("#mm-gchart svg"), series);
  }
  function fundSheet(t) {
    var f = byT[t];
    if (!f) return;
    var facts = WINDOWS.map(function (w) {
      var p = f["p_" + w.id];
      return '<div class="mm-tile"><div class="mm-k">' + w.label + '</div><div class="mm-v mm-' + sign(f[w.id]) + '">' + esc(flow(f[w.id])) + '</div><div class="mm-s">' + (isNum(p) ? esc(pct(p)) + " of its assets" : "&nbsp;") + "</div></div>";
    }).join("");
    var kv = fdoc.cols.filter(function (c) { return WINDOWS.every(function (w) { return w.id !== c; }) && c !== "t" && c !== "name"; }).map(function (c) {
      var v = f[c];
      return "<dt>" + esc(COLS[c][0]) + "</dt><dd>" + (v == null || v === "" || (Array.isArray(v) && !v.length) ? '<span style="color:var(--mm-ink3)">none</span>' : esc(c === "flags" ? v.join("; ") : cellText(c, v))) + "</dd>";
    }).join("");
    openModal("<h2>" + esc(t) + ' <span style="color:var(--mm-ink2);font-weight:500;font-size:14px">' + esc(f.name || "") + '</span></h2><p class="mm-note">' + esc(f.seg) + (f.issuer ? " \u00b7 " + esc(f.issuer) : "") + " \u00b7 " + esc(usd(f.assets)) + " in assets"
      + (isCurrent(f, doc) ? "" : ' \u00b7 <span class="mm-bad">late: newest row ' + esc(f.newest) + ", left out of every total</span>") + ' \u00b7 <a href="/chart.html?symbol=' + encodeURIComponent(t) + '" data-chart="' + esc(t) + '">Open ' + esc(t) + " on the chart</a></p>"
      + '<div class="mm-facts">' + facts + '</div><div id="mm-fhist"><div class="mm-empty">Loading its own days.</div></div><h2 style="margin:12px 0 6px">Every figure the document has for it</h2><dl class="mm-kv" style="columns:2">' + kv + "</dl>");
    var letter = /^[A-Z]/.test(t) ? t[0] : "0";
    function draw(sh) {
      var box = el.querySelector("#mm-fhist"), own = sh && sh.funds && sh.funds[t];
      if (!box || !el.querySelector("#mm-modal").classList.contains("on")) return;
      if (!own) { box.innerHTML = '<div class="mm-empty">Its own days are not in the published history.</div>'; return; }
      var notes = Object.keys(own.x || {}).sort().map(function (d) { return "<li><b>" + esc(d) + "</b> " + esc(own.x[d]) + "</li>"; }).join("");
      var flows = { dates: sh.dates, values: own.f.map(function (v) { return v || 0; }) }, assets = { dates: sh.dates, values: own.a.map(function (v) { return v || 0; }) };
      box.innerHTML = '<h2 style="margin:12px 0 4px">Its flow, a day at a time</h2>' + chartSvg(flows, pal(), { h: 190, label: t + " daily flow" })
        + '<h2 style="margin:12px 0 4px">Its assets (shares \u00d7 NAV)</h2>' + chartSvg(assets, pal(), { h: 150, cumulative: true, label: t + " assets" })
        + (notes ? '<h2 style="margin:12px 0 4px">Days with a note</h2><ul class="mm-list">' + notes + "</ul>" : "");
      var svgs = box.querySelectorAll("svg");
      bind(svgs[0], flows); bind(svgs[1], assets, "assets");
    }
    if (shards[letter]) draw(shards[letter]);
    else get(DOCS.shard + letter + ".json").then(function (sh) { shards[letter] = sh; draw(sh); }).catch(function () { draw(null); });
  }

  // ---- events
  function tipFor(target) {
    var g = target.closest("[data-g]");
    if (!g || !doc) return null;
    var p = g.getAttribute("data-g").split("|"), kind = p[0], id = p.slice(1).join("|"), win = g.getAttribute("data-w") || st.win;
    var row = ((doc.groups || {})[kind] || []).filter(function (x) { return x.id === id; })[0];
    if (!row) return null;
    var x = (row.w || {})[win] || {};
    function tops(list) { return list && list.length ? list.map(function (y) { return esc(y[0]) + " " + esc(flow(y[1])); }).join(", ") : "none"; }
    return "<b>" + esc(id) + "</b> \u00b7 " + WIN[win].label + "<table><tr><td>Net</td><td>" + esc(flow(x.net)) + " (" + esc(pct(x.pct)) + ")</td></tr><tr><td>In</td><td>" + esc(usd(x["in"])) + " \u00b7 " + int(x.n_in) + " funds</td></tr><tr><td>Out</td><td>" + esc(usd(Math.abs(x.out || 0)))
      + " \u00b7 " + int(x.n_out) + " funds</td></tr>" + (isNum(x.prior) ? "<tr><td>Window before</td><td>" + esc(flow(x.prior)) + "</td></tr>" : "") + "<tr><td>Funds</td><td>" + int(row.n) + " \u00b7 " + esc(usd(row.assets)) + "</td></tr></table>Most in: " + tops(x.top_in)
      + "<br>Most out: " + tops(x.top_out) + "<br><span style='color:var(--mm-ink3)'>Click for its funds</span>";
  }
  function wire() {
    var tip = el.querySelector("#mm-tip");
    el.addEventListener("mousemove", function (e) {
      var hit = e.target.closest && e.target.closest(".mm-hit");
      if (hit) {
        var svg = hit.ownerSVGElement, box = hit.getBoundingClientRect(), n = +svg.getAttribute("data-n"), i = Math.max(0, Math.min(n - 1, Math.floor(n * (e.clientX - box.left) / box.width)));
        var src = svg.__series;
        if (src) {
          var cross = svg.querySelector(".mm-cross"), x = 62 + (1000 - 62 - 10) * (i + 0.5) / n;
          cross.setAttribute("x1", x); cross.setAttribute("x2", x); cross.style.display = "";
          tip.innerHTML = "<b>" + esc(dayName(src.dates[i])) + "</b><br>" + esc(src.kind === "assets" ? usd(src.values[i]) : flow(src.values[i]));
          tip.style.display = "block";
        }
      } else {
        var html = tipFor(e.target);
        if (!html) { tip.style.display = "none"; return; }
        tip.innerHTML = html; tip.style.display = "block";
      }
      var w = tip.offsetWidth, h = tip.offsetHeight;
      tip.style.left = Math.max(6, Math.min(root.innerWidth - w - 6, e.clientX + 14)) + "px";
      tip.style.top = Math.max(6, Math.min(root.innerHeight - h - 6, e.clientY + 16)) + "px";
    });
    el.addEventListener("mouseleave", function () { tip.style.display = "none"; });
    el.addEventListener("click", function (e) {
      var t = e.target, b;
      if ((b = t.closest("[data-view]"))) return setView(b.getAttribute("data-view"), true);
      if (t.closest("[data-close]") || t === el.querySelector("#mm-modal")) return closeModal();
      if ((b = t.closest("[data-chart]"))) {
        try { if (embedded && root.top.jhOpenSymbol) { e.preventDefault(); root.top.jhOpenSymbol(b.getAttribute("data-chart")); } } catch (err) { /* another origin: the link opens the chart itself */ }
        return;
      }
      if ((b = t.closest("[data-win]"))) { st.win = b.getAttribute("data-win"); remember(); return render(), after(); }
      if ((b = t.closest("[data-measure]"))) { st.measure = b.getAttribute("data-measure"); return render(), after(); }
      if ((b = t.closest("[data-open]"))) { st.open[b.getAttribute("data-open")] = !st.open[b.getAttribute("data-open")]; return render(), after(); }
      if ((b = t.closest("[data-board]"))) { st.board = b.getAttribute("data-board"); return render(), after(); }
      if ((b = t.closest("[data-boardn]"))) { st.boardN = +b.getAttribute("data-boardn"); return render(), after(); }
      if ((b = t.closest("[data-sortfunds]"))) { st.sort = st.win; st.dir = b.getAttribute("data-sortfunds"); st.page = 0; st.q = ""; render(); after(); return el.querySelector("#mm-funds").scrollIntoView(); }
      if ((b = t.closest("[data-hspan]"))) { st.hspan = +b.getAttribute("data-hspan"); return render(), after(); }
      if ((b = t.closest("[data-hcum]"))) { st.hcum = b.getAttribute("data-hcum") === "1"; return render(), after(); }
      if ((b = t.closest("[data-allcols]"))) { st.allcols = !st.allcols; return render(), after(); }
      if ((b = t.closest("[data-page]"))) { st.page = Math.max(0, st.page + (+b.getAttribute("data-page"))); return render(), after(); }
      if ((b = t.closest("[data-csv]"))) return download();
      if ((b = t.closest("th[data-sort]"))) {
        var c = b.getAttribute("data-sort");
        if (b.closest("#mm-modal")) {
          var g = st.gs;
          if (g.sort === c) g.dir = g.dir === "asc" ? "desc" : "asc"; else { g.sort = c; g.dir = (COLS[c][1] === "text" || COLS[c][1] === "date") ? "asc" : "desc"; }
          el.querySelector("#mm-glist").innerHTML = sheetList();
          return;
        }
        if (st.sort === c) st.dir = st.dir === "asc" ? "desc" : "asc"; else { st.sort = c; st.dir = (COLS[c][1] === "text" || COLS[c][1] === "date") ? "asc" : "desc"; }
        st.page = 0;
        return render(), after();
      }
      if ((b = t.closest("[data-f]"))) return fundSheet(b.getAttribute("data-f"));
      if ((b = t.closest("[data-g]"))) { var p = b.getAttribute("data-g").split("|"); return groupSheet(p[0], p.slice(1).join("|")); }
    });
    root.document.addEventListener("keydown", function (e) { if (e.key === "Escape") closeModal(); });       // wherever the focus is
    el.addEventListener("keydown", function (e) {
      if ((e.key === "Enter" || e.key === " ") && e.target.getAttribute && e.target.getAttribute("role") === "button") { e.preventDefault(); e.target.click(); }
    });
    el.addEventListener("change", function (e) {
      var t = e.target;
      if (t.id === "mm-pal") { st.pal = t.value; remember(); render(); return after(); }
      if (t.id === "mm-hkey") { st.hkey = t.value; render(); return after(); }
      if (t.id === "mm-late") { st.late = t.checked; st.page = 0; render(); return after(); }
      if (t.getAttribute("data-filter")) { st[t.getAttribute("data-filter")] = t.value; st.page = 0; render(); return after(); }
    });
    var timer = 0;
    el.addEventListener("input", function (e) {
      if (e.target.id !== "mm-q") return;
      st.q = e.target.value; st.page = 0;
      clearTimeout(timer);
      timer = setTimeout(function () {
        var list = filtered(), box = el.querySelector("#mm-ftable");
        if (box) box.innerHTML = tableHtml(list, st.allcols ? fdoc.cols : TABLE_COLS, 0, 50);
        var pg = el.querySelector(".mm-pager span");
        if (pg) pg.textContent = int(list.length) + " funds \u00b7 page 1 of " + Math.max(1, Math.ceil(list.length / 50));
      }, 120);
    });
    el.addEventListener("toggle", function (e) {
      if (e.target.id === "mm-whole" && e.target.open) { var box = el.querySelector("#mm-json"); if (!box.textContent) box.textContent = JSON.stringify(doc, null, 1); }
    }, true);
  }
  // after a render: hand each chart its series for the crosshair
  function after() {
    var h = el.querySelector("#mm-hchart svg");
    if (h && hist) bind(h, seriesOf(hist, st.hkey, st.hspan, st.hcum));
  }
  function download() {
    var list = filtered(), blob = new Blob([csvOf(list, fdoc.cols)], { type: "text/csv" }), a = root.document.createElement("a");
    a.href = URL.createObjectURL(blob); a.download = "money-flow-" + doc.as_of + "-" + list.length + "-funds.csv";
    root.document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 4000);
  }
  function setView(view, push) {
    st.view = view === "research" ? "research" : "map";
    var research = root.document.getElementById("research-view");
    el.classList.toggle("mm-research", st.view === "research");
    if (research) research.hidden = st.view !== "research";
    Array.prototype.forEach.call(el.querySelectorAll("[data-view]"), function (b) { b.classList.toggle("on", b.getAttribute("data-view") === st.view); });
    if (push) {
      try { root.history.replaceState(null, "", st.view === "research" ? "#research" : root.location.pathname + root.location.search); } catch (e) { /* a sandboxed frame */ }
      if (st.view === "research") root.dispatchEvent(new Event("resize"));           // the research chart sizes itself on show
    }
  }
  function get(url) {
    return root.fetch(url, { cache: "no-store" }).then(function (r) { if (!r.ok) throw new Error(url + " " + r.status); return r.json(); });
  }
  function mount() {
    el = root.document.getElementById("mm-root");
    if (!el) return;
    var style = root.document.createElement("style");
    style.textContent = CSS;
    root.document.head.appendChild(style);
    el.classList.add("mm");
    if (embedded) el.classList.add("mm-embed");
    shell();
    wire();
    var wantResearch = /(^|[?&])run=/.test(root.location.search) || root.location.hash === "#research";
    setView(wantResearch ? "research" : "map", false);
    get(DOCS.main).then(function (d) {
      if (!d || d.contract !== "global-flow-map.v1") throw new Error("not the money map document");
      doc = d; asof(); render(); after();
      get(DOCS.funds).then(function (f) {
        fdoc = f; funds = fundObjects(f); byT = {};
        funds.forEach(function (x) { byT[x.t] = x; });
        render(); after();
        var open = el.querySelector("#mm-glist .mm-empty");
        if (open && st.gs) el.querySelector("#mm-glist").innerHTML = sheetList();
      }).catch(function (err) { var box = el.querySelector("#mm-funds .mm-empty"); if (box) box.textContent = "The funds document could not be read: " + err.message; });
      get(DOCS.history).then(function (h) { hist = h; render(); after(); }).catch(function () { /* the history card says it is loading */ });
    }).catch(function (err) {
      el.querySelector("#mm-asof").innerHTML = '<span class="mm-bad">The money map could not be read (' + esc(err.message) + ").</span> Nothing is shown in its place. The research view still works.";
    });
  }
  api.mount = mount;
  api._state = function () { return { st: st, doc: doc, funds: funds.length, hist: !!hist }; };
  if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", mount); else mount();
  return api;
});
