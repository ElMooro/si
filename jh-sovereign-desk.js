/* Sovereign stress desk (sovereign-desk.html) - 2026-11-22, second round the same day.
 *
 * Reads data/ciss-sovereign.json, compiled by the CISS engine from retained ECB originals: SovCISS by country and for
 * the euro area, CISS by country (the United States, China and the United Kingdom among them), the all-issuer / AAA
 * government curve, 10-year yields and spreads to Germany, TARGET balances, government debt, deficit and interest.
 * CDS and ratings beside each government come from the site's world document (data/global-sovereign.json) and say so.
 * Every number on the page is a dated measurement from those documents; the page formats, ranks, sorts and draws,
 * and re-reads each date against today's so an old reading says how old it is.  Nothing here is a forecast or a score
 * of its own.  Colour tags and the sort the reader chose are kept in this browser only.
 *
 * The first part is pure (exported for tests/sovereign-desk.test.js); the second part is the page.
 */
(function (root) {
  "use strict";

  var DAY = 86400000;
  var ALLOW = { D: 10, W: 21, M: 100, Q: 230 };   /* days after a period's end before a reading is called old - the engine's own allowances */
  var MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  var UNITS = {
    index: { d: 4, pre: "", suf: "", chg: "" },
    pct: { d: 2, pre: "", suf: "%", chg: " pp" },
    bp: { d: 1, pre: "", suf: " bp", chg: " bp" },
    eur_bn: { d: 1, pre: "€", suf: " bn", chg: " bn" },
    pct_gdp: { d: 1, pre: "", suf: "%", chg: " pp" },
    usd: { d: 4, pre: "$", suf: "", chg: "" }
  };
  var RANGES = [["1Y", 1], ["5Y", 5], ["10Y", 10], ["All", 0]];
  var SPAN = { D: "last 12 months", W: "last 2 years", M: "last 5 years", Q: "last 10 years" };   /* what a tile's small line covers */
  /* colour tags, in the order tagged countries are listed */
  var TAGS = [["amber", "#F0B429"], ["red", "#E07A6A"], ["green", "#6fce8a"], ["blue", "#5b9bd5"], ["purple", "#a78bfa"], ["white", "#e8e8e8"]];
  /* crisis-view sets; the first measure of a set is the one whose own highest readings are listed below the table */
  var CORE = ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "FI", "IE", "PT", "GR"];
  var ENDED = ["GB", "PL", "HU", "CZ", "SE", "DK"];
  function pre(p, list) { return list.map(function (a) { return p + a; }); }
  var SETS = [
    { id: "sovciss", name: "SovCISS by country", ids: ["EA_SOV_GDP"].concat(pre("SOV_", CORE), pre("SOVM_", ENDED)) },
    { id: "spread", name: "10-year spreads to Germany", ids: ["EA_CURVE_10Y"].concat(pre("S10_", CORE.slice(1)), pre("S10_", ["PL", "HU", "RO", "CZ"])) },
    { id: "ciss", name: "CISS by country", ids: ["EA_CISS"].concat(pre("CISS_", ["US", "CN", "GB", "DE", "FR", "IT", "ES", "NL", "BE", "AT", "FI", "IE", "PT"])) },
    { id: "euro", name: "Euro area and TARGET", ids: ["EA_SOV_GDP", "EA_SOV_EQ", "EA_SOV_RANGE", "EA_CURVE_10Y", "EA_CURVE_2Y", "EA_CISS", "EA_TGT_DEBT", "TGT_DE", "TGT_IT", "TGT_ES"] }
  ];
  var SHORT = { EA_SOV_GDP: "Euro area", EA_SOV_EQ: "EA equal-wt", EA_SOV_RANGE: "High − low", EA_CURVE_10Y: "All − AAA 10y", EA_CURVE_2Y: "All − AAA 2y",
                EA_CISS: "Euro area CISS", EA_TGT_DEBT: "TARGET owed", TGT_DE: "TARGET DE", TGT_IT: "TARGET IT", TGT_ES: "TARGET ES" };
  var ALIAS = { "czech republic": "czechia", "united states of america": "united states", "korea": "south korea" };
  /* line colours for a chart of many series; tagged countries take their tag's colour instead */
  var LINES = ["#7fb7e6", "#e39b6a", "#8fd0a6", "#c6a2f2", "#e6d37a", "#e58fb1", "#6fd0d0", "#b9c3cf", "#d9a05b", "#9fb4ff", "#c7e08a", "#f0a3a3", "#8bd3c7", "#d0b4ff", "#ffd27f", "#a8c5e6", "#e0b0a0", "#b4e0b0"];

  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function dayNum(iso) { return Date.UTC(+iso.slice(0, 4), +iso.slice(5, 7) - 1, +iso.slice(8, 10)) / DAY; }
  function esc(t) { return String(t == null ? "" : t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"); }
  function fixed(v, d) { return Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d }); }

  function fmt(unit, v) {
    if (!isNum(v)) return "—";
    var u = UNITS[unit] || UNITS.index;
    return (v < 0 && +v.toFixed(u.d) !== 0 ? "−" : "") + u.pre + fixed(v, u.d) + u.suf;
  }
  function fmtChange(unit, v) {
    if (!isNum(v)) return "—";
    var u = UNITS[unit] || UNITS.index, flat = +v.toFixed(u.d) === 0;
    return (flat ? "" : v > 0 ? "+" : "−") + fixed(v, u.d) + u.chg;
  }
  function fmtDate(iso) { return iso ? +iso.slice(8, 10) + " " + MONTHS[+iso.slice(5, 7) - 1] + " " + iso.slice(0, 4) : "—"; }
  /* the period a reading stands for: a day, a month or a quarter */
  function when(m) {
    if (!m || !m.date) return "—";
    if (m.freq === "M") return MONTHS[+m.date.slice(5, 7) - 1] + " " + m.date.slice(0, 4);
    if (m.freq === "Q") return "Q" + Math.ceil(+m.date.slice(5, 7) / 3) + " " + m.date.slice(0, 4);
    return fmtDate(m.date);
  }
  /* how old a reading is today, and whether that is older than its source's rhythm allows */
  function age(m, todayIso) {
    var days = Math.round(dayNum(todayIso) - dayNum(m.date));
    return { days: days, old: days > (ALLOW[m.freq] || ALLOW.D) };
  }
  /* is a rise in this measure a rise in stress?  true, or null where the page takes no side (a yield, a balance, a debt ratio) */
  function worseUp(m) {
    if (!m) return null;
    if (m.group === "sovciss" || m.group === "sovciss_ended" || m.group === "ciss" || m.group === "clifs" || m.group === "spread" || m.group === "contribution") return true;
    if (m.group === "euro_area") return true;
    return null;
  }
  /* 0..4 from the reading's place in its own history, for stress measures that are still published */
  function heat(m) {
    if (!m || worseUp(m) !== true || !isNum(m.pctile) || m.status === "ended") return 0;
    return m.pctile >= 95 ? 4 : m.pctile >= 85 ? 3 : m.pctile >= 70 ? 2 : m.pctile >= 50 ? 1 : 0;
  }
  function standing(m) {
    if (!m || !isNum(m.pctile)) return "";
    var since = " of its readings since " + m.start.slice(0, 4);
    return m.pctile >= 50 ? "higher than " + Math.floor(m.pctile) + "%" + since : "lower than " + Math.floor(100 - m.pctile) + "%" + since;
  }
  function record(m) { return m && m.high ? "record " + fmt(m.unit, m.high[0]) + ", " + fmtDate(m.high[1]) : ""; }

  function sparkPath(values, w, h) {
    var pts = (values || []).filter(isNum);
    if (pts.length < 2) return "";
    var lo = Math.min.apply(null, pts), hi = Math.max.apply(null, pts), span = hi - lo || 1;
    return pts.map(function (v, i) { return (i ? "L" : "M") + (i / (pts.length - 1) * w).toFixed(1) + "," + (h - 1 - (v - lo) / span * (h - 2)).toFixed(1); }).join(" ");
  }

  /* one key for a country across the ECB's names and the world document's */
  function ckey(name) { var k = String(name || "").trim().toLowerCase(); return ALIAS[k] || k; }

  /* the country boards: governments with a SovCISS (ranked by it), the other EU governments, the six whose SovCISS the ECB
     stopped, and every country CISS.  The order here is the default; the page sorts and tags on top of it. */
  function board(doc) {
    var M = doc.measures || {};
    var rows = (doc.countries || []).map(function (c) {
      var m = {};
      Object.keys(c.ids).forEach(function (k) { m[k] = M[c.ids[k]] || null; });
      return { area: c.area, country: c.country, key: ckey(c.country), euro: c.euro, m: m };
    });
    function val(r, k) { return r.m[k] && isNum(r.m[k].value) ? r.m[k].value : -Infinity; }
    function crisisPeak(m) { var a = (m.peaks || {}).italy2011, b = (m.peaks || {}).spain2012; return a && b ? (a.hi[0] >= b.hi[0] ? a.hi : b.hi) : (a || b || {}).hi || null; }
    var germany = M.SOV_DE;
    var sovereign = rows.filter(function (r) { return r.m.sovciss; }).sort(function (a, b) { return val(b, "sovciss") - val(a, "sovciss") || (a.area < b.area ? -1 : 1); });
    sovereign.forEach(function (r) {
      r.vsGermany = germany && r.area !== "DE" && r.m.sovciss.date === germany.date ? +(r.m.sovciss.value - germany.value).toFixed(5) : null;
      r.crisis = crisisPeak(r.m.sovciss);
    });
    var others = rows.filter(function (r) { return !r.m.sovciss && (r.m.yield || r.m.debt || r.m.target); })
      .sort(function (a, b) { return val(b, "spread") - val(a, "spread") || (a.country < b.country ? -1 : 1); });
    var ended = rows.filter(function (r) { return r.m.sovciss_ended; }).sort(function (a, b) { return val(b, "sovciss_ended") - val(a, "sovciss_ended"); });
    ended.forEach(function (r) { r.crisis = crisisPeak(r.m.sovciss_ended); });
    var systemic = rows.filter(function (r) { return r.m.ciss; }).sort(function (a, b) { return val(b, "ciss") - val(a, "ciss"); });
    return { sovereign: sovereign, others: others, ended: ended, systemic: systemic };
  }

  /* the date most rows of a column carry, so the header can say it once */
  function commonWhen(rows, kind) {
    var seen = {}, best = null;
    rows.forEach(function (r) { var m = r.m[kind]; if (m) { var w = when(m); seen[w] = (seen[w] || 0) + 1; if (!best || seen[w] > seen[best]) best = w; } });
    return best;
  }

  /* the world document by country key: reported fields only */
  function joinWorld(world) {
    var out = {};
    ((world || {}).countries || []).forEach(function (c) {
      out[ckey(c.country)] = { cds: isNum(c.cds_bp) ? c.cds_bp : null, rating: c.rating || null, y: isNum(c.yield_10y_pct) ? c.yield_10y_pct : null, asOf: c.as_of || null };
    });
    return out;
  }

  /* where a tagged country stands among the tags: 0 for the first colour ... -1 for untagged */
  function tagRank(tags, key) {
    var colour = (tags || {})[key];
    for (var i = 0; i < TAGS.length; i++) if (TAGS[i][0] === colour) return i;
    return -1;
  }
  /* Tagged rows first, in the palette's order; inside the tagged and inside the rest, by the chosen column and direction.
     A row with no value in that column goes to the end of its part whichever way the sort runs.
     opt: { tags, value(row) -> number | string | null, dir: -1 (high to low) | 1, show: "all" | "tagged" | a colour } */
  function arrange(rows, opt) {
    var tags = opt.tags || {}, dir = opt.dir === 1 ? 1 : -1, show = opt.show || "all";
    var kept = rows.filter(function (r) { return show === "all" || (show === "tagged" ? tagRank(tags, r.key) >= 0 : tags[r.key] === show); });
    return kept.map(function (r, i) { return [r, i]; }).sort(function (a, b) {
      var ta = tagRank(tags, a[0].key), tb = tagRank(tags, b[0].key);
      if ((ta >= 0) !== (tb >= 0)) return ta >= 0 ? -1 : 1;
      if (ta !== tb) return ta - tb;
      var va = opt.value ? opt.value(a[0]) : null, vb = opt.value ? opt.value(b[0]) : null;
      var na = va === null || va === undefined || va === "", nb = vb === null || vb === undefined || vb === "";
      if (na !== nb) return na ? 1 : -1;
      if (!na && va !== vb) return (typeof va === "string" || typeof vb === "string" ? String(va).localeCompare(String(vb)) : va - vb) * dir;
      return a[1] - b[1];
    }).map(function (x) { return x[0]; });
  }
  /* the next sort when a header is clicked: the same column flips, a new one starts high to low (names start A to Z) */
  function nextSort(cur, col, first) {
    if (cur && cur[0] === col) return [col, -cur[1]];
    return [col, first === 1 ? 1 : -1];
  }

  /* the page's opening lines, in words, from the document's own numbers */
  function read(doc) {
    var M = doc.measures || {}, out = [], b = board(doc), ea = M.EA_SOV_GDP;
    if (ea) out.push("Euro-area sovereign stress (SovCISS, GDP-weighted) reads " + fmt("index", ea.value) + " on " + fmtDate(ea.date) + " — " + standing(ea) + "; " + record(ea) + ".");
    if (b.sovereign.length >= 4) {
      var top = b.sovereign.slice(0, 3).map(function (r) { return r.country + " " + fmt("index", r.m.sovciss.value); }).join(", ");
      var last = b.sovereign[b.sovereign.length - 1];
      out.push("Highest: " + top + ". Lowest: " + last.country + " " + fmt("index", last.m.sovciss.value) + "."
        + (M.EA_SOV_RANGE ? " Highest minus lowest: " + fmt("index", M.EA_SOV_RANGE.value) + " (" + record(M.EA_SOV_RANGE) + ")." : ""));
    }
    var curve = M.EA_CURVE_10Y;
    if (curve) out.push("All euro-area governments together pay " + fmt("bp", curve.value) + " more than the AAA-rated ones at 10 years (" + fmtDate(curve.date) + "; " + record(curve) + ").");
    var spreads = (doc.countries || []).filter(function (c) { return c.euro && M[c.ids.spread]; }).map(function (c) { return [c.country, M[c.ids.spread]]; })
      .sort(function (a, b) { return b[1].value - a[1].value; }).slice(0, 4);
    if (spreads.length) out.push("Widest 10-year spreads to Germany in the euro area (" + when(spreads[0][1]) + "): "
      + spreads.map(function (s) { return s[0] + " " + fmt("bp", s[1].value); }).join(", ") + ".");
    var moved = b.sovereign.filter(function (r) { return isNum(r.m.sovciss.change["1w"]); }).sort(function (x, y) { return y.m.sovciss.change["1w"] - x.m.sovciss.change["1w"]; });
    if (moved.length >= 2) {
      var up = moved[0], down = moved[moved.length - 1];
      out.push("Over the past week: " + (up.m.sovciss.change["1w"] > 0 ? "largest rise " + up.country + " " + fmtChange("index", up.m.sovciss.change["1w"]) : "no government's SovCISS rose")
        + "; " + (down.m.sovciss.change["1w"] < 0 ? "largest fall " + down.country + " " + fmtChange("index", down.m.sovciss.change["1w"]) : "none fell") + ".");
    }
    var world = (doc.world_ciss || []).map(function (id) { return M[id]; }).filter(Boolean);
    if (world.length) out.push("Systemic stress outside the euro area, by the ECB's CISS: " + world.map(function (m) {
      return m.label.replace(" CISS", "") + " " + fmt("index", m.value) + (m.date !== (M.EA_CISS || {}).date ? " (" + fmtDate(m.date) + ")" : ""); }).join(", ")
      + (M.EA_CISS ? "; euro area " + fmt("index", M.EA_CISS.value) : "") + ".");
    return out;
  }

  /* a TARGET balance that is negative on average belongs to a central bank that owes: it is read at its lowest; everything else at its highest */
  function side(m) { return m && m.group === "target" && m.mean < 0 ? "lo" : "hi"; }

  /* a set's columns: its first series, then tagged countries in the palette's order, then the rest as the set lists them */
  function orderIds(ids, M, tags, names) {
    var have = ids.filter(function (id) { return M[id]; });
    if (!have.length) return have;
    var rest = have.slice(1).map(function (id, i) { var area = M[id].area, key = area && names[area] ? ckey(names[area]) : null; return [id, i, key ? tagRank(tags, key) : -1]; });
    rest.sort(function (a, b) { if ((a[2] >= 0) !== (b[2] >= 0)) return a[2] >= 0 ? -1 : 1; return a[2] !== b[2] ? a[2] - b[2] : a[1] - b[1]; });
    return [have[0]].concat(rest.map(function (x) { return x[0]; }));
  }

  /* the crisis table: one row per named window that any column has a reading in, one cell per measure;
     `share` is the cell against the measure's own record */
  function crisis(doc, ids) {
    var M = doc.measures || {};
    var cols = ids.map(function (id) { return M[id]; }).filter(Boolean);
    function cell(m, pair) {
      if (!pair || !isNum(pair[0])) return null;
      var ref = side(m) === "lo" ? m.low[0] : m.high[0];
      return { v: pair[0], d: pair[1], share: ref ? Math.max(0, Math.min(1, pair[0] / ref)) : 0 };
    }
    var rows = (doc.episodes || []).map(function (ep) {
      return { ep: ep, cells: cols.map(function (m) { var p = (m.peaks || {})[ep.id]; return cell(m, p ? p[side(m)] : null); }) };
    }).filter(function (r) { return r.cells.some(Boolean); });
    return { cols: cols, rows: rows,
             now: cols.map(function (m) { return m.status === "ended" ? null : cell(m, [m.value, m.date]); }),
             record: cols.map(function (m) { return cell(m, side(m) === "lo" ? m.low : m.high); }) };
  }

  /* which named window a date falls in, if any */
  function windowOf(episodes, iso) {
    for (var i = 0; i < (episodes || []).length; i++) if (episodes[i].from <= iso && iso <= episodes[i].to) return episodes[i];
    return null;
  }

  /* the part of a history a range asks for: {d, v} from `years` before the last date (0 = all), or between two dates */
  function visible(series, years, span) {
    if (!series || !series.d || !series.d.length) return { d: [], v: [] };
    if (span) {
      var a = 0, b = series.d.length;
      while (a < b && series.d[a] < span[0]) a++;
      while (b > a && series.d[b - 1] > span[1]) b--;
      return { d: series.d.slice(a, b), v: series.v.slice(a, b) };
    }
    if (!years) return { d: series.d.slice(), v: series.v.slice() };
    var last = series.d[series.d.length - 1], from = (+last.slice(0, 4) - years) + last.slice(4), i = 0;
    while (i < series.d.length && series.d[i] < from) i++;
    return { d: series.d.slice(i), v: series.v.slice(i) };
  }
  /* a year either side of a named window */
  function around(ep) { return [(+ep.from.slice(0, 4) - 1) + ep.from.slice(4), (+ep.to.slice(0, 4) + 1) + ep.to.slice(4)]; }

  /* the named windows that overlap a span, as fractions of its width */
  function bands(episodes, from, to) {
    var a = dayNum(from), span = dayNum(to) - a || 1;
    return (episodes || []).filter(function (ep) { return ep.to >= from && ep.from <= to; }).map(function (ep) {
      return { ep: ep, x0: Math.max(0, (dayNum(ep.from) - a) / span), x1: Math.min(1, (dayNum(ep.to) - a) / span) };
    });
  }

  var LEAD = "#35d0e0";   /* the first series of a set on a chart of many: a colour no tag uses */
  /* a line's colour on a chart of many: the country's tag if it has one, else its place in the palette */
  function lineColour(i, tag) {
    for (var k = 0; k < TAGS.length; k++) if (TAGS[k][0] === tag) return TAGS[k][1];
    return LINES[i % LINES.length];
  }

  function ecbPage(m) {
    if (!m || !m.key) return null;
    return "https://data.ecb.europa.eu/data/datasets/" + m.key.split(".")[0] + "/" + m.key;
  }
  function dataPath(key) { return typeof key === "string" && /^data\/[A-Za-z0-9_.\/-]+$/.test(key) && key.indexOf("..") < 0 ? "/" + key : null; }

  /* the world table: reported fields only, widest CDS first */
  function world(doc) {
    return ((doc || {}).countries || []).map(function (c) {
      return { country: c.country, key: ckey(c.country), region: c.region, y: c.yield_10y_pct, spread: c.spread_vs_bund_bp, cds: c.cds_bp, rating: c.rating, policy: c.cb_rate_pct, asOf: c.as_of };
    }).sort(function (a, b) { return (isNum(b.cds) ? b.cds : -1) - (isNum(a.cds) ? a.cds : -1) || (a.country < b.country ? -1 : 1); });
  }

  var api = { fmt: fmt, fmtChange: fmtChange, fmtDate: fmtDate, when: when, age: age, worseUp: worseUp, heat: heat, standing: standing, record: record, sparkPath: sparkPath,
              board: board, commonWhen: commonWhen, read: read, crisis: crisis, side: side, windowOf: windowOf, visible: visible, around: around, bands: bands, ecbPage: ecbPage,
              dataPath: dataPath, world: world, ckey: ckey, joinWorld: joinWorld, tagRank: tagRank, arrange: arrange, nextSort: nextSort, orderIds: orderIds, lineColour: lineColour,
              SETS: SETS, RANGES: RANGES, ALLOW: ALLOW, TAGS: TAGS };
  root.JHSovereignDesk = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;

  /* ------------------------------------------------------------------------------------------------ the page */
  var doc = root.document;
  if (!doc || !doc.getElementById("sov-main")) return;
  var $ = function (id) { return doc.getElementById(id); };
  var D = null, H = null, W = null, WJ = {}, view = "desk", setId = "sovciss";
  var chart = { id: null, years: 0, span: null, ep: null, geo: null }, over = { years: 0, hidden: {}, geo: null };
  var today = new Date().toISOString().slice(0, 10);
  var KEY = "jh.sovereign-desk.v1", prefs = { tags: {}, sort: {}, show: "all" };
  try { var kept = JSON.parse(root.localStorage.getItem(KEY) || "null"); if (kept && typeof kept === "object") { prefs.tags = kept.tags || {}; prefs.sort = kept.sort || {}; prefs.show = kept.show || "all"; } } catch (e) { /* a browser that keeps nothing still works */ }
  function save() { try { root.localStorage.setItem(KEY, JSON.stringify(prefs)); } catch (e) { /* kept for this visit only */ } }

  function getJson(path) {
    return fetch(path, { cache: "no-store" }).then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); });
  }
  function needHistory(then) {
    if (H) { then(); return; }
    getJson("/data/ciss-sovereign-history.json").then(function (h) { H = h; then(); }).catch(function () { H = { series: {} }; then(); });
  }
  function cls(m, v) { var w = worseUp(m); return w === null || !isNum(v) || v === 0 ? "" : (v > 0) === w ? "up" : "dn"; }
  function oldNote(m) { if (m.status === "ended") return ""; var a = age(m, today); return a.old ? " <span class=old>" + a.days + " days old</span>" : ""; }
  function spark(m, w, h) { var p = sparkPath(m.spark, w, h); return p ? "<svg class=sp viewBox='0 0 " + w + " " + h + "' width=" + w + " height=" + h + " aria-hidden=true><path d='" + p + "'/></svg>" : ""; }
  function chg(m, k) { var v = m.change[k]; return "<span class='" + cls(m, v) + "'>" + k + " " + fmtChange(m.unit, v) + "</span>"; }
  function tagColour(key) { var i = tagRank(prefs.tags, key); return i < 0 ? null : TAGS[i][1]; }

  function tile(m) {
    var bar = worseUp(m) === true ? "<div class=tp title='" + esc(standing(m)) + "'><i style='width:" + Math.max(1, m.pctile).toFixed(1) + "%'></i></div>" : "";
    return "<div class='tile h" + heat(m) + "' data-id='" + esc(m.id) + "' tabindex=0 role=button>"
      + "<div class=tl>" + esc(m.label) + "</div>"
      + "<div class=tv>" + fmt(m.unit, m.value) + " <span class=td>" + when(m) + oldNote(m) + "</span></div>"
      + "<div class=tc>" + (m.freq === "D" || m.freq === "W" ? chg(m, "1w") + " " : "") + (m.freq !== "Q" ? chg(m, "1m") + " " : chg(m, "3m") + " ") + chg(m, "12m") + "</div>"
      + bar + "<div class=tn>" + esc(standing(m)) + (m.high ? " · " + esc(record(m)) : "") + "</div>" + spark(m, 220, 30) + "<div class=ts>" + SPAN[m.freq] + "</div></div>";
  }

  /* ---- tables: one description per table, sorted and tagged the same way ---- */
  function num(m) { return m && isNum(m.value) ? m.value : null; }
  function mcell(m, common, extra) {
    if (!m) return "<td class=nil>—</td>";
    return "<td class='c " + (extra || "") + "' data-id='" + esc(m.id) + "' title='" + esc(m.label + " · " + when(m)) + "'>" + fmt(m.unit, m.value)
      + (common && when(m) !== common ? " <span class=old>" + when(m) + "</span>" : "") + oldNote(m) + "</td>";
  }
  function pctBar(m) { return "<span class=pb title='" + esc(standing(m)) + "'><i class='h" + heat(m) + "' style='width:" + Math.max(2, m.pctile).toFixed(0) + "%'></i></span><span class=pn>" + Math.round(m.pctile) + "</span>"; }
  function peakCell(p) { return "<td>" + (p ? fmt("index", p[0]) + " <span class=old>" + MONTHS[+p[1].slice(5, 7) - 1] + " " + p[1].slice(0, 4) + "</span>" : "—") + "</td>"; }
  function chgCell(m, k) { return m ? "<td class='" + cls(m, m.change[k]) + "'>" + fmtChange(m.unit, m.change[k]) + "</td>" : "<td class=nil>—</td>"; }
  function mcol(kind, label, rows) {
    var common = commonWhen(rows, kind);
    return { id: kind, label: label, small: common || "", value: function (r) { return num(r.m[kind]); }, cell: function (r) { return mcell(r.m[kind], common, ""); } };
  }
  function wcol() {
    return [{ id: "cds", label: "CDS", small: "world doc", value: function (r) { return (WJ[r.key] || {}).cds; }, cell: function (r) { var w = WJ[r.key]; return "<td>" + (w && isNum(w.cds) ? fixed(w.cds, 1) + " bp" : "<span class=nil>—</span>") + "</td>"; } },
            { id: "rating", label: "Rating", small: "world doc", cls: "l", first: 1, value: function (r) { return (WJ[r.key] || {}).rating; }, cell: function (r) { var w = WJ[r.key]; return "<td class=l>" + esc(w && w.rating ? w.rating : "—") + "</td>"; } }];
  }
  function tagButton(r) {
    var col = tagColour(r.key);
    return "<button class=tg data-tag='" + esc(r.key) + "' title='Tag " + esc(r.country) + " with a colour' aria-label='Tag " + esc(r.country) + "' style='" + (col ? "background:" + col + ";border-color:" + col : "") + "'></button>";
  }
  function nameCol(primary) {
    return { id: "country", label: "Country", cls: "l", first: 1, value: function (r) { return r.country; },
             cell: function (r) { var m = primary(r);
               return "<td class='l nm'>" + tagButton(r) + (m ? "<span class=cn data-id='" + esc(m.id) + "' tabindex=0 role=button title='" + esc(m.label) + " — its history'>" + esc(r.country) + "</span>" : esc(r.country)) + "</td>"; } };
  }
  function table(id, cols, rows, fallback, cap) {
    var s = prefs.sort[id] || fallback, col = cols.filter(function (c) { return c.id === s[0] && c.value; })[0];
    if (!col) { s = fallback; col = cols.filter(function (c) { return c.id === fallback[0]; })[0]; }
    var shown = arrange(rows, { tags: prefs.tags, value: col.value, dir: s[1], show: prefs.show });
    var html = "<div class=scroll><table class=board data-table='" + id + "' data-sorted='" + col.id + ":" + s[1] + "'><thead><tr>" + cols.map(function (c) {
      var on = c.id === col.id;
      return "<th class='" + (c.cls || "") + (c.value ? " sort" : "") + (on ? " on" : "") + "'" + (c.value ? " data-sort='" + c.id + "' data-first='" + (c.first || -1) + "' title='Sort by " + esc(c.label) + "'" : "") + ">" + esc(c.label)
        + (on ? " <i class=ar>" + (s[1] === 1 ? "▲" : "▼") + "</i>" : "") + (c.small ? "<small>" + esc(c.small) + "</small>" : "") + "</th>"; }).join("") + "</tr></thead><tbody>";
    shown.forEach(function (r) { var colr = tagColour(r.key); html += "<tr data-key='" + esc(r.key) + "'" + (colr ? " class=tagged style='box-shadow:inset 3px 0 0 " + colr + "'" : "") + ">" + cols.map(function (c) { return c.cell(r); }).join("") + "</tr>"; });
    if (!shown.length) html += "<tr><td class='l nil' colspan=" + cols.length + ">" + (rows.length ? "No country here carries the tag being shown." : "Not in this document.") + "</td></tr>";
    return html + "</tbody></table></div>" + (cap ? "<p class=note>" + cap + "</p>" : "");
  }

  function tagBar() {
    var used = {}; Object.keys(prefs.tags).forEach(function (k) { used[prefs.tags[k]] = (used[prefs.tags[k]] || 0) + 1; });
    var n = Object.keys(prefs.tags).length;
    return "<div class=tagbar><span class=tb>Tags</span><span class=th>Click the dot beside a country to give it a colour: tagged countries move to the top of every table and lead the charts. Click a column heading to sort, again to reverse.</span>"
      + "<button class='gbtn" + (prefs.show === "all" ? " on" : "") + "' data-show=all>All</button><button class='gbtn" + (prefs.show === "tagged" ? " on" : "") + "' data-show=tagged>Tagged" + (n ? " " + n : "") + "</button>"
      + TAGS.filter(function (t) { return used[t[0]]; }).map(function (t) { return "<button class='gbtn sw" + (prefs.show === t[0] ? " on" : "") + "' data-show='" + t[0] + "' title='Only " + t[0] + "'><i style='background:" + t[1] + "'></i>" + used[t[0]] + "</button>"; }).join("")
      + (n ? "<button class=gbtn data-cleartags=1>Clear tags</button>" : "") + "</div>";
  }

  function renderDesk() {
    var M = D.measures, b = board(D), html = "";
    html += "<div class=read>" + read(D).map(function (s) { return "<p>" + esc(s) + "</p>"; }).join("") + "</div>";
    html += "<div class=sec>Euro area</div><div class=tiles>" + D.euro_area.map(function (id) { return tile(M[id]); }).join("") + "</div>";
    var outside = (D.world_ciss || ["CISS_US", "CISS_CN", "CISS_GB"]).map(function (id) { return M[id]; }).filter(Boolean);
    if (outside.length) html += "<div class=sec>Systemic stress outside the euro area · the ECB's CISS for the United States, China and the United Kingdom</div><div class=tiles id=sov-world-tiles>" + outside.map(tile).join("") + "</div>";
    html += tagBar();

    var first = b.sovereign[0] && b.sovereign[0].m.sovciss, S = function (r) { return r.m.sovciss; };
    html += "<div class=sec>Governments with a SovCISS" + (first ? " · " + when(first) : "") + " · click a country or any number for its history</div>" + table("sovereign", [
      nameCol(S),
      { id: "sovciss", label: "SovCISS", value: function (r) { return num(S(r)); }, cell: function (r) { var s = S(r); return "<td class='c h" + heat(s) + "' data-id='" + esc(s.id) + "'><b>" + fmt("index", s.value) + "</b>" + (when(s) !== when(first) ? " <span class=old>" + when(s) + "</span>" : "") + "</td>"; } },
      { id: "vsde", label: "vs Germany", value: function (r) { return r.vsGermany; }, cell: function (r) { return "<td>" + (r.vsGermany === null ? "—" : fmtChange("index", r.vsGermany)) + "</td>"; } },
      { id: "w1", label: "1w", value: function (r) { return S(r).change["1w"]; }, cell: function (r) { return chgCell(S(r), "1w"); } },
      { id: "m1", label: "1m", value: function (r) { return S(r).change["1m"]; }, cell: function (r) { return chgCell(S(r), "1m"); } },
      { id: "m12", label: "12m", value: function (r) { return S(r).change["12m"]; }, cell: function (r) { return chgCell(S(r), "12m"); } },
      { id: "hist", label: "Own history", cls: "l", value: function (r) { return S(r).pctile; }, cell: function (r) { return "<td class=l>" + pctBar(S(r)) + "</td>"; } },
      { id: "peak", label: "2011–12 peak", value: function (r) { return r.crisis ? r.crisis[0] : null; }, cell: function (r) { return peakCell(r.crisis); } },
      mcol("ciss", "CISS", b.sovereign), mcol("yield", "10y yield", b.sovereign), mcol("spread", "Spread to DE", b.sovereign)].concat(wcol(), [
      mcol("target", "TARGET", b.sovereign), mcol("debt", "Debt / GDP", b.sovereign), mcol("deficit", "Balance", b.sovereign), mcol("interest", "Interest", b.sovereign), mcol("clifs", "CLIFS", b.sovereign),
      { id: "spark", label: "1 year", cls: "l", cell: function (r) { return "<td class=l>" + spark(S(r), 110, 22) + "</td>"; } }]), b.sovereign, ["sovciss", -1],
      "SovCISS, CISS and CLIFS: the ECB's composite stress indicators, 0 to 1. Balance = government deficit (−) or surplus, and Interest = interest paid, both four-quarter sums in percent of GDP (ECB). "
      + "CDS and rating are as reported in the site's world document, not ECB data.");

    var P = function (r) { return r.m.spread || r.m.yield || r.m.debt; };
    html += "<div class=sec>Other EU governments · the ECB publishes no daily SovCISS for them</div>" + table("others", [
      nameCol(P), { id: "cur", label: "Currency", cls: "l", first: 1, value: function (r) { return r.euro ? "euro" : "own"; }, cell: function (r) { return "<td class=l>" + (r.euro ? "euro" : "own") + "</td>"; } },
      mcol("yield", "10y yield", b.others), mcol("spread", "Spread to DE", b.others),
      { id: "s12", label: "Spread 12m", value: function (r) { return r.m.spread ? r.m.spread.change["12m"] : null; }, cell: function (r) { return chgCell(r.m.spread, "12m"); } }].concat(wcol(), [
      mcol("target", "TARGET", b.others), mcol("debt", "Debt / GDP", b.others), mcol("deficit", "Balance", b.others), mcol("interest", "Interest", b.others), mcol("clifs", "CLIFS", b.others)]), b.others, ["spread", -1]);

    if (b.ended.length) {
      var E = function (r) { return r.m.sovciss_ended; }, lehman = function (r) { return ((E(r).peaks || {}).gfc2008 || {}).hi || null; };
      html += "<div class=sec>SovCISS the ECB stopped publishing · monthly series to " + when(E(b.ended[0])) + ", kept for their history</div>" + table("ended", [
        nameCol(E),
        { id: "last", label: "Last reading", small: when(E(b.ended[0])), value: function (r) { return num(E(r)); }, cell: function (r) { return "<td class=c data-id='" + esc(E(r).id) + "'>" + fmt("index", E(r).value) + "</td>"; } },
        { id: "rec", label: "Record", value: function (r) { return E(r).high[0]; }, cell: function (r) { return peakCell(E(r).high); } },
        { id: "gfc", label: "2008–09 peak", value: function (r) { return lehman(r) ? lehman(r)[0] : null; }, cell: function (r) { return peakCell(lehman(r)); } },
        { id: "peak", label: "2011–12 peak", value: function (r) { return r.crisis ? r.crisis[0] : null; }, cell: function (r) { return peakCell(r.crisis); } },
        { id: "from", label: "History from", cls: "l", first: 1, value: function (r) { return E(r).start; }, cell: function (r) { return "<td class=l>" + MONTHS[+E(r).start.slice(5, 7) - 1] + " " + E(r).start.slice(0, 4) + "</td>"; } },
        mcol("yield", "10y yield", b.ended), mcol("spread", "Spread to DE", b.ended)].concat(wcol(), [mcol("debt", "Debt / GDP", b.ended), mcol("clifs", "CLIFS", b.ended)]), b.ended, ["last", -1],
        "The ECB publishes a daily SovCISS for eleven euro-area governments and for no others. These six had one in its monthly series, which it stopped in 2025: the history is real, the last reading is not current.");
    }

    var C = function (r) { return r.m.ciss; }, win = function (id) { return function (r) { return ((C(r).peaks || {})[id] || {}).hi || null; }; };
    html += "<div class=sec>Systemic stress by country · the ECB's CISS, daily" + (M.EA_CISS ? " · euro area " + fmt("index", M.EA_CISS.value) : "") + "</div>" + table("ciss", [
      nameCol(C),
      { id: "ciss", label: "CISS", value: function (r) { return num(C(r)); }, cell: function (r) { var m = C(r); return "<td class='c h" + heat(m) + "' data-id='" + esc(m.id) + "'><b>" + fmt("index", m.value) + "</b></td>"; } },
      { id: "date", label: "Reading of", cls: "l", first: 1, value: function (r) { return C(r).date; }, cell: function (r) { var m = C(r); return "<td class=l>" + fmtDate(m.date)
          + (m.blank_after ? " <span class=old>the ECB's " + fmtDate(m.blank_after).replace(/ \d{4}$/, "") + " row is blank</span>" : oldNote(m)) + "</td>"; } },
      { id: "w1", label: "1w", value: function (r) { return C(r).change["1w"]; }, cell: function (r) { return chgCell(C(r), "1w"); } },
      { id: "m1", label: "1m", value: function (r) { return C(r).change["1m"]; }, cell: function (r) { return chgCell(C(r), "1m"); } },
      { id: "m12", label: "12m", value: function (r) { return C(r).change["12m"]; }, cell: function (r) { return chgCell(C(r), "12m"); } },
      { id: "hist", label: "Own history", cls: "l", value: function (r) { return C(r).pctile; }, cell: function (r) { return "<td class=l>" + pctBar(C(r)) + "</td>"; } },
      { id: "gfc", label: "2008–09 peak", value: function (r) { var p = win("gfc2008")(r); return p ? p[0] : null; }, cell: function (r) { return peakCell(win("gfc2008")(r)); } },
      { id: "covid", label: "COVID peak", value: function (r) { var p = win("covid2020")(r); return p ? p[0] : null; }, cell: function (r) { return peakCell(win("covid2020")(r)); } },
      { id: "rec", label: "Record", value: function (r) { return C(r).high[0]; }, cell: function (r) { return peakCell(C(r).high); } },
      { id: "from", label: "History from", cls: "l", first: 1, value: function (r) { return C(r).start; }, cell: function (r) { return "<td class=l>" + C(r).start.slice(0, 4) + "</td>"; } },
      { id: "spark", label: "1 year", cls: "l", cell: function (r) { return "<td class=l>" + spark(C(r), 110, 22) + "</td>"; } }], b.systemic, ["ciss", -1]);

    var parts = D.contributions.map(function (id) { return M[id]; }), most = Math.max.apply(null, parts.map(function (m) { return Math.abs(m.value); })) || 1;
    html += "<div class=sec>What the euro-area CISS is made of" + (M.EA_CISS ? " · " + fmt("index", M.EA_CISS.value) + " on " + fmtDate(M.EA_CISS.date) : "") + "</div><div class=parts>" + parts.map(function (m) {
      return "<div class=part data-id='" + esc(m.id) + "' tabindex=0 role=button><span class=pl>" + esc(m.label.replace(" contribution to the euro-area CISS", "")) + "</span><span class=pbar><i class='" + (m.value < 0 ? "neg" : "") + "' style='width:"
        + (Math.abs(m.value) / most * 100).toFixed(0) + "%'></i></span><span class=pv>" + (m.value < 0 ? "−" : "+") + fixed(m.value, 4) + "</span></div>"; }).join("") + "</div>"
      + "<p class=note>Five market contributions plus the correlation term add up to the composite; the correlation term is negative when the five markets are not stressed together.</p>";
    $("sov-main").innerHTML = html;
  }

  /* ---- Crisis peaks ---- */
  function setCols() {
    var set = SETS.filter(function (s) { return s.id === setId; })[0], names = {};
    D.countries.forEach(function (c) { names[c.area] = c.country; });
    return { set: set, ids: orderIds(set.ids, D.measures, prefs.tags, names), names: names };
  }
  function colName(m, names) { return SHORT[m.id] || (m.area && names[m.area]) || m.label; }
  function colTag(m, names) { return m.area && names[m.area] && !SHORT[m.id] ? prefs.tags[ckey(names[m.area])] : null; }
  function renderCrisis() {
    var sc = setCols(), c = crisis(D, sc.ids), names = sc.names;
    var units = {}; c.cols.forEach(function (m) { units[m.unit] = 1; });
    var oneUnit = Object.keys(units).length === 1;
    var UNIT_WORD = { index: "index", bp: "bp", eur_bn: "€ bn", pct: "%", pct_gdp: "% GDP", usd: "$" };
    var html = "<div class=ctrl>" + SETS.map(function (s) { return "<button class='gbtn" + (s.id === setId ? " on" : "") + "' data-set='" + s.id + "'>" + esc(s.name) + "</button>"; }).join("") + "</div>";
    html += "<div class=sec>" + esc(sc.set.name) + " · every series over its whole history, named windows shaded</div>";
    if (oneUnit) {
      html += "<div class=ctrl id=sov-ovrg>" + RANGES.map(function (r) { return "<button class='gbtn" + (r[1] === over.years ? " on" : "") + "' data-oyears='" + r[1] + "'>" + r[0] + "</button>"; }).join("")
        + "<button class=gbtn data-olines=all>Show all</button><button class=gbtn data-olines=tagged>Only tagged</button><button class=gbtn data-olines=none>Hide all</button></div>"
        + "<div class=cvbox><canvas id=sov-ov class=ovc></canvas><div id=sov-otip class=tip></div></div><div class=legend id=sov-leg>" + c.cols.map(function (m, i) {
          var colr = i === 0 && !colTag(m, names) ? LEAD : lineColour(i, colTag(m, names)), off = over.hidden[m.id];
          return "<button class='lg" + (off ? " off" : "") + "' data-line='" + esc(m.id) + "' title='Click to hide or show; double-click for its own chart'><i style='background:" + colr + "'></i>" + esc(colName(m, names)) + (m.status === "ended" ? " <span class=old>ended</span>" : "") + "</button>"; }).join("") + "</div>";
    } else {
      html += "<p class=note>These series are in different units, so each has its own small chart over its whole history. Click one for the full chart.</p><div class=minis>" + c.cols.map(function (m) {
        return "<div class=mini data-id='" + esc(m.id) + "' tabindex=0 role=button><div class=mt>" + esc(colName(m, names)) + " <b>" + fmt(m.unit, m.value) + "</b></div><canvas class=mc data-mini='" + esc(m.id) + "'></canvas><div class=ms>" + m.start.slice(0, 4) + " – " + m.date.slice(0, 4) + "</div></div>"; }).join("") + "</div>";
    }
    html += "<div class=sec>Highest reading inside each window · click a cell for that series' chart around the window</div>"
      + "<p class=note>Each cell is the highest reading of that series inside the window's dates (for a central bank that owes on TARGET, the lowest). Shading is the cell against the series' own record. "
      + "Daily series are read from every daily observation, monthly ones from month-ends. Windows before a series began are left out.</p>";
    function head(m) { var t = colTag(m, names), colr = t ? lineColour(0, t) : null; return "<th data-id='" + esc(m.id) + "' class=c title='" + esc(m.label) + " — its history'>" + (colr ? "<i class=hd style='background:" + colr + "'></i>" : "") + esc(colName(m, names))
      + "<small>" + UNIT_WORD[m.unit] + (m.status === "ended" ? " · ended" : "") + "</small></th>"; }
    function td(x, m, ep) {
      if (!x) return "<td class=nil>—</td>";
      return "<td class=c data-id='" + esc(m.id) + "'" + (ep ? " data-ep='" + esc(ep.id) + "'" : "") + " style='background:rgba(224,122,106," + (x.share * 0.55).toFixed(2) + ")' title='" + esc(colName(m, names) + " · " + fmtDate(x.d)) + "'>"
        + fmt(m.unit, x.v).replace(" bp", "").replace(" bn", "") + "</td>";
    }
    html += "<div class=scroll><table class='board crisis'><thead><tr><th class=l>Window</th>" + c.cols.map(head).join("") + "</tr></thead><tbody>";
    c.rows.forEach(function (r) {
      html += "<tr data-ep='" + esc(r.ep.id) + "'><td class='l nm'>" + esc(r.ep.name) + "<small>" + fmtDate(r.ep.from) + " – " + fmtDate(r.ep.to) + "</small></td>" + r.cells.map(function (x, i) { return td(x, c.cols[i], r.ep); }).join("") + "</tr>";
    });
    html += "<tr class=nowrow><td class='l nm'>Now<small>each at its latest date</small></td>" + c.now.map(function (x, i) { return td(x, c.cols[i]); }).join("") + "</tr>";
    html += "<tr class=recrow><td class='l nm'>Record<small>whole history</small></td>" + c.record.map(function (x, i) { return td(x, c.cols[i]); }).join("") + "</tr></tbody></table></div>";
    var lead = c.cols[0];
    if (lead) {
      html += "<div class=sec>" + esc(lead.label) + " · its highest readings by the data itself, at least 120 days apart</div><div class='chips tops'>" + lead.tops.map(function (t) {
        var ep = windowOf(D.episodes, t[1]); return "<span class=chip data-id='" + esc(lead.id) + "'" + (ep ? " data-ep='" + esc(ep.id) + "'" : "") + " tabindex=0 role=button><b>" + fmt(lead.unit, t[0]) + "</b> " + fmtDate(t[1]) + (ep ? " · " + esc(ep.name) : "") + "</span>"; }).join("") + "</div>";
    }
    $("sov-main").innerHTML = html;
    needHistory(function () { if (view === "crisis") drawSet(); });
  }

  /* ---- drawing: one or many lines over time, named windows shaded ---- */
  function points(series, years, span) {
    var vis = visible(series, years, span), pts = [];
    for (var i = 0; i < vis.d.length; i++) if (isNum(vis.v[i])) pts.push([dayNum(vis.d[i]), vis.v[i], vis.d[i]]);
    return pts;
  }
  function plot(cv, lines, opt) {
    var box = cv.getBoundingClientRect(), ratio = root.devicePixelRatio || 1, wpx = Math.max(opt.mini ? 120 : 300, box.width), hpx = Math.max(opt.mini ? 60 : 220, box.height);
    cv.width = wpx * ratio; cv.height = hpx * ratio;
    var g = cv.getContext("2d"); g.setTransform(ratio, 0, 0, ratio, 0, 0); g.clearRect(0, 0, wpx, hpx);
    g.font = "11px -apple-system,Segoe UI,Roboto,sans-serif";
    var live = lines.filter(function (l) { return l.pts.length >= 2; });
    if (!live.length) { g.fillStyle = "#8b949e"; g.fillText(opt.empty || "Not enough history in this range.", opt.mini ? 6 : 16, opt.mini ? 16 : 28); return null; }
    var L = opt.mini ? 4 : 58, R = opt.mini ? 4 : 12, T = opt.mini ? 4 : 18, B = opt.mini ? 4 : 24, x0 = Infinity, x1 = -Infinity, lo = Infinity, hi = -Infinity, firstIso = null, lastIso = null;
    live.forEach(function (l) {
      if (l.pts[0][0] < x0) { x0 = l.pts[0][0]; firstIso = l.pts[0][2]; }
      if (l.pts[l.pts.length - 1][0] > x1) { x1 = l.pts[l.pts.length - 1][0]; lastIso = l.pts[l.pts.length - 1][2]; }
      l.pts.forEach(function (p) { if (p[1] < lo) lo = p[1]; if (p[1] > hi) hi = p[1]; });
    });
    if (lo > 0 && lo / (hi || 1) < 0.35 && opt.unit !== "eur_bn") lo = 0;
    var pad = (hi - lo || Math.abs(hi) || 1) * 0.06; hi += pad; if (lo !== 0) lo -= pad;
    var X = function (d) { return L + (d - x0) / (x1 - x0 || 1) * (wpx - L - R); }, Y = function (v) { return T + (hi - v) / (hi - lo || 1) * (hpx - T - B); };
    bands(opt.episodes, firstIso, lastIso).forEach(function (b) {
      var bx = L + b.x0 * (wpx - L - R), bw = Math.max(opt.mini ? 1 : 2, (b.x1 - b.x0) * (wpx - L - R)), strong = opt.ep && b.ep.id === opt.ep;
      g.fillStyle = strong ? "rgba(240,180,41,.34)" : "rgba(240,180,41,.13)"; g.fillRect(bx, T, bw, hpx - T - B);
    });
    if (!opt.mini) {
      g.strokeStyle = "#1e242c"; g.fillStyle = "#8b949e"; g.lineWidth = 1; g.textAlign = "right";
      for (var k = 0; k <= 4; k++) { var v = lo + (hi - lo) * k / 4, y = Y(v); g.beginPath(); g.moveTo(L, y); g.lineTo(wpx - R, y); g.stroke(); g.fillText(fmt(opt.unit, v).replace(" bp", "").replace(" bn", ""), L - 6, y + 4); }
      g.textAlign = "center";
      var y0 = +firstIso.slice(0, 4), y1 = +lastIso.slice(0, 4), step = Math.max(1, Math.ceil((y1 - y0 + 1) / Math.max(2, Math.floor((wpx - L - R) / 60))));
      for (var yr = y0 + 1; yr <= y1; yr += step) { var xx = X(dayNum(yr + "-01-01")); if (xx > L + 14 && xx < wpx - R - 14) g.fillText(String(yr), xx, hpx - 7); }
      if (y1 - y0 < 2) { g.fillText(fmtDate(firstIso), L + 34, hpx - 7); g.fillText(fmtDate(lastIso), wpx - R - 34, hpx - 7); }
    }
    if (lo < 0 && hi > 0) { g.strokeStyle = "#3a4452"; g.lineWidth = 1; g.beginPath(); g.moveTo(L, Y(0)); g.lineTo(wpx - R, Y(0)); g.stroke(); }
    live.forEach(function (l) {
      g.globalAlpha = l.alpha || 1; g.strokeStyle = l.colour; g.lineWidth = l.width || 1.4; g.beginPath();
      l.pts.forEach(function (p, i) { if (i) g.lineTo(X(p[0]), Y(p[1])); else g.moveTo(X(p[0]), Y(p[1])); });
      g.stroke();
      if (!l.ended) { var last = l.pts[l.pts.length - 1]; g.fillStyle = l.colour; g.beginPath(); g.arc(X(last[0]), Y(last[1]), opt.mini ? 2 : 3, 0, 6.3); g.fill(); }
    });
    g.globalAlpha = 1;
    return { lines: live, X: X, Y: Y, L: L, R: R, w: wpx, h: hpx, x0: x0, x1: x1 };
  }
  function nearest(pts, day) {
    var a = 0, b = pts.length - 1;
    while (b - a > 1) { var mid = (a + b) >> 1; if (pts[mid][0] <= day) a = mid; else b = mid; }
    return Math.abs(pts[b][0] - day) < Math.abs(pts[a][0] - day) ? pts[b] : pts[a];
  }
  function drawSet() {
    var sc = setCols(), names = sc.names, cols = sc.ids.map(function (id) { return D.measures[id]; });
    var cv = $("sov-ov");
    if (cv) {
      var anyTag = cols.some(function (m) { return colTag(m, names); });
      var lines = cols.map(function (m, i) {
        var t = colTag(m, names), lead = i === 0;
        return { id: m.id, name: colName(m, names), unit: m.unit, ended: m.status === "ended", colour: lead && !t ? LEAD : lineColour(i, t), width: t || lead ? 2.2 : 1.1, alpha: t || lead ? 1 : anyTag ? 0.35 : 0.8,
                 pts: over.hidden[m.id] ? [] : points(H.series[m.id], over.years) };
      });
      over.geo = plot(cv, lines.slice().reverse(), { unit: cols[0].unit, episodes: D.episodes, empty: Object.keys(H.series).length ? "Every series is hidden — pick one in the list below." : "The histories could not be read." });
      cv.setAttribute("data-lines", lines.filter(function (l) { return l.pts.length >= 2; }).length);
    }
    doc.querySelectorAll("canvas[data-mini]").forEach(function (mc) {
      var m = D.measures[mc.getAttribute("data-mini")];
      plot(mc, [{ colour: "#F0B429", width: 1.2, pts: points(H.series[m.id], 0), ended: m.status === "ended" }], { unit: m.unit, episodes: D.episodes, mini: true, empty: "no history" });
    });
  }
  function hoverSet(ev) {
    var geo = over.geo, tip = $("sov-otip"); if (!geo || !tip) return;
    var box = $("sov-ov").getBoundingClientRect(), px = (ev.touches ? ev.touches[0].clientX : ev.clientX) - box.left;
    var day = geo.x0 + (px - geo.L) / (geo.w - geo.L - geo.R) * (geo.x1 - geo.x0);
    if (px < geo.L || px > geo.w - geo.R) { tip.style.display = "none"; return; }
    var rows = geo.lines.map(function (l) { var p = nearest(l.pts, day); return Math.abs(p[0] - day) <= 45 ? [l, p] : null; }).filter(Boolean).sort(function (a, b) { return b[1][1] - a[1][1]; });
    if (!rows.length) { tip.style.display = "none"; return; }
    var iso = rows[0][1][2], ep = windowOf(D.episodes, iso);
    tip.style.display = "block"; tip.style.left = Math.min(geo.w - 200, Math.max(4, px + 12)) + "px"; tip.style.top = "8px";
    tip.innerHTML = "<b>" + fmtDate(iso) + "</b>" + (ep ? " · " + esc(ep.name) : "") + "<br>" + rows.slice(0, 8).map(function (x) {
      return "<i style='background:" + x[0].colour + "'></i>" + esc(x[0].name) + " <b>" + fmt(x[0].unit, x[1][1]) + "</b>"; }).join("<br>") + (rows.length > 8 ? "<br>… and " + (rows.length - 8) + " lower" : "");
  }

  function drawChart() {
    var m = D.measures[chart.id], cv = $("sov-cv");
    if (!H) { var g = cv.getContext("2d"); g.clearRect(0, 0, cv.width, cv.height); return; }
    chart.geo = plot(cv, [{ colour: "#F0B429", width: 1.4, pts: points(H.series[chart.id], chart.years, chart.span), ended: m.status === "ended" }],
      { unit: m.unit, episodes: D.episodes, ep: chart.ep, empty: H.series[chart.id] ? "Not enough history in this range." : "No history for this measure." });
  }
  function hoverChart(ev) {
    var geo = chart.geo, tip = $("sov-tip"); if (!geo) { tip.style.display = "none"; return; }
    var box = $("sov-cv").getBoundingClientRect(), px = (ev.touches ? ev.touches[0].clientX : ev.clientX) - box.left;
    var pts = geo.lines[0].pts, best = nearest(pts, geo.x0 + (px - geo.L) / (geo.w - geo.L - geo.R) * (geo.x1 - geo.x0));
    var m = D.measures[chart.id], ep = windowOf(D.episodes, best[2]);
    tip.style.display = "block"; tip.style.left = Math.min(geo.w - 170, Math.max(4, geo.X(best[0]) + 10)) + "px"; tip.style.top = Math.max(4, geo.Y(best[1]) - 34) + "px";
    tip.innerHTML = "<b>" + fmt(m.unit, best[1]) + "</b> " + fmtDate(best[2]) + (ep ? "<br>" + esc(ep.name) : "");
  }
  function rangeButtons() {
    var ep = chart.ep && D.episodes.filter(function (e) { return e.id === chart.ep; })[0];
    $("sov-rg").innerHTML = RANGES.map(function (r) { return "<button class='gbtn" + (!chart.span && r[1] === chart.years ? " on" : "") + "' data-years='" + r[1] + "'>" + r[0] + "</button>"; }).join("")
      + (ep ? "<button class='gbtn" + (chart.span ? " on" : "") + "' data-window=1>" + esc(ep.name) + " ± 1 year</button>" : "");
  }
  function link(key, text) { var p = dataPath(key); return p ? "<a href='" + esc(p) + "' target=_blank rel=noopener>" + esc(text) + "</a>" : ""; }
  function openChart(id, epId) {
    var m = D.measures[id]; if (!m) return;
    var ep = epId && D.episodes.filter(function (e) { return e.id === epId; })[0];
    chart.id = id; chart.years = 0; chart.ep = ep ? ep.id : null; chart.span = ep ? around(ep) : null;
    $("sov-ct").textContent = m.label;
    var links = [];
    if (m.key && ecbPage(m)) links.push("<a href='" + esc(ecbPage(m)) + "' target=_blank rel=noopener>the series at the ECB</a>");
    if (m.evidence) links.push(link(m.evidence.key, "the response it was read from (.gz)"));
    if (m.key && /^(CISS|CLIFS)\./.test(m.key)) links.push("<a href='/ciss.html?series=" + encodeURIComponent(m.key) + "' target=_blank rel=noopener>in the CISS register</a>");
    $("sov-cm").innerHTML = "<b>" + fmt(m.unit, m.value) + "</b> · " + when(m) + (m.status === "ended" ? " · <span class=old>the ECB stopped this series; this is its last reading</span>" : oldNote(m))
      + (m.blank_after ? " · the ECB's " + esc(m.blank_after) + " row is blank" : "") + " · " + esc(standing(m)) + "<br>"
      + (m.key ? "<code>" + esc(m.key) + "</code>" : esc(m.derived || "")) + (m.exact && m.key ? " · source value " + esc(m.exact) : "") + "<br>" + links.join(" · ");
    var peaks = (D.episodes || []).map(function (e) { var p = (m.peaks || {})[e.id]; return p ? [e, p[side(m)]] : null; }).filter(Boolean);
    $("sov-cs").innerHTML = "<div class=stat>History from " + fmtDate(m.start) + " · " + m.n.toLocaleString("en-US") + " observations · high " + fmt(m.unit, m.high[0]) + " on " + fmtDate(m.high[1]) + " · low " + fmt(m.unit, m.low[0]) + " on "
      + fmtDate(m.low[1]) + " · mean " + fmt(m.unit, m.mean) + (m.freq === "D" ? " · the line is the last reading of each week; highs and lows are from every day" : "") + "</div>"
      + (peaks.length ? "<div class=sec>" + (side(m) === "lo" ? "Lowest" : "Highest") + " reading inside each named window (shaded on the chart) · click one to look around it</div><div class=chips>" + peaks.map(function (p) {
        return "<span class='chip" + (p[0].id === chart.ep ? " cur" : "") + "' data-chart-ep='" + esc(p[0].id) + "' tabindex=0 role=button>" + esc(p[0].name) + " <b>" + fmt(m.unit, p[1][0]) + "</b> " + fmtDate(p[1][1]) + "</span>"; }).join("") + "</div>" : "");
    rangeButtons();
    $("sov-ovl").classList.add("on");
    needHistory(function () { if (chart.id) drawChart(); });
  }
  function closeChart() { $("sov-ovl").classList.remove("on"); chart.id = null; $("sov-tip").style.display = "none"; }

  /* ---- World, Sources ---- */
  function renderWorld() {
    if (!W) { $("sov-main").innerHTML = "<p class=note>Loading the world table…</p>"; return; }
    if (W.error) { $("sov-main").innerHTML = "<p class=note>The world table is not available (" + esc(W.error) + ").</p>"; return; }
    var rows = world(W), txt = function (v) { return "<td class=l>" + esc(v || "") + "</td>"; };
    $("sov-main").innerHTML = "<p class=note>Outside the ECB's reach: 10-year yields, default-swap spreads, ratings and policy rates as reported by World Government Bonds, " + esc(String(W.generated_at || "").slice(0, 10))
      + ". Reported figures only; a missing figure is left blank. <a href='/global-sovereign.html' target=_blank rel=noopener>Open the full page</a>.</p>" + tagBar() + table("world", [
      { id: "country", label: "Country", cls: "l", first: 1, value: function (r) { return r.country; }, cell: function (r) { return "<td class='l nm'>" + tagButton(r) + esc(r.country) + "</td>"; } },
      { id: "region", label: "Region", cls: "l", first: 1, value: function (r) { return r.region; }, cell: function (r) { return txt(r.region); } },
      { id: "y", label: "10y yield", value: function (r) { return isNum(r.y) ? r.y : null; }, cell: function (r) { return "<td>" + (isNum(r.y) ? fixed(r.y, 2) + "%" : "") + "</td>"; } },
      { id: "spread", label: "vs Bund", value: function (r) { return isNum(r.spread) ? r.spread : null; }, cell: function (r) { return "<td>" + (isNum(r.spread) ? fmtChange("bp", r.spread) : "") + "</td>"; } },
      { id: "cds", label: "CDS", value: function (r) { return isNum(r.cds) ? r.cds : null; }, cell: function (r) { return "<td><b>" + (isNum(r.cds) ? fixed(r.cds, 1) + " bp" : "") + "</b></td>"; } },
      { id: "rating", label: "Rating", cls: "l", first: 1, value: function (r) { return r.rating; }, cell: function (r) { return txt(r.rating); } },
      { id: "policy", label: "Policy rate", value: function (r) { return isNum(r.policy) ? r.policy : null; }, cell: function (r) { return "<td>" + (isNum(r.policy) ? fixed(r.policy, 2) + "%" : "") + "</td>"; } },
      { id: "asof", label: "As of", cls: "l", cell: function (r) { return txt(r.asOf); } }], rows, ["cds", -1]);
  }
  function renderSources() {
    var html = "<p class=note>Every ECB number on this desk is read from an ECB response that is kept as it arrived. The run receipt lists them; compiling twice from two separate reads has to give the same document before it is published.</p>";
    html += "<div class=scroll><table class=board><thead><tr><th class=l>ECB response</th><th>Series</th><th>Rows</th><th class=l>Newest period</th><th class=l>Received</th><th class=l>Original</th></tr></thead><tbody>";
    D.sources.forEach(function (s) {
      html += "<tr><td class='l nm'>" + esc(s.label) + (s.path ? "<small>" + esc(s.path) + "</small>" : "") + "</td><td>" + (s.series || "") + "</td><td>" + (s.rows ? s.rows.toLocaleString("en-US") : "") + "</td><td class=l>" + esc(s.newest || "")
        + "</td><td class=l>" + esc(String(s.acquired_at || (s.warehouse || {}).generated_at || "").slice(0, 16).replace("T", " ")) + (s.carried ? " <span class=old>kept from an earlier run</span>" : "") + "</td><td class=l>"
        + (s.evidence ? link(s.evidence.key, "response (.gz)") : s.warehouse ? link(s.warehouse.manifest_key, "warehouse receipt") + " · <a href='/ciss.html' target=_blank rel=noopener>CISS register</a>" : "") + "</td></tr>";
    });
    html += "</tbody></table></div><p class=note>" + link(D.replay.manifest_key, "Run receipt") + " · " + link(D.replay.compiler.key, "the compiler that produced it") + " · " + link("data/ciss-sovereign.json", "this document")
      + " · " + link(D.history.key, "histories") + " · CDS and ratings: " + link("data/global-sovereign.json", "the world document") + " (World Government Bonds, as reported)</p>";
    var errs = Object.keys(D.errors || {});
    html += "<div class=sec>Not available in this run</div><p class=note>" + (errs.length ? errs.map(function (k) { return esc(k + ": " + D.errors[k]); }).join("<br>") : "Nothing: every response arrived and compiled.") + "</p>";
    if ((D.ended_at_source || []).length) html += "<div class=sec>Ended at the source, not shown</div><p class=note>" + D.ended_at_source.map(function (e) { return esc((e.key || e.id) + " — last " + e.last); }).join("<br>") + "</p>";
    html += "<div class=sec>How to read a number</div><p class=note>" + Object.keys(D.definitions).map(function (k) { return "<b>" + esc(k) + "</b>: " + esc(D.definitions[k]); }).join("<br>")
      + "<br><b>tags and sorting</b>: kept in this browser only; nothing about them is sent anywhere.</p>";
    $("sov-main").innerHTML = html;
  }

  function render() {
    doc.querySelectorAll("#sov-tabs .tab").forEach(function (t) { t.classList.toggle("on", t.getAttribute("data-view") === view); });
    closePalette();
    if (!D) return;
    if (view === "desk") renderDesk(); else if (view === "crisis") renderCrisis(); else if (view === "world") renderWorld(); else renderSources();
  }

  /* ---- the colour palette for a tag ---- */
  function closePalette() { var p = $("sov-pal"); if (p) p.style.display = "none"; }
  function openPalette(btn) {
    var p = $("sov-pal"), key = btn.getAttribute("data-tag"), box = btn.getBoundingClientRect();
    p.innerHTML = TAGS.map(function (t) { return "<button class='sw1" + (prefs.tags[key] === t[0] ? " on" : "") + "' data-colour='" + t[0] + "' title='" + t[0] + "' aria-label='" + t[0] + "' style='background:" + t[1] + "'></button>"; }).join("")
      + "<button class=swx data-colour='' title='No tag' aria-label='No tag'>✕</button>";
    p.setAttribute("data-for", key);
    p.style.display = "flex";
    p.style.left = Math.max(6, Math.min(doc.documentElement.clientWidth - 216, box.left + root.scrollX)) + "px"; p.style.top = (box.bottom + root.scrollY + 6) + "px";
  }

  doc.addEventListener("click", function (ev) {
    var t = ev.target, hit;
    if ((hit = t.closest && t.closest("#sov-pal [data-colour]"))) {
      var key = $("sov-pal").getAttribute("data-for"), colour = hit.getAttribute("data-colour");
      if (colour) prefs.tags[key] = colour; else delete prefs.tags[key];
      if (prefs.show !== "all" && !Object.keys(prefs.tags).length) prefs.show = "all";
      save(); render(); return;
    }
    if ((hit = t.closest && t.closest("[data-tag]"))) { var p = $("sov-pal"); if (p.style.display === "flex" && p.getAttribute("data-for") === hit.getAttribute("data-tag")) closePalette(); else openPalette(hit); return; }
    closePalette();
    if ((hit = t.closest && t.closest("#sov-tabs .tab"))) { view = hit.getAttribute("data-view"); render(); return; }
    if ((hit = t.closest && t.closest("[data-show]"))) { prefs.show = hit.getAttribute("data-show"); save(); render(); return; }
    if (t.closest && t.closest("[data-cleartags]")) { prefs.tags = {}; prefs.show = "all"; save(); render(); return; }
    if ((hit = t.closest && t.closest("th[data-sort]"))) {
      var tb = hit.closest("table"), sorted = tb.getAttribute("data-sorted").split(":");
      prefs.sort[tb.getAttribute("data-table")] = nextSort([sorted[0], +sorted[1]], hit.getAttribute("data-sort"), +hit.getAttribute("data-first"));
      save(); render(); return;
    }
    if ((hit = t.closest && t.closest("[data-set]"))) { setId = hit.getAttribute("data-set"); over.hidden = {}; render(); return; }
    if ((hit = t.closest && t.closest("[data-oyears]"))) { over.years = +hit.getAttribute("data-oyears"); hit.parentNode.querySelectorAll("[data-oyears]").forEach(function (b) { b.classList.toggle("on", +b.getAttribute("data-oyears") === over.years); }); drawSet(); return; }
    if ((hit = t.closest && t.closest("[data-olines]"))) {
      var mode = hit.getAttribute("data-olines"), sc = setCols(); over.hidden = {};
      sc.ids.forEach(function (id, i) { var m = D.measures[id]; if (mode === "none" || (mode === "tagged" && i > 0 && !colTag(m, sc.names))) over.hidden[id] = 1; });
      doc.querySelectorAll("#sov-leg .lg").forEach(function (b) { b.classList.toggle("off", !!over.hidden[b.getAttribute("data-line")]); }); drawSet(); return;
    }
    if ((hit = t.closest && t.closest("#sov-leg [data-line]"))) {
      var lid = hit.getAttribute("data-line"); if (over.hidden[lid]) delete over.hidden[lid]; else over.hidden[lid] = 1;
      hit.classList.toggle("off", !!over.hidden[lid]); drawSet(); return;
    }
    if ((hit = t.closest && t.closest("[data-years]"))) { chart.years = +hit.getAttribute("data-years"); chart.span = null; rangeButtons(); drawChart(); return; }
    if (t.closest && t.closest("[data-window]")) { var e0 = D.episodes.filter(function (e) { return e.id === chart.ep; })[0]; chart.span = around(e0); rangeButtons(); drawChart(); return; }
    if ((hit = t.closest && t.closest("[data-chart-ep]"))) { openChart(chart.id, hit.getAttribute("data-chart-ep")); return; }
    if (t.id === "sov-x" || t.id === "sov-ovl") { closeChart(); return; }
    if ((hit = t.closest && t.closest("#sov-main [data-id]"))) openChart(hit.getAttribute("data-id"), hit.getAttribute("data-ep"));
  });
  doc.addEventListener("dblclick", function (ev) { var hit = ev.target.closest && ev.target.closest("#sov-leg [data-line]"); if (hit) openChart(hit.getAttribute("data-line")); });
  doc.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape") { closePalette(); closeChart(); }
    if ((ev.key === "Enter" || ev.key === " ") && ev.target.getAttribute && ev.target.closest("#sov-main, #sov-cs") && (ev.target.getAttribute("data-id") || ev.target.getAttribute("data-chart-ep"))) { ev.preventDefault(); ev.target.click(); }
  });
  $("sov-cv").addEventListener("mousemove", hoverChart);
  $("sov-cv").addEventListener("touchstart", hoverChart, { passive: true });
  $("sov-cv").addEventListener("mouseleave", function () { $("sov-tip").style.display = "none"; });
  doc.addEventListener("mousemove", function (ev) { if (ev.target.id === "sov-ov") hoverSet(ev); else if ($("sov-otip")) $("sov-otip").style.display = "none"; });
  doc.addEventListener("touchstart", function (ev) { if (ev.target.id === "sov-ov") hoverSet(ev); }, { passive: true });
  root.addEventListener("resize", function () { if (chart.id) drawChart(); if (view === "crisis" && H) drawSet(); });

  getJson("/data/ciss-sovereign.json").then(function (d) {
    if (d.contract !== "ciss-sovereign.v1" || !d.measures) throw new Error("unexpected document");
    D = d;
    var ea = d.measures.EA_SOV_GDP, n = Object.keys(d.measures).length;
    $("sov-asof").innerHTML = "ECB data" + (ea ? " to " + fmtDate(ea.date) : "") + " · compiled " + esc(d.generated_at.slice(0, 16).replace("T", " ")) + " UTC · " + n + " measures, " + d.countries.length + " countries"
      + (Object.keys(d.errors || {}).length ? " · <span class=old>" + Object.keys(d.errors).length + " source(s) not available in this run — see Sources</span>" : "");
    render();
    var want = new URLSearchParams(root.location.search).get("m");
    if (want && d.measures[want]) openChart(want);
    getJson("/data/global-sovereign.json").then(function (w) { W = w; WJ = joinWorld(w); render(); }).catch(function (e) { W = { error: e.message }; if (view === "world") render(); });
  }).catch(function (e) {
    $("sov-asof").textContent = "The sovereign-stress document could not be read: " + e.message;
    $("sov-main").innerHTML = "<p class=note>Nothing is shown rather than something old. The CISS register still opens: <a href='/ciss.html' target=_blank rel=noopener>ciss.html</a>.</p>";
  });
})(typeof window !== "undefined" ? window : globalThis);
