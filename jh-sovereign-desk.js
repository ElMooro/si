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
    usd: { d: 4, pre: "$", suf: "", chg: "" },
    usd_bn: { d: 1, small: 3, pre: "$", suf: " bn", chg: " bn" },      /* a facility that is hardly used is read to the million */
    idx: { d: 2, pre: "", suf: "", chg: "" }
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

  function places(u, v, wide) { return u.small && !wide && v !== 0 && Math.abs(v) < 10 ? u.small : u.d; }
  /* wide: the number stands on an axis that spans tens or more, so it is not read to the million */
  function fmt(unit, v, wide) {
    if (!isNum(v)) return "—";
    var u = UNITS[unit] || UNITS.index, d = places(u, v, wide);
    return (v < 0 && +v.toFixed(d) !== 0 ? "−" : "") + u.pre + fixed(v, d) + u.suf;
  }
  /* wide: the level this is a change of is tens or more, so the change is not read to the million either */
  function fmtChange(unit, v, wide) {
    if (!isNum(v)) return "—";
    var u = UNITS[unit] || UNITS.index, d = places(u, v, wide), flat = +v.toFixed(d) === 0;
    return (flat ? "" : v > 0 ? "+" : "−") + fixed(v, d) + u.chg;
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

  var GAP = 45;           /* days of silence after which a traded or reported daily series is not joined by a line */
  /* the longest silence a line is drawn across: none for the ECB's and the OECD's regular series, GAP days for the World tab's daily ones */
  function gapOf(m) { return m && m.source && m.source !== "fred" && m.freq === "D" ? GAP : 0; }      /* the Federal Reserve's daily series have no such holes */
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

  /* ---- the World tab's histories (data/ciss-sovereign-world.json) ---- */
  /* the series of one set: tagged countries first, in the order of the tag colours, then by latest reading, highest first */
  function worldCols(wh, setName, tags) {
    var out = [];
    ((wh && wh.countries) || []).forEach(function (c) {
      var id = c.ids && c.ids[setName], m = id && wh.measures && wh.measures[id];
      if (m) out.push({ id: id, m: m, country: c.country, key: ckey(c.country), rank: tagRank(tags, ckey(c.country)) });
    });
    return out.sort(function (a, b) {
      var ta = a.rank < 0 ? TAGS.length : a.rank, tb = b.rank < 0 ? TAGS.length : b.rank;
      return ta - tb || b.m.value - a.m.value || (a.country < b.country ? -1 : a.country > b.country ? 1 : 0);
    });
  }
  /* {country key: its series ids} */
  function worldIds(wh) {
    var out = {};
    ((wh && wh.countries) || []).forEach(function (c) { out[ckey(c.country)] = c.ids || {}; });
    return out;
  }
  /* which of a country's series a cell of the world table opens: the long history when there is one, else the table's own
     daily record; on an "as reported" set the other way round.  kind: "yield", "cds", or "name" (the set on screen first) */
  function worldPick(ids, kind, setName) {
    if (!ids) return null;
    if (kind === "name") return ids[setName] || ids.yield || ids.cds || ids.yield_reported || ids.cds_reported || null;
    var reported = /_reported$/.test(setName || ""), long = ids[kind] || null, own = ids[kind + "_reported"] || null;
    return reported ? own || long : long || own;
  }
  /* is a line of a chart of many drawn?  mode: all, tagged (tagged countries, and a set's leading aggregate), none; a click
     on one line's name flips that line against the mode */
  function lineShown(mode, tagged, lead, flipped) {
    var base = mode === "none" ? false : mode === "tagged" ? !!(tagged || lead) : true;
    return flipped ? !base : base;
  }

  /* ---- Dollar shortage: the three ECB measures first, then the Federal Reserve's lines in their parts ---- */
  var DOLLAR = ["AUX_EURUSD", "AUX_FX_EA", "AUX_FX_NONEA"];       /* the ECB's three, in the order they were asked for; the Federal Reserve's come with the document */
  /* the three keep the ECB's own titles everywhere on the tab; this says in a few words what each one is */
  var DOLLAR_GLOSS = { AUX_EURUSD: "US dollars for one euro", AUX_FX_EA: "what the Eurosystem lends euro-area banks in foreign currency", AUX_FX_NONEA: "the Eurosystem's foreign reserves other than gold" };
  var DOLLAR_PART = { id: "ecb", name: "The three ECB measures", note: "The euro's reference rate against the dollar and the two foreign-currency items of the Eurosystem's weekly statement, each read from the ECB response kept as it arrived." };
  /* the ECB's three while the document is the one from before it named its parts, or has not arrived */
  var DOLLAR_ECB = { AUX_EURUSD: { short: "EUR/USD", worse: "lower" }, AUX_FX_EA: { short: "Eurosystem foreign-currency lending to euro-area banks", worse: "higher" },
                     AUX_FX_NONEA: { short: "Eurosystem foreign reserves other than gold", worse: null } };
  /* column headings of the windows table */
  var DOLLAR_COL = { FED_SWAPS: "Fed swaps", FED_FIMA: "Fed repos", AUX_FX_EA: "Claims, euro area", USD_DXY: "DXY", USD_BROAD: "Broad", USD_AFE: "Advanced", USD_EME: "Emerging", AUX_EURUSD: "EUR/USD",
                     FED_CUSTODY_13W: "Custody, 13 wk", FED_CUSTODY_UST: "Treasuries", FED_FOREIGN_RRP: "Repo pool", USD_SOFR_IOR: "SOFR − IOR", USD_TED: "TED", H8_NET_DUE: "Net due", AUX_FX_NONEA: "Claims, non-euro area" };
  /* every measure of the tab with its history: the ECB's three first, as one part, then the Federal Reserve's lines in
     the document's parts.  From the dollar-shortage document (every observation) or, while that has not arrived, the
     ECB's three from the desk's own two documents (the reference rate is then weekly) */
  function dollarData(ds, deskDoc, hist) {
    var full = !!(ds && ds.contract === "ciss-dollar-shortage.v1" && ds.measures && ds.series);
    var src = full ? ds.measures : (deskDoc && deskDoc.measures) || {}, S = full ? ds.series : (hist && hist.series) || {};
    var named = full && Array.isArray(ds.parts) && ds.parts.length > 0, M = {};
    var rest = named ? ds.parts.map(function (p) {
      var moved = (p.ids || []).filter(function (id) { return DOLLAR.indexOf(id) >= 0 && src[id]; }).map(function (id) { return src[id].label; });
      return { id: p.id, name: p.name, note: (p.note || "") + (moved.length ? " " + moved.join(" and ") + (moved.length > 1 ? " are" : " is") + " among the three ECB measures above." : ""),
               ids: (p.ids || []).filter(function (id) { return DOLLAR.indexOf(id) < 0; }) }; }) : [];
    var parts = [{ id: DOLLAR_PART.id, name: DOLLAR_PART.name, note: DOLLAR_PART.note, ids: DOLLAR }].concat(rest).map(function (p) {
      return { id: p.id, name: p.name, note: p.note || "", ids: (p.ids || []).filter(function (id) { return src[id]; }) }; }).filter(function (p) { return p.ids.length; });
    var ids = [].concat.apply([], parts.map(function (p) { return p.ids; }));
    ids.forEach(function (id) {
      var m = src[id], e = DOLLAR_ECB[id], c = {};
      if (named || !e) { M[id] = m; return; }
      Object.keys(m).forEach(function (k) { c[k] = m[k]; }); c.short = e.short; c.worse = e.worse; M[id] = c;
    });
    return { ids: ids, parts: parts, measures: M, series: S, full: full, named: named, episodes: (full ? ds.episodes : deskDoc && deskDoc.episodes) || [], errors: (full && ds.errors) || {} };
  }
  /* the end of a window that a dollar shortage shows in: "hi", "lo", or null where the page takes no side */
  function dollarSide(m) { return !m ? null : m.worse === "higher" ? "hi" : m.worse === "lower" ? "lo" : null; }
  /* the ECB's three are called what the ECB calls them; a Federal Reserve line by its short name */
  function dollarName(m) { return !m ? "" : DOLLAR.indexOf(m.id) >= 0 ? m.label || m.short || "" : m.short || m.label || ""; }
  function dollarGloss(m) { return (m && DOLLAR_GLOSS[m.id]) || ""; }
  function dunit(m) { return m.unit === "usd" ? "index" : m.unit; }          /* the euro's rate reads as a rate, not as an amount of dollars */
  /* 0..100: how far the reading stands toward a shortage; null where the page takes no side or the series has ended.
     A lending line that is hardly drawn is higher than most of its own idle weeks, so it is placed by its share of its
     record instead; every other measure by the share of its own readings it stands beyond. */
  function dollarPlace(m) {
    var s = dollarSide(m);
    if (!s || !isNum(m.pctile) || m.status === "ended") return null;
    if (m.part === "backstop") return m.high && m.high[0] > 0 ? Math.max(0, Math.min(100, m.value / m.high[0] * 100)) : null;
    return s === "hi" ? m.pctile : 100 - m.pctile;
  }
  function dollarPlaceText(m) {
    var p = dollarPlace(m);
    if (p === null) return m.status === "ended" ? "ended at its source" : standing(m).replace(/ since \d{4}$/, "");
    if (m.part === "backstop") return (p < 0.1 ? "under 0.1" : p < 10 ? p.toFixed(1) : String(Math.round(p))) + "% of its record";
    return (dollarSide(m) === "hi" ? "higher than " : "lower than ") + Math.floor(p) + "% of its readings";
  }
  function dollarHeat(m) {
    var p = dollarPlace(m);
    if (p === null) return 0;
    return m.part === "backstop" ? (p >= 50 ? 4 : p >= 25 ? 3 : p >= 10 ? 2 : 0) : p >= 95 ? 4 : p >= 85 ? 3 : p >= 70 ? 2 : p >= 50 ? 1 : 0;
  }
  function dwhen(m) { return (m.freq === "W" ? "in the week to " : "on ") + fmtDate(m.date); }
  function dchange(m, k) { return fmtChange(dunit(m), m.change[k], Math.abs(m.value) >= 10); }
  function dmoves(m, keys) { return keys.map(function (k) { return dchange(m, k) + " over a " + { "1w": "week", "1m": "month", "12m": "year" }[k]; }).join(", "); }
  function dpeaks(m, floor, noun) {
    var big = (m.tops || []).filter(function (x) { return x[0] >= floor; });
    return big.length ? " " + noun + " stood above " + fmt(m.unit, floor, true).replace(".0 bn", " bn") + " " + (big.length === 1 ? "once" : "at " + big.length + " separate peaks") + ": "
      + big.map(function (x) { return fmt(m.unit, x[0]) + " on " + fmtDate(x[1]); }).join(", ") + "." : "";
  }
  /* the opening lines, in words, from the measures' own numbers: one paragraph for each part that has a measure */
  function dollarRead(dd) {
    var M = dd.measures, out = [], line;
    var swaps = M.FED_SWAPS, fima = M.FED_FIMA, lend = M.AUX_FX_EA, dxy = M.USD_DXY, broad = M.USD_BROAD, eme = M.USD_EME, eur = M.AUX_EURUSD;
    var ust = M.FED_CUSTODY_UST, c13 = M.FED_CUSTODY_13W, pool = M.FED_FOREIGN_RRP, sofr = M.USD_SOFR_IOR, ted = M.USD_TED, due = M.H8_NET_DUE, res = M.AUX_FX_NONEA;
    if (eur) out.push("The EUR/USD reference rate is " + fmt("index", eur.value) + " on " + fmtDate(eur.date) + ": " + dmoves(eur, ["1w", "1m", "12m"]) + "; " + standing(eur)
      + " (highest " + fmt("index", eur.high[0]) + " on " + fmtDate(eur.high[1]) + ", lowest " + fmt("index", eur.low[0]) + " on " + fmtDate(eur.low[1]) + ").");
    if (lend) out.push("Claims on euro-area residents in foreign currency — what the Eurosystem lends its banks in foreign currency — are " + fmt(lend.unit, lend.value) + " " + dwhen(lend) + " — " + standing(lend) + "; "
      + dmoves(lend, ["1w", "12m"]) + "." + dpeaks(lend, 100, "They have"));
    if (res) out.push("Claims on non-euro-area residents in foreign currency are " + fmt(res.unit, res.value) + " " + dwhen(res) + " — " + standing(res) + "; " + dmoves(res, ["1w", "12m"]) + "."
      + " This is asset item 2 of the Eurosystem's weekly statement: its position at the IMF and its deposits, securities and loans abroad, that is its foreign reserves other than gold. It is not a gauge of dollars lent.");
    line = [];
    if (swaps) line.push("The Federal Reserve's swap lines are drawn for " + fmt(swaps.unit, swaps.value) + " " + dwhen(swaps) + " (" + dmoves(swaps, ["1w", "12m"]) + ")." + dpeaks(swaps, 100, "They have"));
    if (fima) line.push("Its repos with foreign central banks stand at " + fmt(fima.unit, fima.value) + "; their record is " + fmt(fima.unit, fima.high[0]) + " on " + fmtDate(fima.high[1]) + ".");
    if (line.length) out.push("Dollars lent by the Federal Reserve. " + line.join(" "));
    line = [];
    if (dxy) line.push("DXY, rebuilt from the Federal Reserve's H.10 rates, is " + fmt("idx", dxy.value) + " on " + fmtDate(dxy.date) + ": " + dmoves(dxy, ["1w", "1m", "12m"]) + "; " + standing(dxy) + ".");
    if (broad) line.push("The Fed's broad dollar index is " + fmt("idx", broad.value) + " (" + standing(broad) + ")" + (eme ? ", and its index against emerging markets " + fmt("idx", eme.value) + " (" + standing(eme) + ")" : "") + ".");
    if (line.length) out.push("The price of the dollar. " + line.join(" "));
    line = [];
    if (ust) line.push("Foreign central banks keep " + fmt(ust.unit, ust.value) + " of Treasuries in custody at the Federal Reserve " + dwhen(ust) + " (" + dmoves(ust, ["1w", "12m"]) + ").");
    if (c13) line.push("Over 13 weeks that is " + fmtChange(c13.unit, c13.value, true) + "; the largest 13-week fall on record is " + fmt(c13.unit, c13.low[0]) + " in the week to " + fmtDate(c13.low[1]) + ".");
    if (pool) line.push("Their repo pool at the Fed is " + fmt(pool.unit, pool.value) + " (" + dmoves(pool, ["1w", "12m"]) + ").");
    if (line.length) out.push("Foreign official dollars. " + line.join(" "));
    line = [];
    if (sofr) line.push("SOFR is " + fmt("bp", Math.abs(sofr.value)) + (sofr.value < 0 ? " below" : " above") + " the rate the Federal Reserve pays on reserves on " + fmtDate(sofr.date) + "; the record is " + fmt("bp", sofr.high[0]) + " on " + fmtDate(sofr.high[1]) + ".");
    if (due) line.push("Foreign banks' US offices " + (due.value < 0 ? "are owed a net " + fmt(due.unit, -due.value) + " by" : "owe a net " + fmt(due.unit, due.value) + " to") + " their offices abroad " + dwhen(due)
      + "; the lowest reading is " + fmt(due.unit, due.low[0]) + " on " + fmtDate(due.low[1]) + ".");
    if (ted) line.push("The TED spread " + (ted.status === "ended" ? "ended on " + fmtDate(ted.date) + "; its record is " : "is " + fmt("bp", ted.value) + "; its record is ") + fmt("bp", ted.high[0]) + " on " + fmtDate(ted.high[1]) + ".");
    if (line.length) out.push("Banks' dollar funding. " + line.join(" "));
    if (dd.named) out.push("Not here: a cross-currency basis, the market's own price of a dollar shortage. No free official source publishes one.");
    return out;
  }
  /* one row per named window.  The ECB's three give the lowest and the highest reading inside it, the end a shortage
     shows in marked; a Federal Reserve line gives the one end a shortage shows in, and one the page takes no side on
     is in the charts, not here. */
  function dollarWindows(dd) {
    var cols = [], groups = [], left = [], now = [];
    dd.parts.forEach(function (p) {
      var n = 0;
      p.ids.forEach(function (id) {
        var m = dd.measures[id], s = dollarSide(m), ends = DOLLAR.indexOf(id) >= 0 ? ["lo", "hi"] : s ? [s] : [];
        if (!ends.length) { left.push(m); return; }
        ends.forEach(function (end) { cols.push({ m: m, end: end, marked: end === s }); n++; });
        now.push({ m: m, span: ends.length });
      });
      if (n) groups.push({ id: p.id, name: p.name, span: n });
    });
    var rows = dd.episodes.map(function (ep) {
      return { ep: ep, cells: cols.map(function (c) { var p = (c.m.peaks || {})[ep.id]; return p ? p[c.end] : null; }) };
    }).filter(function (r) { return r.cells.some(Boolean); });
    return { cols: cols, groups: groups, rows: rows, left: left, now: now, record: cols.map(function (c) { return c.end === "hi" ? c.m.high : c.m.low; }) };
  }
  /* 0..1: how far a reading lies toward the measure's own record at the end a shortage shows in - counted from zero for a
     measure that has stood on both sides of it (a change, a spread), otherwise from its opposite record */
  function dollarShare(m, end, v) {
    var lo = m.low && m.low[0], hi = m.high && m.high[0];
    if (!isNum(v) || !isNum(lo) || !isNum(hi) || hi <= lo) return 0;
    if (lo < 0 && hi > 0) return Math.max(0, Math.min(1, end === "hi" ? v / hi : v / lo));
    return Math.max(0, Math.min(1, end === "hi" ? (v - lo) / (hi - lo) : (hi - v) / (hi - lo)));
  }

  var api = { fmt: fmt, fmtChange: fmtChange, fmtDate: fmtDate, when: when, age: age, worseUp: worseUp, heat: heat, standing: standing, record: record, sparkPath: sparkPath,
              board: board, commonWhen: commonWhen, read: read, crisis: crisis, side: side, windowOf: windowOf, visible: visible, around: around, bands: bands, ecbPage: ecbPage,
              dataPath: dataPath, world: world, ckey: ckey, joinWorld: joinWorld, tagRank: tagRank, arrange: arrange, nextSort: nextSort, orderIds: orderIds, lineColour: lineColour,
              worldCols: worldCols, worldIds: worldIds, worldPick: worldPick, lineShown: lineShown, gapOf: gapOf, GAP: GAP,
              dollarData: dollarData, dollarSide: dollarSide, dollarName: dollarName, dollarGloss: dollarGloss, dollarPlace: dollarPlace, dollarPlaceText: dollarPlaceText, dollarHeat: dollarHeat, dollarRead: dollarRead, dollarWindows: dollarWindows, dollarShare: dollarShare, DOLLAR: DOLLAR, DOLLAR_COL: DOLLAR_COL,
              SETS: SETS, RANGES: RANGES, ALLOW: ALLOW, TAGS: TAGS };
  root.JHSovereignDesk = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;

  /* ------------------------------------------------------------------------------------------------ the page */
  var doc = root.document;
  if (!doc || !doc.getElementById("sov-main")) return;
  var $ = function (id) { return doc.getElementById(id); };
  var D = null, H = null, W = null, WJ = {}, WH = null, DS = null, view = "desk", setId = "sovciss", wset = "yield", dyears = 0, dgeo = {};
  var VIEWS = ["desk", "crisis", "world", "dollar", "sources"];
  var chart = { id: null, years: 0, span: null, ep: null, geo: null }, over = { years: 0, mode: "all", flip: {}, geo: null };
  var today = new Date().toISOString().slice(0, 10);
  var KEY = "jh.sovereign-desk.v1", prefs = { tags: {}, sort: {}, show: "all" };
  try { var kept = JSON.parse(root.localStorage.getItem(KEY) || "null"); if (kept && typeof kept === "object") { prefs.tags = kept.tags || {}; prefs.sort = kept.sort || {}; prefs.show = kept.show || "all"; if (kept.lines === "tagged") over.mode = "tagged"; } } catch (e) { /* a browser that keeps nothing still works */ }
  function save() { prefs.lines = over.mode === "tagged" ? "tagged" : "all"; try { root.localStorage.setItem(KEY, JSON.stringify(prefs)); } catch (e) { /* kept for this visit only */ } }
  function meas(id) { return (D && D.measures[id]) || (DS && DS.measures && DS.measures[id]) || (WH && WH.measures && WH.measures[id]) || null; }
  function everyDay(id) { return DS && DS.series && DS.series[id] ? DS.series[id] : null; }        /* the dollar-shortage document keeps every observation */
  function seriesOf(id) { return everyDay(id) || (H && H.series && H.series[id]) || (WH && WH.series && WH.series[id]) || null; }

  var DD = null, wanted = null;      /* the Dollar tab's measures as last rendered; a chart asked for by link before its document arrived */
  function getJson(path) {
    return fetch(path, { cache: "no-store" }).then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); });
  }
  function needHistory(then) {
    if (H) { then(); return; }
    getJson("/data/ciss-sovereign-history.json").then(function (h) { H = h; then(); }).catch(function () { H = { series: {} }; then(); });
  }
  function cls(m, v) { var w = worseUp(m); return w === null || !isNum(v) || v === 0 ? "" : (v > 0) === w ? "up" : "dn"; }
  function oldNote(m) {
    if (m.status === "ended") return "";
    var a = age(m, today);
    if (m.source === "fred") return m.status === "late" ? " <span class=old>" + a.days + " days old</span>" : "";      /* its own release rhythm: H.10 comes a week in arrears */
    return a.old ? " <span class=old>" + a.days + " days old</span>" : "";
  }
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
      html += manyHtml(setCtx());
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
    var floor = lo, pad = (hi - lo || Math.abs(hi) || 1) * 0.06; hi += pad; if (lo !== 0) lo -= pad;
    if (floor >= 0 && lo < 0) lo = 0;          /* the margin never takes the axis of a series that is never negative below zero */
    var X = function (d) { return L + (d - x0) / (x1 - x0 || 1) * (wpx - L - R); }, Y = function (v) { return T + (hi - v) / (hi - lo || 1) * (hpx - T - B); };
    bands(opt.episodes, firstIso, lastIso).forEach(function (b) {
      var bx = L + b.x0 * (wpx - L - R), bw = Math.max(opt.mini ? 1 : 2, (b.x1 - b.x0) * (wpx - L - R)), strong = opt.ep && b.ep.id === opt.ep;
      g.fillStyle = strong ? "rgba(240,180,41,.34)" : "rgba(240,180,41,.13)"; g.fillRect(bx, T, bw, hpx - T - B);
    });
    if (!opt.mini) {
      g.strokeStyle = "#1e242c"; g.fillStyle = "#8b949e"; g.lineWidth = 1; g.textAlign = "right";
      for (var k = 0; k <= 4; k++) { var v = lo + (hi - lo) * k / 4, y = Y(v); g.beginPath(); g.moveTo(L, y); g.lineTo(wpx - R, y); g.stroke(); g.fillText(fmt(opt.unit, v, hi - lo >= 10).replace(" bp", "").replace(" bn", ""), L - 6, y + 4); }
      g.textAlign = "center";
      var y0 = +firstIso.slice(0, 4), y1 = +lastIso.slice(0, 4), step = Math.max(1, Math.ceil((y1 - y0 + 1) / Math.max(2, Math.floor((wpx - L - R) / 60))));
      for (var yr = y0 + 1; yr <= y1; yr += step) { var xx = X(dayNum(yr + "-01-01")); if (xx > L + 14 && xx < wpx - R - 14) g.fillText(String(yr), xx, hpx - 7); }
      if (y1 - y0 < 2) { g.fillText(fmtDate(firstIso), L + 34, hpx - 7); g.fillText(fmtDate(lastIso), wpx - R - 34, hpx - 7); }
    }
    if (lo < 0 && hi > 0) { g.strokeStyle = "#3a4452"; g.lineWidth = 1; g.beginPath(); g.moveTo(L, Y(0)); g.lineTo(wpx - R, Y(0)); g.stroke(); }
    live.forEach(function (l) {
      g.globalAlpha = l.alpha || 1; g.strokeStyle = l.colour; g.lineWidth = l.width || 1.4; g.beginPath();
      var n = l.pts.length, gap = l.gap || Infinity;      /* days: two readings further apart than this are not joined */
      l.pts.forEach(function (p, i) { if (i && p[0] - l.pts[i - 1][0] <= gap) g.lineTo(X(p[0]), Y(p[1])); else g.moveTo(X(p[0]), Y(p[1])); });
      g.stroke();
      if (l.gap) { g.fillStyle = l.colour; l.pts.forEach(function (p, i) {
        if ((i && p[0] - l.pts[i - 1][0] <= gap) || (i < n - 1 && l.pts[i + 1][0] - p[0] <= gap)) return;
        g.beginPath(); g.arc(X(p[0]), Y(p[1]), opt.mini ? 1.2 : 1.8, 0, 6.3); g.fill(); }); }
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
  /* ---- a chart of many lines: one description for Crisis peaks and for the World tab ---- */
  function setCtx() {
    var sc = setCols(), names = sc.names;
    return { cols: sc.ids.map(function (id) { return D.measures[id]; }).filter(Boolean), lead: true, name: function (m) { return colName(m, names); }, tag: function (m) { return colTag(m, names); },
             series: function (id) { return H && H.series[id]; }, ready: function () { return !!H; }, empty: "Every series is hidden — pick one in the list below." };
  }
  function worldCtx() {
    return { cols: worldCols(WH, wset, prefs.tags).map(function (c) { return c.m; }), lead: false, name: function (m) { return m.country; }, tag: function (m) { return prefs.tags[ckey(m.country)]; },
             series: function (id) { return WH.series[id]; }, ready: function () { return true; },
             empty: Object.keys(prefs.tags).length ? "Nothing to draw: every line is hidden, or no tagged country has a history in this set." : "No country is tagged yet — click the dot beside a country in the table below." };
  }
  function manyCtx() { return view === "world" ? (WH && WH.measures ? worldCtx() : null) : setCtx(); }
  function shownIn(ctx, m, i) { return lineShown(over.mode, ctx.tag(m), ctx.lead && i === 0, over.flip[m.id]); }
  function manyHtml(ctx) {
    var plain = !Object.keys(over.flip).length;
    return "<div class=ctrl id=sov-ovrg>" + RANGES.map(function (r) { return "<button class='gbtn" + (r[1] === over.years ? " on" : "") + "' data-oyears='" + r[1] + "'>" + r[0] + "</button>"; }).join("")
      + [["all", "Show all"], ["tagged", "Only tagged"], ["none", "Hide all"]].map(function (b) { return "<button class='gbtn" + (plain && over.mode === b[0] ? " on" : "") + "' data-olines=" + b[0] + ">" + b[1] + "</button>"; }).join("") + "</div>"
      + "<div class=cvbox><canvas id=sov-ov class=ovc></canvas><div id=sov-otip class=tip></div></div><div class=legend id=sov-leg>" + ctx.cols.map(function (m, i) {
        var t = ctx.tag(m), colr = ctx.lead && i === 0 && !t ? LEAD : lineColour(i, t);
        return "<button class='lg" + (shownIn(ctx, m, i) ? "" : " off") + "' data-line='" + esc(m.id) + "' title='" + esc((m.source_label ? m.source_label + ". " : "") + "Click to hide or show; double-click for its own chart") + "'><i style='background:" + colr + "'></i>"
          + esc(ctx.name(m)) + (m.status === "ended" ? " <span class=old>ended</span>" : "") + "</button>"; }).join("") + "</div>";
  }
  function drawSet() {
    var ctx = manyCtx(), cv = $("sov-ov");
    if (cv && ctx && ctx.ready()) {
      var anyTag = ctx.cols.some(function (m) { return ctx.tag(m); });
      var lines = ctx.cols.map(function (m, i) {
        var t = ctx.tag(m), lead = ctx.lead && i === 0;
        return { id: m.id, name: ctx.name(m), unit: m.unit, ended: m.status === "ended", gap: gapOf(m), colour: lead && !t ? LEAD : lineColour(i, t), width: t || lead ? 2.2 : 1.1, alpha: t || lead ? 1 : anyTag ? 0.35 : 0.8,
                 pts: shownIn(ctx, m, i) ? points(ctx.series(m.id), over.years) : [] };
      });
      over.geo = plot(cv, lines.slice().reverse(), { unit: (ctx.cols[0] || {}).unit, episodes: D.episodes, empty: lines.some(function (l) { return l.pts.length >= 2; }) || !lines.length ? "No history in this range." : ctx.empty });
      cv.setAttribute("data-lines", lines.filter(function (l) { return l.pts.length >= 2; }).length);
      cv.setAttribute("data-mode", over.mode);
    }
    doc.querySelectorAll("canvas[data-mini]").forEach(function (mc) {
      var m = D.measures[mc.getAttribute("data-mini")];
      plot(mc, [{ colour: "#F0B429", width: 1.2, pts: points(H.series[m.id], 0), ended: m.status === "ended" }], { unit: m.unit, episodes: D.episodes, mini: true, empty: "no history" });
    });
  }
  function syncMany() {
    var ctx = manyCtx(); if (!ctx) return;
    var plain = !Object.keys(over.flip).length;
    doc.querySelectorAll("[data-olines]").forEach(function (b) { b.classList.toggle("on", plain && b.getAttribute("data-olines") === over.mode); });
    ctx.cols.forEach(function (m, i) { var b = doc.querySelector("#sov-leg [data-line='" + m.id + "']"); if (b) b.classList.toggle("off", !shownIn(ctx, m, i)); });
    drawSet();
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
    var m = meas(chart.id), cv = $("sov-cv"), s = seriesOf(chart.id);
    if (!m || (!s && !H)) { var g = cv.getContext("2d"); g.clearRect(0, 0, cv.width, cv.height); return; }
    chart.geo = plot(cv, [{ colour: "#F0B429", width: 1.4, pts: s ? points(s, chart.years, chart.span) : [], ended: m.status === "ended", gap: gapOf(m) }],
      { unit: m.unit, episodes: D.episodes, ep: chart.ep, empty: s ? "Not enough history in this range." : "No history for this measure." });
  }
  function hoverChart(ev) {
    var geo = chart.geo, tip = $("sov-tip"); if (!geo) { tip.style.display = "none"; return; }
    var box = $("sov-cv").getBoundingClientRect(), px = (ev.touches ? ev.touches[0].clientX : ev.clientX) - box.left;
    var pts = geo.lines[0].pts, best = nearest(pts, geo.x0 + (px - geo.L) / (geo.w - geo.L - geo.R) * (geo.x1 - geo.x0));
    var m = meas(chart.id), ep = windowOf(D.episodes, best[2]);
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
    var m = meas(id); if (!m) return;
    var ep = epId && D.episodes.filter(function (e) { return e.id === epId; })[0];
    chart.id = id; chart.years = 0; chart.ep = ep ? ep.id : null; chart.span = ep ? around(ep) : null;
    $("sov-ct").textContent = m.label;
    var links = [], fed = m.source === "fred", own = !!m.source && !fed;          /* a measure of the World tab names its own source; so does one from the Federal Reserve */
    var dm = DS && DS.measures && DS.measures[id], end = dollarSide(dm) || side(m);                /* the end of a window the Dollar tab reads this measure by, where it takes a side */
    if (fed) {
      (m.series || []).forEach(function (sid) { links.push("<a href='https://fred.stlouisfed.org/series/" + encodeURIComponent(sid) + "' target=_blank rel=noopener>" + esc(sid) + " at FRED</a>"); });
      links.push(link("data/ciss-dollar-shortage.json", "the dollar-shortage document"));
    }
    if (own) {
      if (m.source === "ecb" && ecbPage(m)) links.push("<a href='" + esc(ecbPage(m)) + "' target=_blank rel=noopener>the series at the ECB</a>");
      if (m.source === "oecd") links.push("<a href='https://fred.stlouisfed.org/series/" + encodeURIComponent(String(m.key).split(":").pop()) + "' target=_blank rel=noopener>the series at FRED</a>");
      if (m.source === "dtcc") links.push("<a href='/cds.html' target=_blank rel=noopener>the CDS desk that measured it</a>");
      if (m.source === "reported") links.push(link("data/global-sovereign.json", "today's world document"));
      links.push(link("data/ciss-sovereign-world.json", "the stored histories"));
    }
    if (!own && !fed && m.key && ecbPage(m)) links.push("<a href='" + esc(ecbPage(m)) + "' target=_blank rel=noopener>the series at the ECB</a>");
    if (m.evidence) links.push(link(m.evidence.key, "the response it was read from (.gz)"));
    if (!own && m.key && /^(CISS|CLIFS)\./.test(m.key)) links.push("<a href='/ciss.html?series=" + encodeURIComponent(m.key) + "' target=_blank rel=noopener>in the CISS register</a>");
    $("sov-cm").innerHTML = "<b>" + fmt(m.unit, m.value) + "</b> · " + when(m) + (m.status === "ended" ? " · <span class=old>" + (fed ? "this series ended at its source" : "the ECB stopped this series") + "; this is its last reading</span>" : oldNote(m))
      + (m.blank_after ? " · the ECB's " + esc(m.blank_after) + " row is blank" : "") + " · " + esc(standing(m)) + "<br>"
      + (own || fed ? esc(m.source_label) : (m.key ? "<code>" + esc(m.key) + "</code>" : esc(m.derived || "")) + (m.exact && m.key ? " · source value " + esc(m.exact) : "")) + "<br>" + links.filter(Boolean).join(" · ")
      + (dm && dm.what ? "<br><span class=wh>" + esc(dm.what) + "</span>" : "");
    var peaks = (D.episodes || []).map(function (e) { var p = (m.peaks || {})[e.id]; return p ? [e, p[end]] : null; }).filter(Boolean);
    $("sov-cs").innerHTML = "<div class=stat>History from " + fmtDate(m.start) + " · " + m.n.toLocaleString("en-US") + " observations · high " + fmt(m.unit, m.high[0]) + " on " + fmtDate(m.high[1]) + " · low " + fmt(m.unit, m.low[0]) + " on "
      + fmtDate(m.low[1]) + " · mean " + fmt(m.unit, m.mean) + (m.freq === "D" && !own && !everyDay(m.id) ? "<span id=sov-wk class=wk> · the line is the last reading of each week; highs and lows are from every day</span>" : "")
      + (m.source === "dtcc" ? " · one point for each day the name traded; days more than " + GAP + " apart are not joined" : "") + (m.source === "reported" ? " · as the provider showed it each day, unchecked" : "") + "</div>"
      + (peaks.length ? "<div class=sec>" + (end === "lo" ? "Lowest" : "Highest") + " reading inside each named window (shaded on the chart) · click one to look around it</div><div class=chips>" + peaks.map(function (p) {
        return "<span class='chip" + (p[0].id === chart.ep ? " cur" : "") + "' data-chart-ep='" + esc(p[0].id) + "' tabindex=0 role=button>" + esc(p[0].name) + " <b>" + fmt(m.unit, p[1][0]) + "</b> " + fmtDate(p[1][1]) + "</span>"; }).join("") + "</div>" : "");
    rangeButtons();
    $("sov-ovl").classList.add("on");
    if (own || fed || everyDay(id)) drawChart(); else needHistory(function () { if (chart.id) drawChart(); });
  }
  function closeChart() { $("sov-ovl").classList.remove("on"); chart.id = null; $("sov-tip").style.display = "none"; }

  /* ---- World, Sources ---- */
  function needWorld() {
    if (WH) return;
    WH = { loading: true };
    getJson("/data/ciss-sovereign-world.json").then(function (d) { if (d.contract !== "sovereign-world.v1" || !d.measures || !d.series) throw new Error("unexpected document"); WH = d; })
      .catch(function (e) { WH = { error: e.message }; }).then(function () { if (view === "world") render(); });
  }
  function worldChart() {
    if (!WH || WH.loading) return "<p class=note id=sov-wwait>Loading the histories…</p>";
    if (WH.error) return "<p class=note id=sov-wwait>The histories behind this table could not be read (" + esc(WH.error) + "). The table below is complete without them.</p>";
    var set = WH.sets.filter(function (s) { return s.id === wset; })[0] || WH.sets[0]; wset = set.id;
    var ctx = worldCtx(), by = (WH.coverage || {}).by_source || {}, miss = (WH.missing || {})[wset] || [];
    var html = "<div class=ctrl id=sov-wsets>" + WH.sets.map(function (s) { return "<button class='gbtn" + (s.id === wset ? " on" : "") + "' data-wset='" + s.id + "' title='" + esc(s.detail) + "'>" + esc(s.name) + " <small>" + s.n + "</small></button>"; }).join("") + "</div>"
      + "<div class=sec>" + esc(set.name) + " · " + ctx.cols.length + " of " + WH.countries.length + " countries · every series over its whole stored history</div>"
      + "<p class=note>" + esc(set.detail.charAt(0).toUpperCase() + set.detail.slice(1)) + (/_reported$/.test(wset) && WH.reported_since ? " since " + fmtDate(WH.reported_since) : "") + ". Tagged countries are drawn thick in their colour; "
      + "“Only tagged” keeps to them as you tag. Click a name in the list to hide or show its line, double-click it or click a country in the table for its own chart.</p>"
      + manyHtml(ctx);
    if (miss.length) html += "<p class=note id=sov-wmiss><b>No stored history in this set for:</b> " + esc(miss.join(", ")) + "." + (/_reported$/.test(wset) ? "" : " Their daily record is in “" + esc((WH.sets.filter(function (s) { return s.id === wset + "_reported"; })[0] || {}).name || "as reported") + "”.") + " Nothing is estimated for them.</p>";
    html += "<p class=note>Lines: " + ["ecb", "oecd", "dtcc", "reported"].filter(function (k) { return by[k]; }).map(function (k) { return by[k] + " " + { ecb: "from the ECB", oecd: "from the OECD", dtcc: "measured from DTCC prints", reported: "kept from this table day by day" }[k]; }).join(", ")
      + " · compiled " + esc(String(WH.generated_at || "").slice(0, 16).replace("T", " ")) + " UTC · " + link("data/ciss-sovereign-world.json", "the document") + "</p>";
    return html;
  }
  function renderWorld() {
    if (!W) { $("sov-main").innerHTML = "<p class=note>Loading the world table…</p>"; return; }
    if (W.error) { $("sov-main").innerHTML = "<p class=note>The world table is not available (" + esc(W.error) + ").</p>"; return; }
    needWorld();
    var rows = world(W), txt = function (v) { return "<td class=l>" + esc(v || "") + "</td>"; }, ids = worldIds(WH);
    var cell = function (r, kind, text, bold) {
      var id = text ? worldPick(ids[r.key], kind, wset) : null, m = id && WH.measures[id];
      return "<td" + (m ? " class=c data-id='" + esc(id) + "' title='" + esc(m.label + " — its history") + "'" : "") + ">" + (bold ? "<b>" + text + "</b>" : text) + "</td>";
    };
    $("sov-main").innerHTML = "<p class=note>Outside the ECB's reach: 10-year yields, default-swap spreads, ratings and policy rates as reported by World Government Bonds, " + esc(String(W.generated_at || "").slice(0, 10))
      + ". Reported figures only; a missing figure is left blank. <a href='/global-sovereign.html' target=_blank rel=noopener>Open the full page</a>.</p>" + worldChart() + tagBar() + table("world", [
      { id: "country", label: "Country", cls: "l", first: 1, value: function (r) { return r.country; }, cell: function (r) {
          var id = worldPick(ids[r.key], "name", wset), m = id && WH.measures[id];
          return "<td class='l nm'>" + tagButton(r) + (m ? "<span class=cn data-id='" + esc(id) + "' tabindex=0 role=button title='" + esc(m.label + " — its history") + "'>" + esc(r.country) + "</span>" : esc(r.country)) + "</td>"; } },
      { id: "region", label: "Region", cls: "l", first: 1, value: function (r) { return r.region; }, cell: function (r) { return txt(r.region); } },
      { id: "y", label: "10y yield", value: function (r) { return isNum(r.y) ? r.y : null; }, cell: function (r) { return cell(r, "yield", isNum(r.y) ? fixed(r.y, 2) + "%" : ""); } },
      { id: "spread", label: "vs Bund", value: function (r) { return isNum(r.spread) ? r.spread : null; }, cell: function (r) { return "<td>" + (isNum(r.spread) ? fmtChange("bp", r.spread) : "") + "</td>"; } },
      { id: "cds", label: "CDS", value: function (r) { return isNum(r.cds) ? r.cds : null; }, cell: function (r) { return cell(r, "cds", isNum(r.cds) ? fixed(r.cds, 1) + " bp" : "", true); } },
      { id: "rating", label: "Rating", cls: "l", first: 1, value: function (r) { return r.rating; }, cell: function (r) { return txt(r.rating); } },
      { id: "policy", label: "Policy rate", value: function (r) { return isNum(r.policy) ? r.policy : null; }, cell: function (r) { return "<td>" + (isNum(r.policy) ? fixed(r.policy, 2) + "%" : "") + "</td>"; } },
      { id: "asof", label: "As of", cls: "l", cell: function (r) { return txt(r.asOf); } }], rows, ["cds", -1]);
    if (WH && WH.measures) drawSet();
  }
  /* ---- Dollar shortage ---- */
  function needDollar() {
    if (DS) return;
    DS = { loading: true };
    getJson("/data/ciss-dollar-shortage.json").then(function (d) { if (d.contract !== "ciss-dollar-shortage.v1" || !d.measures || !d.series) throw new Error("unexpected document"); DS = d; })
      .catch(function (e) { DS = { error: e.message }; }).then(function () {
        if (view === "dollar") render();
        if (chart.id && everyDay(chart.id)) { var wk = $("sov-wk"); if (wk) wk.parentNode.removeChild(wk); drawChart(); }
        if (wanted) { var id = wanted; wanted = null; if (meas(id)) openChart(id); }
      });
  }
  function dcls(m, v) { var sd = dollarSide(m); return !sd || !isNum(v) || v === 0 ? "" : (v > 0) === (sd === "hi") ? "up" : "dn"; }
  function dstate(m) { return m.status === "ended" ? " <span class=old>ended</span>" : oldNote(m); }
  function dsource(m) {
    if (m.source === "fred") return (m.series || []).map(function (sid) { return "<a href='https://fred.stlouisfed.org/series/" + encodeURIComponent(sid) + "' target=_blank rel=noopener>" + esc(sid) + " at FRED</a>"; }).join(" · ");
    return (ecbPage(m) ? "<a href='" + esc(ecbPage(m)) + "' target=_blank rel=noopener>the series at the ECB</a>" : "") + (m.evidence ? " · " + link(m.evidence.key, "the response it was read from (.gz)") : "");
  }
  function renderDollar() {
    needDollar();
    var dd = DD = dollarData(DS, D, H);
    if (!dd.ids.length) { $("sov-main").innerHTML = "<p class=note>The dollar-shortage measures are not in this run's documents; see Sources.</p>"; return; }
    var M = dd.measures, w = dollarWindows(dd), fed = dd.ids.some(function (id) { return M[id].source === "fred"; });
    var first = dd.parts[0].id === DOLLAR_PART.id ? dd.parts[0] : null, others = first ? dd.parts.slice(1) : dd.parts;
    function chartsOf(p) {
      return "<div class=dpart data-part='" + esc(p.id) + "'><div class=dph>" + esc(p.name) + (p.note ? "<small>" + esc(p.note) + "</small>" : "") + "</div><div class=dcharts>" + p.ids.map(function (id) {
        var m = M[id], gloss = dollarGloss(m);
        return "<div class=dchart><div class=mt data-id='" + esc(id) + "' tabindex=0 role=button title='" + esc(m.label) + "'>" + esc(dollarName(m)) + " <b>" + fmt(dunit(m), m.value) + "</b> <span class=td>" + when(m) + dstate(m) + "</span>"
          + (gloss ? "<small>" + esc(gloss) + "</small>" : "") + "</div>"
          + "<div class=cvbox><canvas class=dcv data-dchart='" + esc(id) + "'></canvas><div class=tip data-dtip='" + esc(id) + "'></div></div>"
          + "<p class=note>" + esc(m.what || m.label) + "</p></div>"; }).join("") + "</div></div>";
    }
    var html = "<div class=read id=sov-dread>" + dollarRead(dd).map(function (p) { return "<p>" + esc(p) + "</p>"; }).join("") + "</div>"
      + "<p class=note>" + (fed ? "The three ECB measures come first, as they were added; the Federal Reserve's lines follow. " : "Three ECB measurements read side by side. ")
      + "In a dollar shortage banks abroad cannot borrow dollars in the market: central banks lend them instead, the dollar rises, and foreign central banks sell or pledge what they hold in the United States. Measurements, not a forecast.</p>";
    /* the three ECB measures: their tiles, then their charts */
    if (first) {
      html += "<div class=sec>The three ECB measures · click one for its own chart</div><div class=tiles id=sov-dtiles>" + first.ids.map(function (id) { return tile(M[id]); }).join("") + "</div>";
    }
    html += "<div class=sec>" + (first ? "Their whole history" : "The whole history of each") + " · named windows shaded · click a name for the larger chart</div><div class=ctrl id=sov-drg>" + RANGES.map(function (r) {
      return "<button class='gbtn" + (r[1] === dyears ? " on" : "") + "' data-dyears='" + r[1] + "'>" + r[0] + "</button>"; }).join("") + "</div>";
    if (first) html += chartsOf(first);
    /* every measure in one table */
    html += "<div class=sec>Every measure · click a row for its own chart</div>"
      + "<p class=note>“A shortage shows as” is the direction a shortage has moved a line in before. The bar is how far the reading stands that way: for a lending line its share of the line's record, for the others the share of the line's own readings it stands beyond.</p>"
      + "<div class=scroll><table class='board dboard' id=sov-dboard><thead><tr><th class=l>Measure</th><th>Now</th><th class=l>As of</th><th>1 week</th><th>1 month</th><th>12 months</th>"
      + "<th class=l>A shortage shows as</th><th class=l>How far that way</th><th class=l>Record</th></tr></thead><tbody>";
    dd.parts.forEach(function (p) {
      html += "<tr class=grp data-part='" + esc(p.id) + "'><td class=l colspan=9>" + esc(p.name) + (p.note ? "<small>" + esc(p.note) + "</small>" : "") + "</td></tr>";
      p.ids.forEach(function (id) {
        var m = M[id], sd = dollarSide(m), place = dollarPlace(m), ended = m.status === "ended", rec = sd === "lo" ? m.low : m.high, gloss = dollarGloss(m);
        function ch(k) { return "<td class='" + (ended ? "nil" : dcls(m, m.change[k])) + "'>" + (ended ? "—" : dchange(m, k)) + "</td>"; }
        html += "<tr class=drow data-id='" + esc(id) + "' tabindex=0 role=button title='" + esc(m.label) + "'><td class='l nm'>" + esc(dollarName(m)) + "<small>" + esc(m.source === "fred" ? "Federal Reserve" : "ECB") + " · " + (m.freq === "D" ? "daily" : "weekly")
          + (gloss ? " · " + esc(gloss) : "") + "</small></td>"
          + "<td><b>" + fmt(dunit(m), m.value) + "</b></td><td class=l>" + when(m) + dstate(m) + "</td>" + ch("1w") + ch("1m") + ch("12m")
          + "<td class=l>" + (sd === "hi" ? "a rise ▲" : sd === "lo" ? "a fall ▼" : "<span class=nil>no side taken</span>") + "</td>"
          + "<td class=l title='" + esc(standing(m)) + "'>" + (place === null ? "" : "<span class=pb><i class='h" + dollarHeat(m) + "' style='width:" + Math.max(2, place).toFixed(0) + "%'></i></span> ")
            + "<span class='pt" + (ended ? " nil" : "") + "'>" + esc(dollarPlaceText(m)) + "</span></td>"
          + "<td class=l>" + (sd ? (sd === "lo" ? "low " : "high ") + fmt(dunit(m), rec[0]) + " <span class=old>" + MONTHS[+rec[1].slice(5, 7) - 1] + " " + rec[1].slice(0, 4) + "</span>"
                                 : fmt(dunit(m), m.low[0]) + " to " + fmt(dunit(m), m.high[0])) + "</td></tr>";
      });
    });
    html += "</tbody></table></div>";
    /* the Federal Reserve's lines, part by part */
    if (others.length) html += (first ? "<div class=sec>The Federal Reserve's lines · whole history · the range buttons above apply here too</div>" : "") + others.map(chartsOf).join("");
    /* inside each named window */
    html += "<div class=sec>Inside each named window · click a cell for the chart around it</div>"
      + "<p class=note>The three ECB measures give the lowest and the highest reading inside each window; a Federal Reserve line gives the end a shortage shows in. The marked columns are the ends a shortage shows in: the lowest euro, the highest lending, "
      + "the highest dollar, the largest fall in custody holdings. The darker a marked cell, the nearer that reading lies to the measure's own record in that direction."
      + (w.left.length ? " Not in this table, because the page takes no side on them: " + w.left.map(function (m) { return esc(dollarName(m)); }).join("; ") + "." : "") + "</p>"
      + "<div class=scroll><table class='board dwin' id=sov-dwin><thead><tr><th class=l rowspan=2>Window</th>" + w.groups.map(function (g) { return "<th class='c grp' colspan=" + g.span + ">" + esc(g.name) + "</th>"; }).join("") + "</tr><tr>"
      + w.cols.map(function (c) { return "<th class='" + (c.marked ? "mk" : "") + "' title='" + esc(c.m.label) + "'>" + esc(DOLLAR_COL[c.m.id] || dollarName(c.m)) + "<small>" + (c.end === "lo" ? "low" : "high") + "</small></th>"; }).join("") + "</tr></thead><tbody>";
    function cellOf(c, x, ep, plain) {
      if (!x) return "<td class=nil>—</td>";
      var shade = c.marked && !plain;
      return "<td class='c" + (shade ? " mk" : "") + "' data-id='" + esc(c.m.id) + "'" + (ep ? " data-ep='" + esc(ep.id) + "'" : "") + (shade ? " style='background:rgba(224,122,106," + (dollarShare(c.m, c.end, x[0]) * 0.55).toFixed(2) + ")'" : "")
        + " title='" + esc(fmtDate(x[1])) + "'>" + fmt(dunit(c.m), x[0]).replace(" bn", "").replace(" bp", "") + "</td>";
    }
    w.rows.forEach(function (r) {
      html += "<tr data-ep='" + esc(r.ep.id) + "'><td class='l nm'>" + esc(r.ep.name) + "<small>" + fmtDate(r.ep.from) + " – " + fmtDate(r.ep.to) + "</small></td>" + r.cells.map(function (x, i) { return cellOf(w.cols[i], x, r.ep); }).join("") + "</tr>";
    });
    html += "<tr class=nowrow><td class='l nm'>Now<small>each at its latest date</small></td>" + w.now.map(function (n) { return "<td class=c colspan=" + n.span + " data-id='" + esc(n.m.id) + "' style='text-align:" + (n.span > 1 ? "center" : "right") + "' title='" + esc(when(n.m)) + "'>"
      + fmt(dunit(n.m), n.m.value).replace(" bn", "").replace(" bp", "") + (n.m.status === "ended" ? " <span class=old>ended</span>" : "") + "</td>"; }).join("") + "</tr>";
    html += "<tr class=recrow><td class='l nm'>Record<small>whole history</small></td>" + w.cols.map(function (c, i) { return cellOf(c, w.record[i], null, true); }).join("") + "</tr></tbody></table></div>";
    /* where each number comes from */
    var bad = Object.keys(dd.errors);
    html += "<div class=sec>Where each number comes from</div><p class=note id=sov-dsrc>" + dd.ids.map(function (id) { var m = M[id];
      return "<b>" + esc(m.label) + "</b> · <code>" + esc(m.key) + "</code> · since " + fmtDate(m.start) + ", " + m.n.toLocaleString("en-US") + " observations · " + dsource(m)
        + (m.source === "fred" && m.read_at ? " · read from the site's warehouse " + esc(String(m.read_at).slice(0, 16).replace("T", " ")) + " UTC" : ""); }).join("<br>")
      + "<br>The ECB's three are checked against their responses on the <a href='/ciss.html' target=_blank rel=noopener>CISS register</a>."
      + (fed ? " A Federal Reserve line begins at its first reading above zero: FRED fills the weeks before the line was printed with zeros. DXY is ICE's formula on the Federal Reserve's H.10 noon rates, so it runs a week behind the market's own print." : "") + " "
      + (dd.full ? link("data/ciss-dollar-shortage.json", "The document") + " · compiled " + esc(String(DS.generated_at).slice(0, 16).replace("T", " ")) + " UTC."
                 : "<span class=old>The complete history has not arrived" + (DS && DS.error ? " (" + esc(DS.error) + ")" : "") + "; the reference rate is drawn from one reading a week.</span>")
      + (bad.length ? "<br><span class=old>Not available in this run: " + bad.map(function (k) { return esc(k + " — " + dd.errors[k]); }).join("; ") + "</span>" : "")
      + " The three ECB measures, the Federal Reserve's lending and the official holdings are also on the <a href='/dollar-indexes.html' target=_blank rel=noopener>Dollar desk</a>.</p>";
    $("sov-main").innerHTML = html;
    if (dd.full) drawDollar(); else needHistory(function () { if (view === "dollar") drawDollar(); });
  }
  function drawDollar() {
    var dd = DD = dollarData(DS, D, H); dgeo = {};
    doc.querySelectorAll("canvas[data-dchart]").forEach(function (cv) {
      var id = cv.getAttribute("data-dchart"), m = dd.measures[id];
      dgeo[id] = plot(cv, [{ colour: "#F0B429", width: 1.3, pts: points(dd.series[id], dyears), ended: m.status === "ended" }],
        { unit: dunit(m), episodes: dd.episodes, empty: dd.series[id] ? (m.status === "ended" ? "This series ended on " + fmtDate(m.date) + "; pick a longer range." : "Not enough history in this range.") : "No history for this measure." });
      cv.setAttribute("data-points", dgeo[id] ? dgeo[id].lines[0].pts.length : 0);
    });
  }
  function hoverDollar(ev) {
    var cv = ev.target, id = cv.getAttribute("data-dchart"), geo = dgeo[id], tip = doc.querySelector("[data-dtip='" + id + "']"); if (!geo || !tip || !DD) return;
    var box = cv.getBoundingClientRect(), px = (ev.touches ? ev.touches[0].clientX : ev.clientX) - box.left;
    var best = nearest(geo.lines[0].pts, geo.x0 + (px - geo.L) / (geo.w - geo.L - geo.R) * (geo.x1 - geo.x0)), m = DD.measures[id], ep = windowOf(D.episodes, best[2]);
    tip.style.display = "block"; tip.style.left = Math.min(geo.w - 170, Math.max(4, geo.X(best[0]) + 10)) + "px"; tip.style.top = Math.max(4, geo.Y(best[1]) - 34) + "px";
    tip.innerHTML = "<b>" + fmt(dunit(m), best[1]) + "</b> " + fmtDate(best[2]) + (ep ? "<br>" + esc(ep.name) : "");
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
      + " · " + link(D.history.key, "histories") + " · CDS and ratings: " + link("data/global-sovereign.json", "the world document") + " (World Government Bonds, as reported) · the World tab's charts: "
      + link("data/ciss-sovereign-world.json", "the stored histories") + " (ECB and OECD monthly yields, CDS measured from DTCC prints, the world table's own daily figures) · the Dollar shortage tab: "
      + link("data/ciss-dollar-shortage.json", "its document") + " (the same ECB responses, every observation)</p>";
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
    if (view === "desk") renderDesk(); else if (view === "crisis") renderCrisis(); else if (view === "world") renderWorld(); else if (view === "dollar") renderDollar(); else renderSources();
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
      if (!Object.keys(prefs.tags).length) { prefs.show = "all"; if (over.mode === "tagged") over.mode = "all"; }
      save(); render(); return;
    }
    if ((hit = t.closest && t.closest("[data-tag]"))) { var p = $("sov-pal"); if (p.style.display === "flex" && p.getAttribute("data-for") === hit.getAttribute("data-tag")) closePalette(); else openPalette(hit); return; }
    closePalette();
    if ((hit = t.closest && t.closest("#sov-tabs .tab"))) { view = hit.getAttribute("data-view"); over.flip = {}; render(); return; }
    if ((hit = t.closest && t.closest("[data-show]"))) { prefs.show = hit.getAttribute("data-show"); save(); render(); return; }
    if (t.closest && t.closest("[data-cleartags]")) { prefs.tags = {}; prefs.show = "all"; if (over.mode === "tagged") over.mode = "all"; save(); render(); return; }
    if ((hit = t.closest && t.closest("th[data-sort]"))) {
      var tb = hit.closest("table"), sorted = tb.getAttribute("data-sorted").split(":");
      prefs.sort[tb.getAttribute("data-table")] = nextSort([sorted[0], +sorted[1]], hit.getAttribute("data-sort"), +hit.getAttribute("data-first"));
      save(); render(); return;
    }
    if ((hit = t.closest && t.closest("[data-set]"))) { setId = hit.getAttribute("data-set"); over.flip = {}; render(); return; }
    if ((hit = t.closest && t.closest("[data-wset]"))) { wset = hit.getAttribute("data-wset"); over.flip = {}; render(); return; }
    if ((hit = t.closest && t.closest("[data-dyears]"))) { dyears = +hit.getAttribute("data-dyears"); hit.parentNode.querySelectorAll("[data-dyears]").forEach(function (b) { b.classList.toggle("on", +b.getAttribute("data-dyears") === dyears); }); drawDollar(); return; }
    if ((hit = t.closest && t.closest("[data-oyears]"))) { over.years = +hit.getAttribute("data-oyears"); hit.parentNode.querySelectorAll("[data-oyears]").forEach(function (b) { b.classList.toggle("on", +b.getAttribute("data-oyears") === over.years); }); drawSet(); return; }
    if ((hit = t.closest && t.closest("[data-olines]"))) { over.mode = hit.getAttribute("data-olines"); over.flip = {}; save(); syncMany(); return; }
    if ((hit = t.closest && t.closest("#sov-leg [data-line]"))) {
      var lid = hit.getAttribute("data-line"); if (over.flip[lid]) delete over.flip[lid]; else over.flip[lid] = 1;
      syncMany(); return;
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
  function isDollarChart(el) { return !!(el && el.getAttribute && el.getAttribute("data-dchart")); }
  doc.addEventListener("mousemove", function (ev) {
    if (ev.target.id === "sov-ov") hoverSet(ev); else if ($("sov-otip")) $("sov-otip").style.display = "none";
    if (isDollarChart(ev.target)) hoverDollar(ev); else doc.querySelectorAll("[data-dtip]").forEach(function (x) { x.style.display = "none"; });
  });
  doc.addEventListener("touchstart", function (ev) { if (ev.target.id === "sov-ov") hoverSet(ev); else if (isDollarChart(ev.target)) hoverDollar(ev); }, { passive: true });
  root.addEventListener("resize", function () { if (chart.id) drawChart(); if ((view === "crisis" && H) || (view === "world" && WH && WH.measures)) drawSet(); if (view === "dollar" && doc.querySelector("canvas[data-dchart]")) drawDollar(); });

  getJson("/data/ciss-sovereign.json").then(function (d) {
    if (d.contract !== "ciss-sovereign.v1" || !d.measures) throw new Error("unexpected document");
    D = d;
    var ea = d.measures.EA_SOV_GDP, n = Object.keys(d.measures).length;
    $("sov-asof").innerHTML = "ECB data" + (ea ? " to " + fmtDate(ea.date) : "") + " · compiled " + esc(d.generated_at.slice(0, 16).replace("T", " ")) + " UTC · " + n + " measures, " + d.countries.length + " countries"
      + (Object.keys(d.errors || {}).length ? " · <span class=old>" + Object.keys(d.errors).length + " source(s) not available in this run — see Sources</span>" : "");
    var params = new URLSearchParams(root.location.search), tab = params.get("view");
    if (tab && VIEWS.indexOf(tab) >= 0) view = tab;                                              /* ?view=dollar opens a tab directly */
    render();
    var want = params.get("m");
    if (want && d.measures[want]) openChart(want);
    else if (want && /^[A-Z0-9_]{3,40}$/.test(want)) { wanted = want; needDollar(); }                 /* a Federal Reserve measure: its document is read first */
    getJson("/data/global-sovereign.json").then(function (w) { W = w; WJ = joinWorld(w); render(); }).catch(function (e) { W = { error: e.message }; if (view === "world") render(); });
  }).catch(function (e) {
    $("sov-asof").textContent = "The sovereign-stress document could not be read: " + e.message;
    $("sov-main").innerHTML = "<p class=note>Nothing is shown rather than something old. The CISS register still opens: <a href='/ciss.html' target=_blank rel=noopener>ciss.html</a>.</p>";
  });
})(typeof window !== "undefined" ? window : globalThis);
