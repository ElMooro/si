/* Sovereign stress desk (sovereign-desk.html) - 2026-11-22.
 *
 * Reads data/ciss-sovereign.json, compiled by the CISS engine from retained ECB originals: SovCISS by country and for
 * the euro area, the all-issuer / AAA government curve, 10-year yields and spreads to Germany, TARGET balances,
 * government debt, CISS and CLIFS.  Every number on the page is a dated measurement from that document; the page
 * formats, ranks and draws, and re-reads each date against today's so an old reading says how old it is.
 * Nothing here is a forecast or a score of its own.
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
  /* crisis-view columns; the first measure of a set is the one whose own highest readings are listed below the table */
  var CORE = ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "FI", "IE", "PT", "GR"];
  var SETS = [
    { id: "sovciss", name: "SovCISS by country", ids: ["EA_SOV_GDP"].concat(CORE.map(function (a) { return "SOV_" + a; })) },
    { id: "spread", name: "10-year spreads to Germany", ids: ["EA_CURVE_10Y"].concat(CORE.slice(1).map(function (a) { return "S10_" + a; })) },
    { id: "euro", name: "Euro area and TARGET", ids: ["EA_SOV_GDP", "EA_SOV_EQ", "EA_SOV_RANGE", "EA_CURVE_10Y", "EA_CURVE_2Y", "EA_CISS", "EA_TGT_DEBT", "TGT_DE", "TGT_IT", "TGT_ES"] }
  ];
  var SHORT = { EA_SOV_GDP: "Euro area", EA_SOV_EQ: "EA equal-wt", EA_SOV_RANGE: "High − low", EA_CURVE_10Y: "All − AAA 10y", EA_CURVE_2Y: "All − AAA 2y",
                EA_CISS: "CISS", EA_TGT_DEBT: "TARGET owed", TGT_DE: "TARGET DE", TGT_IT: "TARGET IT", TGT_ES: "TARGET ES" };

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
  /* is a rise in this measure a rise in stress?  true / false, or null where the page takes no side (a yield, a balance, a debt ratio) */
  function worseUp(m) {
    if (!m) return null;
    if (m.group === "sovciss" || m.group === "ciss" || m.group === "clifs" || m.group === "spread" || m.group === "contribution") return true;
    if (m.group === "euro_area") return true;
    return null;
  }
  /* 0..4 from the reading's place in its own history, for stress measures only */
  function heat(m) {
    if (!m || worseUp(m) !== true || !isNum(m.pctile)) return 0;
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

  /* the country board: governments with a SovCISS (ranked by it), then the other EU governments, then every country CISS */
  function board(doc) {
    var M = doc.measures || {};
    var rows = (doc.countries || []).map(function (c) {
      var m = {};
      Object.keys(c.ids).forEach(function (k) { m[k] = M[c.ids[k]] || null; });
      return { area: c.area, country: c.country, euro: c.euro, m: m };
    });
    function val(r, k) { return r.m[k] && isNum(r.m[k].value) ? r.m[k].value : -Infinity; }
    var germany = M.SOV_DE;
    var sovereign = rows.filter(function (r) { return r.m.sovciss; }).sort(function (a, b) { return val(b, "sovciss") - val(a, "sovciss") || (a.area < b.area ? -1 : 1); });
    sovereign.forEach(function (r) {
      r.vsGermany = germany && r.area !== "DE" && r.m.sovciss.date === germany.date ? +(r.m.sovciss.value - germany.value).toFixed(5) : null;
      var a = (r.m.sovciss.peaks || {}).italy2011, b = (r.m.sovciss.peaks || {}).spain2012;
      r.crisis = a && b ? (a.hi[0] >= b.hi[0] ? a.hi : b.hi) : (a || b || {}).hi || null;
    });
    var others = rows.filter(function (r) { return !r.m.sovciss && (r.m.yield || r.m.debt || r.m.target); })
      .sort(function (a, b) { return val(b, "spread") - val(a, "spread") || (a.country < b.country ? -1 : 1); });
    var systemic = rows.filter(function (r) { return r.m.ciss; }).sort(function (a, b) { return val(b, "ciss") - val(a, "ciss"); });
    return { sovereign: sovereign, others: others, systemic: systemic };
  }

  /* the date most rows of a column carry, so the header can say it once */
  function commonWhen(rows, kind) {
    var seen = {}, best = null;
    rows.forEach(function (r) { var m = r.m[kind]; if (m) { var w = when(m); seen[w] = (seen[w] || 0) + 1; if (!best || seen[w] > seen[best]) best = w; } });
    return best;
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
    return out;
  }

  /* a TARGET balance that is negative on average belongs to a central bank that owes: it is read at its lowest; everything else at its highest */
  function side(m) { return m && m.group === "target" && m.mean < 0 ? "lo" : "hi"; }

  /* the crisis table: one row per named window, one cell per measure; `share` is the cell against the measure's own record */
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
    });
    return { cols: cols, rows: rows,
             now: cols.map(function (m) { return cell(m, [m.value, m.date]); }),
             record: cols.map(function (m) { return cell(m, side(m) === "lo" ? m.low : m.high); }) };
  }

  /* which named window a date falls in, if any */
  function windowOf(episodes, iso) {
    for (var i = 0; i < (episodes || []).length; i++) if (episodes[i].from <= iso && iso <= episodes[i].to) return episodes[i];
    return null;
  }

  /* the part of a history a range button asks for: {d, v} from `years` before the last date (0 = all) */
  function visible(series, years) {
    if (!series || !series.d || !series.d.length) return { d: [], v: [] };
    if (!years) return { d: series.d.slice(), v: series.v.slice() };
    var last = series.d[series.d.length - 1], from = (+last.slice(0, 4) - years) + last.slice(4), i = 0;
    while (i < series.d.length && series.d[i] < from) i++;
    return { d: series.d.slice(i), v: series.v.slice(i) };
  }

  /* the named windows that overlap a span, as fractions of its width */
  function bands(episodes, from, to) {
    var a = dayNum(from), span = dayNum(to) - a || 1;
    return (episodes || []).filter(function (ep) { return ep.to >= from && ep.from <= to; }).map(function (ep) {
      return { ep: ep, x0: Math.max(0, (dayNum(ep.from) - a) / span), x1: Math.min(1, (dayNum(ep.to) - a) / span) };
    });
  }

  function ecbPage(m) {
    if (!m || !m.key) return null;
    return "https://data.ecb.europa.eu/data/datasets/" + m.key.split(".")[0] + "/" + m.key;
  }
  function dataPath(key) { return typeof key === "string" && /^data\/[A-Za-z0-9_.\/-]+$/.test(key) && key.indexOf("..") < 0 ? "/" + key : null; }

  /* the world table: reported fields only, widest CDS first */
  function world(doc) {
    return ((doc || {}).countries || []).map(function (c) {
      return { country: c.country, region: c.region, y: c.yield_10y_pct, spread: c.spread_vs_bund_bp, cds: c.cds_bp, rating: c.rating, policy: c.cb_rate_pct, asOf: c.as_of };
    }).sort(function (a, b) { return (isNum(b.cds) ? b.cds : -1) - (isNum(a.cds) ? a.cds : -1) || (a.country < b.country ? -1 : 1); });
  }

  var api = { fmt: fmt, fmtChange: fmtChange, fmtDate: fmtDate, when: when, age: age, worseUp: worseUp, heat: heat, standing: standing, record: record, sparkPath: sparkPath,
              board: board, commonWhen: commonWhen, read: read, crisis: crisis, side: side, windowOf: windowOf, visible: visible, bands: bands, ecbPage: ecbPage, dataPath: dataPath,
              world: world, SETS: SETS, RANGES: RANGES, ALLOW: ALLOW };
  root.JHSovereignDesk = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;

  /* ------------------------------------------------------------------------------------------------ the page */
  var doc = root.document;
  if (!doc || !doc.getElementById("sov-main")) return;
  var $ = function (id) { return doc.getElementById(id); };
  var D = null, H = null, W = null, view = "desk", setId = "sovciss", chart = { id: null, years: 0 };
  var today = new Date().toISOString().slice(0, 10);

  function getJson(path) {
    return fetch(path, { cache: "no-store" }).then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); });
  }
  function cls(m, v) { var w = worseUp(m); return w === null || !isNum(v) || v === 0 ? "" : (v > 0) === w ? "up" : "dn"; }
  function oldNote(m) { var a = age(m, today); return a.old ? " <span class=old>" + a.days + " days old</span>" : ""; }
  function spark(m, w, h) { var p = sparkPath(m.spark, w, h); return p ? "<svg class=sp viewBox='0 0 " + w + " " + h + "' width=" + w + " height=" + h + " aria-hidden=true><path d='" + p + "'/></svg>" : ""; }
  function chg(m, k) { var v = m.change[k]; return "<span class='" + cls(m, v) + "'>" + k + " " + fmtChange(m.unit, v) + "</span>"; }

  function tile(m) {
    var bar = worseUp(m) === true ? "<div class=tp title='" + esc(standing(m)) + "'><i style='width:" + Math.max(1, m.pctile).toFixed(1) + "%'></i></div>" : "";
    return "<div class='tile h" + heat(m) + "' data-id='" + esc(m.id) + "' tabindex=0 role=button>"
      + "<div class=tl>" + esc(m.label) + "</div>"
      + "<div class=tv>" + fmt(m.unit, m.value) + " <span class=td>" + when(m) + oldNote(m) + "</span></div>"
      + "<div class=tc>" + (m.freq === "D" || m.freq === "W" ? chg(m, "1w") + " " : "") + (m.freq !== "Q" ? chg(m, "1m") + " " : chg(m, "3m") + " ") + chg(m, "12m") + "</div>"
      + bar + "<div class=tn>" + esc(standing(m)) + (m.high ? " · " + esc(record(m)) : "") + "</div>" + spark(m, 220, 30) + "<div class=ts>" + SPAN[m.freq] + "</div></div>";
  }
  function cell(m, text, extra) {
    if (!m) return "<td class=nil>—</td>";
    return "<td class='c " + (extra || "") + "' data-id='" + esc(m.id) + "' title='" + esc(m.label + " · " + when(m)) + "'>" + text + "</td>";
  }
  function value(m, common) { return m ? fmt(m.unit, m.value) + (common && when(m) !== common ? " <span class=old>" + when(m) + "</span>" : "") + oldNote(m) : ""; }
  function pctBar(m) { return "<span class=pb title='" + esc(standing(m)) + "'><i class='h" + heat(m) + "' style='width:" + Math.max(2, m.pctile).toFixed(0) + "%'></i></span><span class=pn>" + Math.round(m.pctile) + "</span>"; }

  function renderDesk() {
    var M = D.measures, b = board(D), html = "";
    html += "<div class=read>" + read(D).map(function (s) { return "<p>" + esc(s) + "</p>"; }).join("") + "</div>";
    html += "<div class=sec>Euro area</div><div class=tiles>" + D.euro_area.map(function (id) { return tile(M[id]); }).join("") + "</div>";

    var cw = { y: commonWhen(b.sovereign, "yield"), t: commonWhen(b.sovereign, "target"), d: commonWhen(b.sovereign, "debt"), c: commonWhen(b.sovereign, "clifs") };
    var first = b.sovereign[0] && b.sovereign[0].m.sovciss;
    html += "<div class=sec>Governments with a SovCISS" + (first ? " · " + when(first) : "") + " · click any number for its history</div><div class=scroll><table class=board><thead><tr>"
      + "<th class=l>Country</th><th>SovCISS</th><th>vs Germany</th><th>1w</th><th>1m</th><th>12m</th><th class=l>Own history</th><th>2011–12 peak</th>"
      + "<th>10y yield<small>" + esc(cw.y || "") + "</small></th><th>Spread to DE</th><th>TARGET<small>" + esc(cw.t || "") + "</small></th><th>Debt / GDP<small>" + esc(cw.d || "") + "</small></th>"
      + "<th>CLIFS<small>" + esc(cw.c || "") + "</small></th><th class=l>1 year</th></tr></thead><tbody>";
    b.sovereign.forEach(function (r) {
      var s = r.m.sovciss;
      html += "<tr><td class='l nm'>" + esc(r.country) + "</td>" + cell(s, "<b>" + fmt("index", s.value) + "</b>" + (when(s) !== when(first) ? " <span class=old>" + when(s) + "</span>" : ""), "h" + heat(s))
        + "<td>" + (r.vsGermany === null ? "—" : fmtChange("index", r.vsGermany)) + "</td>"
        + ["1w", "1m", "12m"].map(function (k) { return "<td class='" + cls(s, s.change[k]) + "'>" + fmtChange("index", s.change[k]) + "</td>"; }).join("")
        + "<td class=l>" + pctBar(s) + "</td><td>" + (r.crisis ? fmt("index", r.crisis[0]) + " <span class=old>" + MONTHS[+r.crisis[1].slice(5, 7) - 1] + " " + r.crisis[1].slice(0, 4) + "</span>" : "—") + "</td>"
        + cell(r.m.yield, value(r.m.yield, cw.y)) + cell(r.m.spread, value(r.m.spread, cw.y)) + cell(r.m.target, value(r.m.target, cw.t)) + cell(r.m.debt, value(r.m.debt, cw.d)) + cell(r.m.clifs, value(r.m.clifs, cw.c))
        + "<td class=l>" + spark(s, 110, 22) + "</td></tr>";
    });
    html += "</tbody></table></div>";

    var ow = { y: commonWhen(b.others, "yield"), t: commonWhen(b.others, "target"), d: commonWhen(b.others, "debt"), c: commonWhen(b.others, "clifs") };
    html += "<div class=sec>Other EU governments · the ECB publishes no SovCISS for them</div><div class=scroll><table class=board><thead><tr><th class=l>Country</th><th class=l>Currency</th>"
      + "<th>10y yield<small>" + esc(ow.y || "") + "</small></th><th>Spread to DE</th><th>12m</th><th>TARGET<small>" + esc(ow.t || "") + "</small></th><th>Debt / GDP<small>" + esc(ow.d || "") + "</small></th>"
      + "<th>CLIFS<small>" + esc(ow.c || "") + "</small></th></tr></thead><tbody>";
    b.others.forEach(function (r) {
      var sp = r.m.spread;
      html += "<tr><td class='l nm'>" + esc(r.country) + "</td><td class=l>" + (r.euro ? "euro" : "own") + "</td>" + cell(r.m.yield, value(r.m.yield, ow.y)) + cell(sp, value(sp, ow.y))
        + "<td class='" + (sp ? cls(sp, sp.change["12m"]) : "") + "'>" + (sp ? fmtChange("bp", sp.change["12m"]) : "—") + "</td>" + cell(r.m.target, value(r.m.target, ow.t)) + cell(r.m.debt, value(r.m.debt, ow.d))
        + cell(r.m.clifs, value(r.m.clifs, ow.c)) + "</tr>";
    });
    html += "</tbody></table></div>";

    html += "<div class=sec>Systemic stress by country · CISS, daily</div><div class=chips>" + b.systemic.map(function (r) {
      var m = r.m.ciss; return "<span class='chip h" + heat(m) + "' data-id='" + esc(m.id) + "' tabindex=0 role=button>" + esc(r.country) + " <b>" + fmt("index", m.value) + "</b>"
        + (m.blank_after ? " <span class=old title='the ECB&#39;s " + esc(m.blank_after) + " row is blank'>" + fmtDate(m.date) + "</span>" : "") + "</span>"; }).join("") + "</div>";
    var parts = D.contributions.map(function (id) { return M[id]; }), most = Math.max.apply(null, parts.map(function (m) { return Math.abs(m.value); })) || 1;
    html += "<div class=sec>What the euro-area CISS is made of" + (M.EA_CISS ? " · " + fmt("index", M.EA_CISS.value) + " on " + fmtDate(M.EA_CISS.date) : "") + "</div><div class=parts>" + parts.map(function (m) {
      return "<div class=part data-id='" + esc(m.id) + "' tabindex=0 role=button><span class=pl>" + esc(m.label.replace(" contribution to the euro-area CISS", "")) + "</span><span class=pbar><i class='" + (m.value < 0 ? "neg" : "") + "' style='width:"
        + (Math.abs(m.value) / most * 100).toFixed(0) + "%'></i></span><span class=pv>" + (m.value < 0 ? "−" : "+") + fixed(m.value, 4) + "</span></div>"; }).join("") + "</div>"
      + "<p class=note>Five market contributions plus the correlation term add up to the composite; the correlation term is negative when the five markets are not stressed together.</p>";
    $("sov-main").innerHTML = html;
  }

  function renderCrisis() {
    var set = SETS.filter(function (s) { return s.id === setId; })[0], c = crisis(D, set.ids), M = D.measures;
    var html = "<div class=ctrl>" + SETS.map(function (s) { return "<button class='gbtn" + (s.id === setId ? " on" : "") + "' data-set='" + s.id + "'>" + esc(s.name) + "</button>"; }).join("") + "</div>";
    html += "<p class=note>Each cell is the highest reading of that series inside the window's dates (for a central bank that owes on TARGET, the lowest). Shading is the cell against the series' own record. "
      + "Daily series are read from every daily observation, monthly ones from month-ends.</p>";
    var UNIT_WORD = { index: "index", bp: "bp", eur_bn: "€ bn", pct: "%", pct_gdp: "% GDP", usd: "$" };
    function head(m) { return "<th data-id='" + esc(m.id) + "' class=c title='" + esc(m.label) + "'>" + esc(SHORT[m.id] || (m.area && D.countries.filter(function (x) { return x.area === m.area; }).map(function (x) { return x.country; })[0]) || m.label)
      + "<small>" + UNIT_WORD[m.unit] + "</small></th>"; }
    function td(x, m) {
      if (!x) return "<td class=nil>—</td>";
      return "<td style='background:rgba(224,122,106," + (x.share * 0.55).toFixed(2) + ")' title='" + esc(fmtDate(x.d)) + "'>" + fmt(m.unit, x.v).replace(" bp", "").replace(" bn", "") + "</td>";
    }
    html += "<div class=scroll><table class='board crisis'><thead><tr><th class=l>Window</th>" + c.cols.map(head).join("") + "</tr></thead><tbody>";
    c.rows.forEach(function (r) {
      html += "<tr><td class='l nm'>" + esc(r.ep.name) + "<small>" + fmtDate(r.ep.from) + " – " + fmtDate(r.ep.to) + "</small></td>" + r.cells.map(function (x, i) { return td(x, c.cols[i]); }).join("") + "</tr>";
    });
    html += "<tr class=nowrow><td class='l nm'>Now<small>each at its latest date</small></td>" + c.now.map(function (x, i) { return td(x, c.cols[i]); }).join("") + "</tr>";
    html += "<tr class=recrow><td class='l nm'>Record<small>whole history</small></td>" + c.record.map(function (x, i) { return td(x, c.cols[i]); }).join("") + "</tr></tbody></table></div>";
    var lead = c.cols[0];
    if (lead) {
      html += "<div class=sec>" + esc(lead.label) + " · its highest readings by the data itself, at least 120 days apart</div><div class=chips>" + lead.tops.map(function (t) {
        var ep = windowOf(D.episodes, t[1]); return "<span class=chip><b>" + fmt(lead.unit, t[0]) + "</b> " + fmtDate(t[1]) + (ep ? " · " + esc(ep.name) : "") + "</span>"; }).join("") + "</div>";
    }
    $("sov-main").innerHTML = html;
  }

  function renderWorld() {
    if (!W) { $("sov-main").innerHTML = "<p class=note>Loading the world table…</p>"; return; }
    if (W.error) { $("sov-main").innerHTML = "<p class=note>The world table is not available (" + esc(W.error) + ").</p>"; return; }
    var rows = world(W);
    var html = "<p class=note>Outside the ECB's reach: 10-year yields, default-swap spreads, ratings and policy rates as reported by World Government Bonds, " + esc(String(W.generated_at || "").slice(0, 10))
      + ". Reported figures only; a missing figure is left blank. <a href='/global-sovereign.html' target=_blank rel=noopener>Open the full page</a>.</p>";
    html += "<div class=scroll><table class=board><thead><tr><th class=l>Country</th><th class=l>Region</th><th>10y yield</th><th>vs Bund</th><th>CDS</th><th class=l>Rating</th><th>Policy rate</th><th class=l>As of</th></tr></thead><tbody>";
    rows.forEach(function (r) {
      html += "<tr><td class='l nm'>" + esc(r.country) + "</td><td class=l>" + esc(r.region || "") + "</td><td>" + (isNum(r.y) ? fixed(r.y, 2) + "%" : "") + "</td><td>" + (isNum(r.spread) ? fmtChange("bp", r.spread) : "") + "</td><td><b>"
        + (isNum(r.cds) ? fixed(r.cds, 1) + " bp" : "") + "</b></td><td class=l>" + esc(r.rating || "") + "</td><td>" + (isNum(r.policy) ? fixed(r.policy, 2) + "%" : "") + "</td><td class=l>" + esc(r.asOf || "") + "</td></tr>";
    });
    $("sov-main").innerHTML = html + "</tbody></table></div>";
  }

  function link(key, text) { var p = dataPath(key); return p ? "<a href='" + esc(p) + "' target=_blank rel=noopener>" + esc(text) + "</a>" : ""; }
  function renderSources() {
    var html = "<p class=note>Every number on this desk is read from an ECB response that is kept as it arrived. The run receipt lists them; compiling twice from two separate reads has to give the same document before it is published.</p>";
    html += "<div class=scroll><table class=board><thead><tr><th class=l>ECB response</th><th>Series</th><th>Rows</th><th class=l>Newest period</th><th class=l>Received</th><th class=l>Original</th></tr></thead><tbody>";
    D.sources.forEach(function (s) {
      html += "<tr><td class='l nm'>" + esc(s.label) + (s.path ? "<small>" + esc(s.path) + "</small>" : "") + "</td><td>" + (s.series || "") + "</td><td>" + (s.rows ? s.rows.toLocaleString("en-US") : "") + "</td><td class=l>" + esc(s.newest || "")
        + "</td><td class=l>" + esc(String(s.acquired_at || (s.warehouse || {}).generated_at || "").slice(0, 16).replace("T", " ")) + (s.carried ? " <span class=old>kept from an earlier run</span>" : "") + "</td><td class=l>"
        + (s.evidence ? link(s.evidence.key, "response (.gz)") : s.warehouse ? link(s.warehouse.manifest_key, "warehouse receipt") + " · <a href='/ciss.html' target=_blank rel=noopener>CISS register</a>" : "") + "</td></tr>";
    });
    html += "</tbody></table></div><p class=note>" + link(D.replay.manifest_key, "Run receipt") + " · " + link(D.replay.compiler.key, "the compiler that produced it") + " · " + link("data/ciss-sovereign.json", "this document")
      + " · " + link(D.history.key, "histories") + "</p>";
    var errs = Object.keys(D.errors || {});
    html += "<div class=sec>Not available in this run</div><p class=note>" + (errs.length ? errs.map(function (k) { return esc(k + ": " + D.errors[k]); }).join("<br>") : "Nothing: every response arrived and compiled.") + "</p>";
    if ((D.ended_at_source || []).length) html += "<div class=sec>Ended at the source, not shown</div><p class=note>" + D.ended_at_source.map(function (e) { return esc((e.key || e.id) + " — last " + e.last); }).join("<br>") + "</p>";
    html += "<div class=sec>How to read a number</div><p class=note>" + Object.keys(D.definitions).map(function (k) { return "<b>" + esc(k) + "</b>: " + esc(D.definitions[k]); }).join("<br>") + "</p>";
    $("sov-main").innerHTML = html;
  }

  function render() {
    doc.querySelectorAll("#sov-tabs .tab").forEach(function (t) { t.classList.toggle("on", t.getAttribute("data-view") === view); });
    if (!D) return;
    if (view === "desk") renderDesk(); else if (view === "crisis") renderCrisis(); else if (view === "world") renderWorld(); else renderSources();
  }

  /* ---- the chart ---- */
  function draw() {
    var m = D.measures[chart.id], cv = $("sov-cv"), s = H && H.series[chart.id];
    var box = cv.getBoundingClientRect(), ratio = root.devicePixelRatio || 1, wpx = Math.max(300, box.width), hpx = Math.max(220, box.height);
    cv.width = wpx * ratio; cv.height = hpx * ratio;
    var g = cv.getContext("2d"); g.setTransform(ratio, 0, 0, ratio, 0, 0); g.clearRect(0, 0, wpx, hpx);
    g.font = "11px -apple-system,Segoe UI,Roboto,sans-serif";
    if (!s) { g.fillStyle = "#8b949e"; g.fillText(H ? "No history for this measure." : "Loading history…", 16, 28); chart.geo = null; return; }
    var vis = visible(s, chart.years), pts = [];
    for (var i = 0; i < vis.d.length; i++) if (isNum(vis.v[i])) pts.push([dayNum(vis.d[i]), vis.v[i], vis.d[i]]);
    if (pts.length < 2) { g.fillStyle = "#8b949e"; g.fillText("Not enough history in this range.", 16, 28); chart.geo = null; return; }
    var L = 58, R = 12, T = 18, B = 24, x0 = pts[0][0], x1 = pts[pts.length - 1][0], lo = Infinity, hi = -Infinity;
    pts.forEach(function (p) { if (p[1] < lo) lo = p[1]; if (p[1] > hi) hi = p[1]; });
    if (lo > 0 && lo / (hi || 1) < 0.35 && m.unit !== "eur_bn") lo = 0;
    var pad = (hi - lo || Math.abs(hi) || 1) * 0.06; hi += pad; if (lo !== 0) lo -= pad;
    var X = function (d) { return L + (d - x0) / (x1 - x0 || 1) * (wpx - L - R); }, Y = function (v) { return T + (hi - v) / (hi - lo || 1) * (hpx - T - B); };
    bands(D.episodes, pts[0][2], pts[pts.length - 1][2]).forEach(function (b) {
      var bx = L + b.x0 * (wpx - L - R), bw = Math.max(2, (b.x1 - b.x0) * (wpx - L - R));
      g.fillStyle = "rgba(240,180,41,.13)"; g.fillRect(bx, T, bw, hpx - T - B);
    });
    g.strokeStyle = "#1e242c"; g.fillStyle = "#8b949e"; g.lineWidth = 1; g.textAlign = "right";
    for (var k = 0; k <= 4; k++) { var v = lo + (hi - lo) * k / 4, y = Y(v); g.beginPath(); g.moveTo(L, y); g.lineTo(wpx - R, y); g.stroke(); g.fillText(fmt(m.unit, v).replace(" bp", "").replace(" bn", ""), L - 6, y + 4); }
    if (lo < 0 && hi > 0) { g.strokeStyle = "#3a4452"; g.beginPath(); g.moveTo(L, Y(0)); g.lineTo(wpx - R, Y(0)); g.stroke(); }
    g.textAlign = "center";
    var y0 = +pts[0][2].slice(0, 4), y1 = +pts[pts.length - 1][2].slice(0, 4), step = Math.max(1, Math.ceil((y1 - y0 + 1) / Math.max(2, Math.floor((wpx - L - R) / 60))));
    for (var yr = y0 + 1; yr <= y1; yr += step) { var xx = X(dayNum(yr + "-01-01")); if (xx > L + 14 && xx < wpx - R - 14) g.fillText(String(yr), xx, hpx - 7); }
    if (y1 === y0) { g.fillText(fmtDate(pts[0][2]), L + 34, hpx - 7); g.fillText(fmtDate(pts[pts.length - 1][2]), wpx - R - 34, hpx - 7); }
    g.strokeStyle = "#F0B429"; g.lineWidth = 1.4; g.beginPath();
    pts.forEach(function (p, i) { if (i) g.lineTo(X(p[0]), Y(p[1])); else g.moveTo(X(p[0]), Y(p[1])); });
    g.stroke();
    var last = pts[pts.length - 1]; g.fillStyle = "#F0B429"; g.beginPath(); g.arc(X(last[0]), Y(last[1]), 3, 0, 6.3); g.fill();
    chart.geo = { pts: pts, X: X, Y: Y, L: L, R: R, T: T, B: B, w: wpx, h: hpx };
  }
  function hover(ev) {
    var geo = chart.geo, tip = $("sov-tip"); if (!geo) { tip.style.display = "none"; return; }
    var box = $("sov-cv").getBoundingClientRect(), px = (ev.touches ? ev.touches[0].clientX : ev.clientX) - box.left, best = null, bd = 1e9;
    geo.pts.forEach(function (p) { var d = Math.abs(geo.X(p[0]) - px); if (d < bd) { bd = d; best = p; } });
    if (!best) return;
    var m = D.measures[chart.id], ep = windowOf(D.episodes, best[2]);
    tip.style.display = "block"; tip.style.left = Math.min(geo.w - 170, Math.max(4, geo.X(best[0]) + 10)) + "px"; tip.style.top = Math.max(4, geo.Y(best[1]) - 34) + "px";
    tip.innerHTML = "<b>" + fmt(m.unit, best[1]) + "</b> " + fmtDate(best[2]) + (ep ? "<br>" + esc(ep.name) : "");
  }
  function openChart(id) {
    var m = D.measures[id]; if (!m) return;
    chart.id = id; chart.years = 0;
    $("sov-ct").textContent = m.label;
    var links = [];
    if (m.key && ecbPage(m)) links.push("<a href='" + esc(ecbPage(m)) + "' target=_blank rel=noopener>the series at the ECB</a>");
    if (m.evidence) links.push(link(m.evidence.key, "the response it was read from (.gz)"));
    if (m.key && /^(CISS|CLIFS)\./.test(m.key)) links.push("<a href='/ciss.html?series=" + encodeURIComponent(m.key) + "' target=_blank rel=noopener>in the CISS register</a>");
    $("sov-cm").innerHTML = "<b>" + fmt(m.unit, m.value) + "</b> · " + when(m) + oldNote(m) + (m.blank_after ? " · the ECB's " + esc(m.blank_after) + " row is blank" : "") + " · " + esc(standing(m)) + "<br>"
      + (m.key ? "<code>" + esc(m.key) + "</code>" : esc(m.derived || "")) + (m.exact && m.key ? " · source value " + esc(m.exact) : "") + "<br>" + links.join(" · ");
    var peaks = (D.episodes || []).map(function (ep) { var p = (m.peaks || {})[ep.id]; return p ? [ep, p[side(m)]] : null; }).filter(Boolean);
    $("sov-cs").innerHTML = "<div class=stat>History from " + fmtDate(m.start) + " · " + m.n.toLocaleString("en-US") + " observations · high " + fmt(m.unit, m.high[0]) + " on " + fmtDate(m.high[1]) + " · low " + fmt(m.unit, m.low[0]) + " on "
      + fmtDate(m.low[1]) + " · mean " + fmt(m.unit, m.mean) + (m.freq === "D" ? " · the line is the last reading of each week; highs and lows are from every day" : "") + "</div>"
      + (peaks.length ? "<div class=sec>" + (side(m) === "lo" ? "Lowest" : "Highest") + " reading inside each named window (shaded on the chart)</div><div class=chips>" + peaks.map(function (p) {
        return "<span class=chip>" + esc(p[0].name) + " <b>" + fmt(m.unit, p[1][0]) + "</b> " + fmtDate(p[1][1]) + "</span>"; }).join("") + "</div>" : "");
    $("sov-rg").innerHTML = RANGES.map(function (r) { return "<button class='gbtn" + (r[1] === chart.years ? " on" : "") + "' data-years='" + r[1] + "'>" + r[0] + "</button>"; }).join("");
    $("sov-ovl").classList.add("on");
    if (!H) getJson("/data/ciss-sovereign-history.json").then(function (h) { H = h; if (chart.id) draw(); }).catch(function (e) { H = { series: {} }; draw(); });
    draw();
  }
  function closeChart() { $("sov-ovl").classList.remove("on"); chart.id = null; $("sov-tip").style.display = "none"; }

  doc.addEventListener("click", function (ev) {
    var t = ev.target, hit;
    if ((hit = t.closest && t.closest("#sov-tabs .tab"))) { view = hit.getAttribute("data-view"); if (view === "world" && !W) getJson("/data/global-sovereign.json").then(function (w) { W = w; render(); }).catch(function (e) { W = { error: e.message }; render(); }); render(); return; }
    if ((hit = t.closest && t.closest("[data-set]"))) { setId = hit.getAttribute("data-set"); render(); return; }
    if ((hit = t.closest && t.closest("[data-years]"))) { chart.years = +hit.getAttribute("data-years"); $("sov-rg").querySelectorAll(".gbtn").forEach(function (b) { b.classList.toggle("on", +b.getAttribute("data-years") === chart.years); }); draw(); return; }
    if (t.id === "sov-x" || t.id === "sov-ovl") { closeChart(); return; }
    if ((hit = t.closest && t.closest("#sov-main [data-id]"))) openChart(hit.getAttribute("data-id"));
  });
  doc.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape") closeChart();
    if ((ev.key === "Enter" || ev.key === " ") && ev.target.getAttribute && ev.target.getAttribute("data-id") && ev.target.closest("#sov-main")) { ev.preventDefault(); openChart(ev.target.getAttribute("data-id")); }
  });
  $("sov-cv").addEventListener("mousemove", hover);
  $("sov-cv").addEventListener("touchstart", hover, { passive: true });
  $("sov-cv").addEventListener("mouseleave", function () { $("sov-tip").style.display = "none"; });
  root.addEventListener("resize", function () { if (chart.id) draw(); });

  getJson("/data/ciss-sovereign.json").then(function (d) {
    if (d.contract !== "ciss-sovereign.v1" || !d.measures) throw new Error("unexpected document");
    D = d;
    var ea = d.measures.EA_SOV_GDP, n = Object.keys(d.measures).length;
    $("sov-asof").innerHTML = "ECB data" + (ea ? " to " + fmtDate(ea.date) : "") + " · compiled " + esc(d.generated_at.slice(0, 16).replace("T", " ")) + " UTC · " + n + " measures, " + d.countries.length + " countries"
      + (Object.keys(d.errors || {}).length ? " · <span class=old>" + Object.keys(d.errors).length + " source(s) not available in this run — see Sources</span>" : "");
    render();
    var want = new URLSearchParams(root.location.search).get("m");
    if (want && d.measures[want]) openChart(want);
  }).catch(function (e) {
    $("sov-asof").textContent = "The sovereign-stress document could not be read: " + e.message;
    $("sov-main").innerHTML = "<p class=note>Nothing is shown rather than something old. The CISS register still opens: <a href='/ciss.html' target=_blank rel=noopener>ciss.html</a>.</p>";
  });
})(typeof window !== "undefined" ? window : globalThis);
