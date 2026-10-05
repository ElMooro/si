/* JustHodl chart · turn any CSV or JSON file into a chart (2026-10-05).
 * Open with the "File" button next to Data, Ctrl+Shift+O, or drop a .csv/.tsv/.txt/.json/.ndjson file anywhere on the chart.
 * Also accepts pasted text and a URL. Detection is automatic and shown before anything is charted:
 *   CSV/TSV/semicolon/pipe (quoted fields, BOM, leading metadata lines, decimal commas, thousands separators,
 *   %, currency, (negatives), NA/./#N/A as missing); JSON arrays of objects/arrays, nested result arrays (FRED, Yahoo,
 *   Alpha Vantage, CoinGecko, warehouse packets), parallel arrays, {date: value} maps and NDJSON.
 *   Dates: ISO dates/times, YYYY-MM, YYYYMM(DD), YYYY, quarters (2020Q1, Q1 2020), 2020M01, month names, MM/DD/YYYY or
 *   DD/MM/YYYY (decided from the data, switchable), Unix seconds/milliseconds, Excel serials (date-named columns).
 * Every source row is kept as evidence: rows with an unreadable date or value are counted and shown, never guessed.
 * Single-value series chart through the same observation contract as warehouse data (line, candles, MoM/QoQ/YoY…);
 * files with Open/High/Low/Close columns chart as real candles. Imports are stored in this browser (IndexedDB) and are
 * searchable by name ("Your files"). */
(function (root) {
  "use strict";
  if (root.JHChartImport) return;
  var doc = root.document, DB = "jh-files", STORE = "series", IDX_KEY = "jh-files-index";
  var MONTHS = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, sept: 9, oct: 10, nov: 11, dec: 12,
    january: 1, february: 2, march: 3, april: 4, june: 6, july: 7, august: 8, september: 9, october: 10, november: 11, december: 12 };
  var MISSING = /^(|\.|-|—|–|na|n\/a|nan|null|none|nil|#n\/a|#value!|#div\/0!|\.\.|x|:|missing)$/i;

  // ------------------------------------------------------------------ parsing: CSV
  function detectDelim(lines) {
    var cands = [",", ";", "\t", "|"], best = ",", bestScore = -1;
    cands.forEach(function (d) {
      var counts = lines.slice(0, 40).map(function (l) { return splitLine(l, d).length; }).filter(function (n) { return n > 1; });
      if (!counts.length) return;
      var freq = {}; counts.forEach(function (n) { freq[n] = (freq[n] || 0) + 1; });
      var mode = 0, mc = 0; Object.keys(freq).forEach(function (k) { if (freq[k] > mc) { mc = freq[k]; mode = +k; } });
      var score = mc * 10 + mode;
      if (score > bestScore) { bestScore = score; best = d; }
    });
    return best;
  }
  function splitLine(line, d) {
    var out = [], cur = "", q = false;
    for (var i = 0; i < line.length; i++) {
      var ch = line[i];
      if (q) { if (ch === '"') { if (line[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += ch; }
      else if (ch === '"' && cur.trim() === "") { q = true; cur = ""; }
      else if (ch === d) { out.push(cur); cur = ""; }
      else cur += ch;
    }
    out.push(cur);
    return out.map(function (s) { return s.trim(); });
  }
  // RFC-4180 record split that honours newlines inside quotes
  function records(text) {
    var out = [], cur = "", q = false;
    for (var i = 0; i < text.length; i++) {
      var ch = text[i];
      if (ch === '"') q = !q;
      if ((ch === "\n" || ch === "\r") && !q) { if (ch === "\r" && text[i + 1] === "\n") i++; out.push(cur); cur = ""; continue; }
      cur += ch;
    }
    if (cur.length) out.push(cur);
    return out;
  }
  function parseCSV(text) {
    text = text.replace(/^\uFEFF/, "");
    var lines = records(text).filter(function (l) { return l.trim() !== ""; });
    if (!lines.length) throw new Error("The file is empty");
    var d = detectDelim(lines), rows = lines.map(function (l) { return splitLine(l, d); });
    var counts = {}; rows.forEach(function (r) { counts[r.length] = (counts[r.length] || 0) + 1; });
    var width = +Object.keys(counts).sort(function (a, b) { return counts[b] - counts[a] || b - a; })[0];
    // skip leading metadata/comment lines (fewer fields, or starting with #) until the table starts
    var start = 0;
    while (start < rows.length - 1 && (rows[start].length < width || /^#/.test(rows[start][0] || ""))) start++;
    var head = rows[start], body = rows.slice(start + 1);
    var headerLike = head.some(function (c) { return c && num(c, false) === null && !parseDate(c); }) || head.every(function (c) { return c && num(c, false) === null; });
    var columns;
    if (headerLike) columns = head.map(function (c, i) { return c || "col" + (i + 1); });
    else { columns = head.map(function (_, i) { return "col" + (i + 1); }); body = rows.slice(start); }
    var skippedMeta = start;
    body = body.filter(function (r) { return r.some(function (c) { return c !== ""; }); });
    return { columns: dedupeNames(columns), rows: body.map(function (r) { return columns.map(function (_, i) { return r[i] === undefined ? "" : r[i]; }); }), format: "CSV (" + ({ ",": "comma", ";": "semicolon", "\t": "tab", "|": "pipe" }[d]) + " separated)", skipped_meta: skippedMeta, delimiter: d };
  }
  function dedupeNames(cols) {
    var seen = {};
    return cols.map(function (c) { var k = c, n = 2; while (seen[k]) k = c + " (" + (n++) + ")"; seen[k] = 1; return k; });
  }

  // ------------------------------------------------------------------ parsing: JSON
  function isPrim(v) { return v === null || /^(string|number|boolean)$/.test(typeof v); }
  function flatten(o, pre, out, depth) {
    out = out || {}; pre = pre || ""; depth = depth || 0;
    Object.keys(o).forEach(function (k) {
      var v = o[k], key = pre ? pre + "." + k : k;
      if (isPrim(v)) out[key] = v;
      else if (v && typeof v === "object" && !Array.isArray(v) && depth < 2) flatten(v, key, out, depth + 1);
    });
    return out;
  }
  function tableFromObjects(arr) {
    var cols = [], seen = {}, rows = arr.map(function (o) { return flatten(o); });
    rows.slice(0, 2000).forEach(function (r) { Object.keys(r).forEach(function (k) { if (!seen[k]) { seen[k] = 1; cols.push(k); } }); });
    return { columns: cols, rows: rows.map(function (r) { return cols.map(function (c) { return r[c] === undefined || r[c] === null ? "" : String(r[c]); }); }) };
  }
  function tableFromArrays(arr) {
    var first = arr[0], header = first.every(function (c) { return typeof c === "string" && num(c, false) === null && !parseDate(c); });
    var w = Math.max.apply(null, arr.slice(0, 200).map(function (r) { return r.length; }));
    var cols = header ? first.map(String) : Array.from({ length: w }, function (_, i) { return i === 0 ? "date" : "value" + (w > 2 ? i : ""); });
    var body = header ? arr.slice(1) : arr;
    return { columns: dedupeNames(cols), rows: body.map(function (r) { return cols.map(function (_, i) { return r[i] === undefined || r[i] === null ? "" : String(r[i]); }); }) };
  }
  // find the best table anywhere inside a JSON document
  function findTables(node, path, out, depth) {
    if (depth > 6 || !node || typeof node !== "object") return;
    if (Array.isArray(node)) {
      if (node.length >= 1) {
        var objs = node.filter(function (x) { return x && typeof x === "object" && !Array.isArray(x); }).length;
        var arrs = node.filter(Array.isArray).length;
        if (objs >= node.length * 0.9) out.push({ path: path, kind: "objects", n: node.length, node: node });
        else if (arrs >= node.length * 0.9 && node.every(function (r) { return !Array.isArray(r) || r.every(isPrim); })) out.push({ path: path, kind: "arrays", n: node.length, node: node });
      }
      if (node.length && typeof node[0] === "object") findTables(node[0], path + "[0]", out, depth + 1);
      return;
    }
    var keys = Object.keys(node);
    // {date: value} or {date: {...}} maps
    var dateKeys = keys.filter(function (k) { return parseDate(k); });
    if (keys.length >= 3 && dateKeys.length >= keys.length * 0.9) {
      var v0 = node[dateKeys[0]];
      if (isPrim(v0)) out.push({ path: path, kind: "map", n: dateKeys.length, node: node });
      else if (v0 && typeof v0 === "object" && !Array.isArray(v0)) out.push({ path: path, kind: "mapobj", n: dateKeys.length, node: node });
    }
    // parallel primitive arrays of equal length (Yahoo chart, {dates:[], values:[]})
    var par = {}; collectArrays(node, path, par, 0);
    Object.keys(par).forEach(function (len) { var g = par[len]; if (g.length >= 2 && +len >= 2) out.push({ path: path, kind: "parallel", n: +len, cols: g }); });
    keys.forEach(function (k) { var v = node[k]; if (v && typeof v === "object") findTables(v, path ? path + "." + k : k, out, depth + 1); });
  }
  function collectArrays(node, path, acc, depth) {
    if (depth > 5 || !node || typeof node !== "object") return;
    Object.keys(node).forEach(function (k) {
      var v = node[k], p = path ? path + "." + k : k;
      if (Array.isArray(v) && v.length && v.every(isPrim)) (acc[v.length] = acc[v.length] || []).push({ name: k, path: p, values: v });
      else if (Array.isArray(v) && v.length === 1 && v[0] && typeof v[0] === "object") collectArrays(v[0], p + "[0]", acc, depth + 1);
      else if (v && typeof v === "object" && !Array.isArray(v)) collectArrays(v, p, acc, depth + 1);
    });
  }
  function parseJSON(text) {
    var docj;
    try { docj = JSON.parse(text.replace(/^\uFEFF/, "")); }
    catch (e) {
      var lines = text.split(/\r?\n/).filter(function (l) { return l.trim(); });
      try { docj = lines.map(function (l) { return JSON.parse(l); }); } catch (e2) { throw new Error("Not valid JSON: " + e.message); }
    }
    var found = []; findTables(docj, "", found, 0);
    if (!found.length) throw new Error("No table-like data found in the JSON document");
    // prefer tables that have a date-like column; then the largest
    found.forEach(function (f) { f.t = toTable(f); f.dateCol = f.t ? detect(f.t).dateCol : -1; });
    found = found.filter(function (f) { return f.t && f.t.rows.length; });
    if (!found.length) throw new Error("No rows found in the JSON document");
    found.sort(function (a, b) { return ((b.dateCol >= 0) - (a.dateCol >= 0)) || (b.t.rows.length - a.t.rows.length); });
    var best = found[0];
    best.t.format = "JSON · " + (best.path || "document root") + " · " + ({ objects: "array of objects", arrays: "array of rows", map: "date → value map", mapobj: "date → record map", parallel: "parallel arrays" }[best.kind]);
    return best.t;
  }
  function toTable(f) {
    if (f.kind === "objects") return tableFromObjects(f.node);
    if (f.kind === "arrays") return tableFromArrays(f.node);
    if (f.kind === "map") return { columns: ["date", "value"], rows: Object.keys(f.node).filter(function (k) { return parseDate(k); }).map(function (k) { var v = f.node[k]; return [k, v === null ? "" : String(v)]; }) };
    if (f.kind === "mapobj") { var ks = Object.keys(f.node).filter(function (k) { return parseDate(k); }); var t = tableFromObjects(ks.map(function (k) { return f.node[k]; })); t.columns = ["date"].concat(t.columns); t.rows = t.rows.map(function (r, i) { return [ks[i]].concat(r); }); return t; }
    if (f.kind === "parallel") {
      var cols = dedupeNames(f.cols.map(function (c) { return c.name; }));
      return { columns: cols, rows: f.cols[0].values.map(function (_, i) { return f.cols.map(function (c) { var v = c.values[i]; return v === null || v === undefined ? "" : String(v); }); }) };
    }
    return null;
  }

  // ------------------------------------------------------------------ values and dates
  function num(s, decimalComma) {
    if (typeof s === "number") return isFinite(s) ? s : null;
    s = String(s == null ? "" : s).trim();
    if (MISSING.test(s)) return null;
    var neg = false;
    if (/^\(.*\)$/.test(s)) { neg = true; s = s.slice(1, -1); }
    s = s.replace(/[\s\u00a0'’]/g, "").replace(/^[$€£¥₹₩₽¢]|[$€£¥₹₩₽¢]$/g, "").replace(/%$/, "").replace(/^\+/, "");
    if (/^-?[A-Za-z]{3}\d/.test(s)) return null;
    if (decimalComma) s = s.replace(/\./g, "").replace(",", ".");
    else if (/^-?\d{1,3}(,\d{3})+(\.\d+)?$/.test(s)) s = s.replace(/,/g, "");
    else if (/^-?\d+,\d+$/.test(s)) return null; // ambiguous decimal comma unless the file uses it
    if (!/^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/.test(s)) return null;
    var v = Number(s); if (!isFinite(v)) return null;
    return neg ? -v : v;
  }
  function pad(n) { return (n < 10 ? "0" : "") + n; }
  function iso(y, m, d) {
    if (!(y >= 1000 && y <= 2300 && m >= 1 && m <= 12 && d >= 1 && d <= 31)) return null;
    var dt = new Date(Date.UTC(y, m - 1, d));
    if (dt.getUTCMonth() !== m - 1) return null;
    return y + "-" + pad(m) + "-" + pad(d);
  }
  /* returns {day:"YYYY-MM-DD", t: unix seconds, prec: "day"|"month"|"quarter"|"year"|"time"} or null.
     dmy: true → DD/MM/YYYY, false → MM/DD/YYYY, undefined → only unambiguous forms */
  function parseDate(s, dmy, allowNumeric) {
    if (s === null || s === undefined) return null;
    var raw = String(s).trim(), m;
    if (!raw) return null;
    if ((m = /^(\d{4})[-\/.](\d{1,2})[-\/.](\d{1,2})(?:[T ](\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?\s*(Z|[+-]\d{2}:?\d{2})?)?$/.exec(raw))) {
      var day = iso(+m[1], +m[2], +m[3]); if (!day) return null;
      if (m[4] !== undefined) {
        var tz = m[7] && m[7] !== "Z" ? m[7].replace(":", "") : "";
        var ms = Date.parse(day + "T" + pad(+m[4]) + ":" + m[5] + ":" + (m[6] || "00") + (m[7] ? (m[7] === "Z" ? "Z" : tz.slice(0, 3) + ":" + tz.slice(3)) : "Z"));
        if (!isFinite(ms)) return null;
        var hasTime = +m[4] || +m[5] || +(m[6] || 0);
        return { day: new Date(ms).toISOString().slice(0, 10), t: ms / 1000, prec: hasTime ? "time" : "day" };
      }
      return { day: day, t: Date.parse(day) / 1000, prec: "day" };
    }
    if ((m = /^(\d{4})[-\/.](\d{1,2})$/.exec(raw)) || (m = /^(\d{4})M(\d{1,2})$/i.exec(raw))) { var d2 = iso(+m[1], +m[2], 1); return d2 && { day: d2, t: Date.parse(d2) / 1000, prec: "month" }; }
    if ((m = /^(\d{4})\s*[-\s]?\s*Q([1-4])$/i.exec(raw)) || (m = /^Q([1-4])\s*[-\s]?\s*(\d{4})$/i.exec(raw))) {
      var y = m[1].length === 4 ? +m[1] : +m[2], q = m[1].length === 4 ? +m[2] : +m[1], d3 = iso(y, q * 3 - 2, 1);
      return d3 && { day: d3, t: Date.parse(d3) / 1000, prec: "quarter" };
    }
    if ((m = /^(\d{4})[-\s]?H([12])$/i.exec(raw))) { var d4 = iso(+m[1], m[2] === "1" ? 1 : 7, 1); return d4 && { day: d4, t: Date.parse(d4) / 1000, prec: "half" }; }
    if ((m = /^(\d{1,2})[-\/.](\d{1,2})[-\/.](\d{2}|\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?\s*([AaPp][Mm])?)?$/.exec(raw))) {
      var a = +m[1], b = +m[2], yy = +m[3]; if (m[3].length === 2) yy += yy < 50 ? 2000 : 1900;
      var mo, dd;
      if (a > 12 && b <= 12) { dd = a; mo = b; } else if (b > 12 && a <= 12) { mo = a; dd = b; }
      else if (dmy === true) { dd = a; mo = b; } else if (dmy === false) { mo = a; dd = b; } else { mo = a; dd = b; }
      var d5 = iso(yy, mo, dd); if (!d5) return null;
      var r5 = { day: d5, t: Date.parse(d5) / 1000, prec: "day", ambiguous: a <= 12 && b <= 12 && a !== b };
      if (m[4] !== undefined) { var hh = +m[4] % 12 + (m[7] && /p/i.test(m[7]) ? 12 : (!m[7] ? Math.floor(+m[4] / 12) * 12 : 0)); r5.t += hh * 3600 + (+m[5]) * 60 + (+(m[6] || 0)); r5.prec = (hh || +m[5]) ? "time" : "day"; }
      return r5;
    }
    // month names: Jan 2020, January 2020, 2020 Jan, Jan-20, 01-Jan-2020, Jan 5, 2020, 5 Jan 2020, 2020-Jan-05
    var low = raw.toLowerCase().replace(/,/g, " ").replace(/\s+/g, " ");
    if ((m = /^([a-z]{3,9})\.?[ \-\/](\d{4}|\d{2})$/.exec(low)) && MONTHS[m[1]]) { var y6 = +m[2]; if (m[2].length === 2) y6 += y6 < 50 ? 2000 : 1900; var d6 = iso(y6, MONTHS[m[1]], 1); return d6 && { day: d6, t: Date.parse(d6) / 1000, prec: "month" }; }
    if ((m = /^(\d{4})[ \-\/]([a-z]{3,9})\.?$/.exec(low)) && MONTHS[m[2]]) { var d7 = iso(+m[1], MONTHS[m[2]], 1); return d7 && { day: d7, t: Date.parse(d7) / 1000, prec: "month" }; }
    if ((m = /^(\d{1,2})[ \-\/]([a-z]{3,9})\.?[ \-\/](\d{4}|\d{2})$/.exec(low)) && MONTHS[m[2]]) { var y8 = +m[3]; if (m[3].length === 2) y8 += y8 < 50 ? 2000 : 1900; var d8 = iso(y8, MONTHS[m[2]], +m[1]); return d8 && { day: d8, t: Date.parse(d8) / 1000, prec: "day" }; }
    if ((m = /^([a-z]{3,9})\.? (\d{1,2}) (\d{4})$/.exec(low)) && MONTHS[m[1]]) { var d9 = iso(+m[3], MONTHS[m[1]], +m[2]); return d9 && { day: d9, t: Date.parse(d9) / 1000, prec: "day" }; }
    if ((m = /^(\d{4})[ \-]([a-z]{3,9})[ \-](\d{1,2})$/.exec(low)) && MONTHS[m[2]]) { var d10 = iso(+m[1], MONTHS[m[2]], +m[3]); return d10 && { day: d10, t: Date.parse(d10) / 1000, prec: "day" }; }
    if (/^\d{8}$/.test(raw)) { var d11 = iso(+raw.slice(0, 4), +raw.slice(4, 6), +raw.slice(6)); if (d11) return { day: d11, t: Date.parse(d11) / 1000, prec: "day" }; }
    if (allowNumeric) {
      if (/^\d{6}$/.test(raw)) { var d12 = iso(+raw.slice(0, 4), +raw.slice(4), 1); if (d12) return { day: d12, t: Date.parse(d12) / 1000, prec: "month" }; }
      if (/^\d{4}$/.test(raw) && +raw >= 1000 && +raw <= 2300) { var d13 = iso(+raw, 1, 1); return { day: d13, t: Date.parse(d13) / 1000, prec: "year" }; }
      var n = Number(raw);
      if (/^\d{9,10}(\.\d+)?$/.test(raw) && n > 1e8) { var ds = new Date(n * 1000); return { day: ds.toISOString().slice(0, 10), t: n, prec: n % 86400 ? "time" : "day" }; }
      if (/^\d{12,13}$/.test(raw)) { var dm = new Date(n); return { day: dm.toISOString().slice(0, 10), t: n / 1000, prec: (n / 1000) % 86400 ? "time" : "day" }; }
      if (/^\d{5}(\.\d+)?$/.test(raw) && n > 1 && n < 80000) { var de = new Date(Date.UTC(1899, 11, 30) + Math.floor(n) * 86400000); return { day: de.toISOString().slice(0, 10), t: de.getTime() / 1000, prec: "day", excel: true }; }
    }
    return null;
  }
  var DATE_NAME = /^(date|time|timestamp|datetime|period|day|month|year|observation_date|obs_date|time_period|dt|ts|as_of|asof|ref_date|refdate|fecha|datum|data|日期)$|date|time/i;

  // ------------------------------------------------------------------ detection
  function detect(t, opts) {
    opts = opts || {};
    var cols = t.columns, rows = t.rows, sample = rows.slice(0, 600), best = -1, bestScore = 0, dmy;
    // decimal comma: semicolon files with 1,23 style numbers
    var commaNums = 0, dotNums = 0;
    sample.forEach(function (r) { r.forEach(function (c) { if (/^-?\d+,\d+$/.test(c)) commaNums++; else if (/^-?\d+\.\d+$/.test(c)) dotNums++; }); });
    var decimalComma = opts.decimalComma !== undefined ? opts.decimalComma : (t.delimiter === ";" || t.delimiter === "\t") && commaNums > dotNums;
    cols.forEach(function (c, i) {
      var named = DATE_NAME.test(c), ok = 0, n = 0;
      sample.forEach(function (r) { var v = r[i]; if (v === "" || v == null) return; n++; if (parseDate(v, undefined, named)) ok++; });
      if (!n) return;
      var sc = ok / n + (named ? 0.15 : 0) - i * 0.01;
      if (ok / n >= 0.8 && sc > bestScore) { bestScore = sc; best = i; }
    });
    if (best >= 0 && opts.dmy === undefined) {
      var a12 = 0, b12 = 0;
      sample.forEach(function (r) { var m = /^(\d{1,2})[-\/.](\d{1,2})[-\/.]\d{2,4}/.exec(String(r[best] || "")); if (m) { if (+m[1] > 12) a12++; if (+m[2] > 12) b12++; } });
      dmy = a12 > 0 && !b12 ? true : b12 > 0 && !a12 ? false : undefined;
    } else dmy = opts.dmy;
    var numeric = [];
    cols.forEach(function (c, i) {
      if (i === best) return;
      var ok = 0, n = 0;
      sample.forEach(function (r) { var v = r[i]; if (v === "" || v == null || MISSING.test(String(v).trim())) return; n++; if (num(v, decimalComma) !== null) ok++; });
      if (n && ok / n >= 0.8) numeric.push(i);
    });
    var lc = cols.map(function (c) { return String(c).toLowerCase().replace(/[^a-z]/g, ""); });
    function find(re) { for (var k = 0; k < numeric.length; k++) if (re.test(lc[numeric[k]])) return numeric[k]; return -1; }
    var ohlc = { open: find(/^(open|o|opening|openprice)$/), high: find(/^(high|h|max|highprice)$/), low: find(/^(low|l|min|lowprice)$/), close: find(/^(close|c|last|price|closeprice|closing)$/), volume: find(/^(volume|vol|v|quantity)$/), adj: find(/^adjclose|adjustedclose$/) };
    var isOhlc = ohlc.open >= 0 && ohlc.high >= 0 && ohlc.low >= 0 && ohlc.close >= 0;
    return { dateCol: best, valueCols: numeric, ohlc: isOhlc ? ohlc : null, decimalComma: decimalComma, dmy: dmy };
  }
  function medianGap(ts) { var g = []; for (var i = 1; i < ts.length; i++) g.push(ts[i] - ts[i - 1]); g.sort(function (a, b) { return a - b; }); return g.length ? g[g.length >> 1] : 0; }
  function freqOf(ts) { var g = medianGap(ts) / 86400; return !g ? null : g < 0.9 ? "intraday" : g < 1.6 ? "D" : g < 8 ? "W" : g < 20 ? "2W" : g < 45 ? "M" : g < 120 ? "Q" : g < 200 ? "SA" : "A"; }
  /* Build series from a table: returns {series:[{col, name, obs:[[day, value]], rows:[...]}], diagnostics} */
  function build(t, det) {
    var dc = det.dateCol; if (dc < 0) throw new Error("No date column found. Choose the column that holds dates.");
    var badDate = [], parsed = [], anyTime = false, ambiguous = 0;
    t.rows.forEach(function (r, i) {
      var p = parseDate(r[dc], det.dmy, DATE_NAME.test(t.columns[dc]) || true);
      if (!p) { if (String(r[dc] || "").trim()) badDate.push({ row: i + 1, value: r[dc] }); return; }
      if (p.ambiguous) ambiguous++;
      if (p.prec === "time") anyTime = true;
      parsed.push({ p: p, r: r, i: i });
    });
    parsed.sort(function (a, b) { return a.p.t - b.p.t; });
    var ts = parsed.map(function (x) { return x.p.t; }), freq = freqOf(ts);
    var intraday = anyTime && freq === "intraday";
    var series = det.valueCols.map(function (ci) {
      var bad = [], obs = [], bars = [];
      parsed.forEach(function (x) {
        var raw = x.r[ci], v = num(raw, det.decimalComma);
        if (v === null) { if (!MISSING.test(String(raw == null ? "" : raw).trim())) bad.push({ row: x.i + 1, value: raw }); obs.push([x.p.day, null]); return; }
        obs.push([x.p.day, v]); bars.push({ time: Math.floor(x.p.t), value: v });
      });
      return { col: t.columns[ci], ci: ci, obs: obs, bars: bars, bad: bad, n: bars.length };
    });
    var ohlcBars = null;
    if (det.ohlc) {
      var o = det.ohlc; ohlcBars = [];
      parsed.forEach(function (x) {
        var O = num(x.r[o.open], det.decimalComma), H = num(x.r[o.high], det.decimalComma), L = num(x.r[o.low], det.decimalComma), C = num(x.r[o.close], det.decimalComma), V = o.volume >= 0 ? num(x.r[o.volume], det.decimalComma) : null;
        if ([O, H, L, C].some(function (v) { return v === null; })) return;
        ohlcBars.push({ time: Math.floor(intraday ? x.p.t : Date.parse(x.p.day) / 1000), open: O, high: Math.max(H, O, C), low: Math.min(L, O, C), close: C, volume: V });
      });
    }
    var dupDays = 0; if (!intraday) { var seen = {}; parsed.forEach(function (x) { if (seen[x.p.day]) dupDays++; seen[x.p.day] = 1; }); }
    return { series: series, ohlc: ohlcBars, freq: freq, intraday: intraday, badDate: badDate, ambiguous: ambiguous, rows: t.rows.length, dupDays: dupDays,
      first: parsed.length ? parsed[0].p.day : null, last: parsed.length ? parsed[parsed.length - 1].p.day : null };
  }

  // ------------------------------------------------------------------ storage (IndexedDB) + chart hook
  var mem = {};
  function idb() {
    return new Promise(function (res, rej) {
      if (!root.indexedDB) { rej(new Error("no IndexedDB")); return; }
      var rq = root.indexedDB.open(DB, 1);
      rq.onupgradeneeded = function () { rq.result.createObjectStore(STORE, { keyPath: "id" }); };
      rq.onsuccess = function () { res(rq.result); }; rq.onerror = function () { rej(rq.error); };
    });
  }
  function put(rec) {
    mem[rec.id.toUpperCase()] = rec; index(rec);
    return idb().then(function (db) { return new Promise(function (res) { var tx = db.transaction(STORE, "readwrite"); tx.objectStore(STORE).put(rec); tx.oncomplete = function () { res(true); }; tx.onerror = function () { res(false); }; }); }).catch(function () { return false; });
  }
  function get(id) {
    var k = String(id).toUpperCase(); if (mem[k]) return Promise.resolve(mem[k]);
    return idb().then(function (db) { return new Promise(function (res) {
      var st = db.transaction(STORE, "readonly").objectStore(STORE), rq = st.getAll();
      rq.onsuccess = function () { (rq.result || []).forEach(function (r) { mem[r.id.toUpperCase()] = r; }); res(mem[k] || null); }; rq.onerror = function () { res(null); };
    }); }).catch(function () { return null; });
  }
  function del(id) {
    delete mem[String(id).toUpperCase()]; var ix = readIndex(); delete ix[id]; writeIndex(ix); names();
    return idb().then(function (db) { db.transaction(STORE, "readwrite").objectStore(STORE).delete(id); }).catch(function () {});
  }
  function readIndex() { try { return JSON.parse(localStorage.getItem(IDX_KEY) || "{}") || {}; } catch (e) { return {}; } }
  function writeIndex(ix) { try { localStorage.setItem(IDX_KEY, JSON.stringify(ix)); } catch (e) {} }
  function index(rec) { var ix = readIndex(); ix[rec.id] = { name: rec.name, file: rec.file, n: rec.n, first: rec.first, last: rec.last, freq: rec.freq, at: rec.created, ohlc: !!rec.bars }; writeIndex(ix); names(); }
  function names() {
    var ix = readIndex(), out = {};
    Object.keys(ix).forEach(function (id) { out[id] = [ix[id].name + " · your file " + (ix[id].file || ""), "file"]; });
    root.JH_FILE_NAMES = out;
    try { root.dispatchEvent(new Event("jh-tvwl-changed")); } catch (e) {}
    return out;
  }
  function isFile(sym) { return /^FILE[:\-]/i.test(String(sym || "")); }
  function klinesFor(sym) {
    return get(sym).then(function (rec) {
      if (!rec) return { d: [], src: "Imported file " + sym + " is not stored in this browser" };
      if (rec.bars) {
        // true OHLC from the file (market path): bars as given, sorted, de-duplicated by time (last row wins is NOT applied: conflicts are dropped)
        var by = {}, conflict = {};
        rec.bars.forEach(function (b) { var k = b.time; if (by[k] && JSON.stringify(by[k]) !== JSON.stringify(b)) conflict[k] = 1; by[k] = b; });
        var d = Object.keys(by).filter(function (k) { return !conflict[k]; }).map(function (k) { return by[k]; }).sort(function (a, b) { return a.time - b.time; });
        return { d: d, src: rec.name + " · your file " + rec.file + " · " + d.length + " OHLC rows" + (Object.keys(conflict).length ? " · " + Object.keys(conflict).length + " conflicting duplicate times withheld" : "") + " · imported " + String(rec.created).slice(0, 10) };
      }
      var H = root.JHObservationSeries;
      var pkt = { id: rec.id, name: rec.name, unit: rec.unit || null, freq: "D", obs: rec.obs, provider: "file", source: rec.file };
      var r = H && H.warehouse ? H.warehouse(pkt, sym, "file://" + rec.file) : null;
      if (r) { r.src = rec.name + " · your file " + rec.file + " · " + r.src; return r; }
      return { d: [], src: "Observation contract unavailable" };
    });
  }
  // watchlist quote for an imported file, computed from the stored rows only
  function quote(sym) {
    return get(sym).then(function (rec) {
      if (!rec) return { ok: false, error: "Imported file is not stored in this browser" };
      var pts = rec.bars ? rec.bars.map(function (b) { return [new Date(b.time * 1000).toISOString().slice(0, 10), b.close]; }) : (rec.obs || []).filter(function (o) { return o && typeof o[1] === "number" && isFinite(o[1]); });
      pts = pts.slice().sort(function (x, y) { return x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0; });
      if (!pts.length) return { ok: false, error: "No numeric rows" };
      var L = pts[pts.length - 1], P = pts.length > 1 ? pts[pts.length - 2] : null;
      function pctBack(days) { var t = Date.parse(L[0]) - days * 864e5, b = null; for (var i = pts.length - 1; i >= 0; i--) if (Date.parse(pts[i][0]) <= t) { b = pts[i]; break; } return b && b[1] ? (L[1] / b[1] - 1) * 100 : null; }
      return { ok: true, name: rec.name + " (your file)", unit: rec.unit || "", freq: rec.freq || null, last: L[1], last_date: L[0], prev: P ? P[1] : null, prev_date: P ? P[0] : null,
        chg: P ? L[1] - P[1] : null, chg_pct: P && P[1] ? (L[1] / P[1] - 1) * 100 : null, mom_pct: pctBack(28), qoq_pct: pctBack(89), yoy_pct: pctBack(364),
        n: pts.length, first: pts[0][0], spark: pts.slice(-40).map(function (x) { return x[1]; }), source: "file " + rec.file };
    });
  }
  function hook() {
    var C = root.JHChartCatalog;
    if (!C || typeof C.klines !== "function" || C.__fileHooked) return !!(C && C.__fileHooked);
    var orig = C.klines;
    C.klines = function (sym) { if (isFile(sym)) return klinesFor(sym); return orig.apply(this, arguments); };
    if (typeof C.go === "function") { var go = C.go; C.go = function (s) { if (isFile(s)) return false; return go.apply(this, arguments); }; }
    C.__fileHooked = true; return true;
  }
  function slug(s) { return String(s || "data").replace(/\.[a-z0-9]+$/i, "").replace(/[^A-Za-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 40).toUpperCase() || "DATA"; }

  // ------------------------------------------------------------------ UI
  var dlg, state = {};
  function css() {
    if (doc.getElementById("jh-imp-css")) return;
    var st = doc.createElement("style"); st.id = "jh-imp-css";
    st.textContent = "#jh-imp{position:fixed;inset:0;z-index:10070;background:rgba(0,0,0,.55);display:none;align-items:flex-start;justify-content:center;padding-top:6vh;font:13px -apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,Ubuntu,sans-serif;color:#d1d4dc}" +
      "#jh-imp.on{display:flex}#jh-imp .box{width:min(860px,94vw);max-height:86vh;overflow:auto;background:#1e222d;border:1px solid #2a2e39;border-radius:8px;box-shadow:0 20px 50px rgba(0,0,0,.5);padding:16px 18px}" +
      "#jh-imp h2{margin:0 0 10px;font-size:18px;font-weight:600;color:#fff;display:flex;justify-content:space-between}#jh-imp .x{background:none;border:0;color:#b2b5be;font-size:20px;cursor:pointer}" +
      "#jh-imp .drop{border:1.5px dashed #434651;border-radius:8px;padding:22px;text-align:center;color:#b2b5be;cursor:pointer}#jh-imp .drop.hot{border-color:#2962ff;background:rgba(41,98,255,.08)}" +
      "#jh-imp textarea{width:100%;min-height:70px;background:#131722;border:1px solid #2a2e39;color:#d1d4dc;border-radius:6px;padding:8px;font:12px ui-monospace,Menlo,monospace;box-sizing:border-box}" +
      "#jh-imp input[type=text],#jh-imp select{background:#131722;border:1px solid #2a2e39;color:#d1d4dc;border-radius:4px;padding:5px 7px;font-size:12px}" +
      "#jh-imp .row{display:flex;gap:8px;align-items:center;margin:8px 0;flex-wrap:wrap}#jh-imp .mut{color:#787b86;font-size:12px}#jh-imp .warn{color:#f0b429;font-size:12px}#jh-imp .err{color:#f23645}" +
      "#jh-imp button.b{background:#2962ff;border:0;color:#fff;border-radius:4px;padding:6px 14px;font-weight:600;cursor:pointer}#jh-imp button.g{background:#2a2e39;border:0;color:#d1d4dc;border-radius:4px;padding:6px 12px;cursor:pointer}" +
      "#jh-imp table{border-collapse:collapse;font:11px ui-monospace,Menlo,monospace;width:100%;margin-top:6px}#jh-imp td,#jh-imp th{border-bottom:1px solid #2a2e39;padding:3px 6px;text-align:right;white-space:nowrap;max-width:160px;overflow:hidden;text-overflow:ellipsis}#jh-imp th{color:#b2b5be;position:sticky;top:0;background:#1e222d}" +
      "#jh-imp th.dc,#jh-imp td.dc{color:#4dd0e1}#jh-imp th.vc,#jh-imp td.vc{color:#fff}#jh-imp .cols label{margin-right:10px;white-space:nowrap}" +
      "#jh-imp .files div{display:flex;justify-content:space-between;gap:8px;padding:4px 0;border-bottom:1px solid #2a2e39}#jh-imp .files a{color:#5b9cf6;cursor:pointer}" +
      "#jh-imp-btn{margin-left:4px}";
    doc.head.appendChild(st);
  }
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function open() {
    css();
    if (!dlg) {
      dlg = doc.createElement("div"); dlg.id = "jh-imp";
      dlg.innerHTML = '<div class="box" role="dialog" aria-label="Chart a CSV or JSON file"><h2>Chart a CSV or JSON file <button class="x" data-a="close" aria-label="Close">×</button></h2>' +
        '<div class="drop" data-a="pick">Drop a .csv, .tsv, .txt, .json or .ndjson file here, or <b style="color:#5b9cf6">choose a file</b><input type="file" accept=".csv,.tsv,.txt,.json,.ndjson,.jsonl,text/csv,application/json" hidden></div>' +
        '<div class="row"><input type="text" data-k="url" placeholder="…or a URL to a CSV/JSON file" style="flex:1"><button class="g" data-a="url">Fetch</button></div>' +
        '<details><summary class="mut" style="cursor:pointer">…or paste data</summary><textarea data-k="paste" placeholder="date,value&#10;2024-01-01,3.1&#10;2024-02-01,3.2"></textarea><div class="row"><button class="g" data-a="paste">Read pasted data</button></div></details>' +
        '<div data-k="out"></div><div data-k="files" class="files"></div></div>';
      doc.body.appendChild(dlg);
      var inp = dlg.querySelector("input[type=file]"), drop = dlg.querySelector(".drop");
      dlg.addEventListener("click", function (e) {
        if (e.target === dlg) { close(); return; }
        var a = e.target.closest("[data-a]"); if (!a) return; var act = a.getAttribute("data-a");
        if (act === "close") close();
        else if (act === "pick") inp.click();
        else if (act === "url") fromURL(dlg.querySelector("[data-k=url]").value.trim());
        else if (act === "paste") ingest(dlg.querySelector("[data-k=paste]").value, "pasted data");
        else if (act === "chart") commit(false);
        else if (act === "openf") { close(); go(a.getAttribute("data-id")); }
        else if (act === "delf") { del(a.getAttribute("data-id")); listFiles(); }
      });
      inp.addEventListener("change", function () { if (inp.files && inp.files[0]) readFile(inp.files[0]); inp.value = ""; });
      ["dragenter", "dragover"].forEach(function (ev) { drop.addEventListener(ev, function (e) { e.preventDefault(); drop.classList.add("hot"); }); });
      ["dragleave", "drop"].forEach(function (ev) { drop.addEventListener(ev, function (e) { e.preventDefault(); drop.classList.remove("hot"); }); });
      drop.addEventListener("drop", function (e) { var f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]; if (f) readFile(f); });
      dlg.addEventListener("change", function (e) { if (e.target.closest("[data-k=opts]")) { readOpts(); render(); } });
      doc.addEventListener("keydown", function (e) { if (e.key === "Escape" && dlg.classList.contains("on")) close(); });
    }
    dlg.classList.add("on"); listFiles();
  }
  function close() { if (dlg) dlg.classList.remove("on"); }
  function out(html) { dlg.querySelector("[data-k=out]").innerHTML = html; }
  function readFile(f) {
    if (f.size > 80 * 1024 * 1024) { out('<p class="err">File is larger than 80 MB.</p>'); return; }
    out('<p class="mut">Reading ' + esc(f.name) + "…</p>");
    var rd = new FileReader();
    rd.onload = function () { ingest(String(rd.result || ""), f.name); };
    rd.onerror = function () { out('<p class="err">Could not read the file.</p>'); };
    rd.readAsText(f);
  }
  function fromURL(u) {
    if (!/^https?:\/\//i.test(u)) { out('<p class="err">Enter an http(s) URL.</p>'); return; }
    out('<p class="mut">Fetching…</p>');
    root.fetch(u).then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.text(); })
      .then(function (t) { ingest(t, u.split("/").pop().split("?")[0] || "url-data"); })
      .catch(function (e) { out('<p class="err">Could not fetch: ' + esc(e.message) + ". The site may not allow cross-site downloads — download the file and drop it here instead.</p>"); });
  }
  function ingest(text, fname) {
    try {
      var t = /^\s*[\[{]/.test(text) ? parseJSON(text) : parseCSV(text);
      state = { t: t, file: fname, det: detect(t) };
      state.name = slug(fname).replace(/_/g, " ");
      render();
    } catch (e) { out('<p class="err">' + esc(e.message) + "</p>"); }
  }
  function readOpts() {
    var box = dlg.querySelector("[data-k=opts]"); if (!box) return;
    var dc = +box.querySelector("[data-k=dc]").value, det = state.det;
    if (dc !== det.dateCol) { state.det = detect(state.t, { dateColForce: dc }); state.det.dateCol = dc; state.det.valueCols = state.det.valueCols.filter(function (i) { return i !== dc; }); }
    var dm = box.querySelector("[data-k=dmy]"); if (dm) state.det.dmy = dm.value === "dmy" ? true : dm.value === "mdy" ? false : undefined;
    var dcm = box.querySelector("[data-k=dcm]"); if (dcm) { var v = dcm.checked; if (v !== state.det.decimalComma) { var keep = state.det.dateCol, kdmy = state.det.dmy; state.det = detect(state.t, { decimalComma: v, dmy: kdmy }); state.det.dateCol = keep; } }
    state.sel = Array.from(box.querySelectorAll("[data-k=vc]:checked")).map(function (x) { return +x.value; });
    var as = box.querySelector("[data-k=as]"); state.as = as ? as.value : "line";
    state.name = box.querySelector("[data-k=name]").value || state.name; state.unit = box.querySelector("[data-k=unit]").value;
  }
  function render() {
    var t = state.t, det = state.det, b;
    try { b = build(t, det); } catch (e) { b = null; state.err = e.message; }
    state.b = b;
    var sel = state.sel || det.valueCols.slice(0, 1);
    state.sel = sel.filter(function (i) { return det.valueCols.indexOf(i) >= 0; });
    if (!state.sel.length && det.valueCols.length) state.sel = det.valueCols.slice(0, 1);
    var h = '<div data-k="opts"><p class="mut">' + esc(state.file) + " · " + esc(t.format || "") + " · " + t.rows.length + " rows · " + t.columns.length + " columns" + (t.skipped_meta ? " · " + t.skipped_meta + " metadata line(s) above the table skipped" : "") + "</p>";
    h += '<div class="row">Date column <select data-k="dc">' + t.columns.map(function (c, i) { return '<option value="' + i + '"' + (i === det.dateCol ? " selected" : "") + ">" + esc(c) + "</option>"; }).join("") + "</select>";
    var amb = b && b.ambiguous;
    if (amb || det.dmy !== undefined) h += ' Day/month order <select data-k="dmy"><option value="mdy"' + (det.dmy === false || det.dmy === undefined ? " selected" : "") + '>MM/DD/YYYY</option><option value="dmy"' + (det.dmy === true ? " selected" : "") + ">DD/MM/YYYY</option></select>";
    h += ' <label><input type="checkbox" data-k="dcm"' + (det.decimalComma ? " checked" : "") + "> decimal comma (1,5 = 1.5)</label></div>";
    h += '<div class="row cols">Values ' + (det.valueCols.length ? det.valueCols.map(function (i) { return '<label><input type="checkbox" data-k="vc" value="' + i + '"' + (state.sel.indexOf(i) >= 0 ? " checked" : "") + "> " + esc(t.columns[i]) + "</label>"; }).join("") : '<span class="err">no numeric columns found</span>') + "</div>";
    h += '<div class="row">Name <input type="text" data-k="name" value="' + esc(state.name) + '" style="width:260px"> Unit <input type="text" data-k="unit" value="' + esc(state.unit || "") + '" placeholder="e.g. %, USD bn" style="width:120px">';
    if (det.ohlc) h += ' Chart as <select data-k="as"><option value="ohlc"' + (state.as !== "line" ? " selected" : "") + '>OHLC candles (Open/High/Low/Close columns)</option><option value="line"' + (state.as === "line" ? " selected" : "") + ">Selected value columns</option></select>";
    h += "</div>";
    if (!b) h += '<p class="err">' + esc(state.err || "Choose the date column") + "</p>";
    else {
      h += '<p class="mut">' + (b.first ? "Dates " + b.first + " → " + b.last + " · frequency " + (b.freq || "unknown") + (b.intraday ? " (intraday timestamps)" : "") : "No readable dates") + "</p>";
      var warns = [];
      if (b.badDate.length) warns.push(b.badDate.length + " row(s) with an unreadable date skipped (e.g. row " + b.badDate[0].row + ': "' + esc(String(b.badDate[0].value).slice(0, 30)) + '")');
      b.series.filter(function (s) { return state.sel.indexOf(s.ci) >= 0 && s.bad.length; }).forEach(function (s) { warns.push(esc(s.col) + ": " + s.bad.length + " unreadable value(s) left as gaps (e.g. row " + s.bad[0].row + ': "' + esc(String(s.bad[0].value).slice(0, 20)) + '")'); });
      if (b.ambiguous && det.dmy === undefined) warns.push("Dates like 03/04/2020 are ambiguous; assumed MM/DD/YYYY — switch above if the file is DD/MM/YYYY");
      if (b.dupDays && !b.intraday) warns.push(b.dupDays + " repeated date(s): equal values are kept once; different values on the same date are withheld, not averaged");
      if (warns.length) h += '<div class="warn">' + warns.map(function (w) { return "• " + w; }).join("<br>") + "</div>";
      h += '<div class="row"><button class="b" data-a="chart">Chart it</button><span class="mut">Saved in this browser · findable in search as “' + esc(state.name) + '”</span></div>';
    }
    // preview table
    var cols = t.columns, prev = t.rows.slice(0, 8).concat(t.rows.length > 12 ? [null] : [], t.rows.length > 12 ? t.rows.slice(-3) : t.rows.slice(8, 12));
    h += '<div style="max-height:220px;overflow:auto"><table><tr>' + cols.map(function (c, i) { return '<th class="' + (i === det.dateCol ? "dc" : state.sel.indexOf(i) >= 0 ? "vc" : "") + '">' + esc(c) + "</th>"; }).join("") + "</tr>" +
      prev.map(function (r) { return r ? "<tr>" + r.map(function (c, i) { return '<td class="' + (i === det.dateCol ? "dc" : state.sel.indexOf(i) >= 0 ? "vc" : "") + '">' + esc(c) + "</td>"; }).join("") + "</tr>" : '<tr><td colspan="' + cols.length + '" style="text-align:center">…</td></tr>'; }).join("") + "</table></div></div>";
    out(h);
  }
  function commit() {
    readOpts(); var b = state.b, det = state.det; if (!b) return;
    var base = slug(state.name), now = new Date().toISOString(), ids = [];
    var jobs = [];
    if (det.ohlc && state.as !== "line" && b.ohlc && b.ohlc.length) {
      var id0 = "FILE-" + base;
      jobs.push(put({ id: id0, name: state.name, unit: state.unit || "", file: state.file, created: now, bars: b.ohlc, n: b.ohlc.length, first: b.first, last: b.last, freq: b.freq }));
      ids.push(id0);
    } else {
      b.series.filter(function (s) { return state.sel.indexOf(s.ci) >= 0; }).forEach(function (s, k) {
        var multi = state.sel.length > 1, id = (b.intraday ? "FILE-" : "FILE:") + base + (multi ? "." + slug(s.col) : "");
        var nm = state.name + (multi ? " · " + s.col : "");
        if (b.intraday) jobs.push(put({ id: id, name: nm, unit: state.unit || "", file: state.file, created: now, bars: s.bars.map(function (x) { return { time: x.time, open: x.value, high: x.value, low: x.value, close: x.value, volume: null }; }), n: s.n, first: b.first, last: b.last, freq: b.freq }));
        else jobs.push(put({ id: id, name: nm, unit: state.unit || "", file: state.file, created: now, obs: s.obs, n: s.n, first: b.first, last: b.last, freq: b.freq, column: s.col }));
        ids.push(id);
      });
    }
    if (!ids.length) { out('<p class="err">Select at least one value column.</p>'); return; }
    Promise.all(jobs).then(function () {
      close(); go(ids[0]);
      if (ids.length > 1 && typeof root.jhAddCompare === "function") setTimeout(function () { ids.slice(1, 6).forEach(function (i) { try { root.jhAddCompare(i); } catch (e) {} }); }, 2500);
    });
  }
  function go(id) { if (typeof root.jhGoSymbol === "function") root.jhGoSymbol(id, "chart"); }
  function listFiles() {
    var ix = readIndex(), ids = Object.keys(ix).sort(function (a, b) { return String(ix[b].at).localeCompare(String(ix[a].at)); });
    dlg.querySelector("[data-k=files]").innerHTML = ids.length ? '<p class="mut" style="margin:14px 0 4px">Your files</p>' + ids.map(function (id) {
      var r = ix[id]; return '<div><a data-a="openf" data-id="' + esc(id) + '">' + esc(r.name) + '</a><span class="mut">' + esc(r.file || "") + " · " + (r.n || 0) + " points · " + esc(r.first || "") + " → " + esc(r.last || "") + (r.ohlc ? " · OHLC" : "") + ' <a data-a="delf" data-id="' + esc(id) + '" title="Delete">✕</a></span></div>';
    }).join("") : "";
  }
  function button() {
    var wrap = doc.getElementById("tv-symwrap"); if (!wrap || doc.getElementById("jh-imp-btn")) return !!wrap;
    var b = doc.createElement("button"); b.type = "button"; b.id = "jh-imp-btn"; b.className = "tv-iconbtn"; b.title = "Chart a CSV or JSON file (Ctrl+Shift+O, or drop a file on the chart)"; b.textContent = "⇪ File";
    b.style.cssText = "background:none;border:0;color:inherit;font:inherit;font-size:13px;cursor:pointer;padding:0 8px;white-space:nowrap";
    b.onclick = open; wrap.appendChild(b); return true;
  }
  function init() {
    names();
    if (!hook()) { var n = 0, t = setInterval(function () { if (hook() || ++n > 120) clearInterval(t); }, 250); }
    if (!button()) { var m = 0, t2 = setInterval(function () { if (button() || ++m > 80) clearInterval(t2); }, 250); }
    doc.addEventListener("keydown", function (e) { if ((e.ctrlKey || e.metaKey) && e.shiftKey && (e.key === "O" || e.key === "o")) { e.preventDefault(); open(); } });
    // drop a file anywhere on the page
    doc.addEventListener("dragover", function (e) { if (e.dataTransfer && Array.from(e.dataTransfer.types || []).indexOf("Files") >= 0) e.preventDefault(); });
    doc.addEventListener("drop", function (e) {
      if (dlg && dlg.contains(e.target)) return;
      var f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
      if (f && /\.(csv|tsv|txt|json|ndjson|jsonl)$/i.test(f.name)) { e.preventDefault(); open(); readFile(f); }
    });
  }
  root.JHChartImport = { open: open, parseCSV: parseCSV, parseJSON: parseJSON, parseDate: parseDate, num: num, detect: detect, build: build, ingest: function (t, n) { open(); ingest(t, n); }, klines: klinesFor, quote: quote, isFile: isFile, list: readIndex, _state: function () { return state; }, commit: commit };
  if (doc && doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", init); else if (doc) init();
})(typeof window !== "undefined" ? window : globalThis);
