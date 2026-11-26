/* jh-engine-data.js - "Engine data": every field an engine wrote, shown on the engine's own page.

   Khalid 2026-11-26, choosing twelve engines for a tab on chart.html: "audit their engine before you add them and
   see if you can expand it to cover more data and that all data from the engine is displaying on the page". The
   audit (ops 6971) compared each engine's documents with its page: the pages name between a third and four fifths
   of the fields, and five of them never load a second document their engine writes. A page's own views stay as
   they are; this kit adds one section under them that leaves nothing out:

     Sections     the document as the engine laid it out - figures as tiles, every list as a table that opens whole,
                  sorts both ways on every column, filters and downloads; a list inside a row opens in a sheet.
     Every field  one row per value with its full path, searchable; its count is the document's count.

   A page opts in with  <div data-jed="options"></div>  and the registry below says which documents that page's
   engines write. Nothing is estimated or relabelled: a number gets a unit only when the field's own name carries it
   (_usd, _pct); the engine's field name is always one hover away and is what the Every field view prints.

   Colours are the site's tokens with the tearsheet kit's fallbacks. */
(function (root, factory) {
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else { root.JHEngineData = api; api.boot(root); }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  var VERSION = '1.1.1';
  var DASH = '\u2014', TIMES = '\u00D7', DOT = ' \u00B7 ';
  var PROXY = 'https://justhodl-data-proxy.raafouis.workers.dev/', RESEARCH = PROXY + 'equity-research/';

  var PAGES = {
    research: { docs: [{ ticker: true, engine: 'justhodl-equity-research', name: 'Company research' }] },
    insiders: { docs: [
      { key: 'data/insider-trades.json', engine: 'justhodl-insider-trades', name: 'Insider trades (SEC Form 4)' },
      { key: 'data/insider-clusters.json', engine: 'justhodl-insider-cluster-scanner', name: 'Insider cluster scanner' },
      { key: 'data/insider-sell-cluster.json', engine: 'justhodl-insider-sell-cluster', name: 'Insider selling clusters' },
      { key: 'data/insider-industry-cluster.json', engine: 'justhodl-insider-industry-cluster', name: 'Insider buying by industry' },
      { key: 'data/insider-trades-full.json', engine: 'justhodl-insider-trades', name: 'Insider trades, the full list' }] },
    options: { docs: [
      { key: 'data/options-analytics.json', engine: 'justhodl-options-analytics', name: 'Options analytics' },
      { key: 'data/opex-calendar.json', engine: 'justhodl-opex-calendar', name: 'OPEX calendar' },
      { key: 'data/options-analytics-iv-history.json', engine: 'justhodl-options-analytics', name: 'Implied volatility history' }] },
    valuations: { docs: [
      { key: 'data/stock-valuations.json', engine: 'justhodl-stock-valuations', name: 'Stock valuations' },
      { key: 'data/valuations-decisive-call.json', engine: 'justhodl-stock-valuations', name: 'Valuations read' }] },
    corpbonds: { docs: [{ key: 'data/corp-bonds.json', engine: 'justhodl-corp-bonds', name: 'Corp bonds' }] },
    plumbing: { docs: [
      { key: 'data/plumbing-stress.json', engine: 'justhodl-plumbing-aggregator', name: 'Plumbing stress' },
      { key: 'data/plumbing-history.json', engine: 'justhodl-plumbing-aggregator', name: 'Plumbing history' }] },
    liquidity: { docs: [
      { key: 'liquidity-data.json', url: PROXY + 'liquidity-data.json', engine: 'justhodl-liquidity-agent', name: 'Liquidity agent' },
      { key: 'data/liquidity-flow.json', engine: 'justhodl-liquidity-flow', name: 'Liquidity flow' }] },
    canaries: { docs: [
      { key: 'data/canary-warroom.json', engine: 'justhodl-canary-warroom', name: 'Canary war room' },
      { key: 'data/warroom-history.json', engine: 'justhodl-canary-warroom', name: 'War room history' },
      { key: 'data/warroom-weights.json', engine: 'justhodl-warroom-weights', name: 'War room weights' },
      { key: 'data/warroom-replay.json', engine: 'justhodl-warroom-weights', name: 'War room replay' },
      { key: 'data/canaries-decisive-call.json', engine: 'justhodl-canary-warroom', name: 'Canaries read' }] },
    breadth: { docs: [
      { key: 'data/breadth-thrust.json', engine: 'justhodl-breadth-thrust', name: 'Breadth thrust' },
      { key: 'data/breadth-history.json', engine: 'justhodl-breadth-thrust', name: 'Breadth history' }] },
    reversal: { docs: [
      { key: 'data/trend-reversal.json', engine: 'justhodl-trend-reversal', name: 'Trend reversal' },
      { key: 'data/trend-reversal-history.json', engine: 'justhodl-trend-reversal', name: 'Trend reversal history' }] },
    calendar: { docs: [{ key: 'data/catalyst-calendar.json', engine: 'justhodl-catalyst-calendar', name: 'Catalyst calendar' }] },
    crypto: { docs: [
      { key: 'data/onchain-ratios.json', engine: 'justhodl-onchain-ratios', name: 'On-chain ratios' },
      { key: 'data/cryptoquant-onchain.json', engine: 'justhodl-cryptoquant-onchain', name: 'CryptoQuant on-chain' }] }
  };

  /* ---------- pure: shape ---------- */
  function isNum(v) { return typeof v === 'number' && isFinite(v); }
  function isObj(v) { return v !== null && typeof v === 'object' && !Array.isArray(v); }
  function isScalar(v) { return v === null || typeof v !== 'object'; }

  function alike(map, keys) {
    var first = Object.keys(map[keys[0]]), hit = 0, n = Math.min(keys.length, 40), i, j, k;
    if (!first.length) return false;
    for (i = 1; i < n; i++) { k = map[keys[i]]; for (j = 0; j < first.length; j++) if (Object.prototype.hasOwnProperty.call(k, first[j])) hit++; }
    return n < 2 ? false : hit / ((n - 1) * first.length) >= 0.5;
  }

  function kind(v) {
    var i, objs = 0, nums = 0, scal = 0, arrs = 0, keys;
    if (isScalar(v)) return 'scalar';
    if (Array.isArray(v)) {
      if (!v.length) return 'empty';
      for (i = 0; i < v.length; i++) { if (isObj(v[i])) objs++; else if (Array.isArray(v[i])) arrs++; else { scal++; if (isNum(v[i])) nums++; } }
      if (objs === v.length) return 'rows';
      if (nums === v.length && v.length >= 8) return 'series';
      if (scal === v.length) return 'list';
      if (arrs === v.length) return 'matrix';
      return 'mixed';
    }
    keys = Object.keys(v);
    if (!keys.length) return 'empty';
    for (i = 0; i < keys.length; i++) { if (isObj(v[keys[i]])) objs++; else if (isScalar(v[keys[i]])) scal++; }
    if (objs === keys.length && keys.length >= 3 && alike(v, keys)) return 'table';
    if (scal === keys.length) return 'pairs';
    return 'group';
  }

  function rowsOf(v) {
    var k = kind(v);
    if (k === 'rows') return v;
    if (k === 'table') return Object.keys(v).map(function (key) { var r = { '(key)': key }, s = v[key], f; for (f in s) if (Object.prototype.hasOwnProperty.call(s, f)) r[f] = s[f]; return r; });
    if (k === 'matrix') return v.map(function (a) { var r = {}; a.forEach(function (x, i) { r['[' + i + ']'] = x; }); return r; });
    if (Array.isArray(v)) return v.map(function (x) { return isObj(x) ? x : { value: x }; });
    return [];
  }

  function columns(rows) {
    var seen = {}, out = [], i, k;
    for (i = 0; i < rows.length; i++) for (k in rows[i]) if (Object.prototype.hasOwnProperty.call(rows[i], k) && !seen[k]) { seen[k] = 1; out.push(k); }
    return out;
  }

  function sortRows(rows, col, dir) {
    var sign = dir === 'asc' ? 1 : -1;
    function rank(v) { return v === null || v === undefined || v === '' ? 2 : isScalar(v) ? 0 : 1; }
    return rows.map(function (r, i) { return [r, i]; }).sort(function (a, b) {
      var x = a[0][col], y = b[0][col], rx = rank(x), ry = rank(y);
      if (rx !== ry) return rx - ry;
      if (rx === 1) { x = Array.isArray(x) ? x.length : Object.keys(x).length; y = Array.isArray(y) ? y.length : Object.keys(y).length; }
      if (rx === 2) return a[1] - b[1];
      if (typeof x === 'number' && typeof y === 'number') return (x - y) * sign || a[1] - b[1];
      if (typeof x === 'boolean') x = x ? 1 : 0;
      if (typeof y === 'boolean') y = y ? 1 : 0;
      if (typeof x === 'number' && typeof y === 'number') return (x - y) * sign || a[1] - b[1];
      x = String(x); y = String(y);
      return (x < y ? -1 : x > y ? 1 : 0) * sign || a[1] - b[1];
    }).map(function (p) { return p[0]; });
  }

  /* every value with its path, without building the list: fn(path, value) returning false stops the walk */
  function eachLeaf(node, fn, path) {
    var i, k, keys;
    path = path || '';
    if (isScalar(node)) return fn(path || '$', node);
    if (Array.isArray(node)) {
      if (!node.length) return fn(path + '[]', '(empty list)');
      for (i = 0; i < node.length; i++) if (eachLeaf(node[i], fn, path + '[' + i + ']') === false) return false;
      return true;
    }
    keys = Object.keys(node);
    if (!keys.length) return fn(path || '$', '(empty)');
    for (i = 0; i < keys.length; i++) { k = keys[i]; if (eachLeaf(node[k], fn, path ? path + '.' + k : k) === false) return false; }
    return true;
  }

  function census(doc) {
    var values = 0, names = {}, n = 0;
    eachLeaf(doc, function (p) { values++; var f = p.replace(/\[\d+\]/g, '[]'); if (!names[f]) { names[f] = 1; n++; } });
    return { values: values, fields: n };
  }

  function leafPage(doc, q, start, size) {
    var out = [], seen = 0, needle = (q || '').toLowerCase();
    eachLeaf(doc, function (p, v) {
      if (needle && p.toLowerCase().indexOf(needle) < 0 && String(v).toLowerCase().indexOf(needle) < 0) return true;
      if (seen >= start && out.length < size) out.push([p, v]);
      seen++;
      return true;
    });
    return { rows: out, total: seen };
  }

  /* ---------- pure: words ---------- */
  var UPPER = { usd: 1, iv: 1, gex: 1, oi: 1, pcr: 1, vrp: 1, etf: 1, cik: 1, sec: 1, fomc: 1, cpi: 1, gdp: 1, ema: 1, sma: 1, rsi: 1, atr: 1, yoy: 1, qoq: 1, ytd: 1, ttm: 1, eps: 1, pe: 1, pb: 1, ps: 1, fcf: 1, roe: 1, roic: 1, ev: 1, ebitda: 1, id: 1, url: 1, btc: 1, eth: 1, mvrv: 1, nupl: 1, nvt: 1, hv: 1, atm: 1, dte: 1, opex: 1, ai: 1, us: 1, eu: 1, uk: 1, ig: 1, hy: 1, oas: 1, cds: 1, ust: 1, tga: 1, rrp: 1, sofr: 1, iorb: 1, effr: 1, ofr: 1, fsi: 1, ecb: 1, boj: 1, ciss: 1, cftc: 1, cot: 1, fda: 1, ipo: 1, macd: 1, ma: 1, dma: 1, nav: 1, aum: 1, trace: 1, finra: 1, fred: 1, fmp: 1, sp: 1, hp: 1, wacc: 1, dcf: 1, json: 1, s3: 1, utc: 1 };
  function label(key) {
    var words = String(key).replace(/_+/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2').trim().split(/\s+/);
    if (!words[0]) return String(key);
    return words.map(function (w, i) { var l = w.toLowerCase(); return UPPER[l] ? l.toUpperCase() : i === 0 ? w.charAt(0).toUpperCase() + w.slice(1) : w; }).join(' ');
  }

  function group3(s) { return s.replace(/\B(?=(\d{3})+(?!\d))/g, ','); }
  function plain(v) {
    var a = Math.abs(v), s, parts;
    if (Math.floor(v) === v && a < 1e15) return group3(String(v));
    s = a >= 1000 ? v.toFixed(1) : a >= 1 ? v.toFixed(2) : a >= 0.01 ? v.toFixed(4) : v.toPrecision(3);
    if (/e/i.test(s)) return s;
    s = s.replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '');
    parts = s.split('.');
    return group3(parts[0]) + (parts[1] ? '.' + parts[1] : '');
  }
  function money(v) {
    var a = Math.abs(v), s = v < 0 ? '\u2212' : '';
    return s + '$' + (a >= 1e12 ? (a / 1e12).toFixed(2) + 'T' : a >= 1e9 ? (a / 1e9).toFixed(2) + 'B' : a >= 1e6 ? (a / 1e6).toFixed(2) + 'M' : a >= 1e3 ? group3(a.toFixed(0)) : a.toFixed(2));
  }
  var STAMP = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})(?::\d{2}(?:\.\d+)?)?(Z|[+-]00:?00)?$/;
  function show(key, v) {
    var k = String(key == null ? '' : key).toLowerCase(), m;
    if (v === null || v === undefined || v === '') return DASH;
    if (v === true) return 'Yes';
    if (v === false) return 'No';
    if (isNum(v)) {
      if (/(^|_)usd($|_)/.test(k) && !/(musd|busd|kusd|usd_(bn|mn|mm|m|b|k))/.test(k)) return money(v);
      if (/(_pct|_percent)$/.test(k) || k === 'pct') return plain(v) + '%';
      return plain(v);
    }
    if (typeof v === 'number') return String(v);
    m = STAMP.exec(v);
    if (m) return m[1] + ' ' + m[2] + (m[3] ? ' UTC' : '');
    return String(v);
  }

  function describe(v) {
    var k = kind(v), rows;
    if (k === 'scalar') return '';
    if (k === 'empty') return 'empty';
    if (k === 'rows' || k === 'table' || k === 'matrix' || k === 'mixed') { rows = rowsOf(v); return group3(String(rows.length)) + (rows.length === 1 ? ' row ' : ' rows ') + TIMES + ' ' + columns(rows.slice(0, 200)).length + ' columns'; }
    if (k === 'series') return group3(String(v.length)) + ' points';
    if (k === 'list') return group3(String(v.length)) + (v.length === 1 ? ' item' : ' items');
    return Object.keys(v).length + ' fields';
  }

  function csvCell(v) {
    var s = v === null || v === undefined ? '' : isScalar(v) ? String(v) : JSON.stringify(v);
    if (/^[=+\-@\t\r]/.test(s) && !/^-?\d/.test(s)) s = "'" + s;
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }
  function csvOf(rows, cols) {
    return [cols.map(csvCell).join(',')].concat(rows.map(function (r) { return cols.map(function (c) { return csvCell(r[c]); }).join(','); })).join('\r\n') + '\r\n';
  }

  function stampOf(doc) {
    var names = ['generated_at', 'as_of', 'asof', 'updated_at', 'generated', 'timestamp', 'last_updated'], i;
    if (!isObj(doc)) return null;
    for (i = 0; i < names.length; i++) if (typeof doc[names[i]] === 'string' && /^\d{4}-\d{2}-\d{2}/.test(doc[names[i]])) return doc[names[i]];
    return null;
  }
  function hoursSince(stamp, now) {
    var t = Date.parse(/[zZ]|[+-]\d{2}:?\d{2}$/.test(stamp) || stamp.length <= 10 ? stamp : stamp + 'Z');
    return isNaN(t) ? null : (now - t) / 3600000;
  }
  function ago(h) { return h === null ? '' : h < 1 ? Math.max(1, Math.round(h * 60)) + ' min ago' : h < 48 ? Math.round(h) + ' h ago' : Math.round(h / 24) + ' days ago'; }

  var SYM = /^[A-Z][A-Z0-9.\-]{0,9}$/, SYMCOL = { ticker: 1, symbol: 1, sym: 1, t: 1, underlying: 1, etf: 1 };
  function isTickerCell(col, v) { return SYMCOL[String(col).toLowerCase()] === 1 && typeof v === 'string' && SYM.test(v); }

  function spark(vals, w, h) {
    var nums = vals.filter(isNum), lo, hi, n = nums.length, pts = [], i, x, y;
    if (n < 2) return '';
    lo = Math.min.apply(null, nums); hi = Math.max.apply(null, nums);
    for (i = 0; i < n; i++) { x = (i / (n - 1)) * (w - 2) + 1; y = hi === lo ? h / 2 : h - 1 - ((nums[i] - lo) / (hi - lo)) * (h - 2); pts.push(x.toFixed(1) + ',' + y.toFixed(1)); }
    return '<svg class="jed-spark" width="' + w + '" height="' + h + '" viewBox="0 0 ' + w + ' ' + h + '" aria-hidden="true"><polyline fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round" points="' + pts.join(' ') + '"/></svg>';
  }

  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }

  /* ---------- page ---------- */
  var CSS = '.jed{--jb:var(--jh-bg,#0C0B09);--jp:var(--jh-panel,#12110C);--jp2:var(--jh-panel-2,#17150E);--ji:var(--jh-ink,#e8e2d4);--jd:var(--jh-dim,#8a836f);--ja:var(--jh-amber,#F0B429);--jg:var(--jh-green,#6fce8a);--jr:var(--jh-red,#E07A6A);--jl:var(--jh-line,#2B2820);'
    + '--jm:var(--jh-mono,ui-monospace,SFMono-Regular,Menlo,monospace);--js:var(--jh-sans,system-ui,-apple-system,sans-serif);box-sizing:border-box;max-width:1180px;margin:34px auto 60px;padding:0 16px;color:var(--ji);font-family:var(--js);font-size:13px;line-height:1.45;text-align:left;scroll-margin-top:70px}'
    + '.jed *{box-sizing:border-box}.jed-h{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;border-top:2px solid var(--ja);padding-top:12px;margin-bottom:4px}.jed-h h2{font-size:17px;margin:0;font-weight:700;color:var(--ji);letter-spacing:0}.jed-h span{color:var(--jd);font-size:12px}'
    + '.jed-lede{color:var(--jd);font-size:12px;max-width:820px;margin:0 0 14px}'
    + '.jed-doc{border:1px solid var(--jl);border-radius:8px;background:var(--jp);margin:0 0 14px;min-width:0}.jed-doc>summary{list-style:none;cursor:pointer;padding:11px 14px;display:flex;gap:10px 16px;align-items:baseline;flex-wrap:wrap}.jed-doc>summary::-webkit-details-marker{display:none}'
    + '.jed-doc>summary b{font-size:14px}.jed-doc>summary i{font-style:normal;color:var(--jd);font-family:var(--jm);font-size:11px}.jed-doc>summary .late{color:var(--jr)}.jed-doc>summary .ok{color:var(--jg)}.jed-doc>summary:before{content:"\\25B8";color:var(--ja);font-size:11px}.jed-doc[open]>summary:before{content:"\\25BE"}'
    + '.jed-body{padding:0 14px 14px;border-top:1px solid var(--jl)}.jed-bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;padding:10px 0}'
    + '.jed button,.jed input{font:inherit;font-size:12px;color:var(--ji);background:var(--jp2);border:1px solid var(--jl);border-radius:6px;padding:5px 10px;margin:0;width:auto;height:auto;min-height:0;line-height:1.3;text-transform:none;letter-spacing:0;box-shadow:none}.jed button{cursor:pointer}.jed button:hover{border-color:var(--ja)}.jed button[aria-pressed=true]{border-color:var(--ja);color:var(--ja)}.jed input{min-width:200px}.jed input:focus{outline:1px solid var(--ja)}.jed a{color:var(--ja);text-decoration:none}.jed a:hover{text-decoration:underline}'
    + '.jed-kv{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:8px;margin:6px 0 12px}.jed-kv>div{background:var(--jp2);border:1px solid var(--jl);border-radius:6px;padding:7px 9px;min-width:0}.jed-kv dt{color:var(--jd);font-size:10.5px;text-transform:uppercase;letter-spacing:.04em;margin:0 0 2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.jed-kv dd{margin:0;font-family:var(--jm);font-size:12.5px;overflow-wrap:anywhere;white-space:pre-wrap}.jed-kv .wide{grid-column:1/-1}.jed-kv .wide dd{font-family:var(--js);font-size:12.5px;line-height:1.5}'
    + '.jed-blk{border:1px solid var(--jl);border-radius:6px;margin:0 0 8px;background:var(--jb);min-width:0}.jed-blk>summary{cursor:pointer;padding:8px 11px;display:flex;gap:10px;align-items:baseline;list-style:none}.jed-blk>summary::-webkit-details-marker{display:none}.jed-blk>summary:before{content:"\\25B8";color:var(--jd);font-size:10px}.jed-blk[open]>summary:before{content:"\\25BE"}.jed-blk>summary b{font-size:12.5px}.jed-blk>summary i{font-style:normal;color:var(--jd);font-size:11px;font-family:var(--jm)}.jed-in{padding:4px 11px 11px;min-width:0}'
    + '.jed-scroll{overflow:auto;max-height:560px;border:1px solid var(--jl);border-radius:6px}.jed table{border-collapse:collapse;width:100%;font-size:12px;margin:0;background:transparent}.jed th,.jed td{padding:5px 9px;border:0;border-bottom:1px solid var(--jl);text-align:left;white-space:nowrap;max-width:380px;overflow:hidden;text-overflow:ellipsis;background:transparent;color:var(--ji);font-weight:400;height:auto}'
    + '.jed th{position:sticky;top:0;background:var(--jp2);color:var(--jd);font-size:10.5px;text-transform:uppercase;letter-spacing:.03em;font-weight:600;cursor:pointer;user-select:none;z-index:1}.jed th:hover{color:var(--ji)}.jed th[aria-sort]{color:var(--ja)}.jed td.n{text-align:right;font-family:var(--jm)}.jed th.n{text-align:right}.jed td.neg{color:var(--jr)}.jed tr:hover td{background:var(--jp2)}.jed td:first-child,.jed th:first-child{position:sticky;left:0;background:var(--jp);z-index:1}.jed th:first-child{background:var(--jp2);z-index:2}'
    + '.jed-cell{font:inherit;font-family:var(--jm);font-size:11px;padding:1px 7px;border-radius:10px;color:var(--ja)}.jed-spark{color:var(--ja);vertical-align:middle;margin-right:6px}.jed-chips{display:flex;flex-wrap:wrap;gap:5px;margin:6px 0}.jed-chips span{background:var(--jp2);border:1px solid var(--jl);border-radius:5px;padding:2px 7px;font-family:var(--jm);font-size:11.5px}'
    + '.jed-foot{display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding-top:8px;color:var(--jd);font-size:11.5px}.jed-series{display:flex;gap:16px;align-items:center;flex-wrap:wrap;margin:6px 0}.jed-series .jed-spark{color:var(--ja)}.jed-series dl{display:flex;gap:14px;margin:0}.jed-series dt{color:var(--jd);font-size:10.5px;text-transform:uppercase}.jed-series dd{margin:0;font-family:var(--jm)}'
    + '.jed-path{font-family:var(--jm);color:var(--jd);font-size:11.5px}.jed-msg{color:var(--jd);padding:14px 0}.jed-msg.bad{color:var(--jr)}'
    + '.jed-modal{position:fixed;inset:0;z-index:2147483000;background:rgba(0,0,0,.66);display:flex;align-items:flex-start;justify-content:center;padding:5vh 12px}.jed-sheet{background:var(--jp);border:1px solid var(--jl);border-top:2px solid var(--ja);border-radius:8px;width:min(1100px,100%);max-height:90vh;overflow:auto;padding:12px 14px 16px}.jed-sheet header{display:flex;justify-content:space-between;gap:12px;align-items:baseline;margin-bottom:8px}.jed-sheet header b{font-size:14px}'
    + '.jed-jump{position:fixed;left:14px;bottom:14px;z-index:900;font:600 12px var(--jh-sans,system-ui,sans-serif);color:#0C0B09;background:var(--jh-amber,#F0B429);border:0;border-radius:18px;padding:8px 14px;cursor:pointer;box-shadow:0 2px 10px rgba(0,0,0,.5)}'
    + '@media(max-width:640px){.jed{padding:0 10px}.jed input{min-width:0;flex:1}.jed-kv{grid-template-columns:repeat(auto-fill,minmax(140px,1fr))}.jed-jump{left:8px;bottom:8px}}';

  function boot(win) {
    var doc = win.document;
    if (!doc || !doc.querySelectorAll) return;
    function start() {
      var hosts = doc.querySelectorAll('[data-jed]'), i;
      if (!hosts.length) return;
      if (!doc.getElementById('jed-css')) { var st = doc.createElement('style'); st.id = 'jed-css'; st.textContent = CSS; doc.head.appendChild(st); }
      for (i = 0; i < hosts.length; i++) mount(win, hosts[i]);
    }
    if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', start); else start();
  }

  function el(doc, tag, cls, html) { var e = doc.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; }

  function mount(win, host) {
    var doc = win.document, page = PAGES[host.getAttribute('data-jed')], state = { docs: [], ticker: null };
    if (!page || host.getAttribute('data-jed-on')) return;
    host.setAttribute('data-jed-on', VERSION);
    host.className = (host.className ? host.className + ' ' : '') + 'jed';
    host.id = host.id || 'engine-data';
    host.innerHTML = '<div class="jed-h"><h2>Engine data</h2><span>everything the engine' + (page.docs.length > 1 ? 's' : '') + ' behind this page wrote, nothing left out</span></div>'
      + '<p class="jed-lede">The views above are this page\u2019s own. Below is each document in full: <b>Sections</b> lays it out as the engine did, with every list a table you can open whole, sort both ways on any column, filter and download; <b>Every field</b> lists each value with its path. Hover a name for the engine\u2019s own field name.</p>';
    page.docs.forEach(function (d, i) {
      var det = el(doc, 'details', 'jed-doc');
      det.setAttribute('data-doc', d.key || 'ticker');
      det.innerHTML = '<summary><b>' + esc(d.name) + '</b><i>' + esc(d.engine) + '</i><i class="jed-st">' + (d.ticker ? 'waiting for a ticker' : 'not loaded yet') + '</i></summary><div class="jed-body"><div class="jed-msg">Loading\u2026</div></div>';
      host.appendChild(det);
      var slot = { def: d, det: det, doc: null, loaded: false, view: 'sections', url: d.url || (d.key ? '/' + d.key : null) };
      state.docs.push(slot);
      det.addEventListener('toggle', function () { if (det.open) load(win, slot); });
      if (i === 0 && !d.ticker) det.open = true;
    });
    if (page.docs[0].ticker) {
      var take = function (t) {
        var slot = state.docs[0];
        if (!t || t === state.ticker) return;
        state.ticker = t; slot.url = RESEARCH + encodeURIComponent(String(t).toUpperCase()) + '.json'; slot.loaded = false; slot.doc = null;
        slot.det.querySelector('summary b').textContent = slot.def.name + DOT + String(t).toUpperCase();
        slot.det.querySelector('.jed-st').textContent = 'not loaded yet';
        if (slot.det.open) load(win, slot); else slot.det.open = true;
      };
      if (win.__JH_TICKER_BUS && win.__JH_TICKER_BUS.subscribe) win.__JH_TICKER_BUS.subscribe(take);
      var q = /[?&](?:ticker|t)=([^&#]+)/.exec(win.location.search || '');
      if (q) take(decodeURIComponent(q[1]));
    }
    if (!doc.querySelector('.jed-jump')) {
      /* shown while the reader is at the top of the page, so it says the section exists without sitting on the content */
      var jump = el(doc, 'button', 'jed-jump', 'All engine data \u2193'), near = false;
      var place = function () { jump.style.display = near || (win.pageYOffset || 0) > 260 ? 'none' : ''; };
      jump.type = 'button';
      jump.onclick = function () { host.scrollIntoView({ behavior: 'smooth', block: 'start' }); };
      doc.body.appendChild(jump);
      if (win.IntersectionObserver) new win.IntersectionObserver(function (es) { near = es[0].isIntersecting; place(); }).observe(host);
      win.addEventListener('scroll', place, { passive: true });
      place();
    }
    /* Opened as a tab inside the chart, the page drops the site furniture that belongs to a page of its own: the
       research pills that float over the figures and the sign-in button. They stay on the page itself. */
    try {
      if (win.top !== win && typeof win.top.jhOpenSymbol === 'function') {
        doc.documentElement.className += ' jh-in-chart';
        if (!doc.getElementById('jed-in-chart')) {
          var hide = doc.createElement('style'); hide.id = 'jed-in-chart';
          hide.textContent = '.jh-in-chart #jh-tenor-pill,.jh-in-chart .jh-tenor-pill,.jh-in-chart #jh-liq-pill,.jh-in-chart .jh-liq-pill,.jh-in-chart #jh-lce-pill,.jh-in-chart .jh-lce-pill,.jh-in-chart #jh-auth-auto-slot,.jh-in-chart .jh-auth-auto-slot,.jh-in-chart #jh-capital-structure-status,.jh-in-chart #private-account-status{display:none!important}';
          doc.head.appendChild(hide);
        }
      }
    } catch (e) { /* another origin above: not the chart */ }
    host.__jed = state;
    doc.addEventListener('keydown', function (e) { if (e.key === 'Escape') { var m = doc.querySelector('.jed-modal'); if (m) m.remove(); } });
  }

  function load(win, slot) {
    var body = slot.det.querySelector('.jed-body'), st = slot.det.querySelector('.jed-st'), url = slot.url;
    if (slot.loaded || !url) return;
    slot.loaded = true;
    body.innerHTML = '<div class="jed-msg">Loading\u2026</div>';
    win.fetch(url, { cache: 'no-cache' }).then(function (r) {
      if (!r.ok) throw new Error('the document answered ' + r.status);
      slot.modified = r.headers.get('last-modified');
      return r.text();
    }).then(function (text) {
      if (slot.url !== url) return;
      slot.bytes = text.length;
      slot.doc = JSON.parse(text);
      slot.census = census(slot.doc);
      var stamp = stampOf(slot.doc) || (slot.modified ? new Date(slot.modified).toISOString() : null), h = stamp ? hoursSince(stamp, Date.now()) : null;
      st.className = 'jed-st ' + (h !== null && h > 30 ? 'late' : 'ok');
      st.textContent = (stamp ? 'written ' + show('', stamp) + (h !== null ? ' (' + ago(h) + ')' : '') + DOT : '') + group3(String(slot.census.values)) + ' values in ' + group3(String(slot.census.fields)) + ' fields' + DOT + group3(String(Math.round(slot.bytes / 1024))) + ' KB';
      draw(win, slot);
    }).catch(function (e) {
      slot.loaded = false;
      st.className = 'jed-st late'; st.textContent = 'not read';
      body.innerHTML = '<div class="jed-msg bad">This document could not be read (' + esc(e.message) + '). <button type="button">Try again</button></div>';
      body.querySelector('button').onclick = function () { load(win, slot); };
    });
  }

  function draw(win, slot) {
    var doc = win.document, body = slot.det.querySelector('.jed-body'), bar = el(doc, 'div', 'jed-bar'), pane = el(doc, 'div');
    body.innerHTML = '';
    bar.innerHTML = '<button type="button" data-v="sections" aria-pressed="' + (slot.view === 'sections') + '">Sections</button><button type="button" data-v="fields" aria-pressed="' + (slot.view === 'fields') + '">Every field</button>'
      + '<input type="search" placeholder="Find a field or a value" aria-label="Find a field or a value"><a href="' + esc(slot.url) + '" target="_blank" rel="noopener">The document itself</a>';
    body.appendChild(bar); body.appendChild(pane);
    var input = bar.querySelector('input'), timer = null;
    function paint() {
      pane.innerHTML = '';
      if (slot.view === 'fields' || input.value) pane.appendChild(fieldsView(win, slot, input.value));
      else pane.appendChild(node(win, slot.doc, '', 0));
      bar.querySelector('[data-v=sections]').setAttribute('aria-pressed', String(slot.view === 'sections' && !input.value));
      bar.querySelector('[data-v=fields]').setAttribute('aria-pressed', String(slot.view === 'fields' || !!input.value));
    }
    bar.addEventListener('click', function (e) { var v = e.target.getAttribute && e.target.getAttribute('data-v'); if (v) { slot.view = v; if (v === 'sections') input.value = ''; paint(); } });
    input.addEventListener('input', function () { clearTimeout(timer); timer = setTimeout(paint, 180); });
    paint();
  }

  function fieldsView(win, slot, q) {
    var doc = win.document, wrap = el(doc, 'div'), size = 100, page = 0;
    function paint() {
      var got = leafPage(slot.doc, q, page * size, size), h = '<div class="jed-scroll"><table><thead><tr><th>Field</th><th>Value</th></tr></thead><tbody>', i, last = Math.max(0, Math.ceil(got.total / size) - 1);
      for (i = 0; i < got.rows.length; i++) h += '<tr><td class="jed-path" title="' + esc(got.rows[i][0]) + '">' + esc(got.rows[i][0]) + '</td><td title="' + esc(String(got.rows[i][1])) + '">' + esc(show(got.rows[i][0].replace(/\[\d+\]$/, '').split('.').pop(), got.rows[i][1])) + '</td></tr>';
      h += '</tbody></table></div><div class="jed-foot"><span data-total="' + got.total + '">' + group3(String(got.total)) + (q ? ' matching values' : ' values') + (got.total ? DOT + 'showing ' + group3(String(page * size + 1)) + '\u2013' + group3(String(page * size + got.rows.length)) : '') + '</span>'
        + (last ? '<button type="button" data-p="-1"' + (page ? '' : ' disabled') + '>Previous</button><button type="button" data-p="1"' + (page < last ? '' : ' disabled') + '>Next</button>' : '') + '</div>';
      wrap.innerHTML = h;
    }
    wrap.addEventListener('click', function (e) { var p = e.target.getAttribute && e.target.getAttribute('data-p'); if (p) { page += +p; paint(); } });
    paint();
    return wrap;
  }

  function node(win, v, key, depth) {
    var doc = win.document, k = kind(v), wrap = el(doc, 'div'), keys, scal = [], rest = [], dl, h, nums, wide;
    if (k === 'scalar') { wrap.textContent = show(key, v); return wrap; }
    if (k === 'empty') { wrap.className = 'jed-msg'; wrap.textContent = 'Empty.'; return wrap; }
    if (k === 'rows' || k === 'table' || k === 'matrix' || k === 'mixed') return table(win, rowsOf(v), key);
    if (k === 'series') {
      nums = v;
      wrap.innerHTML = '<div class="jed-series">' + spark(nums, 320, 64) + '<dl><div><dt>First</dt><dd>' + esc(show(key, nums[0])) + '</dd></div><div><dt>Low</dt><dd>' + esc(show(key, Math.min.apply(null, nums))) + '</dd></div><div><dt>High</dt><dd>' + esc(show(key, Math.max.apply(null, nums))) + '</dd></div><div><dt>Last</dt><dd>' + esc(show(key, nums[nums.length - 1])) + '</dd></div><div><dt>Points</dt><dd>' + nums.length + '</dd></div></dl></div>';
      wrap.appendChild(table(win, nums.map(function (x, i) { return { '#': i + 1, value: x }; }), key));
      return wrap;
    }
    if (k === 'list') {
      if (v.length > 80) return table(win, v.map(function (x, i) { return { '#': i + 1, value: x }; }), key);
      wrap.className = 'jed-chips';
      wrap.innerHTML = v.map(function (x) { return '<span>' + esc(show(key, x)) + '</span>'; }).join('');
      return wrap;
    }
    keys = Object.keys(v);
    keys.forEach(function (f) { (isScalar(v[f]) ? scal : rest).push(f); });
    if (scal.length) {
      dl = el(doc, 'dl', 'jed-kv');
      h = '';
      wide = '';
      scal.forEach(function (f) { var s = show(f, v[f]), one = '<dt title="' + esc(f) + '">' + esc(label(f)) + '</dt><dd>' + esc(s) + '</dd></div>'; if (s.length > 60) wide += '<div class="wide">' + one; else h += '<div>' + one; });
      h += wide;
      dl.innerHTML = h;
      wrap.appendChild(dl);
    }
    rest.forEach(function (f) {
      var det = el(doc, 'details', 'jed-blk'), inner = el(doc, 'div', 'jed-in'), done = false;
      det.setAttribute('data-f', f);
      det.innerHTML = '<summary><b title="' + esc(f) + '">' + esc(label(f)) + '</b><i>' + esc(describe(v[f])) + '</i></summary>';
      det.appendChild(inner);
      function fill() { if (!done) { done = true; inner.appendChild(node(win, v[f], f, depth + 1)); } }
      det.addEventListener('toggle', function () { if (det.open) fill(); });
      if (depth === 0 && rest.length <= 3) { det.open = true; fill(); }
      wrap.appendChild(det);
    });
    return wrap;
  }

  function sheet(win, title, v, key) {
    var doc = win.document, m = el(doc, 'div', 'jed jed-modal'), s = el(doc, 'div', 'jed-sheet');
    m.style.maxWidth = 'none'; m.style.margin = '0';
    s.innerHTML = '<header><b>' + esc(title) + '</b><button type="button">Close</button></header>';
    s.appendChild(node(win, v, key, 1));
    m.appendChild(s);
    m.addEventListener('click', function (e) { if (e.target === m) m.remove(); });
    s.querySelector('header button').onclick = function () { m.remove(); };
    doc.body.appendChild(m);
  }

  function table(win, rows, key) {
    var doc = win.document, wrap = el(doc, 'div'), cols = columns(rows), st = { col: null, dir: 'desc', q: '', page: 0, size: 25 }, numeric = {}, embedded = false;
    try { embedded = win.top !== win && typeof win.top.jhOpenSymbol === 'function'; } catch (e) { embedded = false; }
    cols.forEach(function (c) { var n = 0, t = 0, i; for (i = 0; i < rows.length && i < 300; i++) if (rows[i][c] !== null && rows[i][c] !== undefined) { t++; if (isNum(rows[i][c])) n++; } numeric[c] = t > 0 && n === t; });
    function current() {
      var out = rows, q = st.q.toLowerCase();
      if (q) out = out.filter(function (r) { var i, v; for (i = 0; i < cols.length; i++) { v = r[cols[i]]; if (v !== null && v !== undefined && (isScalar(v) ? String(v) : JSON.stringify(v)).toLowerCase().indexOf(q) >= 0) return true; } return false; });
      if (st.col) out = sortRows(out, st.col, st.dir);
      return out;
    }
    function cell(r, c, i) {
      var v = r[c], k = kind(v);
      if (k === 'scalar') {
        if (isTickerCell(c, v)) return '<td><a data-sym="' + esc(v) + '" href="/chart.html?symbol=' + encodeURIComponent(v) + '"' + (embedded ? '' : ' target="_blank" rel="noopener"') + '>' + esc(v) + '</a></td>';
        var s = show(c, v);
        return '<td class="' + (numeric[c] ? 'n' : '') + (isNum(v) && v < 0 ? ' neg' : '') + '" title="' + esc(s) + '">' + esc(s) + '</td>';
      }
      if (k === 'empty') return '<td>' + DASH + '</td>';
      if (k === 'series') return '<td>' + spark(v, 70, 18) + '<button type="button" class="jed-cell" data-r="' + i + '" data-c="' + esc(c) + '">' + v.length + ' points</button></td>';
      if (k === 'list' && v.length <= 4 && v.join(', ').length <= 60) return '<td title="' + esc(v.join(', ')) + '">' + esc(v.map(function (x) { return show(c, x); }).join(', ')) + '</td>';
      return '<td><button type="button" class="jed-cell" data-r="' + i + '" data-c="' + esc(c) + '">' + esc(describe(v)) + '</button></td>';
    }
    var shown = [];
    function paint() {
      var all = current(), pages = Math.max(1, Math.ceil(all.length / st.size)), h, i;
      if (st.page >= pages) st.page = pages - 1;
      shown = all.slice(st.page * st.size, st.page * st.size + st.size);
      h = '<div class="jed-bar"><input type="search" placeholder="Filter these rows" aria-label="Filter these rows" value="' + esc(st.q) + '"><button type="button" data-a="all">' + (st.size > 25 ? 'Show 25 a page' : 'Open the whole list') + '</button><button type="button" data-a="csv">Download CSV</button></div>';
      h += '<div class="jed-scroll"><table><thead><tr>' + cols.map(function (c) { return '<th data-col="' + esc(c) + '" class="' + (numeric[c] ? 'n' : '') + '" title="' + esc(c) + '"' + (st.col === c ? ' aria-sort="' + (st.dir === 'asc' ? 'ascending' : 'descending') + '"' : '') + '>' + esc(label(c)) + (st.col === c ? (st.dir === 'asc' ? ' \u25B2' : ' \u25BC') : '') + '</th>'; }).join('') + '</tr></thead><tbody>';
      for (i = 0; i < shown.length; i++) h += '<tr>' + cols.map(function (c) { return cell(shown[i], c, i); }).join('') + '</tr>';
      h += '</tbody></table></div><div class="jed-foot"><span data-rows="' + all.length + '">' + group3(String(all.length)) + (all.length === 1 ? ' row' : ' rows') + (all.length !== rows.length ? ' of ' + group3(String(rows.length)) : '') + (pages > 1 ? DOT + 'page ' + (st.page + 1) + ' of ' + pages : '') + '</span>'
        + (pages > 1 ? '<button type="button" data-a="prev"' + (st.page ? '' : ' disabled') + '>Previous</button><button type="button" data-a="next"' + (st.page < pages - 1 ? '' : ' disabled') + '>Next</button>' : '') + '</div>';
      wrap.innerHTML = h;
    }
    wrap.addEventListener('click', function (e) {
      var t = e.target, a = t.getAttribute && t.getAttribute('data-a'), th = t.closest ? t.closest('th') : null, c, blob, link;
      if (th && wrap.contains(th)) { c = th.getAttribute('data-col'); if (st.col === c) st.dir = st.dir === 'desc' ? 'asc' : 'desc'; else { st.col = c; st.dir = 'desc'; } st.page = 0; paint(); return; }
      if (t.getAttribute && t.getAttribute('data-sym') && embedded) { e.preventDefault(); win.top.jhOpenSymbol(t.getAttribute('data-sym')); return; }
      if (t.className === 'jed-cell') { c = t.getAttribute('data-c'); sheet(win, label(c), shown[+t.getAttribute('data-r')][c], c); return; }
      if (a === 'all') { st.size = st.size > 25 ? 25 : 2000; st.page = 0; paint(); }
      else if (a === 'prev') { st.page--; paint(); }
      else if (a === 'next') { st.page++; paint(); }
      else if (a === 'csv') {
        blob = new win.Blob([csvOf(current(), cols)], { type: 'text/csv' });
        link = doc.createElement('a'); link.href = win.URL.createObjectURL(blob); link.download = (key || 'engine-data') + '.csv';
        doc.body.appendChild(link); link.click(); link.remove();
      }
    });
    wrap.addEventListener('input', function (e) {
      if (e.target.type !== 'search') return;
      st.q = e.target.value; st.page = 0;
      var pos = e.target.selectionStart; paint();
      var again = wrap.querySelector('input[type=search]'); again.focus(); try { again.setSelectionRange(pos, pos); } catch (x) { /* some inputs refuse a range */ }
    });
    paint();
    return wrap;
  }

  return { VERSION: VERSION, PAGES: PAGES, kind: kind, rowsOf: rowsOf, columns: columns, sortRows: sortRows, eachLeaf: eachLeaf, census: census, leafPage: leafPage, label: label, show: show, plain: plain, money: money, describe: describe, csvOf: csvOf, stampOf: stampOf, hoursSince: hoursSince, ago: ago, isTickerCell: isTickerCell, spark: spark, boot: boot };
});
