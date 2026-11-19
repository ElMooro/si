/* jh-wyckoff-scan.js - the Wyckoff desk's link to the all-stock scan and to its tearsheet.

   The desk's own script (inline in wyckoff-desk.html) is kept as it is: it reads four feeds and paints one table a
   tab. After every paint it calls window.JHWyckoffHook(tab, DATA) when this file has defined it. From there:

     1. the feeds the desk has already read, plus the two it does not read (data/campaign-scan.json and
        data/capitulation-desk.json), are handed to the tearsheet kit (jh-tearsheet.js, adapter "wyckoff"), and the
        sheet is told which tab is open;
     2. on the Accumulation, Bottom and Distribution tabs a second board is drawn under the desk's own: every stock and
        ETF the chart's Acc / Bot / Dist icon would mark (justhodl-campaign-scan), sortable on every column;
     3. a row of that board opens the symbol on the chart with the icon's marks switched on.

   Nothing the desk painted is changed or removed. Colours come from the page's own tokens. */
(function () {
  'use strict';
  if (typeof window === 'undefined' || typeof document === 'undefined') return;

  var BOARD = { accum: 'accumulation', bottom: 'bottom', dist: 'distribution' };
  var TITLE = { accum: 'Every stock the Acc icon marks', bottom: 'Every stock the Bot icon marks', dist: 'Every stock the Dist icon marks' };
  var FIRST = 300;                          /* rows drawn before SHOW ALL: a phone inside the chart's overlay stays quick */
  var extra = { scan: null, capit: null, tried: false };
  var last = { tab: null, data: null };
  var view = { kind: 'stock', q: '', sort: null, desc: false, all: false };
  var MINUS = '\u2212', DASH = '\u2014', DOT = ' \u00b7 ';

  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function fin(v) { return typeof v === 'number' && isFinite(v); }
  function num(v, dp) { return fin(v) ? v.toLocaleString('en-US', { minimumFractionDigits: dp || 0, maximumFractionDigits: dp || 0 }) : DASH; }
  function px(v) { return fin(v) ? v.toFixed(v >= 1000 ? 0 : 2) : DASH; }
  function pct(v, dp) {
    if (!fin(v)) return DASH;
    var r = +v.toFixed(dp == null ? 1 : dp) || 0;
    return (r < 0 ? MINUS : r > 0 ? '+' : '') + Math.abs(r).toFixed(dp == null ? 1 : dp) + '%';
  }
  function tone(v) { return v > 0 ? 'up' : v < 0 ? 'dn' : ''; }
  function money(v) {
    if (!fin(v) || v <= 0) return DASH;
    return '$' + (v >= 1e12 ? (v / 1e12).toFixed(2) + 'T' : v >= 1e9 ? (v / 1e9).toFixed(1) + 'B' : v >= 1e6 ? (v / 1e6).toFixed(0) + 'M' : (v / 1e3).toFixed(0) + 'K');
  }
  function lower(s) { return String(s || '').toLowerCase().replace(/_/g, ' '); }
  function get(url) {
    return fetch(url + '?v=' + Math.floor(Date.now() / 300000), { cache: 'no-store' }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; });
  }

  /* ---- 1. the tearsheet: hand the feeds over, say which tab is open ---- */
  function handOver() {
    if (!last.data) return;
    var docs = { __wyckoff: 1, bottom: last.data.bottom, phase: last.data.phase, katlin: last.data.katlin, radar: last.data.radar, capit: extra.capit, scan: extra.scan };
    window.JHWyckoffDocs = docs;
    try { window.dispatchEvent(new CustomEvent('jh:wyckoff-docs', { detail: docs })); } catch (e) { /* the sheet reads the feeds itself */ }
  }
  function tellSheet(tab) {
    var el = document.getElementById('jh-tearsheet');
    if (!el) return;
    try { el.dispatchEvent(new CustomEvent('jts:set', { detail: { tab: tab } })); } catch (e) { /* the sheet reads the tab from the address */ }
  }

  /* ---- 2. the all-stock board ---- */
  var STATUS_TONE = { HELD: 'up', ABOVE: 'up', BELOW: 'dn', BROKEN: 'dn', LOST: 'dn', UNDERCUT: 'amb', BASE: 'amb', RANGE: 'amb', RECLAIMED: 'up' };
  function pill(text, cls) { return '<span class="pill ' + (cls || '') + '">' + esc(text) + '</span>'; }
  function common() {
    return [
      { k: 'Ticker', v: function (r) { return r.ticker; }, f: function (r) { return esc(r.ticker) + (r.jump ? ' <span class="amb" title="A one-session move of ' + esc(r.jump.ratio) + 'x on ' + esc(r.jump.date) + ' that no split explains">!</span>' : ''); } },
      { k: 'Name', v: function (r) { return r.name || ''; }, f: function (r) { return esc(r.name || ''); }, wide: true },
      { k: 'Sector', v: function (r) { return r.kind === 'etf' ? 'ETF' : (r.sector || ''); }, f: function (r) { return esc(r.kind === 'etf' ? 'ETF' : (r.sector || DASH)); } },
      { k: 'Size', n: true, v: function (r) { return r.mcap; }, f: function (r) { return money(r.mcap); } },
      { k: 'Last', n: true, v: function (r) { return r.last; }, f: function (r) { return px(r.last); } }
    ];
  }
  function since() { return { k: 'Since', n: true, v: function (r) { return r.since_pct; }, f: function (r) { return '<span class="' + tone(r.since_pct) + '">' + pct(r.since_pct) + '</span>'; } }; }
  function status() { return { k: 'Now', v: function (r) { return r.status || ''; }, f: function (r) { return pill(r.status || DASH, STATUS_TONE[r.status]); } }; }
  function ago(label) { return { k: label, n: true, v: function (r) { return -r.ago; }, f: function (r) { return esc(r.date) + ' <span class="sc-dim">' + (r.ago === 0 ? 'last session' : r.ago + 's') + '</span>'; } }; }
  var COLS = {
    accum: function () {
      return common().concat([ago('ACCUM bar'), since(),
        { k: 'Low under the base', v: function (r) { return r.low_date || ''; }, f: function (r) { return r.low_date ? px(r.low) + ' <span class="sc-dim">' + esc(r.low_date) + '</span>' : DASH; } },
        { k: 'Base', n: true, v: function (r) { return r.base_sessions; }, f: function (r) { return fin(r.base_sessions) ? r.base_sessions + 's' : DASH; } },
        { k: 'Vs low', n: true, v: function (r) { return r.off_low_pct; }, f: function (r) { return '<span class="' + tone(r.off_low_pct) + '">' + pct(r.off_low_pct) + '</span>'; } },
        status(),
        { k: 'Read from', v: function (r) { return r.source; }, f: function (r) { return esc(r.source === 'scan' ? 'chart scan' : r.source === 'both' ? 'scan + bottom desk' : 'bottom desk ' + lower(r.harvest_state)); } }]);
    },
    bottom: function () {
      return common().concat([
        { k: 'Mark', v: function (r) { return r.mark; }, f: function (r) { return pill(r.mark, r.mark === 'CLIMAX' ? 'dn' : 'up'); } },
        ago('Bar'), since(),
        { k: 'Vs low', n: true, v: function (r) { return r.off_low_pct; }, f: function (r) { return '<span class="' + tone(r.off_low_pct) + '">' + pct(r.off_low_pct) + '</span>'; } },
        { k: 'Decline', n: true, v: function (r) { return r.decline_pct; }, f: function (r) { return pct(r.decline_pct, 0); } },
        { k: 'Vol', n: true, v: function (r) { return r.vol_x; }, f: function (r) { return fin(r.vol_x) ? r.vol_x.toFixed(1) + '\u00d7' + (r.vol_x === 12 && r.source !== 'harvest' ? '+' : '') : DASH; } },      /* the chart's scan stops counting at 12 times */
        status(),
        { k: 'Read from', v: function (r) { return r.source; }, f: function (r) { return esc(r.source === 'scan' ? 'chart scan' : (r.source === 'both' ? 'scan + ' : '') + 'bottom desk ' + lower(r.harvest_state)); } }]);
    },
    dist: function () {
      return common().concat([ago('DIST bar'), since(),
        { k: 'Rally high', v: function (r) { return r.high_date || ''; }, f: function (r) { return px(r.high) + ' <span class="sc-dim">' + esc(r.high_date || '') + '</span>'; } },
        { k: 'Vs high', n: true, v: function (r) { return r.below_high_pct; }, f: function (r) { return '<span class="' + tone(r.below_high_pct) + '">' + pct(r.below_high_pct) + '</span>'; } },
        { k: 'Push / prior', n: true, v: function (r) { return r.push_pct; }, f: function (r) { return pct(r.push_pct, 0) + ' / ' + pct(r.prev_push_pct, 0); } },
        { k: 'Why', v: function (r) { return r.why || ''; }, f: function (r) { return esc(r.why === 'more volume bought less' ? 'more volume, less progress' : 'barely cleared the last high'); } },
        status()]);
    }
  };

  function style() {
    if (document.getElementById('jh-wyckoff-scan-css')) return;
    var s = document.createElement('style');
    s.id = 'jh-wyckoff-scan-css';
    s.textContent = '#scan-panel{margin:6px 0 20px}#scan-own-label{font-family:var(--mono);font-size:11px;letter-spacing:.12em;color:var(--amb);margin:0 0 8px;text-transform:uppercase;font-weight:700}#scan-panel h2{font-family:var(--mono);font-size:11px;letter-spacing:.12em;color:var(--amb);margin:0 0 4px;text-transform:uppercase}'
      + '#scan-panel .sc-sub{color:var(--mut);font-size:12px;line-height:1.5;margin:0 0 10px;max-width:980px}'
      + '#scan-panel .sc-bar{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin:0 0 10px}'
      + '#scan-panel .sc-bar button{background:var(--card);border:1px solid var(--line);color:var(--dim);font-family:var(--mono);font-size:11px;letter-spacing:.06em;padding:6px 11px;border-radius:6px;cursor:pointer}'
      + '#scan-panel .sc-bar button[aria-pressed="true"]{color:var(--cy);border-color:var(--cy)}'
      + '#scan-panel .sc-bar input{background:var(--card);border:1px solid var(--line);color:var(--fg);font-family:var(--mono);font-size:12px;padding:6px 10px;border-radius:6px;min-width:200px;flex:1 1 200px;max-width:340px}'
      + '#scan-panel .sc-n{font-family:var(--mono);font-size:11px;color:var(--mut);margin-left:auto}'
      + '#scan-panel .sc-wrap{background:var(--card);border:1px solid var(--line);border-radius:10px;overflow:auto;max-height:78vh}'
      + '#scan-panel th{position:sticky;top:0;background:var(--card);white-space:nowrap;user-select:none}#scan-panel th.n,#scan-panel td.n{text-align:right}'
      + '#scan-panel th[aria-sort="ascending"]::after{content:" \\25B2"}#scan-panel th[aria-sort="descending"]::after{content:" \\25BC"}'
      + '#scan-panel td{white-space:nowrap}#scan-panel td.w{white-space:normal;min-width:150px;max-width:260px;font-family:var(--sans);color:var(--dim)}'
      + '#scan-panel .sc-dim{color:var(--mut);font-size:11px}#scan-panel .sc-more{margin:10px 0 0}';
    document.head.appendChild(s);
  }

  function openOnChart(ticker, mark) {
    try {
      if (window.top !== window && typeof window.top.jhOpenSymbol === 'function') {      /* inside the chart's workspace: load it behind the overlay, marks on */
        if (typeof window.top.jhEnsureMarks === 'function') window.top.jhEnsureMarks(mark);
        window.top.jhOpenSymbol(ticker);
        var x = window.top.document.getElementById('wsx');
        if (x) x.click();
        return;
      }
    } catch (e) { /* another origin: navigate */ }
    var href = '/chart.html?s=' + encodeURIComponent(ticker) + '&marks=' + encodeURIComponent(mark);
    try { window.top.location.href = href; } catch (e2) { location.href = href; }
  }

  var OWN = { accum: 'The desk\u2019s own accumulation rows' + DOT + 'phase detector, radar and Katlin', bottom: 'The bottom desk\u2019s own rows' + DOT + 'its board, graded' };
  function removePanel() {
    ['scan-panel', 'scan-own-label'].forEach(function (id) {
      var old = document.getElementById(id);
      if (old && old.parentNode) old.parentNode.removeChild(old);
    });
    var own = document.querySelector('.panel');
    if (own) own.hidden = false;
  }

  function drawPanel(tab) {
    removePanel();
    var board = BOARD[tab];
    if (!board) return;
    /* the all-stock board goes above the desk's own table: it is the answer to "which stocks", the table under it is the desk's graded subset */
    var d = extra.scan, own0 = document.querySelector('.panel'), host = own0 && own0.parentNode;
    if (!host) return;
    style();
    var el = document.createElement('section');
    el.id = 'scan-panel';
    el.setAttribute('aria-label', TITLE[tab]);
    el.setAttribute('data-jh-chrome', '');             /* it comes and goes with the tab: numbering it would move the addresses of the sections under it */
    host.insertBefore(el, own0);
    if (OWN[tab]) {
      var lab = document.createElement('h2');
      lab.id = 'scan-own-label';
      lab.setAttribute('data-jh-chrome', '');
      lab.textContent = OWN[tab];
      host.insertBefore(lab, own0);
    }
    if (!d || !d.boards || !d.boards[board]) {
      el.innerHTML = '<h2>' + esc(TITLE[tab]) + '</h2><p class="sc-sub">' + (extra.tried ? 'The all-stock scan (data/campaign-scan.json) is not readable right now. The desk above is unaffected.' : 'Reading the all-stock scan\u2026') + '</p>';
      return;
    }
    var all = d.boards[board], u = d.universe || {}, w = d.windows || {};
    var counts = { stock: all.filter(function (r) { return r.kind === 'stock'; }).length, etf: all.filter(function (r) { return r.kind === 'etf'; }).length };
    if (view.kind !== 'all' && !counts[view.kind]) view.kind = counts.stock ? 'stock' : 'all';
    var cols = COLS[tab](), q = view.q.trim().toLowerCase();
    var rows = all.filter(function (r) { return view.kind === 'all' || r.kind === view.kind; });
    if (q) rows = rows.filter(function (r) { return (r.ticker + ' ' + (r.name || '') + ' ' + (r.sector || '') + ' ' + (r.industry || '') + ' ' + (r.status || '') + ' ' + (r.mark || '')).toLowerCase().indexOf(q) >= 0; });
    if (view.sort != null && cols[view.sort]) {
      var c = cols[view.sort], dir = view.desc ? -1 : 1;
      rows = rows.slice().sort(function (a, b) {
        var x = c.v(a), y = c.v(b);
        if (c.n) { x = fin(x) ? x : null; y = fin(y) ? y : null; if (x == null || y == null) return (x == null) - (y == null); return (x - y) * dir; }
        return String(x).localeCompare(String(y)) * dir;
      });
    }
    var shown = view.all ? rows : rows.slice(0, FIRST);
    var windowWord = tab === 'bottom' ? 'a climax inside the last ' + (w.climax_sessions || 40) + ' sessions or a confirmed low inside the last ' + (w.bottom_sessions || 126)
      : tab === 'accum' ? 'an ACCUM bar inside the last ' + (w.accumulation_sessions || 63) + ' sessions' : 'a DIST bar inside the last ' + (w.distribution_sessions || 63) + ' sessions';
    el.innerHTML = '<h2>' + esc(TITLE[tab]) + DOT + num(all.length) + ' names</h2>'
      + '<p class="sc-sub">The chart\u2019s own scan, run over ' + num(u.scanned_stock) + ' listed stocks and ' + num(u.scanned_etf) + ' plain ETFs on the bars of ' + esc(d.as_of) + ': ' + windowWord
      + '. Click a column to sort it, click it again to reverse. Click a row to open it on the chart with the marks on. \u201cNow\u201d is where the last close stands against the mark\u2019s own level.</p>'
      + '<div class="sc-bar">' + [['stock', 'STOCKS ' + num(counts.stock)], ['etf', 'ETFS ' + num(counts.etf)], ['all', 'ALL ' + num(all.length)]].map(function (b) {
        return '<button type="button" data-sc-kind="' + b[0] + '" aria-pressed="' + (view.kind === b[0] ? 'true' : 'false') + '">' + b[1] + '</button>'; }).join('')
      + '<input type="search" id="scan-q" placeholder="Filter: ticker, name, sector, status" aria-label="Filter the board" value="' + esc(view.q) + '">'
      + '<span class="sc-n">' + num(rows.length) + ' shown' + (rows.length > shown.length ? DOT + 'first ' + num(shown.length) + ' drawn' : '') + '</span></div>'
      + '<div class="sc-wrap"><table id="scan-board"><thead><tr>' + cols.map(function (c, i) {
        return '<th data-sc-col="' + i + '" class="' + (c.n ? 'n' : '') + '"' + (view.sort === i ? ' aria-sort="' + (view.desc ? 'descending' : 'ascending') + '"' : '') + '>' + esc(c.k) + '</th>'; }).join('') + '</tr></thead><tbody>'
      + (shown.length ? shown.map(function (r) {
        return '<tr data-t="' + esc(r.ticker) + '">' + cols.map(function (c) { return '<td class="' + (c.n ? 'n' : '') + (c.wide ? ' w' : '') + '">' + c.f(r) + '</td>'; }).join('') + '</tr>'; }).join('')
        : '<tr><td class="empty" colspan="' + cols.length + '">No name matches this filter.</td></tr>')
      + '</tbody></table></div>'
      + (rows.length > shown.length ? '<div class="sc-bar sc-more"><button type="button" data-sc-all="1">SHOW ALL ' + num(rows.length) + '</button></div>' : '');
    /* the desk's own table says "no separate board" on the Distribution tab: with this board drawn, that panel steps aside */
    var own = document.querySelector('.panel'), ownBoard = document.getElementById('board');
    if (own && ownBoard && tab === 'dist' && !ownBoard.querySelector('tr[data-t]')) own.hidden = true;
    var kpi = document.getElementById('kpi');
    if (kpi && !kpi.querySelector('[data-sc-kpi]')) {
      kpi.insertAdjacentHTML('beforeend', '<div data-sc-kpi><span>Chart scan' + DOT + 'all stocks</span><b>' + num(counts.stock) + '</b></div><div data-sc-kpi><span>Chart scan' + DOT + 'ETFs</span><b>' + num(counts.etf) + '</b></div>');
    }
  }

  document.addEventListener('click', function (e) {
    var t = e.target, panel = t && t.closest ? t.closest('#scan-panel') : null;
    if (!panel) return;
    var kind = t.closest('[data-sc-kind]'), col = t.closest('[data-sc-col]'), more = t.closest('[data-sc-all]'), row = t.closest('tr[data-t]');
    if (kind) { view.kind = kind.getAttribute('data-sc-kind'); view.all = false; drawPanel(last.tab); return; }
    if (col) {
      var i = +col.getAttribute('data-sc-col');
      if (view.sort === i) view.desc = !view.desc; else { view.sort = i; view.desc = !!COLS[last.tab]()[i].n; }      /* a number opens largest first */
      drawPanel(last.tab);
      return;
    }
    if (more) { view.all = true; drawPanel(last.tab); return; }
    if (row) openOnChart(row.getAttribute('data-t'), last.tab);
  });
  document.addEventListener('input', function (e) {
    if (!e.target || e.target.id !== 'scan-q') return;
    view.q = e.target.value;
    view.all = false;
    var at = e.target.selectionStart;
    drawPanel(last.tab);
    var box = document.getElementById('scan-q');
    if (box) { box.focus(); try { box.setSelectionRange(at, at); } catch (err) { /* not every input type keeps a caret */ } }
  });

  /* ---- the hook the desk calls after every paint ---- */
  window.JHWyckoffHook = function (tab, data) {
    var moved = last.tab !== tab;
    last.tab = tab;
    last.data = data;
    if (moved) { view.sort = null; view.desc = false; view.all = false; view.q = ''; }
    handOver();
    tellSheet(tab);
    drawPanel(tab);
  };
  window.JHWyckoffScan = { columns: COLS, view: view, open: openOnChart };

  Promise.all([get('/data/campaign-scan.json'), get('/data/capitulation-desk.json')]).then(function (docs) {
    extra.scan = docs[0];
    extra.capit = docs[1];
    extra.tried = true;
    handOver();
    if (last.tab) { tellSheet(last.tab); drawPanel(last.tab); }
  });
})();
