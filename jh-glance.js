/* jh-glance.js - "At a glance": one visual summary at the top of a desk, drawn from the desk's own documents.
   jh-reskin-skip (the palette below is this file's own, in the site's warm dark theme)

   Khalid 2026-11-26, after the twelve chart tabs: "liquidity and plumbing need better visuals for easier digestion
   of data, breadth engine need improvement and better iux, calendar need improvement and a better iux, crypto need
   improvement and a better iux". Each of those pages printed its engine's figures as text and tables. This kit
   draws them: a page opts in with  <div data-glance="plumbing"></div>  and the adapter of that name reads the
   documents the page already reads. The page's own views stay below, untouched.

   Every figure is the document's own. Arithmetic done here is named where it is shown (the 10-day average of the
   breadth ratio is the engine's own formula and is checked against the engine's figure; liquidity's legs are shown
   adding up to its total). Colour never carries a meaning alone: stress and percentile tiles print their number,
   rises and falls print their sign. */
(function (root, factory) {
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else { root.JHGlance = api; api.boot(root); }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  var VERSION = '1.0.0', DASH = '\u2014', MINUS = '\u2212', DOT = ' \u00B7 ';
  var PROXY = 'https://justhodl-data-proxy.raafouis.workers.dev/';

  /* ---------- small pure helpers ---------- */
  function fin(v) { return typeof v === 'number' && isFinite(v); }
  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function group3(s) { return s.replace(/\B(?=(\d{3})+(?!\d))/g, ','); }
  function num(v, dp) { if (!fin(v)) return DASH; var s = Math.abs(v).toFixed(dp == null ? 1 : dp).split('.'); return (v < 0 ? MINUS : '') + group3(s[0]) + (s[1] ? '.' + s[1] : ''); }
  function signed(v, dp, unit) { return fin(v) ? (v > 0 ? '+' : v < 0 ? MINUS : '') + num(Math.abs(v), dp) + (unit || '') : DASH; }
  function usd(v) { if (!fin(v)) return DASH; var a = Math.abs(v), s = v < 0 ? MINUS : ''; return s + '$' + (a >= 1e12 ? (a / 1e12).toFixed(2) + 'T' : a >= 1e9 ? (a / 1e9).toFixed(2) + 'B' : a >= 1e6 ? (a / 1e6).toFixed(1) + 'M' : group3(a.toFixed(a >= 100 ? 0 : 2))); }
  function tone(v) { return !fin(v) || v === 0 ? '' : v > 0 ? 'up' : 'dn'; }
  function day(s) { return String(s || '').slice(0, 10); }
  var MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  function nice(iso) { var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || '')); return m ? MON[+m[2] - 1] + ' ' + (+m[3]) : DASH; }
  function utc(iso) { var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(iso || '')); return m ? Date.UTC(+m[1], +m[2] - 1, +m[3]) : NaN; }
  function addDays(iso, n) { return new Date(utc(iso) + n * 864e5).toISOString().slice(0, 10); }
  function daysBetween(a, b) { return Math.round((utc(b) - utc(a)) / 864e5); }
  /* the breadth engine's own average (compute_ema): seeded with the first value, alpha = 2 / (n + 1) */
  function ema(values, n) { var a = 2 / (n + 1), out = [], e, i; for (i = 0; i < values.length; i++) { e = i ? a * values[i] + (1 - a) * e : values[0]; out.push(e); } return out; }

  /* ---------- models: the documents turned into what is drawn (pure, tested) ---------- */
  var LAYERS = { L0: 'Repo core', L1: 'Eurodollar plumbing', L2: 'Bank risk appetite', L3: 'Real-side damage', L4: 'Cross-border stress' };
  var SEVERITY = ['CRITICAL', 'HIGH', 'ELEVATED', 'WARNING', 'WATCH', 'INFO'];
  function plumbingModel(d) {
    d = d || {};
    var raw = d.raw_indicators || {}, rows = Object.keys(raw).map(function (k) { var r = raw[k] || {}; return { id: k, label: r.label || k, layer: r.layer, value: r.value, date: r.date, z: r.z_score, pct: r.percentile, stress: r.stress_score, interp: r.interp || '', source: r.source || '' }; });
    var scored = rows.filter(function (r) { return fin(r.stress); });
    var layers = Object.keys(LAYERS).map(function (id) { var l = (d.layers || {})[id] || {}; return { id: id, name: LAYERS[id], score: l.score, weight: l.weight, n: rows.filter(function (r) { return r.layer === id; }).length, have: scored.filter(function (r) { return r.layer === id; }).length }; });
    var alerts = (d.alerts || []).slice().sort(function (a, b) { var x = SEVERITY.indexOf(a.severity), y = SEVERITY.indexOf(b.severity); return (x < 0 ? 9 : x) - (y < 0 ? 9 : y) || (b.stress_score || 0) - (a.stress_score || 0); });
    var counts = {}; alerts.forEach(function (a) { counts[a.severity] = (counts[a.severity] || 0) + 1; });
    return { score: d.composite_score, label: d.composite_label, asOf: d.as_of, n: rows.length, have: scored.length, layers: layers, rows: rows,
      top: scored.slice().sort(function (a, b) { return b.stress - a.stress || String(a.id).localeCompare(b.id); }).slice(0, 12),
      calm: scored.slice().sort(function (a, b) { return a.stress - b.stress || String(a.id).localeCompare(b.id); }).slice(0, 6), alerts: alerts, alertCounts: counts,
      weighted: layers.every(function (l) { return fin(l.score) && fin(l.weight); }) ? layers.reduce(function (s, l) { return s + l.score * l.weight; }, 0) : null };
  }

  var GROUPS = { fed_balance_sheet: 'Fed balance sheet', soma: 'Fed holdings (SOMA)', reserves: 'Bank reserves', tga: 'Treasury account', rrp: 'Reverse repo', fed_repo_assets: 'Fed repo assets', funding: 'Funding rates', money_supply: 'Money supply', credit: 'Credit spreads', stress: 'Financial stress', dollar: 'Dollar', yields: 'Treasury yields', equities: 'Equities' };
  function liquidityModel(flow, agent) {
    flow = flow || {};
    var cur = flow.current || {}, obs = cur.observation_dates || {}, hist = (flow.history_180d || []).filter(function (r) { return r && fin(r.net); });
    var legs = [{ k: 'walcl', name: 'Fed balance sheet', v: cur.fed_balance_sheet_b, sign: 1, date: obs.WALCL }, { k: 'tga', name: 'Treasury General Account', v: cur.tga_b, sign: -1, date: obs.WTREGEN }, { k: 'rrp', name: 'Reverse repo', v: cur.rrp_b, sign: -1, date: obs.RRPONTSYD }];
    var sum = legs.every(function (l) { return fin(l.v); }) ? legs.reduce(function (s, l) { return s + l.sign * l.v; }, 0) : null;
    var nets = hist.map(function (r) { return r.net; }), lo = nets.length ? Math.min.apply(null, nets) : null, hi = nets.length ? Math.max.apply(null, nets) : null;
    var windows = ['1d', '1w', '1m', '3m'].map(function (w) { var x = (flow.deltas || {})[w] || {}; return { w: w, net: x.net, walcl: x.walcl, tga: x.tga, rrp: x.rrp, from: x.baseline_valuation_date }; });
    var groups = [];
    if (agent && agent.series) Object.keys(GROUPS).forEach(function (g) {
      var ids = (agent.series_groups || {})[g] || [], rows = ids.map(function (id) {
        var s = agent.series[id]; if (!s) return null; var cc = s.calendar_comparisons || {}, m = cc.month || {}, q = cc.quarter || {}, y = cc.year || {};
        var v = parseFloat(m.current_decimal != null ? m.current_decimal : (q.current_decimal != null ? q.current_decimal : y.current_decimal));
        return { id: id, label: s.label || id, unit: s.unit || '', date: s.latest_date, value: isFinite(v) ? v : null, m1: m.change, q1: q.change, y1: y.change, m1p: m.pct_change, q1p: q.pct_change, y1p: y.pct_change };
      }).filter(Boolean);
      if (rows.length) groups.push({ id: g, name: GROUPS[g], rows: rows });
    });
    return { net: cur.net_liquidity_b, valued: cur.valuation_date, asOf: flow.generated_at || flow.as_of, oldest: flow.as_of, formula: flow.formula, note: flow.formula_note, legs: legs, sum: sum, adds: sum !== null && fin(cur.net_liquidity_b) && Math.abs(sum - cur.net_liquidity_b) < 0.01,
      hist: hist, lo: lo, hi: hi, loDate: hist.length ? hist[nets.indexOf(lo)].date : null, hiDate: hist.length ? hist[nets.indexOf(hi)].date : null,
      place: hist.length && hi > lo && fin(cur.net_liquidity_b) ? (cur.net_liquidity_b - lo) / (hi - lo) : null, windows: windows, groups: groups, nSeries: groups.reduce(function (s, g) { return s + g.rows.length; }, 0) };
  }

  function breadthModel(b, h) {
    b = b || {};
    var all = ((h || {}).history || []).filter(function (r) { return r && fin(r.ratio) && r.date; }).slice().sort(function (x, y) { return x.date < y.date ? -1 : 1; });
    var rows = all.slice(-60);   // the engine's state machine reads the last 60 sessions
    var dodRows = all.filter(function (r) { return fin(r.ratio_dod); }).slice(-60), dodAt = {}, de = ema(dodRows.map(function (r) { return r.ratio_dod; }), 10);
    dodRows.forEach(function (r, i) { dodAt[r.date] = de[i]; });
    var mc = b.mcclellan || {}, dd = b.day_over_day || {}, pre = b.preliminary || null;
    var cr = b.current_readings || {}, th = cr.zweig_thresholds || {}, low = fin(th.low) ? th.low : 0.4, high = fin(th.high) ? th.high : 0.615;
    var e = ema(rows.map(function (r) { return r.ratio; }), 10), last = e.length ? e[e.length - 1] : null, win = e.slice(-11);
    var fwd = ['1m', '3m', '6m', '12m'].map(function (k) { var x = (b.forward_expectations || {})[k] || {}; return { h: k, mean: x.return_pct, median: x.median_pct, best: x.best_pct, worst: x.worst_pct, win: x.win_rate_pct, n: x.n, basis: x.basis }; });
    var ss = b.supporting_signals || {};
    return { state: b.state, since: b.state_since, strength: b.signal_strength, asOf: b.as_of, low: low, high: high, rows: rows, ema: e, emaNow: fin(cr.zweig_10d_ema) ? cr.zweig_10d_ema : last,
      agrees: fin(cr.zweig_10d_ema) && last !== null ? Math.abs(cr.zweig_10d_ema - last) < 0.0006 : null, winMin: win.length ? Math.min.apply(null, win) : null, winMax: win.length ? Math.max.apply(null, win) : null,
      toFire: fin(cr.zweig_10d_ema) ? high - cr.zweig_10d_ema : null, up: rows.filter(function (r) { return r.ratio > 0.5; }).length, n: rows.length, fwd: fwd, episodes: b.historical_episodes || b.trigger_history || [],
      kept: all.length, emaDod: dodRows.length >= 11 ? rows.map(function (r) { return fin(dodAt[r.date]) ? dodAt[r.date] : null; }) : null, dod: dd.available ? dd : null, dodAgrees: dd.available && de.length ? Math.abs(de[de.length - 1] - dd.ema_10d) < 0.0006 : null,
      mcclellan: mc.available ? mc : null, preliminary: pre && pre.status === 'ahead_of_settled' ? pre : null, preGap: pre && pre.status === 'settled' ? pre : null, version: b.version || '',
      counted: rows.filter(function (r) { return fin(r.adv) && fin(r.dec); }).length, lastCount: (function () { for (var i = rows.length - 1; i >= 0; i--) if (fin(rows[i].adv)) return rows[i]; return null; })(), basis: b.breadth_basis || '', whaley: ss.whaley_january_barometer || cr.whaley || null, coppock: ss.coppock_curve || cr.coppock || null, why: b.why_now_explainer || '', trade: (b.recommended_trade || {}).primary || null, conditions: b.trigger_conditions || [] };
  }

  var IMPACT = { HIGH: 3, MEDIUM: 2, LOW: 1 };
  function calendarModel(d, today) {
    d = d || {};
    today = day(today || d.as_of);
    var ahead = (d.events || []).filter(function (e) { return e && e.date >= today; }).sort(function (a, b) { return a.date < b.date ? -1 : a.date > b.date ? 1 : String(a.time || '').localeCompare(String(b.time || '')); });
    function next(test) { for (var i = 0; i < ahead.length; i++) if (test(ahead[i])) return ahead[i]; return null; }
    var start = addDays(today, -((new Date(utc(today)).getUTCDay() + 6) % 7)), cells = [], i, by = {};
    ahead.forEach(function (e) { (by[e.date] = by[e.date] || []).push(e); });
    for (i = 0; i < 35; i++) {
      var iso = addDays(start, i), ev = by[iso] || [], hol = ev.filter(function (e) { return e.type === 'MARKET_HOLIDAY'; })[0];
      cells.push({ date: iso, dom: +iso.slice(8), month: MON[+iso.slice(5, 7) - 1], dow: new Date(utc(iso)).getUTCDay(), past: iso < today, today: iso === today, n: ev.length,
        high: ev.filter(function (e) { return e.impact === 'HIGH'; }).length, medium: ev.filter(function (e) { return e.impact === 'MEDIUM'; }).length, holiday: hol ? hol.title : null,
        marks: ev.filter(function (e) { return ['FOMC', 'WITCHING', 'OPEX', 'QUARTER_END', 'REBALANCE', 'VIX_EXPIRY'].indexOf(e.type) >= 0; }).map(function (e) { return e.type === 'FOMC' ? 'FOMC' : e.type === 'WITCHING' ? 'Quad witching' : e.type === 'OPEX' ? 'OPEX' : e.type === 'QUARTER_END' ? 'Quarter-end' : e.type === 'REBALANCE' ? 'Rebalance' : 'VIX expiry'; }) });
    }
    var week = ahead.filter(function (e) { return e.date <= addDays(today, 7) && e.impact === 'HIGH'; }).map(function (e, i) { return [e, i]; }).sort(function (a, b) { var x = a[0], y = b[0]; return x.date < y.date ? -1 : x.date > y.date ? 1 : ((y.country === 'US' || y.type !== 'ECON') - (x.country === 'US' || x.type !== 'ECON')) || a[1] - b[1]; }).map(function (p) { return p[0]; });
    var countries = {}; ahead.forEach(function (e) { if (e.type === 'ECON' && e.country) countries[e.country] = (countries[e.country] || 0) + 1; });
    function cd(e) { return e ? { title: e.title, date: e.date, days: daysBetween(today, e.date), time: e.time || null, sub: e.subtitle || '' } : null; }
    return { today: today, version: d.version, asOf: d.as_of, nAhead: ahead.length, nHigh7: week.length, byType: d.by_type_ahead || {}, cells: cells, week: week.slice(0, 14), weekMore: Math.max(0, week.length - 14),
      next: { fomc: cd(next(function (e) { return e.type === 'FOMC'; })), release: cd(next(function (e) { return e.type === 'ECON' && e.tier1; })), holiday: cd(next(function (e) { return e.type === 'MARKET_HOLIDAY'; })),
        opex: cd(next(function (e) { return e.type === 'OPEX' || e.type === 'WITCHING'; })), quarter: cd(next(function (e) { return e.type === 'QUARTER_END'; })), earnings: cd(next(function (e) { return e.type === 'EARNINGS' && e.impact === 'HIGH'; })) },
      countries: Object.keys(countries).sort(function (a, b) { return countries[b] - countries[a] || a.localeCompare(b); }).map(function (c) { return [c, countries[c]]; }),
      sources: (d.sources || []).filter(function (s) { return s && !s.events; }).map(function (s) { return s.source; }) };
  }

  var CATS = { market_indicator: 'Valuation', flow_indicator: 'Flow indicators', exchange_flows: 'Exchange flows', network_indicator: 'Network indicators', miner_flows: 'Miners', network_data: 'Network activity', market_data: 'Market data', stablecoins: 'Stablecoins', eth: 'Ethereum' };
  function cryptoModel(o, cq) {
    o = o || {}; cq = cq || {};
    var btc = o.btc || {}, cb = o.cycle_bands || {}, rb = cb.rainbow || {}, bands = Object.keys(rb.band_prices || {}).map(function (k) { return { k: parseFloat(k), price: rb.band_prices[k] }; }).filter(function (x) { return fin(x.k) && fin(x.price); }).sort(function (a, b) { return a.k - b.k; });
    var m = cq.metrics || {}, cats = Object.keys(CATS).map(function (c) {
      var rows = Object.keys(m).filter(function (k) { return m[k] && m[k].category === c; }).map(function (k) { var x = m[k]; return { id: k, label: x.label || k, value: x.value, unit: x.unit || '', pctl: x.pctl_1y, z: x.z365, asOf: x.as_of, wow: x.wow, read: x.hist_read || '', core: !!x.in_composite }; });
      return { id: c, name: CATS[c], rows: rows };
    }).filter(function (c) { return c.rows.length; });
    var all = []; cats.forEach(function (c) { c.rows.forEach(function (r) { all.push(r); }); });
    function fc(x) { return ['h30', 'h90', 'h180', 'h365'].map(function (h) { var f = (x || {})[h]; return f ? { h: +h.slice(1), exp: f.exp_pct, p10: f.p10_pct, p90: f.p90_pct, target: f.price_target, range: f.price_range, n: f.n_metrics } : null; }).filter(Boolean); }
    var f = cq.forecasts || {};
    return { asOf: o.generated_at, fit: fin(rb.slope_a) && fin(rb.intercept_b) && fin(rb.residual_sigma) && rb.history_starts ? { a: rb.slope_a, b: rb.intercept_b, sigma: rb.residual_sigma, start: rb.history_starts } : null, price: fin(cb.price) ? cb.price : btc.price, bandsAsOf: cb.as_of, band: rb.band, z: rb.z_sigma, fair: rb.fair_value, r2: rb.r_squared, bands: bands, history: (cb.history_weekly || []).filter(function (p) { return p && fin(p[1]); }),
      mvrv: btc.mvrv, nupl: btc.nupl, nuplZone: btc.nupl_zone, realized: btc.realized_price, vsRealized: btc.price_vs_realized_pct, fee: btc.fee_sat_vb, hash: btc.hash_rate_eh, mcap: btc.market_cap,
      mayer: cb.mayer_multiple || null, pi: cb.pi_cycle || null, ma200w: cb.ma_200_week || null, cqAsOf: cq.generated_at, cqStatus: cq.status, composite: cq.composite_onchain_risk_z, read: cq.read || '', brief: cq.ai_master_brief || '',
      cats: cats, nMetrics: all.length, high: all.filter(function (r) { return fin(r.pctl) && r.pctl >= 95; }), low: all.filter(function (r) { return fin(r.pctl) && r.pctl <= 5; }),
      fcBtc: fc(f.btc), fcEth: fc(f.eth), ethPrice: f.eth_price_now, btcPriceCq: f.btc_price_now, method: f.method || '', ethErrors: (o.eth || {}).errors || [], ethJoined: Object.keys((o.eth || {}).cryptoquant || {}).length };
  }

  /* ---------- drawing ---------- */
  var seq = 0, charts = {};
  /* a line chart with thresholds and bars; returns svg and registers its points for the hover readout */
  function chart(o) {
    var id = 'jgl-c' + (++seq), w = o.w || 720, h = o.h || 220, L = 46, R = 12, T = 12, B = 22, n = o.x.length, ys = [], i, s, fmt = o.fmt || function (v) { return num(v, 1); };
    (o.series || []).forEach(function (se) { se.y.forEach(function (v) { if (fin(v)) ys.push(v); }); });
    (o.bars || []).forEach(function (v) { if (fin(v)) ys.push(v); });
    (o.lines || []).forEach(function (l) { ys.push(l.y); });
    if (n < 2 || !ys.length) return '<div class="jgl-none">Not enough history to draw.</div>';
    var lo = o.min != null ? o.min : Math.min.apply(null, ys), hi = o.max != null ? o.max : Math.max.apply(null, ys), pad = (hi - lo) * 0.06 || 1;
    if (o.min == null) lo = Math.min.apply(null, ys) >= 0 ? Math.max(0, lo - pad) : lo - pad; if (o.max == null) hi += pad;
    function X(k) { return L + (k / (n - 1)) * (w - L - R); }
    function Y(v) { return T + (1 - (v - lo) / (hi - lo)) * (h - T - B); }
    var g = '';
    var ticks = [];
    if (o.ticks) ticks = o.ticks.filter(function (v) { return v >= lo && v <= hi; }); else for (i = 0; i <= 4; i++) ticks.push(lo + (hi - lo) * i / 4);
    for (i = 0; i < ticks.length; i++) L = Math.max(L, 10 + 6.3 * String(fmt(ticks[i])).length);
    for (i = 0; i < ticks.length; i++) { var gv = ticks[i]; g += '<line class="jgl-gline" x1="' + L + '" x2="' + (w - R) + '" y1="' + Y(gv).toFixed(1) + '" y2="' + Y(gv).toFixed(1) + '"/><text class="jgl-ax" x="' + (L - 6) + '" y="' + (Y(gv) + 3).toFixed(1) + '" text-anchor="end">' + esc(fmt(gv)) + '</text>'; }
    var step = Math.max(1, Math.round(n / 6));
    for (i = 0; i < n; i += step) g += '<text class="jgl-ax" x="' + X(i).toFixed(1) + '" y="' + (h - 6) + '" text-anchor="middle">' + esc((o.xfmt || nice)(o.x[i])) + '</text>';
    if (o.shade) g += '<rect class="jgl-shade" x="' + X(Math.max(0, o.shade[0])).toFixed(1) + '" y="' + T + '" width="' + (X(o.shade[1]) - X(Math.max(0, o.shade[0]))).toFixed(1) + '" height="' + (h - T - B) + '"/>';
    if (o.bars) { var bw = Math.max(2, (w - L - R) / n - 2), base = fin(o.barBase) ? o.barBase : lo; o.bars.forEach(function (v, k) { if (!fin(v)) return; var y0 = Y(base), y1 = Y(v); g += '<rect class="jgl-bar ' + (v >= base ? 'a' : 'b') + '" x="' + (X(k) - bw / 2).toFixed(1) + '" y="' + Math.min(y0, y1).toFixed(1) + '" width="' + bw.toFixed(1) + '" height="' + Math.max(1, Math.abs(y1 - y0) - 0).toFixed(1) + '" rx="1.5"/>'; }); }
    (o.lines || []).forEach(function (l) { g += '<line class="jgl-th ' + (l.cls || '') + '" x1="' + L + '" x2="' + (w - R) + '" y1="' + Y(l.y).toFixed(1) + '" y2="' + Y(l.y).toFixed(1) + '"/><text class="jgl-thl" x="' + (L + 6) + '" y="' + (Y(l.y) - 4).toFixed(1) + '">' + esc(l.label) + '</text>'; });
    (o.series || []).forEach(function (se, si) { var d = ''; se.y.forEach(function (v, k) { if (fin(v)) d += (d ? 'L' : 'M') + X(k).toFixed(1) + ' ' + Y(v).toFixed(1); }); g += '<path class="jgl-ln s' + si + (se.cls ? ' ' + se.cls : '') + '" d="' + d + '"/>'; if (!se.cls) s = se; });
    if (s) { for (i = s.y.length - 1; i >= 0; i--) if (fin(s.y[i])) { g += '<circle class="jgl-pt" cx="' + X(i).toFixed(1) + '" cy="' + Y(s.y[i]).toFixed(1) + '" r="4"/>'; break; } }
    charts[id] = { x: o.x, series: (o.series || []).filter(function (se) { return !se.quiet; }), bars: o.bars || null, barName: o.barName, fmt: fmt, L: L, R: R, w: w, n: n };
    return '<div class="jgl-chart" data-chart="' + id + '"><div class="jgl-read" aria-live="polite">' + esc(o.hint || 'Move over the chart to read a day.') + '</div><svg viewBox="0 0 ' + w + ' ' + h + '" role="img" aria-label="' + esc(o.label || '') + '">' + g + '<line class="jgl-cross" y1="' + T + '" y2="' + (h - B) + '" x1="-9" x2="-9"/></svg></div>';
  }
  function readout(id, k) { var c = charts[id]; if (!c) return ''; var parts = [nice(c.x[k]) + ', ' + String(c.x[k]).slice(0, 4)]; if (c.bars && fin(c.bars[k])) parts.push((c.barName || 'value') + ' ' + c.fmt(c.bars[k])); c.series.forEach(function (s) { if (fin(s.y[k])) parts.push(s.name + ' ' + c.fmt(s.y[k])); }); return parts.join(DOT); }
  function gauge(v, label, sub) {
    var a = fin(v) ? Math.max(0, Math.min(100, v)) / 100 : 0, R0 = 78, cx = 100, cy = 96, ang = Math.PI * (1 - a), x = cx + R0 * Math.cos(ang), y = cy - R0 * Math.sin(ang);
    return '<svg class="jgl-gauge" viewBox="0 0 200 124" role="img" aria-label="' + esc(label + ' ' + num(v, 1) + ' of 100') + '"><path class="jgl-arc" d="M22 96 A78 78 0 0 1 178 96"/><path class="jgl-arcv" d="M22 96 A78 78 0 0 1 ' + x.toFixed(1) + ' ' + y.toFixed(1) + '"/>'
      + [25, 50, 75].map(function (t) { var g = Math.PI * (1 - t / 100); return '<line class="jgl-tick" x1="' + (cx + 68 * Math.cos(g)).toFixed(1) + '" y1="' + (cy - 68 * Math.sin(g)).toFixed(1) + '" x2="' + (cx + 88 * Math.cos(g)).toFixed(1) + '" y2="' + (cy - 88 * Math.sin(g)).toFixed(1) + '"/>'; }).join('')
      + '<text class="jgl-gv" x="100" y="86" text-anchor="middle">' + esc(num(v, 1)) + '</text><text class="jgl-gl" x="100" y="108" text-anchor="middle">' + esc(label || '') + '</text><text class="jgl-ax" x="22" y="118" text-anchor="middle">0</text><text class="jgl-ax" x="178" y="118" text-anchor="middle">100</text></svg>' + (sub ? '<div class="jgl-sub">' + esc(sub) + '</div>' : '');
  }
  function tile(label, value, sub, cls) { return '<div class="jgl-tile' + (cls ? ' ' + cls : '') + '"><div class="jgl-k">' + esc(label) + '</div><div class="jgl-v">' + value + '</div>' + (sub ? '<div class="jgl-s">' + sub + '</div>' : '') + '</div>'; }
  function bar(pct, cls) { return '<span class="jgl-track"><span class="jgl-fill ' + (cls || '') + '" style="width:' + Math.max(0, Math.min(100, pct || 0)).toFixed(1) + '%"></span></span>'; }
  function heatAlpha(v) { return (0.08 + 0.72 * Math.max(0, Math.min(100, v)) / 100).toFixed(2); }
  function card(title, sub, body, cls) { return '<section class="jgl-card' + (cls ? ' ' + cls : '') + '"><header><h3>' + esc(title) + '</h3>' + (sub ? '<span>' + sub + '</span>' : '') + '</header>' + body + '</section>'; }
  function stamp(iso) { var m = /^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}:\d{2}))?/.exec(String(iso || '')); return m ? nice(m[1]) + (m[2] ? ', ' + m[2] + ' UTC' : '') : DASH; }

  function drawPlumbing(M) {
    var layers = M.layers.map(function (l) { return '<div class="jgl-row"><div class="jgl-rl"><b>' + esc(l.id) + '</b> ' + esc(l.name) + '<i>' + (fin(l.weight) ? Math.round(l.weight * 100) + '% weight' : '') + DOT + l.have + '/' + l.n + ' with data</i></div>' + bar(l.score, 'stress') + '<div class="jgl-rv">' + num(l.score, 1) + '</div></div>'; }).join('');
    var top = M.top.map(function (r) { return '<button type="button" class="jgl-row jgl-pick" data-ind="' + esc(r.id) + '"><div class="jgl-rl">' + esc(r.label) + '<i>' + esc(r.layer + DOT + LAYERS[r.layer]) + (fin(r.z) ? DOT + 'z ' + signed(r.z, 2) : '') + '</i></div>' + bar(r.stress, 'stress') + '<div class="jgl-rv">' + num(r.stress, 0) + '</div></button>'; }).join('');
    var heat = M.layers.map(function (l) { var rows = M.rows.filter(function (r) { return r.layer === l.id; }).sort(function (a, b) { return (fin(b.stress) ? b.stress : -1) - (fin(a.stress) ? a.stress : -1); }); return '<div class="jgl-hrow"><div class="jgl-hl"><b>' + esc(l.id) + '</b> ' + esc(l.name) + '</div><div class="jgl-heat">' + rows.map(function (r) { return '<button type="button" class="jgl-cell' + (fin(r.stress) ? '' : ' none') + '" data-ind="' + esc(r.id) + '" style="--al:' + (fin(r.stress) ? heatAlpha(r.stress) : 0) + '" title="' + esc(r.label + (fin(r.stress) ? ': stress ' + num(r.stress, 0) : ': no data')) + '">' + (fin(r.stress) ? num(r.stress, 0) : DASH) + '</button>'; }).join('') + '</div></div>'; }).join('');
    var sev = SEVERITY.filter(function (s) { return M.alertCounts[s]; }).map(function (s) { return '<span class="jgl-chip"><b>' + M.alertCounts[s] + '</b> ' + esc(s.toLowerCase()) + '</span>'; }).join('');
    var alerts = M.alerts.slice(0, 8).map(function (a) { return '<li><b>' + esc(a.label || a.indicator) + '</b><i>' + esc(a.severity || '') + DOT + esc(a.layer || '') + (fin(a.z_score) ? DOT + 'z ' + signed(a.z_score, 2) : '') + '</i><span>' + esc(a.message || '') + '</span></li>'; }).join('');
    return '<div class="jgl-grid g3">' + card('Composite stress', esc(stamp(M.asOf)), gauge(M.score, M.label || '', M.have + ' of ' + M.n + ' indicators with data' + (M.weighted !== null ? DOT + 'layers weighted: ' + num(M.weighted, 1) : '')))
      + card('The five layers', 'score 0\u2013100, higher is more stress', '<div class="jgl-rows">' + layers + '</div>')
      + card('Alerts', M.alerts.length + ' raised', '<div class="jgl-chips">' + (sev || '<span class="jgl-chip">none</span>') + '</div><ul class="jgl-list">' + alerts + '</ul>' + (M.alerts.length > 8 ? '<div class="jgl-sub">and ' + (M.alerts.length - 8) + ' more in the page below</div>' : '')) + '</div>'
      + '<div class="jgl-grid g2">' + card('Most stressed now', 'the twelve highest of ' + M.have, '<div class="jgl-rows">' + top + '</div>')
      + card('Every indicator', 'one tile each, its stress score; pick one to read it', '<div class="jgl-heats">' + heat + '</div><div class="jgl-detail" data-detail>Pick a tile or a row.</div>') + '</div>';
  }
  function wirePlumbing(host, M) {
    var by = {}; M.rows.forEach(function (r) { by[r.id] = r; });
    host.addEventListener('click', function (e) { var b = e.target.closest ? e.target.closest('[data-ind]') : null; if (!b) return; var r = by[b.getAttribute('data-ind')], d = host.querySelector('[data-detail]'); if (!r || !d) return;
      host.querySelectorAll('[data-ind].on').forEach(function (x) { x.classList.remove('on'); }); host.querySelectorAll('[data-ind="' + r.id + '"]').forEach(function (x) { x.classList.add('on'); });
      d.innerHTML = '<b>' + esc(r.label) + '</b><i>' + esc(r.id + DOT + r.layer + ' ' + LAYERS[r.layer] + (r.source ? DOT + r.source : '')) + '</i><div class="jgl-kv"><span>Value <b>' + num(r.value, Math.abs(r.value) >= 1000 ? 0 : 2) + '</b></span><span>Observed <b>' + esc(r.date || DASH) + '</b></span><span>Stress <b>' + num(r.stress, 1) + '</b></span><span>Percentile of its history <b>' + num(r.pct, 1) + '</b></span><span>z-score <b>' + signed(r.z, 2) + '</b></span></div><p>' + esc(r.interp) + '</p>'; });
  }

  function drawLiquidity(M) {
    var max = Math.max.apply(null, M.legs.map(function (l) { return fin(l.v) ? l.v : 0; }).concat([fin(M.net) ? M.net : 0])) || 1;
    var fall = M.legs.map(function (l) { return '<div class="jgl-row"><div class="jgl-rl">' + (l.sign < 0 ? MINUS + ' ' : '') + esc(l.name) + '<i>observed ' + esc(l.date || DASH) + '</i></div>' + bar(100 * (fin(l.v) ? l.v : 0) / max, l.sign < 0 ? 'minus' : 'plus') + '<div class="jgl-rv">' + (fin(l.v) ? '$' + num(l.v, 1) + 'bn' : DASH) + '</div></div>'; }).join('')
      + '<div class="jgl-row total"><div class="jgl-rl">= Net liquidity<i>' + (M.adds ? 'the three legs add to it' : 'the legs do not add to the total shown') + '</i></div>' + bar(100 * (fin(M.net) ? M.net : 0) / max, 'net') + '<div class="jgl-rv">' + (fin(M.net) ? '$' + num(M.net, 1) + 'bn' : DASH) + '</div></div>';
    var names = { '1d': 'Day', '1w': 'Week', '1m': 'Month', '3m': 'Three months' };
    var win = '<table class="jgl-t"><thead><tr><th>Change over</th><th>Net</th><th>Fed balance sheet</th><th>Treasury account</th><th>Reverse repo</th></tr></thead><tbody>' + M.windows.map(function (x) { return '<tr><td>' + names[x.w] + '<i>from ' + esc(nice(x.from)) + '</i></td>' + ['net', 'walcl', 'tga', 'rrp'].map(function (k) { return '<td class="' + tone(x[k]) + '">' + signed(x[k], fin(x[k]) && x[k] !== 0 && Math.abs(x[k]) < 1 ? 2 : 1) + '</td>'; }).join('') + '</tr>'; }).join('') + '</tbody></table><div class="jgl-sub">$ billions. A larger Treasury account or reverse repo balance takes liquidity out, so its rise lowers the net.</div>';
    var x = M.hist.map(function (r) { return r.date; });
    var main = chart({ x: x, series: [{ name: 'Net liquidity', y: M.hist.map(function (r) { return r.net; }) }], fmt: function (v) { return '$' + num(v, 0) + 'bn'; }, label: 'Net liquidity, 180 days', h: 230 });
    var small = [['walcl', 'Fed balance sheet'], ['tga', 'Treasury General Account'], ['rrp', 'Reverse repo']].map(function (p) { return '<div class="jgl-mini"><div class="jgl-k">' + p[1] + '</div>' + chart({ x: x, series: [{ name: p[1], y: M.hist.map(function (r) { return r[p[0]]; }) }], fmt: function (v) { return '$' + num(v, Math.abs(v) < 100 ? 1 : 0) + 'bn'; }, w: 380, h: 130, hint: ' ', label: p[1] + ', 180 days' }) + '</div>'; }).join('');
    var groups = M.groups.map(function (g) { return '<details class="jgl-grp"' + (['reserves', 'funding'].indexOf(g.id) >= 0 ? ' open' : '') + '><summary><b>' + esc(g.name) + '</b><i>' + g.rows.length + ' series</i></summary><table class="jgl-t"><thead><tr><th>Series</th><th>Latest</th><th>Month</th><th>Quarter</th><th>Year</th></tr></thead><tbody>'
      + g.rows.map(function (r) { return '<tr><td title="' + esc(r.id) + '">' + esc(r.label) + '<i>' + esc(r.value === null && !r.date ? 'nothing kept for this series today' : r.unit + DOT + (r.date || '')) + '</i></td><td>' + num(r.value, Math.abs(r.value) >= 1000 ? 0 : 2) + '</td>' + ['m1', 'q1', 'y1'].map(function (k) { return '<td class="' + tone(r[k]) + '">' + signed(r[k], Math.abs(r[k]) >= 1000 ? 0 : 2) + (fin(r[k + 'p']) ? '<i>' + signed(r[k + 'p'], 1, '%') + '</i>' : '') + '</td>'; }).join('') + '</tr>'; }).join('') + '</tbody></table></details>'; }).join('');
    return '<div class="jgl-grid g2w">' + card('Net liquidity', 'valued ' + esc(nice(M.valued)), '<div class="jgl-hero"><div class="jgl-big">' + (fin(M.net) ? '$' + num(M.net, 1) + '<small>bn</small>' : DASH) + '</div><div class="jgl-chips">' + M.windows.map(function (w) { return '<span class="jgl-chip"><i>' + names[w.w] + '</i> <b class="' + tone(w.net) + '">' + signed(w.net, fin(w.net) && w.net !== 0 && Math.abs(w.net) < 1 ? 2 : 1) + '</b></span>'; }).join('') + '</div></div>'
        + (M.place !== null ? '<div class="jgl-range"><span>180-day low $' + num(M.lo, 0) + 'bn<i>' + esc(nice(M.loDate)) + '</i></span>' + bar(100 * M.place, 'net') + '<span>high $' + num(M.hi, 0) + 'bn<i>' + esc(nice(M.hiDate)) + '</i></span></div>' : '') + '<div class="jgl-rows">' + fall + '</div><div class="jgl-sub">' + esc(M.formula || '') + DOT + esc(M.note || '') + '</div>')
      + card('180 days', 'net liquidity, $ billions', main + '<div class="jgl-h2">What moved it <i>each leg, four windows</i></div><div class="jgl-scroll">' + win + '</div>') + '</div>'
      + card('The three legs', '180 days each', '<div class="jgl-minis">' + small + '</div>')
      + (M.groups.length ? card('Around the system', M.nSeries + ' series the liquidity agent keeps, by group; change is the reported level against a month, a quarter and a year before', '<div class="jgl-grps">' + groups + '</div>') : '');
  }

  function drawBreadth(M) {
    var steps = [['NULL', 'Waiting', 'no oversold reading in the window'], ['ARMED', 'Armed', 'the average dipped under ' + M.low + '; it fires if it now climbs over ' + M.high], ['FIRED', 'Fired', 'from under ' + M.low + ' to over ' + M.high + ' inside ten sessions']];
    var at = M.state === 'FIRED' ? 2 : M.state === 'ARMED' ? 1 : 0;
    var stepper = '<ol class="jgl-steps">' + steps.map(function (s, i) { return '<li class="' + (i === at ? 'on' : i < at ? 'done' : '') + '"><b>' + s[1] + '</b><i>' + esc(s[2]) + '</i></li>'; }).join('') + '</ol>';
    var x = M.rows.map(function (r) { return r.date; }), n = M.rows.length;
    var main = chart({ x: x, bars: M.rows.map(function (r) { return r.ratio; }), barBase: 0.5, barName: 'day', series: (M.emaDod ? [{ name: 'day-over-day 10-day average', y: M.emaDod, cls: 'alt' }] : []).concat([{ name: '10-day average', y: M.ema }]), lines: [{ y: M.high, label: 'thrust ' + M.high, cls: 'hi' }, { y: M.low, label: 'oversold ' + M.low, cls: 'lo' }], shade: [n - 11, n - 1], min: 0.15, max: 0.85, fmt: function (v) { return num(v, 3); }, label: 'Advancing share of issues and its 10-day average', w: 1100, h: 300, hint: 'Bars: each day\u2019s advancing share (above the middle line more stocks rose than fell). Line: its 10-day average. Shaded: the ten-session window.' + (M.emaDod ? ' Thin dashed line: the same average on the day-over-day basis.' : '') });
    var mcc = M.mcclellan && M.mcclellan.series && M.mcclellan.series.length > 5 ? chart({ x: M.mcclellan.series.map(function (p) { return p[0]; }), bars: M.mcclellan.series.map(function (p) { return p[1]; }), barBase: 0, barName: 'oscillator', lines: [{ y: 0, label: '' }], fmt: function (v) { return signed(v, 1); }, label: 'McClellan oscillator', w: 1100, h: 200, hint: 'Above zero: the faster average of net advances is over the slower one.' }) : '';
    var fmax = Math.max.apply(null, M.fwd.map(function (f) { return fin(f.best) ? f.best : 0; }).concat([1]));
    var fwd = M.fwd.map(function (f) { if (!fin(f.mean)) return ''; var a = 100 * Math.max(0, f.worst) / fmax, b = 100 * f.best / fmax, m = 100 * f.mean / fmax; return '<div class="jgl-row"><div class="jgl-rl">' + ({ '1m': 'One month', '3m': 'Three months', '6m': 'Six months', '12m': 'Twelve months' })[f.h] + '<i>' + (fin(f.win) ? num(f.win, 0) + '% positive of ' + f.n : '') + '</i></div><span class="jgl-track"><span class="jgl-span" style="left:' + a.toFixed(1) + '%;width:' + Math.max(1, b - a).toFixed(1) + '%"></span><span class="jgl-dot" style="left:' + m.toFixed(1) + '%"></span></span><div class="jgl-rv">' + signed(f.mean, 1, '%') + '<i>' + signed(f.worst, 1, '%') + ' to ' + signed(f.best, 1, '%') + '</i></div></div>'; }).join('');
    var ep = '<table class="jgl-t"><thead><tr><th>Fired</th><th>SPY then</th><th>1 month</th><th>3 months</th><th>6 months</th><th>12 months</th></tr></thead><tbody>' + M.episodes.map(function (e) { return '<tr><td>' + esc(e.date) + '<i>' + esc(e.label || '') + '</i></td><td>' + num(e.spy_at_trigger, 2) + '</td>' + ['fwd_1m_pct', 'fwd_3m_pct', 'fwd_6m_pct', 'fwd_12m_pct'].map(function (k) { return '<td class="' + tone(e[k]) + '">' + signed(e[k], 1, '%') + '</td>'; }).join('') + '</tr>'; }).join('') + '</tbody></table>';
    var word = function (v) { return esc(String(v || DASH).replace(/_/g, ' ')); };
    var sup = (M.whaley ? tile('Whaley January barometer', word(M.whaley.state), fin(M.whaley.first_5d_return_pct) ? 'first five days of ' + esc(M.whaley.year) + ': ' + signed(M.whaley.first_5d_return_pct, 2, '%') + (fin(M.whaley.historical_hit_rate_pct) ? DOT + 'right ' + num(M.whaley.historical_hit_rate_pct, 0) + '% of years' : '') : 'no reading in the document') : '') + (M.coppock ? tile('Coppock curve', word(M.coppock.state), fin(M.coppock.current_value) ? num(M.coppock.current_value, 2) + (fin(M.coppock.previous_value) ? ' from ' + num(M.coppock.previous_value, 2) : '') + DOT + (M.coppock.fresh_buy_signal ? 'fresh buy signal' : 'no fresh buy signal') : 'no reading in the document') : '');
    return '<div class="jgl-grid g2w">' + card('Zweig breadth thrust', 'since ' + esc(stamp(M.since)), stepper + '<div class="jgl-tiles two top">' + tile('10-day average now', num(M.emaNow, 4), M.agrees === true ? 'recomputed here from the 60 days: the same' : M.agrees === false ? 'recomputed here it differs' : '') + tile('Needs', 'over ' + M.high, fin(M.toFire) ? (M.toFire > 0 ? num(M.toFire, 4) + ' to go' : 'reached') : '') + tile('Lowest in the window', num(M.winMin, 4), M.winMin < M.low ? 'under ' + M.low + ': oversold seen' : 'never under ' + M.low) + tile('Days more up than down', M.up + ' of ' + M.n, 'in the sessions the state reads') + (M.lastCount ? tile('Rose / fell, ' + nice(M.lastCount.date), group3(String(M.lastCount.adv)) + ' / ' + group3(String(M.lastCount.dec)), 'against the open' + DOT + group3(String(M.lastCount.unch || 0)) + ' unchanged') + tile('Volume in rising issues', fin(M.lastCount.up_vol_share) ? num(100 * M.lastCount.up_vol_share, 1) + '%' : DASH, 'counts kept for ' + M.counted + ' of ' + M.n + ' sessions') : '')
        + (M.lastCount && fin(M.lastCount.adv_dod) ? tile('Advanced / declined', group3(String(M.lastCount.adv_dod)) + ' / ' + group3(String(M.lastCount.dec_dod)), 'against the prior close' + DOT + group3(String(M.lastCount.unch_dod || 0)) + ' unchanged' + (M.lastCount.set_aside_dod ? DOT + M.lastCount.set_aside_dod + ' set aside as likely splits' : '')) : '')
        + (M.dod ? tile('Day-over-day average', num(M.dod.ema_10d, 4), 'Zweig\u2019s own basis' + DOT + 'would read ' + String(M.dod.would_read || '').toLowerCase() + DOT + 'moves no state') : '')
        + (M.mcclellan ? tile('McClellan oscillator', signed(M.mcclellan.oscillator, 1), 'was ' + signed(M.mcclellan.previous, 1) + DOT + M.mcclellan.n_sessions + ' sessions' + (M.mcclellan.seasoned ? '' : ', still warming up')) : '')
        + (M.preliminary ? tile('Today so far, ' + nice(M.preliminary.session), num(M.preliminary.ratio, 4), 'preliminary, delayed snapshot' + DOT + 'average would be ' + num(M.preliminary.ema_10d_with_it, 4) + DOT + 'moves no state until settled') : '')
        + '</div>')
      + card('What history did after it fired', M.episodes.length + ' times in the engine\u2019s record; SPY afterwards', (M.episodes.length ? '<div class="jgl-rows">' + fwd + '</div><div class="jgl-sub">Dot: the average. Band: worst to best. A handful of cases, not a forecast.</div><div class="jgl-h2">Each time it fired <i>SPY afterwards</i></div><div class="jgl-scroll">' + ep + '</div>' : '<div class="jgl-none">The document carries no past firings today.</div>')
        + (sup ? '<div class="jgl-h2">Beside it <i>two slower readings the engine keeps</i></div><div class="jgl-tiles two">' + sup + '</div>' : '') + (M.basis ? '<div class="jgl-sub">' + esc(M.basis) + '</div>' : '')) + '</div>'
      + card('The last ' + M.n + ' sessions', 'share of issues advancing', main)
      + (mcc ? card('McClellan oscillator', esc(M.mcclellan.basis || ''), mcc + (M.mcclellan.warm_up_note ? '<div class="jgl-sub">' + esc(M.mcclellan.warm_up_note) + '</div>' : '')) : '');
  }

  function drawCalendar(M) {
    function cdTile(label, c) { return c ? tile(label, c.days === 0 ? 'Today' : c.days === 1 ? 'Tomorrow' : 'in ' + c.days + ' days', esc(c.title) + '<br>' + esc(DOW[new Date(utc(c.date)).getUTCDay()] + ' ' + nice(c.date)), 'jgl-go" data-day="' + c.date) : tile(label, DASH, 'none in the window'); }
    var heads = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map(function (d) { return '<div class="jgl-dh">' + d + '</div>'; }).join('');
    var grid = M.cells.map(function (c) { return '<button type="button" class="jgl-day' + (c.past ? ' past' : '') + (c.today ? ' today' : '') + (c.holiday ? ' hol' : '') + (c.dow === 0 || c.dow === 6 ? ' we' : '') + '" data-day="' + c.date + '"' + (c.n ? '' : ' disabled') + ' title="' + esc(c.date + ': ' + c.n + ' events' + (c.high ? ', ' + c.high + ' high impact' : '')) + '"><span class="jgl-dn">' + (c.dom === 1 || c.today ? c.month + ' ' : '') + c.dom + '</span>'
      + (c.holiday ? '<span class="jgl-mk h">' + esc(c.holiday.replace(/ \((market closed|early close)\)/, function (m, w) { return w === 'early close' ? ' (early)' : ''; })) + '</span>' : '') + c.marks.map(function (m) { return '<span class="jgl-mk">' + esc(m) + '</span>'; }).join('')
      + (c.n ? '<span class="jgl-dc">' + (c.high ? '<b>' + c.high + ' high</b> ' : '') + c.n + ' in all</span>' : '') + '</button>'; }).join('');
    var week = M.week.map(function (e) { return '<li data-day="' + e.date + '"><b>' + esc(DOW[new Date(utc(e.date)).getUTCDay()] + ' ' + nice(e.date)) + '</b><span>' + esc(e.title) + '</span><i>' + esc(e.subtitle || '') + '</i></li>'; }).join('');
    return '<div class="jgl-tiles six">' + cdTile('Next Fed decision', M.next.fomc) + cdTile('Next major release', M.next.release) + cdTile('Next large earnings', M.next.earnings) + cdTile('Next options expiry', M.next.opex) + cdTile('Next market holiday', M.next.holiday) + cdTile('Quarter-end', M.next.quarter) + '</div>'
      + '<div class="jgl-grid g2c">' + card('Five weeks ahead', M.nAhead + ' events ahead; pick a day to jump to it', '<div class="jgl-cal">' + heads + grid + '</div>')
      + card('High impact, next seven days', M.nHigh7 + ' events', (week ? '<ul class="jgl-list go">' + week + '</ul>' : '<div class="jgl-none">Nothing high impact in the next seven days.</div>') + (M.weekMore ? '<div class="jgl-sub">and ' + M.weekMore + ' more in the list below</div>' : '')) + '</div>';
  }
  function wireCalendar(host) {
    host.addEventListener('click', function (e) { var b = e.target.closest ? e.target.closest('[data-day]') : null; if (!b) return; var doc = host.ownerDocument, iso = b.getAttribute('data-day');
      if (doc.defaultView && typeof doc.defaultView.jhCalendarShowDay === 'function') doc.defaultView.jhCalendarShowDay(iso);
      var g = doc.querySelector('.day-group[data-date="' + iso + '"]'); if (g) { g.scrollIntoView({ behavior: 'smooth', block: 'start' }); g.classList.add('jgl-flash'); setTimeout(function () { g.classList.remove('jgl-flash'); }, 1600); } });
  }

  function drawCrypto(M) {
    var lo = M.bands.length ? Math.log(M.bands[0].price) : 0, hi = M.bands.length ? Math.log(M.bands[M.bands.length - 1].price) : 1;
    function pos(p) { return 100 * Math.max(0, Math.min(1, (Math.log(p) - lo) / (hi - lo))); }
    var names = { '-2': 'Deep value', '-1.25': 'Accumulate', '-0.5': 'Cheap', '0': 'Fair value', '0.5': 'Warm', '1.25': 'Hot', '2': 'Euphoria' };
    var ladder = M.bands.length && fin(M.price) ? '<div class="jgl-ladder"><div class="jgl-lbar">' + M.bands.map(function (b, i) { return i ? '<span class="jgl-lseg" style="left:' + pos(M.bands[i - 1].price).toFixed(2) + '%;width:' + (pos(b.price) - pos(M.bands[i - 1].price)).toFixed(2) + '%;--al:' + (0.14 + i * 0.11).toFixed(2) + '"></span>' : ''; }).join('') + '<span class="jgl-lnow" style="left:' + pos(M.price).toFixed(2) + '%"><b>' + usd(M.price) + '</b></span></div><div class="jgl-lticks">' + M.bands.map(function (b) { return '<span style="left:' + pos(b.price).toFixed(2) + '%"><b>' + usd(b.price) + '</b><i>' + esc(names[String(b.k)] || signed(b.k, 2) + '\u03C3') + '</i></span>'; }).join('') + '</div></div>' : '<div class="jgl-none">The bands are not in today\u2019s document.</div>';
    var hist = '';
    if (M.history.length > 60) {
      var hx = M.history.map(function (p) { return p[0]; }), lines = [];
      if (M.fit) M.bands.forEach(function (b) { lines.push({ name: names[String(b.k)] || '', quiet: true, cls: 'band' + (b.k === 0 ? ' mid' : ''), y: hx.map(function (d) { var t = daysBetween(M.fit.start, d) + 1; return t > 0 ? (M.fit.a * Math.log(t) + M.fit.b + b.k * M.fit.sigma) / Math.LN10 : null; }) }); });
      lines.push({ name: 'Bitcoin', y: M.history.map(function (p) { return Math.log(p[1]) / Math.LN10; }) });
      hist = chart({ x: hx, series: lines, fmt: function (v) { return usd(Math.pow(10, v)); }, ticks: [-1, 0, 1, 2, 3, 4, 5, 6, 7], xfmt: function (d) { return String(d).slice(0, 4); }, label: 'Bitcoin weekly close on its log trend, with the bands', h: 300, hint: 'Weekly close on a log scale. Thin lines: the seven bands; dashed: the trend itself.' });
    }
    function fcRows(rows, price) { var all = []; rows.forEach(function (f) { all.push(f.p10, f.p90); }); var mn = Math.min.apply(null, all.concat([0])), mx = Math.max.apply(null, all.concat([0])), sp = (mx - mn) || 1; function P(v) { return 100 * (v - mn) / sp; }
      return rows.map(function (f) { return '<div class="jgl-row"><div class="jgl-rl">' + f.h + ' days<i>' + (fin(f.n) ? f.n + ' metrics' : '') + '</i></div><span class="jgl-track"><span class="jgl-zero" style="left:' + P(0).toFixed(1) + '%"></span><span class="jgl-span" style="left:' + P(f.p10).toFixed(1) + '%;width:' + Math.max(1, P(f.p90) - P(f.p10)).toFixed(1) + '%"></span><span class="jgl-dot" style="left:' + P(f.exp).toFixed(1) + '%"></span></span><div class="jgl-rv ' + tone(f.exp) + '">' + signed(f.exp, 1, '%') + '<i>' + signed(f.p10, 1, '%') + ' to ' + signed(f.p90, 1, '%') + (fin(f.target) ? DOT + usd(f.target) : '') + '</i></div></div>'; }).join(''); }
    var cats = M.cats.map(function (c) { return '<div class="jgl-hrow"><div class="jgl-hl"><b>' + esc(c.name) + '</b><i>' + c.rows.length + '</i></div><div class="jgl-heat">' + c.rows.map(function (r) { var p = fin(r.pctl) ? r.pctl : null; return '<button type="button" class="jgl-cell pc' + (p === null ? ' none' : p >= 50 ? ' hi' : ' lo') + '" data-met="' + esc(r.id) + '" style="--al:' + (p === null ? 0 : (0.06 + 0.7 * Math.abs(p - 50) / 50).toFixed(2)) + '" title="' + esc(r.label + (p === null ? '' : ': ' + p + 'th percentile of its year')) + '">' + (p === null ? DASH : p) + '</button>'; }).join('') + '</div></div>'; }).join('');
    function ext(rows) { return rows.slice(0, 10).map(function (r) { return '<span class="jgl-chip" data-met="' + esc(r.id) + '">' + esc(r.label) + ' <b>' + r.pctl + '</b></span>'; }).join('') || '<span class="jgl-chip">none</span>'; }
    return '<div class="jgl-tiles six">' + tile('Bitcoin', usd(M.price), 'close of ' + esc(nice(M.bandsAsOf))) + tile('Against its log trend', esc(M.band || DASH), fin(M.z) ? signed(M.z, 2) + ' sigma' + DOT + 'trend ' + usd(M.fair) : '')
      + tile('MVRV', num(M.mvrv, 2), 'price ' + signed(M.vsRealized, 1, '%') + ' against the average coin\u2019s cost of ' + usd(M.realized)) + tile('Unrealised profit (NUPL)', num(M.nupl, 3), esc(M.nuplZone || ''))
      + tile('Mayer multiple', M.mayer ? num(M.mayer.value, 3) : DASH, M.mayer ? esc(M.mayer.read || '') + DOT + 'lower than ' + num(100 - M.mayer.percentile_all_history, 0) + '% of its history' : '') + tile('200-week average', M.ma200w ? usd(M.ma200w.value) : DASH, M.ma200w ? 'price is ' + signed(M.ma200w.pct_above, 1, '%') + ' from it' : '') + '</div>'
      + card('Where the price sits', 'bands around a log-trend fit of its own history (R\u00B2 ' + num(M.r2, 3) + '); a description, never a target', ladder + hist + (M.pi ? '<div class="jgl-sub">Pi cycle: the 111-day average is ' + signed(M.pi.distance_to_trigger_pct, 1, '%') + ' from twice the 350-day average (' + esc(M.pi.signal || '') + '); it has marked ' + esc(M.pi.historical_n) + ' tops.</div>' : ''))
      + '<div class="jgl-grid g2">' + card('Bitcoin: what similar readings were followed by', M.fcBtc.length ? 'average with the middle 80% of cases' : '', M.fcBtc.length ? '<div class="jgl-rows">' + fcRows(M.fcBtc) + '</div>' : '<div class="jgl-none">Not in today\u2019s document.</div>')
      + card('Ethereum: the same', fin(M.ethPrice) ? 'price ' + usd(M.ethPrice) : '', M.fcEth.length ? '<div class="jgl-rows">' + fcRows(M.fcEth) + '</div>' : '<div class="jgl-none">Not in today\u2019s document.</div>') + '</div>'
      + '<div class="jgl-sub pad">' + esc(M.method ? 'How these are made: ' + M.method : 'Conditional history from the on-chain metrics; provisional, not a forecast to trade on.') + '</div>'
      + card('On-chain metrics', M.nMetrics + ' kept from CryptoQuant (' + esc(String(M.cqStatus || '').toLowerCase()) + ', ' + esc(stamp(M.cqAsOf)) + '); each tile is the metric\u2019s place in its own last year, 0 lowest, 100 highest',
        '<div class="jgl-ext"><div><i>At a one-year high</i>' + ext(M.high) + '</div><div><i>At a one-year low</i>' + ext(M.low) + '</div></div><div class="jgl-heats">' + cats + '</div><div class="jgl-detail" data-detail>Pick a tile to read the metric.</div><div class="jgl-sub">' + esc(M.read) + '</div>');
  }
  function wireCrypto(host, M) {
    var by = {}; M.cats.forEach(function (c) { c.rows.forEach(function (r) { by[r.id] = r; r.cat = c.name; }); });
    host.addEventListener('click', function (e) { var b = e.target.closest ? e.target.closest('[data-met]') : null; if (!b) return; var r = by[b.getAttribute('data-met')], d = host.querySelector('[data-detail]'); if (!r || !d) return;
      host.querySelectorAll('[data-met].on').forEach(function (x) { x.classList.remove('on'); }); host.querySelectorAll('.jgl-cell[data-met="' + r.id + '"]').forEach(function (x) { x.classList.add('on'); });
      d.innerHTML = '<b>' + esc(r.label) + '</b><i>' + esc(r.id + DOT + r.cat + (r.core ? DOT + 'one of the eight core metrics' : '')) + '</i><div class="jgl-kv"><span>Value <b>' + num(r.value, Math.abs(r.value) >= 1000 ? 0 : 4) + ' ' + esc(r.unit) + '</b></span><span>Observed <b>' + esc(r.asOf || DASH) + '</b></span><span>Place in its year <b>' + (fin(r.pctl) ? r.pctl + ' of 100' : DASH) + '</b></span><span>z-score, one year <b>' + signed(r.z, 2) + '</b></span><span>Change on the week <b>' + signed(r.wow, Math.abs(r.wow) >= 1000 ? 0 : 4) + '</b></span></div><p>' + esc(r.read) + '</p>'; d.scrollIntoView({ block: 'nearest' }); });
  }

  var ADAPTERS = {
    plumbing: { title: 'Plumbing at a glance', docs: ['/data/plumbing-stress.json'], model: function (d) { return plumbingModel(d[0]); }, draw: drawPlumbing, wire: wirePlumbing },
    liquidity: { title: 'Liquidity at a glance', docs: ['/data/liquidity-flow.json', PROXY + 'liquidity-data.json'], optional: 1, model: function (d) { return liquidityModel(d[0], d[1]); }, draw: drawLiquidity },
    breadth: { title: 'Breadth at a glance', docs: ['/data/breadth-thrust.json', '/data/breadth-history.json'], model: function (d) { return breadthModel(d[0], d[1]); }, draw: drawBreadth },
    calendar: { title: 'The calendar at a glance', docs: ['/data/catalyst-calendar.json'], model: function (d, now) { return calendarModel(d[0], now); }, draw: drawCalendar, wire: wireCalendar },
    crypto: { title: 'Crypto at a glance', docs: ['/data/onchain-ratios.json', '/data/cryptoquant-onchain.json'], optional: 1, model: function (d) { return cryptoModel(d[0], d[1]); }, draw: drawCrypto, wire: wireCrypto }
  };

  var CSS = '.jgl{--b:#0C0B09;--p:#12110C;--p2:#17150E;--i:#e8e2d4;--d:#8a836f;--a:#F0B429;--g:#6fce8a;--r:#E07A6A;--l:#2B2820;--c:#6aa6d8;--m:ui-monospace,SFMono-Regular,Menlo,monospace;box-sizing:border-box;max-width:1240px;margin:14px auto 26px;padding:0 16px;color:var(--i);font:13px/1.45 system-ui,-apple-system,sans-serif;text-align:left}'
    + '.jgl *{box-sizing:border-box}.jgl-top{display:flex;gap:12px;align-items:baseline;flex-wrap:wrap;margin:0 0 10px}.jgl-top h2{font-size:18px;margin:0;color:var(--i);font-weight:700;letter-spacing:0}.jgl-top span{color:var(--d);font-size:12px}'
    + '.jgl-grid{display:grid;gap:12px;margin:0 0 12px}.jgl-grid.g3{grid-template-columns:minmax(220px,.8fr) 1.2fr 1.2fr}.jgl-grid.g2{grid-template-columns:1fr 1fr}.jgl-grid.g2w{grid-template-columns:minmax(300px,.9fr) 1.4fr}.jgl-grid.g2c{grid-template-columns:1.5fr minmax(280px,1fr)}'
    + '.jgl-card{background:var(--p);border:1px solid var(--l);border-radius:10px;padding:12px 14px;min-width:0;margin:0 0 12px}.jgl-grid>.jgl-card{margin:0}.jgl-card>header{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap;margin:0 0 10px}.jgl-card h3{font-size:13.5px;margin:0;font-weight:700;color:var(--i);text-transform:none;letter-spacing:0}.jgl-card>header span{color:var(--d);font-size:11.5px}'
    + '.jgl-gauge{display:block;width:100%;max-width:260px;margin:0 auto}.jgl-arc{fill:none;stroke:var(--l);stroke-width:14;stroke-linecap:round}.jgl-arcv{fill:none;stroke:var(--a);stroke-width:14;stroke-linecap:round}.jgl-tick{stroke:var(--b);stroke-width:2}.jgl-gv{fill:var(--i);font:700 34px var(--m)}.jgl-gl{fill:var(--a);font:700 12px system-ui;letter-spacing:.08em}.jgl-ax{fill:var(--d);font:10px var(--m)}'
    + '.jgl-sub{color:var(--d);font-size:11.5px;margin-top:8px}.jgl-sub.pad{margin:-4px 2px 12px}.jgl-none{color:var(--d);padding:12px 0}'
    + '.jgl-rows{display:flex;flex-direction:column;gap:7px}.jgl-row{display:grid;grid-template-columns:minmax(150px,1.15fr) 1.4fr auto;gap:10px;align-items:center;background:none;border:0;color:inherit;font:inherit;text-align:left;padding:2px 4px;margin:0;width:100%;border-radius:6px}button.jgl-row{cursor:pointer}button.jgl-row:hover,.jgl-row.on{background:var(--p2)}.jgl-row.total{border-top:1px solid var(--l);padding-top:8px;font-weight:700}'
    + '.jgl-rl{min-width:0;font-size:12.5px;overflow:hidden;text-overflow:ellipsis}.jgl-rl i,.jgl-rv i,.jgl-t td i,.jgl-range i{display:block;font-style:normal;color:var(--d);font-size:10.5px;font-weight:400;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.jgl-rv{font-family:var(--m);font-size:13px;text-align:right;min-width:64px}'
    + '.jgl-track{position:relative;display:block;height:10px;background:var(--p2);border-radius:5px;overflow:hidden;min-width:60px}.jgl-fill{display:block;height:100%;border-radius:5px;background:var(--a)}.jgl-fill.minus{background:var(--c)}.jgl-fill.plus{background:var(--d)}.jgl-fill.net{background:var(--a)}'
    + '.jgl-span{position:absolute;top:2px;height:6px;border-radius:3px;background:var(--d);opacity:.7}.jgl-dot{position:absolute;top:0;width:10px;height:10px;margin-left:-5px;border-radius:50%;background:var(--a);box-shadow:0 0 0 2px var(--p)}.jgl-zero{position:absolute;top:0;bottom:0;width:1px;background:var(--i);opacity:.5}'
    + '.jgl .up{color:var(--g)}.jgl .dn{color:var(--r)}.jgl-chips{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 8px}.jgl-chip{background:var(--p2);border:1px solid var(--l);border-radius:14px;padding:2px 9px;font-size:11.5px;white-space:nowrap}.jgl-chip i{font-style:normal;color:var(--d)}.jgl-chip b{font-family:var(--m)}.jgl-chip[data-met]{cursor:pointer}'
    + '.jgl-list{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:7px}.jgl-list li{display:grid;grid-template-columns:1fr;font-size:12px;line-height:1.35}.jgl-list li i{font-style:normal;color:var(--d);font-size:10.5px}.jgl-list li span{color:var(--i)}.jgl-list.go li{cursor:pointer;grid-template-columns:86px 1fr;column-gap:10px}.jgl-list.go li i{grid-column:2}.jgl-list.go li:hover span{color:var(--a)}'
    + '.jgl-heats{display:flex;flex-direction:column;gap:8px}.jgl-hrow{display:grid;grid-template-columns:150px 1fr;gap:10px;align-items:start}.jgl-hl{font-size:11.5px;color:var(--d)}.jgl-hl b{color:var(--i)}.jgl-hl i{font-style:normal;margin-left:6px}.jgl-heat{display:flex;flex-wrap:wrap;gap:3px}'
    + '.jgl-cell{width:34px;height:26px;border:1px solid var(--l);border-radius:4px;background:rgba(240,180,41,var(--al));color:var(--i);font:11px var(--m);cursor:pointer;padding:0;margin:0}.jgl-cell.pc.lo{background:rgba(106,166,216,var(--al))}.jgl-cell.none{color:var(--d);background:transparent}.jgl-cell:hover,.jgl-cell.on{outline:2px solid var(--i);outline-offset:-1px}'
    + '.jgl-detail{margin-top:10px;padding:10px 12px;background:var(--p2);border:1px solid var(--l);border-radius:8px;font-size:12px;color:var(--d);min-height:44px}.jgl-detail b{color:var(--i);font-size:13px}.jgl-detail>i{display:block;font-style:normal;font-size:10.5px;margin:1px 0 6px}.jgl-detail p{margin:6px 0 0;color:var(--i)}.jgl-kv{display:flex;flex-wrap:wrap;gap:6px 16px}.jgl-kv b{font-family:var(--m);font-size:12px}'
    + '.jgl-chart{position:relative}.jgl-chart svg{display:block;width:100%;height:auto}.jgl-read{font-size:11.5px;color:var(--d);min-height:18px;margin:0 0 2px;font-family:var(--m)}.jgl-gline{stroke:var(--l);stroke-width:1}.jgl-ln{fill:none;stroke:var(--a);stroke-width:2;stroke-linejoin:round}.jgl-ln.alt{stroke:var(--c);stroke-width:1.5;stroke-dasharray:5 4}.jgl-ln.band{stroke:var(--d);stroke-width:1;opacity:.75}.jgl-ln.band.mid{stroke:var(--i);stroke-dasharray:5 4;opacity:.7}.jgl-pt{fill:var(--a);stroke:var(--p);stroke-width:2}'
    + '.jgl-bar{fill:var(--d);opacity:.55}.jgl-bar.b{fill:var(--c);opacity:.6}.jgl-th{stroke:var(--i);stroke-width:1;stroke-dasharray:5 4;opacity:.75}.jgl-thl{fill:var(--i);font:10.5px var(--m);paint-order:stroke;stroke:var(--p);stroke-width:4px;stroke-linejoin:round}.jgl-shade{fill:var(--a);opacity:.07}.jgl-cross{stroke:var(--i);stroke-width:1;opacity:.6}'
    + '.jgl-tiles{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:10px 0 0}.jgl-tiles.six{grid-template-columns:repeat(6,1fr);margin:0 0 12px}.jgl-tiles.two{grid-template-columns:1fr 1fr;margin:0}.jgl-tiles.two.top{margin-top:10px}.jgl-tiles.one{grid-template-columns:1fr;margin:0}.jgl-scroll{overflow-x:auto}.jgl-tile{background:var(--p2);border:1px solid var(--l);border-radius:8px;padding:9px 11px;min-width:0}.jgl-tiles.six>.jgl-tile{background:var(--p)}.jgl-k{color:var(--d);font-size:10.5px;text-transform:uppercase;letter-spacing:.05em}.jgl-v{font:700 17px var(--m);margin-top:3px;overflow-wrap:anywhere}.jgl-s{color:var(--d);font-size:11px;margin-top:3px;line-height:1.35}.jgl-go{cursor:pointer}.jgl-go:hover{border-color:var(--a)}'
    + '.jgl-hero{display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin:0 0 6px}.jgl-big{font:700 38px var(--m)}.jgl-big small{font-size:15px;color:var(--d);margin-left:3px}.jgl-range{display:grid;grid-template-columns:auto 1fr auto;gap:10px;align-items:center;font-size:11px;color:var(--d);margin:0 0 12px}'
    + '.jgl-t{width:100%;border-collapse:collapse;font-size:12px;margin:0;background:transparent}.jgl-t th,.jgl-t td{padding:6px 8px;border:0;border-bottom:1px solid var(--l);text-align:right;background:transparent;color:var(--i);font-weight:400;white-space:nowrap;height:auto}.jgl-t th{color:var(--d);font-size:10.5px;text-transform:uppercase;letter-spacing:.04em;position:static}.jgl-t td{font-family:var(--m)}.jgl-t th:first-child,.jgl-t td:first-child{text-align:left;font-family:inherit;white-space:normal}'
    + '.jgl-minis{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.jgl-h2{font:700 12.5px var(--m);color:var(--t);margin:14px 0 4px}.jgl-h2 i{font:400 11.5px var(--s);color:var(--i);margin-left:6px}.jgl-mini .jgl-read{min-height:14px;font-size:10.5px}.jgl-grps{columns:2;column-gap:14px}.jgl-grp{border:1px solid var(--l);border-radius:8px;background:var(--b);min-width:0;overflow:auto;break-inside:avoid;margin:0 0 8px}.jgl-grp summary{cursor:pointer;padding:8px 10px;display:flex;gap:10px;align-items:baseline}.jgl-grp summary i{font-style:normal;color:var(--d);font-size:11px}'
    + '.jgl-steps{list-style:none;display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:0;padding:0}.jgl-steps li{border:1px solid var(--l);border-radius:8px;padding:8px 10px;color:var(--d);background:var(--b)}.jgl-steps li b{display:block;font-size:13px}.jgl-steps li i{font-style:normal;font-size:10.5px;line-height:1.3;display:block;margin-top:2px}.jgl-steps li.on{border-color:var(--a);background:var(--p2)}.jgl-steps li.on b{color:var(--a)}.jgl-steps li.done b{color:var(--i)}'
    + '.jgl-cal{display:grid;grid-template-columns:repeat(7,1fr);gap:4px}.jgl-dh{color:var(--d);font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;padding:0 4px}.jgl-day{min-height:68px;display:flex;flex-direction:column;gap:2px;align-items:flex-start;text-align:left;background:var(--p2);border:1px solid var(--l);border-radius:6px;padding:5px 6px;color:var(--i);font:inherit;font-size:11px;cursor:pointer;margin:0;overflow:hidden}.jgl-day:hover:not([disabled]){border-color:var(--a)}.jgl-day[disabled]{cursor:default;opacity:.55}.jgl-day.past{opacity:.35}.jgl-day.we{background:var(--b)}.jgl-day.today{border-color:var(--a);box-shadow:inset 0 0 0 1px var(--a)}'
    + '.jgl-dn{font-family:var(--m);color:var(--d)}.jgl-day.today .jgl-dn{color:var(--a);font-weight:700}.jgl-mk{font-size:10px;border-radius:3px;padding:0 4px;background:rgba(240,180,41,.18);white-space:nowrap;max-width:100%;overflow:hidden;text-overflow:ellipsis}.jgl-mk.h{background:rgba(224,122,106,.2)}.jgl-dc{margin-top:auto;color:var(--d);font-size:10px}.jgl-dc b{color:var(--i)}.jgl-flash{outline:2px solid #F0B429;outline-offset:2px}'
    + '.jgl-ladder{margin:6px 8px 4px}.jgl-lbar{position:relative;height:26px;border-radius:6px;background:var(--p2);margin-top:26px}.jgl-lseg{position:absolute;top:0;bottom:0;background:rgba(240,180,41,var(--al));border-right:2px solid var(--p)}.jgl-lnow{position:absolute;top:-6px;bottom:-6px;width:3px;margin-left:-1px;background:var(--i);border-radius:2px}.jgl-lnow b{position:absolute;bottom:100%;left:50%;transform:translateX(-50%);font:700 12px var(--m);white-space:nowrap;padding-bottom:2px}'
    + '.jgl-lticks{position:relative;height:40px;margin-top:4px}.jgl-lticks span{position:absolute;transform:translateX(-50%);text-align:center;font-size:10.5px;white-space:nowrap}.jgl-lticks b{display:block;font-family:var(--m);font-weight:400}.jgl-lticks i{font-style:normal;color:var(--d)}.jgl-ext{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:0 0 12px}.jgl-ext>div{display:flex;flex-wrap:wrap;gap:5px;align-items:center}.jgl-ext>div>i{font-style:normal;color:var(--d);font-size:11px;flex-basis:100%}'
    + '@media(max-width:700px){.jgl-chart{overflow-x:auto}.jgl-chart svg{min-width:600px}.jgl-mini .jgl-chart svg{min-width:0}}'
    + '@media(max-width:900px){.jgl-grid.g3,.jgl-grid.g2,.jgl-grid.g2w,.jgl-grid.g2c,.jgl-ext{grid-template-columns:1fr}.jgl-grps{columns:1}.jgl-tiles,.jgl-tiles.six{grid-template-columns:repeat(2,1fr)}.jgl-minis{grid-template-columns:1fr}.jgl-hrow{grid-template-columns:1fr}.jgl-row{grid-template-columns:1fr 1fr auto}.jgl{padding:0 10px}.jgl-day{min-height:54px}.jgl-lticks span:nth-child(even) i{display:none}}';

  function boot(win) {
    var doc = win.document; if (!doc || !doc.querySelectorAll) return;
    function start() { var hosts = doc.querySelectorAll('[data-glance]'), i; if (!hosts.length) return;
      if (!doc.getElementById('jgl-css')) { var st = doc.createElement('style'); st.id = 'jgl-css'; st.textContent = CSS; doc.head.appendChild(st); }
      for (i = 0; i < hosts.length; i++) mount(win, hosts[i]); }
    if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', start); else start();
  }
  function mount(win, host) {
    var A = ADAPTERS[host.getAttribute('data-glance')]; if (!A || host.getAttribute('data-glance-on')) return;
    host.setAttribute('data-glance-on', VERSION); host.className = (host.className ? host.className + ' ' : '') + 'jgl';
    host.innerHTML = '<div class="jgl-top"><h2>' + esc(A.title) + '</h2><span data-st>Loading\u2026</span></div><div data-body></div>';
    Promise.all(A.docs.map(function (u, i) { return win.fetch(u, { cache: 'no-cache' }).then(function (r) { if (!r.ok) throw new Error(u.split('/').pop() + ' answered ' + r.status); return r.json(); }).catch(function (e) { if (A.optional && i >= A.optional) return null; throw e; }); })).then(function (docs) {
      var M = A.model(docs, new Date().toISOString());
      host.querySelector('[data-body]').innerHTML = A.draw(M);
      host.querySelector('[data-st]').textContent = 'drawn from this page\u2019s own documents' + (M.asOf ? DOT + 'written ' + stamp(M.asOf) : '');
      host.__jgl = M;
      if (A.wire) A.wire(host, M);
      hover(win, host);
    }).catch(function (e) { host.querySelector('[data-st]').textContent = 'not drawn: ' + e.message; host.querySelector('[data-body]').innerHTML = ''; });
  }
  function hover(win, host) {
    host.addEventListener('mousemove', function (e) { var box = e.target.closest ? e.target.closest('.jgl-chart') : null; if (!box) return; var c = charts[box.getAttribute('data-chart')], svg = box.querySelector('svg'); if (!c || !svg) return;
      var r = svg.getBoundingClientRect(), x = (e.clientX - r.left) / r.width * c.w, k = Math.max(0, Math.min(c.n - 1, Math.round((x - c.L) / (c.w - c.L - c.R) * (c.n - 1)))), cx = c.L + k / (c.n - 1) * (c.w - c.L - c.R), line = svg.querySelector('.jgl-cross');
      line.setAttribute('x1', cx.toFixed(1)); line.setAttribute('x2', cx.toFixed(1)); box.querySelector('.jgl-read').textContent = readout(box.getAttribute('data-chart'), k); });
  }

  return { VERSION: VERSION, ADAPTERS: ADAPTERS, ema: ema, plumbingModel: plumbingModel, liquidityModel: liquidityModel, breadthModel: breadthModel, calendarModel: calendarModel, cryptoModel: cryptoModel,
    drawPlumbing: drawPlumbing, drawLiquidity: drawLiquidity, drawBreadth: drawBreadth, drawCalendar: drawCalendar, drawCrypto: drawCrypto, chart: chart, readout: readout, num: num, signed: signed, usd: usd, nice: nice, addDays: addDays, daysBetween: daysBetween, boot: boot };
});
