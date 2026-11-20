/* jh-strong-engine.js — the strength desk's client.
 *
 * Three feeds, nothing asked of a provider:
 *   data/strength.json        justhodl-strength, daily 03:40 UTC — the desk (about 850 names): ranks, bases, flows, 13F, thesis
 *   data/strength-wide.json   the same run — every listed common stock and plain ETF against SPY: the last session's bar,
 *                             the close before it, excess returns, the rank among its kind, the dump-day record
 *   data/warm/polygon-full/today.json.gz   justhodl-polygon-today, every 5 minutes in the session — today's forming day
 *                             bar of every US stock and ETF (15 minutes delayed on the Stocks Starter plan)
 *
 * v3 (Khalid 2026-11-20, the S&P 500 down 2%: "today everything is dumping hard however i cant see which stocks and etfs
 * are showing strength and dumping less than sp500"): the whole-market table is laid under today's forming bars here, in
 * the browser — today's move = the forming bar's last price over the table's close — and read by the engine's own
 * rules: up / flat / down less than SPY / down more. When the market is shut the same read is made of the last session
 * from the table's own bars. Every function from pyRound to sessionRead is a port of aws/lambdas/justhodl-strength/
 * source/wide.py and is held to its figures digit for digit by tests/strength-desk.test.js.
 *
 * v2 (2026-11-04): the old client-side 6-month engine is retired; the desk's figures come from the artifact. */
(function (w) {
  "use strict";
  if (w.JHStrong) return;
  var KEY = "data/strength.json", WIDE_KEY = "data/strength-wide.json", TODAY_KEY = "data/warm/polygon-full/today.json.gz";
  var DOC_TTL_MS = 10 * 60 * 1000;                 // the artifacts change once a day; quotes and forming bars refresh every minute
  var cache = { doc: null, at: 0, wide: null, wideAt: 0, today: null };
  var OUTCOMES = ["up", "flat", "down_less", "down_more"];

  function posture(h, spy) {
    var d = h && h.d, sd = spy && spy.d;
    if (d == null || sd == null) return { code: "NA", label: "—", rs: null };
    var rs = d - sd;
    if (sd <= -0.8 && d > 0) return { code: "BID", label: "UP WHILE SPX DUMPS", rs: rs };
    if (sd <= -0.8 && Math.abs(d) < 0.25) return { code: "FLAT", label: "FLAT WHILE SPX DUMPS", rs: rs };
    if (sd <= -0.8 && d < 0 && d > sd * 0.45) return { code: "HOLD", label: "DUMPING LESS THAN SPX", rs: rs };
    if (sd < 0 && d > 0) return { code: "BID", label: "BID VS SPX", rs: rs };
    if (sd < 0 && d > sd) return { code: "HOLD", label: "HOLDING UP", rs: rs };
    if (rs >= 0.4) return { code: "LEAD", label: "LEADING", rs: rs };
    if (rs <= -0.4) return { code: "LAG", label: "LAGGING", rs: rs };
    return { code: "INLINE", label: "IN LINE", rs: rs };
  }

  function overlay(rows, qx, spyD) {
    rows.forEach(function (r) {
      var q = qx[r.ticker];
      r.live = null;
      if (q && q.price != null) {
        var d = q.changePct != null ? Math.round(q.changePct * 100) / 100 : null;
        r.live = { px: q.price, d: d, vs_d: (d != null && spyD != null) ? Math.round((d - spyD) * 100) / 100 : null,
                   posture: posture({ d: d }, { d: spyD }), asOf: q.updated || q.asOf || q.timestamp || null };
      }
    });
  }

  /* ---- the engine's arithmetic, ported (wide.py) ---- */

  /* Python's round(): correctly rounded on the double's exact value, ties to even. toFixed rounds the exact value too, but
     ties away from zero - so the tie (v * 2^(d+1) is an odd integer) is settled here. */
  function pyRound(v, d) {
    if (v == null || v !== v || v === Infinity || v === -Infinity) return null;
    d = d || 0;
    var k = v * Math.pow(2, d + 1);
    if (Number.isInteger(k) && Math.abs(k % 2) === 1) {
      var p = Math.pow(10, d), lo = Math.floor(v * p);
      return (lo % 2 === 0 ? lo : lo + 1) / p;
    }
    return Number(v.toFixed(d));                              // -0.004 rounds to -0, as Python's does: the sign of zero is part of the figure
  }
  /* Python 3.12's sum() of floats: Neumaier's compensated summation. */
  function nsum(xs) {
    var hi = 0, lo = 0, i, x, t;
    for (i = 0; i < xs.length; i++) {
      x = xs[i];
      t = hi + x;
      if (Math.abs(hi) >= Math.abs(x)) lo += (hi - t) + x; else lo += (x - t) + hi;
      hi = t;
    }
    return (lo && isFinite(lo)) ? hi + lo : hi;
  }
  function median(xs) {
    var a = xs.slice().sort(function (x, y) { return x - y; }), n = a.length;
    return !n ? null : (n % 2 ? a[(n - 1) / 2] : 0.5 * (a[n / 2 - 1] + a[n / 2]));
  }
  function outcome(ret, spy, flat) {
    if (ret > flat) return "up";
    if (ret >= -flat) return "flat";
    return ret > spy ? "down_less" : "down_more";
  }
  function regimeOf(spy, rules) {
    var f = rules.flat_pct;
    return spy <= rules.dump_pct ? "dump" : spy >= rules.rally_pct ? "rally" : spy < -f ? "down" : spy > f ? "up" : "flat";
  }
  /* What one day bar [o, h, l, c, v, vw] says against the close before it. */
  function barRead(bar, prev, bench) {
    var o = bar[0], h = bar[1], l = bar[2], c = bar[3], vw = bar.length > 5 ? bar[5] : null;
    var out = { ret: (c / prev - 1.0) * 100.0, fo: o ? (c / o - 1.0) * 100.0 : null, pos: (h != null && l != null && h > l) ? 100.0 * (c - l) / (h - l) : null,
                vwx: vw ? (c / vw - 1.0) * 100.0 : null };
    if (bench) {
      out.x = out.ret - bench.ret;
      out.fox = (out.fo != null && bench.fo != null) ? out.fo - bench.fo : null;
    }
    return out;
  }
  function rowsOf(doc) {
    var cols = doc.cols || [], n = cols.length;
    return (doc.rows || []).map(function (raw) { var r = {}, i; for (i = 0; i < n; i++) r[cols[i]] = raw[i]; return r; });
  }
  /* The table laid under today's forming bars. null when `today` holds no session after the table's own. */
  function liveJoin(doc, today) {
    var session = String((today && today.session) || ""), asOf = String(doc.as_of || ""), trows = (today && today.rows) || {}, bm = doc.benchmark || {};
    var sbar = trows[bm.ticker || "SPY"];
    if (!session || !asOf || session <= asOf || !sbar || !bm.c) return null;
    var sus = (doc.rules && doc.rules.suspect) || [0.56, 1.8], ahead = doc.splits_ahead || {}, spy = barRead(sbar, bm.c);
    var rows = rowsOf(doc);
    rows.forEach(function (r) {
      var bar = trows[r.t];
      r.ret = null; r.live = null; r.unconfirmed = false; r.split = null;
      if (bar && bar[3] && r.c) {
        var prev = r.c, sa = ahead[r.t];
        if (sa && asOf < sa.date && sa.date <= session && sa.to) { prev = prev * Number(sa.from) / Number(sa.to); r.split = sa; }
        var ratio = bar[3] / prev;
        if (ratio < sus[0] || ratio > sus[1]) r.unconfirmed = true;
        else { r.live = barRead(bar, prev, spy); r.ret = r.live.ret; }
        r.bar = bar.slice(0, 6); r.prev = prev;
      }
    });
    spy.bar = sbar.slice(0, 6); spy.prev = bm.c;
    return { session: session, as_of: asOf, bars_as_of: today.as_of || null, delay_minutes: today.delay_minutes == null ? null : today.delay_minutes, spy: spy, rows: rows };
  }
  /* The same shape from the table's own bars: the last session, when the market is shut. */
  function lastJoin(doc) {
    var bm = doc.benchmark || {}, sbar = [bm.o, bm.h, bm.l, bm.c, bm.v, bm.vw], spy = barRead(sbar, bm.pc);
    var rows = rowsOf(doc);
    rows.forEach(function (r) {
      r.ret = null; r.live = null; r.unconfirmed = false; r.split = null;
      r.bar = [r.o, r.h, r.l, r.c, r.v, r.vw];
      if (r.pc) { r.prev = r.pc; r.live = barRead(r.bar, r.pc, spy); r.ret = r.live.ret; }
    });
    spy.bar = sbar; spy.prev = bm.pc;
    return { session: String(doc.as_of || ""), as_of: String(doc.prior_session || ""), bars_as_of: null, delay_minutes: null, spy: spy, rows: rows };
  }
  function statsOf(items, spy) {
    var n = items.length, out = { n: n };
    if (!n) return out;
    OUTCOMES.forEach(function (k) { out[k] = items.filter(function (x) { return x[1] === k; }).length; });
    var held = n - out.down_more, rets = items.map(function (x) { return x[0]; });
    out.held = held;
    out.held_pct = pyRound(100.0 * held / n, 1);
    out.up_pct = pyRound(100.0 * out.up / n, 1);
    out.beat = rets.filter(function (r) { return r > spy; }).length;
    out.median_ret = pyRound(median(rets), 2);
    out.mean_excess = pyRound(nsum(rets) / n - spy, 2);
    out.beat_pct = pyRound(100.0 * out.beat / n, 1);
    return out;
  }
  function cmpStr(a, b) { return a < b ? -1 : a > b ? 1 : 0; }
  /* Which bucket a dump-day record falls in (held up on every dump day / two thirds or more / a third to two thirds /
     fewer); null under rules.record_min dump days. Integer arithmetic, as in the engine. */
  function recordBucket(dn, dh, min) {
    if (dn == null || dh == null || dn < (min || 3)) return null;
    return dh === dn ? "all" : 3 * dh >= 2 * dn ? "most" : 3 * dh >= dn ? "some" : "few";
  }
  /* The read of one session over the table. Rows carry `ret`; a row that is set aside (`why`) or has no `ret` is not
     counted. rankCol and rec name the rank and the dump-day record each name carried INTO the session: "rsp" and
     ["dnp", "dhp"] for the table's own last session, "rs" and ["dn", "dh"] for today's forming bars. Sets r.out and
     r.x on every row with a move. */
  function sessionRead(rows, spy, rules, rankCol, labels, rec) {
    rankCol = rankCol || "rsp";
    rec = rec || ["dnp", "dhp"];
    var dnCol = rec[0], dhCol = rec[1], RMIN = rules.record_min || 3, SKIP = rules.board_skip_cls || [];
    var flat = rules.flat_pct, B = rules.board_min_mcap, BIG = rules.big_mcap, EA = rules.etf_min_adv, IMIN = rules.industry_min, N = rules.board_n;
    rows.forEach(function (r) { if (r.ret != null) { r.out = outcome(r.ret, spy, flat); r.x = r.ret - spy; } else { r.out = null; r.x = null; } });
    var pop = rows.filter(function (r) { return r.ret != null && !r.why; });
    var stocks = pop.filter(function (r) { return r.kind === "stock"; }), etfs = pop.filter(function (r) { return r.kind === "etf"; });
    var big = stocks.filter(function (r) { return (r.mcap || 0) >= B; });
    function st(g) { return statsOf(g.map(function (r) { return [r.ret, r.out]; }), spy); }
    var liquid = etfs.filter(function (r) { return (r.adv || 0) >= EA; });
    var groups = { stocks: st(stocks), stocks_1b: st(big), stocks_10b: st(stocks.filter(function (r) { return (r.mcap || 0) >= BIG; })),
                   sp500: st(stocks.filter(function (r) { return r.sp; })), etfs: st(etfs), etfs_liquid: st(liquid) };
    var cap = {};
    OUTCOMES.forEach(function (k) { cap[k] = pyRound(nsum(big.filter(function (r) { return r.out === k; }).map(function (r) { return r.mcap || 0; })), 0); });
    function uniq(list) { var seen = {}, out = []; list.forEach(function (v) { if (!seen[v]) { seen[v] = 1; out.push(v); } }); return out.sort(cmpStr); }
    var secOf = function (r) { return r.sector || "Unclassified"; };
    var by_sector = uniq(big.map(secOf)).map(function (sec) {
      var m = big.filter(function (r) { return secOf(r) === sec; }), g = st(m);
      g.sector = sec;
      g.mcap = pyRound(nsum(m.map(function (r) { return r.mcap || 0; })), 0);
      return g;
    }).sort(function (a, b) { return ((b.held_pct || 0) - (a.held_pct || 0)) || cmpStr(a.sector, b.sector); });
    var clsOf = function (r) { return r.cls || "equity_etf"; };
    var by_class = uniq(liquid.map(clsOf)).map(function (c) {
      var g = st(liquid.filter(function (r) { return clsOf(r) === c; }));
      g.cls = c;
      g.label = (labels && labels[c]) || c;
      return g;
    }).sort(function (a, b) { var x = a.median_ret != null ? a.median_ret : -999, y = b.median_ret != null ? b.median_ret : -999; return (y - x) || cmpStr(a.cls, b.cls); });
    var industries = [];
    uniq(big.filter(function (r) { return r.industry; }).map(function (r) { return r.industry; })).forEach(function (ind) {
      var m = big.filter(function (r) { return r.industry === ind; });
      if (m.length < IMIN) return;
      var g = st(m);
      g.industry = ind;
      g.sector = m[0].sector == null ? null : m[0].sector;
      industries.push(g);
    });
    industries.sort(function (a, b) { return ((b.held_pct || 0) - (a.held_pct || 0)) || ((b.median_ret || 0) - (a.median_ret || 0)) || cmpStr(a.industry, b.industry); });
    var ranked = big.filter(function (r) { return r[rankCol] != null; }).sort(function (a, b) { return (a[rankCol] - b[rankCol]) || cmpStr(a.t, b.t); });
    var m = ranked.length, deciles = [[], [], [], [], [], [], [], [], [], []];
    ranked.forEach(function (r, i) { deciles[Math.min(9, Math.floor(10 * i / m))].push(r); });
    var by_rank = m >= 20 ? deciles.map(st) : [];
    function names(g, key, reverse) {
      return g.slice().sort(function (a, b) { return (reverse === false ? a[key] - b[key] : b[key] - a[key]) || cmpStr(a.t, b.t); }).slice(0, N).map(function (r) { return r.t; });
    }
    var bucket = function (r) { return recordBucket(r[dnCol], r[dhCol], RMIN); };
    var by_record = (rules.record_buckets || [["all", "held up on every dump day"], ["most", "on two thirds or more"], ["some", "on a third to two thirds"], ["few", "on fewer than a third"]]).map(function (b) {
      var g = st(big.filter(function (r) { return bucket(r) === b[0]; }));
      g.key = b[0];
      g.label = b[1];
      return g;
    });
    var etfBoard = liquid.filter(function (r) { return SKIP.indexOf(clsOf(r)) < 0; });
    var strong = big.filter(function (r) { return (r[rankCol] || 0) >= 80; }), weak = big.filter(function (r) { return r[rankCol] != null && r[rankCol] <= 20; });
    var boards = {
      up: names(big.filter(function (r) { return r.out === "up"; }), "ret"),
      down_less: names(big.filter(function (r) { return r.out === "down_less"; }), "x"),
      strong_holding: names(strong.filter(function (r) { return r.out !== "down_more"; }), "x"),
      strong_breaking: names(strong.filter(function (r) { return r.out === "down_more"; }), "x", false),
      weak_bid: names(weak.filter(function (r) { return r.out === "up"; }), "ret"),
      repeat: names(big.filter(function (r) { return r.out !== "down_more" && bucket(r) === "all"; }), "x"),
      etf_up: names(etfBoard.filter(function (r) { return r.out === "up"; }), "ret"),
      etf_down_most: names(etfBoard, "ret", false)
    };
    return { n_counted: pop.length, regime: regimeOf(spy, rules), groups: groups, mcap_1b: cap, by_sector: by_sector, by_class: by_class,
             industries: { n: industries.length, holding: industries.slice(0, 15), breaking: industries.slice(-15).reverse() },
             by_rank: by_rank, rank_col: rankCol, n_ranked_1b: m, by_record: by_record, record_cols: [dnCol, dhCol], boards: boards };
  }

  /* ---- the desk's view of a session: the join, the read, and what each bar adds ---- */
  function weekdaysBetween(a, b) {      // sessions a calendar would allow between two ISO days (weekends skipped; a holiday still counts)
    var x = Date.parse(a + "T00:00:00Z"), y = Date.parse(b + "T00:00:00Z"), n = 0;
    if (!isFinite(x) || !isFinite(y)) return null;
    for (var t = x + 864e5; t <= y; t += 864e5) { var wd = new Date(t).getUTCDay(); if (wd !== 0 && wd !== 6) n++; }
    return n;
  }
  /* mode "live": today's forming bars over the table (rank carried in = the table's `rs`); mode "last": the table's own
     last session (rank carried in = `rsp`, one session older). Each row gains: rank, out, x, fo, fox, pos, vwx, vol
     (volume against its own 20-session average), rv (that ratio over the median counted name's - the market's pace is
     the clock, so 1.0 is the pace of the market at this hour), and ticks: how many of five stated facts hold -
     up on the day, above the open, above the day's VWAP, in the upper third of the day's range, volume above the pace. */
  function model(doc, today, opts) {
    opts = opts || {};
    var j = liveJoin(doc, today), mode = j ? "live" : "last";
    if (!j) j = lastJoin(doc);
    var rules = doc.rules || {}, rankCol = mode === "live" ? "rs" : "rsp", rec = mode === "live" ? ["dn", "dh"] : ["dnp", "dhp"];
    var read = sessionRead(j.rows, j.spy.ret, rules, rankCol, opts.classLabels || null, rec);
    var flat = rules.flat_pct, prog = [];
    j.rows.forEach(function (r) {
      r.rank = r[rankCol];
      r.rec_n = r[rec[0]]; r.rec_h = r[rec[1]];                 // the dump-day record carried into the session
      r.bucket = recordBucket(r.rec_n, r.rec_h, rules.record_min);
      var L = r.live || {};
      r.fo = L.fo == null ? null : L.fo; r.fox = L.fox == null ? null : L.fox; r.pos = L.pos == null ? null : L.pos; r.vwx = L.vwx == null ? null : L.vwx;
      r.px = r.bar ? r.bar[3] : r.c;
      r.vol = (r.bar && r.bar[4] != null && r.av) ? r.bar[4] / r.av : null;
      r.counted = r.ret != null && !r.why;
      if (r.counted && r.vol != null && r.vol > 0) prog.push(r.vol);
    });
    var pace = median(prog);
    j.rows.forEach(function (r) {
      r.rv = (r.vol != null && pace) ? r.vol / pace : null;
      var facts = [r.ret == null ? null : r.ret > flat, r.fo == null ? null : r.fo > 0, r.vwx == null ? null : r.vwx > 0, r.pos == null ? null : r.pos >= 200 / 3, r.rv == null ? null : r.rv >= 1.2];
      r.facts = facts;
      r.tickN = facts.filter(function (f) { return f != null; }).length;
      r.ticks = r.ret == null ? null : facts.filter(function (f) { return f === true; }).length;
    });
    var sb = j.spy.bar || [], bmAv = (doc.benchmark || {}).av;
    j.spy.vol = (sb[4] != null && bmAv) ? sb[4] / bmAv : null;
    return { contract: "strength-view.v1", mode: mode, session: j.session, rank_as_of: j.as_of, table_as_of: doc.as_of, bars_as_of: j.bars_as_of, delay_minutes: j.delay_minutes,
             gap_sessions: mode === "live" ? weekdaysBetween(doc.as_of, j.session) : null,
             spy: j.spy, rows: j.rows, read: read, rules: rules, rank_col: rankCol, record_cols: rec, pace: pace,
             n_no_print: j.rows.filter(function (r) { return !r.bar; }).length, unconfirmed: j.rows.filter(function (r) { return r.unconfirmed; }),
             splits_today: j.rows.filter(function (r) { return r.split; }), universe: doc.universe || {}, dump_days: doc.dump_days || [], rally_days: doc.rally_days || [],
             table: { version: doc.version, generated_at: doc.generated_at, as_of: doc.as_of, prior_session: doc.prior_session, definitions: doc.definitions || {}, sources: doc.sources || [], last_session: doc.last_session || null },
             class_labels: opts.classLabels || {} };
  }

  async function run() {
    var D = w.JHDesk;
    var doc = cache.doc && (Date.now() - cache.at) < DOC_TTL_MS ? cache.doc : await D.feed(KEY);
    if (!doc || doc.contract !== "strength.v1") throw new Error("data/strength.json is not published yet (justhodl-strength runs daily 03:40 UTC)");
    if (doc !== cache.doc) { cache.doc = doc; cache.at = Date.now(); }
    var rows = doc.rows || [];
    var desk = rows.filter(function (r) { return (r.scopes || []).indexOf("desk") >= 0; });
    // the whole market: the nightly table, and today's forming bars over it
    var wide = cache.wide && (Date.now() - cache.wideAt) < DOC_TTL_MS ? cache.wide : null, today = null, market = null, marketError = null;
    try {
      if (!wide) {
        wide = await D.feed(WIDE_KEY);
        if (wide && wide.contract === "strength-wide.v1") { cache.wide = wide; cache.wideAt = Date.now(); } else wide = null;
      }
      if (wide) {
        try { today = await D.feed(TODAY_KEY); } catch (e) { today = null; }
        if (today && today.rows) cache.today = today; else today = cache.today;
        market = model(wide, today, { classLabels: (doc.classes && doc.classes.labels) || null });
      } else marketError = "data/strength-wide.json is not published yet (justhodl-strength writes it after the desk, daily 03:40 UTC)";
    } catch (e) { market = null; marketError = String(e && e.message || e); }
    var tickers = ["SPY"].concat(desk.map(function (r) { return r.ticker; }));
    var qx = {};
    try { qx = await D.quotes(tickers) || {}; } catch (e) { qx = {}; }
    var spyLive = qx.SPY && qx.SPY.price != null ? { px: qx.SPY.price, d: qx.SPY.changePct != null ? Math.round(qx.SPY.changePct * 100) / 100 : null } : null;
    overlay(rows, qx, spyLive ? spyLive.d : null);
    var spyD = spyLive && spyLive.d != null ? spyLive.d : (doc.benchmark && doc.benchmark.h ? doc.benchmark.h.d : null);
    var focus = desk.filter(function (r) {
      var p = r.live ? r.live.posture : r.posture;
      return p && (p.code === "BID" || p.code === "FLAT" || p.code === "HOLD");
    }).sort(function (a, b) { return (b.strength || -1) - (a.strength || -1); });
    return {
      doc: doc, rows: rows, desk: desk, focus: focus, spy: { live: spyLive, d: spyD, h: doc.benchmark ? doc.benchmark.h : {}, px: spyLive ? spyLive.px : (doc.benchmark ? doc.benchmark.px : null) },
      dump: spyD != null && spyD < -0.4, quotes_at: new Date().toISOString(), sleeves: Object.keys(doc.sleeves || {}),
      market: market, marketError: marketError
    };
  }

  var api = { KEY: KEY, WIDE_KEY: WIDE_KEY, TODAY_KEY: TODAY_KEY, OUTCOMES: OUTCOMES, run: run, posture: posture, overlay: overlay,
              pyRound: pyRound, nsum: nsum, median: median, outcome: outcome, regimeOf: regimeOf, barRead: barRead, rowsOf: rowsOf, recordBucket: recordBucket,
              liveJoin: liveJoin, lastJoin: lastJoin, statsOf: statsOf, sessionRead: sessionRead, model: model, weekdaysBetween: weekdaysBetween };
  w.JHStrong = api;
  if (typeof module === "object" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
