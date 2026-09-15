/* Institutional volume fuse — labeled clocks, never blended. */
(function (global) {
  "use strict";
  var DP = null, FS = null, F13 = null, DIX = null, LIQ = null, SF = null, OC = null, LT = null;
  var CQ = null, ETFX = null, CFTC = null, SI = null, SPY = null;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var BTC_PROXIES = { BTC:1, BTCUSD:1, XBT:1, IBIT:1, FBTC:1, BITB:1, ARKB:1, HODL:1, BITO:1, GBTC:1, BTCW:1, MSTR:1, COIN:1, MARA:1, RIOT:1 };
  var ETH_PROXIES = { ETH:1, ETHUSD:1, ETHA:1, ETHE:1, ETHW:1, FETH:1, ETHV:1, EZET:1 };
  var COT_MAP = {
    ES: "ES", SPY: "ES", SPX: "ES", "ES1!": "ES",
    NQ: "NQ", QQQ: "NQ", NDX: "NQ", TQQQ: "NQ",
    YM: "YM", DIA: "YM", DJI: "YM",
    RTY: "RTY", IWM: "RTY", RUT: "RTY",
    GC: "GC", GLD: "GC", GOLD: "GC",
    SI: "SI", SLV: "SI", SILVER: "SI",
    CL: "CL", USO: "CL", OIL: "CL",
    NG: "NG", UNG: "NG",
    HG: "HG", CPER: "HG", COPPER: "HG",
    ZB: "ZB", TLT: "ZB",
    ZN: "ZN", IEF: "ZN",
    ZF: "ZF", IEI: "ZF",
    VX: "VX", VIX: "VX", UVXY: "VX", SVXY: "VX",
    DX: "DX", UUP: "DX", DXY: "DX",
    "6E": "6E", FXE: "6E",
    "6J": "6J", FXY: "6J",
    ZC: "ZC", CORN: "ZC",
    ZS: "ZS", SOYB: "ZS",
    ZW: "ZW", WEAT: "ZW"
  };

  function bare(s) {
    s = String(s || "");
    if (global.jhFundTicker) return global.jhFundTicker(s);
    return s.indexOf(":") >= 0 ? s.split(":").pop() : s;
  }
  function num(x) {
    if (x == null) return null;
    var n = Number(x);
    return isFinite(n) ? n : null;
  }
  function load(slot, url) {
    if (slot.p) return slot.p;
    slot.p = fetch(url, { cache: "no-store" }).then(function (r) {
      if (!r.ok) throw new Error("http " + r.status);
      return r.json();
    }).then(function (j) { slot.v = j; return j; }).catch(function () { slot.v = {}; return slot.v; });
    return slot.p;
  }
  function darkPool() { DP = DP || {}; return load(DP, "/data/dark-pool.json"); }
  function finraShort() { FS = FS || {}; return load(FS, "/data/finra-short.json"); }
  function f13() { F13 = F13 || {}; return load(F13, "/data/13f-by-ticker.json"); }
  function dix() { DIX = DIX || {}; return load(DIX, "/data/dix.json"); }
  function liq() { LIQ = LIQ || {}; return load(LIQ, "/data/liquidity-profile.json"); }
  function shareFlows() { SF = SF || {}; return load(SF, "/data/share-flows.json"); }
  function optConf() { OC = OC || {}; return load(OC, "/data/options-confluence.json"); }
  function lookthrough() { LT = LT || {}; return load(LT, "/data/flow-lookthrough.json"); }
  function cq() { CQ = CQ || {}; return load(CQ, "/data/cryptoquant-onchain.json"); }
  function etfx() { ETFX = ETFX || {}; return load(ETFX, "/data/crypto-etf-flows.json"); }
  function cftc() { CFTC = CFTC || {}; return load(CFTC, "/data/cftc-deep-view.json"); }
  function shortInterest() { SI = SI || {}; return load(SI, "/data/short-interest.json"); }

  function volOf(b) { return num(b && (b.volume != null ? b.volume : b.value != null ? b.value : b.v)); }
  function closeOf(b) { return num(b && (b.close != null ? b.close : b.c)); }
  function openOf(b) { return num(b && (b.open != null ? b.open : b.o)); }
  function highOf(b) { return num(b && (b.high != null ? b.high : b.h)); }
  function lowOf(b) { return num(b && (b.low != null ? b.low : b.l)); }
  function vwapOf(b) { return num(b && (b.vw != null ? b.vw : b.vwap)); }

  function median(arr) {
    var a = arr.filter(function (x) { return x != null && isFinite(x); }).sort(function (x, y) { return x - y; });
    if (!a.length) return null;
    var m = Math.floor(a.length / 2);
    return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
  }

  function nyParts(ts) {
    var s = new Date(ts * 1000).toLocaleString("en-US", { timeZone: "America/New_York", hour12: false });
    var p = s.match(/(\d+)\/(\d+)\/(\d+),\s*(\d+):(\d+)/);
    if (!p) return null;
    return { mon: +p[1], day: +p[2], year: +p[3], hh: +p[4], mm: +p[5] };
  }
  function sessionKey(ts) {
    var p = nyParts(ts);
    return p ? p.year + "-" + p.mon + "-" + p.day : "";
  }
  function bucket(ts) {
    var p = nyParts(ts);
    if (!p) return "rth";
    var m = p.hh * 60 + p.mm;
    if (m < 9 * 60 + 30) return "pre";
    if (m < 16 * 60) return "rth";
    return "post";
  }

  function tapeFromBars(bars, shares) {
    if (!bars || !bars.length) return null;
    var last = bars[bars.length - 1];
    var v = volOf(last);
    if (v == null) return null;
    var i, s = 0, n = 0, vols = [], ranges = [], up = 0, dn = 0, px = closeOf(last);
    for (i = Math.max(0, bars.length - 21); i < bars.length - 1; i++) {
      var x = volOf(bars[i]);
      if (x == null) continue;
      s += x; n++;
      vols.push(x);
      var hi = highOf(bars[i]), lo = lowOf(bars[i]), cl = closeOf(bars[i]), op = openOf(bars[i]);
      if (hi != null && lo != null && cl) ranges.push((hi - lo) / cl);
      if (cl != null && op != null) {
        if (cl >= op) up += x;
        else dn += x;
      }
    }
    var avg = n ? s / n : null;
    var med = median(vols);
    var rvol = med ? v / med : (avg ? v / avg : null);
    var hi = highOf(last), lo = lowOf(last);
    var rng = (hi != null && lo != null && px) ? (hi - lo) / px : null;
    var medRng = median(ranges);
    var effort = rvol != null && rvol >= 1.4;
    var tight = rng != null && medRng != null && rng <= medRng * 0.75;
    var wide = rng != null && medRng != null && rng >= medRng * 1.35;
    var absorb = null;
    if (effort && tight) absorb = "ABSORPTION — high volume, tight range (effort without result)";
    else if (effort && wide) absorb = "EXPANSION — high volume, wide range (effort with result)";
    else if (!effort && wide) absorb = "THIN RANGE — wide bar on light volume";
    var sh = num(shares);
    var vw = vwapOf(last);
    return {
      last: v,
      avg20: avg,
      median20: med,
      vs_pct: avg ? (v / avg - 1) * 100 : null,
      rvol: rvol,
      dollar: px != null ? v * px : null,
      close: px,
      vwap: vw,
      vs_vwap_pct: (vw && px) ? (px / vw - 1) * 100 : null,
      up20: up,
      down20: dn,
      up_share: (up + dn) ? up / (up + dn) : null,
      range_pct: rng != null ? rng * 100 : null,
      absorb: absorb,
      float_turn_pct: sh ? (v / sh) * 100 : null,
      n: n,
      label: "this bar vs prior " + n + " bars on the open timeframe — not session ADV unless Daily"
    };
  }

  function sessionSplit(bars) {
    if (!bars || bars.length < 8) return null;
    var last = bars[bars.length - 1];
    var t1 = num(last.time) || num(last.t);
    var t0 = num(bars[0].time) || num(bars[0].t);
    if (t1 == null || t0 == null) return null;
    var span = (t1 - t0) / Math.max(1, bars.length - 1);
    if (span > 3 * 3600) return { status: "DAILY_BARS", note: "Open a 5m/15m chart, or wait for the minute fetch, to split pre / RTH / post." };
    var day = sessionKey(t1);
    var pre = 0, rth = 0, post = 0, n = 0, i;
    for (i = 0; i < bars.length; i++) {
      var ts = num(bars[i].time) || num(bars[i].t);
      if (sessionKey(ts) !== day) continue;
      var v = volOf(bars[i]);
      if (v == null) continue;
      n++;
      var b = bucket(ts);
      if (b === "pre") pre += v;
      else if (b === "post") post += v;
      else rth += v;
    }
    var tot = pre + rth + post;
    if (!tot) return null;
    return {
      status: "OK",
      day: day,
      pre: pre,
      rth: rth,
      post: post,
      tot: tot,
      pre_pct: (pre / tot) * 100,
      rth_pct: (rth / tot) * 100,
      post_pct: (post / tot) * 100,
      n: n,
      note: "NY clock. Pre < 09:30, RTH 09:30–16:00, post ≥ 16:00. Institutions often work the close — a fat post % with a quiet RTH is a different tape."
    };
  }

  function rvolVsSpy(name, spy) {
    if (!name || name.rvol == null || !spy || spy.rvol == null || spy.rvol <= 0) return null;
    var rel = name.rvol / spy.rvol;
    var read = rel >= 1.3 ? "NAME BUSY vs SPY" : rel <= 0.7 ? "QUIET vs SPY" : "IN LINE WITH TAPE";
    return { name_rvol: name.rvol, spy_rvol: spy.rvol, rel: rel, read: read };
  }

  function atsSide(st) {
    if (st === "ACCUMULATION") return 1;
    if (st === "DISTRIBUTION") return -1;
    return 0;
  }
  function f13Side(r) {
    if (!r) return 0;
    var net = (num(r.bought_usd) || 0) - (num(r.sold_usd) || 0);
    var add = (num(r.n_funds_adding) || 0) - (num(r.n_funds_trimming) || 0);
    if (net > 0 && add >= 0) return 1;
    if (net < 0 && add <= 0) return -1;
    return 0;
  }
  function confluence(ats, inst, ping) {
    var a = atsSide(ats && ats.state);
    var f = f13Side(inst);
    if (ping) return { verdict: "NOT INSTITUTIONAL SIZE", why: "ATS avg trade is retail-ping scale. Dark % here is not block flow. Do not treat as smart-money volume.", a: a, f: f };
    if (a && f && a === f) return { verdict: a > 0 ? "CONFIRMED ACCUMULATION" : "CONFIRMED DISTRIBUTION", why: "FINRA ATS weekly state agrees with 13F net holders. Cadences still differ — ATS is weeks, 13F is a quarter.", a: a, f: f };
    if (a && f && a !== f) return { verdict: "DIVERGENT", why: "ATS weekly and 13F quarterly disagree. Do not average them. One of them is stale or the tape is not the 13F book.", a: a, f: f };
    if (a) return { verdict: "ATS ONLY · " + ats.state, why: "No usable 13F confirmation for this name (or 13F is mixed). Weekly ATS stands alone.", a: a, f: f };
    if (f) return { verdict: f > 0 ? "13F ONLY · NET BUYING" : "13F ONLY · NET SELLING", why: "Holdings change without an ATS accumulation/distribution flag this week.", a: a, f: f };
    return { verdict: "NO CONFLUENCE", why: "ATS is NEUTRAL or missing and 13F is mixed/absent. Volume is not a signal here.", a: a, f: f };
  }

  function indexBoard(doc) {
    var map = {}, i, r, lists, L;
    lists = [doc.board, doc.top_accumulation, doc.top_distribution, doc.top_picks];
    for (L = 0; L < lists.length; L++) {
      if (!Array.isArray(lists[L])) continue;
      for (i = 0; i < lists[L].length; i++) {
        r = lists[L][i];
        if (r && r.ticker && !map[r.ticker]) map[r.ticker] = r;
      }
    }
    return map;
  }

  function pickCot(list, t) {
    var key = COT_MAP[t] || (COT_MAP[t.replace("/", "")] ) || null;
    if (!key && !/^(ES|NQ|YM|RTY|GC|SI|CL|NG|ZB|ZN|VX|DX)$/.test(t)) {
      return { key: "ES", row: (list || []).filter(function (c) { return c.symbol === "ES"; })[0] || null, mapped: false, note: "No COT for this equity. ES is the index overlay — not this stock." };
    }
    key = key || t;
    var row = (list || []).filter(function (c) { return c.symbol === key; })[0] || null;
    return { key: key, row: row, mapped: COT_MAP[t] === key && t !== key, note: t === key ? "CFTC futures positioning, weekly, lagged. Not stock volume." : ("Mapped " + t + " → " + key + ". Positioning, not volume.") };
  }

  function pickCrypto(t, cqJ, etfJ) {
    var btc = !!BTC_PROXIES[t], eth = !!ETH_PROXIES[t];
    if (!btc && !eth) return { show: false };
    var side = btc ? "btc" : "eth";
    var m = (cqJ && cqJ.metrics) || {};
    var etf = (etfJ && etfJ[side + "_etf"]) || {};
    var lists = [].concat(etf.top_inflow || [], etf.top_outflow || []);
    var mine = null, i;
    for (i = 0; i < lists.length; i++) if (lists[i] && lists[i].etf === t) mine = lists[i];
    var net = m[side + "_exchange_netflow"] || {};
    return {
      show: true,
      side: side.toUpperCase(),
      netflow: net.value,
      netflow_z: net.z365,
      netflow_asof: net.as_of,
      inflow: (m[side + "_exchange_inflow"] || {}).value,
      outflow: (m[side + "_exchange_outflow"] || {}).value,
      whale: (m.btc_whale_ratio || {}).value,
      etf_today: etf.flow_today_usd,
      etf_5d: etf.cum_5d_usd,
      etf_30d: etf.cum_30d_usd,
      etf_regime: etf.regime,
      etf_date: etf.last_date,
      etf_mine: mine,
      interp: etfJ && etfJ.interpretation
    };
  }

  function spyDaily() {
    SPY = SPY || {};
    if (SPY.p) return SPY.p;
    SPY.p = fetch(PROXY + "/ohlc?ticker=SPY&span=day&mult=1&days=40", { cache: "default" }).then(function (r) {
      return r.json();
    }).then(function (j) {
      var bars = (j && j.bars) || [];
      SPY.tape = tapeFromBars(bars, null);
      return SPY.tape;
    }).catch(function () { SPY.tape = null; return null; });
    return SPY.p;
  }

  function fetchMinute(t) {
    return fetch(PROXY + "/ohlc?ticker=" + encodeURIComponent(t) + "&span=minute&mult=5&days=1", { cache: "default" })
      .then(function (r) { return r.json(); })
      .then(function (j) { return (j && j.bars) || []; })
      .catch(function () { return []; });
  }

  function fetchVwap(t) {
    return fetch(PROXY + "/poly/vwap?ticker=" + encodeURIComponent(t), { cache: "default" })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        var rows = (j && (j.results || j.values)) || [];
        if (j && j.results && j.results.values) rows = j.results.values;
        var last = rows[0] || rows[rows.length - 1] || null;
        return last ? num(last.value != null ? last.value : last.vw) : null;
      }).catch(function () { return null; });
  }

  function snapVwap(snap) {
    var r = snap && (snap.results || snap.ticker || snap);
    var day = (r && r.day) || {};
    return { vwap: num(day.vw), vol: num(day.v), close: num(day.c), prev_vwap: num((r.prevDay || {}).vw), prev_vol: num((r.prevDay || {}).v) };
  }

  async function enrich(row, pack) {
    pack = pack || {};
    var t = row.ticker;
    var bars = pack.bars || [];
    var sh = row.shares && row.shares.shares_outstanding;
    var tape = tapeFromBars(bars, sh);
    var snap = snapVwap(pack.snapshot);
    if (tape && tape.vwap == null && snap.vwap != null) {
      tape.vwap = snap.vwap;
      tape.vs_vwap_pct = (tape.close && snap.vwap) ? (tape.close / snap.vwap - 1) * 100 : null;
      tape.vwap_src = "Polygon snapshot day.vw";
    }
    var jobs = [spyDaily(), fetchVwap(t)];
    var sess = sessionSplit(bars);
    if (!sess || sess.status === "DAILY_BARS") jobs.push(fetchMinute(t));
    else jobs.push(Promise.resolve(null));
    var extra = await Promise.all(jobs);
    var spy = extra[0];
    var polyVw = extra[1];
    if (tape && tape.vwap == null && polyVw != null) {
      tape.vwap = polyVw;
      tape.vs_vwap_pct = (tape.close && polyVw) ? (tape.close / polyVw - 1) * 100 : null;
      tape.vwap_src = "Polygon /v1/indicators/vwap daily";
    }
    if ((!sess || sess.status === "DAILY_BARS") && extra[2] && extra[2].length) sess = sessionSplit(extra[2]) || sess;
    row.tape = tape;
    row.vs_spy = rvolVsSpy(tape, spy);
    row.spy = spy;
    row.session = sess;
    row.snap = snap;
    return row;
  }

  async function of(ticker) {
    var t = bare(ticker);
    var pack = await Promise.all([
      darkPool(), finraShort(), f13(), dix(), liq(), shareFlows(), optConf(), lookthrough(),
      cq(), etfx(), cftc(), shortInterest()
    ]);
    var dp = pack[0] || {}, fs = pack[1] || {}, instDoc = pack[2] || {};
    var dixJ = pack[3] || {}, liqJ = pack[4] || {}, sfJ = pack[5] || {}, ocJ = pack[6] || {}, ltJ = pack[7] || {};
    var cqJ = pack[8] || {}, etfJ = pack[9] || {}, cftcJ = pack[10] || {}, siJ = pack[11] || {};
    var board = indexBoard(dp);
    var row = board[t] || null;
    var xray = (dp.xray_map || {})[t] || null;
    var share = (dp.dark_share_map || {})[t];
    var atsSh = (dp.dark_map || {})[t];
    if (!row && xray) {
      row = { ticker: t, state: xray.st || "NEUTRAL", dark_pool_pct: xray.dp, dark_accel: xray.acc, daily_off_exch_vol: xray.dv, ats_shares_wk: atsSh, dark_share: share };
    }
    if (row && share != null && row.dark_pool_pct == null) row.dark_pool_pct = share * 100;
    var inst = ((instDoc.tickers || instDoc.by_ticker || {})[t]) || null;
    var daily = ((fs.tickers || {})[t]) || null;
    var ping = row && (row.venue_fingerprint === "RETAIL_PING" || (num(row.ats_avg_trade_size) >= 1 && num(row.ats_avg_trade_size) < 200));
    var ownDix = dp.dix || {};
    var si = ((siJ.by_ticker || {})[t]) || null;
    return {
      ticker: t,
      ats: row,
      inst: inst,
      daily: daily,
      tape_week: row && num(row.total_vol_wk),
      dark_share: share,
      ping: !!ping,
      conf: confluence(row, inst, ping),
      venue: ((dp.monthly_ats || {}).share_map || {})[t] || null,
      venue_month: (dp.monthly_ats || {}).month,
      liq: ((liqJ.all_tickers || {})[t]) || null,
      shares: ((sfJ.tickers || {})[t]) || null,
      opt: ((ocJ.ticker_map || {})[t]) || null,
      look: ((ltJ.by_ticker || ltJ.ticker_map || {})[t]) || null,
      si: si,
      si_asof: si && (si.settlement_date || si.short_interest_as_of),
      crypto: pickCrypto(t, cqJ, etfJ),
      cot: pickCot(cftcJ.all_contract_analyses || [], t),
      cot_asof: cftcJ.generated_at,
      dix: {
        pct: (dixJ.current || {}).dix_pct,
        date: (dixJ.current || {}).date,
        regime: dixJ.dix_regime,
        gex_b: (dixJ.current || {}).gex_billions,
        gex_regime: dixJ.gex_regime,
        combined: dixJ.combined_regime,
        signal: dixJ.combined_signal,
        src: dixJ.source,
        own_pct: ownDix.own_dix_pct,
        own_read: ownDix.read,
        own_method: ownDix.method
      },
      meta: {
        ats_week: dp.latest_week,
        ats_age_days: dp.data_age_days,
        ats_lag: dp.expected_lag_days,
        ats_note: dp.lag_note,
        ats_src: dp.data_source,
        ats_caveats: dp.caveats,
        daily_date: fs.data_date,
        f13_quarter: instDoc.as_of_quarter,
        f13_as_of: instDoc.generated_at,
        n_scored: dp.n_scored,
        weekly_source: dp.weekly_source,
        sf_note: sfJ.disclaimer
      }
    };
  }

  global.JHInstVol = {
    of: of,
    enrich: enrich,
    tapeFromBars: tapeFromBars,
    sessionSplit: sessionSplit,
    rvolVsSpy: rvolVsSpy,
    confluence: confluence
  };
})(typeof window !== "undefined" ? window : globalThis);
