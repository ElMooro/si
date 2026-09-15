/* Institutional volume fuse — FINRA ATS + Polygon tape + 13F + derived overlays.
 * Cadences never blended. Bloomberg-grade means labeled, not averaged.
 */
(function (global) {
  "use strict";
  var DP = null, FS = null, F13 = null, DIX = null, LIQ = null, SF = null, OC = null, LT = null;

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

  function median(arr) {
    var a = arr.filter(function (x) { return x != null && isFinite(x); }).sort(function (x, y) { return x - y; });
    if (!a.length) return null;
    var m = Math.floor(a.length / 2);
    return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
  }

  function tapeFromBars(bars, shares) {
    if (!bars || !bars.length) return null;
    var last = bars[bars.length - 1];
    var v = num(last.volume);
    if (v == null) return null;
    var i, s = 0, n = 0, vols = [], ranges = [], up = 0, dn = 0, px = num(last.close) || num(last.c);
    for (i = Math.max(0, bars.length - 21); i < bars.length - 1; i++) {
      var x = num(bars[i].volume);
      if (x == null) continue;
      s += x; n++;
      vols.push(x);
      var hi = num(bars[i].high) || num(bars[i].h);
      var lo = num(bars[i].low) || num(bars[i].l);
      var cl = num(bars[i].close) || num(bars[i].c);
      var op = num(bars[i].open) || num(bars[i].o);
      if (hi != null && lo != null && cl) ranges.push((hi - lo) / cl);
      if (cl != null && op != null) {
        if (cl >= op) up += x;
        else dn += x;
      }
    }
    var avg = n ? s / n : null;
    var med = median(vols);
    var rvol = med ? v / med : (avg ? v / avg : null);
    var hi = num(last.high) || num(last.h);
    var lo = num(last.low) || num(last.l);
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
    return {
      last: v,
      avg20: avg,
      median20: med,
      vs_pct: avg ? (v / avg - 1) * 100 : null,
      rvol: rvol,
      dollar: px != null ? v * px : null,
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

  function confluence(ats, inst, ping) {
    var a = atsSide(ats && ats.state);
    var f = f13Side(inst);
    if (ping) {
      return {
        verdict: "NOT INSTITUTIONAL SIZE",
        why: "ATS avg trade is retail-ping scale. Dark % here is not block flow. Do not treat as smart-money volume.",
        a: a, f: f
      };
    }
    if (a && f && a === f) return { verdict: a > 0 ? "CONFIRMED ACCUMULATION" : "CONFIRMED DISTRIBUTION", why: "FINRA ATS weekly state agrees with 13F net holders. Cadences still differ — ATS is weeks, 13F is a quarter.", a: a, f: f };
    if (a && f && a !== f) return { verdict: "DIVERGENT", why: "ATS weekly and 13F quarterly disagree. Do not average them. One of them is stale or the tape is not the 13F book.", a: a, f: f };
    if (a) return { verdict: "ATS ONLY · " + ats.state, why: "No usable 13F confirmation for this name (or 13F is mixed). Weekly ATS stands alone.", a: a, f: f };
    if (f) return { verdict: f > 0 ? "13F ONLY · NET BUYING" : "13F ONLY · NET SELLING", why: "Holdings change without an ATS accumulation/distribution flag this week.", a: a, f: f };
    return { verdict: "NO CONFLUENCE", why: "ATS is NEUTRAL or missing and 13F is mixed/absent. Volume is not a signal here.", a: a, f: f };
  }

  async function of(ticker) {
    var t = bare(ticker);
    var pack = await Promise.all([darkPool(), finraShort(), f13(), dix(), liq(), shareFlows(), optConf(), lookthrough()]);
    var dp = pack[0] || {}, fs = pack[1] || {}, instDoc = pack[2] || {};
    var dixJ = pack[3] || {}, liqJ = pack[4] || {}, sfJ = pack[5] || {}, ocJ = pack[6] || {}, ltJ = pack[7] || {};
    var board = indexBoard(dp);
    var row = board[t] || null;
    var xray = (dp.xray_map || {})[t] || null;
    var share = (dp.dark_share_map || {})[t];
    var atsSh = (dp.dark_map || {})[t];
    if (!row && xray) {
      row = {
        ticker: t,
        state: xray.st || "NEUTRAL",
        dark_pool_pct: xray.dp,
        dark_accel: xray.acc,
        daily_off_exch_vol: xray.dv,
        ats_shares_wk: atsSh,
        dark_share: share
      };
    }
    if (row && share != null && row.dark_pool_pct == null) row.dark_pool_pct = share * 100;
    var inst = ((instDoc.tickers || instDoc.by_ticker || {})[t]) || null;
    var daily = ((fs.tickers || {})[t]) || null;
    var ping = row && (row.venue_fingerprint === "RETAIL_PING" || (num(row.ats_avg_trade_size) >= 1 && num(row.ats_avg_trade_size) < 200));
    var conf = confluence(row, inst, ping);
    var venue = ((dp.monthly_ats || {}).share_map || {})[t] || null;
    var ownDix = dp.dix || {};
    return {
      ticker: t,
      ats: row,
      inst: inst,
      daily: daily,
      tape_week: row && num(row.total_vol_wk),
      dark_share: share,
      ping: !!ping,
      conf: conf,
      venue: venue,
      venue_month: (dp.monthly_ats || {}).month,
      liq: ((liqJ.all_tickers || {})[t]) || null,
      shares: ((sfJ.tickers || {})[t]) || null,
      opt: ((ocJ.ticker_map || {})[t]) || null,
      look: ((ltJ.by_ticker || ltJ.ticker_map || {})[t]) || null,
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

  global.JHInstVol = { of: of, tapeFromBars: tapeFromBars, confluence: confluence };
})(typeof window !== "undefined" ? window : globalThis);
