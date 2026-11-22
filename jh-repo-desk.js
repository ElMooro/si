/* jh-repo-desk.js - the Repo desk (2026-11-21).

   Khalid: "display an icon for it in the chart.html make sure it cover all the different aspect of the repo market
   and repo rate including their spikes in crisis".

   Two engines publish the repo market: justhodl-repo-market (data/repo-market.json - the overnight desk: rates, the
   SOFR distribution, spreads, Fed facilities, reserves, a 0-100 stress score) and justhodl-repo (data/repo.json - the
   master board of every series, each with its full history under data/repo-history/).  This file reads both as one
   market for repo.html: the Desk view (every aspect on one screen, each number with its own date) and the Crisis
   spikes view (what each measure did in every funding crisis its history covers).

   Nothing here is a stored opinion about a crisis.  A spike is measured: how far an observation stood above the
   median of the observations before it.  The only fixed table is EPISODES, which gives well-known date windows their
   usual names so a measured spike can be called "September 2019" instead of a bare date.

   The arithmetic is exported for tests (tests/repo-desk.test.js runs it on real numbers cut by ops 6896). */
(function (root) {
  "use strict";

  /* ---------------------------------------------------------------- numbers */
  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function dayNum(iso) { return Date.UTC(+iso.slice(0, 4), +iso.slice(5, 7) - 1, +iso.slice(8, 10)) / 86400000; }
  function median(list) {
    var a = list.filter(isNum).sort(function (x, y) { return x - y; });
    if (!a.length) return null;
    var m = a.length >> 1;
    return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
  }
  function place(sorted, v) {                      /* first index whose value is >= v */
    var lo = 0, hi = sorted.length;
    while (lo < hi) { var mid = (lo + hi) >> 1; if (sorted[mid] < v) lo = mid + 1; else hi = mid; }
    return lo;
  }

  /* The board publishes changes as a percentage of the level.  For a rate or a spread the move itself is wanted:
     the level it was measured against is last / (1 + pct / 100). */
  function prevFromPct(last, pct) {
    if (!isNum(last) || !isNum(pct)) return null;
    var ratio = 1 + pct / 100;                      /* below zero when a spread changed sign: -2 bp to +1 bp is -150% */
    return ratio === 0 ? null : last / ratio;
  }
  function moveFromPct(last, pct, scale) {
    var prev = prevFromPct(last, pct);
    return prev === null ? null : (last - prev) * (scale || 1);
  }

  /* ---------------------------------------------------------------- spikes
     excessSeries: for every observation, the median of the `window` observations before it and how far it stands
     above that median (times `scale`).  The first `minBase` observations have no baseline. */
  function excessSeries(dates, values, opt) {
    opt = opt || {};
    var w = opt.window || 60, need = Math.min(opt.minBase || 20, w), scale = opt.scale || 1;
    var n = Math.min(dates.length, values.length), sorted = [], queue = [], out = new Array(n);
    for (var i = 0; i < n; i++) {
      var v = values[i];
      if (sorted.length >= need && isNum(v)) {
        var m = sorted.length >> 1, base = sorted.length % 2 ? sorted[m] : (sorted[m - 1] + sorted[m]) / 2;
        out[i] = { base: base, excess: (v - base) * scale };
      } else out[i] = null;
      if (isNum(v)) {
        sorted.splice(place(sorted, v), 0, v);
        queue.push(v);
        if (queue.length > w) sorted.splice(place(sorted, queue.shift()), 1);
      }
    }
    return out;
  }

  /* spikes: the largest episodes, most extreme first.  Two peaks closer than `gapDays` are one episode (the higher
     one).  An episode runs over the neighbouring observations that stay above half its peak. */
  function spikes(dates, values, opt) {
    opt = opt || {};
    var ex = excessSeries(dates, values, opt), gap = opt.gapDays || 21, top = opt.top || 8, min = opt.min == null ? 0 : opt.min;
    var n = ex.length, order = [], picked = [], i;
    for (i = 0; i < n; i++) if (ex[i] && ex[i].excess > min) order.push(i);
    order.sort(function (a, b) { return ex[b].excess - ex[a].excess || a - b; });
    for (var k = 0; k < order.length && picked.length < top; k++) {
      i = order[k];
      var d = dayNum(dates[i]), clash = false;
      for (var p = 0; p < picked.length; p++) if (Math.abs(picked[p].day - d) <= gap) { clash = true; break; }
      if (clash) continue;
      var half = ex[i].excess / 2, a = i, b = i;
      while (a > 0 && ex[a - 1] && ex[a - 1].excess >= half && d - dayNum(dates[a - 1]) <= gap) a--;
      while (b < n - 1 && ex[b + 1] && ex[b + 1].excess >= half && dayNum(dates[b + 1]) - d <= gap) b++;
      picked.push({ index: i, day: d, date: dates[i], value: values[i], base: ex[i].base, excess: ex[i].excess, from: dates[a], to: dates[b], obs: b - a + 1 });
    }
    return picked;
  }

  /* The highest observation inside a date window, and the median of the `n` observations before the window opens. */
  function windowPeak(dates, values, from, to) {
    var best = null;
    for (var i = 0; i < dates.length; i++) {
      if (dates[i] < from) continue;
      if (dates[i] > to) break;
      if (isNum(values[i]) && (best === null || values[i] > best.value)) best = { date: dates[i], value: values[i], index: i };
    }
    return best;
  }
  function baselineBefore(dates, values, from, n) {
    var i = 0;
    while (i < dates.length && dates[i] < from) i++;
    var slice = values.slice(Math.max(0, i - (n || 60)), i).filter(isNum);
    return slice.length >= 5 ? median(slice) : null;
  }
  function spacingDays(dates) {
    var tail = dates.slice(-8), gaps = [];
    for (var i = 1; i < tail.length; i++) gaps.push(dayNum(tail[i]) - dayNum(tail[i - 1]));
    return gaps.length ? median(gaps) : null;
  }
  /* A baseline of about a quarter of the series' own time: 60 daily prints, 26 weekly, 12 monthly. */
  function windowFor(dates) {
    var gap = spacingDays(dates);
    if (gap === null || gap <= 4) return { window: 60, minBase: 20, gapDays: 21 };
    if (gap <= 10) return { window: 26, minBase: 8, gapDays: 60 };
    return { window: 12, minBase: 4, gapDays: 120 };
  }

  /* ---------------------------------------------------------------- names for dates
     Windows of well-known funding crises, newest first.  They only name dates; every number shown beside a name is
     read from the series. */
  var EPISODES = [
    { id: "ye2025", from: "2025-12-26", to: "2026-01-06", name: "Year-end 2025" },
    { id: "oct2025", from: "2025-10-27", to: "2025-11-06", name: "October 2025 month-end squeeze" },
    { id: "mar2023", from: "2023-03-09", to: "2023-03-31", name: "March 2023 bank failures" },
    { id: "mar2020", from: "2020-03-09", to: "2020-04-09", name: "March 2020 dash for cash" },
    { id: "sep2019", from: "2019-09-13", to: "2019-10-04", name: "September 2019 repo squeeze" },
    { id: "ye2018", from: "2018-12-27", to: "2019-01-04", name: "Year-end 2018" },
    { id: "mbs2010", from: "2010-10-20", to: "2010-12-22", name: "Late 2010 agency MBS fails" },
    { id: "gfc2008", from: "2008-09-12", to: "2008-12-31", name: "Lehman / global financial crisis" },
    { id: "bear2008", from: "2008-03-10", to: "2008-03-31", name: "Bear Stearns" },
    { id: "aug2007", from: "2007-08-09", to: "2007-09-18", name: "August 2007 money-market freeze" },
    { id: "fails2003", from: "2003-06-20", to: "2003-11-28", name: "2003 Treasury fails episode" },
    { id: "sep2001", from: "2001-09-11", to: "2001-10-05", name: "September 11, 2001" },
    { id: "y2k1999", from: "1999-12-27", to: "2000-01-05", name: "Year-end 1999 (Y2K)" },
    { id: "ltcm1998", from: "1998-08-17", to: "1998-10-30", name: "Russia default / LTCM" },
    { id: "oct1987", from: "1987-10-19", to: "1987-10-30", name: "October 1987 crash" }
  ];
  /* The turn of a period: the last days of a month and the first two of the next. */
  function calendarTurn(iso) {
    var day = +iso.slice(8, 10), month = +iso.slice(5, 7);
    if (day > 2 && day < 28) return "";
    if (day <= 2) month = month === 1 ? 12 : month - 1;
    return month === 12 ? "year-end" : month % 3 === 0 ? "quarter-end" : "month-end";
  }
  function labelFor(iso) {
    for (var i = 0; i < EPISODES.length; i++) if (iso >= EPISODES[i].from && iso <= EPISODES[i].to) return { kind: "episode", id: EPISODES[i].id, name: EPISODES[i].name };
    var turn = calendarTurn(iso);
    return turn ? { kind: "turn", id: turn, name: turn } : { kind: "", id: "", name: "" };
  }

  /* ---------------------------------------------------------------- the corridor
     The levels overnight rates trade between: the ON RRP award rate (floor), interest on reserves, the top of the
     target range (the Standing Repo Facility's rate).  From the feed's policy block; a feed older than 2026-11-21
     has only the spreads, and the levels are SOFR minus them. */
  function round2(v) { return Math.round(v * 100) / 100; }
  function corridor(desk) {
    desk = desk || {};
    var r = desk.rates || {}, sp = desk.spreads || {}, p = desk.policy || {}, dist = desk.distribution || {};
    var sofr = (r.sofr || {}).value;
    function back(block, sign) { var b = (sp[block] || {}).bps; return isNum(sofr) && isNum(b) ? round2(sofr - sign * b / 100) : null; }
    function rate(k) { var x = (r[k] || {}).value; return isNum(x) ? x : null; }
    return {
      as_of: (r.sofr || {}).as_of || desk.as_of || null,
      sofr: isNum(sofr) ? sofr : null, tgcr: rate("tgcr"), bgcr: rate("bgcr"), effr: rate("effr"), obfr: rate("obfr"),
      floor: isNum(p.on_rrp_award) ? p.on_rrp_award : back("sofr_floor", 1),
      admin: isNum(p.admin_rate) ? p.admin_rate : back("sofr_iorb", 1),
      top: isNum(p.range_top) ? p.range_top : back("ceiling_room", -1),
      p1: isNum(dist.p1) ? dist.p1 : null, p25: isNum(dist.p25) ? dist.p25 : null, p75: isNum(dist.p75) ? dist.p75 : null, p99: isNum(dist.p99) ? dist.p99 : null,
      from_policy_block: isNum(p.admin_rate)
    };
  }

  /* ---------------------------------------------------------------- freshness
     How old a number may be before the desk says so: a daily series over a long weekend, a weekly print with its
     publication lag, a monthly one with the weeks its source takes. */
  var ALLOW = { d: 6, w: 17, m: 80, q: 200, a: 500 };
  function ageDays(iso, todayIso) { return iso ? Math.round(dayNum(todayIso) - dayNum(iso.slice(0, 10))) : null; }
  function isLate(iso, todayIso, every) { var a = ageDays(iso, todayIso); return a === null ? true : a > (ALLOW[every] || ALLOW.d); }

  /* ---------------------------------------------------------------- formats */
  function fixed(v, d) { return (+v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d }); }
  function usd(v) {                                /* dollars -> $2.33T / $239B / $6.8B / $291M */
    if (!isNum(v)) return "—";
    var a = Math.abs(v);
    if (a >= 1e12) return "$" + fixed(v / 1e12, 2) + "T";
    if (a >= 1e10) return "$" + fixed(v / 1e9, 0) + "B";
    if (a >= 1e9) return "$" + fixed(v / 1e9, 1) + "B";
    if (a >= 1e6) return "$" + fixed(v / 1e6, 0) + "M";
    return "$" + fixed(v, 0);
  }
  var KIND = {
    rate: { fmt: function (v) { return fixed(v, 2) + "%"; }, move: "bp", scale: 100 },
    bp: { fmt: function (v) { return (v > 0 ? "+" : "") + fixed(v, 1) + " bp"; }, move: "bp", scale: 1 },
    pct: { fmt: function (v) { return fixed(v, 1) + "%"; }, move: "pt", scale: 1 },
    usd: { fmt: usd, move: "%", scale: 1 },
    usdm: { fmt: function (v) { return usd(v * 1e6); }, move: "%", scale: 1 },
    usdb: { fmt: function (v) { return usd(v * 1e9); }, move: "%", scale: 1 },
    num: { fmt: function (v) { return fixed(v, 2); }, move: "%", scale: 1 }
  };
  function fmtKind(kind, v) { return isNum(v) ? (KIND[kind] || KIND.num).fmt(v) : "—"; }
  /* The change of a tile over one of the board's horizons, in the unit that suits the kind. */
  function tileMove(kind, last, pct) {
    var k = KIND[kind] || KIND.num;
    if (!isNum(pct)) return null;
    if (k.move === "%") return { text: (pct > 0 ? "+" : "") + fixed(pct, 1) + "%", sign: pct };
    var mv = moveFromPct(last, pct, k.scale);
    if (mv === null) return null;
    return { text: (mv > 0 ? "+" : "") + fixed(mv, k.move === "bp" ? 1 : 2) + " " + k.move, sign: mv };
  }

  /* ---------------------------------------------------------------- what the desk shows
     Each tile reads one row of the master board (id) or one value of the desk feed (feed: path).  `every` is the
     rhythm its source publishes at, which sets when the desk calls the number late. */
  var SECTIONS = [
    { key: "rates", title: "Overnight rates", blurb: "What cash costs overnight, secured and unsecured, venue by venue.", tiles: [
      { id: "FNYR-SOFR-A", label: "SOFR", kind: "rate", every: "d", note: "Secured Overnight Financing Rate: the volume-weighted median of tri-party, GCF and cleared bilateral Treasury repo" },
      { id: "FNYR-BGCR-A", label: "BGCR", kind: "rate", every: "d", note: "Broad General Collateral Rate: tri-party plus GCF" },
      { id: "FNYR-TGCR-A", label: "TGCR", kind: "rate", every: "d", note: "Tri-Party General Collateral Rate" },
      { id: "REPO-TRIV1_AR_OO-P", label: "Tri-party, overnight", kind: "rate", every: "d", note: "Average rate in tri-party repo, excluding the Fed" },
      { id: "REPO-GCF_AR_TOT-P", label: "GCF repo", kind: "rate", every: "d", note: "FICC's blind-brokered inter-dealer market" },
      { id: "REPO-DVP_AR_OO-P", label: "DVP (cleared bilateral), overnight", kind: "rate", every: "d", note: "FICC delivery-versus-payment: where specific securities are financed" },
      { id: "FNYR-EFFR-A", label: "Fed funds effective", kind: "rate", every: "d", note: "The unsecured overnight rate between banks" },
      { id: "FNYR-OBFR-A", label: "OBFR", kind: "rate", every: "d", note: "Overnight Bank Funding Rate: fed funds plus eurodollars" },
      { id: "SOFR30DAYAVG", label: "SOFR, 30-day average", kind: "rate", every: "d" },
      { id: "SOFR90DAYAVG", label: "SOFR, 90-day average", kind: "rate", every: "d" },
      { id: "MMF-MY_MMF_RP_T_OO-M", label: "What money funds earn on Treasury repo, overnight", kind: "rate", every: "m", note: "Median yield, money-market-fund filings (monthly)" }
    ] },
    { key: "spreads", title: "Spreads — where stress shows first", blurb: "A repo squeeze is a spread before it is a headline: secured over unsecured, the tail over the median, one venue over another.", tiles: [
      { id: "D_SOFR_ADMIN", label: "SOFR − interest on reserves", kind: "bp", every: "d", note: "Above zero: cash is scarce enough that repo pays more than the Fed pays banks. +315 bp on 2019-09-17" },
      { id: "D_SOFR_EFFR", label: "SOFR − fed funds", kind: "bp", every: "d", note: "Secured above unsecured: the squeeze is on the collateral side" },
      { id: "D_SOFR_TAIL", label: "SOFR tail (99th percentile − SOFR)", kind: "bp", every: "d", note: "What the marginal borrower pays over the median; it widens before the median moves" },
      { id: "D_SOFR_P75_P25", label: "SOFR 75th − 25th percentile", kind: "bp", every: "d", note: "Dispersion of repo rates" },
      { feed: "desk.spreads.ceiling_room.bps", label: "Room to the ceiling (top of the range − SOFR)", kind: "bp", every: "d", date: "desk.as_of", note: "Zero or below: the market is at the Standing Repo Facility's rate" },
      { feed: "desk.spreads.sofr_floor.bps", label: "SOFR − ON RRP rate (the floor)", kind: "bp", every: "d", date: "desk.as_of", note: "How far repo trades above what the Fed pays money funds" },
      { feed: "desk.spreads.bgcr_tgcr.bps", label: "BGCR − TGCR", kind: "bp", every: "d", date: "desk.as_of", note: "GCF pressure over tri-party" },
      { feed: "desk.spreads.obfr_iorb.bps", label: "OBFR − interest on reserves", kind: "bp", every: "d", date: "desk.as_of", note: "The unsecured side of the same gauge" },
      { id: "D_GCF_TRI", label: "GCF − tri-party", kind: "bp", every: "d", note: "Dealers paying up among themselves over what money funds charge" },
      { id: "D_DVP_TRI", label: "DVP − tri-party", kind: "bp", every: "d", note: "Cleared bilateral premium" },
      { id: "D_DVP_SOFR", label: "DVP − SOFR", kind: "bp", every: "d", note: "Specials and scarcity" },
      { id: "D_TRI_TGCR", label: "Tri-party − TGCR", kind: "bp", every: "d" },
      { id: "D_EFFR_TARGET", label: "Fed funds − top of the target range", kind: "bp", every: "d", note: "Negative in normal times; the single target before December 2008" }
    ] },
    { key: "venues", title: "Venues — where repo trades", blurb: "How much is financed in each market, and how much of it must be rolled tomorrow.", tiles: [
      { id: "FNYR-SOFR_UV-A", label: "Volume behind SOFR", kind: "usd", every: "d" },
      { id: "REPO-TRIV1_TV_TOT-P", label: "Tri-party volume", kind: "usd", every: "d", note: "Excluding the Fed" },
      { id: "D_ON_SHARE_TRI", label: "Tri-party: overnight share", kind: "pct", every: "d", note: "The share that must be rolled tomorrow" },
      { id: "REPO-GCF_TV_TOT-P", label: "GCF volume", kind: "usd", every: "d" },
      { id: "REPO-DVP_TV_TOT-P", label: "DVP volume", kind: "usd", every: "d" },
      { id: "REPO-DVP_OV_TOT-P", label: "DVP outstanding", kind: "usd", every: "d" },
      { id: "D_ON_SHARE_DVP", label: "DVP: overnight share of outstanding", kind: "pct", every: "d" },
      { id: "FICC-SPONSORED_REPO_VOL", label: "FICC sponsored repo", kind: "usd", every: "m", note: "Money funds and hedge funds reaching the cleared market through a sponsor; the source publishes about five weeks behind" },
      { id: "FICC-SPONSORED_REVREPO_VOL", label: "FICC sponsored reverse repo", kind: "usd", every: "m" }
    ] },
    { key: "lenders", title: "Cash lenders — money-market funds", blurb: "Money funds are the largest lenders of cash in repo. Monthly, from their filings.", tiles: [
      { id: "MMF-MMF_RP_TOT-M", label: "Money-fund repo, total", kind: "usd", every: "m" },
      { id: "MMF-MMF_RP_wFR-M", label: "… with the Fed (ON RRP)", kind: "usd", every: "m" },
      { id: "MMF-MMF_RP_wFICC-M", label: "… cleared by FICC", kind: "usd", every: "m" },
      { id: "MMF-MMF_RP_wDFI-M", label: "… with US financial institutions", kind: "usd", every: "m" },
      { id: "MMF-MMF_RP_wFFI-M", label: "… with foreign financial institutions", kind: "usd", every: "m" },
      { id: "MMF-MMF_RP_T_TOT-M", label: "… backed by Treasuries", kind: "usd", every: "m" },
      { id: "MMF-MMF_RP_OO-M", label: "… overnight / open", kind: "usd", every: "m" }
    ] },
    { key: "dealers", title: "Dealers — financing, securities lending, fails", blurb: "Primary dealers sit in the middle: borrowing cash against securities, lending it on, and failing to deliver when collateral is scarce. Weekly.", tiles: [
      { feed: "pd.treasury.repo_out_b", label: "Dealer repo on Treasuries (cash borrowed)", kind: "usdb", every: "w", date: "pd.treasury.repo_out_b_as_of", note: "Primary dealers' securities out, NY Fed weekly statistics" },
      { feed: "pd.treasury.reverse_repo_in_b", label: "Dealer reverse repo on Treasuries (cash lent)", kind: "usdb", every: "w", date: "pd.treasury.reverse_repo_in_b_as_of", note: "Securities in. In a week the New York Fed withholds the figure, the last published week is shown under its own date" },
      { id: "NYPD-PD_SB_T_TOT-A", label: "Securities borrowed, Treasury", kind: "usd", every: "w" },
      { id: "NYPD-PD_SL_T_TOT-A", label: "Securities lent, Treasury", kind: "usd", every: "w" },
      { id: "NYPD-PD_AFtD_T-A", label: "Treasury fails to deliver", kind: "usd", every: "w", note: "$2.68 trillion in the week of 2008-10-22" },
      { id: "NYPD-PD_AFtR_T-A", label: "Treasury fails to receive", kind: "usd", every: "w" },
      { id: "NYPD-PD_AFtD_AG_MBS-A", label: "Agency MBS fails to deliver", kind: "usd", every: "w" },
      { id: "NYPD-PD_AFtD_TOT-A", label: "All fails to deliver", kind: "usd", every: "w" },
      { id: "DTCC-TREASURY-FAILS", label: "FICC daily Treasury fails", kind: "usd", every: "d", note: "DTCC's daily file" }
    ] },
    { key: "fed", title: "The Fed's side — floor, ceiling, reserves", blurb: "The facilities that bound the market and the reserves that fund it.", tiles: [
      { id: "RRPONTSYD", label: "ON RRP balance", kind: "usdb", every: "d", note: "Cash parked at the Fed overnight: the buffer that absorbed the 2022-24 drain" },
      { id: "RRPONTSYAWARD", label: "ON RRP award rate (the floor)", kind: "rate", every: "d" },
      { id: "IORB", label: "Interest on reserve balances", kind: "rate", every: "d" },
      { id: "DFEDTARU", label: "Top of the target range (the ceiling)", kind: "rate", every: "d" },
      { id: "SRF_TAKEUP", label: "Standing Repo Facility take-up", kind: "usdb", every: "d", note: "Anyone paying the ceiling rate is news" },
      { id: "D_FED_REPO", label: "Fed overnight repo operations", kind: "usdb", every: "d", note: "Treasury + agency + MBS collateral" },
      { id: "WREPOFOR", label: "Foreign official reverse-repo pool", kind: "usdm", every: "w", note: "Foreign central banks' cash at the Fed" },
      { id: "WLCFLPCL", label: "Discount window (primary credit)", kind: "usdm", every: "w" },
      { id: "SWPT", label: "Central-bank swap lines", kind: "usdm", every: "w" },
      { id: "WRESBAL", label: "Bank reserves", kind: "usdm", every: "w", note: "The supply side of repo cash" },
      { id: "WTREGEN", label: "Treasury General Account", kind: "usdm", every: "w", note: "When the Treasury rebuilds its balance, reserves fall" },
      { id: "WALCL", label: "Fed total assets", kind: "usdm", every: "w" }
    ] },
    { key: "collateral", title: "Collateral — haircuts in tri-party", blurb: "The cushion a lender takes on each kind of collateral. Median haircut, NY Fed, monthly.", tiles: [
      { id: "HAIRCUT-us-treasuries-excluding-strips--median_haircut", label: "Treasuries", kind: "pct", every: "m" },
      { id: "HAIRCUT-agency-mbs--median_haircut", label: "Agency MBS", kind: "pct", every: "m" },
      { id: "HAIRCUT-corporates-investment-grade--median_haircut", label: "Corporates, investment grade", kind: "pct", every: "m" },
      { id: "HAIRCUT-corporates-non-investment-grade--median_haircut", label: "Corporates, high yield", kind: "pct", every: "m" },
      { id: "HAIRCUT-equities--median_haircut", label: "Equities", kind: "pct", every: "m" },
      { id: "REPO-TRIV1_AR_CORD-P", label: "Tri-party rate on corporate debt", kind: "rate", every: "d" },
      { id: "REPO-TRIV1_AR_T-P", label: "Tri-party rate on Treasuries", kind: "rate", every: "d" }
    ] },
    { key: "europe", title: "Europe", blurb: "Overnight cash raised on each sovereign's collateral (ECB, per maintenance period), and what scarcity of Bunds looks like.", tiles: [
      { id: "MMSR-DE-ON-BORROW-TURNOVER", label: "On German collateral", kind: "num", every: "q", note: "EUR millions, average daily turnover" },
      { id: "MMSR-IT-ON-BORROW-TURNOVER", label: "On Italian collateral", kind: "num", every: "q" },
      { id: "MMSR-FR-ON-BORROW-TURNOVER", label: "On French collateral", kind: "num", every: "q" },
      { id: "MMSR-ES-ON-BORROW-TURNOVER", label: "On Spanish collateral", kind: "num", every: "q" },
      { id: "D_BUND_EA_AAA", label: "Bund − euro-area AAA 10-year", kind: "bp", every: "d", note: "Negative: Bunds trade rich to the AAA curve" },
      { id: "D_BTP_BUND_D", label: "BTP − Bund 10-year", kind: "bp", every: "m" }
    ] }
  ];

  /* What the Crisis view measures.  `scale` turns the series' own unit into the unit shown. */
  var MEASURES = [
    /* the two that reach back to February 1998: the New York Fed's primary-dealer survey rate, carried on by SOFR */
    { id: "D_REPO_TARGET", short: "Repo − policy rate", unit: "bp", scale: 1, group: "Repo rates", min: 25 },
    { id: "D_TARGET_REPO", short: "Policy rate − repo", unit: "bp", scale: 1, group: "Repo rates", min: 25 },
    { id: "D_SOFR_ADMIN", short: "SOFR − IOER/IORB", unit: "bp", scale: 1, group: "Repo rates", min: 5 },
    { id: "D_SOFR_TAIL", short: "SOFR tail", unit: "bp", scale: 1, group: "Repo rates", min: 5 },
    { id: "D_SOFR_EFFR", short: "SOFR − fed funds", unit: "bp", scale: 1, group: "Repo rates", min: 5 },
    { id: "D_GCF_TRI", short: "GCF − tri-party", unit: "bp", scale: 1, group: "Repo rates", min: 5 },
    { id: "D_EFFR_TARGET", short: "Fed funds − target", unit: "bp", scale: 1, group: "Unsecured", min: 10 },
    { id: "TEDRATE", short: "TED spread", unit: "bp", scale: 100, group: "Unsecured", min: 10 },
    { id: "NYPD-PD_AFtD_T-A", short: "Treasury fails", unit: "$B", scale: 1e-9, group: "Settlement", min: 20 },
    { id: "NYPD-PD_AFtD_AG_MBS-A", short: "Agency MBS fails", unit: "$B", scale: 1e-9, group: "Settlement", min: 20 },
    { id: "D_FED_REPO", short: "Fed repo operations", unit: "$B", scale: 1, group: "Fed backstops", min: 1 },
    { id: "SRF_TAKEUP", short: "Standing Repo Facility", unit: "$B", scale: 1, group: "Fed backstops", min: 1 },
    { id: "WLCFLPCL", short: "Discount window", unit: "$B", scale: 1e-3, group: "Fed backstops", min: 1 },
    { id: "SWPT", short: "Swap lines", unit: "$B", scale: 1e-3, group: "Fed backstops", min: 1 }
  ];
  function fmtMeasure(m, v) {
    if (!isNum(v)) return "—";
    var x = v * m.scale;
    return m.unit === "bp" ? (x > 0 ? "+" : "") + fixed(x, Math.abs(x) >= 100 ? 0 : 1) + " bp" : "$" + fixed(x, Math.abs(x) >= 100 ? 0 : 1) + "B";
  }
  /* One measure's history read against the named windows and by itself. */
  function readMeasure(m, hist) {
    var dates = hist.dates || [], values = hist.values || [], opt = windowFor(dates);
    /* a spike counts when the reading itself is above the measure's threshold, not only its distance from the quarter
       before: a spread that climbs from -70 to -8 the day the target changed is a jump, not a squeeze */
    var found = spikes(dates, values, { window: opt.window, minBase: opt.minBase, gapDays: opt.gapDays, top: 16, min: m.min / m.scale })
      .filter(function (s) { return s.value * m.scale >= m.min; }).slice(0, 8);
    var cells = {};
    EPISODES.forEach(function (e) {
      if (!dates.length || e.to < dates[0] || e.from > dates[dates.length - 1]) { cells[e.id] = null; return; }
      var peak = windowPeak(dates, values, e.from, e.to);
      cells[e.id] = peak ? { date: peak.date, value: peak.value, base: baselineBefore(dates, values, e.from, opt.window) } : null;
    });
    return { id: m.id, first: dates[0] || null, last: dates[dates.length - 1] || null, n: dates.length, spikes: found, cells: cells };
  }

  /* The spikes a series' own chart marks and lists.  A crisis measure is read exactly as the Crisis view reads it,
     with its own threshold - a climb from far below zero back to nothing is a return to normal, not a spike - so the
     chart and the table never disagree.  Every other series follows the general rule. */
  function chartSpikes(id, h) {
    var m = null;
    for (var i = 0; i < MEASURES.length; i++) if (MEASURES[i].id === id) m = MEASURES[i];
    if (m) return readMeasure(m, h).spikes.slice(0, 6);
    return spikes(h.dates, h.values, Object.assign({ top: 6 }, windowFor(h.dates)));
  }

  /* Dealers' Treasury financing out of the dealer engine's document (data/nyfed-primary-dealer.json).  In some weeks
     the New York Fed withholds one side, and the engine then publishes no figure for it.  The desk shows each side
     at its last published week under that week's own date, so a withheld week reads as an old number, not a blank. */
  function dealerFinancing(doc) {
    if (!doc || !doc.financing) return null;
    var t = doc.financing.treasury || {}, groups = doc.groups || {}, out = { as_of: t.as_of || null };
    [["repo_out_b", "treasury_repo"], ["reverse_repo_in_b", "treasury_reverse_repo"]].forEach(function (pair) {
      var key = pair[0], v = t[key], when = t.as_of || null, hist = (groups[pair[1]] || {}).history || [];
      if (!isNum(v)) {
        when = null;
        for (var i = hist.length - 1; i >= 0; i--) if (isNum(hist[i][1])) { v = hist[i][1] / 1000; when = hist[i][0]; break; }
      }
      out[key] = isNum(v) ? v : null;
      out[key + "_as_of"] = when;
    });
    return { treasury: out };
  }

  /* ---------------------------------------------------------------- rendering */
  function esc(t) { return String(t == null ? "" : t).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/"/g, "&quot;"); }
  function sign(v) { return v > 0 ? "pos" : v < 0 ? "neg" : ""; }
  function pick(obj, path) { return path.split(".").reduce(function (o, k) { return o == null ? o : o[k]; }, obj); }

  function tileHtml(t, ctx) {
    var row = t.id ? ctx.rows[t.id] : null, value, when, moves = "", click = "";
    if (t.feed) { value = pick(ctx, t.feed); when = t.date ? pick(ctx, t.date) : null; } else if (row) {
      value = row.last_value; when = row.last;
      var c = row.chg || {};
      moves = [["d", c.pop], ["w", c.w], ["m", c.m], ["y", c.y]].map(function (h) {
        var mv = tileMove(t.kind, value, h[1]);
        return mv ? "<span class='rd-mv " + sign(mv.sign) + "'><i>" + h[0] + "</i> " + esc(mv.text) + "</span>" : "";
      }).join("");
      click = " data-sid='" + esc(row.sid) + "' data-id='" + esc(row.id) + "'";
    }
    var missing = !isNum(value), late = !missing && isLate(when, ctx.today, t.every);
    return "<div class='rd-tile" + (click ? " rd-click" : "") + (missing ? " rd-missing" : "") + "'" + click + " title=\"" + esc((t.note || "") + (row ? " · " + row.id : "")) + "\">" +
      "<div class='rd-lab'>" + esc(t.label) + "</div>" +
      "<div class='rd-val'>" + (missing ? "<span class='rd-none'>not published</span>" : esc(fmtKind(t.kind, value))) + "</div>" +
      "<div class='rd-meta'><span class='rd-date" + (late ? " rd-late" : "") + "'>" + (when ? esc(String(when).slice(0, 10)) : "") + (late && when ? " · " + ageDays(String(when), ctx.today) + " days old" : "") + "</span>" + moves + "</div>" +
      (t.note ? "<div class='rd-note'>" + esc(t.note) + "</div>" : "") + "</div>";
  }

  function corridorSvg(c) {
    var marks = [["floor", c.floor, "ON RRP (floor)"], ["admin", c.admin, "IORB"], ["top", c.top, "SRF (ceiling)"]].filter(function (m) { return isNum(m[1]); });
    var rates = [["SOFR", c.sofr], ["TGCR", c.tgcr], ["BGCR", c.bgcr], ["EFFR", c.effr], ["OBFR", c.obfr]].filter(function (m) { return isNum(m[1]); });
    var all = marks.map(function (m) { return m[1]; }).concat(rates.map(function (m) { return m[1]; }), [c.p1, c.p99].filter(isNum));
    if (all.length < 2) return "";
    var lo = Math.min.apply(null, all) - 0.04, hi = Math.max.apply(null, all) + 0.04, W = 1000, H = 150;
    function X(v) { return 20 + (W - 40) * (v - lo) / (hi - lo); }
    var s = "<svg viewBox='0 0 " + W + " " + H + "' class='rd-corr' role='img' aria-label='Overnight rates inside the policy corridor'>";
    if (isNum(c.floor) && isNum(c.top)) s += "<rect x='" + X(c.floor) + "' y='52' width='" + (X(c.top) - X(c.floor)) + "' height='30' class='rd-band'/>";
    if (isNum(c.p1) && isNum(c.p99)) s += "<rect x='" + X(c.p1) + "' y='60' width='" + Math.max(2, X(c.p99) - X(c.p1)) + "' height='14' class='rd-p99'/>";
    if (isNum(c.p25) && isNum(c.p75)) s += "<rect x='" + X(c.p25) + "' y='57' width='" + Math.max(2, X(c.p75) - X(c.p25)) + "' height='20' class='rd-p75'/>";
    marks.forEach(function (m) {
      s += "<line x1='" + X(m[1]) + "' x2='" + X(m[1]) + "' y1='50' y2='90' class='rd-line rd-" + m[0] + "'/>" +
        "<text x='" + X(m[1]) + "' y='108' text-anchor='middle' class='rd-tick'>" + fixed(m[1], 2) + "%</text>" +
        "<text x='" + X(m[1]) + "' y='124' text-anchor='middle' class='rd-ticklab'>" + m[2] + "</text>";
    });
    var seen = {};
    rates.forEach(function (m) {
      var key = fixed(m[1], 2);
      (seen[key] = seen[key] || []).push(m[0]);
    });
    var others = 0;                                 /* SOFR is labelled on the top line; the other rates alternate between two lines so close values stay readable */
    Object.keys(seen).sort().forEach(function (key) {
      var v = +key, names = seen[key], isSofr = names.indexOf("SOFR") >= 0, y = isSofr ? 14 : (others++ % 2 ? 44 : 30);
      s += "<circle cx='" + X(v) + "' cy='67' r='" + (isSofr ? 7 : 5) + "' class='" + (isSofr ? "rd-dot rd-sofr" : "rd-dot") + "'/>" +
        "<text x='" + X(v) + "' y='" + y + "' text-anchor='middle' class='rd-ratelab" + (isSofr ? " rd-sofr" : "") + "'>" + names.join(" · ") + " " + key + "%</text>";
    });
    return s + "</svg>";
  }

  function renderDesk(el, ctx) {
    var d = ctx.desk || {}, c = corridor(d), h = "";
    var score = d.repo_stress_score, regime = d.regime || "—";
    var tone = !isNum(score) ? "" : score < 20 ? "calm" : score < 40 ? "firm" : score < 60 ? "elev" : "stress";
    h += "<div class='rd-hero rd-" + tone + "'><div class='rd-score'><b>" + (isNum(score) ? fixed(score, 1) : "—") + "</b><span>/ 100</span></div>" +
      "<div class='rd-heroTxt'><div class='rd-regime'>" + esc(regime) + (d.prev_regime && d.prev_regime !== regime ? " <small>was " + esc(d.prev_regime) + "</small>" : "") + "</div>" +
      "<div class='rd-head'>" + esc(d.headline || "The overnight desk feed is not available; the board below is.") + "</div>" +
      "<div class='rd-asof'>Rates as of " + esc(c.as_of || "—") + " · " + esc(d.source_primary || "") +
      ((d.calendar || {}).quarter_end_window ? " · <b>inside a quarter-end window</b>" : (d.calendar || {}).month_end_window ? " · <b>inside a month-end window</b>" : "") + "</div></div></div>";
    h += "<div class='rd-sec'><h2>The corridor</h2><p>Where overnight rates stand between the Fed's floor and its ceiling. The shaded bars are the SOFR distribution: 25th to 75th percentile, and 1st to 99th.</p><div class='rd-scroll'>" + corridorSvg(c) + "</div></div>";
    if ((d.components || []).length) {
      h += "<div class='rd-sec'><h2>What the stress score is made of</h2><div class='rd-comps'>" + d.components.map(function (k) {
        return "<div class='rd-comp' title=\"" + esc(k.detail) + "\"><div class='rd-compTop'><span>" + esc(k.label) + "</span><b>" + esc(k.reading) + "</b></div>" +
          "<div class='rd-bar'><i style='width:" + Math.max(0, Math.min(100, k.subscore || 0)) + "%'></i></div><small>" + fixed(k.subscore || 0, 0) + " of 100 · weight " + Math.round((k.weight || 0) * 100) + "%</small></div>";
      }).join("") + "</div></div>";
    }
    SECTIONS.forEach(function (s) {
      h += "<div class='rd-sec' data-sec='" + s.key + "'><h2>" + esc(s.title) + "</h2><p>" + esc(s.blurb) + "</p><div class='rd-grid'>" + s.tiles.map(function (t) { return tileHtml(t, ctx); }).join("") + "</div></div>";
    });
    var cal = d.calendar || {};
    if (isNum(cal.median_tail_quarter_end_bps)) {
      h += "<div class='rd-sec'><h2>The calendar</h2><p>Dealers shrink their balance sheets for reporting dates, so month-ends and quarter-ends are tighter by habit. Since April 2018 the SOFR tail has been " +
        fixed(cal.median_tail_quarter_end_bps, 0) + " bp at a typical quarter-end against " + fixed(cal.median_tail_normal_bps, 0) + " bp on an ordinary day. Today is " +
        (cal.quarter_end_window ? "inside a quarter-end window" : cal.month_end_window ? "inside a month-end window" : "outside any period-end window") + ".</p></div>";
    }
    h += "<div class='rd-foot'>Each number carries the date its source last printed; one older than its source's rhythm allows is marked. Click a number for its full history with its largest spikes marked.</div>";
    el.innerHTML = h;
    Array.prototype.forEach.call(el.querySelectorAll(".rd-click"), function (n) { n.onclick = function () { ctx.open(n.getAttribute("data-sid"), n.getAttribute("data-id")); }; });
  }

  function renderCrisis(el, ctx) {
    var have = MEASURES.filter(function (m) { return ctx.rows[m.id]; });
    el.innerHTML = "<div class='rd-sec'><h2>Crisis spikes</h2><p>Reading " + have.length + " histories…</p></div>";
    return Promise.all(have.map(function (m) {
      return ctx.history(ctx.rows[m.id].sid).then(function (hist) { return { m: m, read: readMeasure(m, hist) }; }, function () { return { m: m, read: null }; });
    })).then(function (list) {
      list = list.filter(function (x) { return x.read && x.read.n; });
      var h = "<div class='rd-sec'><h2>What each measure did in every funding crisis its history covers</h2>" +
        "<p>The highest reading inside each episode's dates, taken from the series itself. A dash means the series had not begun or had ended; the first date of each is under its name. " +
        "The repo rate itself reaches back to February 1998: the New York Fed's primary-dealer survey rate for overnight Treasury repo (daily until February 2018), carried on by SOFR. It is shown against the Fed's policy rate both ways round, because a crisis prints on either side: <b>repo − policy rate</b> jumps when cash is scarce (September 2019, period-ends); <b>policy rate − repo</b> jumps when everyone wants Treasury collateral and repo collapses under the policy rate (August 2007, March and September 2008). October 1987 predates any public daily repo rate and is read through fed funds against its target and the TED spread.</p>" +
        "<div class='rd-scroll'><table class='rd-matrix'><thead><tr><th class='namecell'>Episode</th>" + list.map(function (x) {
          return "<th data-sid='" + esc(ctx.rows[x.m.id].sid) + "' data-id='" + esc(x.m.id) + "' class='rd-click' title=\"" + esc(ctx.rows[x.m.id].label || x.m.id) + "\">" + esc(x.m.short) + "<small>" + esc(x.m.group) + " · from " + esc((x.read.first || "").slice(0, 7)) + "</small></th>";
        }).join("") + "</tr></thead><tbody>";
      EPISODES.forEach(function (e) {
        var cells = list.map(function (x) {
          var cell = x.read.cells[e.id];
          if (!cell) return "<td class='na'>—</td>";
          var over = isNum(cell.base) ? (cell.value - cell.base) * x.m.scale : null, hot = isNum(over) && over >= x.m.min * 3;
          return "<td class='" + (hot ? "rd-hot" : "") + "' title=\"" + esc(cell.date + (isNum(cell.base) ? " · " + fmtMeasure(x.m, cell.base).replace(/^\+/, "") + " before" : "")) + "\"><b>" + esc(fmtMeasure(x.m, cell.value)) + "</b><small>" + esc(cell.date) + "</small></td>";
        }).join("");
        h += "<tr><td class='namecell'><b>" + esc(e.name) + "</b><small>" + e.from + " → " + e.to + "</small></td>" + cells + "</tr>";
      });
      h += "</tbody></table></div></div>";
      h += "<div class='rd-sec'><h2>The largest spikes the data itself shows</h2><p>For each measure, the days it stood furthest above its own median of the quarter before (60 daily prints, 26 weekly). Period-ends are named as such; an unnamed date is simply a date.</p><div class='rd-lists'>";
      list.forEach(function (x) {
        h += "<div class='rd-list'><h3 class='rd-click' data-sid='" + esc(ctx.rows[x.m.id].sid) + "' data-id='" + esc(x.m.id) + "'>" + esc(x.m.short) + " <small>" + esc(x.read.first) + " → " + esc(x.read.last) + "</small></h3>" +
          (x.read.spikes.length ? "<ol>" + x.read.spikes.map(function (s) {
            var lab = labelFor(s.date);
            return "<li><span class='rd-d'>" + esc(s.date) + "</span><b>" + esc(fmtMeasure(x.m, s.value)) + "</b><span class='rd-over'>" + esc(fmtMeasure(x.m, s.value - s.base).replace(/^\+?/, "+")) + " over</span>" +
              (lab.name ? "<span class='rd-tag rd-" + lab.kind + "'>" + esc(lab.name) + "</span>" : "") + "</li>";
          }).join("") + "</ol>" : "<p class='tag'>No spike above its threshold in this history.</p>") + "</div>";
      });
      h += "</div></div>";
      el.innerHTML = h;
      Array.prototype.forEach.call(el.querySelectorAll(".rd-click"), function (n) { n.onclick = function () { ctx.open(n.getAttribute("data-sid"), n.getAttribute("data-id")); }; });
      return list;
    });
  }

  var api = {
    median: median, prevFromPct: prevFromPct, moveFromPct: moveFromPct, excessSeries: excessSeries, spikes: spikes, windowPeak: windowPeak,
    baselineBefore: baselineBefore, spacingDays: spacingDays, windowFor: windowFor, calendarTurn: calendarTurn, labelFor: labelFor, corridor: corridor,
    ageDays: ageDays, isLate: isLate, usd: usd, fmtKind: fmtKind, tileMove: tileMove, fmtMeasure: fmtMeasure, readMeasure: readMeasure, dealerFinancing: dealerFinancing, chartSpikes: chartSpikes,
    EPISODES: EPISODES, SECTIONS: SECTIONS, MEASURES: MEASURES, tileHtml: tileHtml, corridorSvg: corridorSvg, renderDesk: renderDesk, renderCrisis: renderCrisis
  };
  root.JHRepoDesk = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
