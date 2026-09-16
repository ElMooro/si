/* JustHodl institutional studies — FVG, OR/IB, overnight, ADR, LinReg channel,
   earnings AVWAP, equal H/L, session map, vs S&P 500 (cash, exact NY-day join).
   Pure OHLC; no tick, no GEX. */
(function (root) {
  if (root.__jhInstV1) return;
  root.__jhInstV1 = true;
  var UP = "#089981", DN = "#f23645", GOLD = "#f0b429", CYAN = "#26c6da", BLUE = "#2962ff", MUTE = "#787b86", PURP = "#7e57c2", ORG = "#ff6d00";

  function empty(note) {
    return { markers: [], zones: [], lines: [], shapes: [], seps: [], bands: [], note: note || "", legendPts: [] };
  }
  function atrAt(d, i, n) {
    n = n || 14;
    var s = 0, c = 0, j, tr;
    for (j = Math.max(1, i - n); j <= i; j++) {
      tr = Math.max(d[j].high - d[j].low, Math.abs(d[j].high - d[j - 1].close), Math.abs(d[j].low - d[j - 1].close));
      s += tr; c++;
    }
    return c ? s / c : (d[i] && d[i].close ? d[i].close * 0.008 : 1);
  }
  function isIntra(d) {
    if (!d || d.length < 2) return false;
    var g = d[d.length - 1].time - d[d.length - 2].time;
    return g > 0 && g < 18 * 3600;
  }
  function nyClock(ts) {
    var s = new Date(ts * 1000).toLocaleString("en-US", { timeZone: "America/New_York", hour12: false });
    var p = s.match(/(\d+)\/(\d+)\/(\d+),?\s+(\d+):(\d+)/);
    if (!p) return { y: 1970, m: 1, d: 1, hh: 0, mm: 0, min: 0, key: "1970-1-1" };
    var hh = +p[4]; if (hh === 24) hh = 0;
    return { y: +p[3], m: +p[1], d: +p[2], hh: hh, mm: +p[5], min: hh * 60 + +p[5], key: p[3] + "-" + p[1] + "-" + p[2] };
  }
  function nextKey(c) {
    var t = Date.UTC(c.y, c.m - 1, c.d) / 1000 + 36 * 3600;
    return nyClock(t).key;
  }
  function prevKey(c) {
    var t = Date.UTC(c.y, c.m - 1, c.d) / 1000 - 12 * 3600;
    return nyClock(t).key;
  }
  function sessionKey(ts) {
    var c = nyClock(ts);
    if (c.min >= 960) return nextKey(c);
    return c.key;
  }

  function fvgGaps(d, opt) {
    opt = opt || {};
    var out = [], i, j, n = (d && d.length) || 0;
    if (n < 4) return out;
    var minPct = opt.minPct != null ? opt.minPct : 0.0004;
    var start = Math.max(2, n - (opt.lookback || 280));
    for (i = start; i < n; i++) {
      var atr = atrAt(d, i, 14), floor = Math.max((d[i].close || 1) * minPct, atr * 0.12);
      if (d[i - 2].high < d[i].low) {
        var lo = d[i - 2].high, hi = d[i].low;
        if (hi - lo >= floor) out.push({ dir: 1, t0: d[i - 2].time, t1: d[i].time, i: i, lo: lo, hi: hi, mid: (lo + hi) / 2, filled: false });
      } else if (d[i - 2].low > d[i].high) {
        var lo2 = d[i].high, hi2 = d[i - 2].low;
        if (hi2 - lo2 >= floor) out.push({ dir: -1, t0: d[i - 2].time, t1: d[i].time, i: i, lo: lo2, hi: hi2, mid: (lo2 + hi2) / 2, filled: false });
      }
    }
    for (i = 0; i < out.length; i++) {
      var g = out[i];
      for (j = g.i + 1; j < n; j++) {
        if (g.dir > 0 && d[j].low <= g.lo) { g.filled = true; g.fillT = d[j].time; break; }
        if (g.dir < 0 && d[j].high >= g.hi) { g.filled = true; g.fillT = d[j].time; break; }
      }
    }
    var open = out.filter(function (g) { return !g.filled; });
    var filled = out.filter(function (g) { return g.filled; });
    return open.slice(-10).concat(filled.slice(-3));
  }
  function fvgPack(d) {
    var gaps = fvgGaps(d), pack = empty(gaps.length ? "" : "no 3-candle imbalance on this window");
    var lastT = d && d.length ? d[d.length - 1].time : 0;
    gaps.forEach(function (g) {
      var col = g.dir > 0 ? "rgba(8,153,129," + (g.filled ? ".08" : ".22") + ")" : "rgba(242,54,69," + (g.filled ? ".08" : ".22") + ")";
      pack.zones.push({ t0: g.t0, t1: g.filled && g.fillT ? g.fillT : lastT, hi: g.hi, lo: g.lo, color: col, kind: g.dir > 0 ? "dem" : "sup", lab: g.filled ? "" : (g.dir > 0 ? "FVG↑" : "FVG↓") });
    });
    var live = gaps.filter(function (g) { return !g.filled; });
    if (live.length) pack.legendPts = [{ time: lastT, value: live[live.length - 1].mid }];
    return pack;
  }

  function swings(d, L) {
    L = L || (isIntra(d) ? 3 : 5);
    var hi = [], lo = [], i, j, n = d.length, h, l, okh, okl;
    for (i = L; i < n - L; i++) {
      h = d[i].high; l = d[i].low; okh = true; okl = true;
      for (j = i - L; j <= i + L; j++) {
        if (j === i) continue;
        if (d[j].high > h) okh = false;
        if (d[j].low < l) okl = false;
        if (!okh && !okl) break;
      }
      if (okh) hi.push({ i: i, t: d[i].time, px: h, k: "h" });
      if (okl) lo.push({ i: i, t: d[i].time, px: l, k: "l" });
    }
    return { hi: hi, lo: lo };
  }
  function clusterSwings(pts, tol) {
    var out = [], used = [], i, j, g;
    for (i = 0; i < pts.length; i++) used[i] = 0;
    for (i = 0; i < pts.length; i++) {
      if (used[i]) continue;
      g = [pts[i]]; used[i] = 1;
      for (j = i + 1; j < pts.length; j++) {
        if (used[j]) continue;
        if (Math.abs(pts[j].px - pts[i].px) <= tol) { g.push(pts[j]); used[j] = 1; }
      }
      if (g.length >= 2) {
        var s = 0; for (j = 0; j < g.length; j++) s += g[j].px;
        out.push({ px: s / g.length, n: g.length, t0: g[0].t, t1: g[g.length - 1].t, k: g[0].k });
      }
    }
    return out;
  }
  function equalHL(d) {
    if (!d || d.length < 16) return empty("need more bars");
    if (d.length > 260) d = d.slice(-260);
    var sw = swings(d), last = d[d.length - 1];
    var tol = Math.max(atrAt(d, d.length - 1, 14) * 0.18, last.close * 0.0012);
    var eqh = clusterSwings(sw.hi, tol), eql = clusterSwings(sw.lo, tol);
    var pack = empty(""), t1 = last.time;
    eqh.slice(-6).forEach(function (c) {
      pack.lines.push({ t0: c.t0, t1: t1, px: c.px, color: GOLD, dash: "4 3", lab: "EQH×" + c.n, kind: "liq" });
    });
    eql.slice(-6).forEach(function (c) {
      pack.lines.push({ t0: c.t0, t1: t1, px: c.px, color: CYAN, dash: "4 3", lab: "EQL×" + c.n, kind: "liq" });
    });
    if (pack.lines.length) pack.legendPts = [{ time: t1, value: pack.lines[pack.lines.length - 1].px }];
    else pack.note = "no equal highs/lows in window";
    return pack;
  }

  function dailyFrom(d) {
    if (!d || !d.length) return [];
    if (!isIntra(d)) return d.slice();
    var map = {}, order = [], i, k, b, cur;
    for (i = 0; i < d.length; i++) {
      k = nyClock(d[i].time).key;
      b = d[i];
      cur = map[k];
      if (!cur) { cur = { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume || 0 }; map[k] = cur; order.push(cur); }
      else {
        if (b.high > cur.high) cur.high = b.high;
        if (b.low < cur.low) cur.low = b.low;
        cur.close = b.close; cur.volume += b.volume || 0;
      }
    }
    return order;
  }
  function adr20(d, n) {
    n = n || 20;
    var days = dailyFrom(d);
    if (!days || days.length < 6) return null;
    var take = Math.min(n, days.length - 1), s = 0, i, a = days.length - 1 - take;
    if (a < 0) a = 0;
    var cnt = 0;
    for (i = a; i < days.length - 1; i++) { s += days[i].high - days[i].low; cnt++; }
    if (!cnt) return null;
    var adr = s / cnt, today = days[days.length - 1], used = adr ? (today.high - today.low) / adr * 100 : 0;
    return {
      adr: adr, used: used, open: today.open,
      hi: today.open + adr, lo: today.open - adr,
      todayHi: today.high, todayLo: today.low, n: cnt, time: today.time
    };
  }
  function adrUsedSeries(d, n) {
    n = n || 20;
    var days = dailyFrom(d);
    if (days.length < n + 2) return [];
    var o = [], i, j, s;
    for (i = n; i < days.length; i++) {
      s = 0;
      for (j = i - n; j < i; j++) s += days[j].high - days[j].low;
      var adr = s / n;
      o.push({ time: days[i].time, value: adr ? 100 * (days[i].high - days[i].low) / adr : 0 });
    }
    return o;
  }

  function linregChannel(d, n, k) {
    n = n || 100; k = k || 2;
    if (!d || d.length < 12) return { m: [], up: [], dn: [], slope: 0, sigma: 0 };
    n = Math.min(n, d.length);
    var slice = d.slice(d.length - n), sx = 0, sy = 0, sxy = 0, sx2 = 0, i;
    for (i = 0; i < n; i++) { sx += i; sy += slice[i].close; sxy += i * slice[i].close; sx2 += i * i; }
    var den = n * sx2 - sx * sx, sl = den ? (n * sxy - sx * sy) / den : 0, ic = (sy - sl * sx) / n, se = 0;
    for (i = 0; i < n; i++) { var e = slice[i].close - (ic + sl * i); se += e * e; }
    se = Math.sqrt(se / Math.max(1, n - 2));
    var step = n >= 2 ? (slice[n - 1].time - slice[0].time) / Math.max(1, n - 1) : 86400;
    var m = [], up = [], dn = [], extra = 8;
    for (i = 0; i < n + extra; i++) {
      var t = i < n ? slice[i].time : slice[n - 1].time + Math.round((i - n + 1) * step);
      var y = ic + sl * i;
      m.push({ time: t, value: y });
      up.push({ time: t, value: y + k * se });
      dn.push({ time: t, value: y - k * se });
    }
    return { m: m, up: up, dn: dn, slope: sl, sigma: se };
  }

  function avwapFrom(d, t0) {
    if (!d || !d.length || t0 == null) return [];
    var o = [], pv = 0, vv = 0, i, on = false;
    for (i = 0; i < d.length; i++) {
      if (!on && d[i].time < t0) continue;
      on = true;
      var tp = (d[i].high + d[i].low + d[i].close) / 3;
      pv += tp * (d[i].volume || 0); vv += d[i].volume || 0;
      if (vv) o.push({ time: d[i].time, value: pv / vv });
    }
    return o;
  }
  function lastEarnTime(d, pack, tkr, snap) {
    if (!d || !d.length || !pack) return null;
    var want = String(tkr || "").toUpperCase(), best = null, rows = [];
    function add(arr) { (arr || []).forEach(function (ev) { rows.push(ev); }); }
    add(pack.events); add(pack.recent); add(pack.forward); add(pack.upcoming);
    rows.forEach(function (ev) {
      var tk = String(ev.ticker || "").toUpperCase();
      if (tk && tk !== want) return;
      var type = String(ev.type || "");
      if (type && type.indexOf("EARN") < 0 && ev.eps_actual == null && !ev.earnings_date && !ev.date) return;
      var ymd = ev.date || ev.earnings_date || ev.filing_date;
      var t = snap ? snap(d, ymd) : null;
      if (t != null && (best == null || t > best)) best = t;
    });
    return best;
  }

  function hiLoOf(bars) {
    if (!bars || !bars.length) return null;
    var hi = -1e99, lo = 1e99, i;
    for (i = 0; i < bars.length; i++) {
      if (bars[i].high > hi) hi = bars[i].high;
      if (bars[i].low < lo) lo = bars[i].low;
    }
    if (hi < -1e90) return null;
    return { hi: hi, lo: lo, t0: bars[0].time, t1: bars[bars.length - 1].time };
  }
  function sessionLevels(intra) {
    if (!intra || intra.length < 8) return null;
    if (!isIntra(intra)) return null;
    var last = intra[intra.length - 1], want = sessionKey(last.time);
    var or15 = [], or30 = [], ib = [], rth = [], on = [], i, c, sk, prev;
    for (i = 0; i < intra.length; i++) {
      c = nyClock(intra[i].time);
      sk = sessionKey(intra[i].time);
      if (sk === want && c.min >= 570 && c.min < 585) or15.push(intra[i]);
      if (sk === want && c.min >= 570 && c.min < 600) or30.push(intra[i]);
      if (sk === want && c.min >= 570 && c.min < 630) ib.push(intra[i]);
      if (sk === want && c.min >= 570 && c.min < 960) rth.push(intra[i]);
    }
    prev = prevKey(nyClock(last.time));
    if (nyClock(last.time).min >= 960) prev = nyClock(last.time).key;
    for (i = 0; i < intra.length; i++) {
      c = nyClock(intra[i].time);
      sk = sessionKey(intra[i].time);
      if (sk === want && c.min < 570) on.push(intra[i]);
      if (sk !== want && nyClock(intra[i].time).key === prev && c.min >= 960) on.push(intra[i]);
    }
    var a = hiLoOf(or15), b = hiLoOf(or30), ibx = hiLoOf(ib), onx = hiLoOf(on);
    return {
      session: want,
      or15: a, or30: b, ib: ibx, overnight: onx,
      rth: hiLoOf(rth),
      ready: !!(a || b || ibx || onx)
    };
  }

  function globalSessions(d) {
    var pack = empty("");
    if (!isIntra(d)) { pack.note = "Asia/London/NY shades need an intraday interval"; return pack; }
    var i, prev = null, band = null;
    function kindOf(min) {
      if (min >= 570 && min < 960) return "ny";
      if (min >= 180 && min < 570) return "lon";
      return "asia";
    }
    var col = { ny: "rgba(41,98,255,.07)", lon: "rgba(171,71,188,.07)", asia: "rgba(38,198,218,.06)" };
    for (i = 0; i < d.length; i++) {
      var c = nyClock(d[i].time), k = kindOf(c.min);
      if (prev && c.key !== prev.key) pack.seps.push(d[i].time);
      if (!band || band.kind !== k) {
        if (band) pack.bands.push(band);
        band = { kind: k, t0: d[i].time, t1: d[i].time, color: col[k] };
      } else band.t1 = d[i].time;
      prev = c;
    }
    if (band) pack.bands.push(band);
    return pack;
  }
  function separators(d) {
    var pack = empty("");
    if (!isIntra(d)) { pack.note = "session separators need an intraday interval"; return pack; }
    var i, prev = null;
    for (i = 0; i < d.length; i++) {
      var c = nyClock(d[i].time);
      if (prev && c.key !== prev.key) pack.seps.push(d[i].time);
      prev = c;
    }
    return pack;
  }

  function ratioVs(d, spy) {
    var j = alignExact(d, spy), o = [], i;
    for (i = 0; i < j.length; i++) if (j[i].s) o.push({ time: j[i].time, value: j[i].a / j[i].s });
    return o;
  }
  function barGap(d) {
    if (!d || d.length < 3) return 86400;
    var g = d[d.length - 1].time - d[d.length - 2].time;
    return g > 0 ? g : 86400;
  }
  function alignExact(d, bench) {
    if (!d || !bench || !d.length || !bench.length) return [];
    var intra = isIntra(d) && isIntra(bench);
    var o = [], i, k, j;
    if (!intra) {
      var map = {};
      for (i = 0; i < bench.length; i++) {
        k = nyClock(bench[i].time).key;
        if (bench[i].close) map[k] = { s: bench[i].close, si: i };
      }
      for (i = 0; i < d.length; i++) {
        k = nyClock(d[i].time).key;
        if (!map[k] || !d[i].close) continue;
        o.push({ time: d[i].time, a: d[i].close, s: map[k].s, ai: i, si: map[k].si, day: k });
      }
      return o;
    }
    var byT = {}, byDay = {};
    var gap = Math.max(30, Math.min(barGap(d), barGap(bench)));
    for (i = 0; i < bench.length; i++) {
      if (!bench[i].close) continue;
      byT[bench[i].time] = { s: bench[i].close, si: i, t: bench[i].time };
      k = nyClock(bench[i].time).key;
      if (!byDay[k]) byDay[k] = [];
      byDay[k].push(bench[i]);
    }
    for (i = 0; i < d.length; i++) {
      if (!d[i].close) continue;
      var hit = byT[d[i].time];
      if (!hit) {
        k = nyClock(d[i].time).key;
        var rows = byDay[k] || [], best = null, bd = 1e99;
        for (j = 0; j < rows.length; j++) {
          var dd = Math.abs(rows[j].time - d[i].time);
          if (dd < bd) { bd = dd; best = rows[j]; }
        }
        if (best && bd <= gap) hit = { s: best.close, si: 0, t: best.time };
      }
      if (!hit) continue;
      o.push({ time: d[i].time, a: d[i].close, s: hit.s, ai: i, si: hit.si });
    }
    return o;
  }
  function smaVals(pts, n) {
    var o = [], s = 0, i;
    for (i = 0; i < pts.length; i++) {
      s += pts[i].value;
      if (i >= n) s -= pts[i - n].value;
      if (i >= n - 1) o.push({ time: pts[i].time, value: s / n });
    }
    return o;
  }
  function xsBars(j, n) {
    if (!j || j.length <= n) return null;
    var a = j[j.length - 1], b = j[j.length - 1 - n];
    if (!b.a || !b.s || !a.s) return null;
    return (a.a / b.a) / (a.s / b.s) - 1;
  }
  function xsYtd(j) {
    if (!j || j.length < 2) return null;
    var yNow = nyClock(j[j.length - 1].time).y, i, lastPrev = null, firstThis = null;
    for (i = 0; i < j.length; i++) {
      var y = nyClock(j[i].time).y;
      if (y < yNow) lastPrev = j[i];
      else if (y === yNow && !firstThis) firstThis = j[i];
    }
    var b = lastPrev || firstThis, a = j[j.length - 1];
    if (!b || b === a || !b.a || !b.s || !a.s) return null;
    return (a.a / b.a) / (a.s / b.s) - 1;
  }
  function betaLast(j, n) {
    n = n || 252;
    if (j.length < n + 2) n = j.length - 2;
    if (n < 20) return null;
    var i0 = j.length - 1 - n, i, ra, rs, sa = 0, ss = 0, sas = 0, ss2 = 0;
    var ras = [], rss = [];
    for (i = i0 + 1; i < j.length; i++) {
      if (!j[i - 1].a || !j[i - 1].s) continue;
      ra = j[i].a / j[i - 1].a - 1;
      rs = j[i].s / j[i - 1].s - 1;
      ras.push(ra); rss.push(rs);
      sa += ra; ss += rs;
    }
    if (ras.length < 20) return null;
    var ma = sa / ras.length, ms = ss / ras.length;
    for (i = 0; i < ras.length; i++) {
      sas += (ras[i] - ma) * (rss[i] - ms);
      ss2 += (rss[i] - ms) * (rss[i] - ms);
    }
    return ss2 ? sas / ss2 : null;
  }
  function vsSpxPack(d, bench) {
    var j = alignExact(d, bench);
    if (j.length < 2) return { n: 0, rs: [], sma50: [], sma200: [], last: {}, note: "no overlapping S&P 500 session", from: null, to: null };
    var a0 = j[0].a, s0 = j[0].s, rs = [], i;
    for (i = 0; i < j.length; i++) rs.push({ time: j[i].time, value: 100 * (j[i].a / a0) / (j[i].s / s0) });
    var lastRs = rs[rs.length - 1].value;
    return {
      n: j.length,
      from: j[0].time,
      to: j[j.length - 1].time,
      rs: rs,
      sma50: smaVals(rs, 50),
      sma200: smaVals(rs, 200),
      last: {
        rs: lastRs,
        d1: xsBars(j, 1),
        w: xsBars(j, 5),
        m: xsBars(j, 21),
        q: xsBars(j, 63),
        hy: xsBars(j, 126),
        y: xsBars(j, 252),
        ytd: xsYtd(j),
        all: lastRs / 100 - 1,
        beta: betaLast(j, 252)
      },
      note: ""
    };
  }


  function snapYmd(d, ymd) {
    var ts = Date.parse(String(ymd || "").slice(0, 10) + "T20:00:00.000Z") / 1000;
    if (!isFinite(ts) || ts <= 0) return null;
    var best = null, bd = 1e99, i;
    for (i = 0; i < d.length; i++) {
      var dd = Math.abs(d[i].time - ts);
      if (dd < bd) { bd = dd; best = d[i]; }
    }
    if (!best || bd > 4 * 86400) return null;
    return best.time;
  }
  function daysAgoYmd(n) {
    var d = new Date(Date.now() - (n || 0) * 86400000);
    return d.toISOString().slice(0, 10);
  }
  function insiderMarks(d, pack, tkr) {
    var mk = [], want = String(tkr || "").toUpperCase();
    var rows = (pack && (pack.clusters || pack.all_ticker_buys || pack.rows)) || [];
    if (pack && pack.all_ticker_buys && pack.clusters) rows = pack.clusters.concat(pack.all_ticker_buys);
    rows.forEach(function (c) {
      if (String(c.ticker || c.symbol || "").toUpperCase() !== want) return;
      var ymd = c.last_buy || c.first_buy || c.window_end || c.date || c.filing_date;
      var t = snapYmd(d, ymd); if (!t) return;
      mk.push({ time: t, position: "belowBar", color: UP, shape: "arrowUp", text: "INS" });
    });
    return mk.slice(-12);
  }
  function buybackMarks(d, pack, tkr) {
    var mk = [], want = String(tkr || "").toUpperCase();
    var rows = (pack && (pack.top_opportunities || pack.opportunities || pack.rows)) || [];
    rows.forEach(function (o) {
      if (String(o.ticker || o.symbol || "").toUpperCase() !== want) return;
      var ymd = o.announcement_date || o.filed_date || o.date;
      if (!ymd && o.days_since_announcement != null) ymd = daysAgoYmd(+o.days_since_announcement);
      var t = snapYmd(d, ymd); if (!t) return;
      mk.push({ time: t, position: "aboveBar", color: BLUE, shape: "square", text: "BB" });
    });
    return mk.slice(-10);
  }

  function orZones(lv) {
    var pack = empty("");
    if (!lv || !lv.ready) { pack.note = "Opening range needs 5-minute bars"; return pack; }
    function box(x, lab, col) {
      if (!x) return;
      pack.zones.push({ t0: x.t0, t1: x.t1, hi: x.hi, lo: x.lo, color: col, kind: "dem", lab: lab });
    }
    box(lv.or15, "OR15", "rgba(41,98,255,.16)");
    box(lv.or30, "OR30", "rgba(41,98,255,.10)");
    box(lv.ib, "IB", "rgba(126,87,194,.10)");
    return pack;
  }

  root.jhInst = {
    fvgGaps: fvgGaps,
    fvgPack: fvgPack,
    equalHL: equalHL,
    adr20: adr20,
    adrUsedSeries: adrUsedSeries,
    dailyFrom: dailyFrom,
    linregChannel: linregChannel,
    avwapFrom: avwapFrom,
    lastEarnTime: lastEarnTime,
    sessionLevels: sessionLevels,
    sessionKey: sessionKey,
    nyClock: nyClock,
    isIntra: isIntra,
    globalSessions: globalSessions,
    separators: separators,
    ratioVs: ratioVs,
    alignExact: alignExact,
    vsSpxPack: vsSpxPack,
    insiderMarks: insiderMarks,
    buybackMarks: buybackMarks,
    orZones: orZones,
    empty: empty
  };
})(typeof window !== "undefined" ? window : globalThis);
