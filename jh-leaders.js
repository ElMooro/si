/* jh-leaders.js - the leaders desk's client (Khalid 2026-11-04: "build a new engine called leaders and track how leaders
 * on each industry are doing compared to sp500 and whether they are in accumulation or distribution. Is smart money
 * buying or selling them. Buying inflow and outflows. The industry etfs inflows and outflows, how are they compared to
 * their 250, 200, 100, 50, 20 moving average. Which are seeing golden cross and death cross. Also which are seeing
 * double tops and double bottoms. Climax, tight price. I want to be able to sort them ascending and descending.").
 *
 * Reads data/leaders.json (justhodl-leaders, daily 03:50 UTC, a reader of the warehouse). Nothing is computed from a
 * provider here and nothing is estimated: every figure is the artifact's, a missing one is a dash. The page is four
 * tables over the same rows - the board of what is printing now, the industries, the leaders, the industry funds -
 * and every column of every table sorts both ways (blanks last either way).
 *
 * The first half is pure (formatters, the comparator, the column registry, filters, CSV) and runs under node for the
 * tests; the second half paints the DOM. */
(function (w) {
  "use strict";
  if (w.JHLeaders) return;
  var KEY = "data/leaders.json", CONTRACT = "leaders.v1", RENDER_CAP = 200, REFRESH_MS = 15 * 60 * 1000;
  var MINUS = "−", DASH = "—", TIMES = "×";

  // ------------------------------------------------------------------------------------------------------- basics
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return ({ "&": "&" + "amp;", "<": "&" + "lt;", ">": "&" + "gt;", '"': "&" + "quot;", "'": "&#39;" })[c];
    });
  }
  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function at(o, path) {
    var p = path.split(".");
    for (var i = 0; i < p.length; i++) { if (o == null) return null; o = o[p[i]]; }
    return o == null ? null : o;
  }
  function fnum(v, d) {
    if (!isNum(v)) return DASH;
    var s = Math.abs(v).toFixed(d == null ? 2 : d);
    return (v < 0 && Number(s) !== 0 ? MINUS : "") + s;
  }
  function fpct(v, d) {
    if (!isNum(v)) return DASH;
    var s = Math.abs(v).toFixed(d == null ? 1 : d);
    return (Number(s) === 0 ? "" : v > 0 ? "+" : MINUS) + s + "%";
  }
  function fusd(v, signed) {
    if (!isNum(v)) return DASH;
    var a = Math.abs(v), s = a >= 1e12 ? (a / 1e12).toFixed(2) + "T" : a >= 1e9 ? (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? (a / 1e6).toFixed(a >= 1e8 ? 0 : 1) + "M" : a >= 1e3 ? (a / 1e3).toFixed(0) + "K" : a.toFixed(0);
    var zero = a < 0.5;
    return (signed && !zero ? (v > 0 ? "+" : MINUS) : (!signed && v < 0 ? MINUS : "")) + "$" + s;
  }
  function fx(v, d) { return isNum(v) ? v.toFixed(d == null ? 2 : d) + TIMES : DASH; }
  function fpx(v) { return isNum(v) ? (Math.abs(v) >= 1000 ? v.toFixed(0) : Math.abs(v) >= 1 ? v.toFixed(2) : v.toFixed(4)) : DASH; }
  function tone(v) { return !isNum(v) || Math.abs(v) < 1e-9 ? "flat" : v > 0 ? "up" : "dn"; }

  // --------------------------------------------------------------------------------------------------- comparator
  function blank(v) { return v == null || v === "" || (typeof v === "number" && !isFinite(v)); }
  /* dir -1 = largest first, +1 = smallest first. A blank is last either way: it is "no reading", not a small one. */
  function compare(a, b, dir) {
    var an = blank(a), bn = blank(b);
    if (an || bn) return an && bn ? 0 : an ? 1 : -1;
    var c;
    if (typeof a === "number" && typeof b === "number") c = a < b ? -1 : a > b ? 1 : 0;
    else c = String(a).localeCompare(String(b), "en", { numeric: true, sensitivity: "base" });
    return dir < 0 ? -c : c;
  }
  function sortRows(rows, col, dir) {
    if (!col) return rows.slice();
    return rows.map(function (r, i) { return [col.get(r), i, r]; })
      .sort(function (x, y) { return compare(x[0], y[0], dir) || x[1] - y[1]; })
      .map(function (x) { return x[2]; });
  }
  /* A header click: the same column flips; a new column starts largest-first for numbers and A-Z for text. */
  function nextSort(cur, col) {
    if (cur && cur.k === col.id) return { k: col.id, d: -cur.d };
    return { k: col.id, d: col.type === "s" ? 1 : -1 };
  }

  // ------------------------------------------------------------------------------------------------------ cells
  function td(html, cls, title) {
    if (html === DASH) cls = (cls ? cls + " " : "") + "na";       // no reading: dimmed, and last in either sort direction
    return "<td" + (cls ? ' class="' + cls + '"' : "") + (title ? ' title="' + esc(title) + '"' : "") + ">" + html + "</td>";
  }
  function tdTxt(s, cls, title) { return td(blank(s) ? DASH : esc(s), cls, title); }
  function chip(label, cls, title) { return '<span class="ld-chip ' + (cls || "") + '"' + (title ? ' title="' + esc(title) + '"' : "") + ">" + esc(label) + "</span>"; }
  function tk(t) { return '<a class="tk" href="/chart.html?s=' + encodeURIComponent(t) + '" target="_top" data-t="' + esc(t) + '">' + esc(t) + "</a>"; }
  function bar(v) {
    if (!isNum(v)) return DASH;
    return '<span class="ld-bar" title="percentile ' + v.toFixed(1) + '"><i style="left:' + Math.max(0, Math.min(100, v)).toFixed(1) + '%"></i></span>' + v.toFixed(0);
  }
  var MAS = [20, 50, 100, 200, 250];
  function ladder(ma) {
    if (!ma || ma.above == null) return DASH;
    var tip = MAS.map(function (n) { return n + "d " + fpct(ma["d" + n], 1); }).join(" · ") + " (above " + ma.above + " of " + ma.of + ")";
    return '<span class="ld-lad" title="' + esc(tip) + '">' + MAS.map(function (n) {
      var d = ma["d" + n];
      if (!isNum(d)) return '<i class="na"></i>';
      return '<i class="' + (d > 0 ? "up" : "dn") + '" style="opacity:' + (0.35 + 0.65 * Math.min(1, Math.abs(d) / 12)).toFixed(2) + '"></i>';
    }).join("") + "</span>" + ma.above + "/" + ma.of;
  }
  function spark(vals, title) {
    vals = (vals || []).filter(isNum);
    if (vals.length < 2) return DASH;
    var W = 72, H = 20, mn = Math.min.apply(null, vals), mx = Math.max.apply(null, vals), span = Math.max(mx - mn, 1e-9);
    var d = vals.map(function (v, i) { return (i ? "L" : "M") + (1 + i * (W - 2) / (vals.length - 1)).toFixed(1) + "," + (H - 2 - (v - mn) / span * (H - 4)).toFixed(1); }).join("");
    return '<svg class="ld-spark ' + (vals[vals.length - 1] >= vals[0] ? "up" : "dn") + '" viewBox="0 0 ' + W + " " + H + '" preserveAspectRatio="none" role="img" aria-label="' + esc(title || "") + '"><path d="' + d + '"/></svg>';
  }
  function sparkChange(vals) {
    vals = (vals || []).filter(isNum);
    return vals.length < 2 || !vals[0] ? null : (vals[vals.length - 1] / vals[0] - 1) * 100;
  }
  var AD_CLS = { "STRONG ACCUMULATION": "s-up", "ACCUMULATION": "up", "NEUTRAL": "mute", "DISTRIBUTION": "dn", "STRONG DISTRIBUTION": "s-dn" };
  var SMART_CLS = { "BUYING": "s-up", "LEANING BUY": "up", "QUIET": "mute", "NO READ": "mute", "LEANING SELL": "dn", "SELLING": "s-dn" };
  var STANCE_CLS = { "ACCUMULATING": "up", "DISTRIBUTING": "dn", "CONTESTED": "mute", "BALANCED": "mute" };
  var TIER_ORD = { "FORMING": 1, "LONG BASE": 2, "TIGHT": 3, "VERY TIGHT": 4 };
  var STATE_ORD = { "CONFIRMED": 3, "FORMING": 2, "FAILED": 1 };
  var SIG_SHORT = { "GOLDEN CROSS": ["Golden cross", "up"], "DEATH CROSS": ["Death cross", "dn"], "GOLDEN NEAR": ["Golden near", "up"], "DEATH NEAR": ["Death near", "dn"],
    "DOUBLE TOP FORMING": ["2× top forming", "dn"], "DOUBLE TOP CONFIRMED": ["2× top", "s-dn"], "DOUBLE BOTTOM FORMING": ["2× bottom forming", "up"], "DOUBLE BOTTOM CONFIRMED": ["2× bottom", "s-up"],
    "BUYING CLIMAX": ["Buying climax", "amber"], "SELLING CLIMAX": ["Selling climax", "amber"], "VERY TIGHT": ["Very tight", "amber"], "TIGHT": ["Tight", "amber"], "RS HIGH": ["RS high", "up"], "RS LOW": ["RS low", "dn"] };
  function signalChips(sig) {
    var out = (sig || []).filter(function (s) { return SIG_SHORT[s]; }).map(function (s) { return chip(SIG_SHORT[s][0], SIG_SHORT[s][1], s); });
    return out.length ? out.join("") : DASH;
  }
  function patternCell(p) {
    if (!p) return td(DASH);
    var cls = p.state === "FAILED" ? "mute" : p.kind === "DOUBLE TOP" ? (p.state === "CONFIRMED" ? "s-dn" : "dn") : (p.state === "CONFIRMED" ? "s-up" : "up");
    var when = p["break"] ? "broke " + p["break"].date + " (" + p["break"].ago + " sessions ago)" : "second pivot " + p.p2.date + " (" + p.ago + " sessions ago" + (p.p2.provisional ? ", still forming" : "") + ")";
    var tip = p.kind + " " + p.state + ": " + p.p1.date + " @ " + fpx(p.p1.px) + " and " + p.p2.date + " @ " + fpx(p.p2.px) + ", neckline " + fpx(p.neck.px) + " (" + p.neck.date + "); " + when +
      "; target " + fpx(p.target) + (p.target_hit ? " - already printed" : "") + (p.state === "FAILED" ? "; the neckline was reclaimed by " + fpct(p.reclaim_pct, 1) : "");
    return td(chip(p.state + (p.target_hit ? " · target met" : ""), cls, tip) + '<span class="dim"> ' + (p["break"] ? p["break"].ago : p.ago) + "d</span>");
  }
  function climaxCell(c) {
    if (!c) return td(DASH);
    var tip = c.type + " climax " + c.date + ": volume " + fx(c.vol_x, 1) + " the 250-session average, range " + fnum(c.range_atr, 1) + " ATR, close at " + fnum(c.close_pos, 0) + "% of the bar, session " + fpct(c.day_pct, 1) +
      " (gap " + fpct(c.gap_pct, 1) + ") after " + fpct(c.move_pct, 0) + "; " + c.absorption + "; since " + fpct(c.since_pct, 1) + "; extreme " + fpx(c.extreme) + (c.held ? " has held" : " was taken out");
    return td(chip(c.type + " · " + c.ago + "d", "amber", tip) + (c.held ? "" : '<span class="dim"> broken</span>'));
  }
  function tightCell(t) {
    if (!t || !t.tier) return td(DASH);
    if (t.flag) return td(chip(t.flag + " · " + t.base_len, "mute", t.flag_note || ""));
    return td(chip(t.tier + " · " + t.base_len, t.tier === "FORMING" || t.tier === "LONG BASE" ? "mute" : "amber", "closes inside " + fnum(t.base_range_pct, 1) + "% for " + t.base_len + " sessions; score " + fnum(t.score, 1)));
  }
  function crossCell(c) {
    if (!c) return td(DASH);
    if (c.type && c.fresh) return td(chip(c.type + " · " + c.ago + "d", c.type === "GOLDEN" ? "s-up" : "s-dn", (c.type === "GOLDEN" ? "50-session average crossed above the 200 on " : "50-session average crossed below the 200 on ") + c.date + " at " + fpx(c.px_at) + "; " + fpct(c.since_pct, 1) + " since" + (c.n_120 >= 2 ? "; " + c.n_120 + " crosses in 120 sessions (whipsaw)" : "")));
    if (c.near) return td(chip(c.near === "GOLDEN NEAR" ? "golden near" : "death near", c.near === "GOLDEN NEAR" ? "up" : "dn", "gap " + fpct(c.gap_pct, 2) + ", closing; about " + fnum(c.eta, 1) + " sessions at this pace"));
    return td('<span class="dim">' + esc(c.regime || DASH) + (c.type ? " · " + c.ago + "d" : "") + "</span>", "", c.type ? "last cross: " + c.type + " " + c.date : "no cross in the last 260 sessions");
  }
  function flowCell(w) {
    if (!w || !isNum(w.usd)) return td(DASH);
    return td(fusd(w.usd, true) + (isNum(w.pct_aum) ? ' <span class="dim">' + fpct(w.pct_aum, 2) + "</span>" : ""), tone(w.usd), w.end ? "reporting window ending " + w.end : "");
  }
  function coversText(r) {
    var all = r.industries || [], roles = r.roles || {}, own = all.filter(function (i) { return roles[i] !== "sector"; }), rest = all.length - own.length;
    if (!all.length) return "";
    if (!own.length) return "sector fund for " + rest + (rest === 1 ? " industry" : " industries");
    return own.slice(0, 2).join(", ") + (own.length > 2 ? " +" + (own.length - 2) : "") + (rest ? " · sector fund for " + rest + " more" : "");
  }
  function historyNote(h) {
    if (!h) return "";
    var out = [];
    if (h.cut) out.push(h.cut.reason === "gap" ? "history starts " + h.cut.date + ": the ticker printed nothing for " + h.cut.gap_days + " days before (earlier bars are another security's)" :
      h.cut.reason === "listing" ? "history starts at the listing (" + h.cut.ipo_date + ")" : "history starts " + h.cut.date + ": a separation (" + h.cut.listing + " listed that session)");
    (h.stitched || []).forEach(function (s) { out.push("bars before " + s.on + " are " + s.from + "'s - the same security under its previous symbol (" + s.source + ")"); });
    (h.jumps || []).forEach(function (j) { out.push("one-session move " + TIMES + j.ratio + " on " + j.date); });
    return out.join("; ");
  }

  // ---------------------------------------------------------------------------------------------- column registry
  function col(id, t, tip, type, get, cell) { return { id: id, t: t, tip: tip, type: type || "n", get: function (r) { var v = get(r); return v === undefined ? null : v; }, cell: cell }; }
  function pctCol(id, t, tip, path, d) { return col(id, t, tip, "n", function (r) { return at(r, path); }, function (r) { var v = at(r, path); return td(fpct(v, d), tone(v)); }); }
  function numCol(id, t, tip, path, d, suffix, toned) { return col(id, t, tip, "n", function (r) { return at(r, path); }, function (r) { var v = at(r, path); return td(isNum(v) ? fnum(v, d) + (suffix || "") : DASH, toned ? tone(v) : ""); }); }
  function usdCol(id, t, tip, path, signed) { return col(id, t, tip, "n", function (r) { return at(r, path); }, function (r) { var v = at(r, path); return td(fusd(v, signed), signed ? tone(v) : ""); }); }
  function txtCol(id, t, tip, path) { return col(id, t, tip, "s", function (r) { return at(r, path); }, function (r) { return tdTxt(at(r, path)); }); }
  function intCol(id, t, tip, path) { return numCol(id, t, tip, path, 0); }

  var COLS = {};
  [
    col("ticker", "Ticker", "opens the chart", "s", function (r) { return r.ticker; }, function (r) { return td(tk(r.ticker)); }),
    col("name", "Name", "", "s", function (r) { return r.name || r.ticker; }, function (r) {
      var tags = (r.adr ? chip("ADR" + (r.country ? " " + r.country : ""), "mute", "a foreign listing; rank counts it, the ex-ADR rank does not") : "") + (r.small ? chip("small", "mute", "the largest company of an industry with none above $2B") : "") +
        (r.thin ? chip("thin", "mute", "a lightly traded fund") : "") + (r.stale ? chip("stale", "dn", "no bar on the bank's last session") : "") + (r.history && (r.history.cut || r.history.stitched) ? chip("hist", "mute", historyNote(r.history)) : "");
      return td('<span class="nm">' + esc(r.name || (r.kind === "fund" ? (r.industries || []).join(", ") : "")) + "</span>" + tags, "ld-name", (r.name || "") + (r.classes && r.classes.length ? " (also " + r.classes.join(", ") + ")" : ""));
    }),
    col("industry", "Industry", "the FMP industry and its rank by strength among industries", "s", function (r) { return r.industry; }, function (r) {
      return td('<a href="#leaders" class="ld-ind" data-ind="' + esc(r.industry) + '">' + esc(r.industry) + "</a>" + (r.industry_rank != null ? ' <span class="dim">#' + r.industry_rank + "</span>" : ""), "ld-indc");
    }),
    txtCol("sector", "Sector", "", "sector"),
    txtCol("country", "Country", "", "country"),
    col("rank", "Rank", "by market cap inside the industry (ADRs counted)", "n", function (r) { return r.rank; }, function (r) { return td(r.rank == null ? DASH : "#" + r.rank); }),
    col("rank_us", "Ex-ADR", "rank among the industry's US-domiciled listings", "n", function (r) { return r.rank_us; }, function (r) { return td(r.rank_us == null ? DASH : "#" + r.rank_us); }),
    usdCol("mcap", "Mkt cap", "market capitalization (factor book)", "mcap", false),
    numCol("share", "Ind. share", "share of the industry's total market cap", "mcap_share_pct", 1, "%"),
    col("px", "Last", "last close in the Polygon EOD bank", "n", function (r) { return r.px; }, function (r) { return td(fpx(r.px)); }),
    col("strength", "Strength", "percentile (0-100) of the weighted excess-return z-score: 1D 10 / 1W 20 / 1M 30 / 3M 25 / 6M 15", "n", function (r) { return r.strength; }, function (r) { return td(bar(r.strength), "ld-barc"); }),
    pctCol("composite", "Composite", "weighted excess return vs SPY, percentage points", "composite_pp", 1),
    pctCol("vs_d", "vs SPY 1D", "excess return vs SPY, 1 session", "vs.d", 1), pctCol("vs_w", "vs SPY 1W", "excess return vs SPY, 5 sessions", "vs.w", 1), pctCol("vs_m", "vs SPY 1M", "excess return vs SPY, 21 sessions", "vs.m", 1),
    pctCol("vs_q", "vs SPY 3M", "excess return vs SPY, 63 sessions", "vs.q", 1), pctCol("vs_h", "vs SPY 6M", "excess return vs SPY, 126 sessions", "vs.h", 1), pctCol("vs_y", "vs SPY 1Y", "excess return vs SPY, 252 sessions", "vs.y", 1),
    pctCol("h_d", "1D", "own return, 1 session", "h.d", 1), pctCol("h_w", "1W", "own return, 5 sessions", "h.w", 1), pctCol("h_m", "1M", "own return, 21 sessions", "h.m", 1),
    pctCol("h_q", "3M", "own return, 63 sessions", "h.q", 1), pctCol("h_h", "6M", "own return, 126 sessions", "h.h", 1), pctCol("h_y", "1Y", "own return, 252 sessions", "h.y", 1),
    pctCol("rs_trend", "RS trend", "annualized slope of the log RS line over 63 sessions, % per year", "rs_trend_63", 0),
    numCol("rs_52w", "RS 52w pos", "where the RS line sits in its 52-week range, %", "rs_52w_pos", 0, "%"),
    pctCol("rs_vs_52w", "RS vs 52w hi", "the RS line against its 52-week high", "rs_vs_52w", 1),
    numCol("pos_52w", "52w pos", "where the last close sits in its 52-week range, %", "pos_52w", 0, "%"),
    pctCol("off_high", "Off high", "last close against the 52-week high", "off_high_pct", 1),
    numCol("beta", "Beta", "factor book", "beta", 2),
    col("ladder", "20·50·100·200·250", "the close against its 20 / 50 / 100 / 200 / 250-session averages: green above, red below; sorts by how many it is above", "n", function (r) { return at(r, "ma.above"); }, function (r) { return td(ladder(r.ma), "ld-ladc"); }),
    pctCol("d20", "vs 20d", "last close against its 20-session simple average", "ma.d20", 1), pctCol("d50", "vs 50d", "last close against its 50-session simple average", "ma.d50", 1),
    pctCol("d100", "vs 100d", "last close against its 100-session simple average", "ma.d100", 1), pctCol("d200", "vs 200d", "last close against its 200-session simple average", "ma.d200", 1),
    pctCol("d250", "vs 250d", "last close against its 250-session simple average", "ma.d250", 1),
    col("above", "Above", "how many of the five averages the close is above", "n", function (r) { return at(r, "ma.above"); }, function (r) { var m = r.ma; return td(m && m.above != null ? m.above + "/" + m.of : DASH, m && m.above != null ? (m.above >= 4 ? "up" : m.above <= 1 ? "dn" : "") : ""); }),
    col("stack", "Stack", "BULL: close > 20 > 50 > 100 > 200 > 250; BEAR: the reverse; else MIXED (how many of the five links are in bull order)", "s", function (r) { return at(r, "ma.stack"); }, function (r) {
      var m = r.ma || {}; return td(m.stack ? chip(m.stack, m.stack === "BULL" ? "s-up" : m.stack === "BEAR" ? "s-dn" : "mute") + (m.stack === "MIXED" && m.stack_n != null ? '<span class="dim"> ' + m.stack_n + "/5</span>" : "") : DASH); }),
    pctCol("s50_slope", "50d slope", "change of the 50-session average over the last 10 sessions", "ma.s50_slope", 2), pctCol("s200_slope", "200d slope", "change of the 200-session average over the last 20 sessions", "ma.s200_slope", 2),
    col("regime", "50 / 200", "which side the 50-session average is on", "s", function (r) { return at(r, "cross.regime"); }, function (r) { var v = at(r, "cross.regime"); return td(v ? esc(v) : DASH, v === "50>200" ? "up" : v === "50<200" ? "dn" : ""); }),
    pctCol("gap", "50/200 gap", "the 50-session average against the 200, %", "cross.gap_pct", 2),
    col("cross", "Cross", "the last 50 / 200 cross: highlighted when inside 20 sessions; sorts by sessions since it", "n", function (r) { return at(r, "cross.ago"); }, function (r) { return crossCell(r.cross); }),
    col("cross_type", "Type", "", "s", function (r) { return at(r, "cross.type"); }, function (r) { var v = at(r, "cross.type"); return td(v ? chip(v, v === "GOLDEN" ? "up" : "dn") : DASH); }),
    txtCol("cross_date", "Crossed", "session of the last 50 / 200 cross", "cross.date"),
    intCol("cross_ago", "Sessions ago", "", "cross.ago"),
    col("cross_px", "Px then", "close on the cross session", "n", function (r) { return at(r, "cross.px_at"); }, function (r) { return td(fpx(at(r, "cross.px_at"))); }),
    pctCol("cross_since", "Since", "return since the cross session", "cross.since_pct", 1),
    col("cross_near", "Near", "a cross is near when the gap is at most 1.5% and closing fast enough to meet inside 15 sessions", "s", function (r) { return at(r, "cross.near"); }, function (r) { var c = r.cross || {}; return td(c.near ? chip(c.near === "GOLDEN NEAR" ? "golden near" : "death near", c.near === "GOLDEN NEAR" ? "up" : "dn") : DASH); }),
    numCol("cross_eta", "ETA", "sessions until the averages meet at the current pace", "cross.eta", 1),
    col("s200_rising", "200d rising", "", "s", function (r) { var v = at(r, "cross.s200_rising"); return v == null ? null : v ? "yes" : "no"; }, function (r) { var v = at(r, "cross.s200_rising"); return td(v == null ? DASH : v ? "yes" : "no", v == null ? "" : v ? "up" : "dn"); }),
    col("whipsaw", "Crosses/120", "50 / 200 crosses in the last 120 sessions; two or more is a whipsaw", "n", function (r) { return at(r, "cross.n_120"); }, function (r) { var v = at(r, "cross.n_120"); return td(v == null ? DASH : String(v), v >= 2 ? "amber" : ""); }),
    col("ad", "Accum / dist", "tape accumulation / distribution, -100..+100: Chaikin money flow 21 and 63, up/down volume 50, OBV 63, accumulation minus distribution sessions 25", "n", function (r) { return at(r, "ad.score"); }, function (r) {
      var a = r.ad || {}; return td(a.label ? chip(a.label === "NEUTRAL" ? "neutral" : a.label.toLowerCase(), AD_CLS[a.label], "grade " + (a.grade || "")) + " " + fnum(a.score, 0) : DASH, "ld-adc"); }),
    numCol("ad_delta", "Δ 21", "change of the score against 21 sessions ago", "ad.delta_21", 0, "", true),
    numCol("cmf21", "CMF 21", "Chaikin money flow, 21 sessions", "ad.cmf21", 3, "", true), numCol("cmf63", "CMF 63", "Chaikin money flow, 63 sessions", "ad.cmf63", 3, "", true),
    col("ud50", "Up/Dn vol", "volume on up-closes over volume on down-closes, 50 sessions", "n", function (r) { return at(r, "ad.ud50"); }, function (r) { var v = at(r, "ad.ud50"); return td(fnum(v, 2), !isNum(v) ? "" : v >= 1.05 ? "up" : v <= 0.95 ? "dn" : ""); }),
    numCol("obv63", "OBV 63", "net on-balance volume over 63 sessions as a share of the volume traded", "ad.obv63", 3, "", true),
    intCol("acc_days", "Acc days", "sessions up at least 0.2% on volume 1.2x the 50-session average, last 25", "ad.acc_days"), intCol("dist_days", "Dist days", "sessions down at least 0.2% on such volume, last 25", "ad.dist_days"),
    usdCol("mf5", "$ net 5d", "signed dollar volume, 5 sessions: money in on sessions the typical price rose, out when it fell (a tape estimate)", "mf.w.net", true),
    usdCol("mf21", "$ net 21d", "signed dollar volume, 21 sessions (a tape estimate of pressure, not a reported flow)", "mf.m.net", true),
    usdCol("mf21_in", "$ in 21d", "dollar volume on sessions the typical price rose, 21 sessions", "mf.m.in", false), usdCol("mf21_out", "$ out 21d", "dollar volume on sessions it fell, 21 sessions", "mf.m.out", false),
    pctCol("mf21_pct", "Net % 21d", "net as a share of the dollars traded, 21 sessions", "mf.m.net_pct", 1),
    usdCol("mf63", "$ net 63d", "signed dollar volume, 63 sessions", "mf.q.net", true), pctCol("mf63_pct", "Net % 63d", "", "mf.q.net_pct", 1),
    numCol("mfi", "MFI 14", "money flow index, 14 sessions", "mf.mfi14", 0),
    col("volx", "Vol ×", "the heaviest session of the last five against the 50-session average", "n", function (r) { return at(r, "volx.x"); }, function (r) { var v = r.volx; return td(v ? fx(v.x, 1) + ' <span class="dim">' + esc(String(v.date).slice(5)) + " " + fpct(v.move_pct, 1) + "</span>" : DASH, v && v.x >= 2 ? (v.dir === "UP" ? "up" : v.dir === "DOWN" ? "dn" : "") : ""); }),
    usdCol("dv20", "$ vol 20d", "average daily dollar volume, 20 sessions (factor book)", "dv20", false),
    numCol("short_float", "Short % float", "short interest as a share of the float (factor book)", "short_pct_float", 1, "%"), numCol("dtc", "Days to cover", "", "days_to_cover", 1), numCol("svr", "Short vol ratio", "short volume over total volume", "short_volume_ratio", 2),
    col("smart", "Smart money", "the 13F cohort's stance and insiders' open-market trades; sorts by the score (-3..+3)", "n", function (r) { return at(r, "smart.score"); }, function (r) {
      var s = r.smart || {}, l = s.legs || {}; return td(s.verdict ? chip(s.verdict.toLowerCase(), SMART_CLS[s.verdict], "13F leg " + (l.f13 == null ? "none" : l.f13) + ", insider leg " + (l.insider == null ? "none" : l.insider)) : DASH); }),
    col("f13_stance", "13F stance", "the 13F cohort across two dated quarterly disclosures", "s", function (r) { return at(r, "f13.stance"); }, function (r) { var v = at(r, "f13.stance"); return td(v ? chip(v.toLowerCase(), STANCE_CLS[v]) : DASH); }),
    col("f13_holders", "Holders", "cohort managers holding now (change against the prior disclosure)", "n", function (r) { return at(r, "f13.n_holders"); }, function (r) { var f = r.f13; return td(f ? f.n_holders + ' <span class="dim">(' + (f.holder_delta > 0 ? "+" : f.holder_delta < 0 ? MINUS : "") + Math.abs(f.holder_delta || 0) + ")</span>" : DASH); }),
    intCol("f13_delta", "Δ holders", "", "f13.holder_delta"), intCol("bull", "Bull", "managers adding", "f13.bull"), intCol("bear", "Bear", "managers trimming", "f13.bear"),
    usdCol("f13_in", "13F in", "dollars added at reported prices", "f13.inflow_usd", false), usdCol("f13_out", "13F out", "dollars removed at reported prices", "f13.outflow_usd", false),
    usdCol("f13_net", "13F net", "net dollars at reported prices", "f13.net_usd", true), pctCol("f13_net_pct", "Net %", "net against the prior reported value", "f13.net_pct", 1),
    usdCol("f13_value", "13F value", "reported value now", "f13.value_usd", false), intCol("conc", "Conc. holders", "concentrated books holding", "f13.conc_n_holders"),
    intCol("ins_buys", "Ins. buys", "open-market purchases on Form 4, 90 days", "insider.buys"), usdCol("ins_buy_usd", "Ins. bought", "", "insider.buy_usd", false),
    intCol("ins_sells", "Ins. sells", "open-market sales on Form 4, 90 days", "insider.sells"), usdCol("ins_sell_usd", "Ins. sold", "", "insider.sell_usd", false),
    usdCol("ins_net", "Ins. net", "insider purchases minus sales, 90 days", "insider.net_usd", true), txtCol("ins_last", "Last Form 4", "", "insider.last"),
    txtCol("phase", "Phase", "the phase detector's Wyckoff phase (corroboration)", "desks.phase"),
    col("bottom", "Bottom desk", "the Wyckoff bottom desk's state (corroboration)", "s", function (r) { return at(r, "desks.bottom.state"); }, function (r) { var b = at(r, "desks.bottom"); return td(b ? esc(b.state || "") + ' <span class="dim">' + esc(b.grade || "") + " " + esc(b.frame || "") + "</span>" : DASH); }),
    col("dtop", "Double top", "two swing highs within 3-5%, a neckline between; sorts confirmed, forming, failed", "n", function (r) { return r.dtop ? STATE_ORD[r.dtop.state] * 1000 - (r.dtop["break"] ? r.dtop["break"].ago : r.dtop.ago) : null; }, function (r) { return patternCell(r.dtop); }),
    col("dbot", "Double bottom", "two swing lows within 3-5%, a neckline between; sorts confirmed, forming, failed", "n", function (r) { return r.dbot ? STATE_ORD[r.dbot.state] * 1000 - (r.dbot["break"] ? r.dbot["break"].ago : r.dbot.ago) : null; }, function (r) { return patternCell(r.dbot); }),
    col("climax", "Climax", "a buying or selling climax in the last 30 sessions; sorts by how recent", "n", function (r) { return r.climax ? -r.climax.ago : null; }, function (r) { return climaxCell(r.climax); }),
    col("tight", "Tight base", "the strength desk's coil tier and base length in sessions", "n", function (r) { var t = r.tight; return t && t.tier && !t.flag ? TIER_ORD[t.tier] * 1000 + (t.base_len || 0) : null; }, function (r) { return tightCell(r.tight); }),
    numCol("base_len", "Base", "sessions inside the band", "tight.base_len", 0), numCol("base_range", "Range", "close range over the base, % of the last close", "tight.base_range_pct", 1, "%"),
    numCol("wick_range", "Wick range", "high-low range over the base", "tight.wick_range_pct", 1, "%"), numCol("atr14", "ATR %", "14-session average true range, % of price", "tight.atr14_pct", 2, "%"),
    numCol("bb", "BB pctile", "Bollinger bandwidth percentile over 252 sessions (low = squeezed)", "tight.bb_width_pctile_252", 0), numCol("vol_dry", "Vol dry-up", "last 20 sessions' volume over the prior 120", "tight.vol_dry", 2, TIMES),
    numCol("cmf_base", "CMF base", "Chaikin money flow inside the base", "tight.cmf_base", 3, "", true), numCol("ud_base", "Up/Dn base", "up-volume over down-volume inside the base", "tight.updown_vol", 2),
    pctCol("rs_hold", "RS hold", "excess return vs SPY over the base", "tight.rs_hold_base", 1), numCol("pos_in_base", "In base", "where the close sits inside the base, %", "tight.pos_in_base", 0, "%"),
    numCol("tight_score", "Score", "0-100: length + tightness + squeeze + dry-up + accumulation + RS", "tight.score", 1),
    col("signals", "Printing now", "crosses, patterns, climaxes, bases and RS extremes on this row", "n", function (r) { return (r.signals || []).filter(function (s) { return SIG_SHORT[s]; }).length || null; }, function (r) { return td(signalChips(r.signals), "ld-sig"); }),
    col("spark", "60 sessions", "the last 60 closes; sorts by the change across them", "n", function (r) { return sparkChange(r.spark); }, function (r) { return td(spark(r.spark, r.ticker + " last 60 closes")); }),
    col("rs_spark", "RS line", "price over SPY, last 60 sessions, rebased to 100; sorts by its change", "n", function (r) { return sparkChange(r.rs_spark); }, function (r) { return td(spark(r.rs_spark, r.ticker + " relative to SPY")); }),
    col("fund", "Fund", "the industry's first fund", "s", function (r) { return r.etf; }, function (r) { return td(r.etf ? tk(r.etf) : DASH); }),
    intCol("sessions", "Sessions", "bars the row is computed from", "history.sessions"), txtCol("from", "From", "first session of the history", "history.from"),
    col("cut", "Cut", "where the series was shortened: a gap, the listing, a separation", "s", function (r) { return at(r, "history.cut.reason"); }, function (r) { var c = at(r, "history.cut"); return td(c ? esc(c.reason) + ' <span class="dim">' + esc(c.date || "") + "</span>" : DASH, "", historyNote(r.history)); }),
    col("stitched", "Before that", "the previous symbol's bars put back in front", "s", function (r) { var s = at(r, "history.stitched"); return s && s.length ? s[0].from : null; }, function (r) { var s = at(r, "history.stitched"); return td(s && s.length ? s.map(function (x) { return esc(x.from) + ' <span class="dim">to ' + esc(x.on) + "</span>"; }).join(", ") : DASH, "", s && s.length ? s.map(function (x) { return x.source + ", hand-over " + TIMES + x.handover_x; }).join("; ") : ""); }),
    col("jumps", "Jumps", "one-session moves beyond x1.8 / x0.56 that are real prints", "n", function (r) { var j = at(r, "history.jumps"); return j ? j.length : null; }, function (r) { var j = at(r, "history.jumps"); return td(j ? j.map(function (x) { return TIMES + x.ratio + ' <span class="dim">' + esc(x.date) + "</span>"; }).join(", ") : DASH); }),
    col("splits", "Splits rebased", "splits the bank's bars still printed, rebased here", "n", function (r) { return r.splits ? r.splits.length : null; }, function (r) { return td(r.splits ? r.splits.map(function (x) { return esc(x.date) + " " + (x.split_to && x.split_from ? esc(x.split_to) + "-for-" + esc(x.split_from) : TIMES + fnum(x.factor, 3)) + ' <span class="dim">' + esc(x.source) + "</span>"; }).join(", ") : DASH); }),
    txtCol("as_of", "Bar date", "", "as_of"),
    col("classes", "Other classes", "share classes folded into this line", "s", function (r) { return (r.classes || []).join(" ") || null; }, function (r) { return tdTxt((r.classes || []).join(", ")); }),
    // double tops / bottoms and climaxes, one figure per column (the boards)
    col("dt_p1", "First high", "", "s", function (r) { return at(r, "dtop.p1.date"); }, function (r) { var p = r.dtop; return td(p ? esc(p.p1.date) + ' <span class="dim">@ ' + fpx(p.p1.px) + "</span>" : DASH); }),
    col("dt_p2", "Second high", "", "s", function (r) { return at(r, "dtop.p2.date"); }, function (r) { var p = r.dtop; return td(p ? esc(p.p2.date) + ' <span class="dim">@ ' + fpx(p.p2.px) + (p.p2.provisional ? " …" : "") + "</span>" : DASH); }),
    col("dt_neck", "Neckline", "the swing low between the highs", "n", function (r) { return at(r, "dtop.neck.px"); }, function (r) { return td(fpx(at(r, "dtop.neck.px"))); }),
    numCol("dt_diff", "Highs apart", "difference between the two highs", "dtop.diff_pct", 1, "%"), numCol("dt_depth", "Depth", "the neckline below the lower high", "dtop.depth_pct", 1, "%"), intCol("dt_sep", "Sessions apart", "", "dtop.sep"),
    col("dt_break", "Broke", "session the close broke the neckline", "s", function (r) { return at(r, "dtop.break.date"); }, function (r) { return tdTxt(at(r, "dtop.break.date")); }),
    pctCol("dtop_neck", "To neckline", "the neckline against the last close (negative: still above it)", "dtop.to_neck_pct", 1), pctCol("dtop_target", "To target", "the measured target (neckline minus the pattern's height) against the last close", "dtop.target_pct", 1),
    numCol("dtop_vol", "Vol 2nd/1st", "volume around the second high over the first", "dtop.vol_ratio", 2, TIMES),
    col("db_p1", "First low", "", "s", function (r) { return at(r, "dbot.p1.date"); }, function (r) { var p = r.dbot; return td(p ? esc(p.p1.date) + ' <span class="dim">@ ' + fpx(p.p1.px) + "</span>" : DASH); }),
    col("db_p2", "Second low", "", "s", function (r) { return at(r, "dbot.p2.date"); }, function (r) { var p = r.dbot; return td(p ? esc(p.p2.date) + ' <span class="dim">@ ' + fpx(p.p2.px) + (p.p2.provisional ? " …" : "") + "</span>" : DASH); }),
    col("db_neck", "Neckline", "the swing high between the lows", "n", function (r) { return at(r, "dbot.neck.px"); }, function (r) { return td(fpx(at(r, "dbot.neck.px"))); }),
    numCol("db_diff", "Lows apart", "difference between the two lows", "dbot.diff_pct", 1, "%"), numCol("db_depth", "Depth", "the neckline above the higher low", "dbot.depth_pct", 1, "%"), intCol("db_sep", "Sessions apart", "", "dbot.sep"),
    col("db_break", "Broke", "session the close broke the neckline", "s", function (r) { return at(r, "dbot.break.date"); }, function (r) { return tdTxt(at(r, "dbot.break.date")); }),
    pctCol("dbot_neck", "To neckline", "the neckline against the last close (positive: still below it)", "dbot.to_neck_pct", 1), pctCol("dbot_target", "To target", "the measured target (neckline plus the pattern's height) against the last close", "dbot.target_pct", 1),
    numCol("dbot_vol", "Vol 2nd/1st", "volume around the second low over the first; under 1 is a lighter retest", "dbot.vol_ratio", 2, TIMES),
    txtCol("climax_date", "Session", "", "climax.date"), intCol("climax_ago", "Sessions ago", "", "climax.ago"),
    numCol("climax_volx", "Vol ×", "volume against the 250-session average", "climax.vol_x", 1, TIMES), numCol("climax_range", "Range/ATR", "the session's range in ATRs", "climax.range_atr", 1),
    numCol("climax_close", "Close pos", "where the close sits in the session's range (0 low, 100 high)", "climax.close_pos", 0, "%"),
    pctCol("climax_day", "Session", "the climax session's own move", "climax.day_pct", 1), pctCol("climax_gap", "Gap", "its opening gap", "climax.gap_pct", 1),
    pctCol("climax_move", "Prior move", "the advance or decline it ended", "climax.move_pct", 0), pctCol("climax_since", "Since", "return since the climax close", "climax.since_pct", 1),
    col("climax_held", "Extreme", "whether the climax high / low has stood since", "s", function (r) { var c = r.climax; return c ? (c.held ? "held" : "broken") : null; }, function (r) { var c = r.climax; return td(c ? (c.held ? "held" : "broken") : DASH, c ? (c.held ? "" : "amber") : ""); }),
    txtCol("climax_abs", "Stopping action", "", "climax.absorption"),
    // funds
    col("role", "Role", "industry fund, theme fund or the sector fund", "s", function (r) { return r.role; }, function (r) { return td(r.role ? chip(r.role, "mute") : DASH); }),
    col("covers", "Covers", "the industries this fund is mapped to: its own first, then how many it stands in for as their sector fund", "s", function (r) { return coversText(r) || null; }, function (r) {
      var t = coversText(r); return td(t ? '<span class="nm wide">' + esc(t) + "</span>" : DASH, "ld-name", (r.industries || []).map(function (i) { return i + " (" + ((r.roles || {})[i] || "") + ")"; }).join(", ")); }),
    usdCol("aum", "Assets", "reported assets on the last observation", "flow.aum_usd", false),
    col("flow1", "Flow 1 obs", "reported creations / redemptions, the fund's last observation (USD, % of assets)", "n", function (r) { return at(r, "flow.w1.usd"); }, function (r) { return flowCell(at(r, "flow.w1")); }),
    col("flow5", "Flow 5 obs", "reported creations / redemptions, last 5 observations", "n", function (r) { return at(r, "flow.w5.usd"); }, function (r) { return flowCell(at(r, "flow.w5")); }),
    pctCol("flow5_pct", "5 obs % assets", "", "flow.w5.pct_aum", 2),
    col("flow21", "Flow 21 obs", "reported creations / redemptions, last 21 observations", "n", function (r) { return at(r, "flow.w21.usd"); }, function (r) { return flowCell(at(r, "flow.w21")); }),
    pctCol("flow21_pct", "21 obs % assets", "", "flow.w21.pct_aum", 2),
    col("flow63", "Flow 63 obs", "reported creations / redemptions, last 63 observations - always from the per-fund history the Polygon bank keeps (the desks publish 1 / 5 / 21)", "n", function (r) { return at(r, "flow.w63.usd"); }, function (r) { return flowCell(at(r, "flow.w63")); }),
    col("flow_src", "Source", "where the flow was read: the ETF desk, the canonical 300-fund packet or the Polygon bank's per-fund history", "s", function (r) { var f = r.flow; return f ? (f.current ? f.source : "not current") : null; }, function (r) {
      var f = r.flow; return td(f ? (f.current ? esc(f.source) : chip("not current", "mute", "last observation " + (f.effective || "?"))) + (f.effective ? ' <span class="dim">' + esc(f.effective) + "</span>" : "") : '<span class="dim">no flow file yet</span>'); })
  ].forEach(function (c) { COLS[c.id] = c; });

  // industries: their own column set over the industry rows
  var ICOLS = {};
  [
    col("rank", "#", "rank by strength among industries", "n", function (r) { return r.rank; }, function (r) { return td(r.rank == null ? DASH : String(r.rank)); }),
    col("industry", "Industry", "shows this industry's leaders below", "s", function (r) { return r.industry; }, function (r) { return td('<a href="#leaders" class="ld-ind" data-ind="' + esc(r.industry) + '">' + esc(r.industry) + "</a>" + (r.small ? chip("small", "mute", "no company above $2B; the largest above $300M stands in") : ""), "ld-indc"); }),
    txtCol("sector", "Sector", "", "sector"),
    col("leaders", "Leaders", "the five largest by market cap; sorts by how many", "n", function (r) { return r.n_leaders; }, function (r) { return td((r.leaders || []).map(tk).join(" "), "ld-tks"); }),
    col("strength", "Strength", "percentile among industries of the cap-weighted excess-return composite", "n", function (r) { return r.strength; }, function (r) { return td(bar(r.strength), "ld-barc"); }),
    pctCol("vs_w", "vs SPY 1W", "cap-weighted excess return of the leaders, 5 sessions", "vs.w", 1), pctCol("vs_m", "vs SPY 1M", "21 sessions", "vs.m", 1), pctCol("vs_q", "vs SPY 3M", "63 sessions", "vs.q", 1),
    pctCol("vs_h", "vs SPY 6M", "126 sessions", "vs.h", 1), pctCol("vs_y", "vs SPY 1Y", "252 sessions", "vs.y", 1), pctCol("ew_m", "EW 1M", "equal-weighted excess return, 21 sessions", "ew_vs.m", 1),
    col("above50", "> 50d", "leaders above their 50-session average", "n", function (r) { return at(r, "breadth.pct_above_50"); }, function (r) { var b = r.breadth || {}; return td(b.n ? b.above_50 + "/" + b.n : DASH, b.n ? (b.pct_above_50 >= 60 ? "up" : b.pct_above_50 <= 40 ? "dn" : "") : ""); }),
    col("above200", "> 200d", "leaders above their 200-session average", "n", function (r) { return at(r, "breadth.pct_above_200"); }, function (r) { var b = r.breadth || {}; return td(b.n ? b.above_200 + "/" + b.n : DASH, b.n ? (b.pct_above_200 >= 60 ? "up" : b.pct_above_200 <= 40 ? "dn" : "") : ""); }),
    col("ad", "Accum / dist", "average tape score of the leaders (how many accumulating : distributing)", "n", function (r) { return at(r, "ad.score"); }, function (r) {
      var a = r.ad || {}; return td(a.label ? chip(a.label === "NEUTRAL" ? "neutral" : a.label.toLowerCase(), AD_CLS[a.label]) + " " + fnum(a.score, 0) + ' <span class="dim">' + a.acc + ":" + a.dist + "</span>" : DASH, "ld-adc"); }),
    usdCol("mf21", "$ net 21d", "the leaders' signed dollar volume, 21 sessions (tape estimate)", "mf21_usd", true), usdCol("mf63", "$ net 63d", "63 sessions", "mf63_usd", true),
    col("smart", "Smart money", "leaders the cohort and insiders are buying : selling (leaning included)", "n", function (r) { var s = r.smart || {}; return s.buying == null ? null : s.buying - s.selling; }, function (r) {
      var s = r.smart || {}; return td(s.buying == null ? DASH : '<span class="up">' + s.buying + '</span> : <span class="dn">' + s.selling + "</span>", "", "buying : selling among the leaders"); }),
    usdCol("f13_leaders", "13F net, leaders", "the cohort's net dollars in the leaders at reported prices", "smart.f13_net_usd", true),
    usdCol("f13_ind", "13F net, industry", "the 13F book's industry layer: every cohort holding in the industry", "f13.net_usd", true),
    col("f13_money", "13F stance", "the book's money stance for the industry", "s", function (r) { return at(r, "f13.money_stance"); }, function (r) { var v = at(r, "f13.money_stance"); return td(v ? chip(v.toLowerCase(), STANCE_CLS[v]) : DASH); }),
    usdCol("ins_net", "Insiders net", "the leaders' insider purchases minus sales, 90 days", "smart.insider_net_usd", true),
    col("flow5", "Fund flow 5 obs", "reported creations / redemptions of the industry's funds, 5 observations (USD, % of assets)", "n", function (r) { return at(r, "etf_flow.w5.usd"); }, function (r) { return flowCell(at(r, "etf_flow.w5")); }),
    col("flow21", "Fund flow 21 obs", "21 observations", "n", function (r) { return at(r, "etf_flow.w21.usd"); }, function (r) { return flowCell(at(r, "etf_flow.w21")); }),
    pctCol("flow21_pct", "21 obs % assets", "", "etf_flow.w21.pct_aum", 2),
    col("funds", "Funds", "the funds the flow is read from; a sector fund stands in when the industry has none of its own", "s", function (r) { return (at(r, "etf_flow.funds") || []).join(" ") || null; }, function (r) {
      var e = r.etf_flow || {}, f = e.funds || []; return td(f.length ? f.map(tk).join(" ") + (e.proxy ? chip("sector proxy", "mute", "no fund of its own with a current flow") : "") : '<span class="dim">' + esc((r.etfs || []).join(" ")) + " · no current flow</span>", "ld-tks"); }),
    col("signals", "Printing now", "how many leaders print each signal", "n", function (r) { var s = r.signals || {}, n = 0; for (var k in s) n += s[k]; return n || null; }, function (r) {
      var s = r.signals || {}, short = { "GOLDEN CROSS": ["golden", "up"], "DEATH CROSS": ["death", "dn"], "DOUBLE TOP": ["2× top", "dn"], "DOUBLE BOTTOM": ["2× bottom", "up"], "BUYING CLIMAX": ["buy climax", "amber"], "SELLING CLIMAX": ["sell climax", "amber"], "VERY TIGHT": ["very tight", "amber"], "TIGHT": ["tight", "amber"] };
      var out = Object.keys(short).filter(function (k) { return s[k]; }).map(function (k) { return chip(s[k] + " " + short[k][0], short[k][1]); });
      return td(out.join("") || DASH, "ld-sig"); }),
    usdCol("mcap", "Industry cap", "market cap of every common stock in the industry", "mcap_usd", false), intCol("members", "Members", "common stocks in the industry", "n_members")
  ].forEach(function (c) { ICOLS[c.id] = c; });

  // what a CSV carries where the sort value is not the reading (a chip, a list)
  COLS.cross.csv = function (r) { var c = r.cross; return c ? (c.type ? c.type + " " + c.date : c.regime) + (c.near ? " / " + c.near : "") : null; };
  COLS.ad.csv = function (r) { return at(r, "ad.label"); };
  COLS.smart.csv = function (r) { return at(r, "smart.verdict"); };
  COLS.dtop.csv = function (r) { return r.dtop ? r.dtop.state + (r.dtop.target_hit ? " (target met)" : "") : null; };
  COLS.dbot.csv = function (r) { return r.dbot ? r.dbot.state + (r.dbot.target_hit ? " (target met)" : "") : null; };
  COLS.climax.csv = function (r) { return r.climax ? r.climax.type + " " + r.climax.date : null; };
  COLS.tight.csv = function (r) { var t = r.tight; return t && t.tier ? (t.flag || t.tier) + " " + t.base_len : null; };
  COLS.signals.csv = function (r) { return (r.signals || []).join("; ") || null; };
  COLS.jumps.csv = function (r) { var j = at(r, "history.jumps"); return j ? j.map(function (x) { return x.date + " x" + x.ratio; }).join("; ") : null; };
  COLS.splits.csv = function (r) { return r.splits ? r.splits.map(function (x) { return x.date + " x" + x.factor; }).join("; ") : null; };
  ICOLS.leaders.csv = function (r) { return (r.leaders || []).join(" "); };
  ICOLS.ad.csv = function (r) { return at(r, "ad.label"); };
  ICOLS.smart.csv = function (r) { var x = r.smart || {}; return x.buying == null ? null : x.buying + " buying / " + x.selling + " selling"; };
  ICOLS.signals.csv = function (r) { var x = r.signals || {}; return Object.keys(x).filter(function (k) { return x[k]; }).map(function (k) { return x[k] + " " + k; }).join("; ") || null; };
  var LEADER_VIEWS = [
    ["overview", "Overview", ["ticker", "name", "industry", "strength", "vs_m", "vs_q", "ladder", "cross", "ad", "smart", "mf21", "signals", "mcap", "px", "spark"]],
    ["rs", "vs S&P 500", ["ticker", "name", "industry", "rank", "strength", "composite", "vs_d", "vs_w", "vs_m", "vs_q", "vs_h", "vs_y", "rs_trend", "rs_52w", "rs_vs_52w", "h_m", "h_y", "pos_52w", "off_high", "beta", "rs_spark"]],
    ["ma", "Moving averages", ["ticker", "name", "px", "d20", "d50", "d100", "d200", "d250", "above", "stack", "s50_slope", "s200_slope", "regime", "gap", "cross", "cross_date", "cross_since", "cross_near", "whipsaw"]],
    ["ad", "Accumulation · money flow", ["ticker", "name", "ad", "ad_delta", "cmf21", "cmf63", "ud50", "obv63", "acc_days", "dist_days", "mf5", "mf21", "mf21_in", "mf21_out", "mf21_pct", "mf63", "mfi", "volx", "dv20", "short_float", "dtc", "svr"]],
    ["smart", "Smart money", ["ticker", "name", "smart", "f13_stance", "f13_holders", "bull", "bear", "f13_in", "f13_out", "f13_net", "f13_net_pct", "f13_value", "conc", "ins_buys", "ins_buy_usd", "ins_sells", "ins_sell_usd", "ins_net", "ins_last", "phase", "bottom"]],
    ["pat", "Patterns", ["ticker", "name", "signals", "dtop", "dtop_neck", "dtop_target", "dtop_vol", "dbot", "dbot_neck", "dbot_target", "dbot_vol", "climax", "climax_volx", "climax_day", "climax_since", "tight", "base_len", "base_range", "tight_score", "bb", "vol_dry"]],
    ["bars", "Bars · identity", ["ticker", "name", "sector", "country", "rank", "rank_us", "share", "fund", "sessions", "from", "cut", "stitched", "jumps", "splits", "as_of", "classes"]]
  ];
  var ETF_VIEWS = [
    ["flows", "Flows", ["ticker", "role", "covers", "aum", "flow1", "flow5", "flow21", "flow63", "flow_src", "vs_m", "vs_q", "ladder", "ad", "signals"]],
    ["rs", "vs S&P 500", ["ticker", "role", "covers", "px", "strength", "composite", "vs_d", "vs_w", "vs_m", "vs_q", "vs_h", "vs_y", "rs_trend", "rs_52w", "pos_52w", "off_high", "spark"]],
    ["ma", "Moving averages", ["ticker", "role", "px", "d20", "d50", "d100", "d200", "d250", "above", "stack", "s50_slope", "s200_slope", "regime", "gap", "cross", "cross_date", "cross_since", "cross_near", "whipsaw"]],
    ["pat", "Accumulation · patterns", ["ticker", "role", "ad", "ad_delta", "cmf21", "ud50", "mf21", "mf21_pct", "f13_stance", "f13_net", "dtop", "dbot", "climax", "tight", "tight_score", "sessions"]]
  ];
  var IND_VIEW = ["rank", "industry", "sector", "leaders", "strength", "vs_w", "vs_m", "vs_q", "vs_h", "vs_y", "ew_m", "above50", "above200", "ad", "mf21", "mf63", "smart", "f13_leaders", "f13_ind", "f13_money", "ins_net", "flow5", "flow21", "flow21_pct", "funds", "signals", "mcap", "members"];
  var X_CROSS = ["cross_date", "cross_ago", "cross_px", "cross_since", "gap", "s200_rising", "whipsaw", "vs_q", "ad", "smart"];
  var X_NEAR = ["gap", "cross_eta", "s200_rising", "d50", "d200", "vs_q", "ad", "smart"];
  var X_CLIMAX = ["climax_date", "climax_ago", "climax_volx", "climax_range", "climax_close", "climax_day", "climax_gap", "climax_move", "climax_since", "climax_held", "climax_abs", "ad"];
  var X_SMART = ["smart", "f13_stance", "f13_holders", "bull", "bear", "f13_in", "f13_out", "f13_net", "f13_net_pct", "ins_buy_usd", "ins_sell_usd", "ad", "vs_q"];
  var X_AD = ["ad", "ad_delta", "cmf21", "cmf63", "ud50", "obv63", "acc_days", "dist_days", "mf21", "mf21_pct", "vs_m"];
  var X_STACK = ["strength", "d20", "d50", "d100", "d200", "d250", "vs_m", "vs_q", "ad"];
  var X_RS = ["strength", "rs_trend", "rs_vs_52w", "vs_m", "vs_q", "vs_h", "pos_52w", "ad"];
  /* board id, label, tone, the columns after ticker / name / industry */
  var BOARDS = [
    ["golden_cross", "Golden cross", "up", X_CROSS], ["death_cross", "Death cross", "dn", X_CROSS], ["golden_near", "Golden cross near", "up", X_NEAR], ["death_near", "Death cross near", "dn", X_NEAR],
    ["double_top", "Double top", "dn", ["dtop", "dt_p1", "dt_p2", "dt_neck", "dt_diff", "dt_depth", "dt_sep", "dt_break", "dtop_neck", "dtop_target", "dtop_vol", "ad"]],
    ["double_bottom", "Double bottom", "up", ["dbot", "db_p1", "db_p2", "db_neck", "db_diff", "db_depth", "db_sep", "db_break", "dbot_neck", "dbot_target", "dbot_vol", "ad"]],
    ["buying_climax", "Buying climax", "amber", X_CLIMAX], ["selling_climax", "Selling climax", "amber", X_CLIMAX],
    ["tight", "Tight price", "amber", ["tight", "base_len", "base_range", "wick_range", "atr14", "bb", "vol_dry", "cmf_base", "ud_base", "rs_hold", "pos_in_base", "tight_score", "ad", "smart"]],
    ["accumulation", "Strong accumulation", "up", X_AD], ["distribution", "Strong distribution", "dn", X_AD],
    ["smart_buying", "Smart money buying", "up", X_SMART], ["smart_selling", "Smart money selling", "dn", X_SMART],
    ["bull_stack", "Above all five, in order", "up", X_STACK], ["bear_stack", "Below all five, in order", "dn", X_STACK],
    ["rs_new_highs", "RS line at a 52-week high", "up", X_RS], ["rs_new_lows", "RS line at a 52-week low", "dn", X_RS]
  ];
  /* Cells that hold words, chips or tickers read from the left; figures from the right. */
  var LEFT = {};
  ["ladder", "cross", "ad", "smart", "dtop", "dbot", "climax", "tight", "signals", "leaders", "funds", "stack", "volx", "f13_holders", "flow_src", "cut", "stitched", "jumps", "splits", "bottom"].forEach(function (id) { LEFT[id] = 1; });
  function isLeft(c) { return c.type === "s" || LEFT[c.id] === 1; }
  function cellOf(c, r) {
    var h = c.cell(r);
    if (!isLeft(c)) return h;
    return h.indexOf('<td class="') === 0 ? '<td class="l ' + h.slice(11) : '<td class="l"' + h.slice(3);
  }
  /* the sort a view opens with: the column the view is about, strongest reading first */
  var VIEW_SORT = { overview: ["strength", -1], rs: ["strength", -1], ma: ["d50", -1], ad: ["ad", -1], smart: ["smart", -1], pat: ["signals", -1], bars: ["sessions", 1] };
  var ETF_VIEW_SORT = { flows: ["flow21", -1], rs: ["strength", -1], ma: ["d50", -1], pat: ["ad", -1] };
  function viewSort(map, id) { var v = map[id]; return v ? { k: v[0], d: v[1] } : null; }
  function viewCols(views, id) { for (var i = 0; i < views.length; i++) if (views[i][0] === id) return views[i][2]; return views[0][2]; }
  function boardDef(id) { for (var i = 0; i < BOARDS.length; i++) if (BOARDS[i][0] === id) return BOARDS[i]; return BOARDS[0]; }
  function boardCols(id, scope) { return (scope === "etfs" ? ["ticker", "role", "covers"] : ["ticker", "name", "industry"]).concat(boardDef(id)[3].filter(function (c) { return scope !== "etfs" || (c !== "smart" && c.indexOf("ins_") !== 0); })); }
  function defs(reg, ids) { return ids.map(function (id) { return reg[id]; }).filter(Boolean); }

  // ----------------------------------------------------------------------------------------------------- filters
  var RANKS = [["all", "All"], ["1", "#1 only"], ["3", "Top 3"], ["5", "Top 5"], ["us", "Top 5 ex-ADR"]];
  function leaderFilter(st) {
    var q = String(st.q || "").trim().toLowerCase(), sig = st.sig || [];
    return function (r) {
      if (st.sector && r.sector !== st.sector) return false;
      if (st.industry && r.industry !== st.industry) return false;
      if (st.rank === "1" && r.rank !== 1) return false;
      if (st.rank === "3" && !(r.rank <= 3)) return false;
      if (st.rank === "5" && !(r.rank <= 5)) return false;
      if (st.rank === "us" && !(r.rank_us != null && r.rank_us <= 5)) return false;
      if (q && String(r.ticker).toLowerCase().indexOf(q) < 0 && String(r.name || "").toLowerCase().indexOf(q) < 0 && String(r.industry || "").toLowerCase().indexOf(q) < 0) return false;
      for (var i = 0; i < sig.length; i++) if ((r.signals || []).indexOf(sig[i]) < 0) return false;
      return true;
    };
  }
  function industryFilter(st) {
    var q = String(st.q || "").trim().toLowerCase(), min = Number(st.min) || 0;
    return function (r) {
      if (st.sector && r.sector !== st.sector) return false;
      if (min && !(r.n_leaders >= min)) return false;
      if (q && String(r.industry).toLowerCase().indexOf(q) < 0 && (r.leaders || []).join(" ").toLowerCase().indexOf(q) < 0) return false;
      return true;
    };
  }
  function etfFilter(st) {
    var q = String(st.q || "").trim().toLowerCase();
    return function (r) {
      if (st.role && st.role !== "all" && r.role !== st.role) return false;
      if (st.current && !(r.flow && r.flow.current)) return false;
      if (q && String(r.ticker).toLowerCase().indexOf(q) < 0 && (r.industries || []).join(" ").toLowerCase().indexOf(q) < 0) return false;
      return true;
    };
  }
  function boardRows(doc, id, scope) {
    var list = at(doc, "boards." + (scope === "etfs" ? "etfs" : "leaders") + "." + id) || [], by = {};
    (scope === "etfs" ? doc.etfs : doc.rows).forEach(function (r) { by[r.ticker] = r; });
    return list.map(function (t) { return by[t]; }).filter(Boolean);
  }
  function csvOf(cols, rows) {
    function q(v) { v = v == null ? "" : String(v); return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; }
    return [cols.map(function (c) { return q(c.t); }).join(",")].concat(rows.map(function (r) { return cols.map(function (c) { var v = c.csv ? c.csv(r) : c.get(r); return q(isNum(v) ? String(v) : v); }).join(","); })).join("\n");
  }
  /* The verdict in one sentence, from the summary's own numbers. */
  function verdictLine(doc) {
    var s = doc.summary || {}, b = doc.benchmark || {}, h = b.h || {};
    if (!s.n) return "";
    return fnum(s.pct_beating_m, 0) + "% of " + s.n + " leaders beat SPY over 21 sessions (SPY " + fpct(h.m, 1) + "); " + fnum(s.pct_above_50, 0) + "% sit above their 50-session average and " + fnum(s.pct_above_200, 0) + "% above the 200. " +
      "The tape reads accumulation in " + s.accumulation + " and distribution in " + s.distribution + "; the 13F cohort and insiders are buying " + s.smart_buying + " and selling " + s.smart_selling + ".";
  }

  var api = { KEY: KEY, CONTRACT: CONTRACT, COLS: COLS, ICOLS: ICOLS, LEADER_VIEWS: LEADER_VIEWS, ETF_VIEWS: ETF_VIEWS, IND_VIEW: IND_VIEW, BOARDS: BOARDS, RANKS: RANKS, SIG_SHORT: SIG_SHORT,
    esc: esc, at: at, fnum: fnum, fpct: fpct, fusd: fusd, fx: fx, fpx: fpx, tone: tone, compare: compare, sortRows: sortRows, nextSort: nextSort, viewCols: viewCols, boardCols: boardCols, boardDef: boardDef, defs: defs,
    VIEW_SORT: VIEW_SORT, ETF_VIEW_SORT: ETF_VIEW_SORT, viewSort: viewSort, isLeft: isLeft, cellOf: cellOf, leaderFilter: leaderFilter, industryFilter: industryFilter, etfFilter: etfFilter, boardRows: boardRows, csvOf: csvOf, verdictLine: verdictLine, coversText: coversText, ladder: ladder, historyNote: historyNote, signalChips: signalChips };
  w.JHLeaders = api;
  if (!w.document || !w.document.getElementById) return;

  // ================================================================================================== the page
  var doc = null, D = w.JHDesk, $ = function (id) { return w.document.getElementById(id); };
  var S = { board: null, scope: "leaders", view: "overview", sector: "", industry: "", rank: "all", q: "", sig: [], all: false,
            isector: "", imin: 0, iq: "", eview: "flows", erole: "all", ecurrent: false, eq: "",
            sort: { board: null, ind: { k: "rank", d: 1 }, lead: { k: "strength", d: -1 }, etf: { k: "flow21", d: -1 } } };
  var HASH_KEYS = ["board", "scope", "view", "sector", "industry", "rank", "q", "isector", "imin", "iq", "eview", "erole", "eq"];

  function readHash() {
    var h = String(w.location.hash || "").replace(/^#/, "");
    if (!h || h.indexOf("=") < 0) return;
    h.split("&").forEach(function (kv) {
      var i = kv.indexOf("="), k = kv.slice(0, i), v = "";
      try { v = decodeURIComponent(kv.slice(i + 1)); } catch (e) { return; }
      if (HASH_KEYS.indexOf(k) >= 0) S[k] = k === "imin" ? Number(v) || 0 : v;
      else if (k === "sig") S.sig = v ? v.split("|") : [];
      else if (/^s_(board|ind|lead|etf)$/.test(k)) { var p = v.split(":"); if (p[0]) S.sort[k.slice(2)] = { k: p[0], d: p[1] === "asc" ? 1 : -1 }; }
    });
  }
  function writeHash() {
    var out = [];
    HASH_KEYS.forEach(function (k) { var v = S[k]; if (v && !(k === "scope" && v === "leaders") && !(k === "view" && v === "overview") && !(k === "rank" && v === "all") && !(k === "eview" && v === "flows") && !(k === "erole" && v === "all")) out.push(k + "=" + encodeURIComponent(v)); });
    if (S.sig.length) out.push("sig=" + encodeURIComponent(S.sig.join("|")));
    ["board", "ind", "lead", "etf"].forEach(function (t) { var s = S.sort[t]; if (s) out.push("s_" + t + "=" + encodeURIComponent(s.k + ":" + (s.d > 0 ? "asc" : "desc"))); });
    try { w.history.replaceState(null, "", out.length ? "#" + out.join("&") : w.location.pathname + w.location.search); } catch (e) {}
  }

  function head(table, cols) {
    var s = S.sort[table];
    return "<thead><tr>" + cols.map(function (c) {
      var on = s && s.k === c.id;
      return '<th scope="col" tabindex="0" data-sort="' + c.id + '"' + (on ? ' aria-sort="' + (s.d < 0 ? "descending" : "ascending") + '"' : "") + (isLeft(c) ? ' class="txt"' : "") +
        ' title="' + esc((c.tip ? c.tip + " — " : "") + "click to sort, click again to reverse") + '">' + esc(c.t) + "</th>";
    }).join("") + "</tr></thead>";
  }
  function paint(table, hostId, cols, rows, empty, cap, cls) {
    var s = S.sort[table], c = null;
    if (s) cols.forEach(function (x) { if (x.id === s.k) c = x; });
    var sorted = c ? sortRows(rows, c, s.d) : rows, shown = cap && sorted.length > cap ? sorted.slice(0, cap) : sorted;
    $(hostId).innerHTML = '<table class="jh-tbl ld-tbl' + (cls ? " " + cls : "") + '">' + head(table, cols) + "<tbody>" +
      (shown.map(function (r) { return "<tr>" + cols.map(function (x) { return cellOf(x, r); }).join("") + "</tr>"; }).join("") || '<tr><td class="jh-load" colspan="' + cols.length + '">' + esc(empty) + "</td></tr>") + "</tbody></table>";
    var ths = $(hostId).querySelectorAll("th[data-sort]");
    Array.prototype.forEach.call(ths, function (th) {
      function go() {
        var id = th.getAttribute("data-sort"), cdef = null;
        cols.forEach(function (x) { if (x.id === id) cdef = x; });
        if (!cdef) return;
        S.sort[table] = nextSort(S.sort[table], cdef);
        draw(table);
      }
      th.onclick = go;
      th.onkeydown = function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(); } };
    });
    return { n: sorted.length, shown: shown.length, cols: cols, rows: sorted };
  }
  function chips(hostId, items, cur, on, multi) {
    $(hostId).innerHTML = items.map(function (it) {
      var active = multi ? cur.indexOf(it[0]) >= 0 : cur === it[0];
      return '<button type="button" class="jh-chip' + (active ? " on" : "") + (it[2] ? " t-" + it[2] : "") + '" aria-pressed="' + (active ? "true" : "false") + '" data-v="' + esc(it[0]) + '"' + (it[3] ? ' title="' + esc(it[3]) + '"' : "") + ">" + esc(it[1]) + "</button>";
    }).join("");
    Array.prototype.forEach.call($(hostId).querySelectorAll("[data-v]"), function (b) { b.onclick = function () { on(b.getAttribute("data-v")); }; });
  }
  function options(sel, values, cur, all) {
    sel.innerHTML = '<option value="">' + esc(all) + "</option>" + values.map(function (v) { return '<option value="' + esc(v) + '"' + (v === cur ? " selected" : "") + ">" + esc(v) + "</option>"; }).join("");
  }
  var LAST = {};
  function download(table) {
    var t = LAST[table];
    if (!t) return;
    var blob = new Blob([csvOf(t.cols, t.rows)], { type: "text/csv;charset=utf-8" }), a = w.document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "leaders-" + table + "-" + (doc.as_of || "") + ".csv";
    w.document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 2000);
  }

  // ----------------------------------------------------------------------------------------------- verdict (KPIs)
  function kpi(label, value, sub) { return '<div class="jh-kpi"><div class="l">' + label + '</div><div class="v">' + value + '</div><div class="s">' + sub + "</div></div>"; }
  function jump(id, text, cls) { return '<button type="button" class="ld-jump ' + (cls || "") + '" data-board="' + id + '">' + text + "</button>"; }
  function drawVerdict() {
    var s = doc.summary || {}, b = doc.benchmark || {}, h = b.h || {}, bl = at(doc, "boards.leaders") || {}, ad = b.ad || {}, cr = b.cross || {};
    function n(k) { return (bl[k] || []).length; }
    var dts = doc.rows.filter(function (r) { return r.dtop && r.dtop.state === "CONFIRMED"; }).length, dbs = doc.rows.filter(function (r) { return r.dbot && r.dbot.state === "CONFIRMED"; }).length;
    $("verdict-line").textContent = verdictLine(doc);
    $("verdict-meta").textContent = (s.n || 0) + " leaders ranked in " + (at(doc, "universe.n_industries") || 0) + " industries · bars as of " + (doc.as_of || DASH);
    $("kpis").innerHTML = [
      kpi("S&amp;P 500 · SPY", fpx(b.px) + ' <small class="' + tone(h.d) + '">' + fpct(h.d, 2) + "</small>", '<span class="' + tone(h.w) + '">1W ' + fpct(h.w) + '</span> · <span class="' + tone(h.m) + '">1M ' + fpct(h.m) + '</span> · <span class="' + tone(h.q) + '">3M ' + fpct(h.q) + "</span><br>" +
        ladder(b.ma) + " of its averages" + (cr.regime ? ' · <span class="dim">' + esc(cr.regime) + "</span>" : "") + "<br>" + (ad.label ? chip(ad.label.toLowerCase(), AD_CLS[ad.label], "SPY's own tape score") + '<span class="dim">' + fnum(ad.score, 0) + "</span>" : "")),
      kpi("Leaders beating SPY", fnum(s.pct_beating_m, 0) + "% <small>1M</small>", "1W " + fnum(s.pct_beating_w, 0) + "% · 3M " + fnum(s.pct_beating_q, 0) + "% · 6M " + fnum(s.pct_beating_h, 0) + "% · 1Y " + fnum(s.pct_beating_y, 0) + "%<br>" + (s.n || 0) + " leaders ranked"),
      kpi("Above their averages", fnum(s.pct_above_50, 0) + "% <small>50d</small>", "20d " + fnum(s.pct_above_20, 0) + "% · 100d " + fnum(s.pct_above_100, 0) + "% · 200d " + fnum(s.pct_above_200, 0) + "% · 250d " + fnum(s.pct_above_250, 0) + "%<br>" +
        jump("bull_stack", n("bull_stack") + " above all five in order", "up") + " · " + jump("bear_stack", n("bear_stack") + " below", "dn")),
      kpi("Accumulation : distribution", '<span class="up">' + (s.accumulation || 0) + '</span> : <span class="dn">' + (s.distribution || 0) + "</span>", jump("accumulation", (s.strong_accumulation || 0) + " strong accumulation", "up") + "<br>" + jump("distribution", (s.strong_distribution || 0) + " strong distribution", "dn") +
        '<br><span class="dim">signed $ volume 21d ' + fusd(s.mf21_net_usd, true) + "</span>"),
      kpi("Smart money", jump("smart_buying", (s.smart_buying || 0) + " buying", "up") + " : " + jump("smart_selling", (s.smart_selling || 0) + " selling", "dn"), "leaning " + (s.smart_leaning_buy || 0) + " buy · " + (s.smart_leaning_sell || 0) + " sell<br>13F period " + esc(at(doc, "f13.period") || DASH) + " · Form 4, " + (at(doc, "insiders.window_days") || 90) + " days"),
      kpi("50 / 200 crosses", jump("golden_cross", n("golden_cross") + " golden", "up") + " : " + jump("death_cross", n("death_cross") + " death", "dn"), "inside 20 sessions<br>near: " + jump("golden_near", n("golden_near") + " golden", "up") + " · " + jump("death_near", n("death_near") + " death", "dn") +
        '<br><span class="dim">50 over 200 in ' + (s.regime_up || 0) + ", under in " + (s.regime_down || 0) + "</span>"),
      kpi("Double tops : bottoms", jump("double_top", n("double_top") + " tops", "dn") + " : " + jump("double_bottom", n("double_bottom") + " bottoms", "up"), dts + " tops and " + dbs + " bottoms confirmed by a neckline break<br>the rest still forming"),
      kpi("Climaxes · tight price", jump("buying_climax", n("buying_climax") + " buying", "amber") + " · " + jump("selling_climax", n("selling_climax") + " selling", "amber"), "climaxes inside 30 sessions<br>" + jump("tight", n("tight") + " tight bases", "amber"))
    ].join("");
    Array.prototype.forEach.call($("kpis").querySelectorAll("[data-board]"), function (x) {
      x.onclick = function () { S.board = x.getAttribute("data-board"); S.scope = "leaders"; S.sort.board = null; draw("board"); $("printing").scrollIntoView({ behavior: "smooth", block: "start" }); };
    });
    var ch = doc.changes || {}, parts = [];
    function list(xs, cls) { return xs.slice(0, 24).map(function (x) { return tk(x.ticker) + " " + chip((SIG_SHORT[x.signal] || [x.signal.toLowerCase()])[0], cls); }).join(" "); }
    if (ch.prev_as_of) {
      parts.push("<b>Since " + esc(ch.prev_as_of) + ":</b> " + ((ch.new || []).length ? (ch.n_new || ch.new.length) + " new — " + list(ch.new, "amber") : "nothing new"));
      if ((ch.gone || []).length) parts.push("<b>Gone:</b> " + list(ch.gone, "mute") + ((ch.n_gone || 0) > 24 ? " …" : ""));
    } else parts.push('<span class="dim">' + esc(ch.note || "first publication: nothing to compare with yet") + "</span>");
    $("changes").innerHTML = parts.join("<br>");
  }

  // ------------------------------------------------------------------------------------------------------ boards
  function drawBoard() {
    var scope = S.scope === "etfs" ? "etfs" : "leaders", lists = at(doc, "boards." + scope) || {};
    if (!S.board || boardDef(S.board)[0] !== S.board || (scope === "etfs" && S.board.indexOf("smart_") === 0)) S.board = null;      // an unknown board, or one a fund cannot print
    if (!S.board) { for (var i = 0; i < BOARDS.length; i++) if ((lists[BOARDS[i][0]] || []).length) { S.board = BOARDS[i][0]; break; } }
    if (!S.board) S.board = BOARDS[0][0];
    chips("board-scope", [["leaders", "Leaders"], ["etfs", "Industry funds"]], scope, function (v) { S.scope = v; S.sort.board = null; draw("board"); });
    chips("board-chips", BOARDS.filter(function (b) { return scope !== "etfs" || b[0].indexOf("smart_") !== 0; }).map(function (b) { return [b[0], b[1] + " " + (lists[b[0]] || []).length, b[2]]; }), S.board, function (v) { S.board = v; S.sort.board = null; draw("board"); });
    var def = boardDef(S.board), rows = boardRows(doc, S.board, scope);
    LAST.board = paint("board", "board", defs(COLS, boardCols(S.board, scope)), rows, "Nothing is printing " + def[1].toLowerCase() + " in this scope today.");
    $("board-meta").textContent = rows.length + " " + (scope === "etfs" ? "funds" : "leaders") + " · " + def[1].toLowerCase() + " · as of " + (doc.as_of || DASH) + (S.sort.board ? "" : " · in board order");
  }

  // -------------------------------------------------------------------------------------------------- industries
  function drawIndustries() {
    var sectors = [], seen = {};
    doc.industries.forEach(function (r) { if (r.sector && !seen[r.sector]) { seen[r.sector] = 1; sectors.push(r.sector); } });
    options($("ind-sector"), sectors.sort(), S.isector, "All sectors");
    chips("ind-min", [["0", "All industries"], ["3", "3+ leaders"], ["5", "5 leaders"]], String(S.imin || 0), function (v) { S.imin = Number(v); draw("ind"); });
    var rows = doc.industries.filter(industryFilter({ sector: S.isector, min: S.imin, q: S.iq }));
    LAST.ind = paint("ind", "ind", defs(ICOLS, IND_VIEW), rows, "No industry matches these filters.", 0, "ld-pin2");
    $("ind-meta").textContent = rows.length + " of " + doc.industries.length + " industries · each row is its five largest companies";
  }

  // ----------------------------------------------------------------------------------------------------- leaders
  function drawLeaders() {
    var sectors = [], inds = [], seenS = {}, seenI = {};
    doc.rows.forEach(function (r) {
      if (r.sector && !seenS[r.sector]) { seenS[r.sector] = 1; sectors.push(r.sector); }
      if (r.industry && !seenI[r.industry] && (!S.sector || r.sector === S.sector)) { seenI[r.industry] = 1; inds.push(r.industry); }
    });
    if (S.industry && !seenI[S.industry]) S.industry = "";
    options($("lead-sector"), sectors.sort(), S.sector, "All sectors");
    options($("lead-industry"), inds.sort(), S.industry, "All industries");
    chips("lead-view", LEADER_VIEWS.map(function (v) { return [v[0], v[1]]; }), S.view, function (v) { S.view = v; S.sort.lead = viewSort(VIEW_SORT, v); draw("lead"); });
    chips("lead-rank", RANKS, S.rank, function (v) { S.rank = v; draw("lead"); });
    var counts = {};
    doc.rows.forEach(function (r) { (r.signals || []).forEach(function (s) { counts[s] = (counts[s] || 0) + 1; }); });
    chips("lead-sig", Object.keys(SIG_SHORT).filter(function (s) { return counts[s]; }).map(function (s) { return [s, SIG_SHORT[s][0] + " " + counts[s], SIG_SHORT[s][1]]; }), S.sig, function (v) {
      var i = S.sig.indexOf(v); if (i >= 0) S.sig.splice(i, 1); else S.sig.push(v); draw("lead"); }, true);
    var rows = doc.rows.filter(leaderFilter(S));
    var res = paint("lead", "lead", defs(COLS, viewCols(LEADER_VIEWS, S.view)), rows, "No leader matches these filters.", S.all ? 0 : RENDER_CAP);
    LAST.lead = res;
    $("lead-meta").textContent = res.n + " of " + doc.rows.length + " leaders" + (S.industry ? " · " + S.industry : S.sector ? " · " + S.sector : "") + " · bars as of " + (doc.as_of || DASH);
    $("lead-more").hidden = res.shown >= res.n;
    $("lead-more").textContent = "Show all " + res.n + " rows (showing the first " + res.shown + ")";
  }

  // ------------------------------------------------------------------------------------------------------- funds
  function drawFunds() {
    chips("etf-view", ETF_VIEWS.map(function (v) { return [v[0], v[1]]; }), S.eview, function (v) { S.eview = v; S.sort.etf = viewSort(ETF_VIEW_SORT, v); draw("etf"); });
    chips("etf-role", [["all", "All funds"], ["industry", "Industry"], ["theme", "Theme"], ["sector", "Sector"]], S.erole, function (v) { S.erole = v; draw("etf"); });
    var rows = doc.etfs.filter(etfFilter({ role: S.erole, current: S.ecurrent, q: S.eq }));
    LAST.etf = paint("etf", "etf", defs(COLS, viewCols(ETF_VIEWS, S.eview)), rows, "No fund matches these filters.");
    var fl = doc.flows || {};
    $("etf-meta").textContent = rows.length + " of " + doc.etfs.length + " funds · " + (fl.n_current == null ? "?" : fl.n_current) + " with a current reported flow";
  }

  // ---------------------------------------------------------------------------------- reading rules + provenance
  function drawRules() {
    var d = doc.definitions || {};
    $("rules").innerHTML = "<dl>" + Object.keys(d).map(function (k) { return "<dt>" + esc(RULE_LABEL[k] || k.replace(/_/g, " ")) + "</dt><dd>" + esc(d[k]) + "</dd>"; }).join("") + "</dl>";
    $("limits").innerHTML = (doc.limitations || []).map(function (x) { return "<li>" + esc(x) + "</li>"; }).join("");
    $("note").textContent = doc.note || "";
  }
  function tl(s, extra) { return tdTxt(s, "l" + (extra ? " " + extra : "")); }
  function drawSources() {
    var src = doc.sources || [], u = doc.universe || {}, us = u.stats || {}, fl = doc.flows || {}, f13 = doc.f13 || {}, ins = doc.insiders || {}, hs = doc.history || {}, sp = doc.splits || {}, dk = doc.desks || {}, z = doc.z_stats || {}, bank = doc.bank || {};
    var age = (Date.now() - Date.parse(doc.generated_at)) / 36e5;
    $("sources").innerHTML = '<table class="jh-tbl ld-tbl ld-src"><thead><tr><th class="txt">Engine</th><th class="txt">What it supplies</th><th class="txt">Schedule</th><th class="txt">Artifact</th><th class="txt">Last modified</th><th>Age (h)</th><th class="txt">Tier</th></tr></thead><tbody>' +
      src.map(function (s) { return "<tr>" + tl(s.engine) + tl(s.role, "wrap") + tl(s.schedule) + tl(s.key) + tl(String(s.last_modified || "").slice(0, 16).replace("T", " ")) + td(s.age_h == null ? DASH : String(s.age_h)) + td(chip(s.tier || "?", s.tier === "fresh" ? "up" : s.tier === "stale" ? "amber" : "dn"), "l") + "</tr>"; }).join("") +
      "<tr>" + tl(doc.engine) + tl("this desk: leaders, industries, funds, boards", "wrap") + tl("daily 03:50 UTC (Scheduler justhodl-leaders-daily)") + tl(KEY) + tl(String(doc.generated_at || "").slice(0, 16).replace("T", " ")) + td(isNum(age) ? age.toFixed(1) : DASH) +
      td(esc(doc.version || "") + ' <span class="dim">' + esc(doc.run_seconds) + "s · " + esc(at(doc, "trigger.kind") || "scheduled") + "</span>", "l") + "</tr></tbody></table>";
    var cut = hs.cut || {}, st = hs.stitched || {}, hist = [];
    Object.keys(cut).forEach(function (t) { var c = cut[t]; hist.push("<tr>" + td(tk(t), "l") + tl("starts later: " + c.reason) + tl(c.date) + tl(c.reason === "gap" ? "the ticker printed nothing for " + c.gap_days + " days before (last bar " + c.prev + "): the earlier bars are another security's or another stretch" : c.reason === "listing" ? "listed " + c.ipo_date + ": older bars belong to the ticker's previous holder" :
      c.listing + " printed its first bar that session, worth " + fusd(c.listing_mcap_usd) + "; " + fusd(c.vanished_usd) + " left this price (" + TIMES + c.ratio + "): a separation, not a loss", "wrap") + td(String(c.dropped), "", "bars set aside") + "</tr>"); });
    Object.keys(st).forEach(function (t) { (st[t] || []).forEach(function (x) { hist.push("<tr>" + td(tk(t), "l") + tl("joined to " + x.from) + tl(x.on) + tl("the same security under its previous symbol (" + x.source + "); the price handed over at " + TIMES + x.handover_x + "; history back to " + x.first, "wrap") + td(String(x.sessions), "", "bars available under the previous symbol") + "</tr>"); }); });
    Object.keys(hs.refused || {}).forEach(function (t) { hist.push("<tr>" + td(tk(t), "l") + tl("not joined") + tl("") + tl(hs.refused[t], "wrap") + td(DASH) + "</tr>"); });
    $("history").innerHTML = '<table class="jh-tbl ld-tbl ld-src"><thead><tr><th class="txt">Ticker</th><th class="txt">What was done</th><th class="txt">From</th><th class="txt">Why</th><th>Bars</th></tr></thead><tbody>' + (hist.join("") || '<tr><td class="jh-load" colspan="5">Every series is one continuous history.</td></tr>') + "</tbody></table>";
    function kv(k, v) { return "<b>" + k + ".</b> " + v; }
    $("ops").innerHTML = [
      kv("Universe", esc(u.n_leaders) + " leaders (" + esc(u.n_top) + " in an industry's top " + esc(u.top_n) + ", " + esc(u.n_adr) + " ADRs) across " + esc(u.n_industries) + " industries and " + esc(u.n_sectors) + " sectors, from " + esc(us.common) + " common stocks in " + esc(us.rows) +
        " factor-book rows (" + esc(us.folded) + " share classes folded, " + esc(us.no_tape) + " without a tape, " + esc(us.no_cap) + " without a market cap); a leader needs " + fusd(u.min_cap_usd) + " of market cap and " + fusd(u.min_dollar_vol_usd) + " of daily dollar volume, the floor for an industry with none is " + fusd(u.floor_cap_usd) + " / " + fusd(u.floor_dollar_vol_usd) + "." +
        ((u.industries_without_leader || []).length ? " Industries with no company above the floor: " + esc(u.industries_without_leader.join(", ")) + "." : "") + ((u.unbanked || []).length ? " No bars in the bank: " + esc(u.unbanked.join(", ")) + "." : "") + ((u.skipped_for_time || []).length ? " Skipped for time: " + esc(u.skipped_for_time.join(", ")) + "." : "")),
      kv("Bars", "Polygon EOD bank, last session " + esc(bank.last_session) + " (" + esc(bank.tickers) + " tickers, updated " + esc(String(bank.updated_at || "").slice(0, 16).replace("T", " ")) + " UTC), " + esc(bank.lookback_sessions) + " sessions read per name. " + esc(sp.n_rebased) + " series rebased for splits (" + esc(sp.recorded) + " recorded, " + esc(sp.inferred) + " inferred) from " + esc(sp.windows_read) + " monthly windows: " +
        (Object.keys(sp.tickers || {}).map(function (t) { return esc(t) + " " + sp.tickers[t].map(function (e) { return esc(e.date) + " " + (e.split_to && e.split_from ? esc(e.split_to) + "-for-" + esc(e.split_from) : TIMES + fnum(e.factor, 3)); }).join(", "); }).join("; ") || "none") + ". " + esc(hs.n_cut) + " series cut and " + esc(hs.n_stitched) + " stitched to a previous symbol; " + esc((hs.short || []).length) + " have less than a year of history" + ((hs.short || []).length ? " (" + esc(hs.short.join(", ")) + ")" : "") + "."),
      kv("Fund flows", esc(fl.n_current) + " of " + esc(fl.n_funds) + " funds have a current reported flow: " + Object.keys(fl.by_source || {}).map(function (k) { return esc(fl.by_source[k]) + " from " + esc(k); }).join(", ") + ". ETF desk generated " + esc(String(fl.desk_generated_at || "").slice(0, 16).replace("T", " ")) + " UTC, canonical packet " + esc(String(fl.canonical_generated_at || "").slice(0, 16).replace("T", " ")) + " UTC." +
        (fl.polygon_bank ? " Polygon bank histories asked " + esc(fl.polygon_bank.asked) + ": " + esc(fl.polygon_bank.this_fund) + " are the fund's own, " + esc(fl.polygon_bank.other_funds) + " hold another fund's rows and were refused, " + esc(fl.polygon_bank.absent) + " are not banked yet" + (fl.polygon_bank.w63_filled ? "; the 63-observation window of " + esc(fl.polygon_bank.w63_filled) + " desk funds comes from that history" : "") + "." : "") + ((fl.no_flow || []).length ? " No current flow: " + esc(fl.no_flow.join(" ")) + "." : "")),
      kv("13F", "period " + esc(f13.period) + ", " + esc(f13.n_cohort) + " managers in the cohort, " + esc(f13.n_leaders_held) + " leaders held, " + esc(f13.n_industries) + " industries in the book's industry layer (generated " + esc(String(f13.generated_at || "").slice(0, 16).replace("T", " ")) + " UTC)."),
      kv("Insiders", "Form 4 feed, " + esc(ins.window_days) + " days (" + esc(ins.days_read) + " filing days read, " + esc(ins.oldest_day) + " to " + esc(ins.newest_day) + "): " + esc(ins.n_leaders) + " leaders with an open-market trade, " + esc(ins.n_buying) + " with purchases, " + esc(ins.n_selling) + " with sales."),
      kv("Other desks", esc(dk.n_leaders_with_phase) + " leaders carry a phase from the phase detector (" + esc(dk.phase_n) + " tickers, " + esc(String(dk.phase_generated_at || "").slice(0, 10)) + "), " + esc(dk.n_leaders_on_bottom_desk) + " are on the bottom desk (" + esc(dk.bottom_n) + " tickers, " + esc(String(dk.bottom_generated_at || "").slice(0, 10)) + ")."),
      kv("Strength", "the composite is a z-score across the leaders; today's cross-section: " + ["d", "w", "m", "q", "h"].map(function (k) { return k.toUpperCase() + " mean " + fpct(at(z, k + ".mean"), 1) + " sd " + fnum(at(z, k + ".sd"), 1); }).join(" · ") + "."),
      kv("Symbol changes", esc(at(hs, "symbol_changes.rows")) + " rows in FMP's list, newest " + esc(at(hs, "symbol_changes.newest")) + ". " + esc(hs.note || "")),
      kv("Errors", doc.n_errors ? esc(doc.n_errors) + ": " + esc((doc.errors || []).map(function (e) { return (e.ticker || e.input || "") + " " + (e.error || ""); }).join("; ")) : "none in this run.")
    ].map(function (x) { return "<p>" + x + "</p>"; }).join("");
  }

  var RULE_LABEL = { leaders: "Leaders", vs: "vs S&P 500", strength: "Strength", ma: "Moving averages", cross: "Golden / death cross", double_top: "Double top", double_bottom: "Double bottom", climax: "Climax", tight: "Tight price",
    ad: "Accumulation / distribution", mf: "Money in / out", f13: "13F cohort", insider: "Insiders", smart: "Smart money", etf_flow: "Fund flows", industry: "Industry rows", history: "A series' history", changes: "Changes" };
  var DRAW = { board: drawBoard, ind: drawIndustries, lead: drawLeaders, etf: drawFunds };
  function draw(which) {
    if (!doc) return;
    if (which) DRAW[which](); else { drawVerdict(); drawBoard(); drawIndustries(); drawLeaders(); drawFunds(); drawRules(); drawSources(); }
    writeHash();
  }
  function stamp() {
    var b = doc.benchmark || {};
    $("stamp").innerHTML = "bars " + esc(doc.as_of || "") + " · engine " + esc(String(doc.generated_at || "").slice(0, 16).replace("T", " ")) + " UTC<br>SPY " + fpx(b.px) + ' <span class="' + tone(at(b, "h.d")) + '">' + fpct(at(b, "h.d"), 2) + "</span> on the session";
  }
  function fail(msg) {
    $("stamp").textContent = "leaders artifact unavailable";
    $("verdict-line").textContent = msg;
  }
  function load() {
    return D.feed(KEY).then(function (d) {
      if (!d || d.contract !== CONTRACT) { if (!doc) fail("data/leaders.json is not published yet - justhodl-leaders runs daily at 03:50 UTC, after the Polygon EOD bank folds the session."); return; }
      if (doc && doc.generated_at === d.generated_at) return;
      doc = d;
      stamp();
      draw();
    }).catch(function (e) { if (!doc) fail(String(e && e.message || e)); });
  }
  function wire() {
    function bind(id, key, table) { var el = $(id); el.oninput = el.onchange = function () { S[key] = el.type === "checkbox" ? el.checked : el.value; if (key === "sector") S.industry = ""; S.all = false; draw(table); }; }
    bind("lead-sector", "sector", "lead"); bind("lead-industry", "industry", "lead"); bind("lead-q", "q", "lead");
    bind("ind-sector", "isector", "ind"); bind("ind-q", "iq", "ind"); bind("etf-q", "eq", "etf"); bind("etf-current", "ecurrent", "etf");
    $("lead-q").value = S.q; $("ind-q").value = S.iq; $("etf-q").value = S.eq;
    $("lead-more").onclick = function () { S.all = true; draw("lead"); };
    $("lead-reset").onclick = function () { S.sector = S.industry = S.q = ""; S.rank = "all"; S.sig = []; S.all = false; $("lead-q").value = ""; draw("lead"); };
    Array.prototype.forEach.call(w.document.querySelectorAll("[data-csv]"), function (b) { b.onclick = function () { download(b.getAttribute("data-csv")); }; });
    w.document.addEventListener("click", function (e) {
      var a = e.target && e.target.closest ? e.target.closest("a") : null;
      if (!a) return;
      if (a.classList.contains("ld-ind")) {                       // an industry name: show its leaders
        e.preventDefault();
        S.industry = a.getAttribute("data-ind"); S.sector = ""; S.q = ""; S.rank = "all"; S.sig = []; S.all = false; $("lead-q").value = "";
        draw("lead");
        $("leaders").scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }
      if (a.classList.contains("tk") && w.top !== w) {            // inside the chart's workspace: load the symbol in the chart behind it
        try {
          if (typeof w.top.jhOpenSymbol === "function") {
            e.preventDefault();
            w.top.jhOpenSymbol(a.getAttribute("data-t"));
            var x = w.top.document.getElementById("wsx");
            if (x) x.click();
          }
        } catch (err) { /* another origin: the link's own target=_top navigation stands */ }
      }
    });
  }
  function boot() {
    if (!D || !$("kpis")) return;
    readHash();
    wire();
    load();
    setInterval(load, REFRESH_MS);
  }
  if (w.document.readyState === "loading") w.document.addEventListener("DOMContentLoaded", boot); else boot();
})(typeof window !== "undefined" ? window : globalThis);
