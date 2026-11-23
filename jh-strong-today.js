/* jh-strong-today.js - the strength desk's two market-wide sections: "against the S&P 500" (who is holding up, by
 * group and by what each stock carried in) and "every stock and ETF" (the table, any column ranks it).
 *
 * Khalid 2026-11-20, the S&P 500 down 2%: "today everything is dumping hard however i cant see which stocks and etfs
 * are showing strength and dumping less than sp500".
 *
 * It draws the view JHStrong.model() builds (jh-strong-engine.js): the nightly whole-market table under today's
 * forming bars while the market is open, the table's own last session when it is shut. Nothing is computed here that
 * the view does not carry; this file is layout, filters, sorting, the CSV and the URL state.
 * Colour is never the only carrier: the outcome is a word in every row, and in the bars one hue steps from "up" to
 * "down less" while "down more" is hatched. */
(function (w) {
  "use strict";
  if (w.JHStrongToday) return;
  var MINUS = "−", DASH = "—", MID = "·", TIMES = "×";
  var $ = function (id) { return w.document.getElementById(id); };
  var V = null;                                                      // the view on screen
  /* the leaders landing, or the order switched on the tearsheet above: the table of names is arranged again */
  if (w.addEventListener) ["jh:leaders-index", "jh:stock-order"].forEach(function (evt) { w.addEventListener(evt, function () { try { if (V) drawNames(V); } catch (e) {} }); });
  var S = { kind: "stock", scope: "b1", out: "auto", rank: "all", show: "counted", sector: "", industry: "", q: "", view: "today", group: "sector", carried: "record", all: false,
            sort: { names: null, groups: null } };
  var HASH_KEYS = ["kind", "scope", "out", "rank", "show", "sector", "industry", "q", "view", "group", "carried"];
  var DEFAULTS = { kind: "stock", scope: "b1", out: "auto", rank: "all", show: "counted", view: "today", group: "sector", carried: "record" };
  var PAGE = 200;
  var OUT_WORD = { up: "UP", flat: "FLAT", down_less: "DOWN LESS", down_more: "DOWN MORE" };
  var OUT_LONG = { up: "up", flat: "flat", down_less: "down less than SPY", down_more: "down more than SPY" };
  var WHY_WORD = { pinned: "pinned to a deal price", geared: "leveraged or inverse fund", jump: "unexplained jump", thin: "thin trading" };

  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]; }); }
  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function fx(v, d) { return isNum(v) ? (v < 0 ? MINUS : "") + Math.abs(v).toFixed(d == null ? 2 : d) : DASH; }
  function sp(v, d) { if (!isNum(v)) return DASH; var a = Math.abs(v).toFixed(d == null ? 2 : d); return (Number(a) === 0 ? "" : v < 0 ? MINUS : "+") + a + "%"; }
  function pp(v, d) { if (!isNum(v)) return DASH; var a = Math.abs(v).toFixed(d == null ? 2 : d); return (Number(a) === 0 ? "" : v < 0 ? MINUS : "+") + a; }
  function pc(v, d) { return isNum(v) ? v.toFixed(d == null ? 0 : d) + "%" : DASH; }
  function fint(v) { return isNum(v) ? Math.round(v).toLocaleString("en-US") : DASH; }
  function money(v) { if (!isNum(v)) return DASH; var a = Math.abs(v); return "$" + (a >= 1e12 ? (a / 1e12).toFixed(2) + "T" : a >= 1e9 ? (a / 1e9).toFixed(a >= 1e11 ? 0 : 1) + "B" : a >= 1e6 ? (a / 1e6).toFixed(0) + "M" : a.toFixed(0)); }
  function px(v) { return isNum(v) ? v.toFixed(Math.abs(v) >= 1 ? 2 : 4) : DASH; }
  function tone(v) { return !isNum(v) || Math.abs(v) < 0.005 ? "flat" : v > 0 ? "up" : "dn"; }
  function tk(t) { return '<a class="tk" href="/chart.html?s=' + encodeURIComponent(t) + '">' + esc(t) + "</a>"; }
  function clock(iso) {                                               // an ISO instant as New York wall time
    if (!iso) return null;
    try { return new Date(iso).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }) + " ET"; } catch (e) { return String(iso).slice(11, 16) + " UTC"; }
  }
  function red(v) { return v && (v.read.regime === "dump" || v.read.regime === "down"); }
  /* a finished session is told in the past tense */
  function words(v) {
    var live = v.mode === "live";
    return { live: live, hold: live ? "holding up" : "held up", areHold: live ? "are holding up" : "held up", areUp: live ? "are up" : "were up", ahead: live ? "ahead of SPY" : "ahead of SPY",
             areAhead: live ? "are ahead of SPY" : "were ahead of SPY", again: live ? "are holding up again" : "held up again", who: live ? "Who is holding up" : "Who held up" };
  }
  function sessionWord(v) { return v.mode === "live" ? "today" : "the last session"; }
  function asOfWord(v) {
    return v.mode === "live" ? "today " + v.session + (v.bars_as_of ? ", bars as of " + clock(v.bars_as_of) : "") + (v.delay_minutes ? " (" + v.delay_minutes + " minutes delayed)" : "")
      : "last session " + v.session + " (final; the market is shut or has not printed)";
  }

  /* ------------------------------------------------------------------------------------------------ the sentence */
  function verdictLine(v) {
    var g = v.read.groups, b1 = g.stocks_1b || {}, spx = g.sp500 || {}, br = v.read.by_rank || [], top = br[9] || {}, bot = br[0] || {}, sec = v.read.by_sector || [];
    var W = words(v), when = W.live ? "Today" + (v.bars_as_of ? ", as of " + clock(v.bars_as_of) : "") : "Last session (" + v.session + ")";
    var out = when + ": SPY " + sp(v.spy.ret) + ". ";
    if (!b1.n) return out + "No stock has printed yet.";
    if (red(v)) {
      out += pc(b1.held_pct) + " of stocks worth $1B or more " + W.areHold + " (up, flat, or down less than SPY) and " + pc(b1.up_pct) + " " + W.areUp;
      if (spx.n) out += "; among S&P 500 members " + pc(spx.held_pct) + " " + W.areHold;
      out += ". ";
      if (sec.length > 1) out += (W.live ? "Holding best: " : "Held best: ") + sec[0].sector + " (" + pc(sec[0].held_pct) + "); " + (W.live ? "breaking most: " : "broke most: ") + sec[sec.length - 1].sector + " (" + pc(sec[sec.length - 1].held_pct) + "). ";
      if (top.n && bot.n) out += "The strongest tenth by rank coming in: " + pc(top.held_pct) + " " + W.hold + ", against " + pc(bot.held_pct) + " of the weakest tenth.";
      var every = (v.read.by_record || [])[0] || {}, few = (v.read.by_record || [])[3] || {};
      if (every.n >= 5) out += " Of the " + fint(every.n) + " that had held up on every dump day of the quarter, " + pc(every.held_pct) + " " + W.again + (few.n >= 5 ? "; of the " + fint(few.n) + " that had held on fewer than a third, " + pc(few.held_pct) : "") + ".";
    } else {
      out += pc(b1.beat_pct) + " of stocks worth $1B or more " + W.areAhead + " and " + pc(b1.up_pct) + " " + W.areUp;
      if (spx.n) out += "; among S&P 500 members " + pc(spx.beat_pct) + " " + (W.live ? "are ahead" : "were ahead");
      out += ". ";
      if (top.n && bot.n) out += "The strongest tenth by rank coming in: " + pc(top.beat_pct) + " ahead of SPY, against " + pc(bot.beat_pct) + " of the weakest tenth.";
    }
    return out;
  }

  /* ---------------------------------------------------------------------------------------- section 1: the groups */
  /* data-jh-chrome: a tile is not a panel of its own, so the section numbering (jh-sections.js) does not number six tiles on a phone */
  function kpi(label, value, sub) { return '<div class="jh-kpi" data-jh-chrome><div class="l">' + label + '</div><div class="v">' + value + '</div><div class="s">' + sub + "</div></div>"; }
  function split(g) { return g && g.n ? "up " + fint(g.up) + " " + MID + " flat " + fint(g.flat) + " " + MID + " down less " + fint(g.down_less) + " " + MID + " down more " + fint(g.down_more) : "none counted"; }
  function headline(g, isRed, hold) { return !g || !g.n ? DASH : pc(isRed ? g.held_pct : g.beat_pct) + " <small>" + (isRed ? hold : "ahead of SPY") + "</small>"; }
  function drawKpis(v) {
    var g = v.read.groups, isRed = red(v), s = v.spy, br = v.read.by_rank || [], top = br[9] || {}, bot = br[0] || {}, H = words(v).hold;
    var where = isNum(s.pos) ? pc(s.pos) + " of the day's range" : "";
    $("today-kpis").innerHTML = [
      kpi("S&amp;P 500 fund (SPY)", '<span class="' + tone(s.ret) + '">' + sp(s.ret) + "</span>", "from the open " + sp(s.fo) + (where ? " " + MID + " " + where : "") + (isNum(s.vwx) ? " " + MID + " " + (s.vwx >= 0 ? "above" : "below") + " the day's VWAP" : "") + (isNum(s.vol) ? " " + MID + " volume " + pc(s.vol * 100) + " of a normal day" : "")),
      kpi("S&amp;P 500 members", headline(g.sp500, isRed, H), split(g.sp500) + (g.sp500.n ? " " + MID + " median " + sp(g.sp500.median_ret) : "")),
      kpi("Stocks worth $1B or more", headline(g.stocks_1b, isRed, H), split(g.stocks_1b) + (g.stocks_1b.n ? " " + MID + " median " + sp(g.stocks_1b.median_ret) : "")),
      kpi("Every stock", headline(g.stocks, isRed, H), split(g.stocks) + (g.stocks.n ? " " + MID + " median " + sp(g.stocks.median_ret) : "")),
      kpi("ETFs trading $5M+ a day", headline(g.etfs_liquid, isRed, H), split(g.etfs_liquid) + (g.etfs_liquid.n ? " " + MID + " median " + sp(g.etfs_liquid.median_ret) : "")),
      kpi("Strongest tenth coming in", top.n ? pc(isRed ? top.held_pct : top.beat_pct) + " <small>" + (isRed ? H : "ahead of SPY") + "</small>" : DASH,
          top.n ? "median " + sp(top.median_ret) + " " + MID + " weakest tenth " + pc(isRed ? bot.held_pct : bot.beat_pct) + ", median " + sp(bot.median_ret) + " " + MID + " ranks as of " + v.rank_as_of : "fewer than 20 ranked $1B+ stocks have printed")
    ].join("");
  }
  /* one group as a stacked bar: up / flat / down less in one hue stepping down, down more hatched */
  function stack(g) {
    if (!g || !g.n) return '<div class="sx-stack empty"></div>';
    var seg = function (k, cls) {
      var share = 100 * g[k] / g.n;
      return share > 0 ? '<i class="' + cls + '" style="width:' + share.toFixed(3) + '%" title="' + esc(OUT_LONG[k] + ": " + fint(g[k]) + " of " + fint(g.n) + " (" + share.toFixed(1) + "%)") + '">' + (share >= 9 ? "<b>" + share.toFixed(0) + "%</b>" : "") + "</i>" : "";
    };
    return '<div class="sx-stack" role="img" aria-label="' + esc("up " + g.up + ", flat " + g.flat + ", down less " + g.down_less + ", down more " + g.down_more + " of " + g.n) + '">' + seg("up", "s-up") + seg("flat", "s-flat") + seg("down_less", "s-less") + seg("down_more", "s-more") + "</div>";
  }
  function drawBars(v) {
    var g = v.read.groups, isRed = red(v), W = words(v);
    $("bars-h").innerHTML = esc(W.who) + ' <span class="m">' + MID + " each group split by how it " + (W.live ? "is doing" : "did") + " against SPY</span>";
    var rows = [["S&P 500 members", g.sp500], ["Stocks $10B+", g.stocks_10b], ["Stocks $1B+", g.stocks_1b], ["Every stock", g.stocks], ["ETFs $5M+ a day", g.etfs_liquid], ["Every ETF", g.etfs]];
    $("today-bars").innerHTML = rows.map(function (x) {
      var q = x[1] || {};
      return '<div class="sx-barrow"><div class="sx-barl"><b>' + esc(x[0]) + '</b><span>' + (q.n ? fint(q.n) + " counted" : "none") + "</span></div>" + stack(q) +
        '<div class="sx-barv">' + (q.n ? "<b>" + pc(isRed ? q.held_pct : q.beat_pct) + "</b><span>" + (isRed ? W.hold : "ahead") + "</span>" : DASH) + "</div></div>";
    }).join("");
    $("today-key").innerHTML = '<span><i class="k s-up"></i>up (more than +' + v.rules.flat_pct + '%)</span><span><i class="k s-flat"></i>flat</span><span><i class="k s-less"></i>down less than SPY</span><span><i class="k s-more"></i>down more than SPY</span>';
  }
  function chips(hostId, items, cur, on) {
    var host = $(hostId);
    if (!host) return;
    host.innerHTML = items.map(function (it) {
      return '<button type="button" class="jh-chip' + (it[0] === cur ? " on" : "") + '" data-v="' + esc(it[0]) + '" aria-pressed="' + (it[0] === cur) + '"' + (it[2] ? ' title="' + esc(it[2]) + '"' : "") + ">" + esc(it[1]) + "</button>";
    }).join("");
    Array.prototype.forEach.call(host.querySelectorAll("[data-v]"), function (b) { b.onclick = function () { on(b.getAttribute("data-v")); }; });
  }
  /* held-up share by what each $1B+ stock carried into the session: its rank (ten tenths) or its dump-day record */
  function drawCarried(v) {
    var isRed = red(v), key = isRed ? "held_pct" : "beat_pct", word = isRed ? words(v).hold : "ahead of SPY";
    chips("carried-mode", [["record", "BY DUMP-DAY RECORD", "by how many of the quarter's dump days each stock held up on, before this session"], ["rank", "BY RANK", "ten tenths of $1B+ stocks by the rank each carried into the session"]], S.carried, function (x) { S.carried = x; writeHash(); drawCarried(V); });
    var items, note;
    if (S.carried === "record") {
      items = (v.read.by_record || []).map(function (g) { return { label: g.label, sub: g.n ? fint(g.n) + " stocks" : "none", g: g }; });
      var nd = (v.dump_days || []).length;
      note = "Stocks worth $1B or more, grouped by how they did on the S&P 500's dump days (down " + Math.abs(v.rules.dump_pct) + "% or more) of the " + (v.rules.record_sessions || 63) + " sessions before " + sessionWord(v) + " " + MID + " " + nd + " dump days in the table's window " + MID + " a record is read from " + (v.rules.record_min || 3) + " dump days on.";
    } else {
      var lab = ["Weakest tenth", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "Strongest tenth"];
      items = (v.read.by_rank || []).map(function (g, i) { return { label: lab[i], sub: g.n ? fint(g.n) : "", g: g }; });
      note = items.length ? "Stocks worth $1B or more in ten equal groups by the rank each carried into the session (as of " + v.rank_as_of + ": the desk's composite of excess return vs SPY, a percentile among all ranked stocks)." : "";
    }
    $("carried").innerHTML = items.length ? items.map(function (it) {
      var q = it.g, val = q.n ? q[key] : null;
      return '<div class="sx-crow"><div class="sx-cl"><b>' + esc(it.label) + "</b><span>" + esc(it.sub) + '</span></div><div class="sx-meter"><i style="width:' + (isNum(val) ? Math.max(0, Math.min(100, val)).toFixed(1) : 0) + '%"></i></div>' +
        '<div class="sx-cv"><b>' + pc(val) + "</b><span>median " + sp(q.median_ret) + "</span></div></div>";
    }).join("") : '<p class="jh-load">Fewer than 20 ranked $1B+ stocks have printed.</p>';
    $("carried-h").textContent = "Share " + (word === "held up" ? "that held up" : word) + ", by what each stock carried in";
    $("carried-note").textContent = note;
  }
  function cmp(a, b) { return a < b ? -1 : a > b ? 1 : 0; }
  /* blanks last either way; equal values fall back to `tie` (the names table: the move against SPY, best first, so a list ranked by industry
     or sector reads best-to-worst inside each one), then the ticker */
  function sortRows(rows, col, dir, tie) {
    var last = function (a, b) { return (tie ? tie(a, b) : 0) || cmp(String(a.t || a.name || ""), String(b.t || b.name || "")); };
    return rows.slice().sort(function (a, b) {
      var x = col.get(a), y = col.get(b);
      if (x == null && y == null) return last(a, b);
      if (x == null) return 1;
      if (y == null) return -1;
      return (dir < 0 ? -cmp(x, y) : cmp(x, y)) || last(a, b);
    });
  }
  function byX(a, b) { var x = a.x, y = b.x; return ((x == null) - (y == null)) || (x == null ? 0 : cmp(y, x)); }
  /* 2026-11-23, Khalid: "the leaders and largest market caps should show up first at the top of the engines". Until a column is
     clicked, the table of names stands with the industry leaders first (largest first), then every other name by market value; the
     desk's own ranking is one button away - the same switch as the tearsheet above, kept in the browser. Nothing is left out. */
  function largestFirst(table) {
    var P = w.JHTearsheet && w.JHTearsheet.priority;
    if (!P || !(table === "names")) return null;
    return { P: P, on: P.mode() === "priority" && !(S.own && S.own[table]) };
  }
  function orderBar(table, L) {
    if (!L) return "";
    var chosen = !!(S.own && S.own[table]);
    return '<div data-order-for="' + table + '" style="display:flex;gap:6px;flex-wrap:wrap;margin:0 0 8px">' +
      '<button type="button" class="jh-chip' + (L.on ? " on" : "") + '" aria-pressed="' + (L.on ? "true" : "false") + '" data-stock-order="priority" title="Industry leaders first, largest first, then every other name by market value. Nothing is left out.">Leaders \u00b7 largest first</button>' +
      '<button type="button" class="jh-chip' + (!L.on && !chosen ? " on" : "") + '" aria-pressed="' + (!L.on && !chosen ? "true" : "false") + '" data-stock-order="engine" title="The order this desk ranks the table in.">The desk\u2019s own order</button>' +
      (chosen ? '<span class="jh-chip on" aria-hidden="true">Ranked by the column you clicked</span>' : "") + "</div>";
  }
  function head(table, cols, numbered, unranked) {
    var s = unranked ? null : S.sort[table];
    return "<thead><tr>" + (numbered ? '<th class="sx-pos" title="position in the list on screen">#</th>' : "") + cols.map(function (c) {
      var on = s && s.k === c.id;
      return '<th scope="col" tabindex="0" data-sort="' + c.id + '" class="c-' + c.id + (c.type === "s" ? " txt" : "") + '"' + (on ? ' aria-sort="' + (s.d > 0 ? "ascending" : "descending") + '"' : "") + (c.tip ? ' title="' + esc(c.tip) + '"' : "") + ">" + esc(c.t) + "</th>";
    }).join("") + "</tr></thead>";
  }
  function paint(table, hostId, cols, rows, empty, cap, numbered, redraw) {
    var s = S.sort[table], c = null, L = largestFirst(table);
    if (L && L.on) s = null;
    if (s) cols.forEach(function (x) { if (x.id === s.k) c = x; });
    if (s && !c && table === "names" && COL[s.k]) c = COL[s.k];        // ranked by a column the view on screen does not show: the order holds
    var sorted = L && L.on ? L.P.order(rows) : c ? sortRows(rows, c, s.d, table === "names" ? byX : null) : rows, shown = cap ? sorted.slice(0, cap) : sorted;
    $(hostId).innerHTML = orderBar(table, L) + '<table class="jh-tbl sx-tbl">' + head(table, cols, numbered, L && L.on) + "<tbody>" +
      (shown.map(function (r, i) { return "<tr>" + (numbered ? '<td class="sx-pos">' + (i + 1) + "</td>" : "") + cols.map(function (x) { var h = x.cell(r); return h.indexOf('<td class="') === 0 ? '<td class="c-' + x.id + " " + h.slice(11) : '<td class="c-' + x.id + '"' + h.slice(3); }).join("") + "</tr>"; }).join("") ||
        '<tr><td class="jh-load" colspan="' + (cols.length + (numbered ? 1 : 0)) + '">' + esc(empty) + "</td></tr>") + "</tbody></table>";
    Array.prototype.forEach.call($(hostId).querySelectorAll("[data-stock-order]"), function (b) {
      b.onclick = function () { S.own = S.own || {}; S.own[table] = false; S.sort[table] = null; if (L) L.P.set(b.getAttribute("data-stock-order")); writeHash(); redraw(); };
    });
    Array.prototype.forEach.call($(hostId).querySelectorAll("th[data-sort]"), function (th) {
      function go() {
        var id = th.getAttribute("data-sort"), cur = L && L.on ? null : S.sort[table], def = null;
        cols.forEach(function (x) { if (x.id === id) def = x; });
        if (!def) return;
        S.own = S.own || {}; S.own[table] = true;
        S.sort[table] = cur && cur.k === id ? { k: id, d: -cur.d } : { k: id, d: def.type === "s" ? 1 : -1 };
        writeHash();
        redraw();
      }
      th.onclick = go;
      th.onkeydown = function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(); } };
    });
    return sorted;
  }
  function num(v, f, cls) { return '<td class="' + (cls || "") + (isNum(v) ? "" : " na") + '">' + (isNum(v) ? f(v) : DASH) + "</td>"; }
  function GROUP_COLS(v, nameT) {
    var isRed = red(v);
    return [
      { id: "group", t: nameT, type: "s", get: function (g) { return g.name; },
        cell: function (g) { return '<td class="l"><button type="button" class="sx-lnk" data-pick="' + esc(g.name) + '" title="Show these names in the table below">' + esc(g.name) + "</button>" + (g.sub ? ' <span class="dim">' + esc(g.sub) + "</span>" : "") + "</td>"; } },
      { id: "n", t: "Counted", get: function (g) { return g.n; }, cell: function (g) { return num(g.n, fint); } },
      { id: "mix", t: "Up / flat / down less / down more", type: "s", get: function (g) { return g.n ? g.held_pct : null; }, cell: function (g) { return '<td class="l sx-mix">' + stack(g) + "</td>"; } },
      { id: "held", t: words(v).live ? "Holding up" : "Held up", tip: "share up, flat, or down less than SPY", get: function (g) { return g.held_pct; }, cell: function (g) { return num(g.held_pct, function (x) { return pc(x); }, isRed ? "strong" : ""); } },
      { id: "up", t: "Up", tip: "share up more than the flat band", get: function (g) { return g.up_pct; }, cell: function (g) { return num(g.up_pct, function (x) { return pc(x); }); } },
      { id: "beat", t: "Ahead of SPY", tip: "share whose move is above SPY's", get: function (g) { return g.beat_pct; }, cell: function (g) { return num(g.beat_pct, function (x) { return pc(x); }, isRed ? "" : "strong"); } },
      { id: "more", t: "Down more", tip: "number down at least as much as SPY", get: function (g) { return g.down_more; }, cell: function (g) { return num(g.down_more, fint); } },
      { id: "med", t: "Median move", get: function (g) { return g.median_ret; }, cell: function (g) { return num(g.median_ret, sp, tone(g.median_ret)); } },
      { id: "x", t: "Mean vs SPY", tip: "the group's mean move minus SPY's, percentage points", get: function (g) { return g.mean_excess; }, cell: function (g) { return num(g.mean_excess, pp, tone(g.mean_excess)); } },
      { id: "cap", t: "Market value", get: function (g) { return g.mcap == null ? null : g.mcap; }, cell: function (g) { return num(g.mcap, money); } }
    ];
  }
  function drawGroups(v) {
    chips("today-group", [["sector", "SECTORS", "stocks worth $1B or more, by sector"], ["industry", "INDUSTRIES", "every industry with five or more $1B+ stocks"], ["class", "ETF CLASSES", "ETFs trading $5M or more a day, by asset class"]], S.group, function (x) { S.group = x; S.sort.groups = null; writeHash(); drawGroups(V); });
    var rows, nameT, empty = "No group has a counted name yet.";
    if (S.group === "class") { rows = (v.read.by_class || []).map(function (g) { return Object.assign({ name: g.label }, g); }); nameT = "ETF class"; }
    else if (S.group === "industry") {
      // every industry with enough names (the view's own list); a view from before that list existed falls back to the read's fifteen at each end
      var ind = v.read.industries || {}, seen = {}, every = v.industries && v.industries.length ? v.industries : (ind.holding || []).concat(ind.breaking || []);
      rows = every.filter(function (g) { if (seen[g.industry]) return false; seen[g.industry] = 1; return true; }).map(function (g) { return Object.assign({ name: g.industry, sub: g.sector }, g); });
      nameT = "Industry (" + fint(rows.length) + " with 5 or more stocks)";
    } else { rows = (v.read.by_sector || []).map(function (g) { return Object.assign({ name: g.sector }, g); }); nameT = "Sector"; }
    if (!S.sort.groups) S.sort.groups = S.group === "class" ? { k: "med", d: -1 } : { k: red(v) ? "held" : "beat", d: -1 };
    var cols = GROUP_COLS(v, nameT).filter(function (c) { return c.id !== "cap" || S.group === "sector"; });
    paint("groups", "today-groups", cols, rows, empty, 0, false, function () { drawGroups(V); });
  }

  /* ---------------------------------------------------------------------------------------- section 2: every name */
  function outChip(r) {
    if (r.unconfirmed) return '<span class="sx-chip mute" title="a move beyond the split-size bounds with no split on record: shown, not counted until a close confirms it">UNCONFIRMED</span>';
    if (!r.bar) return '<span class="sx-chip mute">NO PRINT YET</span>';
    if (!r.out) return '<span class="sx-chip mute">' + DASH + "</span>";
    return '<span class="sx-chip o-' + r.out + '">' + OUT_WORD[r.out] + "</span>" + (r.why ? ' <span class="sx-chip mute" title="measured and shown, kept out of the counts and the ranks">' + esc(WHY_WORD[r.why] || r.why) + "</span>" : "");
  }
  function track(v) { return isNum(v) ? '<span class="sx-track" title="' + v.toFixed(0) + "% of the day's range, low to high\"><i style=\"left:" + Math.max(0, Math.min(100, v)).toFixed(1) + '%"></i></span> ' + v.toFixed(0) + "%" : DASH; }
  function dots(r) {
    if (r.ticks == null) return DASH;
    var names = ["up on the day", "above the open", "above the day's VWAP", "in the upper third of the day's range", "volume above the market's pace"];
    return '<span class="sx-dots" title="' + esc(names.map(function (n, i) { return (r.facts[i] === true ? "✓ " : r.facts[i] === false ? "✗ " : "? ") + n; }).join("\n")) + '">' +
      r.facts.map(function (f) { return '<i class="' + (f === true ? "on" : f === false ? "off" : "na") + '"></i>'; }).join("") + "</span> " + r.ticks + "/" + r.tickN;
  }
  function rec(n, h) { return n == null ? DASH : h + "/" + n; }
  function nameCell(r) { return '<td class="l c-name"><span class="nm" title="' + esc(r.name || "") + '">' + esc(r.name || "") + "</span></td>"; }
  var LABELS = {};                                                     // the desk's asset-class labels, from the view being drawn
  function whereOf(r) { return r.kind === "etf" ? (LABELS[r.cls] || r.cls || "ETF") : (r.sector || "Unclassified"); }
  var COL = {
    t: { id: "t", t: "Ticker", type: "s", get: function (r) { return r.t; }, cell: function (r) { return '<td class="l">' + tk(r.t) + '<span class="nm2">' + esc(r.name || "") + "</span></td>"; } },
    name: { id: "name", t: "Name", type: "s", get: function (r) { return r.name; }, cell: nameCell },
    where: { id: "where", t: "Sector / class", type: "s", get: whereOf, cell: function (r) { return '<td class="l dim" title="' + esc(whereOf(r) + (r.industry ? " \u00b7 " + r.industry : "")) + '"><span class="nm wh">' + esc(whereOf(r)) + "</span></td>"; } },
    ind: { id: "ind", t: "Industry", type: "s", tip: "the company's industry; a fund has none", get: function (r) { return r.kind === "etf" ? null : (r.industry || null); },
           cell: function (r) { var x = r.kind === "etf" ? "" : (r.industry || ""); return '<td class="l dim" title="' + esc(x) + '"><span class="nm wh">' + esc(x || DASH) + "</span></td>"; } },
    mcap: { id: "mcap", t: "Mkt value", tip: "market value (a fund's assets)", get: function (r) { return r.mcap; }, cell: function (r) { return num(r.mcap, money); } },
    px: { id: "px", t: "Last", get: function (r) { return r.px; }, cell: function (r) { return num(r.px, px); } },
    ret: { id: "ret", t: "Move", tip: "the session's move: the last price over the close before", get: function (r) { return r.ret; }, cell: function (r) { return num(r.ret, sp, tone(r.ret)); } },
    x: { id: "x", t: "vs SPY", tip: "the move minus SPY's, percentage points", get: function (r) { return r.x; }, cell: function (r) { return num(r.x, pp, tone(r.x) + " strong"); } },
    fo: { id: "fo", t: "From open", tip: "the last price over the session's open", get: function (r) { return r.fo; }, cell: function (r) { return num(r.fo, sp, tone(r.fo)); } },
    pos: { id: "pos", t: "Day range", tip: "where the last price sits between the session's low and high", get: function (r) { return r.pos; }, cell: function (r) { return '<td class="l' + (isNum(r.pos) ? "" : " na") + '">' + track(r.pos) + "</td>"; } },
    vwx: { id: "vwx", t: "vs VWAP", tip: "the last price against the session's volume-weighted average price", get: function (r) { return r.vwx; }, cell: function (r) { return num(r.vwx, sp, tone(r.vwx)); } },
    rv: { id: "rv", t: "Vol pace", tip: "volume against the name's own 20-session average, over the median name's same ratio: 1.0 is the market's pace at this hour", get: function (r) { return r.rv; }, cell: function (r) { return num(r.rv, function (x) { return x.toFixed(1) + TIMES; }, r.rv >= 1.5 ? "amber" : ""); } },
    ticks: { id: "ticks", t: "Facts", tip: "how many of five hold: up on the day, above the open, above the day's VWAP, upper third of the day's range, volume above the market's pace", get: function (r) { return r.ticks; }, cell: function (r) { return '<td class="l">' + dots(r) + "</td>"; } },
    out: { id: "out", t: "Against SPY", type: "s", get: function (r) { return r.out ? ["down_more", "down_less", "flat", "up"].indexOf(r.out) : null; }, cell: function (r) { return '<td class="l">' + outChip(r) + "</td>"; } },
    rank: { id: "rank", t: "Rank in", tip: "the rank carried into the session: the desk's composite of excess return vs SPY, a percentile among the name's kind", get: function (r) { return r.rank; }, cell: function (r) { return isNum(r.rank) ? '<td><span class="sx-bar"><i style="left:' + r.rank.toFixed(0) + '%"></i></span>' + r.rank.toFixed(0) + "</td>" : '<td class="na">' + DASH + "</td>"; } },
    dump: { id: "dump", t: "Dump days", tip: "dump days of the quarter the name held up on, before this session (held / dump days)", get: function (r) { return r.rec_n ? r.rec_h / r.rec_n : null; }, cell: function (r) { return '<td class="' + (r.bucket === "all" ? "amber" : r.rec_n == null ? "na" : "") + '">' + rec(r.rec_n, r.rec_h) + "</td>"; } },
    ds: { id: "ds", t: "Streak", tip: "the most recent dump days held up on, in a row (through the table's session)", get: function (r) { return r.ds; }, cell: function (r) { return num(r.ds, fint); } },
    dx: { id: "dx", t: "Dump-day pp", tip: "mean move minus SPY's on the quarter's dump days, percentage points", get: function (r) { return r.dx; }, cell: function (r) { return num(r.dx, pp, tone(r.dx)); } },
    rally: { id: "rally", t: "Rally days", tip: "rally days of the quarter the name beat SPY on (beat / rally days)", get: function (r) { return r.rn ? r.rb / r.rn : null; }, cell: function (r) { return '<td class="' + (r.rn == null ? "na" : "") + '">' + rec(r.rn, r.rb) + "</td>"; } },
    rx: { id: "rx", t: "Rally-day pp", tip: "mean move minus SPY's on the quarter's rally days, percentage points", get: function (r) { return r.rx; }, cell: function (r) { return num(r.rx, pp, tone(r.rx)); } },
    dnx: { id: "dnx", t: "Down days pp", tip: "mean excess on all of SPY's down days, 63 sessions", get: function (r) { return r.dnx; }, cell: function (r) { return num(r.dnx, pp, tone(r.dnx)); } },
    upx: { id: "upx", t: "Up days pp", tip: "mean excess on all of SPY's up days, 63 sessions", get: function (r) { return r.upx; }, cell: function (r) { return num(r.upx, pp, tone(r.upx)); } },
    both: { id: "both", t: "Both sides pp", tip: "the weaker of the two: positive = falls less than SPY AND rises more", get: function (r) { return isNum(r.dnx) && isNum(r.upx) ? Math.min(r.dnx, r.upx) : null; }, cell: function (r) { var b = isNum(r.dnx) && isNum(r.upx) ? Math.min(r.dnx, r.upx) : null; return num(b, pp, tone(b)); } },
    x5: { id: "x5", t: "vs SPY 1W", tip: "excess return over 5 sessions through the table's session", get: function (r) { return r.x5; }, cell: function (r) { return num(r.x5, pp, tone(r.x5)); } },
    x21: { id: "x21", t: "1M", tip: "21 sessions", get: function (r) { return r.x21; }, cell: function (r) { return num(r.x21, pp, tone(r.x21)); } },
    x63: { id: "x63", t: "3M", tip: "63 sessions", get: function (r) { return r.x63; }, cell: function (r) { return num(r.x63, pp, tone(r.x63)); } },
    x126: { id: "x126", t: "6M", tip: "126 sessions", get: function (r) { return r.x126; }, cell: function (r) { return num(r.x126, pp, tone(r.x126)); } },
    p52: { id: "p52", t: "52w place", tip: "where the table's close sits in its 52-week range", get: function (r) { return r.p52; }, cell: function (r) { return num(r.p52, function (x) { return x.toFixed(0) + "%"; }); } },
    ma: { id: "ma", t: "50 / 200", tip: "above the 50-day and the 200-day average at the table's close", get: function (r) { return r.ma; }, cell: function (r) { return '<td class="l' + (r.ma == null ? " na" : "") + '">' + (r.ma == null ? DASH : (r.ma & 1 ? "above" : "below") + " / " + (r.ma & 2 ? "above" : "below")) + "</td>"; } },
    atr: { id: "atr", t: "ATR", tip: "14-session average true range, percent of price", get: function (r) { return r.atr; }, cell: function (r) { return num(r.atr, function (x) { return x.toFixed(1) + "%"; }); } }
  };
  var VIEWS = {
    today: ["t", "name", "where", "ind", "mcap", "px", "ret", "x", "out", "rank", "dump", "fo", "pos", "vwx", "rv", "ticks"],
    record: ["t", "name", "ret", "x", "out", "rank", "dump", "ds", "dx", "rally", "rx", "dnx", "upx", "both"],
    trend: ["t", "name", "ret", "x", "out", "rank", "x5", "x21", "x63", "x126", "p52", "ma", "atr", "mcap"]
  };
  var PHONE_TODAY = ["t", "name", "ret", "x", "out", "mcap", "where", "ind", "rank", "dump", "px", "fo", "pos", "vwx", "rv", "ticks"];
  function narrow() { try { return !!(w.matchMedia && w.matchMedia("(max-width: 640px)").matches); } catch (e) { return false; } }
  function outFilter(v) { return S.out === "auto" ? (red(v) ? "held" : "all") : S.out; }
  function rowFilter(v, st) {
    LABELS = v.class_labels || LABELS;
    var R = v.rules, q = (st.q || "").trim().toUpperCase(), of = st.out === "auto" ? (red(v) ? "held" : "all") : st.out;
    return function (r) {
      if (st.kind !== "all" && r.kind !== st.kind) return false;
      if (r.kind === "stock") {
        if (st.scope === "b1" && !((r.mcap || 0) >= R.board_min_mcap)) return false;
        if (st.scope === "b10" && !((r.mcap || 0) >= R.big_mcap)) return false;
        if (st.scope === "sp" && !r.sp) return false;
      } else if (st.scope !== "all" && !((r.adv || 0) >= R.etf_min_adv)) return false;
      if (st.show === "counted" && !r.counted) return false;
      if (st.show === "aside" && (r.counted || !r.bar)) return false;
      if (of === "held" && !(r.out && r.out !== "down_more")) return false;
      if (of !== "all" && of !== "held" && r.out !== of) return false;
      if (st.rank === "strong" && !(isNum(r.rank) && r.rank >= 80)) return false;
      if (st.rank === "weak" && !(isNum(r.rank) && r.rank <= 20)) return false;
      if (st.rank === "record" && r.bucket !== "all") return false;
      if (st.sector && whereOf(r) !== st.sector) return false;
      if (st.industry && r.industry !== st.industry) return false;
      if (q && !(String(r.t).toUpperCase().indexOf(q) === 0 || String(r.name || "").toUpperCase().indexOf(q) >= 0 || String(r.industry || "").toUpperCase().indexOf(q) >= 0)) return false;
      return true;
    };
  }
  var last = { cols: [], rows: [] };
  function drawNames(v) {
    var isRed = red(v), of = outFilter(v);
    chips("names-kind", [["stock", "STOCKS"], ["etf", "ETFS"], ["all", "BOTH"]], S.kind, function (x) { S.kind = x; S.sector = ""; S.industry = ""; S.all = false; writeHash(); drawNames(V); });
    chips("names-scope", S.kind === "etf" ? [["b1", "TRADING $5M+ A DAY"], ["all", "EVERY ETF"]] : [["sp", "S&P 500"], ["b10", "$10B+"], ["b1", "$1B+"], ["all", "EVERY STOCK"]],
          S.kind === "etf" && S.scope !== "all" ? "b1" : S.scope, function (x) { S.scope = x; S.all = false; writeHash(); drawNames(V); });
    chips("names-out", [["held", v.mode === "live" ? "HOLDING UP" : "HELD UP", "up, flat, or down less than SPY"], ["up", "UP"], ["flat", "FLAT"], ["down_less", "DOWN LESS"], ["down_more", "DOWN MORE"], ["all", "ALL"]], of, function (x) { S.out = x; S.all = false; writeHash(); drawNames(V); });
    chips("names-rank", [["all", "ANY RANK"], ["strong", "STRONG COMING IN", "rank 80 or higher carried into the session"], ["weak", "WEAK COMING IN", "rank 20 or lower"], ["record", "HELD EVERY DUMP DAY", "held up on every dump day of the quarter before this session (three or more)"]], S.rank, function (x) { S.rank = x; S.all = false; writeHash(); drawNames(V); });
    chips("names-view", [["today", v.mode === "live" ? "TODAY" : "THE SESSION", "the move, the bar and the facts"], ["record", "RECORD", "the dump-day and rally-day record"], ["trend", "TREND", "excess returns, the 52-week place, the averages"]], S.view, function (x) { S.view = x; writeHash(); drawNames(V); });
    chips("names-show", [["counted", "COUNTED"], ["aside", "SET ASIDE", "pinned to a deal price, leveraged or inverse, an unexplained jump, thin trading, or an unconfirmed move"], ["all", "ALL ROWS"]], S.show, function (x) { S.show = x; S.all = false; writeHash(); drawNames(V); });
    // the sector / class menu, from the rows the other filters leave
    var base = Object.assign({}, S, { sector: "" }), pool = v.rows.filter(rowFilter(v, base)), seen = {}, opts = [];
    pool.forEach(function (r) { var k = whereOf(r); seen[k] = (seen[k] || 0) + 1; });
    Object.keys(seen).sort().forEach(function (k) { opts.push('<option value="' + esc(k) + '"' + (k === S.sector ? " selected" : "") + ">" + esc(k) + " (" + seen[k] + ")</option>"); });
    $("names-sector").innerHTML = '<option value="">' + (S.kind === "etf" ? "every class" : S.kind === "all" ? "every sector and class" : "every sector") + "</option>" + opts.join("");
    if (S.sector && !seen[S.sector]) S.sector = "";
    // the industry menu: the industries of the rows the other filters (the sector among them) leave; a fund has no industry
    var poolI = v.rows.filter(rowFilter(v, Object.assign({}, S, { industry: "" }))), seenI = {}, optsI = [], im = $("names-industry");
    poolI.forEach(function (r) { if (r.kind !== "etf" && r.industry) seenI[r.industry] = (seenI[r.industry] || 0) + 1; });
    if (S.industry && !seenI[S.industry]) S.industry = "";
    Object.keys(seenI).sort().forEach(function (k) { optsI.push('<option value="' + esc(k) + '"' + (k === S.industry ? " selected" : "") + ">" + esc(k) + " (" + seenI[k] + ")</option>"); });
    im.innerHTML = '<option value="">every industry</option>' + optsI.join("");
    im.parentNode.hidden = S.kind === "etf";
    var rows = v.rows.filter(rowFilter(v, S));
    var ids = VIEWS[S.view] || VIEWS.today;
    // on a phone the answer comes first: the move, the move against SPY and the word for it sit beside the ticker, the rest scrolls
    if (narrow() && ids === VIEWS.today) ids = PHONE_TODAY;
    var cols = ids.map(function (id) { return COL[id]; });
    if (!S.sort.names) S.sort.names = { k: "x", d: -1 };
    var sorted = paint("names", "names-tbl", cols, rows, v.mode === "live" && !v.read.n_counted ? "No name has printed yet today." : "No name passes these filters.", S.all ? 0 : PAGE, true, function () { drawNames(V); });
    last = { cols: Object.keys(COL).map(function (k) { return COL[k]; }), rows: sorted };
    var more = $("names-more");
    more.hidden = S.all || sorted.length <= PAGE;
    more.textContent = "Show all " + fint(sorted.length) + " rows";
    more.onclick = function () { S.all = true; drawNames(V); };
    var u = v.universe || {};
    $("names-meta").textContent = fint(rows.length) + " of " + fint(v.rows.length) + " names " + MID + " " + asOfWord(v) + " " + MID + " rank and record carried in as of " + v.rank_as_of;
    var notes = [];
    if (v.mode === "live" && v.gap_sessions > 1) notes.push("The whole-market table is from " + v.table_as_of + ": these are moves since that close, " + v.gap_sessions + " weekdays ago. If a session ran in between they are not one day's moves.");
    if (v.unconfirmed.length) notes.push("Unconfirmed, not counted (a move beyond the size of a split with none on record): " + v.unconfirmed.slice(0, 20).map(function (r) { return r.t; }).join(", ") + (v.unconfirmed.length > 20 ? " and " + (v.unconfirmed.length - 20) + " more" : "") + ".");
    if (v.splits_today.length) notes.push("Split in force today, the last close divided by its ratio: " + v.splits_today.map(function (r) { return r.t + " (" + r.split.to + "-for-" + r.split.from + ")"; }).join(", ") + ".");
    if (v.mode === "live" && v.n_no_print) notes.push(fint(v.n_no_print) + " names in the table have not printed today.");
    var sa = u.set_aside || {};
    notes.push("Measured and set aside from every count and rank: " + Object.keys(sa).map(function (k) { return fint(sa[k]) + " " + (WHY_WORD[k] || k); }).join(", ") + ".");
    $("names-aside").innerHTML = notes.map(esc).join("<br>");
  }
  function csvOf(cols, rows, labels) {
    if (labels) LABELS = labels;
    function q(x) { x = x == null ? "" : String(x); return /[",\n]/.test(x) ? '"' + x.replace(/"/g, '""') + '"' : x; }
    var extra = [["sector", function (r) { return r.sector; }], ["kind", function (r) { return r.kind; }], ["set_aside", function (r) { return r.why; }], ["outcome", function (r) { return r.out; }]];
    return [cols.map(function (c) { return q(c.id === "ind" ? "industry" : c.id); }).concat(extra.map(function (e) { return e[0]; })).join(",")].concat(rows.map(function (r) {
      return cols.map(function (c) { var x = c.id === "out" ? r.out : c.id === "ticks" ? r.ticks : c.id === "dump" ? rec(r.rec_n, r.rec_h) : c.id === "rally" ? rec(r.rn, r.rb) : c.id === "where" ? whereOf(r) : c.get(r); return q(isNum(x) ? String(Math.round(x * 10000) / 10000) : x); })
        .concat(extra.map(function (e) { return q(e[1](r)); })).join(",");
    })).join("\n");
  }
  function download() {
    if (!V) return;
    var a = w.document.createElement("a");
    a.href = URL.createObjectURL(new Blob([csvOf(last.cols, last.rows)], { type: "text/csv" }));
    a.download = "strength-vs-spy-" + V.session + (V.mode === "live" ? "-live" : "") + "-" + S.kind + ".csv";
    w.document.body.appendChild(a);
    a.click();
    a.remove();
  }

  /* ------------------------------------------------------------------------------------------------------- state */
  function readHash() {
    var h = String(w.location.hash || "").replace(/^#/, "");
    if (!h || h.indexOf("=") < 0) return;
    h.split("&").forEach(function (kv) {
      var i = kv.indexOf("="), k = kv.slice(0, i), val = decodeURIComponent(kv.slice(i + 1));
      if (HASH_KEYS.indexOf(k) >= 0) S[k] = val;
      else if (k === "s_names" || k === "s_groups") { var p = val.split(":"); S.sort[k.slice(2)] = { k: p[0], d: p[1] === "asc" ? 1 : -1 }; S.own = S.own || {}; S.own[k.slice(2)] = true; }
    });
  }
  function writeHash() {
    var out = [];
    HASH_KEYS.forEach(function (k) { var x = S[k]; if (x && DEFAULTS[k] !== x) out.push(k + "=" + encodeURIComponent(x)); });
    if (S.sort.names && !(S.sort.names.k === "x" && S.sort.names.d === -1)) out.push("s_names=" + encodeURIComponent(S.sort.names.k + ":" + (S.sort.names.d > 0 ? "asc" : "desc")));
    try { w.history.replaceState(null, "", out.length ? "#" + out.join("&") : w.location.pathname + w.location.search); } catch (e) { /* a sandboxed frame: the state stays in memory */ }
  }
  /* A group picked anywhere on the page - a row of the sector / industry / class table above, or a row of the tearsheet's sector and industry
     lists opened in full (the kit's jts:pick event) - is shown in this table: every counted name of it, ranked by the move against SPY. */
  function pick(d) {
    if (!d || !V) return false;
    S.kind = d.kind === "etf" ? "etf" : "stock";
    S.scope = S.kind === "etf" ? (d.scope === "all" ? "all" : "b1") : (["sp", "b10", "b1", "all"].indexOf(d.scope) >= 0 ? d.scope : "b1");
    S.out = d.out || "all"; S.rank = "all"; S.show = "counted"; S.q = ""; S.sector = d.sector || ""; S.industry = d.industry || ""; S.all = false; S.sort.names = null; if (S.own) S.own.names = false;
    var qi = $("names-q"); if (qi) qi.value = "";
    writeHash(); drawNames(V);
    var sec = $("names");
    try { sec.scrollIntoView({ behavior: "smooth", block: "start" }); } catch (e) { try { sec.scrollIntoView(); } catch (e2) { /* no layout */ } }
    return true;
  }
  var wired = false;
  function wire() {
    if (wired) return;
    wired = true;
    readHash();
    var qi = $("names-q"), t = null;
    qi.value = S.q || "";
    qi.oninput = function () { clearTimeout(t); t = setTimeout(function () { S.q = qi.value; S.all = false; writeHash(); if (V) drawNames(V); }, 140); };
    $("names-sector").onchange = function () { S.sector = this.value; S.all = false; writeHash(); if (V) drawNames(V); };
    $("names-industry").onchange = function () { S.industry = this.value; S.all = false; writeHash(); if (V) drawNames(V); };
    $("today-groups").addEventListener("click", function (e) {
      var b = e.target.closest ? e.target.closest("[data-pick]") : null;
      if (!b) return;
      var name = b.getAttribute("data-pick");
      pick(S.group === "class" ? { kind: "etf", scope: "b1", sector: name } : S.group === "industry" ? { kind: "stock", scope: "b1", industry: name } : { kind: "stock", scope: "b1", sector: name });
    });
    w.document.addEventListener("jts:pick", function (e) { if (e && e.detail && e.detail.desk === "strength") pick(e.detail); });
    $("names-reset").onclick = function () { Object.keys(DEFAULTS).forEach(function (k) { S[k] = DEFAULTS[k]; }); S.sector = ""; S.industry = ""; S.q = ""; qi.value = ""; S.all = false; S.sort.names = null; if (S.own) S.own.names = false; writeHash(); if (V) { drawGroups(V); drawCarried(V); drawNames(V); } };
    $("names-csv").onclick = download;
  }
  function render(v) {
    V = v;
    LABELS = v.class_labels || {};
    wire();
    var isRed = red(v);
    $("verdict-line").textContent = verdictLine(v);
    $("today-h").textContent = (v.mode === "live" ? "Today" : "The last session") + " against the S&P 500";
    $("today-meta").textContent = asOfWord(v) + " " + MID + " " + fint(v.read.n_counted) + " names counted";
    $("today-sub").textContent = (v.mode === "live"
      ? "Today's forming day bar of every listed stock and ETF, refreshed every five minutes from the Polygon feed" + (v.delay_minutes ? " (" + v.delay_minutes + " minutes delayed)" : "") + ", over last night's close. "
      : "The last session's own bar of every listed stock and ETF over the close before it. When the market is open this section follows today's forming bars. ")
      + "Up = more than +" + v.rules.flat_pct + "%; flat = within " + v.rules.flat_pct + "% either way; down less = down, but less than SPY; down more = down at least as much as SPY. "
      + (isRed ? (v.mode === "live" ? "Holding up" : "Held up") + " = up, flat or down less." : "SPY " + (v.mode === "live" ? "is" : "was") + " not down, so the headline is the share ahead of it.");
    drawKpis(v);
    drawBars(v);
    drawCarried(v);
    drawGroups(v);
    drawNames(v);
  }
  function fail(msg) {
    wire();
    $("verdict-line").textContent = msg;
    ["today-kpis", "today-bars", "carried", "today-groups", "names-tbl"].forEach(function (id) { var el = $(id); if (el) el.innerHTML = '<p class="jh-load">' + esc(msg) + "</p>"; });
  }

  w.JHStrongToday = { render: render, fail: fail, verdictLine: verdictLine, rowFilter: rowFilter, csvOf: csvOf, state: S, COL: COL, VIEWS: VIEWS, PHONE_TODAY: PHONE_TODAY, pick: pick };
  if (typeof module === "object" && module.exports) module.exports = w.JHStrongToday;
})(typeof window !== "undefined" ? window : globalThis);
