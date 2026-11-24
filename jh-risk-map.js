/* jh-risk-map.js - the Risk Map as one heat map: every asset class on a screen, what is booming and what is getting destroyed.
 *
 * Reads the document justhodl-regime-map publishes (data/regime-map.json, contract risk-map.v2) and draws it. It measures
 * nothing itself: every state, score and change on a tile is the engine's. Two things are computed here, both from figures
 * the document carries:
 *   - audit(): each change is re-derived from the close and the anchor close it is said to be measured from, and each
 *     BOOMING / DESTROYED tile is held to the rule. The page prints the result; a document that fails says so on its face.
 *   - live(): for funds, the day's regular-session price from the Worker's /quotes (Polygon snapshot, 15 minutes delayed)
 *     over the same anchor closes. It moves the percentages, never a state.
 *
 * Pure functions are exported for node tests (module.exports) and the browser (window.JHRiskMap).
 *
 * jh-reskin-skip: on this map a colour is a measurement. The site build's re-skin (scripts/reskin_site.py) turns every cool
 * hue into the site's amber, and on 2026-11-24 it drew rising tiles amber and the blue palette amber too. Every colour that
 * carries meaning is therefore defined in this file, which the re-skin leaves alone; the page's own frame takes the site's.
 */
(function (root, factory) {
  var api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.JHRiskMap = api;
})(typeof window !== "undefined" ? window : null, function (root) {
  "use strict";

  var WINDOWS = [
    { id: "d", label: "1D", word: "the day", scale: 3 },
    { id: "w", label: "1W", word: "a week", scale: 6 },
    { id: "m", label: "1M", word: "a month", scale: 10 },
    { id: "q", label: "3M", word: "three months", scale: 20 },
    { id: "h", label: "6M", word: "six months", scale: 30 },
    { id: "ytd", label: "YTD", word: "this year", scale: 40 },
    { id: "y", label: "1Y", word: "a year", scale: 50 }
  ];
  var WIN = {};
  WINDOWS.forEach(function (w) { WIN[w.id] = w; });
  var STATES = ["DESTROYED", "RISK-OFF", "NEUTRAL", "RISK-ON", "BOOMING"];
  var PLAIN = { BOOMING: "Booming", "RISK-ON": "Rising", NEUTRAL: "Flat", "RISK-OFF": "Falling", DESTROYED: "Destroyed" };
  var KIND = { fund: "Fund", coin: "Coin", fx: "Currency", fut: "Front-month future", idx: "Index" };
  var KINDS = { fund: "Funds", coin: "Coins", fx: "Currencies", fut: "Futures", idx: "Indexes" };
  var UNUSUAL_FULL = 2.5;              // a move 2.5 times the instrument's own usual move is the full colour
  var SPLIT_BAND = [0.56, 1.8];        // a live price outside this multiple of the engine's close is a split or a bad print: ignored
  var PALETTES = {
    rg: { name: "Red and green", neg: "#f23645", pos: "#089981", mid: "#363a45", none: "#20242e", up: "#2bb89f", dn: "#f7525f" },
    cb: { name: "Orange and blue", neg: "#e8623a", pos: "#3b82f6", mid: "#363a45", none: "#20242e", up: "#6aa8ff", dn: "#f08c5a" }
  };
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var STORE = "jh-risk-map.v1";

  // ------------------------------------------------------------------------------------------------ small things
  function clamp(v, lo, hi) { return v < lo ? lo : v > hi ? hi : v; }
  function median(xs) {
    var s = xs.slice().sort(function (a, b) { return a - b; }), n = s.length;
    return n ? (n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2) : 0;
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function rgb(hex) { return [parseInt(hex.slice(1, 3), 16), parseInt(hex.slice(3, 5), 16), parseInt(hex.slice(5, 7), 16)]; }
  function mix(a, b, t) {
    var x = rgb(a), y = rgb(b), out = "#", i, v;
    for (i = 0; i < 3; i++) { v = Math.round(x[i] + (y[i] - x[i]) * clamp(t, 0, 1)); out += (v < 16 ? "0" : "") + v.toString(16); }
    return out;
  }
  function luminance(hex) {
    var c = rgb(hex).map(function (v) { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); });
    return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2];
  }
  function sign(v) { return v > 0 ? "+" : v < 0 ? "−" : ""; }
  function fmtPct(v, digits) {
    if (v == null || !isFinite(v)) return "–";
    var a = Math.abs(v), d = digits != null ? digits : a >= 100 ? 0 : a >= 10 ? 1 : 2;
    if (+a.toFixed(d) === 0) return (0).toFixed(d) + "%";
    return sign(v) + a.toFixed(d) + "%";
  }
  function fmtScore(v) { if (v == null) return "–"; var n = Math.round(v); return sign(n) + Math.abs(n); }
  function fmtTimes(v) { return v == null || !isFinite(v) ? "–" : sign(v) + Math.abs(v).toFixed(1) + "×"; }
  function fmtPrice(c) {
    if (c == null || !isFinite(c)) return "–";
    var a = Math.abs(c);
    if (a >= 1000) return c.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (a >= 1) return c.toFixed(2);
    if (a >= 0.01) return c.toFixed(4);
    return c.toPrecision(4);
  }
  function fmtBig(v) {
    if (!(v > 0)) return "–";
    return v >= 1e12 ? "$" + (v / 1e12).toFixed(2) + "T" : v >= 1e9 ? "$" + (v / 1e9).toFixed(1) + "B" : v >= 1e6 ? "$" + (v / 1e6).toFixed(1) + "M" : "$" + Math.round(v / 1e3) + "K";
  }
  function fmtClock(iso) { return iso ? String(iso).slice(0, 16).replace("T", " ") + " UTC" : "–"; }
  function nyDate(ms) {
    try {
      var p = new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(new Date(ms)), o = {};
      p.forEach(function (x) { o[x.type] = x.value; });
      return o.year + "-" + o.month + "-" + o.day;
    } catch (e) { return new Date(ms).toISOString().slice(0, 10); }
  }
  function nyClock(ms) {
    try { return new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "2-digit", minute: "2-digit", hour12: false }).format(new Date(ms)) + " New York"; }
    catch (e) { return new Date(ms).toISOString().slice(11, 16) + " UTC"; }
  }

  // ------------------------------------------------------------------------------------------------ colour
  /* How far from "nothing" a tile is, from -1 (the worst colour) to +1 (the best), or null when the tile has no figure for the
   * window. In the state view the five states sit in five separate bands, so a booming tile can never be mistaken for a
   * rising one; inside a band the score shades it. */
  function tone(tile, mode, norm, lv) {
    if (mode === "state") {
      var s = tile.score;
      if (tile.state === "BOOMING") return 0.8 + 0.2 * clamp((s - 45) / 55, 0, 1);
      if (tile.state === "RISK-ON") return 0.3 + 0.22 * clamp((s - 15) / 30, 0, 1);
      if (tile.state === "RISK-OFF") return -(0.3 + 0.22 * clamp((-15 - s) / 30, 0, 1));
      if (tile.state === "DESTROYED") return -(0.8 + 0.2 * clamp((-45 - s) / 55, 0, 1));
      return 0;
    }
    var w = WIN[mode];
    if (!w) return null;
    if (norm) {
      var u = tile.u ? tile.u[mode] : null;
      return u == null ? null : clamp(u / UNUSUAL_FULL, -1, 1);
    }
    var v = valueOf(tile, mode, lv);
    return v == null ? null : clamp(v / w.scale, -1, 1);
  }
  function fill(t, mode, palette) {
    var pal = PALETTES[palette] || PALETTES.rg;
    if (t == null) return pal.none;
    var a = Math.abs(t);
    if (a < 0.02) return pal.mid;
    return mix(pal.mid, t < 0 ? pal.neg : pal.pos, mode === "state" ? a : 0.16 + 0.84 * a);
  }
  /* The number a tile is coloured by: its score in the state view, otherwise the change over the window (live when given). */
  function valueOf(tile, mode, lv) {
    if (mode === "state") return tile.score;
    if (lv && lv.r && lv.r[mode] != null) return lv.r[mode];
    return tile.r && tile.r[mode] != null ? tile.r[mode] : null;
  }
  function labelOf(tile, mode, lv) {
    if (mode === "state") return (tile.state === "BOOMING" ? "▲ " : tile.state === "DESTROYED" ? "▼ " : "") + fmtScore(tile.score);
    return fmtPct(valueOf(tile, mode, lv));
  }

  // ------------------------------------------------------------------------------------------------ size and layout
  function sizeOf(t) { return t.k === "coin" ? +t.mc || 0 : t.k === "fund" ? +t.dv || 0 : 0; }
  /* A tile's share of its group: the square root of how heavily it trades against the group's median (funds: dollars a day;
   * coins: market value), held between 0.45 and 3.2 so nothing vanishes and nothing swallows its group. Currencies, futures
   * and indexes have no such figure and are one size. */
  function weights(tiles, sizing) {
    var by = {}, out = {};
    tiles.forEach(function (t) { (by[t.grp] = by[t.grp] || []).push(t); });
    Object.keys(by).forEach(function (g) {
      var med = median(by[g].map(sizeOf).filter(function (x) { return x > 0; }));
      by[g].forEach(function (t) {
        var x = sizeOf(t);
        out[t.t] = sizing === "equal" || !(x > 0) || !(med > 0) ? 1 : clamp(Math.sqrt(x / med), 0.45, 3.2);
      });
    });
    return out;
  }
  /* Squarified treemap (Bruls, Huizing, van Wijk): rectangles whose areas are in proportion to v, as near square as they go. */
  function squarify(nodes, x, y, w, h) {
    var list = nodes.filter(function (n) { return n.v > 0; }).sort(function (a, b) { return b.v - a.v; });
    var total = list.reduce(function (s, n) { return s + n.v; }, 0), out = [], i = 0;
    if (!list.length || !(w > 0) || !(h > 0)) return out;
    var k = w * h / total;
    while (i < list.length) {
      var side = Math.min(w, h), row = [], sum = 0, best = Infinity;
      while (i < list.length) {
        var a = list[i].v * k, s2 = sum + a, mx = row.length ? row[0] : a;
        var worst = Math.max(side * side * mx / (s2 * s2), s2 * s2 / (side * side * a));
        if (row.length && worst > best) break;
        row.push(a); sum = s2; best = worst; i++;
      }
      var thick = sum / side, off = 0, start = i - row.length, j, len;
      for (j = 0; j < row.length; j++) {
        len = row[j] / thick;
        if (w >= h) out.push({ node: list[start + j], x: x, y: y + off, w: thick, h: len });
        else out.push({ node: list[start + j], x: x + off, y: y, w: len, h: thick });
        off += len;
      }
      if (w >= h) { x += thick; w -= thick; } else { y += thick; h -= thick; }
    }
    return out;
  }
  var CLASS_HEAD = 22, GROUP_HEAD = 15, CLASS_GAP = 3, GROUP_GAP = 1;
  /* Classes, then their groups, then the tiles: three squarified levels inside W x H. opt = {w, h, sizing, cls}. */
  function layout(doc, opt) {
    var m = doc.map, tiles = m.tiles.filter(function (t) { return !opt.cls || t.cls === opt.cls; });
    var wt = weights(tiles, opt.sizing), W = opt.w, H = opt.h, out = { w: W, h: H, classes: [], n: 0 };
    var cnodes = m.classes.map(function (c) {
      var mine = tiles.filter(function (t) { return t.cls === c.id; });
      return { c: c, tiles: mine, v: mine.reduce(function (s, t) { return s + wt[t.t]; }, 0) };
    });
    squarify(cnodes, 0, 0, W, H).forEach(function (cr) {
      var c = cr.node.c, head = cr.h > 54 && cr.w > 80 ? CLASS_HEAD : 0;
      var box = { id: c.id, name: c.name, x: cr.x, y: cr.y, w: cr.w, h: cr.h, head: head, groups: [] };
      var ix = cr.x + CLASS_GAP, iy = cr.y + CLASS_GAP + head, iw = cr.w - 2 * CLASS_GAP, ih = cr.h - 2 * CLASS_GAP - head;
      var gnodes = c.groups.map(function (g) {
        var mine = cr.node.tiles.filter(function (t) { return t.grp === g.id; });
        return { g: g, tiles: mine, v: mine.reduce(function (s, t) { return s + wt[t.t]; }, 0) };
      });
      squarify(gnodes, ix, iy, iw, ih).forEach(function (gr) {
        var g = gr.node.g, gh = c.groups.length > 1 && gr.h > 46 && gr.w > 66 ? GROUP_HEAD : 0;
        var gbox = { id: g.id, name: g.name, role: g.role, x: gr.x, y: gr.y, w: gr.w, h: gr.h, head: gh, tiles: [] };
        var tnodes = gr.node.tiles.map(function (t) { return { t: t, v: wt[t.t] }; });
        squarify(tnodes, gr.x + GROUP_GAP, gr.y + GROUP_GAP + gh, gr.w - 2 * GROUP_GAP, gr.h - 2 * GROUP_GAP - gh).forEach(function (tr) {
          gbox.tiles.push({ t: tr.node.t.t, x: tr.x, y: tr.y, w: tr.w, h: tr.h, v: tr.node.v });
          out.n++;
        });
        box.groups.push(gbox);
      });
      out.classes.push(box);
    });
    return out;
  }
  /* The map's height for a width: the screen's, but never so small that the average tile cannot hold its name. */
  function mapHeight(width, viewport, weight) {
    var floor = Math.ceil(weight * (width < 700 ? 2500 : 2300) / Math.max(1, width));
    return Math.max(width < 700 ? 420 : 440, viewport, floor);
  }
  function tileText(w, h, name, label) {
    if (w < 28 || h < 17) return { fs: 0, vs: 0, two: false, three: false };
    var fs = clamp(Math.min((w - 8) / (Math.max(3, name.length) * 0.76), (h - 4) / 2.4), 8.5, 30);
    var vs = clamp(Math.min(fs * 0.72, (w - 6) / (Math.max(3, String(label || "").length) * 0.6)), 8, 22);
    return { fs: Math.round(fs * 10) / 10, vs: Math.round(vs * 10) / 10, two: h >= fs * 1.14 + vs * 1.2 + 4 && w >= 38, three: h >= fs * 1.14 + vs * 1.2 + 18 && w >= 86 };
  }

  // ------------------------------------------------------------------------------------------------ the lists and the check
  function lists(doc) {
    var tiles = doc.map.tiles;
    return {
      booming: tiles.filter(function (t) { return t.state === "BOOMING"; }).sort(function (a, b) { return b.score - a.score || b.r.q - a.r.q; }),
      destroyed: tiles.filter(function (t) { return t.state === "DESTROYED"; }).sort(function (a, b) { return a.score - b.score || a.r.q - b.r.q; }),
      held: tiles.filter(function (t) { return t.held; }).sort(function (a, b) { return b.score - a.score; })
    };
  }
  /* The rule, on the figures a tile carries (rounded to two places, so a figure that prints as 0.00 passes either sign). */
  function isBooming(t) { return t.score >= 45 && t.r.m >= 0 && t.r.q >= 0 && Math.max(t.r.m, t.r.q) >= 5 && t.v50 >= 0 && t.v200 >= 0; }
  function isDestroyed(t) { return t.score <= -45 && t.r.m <= 0 && t.r.q <= 0 && Math.min(t.r.m, t.r.q) <= -5 && t.v50 <= 0 && t.v200 <= 0; }
  /* Re-derive what can be re-derived from the document itself. Returns {changes, tiles, bad[]}; bad is empty for a sound one. */
  function audit(doc) {
    var bad = [], n = 0, m = doc && doc.map, seen = {};
    if (!m || !m.tiles || !m.tiles.length) return { changes: 0, tiles: 0, bad: ["the document has no map"] };
    m.tiles.forEach(function (t) {
      if (seen[t.t]) bad.push(t.t + " appears twice");
      seen[t.t] = 1;
      Object.keys(t.a || {}).forEach(function (k) {
        n++;
        var a = t.a[k], want = (t.c / a[1] - 1) * 100;
        if (!(a[1] > 0) || Math.abs(want - t.r[k]) > 0.011) bad.push(t.t + " " + (WIN[k] ? WIN[k].label : k) + ": shown " + t.r[k] + "%, the closes give " + want.toFixed(2) + "%");
        if (a[0] > t.d) bad.push(t.t + " " + k + ": measured from a day after its last bar");
      });
      if (STATES.indexOf(t.state) < 0) bad.push(t.t + " has no state");
      if (t.state === "BOOMING" && !isBooming(t)) bad.push(t.t + " is called booming against its own record");
      if (t.state === "DESTROYED" && !isDestroyed(t)) bad.push(t.t + " is called destroyed against its own record");
      if (t.state === "RISK-ON" && t.score < 15) bad.push(t.t + " is called rising on a score of " + t.score);
      if (t.state === "RISK-OFF" && t.score > -15) bad.push(t.t + " is called falling on a score of " + t.score);
      if (t.state === "NEUTRAL" && (t.score >= 15 || t.score <= -15)) bad.push(t.t + " is called flat on a score of " + t.score);
    });
    var want = lists(doc);
    if ((m.booming || []).slice().sort().join() !== want.booming.map(function (t) { return t.t; }).sort().join()) bad.push("the booming list is not exactly the tiles in that state");
    if ((m.destroyed || []).slice().sort().join() !== want.destroyed.map(function (t) { return t.t; }).sort().join()) bad.push("the destroyed list is not exactly the tiles in that state");
    (doc.booming || []).forEach(function (x) { if (x.state !== "BOOMING") bad.push(x.ticker + " is on the first map's booming list in the state " + x.state); });
    (doc.destroyed || []).forEach(function (x) { if (x.state !== "DESTROYED") bad.push(x.ticker + " is on the first map's destroyed list in the state " + x.state); });
    return { changes: n, tiles: m.tiles.length, bad: bad };
  }
  /* In words, why a tile is (or is not) where it is: the rule's tests with the tile's own figures. */
  function why(t) {
    var up = t.score >= 0, parts = [
      "score " + fmtScore(t.score),
      "one month " + fmtPct(t.r.m),
      "three months " + fmtPct(t.r.q),
      fmtPct(Math.abs(t.v50), 1).replace("+", "") + (t.v50 >= 0 ? " over" : " under") + " its 50-day average",
      fmtPct(Math.abs(t.v200), 1).replace("+", "") + (t.v200 >= 0 ? " over" : " under") + " its 200-day"
    ];
    if (t.state === "BOOMING") return "Booming: " + parts.join(", ") + ".";
    if (t.state === "DESTROYED") return "Destroyed: " + parts.join(", ") + ".";
    if (t.held) return "Not called " + (up ? "booming" : "destroyed") + ": it " + t.held + ".";
    return PLAIN[t.state] + ": " + parts.slice(0, 3).join(", ") + ".";
  }

  // ------------------------------------------------------------------------------------------------ live prices (funds)
  /* q is one entry of the Worker's /quotes: {dayClose, prevClose, updatedTs}. The regular session's price replaces the engine's
   * close when the quote is of the tile's day or a later one, both closes exist, and the price is no split away. The day's
   * change is then over the quote's own previous close; every other window is over the engine's anchor close, named on the
   * card. Returns {c, at, day, r} or null. */
  function live(tile, q) {
    if (!tile || tile.k !== "fund" || !q) return null;
    var c = +q.dayClose, p = +q.prevClose, at = +q.updatedTs;
    if (!(c > 0) || !(p > 0) || !(at > 0) || !(tile.c > 0)) return null;
    var day = nyDate(at);
    if (day < tile.d) return null;
    var k = c / tile.c;
    if (k < SPLIT_BAND[0] || k > SPLIT_BAND[1]) return null;
    var same = day === tile.d, base = same ? tile.p : tile.c;
    if (!(base > 0) || Math.abs(p / base - 1) > (same || !tile.f ? 0.005 : 0.08)) return null;   // its previous close is not the bar the engine holds
    var r = {};
    Object.keys(tile.a || {}).forEach(function (w) {
      if (w === "d") r.d = Math.round((c / p - 1) * 10000) / 100;
      else if (tile.a[w][1] > 0) r[w] = Math.round((c / tile.a[w][1] - 1) * 10000) / 100;
    });
    return { c: c, at: at, day: day, r: r };
  }

  // ------------------------------------------------------------------------------------------------ the card
  function roleNote(t) {
    if (t.cls === "vol") return "A fear gauge: it rises when investors are afraid. A booming fear gauge is a warning, not a rally.";
    if (t.role === "haven") return "A safe haven: money goes here when investors are afraid.";
    return "";
  }
  function cardHtml(doc, t, lv, ctx) {
    var m = doc.map, cls = (m.classes.filter(function (c) { return c.id === t.cls; })[0] || {}), grp = ((cls.groups || []).filter(function (g) { return g.id === t.grp; })[0] || {});
    var h = "<div class=rm-c-h><b>" + esc(t.t) + "</b><span>" + esc(t.n) + "</span>" + (ctx && ctx.pinned ? "<button type=button class=rm-x aria-label=Close>×</button>" : "") + "</div>";
    h += "<div class=rm-c-sub>" + esc(cls.name || "") + " · " + esc(grp.name || "") + " · " + esc(KIND[t.k] || t.k) + (t.sym && t.sym !== t.t ? " · " + esc(t.sym) : "") + "</div>";
    h += "<div class=rm-c-state><i style='background:" + fill(tone(t, "state"), "state", ctx && ctx.palette) + "'></i><b>" + esc(PLAIN[t.state]) + "</b><span>score " + fmtScore(t.score) + " of ±100</span></div>";
    h += "<div class=rm-c-why>" + esc(why(t)) + "</div>";
    var pc = function (v) { return "<td class='" + (v > 0 ? "rm-up" : v < 0 ? "rm-dn" : "") + "'>" + fmtPct(v) + "</td>"; };
    h += "<table class=rm-c-w><thead><tr><th></th><th>Change</th>" + (lv ? "<th title='The same anchor closes against the price now'>Now</th>" : "") +
      "<th>Since</th><th>Close then</th><th title='The move divided by a typical move of its own over that long'>vs usual</th></tr></thead><tbody>";
    WINDOWS.forEach(function (w) {
      var a = t.a && t.a[w.id];
      if (!a) { h += "<tr><td>" + w.label + "</td><td colspan=" + (lv ? 5 : 4) + " class=rm-dim>not enough history</td></tr>"; return; }
      h += "<tr><td>" + w.label + "</td>" + pc(t.r[w.id]) + (lv ? pc(lv.r[w.id]) : "") + "<td>" + esc(a[0]) + "</td><td>" + fmtPrice(a[1]) + "</td><td>" + fmtTimes(t.u ? t.u[w.id] : null) + "</td></tr>";
    });
    h += "</tbody></table>";
    if (lv) h += "<div class=rm-c-l><b>" + fmtPrice(lv.c) + "</b> now · " + esc(nyClock(lv.at)) + ", 15 minutes delayed</div>";
    var forming = t.f ? " · that day's bar, still forming at " + fmtClock(doc.as_of && doc.as_of.funds_forming_as_of) : "";
    h += "<div class=rm-c-l>" + (lv ? "The engine's close: " : "Close ") + "<b>" + fmtPrice(t.c) + "</b> on " + esc(t.d) + forming + "</div>";
    if (t.k === "fx") h += "<div class=rm-c-l>One " + esc(t.t) + " in dollars" + (t.inv ? " (the market quotes " + esc(t.sym) + " " + fmtPrice(1 / t.c) + ")" : "") + ". Green means the currency rose against the dollar.</div>";
    h += "<div class=rm-c-l>" + fmtPct(Math.abs(t.off_hi), 1).replace("+", "") + " under its 52-week high · " + fmtPct(Math.abs(t.off_lo), 1).replace("+", "") + " over its low" +
      (t.dv ? " · " + fmtBig(t.dv) + " traded a day" : "") + (t.mc ? " · worth " + fmtBig(t.mc) : "") + "</div>";
    var notes = [];
    if (roleNote(t)) notes.push(roleNote(t));
    if (t.late) notes.push("Its last bar is " + t.late + " day" + (t.late > 1 ? "s" : "") + " behind the newest of its kind: the warehouse has not received a later one.");
    (t.splits || []).forEach(function (s) { notes.push("Its history before " + s.date + " is rebased for a share split (" + (s.factor < 1 ? Math.round(1 / s.factor) + "-for-1" : "1-for-" + Math.round(s.factor)) + ")."); });
    if (t.jump) notes.push("A one-day move with no split on record: not called booming or destroyed.");
    notes.forEach(function (n) { h += "<div class=rm-c-n>" + esc(n) + "</div>"; });
    if (ctx && ctx.pinned) h += "<div class=rm-c-b>" + (t.chart ? "<button type=button class=rm-open data-chart='" + esc(t.chart) + "'>Open " + esc(t.t) + " in the chart</button>" : "<span class=rm-dim>No chart for this " + esc((KIND[t.k] || "").toLowerCase()) + " yet.</span>") + "</div>";
    return h;
  }

  // ------------------------------------------------------------------------------------------------ the page
  function load() { try { return JSON.parse(root.localStorage.getItem(STORE) || "{}") || {}; } catch (e) { return {}; } }
  function save(s) { try { root.localStorage.setItem(STORE, JSON.stringify({ mode: s.mode, norm: s.norm, sizing: s.sizing, palette: s.palette, live: s.live })); } catch (e) {} }

  function mount(host, doc, opt) {
    opt = opt || {};
    var d = root.document, saved = load(), m = doc.map, byT = {}, liveBy = {}, liveAt = 0, liveTimer = null, pinned = null;
    m.tiles.forEach(function (t) { byT[t.t] = t; });
    var S = {
      mode: opt.mode || saved.mode || "state", norm: !!saved.norm, sizing: saved.sizing === "equal" ? "equal" : "size",
      palette: saved.palette === "cb" ? "cb" : "rg", live: saved.live !== false, cls: opt.cls || null
    };
    if (S.mode !== "state" && !WIN[S.mode]) S.mode = "state";
    if (S.cls && !m.classes.some(function (c) { return c.id === S.cls; })) S.cls = null;
    var check = audit(doc), L = lists(doc);

    host.innerHTML =
      "<div class=rm-banner id=rm-banner></div>" +
      "<div class=rm-bar id=rm-bar></div>" +
      "<div class=rm-chips id=rm-chips></div>" +
      "<div class=rm-map id=rm-map role=img></div>" +
      "<div class=rm-legend id=rm-legend></div>" +
      "<div class=rm-below id=rm-below></div>" +
      "<div class=rm-tip id=rm-tip hidden></div><div class=rm-card id=rm-card hidden></div>";
    var $ = function (id) { return d.getElementById(id); };
    var mapEl = $("rm-map"), tip = $("rm-tip"), card = $("rm-card");

    function lv(t) { return S.live && S.mode !== "state" && !S.norm ? liveBy[t.t] || null : null; }
    function anyLive() { return Object.keys(liveBy).length; }

    // ---- the banner: what the tape is doing, and how old every figure is
    function banner() {
      var r = doc.regime || {}, x = doc.cross_asset || {}, a = doc.as_of || {}, st = { DESTROYED: 0, "RISK-OFF": 0, NEUTRAL: 0, "RISK-ON": 0, BOOMING: 0 };
      m.tiles.forEach(function (t) { st[t.state]++; });
      var dates = Object.keys(KINDS).filter(function (k) { return a[k]; }).map(function (k) {
        return "<span>" + KINDS[k] + " <b>" + esc(a[k]) + "</b>" + (k === "fund" && a.funds_forming ? " (today, still forming at " + esc(fmtClock(a.funds_forming_as_of).slice(11)) + ")" : "") + "</span>";
      }).join("");
      var h = "<div class=rm-b-two>" +
        "<div class=rm-b-box><div class=rm-b-k>Across every asset class</div><div class=rm-b-l>" + esc(x.label || "–") + "</div><div class=rm-b-s>" + esc(x.summary || "") + "</div></div>" +
        "<div class=rm-b-box><div class=rm-b-k>US stocks: broad, narrow or bifurcated</div><div class=rm-b-l>" + esc(r.label || "–") + "</div><div class=rm-b-s>" + esc(r.summary || "") +
        (doc.regime_changed && doc.turns_since ? " <b>Changed from " + esc(doc.prev_regime) + ".</b>" : "") + "</div></div></div>";
      h += "<div class=rm-b-count><b>" + m.n + "</b> instruments in " + m.classes.length + " asset classes: <b class=rm-up>" + st.BOOMING + " booming</b>, " + st["RISK-ON"] + " rising, " + st.NEUTRAL + " flat, " +
        st["RISK-OFF"] + " falling, <b class=rm-dn>" + st.DESTROYED + " getting destroyed</b>.</div>";
      h += "<div class=rm-b-dates>Last bar: " + dates + "<span>Engine run <b>" + esc(fmtClock(doc.generated_at)) + "</b></span><span id=rm-live-badge></span></div>";
      if (check.bad.length) h += "<div class=rm-warn><b>This document fails its own arithmetic in " + check.bad.length + " place" + (check.bad.length > 1 ? "s" : "") + ".</b> " + esc(check.bad.slice(0, 4).join("; ")) + "</div>";
      (doc.notes || []).forEach(function (n) { h += "<div class=rm-warn>" + esc(n) + "</div>"; });
      if (m.left_out && m.left_out.length) h += "<div class=rm-note>Left out: " + m.left_out.map(function (x) { return esc(x.t) + " (" + esc(x.why) + ")"; }).join("; ") + "</div>";
      $("rm-banner").innerHTML = h;
      liveBadge();
    }
    function liveBadge() {
      var el = $("rm-live-badge");
      if (!el) return;
      var n = anyLive();
      el.innerHTML = !S.live ? "Live prices off" : n ? "<i class=rm-dot></i>" + n + " funds live at <b>" + esc(nyClock(liveAt)) + "</b> (15 min delayed) in the 1D–1Y views" : "";
    }

    // ---- the controls
    function bar() {
      var h = "<div class=rm-seg role=group aria-label='Colour by'><span class=rm-lab>Colour</span>" +
        "<button type=button data-mode=state class='" + (S.mode === "state" ? "on" : "") + "'>Boom ↔ Destroyed</button>" +
        WINDOWS.map(function (w) { return "<button type=button data-mode=" + w.id + " class='" + (S.mode === w.id ? "on" : "") + "' title='Change over " + w.word + "'>" + w.label + "</button>"; }).join("") + "</div>";
      h += "<label class=rm-chk title='Colour each tile by how big the move is for that instrument: a currency that moves 1% has moved more than a coin that moves 3%'><input type=checkbox id=rm-norm" +
        (S.norm ? " checked" : "") + (S.mode === "state" ? " disabled" : "") + "> against its own normal</label>";
      h += "<div class=rm-seg role=group aria-label='Tile size'><span class=rm-lab>Size</span><button type=button data-size=size class='" + (S.sizing === "size" ? "on" : "") +
        "' title='Bigger tiles trade more than the others in their group'>By size</button><button type=button data-size=equal class='" + (S.sizing === "equal" ? "on" : "") + "'>Equal</button></div>";
      h += "<div class=rm-seg role=group aria-label=Colours><button type=button data-pal=rg class='" + (S.palette === "rg" ? "on" : "") + "'>Red / green</button><button type=button data-pal=cb class='" +
        (S.palette === "cb" ? "on" : "") + "' title='For red-green colour blindness'>Orange / blue</button></div>";
      h += "<label class=rm-chk title='Funds only: the regular session price from Polygon, 15 minutes delayed'><input type=checkbox id=rm-livebox" + (S.live ? " checked" : "") + "> live prices</label>";
      $("rm-bar").innerHTML = h;
      host.dataset.palette = S.palette;
      host.style.setProperty("--rm-up", PALETTES[S.palette].up);          // the colour of a rising and of a falling number, by the palette
      host.style.setProperty("--rm-dn", PALETTES[S.palette].dn);
    }
    function chips() {
      var h = "<button type=button data-cls='' class='" + (!S.cls ? "on" : "") + "'>All <em>" + m.n + "</em></button>";
      m.classes.forEach(function (c) {
        var st = (m.stats && m.stats.classes && m.stats.classes[c.id]) || { n: 0 };
        if (!st.n) return;
        h += "<button type=button data-cls=" + c.id + " class='" + (S.cls === c.id ? "on" : "") + "'><i style='background:" + fill(clamp((st.score || 0) / 60, -1, 1), "d", S.palette) + "'></i>" + esc(c.name) + " <em>" + st.n + "</em></button>";
      });
      $("rm-chips").innerHTML = h;
    }

    // ---- the map
    function draw() {
      var W = Math.floor(mapEl.clientWidth || host.clientWidth || 1000);
      var tiles = m.tiles.filter(function (t) { return !S.cls || t.cls === S.cls; });
      var wt = weights(tiles, S.sizing), weight = tiles.reduce(function (s, t) { return s + wt[t.t]; }, 0);
      var top = mapEl.getBoundingClientRect().top + (root.pageYOffset || 0);
      var H = mapHeight(W, Math.floor((root.innerHeight || 800) - Math.min(top, 330) - 54), weight);
      var lay = layout(doc, { w: W, h: H, sizing: S.sizing, cls: S.cls }), h = "";
      mapEl.style.height = H + "px";
      lay.classes.forEach(function (c) {
        var st = (m.stats && m.stats.classes && m.stats.classes[c.id]) || {};
        h += "<div class=rm-cls style='left:" + c.x.toFixed(1) + "px;top:" + c.y.toFixed(1) + "px;width:" + c.w.toFixed(1) + "px;height:" + c.h.toFixed(1) + "px'>";
        if (c.head) h += "<button type=button class=rm-ch data-cls=" + c.id + " title='" + (S.cls ? "Back to every asset class" : "Show only " + esc(c.name)) + "'><b>" + esc(c.name) + "</b><span>" +
          (st.n || 0) + " · score " + fmtScore(st.score) + " · " + (st.rising_pct == null ? "–" : st.rising_pct + "% rising") + "</span></button>";
        h += "</div>";
        c.groups.forEach(function (g) {
          if (g.head) h += "<div class=rm-gh style='left:" + (g.x + 1).toFixed(1) + "px;top:" + (g.y + 1).toFixed(1) + "px;width:" + (g.w - 2).toFixed(1) + "px'>" + esc(g.name) + "</div>";
          g.tiles.forEach(function (r) {
            var t = byT[r.t], w = r.w - 1, hh = r.h - 1, l = lv(t), lab = labelOf(t, S.mode, l), tx = tileText(w, hh, t.t, lab.length < 6 ? "000000" : lab), tn = tone(t, S.mode, S.norm, l);
            h += "<div class='rm-t" + (t.role === "haven" ? " rm-haven" : "") + (tn == null ? " rm-none" : "") + (t.state === "BOOMING" ? " rm-boom" : t.state === "DESTROYED" ? " rm-bust" : "") +
              "' data-t='" + esc(t.t) + "' style='left:" + r.x.toFixed(1) + "px;top:" + r.y.toFixed(1) + "px;width:" + w.toFixed(1) + "px;height:" + hh.toFixed(1) + "px;background:" + fill(tn, S.mode, S.palette) + "'>";
            if (tx.fs) {
              h += "<b style='font-size:" + tx.fs + "px'>" + esc(t.t) + "</b>";
              if (tx.two) h += "<span style='font-size:" + tx.vs + "px'>" + esc(lab) + "</span>";
              if (tx.three) h += "<i>" + esc(S.mode === "state" ? PLAIN[t.state] : t.n) + "</i>";
            }
            if (t.late) h += "<u title='A bar behind'></u>";
            h += "</div>";
          });
        });
      });
      mapEl.innerHTML = h;
      mapEl.setAttribute("aria-label", "Heat map of " + tiles.length + " instruments: " + L.booming.length + " booming, " + L.destroyed.length + " getting destroyed. The same figures are in the table below.");
      mapEl.dataset.tiles = String(lay.n);
      legend();
    }
    function repaint() {                         // colours and numbers only: nothing moves
      Array.prototype.forEach.call(mapEl.querySelectorAll(".rm-t"), function (el) {
        var t = byT[el.dataset.t], l = lv(t), tn = tone(t, S.mode, S.norm, l), sp = el.querySelector("span"), it = el.querySelector("i");
        el.style.background = fill(tn, S.mode, S.palette);
        el.classList.toggle("rm-none", tn == null);
        if (sp) sp.textContent = labelOf(t, S.mode, l);
        if (it) it.textContent = S.mode === "state" ? PLAIN[t.state] : t.n;
      });
      legend();
    }
    function legend() {
      var h = "<div class=rm-scale>", st = {};
      if (S.mode === "state") {
        m.tiles.forEach(function (t) { if (!S.cls || t.cls === S.cls) st[t.state] = (st[t.state] || 0) + 1; });
        [["DESTROYED", -0.9], ["RISK-OFF", -0.41], ["NEUTRAL", 0], ["RISK-ON", 0.41], ["BOOMING", 0.9]].forEach(function (x) {
          h += "<span><i style='background:" + fill(x[1], "state", S.palette) + "'></i>" + PLAIN[x[0]] + " <em>" + (st[x[0]] || 0) + "</em></span>";
        });
      } else {
        var w = WIN[S.mode];
        [-1, -2 / 3, -1 / 3, 0, 1 / 3, 2 / 3, 1].forEach(function (f) {
          var v = S.norm ? f * UNUSUAL_FULL : f * w.scale;
          h += "<span><i style='background:" + fill(f, S.mode, S.palette) + "'></i>" + (S.norm ? fmtTimes(v) : fmtPct(v, 0)) + (Math.abs(f) === 1 ? (f < 0 ? " or worse" : " or more") : "") + "</span>";
        });
      }
      h += "</div><div class=rm-keys>" +
        (S.mode === "state" ? "<span>Booming and Destroyed are not just scores: the last month and three months must both point that way, one of them by 5% or more, with the close on that side of its 50- and 200-day averages.</span>"
          : "<span>Change over " + WIN[S.mode].word + (S.norm ? ", in multiples of the instrument's own usual move over that long" : ", by the calendar") + ". The state behind each tile is on its card.</span>") +
        "<span><i class=rm-k-haven></i>dashed edge: a safe haven or a fear gauge (its rise means fear)</span>" +
        "<span><i class=rm-k-late></i>a bar behind its kind</span>" +
        "<span>Tile size: " + (S.sizing === "equal" ? "equal" : "how heavily it trades against the others in its group; currencies, futures and indexes are one size") + "</span></div>";
      $("rm-legend").innerHTML = h;
    }

    // ---- under the map: the lists, the classes, the turns, the table, the rule
    function row(t, extra) {
      return "<tr data-t='" + esc(t.t) + "'><td><b>" + esc(t.t) + "</b></td><td class=rm-nm>" + esc(t.n) + (roleNote(t) ? " <em class=rm-tag>" + (t.cls === "vol" ? "fear gauge" : "safe haven") + "</em>" : "") + "</td><td class=rm-dim>" +
        esc((m.classes.filter(function (c) { return c.id === t.cls; })[0] || {}).name || "") + "</td><td>" + fmtScore(t.score) + "</td><td class='" + (t.r.m > 0 ? "rm-up" : "rm-dn") + "'>" + fmtPct(t.r.m, 1) +
        "</td><td class='" + (t.r.q > 0 ? "rm-up" : "rm-dn") + "'>" + fmtPct(t.r.q, 1) + "</td><td>" + fmtPct(t.v50, 1) + "</td><td>" + fmtPct(t.v200, 1) + "</td>" + (extra || "") + "</tr>";
    }
    var HEAD = "<thead><tr><th></th><th></th><th>Class</th><th>Score</th><th>1M</th><th>3M</th><th title='The close against its 50-day average'>vs 50d</th><th title='The close against its 200-day average'>vs 200d</th></tr></thead>";
    function listBox(title, cls, rows, empty, id) {
      var cap = 14, h = "<section class='rm-list " + cls + "'><h3>" + title + " <em>" + rows.length + "</em></h3>";
      if (!rows.length) return h + "<p class=rm-dim>" + empty + "</p></section>";
      h += "<div class=rm-scroll><table>" + HEAD + "<tbody>" + rows.slice(0, cap).map(function (t) { return row(t); }).join("") + "</tbody>";
      if (rows.length > cap) h += "<tbody id=" + id + "-more hidden>" + rows.slice(cap).map(function (t) { return row(t); }).join("") + "</tbody>";
      h += "</table></div>";
      if (rows.length > cap) h += "<button type=button class=rm-more data-more=" + id + "-more>Show all " + rows.length + "</button>";
      return h + "</section>";
    }
    function below() {
      var h = "<div class=rm-lists>" +
        listBox("🔥 Booming", "rm-l-up", L.booming, "Nothing on the map is booming: no instrument has a score of +45 with a month and three months both up.", "rm-up") +
        listBox("💀 Getting destroyed", "rm-l-dn", L.destroyed, "Nothing on the map is being destroyed: no instrument has a score of −45 with a month and three months both down.", "rm-dn") + "</div>";
      if (L.held.length) {
        h += "<section class=rm-held><h3>Scored that far, not called it <em>" + L.held.length + "</em></h3><p class=rm-dim>Their score alone would put them on a list above. Their record does not, so they are shown one state in.</p><ul>" +
          L.held.map(function (t) { return "<li data-t='" + esc(t.t) + "'><b>" + esc(t.t) + "</b> \u00b7 " + esc(t.n) + ": " + esc(t.held) + ".</li>"; }).join("") + "</ul></section>";
      }
      // classes
      h += "<section class=rm-classes><h3>By asset class</h3><div class=rm-scroll><table><thead><tr><th>Class</th><th>#</th><th>Score</th><th>Rising</th><th class=rm-mix>Destroyed → Booming</th>" +
        WINDOWS.map(function (w) { return "<th title='Median change over " + w.word + "'>" + w.label + "</th>"; }).join("") + "<th>Strongest</th><th>Weakest</th></tr></thead><tbody>";
      m.classes.forEach(function (c) {
        var st = m.stats.classes[c.id];
        if (!st || !st.n) return;
        h += "<tr data-cls=" + c.id + "><td><b>" + esc(c.name) + "</b></td><td>" + st.n + "</td><td>" + fmtScore(st.score) + "</td><td>" + st.rising_pct + "%</td><td class=rm-mix><div class=rm-stack title='" +
          STATES.map(function (s) { return PLAIN[s] + " " + (st.states[s] || 0); }).join(", ") + "'>" +
          [["DESTROYED", -0.9], ["RISK-OFF", -0.41], ["NEUTRAL", 0], ["RISK-ON", 0.41], ["BOOMING", 0.9]].map(function (x) {
            var n = st.states[x[0]] || 0;
            return n ? "<i style='flex:" + n + ";background:" + fill(x[1], "state", S.palette) + "'>" + (n / st.n > 0.09 ? n : "") + "</i>" : "";
          }).join("") + "</div></td>" +
          WINDOWS.map(function (w) { var v = st.r ? st.r[w.id] : null; return "<td class='" + (v > 0 ? "rm-up" : v < 0 ? "rm-dn" : "") + "'>" + fmtPct(v, 1) + "</td>"; }).join("") +
          "<td>" + esc(st.best || "") + "</td><td>" + esc(st.worst || "") + "</td></tr>";
      });
      h += "</tbody></table></div><p class=rm-dim>Score: the average of the class. Rising: the share with a score above zero. 1D to 1Y: the median change of the class.</p></section>";
      // turns
      h += "<section class=rm-turns><h3>Turns</h3>";
      if (!doc.turns_since) h += "<p class=rm-dim>Turns show from the next run: this is the first publication of the whole map, and there is no earlier one to compare it with.</p>";
      else if (!(m.transitions || []).length) h += "<p class=rm-dim>Nothing changed state since the run of " + esc(fmtClock(doc.turns_since)) + ".</p>";
      else {
        h += "<p class=rm-dim>What changed state, or moved 18 points, since the run of " + esc(fmtClock(doc.turns_since)) + (m.n_transitions > m.transitions.length ? " (the largest " + m.transitions.length + " of " + m.n_transitions + ")" : "") + ".</p><div class=rm-scroll><table><tbody>" +
          m.transitions.map(function (x) {
            return "<tr data-t='" + esc(x.ticker) + "'><td><b>" + esc(x.ticker) + "</b></td><td class=rm-nm>" + esc(x.name) + "</td><td>" + esc(PLAIN[x.from] || x.from) + " → " + esc(PLAIN[x.to] || x.to) + "</td><td class='" +
              (x.delta > 0 ? "rm-up" : x.delta < 0 ? "rm-dn" : "") + "'>" + fmtScore(x.delta) + "</td><td class=rm-dim>" + esc(String(x.kind || "").toLowerCase()) + "</td></tr>";
          }).join("") + "</tbody></table></div>";
      }
      h += "</section>";
      // the whole table
      h += "<details class=rm-all><summary>Every instrument as a table <em>" + m.n + "</em></summary><div class=rm-scroll><table id=rm-table><thead><tr>" +
        [["t", "Ticker"], ["n", "Name"], ["cls", "Class"], ["state", "State"], ["score", "Score"]].concat(WINDOWS.map(function (w) { return ["r." + w.id, w.label]; })).concat([["d", "Last bar"]])
          .map(function (c) { return "<th data-sort='" + c[0] + "'>" + c[1] + "</th>"; }).join("") + "</tr></thead><tbody></tbody></table></div></details>";
      // the rule
      var ru = doc.rules || {};
      h += "<details class=rm-rules><summary>How it is measured, and where the prices come from</summary>" +
        ["windows", "score", "state", "unusual", "currencies", "left_out"].filter(function (k) { return ru[k]; }).map(function (k) { return "<p>" + esc(ru[k]) + "</p>"; }).join("") +
        (doc.cross_asset && doc.cross_asset.rule ? "<p>" + esc(doc.cross_asset.rule) + "</p>" : "") +
        "<p>Prices: " + (doc.sources || []).map(function (s) { return esc(s.what) + " – " + esc(s.note); }).join("; ") + ". All read from the JustHodl warehouse; nothing is fetched from a provider by the engine.</p>" +
        "<p>Research, not investment advice.</p></details>";
      h += "<div class=rm-check" + (check.bad.length ? " data-bad=" + check.bad.length : "") + " id=rm-check>" + (check.bad.length ? "" : "✓ ") + "Checked in your browser: " + check.changes.toLocaleString("en-US") + " changes on " + check.tiles +
        " instruments re-derived from their closes, and every booming and destroyed tile held to the rule" + (check.bad.length ? " – <b>" + check.bad.length + " do not agree</b>." : " – all agree.") + "</div>";
      $("rm-below").innerHTML = h;
      table("score", -1);
    }
    var sortKey = "score", sortDir = -1;
    function table(key, dir) {
      sortKey = key; sortDir = dir;
      var get = function (t) { return key.indexOf("r.") === 0 ? (t.r[key.slice(2)] == null ? -Infinity * dir : t.r[key.slice(2)]) : key === "state" ? STATES.indexOf(t.state) * 1000 + t.score : t[key]; };
      var rows = m.tiles.slice().sort(function (a, b) { var x = get(a), y = get(b); return (x < y ? -1 : x > y ? 1 : 0) * dir || (a.t < b.t ? -1 : 1); });
      var names = {};
      m.classes.forEach(function (c) { names[c.id] = c.name; });
      var body = $("rm-table").querySelector("tbody");
      body.innerHTML = rows.map(function (t) {
        return "<tr data-t='" + esc(t.t) + "'><td><b>" + esc(t.t) + "</b></td><td class=rm-nm>" + esc(t.n) + "</td><td class=rm-dim>" + esc(names[t.cls] || "") + "</td><td><i class=rm-sw style='background:" +
          fill(tone(t, "state"), "state", S.palette) + "'></i>" + PLAIN[t.state] + "</td><td>" + fmtScore(t.score) + "</td>" +
          WINDOWS.map(function (w) { var v = t.r[w.id]; return "<td class='" + (v > 0 ? "rm-up" : v < 0 ? "rm-dn" : "") + "'>" + fmtPct(v, 1) + "</td>"; }).join("") + "<td class=rm-dim>" + esc(t.d) + "</td></tr>";
      }).join("");
      Array.prototype.forEach.call($("rm-table").querySelectorAll("th"), function (th) { th.setAttribute("aria-sort", th.dataset.sort === key ? (dir > 0 ? "ascending" : "descending") : "none"); });
    }

    // ---- hover, pin, open
    function place(el, x, y) {
      var w = el.offsetWidth, h = el.offsetHeight, vw = root.innerWidth, vh = root.innerHeight;
      el.style.left = Math.max(6, Math.min(vw - w - 6, x + 14)) + "px";
      el.style.top = Math.max(6, y + 16 + h > vh ? y - h - 12 : y + 16) + "px";
    }
    function showTip(t, ev) {
      if (pinned) return;
      tip.innerHTML = cardHtml(doc, t, S.live ? liveBy[t.t] || null : null, { palette: S.palette });
      tip.hidden = false;
      place(tip, ev.clientX, ev.clientY);
    }
    function pin(t) {
      pinned = t.t;
      tip.hidden = true;
      card.innerHTML = cardHtml(doc, t, S.live ? liveBy[t.t] || null : null, { pinned: true, palette: S.palette });
      card.hidden = false;
      card.dataset.t = t.t;
      Array.prototype.forEach.call(mapEl.querySelectorAll(".rm-t.rm-sel"), function (el) { el.classList.remove("rm-sel"); });
      var el = mapEl.querySelector(".rm-t[data-t='" + t.t.replace(/'/g, "") + "']");
      if (el) {
        el.classList.add("rm-sel");
        var b = el.getBoundingClientRect();
        card.classList.toggle("rm-left", b.left + b.width / 2 > (root.innerWidth || 0) / 2);
      }
    }
    function unpin() {
      pinned = null; card.hidden = true;
      Array.prototype.forEach.call(mapEl.querySelectorAll(".rm-t.rm-sel"), function (el) { el.classList.remove("rm-sel"); });
    }
    function openChart(sym) {
      try {
        if (root.parent && root.parent !== root && typeof root.parent.jhOpenSymbol === "function") { root.parent.jhOpenSymbol(sym); return; }
      } catch (e) {}
      root.location.href = "/chart.html?s=" + encodeURIComponent(sym);
    }
    mapEl.addEventListener("mousemove", function (ev) {
      var el = ev.target.closest ? ev.target.closest(".rm-t") : null;
      if (!el) { tip.hidden = true; return; }
      showTip(byT[el.dataset.t], ev);
    });
    mapEl.addEventListener("mouseleave", function () { tip.hidden = true; });
    host.addEventListener("click", function (ev) {
      var q = function (sel) { return ev.target.closest ? ev.target.closest(sel) : null; }, el;
      if ((el = q(".rm-x"))) { unpin(); return; }
      if ((el = q(".rm-open"))) { openChart(el.dataset.chart); return; }
      if ((el = q(".rm-seg [data-mode]"))) { S.mode = el.dataset.mode; save(S); bar(); repaint(); liveBadge(); return; }
      if ((el = q(".rm-seg [data-size]"))) { S.sizing = el.dataset.size; save(S); bar(); unpin(); draw(); return; }
      if ((el = q(".rm-seg [data-pal]"))) { S.palette = el.dataset.pal; save(S); bar(); chips(); draw(); below(); return; }
      if ((el = q(".rm-more[data-more]"))) { var more = d.getElementById(el.dataset.more); if (more) { more.hidden = false; el.remove(); } return; }
      if ((el = q("th[data-sort]"))) { table(el.dataset.sort, el.dataset.sort === sortKey ? -sortDir : (el.dataset.sort === "t" || el.dataset.sort === "n" || el.dataset.sort === "cls" ? 1 : -1)); return; }
      if ((el = q(".rm-chips [data-cls], .rm-ch[data-cls], tr[data-cls]"))) {
        var id = el.dataset.cls || null;
        S.cls = el.classList.contains("rm-ch") && S.cls ? null : (S.cls === id ? null : id);
        unpin(); chips(); draw();
        if (el.tagName === "TR") mapEl.scrollIntoView({ block: "start", behavior: "smooth" });
        return;
      }
      if ((el = q(".rm-t, tr[data-t], li[data-t]"))) { var t = byT[el.dataset.t]; if (t) pin(t); return; }
      if (pinned && !q(".rm-card")) unpin();
    });
    host.addEventListener("change", function (ev) {
      if (ev.target.id === "rm-norm") { S.norm = ev.target.checked; save(S); repaint(); liveBadge(); }
      if (ev.target.id === "rm-livebox") { S.live = ev.target.checked; save(S); if (S.live) pull(); repaint(); liveBadge(); }
    });
    d.addEventListener("keydown", function (ev) { if (ev.key === "Escape") { unpin(); tip.hidden = true; } });
    var rt = null, lastW = 0;
    root.addEventListener("resize", function () {
      clearTimeout(rt);
      rt = setTimeout(function () { var w = mapEl.clientWidth; if (w && w !== lastW) { lastW = w; draw(); } }, 120);
    });

    // ---- live prices for the funds
    function pull() {
      if (!S.live || opt.live === false || !root.fetch) return Promise.resolve(0);
      var funds = m.tiles.filter(function (t) { return t.k === "fund"; }).map(function (t) { return t.t; }), jobs = [], i;
      for (i = 0; i < funds.length; i += 50) {
        jobs.push(root.fetch((opt.proxy || PROXY) + "/quotes?tickers=" + encodeURIComponent(funds.slice(i, i + 50).join(",")))
          .then(function (r) { return r.ok ? r.json() : {}; }).catch(function () { return {}; }));
      }
      return Promise.all(jobs).then(function (parts) {
        var next = {}, at = 0;
        parts.forEach(function (p) {
          var map = (p && p.tickers) || {};
          Object.keys(map).forEach(function (k) {
            var l = byT[k] ? live(byT[k], map[k]) : null;
            if (l) { next[k] = l; if (l.at > at) at = l.at; }
          });
        });
        liveBy = next; liveAt = at;
        host.dataset.live = String(Object.keys(next).length);
        if (S.mode !== "state" && !S.norm) repaint();
        liveBadge();
        return Object.keys(next).length;
      });
    }

    banner(); bar(); chips(); draw(); below();
    lastW = mapEl.clientWidth;
    host.dataset.ready = "1";
    host.dataset.tiles = String(m.tiles.length);
    var first = pull();
    liveTimer = root.setInterval(function () { if (d.visibilityState !== "hidden") pull(); }, 90000);
    return {
      state: S, audit: check, lists: L, pull: pull, ready: first,
      set: function (k, v) { S[k] = v; bar(); chips(); draw(); },
      liveOf: function (t) { return liveBy[t] || null; },
      stop: function () { root.clearInterval(liveTimer); }
    };
  }

  return {
    WINDOWS: WINDOWS, STATES: STATES, PLAIN: PLAIN, PALETTES: PALETTES, UNUSUAL_FULL: UNUSUAL_FULL, SPLIT_BAND: SPLIT_BAND,
    clamp: clamp, median: median, mix: mix, luminance: luminance, fmtPct: fmtPct, fmtScore: fmtScore, fmtPrice: fmtPrice, fmtBig: fmtBig, fmtTimes: fmtTimes, nyDate: nyDate,
    tone: tone, fill: fill, valueOf: valueOf, labelOf: labelOf, weights: weights, squarify: squarify, layout: layout, mapHeight: mapHeight, tileText: tileText,
    lists: lists, isBooming: isBooming, isDestroyed: isDestroyed, audit: audit, why: why, live: live, cardHtml: cardHtml, mount: mount
  };
});
