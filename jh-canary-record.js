/* jh-canary-record.js - "What the record says" on canaries.html.   jh-reskin-skip
 *
 * Reads two documents and computes nothing of its own:
 *   data/canary-warroom.json   key `calibrated`  - today's level and three-month turn on the record's scale
 *   data/canary-record.json    (justhodl-warroom-weights) - every canary rebuilt through history, the barometer month by
 *                              month, and what the S&P 500 did after each reading
 * Every colour that carries data lives here (the site build re-skins a page's own CSS).
 * Khalid 2026-11-27: "improve the engine ... test it against historical data to calibrate it".
 */
(function (root, factory) {
  var api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.JHCanaryRecord = api;
})(typeof window !== "undefined" ? window : null, function () {
  "use strict";
  var C = { ink: "#e6edf3", dim: "#8b98a9", line: "#1e2836", card: "#121822", card2: "#0f141d", gold: "#e8c26a", up: "#e8730c", hot: "#f85149", calm: "#2ea043", watch: "#2f81f7", elev: "#d29922", blue: "#5b8dd6" };
  var LEVEL_COL = { CALM: C.calm, GUARDED: C.watch, ELEVATED: C.elev, "HIGH RISK": C.up, CRISIS: C.hot, "NO DATA": C.dim };
  var TURN_COL = { EASING: C.calm, STEADY: C.watch, RISING: C.elev, "RISING FAST": C.hot, "NO DATA": C.dim };
  var VERDICT = { "leads": ["Leads", C.calm, "its stress ranked the months before a fall better than chance, beyond its error"],
    "no edge shown": ["No edge shown", C.dim, "its record does not separate the months before a fall from the rest"],
    "wrong way": ["Wrong way", C.hot, "its stress ran LOWER before falls, beyond its error: it peaks after the damage"],
    "stuck": ["Stuck", C.elev, "it fired in over 90% or under 2% of months: a level on a trend, not a signal"],
    "short record": ["Short record", C.watch, "under ten years of months or under three falls: not judged"],
    "no record": ["No record", "#5c6675", "could not be rebuilt through history"] };
  var VERDICT_ORDER = ["leads", "no edge shown", "wrong way", "stuck", "short record", "no record"];
  var MECH = { macro_grid: "Macro grid", plumbing: "Plumbing", ciss: "CISS (ECB)" };
  var COLS = [
    ["name", "Canary", "text"], ["mechanism", "Mechanism", "mech"], ["verdict", "Verdict", "verdict"], ["months", "Months", "int"], ["from", "Since", "day"],
    ["fired_share", "Fires", "share"], ["early_fall_after_firing", "Fall after firing", "share"], ["early_fall_after_quiet", "Fall when quiet", "share"],
    ["auc_early", "Early score", "score"], ["auc_early_se", "± error", "num3"], ["auc", "Score, all months", "num3"],
    ["warned_before_high", "Warned before the high", "warned"], ["lead_months", "Lead, months", "num1"], ["weight", "Weight", "num2"],
    ["live_stress", "Stress now", "num1"], ["rule", "Its rule", "text"]];
  var TIPS = { fired_share: "share of its months at or over its firing line", early_fall_after_firing: "months near the market's high in which it was firing: share followed by a 10% fall inside six months",
    early_fall_after_quiet: "the same, in months it was not firing", auc_early: "0.5 is chance; 1 would rank every month before a fall above every other month. Counted on months with the S&P 500 within 5% of its one-year high",
    auc_early_se: "leave one calendar year out", auc: "the same score on every month, near a high or not", warned_before_high: "falls of 10% or more inside its record: at how many it was firing at a month-end in the six months before the high",
    lead_months: "when it warned: how many month-ends before the high, on average", weight: "what its record earns it in the weighted level: 1 is neutral", live_stress: "the engine's own reading at the record's cut" };

  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function share(v, digits) { return v == null ? "—" : (v * 100).toFixed(digits == null ? 0 : digits) + "%"; }
  function num(v, d) { return v == null || isNaN(v) ? "—" : Number(v).toFixed(d); }
  function signed(v, d) { return v == null ? "—" : (v > 0 ? "+" : v < 0 ? "−" : "") + Math.abs(v).toFixed(d == null ? 1 : d); }
  function day(v) { if (!v) return "—"; var s = String(v); return s.slice(0, 4) + "-" + s.slice(4, 6) + (s.length > 6 ? "-" + s.slice(6, 8) : ""); }
  function monthName(v) { var m = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]; var s = String(v); return m[+s.slice(4, 6) - 1] + " " + s.slice(0, 4); }
  function dayName(v) { var s = String(v); return monthName(v).split(" ")[0] + " " + (+s.slice(6, 8)) + ", " + s.slice(0, 4); }
  function ord(th) { var n = Math.round(th), s = ["th", "st", "nd", "rd"], v = n % 100; return n + (s[(v - 20) % 10] || s[v] || s[0]); }
  function count(n) { return n == null ? "—" : Number(n).toLocaleString("en-US"); }

  function cellText(row, col) {
    var v = row[col[0]];
    switch (col[2]) {
      case "mech": return MECH[v] || v || "—";
      case "verdict": return (VERDICT[v] || [v])[0];
      case "int": return v ? count(v) : "—";
      case "day": return v ? monthName(v) : "—";
      case "share": return share(v);
      case "score": return num(v, 3);
      case "num3": return num(v, 3);
      case "num2": return num(v, 2);
      case "num1": return num(v, 1);
      case "warned": return row.falls_covered ? (row.warned_before_high + " of " + row.falls_covered) : "—";
      default: return v == null ? "—" : String(v);
    }
  }
  function sortKey(row, col) {
    if (col[2] === "verdict") return VERDICT_ORDER.indexOf(row.verdict);
    if (col[2] === "warned") return row.falls_covered ? row.warned_before_high / row.falls_covered : null;
    var v = row[col[0]];
    return (col[2] === "text" || col[2] === "mech") ? String(v == null ? "" : v).toLowerCase() : v;
  }
  /* rows in a chosen order; blanks always last, whichever way */
  function sorted(rows, key, dir) {
    var col = COLS.filter(function (c) { return c[0] === key; })[0] || COLS[2];
    return rows.map(function (r, i) { return [sortKey(r, col), i, r]; }).sort(function (a, b) {
      var x = a[0], y = b[0], ex = x == null || x === "" || x === -1, ey = y == null || y === "" || y === -1;
      if (ex || ey) return ex === ey ? a[1] - b[1] : ex ? 1 : -1;
      if (x < y) return dir === "asc" ? -1 : 1;
      if (x > y) return dir === "asc" ? 1 : -1;
      return a[1] - b[1];
    }).map(function (t) { return t[2]; });
  }
  function filtered(rows, st) {
    var q = (st.q || "").toLowerCase();
    return rows.filter(function (r) {
      return (!st.verdict || r.verdict === st.verdict) && (!st.mech || r.mechanism === st.mech) && (!q || (r.name + " " + r.rule + " " + r.id).toLowerCase().indexOf(q) >= 0);
    });
  }
  function defaultOrder(rows) {
    return rows.slice().sort(function (a, b) {
      var d = VERDICT_ORDER.indexOf(a.verdict) - VERDICT_ORDER.indexOf(b.verdict);
      return d || ((b.auc_early == null ? -1 : b.auc_early) - (a.auc_early == null ? -1 : a.auc_early));
    });
  }
  function csv(rows) {
    var q = function (v) { v = v == null ? "" : String(v); return /[",\r\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; };
    var head = ["id"].concat(COLS.map(function (c) { return c[1]; }), ["falls in its record", "why no record", "flag"]);
    var out = [head.map(q).join(",")];
    rows.forEach(function (r) {
      out.push([r.id].concat(COLS.map(function (c) { var v = r[c[0]]; return c[2] === "mech" || c[2] === "verdict" || c[2] === "text" || c[2] === "day" ? (c[2] === "day" ? day(v) : v) : v; }), [r.falls_covered, r.why_not, r.flag]).map(q).join(","));
    });
    return out.join("\r\n") + "\r\n";
  }

  /* ── pieces ─────────────────────────────────────────────────────────────────────────────────────────────────── */
  function bandBar(bands, pct, cols) {
    var segs = bands.map(function (b, i) {
      var to = i + 1 < bands.length ? bands[i + 1].from_percentile : 100;
      return '<div title="' + esc(b.band) + ": " + b.from_percentile + "th to " + to + 'th percentile" style="flex:' + (to - b.from_percentile) + ";height:12px;background:" + cols[b.band] + ';opacity:.85"></div>';
    }).join('<div style="width:2px"></div>');
    var mark = pct == null ? "" : '<div class="cr-mark" style="position:absolute;top:-5px;left:' + Math.max(0.5, Math.min(99.5, pct)) + '%;transform:translateX(-50%);width:3px;height:22px;background:#fff;border-radius:2px;box-shadow:0 0 0 2px #0a0e14"></div>';
    return '<div style="position:relative;margin:12px 0 4px"><div style="display:flex;border-radius:6px;overflow:hidden">' + segs + "</div>" + mark + "</div>" +
      '<div style="display:flex;font-size:10px;color:' + C.dim + '">' + bands.map(function (b, i) { var to = i + 1 < bands.length ? bands[i + 1].from_percentile : 100; return '<span style="flex:' + (to - b.from_percentile) + ';overflow:hidden;white-space:nowrap;text-overflow:ellipsis">' + esc(b.band) + "</span>"; }).join("") + "</div>";
  }
  function after(a, what) {
    if (!a || !a.months) return '<p class="cr-p">No month in the record sat in this band.</p>';
    return '<p class="cr-p">After readings in this band the S&amp;P 500 ' + what + " in <b>" + a.fell + " of " + count(a.months) + " months (" + share(a.fall_share) + ")</b>" +
      (a.early_months ? "; with the market still near its high, in <b>" + a.early_fell + " of " + a.early_months + " (" + share(a.early_fall_share) + ")</b>" : "") + ".</p>";
  }
  function gauges(cal, rec) {
    if (!cal) return '<div class="cr-block"><h3>Today on the record’s scale</h3><p class="cr-p">The record has not been published yet; the board below is as it was.</p></div>';
    var L = cal.level || {}, T = cal.turn, R = cal.record || {}, W = cal.what_is_counted || {}, m = (rec && rec.method) || {};
    var lb = m.level_bands || [], tb = m.turn_bands || [];
    var base = "In all months of the record: " + share(W.fall_share) + "; near a high: " + share(W.early_fall_share) + ".";
    var lv = '<div class="cr-card" id="cr-level"><div class="cr-k">Stress level</div>' +
      '<div class="cr-big" style="color:' + LEVEL_COL[L.band] + '">' + (L.percentile == null ? "—" : ord(L.percentile)) + '<span class="cr-unit"> percentile</span></div>' +
      '<div class="cr-sub"><span class="cr-chip" style="color:' + LEVEL_COL[L.band] + ";border-color:" + LEVEL_COL[L.band] + '">' + esc(L.band) + "</span> score " + num(L.score, 1) + " · " + count(cal.canaries) + " tested canaries, each mechanism one vote · against its own history since " + (R.level_from ? String(R.level_from).slice(0, 4) : "—") + "</div>" +
      bandBar(lb, L.percentile, LEVEL_COL) + after(L.after, "fell 10% or more inside six months") + '<p class="cr-p cr-dim">' + base + "</p>" +
      '<p class="cr-verdict"><b>The level confirms; it has not warned.</b> Early score ' + num(R.level_score_early, 2) + " ± " + num(R.level_score_early_error, 2) + " (0.5 is chance); on every month " + num(R.level_score_all, 2) + ".</p></div>";
    var tv;
    if (!T) tv = '<div class="cr-card" id="cr-turn"><div class="cr-k">The turn, three months</div><p class="cr-p">No reading three months back to set today against.</p></div>';
    else {
      var chips = Object.keys(T.by_mechanism || {}).map(function (k) {
        var w = (T.weights || {})[k];
        return '<span class="cr-chip" title="weight ' + (w == null ? "not judged" : num(w, 3)) + '" style="border-color:' + C.line + ";color:" + (w ? C.ink : C.dim) + '">' + esc(MECH[k] || k) + " " + signed(T.by_mechanism[k]) + (w ? "" : " · no weight") + "</span>";
      }).join(" ");
      tv = '<div class="cr-card" id="cr-turn"><div class="cr-k">The turn, three months</div>' +
        '<div class="cr-big" style="color:' + TURN_COL[T.band] + '">' + signed(T.change) + '<span class="cr-unit"> points</span></div>' +
        '<div class="cr-sub"><span class="cr-chip" style="color:' + TURN_COL[T.band] + ";border-color:" + TURN_COL[T.band] + '">' + esc(T.band) + "</span> " + (T.percentile == null ? "" : ord(T.percentile) + " percentile of its own history · ") + "against " + dayName(T.against_day) + " (" + T.days_back + " days back, " + esc(T.against_from) + ")</div>" +
        bandBar(tb, T.percentile, TURN_COL) + after(T.after, "fell 10% or more inside six months") + '<p class="cr-p cr-dim">' + base + "</p>" +
        '<div class="cr-chips">' + chips + "</div>" +
        '<p class="cr-verdict"><b>The turn has tilted the odds ahead of falls.</b> Early score ' + num(R.turn_score_early, 2) + " ± " + num(R.turn_score_early_error, 2) + " over " + count(R.turn_months) + " months since " + (R.turn_from ? String(R.turn_from).slice(0, 4) : "—") + "; a tilt, not a signal.</p></div>";
    }
    return '<div class="cr-block" id="cr-now"><h3>Today on the record’s scale</h3><div class="cr-two">' + lv + tv + "</div></div>";
  }
  function followTable(title, rows, cols, today, id) {
    var maxv = Math.max.apply(null, rows.map(function (r) { return Math.max(r.fall_share || 0, r.early_fall_share || 0); }).concat([0.01]));
    var body = rows.map(function (r) {
      var on = today && r.band === today;
      var bar = function (v) { return v == null ? "" : '<span style="display:inline-block;height:8px;width:' + Math.round(64 * v / maxv) + "px;background:" + cols[r.band] + ';border-radius:2px;vertical-align:middle;margin-left:6px"></span>'; };
      return "<tr" + (on ? ' class="cr-on"' : "") + '><td><span class="cr-dot" style="background:' + cols[r.band] + '"></span>' + esc(r.band) + (on ? ' <span class="cr-today">today</span>' : "") + '</td><td class="rt">' + r.from_pct + "–" + r.to_pct + '</td><td class="rt">' + count(r.months) + '</td><td class="rt">' + r.fell + '</td><td class="rt"><b>' + share(r.fall_share) + "</b>" + bar(r.fall_share) + '</td><td class="rt">' + share(r.deep_share) + '</td><td class="rt">' + r.early_months + '</td><td class="rt">' + r.early_fell + '</td><td class="rt"><b>' + share(r.early_fall_share) + "</b>" + bar(r.early_fall_share) + '</td><td class="rt">' + (r.median_ret6 == null ? "—" : signed(r.median_ret6 * 100) + "%") + "</td></tr>";
    }).join("");
    return '<div class="cr-card" id="' + id + '"><div class="cr-k">' + title + '</div><div class="cr-scroll"><table class="cr-t" data-jh-nosort><thead><tr><th>Band</th><th class="rt">Percentile</th><th class="rt">Months</th><th class="rt">Fell 10%</th><th class="rt">Share</th><th class="rt" title="20% or more inside twelve months">Fell 20%</th><th class="rt" title="months with the S&amp;P 500 within 5% of its one-year high">Near a high</th><th class="rt">Fell 10%</th><th class="rt">Share</th><th class="rt" title="the S&amp;P 500 six months on, middle month">Six months on</th></tr></thead><tbody>' + body + "</tbody></table></div></div>";
  }
  function crossTable(cells, cell) {
    var levels = ["low", "middle", "high"], turns = ["easing", "steady", "rising"], maxv = 0.5;
    var by = {};
    (cells || []).forEach(function (c) { by[c.level + "|" + c.turn] = c; });
    var tone = function (v) { if (v == null) return C.card2; var a = Math.min(1, v / maxv); return "rgba(248,81,73," + (0.08 + 0.55 * a).toFixed(2) + ")"; };
    var head = "<tr><th>Level \\ turn</th>" + turns.map(function (t) { return '<th class="rt">' + t + "</th>"; }).join("") + "</tr>";
    var body = levels.map(function (l) {
      return "<tr><td><b>" + l + "</b></td>" + turns.map(function (t) {
        var c = by[l + "|" + t] || {}, on = cell && cell.level === l && cell.turn === t;
        return '<td class="rt" style="background:' + tone(c.fall_share) + (on ? ";outline:2px solid #fff;outline-offset:-2px" : "") + '" title="near a high: ' + (c.early_fell == null ? "—" : c.early_fell + " of " + c.early_months + " (" + share(c.early_fall_share) + ")") + '"><b>' + share(c.fall_share) + '</b><div class="cr-dim" style="font-size:11px">' + (c.fell == null ? "" : c.fell + " of " + c.months) + (on ? " · today" : "") + "</div></td>";
      }).join("") + "</tr>";
    }).join("");
    return '<div class="cr-card" id="cr-cross"><div class="cr-k">Level against turn</div><p class="cr-p cr-dim">Share of months followed by a 10% fall inside six months. Level: below the 40th, 40th–70th, above the 70th percentile. Turn: below the 30th, 30th–70th, above the 70th. Hover a cell for the months near a high.</p><div class="cr-scroll"><table class="cr-t" data-jh-nosort><thead>' + head + "</thead><tbody>" + body + "</tbody></table></div></div>";
  }
  function chart(rec, w) {
    var months = ((rec.barometer || {}).months || []).filter(function (m) { return m.per_mechanism != null; });
    if (months.length < 24) return "";
    var W = w || 1100, L = 44, R = 12, H1 = 210, H2 = 120, G = 26, T = 14, B = 26, pw = W - L - R, H = T + H1 + G + H2 + B;
    var o0 = dord(months[0].day), o1 = dord(months[months.length - 1].day);
    var x = function (d) { return L + pw * (dord(d) - o0) / (o1 - o0); };
    var y1 = function (v) { return T + H1 * (1 - (v - 20) / 60); };
    var tv = months.map(function (m) { return m.turn; }).filter(function (v) { return v != null; });
    var tmax = Math.max(8, Math.ceil(Math.max.apply(null, tv.map(Math.abs))));
    var y2 = function (v) { return T + H1 + G + H2 * (1 - (v + tmax) / (2 * tmax)); };
    var path = function (key, yf) { var d = "", pen = false; months.forEach(function (m) { var v = m[key]; if (v == null) { pen = false; return; } d += (pen ? "L" : "M") + x(m.day).toFixed(1) + " " + yf(Math.max(key === "turn" ? -tmax : 20, Math.min(key === "turn" ? tmax : 80, v))).toFixed(1) + " "; pen = true; }); return d; };
    var falls = ((rec.market || {}).falls || []).filter(function (f) { return f.low >= months[0].day; }).map(function (f) {
      var a = x(Math.max(f.peak, months[0].day)), b = x(f.low);
      return '<rect x="' + a.toFixed(1) + '" y="' + T + '" width="' + Math.max(1.5, b - a).toFixed(1) + '" height="' + (H1 + G + H2) + '" fill="' + C.hot + '" opacity="' + (f.depth <= -0.2 ? 0.2 : 0.1) + '"><title>S&amp;P 500 ' + (f.depth * 100).toFixed(1) + "% from its high of " + day(f.peak) + " to " + day(f.low) + "</title></rect>";
    }).join("");
    var grid = [30, 40, 50, 60, 70].map(function (g) { return '<line x1="' + L + '" x2="' + (W - R) + '" y1="' + y1(g) + '" y2="' + y1(g) + '" stroke="' + C.line + '"/><text x="' + (L - 6) + '" y="' + (y1(g) + 3) + '" text-anchor="end" font-size="10" fill="' + C.dim + '">' + g + "</text>"; }).join("");
    var grid2 = [-tmax, 0, tmax].map(function (g) { return '<line x1="' + L + '" x2="' + (W - R) + '" y1="' + y2(g) + '" y2="' + y2(g) + '" stroke="' + (g === 0 ? C.dim : C.line) + '"/><text x="' + (L - 6) + '" y="' + (y2(g) + 3) + '" text-anchor="end" font-size="10" fill="' + C.dim + '">' + signed(g, 0) + "</text>"; }).join("");
    var years = "";
    for (var yr = Math.ceil(months[0].day / 10000 / 5) * 5; yr * 10000 <= months[months.length - 1].day; yr += 5) years += '<text x="' + x(yr * 10000 + 101).toFixed(1) + '" y="' + (H - 8) + '" text-anchor="middle" font-size="10" fill="' + C.dim + '">' + yr + "</text>";
    return '<svg id="cr-svg" viewBox="0 0 ' + W + " " + H + '" width="100%" role="img" aria-label="The barometer rebuilt month by month with every fall of the S&amp;P 500 of 10% or more shaded" style="display:block;touch-action:none">' +
      falls + grid + grid2 + years +
      '<text x="' + L + '" y="' + (T - 3) + '" font-size="10.5" fill="' + C.dim + '">Level: mean stress of the tested canaries, each mechanism one vote</text>' +
      '<text x="' + L + '" y="' + (T + H1 + G - 5) + '" font-size="10.5" fill="' + C.dim + '">Turn: change over three months, mechanisms weighted by their own record</text>' +
      '<path d="' + path("per_mechanism", y1) + '" fill="none" stroke="' + C.gold + '" stroke-width="1.8"/>' +
      '<path d="' + path("turn", y2) + '" fill="none" stroke="' + C.blue + '" stroke-width="1.6"/>' +
      '<line id="cr-cross-x" x1="0" x2="0" y1="' + T + '" y2="' + (T + H1 + G + H2) + '" stroke="#fff" stroke-width="1" opacity="0"/></svg>';
  }
  function dord(d) { d = +d; var y = Math.floor(d / 10000), m = Math.floor(d / 100) % 100, dd = d % 100; return y * 372 + (m - 1) * 31 + (dd - 1); }
  function nearest(months, frac) {
    var o0 = dord(months[0].day), o1 = dord(months[months.length - 1].day), want = o0 + frac * (o1 - o0), best = months[0], gap = Infinity;
    months.forEach(function (m) { var g = Math.abs(dord(m.day) - want); if (g < gap) { gap = g; best = m; } });
    return best;
  }
  function readout(m) {
    if (!m) return "";
    return "<b>" + monthName(m.day) + "</b> · level " + num(m.per_mechanism, 1) + (m.pct_per_mechanism == null ? "" : " (" + ord(m.pct_per_mechanism) + " pct)") + " · turn " + signed(m.turn) + (m.pct_turn == null ? "" : " (" + ord(m.pct_turn) + " pct)") + " · " + m.n + " canaries";
  }
  function mechTable(rec) {
    var rows = ((rec.barometer || {}).mechanisms || []).map(function (m) {
      return "<tr><td><b>" + esc(MECH[m.mechanism] || m.mechanism) + '</b></td><td class="rt">' + m.canaries + '</td><td class="rt">' + num(m.level.auc_early, 3) + " ± " + num(m.level.auc_early_se, 3) + '</td><td class="rt"><b>' + num(m.turn.auc_early, 3) + "</b> ± " + num(m.turn.auc_early_se, 3) + '</td><td class="rt">' + count(m.turn.months) + '</td><td class="rt">' + (m.turn_weight_now == null ? "not judged" : num(m.turn_weight_now, 3)) + "</td></tr>";
    }).join("");
    var fam = Object.keys((rec.barometer || {}).turns || {}).sort(function (a, b) { return a - b; }).map(function (k) {
      var t = rec.barometer.turns[k];
      return "<tr><td>" + k + (k === "1" ? " month" : " months") + '</td><td class="rt"><b>' + num(t.earned.auc_early, 3) + "</b> ± " + num(t.earned.auc_early_se, 3) + '</td><td class="rt">' + num(t.earned.auc, 3) + '</td><td class="rt">' + num(t.equal.auc_early, 3) + '</td><td class="rt">' + count(t.earned.months) + "</td></tr>";
    }).join("");
    var per = (((rec.barometer || {}).turns || {})["3"] || {}).by_period || [];
    var pr = per.map(function (p) { return "<tr><td>" + monthName(p.from) + " – " + monthName(p.to) + '</td><td class="rt"><b>' + num(p.auc_early, 3) + "</b> ± " + num(p.auc_early_se, 3) + '</td><td class="rt">' + num(p.auc, 3) + '</td><td class="rt">' + count(p.months) + "</td></tr>"; }).join("");
    return '<div class="cr-three"><div class="cr-card" id="cr-mech"><div class="cr-k">Each mechanism’s own record</div><div class="cr-scroll"><table class="cr-t" data-jh-nosort><thead><tr><th>Mechanism</th><th class="rt">Canaries tested</th><th class="rt">Level, early score</th><th class="rt">Turn, early score</th><th class="rt">Months</th><th class="rt">Weight in the turn</th></tr></thead><tbody>' + rows + "</tbody></table></div></div>" +
      '<div class="cr-card" id="cr-family"><div class="cr-k">Every span the turn was tried on</div><div class="cr-scroll"><table class="cr-t" data-jh-nosort><thead><tr><th>Span</th><th class="rt">Early score</th><th class="rt">All months</th><th class="rt" title="every mechanism equal instead of weighted">Unweighted</th><th class="rt">Months</th></tr></thead><tbody>' + fam + "</tbody></table></div></div>" +
      '<div class="cr-card" id="cr-period"><div class="cr-k">The three-month turn, by period</div><div class="cr-scroll"><table class="cr-t" data-jh-nosort><thead><tr><th>Period</th><th class="rt">Early score</th><th class="rt">All months</th><th class="rt">Months</th></tr></thead><tbody>' + pr + "</tbody></table></div></div></div>";
  }
  function tableHtml(rec, st) {
    var all = rec.canaries || [], rows = filtered(all, st);
    rows = st.key ? sorted(rows, st.key, st.dir) : defaultOrder(rows);
    var counts = rec.verdicts || {};
    var chips = '<button class="cr-f' + (!st.verdict ? " on" : "") + '" data-cr-verdict="">All ' + all.length + "</button>" + VERDICT_ORDER.filter(function (v) { return counts[v]; }).map(function (v) {
      return '<button class="cr-f' + (st.verdict === v ? " on" : "") + '" data-cr-verdict="' + v + '" title="' + esc(VERDICT[v][2]) + '"><span class="cr-dot" style="background:' + VERDICT[v][1] + '"></span>' + VERDICT[v][0] + " " + counts[v] + "</button>";
    }).join("");
    var mechs = '<button class="cr-f' + (!st.mech ? " on" : "") + '" data-cr-mech="">Every mechanism</button>' + Object.keys(MECH).map(function (k) { return '<button class="cr-f' + (st.mech === k ? " on" : "") + '" data-cr-mech="' + k + '">' + MECH[k] + " " + all.filter(function (r) { return r.mechanism === k; }).length + "</button>"; }).join("");
    var head = COLS.map(function (c) {
      var on = st.key === c[0];
      return '<th data-cr-sort="' + c[0] + '"' + (c[2] !== "text" && c[2] !== "mech" && c[2] !== "verdict" ? ' class="rt"' : "") + (TIPS[c[0]] ? ' title="' + esc(TIPS[c[0]]) + '"' : "") + ' aria-sort="' + (on ? (st.dir === "asc" ? "ascending" : "descending") : "none") + '">' + c[1] + (on ? (st.dir === "asc" ? " ▲" : " ▼") : "") + "</th>";
    }).join("");
    var body = rows.map(function (r) {
      return "<tr>" + COLS.map(function (c) {
        var txt = esc(cellText(r, c));
        if (c[0] === "name") return '<td class="cr-name"><b>' + txt + "</b>" + (r.flag ? '<div class="cr-flag">' + esc(r.flag) + "</div>" : "") + (r.why_not ? '<div class="cr-why">' + esc(r.why_not) + "</div>" : "") + (r.votes_live === false ? '<div class="cr-why">tested, but it does not vote on the board (another row carries it)</div>' : "") + "</td>";
        if (c[2] === "verdict") return '<td><span class="cr-chip" title="' + esc((VERDICT[r.verdict] || ["", "", ""])[2]) + '" style="color:' + (VERDICT[r.verdict] || ["", C.dim])[1] + ";border-color:" + (VERDICT[r.verdict] || ["", C.dim])[1] + '">' + txt + "</span></td>";
        if (c[0] === "rule") return '<td class="cr-rule">' + txt + (r.source ? ' <span class="cr-dim">· ' + esc(r.source) + "</span>" : "") + "</td>";
        return "<td" + (c[2] !== "text" && c[2] !== "mech" ? ' class="rt"' : "") + ">" + txt + "</td>";
      }).join("") + "</tr>";
    }).join("");
    return '<div class="cr-controls"><div class="cr-chips">' + chips + '</div><div class="cr-chips">' + mechs + '</div><div class="cr-chips"><input id="cr-q" type="search" placeholder="Find a canary" value="' + esc(st.q || "") + '" aria-label="Find a canary"><button class="cr-f" id="cr-csv">Save as a file (' + rows.length + " rows)</button>" + (st.key ? '<button class="cr-f" data-cr-reset="1">The record’s own order</button>' : "") + "</div></div>" +
      '<div class="cr-count">' + rows.length + " of " + all.length + " canaries · click a column to sort it, again to turn it round</div>" +
      '<div class="cr-scroll cr-tall"><table class="cr-t cr-all" data-jh-nosort><thead><tr>' + head + "</tr></thead><tbody>" + (body || '<tr><td colspan="' + COLS.length + '" class="cr-dim">No canary matches.</td></tr>') + "</tbody></table></div>";
  }
  function notRebuilt(doc) {
    var by = {};
    ((doc && doc.all_canaries) || []).forEach(function (c) { if (c.record && c.record.verdict === "no record" && !MECH[c.mechanism]) by[c.mech_label || c.mechanism] = (by[c.mech_label || c.mechanism] || 0) + 1; });
    var silent = ((doc && doc.mechanisms) || []).filter(function (m) { return !m.n_total && !m.unavailable; }).map(function (m) { return m.label; });
    var parts = Object.keys(by).map(function (k) { return "<b>" + esc(k) + "</b> (" + by[k] + ")"; });
    return '<p class="cr-p cr-dim" id="cr-not">On the board but not rebuilt through history yet, so outside both readings: ' + (parts.join(", ") || "none") + "." + (silent.length ? " Mechanisms with no canaries today: " + silent.map(esc).join(", ") + "." : "") + "</p>";
  }
  function method(rec) {
    var m = rec.method || {}, k = rec.market || {}, lc = rec.live_check || {}, inp = rec.inputs || {};
    var d = (inp.closes_dropped || {});
    return '<div class="cr-block" id="cr-method"><h3>How it was counted, and what it cannot say</h3><div class="cr-prose">' +
      "<p><b>What is counted.</b> " + esc(m.fall) + "; also " + esc(m.deep) + ". On the S&amp;P 500’s " + count(k.closes) + " daily closes from " + day(k.from) + " to " + day(k.to) + ". Since the barometer starts: " + count(k.months) + " month-ends, " + k.fell + " followed by such a fall (" + share(k.fall_share) + "); with the market near its high " + k.early_fell + " of " + k.early_months + " (" + share(k.early_fall_share) + ").</p>" +
      "<p><b>Early.</b> " + esc(m.early) + ". A canary that only rises once the market has broken is a thermometer, so each is scored on these months. <b>Score.</b> " + esc(m.score) + ". <b>Error.</b> " + esc(m.error) + ".</p>" +
      "<p><b>Rebuilt by each engine’s own rule.</b> " + rec.tested + " of " + rec.defined + " canaries; " + esc(m.sampling) + "; " + esc(m.lag) + ". Judged on " + esc(m.judged_on) + ".</p>" +
      "<p><b>The level.</b> " + esc(m.level) + ". <b>The turn.</b> " + esc(m.turn) + ". <b>Weights of canaries.</b> " + esc(m.weights) + "; " + esc(m.walk_forward) + ".</p>" +
      "<p><b>Checked against the engines.</b> Rebuilt today, " + (lc.canaries || "—") + " canaries average " + num(lc.rebuilt, 1) + " against the engines’ own " + num(lc.engines_own, 1) + "; the middle gap is " + num(lc.median_gap, 1) + " points. " + ((lc.flagged || []).length) + " differ by more than 15 and say so in their row.</p>" +
      "<p><b>The index.</b> " + (d.weekend || 0) + " weekend rows, " + (d.second_row || 0) + " second rows on one day and " + (d.stray_quarter_row || 0) + " stray quarter rows were taken out of the banked file before any fall was counted: it carries quarterly bars among its daily ones.</p>" +
      "<p><b>What it cannot say.</b> With " + rec.tested + " canaries tested, about one in twenty would look like a leader by luck. Series are as they stand today, not as first published, so revisions flatter them. The three-month span is the best of four tried; all four are shown. The turn was weak before 1998. Mechanisms not rebuilt yet are outside both readings. These are counts of what happened, not a forecast.</p>" +
      "</div></div>";
  }
  function sectionHtml(doc, rec, st) {
    var cal = doc && doc.calibrated, R = (cal && cal.record) || {};
    var when = rec ? "record of " + dayName(rec.cut_day) + " · " + rec.tested + " of " + rec.defined + " canaries rebuilt · S&amp;P 500 " + signed(((rec.market || {}).from_high_now || 0) * 100) + "% from its one-year high at that close" : (R.cut_day ? "record of " + dayName(R.cut_day) : "");
    var h = '<h2 class="cr-h2">What the record says <span class="cr-when">' + when + "</span></h2>" +
      '<p class="cr-lede">Every canary whose series the warehouse holds was rebuilt month by month with its engine’s own rule, and set against what the S&amp;P 500 did next. The readings below place today on that record.</p>' + gauges(cal, rec);
    if (!rec) return h + '<div class="cr-block"><h3>The record</h3><p class="cr-p" id="cr-wait">Loading the record…</p></div>';
    var lvl = ((rec.barometer || {}).views || {}).per_mechanism || {}, t3 = ((rec.barometer || {}).turns || {})["3"] || {};
    h += '<div class="cr-block" id="cr-follow"><h3>What followed each reading</h3><div class="cr-stack">' +
      followTable("The turn, by band", t3.reliability || [], TURN_COL, cal && cal.turn && cal.turn.band, "cr-follow-turn") +
      followTable("The level, by band", lvl.reliability || [], LEVEL_COL, cal && cal.level && cal.level.band, "cr-follow-level") + "</div>" + crossTable((rec.barometer || {}).level_against_turn, cal && cal.cell) + "</div>";
    h += '<div class="cr-block" id="cr-replay"><h3>The barometer through history</h3><div class="cr-card"><div class="cr-legend"><span><i style="background:' + C.gold + '"></i>level</span><span><i style="background:' + C.blue + '"></i>turn</span><span><i style="background:' + C.hot + ';opacity:.35;height:10px"></i>S&amp;P 500 falls of 10% or more, high to low (darker: 20% or more)</span></div>' + chart(rec) + '<div class="cr-read" id="cr-read">' + readout(((rec.barometer || {}).months || []).slice(-1)[0]) + "</div></div>" + mechTable(rec) + "</div>";
    h += '<div class="cr-block" id="cr-canaries"><h3>Every canary’s record</h3><div id="cr-table">' + tableHtml(rec, st) + "</div>" + notRebuilt(doc) + "</div>";
    h += method(rec);
    return h;
  }

  /* ── mount ──────────────────────────────────────────────────────────────────────────────────────────────────── */
  function mount(win, doc) {
    var D = win.document, host = D.getElementById("s-record");
    if (!host) return;
    var st = { verdict: "", mech: "", q: "", key: "", dir: "desc" }, rec = null;
    var draw = function () { host.innerHTML = sectionHtml(doc, rec, st); wire(); };
    var redrawTable = function () { var t = D.getElementById("cr-table"); if (t && rec) { var q = D.getElementById("cr-q"), pos = q && D.activeElement === q ? q.selectionStart : null; t.innerHTML = tableHtml(rec, st); if (pos != null) { var n = D.getElementById("cr-q"); n.focus(); n.setSelectionRange(pos, pos); } } };
    function wire() {
      var svg = D.getElementById("cr-svg");
      if (svg && rec) {
        var months = rec.barometer.months.filter(function (m) { return m.per_mechanism != null; });
        var move = function (ev) {
          var box = svg.getBoundingClientRect(), W = 1100, L = 44, R = 12;
          var fx = ((ev.touches ? ev.touches[0].clientX : ev.clientX) - box.left) / box.width * W, frac = Math.max(0, Math.min(1, (fx - L) / (W - L - R)));
          var m = nearest(months, frac), line = D.getElementById("cr-cross-x"), xx = L + (W - L - R) * (dord(m.day) - dord(months[0].day)) / (dord(months[months.length - 1].day) - dord(months[0].day));
          line.setAttribute("x1", xx); line.setAttribute("x2", xx); line.setAttribute("opacity", ".6");
          D.getElementById("cr-read").innerHTML = readout(m);
        };
        svg.addEventListener("mousemove", move); svg.addEventListener("touchmove", move, { passive: true });
      }
    }
    host.addEventListener("click", function (ev) {
      var t = ev.target.closest ? ev.target.closest("[data-cr-verdict],[data-cr-mech],[data-cr-sort],[data-cr-reset],#cr-csv") : null;
      if (!t || !rec) return;
      if (t.id === "cr-csv") {
        var rows = filtered(rec.canaries, st); rows = st.key ? sorted(rows, st.key, st.dir) : defaultOrder(rows);
        var a = D.createElement("a"); a.href = win.URL.createObjectURL(new win.Blob([csv(rows)], { type: "text/csv" })); a.download = "canaries-record-" + rec.cut_day + ".csv"; D.body.appendChild(a); a.click(); a.remove(); return;
      }
      if (t.hasAttribute("data-cr-verdict")) st.verdict = t.getAttribute("data-cr-verdict");
      else if (t.hasAttribute("data-cr-mech")) st.mech = t.getAttribute("data-cr-mech");
      else if (t.hasAttribute("data-cr-reset")) { st.key = ""; st.dir = "desc"; }
      else { var k = t.getAttribute("data-cr-sort"); if (st.key === k) st.dir = st.dir === "desc" ? "asc" : "desc"; else { st.key = k; st.dir = (k === "name" || k === "mechanism" || k === "rule") ? "asc" : "desc"; } }
      redrawTable();
    });
    host.addEventListener("input", function (ev) { if (ev.target && ev.target.id === "cr-q") { st.q = ev.target.value; redrawTable(); } });
    draw();
    var key = ((doc.calibrated || {}).record || {}).key || "data/canary-record.json";
    win.fetch("/" + key, { cache: "no-store" }).then(function (r) { if (!r.ok) throw new Error(r.status); return r.json(); }).then(function (j) { rec = j; draw(); })
      .catch(function () { var w = D.getElementById("cr-wait"); if (w) w.textContent = "The record could not be loaded."; });
  }

  return { COLS: COLS, VERDICT: VERDICT, VERDICT_ORDER: VERDICT_ORDER, MECH: MECH, cellText: cellText, sorted: sorted, filtered: filtered, defaultOrder: defaultOrder, csv: csv,
    gauges: gauges, followTable: followTable, crossTable: crossTable, chart: chart, mechTable: mechTable, tableHtml: tableHtml, notRebuilt: notRebuilt, method: method,
    sectionHtml: sectionHtml, readout: readout, nearest: nearest, mount: mount, share: share, ord: ord, signed: signed };
});
