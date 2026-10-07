/* jh-reskin-skip */
/* JustHodl — extra watchlist details sections (2026-10-06), rendered under the price/ranges of the details card:
 *   1. Trend: where price stands against its 9/20/50/100/200/250/300-day simple moving averages, and the latest
 *      golden / death cross (50-day vs 200-day) plus the short-term 20/50 cross — computed from the same daily bars
 *      the chart draws, with today's live price as the last close when the market is trading.
 *   2. Insider activity (stocks): SEC Form 4 open-market buys and sales, last 90 days (justhodl-edgar-insiders).
 *   3. Industry: the stock's industry ETF and sector ETF — trend, crosses, and fund flows for the last week, month
 *      and quarter (provider fund-flow history; reported, not estimated).
 *   4. Leaders: the largest holdings of that industry ETF (or of the ETF itself) — price, 1-day / 1-month moves,
 *      50/200-day stance and their insiders' net 90-day activity.
 * Also exposes flowHist(t): the full daily flow history for an ETF, the retained legacy history joined with the
 * fresher provider history (identical values on overlapping dates), used by the watchlist's flow strip.
 * Nothing here is a recommendation; every number names its source and date.
 */
(function (root) {
  "use strict";
  if (root.JHTvSignals) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var INSIDERS = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/";
  var C = {};
  function getJ(u) { if (!C[u]) { C[u] = root.fetch(u).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }); } return C[u]; }
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function ok(v) { return v != null && v !== "" && isFinite(+v); }
  function num(v) { if (!ok(v)) return "—"; v = +v; return v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: Math.abs(v) < 1 ? 4 : 2 }); }
  function pc(v, dp) { return ok(v) ? (v > 0 ? "+" : "") + (+v).toFixed(dp == null ? 2 : dp) + "%" : "—"; }
  function money(v) { if (!ok(v)) return "—"; v = +v; var a = Math.abs(v), s = v < 0 ? "−" : v > 0 ? "+" : ""; return s + "$" + (a >= 1e9 ? (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? (a / 1e6).toFixed(1) + "M" : a >= 1e3 ? (a / 1e3).toFixed(0) + "K" : a.toFixed(0)); }
  function cnum(v) { if (!ok(v)) return "—"; v = Math.abs(+v); return v >= 1e6 ? (v / 1e6).toFixed(2) + "M" : v >= 1e3 ? (v / 1e3).toFixed(1) + "K" : String(Math.round(v)); }
  function cls(v) { return v > 0 ? "tvd-up" : v < 0 ? "tvd-dn" : ""; }
  function ts(t) { return typeof t === "number" ? t : Date.parse(t) / 1000; }
  function dayOf(t) { return new Date(ts(t) * 1000).toISOString().slice(0, 10); }
  function fdate(s) { if (!s) return "—"; var d = new Date(String(s).slice(0, 10) + "T12:00:00Z"); return isNaN(d) ? esc(s) : d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }); }
  function nyToday() { try { return new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" }); } catch (e) { return new Date().toISOString().slice(0, 10); } }

  // ------------------------------------------------------------------ styles
  function css() {
    if (document.getElementById("jh-tvs-css")) return;
    var s = document.createElement("style"); s.id = "jh-tvs-css";
    s.textContent = [
      "#jhwl .tvs h4{font-size:14px;font-weight:600;margin:16px 0 8px;display:flex;align-items:center;gap:8px}#jhwl .tvs h4 small{margin-left:auto;font-weight:400;color:var(--mut);font-size:11px;text-align:right}",
      "#jhwl .tvs .tvs-mu{color:var(--mut);font-size:11px;line-height:1.4}#jhwl .tvs .tvd-up{color:#089981}#jhwl .tvs .tvd-dn{color:#f23645}",
      "#jhwl .tvs table{width:100%;border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums}#jhwl .tvs td,#jhwl .tvs th{padding:4px 2px;text-align:right;white-space:nowrap}",
      "#jhwl .tvs th{color:var(--mut);font-weight:400;font-size:11px;border-bottom:1px solid var(--bd)}#jhwl .tvs td:first-child,#jhwl .tvs th:first-child{text-align:left}",
      "#jhwl .tvs tr+tr td{border-top:1px solid rgba(128,128,128,.12)}",
      "#jhwl .tvs .tvs-x{border-radius:6px;padding:7px 9px;margin:6px 0;font-size:12px;line-height:1.4;border:1px solid var(--bd)}",
      "#jhwl .tvs .tvs-x.g{background:rgba(8,153,129,.12);border-color:rgba(8,153,129,.4)}#jhwl .tvs .tvs-x.d{background:rgba(242,54,69,.10);border-color:rgba(242,54,69,.4)}",
      "#jhwl .tvs .tvs-x b{font-weight:600}#jhwl .tvs .tvs-new{display:inline-block;font-size:10px;font-weight:700;letter-spacing:.04em;padding:1px 5px;border-radius:3px;background:#f0b90b;color:#000;margin-left:4px;vertical-align:1px}",
      "#jhwl .tvs .tvs-chips{display:flex;flex-wrap:wrap;gap:4px;margin:4px 0}#jhwl .tvs .tvs-chips span{font-size:11px;padding:2px 6px;border-radius:3px;font-variant-numeric:tabular-nums}",
      "#jhwl .tvs .tvs-chips .a{background:rgba(8,153,129,.15);color:#089981}#jhwl .tvs .tvs-chips .b{background:rgba(242,54,69,.13);color:#f23645}#jhwl .tvs .tvs-chips .n{background:var(--bg2);color:var(--mut)}",
      "#jhwl .tvs .tvs-card{background:var(--bg2);border-radius:6px;padding:8px 10px;margin:0 0 8px}#jhwl .tvs .tvs-card .tvs-h{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap}",
      "#jhwl .tvs .tvs-card .tvs-h a{color:var(--fg);font-weight:600;text-decoration:none;cursor:pointer}#jhwl .tvs .tvs-card .tvs-h a:hover{color:var(--blue)}",
      "#jhwl .tvs .tvs-g{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-top:6px}#jhwl .tvs .tvs-g div{display:flex;flex-direction:column;gap:1px}#jhwl .tvs .tvs-g small{color:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.05em}",
      "#jhwl .tvs .tvs-g b{font-size:13px;font-weight:600;font-variant-numeric:tabular-nums}",
      "#jhwl .tvs .tvs-bar{display:flex;height:6px;border-radius:3px;overflow:hidden;background:var(--bd);margin:6px 0 2px}#jhwl .tvs .tvs-bar i{display:block}",
      "#jhwl .tvs a.tvs-s{color:var(--fg);text-decoration:none;cursor:pointer;font-weight:600}#jhwl .tvs a.tvs-s:hover{color:var(--blue)}#jhwl .tvs tr.me td{background:rgba(41,98,255,.10)}",
      "#jhwl .tvs .tvs-sum{font-size:12px;line-height:1.45;margin:2px 0 6px}"
    ].join("\n");
    document.head.appendChild(s);
  }

  // ------------------------------------------------------------------ moving averages / crosses
  var MAS = [9, 20, 50, 100, 200, 250, 300];
  function closes(bars, q) {
    var b = (bars || []).filter(function (r) { return r && r.time != null && ok(r.close); });
    var c = b.map(function (r) { return +r.close; }), d = b.map(function (r) { return dayOf(r.time); });
    if (q && q.ok && q.live && ok(q.last) && q.last_date && c.length) {
      if (q.last_date === "same") c[c.length - 1] = +q.last;
      else if (q.last_date > d[d.length - 1]) { c.push(+q.last); d.push(q.last_date); }
      else if (q.last_date === d[d.length - 1]) c[c.length - 1] = +q.last;
    }
    return { c: c, d: d };
  }
  function smaSeries(c, n) {
    var out = new Array(c.length), s = 0;
    for (var i = 0; i < c.length; i++) { s += c[i]; if (i >= n) s -= c[i - n]; out[i] = i >= n - 1 ? s / n : null; }
    return out;
  }
  function lastCross(fast, slow, d) {
    var prev = null;
    for (var i = d.length - 1; i > 0; i--) {
      if (fast[i] == null || slow[i] == null || fast[i - 1] == null || slow[i - 1] == null) break;
      var a = fast[i] - slow[i], b = fast[i - 1] - slow[i - 1];
      if ((a > 0 && b <= 0) || (a < 0 && b >= 0)) return { up: a > 0, date: d[i], ago: d.length - 1 - i };
      prev = i;
    }
    var n = d.length - 1;
    return fast[n] != null && slow[n] != null ? { up: fast[n] > slow[n], date: null, ago: null, since: prev != null ? d[prev] : null } : null;
  }
  function trend(bars, q) {
    var x = closes(bars, q), c = x.c, d = x.d, n = c.length; if (n < 10) return null;
    var P = c[n - 1], rows = [], S = {};
    MAS.forEach(function (m) {
      if (n < m) { rows.push({ m: m, v: null }); return; }
      var s = S[m] = smaSeries(c, m), v = s[n - 1], v5 = n - 6 >= 0 ? s[n - 6] : null;
      rows.push({ m: m, v: v, diff: (P / v - 1) * 100, above: P >= v, slope: v5 != null ? v - v5 : null });
    });
    var gx = S[50] && S[200] ? lastCross(S[50], S[200], d) : null;
    var sx = S[20] && S[50] ? lastCross(S[20], S[50], d) : null;
    // price crossing the 200-day / 50-day (most recent day the close moved to the other side)
    function pxCross(m) { var s = S[m]; if (!s) return null; return lastCross(c, s, d); }
    return { P: P, date: d[n - 1], rows: rows, golden: gx, short: sx, p200: pxCross(200), p50: pxCross(50), n: n, live: !!(q && q.live) };
  }
  function crossText(x, fast, slow, name) {
    if (!x) return "";
    if (x.date == null) return (x.up ? "The " + fast + "-day has been above the " + slow + "-day" : "The " + fast + "-day has been below the " + slow + "-day") + " for the whole loaded history";
    return (name ? (x.up ? "Golden cross" : "Death cross") + " — " : "") + "the " + fast + "-day crossed " + (x.up ? "above" : "below") + " the " + slow + "-day on " + fdate(x.date) + " (" + (x.ago === 0 ? "today" : x.ago + " session" + (x.ago > 1 ? "s" : "") + " ago") + ")";
  }
  function chips(t) {
    return '<div class="tvs-chips">' + t.rows.map(function (r) {
      if (r.v == null) return '<span class="n" title="Not enough history">' + r.m + "D —</span>";
      return '<span class="' + (r.above ? "a" : "b") + '" title="' + r.m + "-day SMA " + num(r.v) + " · price " + pc(r.diff) + '">' + r.m + "D " + (r.above ? "▲" : "▼") + "</span>";
    }).join("") + "</div>";
  }
  function crossLine(t, compact) {
    var g = t.golden; if (!g) return "";
    var fresh = g.ago != null && g.ago <= 10;
    var txt = g.date ? (g.up ? "Golden cross" : "Death cross") + " " + (g.ago === 0 ? "today" : g.ago + " sessions ago") + " (" + fdate(g.date) + ")" : (g.up ? "50-day above 200-day (no cross in loaded history)" : "50-day below 200-day (no cross in loaded history)");
    return '<div class="tvs-mu" style="margin-top:2px"><span class="' + (g.up ? "tvd-up" : "tvd-dn") + '">' + esc(txt) + "</span>" + (fresh ? '<span class="tvs-new">NEW</span>' : "") + (compact ? "" : "") + "</div>";
  }
  function trendHtml(t, label) {
    var g = t.golden, fresh = g && g.ago != null && g.ago <= 10;
    var above = t.rows.filter(function (r) { return r.v != null && r.above; }).length, have = t.rows.filter(function (r) { return r.v != null; }).length;
    var h = "<h4>Moving averages<small>daily SMA · " + (t.live ? "live price" : "close " + fdate(t.date)) + "</small></h4>" +
      '<div class="tvs-sum">' + esc(label) + " is above <b>" + above + " of " + have + "</b> moving averages.</div>";
    if (g) h += '<div class="tvs-x ' + (g.up ? "g" : "d") + '"><b>' + (g.date ? (g.up ? "Golden cross" : "Death cross") : g.up ? "Bullish 50/200 alignment" : "Bearish 50/200 alignment") + "</b>" + (fresh ? '<span class="tvs-new">JUST CROSSED</span>' : "") + "<br>" + esc(crossText(g, 50, 200)) + "." +
      (t.short ? "<br>" + "Short-term: " + esc(crossText(t.short, 20, 50)) + "." : "") +
      (t.p200 && t.p200.date ? "<br>Price moved " + (t.p200.up ? "above" : "below") + " the 200-day on " + fdate(t.p200.date) + "." : "") + "</div>";
    h += "<table><tr><th>SMA</th><th>Value</th><th>Price vs MA</th><th>MA slope (5d)</th><th></th></tr>" + t.rows.map(function (r) {
      if (r.v == null) return "<tr><td>" + r.m + '-day</td><td colspan="4" class="tvs-mu">needs ' + r.m + " sessions</td></tr>";
      return "<tr><td>" + r.m + "-day</td><td>" + num(r.v) + '</td><td class="' + cls(r.diff) + '">' + pc(r.diff) + '</td><td class="' + cls(r.slope) + '">' + (r.slope == null ? "—" : r.slope > 0 ? "rising" : r.slope < 0 ? "falling" : "flat") + '</td><td class="' + (r.above ? "tvd-up" : "tvd-dn") + '">' + (r.above ? "Above" : "Below") + "</td></tr>";
    }).join("") + "</table>";
    return h;
  }

  // ------------------------------------------------------------------ fund flows (legacy daily history + provider history)
  var DESK = null;
  function desk() { if (!DESK) DESK = getJ(PROXY + "/data/etf-desk-research.json").then(function (d) { var m = {}; Object.keys((d && d.funds) || {}).forEach(function (k) { var f = d.funds[k] && d.funds[k].flows; if (f) m[k] = { key: f.history && f.history.key, last: f.latest_effective_date, obs: f.latest_observation || null }; }); return m; }); return DESK; }
  var FH = {};
  function flowHist(t) {
    t = String(t || "").toUpperCase();
    if (FH[t]) return FH[t];
    FH[t] = Promise.all([getJ(PROXY + "/data/etf-flow-hist/" + encodeURIComponent(t) + ".json"), desk().then(function (m) { var e = m[t]; return e && e.key ? getJ(PROXY + "/" + e.key).then(function (h) { return { h: h, e: e }; }) : null; })]).then(function (a) {
      var leg = a[0] && a[0].d && a[0].f ? a[0] : null, fr = a[1] && a[1].h && Array.isArray(a[1].h.history) ? a[1] : null;
      if (!leg && !fr) return null;
      var by = {};
      if (leg) leg.d.forEach(function (d, i) { if (isFinite(leg.f[i])) by[d] = +leg.f[i]; });
      if (fr) fr.h.history.forEach(function (r) { var v = parseFloat(r.flow_decimal); if (r.date && isFinite(v) && !(r.invalid_fields || []).length) by[r.date] = v; });
      var d = Object.keys(by).sort(), out = { ticker: t, d: d, f: d.map(function (k) { return by[k]; }) };
      out.asof = d[d.length - 1];
      out.source = fr ? "Provider fund-flow history" + (leg ? " joined with the retained daily history from " + leg.d[0] : "") : (leg.source || "retained daily flow history");
      out.aum = fr && fr.e.obs ? parseFloat(fr.e.obs.reported_assets_usd_decimal) : null;
      out.stale = (Date.parse(nyToday()) - Date.parse(out.asof)) > 7 * 864e5;
      return out;
    });
    return FH[t];
  }
  function flowWin(fh, sessions) {
    if (!fh || !fh.d.length) return null;
    var n = fh.d.length, s = 0, k = 0;
    for (var i = n - 1; i >= 0 && k < sessions; i--, k++) s += fh.f[i];
    return k ? { v: s, from: fh.d[Math.max(0, n - sessions)], n: k } : null;
  }

  // ------------------------------------------------------------------ industry / sector ETF
  var IND = [
    [/semiconductor/i, "SMH", "Semiconductors"], [/software|information technology services/i, "IGV", "Software"],
    [/banks - regional/i, "KRE", "Regional banks"], [/banks/i, "KBE", "Banks"], [/biotech/i, "XBI", "Biotech"],
    [/drug manufacturers|pharmaceutical/i, "XPH", "Pharmaceuticals"], [/medical (devices|instruments)/i, "IHI", "Medical devices"],
    [/oil & gas (e&p|exploration)/i, "XOP", "Oil & gas E&P"], [/oil & gas equipment|oil & gas drilling/i, "OIH", "Oil services"],
    [/gold|silver|precious/i, "GDX", "Gold miners"], [/aerospace|defense/i, "ITA", "Aerospace & defense"], [/airlines/i, "JETS", "Airlines"],
    [/residential construction|building products/i, "XHB", "Homebuilders"], [/insurance/i, "KIE", "Insurance"],
    [/asset management|capital markets/i, "IAI", "Broker-dealers & asset managers"], [/reit|real estate/i, "VNQ", "Real estate"],
    [/solar/i, "TAN", "Solar"], [/steel/i, "SLX", "Steel"], [/copper|aluminum|industrial metals|other industrial metals|coking coal/i, "XME", "Metals & mining"],
    [/trucking|railroads|integrated freight|marine shipping|airports/i, "IYT", "Transportation"],
    [/specialty retail|department stores|discount stores|apparel retail|internet retail|home improvement/i, "XRT", "Retail"],
    [/internet content|interactive media/i, "FDN", "Internet"], [/telecom/i, "IYZ", "Telecom"], [/auto/i, "CARZ", "Autos"],
    [/gambling|resorts|casinos/i, "BJK", "Gaming"], [/restaurants/i, "PBJ", "Food & beverage"]
  ];
  var SECT = { "technology": ["XLK", "Technology"], "healthcare": ["XLV", "Health care"], "financial services": ["XLF", "Financials"], "energy": ["XLE", "Energy"], "consumer cyclical": ["XLY", "Consumer discretionary"], "consumer defensive": ["XLP", "Consumer staples"], "industrials": ["XLI", "Industrials"], "basic materials": ["XLB", "Materials"], "real estate": ["XLRE", "Real estate"], "utilities": ["XLU", "Utilities"], "communication services": ["XLC", "Communication services"] };
  function etfsFor(p) {
    var out = [], ind = String((p && p.industry) || ""), sec = String((p && p.sector) || "").toLowerCase();
    for (var i = 0; i < IND.length; i++) if (IND[i][0].test(ind)) { out.push({ t: IND[i][1], why: IND[i][2] + " industry ETF", role: "industry" }); break; }
    var s = SECT[sec]; if (s && !out.some(function (o) { return o.t === s[0]; })) out.push({ t: s[0], why: s[1] + " sector ETF", role: "sector" });
    return out;
  }
  function quotes(ts0) {
    var u = PROXY + "/quotes?tickers=" + ts0.map(encodeURIComponent).join(",");
    return getJ(u).then(function (r) { return (r && r.tickers) || {}; });
  }

  // ------------------------------------------------------------------ insiders (SEC Form 4)
  var INS = {};
  // The Form 4 Lambda keeps a 24 h copy at edgar-insiders/<T>.json (read through the proxy). Its function URL sends
  // a duplicated CORS header that browsers refuse, so when the copy is missing or old the Lambda is only poked
  // (no-cors, response unread) to rebuild it, and the copy is read again.
  function insCopy(t, bust) { return root.fetch(PROXY + "/edgar-insiders/" + encodeURIComponent(t) + ".json" + (bust ? "?r=" + Date.now() : "")).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }); }
  function insiders(t) {
    t = String(t).toUpperCase();
    if (!INS[t]) INS[t] = insCopy(t).then(function (d) {
      var age = d && d.generated_at ? Date.now() - Date.parse(d.generated_at) : Infinity;
      if (d && age < 3 * 864e5) return d;
      return root.fetch(INSIDERS + "?ticker=" + encodeURIComponent(t), { mode: "no-cors" }).catch(function () {}).then(function () { return insCopy(t, true); }).then(function (d2) { return d2 || d; });
    });
    return INS[t];
  }
  // TradingView convention for the price shown: live during the regular session, the session close after it
  function phaseNY() { try { var p = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "numeric", hour12: false }).formatToParts(new Date()); var g = {}; p.forEach(function (z) { g[z.type] = z.value; }); if (g.weekday === "Sat" || g.weekday === "Sun") return "closed"; var m = (+g.hour % 24) * 60 + (+g.minute); return m >= 570 && m < 960 ? "open" : "other"; } catch (e) { return "open"; } }
  function regPx(lq) {
    if (!lq || !ok(lq.price)) return null;
    var px = phaseNY() === "open" || !(lq.dayClose > 0) || !(lq.volume > 0) ? +lq.price : +lq.dayClose;
    var pv = +lq.prevClose || 0;
    return { price: px, changePct: pv ? (px / pv - 1) * 100 : lq.changePct };
  }
  var LBL = { ROUTINE_SELLING: "Routine selling", ACCELERATING_SELL: "Accelerating selling", INSIDER_BUYING: "Insider buying", STRONG_CLUSTER_BUY: "Cluster buying", BULLISH_INSIDER_BUY: "Significant insider buying", BEARISH_INSIDER_SELL: "Heavy insider selling", NEUTRAL: "Quiet", NO_ACTIVITY: "No activity" };
  function lbl(s) { return LBL[s] || String(s || "—").replace(/_/g, " ").toLowerCase().replace(/^./, function (c) { return c.toUpperCase(); }); }
  function insiderHtml(d, t) {
    if (!d || d.error) return "<h4>Insider activity<small>SEC Form 4</small></h4><div class=\"tvs-mu\">No Form 4 data for " + esc(t) + (d && d.error ? " (" + esc(String(d.error).slice(0, 80)) + ")" : "") + ".</div>";
    var nb = +d.n_buys || 0, ns = +d.n_sells || 0, db = +d.total_dollars_buy || 0, ds = +d.total_dollars_sell || 0, tot = db + ds;
    var h = "<h4>Insider activity<small>SEC Form 4 · last 90 days · checked " + fdate(String(d.generated_at || "").slice(0, 10)) + "</small></h4>";
    if (!nb && !ns) return h + '<div class="tvs-mu">No open-market insider buys or sales filed in the last 90 days.</div>';
    var buyish = db > ds;
    h += '<div class="tvs-x ' + (buyish ? "g" : "d") + '"><b>' + esc(lbl(d.signal_label)) + "</b>" + (d.cluster_detected ? '<span class="tvs-new">CLUSTER</span>' : "") + "<br>" +
      '<span class="tvd-up">' + nb + " buy" + (nb === 1 ? "" : "s") + " · " + money(db).replace("+", "") + '</span> &nbsp; <span class="tvd-dn">' + ns + " sale" + (ns === 1 ? "" : "s") + " · " + money(-ds) + "</span>" +
      " &nbsp; net <b class=\"" + cls(db - ds) + '">' + money(db - ds) + "</b>" + (d.n_csuite_sellers ? " · " + d.n_csuite_sellers + " C-suite seller" + (d.n_csuite_sellers > 1 ? "s" : "") : "") +
      (d.signal_note ? '<br><span class="tvs-mu">' + esc(d.signal_note) + "</span>" : "") + "</div>";
    if (tot > 0) h += '<div class="tvs-bar"><i style="width:' + (db / tot * 100).toFixed(1) + '%;background:#089981"></i><i style="width:' + (ds / tot * 100).toFixed(1) + '%;background:#f23645"></i></div><div class="tvs-mu">Buys vs sales by value · prior 90 days: ' + (+d.prior_n_buys || 0) + " buys " + money(d.prior_dollars_buy || 0).replace("+", "") + ", " + (+d.prior_n_sells || 0) + " sales " + money(-(d.prior_dollars_sell || 0)) + "</div>";
    var who = (d.top_buyers || []).map(function (x) { x.b = 1; return x; }).concat(d.top_sellers || []).slice(0, 6);
    if (who.length) h += '<table style="margin-top:6px"><tr><th>Insider</th><th>Role</th><th>Shares</th><th>Value</th></tr>' + who.map(function (x) {
      return "<tr><td title=\"" + esc(x.filer) + '">' + esc(String(x.filer || "").slice(0, 22)) + '</td><td title="' + esc(x.role) + '">' + esc(String(x.role || "").replace(/^Officer · /, "").slice(0, 18)) + '</td><td class="' + (x.b ? "tvd-up" : "tvd-dn") + '">' + (x.b ? "+" : "−") + cnum(x.shares) + '</td><td class="' + (x.b ? "tvd-up" : "tvd-dn") + '">' + money(x.b ? x.dollars : -x.dollars) + "</td></tr>";
    }).join("") + "</table>";
    var tx = (d.transactions || []).slice().sort(function (a, b) { return a.date < b.date ? 1 : -1; }).slice(0, 8);
    if (tx.length) h += '<table style="margin-top:8px"><tr><th>Date</th><th>Insider</th><th>Type</th><th>Shares</th><th>Price</th><th>Value</th></tr>' + tx.map(function (x) {
      var b = x.direction === "BUY";
      return "<tr><td>" + esc(String(x.date).slice(5)) + '</td><td title="' + esc(x.filer + " · " + (x.role || "")) + '">' + esc(String(x.filer || "").split(" ").slice(0, 2).join(" ").slice(0, 16)) + '</td><td class="' + (b ? "tvd-up" : "tvd-dn") + '">' + (b ? "Buy" : x.direction === "TAX_SELL" ? "Tax sale" : "Sale") + "</td><td>" + cnum(x.shares) + "</td><td>" + num(x.price) + '</td><td class="' + (b ? "tvd-up" : "tvd-dn") + '">' + money(b ? x.dollars : -x.dollars) + "</td></tr>";
    }).join("") + "</table>";
    return h + '<div class="tvs-mu" style="margin-top:4px">Open-market purchases (P) and sales (S) from SEC EDGAR Form 4; grants, option exercises and gifts are excluded.</div>';
  }

  // daily bars for an ETF / leader: the chart's own bars first; when they are missing or too short for the
  // 300-day average, the proxy's Polygon daily aggregates (5 years)
  function barsVia(x, sym) {
    return Promise.resolve(x.barsFor(sym)).catch(function () { return []; }).then(function (b) {
      if (Array.isArray(b) && b.length >= 320) return b;
      return getJ(PROXY + "/ohlc?ticker=" + encodeURIComponent(sym) + "&span=day&days=2200").then(function (r) {
        var pb = r && Array.isArray(r.bars) ? r.bars : [];
        return pb.length > (b ? b.length : 0) ? pb : b || [];
      });
    });
  }
  // ------------------------------------------------------------------ provider reads used by the new sections
  function fmp(ep, t, extra) { return getJ(PROXY + "/fmp?ep=" + ep + "&symbol=" + encodeURIComponent(t) + (extra || "")).then(function (r) { return r && Array.isArray(r.data) ? r.data : null; }); }
  function profile(t) { return fmp("profile", t).then(function (d) { return d && d[0] ? d[0] : null; }); }
  function earnings(t) { return fmp("earnings", t); }
  function shortInt(t) { return getJ(PROXY + "/poly/short?ticker=" + encodeURIComponent(t)).then(function (r) { return r && Array.isArray(r.results) ? r.results : null; }); }
  function floatOf(t) { return fmp("shares-float", t).then(function (d) { return d && d[0] ? d[0] : null; }); }
  // the last four quarters whose 13F window has closed (quarter end + 50 days), newest first
  function quarters(n) {
    var out = [], d = new Date(nyToday() + "T12:00:00Z"), y = d.getUTCFullYear(), q = Math.floor(d.getUTCMonth() / 3) + 1;
    for (var k = 0; out.length < n && k < 12; k++) {
      q--; if (q < 1) { q = 4; y--; }
      var end = Date.UTC(y, q * 3, 0, 12);
      if (Date.now() - end > 50 * 864e5) out.push({ y: y, q: q });
    }
    return out;
  }
  function instTrend(t) { return Promise.all(quarters(4).map(function (z) { return fmp("institutional-ownership/symbol-positions-summary", t, "&year=" + z.y + "&quarter=" + z.q).then(function (d) { return d && d[0] ? d[0] : null; }); })).then(function (a) { return a.filter(Boolean); }); }

  // ------------------------------------------------------------------ earnings
  // reaction = close on the session before the report date -> close on the session after it (covers both
  // before-open and after-close reports, which the provider does not distinguish)
  function earnInfo(rows, bars) {
    if (!rows || !rows.length) return null;
    var today = nyToday(), up = null, last = null;
    rows.forEach(function (r) { if (!r || !r.date) return; if (r.date >= today && r.epsActual == null) { if (!up || r.date < up.date) up = r; } else if (r.epsActual != null && r.date <= today) { if (!last || r.date > last.date) last = r; } });
    var x = closes(bars), react = null, hist = [];
    function move(dt) {
      var i = -1; for (var k = 0; k < x.d.length; k++) if (x.d[k] < dt) i = k; else break;
      var j = -1; for (var m = 0; m < x.d.length; m++) if (x.d[m] > dt) { j = m; break; }
      return i >= 0 && j > i ? { pct: (x.c[j] / x.c[i] - 1) * 100, from: x.d[i], to: x.d[j] } : null;
    }
    if (last) react = move(last.date);
    rows.filter(function (r) { return r && r.epsActual != null && r.date <= today; }).sort(function (a, b) { return a.date < b.date ? 1 : -1; }).slice(0, 4).forEach(function (r) { hist.push({ r: r, m: move(r.date) }); });
    var days = up ? Math.round((Date.parse(up.date + "T12:00:00Z") - Date.parse(today + "T12:00:00Z")) / 864e5) : null;
    return { next: up, days: days, last: last, react: react, hist: hist };
  }
  function surprise(a, e) { return ok(a) && ok(e) && +e !== 0 ? (a - e) / Math.abs(e) * 100 : null; }
  function bigMoney(v) { if (!ok(v)) return "—"; v = +v; var a = Math.abs(v); return (v < 0 ? "−" : "") + "$" + (a >= 1e9 ? (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? (a / 1e6).toFixed(1) + "M" : a.toFixed(0)); }
  function earnHtml(e, t) {
    var h = "<h4>Earnings<small>FMP earnings calendar</small></h4>";
    if (!e) return h + '<div class="tvs-mu">No earnings calendar for ' + esc(t) + ".</div>";
    if (e.next) h += '<div class="tvs-x"><b>Next report ' + fdate(e.next.date) + "</b>" + (e.days != null && e.days <= 14 ? '<span class="tvs-new">' + (e.days === 0 ? "TODAY" : "IN " + e.days + "D") + "</span>" : "") + '<br><span class="tvs-mu">' + (e.days != null ? e.days + " day" + (e.days === 1 ? "" : "s") + " away · " : "") + "EPS est. " + num(e.next.epsEstimated) + (ok(e.next.revenueEstimated) ? " · revenue est. " + bigMoney(e.next.revenueEstimated) : "") + "</span></div>";
    else h += '<div class="tvs-mu">No upcoming report date published yet.</div>';
    if (e.last) {
      var es = surprise(e.last.epsActual, e.last.epsEstimated), rs = surprise(e.last.revenueActual, e.last.revenueEstimated), r = e.react;
      h += '<div class="tvs-x ' + (r ? (r.pct >= 0 ? "g" : "d") : "") + '"><b>Last report ' + fdate(e.last.date) + "</b>" + (r ? ' · stock <b class="' + cls(r.pct) + '">' + pc(r.pct) + "</b>" : "") +
        '<br><span class="tvs-mu">EPS ' + num(e.last.epsActual) + " vs " + num(e.last.epsEstimated) + ' est. (<span class="' + cls(es) + '">' + pc(es, 1) + "</span>)" + (ok(e.last.revenueActual) ? " · revenue " + bigMoney(e.last.revenueActual) + ' (<span class="' + cls(rs) + '">' + pc(rs, 1) + "</span>)" : "") +
        (r ? "<br>Reaction: close " + fdate(r.from) + " → close " + fdate(r.to) : "") + "</span></div>";
    }
    if (e.hist.length > 1) h += "<table><tr><th>Report</th><th>EPS</th><th>Est.</th><th>Surprise</th><th>Reaction</th></tr>" + e.hist.map(function (z) { var s = surprise(z.r.epsActual, z.r.epsEstimated); return "<tr><td>" + fdate(z.r.date) + "</td><td>" + num(z.r.epsActual) + "</td><td>" + num(z.r.epsEstimated) + '</td><td class="' + cls(s) + '">' + pc(s, 1) + '</td><td class="' + cls(z.m && z.m.pct) + '">' + (z.m ? pc(z.m.pct) : "—") + "</td></tr>"; }).join("") + "</table>";
    return h + '<div class="tvs-mu" style="margin-top:4px">Reaction = close the session before the report date to close the session after it.</div>';
  }

  // ------------------------------------------------------------------ relative strength
  var RSW = [["1M", 21], ["3M", 63], ["6M", 126]];
  function retN(c, n) { return c.length > n ? (c[c.length - 1] / c[c.length - 1 - n] - 1) * 100 : null; }
  function rsCalc(sb, eb, mb, qs) {
    var a = closes(sb, qs && qs[0]).c, b = eb ? closes(eb, qs && qs[1]).c : [], m = closes(mb, qs && qs[2]).c;
    return RSW.map(function (w) { var s = retN(a, w[1]), e = b.length ? retN(b, w[1]) : null, p = retN(m, w[1]); return { k: w[0], s: s, e: e, m: p, ve: s != null && e != null ? s - e : null, vm: s != null && p != null ? s - p : null }; });
  }
  function rsHtml(rs, t, etf) {
    var h = "<h4>Relative strength<small>total price return · " + (etf ? "vs " + esc(etf) + " and " : "vs ") + "SPY</small></h4>";
    var strong = rs.filter(function (r) { return r.vm != null && r.vm > 0; }).length;
    h += '<div class="tvs-sum">' + esc(t) + " beat the S&amp;P 500 over <b>" + strong + " of 3</b> periods" + (etf ? ", and its industry ETF over <b>" + rs.filter(function (r) { return r.ve != null && r.ve > 0; }).length + " of 3</b>" : "") + ".</div>";
    h += "<table><tr><th>Period</th><th>" + esc(t) + "</th>" + (etf ? "<th>" + esc(etf) + "</th>" : "") + "<th>SPY</th>" + (etf ? "<th>vs " + esc(etf) + "</th>" : "") + "<th>vs SPY</th></tr>" + rs.map(function (r) {
      return "<tr><td>" + r.k + '</td><td class="' + cls(r.s) + '">' + pc(r.s, 1) + "</td>" + (etf ? '<td class="' + cls(r.e) + '">' + pc(r.e, 1) + "</td>" : "") + '<td class="' + cls(r.m) + '">' + pc(r.m, 1) + "</td>" + (etf ? '<td class="' + cls(r.ve) + '"><b>' + (r.ve == null ? "—" : (r.ve > 0 ? "+" : "") + r.ve.toFixed(1) + " pp") + "</b></td>" : "") + '<td class="' + cls(r.vm) + '"><b>' + (r.vm == null ? "—" : (r.vm > 0 ? "+" : "") + r.vm.toFixed(1) + " pp") + "</b></td></tr>";
    }).join("") + '</table><div class="tvs-mu" style="margin-top:4px">pp = percentage points of out- (+) or under- (−) performance over 21 / 63 / 126 sessions.</div>';
    return h;
  }

  // ------------------------------------------------------------------ ownership: short interest + 13F trend
  function ownHtml(si, fl, inst, t, adv) {
    var h = "<h4>Ownership trend<small>FINRA short interest · 13F filings</small></h4>";
    if (si && si.length) {
      var L = si[0], P = si[1], fs = fl && +fl.floatShares > 0 ? +fl.floatShares : null, pf = fs ? L.short_interest / fs * 100 : null, ch = P && P.short_interest ? (L.short_interest / P.short_interest - 1) * 100 : null;
      h += '<div class="tvs-card"><div class="tvs-h"><b>Short interest</b><span class="tvs-mu">settled ' + fdate(L.settlement_date) + "</span></div>" +
        '<div class="tvs-g"><div><small>Shares short</small><b>' + cnum(L.short_interest) + '</b><span class="tvs-mu ' + cls(ch) + '">' + (ch == null ? "" : pc(ch, 1) + " vs " + fdate(P.settlement_date).replace(/, \d{4}$/, "")) + "</span></div>" +
        "<div><small>% of float</small><b>" + (pf == null ? "—" : pf.toFixed(2) + "%") + '</b><span class="tvs-mu">' + (fs ? "float " + cnum(fs) : "float n/a") + "</span></div>" +
        "<div><small>Days to cover</small><b>" + (ok(L.days_to_cover) ? (+L.days_to_cover).toFixed(2) : "—") + '</b><span class="tvs-mu">avg vol ' + cnum(L.avg_daily_volume) + "</span></div></div>" +
        '<table style="margin-top:6px"><tr><th>Settlement</th><th>Short</th><th>Change</th><th>Days to cover</th></tr>' + si.slice(0, 6).map(function (r, i) { var n = si[i + 1], c = n && n.short_interest ? (r.short_interest / n.short_interest - 1) * 100 : null; return "<tr><td>" + fdate(r.settlement_date) + "</td><td>" + cnum(r.short_interest) + '</td><td class="' + cls(-c) + '">' + pc(c, 1) + "</td><td>" + (ok(r.days_to_cover) ? (+r.days_to_cover).toFixed(2) : "—") + "</td></tr>"; }).join("") + "</table></div>";
    } else h += '<div class="tvs-mu">No short-interest settlements for ' + esc(t) + ".</div>";
    if (inst && inst.length) {
      var o = inst.slice().sort(function (a, b) { return a.date < b.date ? 1 : -1; });
      var d4 = o.length > 1 && ok(o[0].ownershipPercent) && ok(o[o.length - 1].lastOwnershipPercent) ? o[0].ownershipPercent - o[o.length - 1].lastOwnershipPercent : null;
      h += '<div class="tvs-card"><div class="tvs-h"><b>Institutional ownership</b><span class="tvs-mu">last ' + o.length + " quarters" + (d4 != null ? ' · <span class="' + cls(d4) + '">' + (d4 > 0 ? "+" : "") + d4.toFixed(2) + " pp</span>" : "") + "</span></div>" +
        '<table style="margin-top:4px"><tr><th>Quarter</th><th>Owned</th><th>Δ pp</th><th>Holders</th><th>Shares Δ</th><th>New / closed</th></tr>' + o.map(function (r) {
          var sc = r.lastNumberOf13Fshares ? (r.numberOf13Fshares / r.lastNumberOf13Fshares - 1) * 100 : null;
          return "<tr><td>" + esc(String(r.date).slice(0, 7)) + "</td><td>" + (ok(r.ownershipPercent) ? (+r.ownershipPercent).toFixed(1) + "%" : "—") + '</td><td class="' + cls(r.ownershipPercentChange) + '">' + (ok(r.ownershipPercentChange) ? (r.ownershipPercentChange > 0 ? "+" : "") + (+r.ownershipPercentChange).toFixed(2) : "—") + "</td><td>" + (r.investorsHolding || 0).toLocaleString("en-US") + ' <span class="' + cls(r.investorsHoldingChange) + '">(' + (r.investorsHoldingChange > 0 ? "+" : "") + (r.investorsHoldingChange || 0) + ')</span></td><td class="' + cls(sc) + '">' + pc(sc, 1) + "</td><td>" + (r.newPositions || 0) + " / " + (r.closedPositions || 0) + "</td></tr>";
        }).join("") + '</table><div class="tvs-mu" style="margin-top:4px">From 13F filings; a quarter appears once its 45-day filing window has closed.</div></div>';
    }
    return h;
  }

  // ------------------------------------------------------------------ scan record (screener columns + alerts)
  // A compact per-symbol summary, cached in localStorage for 6 hours.
  var SCN = {}, SCAN_TTL = 6 * 3600e3;
  function scanCached(t) {
    t = String(t || "").toUpperCase();
    if (SCN[t] && SCN[t].v) return SCN[t].v;
    try { var r = JSON.parse(localStorage.getItem("jh-scan:" + t) || "null"); if (r && Date.now() - r.at < SCAN_TTL) { SCN[t] = { v: r }; return r; } } catch (e) {}
    return null;
  }
  function scan(t, barsFor) {
    t = String(t || "").toUpperCase();
    var c = scanCached(t); if (c) return Promise.resolve(c);
    if (SCN[t] && SCN[t].p) return SCN[t].p;
    var x = { barsFor: barsFor || function () { return []; } };
    var p = profile(t).then(function (pf) {
      var isEtf = !!(pf && (pf.isEtf || pf.isFund));
      var etf = isEtf ? null : (etfsFor(pf)[0] || null);
      return Promise.all([barsVia(x, t), isEtf ? null : insiders(t), etf ? flowHist(etf.t) : null, isEtf ? flowHist(t) : null, isEtf ? null : earnings(t)]).then(function (a) {
        var bars = a[0] || [], tr = trend(bars, null), ins = a[1], fh = a[2] || a[3], er = earnInfo(a[4], bars);
        var above = tr ? tr.rows.filter(function (r) { return r.v != null && r.above; }).length : null;
        var w = flowWin(fh, 5);
        var rec = { t: t, at: Date.now(), isEtf: isEtf, etf: etf ? etf.t : isEtf ? t : null,
          above: above, have: tr ? tr.rows.filter(function (r) { return r.v != null; }).length : null, last: tr ? tr.date : null,
          gx: tr && tr.golden && tr.golden.date ? { up: tr.golden.up, date: tr.golden.date, ago: tr.golden.ago } : null,
          ins: ins && !ins.error ? { net: (+ins.total_dollars_buy || 0) - (+ins.total_dollars_sell || 0), nb: +ins.n_buys || 0, ns: +ins.n_sells || 0, label: ins.signal_label || "", cluster: !!ins.cluster_detected, buyers: (ins.top_buyers || []).length } : null,
          flow: w ? { v: w.v, pct: fh.aum ? w.v / fh.aum * 100 : null, asof: fh.asof } : null,
          earn: er ? { next: er.next ? er.next.date : null, days: er.days, last: er.last ? er.last.date : null, react: er.react ? +er.react.pct.toFixed(2) : null } : null };
        try { localStorage.setItem("jh-scan:" + t, JSON.stringify(rec)); } catch (e) {}
        SCN[t] = { v: rec };
        return rec;
      });
    }).catch(function () { SCN[t] = null; return null; });
    SCN[t] = { p: p };
    return p;
  }
  // ------------------------------------------------------------------ main
  function render(host, x) {
    css();
    host.classList.add("tvs");
    var seq = (host._tvsSeq = (host._tvsSeq || 0) + 1);
    function alive() { return host._tvsSeq === seq && host.isConnected; }
    host.innerHTML = '<div data-s="trend"></div><div data-s="rs"></div><div data-s="earn"></div><div data-s="ins"></div><div data-s="own"></div><div data-s="ind"></div><div data-s="lead"></div>';
    function S(k) { return host.querySelector('[data-s="' + k + '"]'); }
    var go = x.go || function () {};
    host.addEventListener("click", function (e) { var a = e.target.closest("[data-go]"); if (a) { e.preventDefault(); e.stopPropagation(); go(a.getAttribute("data-go")); } });
    // 1. trend
    Promise.resolve(x.bars).then(function (b) {
      if (!alive()) return;
      var t = trend(b, x.q); if (!t) return;
      S("trend").innerHTML = trendHtml(t, x.sym || x.T || "Price");
    });
    if (!x.T) return;
    Promise.resolve(x.profile).then(function (p) {
      if (!alive()) return;
      var isEtf = !!(p && (p.isEtf || p.isFund)) || !!x.isEtf;
      // relative strength vs the industry ETF and SPY; earnings; ownership
      var rsEtf = isEtf ? null : (etfsFor(p)[0] || null);
      Promise.all([Promise.resolve(x.bars), rsEtf ? barsVia(x, rsEtf.t) : null, barsVia(x, "SPY")]).then(function (b) {
        if (!alive() || !b[0] || !b[0].length) return;
        S("rs").innerHTML = rsHtml(rsCalc(b[0], b[1], b[2], [x.q]), x.sym || x.T, rsEtf && b[1] && b[1].length ? rsEtf.t : null);
        if (!isEtf) earnings(x.T).then(function (r) { if (alive()) S("earn").innerHTML = earnHtml(earnInfo(r, b[0]), x.T); });
      });
      Promise.all([shortInt(x.T), floatOf(x.T), isEtf ? [] : instTrend(x.T)]).then(function (a) { if (alive() && ((a[0] && a[0].length) || (a[2] && a[2].length))) S("own").innerHTML = ownHtml(a[0], a[1], a[2], x.T); });
      // 2. insiders (operating companies only)
      if (!isEtf) { S("ins").innerHTML = '<h4>Insider activity<small>SEC Form 4</small></h4><div class="tvs-mu">Loading Form 4 filings…</div>'; insiders(x.T).then(function (d) { if (alive()) S("ins").innerHTML = insiderHtml(d, x.T); }); }
      // 3. industry / sector ETFs
      var etfs = isEtf ? [] : etfsFor(p);
      if (etfs.length) {
        S("ind").innerHTML = "<h4>Industry & sector ETFs<small>" + esc([p && p.industry, p && p.sector].filter(Boolean).join(" · ")) + '</small></h4><div class="tvs-mu">Loading…</div>';
        Promise.all([quotes(etfs.map(function (e) { return e.t; }))].concat(etfs.map(function (e) { return Promise.all([barsVia(x, e.t), flowHist(e.t)]); }))).then(function (a) {
          if (!alive()) return;
          var qs = a[0];
          S("ind").innerHTML = "<h4>Industry & sector ETFs<small>" + esc([p && p.industry, p && p.sector].filter(Boolean).join(" · ")) + "</small></h4>" + etfs.map(function (e, i) {
            var bars = a[i + 1][0], fh = a[i + 1][1], lq = regPx(qs[e.t]), q2 = lq && ok(lq.price) ? { ok: true, live: true, last: lq.price, last_date: phaseNY() === "open" ? nyToday() : "same" } : null;
            var tr = trend(bars, q2);
            var w = [["Last week", flowWin(fh, 5)], ["Last month", flowWin(fh, 21)], ["Last quarter", flowWin(fh, 63)]];
            return '<div class="tvs-card"><div class="tvs-h"><a data-go="' + esc(e.t) + '" title="Open ' + esc(e.t) + ' on the chart">' + esc(e.t) + '</a><span class="tvs-mu">' + esc(e.why) + "</span>" +
              (lq ? '<span style="margin-left:auto">' + num(lq.price) + ' <span class="' + cls(lq.changePct) + '">' + pc(lq.changePct) + "</span></span>" : "") + "</div>" +
              (tr ? chips(tr) + crossLine(tr) : '<div class="tvs-mu">No daily bars for ' + esc(e.t) + "</div>") +
              (fh ? '<div class="tvs-g">' + w.map(function (z) { var v = z[1]; return "<div><small>" + z[0] + '</small><b class="' + (v ? cls(v.v) : "") + '">' + (v ? money(v.v) : "—") + "</b>" + (v && fh.aum ? '<span class="tvs-mu">' + pc(v.v / fh.aum * 100) + " of AUM</span>" : "") + "</div>"; }).join("") + "</div>" +
                '<div class="tvs-mu" style="margin-top:3px">' + (w[0][1] && w[0][1].v > 0 ? "Inflows" : "Outflows") + " last week · flows through " + fdate(fh.asof) + (fh.stale ? ' · <span class="tvd-dn">older than a week</span>' : "") + "</div>"
                : '<div class="tvs-mu" style="margin-top:4px">' + esc(e.t) + " is not covered by the fund-flow desk.</div>") + "</div>";
          }).join("") + '<div class="tvs-mu">Chips: price above (▲) or below (▼) each simple moving average. Flows are reported creations minus redemptions in US dollars, summed over the last 5 / 21 / 63 reported sessions.</div>';
        });
      }
      // 4. leaders: largest holdings of the industry ETF (or of this ETF)
      var srcEtf = isEtf ? x.T : etfs.length ? etfs[0].t : null;
      if (!srcEtf) return;
      getJ(PROXY + "/data/watchlist-insights/etf/" + encodeURIComponent(srcEtf) + ".json").then(function (ins) {
        var hold = ins && Array.isArray(ins.holdings) ? ins.holdings.filter(function (h) { return h && /^[A-Z][A-Z0-9.\-]{0,9}$/.test(String(h[0] || "")); }) : [];
        if (!hold.length && etfs[1] && !isEtf) return getJ(PROXY + "/data/watchlist-insights/etf/" + etfs[1].t + ".json").then(function (i2) { srcEtf = etfs[1].t; return i2 && i2.holdings ? i2.holdings : []; });
        return hold;
      }).then(function (hold) {
        if (!alive() || !hold || !hold.length) return;
        var top = hold.slice(0, 6).map(function (h) { return { t: String(h[0]).toUpperCase().replace("/", "."), n: h[1], w: h[2] }; });
        S("lead").innerHTML = "<h4>Industry leaders<small>top holdings of " + esc(srcEtf) + '</small></h4><div class="tvs-mu">Loading…</div>';
        Promise.all([quotes(top.map(function (l) { return l.t; }))].concat(top.map(function (l) { return Promise.all([barsVia(x, l.t), insiders(l.t)]); }))).then(function (a) {
          if (!alive()) return;
          var qs = a[0];
          S("lead").innerHTML = "<h4>Industry leaders<small>largest holdings of " + esc(srcEtf) + "</small></h4>" +
            "<table><tr><th>Symbol</th><th>Price</th><th>1D</th><th>1M</th><th>50D/200D</th><th>Insiders 90d</th></tr>" + top.map(function (l, i) {
              var bars = a[i + 1][0] || [], ins = a[i + 1][1], lq = regPx(qs[l.t]);
              var q2 = lq && ok(lq.price) ? { ok: true, live: true, last: lq.price, last_date: phaseNY() === "open" ? nyToday() : "same" } : null, tr = trend(bars, q2);
              var c = closes(bars, q2).c, m1 = c.length > 22 ? (c[c.length - 1] / c[c.length - 22] - 1) * 100 : null;
              var r50 = tr && tr.rows.filter(function (r) { return r.m === 50; })[0], r200 = tr && tr.rows.filter(function (r) { return r.m === 200; })[0];
              var st = function (r) { return r && r.v != null ? '<span class="' + (r.above ? "tvd-up" : "tvd-dn") + '">' + (r.above ? "▲" : "▼") + "</span>" : "—"; };
              var g = tr && tr.golden, gx = g && g.ago != null && g.ago <= 10 ? '<span class="tvs-new" title="' + esc(crossText(g, 50, 200)) + '">' + (g.up ? "GC" : "DC") + "</span>" : "";
              var net = ins && !ins.error ? (+ins.total_dollars_buy || 0) - (+ins.total_dollars_sell || 0) : null;
              return '<tr class="' + (l.t === x.T ? "me" : "") + '"><td><a class="tvs-s" data-go="' + esc(l.t) + '" title="' + esc((l.n || "") + (l.w != null ? " · " + (+l.w).toFixed(2) + "% of " + srcEtf : "")) + '">' + esc(l.t) + "</a></td><td>" + (lq ? num(lq.price) : c.length ? num(c[c.length - 1]) : "—") + '</td><td class="' + cls(lq && lq.changePct) + '">' + (lq ? pc(lq.changePct, 1) : "—") + '</td><td class="' + cls(m1) + '">' + pc(m1, 1) + "</td><td>" + st(r50) + " " + st(r200) + gx + '</td><td class="' + cls(net) + '" title="' + esc(ins && !ins.error ? lbl(ins.signal_label) + " · " + (ins.n_buys || 0) + " buys, " + (ins.n_sells || 0) + " sales" : "No Form 4 data") + '">' + (net == null ? "—" : net === 0 ? "none" : money(net)) + "</td></tr>";
            }).join("") + '</table><div class="tvs-mu" style="margin-top:4px">1M = last 21 sessions. ▲/▼ = price above/below the 50-day and 200-day SMA; GC/DC = golden/death cross in the last 10 sessions. Insiders = net open-market Form 4 dollars, last 90 days.</div>';
        });
      });
    });
  }

  root.JHTvSignals = { render: render, flowHist: flowHist, trend: trend, etfsFor: etfsFor, scan: scan, scanCached: scanCached, earnings: earnings, earnInfo: earnInfo, _flowWin: flowWin, _rs: rsCalc, _quarters: quarters };
})(window);
