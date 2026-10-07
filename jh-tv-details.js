/* jh-reskin-skip */
/* JustHodl — TradingView-style symbol details card for the chart watchlist.
 *
 * Rendered under the watchlist (jh-tv-watchlist.js calls JHTvDetails.render). Card order follows TradingView's
 * details pane: header icons, name / exchange / sector, price with market status, day and 52-week range bars,
 * the JustHodl insights strip (ETF flows, fund composition changes, 13F, valuation), key facts, key stats,
 * performance / returns, fund exposure, technicals gauge, earnings, dividends, income statement, seasonals.
 *
 * Every number comes from the data proxy (FMP read-through /fmp) or from the same daily bars the chart draws
 * (window.jhKlines). Nothing is estimated: a card whose source returns nothing is left out.
 */
(function (root) {
  "use strict";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var C = {};
  function getJ(u) { if (!C[u]) { C[u] = root.fetch(u).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }); } return C[u]; }
  function fmp(ep, t, extra) { return getJ(PROXY + "/fmp?ep=" + ep + "&symbol=" + encodeURIComponent(t) + (extra || "")).then(function (d) { return d && d.data != null ? d.data : null; }); }
  function first(a) { return Array.isArray(a) ? a[0] || null : a || null; }
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function ok(v) { return v != null && v !== "" && isFinite(+v); }
  function big(v, cur) { if (!ok(v)) return "—"; v = +v; var a = Math.abs(v), s = v < 0 ? "−" : ""; return s + (cur ? "$" : "") + (a >= 1e12 ? (a / 1e12).toFixed(2) + " T" : a >= 1e9 ? (a / 1e9).toFixed(2) + " B" : a >= 1e6 ? (a / 1e6).toFixed(2) + " M" : a >= 1e3 ? (a / 1e3).toFixed(2) + " K" : a.toFixed(2)); }
  function num(v, dp) { if (!ok(v)) return "—"; v = +v; var a = Math.abs(v); dp = dp != null ? dp : a >= 1000 ? 2 : a >= 1 ? 2 : a >= 0.01 ? 4 : 8; return v.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp }); }
  function pc(v, dp) { return ok(v) ? (v > 0 ? "+" : "") + (+v).toFixed(dp == null ? 2 : dp) + "%" : "—"; }
  function cls(v) { return v > 0 ? "tvd-up" : v < 0 ? "tvd-dn" : ""; }
  function day(t) { return new Date((typeof t === "number" ? t : Date.parse(t) / 1000) * 1000).toISOString().slice(0, 10); }
  function ts(t) { return typeof t === "number" ? t : Date.parse(t) / 1000; }
  function fdate(s) { if (!s) return "—"; var d = new Date(String(s).slice(0, 10) + "T12:00:00Z"); return isNaN(d) ? esc(s) : d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }); }

  // ------------------------------------------------------------------ styles
  function css() {
    if (document.getElementById("jh-tvd-css")) return;
    var s = document.createElement("style"); s.id = "jh-tvd-css";
    s.textContent = [
      "#jhwl .tvd{font-size:13px;color:var(--fg)}",
      "#jhwl .tvd .tvd-hd{display:flex;align-items:center;gap:8px;margin:0 0 6px}",
      "#jhwl .tvd .tvd-hd .tvd-lg{width:24px;height:24px;flex:0 0 24px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;color:#fff;font-size:12px;font-weight:600;overflow:hidden;background:#2a2e39}",
      "#jhwl .tvd .tvd-hd .tvd-lg img{width:100%;height:100%;object-fit:cover;background:#fff}",
      "#jhwl .tvd .tvd-hd b{font-size:15px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      "#jhwl .tvd .tvd-hd .tvd-ic{margin-left:auto;display:flex;gap:2px}",
      "#jhwl .tvd .tvd-hd .tvd-ic a,#jhwl .tvd .tvd-hd .tvd-ic button{width:28px;height:28px;display:inline-flex;align-items:center;justify-content:center;border-radius:4px;color:var(--mut);background:none;border:0;cursor:pointer;text-decoration:none}",
      "#jhwl .tvd .tvd-hd .tvd-ic a:hover,#jhwl .tvd .tvd-hd .tvd-ic button:hover{background:var(--bg2);color:var(--fg)}",
      "#jhwl .tvd .tvd-hd .tvd-ic svg{width:17px;height:17px}",
      "#jhwl .tvd .tvd-mn{position:absolute;right:12px;z-index:5;background:var(--bg);border:1px solid var(--bd);border-radius:6px;box-shadow:0 6px 18px rgba(0,0,0,.4);padding:4px 0;min-width:180px}",
      "#jhwl .tvd .tvd-mn button{display:block;width:100%;text-align:left;background:none;border:0;color:var(--fg);padding:7px 14px;cursor:pointer;font-size:13px}",
      "#jhwl .tvd .tvd-mn button:hover{background:var(--bg2)}",
      "#jhwl .tvd .tvd-nm2{font-size:13px;line-height:1.35}#jhwl .tvd .tvd-nm2 a{color:var(--fg);text-decoration:none}#jhwl .tvd .tvd-nm2 a:hover{color:var(--blue)}",
      "#jhwl .tvd .tvd-nm2 .tvd-ex{color:var(--mut)}#jhwl .tvd .tvd-sub{color:var(--mut);font-size:12px;margin:1px 0 8px}",
      "#jhwl .tvd .tvd-pr{display:flex;align-items:baseline;gap:6px;flex-wrap:wrap}#jhwl .tvd .tvd-pr .tvd-p{font-size:28px;font-weight:600;font-variant-numeric:tabular-nums}",
      "#jhwl .tvd .tvd-pr .tvd-u{font-size:11px;color:var(--mut)}#jhwl .tvd .tvd-pr .tvd-c{font-size:15px;font-variant-numeric:tabular-nums}",
      "#jhwl .tvd .tvd-st{font-size:12px;color:var(--mut);margin:2px 0 10px;display:flex;align-items:center;gap:5px}#jhwl .tvd .tvd-st i{width:7px;height:7px;border-radius:50%;background:#787b86;display:inline-block}",
      "#jhwl .tvd .tvd-xt{font-size:12px;margin:-6px 0 10px;font-variant-numeric:tabular-nums}#jhwl .tvd .tvd-xt>span:first-child{color:var(--mut)}#jhwl .tvd .tvd-xt .tvd-mu{color:var(--mut);font-size:11px}",
      "#jhwl .tvd .tvd-st.tvd-open i{background:#089981}#jhwl .tvd .tvd-st.tvd-ext i{background:#f0b90b}#jhwl .tvd .tvd-st.tvd-closed i{background:#787b86}",
      "#jhwl .tvd .tvd-rg{margin:6px 0 12px}#jhwl .tvd .tvd-rg .tvd-l{display:flex;justify-content:space-between;font-size:12px;font-variant-numeric:tabular-nums}",
      "#jhwl .tvd .tvd-rg .tvd-l span:nth-child(2){color:var(--mut);font-size:10px;letter-spacing:.06em;text-transform:uppercase}",
      "#jhwl .tvd .tvd-rg .tvd-b{position:relative;height:4px;background:var(--bd);border-radius:2px;margin:6px 0 2px}#jhwl .tvd .tvd-rg .tvd-b em{position:absolute;top:0;bottom:0;background:#089981;border-radius:2px}",
      "#jhwl .tvd .tvd-rg .tvd-b s{position:absolute;top:6px;width:0;height:0;border-left:5px solid transparent;border-right:5px solid transparent;border-bottom:6px solid var(--fg);transform:translateX(-5px)}",
      "#jhwl .tvd h4{font-size:14px;font-weight:600;margin:16px 0 8px;display:flex;align-items:center;gap:8px}#jhwl .tvd h4 small{margin-left:auto;font-weight:400;color:var(--mut);font-size:11px}",
      "#jhwl .tvd .tvd-tg{margin-left:auto;display:inline-flex;gap:8px}#jhwl .tvd .tvd-tg button{background:none;border:0;color:var(--mut);cursor:pointer;font-size:12px;padding:0}#jhwl .tvd .tvd-tg button.on{color:var(--blue);text-decoration:underline;text-underline-offset:4px}",
      "#jhwl .tvd .tvd-kf{background:linear-gradient(135deg,rgba(41,98,255,.12),rgba(156,39,176,.10));border:1px solid rgba(41,98,255,.25);border-radius:6px;padding:9px 10px;margin:4px 0 6px;font-size:12px;line-height:1.45}",
      "#jhwl .tvd .tvd-kf b{display:block;font-size:12px;margin-bottom:3px}#jhwl .tvd .tvd-kf a{color:var(--mut);text-decoration:none;font-size:12px}#jhwl .tvd .tvd-kf a:hover{color:var(--blue)}",
      "#jhwl .tvd .tvd-ks{display:grid;grid-template-columns:1fr auto;gap:7px 10px;font-size:13px}#jhwl .tvd .tvd-ks span:nth-child(even){text-align:right;font-variant-numeric:tabular-nums;font-weight:500}",
      "#jhwl .tvd .tvd-ks.tvd-more{margin-top:7px}#jhwl .tvd .tvd-chev{display:flex;justify-content:center;margin:6px 0}#jhwl .tvd .tvd-chev button{background:var(--bg2);border:0;border-radius:12px;color:var(--mut);cursor:pointer;width:34px;height:20px}",
      "#jhwl .tvd .tvd-tiles{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}#jhwl .tvd .tvd-tiles div{border-radius:4px;padding:6px 4px;text-align:center;background:var(--bg2)}",
      "#jhwl .tvd .tvd-tiles div b{display:block;font-size:13px;font-weight:600}#jhwl .tvd .tvd-tiles div small{font-size:11px;color:var(--fg);opacity:.85}",
      "#jhwl .tvd .tvd-tiles div.tvd-up{background:rgba(8,153,129,.22)}#jhwl .tvd .tvd-tiles div.tvd-up b{color:#22ab94}#jhwl .tvd .tvd-tiles div.tvd-dn{background:rgba(242,54,69,.20)}#jhwl .tvd .tvd-tiles div.tvd-dn b{color:#f7525f}",
      "#jhwl .tvd .tvd-pill{display:flex;justify-content:center;margin:10px 0 2px}#jhwl .tvd .tvd-pill button,#jhwl .tvd .tvd-pill a{background:var(--bg2);border:0;border-radius:14px;color:var(--fg);padding:5px 14px;font-size:12px;cursor:pointer;text-decoration:none}",
      "#jhwl .tvd .tvd-pill button:hover,#jhwl .tvd .tvd-pill a:hover{background:var(--bd)}",
      "#jhwl .tvd .tvd-dn2{display:flex;justify-content:center;margin:4px 0 8px}#jhwl .tvd .tvd-lst{display:grid;grid-template-columns:1fr auto;gap:6px 10px;font-size:13px}",
      "#jhwl .tvd .tvd-lst span:nth-child(even){text-align:right;font-variant-numeric:tabular-nums}#jhwl .tvd .tvd-lst i{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px}",
      "#jhwl .tvd .tvd-gauge{display:flex;flex-direction:column;align-items:center}#jhwl .tvd .tvd-gauge b{font-size:17px;margin-top:-6px}",
      "#jhwl .tvd .tvd-tdet{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:8px;font-size:12px}#jhwl .tvd .tvd-tdet div{background:var(--bg2);border-radius:4px;padding:6px 8px}",
      "#jhwl .tvd svg text{font-family:inherit}#jhwl .tvd .tvd-leg{display:flex;justify-content:center;gap:12px;font-size:11px;color:var(--mut);margin-top:2px}#jhwl .tvd .tvd-leg i{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:4px}",
      "#jhwl .tvd .tvd-mu{color:var(--mut);font-size:12px}#jhwl .tvd .tvd-up{color:#22ab94}#jhwl .tvd .tvd-dn{color:#f7525f}"
    ].join("");
    document.head.appendChild(s);
  }

  // ------------------------------------------------------------------ helpers over bars
  function closeAt(b, t) { for (var i = b.length - 1; i >= 0; i--) if (ts(b[i].time) <= t) return +b[i].close; return null; }
  function perf(b, P) {
    var L = b[b.length - 1], lt = ts(L.time), out = {};
    var spans = { "1W": 7, "1M": 30.4, "3M": 91.3, "6M": 182.6, "1Y": 365.25, "3Y": 1095.75, "5Y": 1826.25, "10Y": 3652.5 };
    Object.keys(spans).forEach(function (k) { var t = lt - spans[k] * 86400; if (ts(b[0].time) > t + 5 * 86400) { out[k] = null; return; } var c = closeAt(b, t); out[k] = c ? (L.close / c - 1) * 100 : null; });
    var y = new Date(lt * 1000).getUTCFullYear(), yc = closeAt(b, Date.UTC(y, 0, 1) / 1000 - 1);
    out.YTD = yc ? (L.close / yc - 1) * 100 : null;
    return out;
  }
  function totalRet(b, divs, days) {
    // price change with each distribution reinvested at that ex-date's close
    var L = b[b.length - 1], lt = ts(L.time), t0 = days === "YTD" ? Date.UTC(new Date(lt * 1000).getUTCFullYear(), 0, 1) / 1000 - 1 : lt - days * 86400;
    if (ts(b[0].time) > t0 + 5 * 86400) return null;
    var c0 = closeAt(b, t0); if (!c0) return null;
    var f = 1; (divs || []).forEach(function (d) { var t = Date.parse(d.date) / 1000; if (t > t0 && t <= lt) { var c = closeAt(b, t); if (c && ok(d.dividend)) f *= 1 + d.dividend / c; } });
    return (L.close / c0 * f - 1) * 100;
  }
  function sma(a, n, i) { if (i < n - 1) return null; var s = 0; for (var k = i - n + 1; k <= i; k++) s += a[k]; return s / n; }
  function emaArr(a, n) { var k = 2 / (n + 1), out = [], e = null; for (var i = 0; i < a.length; i++) { e = e == null ? a[i] : a[i] * k + e * (1 - k); out.push(i >= n - 1 ? e : null); } return out; }
  function rsiArr(a, n) {
    var out = new Array(a.length).fill(null), g = 0, l = 0;
    for (var i = 1; i < a.length; i++) {
      var d = a[i] - a[i - 1], up = Math.max(d, 0), dn = Math.max(-d, 0);
      if (i <= n) { g += up / n; l += dn / n; if (i === n) out[i] = l ? 100 - 100 / (1 + g / l) : 100; }
      else { g = (g * (n - 1) + up) / n; l = (l * (n - 1) + dn) / n; out[i] = l ? 100 - 100 / (1 + g / l) : 100; }
    }
    return out;
  }
  // TradingView's technical rating: moving averages (price above → buy) and oscillators, averaged.
  function technicals(b) {
    if (b.length < 60) return null;
    var c = b.map(function (x) { return +x.close; }), h = b.map(function (x) { return +(x.high != null ? x.high : x.close); }), lo = b.map(function (x) { return +(x.low != null ? x.low : x.close); });
    var i = c.length - 1, px = c[i], ma = [], os = [];
    [10, 20, 30, 50, 100, 200].forEach(function (n) {
      var s = sma(c, n, i); if (s != null) ma.push(["SMA " + n, px > s ? 1 : px < s ? -1 : 0, s]);
      var e = emaArr(c, n)[i]; if (e != null) ma.push(["EMA " + n, px > e ? 1 : px < e ? -1 : 0, e]);
    });
    var r = rsiArr(c, 14); if (r[i] != null && r[i - 1] != null) os.push(["RSI (14)", r[i] < 30 && r[i] > r[i - 1] ? 1 : r[i] > 70 && r[i] < r[i - 1] ? -1 : 0, r[i]]);
    var K = [], n = 14; for (var j = 0; j <= i; j++) { if (j < n - 1) { K.push(null); continue; } var hh = -Infinity, ll = Infinity; for (var q = j - n + 1; q <= j; q++) { hh = Math.max(hh, h[q]); ll = Math.min(ll, lo[q]); } K.push(hh > ll ? (c[j] - ll) / (hh - ll) * 100 : 50); }
    var Ks = K.map(function (_, j) { if (j < n + 1) return null; return (K[j] + K[j - 1] + K[j - 2]) / 3; }), Ds = Ks.map(function (_, j) { if (Ks[j - 2] == null) return null; return (Ks[j] + Ks[j - 1] + Ks[j - 2]) / 3; });
    if (Ks[i] != null && Ds[i] != null) os.push(["Stochastic %K (14, 3, 3)", Ks[i] < 20 && Ks[i] > Ds[i] ? 1 : Ks[i] > 80 && Ks[i] < Ds[i] ? -1 : 0, Ks[i]]);
    var tp = c.map(function (v, j) { return (v + h[j] + lo[j]) / 3; });
    function cci(j) { var m = sma(tp, 20, j); if (m == null) return null; var md = 0; for (var q = j - 19; q <= j; q++) md += Math.abs(tp[q] - m); md /= 20; return md ? (tp[j] - m) / (0.015 * md) : 0; }
    var cc = cci(i), cp = cci(i - 1); if (cc != null && cp != null) os.push(["CCI (20)", cc < -100 && cc > cp ? 1 : cc > 100 && cc < cp ? -1 : 0, cc]);
    var mom = c[i] - c[i - 10], momp = c[i - 1] - c[i - 11]; os.push(["Momentum (10)", mom > momp ? 1 : mom < momp ? -1 : 0, mom]);
    var e12 = emaArr(c, 12), e26 = emaArr(c, 26), macd = c.map(function (_, j) { return e12[j] != null && e26[j] != null ? e12[j] - e26[j] : null; });
    var mv = macd.filter(function (x) { return x != null; }), sig = emaArr(mv, 9), ms = sig[sig.length - 1], mm = mv[mv.length - 1];
    if (ms != null) os.push(["MACD (12, 26)", mm > ms ? 1 : mm < ms ? -1 : 0, mm]);
    function wr(j) { var hh = -Infinity, ll = Infinity; for (var q = j - 13; q <= j; q++) { hh = Math.max(hh, h[q]); ll = Math.min(ll, lo[q]); } return hh > ll ? (hh - c[j]) / (hh - ll) * -100 : -50; }
    var w = wr(i), wp = wr(i - 1); os.push(["Williams %R (14)", w < -80 && w > wp ? 1 : w > -20 && w < wp ? -1 : 0, w]);
    function avg(a) { return a.length ? a.reduce(function (s, x) { return s + x[1]; }, 0) / a.length : 0; }
    var rm = avg(ma), ro = avg(os), all = (rm + ro) / 2;
    return { all: all, ma: ma, os: os, rm: rm, ro: ro };
  }
  function label(v) { return v > 0.5 ? "Strong buy" : v > 0.1 ? "Buy" : v >= -0.1 ? "Neutral" : v >= -0.5 ? "Sell" : "Strong sell"; }
  function gauge(v) {
    var W = 150, H = 86, cx = 75, cy = 76, R = 58, segs = [["#f23645", -1, -0.5], ["#f7525f", -0.5, -0.1], ["#787b86", -0.1, 0.1], ["#5b9cf6", 0.1, 0.5], ["#2962ff", 0.5, 1]];
    function pt(x, r) { var a = Math.PI * (1 - (x + 1) / 2); return [cx + r * Math.cos(a), cy - r * Math.sin(a)]; }
    var arcs = segs.map(function (s) { var a = pt(s[1] + 0.03, R), b = pt(s[2] - 0.03, R); return '<path d="M' + a[0].toFixed(1) + " " + a[1].toFixed(1) + " A" + R + " " + R + " 0 0 1 " + b[0].toFixed(1) + " " + b[1].toFixed(1) + '" stroke="' + s[0] + '" stroke-width="6" fill="none" stroke-linecap="round"/>'; }).join("");
    var n = pt(Math.max(-1, Math.min(1, v)), R - 16);
    return '<svg width="' + W + '" height="' + H + '" viewBox="0 0 ' + W + " " + H + '" aria-hidden="true">' + arcs + '<line x1="' + cx + '" y1="' + cy + '" x2="' + n[0].toFixed(1) + '" y2="' + n[1].toFixed(1) + '" stroke="var(--fg)" stroke-width="3" stroke-linecap="round"/><circle cx="' + cx + '" cy="' + cy + '" r="3" fill="var(--fg)"/></svg>';
  }
  function donut(parts, centre, size) {
    size = size || 96; var r = size / 2 - 8, cx = size / 2, tot = parts.reduce(function (s, p) { return s + Math.max(0, p[1]); }, 0) || 1, a0 = -Math.PI / 2, out = "";
    parts.forEach(function (p) {
      var f = Math.max(0, p[1]) / tot; if (f <= 0) return;
      if (f >= 0.9999) { out += '<circle cx="' + cx + '" cy="' + cx + '" r="' + r + '" stroke="' + p[2] + '" stroke-width="13" fill="none"/>'; return; }
      var a1 = a0 + f * Math.PI * 2, x0 = cx + r * Math.cos(a0), y0 = cx + r * Math.sin(a0), x1 = cx + r * Math.cos(a1), y1 = cx + r * Math.sin(a1);
      out += '<path d="M' + x0.toFixed(2) + " " + y0.toFixed(2) + " A" + r + " " + r + " 0 " + (f > 0.5 ? 1 : 0) + " 1 " + x1.toFixed(2) + " " + y1.toFixed(2) + '" stroke="' + p[2] + '" stroke-width="13" fill="none"/>';
      a0 = a1;
    });
    return '<svg width="' + size + '" height="' + size + '" viewBox="0 0 ' + size + " " + size + '" aria-hidden="true"><circle cx="' + cx + '" cy="' + cx + '" r="' + r + '" stroke="var(--bd)" stroke-width="13" fill="none"/>' + out + (centre ? '<text x="' + cx + '" y="' + (cx + 5) + '" text-anchor="middle" fill="var(--fg)" font-size="15" font-weight="600">' + esc(centre) + "</text>" : "") + "</svg>";
  }
  var PAL = ["#ff9800", "#7e57c2", "#2962ff", "#26a69a", "#ef5350", "#ab47bc", "#66bb6a", "#ffca28", "#8d6e63", "#78909c"];
  var REGION = {
    "North America": ["United States", "Canada", "Bermuda", "Puerto Rico"],
    "Latin America": ["Mexico", "Brazil", "Chile", "Colombia", "Peru", "Argentina", "Panama", "Uruguay", "Costa Rica", "Bahamas", "Virgin Islands (British)", "British Virgin Islands"],
    "Europe": ["United Kingdom", "Germany", "France", "Netherlands", "Switzerland", "Ireland", "Spain", "Italy", "Sweden", "Denmark", "Norway", "Finland", "Belgium", "Austria", "Portugal", "Luxembourg", "Poland", "Greece", "Czech Republic", "Hungary", "Jersey", "Guernsey", "Isle of Man", "Monaco", "Iceland", "Russia", "Turkey", "Cyprus", "Malta", "Romania"],
    "Asia": ["China", "Japan", "Taiwan", "South Korea", "Korea", "Korea, Republic of", "Hong Kong", "India", "Singapore", "Indonesia", "Thailand", "Malaysia", "Philippines", "Vietnam", "Cayman Islands", "Macau", "Pakistan", "Kazakhstan"],
    "Middle East": ["Israel", "Saudi Arabia", "United Arab Emirates", "Qatar", "Kuwait", "Bahrain", "Oman", "Jordan"],
    "Africa": ["South Africa", "Egypt", "Nigeria", "Morocco", "Kenya", "Mauritius"],
    "Oceania": ["Australia", "New Zealand", "Papua New Guinea"]
  };
  function regionOf(c) { for (var k in REGION) if (REGION[k].indexOf(c) >= 0) return k; return null; }
  function pctNum(v) { return ok(v) ? +v : ok(String(v || "").replace("%", "")) ? +String(v).replace("%", "") : null; }

  // US cash-equity session status in New York time (weekends closed; exchange holidays are not modelled)
  function usSession() {
    try {
      var p = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "numeric", hour12: false }).formatToParts(new Date());
      var g = {}; p.forEach(function (x) { g[x.type] = x.value; });
      var m = (+g.hour % 24) * 60 + +g.minute;
      if (g.weekday === "Sat" || g.weekday === "Sun") return ["closed", "Market closed"];
      if (m >= 570 && m < 960) return ["open", "Market open"];
      if (m >= 240 && m < 570) return ["ext", "Pre-market"];
      if (m >= 960 && m < 1200) return ["ext", "Post-market"];
      return ["closed", "Market closed"];
    } catch (e) { return ["closed", ""]; }
  }

  // ------------------------------------------------------------------ SVG mini charts
  function linesSvg(series, W, H, opts) {
    opts = opts || {}; var lo = Infinity, hi = -Infinity;
    series.forEach(function (s) { s.pts.forEach(function (p) { lo = Math.min(lo, p[1]); hi = Math.max(hi, p[1]); }); });
    if (!isFinite(lo)) return "";
    if (hi === lo) { hi += 1; lo -= 1; }
    var pad = (hi - lo) * 0.08; lo -= pad; hi += pad;
    var X = function (x) { return 4 + x * (W - 44); }, Y = function (v) { return 6 + (hi - v) / (hi - lo) * (H - 26); };
    var grid = "", k; for (k = 0; k <= 4; k++) { var v = lo + (hi - lo) * k / 4; grid += '<line x1="4" x2="' + (W - 40) + '" y1="' + Y(v).toFixed(1) + '" y2="' + Y(v).toFixed(1) + '" stroke="var(--bd)" stroke-width=".5"/><text x="' + (W - 36) + '" y="' + (Y(v) + 3).toFixed(1) + '" fill="var(--mut)" font-size="9">' + esc(opts.fmt ? opts.fmt(v) : v.toFixed(1)) + "</text>"; }
    var xl = (opts.xlabels || []).map(function (l) { return '<text x="' + X(l[0]).toFixed(1) + '" y="' + (H - 4) + '" fill="var(--mut)" font-size="9" text-anchor="middle">' + esc(l[1]) + "</text>" + (opts.vgrid ? '<line x1="' + X(l[0]).toFixed(1) + '" x2="' + X(l[0]).toFixed(1) + '" y1="4" y2="' + (H - 18) + '" stroke="var(--bd)" stroke-dasharray="2 3" stroke-width=".6"/>' : ""); }).join("");
    var paths = series.map(function (s) { return '<path d="' + s.pts.map(function (p, i) { return (i ? "L" : "M") + X(p[0]).toFixed(1) + " " + Y(p[1]).toFixed(1); }).join("") + '" stroke="' + s.color + '" stroke-width="' + (s.w || 1.4) + '" fill="none"/>'; }).join("");
    return '<svg viewBox="0 0 ' + W + " " + H + '" width="100%" height="' + H + '" preserveAspectRatio="none" aria-hidden="true">' + grid + xl + paths + "</svg>";
  }

  // ------------------------------------------------------------------ render
  // price header (re-painted in place on every live tick by update())
  function pxHtml(q, T) {
    var okq = q && q.ok, ses = T ? usSession() : null;
    var stale = okq && !q.live && q.last_date && (Date.now() - Date.parse(q.last_date + "T23:59:59Z")) > 4 * 864e5;
    var tfmt = function (ms) { try { return new Date(ms).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }) + " ET"; } catch (e) { return ""; } };
    var extH = okq && q.ext && ok(q.ext.px) ? '<div class="tvd-xt"><span>' + esc(q.ext.kind) + '</span> <b>' + num(q.ext.px) + '</b> <span class="' + cls(q.ext.chg) + '">' + (q.ext.chg > 0 ? "+" : "") + num(q.ext.chg) + " " + pc(q.ext.pct) + '</span> <span class="tvd-mu">' + tfmt(q.ext.ts) + "</span></div>" : "";
    var liveTxt = okq && q.live && q.reg_src ? " · at close" : okq && q.live ? " · live " + (function () { try { return new Date(q.live_ts).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }) + " ET"; } catch (e) { return ""; } })() : "";
    return okq ? '<div class="tvd-pr"><span class="tvd-p">' + num(q.last) + '</span><span class="tvd-u">' + esc(q.unit && q.unit.length < 8 ? q.unit : T ? "USD" : "") + '</span><span class="tvd-c ' + cls(q.chg) + '">' + (q.chg != null ? (q.chg > 0 ? "+" : "") + num(q.chg) : "") + " " + pc(q.chg_pct) + "</span></div>" +
      '<div class="tvd-st ' + "tvd-" + (ses ? ses[0] : "closed") + '"><i></i>' + (ses ? esc(ses[1]) + (q.live ? '<span title="' + esc((q.reg_src ? "Price is the " + q.reg_src + "; " : "") + (q.live_src || "")) + (q.bar_last_date ? " · stored daily history through " + esc(q.bar_last_date) : "") + '">' + esc(liveTxt) + "</span>" : q.last_date ? " · last bar " + fdate(q.last_date) : "") : "As of " + fdate(q.last_date) + (q.freq ? " · " + esc(q.freq) : "")) + (stale ? ' · <span title="The newest observation is more than four days old">delayed</span>' : "") + "</div>" + extH
      : '<div class="tvd-mu" style="margin:6px 0 10px">' + (q && q.pending ? "Loading the full history the chart uses…" : q && q.error ? "No quote from the warehouse for this symbol: " + esc(String(q.error).split("(")[0].slice(0, 140)) : "Loading quote…") + "</div>";
  }
  function update(el, q) {
    var px = el.querySelector('.tvd [data-k="px"]'); if (!px) return false;
    px.innerHTML = pxHtml(q, el._tvdT);
    if (el._tvdRanges) el._tvdRanges(q);
    return true;
  }
  var SEQ = 0;
  function render(el, x) {
    css();
    var seq = ++SEQ, T = x.info, q = x.q || {}, okq = q && q.ok;
    el.setAttribute("data-tvd", x.id); el._tvdT = T; el._tvdRanges = null;
    var price = pxHtml(q, T);
    var ICON = {
      grid: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><rect x="4" y="4" width="7" height="7" rx="1"/><rect x="13" y="4" width="7" height="7" rx="1"/><rect x="4" y="13" width="7" height="7" rx="1"/><rect x="13" y="13" width="7" height="7" rx="1"/></svg>',
      edit: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M4 20h4L19 9l-4-4L4 16v4z"/><path d="M14 6l4 4"/></svg>',
      dots: '<svg viewBox="0 0 24 24" fill="currentColor"><circle cx="5" cy="12" r="1.7"/><circle cx="12" cy="12" r="1.7"/><circle cx="19" cy="12" r="1.7"/></svg>',
      ext: '<svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:-1px"><path d="M14 4h6v6M20 4l-9 9M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"/></svg>'
    };
    var page = T ? "/symbol.html?s=" + encodeURIComponent(T) : null;
    el.innerHTML = '<div class="tvd">' +
      '<div class="tvd-hd"><span class="tvd-lg" style="background:' + esc(x.color || "#2a2e39") + '">' + (T ? '<img alt="" src="https://images.financialmodelingprep.com/symbol/' + encodeURIComponent(T) + '.png" onerror="this.remove()">' : "") + esc(String(x.sym || "?").charAt(0).toUpperCase()) + "</span><b title=\"" + esc(x.id) + '">' + esc(x.alias || x.sym) + "</b>" +
        '<span class="tvd-ic">' + (page ? '<a href="' + page + '" target="_blank" rel="noopener" title="Open the full symbol page: financials, holdings, ownership, flows">' + ICON.grid + "</a>" : "") +
        '<button type="button" data-tvd="rename" title="Rename & colour tag">' + ICON.edit + '</button><button type="button" data-tvd="menu" title="More" aria-haspopup="menu">' + ICON.dots + "</button></span></div>" +
      '<div class="tvd-mn" hidden></div>' +
      '<div class="tvd-nm2" data-k="name">' + (page ? '<a href="' + page + '" target="_blank" rel="noopener">' + esc(x.name || x.id) + " " + ICON.ext + "</a>" : esc(x.name || x.id)) + (x.alias ? ' <span class="tvd-ex">(' + esc(x.sym) + ")</span>" : "") + '<span class="tvd-ex" data-k="exch"></span></div>' +
      '<div class="tvd-sub" data-k="sub">' + esc(x.id) + "</div>" +
      '<div data-k="px">' + price + "</div>" +
      '<div data-k="ranges"></div>' +
      (x.insights ? '<div class="wl-ins" data-ins="' + esc(x.id) + '">' + x.insights + "</div>" : "") +
      '<div data-k="sig"></div>' +
      '<div data-k="facts"></div><div data-k="stats"></div><div data-k="perf"></div><div data-k="expo"></div><div data-k="tech"></div>' +
      '<div data-k="earn"></div><div data-k="divs"></div><div data-k="inc"></div><div data-k="seas"></div><div data-k="extra"></div>' +
      "</div>";
    var root0 = el.querySelector(".tvd");
    function slot(k) { return root0.querySelector('[data-k="' + k + '"]'); }
    function alive() { return seq === SEQ && el.isConnected && el.getAttribute("data-tvd") === x.id; }
    // header menu
    root0.addEventListener("click", function (e) {
      var b = e.target.closest("[data-tvd]"); if (!b) return;
      var a = b.getAttribute("data-tvd"), mn = root0.querySelector(".tvd-mn");
      if (a === "menu") {
        e.stopPropagation();
        if (!mn.hidden) { mn.hidden = true; return; }
        mn.className = "tvd-mn mn"; mn.hidden = false;
        mn.innerHTML = [["chart", "Open on chart"], ["compare", "Compare on chart"], ["flag", "Colour tag…"], ["rename", "Rename…"], page ? ["page", "Full symbol page ↗"] : null, ["remove", "Remove from watchlist"]].filter(Boolean).map(function (m) { return '<button type="button" data-tvd="' + m[0] + '">' + m[1] + "</button>"; }).join("");
        var close = function (ev) { if (!mn.contains(ev.target)) { mn.hidden = true; document.removeEventListener("mousedown", close, true); } };
        document.addEventListener("mousedown", close, true);
        return;
      }
      if (mn) mn.hidden = true;
      if (a === "page" && page) { root.open(page, "_blank", "noopener"); return; }
      if (a === "seasonals") { var tab = Array.from(document.querySelectorAll("button,a,[role=tab]")).filter(function (n) { return /^\s*Season/.test(n.textContent || "") && !root0.contains(n); })[0]; if (tab) tab.click(); return; }
      if (x.act && x.act[a]) x.act[a]();
    });

    // ---------------- bars-driven cards (ranges, performance, technicals, seasonals)
    var barsP = x.bars ? x.bars() : Promise.resolve(null);
    var profP = T ? fmp("profile", T).then(first) : Promise.resolve(null);
    var isEtfP = profP.then(function (p) { return !!(p && (p.isEtf || p.isFund)); });
    var divP = T ? fmp("dividends", T).then(function (d) { return Array.isArray(d) ? d : []; }) : Promise.resolve([]);

    // moving averages / crosses, insiders, industry & sector ETFs, industry leaders (jh-tv-signals.js)
    Promise.resolve(x.sigReady || null).then(function (S0) {
      S0 = S0 || root.JHTvSignals; if (!S0 || !alive() || !slot("sig")) return;
      try { S0.render(slot("sig"), { T: T, sym: x.alias || x.sym, q: q, bars: barsP, profile: profP, barsFor: x.barsFor || function () { return Promise.resolve([]); }, go: x.act && x.act.goto }); } catch (e) {}
    });
    barsP.then(function (b) {
      if (!alive()) return;
      b = (b || []).filter(function (r) { return r && r.time != null && ok(r.close); });
      if (!b.length) return;
      var L0 = b[b.length - 1], lt = ts(L0.time), y1 = b.filter(function (r) { return ts(r.time) > lt - 365.25 * 86400; });
      var lo52b = Math.min.apply(null, y1.map(function (r) { return +(r.low != null ? r.low : r.close); })), hi52b = Math.max.apply(null, y1.map(function (r) { return +(r.high != null ? r.high : r.close); }));
      var last = okq ? +q.last : +L0.close;
      function ranges(q) {
      var okq = q && q.ok, L = L0, lo52 = lo52b, hi52 = hi52b;
      var dl = +(L.low != null ? L.low : L.close), dh = +(L.high != null ? L.high : L.close), last = okq ? +q.last : +L.close, dLbl = fdate(day(L.time));
      // live session: today's range from the snapshot, and the 52-week range includes today
      if (okq && q.live && q.day && q.day.h > 0 && q.day.l > 0 && q.day.h >= q.day.l) {
        dl = +q.day.l; dh = +q.day.h; dLbl = fdate(q.last_date);
        L = { time: L.time, high: dh, low: dl, close: last };
      }
      function bar(lo, hi, lbl, fill) {
        var f = hi > lo ? Math.max(0, Math.min(1, (last - lo) / (hi - lo))) : 0.5;
        return '<div class="tvd-rg"><div class="tvd-l"><span>' + num(lo) + "</span><span>" + lbl + "</span><span>" + num(hi) + '</span></div><div class="tvd-b">' + (fill ? '<em style="left:0;width:' + (f * 100).toFixed(1) + '%"></em>' : "") + '<s style="left:' + (f * 100).toFixed(1) + '%"></s></div></div>';
      }
      if (okq && q.live) { lo52 = Math.min(lo52, dl); hi52 = Math.max(hi52, dh); }
      var hasDay = L.high != null && L.low != null && dh > dl;
      var rs = slot("ranges"); if (rs) rs.innerHTML = (hasDay ? bar(dl, dh, (T ? "Day's range" : "Latest bar range") + " · " + dLbl, true) : "") + (y1.length > 20 ? bar(lo52, hi52, "52wk range", false) : "");
      }
      ranges(q); el._tvdRanges = ranges;
      // performance / returns
      var pf = perf(b, last);
      isEtfP.then(function (etf) {
        if (!alive()) return;
        var keys = etf ? ["1M", "3M", "YTD", "1Y", "3Y", "5Y"] : ["1W", "1M", "3M", "6M", "YTD", "1Y"];
        var span = { "1M": 30.4, "3M": 91.3, "YTD": "YTD", "1Y": 365.25, "3Y": 1095.75, "5Y": 1826.25 };
        function tiles(tot, divs) { return '<div class="tvd-tiles">' + keys.map(function (k) { var v = tot ? totalRet(b, divs, span[k]) : pf[k]; return '<div class="' + cls(v) + '"><b>' + (ok(v) ? (+v).toFixed(2) + "%" : "—") + "</b><small>" + k + "</small></div>"; }).join("") + "</div>"; }
        var host = slot("perf");
        host.innerHTML = "<h4>" + (etf ? "Returns" : "Performance") + (etf ? '<span class="tvd-tg"><button type="button" data-ret="p" class="on">Price</button><button type="button" data-ret="t">Total</button></span>' : "") + "</h4>" + tiles(false);
        if (etf) host.querySelector(".tvd-tg").onclick = function (e) {
          var t = e.target.closest("[data-ret]"); if (!t) return;
          host.querySelectorAll("[data-ret]").forEach(function (z) { z.classList.toggle("on", z === t); });
          var tot = t.getAttribute("data-ret") === "t";
          divP.then(function (dv) { if (!alive()) return; host.querySelector(".tvd-tiles").outerHTML = tiles(tot, dv); });
        };
      });
      // technicals
      var tr = technicals(b);
      if (tr) {
        var tH = slot("tech");
        tH.innerHTML = '<h4>Technicals<small>daily · ' + (tr.ma.length + tr.os.length) + " indicators</small></h4>" + '<div class="tvd-gauge">' + gauge(tr.all) + "<b>" + label(tr.all) + '</b></div><div class="tvd-pill"><button type="button" data-tvd="techmore">More technicals</button></div><div class="tvd-tdx" hidden></div>';
        tH.querySelector('[data-tvd="techmore"]').onclick = function (e) {
          e.stopPropagation(); var d = tH.querySelector(".tvd-tdx");
          if (!d.hidden) { d.hidden = true; return; }
          function cnt(a, s) { return a.filter(function (z) { return z[1] === s; }).length; }
          function row(z) { return "<div><span>" + esc(z[0]) + '</span><span style="float:right" class="' + (z[1] > 0 ? "tvd-up" : z[1] < 0 ? "tvd-dn" : "tvd-mu") + '">' + (z[1] > 0 ? "Buy" : z[1] < 0 ? "Sell" : "Neutral") + "</span></div>"; }
          d.hidden = false;
          d.innerHTML = '<div class="tvd-tdet"><div><b>Oscillators: ' + label(tr.ro) + '</b><br><span class="tvd-mu">Sell ' + cnt(tr.os, -1) + " · Neutral " + cnt(tr.os, 0) + " · Buy " + cnt(tr.os, 1) + '</span></div><div><b>Moving averages: ' + label(tr.rm) + '</b><br><span class="tvd-mu">Sell ' + cnt(tr.ma, -1) + " · Neutral " + cnt(tr.ma, 0) + " · Buy " + cnt(tr.ma, 1) + "</span></div></div>" +
            '<div class="tvd-tdet">' + tr.os.map(row).join("") + tr.ma.map(row).join("") + "</div>" +
            '<p class="tvd-mu" style="margin:6px 0 0">Same rules as TradingView\'s technical rating, computed here from the daily bars the chart draws. A rating is not a recommendation.</p>';
        };
      }
      // seasonals: cumulative % from each year's start, last three calendar years
      var yNow = new Date(lt * 1000).getUTCFullYear(), cols3 = ["#2962ff", "#4caf50", "#ff9800"], ser = [];
      [yNow, yNow - 1, yNow - 2].forEach(function (y, i) {
        var base = closeAt(b, Date.UTC(y, 0, 1) / 1000 - 1); if (!base) return;
        var t0 = Date.UTC(y, 0, 1) / 1000, t1 = Date.UTC(y + 1, 0, 1) / 1000, pts = [];
        b.forEach(function (r) { var t = ts(r.time); if (t >= t0 && t < t1) pts.push([(t - t0) / (t1 - t0), (r.close / base - 1) * 100]); });
        if (pts.length > 3) ser.push({ y: y, color: cols3[i], pts: pts });
      });
      if (ser.length) {
        var mo = [["Jan", 0], ["Mar", 59 / 365], ["May", 120 / 365], ["Jul", 181 / 365], ["Sep", 243 / 365], ["Nov", 304 / 365]];
        slot("seas").innerHTML = "<h4>Seasonals<small>% from each year's start</small></h4>" + linesSvg(ser.slice().reverse(), 300, 130, { xlabels: mo.map(function (m) { return [m[1], m[0]]; }), vgrid: true, fmt: function (v) { return v.toFixed(0) + "%"; } }) +
          '<div class="tvd-leg">' + ser.map(function (s) { return '<span><i style="background:' + s.color + '"></i>' + s.y + "</span>"; }).join("") + '</div><div class="tvd-pill"><button type="button" data-tvd="seasonals">More seasonals</button></div>';
      }
      if (!T) slot("extra").innerHTML = '<div class="tvd-ks" style="margin-top:14px"><span>Symbol</span><span>' + esc(x.id) + "</span><span>History</span><span>" + fdate(day(b[0].time)) + " → " + fdate(day(L.time)) + "</span><span>Observations</span><span>" + b.length.toLocaleString("en-US") + "</span></div>";
    });

    if (!T) return;

    // ---------------- FMP-driven cards
    Promise.all([profP, fmp("ratios-ttm", T).then(first), fmp("key-metrics-ttm", T).then(first), fmp("earnings", T), isEtfP.then(function (e) { return e ? fmp("etf/info", T).then(first) : null; }), fmp("shares-float", T).then(first)]).then(function (a) {
      if (!alive()) return;
      var p = a[0] || {}, r = a[1] || {}, k = a[2] || {}, E = Array.isArray(a[3]) ? a[3] : [], ei = a[4], sf = a[5] || {}, etf = !!(p.isEtf || p.isFund);
      if (p.companyName) { var nmA = slot("name").querySelector("a"); if (nmA) nmA.firstChild.nodeValue = p.companyName + " "; }
      if (p.exchange) slot("exch").textContent = " · " + p.exchange;
      slot("sub").textContent = etf ? [ei && ei.assetClass ? ei.assetClass + " ETF" : "ETF", ei && ei.etfCompany].filter(Boolean).join(" • ") : [p.sector, p.industry].filter(Boolean).join(" • ") || x.id;
      // key facts
      var desc = String((ei && ei.description) || p.description || "").trim();
      if (desc) { var short = desc.length > 260 ? desc.slice(0, 250).replace(/\s+\S*$/, "") + "…" : desc; slot("facts").innerHTML = '<div class="tvd-kf"><b>✦ Key facts</b>' + esc(short) + (page ? '<br><a href="' + page + '" target="_blank" rel="noopener">Keep reading ›</a>' : "") + "</div>"; }
      // key stats
      var now = Date.now(), nxt = E.filter(function (e) { return e.epsActual == null && Date.parse(e.date) >= now - 864e5; }).sort(function (x1, x2) { return Date.parse(x1.date) - Date.parse(x2.date); })[0];
      var inDays = nxt ? Math.max(0, Math.round((Date.parse(nxt.date + "T12:00:00Z") - now) / 864e5)) : null;
      var dy = ok(r.dividendYieldTTM) ? r.dividendYieldTTM * 100 : null;
      var rows = [];
      if (!etf) rows.push(["Next earnings report", nxt ? (inDays === 0 ? "Today" : "In " + inDays + " day" + (inDays === 1 ? "" : "s")) + ' <span class="tvd-mu">(' + fdate(nxt.date) + ")</span>" : "—"]);
      rows.push(["Volume", big(okq && q.live && q.day && q.day.v ? q.day.v : p.volume)], ["Average Volume (30D)", big(p.averageVolume || (ei && ei.avgVolume))]);
      rows.push(etf ? ["AUM", big(ei && ei.assetsUnderManagement, true)] : ["Market capitalization", big(p.marketCap, true)]);
      var more = etf ? [["Expense ratio", ei && ok(ei.expenseRatio) ? (+ei.expenseRatio).toFixed(2) + "%" : "—"], ["NAV", ei && ok(ei.nav) ? num(ei.nav) + " " + esc(ei.navCurrency || "") : "—"], ["Holdings", ei && ok(ei.holdingsCount) ? cnt(ei.holdingsCount) : "—"], ["Inception date", ei ? fdate(ei.inceptionDate) : "—"], ["Issuer", ei ? esc(ei.etfCompany || "—") : "—"], ["Beta", ok(p.beta) && p.beta ? n2(p.beta) : "—"]]
        : [["P/E (TTM)", n2(r.priceToEarningsRatioTTM)], ["P/S (TTM)", n2(r.priceToSalesRatioTTM)], ["PEG (TTM)", n2(r.priceToEarningsGrowthRatioTTM)], ["EPS (TTM)", n2(r.netIncomePerShareTTM)], ["EV/EBITDA", n2(k.evToEBITDATTM)], ["Beta (1Y)", n2(p.beta)], ["Free float", ok(sf.floatShares) ? big(sf.floatShares) : "—"], ["Employees", ok(p.fullTimeEmployees) ? cnt(p.fullTimeEmployees) : "—"]];
      function cnt(v) { return Math.round(+v).toLocaleString("en-US"); }
      function n2(v) { return ok(v) ? (+v).toFixed(2) : "—"; }
      var sH = slot("stats");
      sH.innerHTML = "<h4>Key stats</h4>" + '<div class="tvd-ks" data-k="ks1">' + rows.map(function (z) { return "<span>" + z[0] + "</span><span>" + z[1] + "</span>"; }).join("") + "<span>Dividend yield (indicated)</span><span data-k=\"dy\">" + (dy != null ? dy.toFixed(2) + "%" : "—") + "</span></div>" +
        '<div class="tvd-ks tvd-more" hidden>' + more.map(function (z) { return "<span>" + z[0] + "</span><span>" + z[1] + "</span>"; }).join("") + '</div><div class="tvd-chev"><button type="button" aria-label="More key stats" title="More key stats">⌄</button></div>';
      sH.querySelector(".tvd-chev button").onclick = function (e) { e.stopPropagation(); var m = sH.querySelector(".tvd-ks.tvd-more"); m.hidden = !m.hidden; this.textContent = m.hidden ? "⌄" : "⌃"; };
      // ETF yield when the ratios feed carries none: trailing distributions / price
      if (dy == null) divP.then(function (dv) {
        if (!alive() || !dv.length || !ok(p.price)) return; var cut = Date.now() - 365 * 864e5, s = 0;
        dv.forEach(function (d) { if (Date.parse(d.date) > cut && ok(d.dividend)) s += +d.dividend; });
        if (s > 0) sH.querySelector('[data-k="dy"]').textContent = (s / p.price * 100).toFixed(2) + "%";
      });
      // fund exposure
      if (etf) Promise.all([fmp("etf/sector-weightings", T), fmp("etf/country-weightings", T)]).then(function (w) {
        if (!alive()) return;
        var sec = (Array.isArray(w[0]) ? w[0] : []).map(function (s) { return [s.sector, pctNum(s.weightPercentage)]; }).filter(function (s) { return s[1] > 0; }).sort(function (a1, a2) { return a2[1] - a1[1]; });
        var reg = {}, tot = 0; (Array.isArray(w[1]) ? w[1] : []).forEach(function (c) { var v = pctNum(c.weightPercentage); if (!(v > 0)) return; var g = regionOf(c.country) || "Other / unclassified"; reg[g] = (reg[g] || 0) + v; tot += v; });
        var h = "";
        if (sec.length) h += "<h4>Sector breakdown</h4>" + '<div class="tvd-dn2">' + donut(sec.map(function (s, i) { return [s[0], s[1], PAL[i % PAL.length]]; })) + '</div><div class="tvd-lst">' + sec.slice(0, 10).map(function (s, i) { return '<span><i style="background:' + PAL[i % PAL.length] + '"></i>' + esc(s[0]) + "</span><span>" + s[1].toFixed(2) + "%</span>"; }).join("") + "</div>";
        if (tot) h += "<h4>Stock breakdown by region</h4>" + '<div class="tvd-lst">' + ["North America", "Asia", "Latin America", "Europe", "Africa", "Middle East", "Oceania", "Other / unclassified"].filter(function (g) { return g !== "Other / unclassified" || reg[g]; }).map(function (g) { return "<span>" + g + "</span><span>" + ((reg[g] || 0) / tot * 100).toFixed(2) + "%</span>"; }).join("") + "</div>";
        if (h) slot("expo").innerHTML = h + (page ? '<div class="tvd-pill"><a href="' + page + '&tab=holdings" target="_blank" rel="noopener">More about fund</a></div>' : "");
      });
      // earnings: last four reported quarters + the next estimate
      if (!etf && E.length) fmp("income-statement", T, "&period=quarter&limit=12").then(function (iq) {
        if (!alive()) return;
        iq = Array.isArray(iq) ? iq : [];
        var rep = E.filter(function (e) { return e.epsActual != null; }).sort(function (x1, x2) { return Date.parse(x2.date) - Date.parse(x1.date); }).slice(0, 4).reverse();
        if (!rep.length) return;
        function lab(e) {
          var t = Date.parse(e.date), m = iq.filter(function (s) { var d = t - Date.parse(s.date); return d >= 0 && d < 120 * 864e5; }).sort(function (s1, s2) { return Date.parse(s2.date) - Date.parse(s1.date); })[0];
          return m ? [m.period, +m.fiscalYear] : null;
        }
        var pts = rep.map(function (e) { var l = lab(e); return { e: e, l: l }; });
        if (nxt) { var pl = pts[pts.length - 1].l, nl = pl ? (pl[0] === "Q4" ? ["Q1", pl[1] + 1] : ["Q" + (+pl[0].slice(1) + 1), pl[1]]) : null; pts.push({ e: nxt, l: nl, next: true }); }
        var vals = []; pts.forEach(function (z) { if (ok(z.e.epsActual)) vals.push(+z.e.epsActual); if (ok(z.e.epsEstimated)) vals.push(+z.e.epsEstimated); });
        var lo = Math.min.apply(null, vals.concat([0])), hi = Math.max.apply(null, vals), W = 300, H = 140, rng = (hi - lo) || 1; lo -= rng * 0.15; hi += rng * 0.15;
        var X = function (i) { return 24 + i * (W - 70) / Math.max(1, pts.length - 1); }, Y = function (v) { return 8 + (hi - v) / (hi - lo) * (H - 34); };
        var g = "", s2; for (s2 = 0; s2 <= 4; s2++) { var v = lo + (hi - lo) * s2 / 4; g += '<line x1="10" x2="' + (W - 36) + '" y1="' + Y(v).toFixed(1) + '" y2="' + Y(v).toFixed(1) + '" stroke="var(--bd)" stroke-width=".5"/><text x="' + (W - 32) + '" y="' + (Y(v) + 3).toFixed(1) + '" fill="var(--mut)" font-size="9">' + v.toFixed(2) + "</text>"; }
        pts.forEach(function (z, i) {
          var e = z.e; g += '<text x="' + X(i).toFixed(1) + '" y="' + (H - 6) + '" fill="var(--mut)" font-size="9" text-anchor="middle">' + esc(z.l ? z.l[0] + " '" + String(z.l[1]).slice(2) : fdate(e.date)) + "</text>";
          if (ok(e.epsEstimated)) g += '<circle cx="' + X(i).toFixed(1) + '" cy="' + Y(e.epsEstimated).toFixed(1) + '" r="6" fill="none" stroke="var(--mut)" stroke-width="1.5"><title>Estimate ' + (+e.epsEstimated).toFixed(2) + "</title></circle>";
          if (ok(e.epsActual)) g += '<circle cx="' + X(i).toFixed(1) + '" cy="' + Y(e.epsActual).toFixed(1) + '" r="6" fill="' + (!ok(e.epsEstimated) || e.epsActual >= e.epsEstimated ? "#089981" : "#f23645") + '"><title>Reported ' + (+e.epsActual).toFixed(2) + (ok(e.epsEstimated) ? " vs est. " + (+e.epsEstimated).toFixed(2) : "") + " · " + esc(e.date) + "</title></circle>";
        });
        slot("earn").innerHTML = "<h4>Earnings<small>EPS per share" + (inDays != null ? " · next in " + inDays + " days" : "") + '</small></h4><svg viewBox="0 0 ' + W + " " + H + '" width="100%" height="' + H + '" aria-hidden="true">' + g + '</svg><div class="tvd-leg"><span><i style="background:#089981"></i>Actual (beat)</span><span><i style="background:#f23645"></i>Actual (miss)</span><span><i style="border:1.5px solid var(--mut);width:5px;height:5px"></i>Estimate</span></div>' + (page ? '<div class="tvd-pill"><a href="' + page + '&tab=earnings" target="_blank" rel="noopener">More info</a></div>' : "");
      });
      // dividends
      divP.then(function (dv) {
        if (!alive() || !dv.length) return;
        var d0 = dv[0], pr = ok(r.dividendPayoutRatioTTM) ? r.dividendPayoutRatioTTM * 100 : null, cut = Date.now() - 365 * 864e5, ttm = 0;
        dv.forEach(function (d) { if (Date.parse(d.date) > cut && ok(d.dividend)) ttm += +d.dividend; });
        var yld = dy != null ? dy : ttm && ok(p.price) ? ttm / p.price * 100 : null;
        slot("divs").innerHTML = "<h4>Dividends</h4>" + (pr != null ? '<div class="tvd-dn2">' + donut([["Payout", Math.min(100, Math.max(0, pr)), "#2962ff"], ["Retained", Math.max(0, 100 - pr), "transparent"]], pr.toFixed(2) + "%", 110) + '</div><div class="tvd-mu" style="text-align:center;margin:-2px 0 8px"><i style="display:inline-block;width:7px;height:7px;border-radius:50%;background:#2962ff;margin-right:5px"></i>Payout ratio (TTM)</div>' : "") +
          '<div class="tvd-ks"><span>Dividend yield TTM</span><span>' + (yld != null ? yld.toFixed(2) + "%" : "—") + "</span><span>Last payment</span><span>" + (ok(d0.dividend) ? (+d0.dividend).toFixed(d0.dividend < 0.1 ? 4 : 2) : "—") + "</span><span>Last ex-dividend date</span><span>" + fdate(d0.date) + "</span><span>Last payment date</span><span>" + fdate(d0.paymentDate) + "</span>" + (d0.frequency ? "<span>Frequency</span><span>" + esc(d0.frequency) + "</span>" : "") + "</div>" +
          (page ? '<div class="tvd-pill"><a href="' + page + '&tab=dividends" target="_blank" rel="noopener">More info</a></div>' : "");
      });
      // income statement: revenue and net income bars, net margin line
      if (!etf) {
        var iH = slot("inc"), per = "annual";
        var drawInc = function () {
          fmp("income-statement", T, "&period=" + per + "&limit=" + (per === "annual" ? 5 : 8)).then(function (d) {
            if (!alive()) return;
            d = Array.isArray(d) ? d.slice().reverse() : [];
            if (!d.length) { iH.innerHTML = ""; return; }
            var W = 300, H = 150, mx = 0, mn = 0, mg = d.map(function (s) { return s.revenue ? s.netIncome / s.revenue * 100 : null; });
            d.forEach(function (s) { mx = Math.max(mx, +s.revenue || 0, +s.netIncome || 0); mn = Math.min(mn, +s.netIncome || 0, +s.revenue || 0); });
            var mgv = mg.filter(ok), mlo = Math.min.apply(null, mgv.concat([0])), mhi = Math.max.apply(null, mgv.concat([1]));
            var gw = (W - 80) / d.length, Y = function (v) { return 8 + (mx - v) / ((mx - mn) || 1) * (H - 34); }, Ym = function (v) { return 8 + (mhi - v) / ((mhi - mlo) || 1) * (H - 34); };
            var g = "", s3;
            for (s3 = 0; s3 <= 4; s3++) { var v = mn + (mx - mn) * s3 / 4, vm = mlo + (mhi - mlo) * s3 / 4; g += '<line x1="36" x2="' + (W - 40) + '" y1="' + Y(v).toFixed(1) + '" y2="' + Y(v).toFixed(1) + '" stroke="var(--bd)" stroke-width=".5"/><text x="' + (W - 36) + '" y="' + (Y(v) + 3).toFixed(1) + '" fill="var(--mut)" font-size="9">' + big(v) + '</text><text x="2" y="' + (Ym(vm) + 3).toFixed(1) + '" fill="var(--mut)" font-size="9">' + vm.toFixed(0) + "%</text>"; }
            var line = "";
            d.forEach(function (s, i) {
              var x0 = 40 + i * gw, bw = Math.min(14, gw / 3);
              [[+s.revenue || 0, "#2962ff"], [+s.netIncome || 0, "#00bcd4"]].forEach(function (z, j) { var ya = Y(Math.max(0, z[0])), yb = Y(Math.min(0, z[0])); g += '<rect x="' + (x0 + gw / 2 - bw + j * bw).toFixed(1) + '" y="' + ya.toFixed(1) + '" width="' + (bw - 1).toFixed(1) + '" height="' + Math.max(1, yb - ya).toFixed(1) + '" fill="' + z[1] + '"><title>' + (j ? "Net income " : "Revenue ") + big(z[0], true) + "</title></rect>"; });
              g += '<text x="' + (x0 + gw / 2).toFixed(1) + '" y="' + (H - 6) + '" fill="var(--mut)" font-size="9" text-anchor="middle">' + esc(per === "annual" ? "'" + String(s.fiscalYear || String(s.date).slice(0, 4)).slice(2) : s.period + " '" + String(s.fiscalYear || "").slice(2)) + "</text>";
              if (ok(mg[i])) line += (line ? "L" : "M") + (x0 + gw / 2).toFixed(1) + " " + Ym(mg[i]).toFixed(1);
            });
            g += '<path d="' + line + '" stroke="#ff9800" stroke-width="1.6" fill="none"/>' + d.map(function (s, i) { return ok(mg[i]) ? '<circle cx="' + (40 + i * gw + gw / 2).toFixed(1) + '" cy="' + Ym(mg[i]).toFixed(1) + '" r="2.4" fill="#ff9800"><title>Net margin ' + mg[i].toFixed(2) + "%</title></circle>" : ""; }).join("");
            iH.innerHTML = '<h4>Income statement<span class="tvd-tg"><button type="button" data-per="annual"' + (per === "annual" ? ' class="on"' : "") + '>Annual</button><button type="button" data-per="quarter"' + (per === "quarter" ? ' class="on"' : "") + ">Quarterly</button></span></h4>" +
              '<svg viewBox="0 0 ' + W + " " + H + '" width="100%" height="' + H + '" aria-hidden="true">' + g + '</svg><div class="tvd-leg"><span><i style="background:#2962ff"></i>Revenue</span><span><i style="background:#00bcd4"></i>Net income</span><span><i style="background:#ff9800"></i>Net margin %</span></div>' +
              (page ? '<div class="tvd-pill"><a href="' + page + '&tab=financials" target="_blank" rel="noopener">More financials</a></div>' : "");
            iH.querySelector(".tvd-tg").onclick = function (e) { var t = e.target.closest("[data-per]"); if (!t) return; e.stopPropagation(); per = t.getAttribute("data-per"); drawInc(); };
          });
        };
        drawInc();
      }
    });
  }

  root.JHTvDetails = { render: render, update: update, technicals: technicals, perf: perf, usSession: usSession };
})(typeof window !== "undefined" ? window : globalThis);
