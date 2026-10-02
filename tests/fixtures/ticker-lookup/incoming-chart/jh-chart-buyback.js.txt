/* Buyback pane. Own indicator (id buyback). Not the Events pin buyb.
   Level = quarterly net repurchase / latest market cap, from data/buyback-engine.json.
   Step-held on the price bars. Zero line. Last percent on the right. */
(function () {
  var URL = "/data/buyback-engine.json";
  var pack = null;
  var loading = null;

  function num(v) {
    if (v == null) return null;
    if (typeof v === "number") return isFinite(v) ? v : null;
    if (typeof v === "object" && v.value != null && isFinite(+v.value)) return +v.value;
    var n = +v;
    return isFinite(n) ? n : null;
  }
  function symbol() {
    var a = window.jhActive;
    if (typeof a === "string" && a) return a.replace(/[^A-Za-z0-9.\-]/g, "").toUpperCase();
    var el = document.getElementById("symin");
    if (el && el.value) return String(el.value).trim().toUpperCase();
    var tab = document.querySelector(".tab.on");
    var t = tab ? (tab.textContent || "").trim().split(/\s+/)[0] : "";
    return t.replace(/[^A-Za-z0-9.\-]/g, "").toUpperCase();
  }
  function item() {
    var osc = window.OSC || [];
    var i;
    for (i = 0; i < osc.length; i++) if (osc[i] && osc[i].id === "buyback") return osc[i];
    return null;
  }
  function ensure() {
    if (!window.OSC) return;
    if (item()) return;
    window.OSC.push({ id: "buyback", n: "Buyback", on: 0, cat: "Buyback", c: "#7ec8c4" });
  }
  function load() {
    if (pack) return Promise.resolve(pack);
    if (loading) return loading;
    loading = fetch(URL, { cache: "no-cache" }).then(function (r) {
      if (!r.ok) throw new Error("buyback engine " + r.status);
      return r.json();
    }).then(function (j) {
      pack = j;
      return j;
    }).catch(function () { loading = null; return null; });
    return loading;
  }
  function quarters(row) {
    var obs = row && row.measurements && row.measurements.cashflow_observations || [];
    var cap = num(row.market_cap);
    var out = [], i, met, net, sign, y;
    for (i = 0; i < obs.length; i++) {
      met = obs[i].metrics || {};
      net = num(met.net_common_repurchases);
      if (net == null) net = num(met.gross_common_repurchases);
      if (net == null || !cap) continue;
      sign = met.net_common_repurchases && met.net_common_repurchases.sign;
      y = net / cap * 100;
      if (sign === "positive") y = -y;
      out.push({ date: obs[i].date, y: y });
    }
    out.sort(function (a, b) { return a.date < b.date ? -1 : 1; });
    return out;
  }
  function day(ts) {
    var d = new Date(ts * 1000);
    return d.toISOString().slice(0, 10);
  }
  function held(qs, bars) {
    var pts = [], i, q = 0, y = null;
    for (i = 0; i < bars.length; i++) {
      var t = bars[i].time;
      if (typeof t !== "number") continue;
      var ds = day(t);
      while (q < qs.length && qs[q].date <= ds) { y = qs[q].y; q++; }
      if (y != null) pts.push({ x: i, y: y, t: t });
    }
    return pts;
  }
  function pane() {
    var el = document.getElementById("jh-buyback-pane");
    if (el) return el;
    var host = document.getElementById("oscwrap");
    el = document.createElement("div");
    el.id = "jh-buyback-pane";
    el.style.cssText = "display:none;height:110px;border-top:1px solid var(--line,#2a2e39);position:relative;background:var(--bg,#131722);flex:none";
    if (host && host.parentNode) host.parentNode.insertBefore(el, host.nextSibling);
    else (document.getElementById("stage") || document.body).appendChild(el);
    return el;
  }
  function draw(bars) {
    ensure();
    var it = item();
    var el = pane();
    if (!it || !it.on) { el.style.display = "none"; return; }
    el.style.display = "block";
    load().then(function () { paint(el, bars || window.lastBars || []); });
  }
  function paint(el, bars) {
    var sym = symbol();
    var row = pack && pack.tickers && pack.tickers[sym];
    var w = el.clientWidth || 640, h = 110;
    var qs = row ? quarters(row) : [];
    var pts = qs.length && bars && bars.length ? held(qs, bars) : [];
    var last = qs.length ? qs[qs.length - 1] : null;
    var label = !row ? (sym || "Ticker") + " not in buyback engine" : (last ? (last.y >= 0 ? "+" : "") + last.y.toFixed(2) + "%" : "no quarter");
    var sub = last ? last.date + " net repurchase / latest mkt cap" : "buyback-engine.json";
    var min = 0, max = 0, i;
    for (i = 0; i < pts.length; i++) { if (pts[i].y < min) min = pts[i].y; if (pts[i].y > max) max = pts[i].y; }
    if (min === max) { min -= 0.05; max += 0.05; }
    var pad = (max - min) * 0.15;
    min -= pad; max += pad;
    function X(i) { return bars.length < 2 ? 8 : 8 + (w - 78) * (i / (bars.length - 1)); }
    function Y(v) { return 18 + (h - 28) * (1 - (v - min) / (max - min)); }
    var d = "", zero = Y(0);
    for (i = 0; i < pts.length; i++) {
      var x = X(pts[i].x), y = Y(pts[i].y);
      d += (i ? "L" : "M") + x.toFixed(1) + " " + y.toFixed(1) + " ";
    }
    var col = last && last.y < 0 ? "#f23645" : "#089981";
    el.innerHTML = "<div style='position:absolute;left:8px;top:4px;font:11px IBM Plex Mono,monospace;color:#d1d4dc'>Buyback <span style='color:" + col + "'>" + label + "</span> <span style='color:#787b86'>" + sub + "</span></div>" +
      "<svg width='" + w + "' height='" + h + "' style='display:block'>" +
      "<line x1='8' x2='" + (w - 64) + "' y1='" + zero.toFixed(1) + "' y2='" + zero.toFixed(1) + "' stroke='#2a2e39'/>" +
      (d ? "<path d='" + d + "' fill='none' stroke='#7ec8c4' stroke-width='1.6'/>" : "") +
      "<text x='" + (w - 8) + "' y='" + (last ? Y(last.y) : 24) + "' text-anchor='end' fill='" + col + "' font-size='11' font-family='IBM Plex Mono,monospace'>" + (last ? label : "") + "</text></svg>";
  }
  function hook() {
    ensure();
    if (window.paint && !window.paint.__jhBuyback) {
      var orig = window.paint;
      var wrapped = function (d) {
        var r = orig.apply(this, arguments);
        try { draw(d || window.lastBars); } catch (e) {}
        return r;
      };
      wrapped.__jhBuyback = 1;
      window.paint = wrapped;
    }
  }
  var n = 0;
  var timer = setInterval(function () {
    hook();
    if (++n > 40) clearInterval(timer);
  }, 250);
  window.jhBuybackDraw = draw;
})();
