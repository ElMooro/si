/* Five lights from warehouse ETFs + risk-gate. Not TV live. */
(function () {
  function el() {
    var q = document.getElementById("quote");
    if (!q) return null;
    var n = document.getElementById("jh-rail");
    if (!n) {
      n = document.createElement("div");
      n.id = "jh-rail";
      n.style.cssText = "flex-basis:100%;font-size:10px;letter-spacing:.04em";
      q.appendChild(n);
    }
    return n;
  }
  function light(name, ok, warn) {
    var c = ok ? "#089981" : warn ? "#f0b429" : "#f23645";
    return "<span style=\"color:" + c + "\">● " + name + "</span>";
  }
  function ret(d, n) {
    if (!d || d.length <= n) return null;
    var a = d[d.length - 1].close, b = d[d.length - 1 - n].close;
    return b ? a / b - 1 : null;
  }
  async function bars(sym) {
    try {
      var r = await fetch("https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=" + encodeURIComponent(sym), { cache: "no-store" });
      var j = await r.json();
      window.jhOhlcMeta = window.jhOhlcMeta || {};
      window.jhOhlcMeta[sym] = { source: j.source, key: j.warehouse_key };
      return (j.bars || []).map(function (b) {
        var t = b.time; if (t > 1e12) t = Math.floor(t / 1000);
        return { time: t, close: +b.close };
      });
    } catch (e) { return []; }
  }
  async function run() {
    var spy = await bars("SPY"), iwm = await bars("IWM"), hyg = await bars("HYG");
    var lqd = await bars("LQD"), gld = await bars("GLD"), tlt = await bars("TLT");
    var rg = null;
    try { rg = await (await fetch("/data/risk-gate.json", { cache: "no-store" })).json(); } catch (e) {}
    function rs20(a, b) {
      if (!a.length || !b.length || a.length < 21) return null;
      var map = {};
      b.forEach(function (x) { map[x.time] = x.close; });
      var x = a[a.length - 1], y = a[a.length - 21];
      if (!map[x.time] || !map[y.time] || !y.close) return null;
      return (x.close / map[x.time]) / (y.close / map[y.time]) - 1;
    }
    var fund = rg && rg.legs && rg.legs.funding && rg.legs.funding.score;
    var hy = rs20(hyg, lqd);
    var sm = rs20(iwm, spy);
    var gd = rs20(gld, spy);
    var du = ret(tlt, 20);
    var html = [
      light("PLUMB", fund == null ? false : fund > -1, fund != null && fund > -1.5),
      light("CREDIT", hy != null && hy > -0.01, hy != null && hy > -0.03),
      light("IWM", sm != null && sm > -0.01, sm != null && sm > -0.03),
      light("GOLD", gd != null && gd > 0, gd != null && gd > -0.02),
      light("TLT", du != null && du > -0.02, du != null && du > -0.04)
    ].join("   ");
    var n = el();
    if (n) n.innerHTML = html + " <span style=\"opacity:.55\">watchlist doctrine · warehouse ETFs</span>";
    var meta = window.jhOhlcMeta && window.jhOhlcMeta.SPY;
    var src = document.getElementById("jh-src");
    if (!src && el()) {
      src = document.createElement("div");
      src.id = "jh-src";
      src.style.cssText = "flex-basis:100%;font-size:10px;opacity:.7";
      el().parentNode.appendChild(src);
    }
    if (src) {
      var key = (meta && (meta.key || meta.source)) || window.lastSource || "—";
      var kind = /us-equities-daily|polygon-full/i.test(String(key)) ? "MASSIVE"
        : /tv-bars/i.test(String(key)) ? "WAREHOUSE tv-bars (import tape, not TV live)"
        : String(key);
      src.textContent = "TAPE " + kind + " · " + String(key).replace(/^data\//, "");
    }
  }
  window.jhRail = run;
  window.addEventListener("load", function () { setTimeout(run, 900); });
  setInterval(run, 180000);
})();
