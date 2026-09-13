/* Watchlist doctrine on the chart. Uses warehouse ETFs we can actually load.
   Lists taught: Bottom Indicators, Top Indicators, Black Swan, Trend Reversal,
   Gold bottoms first, small caps crack first, HYG/LQD credit, TLT dump warning. */
(function () {
  var CACHE = {};
  var BASKET = ["SPY", "IWM", "TLT", "HYG", "LQD", "GLD", "EEM", "QQQ"];
  function barsOf(sym) {
    return CACHE[sym] || [];
  }
  function last(d, n) { n = n || 1; return d[d.length - n]; }
  function ret(d, n) {
    if (!d || d.length <= n) return null;
    var a = d[d.length - 1].close, b = d[d.length - 1 - n].close;
    return b ? a / b - 1 : null;
  }
  function rsNow(a, b) {
    if (!a.length || !b.length) return null;
    var map = {};
    b.forEach(function (x) { map[x.time] = x.close; });
    var x = a[a.length - 1];
    return map[x.time] ? x.close / map[x.time] : null;
  }
  function rsChange(a, b, n) {
    if (!a.length || !b.length || a.length <= n) return null;
    var map = {};
    b.forEach(function (x) { map[x.time] = x.close; });
    var x = a[a.length - 1], y = a[a.length - 1 - n];
    if (!map[x.time] || !map[y.time] || !y.close) return null;
    return (x.close / map[x.time]) / (y.close / map[y.time]) - 1;
  }
  function flags() {
    var spy = barsOf("SPY"), iwm = barsOf("IWM"), tlt = barsOf("TLT");
    var hyg = barsOf("HYG"), lqd = barsOf("LQD"), gld = barsOf("GLD"), eem = barsOf("EEM");
    var out = [];
    var rSpy1 = ret(spy, 1), rHyg1 = ret(hyg, 1), rTlt1 = ret(tlt, 1), rGld1 = ret(gld, 1);
    if (rSpy1 != null && rHyg1 != null && rSpy1 <= -0.03 && rHyg1 <= -0.02)
      out.push({ tag: "SWAN", why: "Black Swan list: SPY+HYG same-day dump", list: "Black Swan Event" });
    var gldLead = rsChange(gld, spy, 20);
    var spy20 = ret(spy, 20);
    if (gldLead != null && spy20 != null && gldLead > 0.03 && spy20 < 0)
      out.push({ tag: "GOLD-LEAD", why: "Gold bottoms first (your list)", list: "Gold ALWAYS BOTTOM FIRST" });
    var iwmRs = rsChange(iwm, spy, 20);
    if (iwmRs != null && iwmRs < -0.04)
      out.push({ tag: "WEAK-IWM", why: "Small caps first crack", list: "small caps warning cracks" });
    var eemRs = rsChange(eem, spy, 20);
    if (eemRs != null && eemRs < -0.04)
      out.push({ tag: "WEAK-EM", why: "EM = liquidity + risk appetite", list: "EM BEST GAUGE" });
    var hygLqd = rsChange(hyg, lqd, 20);
    if (hygLqd != null && hygLqd < -0.03)
      out.push({ tag: "CREDIT", why: "HY vs IG deteriorating", list: "Bottom/Stress credit" });
    if (rTlt1 != null && rSpy1 != null && rTlt1 <= -0.015 && rSpy1 <= -0.01)
      out.push({ tag: "BOND-WARN", why: "Sovereign/Treasury dump with stocks", list: "Bonds dumping warning" });
    var tlt20 = ret(tlt, 20), spy5 = ret(spy, 5);
    if (tlt20 != null && spy5 != null && tlt20 > 0.04 && spy5 < 0)
      out.push({ tag: "REV-WATCH", why: "Duration bid + equity still weak", list: "Trend Reversal Indicators" });
    if (spy20 != null && spy20 < -0.08 && gldLead != null && gldLead > 0)
      out.push({ tag: "BOTTOM-SET", why: "Equity washed out, gold already up", list: "Bottom Indicators" });
    if (spy20 != null && spy20 > 0.08 && iwmRs != null && iwmRs < 0 && hygLqd != null && hygLqd < 0)
      out.push({ tag: "TOP-SET", why: "Index up, small cap+credit lag", list: "Top Indicators / sector tops" });
    return out;
  }
  function paint(d) {
    var f = flags();
    var q = document.getElementById("quote");
    if (!q) return [];
    var id = "jh-macro-intel";
    var n = document.getElementById(id);
    if (!n) {
      n = document.createElement("div");
      n.id = id;
      n.style.cssText = "flex-basis:100%;font-size:10px;color:var(--mut)";
      q.appendChild(n);
    }
    n.textContent = f.length
      ? f.map(function (x) { return x.tag + " — " + x.why; }).join(" | ")
      : "MACRO intel quiet (watchlist doctrine on warehouse ETFs)";
    if (!d || !d.length || !f.length) return [];
    var lastT = d[d.length - 1].time;
    return f.slice(0, 4).map(function (x) {
      return { time: lastT, position: x.tag.indexOf("TOP") >= 0 || x.tag === "SWAN" ? "aboveBar" : "belowBar",
        color: x.tag === "SWAN" || x.tag.indexOf("TOP") >= 0 ? "#f23645" : "#089981",
        shape: "square", text: x.tag };
    });
  }
  async function load() {
    var i, sym;
    for (i = 0; i < BASKET.length; i++) {
      sym = BASKET[i];
      try {
        var r = await fetch("https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=" + encodeURIComponent(sym), { cache: "no-store" });
        var j = await r.json();
        CACHE[sym] = (j.bars || []).map(function (b) {
          var t = b.time; if (t > 1e12) t = Math.floor(t / 1000);
          return { time: t, close: +b.close, open: +b.open, high: +b.high, low: +b.low, volume: +(b.volume || b.value || 0) };
        });
      } catch (e) { CACHE[sym] = CACHE[sym] || []; }
    }
  }
  window.jhMacroIntel = function (d) {
    return load().then(function () { return paint(d || window.lastBars || []); });
  };
})();
