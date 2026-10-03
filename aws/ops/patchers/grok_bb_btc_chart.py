#!/usr/bin/env python3
"""BTC volume units and Bollinger setData. No chart-engine edit."""
from pathlib import Path
import re
JS = r"""/* Bollinger safety and BTC volume. No engine edit.
   The engine paints with its own paint(), not window.paint, and one bad
   setData aborts the rest of the indicator pass. Clean the line data and
   continue. BTC daily volume is quote dollars until the warehouse join and
   base coins after it, so the histogram dies from 2021. Scale only the coin
   side by close, in place, before the volume series reads the bars. */
(function () {
  if (typeof window === "undefined" || window.__jhBbFix) return;
  window.__jhBbFix = 1;
  function finite(v) { return typeof v === "number" && isFinite(v); }
  function isBtc() {
    var s = String(window.jhActive || window.active || "").toUpperCase();
    return s === "BTC" || s === "BTCUSD" || s === "BTCUSDT" || s === "BTC-USD" || s === "X:BTCUSD";
  }
  function align(d) {
    if (!d || d.length < 40 || d.__jhVol || !isBtc()) return;
    function med(i0, n) {
      var s = [], k, row, q;
      for (k = i0; k < i0 + n && k < d.length; k++) {
        row = d[k];
        if (row && row.close > 0 && row.volume > 0) {
          q = row.volume / row.close;
          if (finite(q)) s.push(q);
        }
      }
      if (s.length < 8) return null;
      s.sort(function (a, c) { return a - c; });
      return s[s.length >> 1];
    }
    var cut = -1, i, before, after, b;
    for (i = 30; i < d.length - 20; i++) {
      before = med(i - 30, 30);
      after = med(i, 20);
      if (before > 1000 && after != null && after < 100) { cut = i; break; }
    }
    if (cut >= 0) {
      for (i = cut; i < d.length; i++) {
        b = d[i];
        if (!b || !(b.close > 0) || !(b.volume > 0) || b.volume / b.close >= 100) continue;
        b.volume = b.volume * b.close;
      }
    }
    d.__jhVol = 1;
  }
  function sane(data) {
    if (!data || !data.length) return data || [];
    var out = [], i, p, last = null;
    for (i = 0; i < data.length; i++) {
      p = data[i];
      if (!p || typeof p.time !== "number" || !finite(p.time)) continue;
      if (p.value != null && !finite(p.value)) continue;
      if (last != null && !(p.time > last)) continue;
      out.push(p);
      last = p.time;
    }
    return out;
  }
  function hook(chart) {
    if (!chart || chart.__jhBbHook || !chart.applyOptions || !chart.addLineSeries) return false;
    chart.__jhBbHook = 1;
    var apply = chart.applyOptions.bind(chart);
    chart.applyOptions = function () {
      try { align(window.lastBars); } catch (e) {}
      return apply.apply(chart, arguments);
    };
    var addLine = chart.addLineSeries.bind(chart);
    chart.addLineSeries = function () {
      var s = addLine.apply(chart, arguments);
      if (s && s.setData && !s.setData.__jh) {
        var sd = s.setData.bind(s);
        function setData(data) {
          try { return sd(data); }
          catch (e) { try { return sd(sane(data)); } catch (e2) {} }
        }
        setData.__jh = 1;
        s.setData = setData;
      }
      return s;
    };
    return true;
  }
  var painted = 0;
  var n = 0;
  var t = setInterval(function () {
    var chart = window.jhDeskChart || window.chart;
    var ok = hook(chart);
    if (ok && !painted && window.paint && window.lastBars && window.lastBars.length) {
      painted = 1;
      try { window.paint(window.lastBars); } catch (e) {}
    }
    if ((ok && painted) || ++n > 80) clearInterval(t);
  }, 50);
})();
"""
def root():
    if Path("jh-chart-bbfix.js").exists() or Path("jh-chart-distribution.js").exists():
        return Path(".")
    return Path(__file__).resolve().parents[3]
def main():
    base = root()
    bb = base / "jh-chart-bbfix.js"
    cur = bb.read_text(encoding="utf-8") if bb.exists() else ""
    if cur != JS:
        bb.write_text(JS, encoding="utf-8")
        print("bbfix written")
    else:
        print("bbfix current")
    dist = base / "jh-chart-distribution.js"
    if not dist.exists():
        print("distribution missing")
        return 0
    t = dist.read_text(encoding="utf-8")
    t2, n = re.subn(
        r"/jh-chart-bbfix\.js\?v=[^\"']+",
        "/jh-chart-bbfix.js?v=20261003bb3",
        t,
        count=1,
    )
    if n and t2 != t:
        dist.write_text(t2, encoding="utf-8")
        print("distribution cache bust")
    else:
        print("distribution loader unchanged")
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
