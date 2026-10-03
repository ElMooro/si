#!/usr/bin/env python3
"""Level-match Bitcoin volume across the warehouse join. No engine edit."""
from pathlib import Path
import re
JS = r"""/* Bollinger safety and BTC volume. No engine edit.
   The engine paints with its own paint(), not window.paint, and one bad
   setData aborts the rest of the indicator pass. Clean the line data and
   continue. BTC daily volume is quote dollars until the warehouse join and base coins
   after it. Turn the coin side into dollars, then level-match the older dollar
   prints to that feed so one histogram can show 2021 onward. */
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
    function med(arr) {
      if (!arr || arr.length < 8) return null;
      var s = arr.slice().sort(function (a, c) { return a - c; });
      return s[s.length >> 1];
    }
    function ratios(i0, n) {
      var s = [], k, row, q;
      for (k = i0; k < i0 + n && k < d.length; k++) {
        row = d[k];
        if (row && row.close > 0 && row.volume > 0) {
          q = row.volume / row.close;
          if (finite(q)) s.push(q);
        }
      }
      return med(s);
    }
    function vols(i0, n) {
      var s = [], k, row;
      for (k = i0; k < i0 + n && k < d.length; k++) {
        row = d[k];
        if (row && row.volume > 0) s.push(row.volume);
      }
      return med(s);
    }
    var dollar = [], i, b, q, cut = -1, before, after, pre, post, factor;
    for (i = 0; i < d.length; i++) {
      b = d[i];
      q = b && b.close > 0 && b.volume > 0 ? b.volume / b.close : 0;
      dollar[i] = q >= 100;
    }
    for (i = 30; i < d.length - 20; i++) {
      before = ratios(i - 30, 30);
      after = ratios(i, 20);
      if (before > 1000 && after != null && after < 100) { cut = i; break; }
    }
    if (cut >= 0) {
      for (i = cut; i < d.length; i++) {
        b = d[i];
        if (dollar[i] || !b || !(b.close > 0) || !(b.volume > 0)) continue;
        b.volume = b.volume * b.close;
      }
      pre = vols(Math.max(0, cut - 40), Math.min(40, cut));
      post = vols(cut, 40);
      if (pre > 0 && post > 0 && pre > post * 3) {
        factor = post / pre;
        for (i = 0; i < d.length; i++) {
          if (!dollar[i] || !(d[i].volume > 0)) continue;
          d[i].volume = d[i].volume * factor;
        }
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
        "/jh-chart-bbfix.js?v=20261003bb4",
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
