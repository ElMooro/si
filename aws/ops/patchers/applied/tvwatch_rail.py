#!/usr/bin/env python3
"""Merge Chart Pro watchlists into the chart, then load the TradingView pane.

Add-only. Never deletes a chart list, a Chart Pro list, or a symbol the user removed
after it was imported once.
"""
from pathlib import Path

IMPORT = r"""  (function () {
    var MARK = "jh-chart-pro-imported";
    function read(k, fb) { try { var v = JSON.parse(localStorage.getItem(k) || ""); return v == null ? fb : v; } catch (e) { return fb; } }
    function write(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} }
    var map = { red: "#f23645", orange: "#ff6d00", yellow: "#fdd835", green: "#089981", blue: "#2962ff", purple: "#ab47bc", "var(--cyan)": "#22d3ee", "var(--green)": "#089981", "var(--amber)": "#fbbf24", "var(--violet)": "#ab47bc", "var(--pink)": "#e91e63", "var(--blue)": "#2962ff" };
    function hex(c) { if (!c) return ""; c = String(c); if (c.charAt(0) === "#") return c; return map[c] || ""; }
    try {
      var seen = read(MARK, { lists: {} }); if (!seen.lists) seen.lists = {};
      var src = read("jh_custom_watchlists", null);
      var dst = read("jh-chart-custom-lists", []); if (!Array.isArray(dst)) dst = [];
      var byName = {}; dst.forEach(function (l) { if (l && l.name) byName[l.name] = l; });
      if (src && typeof src === "object") {
        Object.keys(src).forEach(function (id) {
          var w = src[id]; if (!w || !w.name) return;
          var tick = (w.tickers || []).filter(Boolean);
          var rec = seen.lists[id] || [];
          var have = {}; rec.forEach(function (s) { have[s] = 1; });
          var L = byName[w.name];
          if (!L) { L = { id: id, name: w.name, symbols: [], n: 0, custom: 1, color: hex(w.color) || null, from: "chart-pro" }; dst.unshift(L); byName[w.name] = L; }
          tick.forEach(function (s) { if (have[s]) return; if (L.symbols.indexOf(s) < 0) L.symbols.push(s); have[s] = 1; rec.push(s); });
          L.n = L.symbols.length; if (!L.color) L.color = hex(w.color) || null;
          seen.lists[id] = rec;
        });
        write("jh-chart-custom-lists", dst);
      }
      var pf = read("jh_symbol_flags", {}); var cf = read("jh-chart-flags", {});
      if (pf && typeof pf === "object") { Object.keys(pf).forEach(function (k) { if (!cf[k] && hex(pf[k])) cf[k] = hex(pf[k]); }); write("jh-chart-flags", cf); }
      var mine = read("jh-chart-favs", []); if (!Array.isArray(mine)) mine = [];
      var set = {}; mine.forEach(function (s) { set[s] = 1; });
      function addFav(k) { if (k && !set[k]) { mine.push(k); set[k] = 1; } }
      var fav = read("jh_favorites", null);
      if (fav && typeof fav === "object" && !Array.isArray(fav)) Object.keys(fav).forEach(function (k) { if (fav[k]) addFav(k); });
      var favA = read("jh_favs", null); if (Array.isArray(favA)) favA.forEach(addFav);
      write("jh-chart-favs", mine); write(MARK, seen);
    } catch (e) {}
  })();
  if (!document.getElementById("jh-tvwatch-js")) { var sc = document.createElement("script"); sc.id = "jh-tvwatch-js"; sc.src = "/jh-chart-tvwatch.js?v=20261003-tvwl"; document.head.appendChild(sc); }
"""

OLD_H = 'var lh = parseInt(localStorage.getItem(LKEY) || "240", 10);\n    if (isFinite(lh)) listH = Math.max(96, Math.min(640, lh));'
NEW_H = 'var lh = parseInt(localStorage.getItem(LKEY) || "", 10);\n    if (isFinite(lh) && localStorage.getItem(LKEY)) listH = Math.max(96, Math.min(640, lh));\n    else listH = Math.max(220, Math.min(640, Math.round((window.innerHeight || 800) * 0.46)));'


def root():
    if Path("jh-chart-tvrail.js").exists():
        return Path(".")
    return Path(__file__).resolve().parents[3]


def main():
    path = root() / "jh-chart-tvrail.js"
    text = path.read_text(encoding="utf-8")
    needle = "  window.__jhTvRail = true;\n"
    if text.count(needle) != 1:
        raise SystemExit("rail guard missing")
    if "jh-chart-pro-imported" not in text:
        text = text.replace(needle, needle + IMPORT, 1)
    if OLD_H not in text:
        if "jh-tv-list-sized" in text or "innerHeight || 800) * 0.46" in text:
            pass
        else:
            raise SystemExit("list height needle missing")
    else:
        text = text.replace(OLD_H, NEW_H, 1)
    if "etf: [\"ETF Desk\"" not in text or "kind === \"sniper\"" not in text:
        raise SystemExit("rail markers lost")
    path.write_text(text, encoding="utf-8")
    print("tv watch rail wired")


if __name__ == "__main__":
    main()
