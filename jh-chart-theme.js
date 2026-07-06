/* jh-chart-theme.js — Amber Terminal defaults for charting libraries.
   Injected at BUILD time into every page <head> (index + screener excluded).
   Problem it solves: library default themes live inside CDN code (ECharts blue
   series, LightweightCharts WHITE panels, Chart.js greys, Plotly white paper)
   and can never be reached by the static reskin engine.
   Mechanism: intercept each library global at the moment the CDN script
   assigns it (Object.defineProperty setter), patch defaults/constructors,
   then hand the value through. Load-order-proof; pages' own explicit options
   always win — this layer only fills what was left unset. ES5, no deps. */
(function () {
  "use strict";
  var BG = "#0b0906", PANEL = "#141008", LINE = "#2a2318", LINE2 = "#3a3428",
      INK = "#e8e2d4", DIM = "#8a836f", MID = "#b5ad99",
      AMBER = "#f5b93e", AMBER2 = "#d99a2b", GOLD = "#ffd479",
      GREEN = "#6fce8a", RED = "#e0685f",
      MONO = "'IBM Plex Mono', ui-monospace, Menlo, monospace",
      WAY = [AMBER, GREEN, RED, AMBER2, MID, GOLD, "#3f7d55", "#b04a43"];

  function onGlobal(name, patch) {
    var cur = window[name];
    if (cur) { try { patch(cur); } catch (e) {} return; }
    try {
      Object.defineProperty(window, name, {
        configurable: true,
        get: function () { return cur; },
        set: function (v) { cur = v; try { patch(v); } catch (e) {} }
      });
    } catch (e) {}
  }
  function merge(dst, src) {           /* fill-only deep merge: never overwrite */
    for (var k in src) {
      if (dst[k] === undefined || dst[k] === null) dst[k] = src[k];
      else if (typeof dst[k] === "object" && typeof src[k] === "object" &&
               !Array.isArray(dst[k])) merge(dst[k], src[k]);
    }
    return dst;
  }

  /* ── ECharts: register theme + default it into init() ── */
  onGlobal("echarts", function (ec) {
    if (!ec || !ec.init || ec.__jhThemed) return;
    ec.__jhThemed = 1;
    try {
      ec.registerTheme("jh-amber", {
        color: WAY, backgroundColor: "transparent",
        textStyle: { color: MID, fontFamily: MONO },
        axisLine: { lineStyle: { color: LINE2 } },
        splitLine: { lineStyle: { color: LINE } },
        categoryAxis: { axisLine: { lineStyle: { color: LINE2 } },
                        splitLine: { lineStyle: { color: LINE } },
                        axisLabel: { color: DIM } },
        valueAxis: { axisLine: { lineStyle: { color: LINE2 } },
                     splitLine: { lineStyle: { color: LINE } },
                     axisLabel: { color: DIM } },
        legend: { textStyle: { color: MID } },
        tooltip: { backgroundColor: PANEL, borderColor: LINE,
                   textStyle: { color: INK } }
      });
    } catch (e) {}
    var init0 = ec.init;
    ec.init = function (dom, theme, opts) {
      return init0.call(ec, dom, theme || "jh-amber", opts);
    };
  });

  /* ── LightweightCharts: default the WHITE panels away ── */
  onGlobal("LightweightCharts", function (lw) {
    if (!lw || !lw.createChart || lw.__jhThemed) return;
    lw.__jhThemed = 1;
    var cc0 = lw.createChart;
    lw.createChart = function (el, o) {
      o = o || {};
      merge(o, {
        layout: { background: { type: "solid", color: BG },
                  textColor: MID, fontFamily: MONO },
        grid: { vertLines: { color: LINE }, horzLines: { color: LINE } },
        timeScale: { borderColor: LINE2 },
        rightPriceScale: { borderColor: LINE2 },
        crosshair: { vertLine: { color: AMBER2 }, horzLine: { color: AMBER2 } }
      });
      return cc0.call(lw, el, o);
    };
  });

  /* ── Chart.js: global defaults (per-dataset colors in pages already amber) ── */
  onGlobal("Chart", function (C) {
    if (!C || !C.defaults || C.__jhThemed) return;
    C.__jhThemed = 1;
    try {
      C.defaults.color = DIM;
      C.defaults.borderColor = LINE;
      C.defaults.backgroundColor = "rgba(245,185,62,0.22)";
      C.defaults.font = merge(C.defaults.font || {}, { family: MONO });
      if (C.defaults.elements) {
        merge(C.defaults.elements, {
          line: { borderColor: AMBER }, point: { backgroundColor: AMBER },
          bar: { backgroundColor: "rgba(245,185,62,0.55)", borderColor: AMBER2 },
          arc: { borderColor: BG }
        });
      }
      if (C.defaults.scale && C.defaults.scale.grid)
        C.defaults.scale.grid.color = LINE;
    } catch (e) {}
  });

  /* ── Plotly: fill-only layout merge on every plot call ── */
  onGlobal("Plotly", function (P) {
    if (!P || !P.newPlot || P.__jhThemed) return;
    P.__jhThemed = 1;
    var LAY = { paper_bgcolor: BG, plot_bgcolor: BG, colorway: WAY,
                font: { color: MID, family: MONO },
                xaxis: { gridcolor: LINE, zerolinecolor: LINE2, linecolor: LINE2 },
                yaxis: { gridcolor: LINE, zerolinecolor: LINE2, linecolor: LINE2 } };
    ["newPlot", "react", "update"].forEach(function (fn) {
      var f0 = P[fn];
      if (!f0) return;
      P[fn] = function (el, data, layout) {
        var a = Array.prototype.slice.call(arguments);
        a[2] = merge(layout || {}, JSON.parse(JSON.stringify(LAY)));
        return f0.apply(P, a);
      };
    });
  });
})();
