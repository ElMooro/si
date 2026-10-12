/* jh-chart-evr — Effort & Spread on chart.html, next to Price Spread. Off until toggled. */
(function () {
  if (window.__jhEvr) return;
  window.__jhEvr = 1;
  var ID = "effspread";
  var LAY = "jh-chart-v12-tv-layout";
  var chart = null;
  var effortS = null;
  var resultS = null;
  var oneS = null;

  function find() {
    var list = window.OSC || [];
    var i;
    for (i = 0; i < list.length; i++) if (list[i] && list[i].id === ID) return list[i];
    return null;
  }

  function ensure() {
    var OSC = window.OSC;
    if (!OSC || !OSC.splice) return false;
    if (find()) return true;
    var item = { id: ID, n: "Effort & Spread", on: 0, cat: "Volume", p: 20, externalPane: true, c: "#9aa3b5", c2: "#e0b15a" };
    var at = -1, i;
    for (i = 0; i < OSC.length; i++) if (OSC[i] && OSC[i].id === "pspread") at = i;
    if (at >= 0) OSC.splice(at + 1, 0, item);
    else OSC.push(item);
    try {
      var lay = JSON.parse(localStorage.getItem(LAY) || "null");
      var rows = lay && lay.osc;
      if (rows) for (i = 0; i < rows.length; i++) {
        if (rows[i] && rows[i].id === ID) item.on = rows[i].on ? 1 : 0;
      }
    } catch (e) {}
    return true;
  }

  function on(item) {
    return !!(item && (item.on === 1 || item.on === true) && !item.hide);
  }

  function points(n) {
    var d = window.lastBars || [];
    var out = [];
    var i, j, c, sv, ss, b, p, spread, effort, result;
    n = n || 20;
    for (i = 0; i < d.length; i++) {
      b = d[i];
      if (!b || b.time == null || !(b.close > 0)) continue;
      spread = ((b.high - b.low) / b.close) * 100;
      if (i < n) { out.push({ time: b.time }); continue; }
      sv = 0; ss = 0; c = 0;
      for (j = i - n; j < i; j++) {
        p = d[j];
        if (!p || !(p.close > 0)) continue;
        sv += p.volume || 0;
        ss += ((p.high - p.low) / p.close) * 100;
        c++;
      }
      if (!c || !(sv > 0) || !(ss > 0)) { out.push({ time: b.time }); continue; }
      effort = (b.volume || 0) / (sv / c);
      result = spread / (ss / c);
      out.push({ time: b.time, effort: effort, result: result });
    }
    return out;
  }

  function kill() {
    if (chart) { try { chart.remove(); } catch (e) {} }
    chart = null; effortS = null; resultS = null; oneS = null;
  }

  function draw() {
    if (!ensure()) return;
    var item = find();
    var wrap = document.getElementById("oscwrap");
    var pane = document.getElementById("jh-effspread");
    if (!on(item)) {
      kill();
      if (pane && pane.parentNode) pane.parentNode.removeChild(pane);
      if (wrap && !wrap.querySelector(".osc")) { wrap.className = ""; wrap.innerHTML = ""; }
      return;
    }
    if (!wrap || !window.LightweightCharts) return;
    wrap.className = "on";
    if (!pane || !pane.isConnected) {
      kill();
      pane = document.createElement("div");
      pane.id = "jh-effspread";
      pane.className = "osc";
      pane.setAttribute("data-oid", ID);
      pane.style.height = "220px";
      pane.innerHTML = "<div class=osc-head><span class=osc-n>Effort & Spread</span><span class=osc-v id=jh-eff-v></span><button type=button data-act=x title='Remove'>×</button></div><div class=osc-host id=jh-eff-host></div>";
      wrap.appendChild(pane);
      pane.querySelector("[data-act=x]").onclick = function (ev) {
        ev.preventDefault(); ev.stopPropagation();
        item.on = 0;
        try { if (window.jhSaveLay) window.jhSaveLay(); } catch (e) {}
        if (window.paint && window.lastBars && window.lastBars.length) window.paint(window.lastBars);
        else draw();
      };
    }
    var host = document.getElementById("jh-eff-host");
    if (!host) return;
    if (!chart) {
      chart = window.LightweightCharts.createChart(host, {
        layout: { background: { color: "transparent" }, textColor: "#787b86", fontFamily: "IBM Plex Mono, monospace", fontSize: 11 },
        grid: { vertLines: { color: "rgba(42,46,57,.55)" }, horzLines: { color: "rgba(42,46,57,.55)" } },
        rightPriceScale: { borderVisible: false },
        timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
        crosshair: { mode: 0 },
        handleScroll: false,
        handleScale: false
      });
      effortS = chart.addHistogramSeries({
        priceFormat: { type: "custom", minMove: 0.01, formatter: function (v) { return Number(v).toFixed(2) + "×"; } },
        title: "Effort",
        priceLineVisible: false,
        lastValueVisible: false
      });
      resultS = chart.addLineSeries({ color: "#e0b15a", lineWidth: 2, title: "Result", priceLineVisible: false });
      oneS = chart.addLineSeries({ color: "rgba(209,212,220,.35)", lineWidth: 1, lineStyle: 2, title: "1×", priceLineVisible: false, lastValueVisible: false });
    }
    var pts = points(item.p || 20);
    var barsE = [];
    var lineR = [];
    var line1 = [];
    var i, p, yieldR, col;
    for (i = 0; i < pts.length; i++) {
      p = pts[i];
      if (p.effort == null || p.result == null) continue;
      yieldR = p.result / Math.max(p.effort, 1e-6);
      col = p.effort >= 0.85 && yieldR < 0.67 ? "#e0b15a" : (p.effort < 0.85 && yieldR > 1.4 ? "#26c6da" : "#9aa3b5");
      barsE.push({ time: p.time, value: p.effort, color: col });
      lineR.push({ time: p.time, value: p.result });
      line1.push({ time: p.time, value: 1 });
    }
    effortS.setData(barsE);
    resultS.setData(lineR);
    oneS.setData(line1);
    try {
      var main = window.chart || window.jhDeskChart;
      var range = main && main.timeScale && main.timeScale().getVisibleRange();
      if (range && range.from != null) chart.timeScale().setVisibleRange(range);
      else chart.timeScale().fitContent();
    } catch (e2) { try { chart.timeScale().fitContent(); } catch (e3) {} }
    chart.resize(Math.max(10, host.clientWidth), Math.max(10, host.clientHeight));
    var lab = document.getElementById("jh-eff-v");
    var last = barsE.length ? pts[pts.length - 1] : null;
    if (lab && last && last.effort != null) {
      lab.textContent = "effort " + last.effort.toFixed(2) + "×  result " + last.result.toFixed(2) + "×  yield " + Math.round(100 * last.result / Math.max(last.effort, 1e-6)) + "%";
    }
    pane.title = "Steel bars: volume ÷ its prior average. Gold line: price spread ÷ its prior average. Same height as the 1× line means usual effort bought the usual range.";
  }

  function armMenu() {
    var menu = document.getElementById("menu");
    if (!menu || !menu.classList.contains("on") || menu.getAttribute("data-evr") === "1") return;
    var tog = menu.querySelector("[data-tog='pspread']");
    if (!tog) return;
    var row = tog.closest(".irow");
    if (!row || !row.parentNode) return;
    if (!ensure()) return;
    var item = find();
    var live = on(item);
    var div = document.createElement("div");
    div.className = "irow" + (live ? " on" : "");
    div.innerHTML = "<button type=button class=nm data-tog='effspread'>" + (live ? "✓  " : "") + "Effort & Spread</button>";
    row.parentNode.insertBefore(div, row.nextSibling);
    menu.setAttribute("data-evr", "1");
    div.querySelector("button").onclick = function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      item.on = on(item) ? 0 : 1;
      item.hide = false;
      try { if (window.jhSaveLay) window.jhSaveLay(); } catch (e) {}
      if (window.paint && window.lastBars && window.lastBars.length) window.paint(window.lastBars);
      else draw();
      var menuNow = document.getElementById("menu");
      if (menuNow) menuNow.className = "menu";
    };
  }

  function hookPaint() {
    if (!window.paint || window.paint.__evr) return;
    var orig = window.paint;
    function wrapped() {
      var r = orig.apply(this, arguments);
      try { draw(); } catch (e) {}
      return r;
    }
    wrapped.__evr = 1;
    window.paint = wrapped;
  }

  setInterval(function () {
    try { ensure(); hookPaint(); armMenu(); } catch (e) {}
  }, 400);
})();
