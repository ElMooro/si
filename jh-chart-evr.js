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
    var existing = find();
    if (existing) {
      existing.cat = "Volatility";
      existing.externalPane = true;
      if (!existing.n) existing.n = "Effort & Spread";
      return true;
    }
    var item = { id: ID, n: "Effort & Spread", on: 0, cat: "Volatility", p: 20, externalPane: true, c: "#9aa3b5", c2: "#e0b15a" };
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
    if (!menu || !menu.classList.contains("on")) return;
    if (menu.querySelector("[data-tog='effspread']")) return;
    var tog = menu.querySelector("[data-tog='pspread']");
    if (!tog) return;
    var row = tog.closest(".irow");
    if (!row || !row.parentNode) return;
    if (!ensure()) return;
    var item = find();
    var live = on(item);
    var div = document.createElement("div");
    div.className = "irow" + (live ? " on" : "");
    div.innerHTML = "<button type=button class=nm data-tog='effspread'>" + (live ? "\u2713  " : "") + "Effort & Spread</button><button type=button class=gr data-gear='effspread' title='Inputs and colors'>\u2699</button>";
    row.parentNode.insertBefore(div, row.nextSibling);
    div.querySelector("[data-tog]").onclick = function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      item.on = on(item) ? 0 : 1;
      item.hide = false;
      try { if (window.jhSaveLay) window.jhSaveLay(); } catch (e) {}
      var pr = (window.paint && window.lastBars && window.lastBars.length) ? window.paint(window.lastBars) : null;
      if (pr && typeof pr.then === "function") pr.then(function () { draw(); }, function () { draw(); });
      else setTimeout(draw, 0);
      var menuNow = document.getElementById("menu");
      if (menuNow) menuNow.className = "menu";
    };
    var gear = div.querySelector("[data-gear]");
    if (gear) gear.onclick = function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      var menuNow = document.getElementById("menu");
      if (menuNow) menuNow.className = "menu";
      if (window.jhInduxSet) window.jhInduxSet(item, true);
    };
  }

  function hookPaint() {
    if (!window.paint || window.paint.__evr) return;
    var orig = window.paint;
    function wrapped() {
      var r = orig.apply(this, arguments);
      function finish() { try { draw(); } catch (e) {} }
      if (r && typeof r.then === "function") r.then(finish, finish);
      else finish();
      return r;
    }
    wrapped.__evr = 1;
    window.paint = wrapped;
  }

  function watchWrap() {
    var wrap = document.getElementById("oscwrap");
    if (!wrap || wrap.getAttribute("data-evr-watch") === "1") return;
    wrap.setAttribute("data-evr-watch", "1");
    var busy = false;
    new MutationObserver(function () {
      if (busy) return;
      var item = find();
      if (!on(item)) return;
      if (document.getElementById("jh-effspread")) return;
      busy = true;
      setTimeout(function () {
        busy = false;
        try { draw(); } catch (e) {}
      }, 0);
    }).observe(wrap, { childList: true });
  }

  function watchMenu() {
    var menu = document.getElementById("menu");
    if (!menu || menu.getAttribute("data-evr-watch") === "1") return;
    menu.setAttribute("data-evr-watch", "1");
    new MutationObserver(function () {
      try { armMenu(); } catch (e) {}
    }).observe(menu, { childList: true, attributes: true, attributeFilter: ["class"] });
  }

  /* evr-show-2 */
  try { ensure(); hookPaint(); watchWrap(); watchMenu(); armMenu(); } catch (e0) {}
  setInterval(function () {
    try {
      ensure(); hookPaint(); watchWrap(); watchMenu(); armMenu();
      var item = find();
      if (on(item) && !document.getElementById("jh-effspread")) draw();
    } catch (e) {}
  }, 400);
})();
/* corr-fill-1 */
(function () {
  if (window.__jhCorrFill) return;
  window.__jhCorrFill = 1;
  var cache = {};

  function bare(s) {
    s = String(s || "");
    var i = s.lastIndexOf(":");
    return (i >= 0 ? s.slice(i + 1) : s).replace(/[^A-Za-z0-9.\-]/g, "");
  }
  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      if (c === "&") return "&" + "amp;";
      if (c === "<") return "&" + "lt;";
      if (c === ">") return "&" + "gt;";
      return "&" + "quot;";
    });
  }
  function dayKey(t) {
    var n = +t;
    if (n > 1e12) n = Math.floor(n / 1000);
    var d = new Date(n * 1000);
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  }
  function retsOf(bars) {
    var close = {}, order = [], i, t, c, k, prev;
    for (i = 0; i < bars.length; i++) {
      t = +bars[i][0];
      c = +bars[i][4];
      if (!(c > 0) || !isFinite(t)) continue;
      k = dayKey(t);
      if (close[k] == null) order.push(k);
      close[k] = c;
    }
    order.sort(function (a, b) { return a - b; });
    var out = {};
    for (i = 1; i < order.length; i++) {
      prev = close[order[i - 1]];
      if (prev) out[order[i]] = close[order[i]] / prev - 1;
    }
    return out;
  }
  function pearson(a, b) {
    var n = a.length;
    if (n < 8 || b.length !== n) return null;
    var sa = 0, sb = 0, i, ma, mb, num = 0, da = 0, db = 0, xa, xb, den;
    for (i = 0; i < n; i++) { sa += a[i]; sb += b[i]; }
    ma = sa / n; mb = sb / n;
    for (i = 0; i < n; i++) {
      xa = a[i] - ma; xb = b[i] - mb;
      num += xa * xb; da += xa * xa; db += xb * xb;
    }
    den = Math.sqrt(da * db);
    return den ? num / den : 0;
  }
  function pair(a, b) {
    if (!a || !b) return null;
    var keys = [], k;
    for (k in a) if (Object.prototype.hasOwnProperty.call(b, k)) keys.push(+k);
    keys.sort(function (x, y) { return x - y; });
    if (keys.length > 120) keys = keys.slice(-120);
    var xa = [], xb = [], i;
    for (i = 0; i < keys.length; i++) { xa.push(a[keys[i]]); xb.push(b[keys[i]]); }
    return pearson(xa, xb);
  }
  function loadSym(sym) {
    var key = bare(sym).toUpperCase();
    if (!key) return Promise.resolve(null);
    if (cache[key]) return cache[key];
    cache[key] = fetch("/data/warm/tv-bars/universe/US__" + encodeURIComponent(key) + ".json.gz", { credentials: "omit" })
      .then(function (res) {
        if (!res.ok) throw new Error(String(res.status));
        return res.arrayBuffer();
      })
      .then(function (buf) {
        var stream = new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip"));
        return new Response(stream).text();
      })
      .then(function (text) {
        var data = JSON.parse(text);
        if (!data || !data.bars || data.bars.length < 8) throw new Error("short");
        return retsOf(data.bars);
      })
      .catch(function () { delete cache[key]; return null; });
    return cache[key];
  }
  function symbols() {
    var nodes = document.querySelectorAll("#tabs button.tab[data-id]");
    var out = [], i, id;
    for (i = 0; i < nodes.length && out.length < 8; i++) {
      id = nodes[i].getAttribute("data-id");
      if (id && out.indexOf(id) < 0) out.push(id);
    }
    return out.length >= 2 ? out : ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "META"];
  }
  function write(el, syms, maps, tf) {
    var html = "<b data-corrfill=1>CORR · " + esc(tf) + "</b><div class=corm style='grid-template-columns:64px repeat(" + syms.length + ",1fr)'><span></span>";
    html += syms.map(function (s) { return "<span>" + esc(bare(s).slice(0, 5)) + "</span>"; }).join("");
    var i, j, c, bg;
    for (i = 0; i < syms.length; i++) {
      html += "<span>" + esc(bare(syms[i]).slice(0, 6)) + "</span>";
      for (j = 0; j < syms.length; j++) {
        c = i === j ? (maps[i] ? 1 : null) : pair(maps[i], maps[j]);
        bg = c == null ? "var(--chip)" : c >= 0 ? "rgba(8,153,129," + Math.min(0.7, Math.abs(c)) + ")" : "rgba(242,54,69," + Math.min(0.7, Math.abs(c)) + ")";
        html += "<span style='background:" + bg + ";text-align:center;padding:4px;color:#fff'>" + (c == null ? "—" : c.toFixed(2)) + "</span>";
      }
    }
    html += "</div>";
    el.innerHTML = html;
  }
  function needs(el) {
    var text = el.textContent || "";
    if (text.indexOf("CORR") < 0 || text.indexOf("Computing") >= 0) return false;
    if (text.indexOf("—") < 0) return false;
    if (el.querySelector("[data-corrfill]")) return false;
    return true;
  }
  var pending = false;
  function fill() {
    var el = document.getElementById("corr");
    if (!el || pending || !needs(el)) return Promise.resolve();
    var syms = symbols();
    var tf = "1d";
    var m = (el.textContent || "").match(/CORR · (\S+)/);
    if (m) tf = m[1];
    pending = true;
    return Promise.all(syms.map(loadSym)).then(function (maps) {
      pending = false;
      var now = document.getElementById("corr");
      if (!now || !needs(now)) return;
      write(now, syms, maps, tf);
    }).catch(function () { pending = false; });
  }
  function arm() {
    var fn = window.jhRenderCorr;
    if (!fn || fn.__corrfill) return;
    var wrapped = function () {
      return Promise.resolve(fn.apply(this, arguments)).then(function () { return fill(); });
    };
    wrapped.__corrfill = 1;
    window.jhRenderCorr = wrapped;
  }
  arm();
  setInterval(function () { try { arm(); fill(); } catch (e) {} }, 700);
  var host = document.getElementById("corr");
  if (host && window.MutationObserver) {
    new MutationObserver(function () { try { fill(); } catch (e) {} }).observe(host, { childList: true, subtree: true, characterData: true });
  }
  ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "META"].forEach(function (s) { loadSym(s); });
})();
