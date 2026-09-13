/* JustHodl Chart engine v3 — TradingView-native. Does not touch Chart Pro. */
(function () {
  if (window.__jhChartEngineV3) return;
  window.__jhChartEngineV3 = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var TFS = [
    ["1s", "1s", "1m", "1d"],
    ["1m", "1m", "1m", "5d"],
    ["1h", "1h", "60m", "2y"],
    ["4h", "4h", "60m", "2y"],
    ["12h", "12h", "60m", "2y"],
    ["1d", "D", "1d", "5y"],
    ["2d", "2D", "1d", "5y"],
    ["5d", "5D", "1d", "5y"],
    ["1w", "W", "1wk", "10y"],
    ["2w", "2W", "1wk", "10y"],
    ["1M", "M", "1mo", "10y"],
    ["3M", "3M", "1d", "10y"]
  ];
  var CHG = [["price", "Price"], ["dod", "DoD"], ["wow", "WoW"], ["mom", "MoM"], ["qoq", "QoQ"], ["yoy", "YoY"], ["ytd", "YTD"], ["fromhigh", "From High"], ["fromlow", "From Low"], ["vsspy", "vs SPY"]];
  var BARS = { dod: 1, wow: 5, mom: 21, qoq: 63, yoy: 252 };
  var TABS = ["BTCUSDT", "ETHB", "PEPEUSDT", "CNEQ", "PURR", "BMNR", "ATO"];
  var ISHARES = ["GSG", "COMT", "EWZS", "CMDY", "EWZ", "LOCK", "IAT", "IVV", "IWM", "EEM", "LQD", "HYG", "TLT", "IEI"];
  var INDS = [
    { id: "sma50", n: "SMA 50 close", c: "#2962ff", on: 1, p: 50, k: "sma" },
    { id: "sma250", n: "SMA 250 close", c: "#e91e63", on: 1, p: 250, k: "sma" },
    { id: "sma200", n: "SMA 200 close", c: "#ff6d00", on: 1, p: 200, k: "sma" },
    { id: "ema250", n: "EMA 250 close", c: "#089981", on: 1, p: 250, k: "ema" },
    { id: "sma20", n: "SMA 20 close", c: "#26c6da", on: 1, p: 20, k: "sma" },
    { id: "bb", n: "BB 20 SMA close 2", c: "#ab47bc", on: 0, k: "bb" }
  ];
  var KINDS = [["candles", "Candles"], ["bars", "Bars"], ["line", "Line"], ["area", "Area"]];
  var active = "PEPEUSDT", tf = "1d", mode = "price", kind = "candles", logScale = false;
  var quotes = {}, lists = [], listId = "ishares", letter = "", filter = "", lastBars = [], series = [], spyBars = null;
  var UP = "#089981", DN = "#f23645", BG = "#ffffff", CUSTOM_KEY = "jh-chart-custom-lists";

  function isCrypto(s) { return /USDT$|BUSD$|USDC$|-USD$/.test(String(s || "").toUpperCase()); }
  function bare(s) { s = String(s || ""); return s.indexOf(":") >= 0 ? s.split(":").pop() : s; }
  function yahooSym(s) {
    s = bare(s).toUpperCase();
    if (/USDT$/.test(s)) return s.slice(0, -4) + "-USD";
    if (/BUSD$/.test(s)) return s.slice(0, -4) + "-USD";
    if (/USDC$/.test(s)) return s.slice(0, -4) + "-USD";
    return s;
  }
  function fmt(p) {
    if (p == null || !isFinite(p)) return "—";
    var a = Math.abs(p);
    if (a >= 1000) return p.toLocaleString(undefined, { maximumFractionDigits: 2 });
    if (a >= 1) return p.toFixed(2);
    if (a >= 0.01) return p.toFixed(4);
    return Number(p).toPrecision(6).replace(/0+$/, "").replace(/\.$/, "");
  }
  function sma(d, n) {
    var o = [], s = 0;
    for (var i = 0; i < d.length; i++) {
      s += d[i].close;
      if (i >= n) s -= d[i - n].close;
      if (i >= n - 1) o.push({ time: d[i].time, value: s / n });
    }
    return o;
  }
  function ema(d, n) {
    if (!d.length) return [];
    var o = [], k = 2 / (n + 1), p = d[0].close;
    for (var i = 0; i < d.length; i++) {
      p = d[i].close * k + p * (1 - k);
      if (i >= n - 1) o.push({ time: d[i].time, value: p });
    }
    return o;
  }
  function uniq(rows) {
    var out = [], last = -1;
    for (var i = 0; i < rows.length; i++) {
      var t = rows[i].time;
      if (!t || t <= last || !isFinite(rows[i].close)) continue;
      out.push(rows[i]);
      last = t;
    }
    return out;
  }
  function toBars(j) {
    if (!j) return [];
    var rows = j.bars || j.ohlc || j.results || j.obs || j.points || j.data || (Array.isArray(j) ? j : []);
    var out = [];
    for (var i = 0; i < rows.length; i++) {
      var b = rows[i];
      if (Array.isArray(b)) {
        var t = +b[0];
        if (t > 1e12) t = Math.floor(t / 1000);
        var c = b[4] != null ? +b[4] : +b[1];
        if (!isFinite(c)) continue;
        out.push({ time: t, open: +(b[1] || c), high: +(b[2] || c), low: +(b[3] || c), close: c, volume: +(b[5] || 0) });
      } else {
        var tm = b.time || b.t || b.date;
        var c2 = b.close != null ? b.close : (b.c != null ? b.c : b.value);
        if (c2 == null) continue;
        if (typeof tm === "string") tm = Math.floor(Date.parse(tm.length <= 10 ? tm + "T00:00:00Z" : tm) / 1000);
        if (tm > 1e12) tm = Math.floor(tm / 1000);
        var c3 = +c2;
        if (!isFinite(c3)) continue;
        out.push({ time: +tm, open: +(b.open || b.o || c3), high: +(b.high || b.h || c3), low: +(b.low || b.l || c3), close: c3, volume: +(b.volume || b.v || b.value || 0) });
      }
    }
    if (!out.length && j.chart && j.chart.result && j.chart.result[0]) {
      var res = j.chart.result[0], q = (res.indicators.quote || [])[0] || {}, ts = res.timestamp || [];
      for (var k = 0; k < ts.length; k++) {
        if (q.close[k] == null || !isFinite(+q.close[k])) continue;
        out.push({ time: ts[k], open: +(q.open[k] || q.close[k]), high: +(q.high[k] || q.close[k]), low: +(q.low[k] || q.close[k]), close: +q.close[k], volume: +(q.volume[k] || 0) });
      }
    }
    if (!out.length && Array.isArray(j.timestamp) && Array.isArray(j.close)) {
      for (var i2 = 0; i2 < j.timestamp.length; i2++) {
        if (j.close[i2] == null || !isFinite(+j.close[i2])) continue;
        out.push({ time: j.timestamp[i2], open: +(j.open && j.open[i2] || j.close[i2]), high: +(j.high && j.high[i2] || j.close[i2]), low: +(j.low && j.low[i2] || j.close[i2]), close: +j.close[i2], volume: +(j.volume && j.volume[i2] || 0) });
      }
    }
    return uniq(out);
  }
  async function fetchJson(url) {
    var r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error(String(r.status));
    return r.json();
  }
  function spec(tfId) {
    for (var i = 0; i < TFS.length; i++) if (TFS[i][0] === tfId) return TFS[i];
    return TFS[5];
  }
  async function klines(sym, tfId) {
    var t = bare(sym);
    var sp = spec(tfId);
    var yInterval = sp[2], range = sp[3];
    var ys = yahooSym(t);
    var urls = [
      PROXY + "/yf-ohlc?symbol=" + encodeURIComponent(ys) + "&range=" + range + "&interval=" + yInterval,
      PROXY + "/yf-ohlc?symbol=" + encodeURIComponent(t) + "&range=" + range + "&interval=" + yInterval,
      PROXY + "/ohlc?ticker=" + encodeURIComponent(t),
      "/data/series/" + encodeURIComponent(ys) + ".json"
    ];
    var lastErr = "";
    for (var i = 0; i < urls.length; i++) {
      try {
        var d = toBars(await fetchJson(urls[i]));
        if (d.length >= 8) return d;
      } catch (e) { lastErr = String(e && e.message || e); }
    }
    console.warn("jh-chart klines miss", t, lastErr);
    return [];
  }
  function computeChange(d, m) {
    if (m === "price" || !d.length) return null;
    var closes = d.map(function (b) { return { time: b.time, value: b.close }; });
    if (m === "ytd") {
      var yr = new Date().getUTCFullYear(), base = null, out = [];
      for (var i = 0; i < closes.length; i++) {
        var y = new Date(closes[i].time * 1000).getUTCFullYear();
        if (y === yr && base == null) base = closes[i].value;
        if (base) out.push({ time: closes[i].time, value: (closes[i].value / base - 1) * 100 });
      }
      return out;
    }
    if (m === "fromhigh" || m === "fromlow") {
      var cutoff = d[d.length - 1].time - 365 * 86400, out2 = [];
      for (var i = 0; i < closes.length; i++) {
        var ext = m === "fromhigh" ? -1e99 : 1e99;
        for (var j = 0; j <= i; j++) {
          if (closes[j].time < closes[i].time - 365 * 86400 && closes[j].time < cutoff) continue;
          var v = closes[j].value;
          if (m === "fromhigh") { if (v > ext) ext = v; } else if (v < ext) ext = v;
        }
        if (ext && isFinite(ext)) out2.push({ time: closes[i].time, value: (closes[i].value / ext - 1) * 100 });
      }
      return out2;
    }
    var n = BARS[m] || 1, out3 = [];
    for (var i = n; i < closes.length; i++) {
      var then = closes[i - n].value;
      if (!then) continue;
      out3.push({ time: closes[i].time, value: (closes[i].value / then - 1) * 100 });
    }
    return out3;
  }
  async function vsSpy(d) {
    if (!spyBars) spyBars = await klines("SPY", "1d");
    var spy = spyBars || [];
    if (spy.length < 2) return [];
    var j = 0, joined = [];
    for (var i = 0; i < d.length; i++) {
      var ts = d[i].time;
      while (j + 1 < spy.length && Math.abs(spy[j + 1].time - ts) <= Math.abs(spy[j].time - ts)) j++;
      if (Math.abs(spy[j].time - ts) > 7 * 86400) continue;
      joined.push({ time: ts, value: d[i].close, spy: spy[j].close });
    }
    if (joined.length < 2) return [];
    var t0 = joined[0].value, s0 = joined[0].spy;
    return joined.map(function (p) { return { time: p.time, value: ((p.value / t0) / (p.spy / s0) - 1) * 100 }; });
  }
  function loadCustom() {
    try {
      var arr = JSON.parse(localStorage.getItem(CUSTOM_KEY) || "[]");
      return Array.isArray(arr) ? arr : [];
    } catch (e) { return []; }
  }
  function saveCustom(name, symbols) {
    var arr = loadCustom();
    var hit = arr.filter(function (l) { return l.name === name; })[0];
    if (hit) {
      var u = {};
      hit.symbols.concat(symbols).forEach(function (s) { u[s] = 1; });
      hit.symbols = Object.keys(u);
      hit.n = hit.symbols.length;
    } else {
      hit = { id: "custom-" + Date.now(), name: name, symbols: symbols, n: symbols.length, custom: 1 };
      arr.push(hit);
    }
    localStorage.setItem(CUSTOM_KEY, JSON.stringify(arr));
    return hit;
  }
  function parsePaste(text) {
    var out = [];
    String(text || "").split(/[\s,;]+/).forEach(function (t) {
      t = t.replace(/^["']|["']$/g, "").trim();
      if (t && /^[A-Za-z0-9_.:\-]+$/.test(t) && /[A-Za-z]/.test(t)) out.push(t.toUpperCase());
    });
    return out.filter(function (t, i, a) { return a.indexOf(t) === i; });
  }

  var host = document.getElementById("host");
  var LW = window.LightweightCharts;
  if (!LW || !host) {
    var qel = document.getElementById("quote");
    if (qel) qel.textContent = "Chart library missing";
    return;
  }
  var chart = LW.createChart(host, {
    autoSize: true,
    layout: { background: { type: "solid", color: BG }, textColor: "#6a6d78", fontFamily: "IBM Plex Sans,sans-serif", fontSize: 11 },
    grid: { vertLines: { color: "#f0f3fa" }, horzLines: { color: "#f0f3fa" } },
    rightPriceScale: { borderColor: "#e0e3eb", scaleMargins: { top: 0.06, bottom: 0.18 } },
    timeScale: { borderColor: "#e0e3eb", timeVisible: true },
    localization: { priceFormatter: function (p) { return mode === "price" ? fmt(p) : p.toFixed(2) + "%"; } }
  });
  function wipe() { series.forEach(function (s) { try { chart.removeSeries(s); } catch (e) {} }); series = []; }
  async function paint(d) {
    if (!d || !d.length) {
      document.getElementById("quote").textContent = "No bars for " + active + " — warehouse/proxy miss";
      return;
    }
    wipe(); lastBars = d;
    chart.applyOptions({ localization: { priceFormatter: function (p) { return mode === "price" ? fmt(p) : p.toFixed(2) + "%"; } } });
    try { chart.priceScale("right").applyOptions({ mode: logScale && mode === "price" ? 1 : 0 }); } catch (e) {}
    if (mode === "price") {
      var c;
      if (kind === "line") {
        c = chart.addLineSeries({ color: UP, lineWidth: 2 });
        c.setData(d.map(function (b) { return { time: b.time, value: b.close }; }));
      } else if (kind === "area") {
        c = chart.addAreaSeries({ lineColor: UP, topColor: "rgba(8,153,129,0.28)", bottomColor: "rgba(8,153,129,0.02)" });
        c.setData(d.map(function (b) { return { time: b.time, value: b.close }; }));
      } else if (kind === "bars") {
        c = chart.addBarSeries({ upColor: UP, downColor: DN });
        c.setData(d);
      } else {
        c = chart.addCandlestickSeries({ upColor: UP, downColor: DN, borderUpColor: UP, borderDownColor: DN, wickUpColor: UP, wickDownColor: DN });
        c.setData(d);
      }
      series.push(c);
      var v = chart.addHistogramSeries({ priceFormat: { type: "volume" }, priceScaleId: "vol" });
      chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
      v.setData(d.map(function (b) { return { time: b.time, value: b.volume, color: b.close >= b.open ? "rgba(8,153,129,.35)" : "rgba(242,54,69,.35)" }; }));
      series.push(v);
      INDS.forEach(function (ind) {
        if (!ind.on) return;
        if (ind.k === "sma") { var s = chart.addLineSeries({ color: ind.c, lineWidth: 1, lastValueVisible: false, priceLineVisible: false }); s.setData(sma(d, ind.p)); series.push(s); }
        if (ind.k === "ema") { var s2 = chart.addLineSeries({ color: ind.c, lineWidth: 1, lastValueVisible: false, priceLineVisible: false }); s2.setData(ema(d, ind.p)); series.push(s2); }
        if (ind.k === "bb") {
          var m = sma(d, 20), up = [], dn = [];
          for (var i = 19; i < d.length; i++) {
            var ss = 0;
            for (var j = 0; j < 20; j++) { var dv = d[i - j].close - m[i - 19].value; ss += dv * dv; }
            var sd = Math.sqrt(ss / 20);
            up.push({ time: d[i].time, value: m[i - 19].value + 2 * sd });
            dn.push({ time: d[i].time, value: m[i - 19].value - 2 * sd });
          }
          [up, m, dn].forEach(function (x) { var s3 = chart.addLineSeries({ color: ind.c, lineWidth: 1, lastValueVisible: false, priceLineVisible: false }); s3.setData(x); series.push(s3); });
        }
      });
    } else {
      var pct = mode === "vsspy" ? await vsSpy(d) : computeChange(d, mode);
      var h = chart.addHistogramSeries({ priceFormat: { type: "percent" } });
      h.setData((pct || []).map(function (p) { return { time: p.time, value: p.value, color: p.value >= 0 ? UP : DN }; }));
      series.push(h);
    }
    chart.timeScale().fitContent();
    var last = d[d.length - 1], prev = d[d.length - 2] || last;
    var chg = prev.close ? (last.close - prev.close) / prev.close : 0, up = chg >= 0;
    document.getElementById("quote").innerHTML = "<b>" + active + " · " + tf + " · " + mode + "</b> <span>O" + fmt(last.open) + " H<span class=up>" + fmt(last.high) + "</span> L<span class=dn>" + fmt(last.low) + "</span> C<span class=" + (up ? "up" : "dn") + ">" + fmt(last.close) + "</span></span> <span class=" + (up ? "up" : "dn") + ">" + (up ? "+" : "") + fmt(last.close - last.open) + " (" + (chg * 100).toFixed(2) + "%)</span> <span style='margin-left:auto' class=sell>" + fmt(last.close) + " SELL</span> <span class=buy>" + fmt(last.close * 1.001) + " BUY</span>";
    var hi = Math.max.apply(null, d.slice(-40).map(function (b) { return b.high; }));
    var lo = Math.min.apply(null, d.slice(-40).map(function (b) { return b.low; }));
    var yhi = Math.max.apply(null, d.map(function (b) { return b.high; }));
    var ylo = Math.min.apply(null, d.map(function (b) { return b.low; }));
    var dp = Math.min(100, Math.max(0, (last.close - lo) / (hi - lo || 1) * 100));
    var yp = Math.min(100, Math.max(0, (last.close - ylo) / (yhi - ylo || 1) * 100));
    document.getElementById("detail").innerHTML = "<div style=font-weight:600>" + active + "</div><div class=px>" + fmt(last.close) + "</div><div class=" + (up ? "up" : "dn") + ">" + (up ? "+" : "") + fmt(last.close - prev.close) + " " + (chg * 100).toFixed(2) + "%</div><div style='margin-top:8px;font-size:10px;color:var(--mut)'>DAY RANGE</div><div class=rg><i style=width:" + dp + "%></i><b style=left:" + dp + "%></b></div><div style=display:flex;justify-content:space-between;font-size:10px;font-family:IBM+Plex+Mono,monospace><span>" + fmt(lo) + "</span><span>" + fmt(hi) + "</span></div><div style='margin-top:8px;font-size:10px;color:var(--mut)'>52-WEEK RANGE</div><div class=rg><i style=width:" + yp + "%></i><b style=left:" + yp + "%></b></div><div style=display:flex;justify-content:space-between;font-size:10px;font-family:IBM+Plex+Mono,monospace><span>" + fmt(ylo) + "</span><span>" + fmt(yhi) + "</span></div>";
  }
  async function load() {
    try {
      await paint(await klines(active, tf));
    } catch (e) {
      document.getElementById("quote").textContent = "Chart error: " + (e && e.message || e);
    }
  }
  function renderTabs() {
    document.getElementById("tabs").innerHTML =
      "<a class='tab brand' href='/'>JustHodl</a>" +
      TABS.map(function (s) {
        var q = quotes[s], up = q && q.chg >= 0;
        return "<button class='tab " + (s === active ? "on" : "") + "' data-id='" + s + "'>" + s.replace("USDT", "") + (q ? " <span class=" + (up ? "up" : "dn") + ">" + fmt(q.last) + " " + (up ? "+" : "") + (q.chg * 100).toFixed(2) + "%</span>" : "") + "</button>";
      }).join("") +
      "<button class=tab id=add>+</button>" +
      "<a class='tab pro' href='/chart-pro.html' title='Chart Pro safety net'>Pro</a>";
    document.querySelectorAll(".tab[data-id]").forEach(function (b) { b.onclick = function () { active = b.dataset.id; renderTabs(); renderLegend(); load(); }; });
    var add = document.getElementById("add");
    if (add) add.onclick = function () { document.getElementById("q").focus(); };
  }
  function renderTf() {
    document.getElementById("tfbar").innerHTML = TFS.map(function (t) { return "<button class='" + (t[0] === tf ? "on" : "") + "' data-tf='" + t[0] + "'>" + t[1] + "</button>"; }).join("") +
      KINDS.map(function (k) { return "<button class='" + (k[0] === kind ? "on" : "") + "' data-k='" + k[0] + "'>" + k[1] + "</button>"; }).join("") +
      "<button class='" + (logScale ? "on" : "") + "' id=log>log</button>";
    document.querySelectorAll("#tfbar [data-tf]").forEach(function (b) { b.onclick = function () { tf = b.dataset.tf; renderTf(); load(); }; });
    document.querySelectorAll("#tfbar [data-k]").forEach(function (b) { b.onclick = function () { kind = b.dataset.k; renderTf(); if (lastBars.length) paint(lastBars); }; });
    var log = document.getElementById("log");
    if (log) log.onclick = function () { logScale = !logScale; renderTf(); if (lastBars.length) paint(lastBars); };
    document.getElementById("chgbar").innerHTML = CHG.map(function (t) { return "<button class='" + (t[0] === mode ? "on" : "") + "' data-m='" + t[0] + "'>" + t[1] + "</button>"; }).join("");
    document.querySelectorAll("#chgbar [data-m]").forEach(function (b) { b.onclick = function () { mode = b.dataset.m; renderTf(); if (lastBars.length) paint(lastBars); }; });
  }
  function renderLegend() {
    document.getElementById("legend").innerHTML = "<div style='color:#131722;font-weight:500;margin-bottom:4px'>" + active + "</div>" + INDS.map(function (i) { return "<button style=color:" + (i.on ? i.c : "#6a6d78") + " data-i='" + i.id + "'>" + i.n + "</button>"; }).join("");
    document.querySelectorAll("#legend [data-i]").forEach(function (b) { b.onclick = function () { var i = INDS.find(function (x) { return x.id === b.dataset.i; }); i.on = !i.on; renderLegend(); if (lastBars.length) paint(lastBars); }; });
  }
  function renderRail() {
    document.getElementById("rail").innerHTML = [["cursor", "+"], ["trend", "/"], ["hline", "—"], ["rect", "[]"], ["fib", "fib"], ["text", "T"]].map(function (x) {
      return "<button title='" + x[0] + "'>" + x[1] + "</button>";
    }).join("");
  }
  function currentSyms() {
    var L = lists.find(function (x) { return x.id === listId; }) || lists[0] || { symbols: ISHARES };
    return (L.symbols || []).filter(function (s) {
      var b = bare(s).toUpperCase();
      if (letter && b.indexOf(letter) !== 0) return false;
      if (filter && b.indexOf(filter.toUpperCase()) < 0) return false;
      return true;
    }).slice(0, 120);
  }
  async function lastPx(sym) {
    try {
      var d = await klines(sym, "1d");
      if (d.length < 2) return null;
      var last = d[d.length - 1], prev = d[d.length - 2] || last;
      return { last: last.close, chg: prev.close ? (last.close - prev.close) / prev.close : 0, chgv: last.close - prev.close };
    } catch (e) { return null; }
  }
  function renderLetters() {
    document.getElementById("letters").innerHTML = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("").map(function (L) {
      return "<button class='" + (letter === L ? "on" : "") + "' data-l='" + L + "'>" + L + "</button>";
    }).join("");
    document.querySelectorAll("#letters [data-l]").forEach(function (b) { b.onclick = function () { letter = letter === b.dataset.l ? "" : b.dataset.l; renderLetters(); renderList(); }; });
  }
  function renderList() {
    var sel = document.getElementById("list");
    if (sel && !sel.dataset.bound) {
      sel.onchange = function () { listId = sel.value; renderList(); };
      sel.dataset.bound = "1";
    }
    if (sel) {
      var keep = sel.value;
      sel.innerHTML = lists.map(function (l) { return "<option value='" + l.id + "'" + (l.id === listId ? " selected" : "") + ">" + l.name + " (" + (l.n || (l.symbols || []).length) + ")</option>"; }).join("");
      if (keep && lists.some(function (l) { return l.id === keep; })) { sel.value = keep; listId = keep; }
    }
    document.getElementById("nlists").textContent = lists.length + " lists";
    var box = document.getElementById("wlist");
    var syms = currentSyms();
    box.innerHTML = syms.map(function (s) {
      var q = quotes[s] || quotes[bare(s)];
      var up = !q || q.chg >= 0;
      return "<button class='wrow " + (bare(s) === active ? "on" : "") + "' data-s='" + s + "'><span>" + s + "</span><span>" + (q ? fmt(q.last) : "—") + "</span><span class=" + (up ? "up" : "dn") + ">" + (q ? (q.chg >= 0 ? "+" : "") + (q.chg * 100).toFixed(2) + "%" : "—") + "</span><span class=" + (up ? "up" : "dn") + ">" + (q ? (q.chgv >= 0 ? "+" : "") + fmt(q.chgv) : "—") + "</span></button>";
    }).join("") || "<div style='padding:12px;color:var(--mut)'>No symbols in this filter</div>";
    box.querySelectorAll("[data-s]").forEach(function (b) {
      b.onclick = function () {
        var s = b.dataset.s;
        if (TABS.indexOf(s) < 0 && TABS.indexOf(bare(s)) < 0) TABS.push(bare(s));
        active = bare(s);
        renderTabs(); renderLegend(); load();
      };
    });
    var q = document.getElementById("q");
    if (q && !q.dataset.bound) {
      q.oninput = function () {
        filter = q.value;
        renderHits();
        renderList();
      };
      q.onkeydown = function (e) {
        if (e.key === "Enter" && q.value.trim()) {
          var s = q.value.trim().toUpperCase();
          if (TABS.indexOf(s) < 0) TABS.push(s);
          active = s; q.value = ""; filter = "";
          renderTabs(); renderLegend(); renderList(); load();
        }
      };
      q.dataset.bound = "1";
    }
    var paste = document.getElementById("paste");
    if (paste && !paste.dataset.bound) {
      paste.onkeydown = function (e) {
        if (e.key !== "Enter") return;
        var syms2 = parsePaste(paste.value);
        if (!syms2.length) return;
        var saved = saveCustom("Paste " + new Date().toISOString().slice(0, 16), syms2);
        lists = [saved].concat(lists.filter(function (l) { return l.id !== saved.id; }));
        listId = saved.id; paste.value = "";
        renderList();
      };
      paste.dataset.bound = "1";
    }
    syms.slice(0, 24).forEach(function (s) {
      if (quotes[s] || quotes[bare(s)]) return;
      lastPx(s).then(function (px) {
        if (!px) return;
        quotes[s] = px; quotes[bare(s)] = px;
        renderList(); renderTabs();
      });
    });
  }
  function renderHits() {
    var needle = (document.getElementById("q").value || "").trim().toLowerCase();
    var box = document.getElementById("hits");
    if (needle.length < 2) { box.innerHTML = ""; return; }
    var out = [];
    for (var i = 0; i < lists.length && out.length < 40; i++) {
      var L = lists[i];
      (L.symbols || []).forEach(function (s) {
        if (out.length >= 40) return;
        if (String(s).toLowerCase().indexOf(needle) >= 0) out.push({ s: s, list: L.name });
      });
    }
    box.innerHTML = out.map(function (h) {
      return "<button class=hit data-s='" + h.s + "'><span>" + h.s + "</span><span>" + h.list + "</span></button>";
    }).join("");
    box.querySelectorAll("[data-s]").forEach(function (b) {
      b.onclick = function () {
        var s = b.dataset.s;
        if (TABS.indexOf(bare(s)) < 0) TABS.push(bare(s));
        active = bare(s); document.getElementById("q").value = ""; filter = "";
        renderTabs(); renderLegend(); renderHits(); renderList(); load();
      };
    });
  }
  async function loadLists() {
    var local = [
      { id: "ishares", name: "iShares ETFs — BlackRock", symbols: ISHARES, n: ISHARES.length },
      { id: "tabs", name: "Open tabs", symbols: TABS, n: TABS.length }
    ];
    var custom = loadCustom();
    var copied = [];
    try {
      var j = await fetchJson("/data/tv-watchlists.json");
      var arr = Array.isArray(j) ? j : (j.lists || []);
      copied = arr.filter(function (l) { return l && l.name && Array.isArray(l.symbols); }).map(function (l) {
        return { id: String(l.id || l.name), name: l.name, symbols: l.symbols, n: l.n || l.symbols.length };
      });
    } catch (e) {}
    lists = custom.concat(local, copied);
  }
  async function loadIntel() {
    try {
      var j = await fetchJson("/data/jh-internals.json");
      var f = j.fields || j;
      document.getElementById("intel").innerHTML = "<b>INTERNALS · warehouse</b>" +
        [["2s10s", f.twos_tens != null ? f.twos_tens + "%" : "—"],
         ["LIQ $B", f.liq_proxy_bn != null ? f.liq_proxy_bn : "—"],
         ["NFCI", f.nfci != null ? f.nfci : "—"],
         ["A-D", f.ad_breadth != null ? Number(f.ad_breadth).toFixed(3) : "—"],
         ["NH-NL", f.nh_nl != null ? f.nh_nl + " (" + (f.n_new_high || "—") + "H / " + (f.n_new_low || "—") + "L)" : "—"]
        ].map(function (x) { return "<div class=cell><span>" + x[0] + "</span><span>" + x[1] + "</span></div>"; }).join("") +
        "<div class=cell><span>as of</span><span>" + String(j.generated_at || "").slice(0, 19) + "</span></div>";
    } catch (e) {}
  }
  function clock() {
    var el = document.getElementById("clock");
    if (el) el.textContent = new Date().toISOString().slice(11, 19) + " UTC";
  }
  TABS.forEach(function (s) {
    lastPx(s).then(function (px) { if (px) { quotes[s] = px; renderTabs(); } });
  });
  renderTabs(); renderTf(); renderLegend(); renderRail(); renderLetters();
  loadLists().then(renderList);
  loadIntel();
  load();
  clock(); setInterval(clock, 1000);
})();
