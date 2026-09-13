/* JustHodl Chart engine — copy of Pro lists/features. Does not touch Chart Pro. */
(function () {
  if (window.__jhChartEngine) return;
  window.__jhChartEngine = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var TFS = [["5m","5m"],["1h","1H"],["1d","1D"],["1w","1W"],["1M","1M"],["5Y","5Y"],["MAX","MAX"],["1s","1s"],["1m","1m"],["4h","4h"],["12h","12h"],["3M","3M"]];
  var CHG = [["price","Price"],["dod","DoD"],["wow","WoW"],["mom","MoM"],["qoq","QoQ"],["yoy","YoY"],["ytd","YTD"],["fromhigh","From High"],["fromlow","From Low"],["vsspy","vs SPY"]];
  var BARS = { dod:1, wow:5, mom:21, qoq:63, yoy:252 };
  var TABS = ["BTCUSDT","ETHB","PEPEUSDT","CNEQ","PURR","BMNR","ATO"];
  var ISHARES = ["GSG","COMT","EWZS","CMDY","EWZ","LOCK","IAT","IVV","IWM","EEM","LQD","HYG","TLT","IEI"];
  var INDS = [
    {id:"sma50",n:"SMA 50 close",c:"#2962ff",on:1,p:50,k:"sma"},
    {id:"sma250",n:"SMA 250 close",c:"#e91e63",on:1,p:250,k:"sma"},
    {id:"sma200",n:"SMA 200 close",c:"#ff6d00",on:1,p:200,k:"sma"},
    {id:"ema250",n:"EMA 250 close",c:"#089981",on:1,p:250,k:"ema"},
    {id:"sma20",n:"SMA 20 close",c:"#26c6da",on:1,p:20,k:"sma"},
    {id:"bb",n:"BB 20 SMA close 2",c:"#ab47bc",on:0,k:"bb"}
  ];
  var KINDS = [["candles","Candles"],["bars","Bars"],["line","Line"]];
  var active = "PEPEUSDT", tf = "1d", mode = "price", kind = "candles", logScale = false;
  var quotes = {}, lists = [], listId = "ishares", letter = "", filter = "", lastBars = [], series = [], spyBars = null;
  var UP = "#089981", DN = "#f23645", BG = "#ffffff";
  var CUSTOM_KEY = "jh-chart-custom-lists";

  function isBin(s) { return /USDT$|BUSD$|USDC$/.test(String(s)); }
  function bare(s) { s = String(s || ""); return s.indexOf(":") >= 0 ? s.split(":").pop() : s; }
  function fmt(p) {
    if (p == null || !isFinite(p)) return "—";
    var a = Math.abs(p);
    if (a >= 1000) return p.toLocaleString(undefined, { maximumFractionDigits: 2 });
    if (a >= 1) return p.toFixed(2);
    if (a >= 0.01) return p.toFixed(4);
    return Number(p).toFixed(8).replace(/0+$/, "").replace(/\.$/, "");
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
    var o = [], k = 2 / (n + 1), p = d[0] && d[0].close;
    for (var i = 0; i < d.length; i++) {
      p = d[i].close * k + p * (1 - k);
      if (i >= n - 1) o.push({ time: d[i].time, value: p });
    }
    return o;
  }
  function toBars(j) {
    if (!j) return [];
    var rows = j.bars || j.ohlc || j.results || j.obs || j.points || j.data || (Array.isArray(j) ? j : []);
    var out = [];
    for (var i = 0; i < rows.length; i++) {
      var b = rows[i];
      if (Array.isArray(b)) {
        var t = b[0];
        if (t > 1e12) t = Math.floor(t / 1000);
        if (typeof t === "string") t = Math.floor(Date.parse(t) / 1000);
        var c = b[4] != null ? +b[4] : +b[1];
        if (!c) continue;
        out.push({ time: t, open: +(b[1] || c), high: +(b[2] || c), low: +(b[3] || c), close: c, volume: +(b[5] || 0) });
      } else {
        var tm = b.time || b.t || b.date || b[0];
        var c2 = b.close != null ? b.close : (b.c != null ? b.c : b.value);
        if (c2 == null) continue;
        if (typeof tm === "string") tm = Math.floor(Date.parse(tm.length <= 10 ? tm + "T00:00:00Z" : tm) / 1000);
        if (tm > 1e12) tm = Math.floor(tm / 1000);
        out.push({ time: +tm, open: +(b.open || b.o || c2), high: +(b.high || b.h || c2), low: +(b.low || b.l || c2), close: +c2, volume: +(b.volume || b.v || 0) });
      }
    }
    return out.filter(function (x) { return x.time && isFinite(x.close); });
  }
  function synth(sym, n) {
    var h = 2166136261;
    for (var i = 0; i < sym.length; i++) h = Math.imul(h ^ sym.charCodeAt(i), 16777619);
    var rng = h >>> 0 || 1;
    var rnd = function () { rng = (Math.imul(1664525, rng) + 1013904223) >>> 0; return rng / 4294967296; };
    var px = /PEPE/i.test(sym) ? 0.0000034 : (/BTC/i.test(sym) ? 70000 : 50 + rnd() * 100);
    var now = Math.floor(Date.now() / 1000) - 86400, out = [];
    for (var i = n; i >= 0; i--) {
      var o = px;
      px = Math.max(px * (1 + (rnd() - 0.48) * 0.03), 1e-8);
      out.push({ time: now - i * 86400, open: o, high: Math.max(o, px) * 1.01, low: Math.min(o, px) * 0.99, close: px, volume: 1e6 * rnd() });
    }
    return out;
  }
  async function fetchJson(url) {
    var r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw 0;
    return r.json();
  }
  async function klines(sym, tfId) {
    var t = bare(sym);
    var range = tfId === "MAX" ? "max" : (tfId === "5Y" ? "5y" : (tfId === "5m" || tfId === "1m" || tfId === "1s" ? "5d" : "5y"));
    var interval = tfId === "5m" || tfId === "1m" || tfId === "1s" ? "5m" : tfId === "1h" || tfId === "4h" || tfId === "12h" ? "60m" : tfId === "1w" ? "1wk" : tfId === "1M" ? "1mo" : "1d";
    var binIv = tfId === "5m" ? "5m" : tfId === "1h" ? "1h" : tfId === "1d" ? "1d" : tfId === "1w" ? "1w" : tfId === "1m" ? "1m" : tfId === "4h" ? "4h" : "1d";
    var urls = [];
    if (isBin(sym) || isBin(t)) {
      urls.push("https://api.binance.com/api/v3/klines?symbol=" + t + "&interval=" + binIv + "&limit=1000");
      urls.push(PROXY + "/ohlc?ticker=" + encodeURIComponent(t));
    }
    urls.push(PROXY + "/yf-ohlc?symbol=" + encodeURIComponent(t) + "&range=" + range + "&interval=" + interval);
    urls.push("https://query1.finance.yahoo.com/v8/finance/chart/" + encodeURIComponent(t) + "?range=" + range + "&interval=" + interval);
    urls.push("/data/series/" + encodeURIComponent(t) + ".json");
    for (var i = 0; i < urls.length; i++) {
      try {
        var j = await fetchJson(urls[i]);
        var d = toBars(j);
        if (!d.length && j.chart && j.chart.result && j.chart.result[0]) {
          var res = j.chart.result[0], q = res.indicators.quote[0], ts = res.timestamp || [];
          for (var k = 0; k < ts.length; k++) if (q.close[k] != null) d.push({ time: ts[k], open: q.open[k], high: q.high[k], low: q.low[k], close: q.close[k], volume: q.volume[k] || 0 });
        }
        if (d.length >= 8) return d;
      } catch (e) {}
    }
    return synth(t, 400);
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
      var w = Math.min(closes.length, 260), out2 = [];
      for (var i = 0; i < closes.length; i++) {
        var ext = m === "fromhigh" ? -1e99 : 1e99;
        for (var j = Math.max(0, i - w + 1); j <= i; j++) {
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
    if (!spyBars) spyBars = await klines("SPY", "MAX");
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

  var chart = LightweightCharts.createChart(document.getElementById("host"), {
    autoSize: true,
    layout: { background: { type: "solid", color: BG }, textColor: "#6a6d78", fontFamily: "IBM Plex Sans,sans-serif", fontSize: 11 },
    grid: { vertLines: { color: "#f0f3fa" }, horzLines: { color: "#f0f3fa" } },
    rightPriceScale: { borderColor: "#e0e3eb", scaleMargins: { top: 0.06, bottom: 0.18 } },
    timeScale: { borderColor: "#e0e3eb", timeVisible: true },
    localization: { priceFormatter: function (p) { return mode === "price" ? fmt(p) : p.toFixed(2) + "%"; } }
  });
  function wipe() { series.forEach(function (s) { try { chart.removeSeries(s); } catch (e) {} }); series = []; }
  async function paint(d) {
    if (!d || !d.length) return;
    wipe(); lastBars = d;
    chart.applyOptions({ localization: { priceFormatter: function (p) { return mode === "price" ? fmt(p) : p.toFixed(2) + "%"; } } });
    try { chart.priceScale("right").applyOptions({ mode: logScale && mode === "price" ? 1 : 0 }); } catch (e) {}
    if (mode === "price") {
      var c;
      if (kind === "line") {
        c = chart.addLineSeries({ color: UP, lineWidth: 2 });
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
        if (ind.k === "ema") { var s = chart.addLineSeries({ color: ind.c, lineWidth: 1, lastValueVisible: false, priceLineVisible: false }); s.setData(ema(d, ind.p)); series.push(s); }
        if (ind.k === "bb") {
          var m = sma(d, 20), up = [], dn = [];
          for (var i = 19; i < d.length; i++) {
            var ss = 0;
            for (var j = 0; j < 20; j++) { var dv = d[i - j].close - m[i - 19].value; ss += dv * dv; }
            var sd = Math.sqrt(ss / 20);
            up.push({ time: d[i].time, value: m[i - 19].value + 2 * sd });
            dn.push({ time: d[i].time, value: m[i - 19].value - 2 * sd });
          }
          [up, m, dn].forEach(function (x) { var s = chart.addLineSeries({ color: ind.c, lineWidth: 1, lastValueVisible: false, priceLineVisible: false }); s.setData(x); series.push(s); });
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
  async function load() { paint(await klines(active, tf)); }
  function renderTabs() {
    document.getElementById("tabs").innerHTML = TABS.map(function (s) {
      var q = quotes[s], up = q && q.chg >= 0;
      return "<button class='tab " + (s === active ? "on" : "") + "' data-id='" + s + "'>" + s.replace("USDT", "") + (q ? " <span class=" + (up ? "up" : "dn") + ">" + fmt(q.last) + " " + (up ? "+" : "") + (q.chg * 100).toFixed(2) + "%</span>" : "") + "</button>";
    }).join("") + "<button class=tab id=add>+</button>";
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
      if (isBin(sym)) {
        var j = await fetchJson("https://api.binance.com/api/v3/ticker/24hr?symbol=" + sym);
        return { last: +j.lastPrice, chg: +j.priceChangePercent / 100, chgv: +j.priceChange };
      }
      var d = await klines(sym, "1d");
      var last = d[d.length - 1], prev = d[d.length - 2] || last;
      return { last: last.close, chg: prev.close ? (last.close - prev.close) / prev.close : 0, chgv: last.close - prev.close };
    } catch (e) { return null; }
  }
  function searchHits() {
    var needle = filter.trim().toLowerCase();
    if (needle.length < 2) return [];
    var out = [];
    for (var i = 0; i < lists.length && out.length < 40; i++) {
      var L = lists[i];
      for (var j = 0; j < (L.symbols || []).length && out.length < 40; j++) {
        if (String(L.symbols[j]).toLowerCase().indexOf(needle) >= 0) out.push({ s: L.symbols[j], n: L.name });
      }
    }
    return out;
  }
  async function renderWatch() {
    var sel = document.getElementById("list");
    sel.innerHTML = lists.map(function (l) { return "<option value='" + l.id + "' " + (l.id === listId ? "selected" : "") + ">" + l.name + " (" + (l.n || l.symbols.length) + ")</option>"; }).join("");
    sel.onchange = function () { listId = sel.value; renderWatch(); };
    document.getElementById("nlists").textContent = lists.length + " lists";
    var letters = document.getElementById("letters");
    letters.innerHTML = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("").map(function (L) { return "<button class='" + (letter === L ? "on" : "") + "' data-l='" + L + "'>" + L + "</button>"; }).join("");
    letters.querySelectorAll("button").forEach(function (b) { b.onclick = function () { letter = letter === b.dataset.l ? "" : b.dataset.l; renderWatch(); }; });
    var hits = searchHits();
    var hitBox = document.getElementById("hits");
    if (hitBox) {
      hitBox.innerHTML = hits.map(function (h) { return "<button class=hit data-s='" + h.s + "'><span>" + bare(h.s) + "</span><span>" + h.n + "</span></button>"; }).join("");
      hitBox.querySelectorAll(".hit").forEach(function (b) {
        b.onclick = function () {
          var s = bare(b.dataset.s);
          active = s;
          if (TABS.indexOf(s) < 0) TABS.push(s);
          renderTabs(); renderLegend(); load();
        };
      });
    }
    var syms = currentSyms();
    document.getElementById("wlist").innerHTML = syms.map(function (s) { return "<button class=wrow data-s='" + s + "'><span>" + bare(s) + "</span><span>—</span><span>—</span><span>—</span></button>"; }).join("");
    document.querySelectorAll(".wrow").forEach(function (b) {
      b.onclick = function () {
        var s = bare(b.dataset.s);
        active = s;
        if (TABS.indexOf(s) < 0) TABS.push(s);
        renderTabs(); renderLegend(); load();
      };
    });
    var batch = syms.slice(0, 24);
    for (var i = 0; i < batch.length; i++) {
      (function (s) {
        lastPx(isBin(s) ? s : bare(s)).then(function (q) {
          if (!q) return;
          quotes[bare(s)] = q;
          var row = [].slice.call(document.querySelectorAll(".wrow")).find(function (x) { return bare(x.dataset.s) === bare(s); });
          if (!row) return;
          var up = q.chg >= 0;
          row.children[1].textContent = fmt(q.last);
          row.children[2].textContent = (up ? "+" : "") + fmt(q.chgv);
          row.children[2].className = up ? "up" : "dn";
          row.children[3].textContent = (up ? "+" : "") + (q.chg * 100).toFixed(2) + "%";
          row.children[3].className = up ? "up" : "dn";
          renderTabs();
        });
      })(batch[i]);
    }
  }
  async function bootLists() {
    lists = loadCustom().concat([
      { id: "ishares", name: "iShares ETFs — BlackRock", symbols: ISHARES, n: ISHARES.length },
      { id: "tabs", name: "Open tabs", symbols: TABS, n: TABS.length }
    ]);
    try {
      var j = await fetchJson("/data/tv-watchlists.json");
      var arr = Array.isArray(j) ? j : (j.lists || []);
      arr.forEach(function (l) { if (l && l.name && l.symbols) lists.push({ id: String(l.id || l.name), name: l.name, symbols: l.symbols, n: l.n || l.symbols.length }); });
    } catch (e) {}
    renderWatch();
  }
  async function bootIntel() {
    try {
      var j = await fetchJson("/data/jh-internals.json");
      var f = j.fields || {};
      var ad = f.ad_breadth != null ? Number(f.ad_breadth).toFixed(3) + " (" + f.n_up + "/" + f.n_down + ")" : "—";
      var nh = f.nh_nl != null ? (f.nh_nl + " (" + f.n_new_high + "H / " + f.n_new_low + "L)") : "—";
      document.getElementById("intel").innerHTML = "<b>INTERNALS · warehouse</b>"
        + "<div class=cell><span>2s10s</span><span>" + (f.twos_tens != null ? f.twos_tens + "%" : "—") + "</span></div>"
        + "<div class=cell><span>LIQ $B</span><span>" + (f.liq_proxy_bn || "—") + "</span></div>"
        + "<div class=cell><span>NFCI</span><span>" + (f.nfci || "—") + "</span></div>"
        + "<div class=cell><span>A-D</span><span>" + ad + "</span></div>"
        + "<div class=cell><span>%>50d</span><span>" + (f.pct_above_50 != null ? (f.pct_above_50 * 100).toFixed(1) + "%" : "—") + "</span></div>"
        + "<div class=cell><span>%>200d</span><span>" + (f.pct_above_200 != null ? (f.pct_above_200 * 100).toFixed(1) + "%" : "—") + "</span></div>"
        + "<div class=cell><span>NH-NL</span><span>" + nh + "</span></div>";
    } catch (e) { document.getElementById("intel").textContent = "internals offline"; }
  }
  document.getElementById("rail").innerHTML = ["+","/","—","[]"].map(function (x, i) { return "<button " + (i === 0 ? "class=on" : "") + ">" + x + "</button>"; }).join("");
  document.getElementById("q").oninput = function (e) { filter = e.target.value; renderWatch(); };
  var paste = document.getElementById("paste");
  if (paste) paste.onkeydown = function (e) {
    if (e.key !== "Enter") return;
    var syms = parsePaste(e.target.value);
    if (!syms.length) return;
    var saved = saveCustom("Paste " + new Date().toISOString().slice(0, 16), syms);
    lists = [saved].concat(lists.filter(function (l) { return l.id !== saved.id; }));
    listId = saved.id;
    e.target.value = "";
    renderWatch();
  };
  function clock() { document.getElementById("clock").textContent = new Date().toISOString().slice(11, 19) + " UTC"; }
  renderTabs(); renderTf(); renderLegend(); load(); bootLists(); bootIntel(); clock();
  setInterval(clock, 1000);
})();
