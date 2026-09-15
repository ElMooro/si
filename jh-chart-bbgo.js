/* Bloomberg <GO> command layer — maps popular Terminal functions onto JustHodl harvests + GP. */
(function () {
  if (window.__jhBbGo) return;
  window.__jhBbGo = true;

  var YELLOW = ["US", "EQUITY", "INDEX", "COMDTY", "CURNCY", "GOVT", "CORP", "CMDTY", "<GO>", "GO"];
  var CATALOG = [
    { id: "DES", n: "Security description", g: "Equity", tab: "over" },
    { id: "FA", n: "Financial analysis", g: "Equity", tab: "fin" },
    { id: "GP", n: "Price graph", g: "Chart" },
    { id: "GPC", n: "Candle chart", g: "Chart", kind: "candles" },
    { id: "GPO", n: "Bar chart (OHLC)", g: "Chart", kind: "bars" },
    { id: "GPL", n: "Line chart", g: "Chart", kind: "line" },
    { id: "GIP", n: "Intraday graph", g: "Chart", tf: "5m" },
    { id: "HP", n: "Historical prices", g: "Chart" },
    { id: "RV", n: "Relative value / vs SPY", g: "Equity" },
    { id: "EE", n: "Earnings & estimates", g: "Equity", tab: "est" },
    { id: "ANR", n: "Analyst ratings", g: "Equity", href: "/analyst-actions.html" },
    { id: "CN", n: "Company news", g: "Equity", tab: "news" },
    { id: "DVD", n: "Dividend history", g: "Equity", tab: "div" },
    { id: "HDS", n: "Holders / 13F", g: "Equity", tab: "inst" },
    { id: "13F", n: "13F institutional book", g: "Equity", ws: "13f" },
    { id: "OMON", n: "Options monitor", g: "Equity", tab: "opt" },
    { id: "QR", n: "Quote recap / tape", g: "Equity" },
    { id: "EVT", n: "Events on chart", g: "Chart", ind: "earn" },
    { id: "TRA", n: "Total return (indexed 100)", g: "Chart", osc: "tra" },
    { id: "BETA", n: "Beta vs SPY", g: "Chart", osc: "beta" },
    { id: "CORR", n: "Correlation", g: "Market", ws: "corr" },
    { id: "CMP", n: "Compare (add overlay)", g: "Chart" },
    { id: "GF", n: "Fundamentals / valuation", g: "Equity", tab: "val" },
    { id: "GE", n: "Estimates graph", g: "Equity", tab: "est" },
    { id: "INS", n: "Insider clusters", g: "Equity", href: "/insider-clusters.html" },
    { id: "SHORT", n: "Short interest", g: "Equity", tab: "short" },
    { id: "HOLD", n: "Holders", g: "Equity", tab: "hold" },
    { id: "TOP", n: "Top news", g: "Market", href: "/news.html" },
    { id: "N", n: "News tape", g: "Market", href: "/news.html" },
    { id: "HM", n: "Heat map", g: "Market", ws: "heat" },
    { id: "MOST", n: "Most active / movers", g: "Market", href: "/hot-stocks.html" },
    { id: "WEI", n: "World equity indices", g: "Market", href: "/global-cycle.html" },
    { id: "ECO", n: "Economic calendar", g: "Macro", href: "/econ-calendar.html" },
    { id: "YCRV", n: "Yield curve", g: "Macro", href: "/yield-curve.html" },
    { id: "WIRP", n: "Rate-move probabilities", g: "Macro", href: "/implied-prob.html" },
    { id: "FOMC", n: "FOMC monitor", g: "Macro", href: "/fomc.html" },
    { id: "BTMM", n: "Bonds / money markets", g: "Macro", ws: "bonds" },
    { id: "ECST", n: "Economic statistics", g: "Macro", ws: "macro" },
    { id: "WATC", n: "Watchlist", g: "Market" },
    { id: "EQS", n: "Equity screener", g: "Market" },
    { id: "RSI", n: "RSI 14 study", g: "Studies", osc: "rsi" },
    { id: "MACD", n: "MACD study", g: "Studies", osc: "macd" },
    { id: "BOLL", n: "Bollinger bands", g: "Studies", ind: "bb" },
    { id: "ICHI", n: "Ichimoku cloud", g: "Studies", ind: "ich" },
    { id: "VWAP", n: "VWAP", g: "Studies", ind: "vwap" },
    { id: "SMA", n: "SMA 20/50/200 pack", g: "Studies" },
    { id: "NEWS", n: "News markers", g: "Studies", ind: "news" },
    { id: "GSEAS", n: "Seasonality", g: "Studies", osc: "gseas" },
    { id: "AVG", n: "Historical average ±σ", g: "Studies", ind: "avgdev" },
    { id: "VOL", n: "Volume pane", g: "Studies" },
    { id: "HELP", n: "Function list", g: "System", ws: "go" }
  ];
  var BY = {};
  CATALOG.forEach(function (f) { BY[f.id] = f; });

  function catalog() { return CATALOG.slice(); }

  function parse(raw) {
    var s = String(raw || "").toUpperCase().replace(/<GO>/g, " ").replace(/\s+/g, " ").trim();
    if (!s) return null;
    var tok = s.split(" ").filter(function (t) { return t && YELLOW.indexOf(t) < 0; });
    if (!tok.length) return null;
    var fn = null, syms = [];
    tok.forEach(function (t) {
      if (BY[t] && !fn) fn = t;
      else if (/^[A-Z][A-Z0-9.\-]{0,15}$/.test(t) && t !== fn) syms.push(t);
    });
    if (!fn && tok.length === 1 && BY[tok[0]]) fn = tok[0];
    if (!fn && tok.length === 1) return { fn: "GP", sym: tok[0] };
    return { fn: fn, sym: syms[0] || null, extra: tok };
  }

  function activeSym() {
    return String(window.jhActive || window.active || "SPY").split(":").pop();
  }

  function loadSym(sym) {
    if (!sym) return;
    if (window.jhGoSymbol) window.jhGoSymbol(sym, "chart");
  }

  function ctxWin() {
    if (window.parent && window.parent !== window && window.parent.INDS) return window.parent;
    if (window.parent && window.parent !== window && window.parent.jhGoSymbol) return window.parent;
    return window;
  }
  function toggleInd(id, on) {
    var ctx = ctxWin();
    var list = ctx.INDS || [];
    for (var i = 0; i < list.length; i++) if (list[i].id === id) list[i].on = on !== false ? true : !list[i].on;
    if (ctx.paint && ctx.lastBars) ctx.paint(ctx.lastBars);
    if (ctx.jhSaveLay) ctx.jhSaveLay();
  }
  function toggleOsc(id) {
    var ctx = ctxWin();
    var list = ctx.OSC || [];
    for (var i = 0; i < list.length; i++) if (list[i].id === id) list[i].on = true;
    if (ctx.paint && ctx.lastBars) ctx.paint(ctx.lastBars);
    if (ctx.jhSaveLay) ctx.jhSaveLay();
  }
  function setKind(k) {
    try {
      var btn = document.getElementById("btn-kind");
      if (window.jhSetKind) window.jhSetKind(k);
    } catch (e) {}
    var kinds = { candles: 1, bars: 1, line: 1, hollow: 1, area: 1, heikin: 1 };
    if (!kinds[k]) return;
    var ev = document.createEvent("Event");
    /* engine kind is closed over — click the menu path via a custom hook */
    window.__jhKind = k;
    var drop = document.getElementById("btn-kind");
    if (drop) {
      drop.click();
      setTimeout(function () {
        var b = document.querySelector("#menu [data-k='" + k + "']");
        if (b) b.click();
      }, 0);
    }
  }
  function setTf(tf) {
    var b = document.querySelector("#tfbar [data-tf='" + tf + "']");
    if (b) b.click();
    else {
      var more = document.getElementById("btn-tfmore");
      if (more) {
        more.click();
        setTimeout(function () {
          var x = document.querySelector("#menu [data-tfm='" + tf + "']");
          if (x) x.click();
        }, 0);
      }
    }
  }

  function openPage(href, title) {
    if (window.top && window.top !== window && window.top.jhOpenWorkspace) {
      /* still an iframe — navigate top */
    }
    if (window.jhOpenWorkspace && href.indexOf("/bb-go") < 0) {
      /* dedicated pages via overlay */
    }
    var host = window.top || window;
    if (host !== window && host.location) {
      /* keep chart; overlay from parent if possible */
    }
    if (window.jhOpenWorkspace) {
      var ov = document.getElementById("ws-overlay");
      if (ov) {
        ov.className = "on";
        ov.innerHTML = "<div class=wsbox><div class=wshd><span>" + (title || href) + "</span><a href='" + href + "' target=_blank rel=noopener>Open page ↗</a><button type=button class=x id=wsx>×</button></div><iframe src='" + href + "' title='" + (title || "") + "'></iframe></div>";
        var x = document.getElementById("wsx");
        if (x) x.onclick = function () { ov.className = ""; };
        ov.onclick = function (e) { if (e.target === ov) ov.className = ""; };
        return true;
      }
    }
    if (window.top && window.top !== window) window.top.location.href = href;
    else location.href = href;
    return true;
  }

  function renderHp() {
    var ctx = ctxWin();
    var d = ctx.lastBars || window.lastBars || [];
    var rows = d.slice(-60).reverse();
    var html = "<div style='padding:14px;font-family:IBM Plex Mono,monospace;font-size:12px;color:#d1d4dc'><b>HP · " + activeSym() + " · last " + rows.length + " bars · " + (window.tf || "") + "</b><table style='width:100%;margin-top:10px;border-collapse:collapse'>";
    html += "<tr style='color:#787b86'><th>Date</th><th>O</th><th>H</th><th>L</th><th>C</th><th>Vol</th><th>%</th></tr>";
    rows.forEach(function (b, i) {
      var prev = rows[i + 1];
      var chg = prev && prev.close ? (b.close - prev.close) / prev.close * 100 : 0;
      var t = new Date(b.time * 1000).toISOString().slice(0, 10);
      html += "<tr><td>" + t + "</td><td>" + n(b.open) + "</td><td>" + n(b.high) + "</td><td>" + n(b.low) + "</td><td>" + n(b.close) + "</td><td>" + (b.volume ? Math.round(b.volume).toLocaleString() : "—") + "</td><td style=color:" + (chg >= 0 ? "#089981" : "#f23645") + ">" + (chg >= 0 ? "+" : "") + chg.toFixed(2) + "</td></tr>";
    });
    html += "</table><div style='color:#787b86;margin-top:8px'>Warehouse bars, delayed. Not Bloomberg HP.</div></div>";
    var ov = document.getElementById("ws-overlay");
    if (!ov) return false;
    ov.className = "on";
    ov.innerHTML = "<div class=wsbox><div class=wshd><span>HP · Historical prices</span><button type=button class=x id=wsx>×</button></div><div id=ws-hp style='overflow:auto;height:100%'>" + html + "</div></div>";
    document.getElementById("wsx").onclick = function () { ov.className = ""; };
    return true;
  }
  function n(x) { x = Number(x); return isFinite(x) ? x.toFixed(2) : "—"; }

  function runFn(fn, sym, opts) {
    opts = opts || {};
    var spec = BY[fn];
    if (!spec) return { ok: false, err: "Unknown function " + fn };
    var ctx = ctxWin();
    if (sym && ctx.jhGoSymbol) ctx.jhGoSymbol(sym, "chart");
    if (spec.tab && ctx.jhOpenDataTypePanel) {
      setTimeout(function () { ctx.jhOpenDataTypePanel(spec.tab); }, 80);
      return { ok: true, fn: fn, sym: sym || activeSym() };
    }
    if (spec.ws && ctx.jhOpenWorkspace) {
      ctx.jhOpenWorkspace(spec.ws);
      return { ok: true, fn: fn, sym: sym || activeSym() };
    }
    if (spec.href) {
      if (ctx.jhOpenWorkspace) openPage.call(ctx, spec.href, spec.n);
      else if (opts.page) location.href = spec.href;
      else openPage(spec.href, spec.n);
      return { ok: true, fn: fn, sym: sym || activeSym() };
    }
    if (spec.ind) { (ctx.toggleInd || toggleInd)(spec.ind, true); return { ok: true, fn: fn }; }
    if (spec.osc) { toggleOsc(spec.osc); return { ok: true, fn: fn }; }
    if (spec.kind) { setKind(spec.kind); return { ok: true, fn: fn }; }
    if (spec.tf) { setTf(spec.tf); return { ok: true, fn: fn }; }
    if (fn === "GP") return { ok: true, fn: fn, sym: sym || activeSym() };
    if (fn === "HP") { if (!renderHp() && opts.page) location.href = "/chart.html?s=" + encodeURIComponent(sym || activeSym()) + "&fn=HP"; return { ok: true, fn: fn }; }
    if (fn === "RV") {
      toggleOsc("rsline");
      toggleOsc("beta");
      if (ctx.jhOpenDataTypePanel) ctx.jhOpenDataTypePanel("val");
      return { ok: true, fn: fn };
    }
    if (fn === "CMP" && sym && ctx.jhAddCompare) { ctx.jhAddCompare(sym); return { ok: true, fn: fn, sym: sym }; }
    if (fn === "QR") {
      var q = document.getElementById("qqr");
      if (q) q.click();
      return { ok: true, fn: fn };
    }
    if (fn === "WATC") {
      var fab = document.getElementById("listfab");
      if (fab) fab.click();
      return { ok: true, fn: fn };
    }
    if (fn === "EQS") {
      var sc = document.querySelector("[data-w='screen']");
      if (sc) sc.click();
      return { ok: true, fn: fn };
    }
    if (fn === "SMA") {
      ["sma20", "sma50", "sma200"].forEach(function (id) { toggleInd(id, true); });
      return { ok: true, fn: fn };
    }
    if (fn === "VOL" && window.jhSetVol) { window.jhSetVol(true); return { ok: true, fn: fn }; }
    if (fn === "HELP" && ctx.jhOpenWorkspace) { ctx.jhOpenWorkspace("go"); return { ok: true, fn: fn }; }
    if (opts.page && spec.href) { location.href = spec.href; return { ok: true, fn: fn }; }
    if (opts.page) {
      location.href = "/chart.html?s=" + encodeURIComponent(sym || "SPY") + "&fn=" + encodeURIComponent(fn);
      return { ok: true, fn: fn, sym: sym };
    }
    return { ok: true, fn: fn, sym: sym || activeSym() };
  }

  function tryRun(raw, opts) {
    var p = parse(raw);
    if (!p || !p.fn) return { ok: false, err: "Type a function (DES FA GP MOST ECO…) or TICKER FN" };
    return runFn(p.fn, p.sym, opts || {});
  }

  window.jhBbGo = {
    catalog: catalog,
    parse: parse,
    tryRun: tryRun,
    run: runFn
  };

  function bindInputs() {
    document.addEventListener("keydown", function (e) {
      if (e.key !== "Enter") return;
      var el = e.target;
      if (!el || (el.id !== "ssin" && el.id !== "cmdin" && el.id !== "symin" && el.id !== "q")) return;
      var raw = String(el.value || "");
      var p = parse(raw);
      if (!p || !p.fn) return;
      /* only steal the event when a Bloomberg function is present */
      e.preventDefault();
      e.stopPropagation();
      tryRun(raw);
      if (el.id === "ssin" && window.jhGoSymbol) { /* already loaded inside runFn */ }
    }, true);
  }

  function applyQuery() {
    try {
      var u = new URLSearchParams(location.search);
      var fn = u.get("fn");
      var s = u.get("s") || u.get("symbol");
      if (fn) setTimeout(function () { runFn(String(fn).toUpperCase(), s ? String(s).toUpperCase() : null); }, 400);
    } catch (e) {}
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", function () { bindInputs(); applyQuery(); });
  else { bindInputs(); applyQuery(); }
})();
