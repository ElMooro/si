/* Bloomberg <GO> command layer — maps popular Terminal functions onto JustHodl harvests + GP. */
(function () {
  if (window.__jhBbGo) return;
  window.__jhBbGo = true;

  var YELLOW = ["US", "EQUITY", "INDEX", "COMDTY", "CURNCY", "GOVT", "CORP", "CMDTY", "<GO>", "GO"];
  var MAG7 = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"];
  var CATALOG = [
    { id: "DES", n: "Security description", g: "Equity", tab: "over" },
    { id: "FA", n: "Financial analysis", g: "Equity", tab: "fin" },
    { id: "GP", n: "Price graph (default studies)", g: "Chart" },
    { id: "GPC", n: "Candle chart", g: "Chart", kind: "candles" },
    { id: "GPO", n: "Bar chart (OHLC)", g: "Chart", kind: "bars" },
    { id: "GPL", n: "Line chart", g: "Chart", kind: "line" },
    { id: "GIP", n: "Intraday graph + RTH shade", g: "Chart", tf: "5m" },
    { id: "HP", n: "Historical prices", g: "Chart" },
    { id: "RV", n: "Relative value / vs SPY", g: "Equity" },
    { id: "EE", n: "Earnings & estimates", g: "Equity", tab: "est" },
    { id: "ANR", n: "Analyst ratings", g: "Equity", href: "/analyst-actions.html" },
    { id: "CN", n: "Company news", g: "Equity", tab: "news" },
    { id: "DVD", n: "Dividend history", g: "Equity", tab: "div" },
    { id: "HDS", n: "Holders / 13F", g: "Equity", tab: "inst" },
    { id: "OWN", n: "Ownership (13F book)", g: "Equity", ws: "13f" },
    { id: "13F", n: "13F institutional book", g: "Equity", ws: "13f" },
    { id: "13D", n: "Activist 13D", g: "Equity", ws: "act" },
    { id: "SPLC", n: "Supply chain", g: "Equity", ws: "splc" },
    { id: "CF", n: "Company filings", g: "Equity", ws: "cf" },
    { id: "MA", n: "M&A / merger arb", g: "Equity", ws: "ma" },
    { id: "CACS", n: "Corporate actions on chart", g: "Equity" },
    { id: "ERN", n: "Earnings desk", g: "Equity", ws: "earn" },
    { id: "PEAD", n: "Post-earnings drift", g: "Equity", ws: "pead" },
    { id: "SPIN", n: "Spin-offs", g: "Equity", ws: "spin" },
    { id: "OMON", n: "Options monitor", g: "Equity", tab: "opt" },
    { id: "OVME", n: "Options valuation", g: "Equity", ws: "opt" },
    { id: "IVOL", n: "Implied vol / options", g: "Equity", ws: "opt" },
    { id: "QR", n: "Quote recap / tape", g: "Equity" },
    { id: "BQ", n: "Quote panel (DES)", g: "Equity", tab: "over" },
    { id: "ALLQ", n: "All quotes / tape", g: "Equity" },
    { id: "TSM", n: "Trade summary / tape", g: "Equity" },
    { id: "EVT", n: "Events on chart", g: "Chart", ind: "earn" },
    { id: "GPEX", n: "GP with events + news + DVD", g: "Chart" },
    { id: "GPV", n: "GP with volume", g: "Chart" },
    { id: "GPF", n: "Fundamentals overlay (FA)", g: "Chart", tab: "val" },
    { id: "TRA", n: "Total return (indexed 100)", g: "Chart", osc: "tra" },
    { id: "BETA", n: "Beta vs SPY", g: "Chart", osc: "beta" },
    { id: "CORR", n: "Correlation", g: "Market", ws: "corr" },
    { id: "CMP", n: "Compare (add overlay)", g: "Chart" },
    { id: "MAGS", n: "Magnificent 7 vs this name", g: "Chart" },
    { id: "GF", n: "Fundamentals / valuation", g: "Equity", tab: "val" },
    { id: "GE", n: "Estimates graph", g: "Equity", tab: "est" },
    { id: "INS", n: "Insider clusters", g: "Equity", href: "/insider-clusters.html" },
    { id: "SHORT", n: "Short interest", g: "Equity", tab: "short" },
    { id: "SIQ", n: "Short interest quote", g: "Equity", tab: "short" },
    { id: "HOLD", n: "Holders", g: "Equity", tab: "hold" },
    { id: "MEMB", n: "Index members (S&P)", g: "Market", ws: "memb" },
    { id: "FL", n: "Fund flows", g: "Market", ws: "fl" },
    { id: "ETF", n: "ETF desk / holdings", g: "Market", ws: "etf" },
    { id: "PORT", n: "Portfolio", g: "Market", ws: "port" },
    { id: "TOP", n: "Top news", g: "Market", href: "/news.html" },
    { id: "N", n: "News tape", g: "Market", href: "/news.html" },
    { id: "NL", n: "News line", g: "Market", href: "/news.html" },
    { id: "HM", n: "Heat map", g: "Market", ws: "heat" },
    { id: "MOST", n: "Most active / movers", g: "Market", href: "/hot-stocks.html" },
    { id: "WEI", n: "World equity indices", g: "Market", ws: "wei" },
    { id: "EQS", n: "Equity screener", g: "Market" },
    { id: "WATC", n: "Watchlist", g: "Market" },
    { id: "ALRT", n: "Alerts", g: "Market", ws: "alert" },
    { id: "ECO", n: "Economic calendar", g: "Macro", ws: "eco" },
    { id: "YCRV", n: "Yield curve", g: "Macro", href: "/yield-curve.html" },
    { id: "WIRP", n: "Rate-move probabilities", g: "Macro", href: "/implied-prob.html" },
    { id: "FOMC", n: "FOMC monitor", g: "Macro", href: "/fomc.html" },
    { id: "BTMM", n: "Bonds / money markets", g: "Macro", ws: "bonds" },
    { id: "ECST", n: "Economic statistics", g: "Macro", ws: "macro" },
    { id: "COT", n: "CFTC positioning", g: "Macro", ws: "cot" },
    { id: "VIX", n: "VIX curve", g: "Macro", ws: "vix" },
    { id: "CRPR", n: "Credit desk", g: "Macro", href: "/credit-desk.html" },
    { id: "YAS", n: "Yields & spreads", g: "Macro", href: "/yield-curve.html" },
    { id: "DARK", n: "Dark pool / ATS", g: "Market", ws: "dark" },
    { id: "ATS", n: "ATS / institutional volume", g: "Equity", tab: "ivol" },
    { id: "HIVOL", n: "Historical volatility", g: "Studies", osc: "hv" },
    { id: "RSI", n: "RSI 14 study", g: "Studies", osc: "rsi" },
    { id: "MACD", n: "MACD study", g: "Studies", osc: "macd" },
    { id: "STOCH", n: "Stochastic", g: "Studies", osc: "stoch" },
    { id: "ADX", n: "DMI / ADX", g: "Studies", osc: "adx" },
    { id: "ATR", n: "ATR 14", g: "Studies", osc: "atr" },
    { id: "BOLL", n: "Bollinger bands", g: "Studies", ind: "bb" },
    { id: "ICHI", n: "Ichimoku cloud", g: "Studies", ind: "ich" },
    { id: "VWAP", n: "VWAP", g: "Studies", ind: "vwap" },
    { id: "SAR", n: "Parabolic SAR", g: "Studies", ind: "sar" },
    { id: "PIV", n: "Classic pivots", g: "Studies", ind: "piv" },
    { id: "FIB", n: "Auto Fibonacci", g: "Studies", ind: "fibauto" },
    { id: "GAP", n: "Unfilled gaps", g: "Studies", ind: "gaps" },
    { id: "HILO", n: "52-week high / low", g: "Studies", ind: "hilo52" },
    { id: "EMA", n: "EMA 21/50/200 pack", g: "Studies" },
    { id: "SMA", n: "SMA 20/50/200 pack", g: "Studies" },
    { id: "NEWS", n: "News markers", g: "Studies", ind: "news" },
    { id: "INSC", n: "Insider clusters on chart", g: "Studies", ind: "ins" },
    { id: "BUYB", n: "Buyback markers on chart", g: "Studies", ind: "buyb" },
    { id: "GSEAS", n: "Seasonality", g: "Studies", osc: "gseas" },
    { id: "AVG", n: "Historical average ±σ", g: "Studies", ind: "avgdev" },
    { id: "VOL", n: "Volume pane", g: "Studies" },
    { id: "SESS", n: "RTH session shading", g: "Studies", ind: "sess" },
    { id: "FVG", n: "Fair value gaps", g: "Studies", ind: "fvg" },
    { id: "EQH", n: "Equal highs / lows", g: "Studies", ind: "eqh" },
    { id: "OR", n: "Opening range 15/30", g: "Studies", ind: "or15" },
    { id: "IB", n: "Initial balance", g: "Studies", ind: "ib" },
    { id: "ONH", n: "Overnight high / low", g: "Studies", ind: "onhl" },
    { id: "ADR", n: "ADR 20 expected range", g: "Studies", ind: "adr" },
    { id: "LRCH", n: "LinReg channel 2σ", g: "Studies", ind: "lrch" },
    { id: "EAVWAP", n: "AVWAP from last earnings", g: "Studies", ind: "eavwap" },
    { id: "GSESS", n: "Asia / London / NY sessions", g: "Studies", ind: "gsess" },
    { id: "SEP", n: "Session separators", g: "Studies", ind: "sep" },
    { id: "RATIO", n: "Price ratio vs SPY", g: "Studies", osc: "ratio" },
    { id: "GPDESK", n: "Desk template (SMA/ADR/FVG/events)", g: "Chart" },
    { id: "LIN", n: "Linear scale", g: "Chart", scale: 0 },
    { id: "LOG", n: "Log scale", g: "Chart", scale: 1 },
    { id: "PCT", n: "Percent scale", g: "Chart", scale: 2 },
    { id: "IDX", n: "Index 100 scale", g: "Chart", scale: 3 },
    { id: "MULT", n: "Multi-pane layout", g: "Chart" },
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
    /* a lone ticker is not stolen from symbol search; yellow-key / page GO still treat it as GP */
    if (!fn) return { fn: null, sym: tok[0] || null, extra: tok };
    return { fn: fn, sym: syms[0] || null, extra: tok };
  }

  function activeSym() {
    return String(window.jhActive || window.active || "SPY").split(":").pop();
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
    var ctx = ctxWin();
    if (ctx.jhSetKind) { ctx.jhSetKind(k); return; }
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
    var ctx = ctxWin();
    if (ctx.jhSetTf) { ctx.jhSetTf(tf); return; }
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
  function setScale(m) {
    var ctx = ctxWin();
    if (ctx.jhSetScale) ctx.jhSetScale(m);
  }

  function openPage(href, title) {
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

  function gpDefault(ctx) {
    var list = ctx.INDS || [];
    var any = false;
    for (var i = 0; i < list.length; i++) if (list[i].on) { any = true; break; }
    if (any) return;
    ["sma50", "sma200", "earn"].forEach(function (id) { toggleInd(id, true); });
    if (ctx.jhSetVol) ctx.jhSetVol(true);
  }

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
    if (spec.tf) {
      setTf(spec.tf);
      if (fn === "GIP") toggleInd("sess", true);
      return { ok: true, fn: fn };
    }
    if (spec.scale != null) { setScale(spec.scale); return { ok: true, fn: fn }; }
    if (fn === "GP") {
      gpDefault(ctx);
      if (opts.page && !ctx.jhGoSymbol) {
        location.href = "/chart.html?s=" + encodeURIComponent(sym || activeSym() || "SPY");
      }
      return { ok: true, fn: fn, sym: sym || activeSym() };
    }
    if (fn === "HP") { if (!renderHp() && opts.page) location.href = "/chart.html?s=" + encodeURIComponent(sym || activeSym()) + "&fn=HP"; return { ok: true, fn: fn }; }
    if (fn === "RV") {
      toggleOsc("rsline");
      toggleOsc("beta");
      if (ctx.jhOpenDataTypePanel) ctx.jhOpenDataTypePanel("val");
      return { ok: true, fn: fn };
    }
    if (fn === "CMP" && sym && ctx.jhAddCompare) { ctx.jhAddCompare(sym); return { ok: true, fn: fn, sym: sym }; }
    if (fn === "MAGS") {
      setScale(2);
      MAG7.forEach(function (s) {
        if (s === activeSym()) return;
        if (ctx.jhAddCompare) ctx.jhAddCompare(s);
      });
      return { ok: true, fn: fn };
    }
    if (fn === "GPEX") {
      gpDefault(ctx);
      ["earn", "news", "dvd", "split"].forEach(function (id) { toggleInd(id, true); });
      return { ok: true, fn: fn };
    }
    if (fn === "GPV") {
      gpDefault(ctx);
      if (ctx.jhSetVol) ctx.jhSetVol(true);
      return { ok: true, fn: fn };
    }
    if (fn === "CACS") {
      ["dvd", "split", "earn"].forEach(function (id) { toggleInd(id, true); });
      return { ok: true, fn: fn };
    }
    if (fn === "QR" || fn === "ALLQ" || fn === "TSM") {
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
    if (fn === "EMA") {
      ["ema21", "ema50", "ema200"].forEach(function (id) { toggleInd(id, true); });
      return { ok: true, fn: fn };
    }
    if (fn === "VOL" && ctx.jhSetVol) { ctx.jhSetVol(true); return { ok: true, fn: fn }; }
    if (fn === "GPDESK") {
      gpDefault(ctx);
      ["sma50", "sma200", "earn", "keylv", "adr", "fvg"].forEach(function (id) { toggleInd(id, true); });
      if (ctx.jhSetVol) ctx.jhSetVol(true);
      return { ok: true, fn: fn };
    }
    if (fn === "MULT" && ctx.jhSetLayout) { ctx.jhSetLayout(2); return { ok: true, fn: fn }; }
    if (fn === "HELP" && ctx.jhOpenWorkspace) { ctx.jhOpenWorkspace("go"); return { ok: true, fn: fn }; }
    if (opts.page && spec.href) { location.href = spec.href; return { ok: true, fn: fn }; }
    if (opts.page) {
      location.href = "/chart.html?s=" + encodeURIComponent(sym || "SPY") + "&fn=" + encodeURIComponent(fn);
      return { ok: true, fn: fn, sym: sym };
    }
    return { ok: true, fn: fn, sym: sym || activeSym() };
  }

  function tryRun(raw, opts) {
    opts = opts || {};
    var p = parse(raw);
    if (!p) return { ok: false, err: "Type a ticker or a function (AAPL  ·  AAPL DES  ·  MOST)" };
    if (!p.fn && p.sym && (opts.page || opts.yellow)) p.fn = "GP";
    if (!p.fn) return { ok: false, err: "Type a ticker or a function (AAPL  ·  AAPL DES  ·  MOST)" };
    return runFn(p.fn, p.sym, opts);
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
      if (!el || (el.id !== "ssin" && el.id !== "cmdin" && el.id !== "symin")) return;
      var raw = String(el.value || "");
      var p = parse(raw);
      if (!p || !p.fn) return;
      e.preventDefault();
      e.stopPropagation();
      tryRun(raw);
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
