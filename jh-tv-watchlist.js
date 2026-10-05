/* jh-reskin-skip */
/* jh-tv-watchlist — TradingView-style watchlist for chart.html.
 * Lists, sections (###NAME), colour flags + flagged lists, sortable columns, multi-select, drag & drop,
 * context menus, keyboard navigation, import/export (TradingView .txt format), details panel.
 * Storage: localStorage "jh-tvwl-v1" (seeded once from the chart's existing lists, Chart Pro lists and the
 * 492 TradingView lists in /data/tv-watchlists.json). Quotes: PROXY /quote?ids= (same endpoint Chart Pro uses).
 */
(function (root) {
  "use strict";
  if (root.JHTvWatchlist) return;
  var doc = root.document;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var KEY = "jh-tvwl-v1";
  var FLAGS = [["red", "#f23645"], ["blue", "#2962ff"], ["green", "#089981"], ["orange", "#ff9800"], ["purple", "#9c27b0"], ["cyan", "#00bcd4"], ["pink", "#e91e63"]];
  var FLAG_HEX = {}; FLAGS.forEach(function (f) { FLAG_HEX[f[0]] = f[1]; });
  var COLS = [
    ["last", "Last", true], ["chg", "Chg", true], ["chgp", "Chg%", true],
    ["m1", "1M%", false], ["m3", "3M%", false], ["y1", "1Y%", false], ["date", "Date", false], ["spark", "Spark", false]
  ];

  // ------------------------------------------------------------------ helpers
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function uid() { return "l" + Date.now().toString(36) + Math.random().toString(36).slice(2, 7); }
  function isSec(s) { return typeof s === "string" && s.indexOf("###") === 0; }
  function secName(s) { return String(s).slice(3); }
  var EXCH = /^(NASDAQ|NYSE|AMEX|ARCA|BATS|CBOE|TVC|INDEX|FX|FX_IDC|OANDA|FOREXCOM|FXCM|COINBASE|BINANCE|BITSTAMP|KRAKEN|CRYPTOCAP|CME|CME_MINI|COMEX|NYMEX|CBOT|ICE|ICEUS|ICEEUR|EUREX|LSE|TSX|ECONOMICS|FRED|USI|DJ|SP|FTSE|SGX|SSE|SZSE|HKEX|TSE|ASX|NSE|BSE|XETR|EURONEXT|SWB|BYBIT|OKX|GLASSNODE|INTOTHEBLOCK|COT|COT3|QUANDL|OTC|1-TVC)$/;
  function short(id) {
    id = String(id || "");
    var i = id.indexOf(":"); if (i < 0) return id;
    var p = id.slice(0, i), rest = id.slice(i + 1);
    if (EXCH.test(p) || /^[a-z0-9-]+$/.test(p) || p === "X" || p === "C") return rest;
    return id;
  }
  function norm(id) { return String(id || "").trim(); }
  function sameId(a, b) { return String(a).toUpperCase() === String(b).toUpperCase(); }
  function fmt(v, d) {
    if (v == null || isNaN(v)) return "—";
    var a = Math.abs(v);
    if (d == null) d = a >= 1000 ? 2 : a >= 1 ? 2 : a >= 0.01 ? 4 : 6;
    if (a >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (a >= 1e6 && a < 1e9) return (v / 1e6).toFixed(2) + "M";
    return Number(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  }
  function pct(v) { return v == null || isNaN(v) ? "—" : (v > 0 ? "+" : "") + Number(v).toFixed(2) + "%"; }
  function sgn(v) { return v == null || isNaN(v) || v === 0 ? "" : v > 0 ? "up" : "dn"; }
  function hashC(s) { var h = 0; s = String(s); for (var i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0; return ["#2962ff", "#089981", "#f23645", "#ff9800", "#9c27b0", "#00bcd4", "#e91e63", "#4caf50", "#795548", "#607d8b"][Math.abs(h) % 10]; }
  function getJSON(u) { return root.fetch(u).then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); }); }
  function toast(m) {
    var t = doc.getElementById("jhwl-toast");
    if (!t) { t = doc.createElement("div"); t.id = "jhwl-toast"; t.setAttribute("role", "status"); doc.body.appendChild(t); }
    t.textContent = m; t.className = "on"; clearTimeout(t._t); t._t = setTimeout(function () { t.className = ""; }, 2200);
  }
  function download(name, text) {
    var a = doc.createElement("a"); a.href = URL.createObjectURL(new Blob([text], { type: "text/plain" })); a.download = name; doc.body.appendChild(a); a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 800);
  }

  // ------------------------------------------------------------------ store
  var D = null;
  function blank() {
    return { v: 1, lists: {}, order: [], active: null, recent: [], flags: {}, cols: COLS.reduce(function (o, c) { o[c[0]] = c[2]; return o; }, {}), sort: { col: null, dir: 0 }, collapsed: {}, details: true, detH: 230, desc: false, seeded: false };
  }
  function load() {
    try { D = JSON.parse(root.localStorage.getItem(KEY) || "null"); } catch (e) { D = null; }
    if (!D || D.v !== 1 || typeof D.lists !== "object") D = blank();
    var b = blank(); Object.keys(b).forEach(function (k) { if (D[k] === undefined) D[k] = b[k]; });
    return D;
  }
  var saveT = 0;
  function save() {
    clearTimeout(saveT);
    saveT = setTimeout(function () {
      try { root.localStorage.setItem(KEY, JSON.stringify(D)); }
      catch (e) { toast("Watchlist storage is full — export lists to keep a copy"); }
      try { root.dispatchEvent(new CustomEvent("jh-tvwl-changed")); } catch (e) {}
    }, 120);
  }
  function addList(name, items, opts) {
    opts = opts || {};
    var id = opts.id || uid();
    if (D.lists[id]) id = uid();
    D.lists[id] = { id: id, name: String(name || "Watchlist").slice(0, 120), items: (items || []).slice(), origin: opts.origin || "user", created: Date.now() };
    if (opts.front) D.order.unshift(id); else D.order.push(id);
    return id;
  }
  function cur() {
    if (D.active && /^flag:/.test(D.active)) {
      var col = D.active.slice(5);
      return { id: D.active, name: (col.charAt(0).toUpperCase() + col.slice(1)) + " list", items: Object.keys(D.flags).filter(function (s) { return D.flags[s] === col; }), flag: col };
    }
    return D.lists[D.active] || D.lists[D.order[0]] || null;
  }
  function setActive(id) {
    if (!D.lists[id] && !/^flag:/.test(id)) return;
    D.active = id; D.recent = [id].concat(D.recent.filter(function (x) { return x !== id; })).slice(0, 8);
    sel = {}; anchor = null; save(); render();
  }

  // Seed once from every existing source; afterwards this store is authoritative for chart.html.
  function seed() {
    if (D.seeded) return Promise.resolve();
    var tasks = [];
    var wsP = root.jhWatchlistStore && root.jhWatchlistStore.ready ? Promise.resolve(root.jhWatchlistStore.ready).then(function () {
      try {
        var lists = root.jhWatchlistStore.read("jh-chart-custom-lists") || [];
        var flags = root.jhWatchlistStore.read("jh-chart-flags") || {};
        var favs = root.jhWatchlistStore.read("jh-chart-favs") || [];
        return { lists: lists, flags: flags, favs: favs };
      } catch (e) { return { lists: [], flags: {}, favs: [] }; }
    }).catch(function () { return { lists: [], flags: {}, favs: [] }; }) : Promise.resolve({ lists: [], flags: {}, favs: [] });
    tasks.push(wsP);
    tasks.push(getJSON("/data/tv-watchlists.json").catch(function () { return getJSON(PROXY + "/data/tv-watchlists.json"); }).catch(function () { return { lists: [] }; }));
    return Promise.all(tasks).then(function (a) {
      var ws = a[0], tv = a[1];
      var hexToName = {}; FLAGS.forEach(function (f) { hexToName[f[1].toLowerCase()] = f[0]; });
      hexToName["#ff6d00"] = "orange"; hexToName["#fdd835"] = "orange"; hexToName["#22d3ee"] = "cyan"; hexToName["#ab47bc"] = "purple";
      // chart.html's own lists first
      (ws.lists || []).forEach(function (l) { if (l && l.name && Array.isArray(l.symbols)) addList(l.name, l.symbols, { origin: "chart" }); });
      if ((ws.favs || []).length) addList("Favorites", ws.favs, { origin: "chart" });
      Object.keys(ws.flags || {}).forEach(function (s) { var c = hexToName[String(ws.flags[s]).toLowerCase()] || (FLAG_HEX[ws.flags[s]] ? ws.flags[s] : "red"); D.flags[s] = c; });
      // Chart Pro lists
      try {
        var cp = JSON.parse(root.localStorage.getItem("jh_custom_watchlists") || "{}");
        Object.keys(cp).forEach(function (k) { var w = cp[k]; if (w && w.name && Array.isArray(w.tickers)) addList(w.name, w.tickers, { origin: "chart-pro" }); });
      } catch (e) {}
      // The user's TradingView account lists (flag lists become colour flags)
      ((tv && tv.lists) || []).forEach(function (l) {
        if (!l || !Array.isArray(l.symbols)) return;
        if (l.color && (FLAG_HEX[l.color] || l.name === "Red list")) { l.symbols.forEach(function (s) { D.flags[s] = FLAG_HEX[l.color] ? l.color : "red"; }); return; }
        addList(l.name || String(l.id), l.symbols, { origin: "tradingview", id: "tv" + l.id });
      });
      if (!D.order.length) addList("Watchlist", ["SPY", "QQQ", "IWM", "###MACRO", "fred:DGS10", "fred:WALCL", "BTCUSD", "GLD"], { origin: "user" });
      if (!D.active) D.active = D.order[0];
      D.seeded = true; save();
    });
  }

  // ------------------------------------------------------------------ quotes
  var Q = {}, QT = {}, inflight = {}, queue = [], running = 0;
  function needQuote(id) { var t = QT[id]; return !inflight[id] && (!t || Date.now() - t > 60000); }
  function requestQuotes(ids) {
    ids.forEach(function (id) { if (needQuote(id) && queue.indexOf(id) < 0) queue.push(id); });
    pump();
  }
  function pump() {
    while (running < 3 && queue.length) {
      var batch = queue.splice(0, 20);
      batch.forEach(function (id) { inflight[id] = 1; });
      running++;
      (function (b) {
        root.fetch(PROXY + "/quote?ids=" + encodeURIComponent(b.join(","))).then(function (r) { return r.ok ? r.json() : { quotes: {} }; }).catch(function () { return { quotes: {} }; }).then(function (d) {
          var qs = d.quotes || {};
          b.forEach(function (id) {
            var q = qs[id]; if (!q) { var k = Object.keys(qs).filter(function (x) { return sameId(x, id); })[0]; q = k ? qs[k] : null; }
            Q[id] = q || { ok: false, error: "no quote" }; QT[id] = Date.now(); delete inflight[id];
          });
          running--; paintQuotes(b); pump();
        });
      })(batch);
    }
  }

  // ------------------------------------------------------------------ DOM
  var panel, body, det, hdr, menuEl, sel = {}, anchor = null, focusIdx = -1, view = [], activeSym = "";
  function css() {
    if (doc.getElementById("jhwl-css")) return;
    var st = doc.createElement("style"); st.id = "jhwl-css";
    st.textContent = [
      "#watch.jhwl-on>:not(#jhwl):not(#w-split){display:none!important}",
      "#jhwl{--bg:#131722;--bg2:#1e222d;--bd:#2a2e39;--fg:#d1d4dc;--mut:#787b86;--hov:#2a2e39;--sel:#142e61;--act:#1c2a4a;--blue:#2962ff;--up:#089981;--dn:#f23645;display:none;flex-direction:column;height:100%;min-height:0;background:var(--bg);color:var(--fg);font:13px/1.3 -apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,Ubuntu,sans-serif;position:relative;user-select:none}",
      "html[data-theme=light] #jhwl{--bg:#fff;--bg2:#f8f9fd;--bd:#e0e3eb;--fg:#131722;--mut:#787b86;--hov:#f0f3fa;--sel:#bbd9fb;--act:#e3effd}",
      "#watch.jhwl-on #jhwl{display:flex}",
      "#jhwl .wl-top{display:flex;align-items:center;gap:2px;padding:6px 6px 6px 10px;border-bottom:1px solid var(--bd);flex:0 0 auto}",
      "#jhwl .wl-name{display:flex;align-items:center;gap:4px;background:none;border:0;color:var(--fg);font-weight:600;font-size:14px;cursor:pointer;padding:5px 6px;border-radius:4px;min-width:0;flex:1;text-align:left}",
      "#jhwl .wl-name span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      "#jhwl .wl-name:hover,#jhwl .wl-ib:hover{background:var(--hov)}",
      "#jhwl .wl-name i{font-style:normal;color:var(--mut);font-size:10px;flex:0 0 auto}",
      "#jhwl .wl-ib{width:30px;height:30px;flex:0 0 30px;display:inline-flex;align-items:center;justify-content:center;background:none;border:0;border-radius:4px;color:var(--fg);cursor:pointer;font-size:17px}",
      "#jhwl .wl-cols{display:grid;align-items:center;padding:0 8px 0 10px;height:28px;color:var(--mut);font-size:11px;border-bottom:1px solid var(--bd);flex:0 0 auto}",
      "#jhwl .wl-cols span{cursor:pointer;text-align:right;white-space:nowrap;padding:0 2px}",
      "#jhwl .wl-cols span:first-child{text-align:left}",
      "#jhwl .wl-cols span:hover{color:var(--fg)}",
      "#jhwl .wl-cols span.s{color:var(--fg)}",
      "#jhwl .wl-body{flex:1 1 auto;overflow-y:auto;overflow-x:hidden;min-height:60px;outline:none;scrollbar-width:thin}",
      "#jhwl .wl-row{display:grid;align-items:center;height:30px;padding:0 8px 0 10px;cursor:pointer;position:relative;border-left:2px solid transparent}",
      "#jhwl .wl-row.desc{height:40px}",
      "#jhwl .wl-row:hover{background:var(--hov)}",
      "#jhwl .wl-row.act{background:var(--act);border-left-color:var(--blue)}",
      "#jhwl .wl-row.sel{background:var(--sel)}",
      "#jhwl .wl-row.foc{box-shadow:inset 0 0 0 1px var(--blue)}",
      "#jhwl .wl-sym{display:flex;align-items:center;gap:6px;min-width:0}",
      "#jhwl .wl-flag{width:10px;height:10px;flex:0 0 10px;border-radius:2px;border:1px solid transparent;cursor:pointer;margin-left:-4px}",
      "#jhwl .wl-row:hover .wl-flag:not([data-c]){border-color:var(--mut)}",
      "#jhwl .wl-logo{width:18px;height:18px;flex:0 0 18px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#fff;font-style:normal}",
      "#jhwl .wl-tk{overflow:hidden;min-width:0}",
      "#jhwl .wl-tk b{display:block;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      "#jhwl .wl-tk small{display:block;color:var(--mut);font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      "#jhwl .wl-c{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums;padding:0 2px;overflow:hidden;text-overflow:ellipsis}",
      "#jhwl .up{color:var(--up)}#jhwl .dn{color:var(--dn)}#jhwl .na{color:var(--mut)}",
      "#jhwl .wl-x{position:absolute;right:4px;top:50%;transform:translateY(-50%);width:20px;height:20px;border:0;border-radius:3px;background:var(--bg2);color:var(--mut);cursor:pointer;display:none;font-size:14px;line-height:1}",
      "#jhwl .wl-row:hover .wl-x{display:block}",
      "#jhwl .wl-x:hover{color:var(--fg);background:var(--bd)}",
      "#jhwl .wl-sec{display:flex;align-items:center;gap:6px;height:28px;padding:0 8px 0 8px;font-size:11px;font-weight:600;letter-spacing:.04em;text-transform:uppercase;color:var(--mut);cursor:pointer;position:relative;border-top:1px solid var(--bd)}",
      "#jhwl .wl-sec:hover{background:var(--hov);color:var(--fg)}",
      "#jhwl .wl-sec i{font-style:normal;display:inline-block;transition:transform .12s;width:10px}",
      "#jhwl .wl-sec.col i{transform:rotate(-90deg)}",
      "#jhwl .wl-sec em{font-style:normal;font-weight:400;opacity:.7}",
      "#jhwl .wl-sec:hover .wl-x{display:block}",
      "#jhwl .drop-a{box-shadow:inset 0 2px 0 var(--blue)}#jhwl .drop-b{box-shadow:inset 0 -2px 0 var(--blue)}",
      "#jhwl .wl-empty{padding:28px 16px;text-align:center;color:var(--mut)}",
      "#jhwl .wl-empty button{margin-top:10px;background:var(--blue);color:#fff;border:0;border-radius:4px;padding:7px 14px;cursor:pointer}",
      "#jhwl .wl-split{flex:0 0 5px;cursor:ns-resize;border-top:1px solid var(--bd);background:transparent}",
      "#jhwl .wl-split:hover{background:var(--blue)}",
      "#jhwl .wl-det{flex:0 0 auto;overflow:auto;padding:10px 12px 12px;border-top:0}",
      "#jhwl .wl-det h3{margin:0;font-size:15px;display:flex;align-items:center;gap:8px}",
      "#jhwl .wl-det .nm{color:var(--mut);font-size:12px;margin:2px 0 8px;overflow:hidden;text-overflow:ellipsis}",
      "#jhwl .wl-det .px{font-size:26px;font-weight:600;font-variant-numeric:tabular-nums}",
      "#jhwl .wl-det .ch{font-size:14px;margin-left:6px}",
      "#jhwl .wl-det .asof{color:var(--mut);font-size:11px;margin:2px 0 8px}",
      "#jhwl .wl-det .perf{display:grid;grid-template-columns:repeat(4,1fr);gap:4px;margin-top:8px}",
      "#jhwl .wl-det .perf div{background:var(--bg2);border-radius:4px;padding:5px 4px;text-align:center;font-size:12px}",
      "#jhwl .wl-det .perf small{display:block;color:var(--mut);font-size:10px}",
      "#jhwl .wl-det .kv{display:grid;grid-template-columns:auto 1fr;gap:3px 10px;margin-top:8px;font-size:12px}",
      "#jhwl .wl-det .kv span:nth-child(odd){color:var(--mut)}",
      "#jhwl .wl-det .acts{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}",
      "#jhwl .wl-det .acts button{background:var(--bg2);border:1px solid var(--bd);color:var(--fg);border-radius:4px;padding:4px 10px;cursor:pointer;font-size:12px}",
      "#jhwl .wl-det .acts button:hover{border-color:var(--blue)}",
      ".jhwl-menu{position:fixed;z-index:10040;min-width:220px;max-width:320px;max-height:75vh;overflow:auto;background:#1e222d;color:#d1d4dc;border:1px solid #363a45;border-radius:6px;box-shadow:0 8px 28px rgba(0,0,0,.5);padding:6px 0;font:13px/1.3 -apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,sans-serif}",
      "html[data-theme=light] .jhwl-menu{background:#fff;color:#131722;border-color:#e0e3eb}",
      ".jhwl-menu button{display:flex;align-items:center;gap:10px;width:100%;text-align:left;background:none;border:0;color:inherit;padding:7px 14px;cursor:pointer;font:inherit}",
      ".jhwl-menu button:hover,.jhwl-menu button.f{background:#2a2e39}",
      "html[data-theme=light] .jhwl-menu button:hover{background:#f0f3fa}",
      ".jhwl-menu button kbd{margin-left:auto;color:#787b86;font:11px sans-serif}",
      ".jhwl-menu hr{border:0;border-top:1px solid #363a45;margin:5px 0}",
      ".jhwl-menu .lab{padding:6px 14px 3px;color:#787b86;font-size:11px;text-transform:uppercase;letter-spacing:.05em}",
      ".jhwl-menu .flags{display:flex;gap:8px;padding:6px 14px}",
      ".jhwl-menu .flags i{width:18px;height:18px;border-radius:3px;cursor:pointer;display:inline-block;border:2px solid transparent}",
      ".jhwl-menu .flags i.on,.jhwl-menu .flags i:hover{border-color:#fff}",
      ".jhwl-menu .chk{width:14px;display:inline-block;color:#2962ff}",
      "#jhwl-dlg{position:fixed;inset:0;z-index:10045;background:rgba(0,0,0,.45);display:flex;align-items:flex-start;justify-content:center}",
      "#jhwl-dlg .box{margin-top:9vh;width:min(560px,calc(100vw - 16px));max-height:76vh;display:flex;flex-direction:column;background:#1e222d;color:#d1d4dc;border:1px solid #363a45;border-radius:6px;box-shadow:0 12px 48px rgba(0,0,0,.55);font:13px/1.35 -apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,sans-serif}",
      "html[data-theme=light] #jhwl-dlg .box{background:#fff;color:#131722;border-color:#e0e3eb}",
      "#jhwl-dlg .hd{display:flex;align-items:center;padding:14px 18px 8px}#jhwl-dlg .hd h2{flex:1;margin:0;font-size:19px;font-weight:600}",
      "#jhwl-dlg .hd button{background:none;border:0;color:#787b86;font-size:22px;cursor:pointer}",
      "#jhwl-dlg input[type=text]{margin:0 18px 10px;padding:8px 10px;background:transparent;border:1px solid #434651;border-radius:4px;color:inherit;font-size:14px;outline:none}",
      "#jhwl-dlg input[type=text]:focus{border-color:#2962ff}",
      "#jhwl-dlg .tabs{display:flex;gap:6px;padding:0 18px 8px;flex-wrap:wrap}",
      "#jhwl-dlg .tabs button{border:1px solid #434651;background:none;color:inherit;border-radius:16px;padding:4px 11px;cursor:pointer;font-size:12px}",
      "#jhwl-dlg .tabs button.on{background:#d1d4dc;color:#131722;border-color:#d1d4dc}",
      "#jhwl-dlg .ls{overflow:auto;flex:1;border-top:1px solid #2a2e39}",
      "#jhwl-dlg .li{display:flex;align-items:center;gap:10px;padding:8px 18px;cursor:pointer}",
      "#jhwl-dlg .li:hover,#jhwl-dlg .li.f{background:#2a2e39}",
      "html[data-theme=light] #jhwl-dlg .li:hover{background:#f0f3fa}",
      "#jhwl-dlg .li .n{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
      "#jhwl-dlg .li .n.on{color:#2962ff;font-weight:600}",
      "#jhwl-dlg .li small{color:#787b86;white-space:nowrap}",
      "#jhwl-dlg .li .del{visibility:hidden;background:none;border:0;color:#787b86;cursor:pointer;font-size:15px}",
      "#jhwl-dlg .li:hover .del{visibility:visible}",
      "#jhwl-dlg .li .del:hover{color:#f23645}",
      "#jhwl-dlg .ft{display:flex;gap:8px;padding:10px 18px;border-top:1px solid #2a2e39}",
      "#jhwl-dlg .ft button{background:#2962ff;color:#fff;border:0;border-radius:4px;padding:7px 14px;cursor:pointer}",
      "#jhwl-dlg .ft button.sec{background:none;border:1px solid #434651;color:inherit}",
      "#jhwl-dlg .sw{width:12px;height:12px;border-radius:3px;flex:0 0 12px}",
      "#jhwl-toast{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);background:#2a2e39;color:#fff;padding:8px 14px;border-radius:4px;font:13px sans-serif;z-index:10060;box-shadow:0 4px 18px rgba(0,0,0,.4);display:none}",
      "#jhwl-toast.on{display:block}"
    ].join("\n");
    doc.head.appendChild(st);
  }
  function colsOn() { return COLS.filter(function (c) { return D.cols[c[0]]; }); }
  function gridTpl() {
    var w = { last: "minmax(54px,1fr)", chg: "minmax(44px,.8fr)", chgp: "minmax(52px,.8fr)", m1: "minmax(48px,.7fr)", m3: "minmax(48px,.7fr)", y1: "minmax(48px,.7fr)", date: "minmax(62px,.8fr)", spark: "56px" };
    return "minmax(84px,1.6fr) " + colsOn().map(function (c) { return w[c[0]]; }).join(" ");
  }

  function mount() {
    var w = doc.getElementById("watch");
    if (!w) return false;
    if (doc.getElementById("jhwl")) return true;
    css();
    panel = doc.createElement("div"); panel.id = "jhwl";
    panel.innerHTML =
      '<div class="wl-top"><button type="button" class="wl-name" data-a="listmenu" aria-haspopup="menu" title="Watchlist menu"><span></span><i>▼</i></button>' +
      '<button type="button" class="wl-ib" data-a="add" title="Add symbol" aria-label="Add symbol">+</button>' +
      '<button type="button" class="wl-ib" data-a="settings" title="Columns & view settings" aria-label="Watchlist settings">⋯</button></div>' +
      '<div class="wl-cols" role="row"></div>' +
      '<div class="wl-body" tabindex="0" role="listbox" aria-label="Watchlist symbols" aria-multiselectable="true"></div>' +
      '<div class="wl-split" title="Drag to resize details"></div>' +
      '<div class="wl-det"></div>';
    w.appendChild(panel);
    w.classList.add("jhwl-on");
    body = panel.querySelector(".wl-body"); det = panel.querySelector(".wl-det"); hdr = panel.querySelector(".wl-cols");
    wire();
    return true;
  }

  // ------------------------------------------------------------------ rendering
  function buildView() {
    var L = cur(); view = [];
    if (!L) return view;
    var items = L.items, coll = D.collapsed[L.id] || {}, secIdx = -1, secTitle = "";
    // sections: an item "###NAME" opens a section until the next "###"
    var groups = [{ title: null, rows: [] }];
    items.forEach(function (s, i) {
      if (isSec(s)) { groups.push({ title: secName(s), idx: i, rows: [] }); return; }
      groups[groups.length - 1].rows.push({ id: s, idx: i });
    });
    var sc = D.sort.col, dir = D.sort.dir;
    groups.forEach(function (g) {
      if (g.title !== null) view.push({ sec: true, title: g.title, idx: g.idx, n: g.rows.length, col: !!coll[g.title] });
      if (g.title !== null && coll[g.title]) return;
      var rows = g.rows.slice();
      if (sc && dir) {
        rows.sort(function (a, b) {
          var A = sortVal(a.id, sc), B = sortVal(b.id, sc);
          if (A == null && B == null) return 0; if (A == null) return 1; if (B == null) return -1;
          return (A < B ? -1 : A > B ? 1 : 0) * dir;
        });
      }
      rows.forEach(function (r) { view.push(r); });
    });
    return view;
  }
  function sortVal(id, c) {
    if (c === "sym") return short(id).toUpperCase();
    var q = Q[id]; if (!q || !q.ok) return null;
    return { last: q.last, chg: q.chg, chgp: q.chg_pct, m1: q.mom_pct, m3: q.qoq_pct, y1: q.yoy_pct, date: q.last_date }[c];
  }
  function cells(id) {
    var q = Q[id] || null, ok = q && q.ok;
    return colsOn().map(function (c) {
      var k = c[0], v, cl = "";
      if (!ok) return '<span class="wl-c na" data-k="' + k + '"' + (q && q.error ? ' title="' + esc(q.error) + '"' : "") + ">" + (k === "spark" ? "" : (q ? "—" : "…")) + "</span>";
      if (k === "last") v = fmt(q.last);
      else if (k === "chg") { v = (q.chg > 0 ? "+" : "") + fmt(q.chg); cl = sgn(q.chg); }
      else if (k === "chgp") { v = pct(q.chg_pct); cl = sgn(q.chg_pct); }
      else if (k === "m1") { v = pct(q.mom_pct); cl = sgn(q.mom_pct); }
      else if (k === "m3") { v = pct(q.qoq_pct); cl = sgn(q.qoq_pct); }
      else if (k === "y1") { v = pct(q.yoy_pct); cl = sgn(q.yoy_pct); }
      else if (k === "date") v = esc(q.last_date || "");
      else if (k === "spark") return '<span class="wl-c" data-k="spark">' + spark(q.spark, 52, 16) + "</span>";
      return '<span class="wl-c ' + cl + '" data-k="' + k + '">' + v + "</span>";
    }).join("");
  }
  function spark(arr, w, h) {
    if (!Array.isArray(arr) || arr.length < 2) return "";
    var lo = Math.min.apply(null, arr), hi = Math.max.apply(null, arr), r = hi - lo || 1;
    var pts = arr.map(function (v, i) { return (i * (w / (arr.length - 1))).toFixed(1) + "," + (h - ((v - lo) / r) * (h - 2) - 1).toFixed(1); }).join(" ");
    var col = arr[arr.length - 1] >= arr[0] ? "#089981" : "#f23645";
    return '<svg width="' + w + '" height="' + h + '" viewBox="0 0 ' + w + " " + h + '" aria-hidden="true"><polyline fill="none" stroke="' + col + '" stroke-width="1.3" points="' + pts + '"/></svg>';
  }
  function rowHtml(r, vi) {
    if (r.sec) {
      return '<div class="wl-sec' + (r.col ? " col" : "") + '" data-vi="' + vi + '" data-sec="' + esc(r.title) + '" draggable="true"><i>▾</i>' + esc(r.title) + " <em>" + r.n + '</em><button type="button" class="wl-x" data-a="delsec" title="Remove section (keep symbols)">×</button></div>';
    }
    var id = r.id, fl = D.flags[id], q = Q[id], s = short(id);
    var name = q && q.name ? q.name : "";
    var cls = "wl-row" + (D.desc ? " desc" : "") + (sel[r.idx] ? " sel" : "") + (sameId(id, activeSym) || sameId(short(id), activeSym) ? " act" : "") + (vi === focusIdx ? " foc" : "");
    return '<div class="' + cls + '" data-vi="' + vi + '" data-id="' + esc(id) + '" role="option" aria-selected="' + (!!sel[r.idx]) + '" draggable="' + (!(D.sort.col && D.sort.dir)) + '" title="' + esc(id + (name ? " — " + name : "")) + '" style="grid-template-columns:' + gridTpl() + '">' +
      '<span class="wl-sym"><i class="wl-flag" data-a="flag"' + (fl ? ' data-c="' + fl + '" style="background:' + FLAG_HEX[fl] + '"' : "") + ' title="Flag"></i>' +
      '<i class="wl-logo" style="background:' + hashC(s) + '">' + esc(s.replace(/^[^A-Za-z0-9]+/, "").charAt(0).toUpperCase() || "?") + "</i>" +
      '<span class="wl-tk"><b>' + esc(s) + "</b>" + (D.desc ? "<small>" + esc(name || id) + "</small>" : "") + "</span></span>" +
      cells(id) + '<button type="button" class="wl-x" data-a="rm" title="Remove from watchlist" aria-label="Remove ' + esc(s) + '">×</button></div>';
  }
  function render() {
    if (!panel) return;
    var L = cur();
    panel.querySelector(".wl-name span").textContent = L ? L.name : "Watchlist";
    var fl = L && L.flag ? FLAG_HEX[L.flag] : null;
    panel.querySelector(".wl-name").style.color = fl || "";
    // header
    var sc = D.sort.col, dir = D.sort.dir;
    function h(k, lab) { return '<span data-sort="' + k + '" class="' + (sc === k && dir ? "s" : "") + '">' + lab + (sc === k && dir ? (dir > 0 ? " ▲" : " ▼") : "") + "</span>"; }
    hdr.style.gridTemplateColumns = gridTpl();
    hdr.innerHTML = h("sym", "Symbol") + colsOn().map(function (c) { return h(c[0], c[1]); }).join("");
    buildView();
    if (!L || !view.length) {
      body.innerHTML = '<div class="wl-empty">' + (L && L.flag ? "No symbols flagged " + esc(L.flag) + ". Click the flag square next to any symbol." : "This list is empty.") + '<br><button type="button" data-a="add">+ Add symbol</button></div>';
    } else body.innerHTML = view.map(rowHtml).join("");
    observe();
    renderDetails();
  }
  function paintQuotes(ids) {
    if (!panel) return;
    ids.forEach(function (id) {
      body.querySelectorAll('.wl-row[data-id="' + cssEsc(id) + '"]').forEach(function (row) {
        var tmp = doc.createElement("div"); tmp.innerHTML = cells(id);
        row.querySelectorAll(".wl-c").forEach(function (c) { c.remove(); });
        var x = row.querySelector(".wl-x");
        Array.prototype.slice.call(tmp.children).forEach(function (c) { row.insertBefore(c, x); });
        var q = Q[id]; if (q && q.name) row.title = id + " — " + q.name;
        if (D.desc && q && q.name) { var sm = row.querySelector(".wl-tk small"); if (sm) sm.textContent = q.name; }
      });
      if (sameId(id, detId())) renderDetails();
    });
    if (D.sort.col && D.sort.dir && D.sort.col !== "sym") { clearTimeout(paintQuotes._t); paintQuotes._t = setTimeout(render, 400); }
  }
  function cssEsc(s) { return root.CSS && CSS.escape ? CSS.escape(s) : String(s).replace(/["\\]/g, "\\$&"); }
  var io = null;
  function observe() {
    if (!root.IntersectionObserver) { requestQuotes(view.filter(function (r) { return !r.sec; }).map(function (r) { return r.id; })); return; }
    if (io) io.disconnect();
    io = new IntersectionObserver(function (ents) {
      var ids = [];
      ents.forEach(function (e) { if (e.isIntersecting) { var id = e.target.getAttribute("data-id"); if (id) ids.push(id); } });
      if (ids.length) requestQuotes(ids);
    }, { root: body, rootMargin: "200px 0px" });
    body.querySelectorAll(".wl-row").forEach(function (r) { io.observe(r); });
  }
  function detId() {
    var L = cur(); if (!L) return activeSym;
    var inList = L.items.filter(function (s) { return !isSec(s) && (sameId(s, activeSym) || sameId(short(s), activeSym)); })[0];
    if (inList) return inList;
    var f = focusIdx >= 0 && view[focusIdx] && !view[focusIdx].sec ? view[focusIdx].id : null;
    return f || activeSym;
  }
  function renderDetails() {
    if (!det) return;
    var split = panel.querySelector(".wl-split");
    if (!D.details) { det.style.display = "none"; split.style.display = "none"; return; }
    det.style.display = ""; split.style.display = ""; det.style.height = D.detH + "px";
    var id = detId(); if (!id) { det.innerHTML = ""; return; }
    var q = Q[id]; if (!q) { requestQuotes([id]); }
    var s = short(id), ok = q && q.ok;
    det.innerHTML =
      '<h3><i class="wl-logo" style="background:' + hashC(s) + ';width:24px;height:24px;flex:0 0 24px;font-size:12px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;color:#fff;font-style:normal">' + esc(s.charAt(0).toUpperCase()) + "</i>" + esc(s) + (D.flags[id] ? '<i style="width:10px;height:10px;border-radius:2px;background:' + FLAG_HEX[D.flags[id]] + '"></i>' : "") + "</h3>" +
      '<div class="nm" title="' + esc(id) + '">' + esc((ok && q.name) || id) + "</div>" +
      (ok ? '<div><span class="px">' + fmt(q.last) + '</span><span class="ch ' + sgn(q.chg) + '">' + (q.chg > 0 ? "+" : "") + fmt(q.chg) + " (" + pct(q.chg_pct) + ")</span>" + (q.unit ? ' <span style="color:var(--mut)">' + esc(q.unit) + "</span>" : "") + "</div>" +
        '<div class="asof">As of ' + esc(q.last_date || "?") + (q.freq ? " · " + esc(q.freq) : "") + (q.prev_date ? " · prev " + esc(q.prev_date) : "") + "</div>" +
        (Array.isArray(q.spark) ? '<div>' + spark(q.spark, Math.max(120, (panel.clientWidth || 300) - 28), 46) + "</div>" : "") +
        '<div class="perf"><div><small>1D</small><span class="' + sgn(q.chg_pct) + '">' + pct(q.chg_pct) + '</span></div><div><small>1M</small><span class="' + sgn(q.mom_pct) + '">' + pct(q.mom_pct) + '</span></div><div><small>3M</small><span class="' + sgn(q.qoq_pct) + '">' + pct(q.qoq_pct) + '</span></div><div><small>1Y</small><span class="' + sgn(q.yoy_pct) + '">' + pct(q.yoy_pct) + "</span></div></div>" +
        '<div class="kv"><span>Symbol</span><span>' + esc(id) + "</span><span>History</span><span>" + esc(q.first || "?") + " → " + esc(q.last_date || "?") + (q.n ? " · " + Number(q.n).toLocaleString("en-US") + " obs" : "") + "</span></div>"
        : '<div class="asof">' + (q ? "No quote from the warehouse for this symbol" + (q.error ? ": " + esc(String(q.error).slice(0, 140)) : "") : "Loading quote…") + "</div>") +
      '<div class="acts"><button type="button" data-a="dchart">Chart</button><button type="button" data-a="dcompare">Compare</button><button type="button" data-a="dflag">Flag</button>' + (cur() && cur().items.indexOf(id) >= 0 ? '<button type="button" data-a="drm">Remove</button>' : '<button type="button" data-a="dadd">+ Add</button>') + "</div>";
    det.setAttribute("data-id", id);
  }

  // ------------------------------------------------------------------ mutations
  function add(id, meta) {
    id = norm(id); if (!id) return false;
    var L = cur();
    if (!L) { var nid = addList("Watchlist", []); D.active = nid; L = cur(); }
    if (L.flag) { D.flags[id] = L.flag; save(); render(); return true; }
    if (L.items.some(function (s) { return sameId(s, id); })) { flash(id); return "dup"; }
    L.items.push(id); save(); render(); flash(id);
    return true;
  }
  function flash(id) {
    setTimeout(function () {
      var row = body && body.querySelector('.wl-row[data-id="' + cssEsc(id) + '"]');
      if (row) { row.scrollIntoView({ block: "nearest" }); row.animate && row.animate([{ background: "rgba(41,98,255,.45)" }, { background: "transparent" }], { duration: 900 }); }
    }, 30);
  }
  function selectedIdx() { return Object.keys(sel).map(Number).filter(function (i) { return sel[i]; }).sort(function (a, b) { return a - b; }); }
  function removeIdx(idxs) {
    var L = cur(); if (!L || !idxs.length) return;
    if (L.flag) { idxs.forEach(function (i) { delete D.flags[L.items[i]]; }); }
    else { var set = {}; idxs.forEach(function (i) { set[i] = 1; }); L.items = L.items.filter(function (_, i) { return !set[i]; }); }
    sel = {}; save(); render();
  }
  function moveIdx(idxs, to) {
    var L = cur(); if (!L || L.flag) return;
    var set = {}; idxs.forEach(function (i) { set[i] = 1; });
    var moving = idxs.map(function (i) { return L.items[i]; });
    var before = 0; idxs.forEach(function (i) { if (i < to) before++; });
    var rest = L.items.filter(function (_, i) { return !set[i]; });
    var at = Math.max(0, Math.min(rest.length, to - before));
    L.items = rest.slice(0, at).concat(moving, rest.slice(at));
    sel = {}; for (var k = 0; k < moving.length; k++) sel[at + k] = true;
    if (D.sort.dir) D.sort = { col: null, dir: 0 };
    save(); render();
  }
  function setFlag(id, color) { if (color) D.flags[id] = color; else delete D.flags[id]; save(); render(); }
  function goChart(id) {
    activeSym = id;
    if (typeof root.jhGoSymbol === "function") root.jhGoSymbol(id, "chart");
    else if (typeof root.jhWatchlistOpen === "function") root.jhWatchlistOpen(id);
    body.querySelectorAll(".wl-row.act").forEach(function (r) { r.classList.remove("act"); });
    var row = body.querySelector('.wl-row[data-id="' + cssEsc(id) + '"]'); if (row) row.classList.add("act");
    renderDetails();
  }
  function exportText(L) {
    // TradingView format: comma separated, sections as ###NAME
    return L.items.join(",");
  }
  function parseImport(text) {
    return String(text || "").split(/[\s,;]+/).map(function (s) { return s.trim(); }).filter(Boolean);
  }

  // ------------------------------------------------------------------ menus & dialogs
  function closeMenu() { if (menuEl) { menuEl.remove(); menuEl = null; } doc.removeEventListener("mousedown", outside, true); }
  function outside(e) { if (menuEl && !menuEl.contains(e.target)) closeMenu(); }
  function menu(x, y, html, onAct) {
    closeMenu();
    menuEl = doc.createElement("div"); menuEl.className = "jhwl-menu"; menuEl.setAttribute("role", "menu"); menuEl.innerHTML = html;
    doc.body.appendChild(menuEl);
    var r = menuEl.getBoundingClientRect();
    menuEl.style.left = Math.max(4, Math.min(x, root.innerWidth - r.width - 6)) + "px";
    menuEl.style.top = Math.max(4, Math.min(y, root.innerHeight - r.height - 6)) + "px";
    menuEl.addEventListener("click", function (e) {
      var b = e.target.closest("[data-m]"); if (!b) return;
      var keep = onAct(b.getAttribute("data-m"), b);
      if (!keep) closeMenu();
    });
    menuEl.addEventListener("keydown", function (e) {
      e.stopPropagation();
      var bs = Array.prototype.slice.call(menuEl.querySelectorAll("button")), i = bs.indexOf(doc.activeElement);
      if (e.key === "Escape") { closeMenu(); body.focus(); }
      if (e.key === "ArrowDown") { e.preventDefault(); (bs[i + 1] || bs[0]).focus(); }
      if (e.key === "ArrowUp") { e.preventDefault(); (bs[i - 1] || bs[bs.length - 1]).focus(); }
    });
    setTimeout(function () { doc.addEventListener("mousedown", outside, true); var f = menuEl && menuEl.querySelector("button"); if (f) f.focus(); }, 0);
  }
  function listMenu(btn) {
    var r = btn.getBoundingClientRect(), L = cur();
    var rec = D.recent.filter(function (id) { return id !== D.active && (D.lists[id] || /^flag:/.test(id)); }).slice(0, 5);
    var html =
      (L && !L.flag ? '<button data-m="addsec">Add section</button><hr>' : "") +
      '<button data-m="share">Copy list to clipboard</button>' +
      (L && !L.flag ? '<button data-m="copy">Make a copy…</button><button data-m="rename">Rename…</button>' : "") +
      '<button data-m="clear">Clear list</button>' +
      (L && !L.flag ? '<button data-m="delete">Delete list</button>' : "") +
      "<hr>" +
      '<button data-m="new">Create new list…</button>' +
      (L && !L.flag ? '<button data-m="upload">Upload list…</button>' : "") +
      '<button data-m="export">Export list…</button>' +
      "<hr>" +
      '<button data-m="open">Open list…<kbd>' + Object.keys(D.lists).length + "</kbd></button>" +
      (rec.length ? '<div class="lab">Recently used</div>' + rec.map(function (id) { var l = D.lists[id]; var nm = l ? l.name : (id.slice(5) + " list"); return '<button data-m="go" data-id="' + esc(id) + '">' + (l ? "" : '<i class="sw" style="width:10px;height:10px;border-radius:2px;background:' + FLAG_HEX[id.slice(5)] + '"></i>') + esc(nm) + "</button>"; }).join("") : "");
    menu(r.left, r.bottom + 4, html, function (m, b) {
      var L = cur();
      if (m === "go") setActive(b.getAttribute("data-id"));
      else if (m === "addsec") { var n = root.prompt("Section name", "NEW SECTION"); if (n) { L.items.unshift("###" + n.trim().toUpperCase()); save(); render(); } }
      else if (m === "share") { var t = exportText(L); (navigator.clipboard ? navigator.clipboard.writeText(t) : Promise.reject()).then(function () { toast("Copied " + L.items.filter(function (s) { return !isSec(s); }).length + " symbols"); }, function () { root.prompt("Copy the list", t); }); }
      else if (m === "copy") { var n2 = root.prompt("Name of the copy", L.name + " (copy)"); if (n2) { var id = addList(n2, L.items, { front: true }); setActive(id); } }
      else if (m === "rename") { var n3 = root.prompt("Rename list", L.name); if (n3 && n3.trim()) { L.name = n3.trim(); save(); render(); } }
      else if (m === "clear") { if (root.confirm("Remove all symbols from " + L.name + "?")) { if (L.flag) Object.keys(D.flags).forEach(function (s) { if (D.flags[s] === L.flag) delete D.flags[s]; }); else L.items = []; save(); render(); } }
      else if (m === "delete") deleteList(L.id);
      else if (m === "new") { var n4 = root.prompt("New list name", "Watchlist"); if (n4 && n4.trim()) { var nid = addList(n4.trim(), [], { front: true }); setActive(nid); } }
      else if (m === "upload") upload();
      else if (m === "export") download((L.name || "watchlist").replace(/[^\w.-]+/g, "_") + ".txt", exportText(L));
      else if (m === "open") openLists();
    });
  }
  function deleteList(id) {
    var L = D.lists[id]; if (!L) return;
    if (!root.confirm('Delete the list "' + L.name + '"? This cannot be undone.')) return;
    delete D.lists[id]; D.order = D.order.filter(function (x) { return x !== id; }); D.recent = D.recent.filter(function (x) { return x !== id; });
    if (!D.order.length) addList("Watchlist", []);
    if (D.active === id) D.active = D.recent[0] && (D.lists[D.recent[0]] || /^flag:/.test(D.recent[0])) ? D.recent[0] : D.order[0];
    save(); render();
  }
  function upload() {
    var f = doc.createElement("input"); f.type = "file"; f.accept = ".txt,.csv,text/plain";
    f.onchange = function () {
      var file = f.files && f.files[0]; if (!file) return;
      file.text().then(function (t) {
        var items = parseImport(t); if (!items.length) { toast("No symbols found in file"); return; }
        var id = addList(file.name.replace(/\.(txt|csv)$/i, ""), items, { front: true }); setActive(id);
        toast("Imported " + items.filter(function (s) { return !isSec(s); }).length + " symbols");
      });
    };
    f.click();
  }
  function settingsMenu(btn) {
    var r = btn.getBoundingClientRect();
    function chk(on) { return '<span class="chk">' + (on ? "✓" : "") + "</span>"; }
    var html = '<div class="lab">Columns</div>' + COLS.map(function (c) { return '<button data-m="col" data-k="' + c[0] + '">' + chk(D.cols[c[0]]) + c[1] + "</button>"; }).join("") +
      '<hr><button data-m="desc">' + chk(D.desc) + "Show description</button>" +
      '<button data-m="det">' + chk(D.details) + "Show symbol details</button>" +
      '<hr><div class="lab">Sort</div><button data-m="sort" data-k="sym">Symbol A→Z</button><button data-m="sort" data-k="chgp">Change % (high first)</button><button data-m="sort" data-k="none">Custom order</button>' +
      '<hr><button data-m="refresh">Refresh quotes</button>';
    menu(r.right - 230, r.bottom + 4, html, function (m, b) {
      if (m === "col") { var k = b.getAttribute("data-k"); D.cols[k] = !D.cols[k]; save(); render(); b.querySelector(".chk").textContent = D.cols[k] ? "✓" : ""; return true; }
      if (m === "desc") { D.desc = !D.desc; save(); render(); }
      if (m === "det") { D.details = !D.details; save(); render(); }
      if (m === "sort") { var s = b.getAttribute("data-k"); D.sort = s === "none" ? { col: null, dir: 0 } : { col: s, dir: s === "sym" ? 1 : -1 }; save(); render(); }
      if (m === "refresh") { QT = {}; observe(); toast("Refreshing quotes…"); }
    });
  }
  function rowMenu(x, y, vi) {
    var r = view[vi]; if (!r) return;
    if (r.sec) {
      menu(x, y, '<button data-m="rensec">Rename section…</button><button data-m="delsec">Remove section</button><button data-m="delsecall">Remove section and its symbols</button>', function (m) {
        var L = cur();
        if (m === "rensec") { var n = root.prompt("Rename section", r.title); if (n) { L.items[r.idx] = "###" + n.trim().toUpperCase(); save(); render(); } }
        if (m === "delsec") removeIdx([r.idx]);
        if (m === "delsecall") { var idx = [r.idx], j = r.idx + 1; while (j < L.items.length && !isSec(L.items[j])) idx.push(j++); removeIdx(idx); }
      });
      return;
    }
    if (!sel[r.idx]) { sel = {}; sel[r.idx] = true; anchor = r.idx; render(); }
    var idxs = selectedIdx(), n = idxs.length, others = D.order.filter(function (id) { return id !== D.active; });
    var html = '<button data-m="chart">Open chart<kbd>↵</kbd></button><button data-m="compare">Add to compare</button>' +
      '<div class="lab">Flag</div><div class="flags">' + FLAGS.map(function (f) { return '<i data-m="flag" data-c="' + f[0] + '" class="' + (D.flags[r.id] === f[0] ? "on" : "") + '" style="background:' + f[1] + '" title="' + f[0] + '"></i>'; }).join("") + '</div><button data-m="unflag">Remove flag</button><hr>' +
      (cur().flag ? "" : '<button data-m="secabove">Add section above</button><button data-m="top">Move to top</button><button data-m="bottom">Move to bottom</button>') +
      '<button data-m="copyto">Add ' + (n > 1 ? n + " symbols" : "to another list") + "…</button>" +
      '<button data-m="copysym">Copy symbol' + (n > 1 ? "s" : "") + "</button><hr>" +
      '<button data-m="rm">Remove ' + (n > 1 ? n + " symbols" : "from watchlist") + "<kbd>Del</kbd></button>";
    menu(x, y, html, function (m, b) {
      var L = cur(), ids = idxs.map(function (i) { return L.items[i]; });
      if (m === "chart") goChart(r.id);
      else if (m === "compare") ids.forEach(function (id) { if (root.jhAddCompare) root.jhAddCompare(id); else if (root.jhGoSymbol) root.jhGoSymbol(id, "compare"); });
      else if (m === "flag") { ids.forEach(function (id) { D.flags[id] = b.getAttribute("data-c"); }); save(); render(); }
      else if (m === "unflag") { ids.forEach(function (id) { delete D.flags[id]; }); save(); render(); }
      else if (m === "secabove") { var nm = root.prompt("Section name", "NEW SECTION"); if (nm) { L.items.splice(idxs[0], 0, "###" + nm.trim().toUpperCase()); sel = {}; save(); render(); } }
      else if (m === "top") moveIdx(idxs, 0);
      else if (m === "bottom") moveIdx(idxs, L.items.length);
      else if (m === "copyto") pickList(function (lid) { var T = D.lists[lid]; ids.forEach(function (id) { if (!T.items.some(function (s) { return sameId(s, id); })) T.items.push(id); }); save(); toast("Added " + ids.length + " to " + T.name); });
      else if (m === "copysym") { var t = ids.join(","); if (navigator.clipboard) navigator.clipboard.writeText(t).then(function () { toast("Copied"); }); }
      else if (m === "rm") removeIdx(idxs);
    });
  }
  function flagMenu(x, y, id) {
    menu(x, y, '<div class="flags">' + FLAGS.map(function (f) { return '<i data-m="flag" data-c="' + f[0] + '" class="' + (D.flags[id] === f[0] ? "on" : "") + '" style="background:' + f[1] + '" title="' + f[0] + '"></i>'; }).join("") + '</div><button data-m="unflag">Remove flag</button>', function (m, b) {
      if (m === "flag") { D.lastFlag = b.getAttribute("data-c"); setFlag(id, D.lastFlag); }
      if (m === "unflag") setFlag(id, null);
    });
  }
  function dlg(title, inner, onReady) {
    var old = doc.getElementById("jhwl-dlg"); if (old) old.remove();
    var d = doc.createElement("div"); d.id = "jhwl-dlg";
    d.innerHTML = '<div class="box" role="dialog" aria-modal="true" aria-label="' + esc(title) + '"><div class="hd"><h2>' + esc(title) + '</h2><button type="button" data-x aria-label="Close">×</button></div>' + inner + "</div>";
    doc.body.appendChild(d);
    function close() { d.remove(); body && body.focus(); }
    d.addEventListener("mousedown", function (e) { if (e.target === d) close(); });
    d.querySelector("[data-x]").onclick = close;
    d.addEventListener("keydown", function (e) { e.stopPropagation(); if (e.key === "Escape") close(); }, true);
    d.addEventListener("keypress", function (e) { e.stopPropagation(); }, true);
    onReady(d, close);
  }
  function openLists(pickMode, onPick) {
    var tab = "all";
    dlg(pickMode ? "Add to list" : "Watchlists", '<input type="text" placeholder="Search lists" aria-label="Search lists"><div class="tabs">' +
      [["all", "All lists"], ["mine", "Created lists"], ["tradingview", "TradingView"], ["flag", "Flagged lists"]].filter(function (t) { return !pickMode || t[0] !== "flag"; }).map(function (t) { return '<button type="button" data-t="' + t[0] + '" class="' + (t[0] === "all" ? "on" : "") + '">' + t[1] + "</button>"; }).join("") +
      '</div><div class="ls"></div><div class="ft"><button type="button" data-new>Create new list</button>' + (pickMode ? "" : '<button type="button" class="sec" data-up>Upload list</button>') + "</div>",
      function (d, close) {
        var inp = d.querySelector("input"), ls = d.querySelector(".ls"), f = 0, items = [];
        function draw() {
          var q = inp.value.trim().toLowerCase();
          items = [];
          if (tab === "flag" || (tab === "all" && !pickMode)) FLAGS.forEach(function (c) {
            var n = Object.keys(D.flags).filter(function (s) { return D.flags[s] === c[0]; }).length;
            var nm = c[0].charAt(0).toUpperCase() + c[0].slice(1) + " list";
            if (!q || nm.toLowerCase().indexOf(q) >= 0) items.push({ id: "flag:" + c[0], name: nm, n: n, sw: c[1] });
          });
          if (tab !== "flag") D.order.forEach(function (id) {
            var l = D.lists[id]; if (!l) return;
            if (tab === "mine" && l.origin === "tradingview") return;
            if (tab === "tradingview" && l.origin !== "tradingview") return;
            if (q && l.name.toLowerCase().indexOf(q) < 0) return;
            items.push({ id: id, name: l.name, n: l.items.filter(function (s) { return !isSec(s); }).length, origin: l.origin });
          });
          if (f >= items.length) f = Math.max(0, items.length - 1);
          ls.innerHTML = items.slice(0, 1500).map(function (it, i) {
            return '<div class="li' + (i === f ? " f" : "") + '" data-i="' + i + '">' + (it.sw ? '<i class="sw" style="background:' + it.sw + '"></i>' : "") + '<span class="n' + (it.id === D.active ? " on" : "") + '">' + esc(it.name) + "</span><small>" + it.n + (it.origin === "tradingview" ? " · TV" : "") + "</small>" + (!it.sw && !pickMode ? '<button type="button" class="del" data-del="' + esc(it.id) + '" title="Delete list">🗑</button>' : "") + "</div>";
          }).join("") || '<div class="li"><span class="n" style="color:#787b86">No lists match</span></div>';
        }
        function choose(it) { if (!it) return; close(); if (pickMode) onPick(it.id); else setActive(it.id); }
        inp.oninput = function () { f = 0; draw(); };
        d.querySelector(".tabs").onclick = function (e) { var b = e.target.closest("[data-t]"); if (!b) return; tab = b.getAttribute("data-t"); d.querySelectorAll(".tabs button").forEach(function (x) { x.classList.toggle("on", x === b); }); f = 0; draw(); };
        ls.onclick = function (e) {
          var del = e.target.closest("[data-del]"); if (del) { e.stopPropagation(); deleteList(del.getAttribute("data-del")); draw(); return; }
          var li = e.target.closest("[data-i]"); if (li) choose(items[+li.getAttribute("data-i")]);
        };
        inp.addEventListener("keydown", function (e) {
          if (e.key === "ArrowDown") { e.preventDefault(); f = Math.min(items.length - 1, f + 1); draw(); var el = ls.querySelector(".li.f"); if (el) el.scrollIntoView({ block: "nearest" }); }
          if (e.key === "ArrowUp") { e.preventDefault(); f = Math.max(0, f - 1); draw(); var el2 = ls.querySelector(".li.f"); if (el2) el2.scrollIntoView({ block: "nearest" }); }
          if (e.key === "Enter") { e.preventDefault(); choose(items[f]); }
        });
        d.querySelector("[data-new]").onclick = function () { var n = root.prompt("New list name", "Watchlist"); if (n && n.trim()) { var id = addList(n.trim(), [], { front: true }); save(); close(); if (pickMode) onPick(id); else setActive(id); } };
        var up = d.querySelector("[data-up]"); if (up) up.onclick = function () { close(); upload(); };
        draw(); inp.focus();
      });
  }
  function pickList(cb) { openLists(true, cb); }

  // ------------------------------------------------------------------ events
  function rowFromEvent(e) { return e.target.closest(".wl-row,.wl-sec"); }
  function wire() {
    panel.addEventListener("click", function (e) {
      var a = e.target.closest("[data-a]"), act = a && a.getAttribute("data-a");
      if (act === "listmenu") { listMenu(a); return; }
      if (act === "add") { openAdd(); return; }
      if (act === "settings") { settingsMenu(a); return; }
      if (act === "dchart") { goChart(det.getAttribute("data-id")); return; }
      if (act === "dcompare") { var did = det.getAttribute("data-id"); if (root.jhAddCompare) root.jhAddCompare(did); return; }
      if (act === "dflag") { var r0 = a.getBoundingClientRect(); flagMenu(r0.left, r0.bottom + 4, det.getAttribute("data-id")); return; }
      if (act === "drm") { var L0 = cur(), i0 = L0.items.indexOf(det.getAttribute("data-id")); if (i0 >= 0) removeIdx([i0]); return; }
      if (act === "dadd") { add(det.getAttribute("data-id")); return; }
      var srt = e.target.closest("[data-sort]");
      if (srt) {
        var k = srt.getAttribute("data-sort");
        if (D.sort.col !== k) D.sort = { col: k, dir: k === "sym" ? 1 : -1 };
        else if (D.sort.dir === (k === "sym" ? 1 : -1)) D.sort.dir = -D.sort.dir;
        else D.sort = { col: null, dir: 0 };
        save(); render(); return;
      }
      var row = rowFromEvent(e); if (!row) return;
      var vi = +row.getAttribute("data-vi"), r = view[vi]; if (!r) return;
      if (r.sec) {
        if (act === "delsec") { removeIdx([r.idx]); return; }
        var L = cur(); D.collapsed[L.id] = D.collapsed[L.id] || {}; D.collapsed[L.id][r.title] = !D.collapsed[L.id][r.title]; save(); render(); return;
      }
      if (act === "rm") { removeIdx(sel[r.idx] ? selectedIdx() : [r.idx]); return; }
      if (act === "flag") {
        e.stopPropagation();
        if (D.flags[r.id]) { setFlag(r.id, null); } else { var c = D.lastFlag || "red"; setFlag(r.id, c); }
        return;
      }
      focusIdx = vi;
      if (e.shiftKey && anchor != null) {
        sel = {}; var a1 = Math.min(anchor, vi), b1 = Math.max(anchor, vi);
        for (var i = a1; i <= b1; i++) if (view[i] && !view[i].sec) sel[view[i].idx] = true;
        render(); return;
      }
      if (e.ctrlKey || e.metaKey) { sel[r.idx] = !sel[r.idx]; anchor = vi; render(); return; }
      sel = {}; sel[r.idx] = true; anchor = vi;
      body.querySelectorAll(".wl-row.sel,.wl-row.foc").forEach(function (x) { x.classList.remove("sel", "foc"); });
      row.classList.add("sel", "foc");
      goChart(r.id);
      body.focus({ preventScroll: true });
    });
    panel.addEventListener("dblclick", function (e) {
      var row = rowFromEvent(e); if (!row) return; var r = view[+row.getAttribute("data-vi")];
      if (r && r.sec) { var n = root.prompt("Rename section", r.title); if (n) { cur().items[r.idx] = "###" + n.trim().toUpperCase(); save(); render(); } }
    });
    panel.addEventListener("contextmenu", function (e) {
      var fl = e.target.closest(".wl-flag");
      var row = rowFromEvent(e); if (!row) return;
      e.preventDefault();
      var vi = +row.getAttribute("data-vi");
      if (fl && view[vi] && !view[vi].sec) { flagMenu(e.clientX, e.clientY, view[vi].id); return; }
      rowMenu(e.clientX, e.clientY, vi);
    });
    body.addEventListener("keydown", function (e) {
      var k = e.key;
      if (["ArrowDown", "ArrowUp", "Delete", "Backspace", "Enter", "Home", "End"].indexOf(k) < 0 && !(k === "a" && (e.ctrlKey || e.metaKey))) return;
      e.preventDefault(); e.stopPropagation();
      if (k === "a") { sel = {}; view.forEach(function (r) { if (!r.sec) sel[r.idx] = true; }); render(); return; }
      if (k === "Delete" || k === "Backspace") { var s = selectedIdx(); if (s.length) removeIdx(s); return; }
      var rows = view.map(function (r, i) { return r.sec ? -1 : i; }).filter(function (i) { return i >= 0; });
      if (!rows.length) return;
      var p = rows.indexOf(focusIdx);
      if (k === "Enter") { if (view[focusIdx]) goChart(view[focusIdx].id); return; }
      if (k === "Home") p = 0; else if (k === "End") p = rows.length - 1;
      else p = p < 0 ? 0 : Math.max(0, Math.min(rows.length - 1, p + (k === "ArrowDown" ? 1 : -1)));
      focusIdx = rows[p];
      var r = view[focusIdx];
      if (e.shiftKey) { sel[r.idx] = true; } else { sel = {}; sel[r.idx] = true; anchor = focusIdx; }
      body.querySelectorAll(".wl-row.sel,.wl-row.foc").forEach(function (x) { x.classList.remove("sel", "foc"); });
      body.querySelectorAll(".wl-row").forEach(function (x) { var i = +x.getAttribute("data-vi"); var rr = view[i]; if (rr && sel[rr.idx]) x.classList.add("sel"); if (i === focusIdx) { x.classList.add("foc"); x.scrollIntoView({ block: "nearest" }); } });
      if (!e.shiftKey) goChart(r.id);
    });
    // drag & drop (multi-select aware)
    var dragIdx = null;
    body.addEventListener("dragstart", function (e) {
      var row = rowFromEvent(e); if (!row) return;
      var r = view[+row.getAttribute("data-vi")]; if (!r) return;
      if (r.sec) { var L = cur(); dragIdx = [r.idx]; var j = r.idx + 1; while (j < L.items.length && !isSec(L.items[j])) dragIdx.push(j++); }
      else dragIdx = sel[r.idx] ? selectedIdx() : [r.idx];
      e.dataTransfer.effectAllowed = "move";
      try { e.dataTransfer.setData("text/plain", dragIdx.map(function (i) { return cur().items[i]; }).join(",")); } catch (x) {}
    });
    body.addEventListener("dragover", function (e) {
      if (!dragIdx) return; e.preventDefault();
      body.querySelectorAll(".drop-a,.drop-b").forEach(function (x) { x.classList.remove("drop-a", "drop-b"); });
      var row = rowFromEvent(e); if (!row) return;
      var rc = row.getBoundingClientRect(); row.classList.add(e.clientY < rc.top + rc.height / 2 ? "drop-a" : "drop-b");
      var br = body.getBoundingClientRect(); if (e.clientY < br.top + 24) body.scrollTop -= 12; else if (e.clientY > br.bottom - 24) body.scrollTop += 12;
    });
    body.addEventListener("dragleave", function (e) { if (!body.contains(e.relatedTarget)) body.querySelectorAll(".drop-a,.drop-b").forEach(function (x) { x.classList.remove("drop-a", "drop-b"); }); });
    body.addEventListener("drop", function (e) {
      if (!dragIdx) return; e.preventDefault();
      var row = rowFromEvent(e), L = cur(), to = L.items.length;
      if (row) {
        var r = view[+row.getAttribute("data-vi")], rc = row.getBoundingClientRect(), after = e.clientY >= rc.top + rc.height / 2;
        to = r.idx + (after ? 1 : 0);
        if (r.sec && after) to = r.idx + 1;
      }
      body.querySelectorAll(".drop-a,.drop-b").forEach(function (x) { x.classList.remove("drop-a", "drop-b"); });
      var d = dragIdx; dragIdx = null;
      if (d.indexOf(to) >= 0 && d.length === 1) return;
      moveIdx(d, to);
    });
    body.addEventListener("dragend", function () { dragIdx = null; body.querySelectorAll(".drop-a,.drop-b").forEach(function (x) { x.classList.remove("drop-a", "drop-b"); }); });
    // details splitter
    var sp = panel.querySelector(".wl-split");
    sp.addEventListener("pointerdown", function (e) {
      e.preventDefault(); var y0 = e.clientY, h0 = D.detH; sp.setPointerCapture(e.pointerId);
      function mv(ev) { D.detH = Math.max(90, Math.min(panel.clientHeight - 120, h0 - (ev.clientY - y0))); det.style.height = D.detH + "px"; }
      function up() { sp.removeEventListener("pointermove", mv); sp.removeEventListener("pointerup", up); save(); renderDetails(); }
      sp.addEventListener("pointermove", mv); sp.addEventListener("pointerup", up);
    });
  }
  function openAdd() {
    if (root.JHUniSearch) root.JHUniSearch.open({ dest: "add" });
    else { var s = root.prompt("Add symbol"); if (s) add(s); }
  }

  // Show our panel for the Watchlist rail button; give Details/News/Alerts back to the legacy panel.
  function railHook() {
    doc.addEventListener("click", function (e) {
      var b = e.target.closest("#rrail [data-rail], [data-rail]"); if (!b) return;
      var w = doc.getElementById("watch"); if (!w) return;
      var r = b.getAttribute("data-rail");
      if (r === "watch") w.classList.add("jhwl-on"); else w.classList.remove("jhwl-on");
    }, true);
  }
  function trackActive() {
    setInterval(function () {
      var a = root.jhActive || "";
      if (a && a !== activeSym) {
        activeSym = a;
        if (body) {
          body.querySelectorAll(".wl-row.act").forEach(function (r) { r.classList.remove("act"); });
          body.querySelectorAll(".wl-row").forEach(function (r) { var id = r.getAttribute("data-id"); if (sameId(id, a) || sameId(short(id), a)) r.classList.add("act"); });
          renderDetails();
        }
      }
    }, 600);
    setInterval(function () { if (panel && !doc.hidden && doc.getElementById("watch") && doc.getElementById("watch").classList.contains("is-open")) observe(); }, 60000);
  }
  // Global shortcut like TradingView: Alt+W adds the current chart symbol to the active list.
  function keys() {
    doc.addEventListener("keydown", function (e) {
      if (e.altKey && !e.ctrlKey && !e.metaKey && (e.key === "w" || e.key === "W" || e.code === "KeyW")) {
        var t = e.target; if (t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.isContentEditable)) return;
        e.preventDefault(); var a = root.jhActive; if (a) { var r = add(a); toast(r === "dup" ? a + " is already in " + cur().name : "Added " + a + " to " + cur().name); }
      }
    });
  }

  function boot() {
    load();
    var tries = 0;
    (function tryMount() {
      if (!mount()) { if (++tries < 120) setTimeout(tryMount, 250); return; }
      render();
      seed().then(function () { render(); });
    })();
    railHook(); trackActive(); keys();
  }

  root.JHTvWatchlist = {
    add: function (id) { var r = add(id); return r; },
    remove: function (id) { var L = cur(); if (!L) return; var i = L.items.findIndex(function (s) { return sameId(s, id); }); if (i >= 0) removeIdx([i]); },
    activeName: function () { var L = cur(); return L ? L.name : "watchlist"; },
    lists: function () { return D.order.map(function (id) { return { id: id, name: D.lists[id].name, n: D.lists[id].items.length }; }); },
    open: function (id) { setActive(id); },
    show: function () { var w = doc.getElementById("watch"); if (w) w.classList.add("jhwl-on"); if (root.jhWatchSet) root.jhWatchSet(true); },
    _data: function () { return D; },
    render: function () { render(); }
  };
  if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", boot); else boot();
})(window);
