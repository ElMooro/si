/* tvwatch-layout-3 — TradingView watchlist.
   Columns, flags, sections, drag, sort, resize, table, advanced view, import/export, notes, alerts.
   Warehouse lists are not deleted. Order, hides, and colors stay on this device. */
(function () {
  if (window.__jhTvWatch2) return;
  window.__jhTvWatch2 = 1;
  var UP = "#089981", DN = "#f23645";
  var FLAG_COLORS = ["#f23645", "#ff6d00", "#fdd835", "#089981", "#2962ff", "#ab47bc"];
  var MET = [
    { id: "last", label: "Last" },
    { id: "chg", label: "Chg" },
    { id: "chgp", label: "Chg%" },
    { id: "vol", label: "Vol" },
    { id: "ext", label: "Ext" },
    { id: "avg", label: "Avg Vol", table: 1 },
    { id: "w1", label: "1W", table: 1 },
    { id: "m1", label: "1M", table: 1 },
    { id: "m3", label: "3M", table: 1 },
    { id: "ytd", label: "YTD", table: 1 },
    { id: "y1", label: "1Y", table: 1 },
    { id: "rsi", label: "RSI", table: 1 },
    { id: "mom", label: "Mom", table: 1 }
  ];
  var UI_KEY = "jh-tv-watch-ui";
  var ui = {
    cols: { last: 1, chg: 1, chgp: 1, vol: 1, ext: 1, w1: 0, m1: 0, ytd: 0 },
    colOrder: ["last", "chg", "chgp", "vol", "ext", "avg", "w1", "m1", "m3", "ytd", "y1", "rsi", "mom"],
    logo: 1, ticker: 1, desc: 0, sort: "", dir: -1, table: 0, adv: 0, advTab: "price", group: "none", summary: 1,
    active: "", flag: "", collapsed: {}, alias: {}, order: {}, hide: {}, extra: {}, starred: {}, listColor: {}, widths: {}
  };
  try { var saved = JSON.parse(localStorage.getItem(UI_KEY) || "null"); if (saved && typeof saved === "object") { Object.keys(saved).forEach(function (k) { ui[k] = saved[k]; }); } } catch (e) {}
  if (!ui.cols) ui.cols = { last: 1, chg: 1, chgp: 1, vol: 1, ext: 1 };
  if (!ui.colOrder) ui.colOrder = ["last", "chg", "chgp", "vol", "ext", "avg", "w1", "m1", "m3", "ytd", "y1", "rsi", "mom"];
  ["order", "hide", "extra", "collapsed", "alias", "starred", "listColor", "widths"].forEach(function (k) { if (!ui[k] || typeof ui[k] !== "object") ui[k] = {}; });
  if (!ui.advTab) ui.advTab = "price";
  if (!ui.group) ui.group = "none";
  if (ui.summary == null) ui.summary = 1;
  ["avg", "w1", "m1", "m3", "ytd", "y1", "rsi", "mom"].forEach(function (id) {
    if (ui.colOrder.indexOf(id) < 0) ui.colOrder.push(id);
    if (ui.cols[id] == null) ui.cols[id] = 0;
  });
  function saveUi() { try { localStorage.setItem(UI_KEY, JSON.stringify(ui)); } catch (e) {} }
  function read(k, fb) { try { var v = JSON.parse(localStorage.getItem(k) || ""); return v == null ? fb : v; } catch (e) { return fb; } }
  function write(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} }

  var catalog = {}, quotes = {}, qSet = {}, eSet = {}, inflight = 0, einflight = 0, wait = [], ewait = [], lock = 0, dragId = "";
  var css = document.createElement("style");
  css.id = "jh-tvwatch-css";
  css.textContent = [
    "#watch{font-family:-apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,Ubuntu,sans-serif;background:#131722}",
    "#letters,#watch .filt,#paste,#nlists,.listbtn .n{display:none!important}",
    "#w-list{background:#131722}",
    "#cols{display:grid;align-items:center;gap:0 6px;padding:2px 8px 2px 0;color:#787b86;font-size:11px;user-select:none;border-bottom:1px solid #2a2e39}",
    "#cols [data-sort]{cursor:pointer;text-align:right}",
    "#cols [data-sort].on{color:#d1d4dc}",
    "#cols .symh{text-align:left!important}",
    "#wlist .wrow{display:grid;align-items:center;gap:0 6px;min-height:32px;padding:0 4px 0 0;background:transparent;color:#d1d4dc;font-size:13px;position:relative;width:100%;text-align:left;border:0;border-radius:0}",
    "#wlist .wrow:hover{background:#2a2e39}",
    "#wlist .wrow.on{background:#2a2e39}",
    "#wlist .wrow.drop{box-shadow:inset 0 -2px 0 #2962ff}",
    "#wlist .wacc{width:3px;align-self:stretch;background:transparent!important}",
    "#wlist .wrow.on .wacc{background:#2962ff!important}",
    "#wlist .grip{width:16px;height:24px;color:#787b86;opacity:0;cursor:grab;font-size:12px;padding:0}",
    "#wlist .wrow:hover .grip{opacity:1}",
    "#wlist .tvflag{width:16px;height:16px;border-radius:2px;color:#787b86;font-size:12px;padding:0}",
    "#wlist .tvflag.on{color:#fff}",
    "#wlist .tvlogo{width:22px;height:22px;border-radius:50%;overflow:hidden;display:inline-flex;align-items:center;justify-content:center;font-size:10px;font-weight:600;color:#fff;background:#363a45;position:relative}",
    "#wlist .tvlogo img{width:22px;height:22px;object-fit:cover;position:absolute;inset:0}",
    "#wlist .wmain{min-width:0;display:flex;flex-direction:column;line-height:1.15}",
    "#wlist .wsym{font-weight:400;color:#d1d4dc;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
    "#wlist .wdesc{color:#787b86;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
    "#wlist [data-k]{text-align:right;font-variant-numeric:tabular-nums;font-size:13px}",
    "#wlist .up{color:#089981}#wlist .dn{color:#f23645}",
    "#wlist .ext{font-size:11px}",
    "#wlist .wx{width:18px;height:18px;color:#787b86;opacity:0;padding:0}",
    "#wlist .wrow:hover .wx{opacity:1}",
    "#wlist .wsec{display:flex;align-items:center;gap:6px;height:28px;padding:0 10px;color:#787b86;font-size:11px;letter-spacing:.06em;text-transform:uppercase;background:#1e222d;cursor:pointer}",
    "#wlist .wsec .sc{margin-left:auto}",
    "#wlist .flash-up{animation:jhup .45s}#wlist .flash-dn{animation:jhdn .45s}",
    "@keyframes jhup{from{background:rgba(8,153,129,.35)}to{background:transparent}}",
    "@keyframes jhdn{from{background:rgba(242,54,69,.35)}to{background:transparent}}",
    ".listbtn{gap:8px!important}",
    ".listbtn .tvflag{width:12px;height:12px;border-radius:2px;flex:none;display:inline-block}",
    ".addsym{border:0!important;text-align:left!important;color:#2962ff!important;font-size:13px!important;padding:8px 12px!important;background:transparent}",
    "#watch .wtitle{height:0;padding:0;border:0;overflow:visible}",
    "#watch .wtitle b{display:none}",
    "#watch .wtitle .wops{position:absolute;right:6px;top:6px;z-index:6}",
    "#watch .whead{height:38px;padding:4px 86px 4px 8px;background:#131722}",
    "#tvadd{display:none;margin:0 10px 8px}",
    "#tvadd.on{display:block}",
    "#tvadd input{width:100%;height:28px;background:#131722;border:1px solid #2a2e39;border-radius:4px;color:#d1d4dc;padding:0 8px;font-size:13px}",
    "#tvmenu,#tvflags{display:none;position:fixed;z-index:96;background:#1e222d;border:1px solid #2a2e39;border-radius:6px;box-shadow:0 12px 32px rgba(0,0,0,.45);min-width:220px;max-height:70vh;overflow:auto;padding:6px 0;color:#d1d4dc;font-size:13px}",
    "#tvmenu.on,#tvflags.on{display:block}",
    "#tvmenu button,#tvmenu label{display:flex;width:100%;text-align:left;padding:7px 14px;gap:8px;align-items:center;color:#d1d4dc;cursor:pointer}",
    "#tvmenu button:hover,#tvmenu label:hover{background:#2a2e39}",
    "#tvmenu .lab{padding:8px 14px 2px;font-size:11px;letter-spacing:.08em;color:#787b86}",
    "#tvmenu i{width:14px;color:#2962ff;font-style:normal}",
    "#tvflags{display:none;padding:8px;gap:6px;min-width:0}",
    "#tvflags.on{display:flex}",
    "#tvflags button{width:18px;height:18px;border-radius:50%;border:2px solid transparent}",
    "#tvflags button.on{border-color:#fff}",
    "#tvcard{padding:12px 14px 8px;border-bottom:1px solid #2a2e39}",
    "#tvcard .row1{display:flex;align-items:center;gap:8px}",
    "#tvcard .px{font-size:22px;font-weight:600;font-variant-numeric:tabular-nums}",
    "#tvcard .rng{position:relative;height:4px;background:#2a2e39;border-radius:2px;margin:8px 0 4px}",
    "#tvcard .rng i{position:absolute;top:-3px;width:8px;height:8px;border-radius:50%;background:#d1d4dc}",
    "#tvcard .rlab{display:flex;justify-content:space-between;color:#787b86;font-size:11px}",
    "#listres .ld-row{display:flex;align-items:center;gap:8px}",
    "#listres .ld-flag{width:10px;height:10px;border-radius:2px;flex:none}",
    "#wlist.table{overflow-x:auto}",
    "#wlist.table .wrow{min-width:860px}",
"#w-list{display:flex;flex-direction:column;min-height:0;flex:1}",
    "#wlist{overflow:auto;flex:1}",
    "#cols{flex:none}",
    "#cols [data-sort]{position:relative}",
    "#cols .rz{position:absolute;right:-3px;top:0;width:6px;height:100%;cursor:col-resize}",
    "#tvfbar{display:flex;align-items:center;gap:6px;padding:4px 10px;border-bottom:1px solid #2a2e39;flex:none}",
    "#tvfbar button{height:18px;min-width:18px;border-radius:9px;border:1px solid #2a2e39;background:transparent;color:#787b86;font-size:11px;padding:0 6px}",
    "#tvfbar button.on{border-color:#d1d4dc;color:#fff}",
    "#tv-pie{color:#787b86;font-size:14px;padding:0 4px}",
    ".listbtn em{margin-left:auto;color:#787b86;font-style:normal;font-size:11px}",
    "#listdrop .ld-h{padding:8px 12px 2px;font-size:11px;letter-spacing:.08em;color:#787b86}",
    "#listdrop .ld-row{display:flex;align-items:center;width:100%;background:transparent;color:#d1d4dc}",
    "#listdrop .ld-row>button[data-id]{flex:1;display:flex;align-items:center;gap:8px;background:transparent;color:#d1d4dc;padding:6px 10px;text-align:left;min-width:0}",
    "#listdrop .ld-row.on,#listdrop .ld-row:hover{background:#2a2e39}",
    "#listdrop .ld-row b{flex:1;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
    "#listdrop .ld-row span{color:#787b86;font-size:12px}",
    "#listdrop .ld-star{color:#787b86;background:transparent;border:0;padding:0 8px}",
    "#listdrop .ld-star.on{color:#fdd835}",
    "#ld-foot{border-top:1px solid #2a2e39;display:flex;flex-wrap:wrap;padding:4px;background:#131722;flex:none}",
    "#ld-foot button{flex:1 1 45%;text-align:left;padding:6px 8px;color:#d1d4dc;background:transparent;font-size:12px}",
    "#ld-foot button:hover{background:#2a2e39}",
    "#listres{overflow:auto;max-height:42vh}",
    "#tvimp{display:none;position:fixed;z-index:97;width:min(420px,92vw);background:#1e222d;border:1px solid #2a2e39;border-radius:8px;box-shadow:0 16px 40px rgba(0,0,0,.5);padding:12px;color:#d1d4dc}",
    "#tvimp.on{display:block}",
    "#tvimp textarea{width:100%;height:140px;margin:8px 0;background:#131722;color:#d1d4dc;border:1px solid #2a2e39;border-radius:4px;padding:8px;font-size:12px}",
    "#tvimp .row{display:flex;gap:8px;flex-wrap:wrap}",
    "#tvimp button{padding:6px 10px;background:#2a2e39;color:#d1d4dc;border-radius:4px}",
    "#tvtoast{display:none;position:fixed;z-index:120;right:16px;bottom:48px;background:#1e222d;color:#d1d4dc;border:1px solid #2962ff;border-radius:6px;padding:10px 12px;max-width:320px;box-shadow:0 8px 24px rgba(0,0,0,.4)}",
    "#tvtoast.on{display:block}",
    "#tvcard textarea{width:100%;margin-top:8px;height:48px;background:#131722;color:#d1d4dc;border:1px solid #2a2e39;border-radius:4px;padding:6px;font-size:12px;resize:vertical}",
    "#tvcard .meta{display:flex;justify-content:space-between;gap:8px;color:#787b86;font-size:11px;margin-top:4px}",
    "#tvadv{display:none;position:absolute;inset:0;z-index:40;background:#131722;color:#d1d4dc;flex-direction:column}",
    "#tvadv.on{display:flex}",
    "#tvadv .ah{display:flex;align-items:center;gap:8px;padding:8px 10px;border-bottom:1px solid #2a2e39;flex:none}",
    "#tvadv .ah b{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}",
    "#tvadv .tabs{display:flex;gap:4px;padding:6px 10px;flex:none}",
    "#tvadv .tabs button,#tvadv .ah button,#tvadv .tool button,#tvadv .tool select{background:#1e222d;color:#d1d4dc;border:1px solid #2a2e39;border-radius:4px;padding:4px 8px;font-size:12px}",
    "#tvadv .tabs button.on{border-color:#2962ff;color:#fff}",
    "#tvadv .tool{display:flex;gap:8px;align-items:center;padding:0 10px 6px;color:#787b86;font-size:12px;flex:none;flex-wrap:wrap}",
    "#tvadv .advsc{overflow:auto;flex:1}",
    "#tvadv table{border-collapse:collapse;font-size:12px;min-width:100%}",
    "#tvadv th,#tvadv td{padding:4px 8px;border-bottom:1px solid #2a2e39;white-space:nowrap;text-align:right}",
    "#tvadv th:first-child,#tvadv td:first-child{text-align:left;position:sticky;left:0;background:#131722}",
    "#tvadv tr.gh td{background:#1e222d;color:#787b86;font-size:11px;letter-spacing:.04em;text-align:left}",
    "#tvadv tbody tr[data-s]:hover td{background:#2a2e39}",
    "#tvadv .pies{display:flex;gap:16px;padding:8px 12px;border-top:1px solid #2a2e39;flex:none}",
    "#tvadv .pie{width:64px;height:64px;border-radius:50%}",
    "#tvadv .plab{font-size:11px;color:#787b86;max-width:180px}",
    "#tvadv .note{padding:0 12px 8px;color:#787b86;font-size:11px;flex:none}",
    "#wlist .tvflag{font-size:0;border:1px solid #787b86;background:transparent;border-radius:2px;width:10px;height:10px}",
    "#wlist .tvflag.on{border-color:transparent}"
  ].join("");
  document.documentElement.appendChild(css);

  function bare(s) { s = String(s || ""); var i = s.lastIndexOf(":"); return i >= 0 ? s.slice(i + 1) : s; }
  function exch(s) { s = String(s || ""); var i = s.indexOf(":"); return i > 0 ? s.slice(0, i) : ""; }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      if (c === "&") return "&" + "amp;";
      if (c === "<") return "&" + "lt;";
      if (c === ">") return "&" + "gt;";
      if (c === "\"") return "&" + "quot;";
      return "&" + "#39;";
    });
  }
  function hue(s) { var h = 0, i; s = bare(s); for (i = 0; i < s.length; i++) h = (h * 33 + s.charCodeAt(i)) >>> 0; return h % 360; }
  function num(n) {
    if (typeof n !== "number" || !isFinite(n)) return "\u2014";
    var a = Math.abs(n), d = a >= 100 ? 2 : a >= 1 ? 2 : a >= 0.01 ? 4 : 6;
    return n.toLocaleString("en-US", { minimumFractionDigits: Math.min(2, d), maximumFractionDigits: d });
  }
  function vol(n) {
    if (!(n > 0) || !isFinite(n)) return "\u2014";
    if (n >= 1e9) return (n / 1e9).toFixed(2) + "B";
    if (n >= 1e6) return (n / 1e6).toFixed(2) + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(2) + "K";
    return String(Math.round(n));
  }
  function pct(n) { if (typeof n !== "number" || !isFinite(n)) return "\u2014"; return (n >= 0 ? "+" : "") + (n * 100).toFixed(2) + "%"; }
  function cls(n) { return n < 0 ? "dn" : "up"; }
  function activeSym() { var el = document.getElementById("symin"); return el ? String(el.value || "").trim() : ""; }
  function flags() { return read("jh-chart-flags", {}) || {}; }
  function setFlag(s, color) {
    var f = flags();
    if (color) f[s] = color; else delete f[s];
    write("jh-chart-flags", f);
    if (!quotes[s]) quotes[s] = {};
    quotes[s].flag = color || "";
  }
  function customLists() {
    var dst = read("jh-chart-custom-lists", []);
    return Array.isArray(dst) ? dst : [];
  }
  function saveCustom(arr) { write("jh-chart-custom-lists", arr); }
  function findCustom(id) {
    var arr = customLists(), i;
    for (i = 0; i < arr.length; i++) if (String(arr[i].id) === String(id)) return arr[i];
    return null;
  }
  function findList(id) {
    if (!id) return null;
    if (String(id).indexOf("flag:") === 0) {
      var c = id.slice(5), f = flags(), syms = [];
      Object.keys(f).forEach(function (k) { if (f[k] === c) syms.push(k); });
      return { id: id, name: "Flagged", symbols: syms, color: c, virtual: 1 };
    }
    if (id === "favorites") {
      var fav = read("jh-chart-favs", []);
      return { id: "favorites", name: "Favorites", symbols: Array.isArray(fav) ? fav : [], color: "#fdd835", virtual: 1 };
    }
    if (catalog[id]) return catalog[id];
    return findCustom(id);
  }
  function mergeOrder(source, extras, order, hidden) {
    var pool = {}, seq = [], used = {}, i, s;
    (source || []).forEach(function (x) { pool[x] = 1; });
    (extras || []).forEach(function (x) { pool[x] = 1; });
    (order || []).forEach(function (x) {
      if (String(x).indexOf("###") === 0) { seq.push(x); return; }
      if (pool[x] && !hidden[x] && !used[x]) { seq.push(x); used[x] = 1; }
    });
    (source || []).concat(extras || []).forEach(function (x) {
      if (!used[x] && !hidden[x]) { seq.push(x); used[x] = 1; }
    });
    return seq;
  }
  function sortSeq(seq, key, dir) {
    if (!key) return seq.slice();
    var out = [], block = [];
    function val(s) {
      if (key === "sym") return bare(s);
      var q = quotes[s] || quotes[bare(s)] || {};
      if (key === "last") return q.last;
      if (key === "chg") return q.chgv;
      if (key === "chgp") return q.chg;
      if (key === "vol") return q.vol;
      if (key === "ext") return q.ext;
      return q[key];
    }
    function flush() {
      block.sort(function (a, b) {
        var va = val(a), vb = val(b);
        var sa = key === "sym";
        if (va == null || va !== va) return 1;
        if (vb == null || vb !== vb) return -1;
        if (va < vb) return -dir;
        if (va > vb) return dir;
        return sa ? 0 : 0;
      });
      out = out.concat(block);
      block = [];
    }
    seq.forEach(function (s) {
      if (String(s).indexOf("###") === 0) { flush(); out.push(s); }
      else block.push(s);
    });
    flush();
    return out;
  }
  function sectionCounts(seq) {
    var map = {}, name = "", n = 0, i;
    function close() { if (name) map[name] = n; }
    for (i = 0; i < seq.length; i++) {
      if (String(seq[i]).indexOf("###") === 0) { close(); name = String(seq[i]).replace(/^#+/, ""); n = 0; }
      else n++;
    }
    close();
    return map;
  }
  function applyCollapse(seq, id) {
    var out = [], hide = false, name = "";
    seq.forEach(function (s) {
      if (String(s).indexOf("###") === 0) {
        name = String(s).replace(/^#+/, "");
        hide = !!ui.collapsed[id + "|" + name];
        out.push(s);
      } else if (!hide) out.push(s);
    });
    return out;
  }
  function viewOf(L) {
    if (!L) return [];
    var hidden = ui.hide[L.id] || {};
    var seq = mergeOrder(L.symbols || [], ui.extra[L.id] || [], ui.order[L.id] || [], hidden);
    if (ui.flag) seq = seq.filter(function (s) { return String(s).indexOf("###") === 0 || (flags()[s] === ui.flag); });
    seq = sortSeq(seq, ui.sort, ui.dir || 1);
    return { full: seq, shown: applyCollapse(seq, L.id), counts: sectionCounts(seq) };
  }
  window.__jhTvAPI = { mergeOrder: mergeOrder, sortSeq: sortSeq, sectionCounts: sectionCounts, parseImport: parseImport, rsi: rsi, kindOf: kindOf };

  function metrics() {
    return ui.colOrder.filter(function (id) {
      var m = MET.filter(function (x) { return x.id === id; })[0];
      if (!m) return false;
      if (m.table && !ui.table) return false;
      return !!ui.cols[id];
    });
  }
  function gridCss() {
    var cols = ["3px", "16px", "16px"];
    if (ui.logo) cols.push("22px");
    if (ui.ticker || ui.desc) cols.push(colW("sym"));
    metrics().forEach(function (id) { cols.push(colW(id)); });
    cols.push("16px");
    return cols.join(" ");
  }
  function tickerOf(s) {
    var t = String(s || "").toUpperCase();
    if (/^[A-Z0-9.\-]{1,12}$/.test(t)) return t;
    t = bare(t).toUpperCase();
    return /^[A-Z0-9.\-]{1,12}$/.test(t) ? t : "";
  }
  function logoHtml(s) {
    if (!ui.logo) return "";
    var b = bare(s), letter = esc((b || "?").slice(0, 1));
    var img = /^[A-Z]{1,5}$/.test(b) ? "<img alt='' src='https://financialmodelingprep.com/image-stock/" + esc(b) + ".png' onerror='this.remove()'>" : "";
    return "<i class=tvlogo style='background:hsl(" + hue(s) + ",42%,42%)'>" + img + letter + "</i>";
  }
  function cell(k, text, klass) { return "<span data-k='" + k + "' class='" + (klass || "") + "'>" + text + "</span>"; }
  function cells(s) {
    var q = quotes[s] || quotes[bare(s)] || {};
    var html = "";
    metrics().forEach(function (id) { html += cell(id, fmtCell(id, q), cellClass(id, q)); });
    return html;
  }
  function rowHtml(s, on) {
    var b = bare(s), f = flags()[s] || flags()[b] || "";
    var desc = ui.desc ? descOf(s) : "";
    return "<div class='wrow" + (on ? " on" : "") + "' draggable='true' data-s='" + esc(s) + "' role='button' tabindex='0' style='grid-template-columns:" + gridCss() + "'>" +
      "<i class=wacc></i><button type=button class=grip title='Drag to reorder'>\u2807</button>" +
      "<button type=button class='tvflag" + (f ? " on" : "") + "' data-flag='1' title='Flag' style='" + (f ? "background:" + esc(f) : "") + "'></button>" +
      logoHtml(s) +
      ((ui.ticker || desc) ? "<span class=wmain>" + (ui.ticker ? "<span class=wsym>" + esc(b) + "</span>" : "") + (desc ? "<span class=wdesc>" + esc(desc) + "</span>" : "") + "</span>" : "") +
      cells(s) +
      "<button type=button class=wx title='Remove'>\u00d7</button></div>";
  }
  function paintHeader() {
    var cols = document.getElementById("cols"); if (!cols) return;
    var html = "<span></span><span></span><span></span>" + (ui.logo ? "<span></span>" : "") +
      ((ui.ticker || ui.desc) ? "<span class='symh' data-sort='sym'>Symbol" + (ui.sort === "sym" ? (ui.dir < 0 ? " \u25bc" : " \u25b2") : "") + "</span>" : "");
    metrics().forEach(function (id) {
      var m = MET.filter(function (x) { return x.id === id; })[0];
      html += "<span data-sort='" + id + "'" + (ui.sort === id ? " class=on" : "") + ">" + m.label + (ui.sort === id ? (ui.dir < 0 ? " \u25bc" : " \u25b2") : "") + "</span>";
    });
    html += "<span></span>";
    cols.style.gridTemplateColumns = gridCss();
    cols.innerHTML = html;
    cols.querySelectorAll("[data-sort]").forEach(function (n) {
      var rz = document.createElement("i"); rz.className = "rz"; n.appendChild(rz);
      rz.onmousedown = function (e) {
        e.preventDefault(); e.stopPropagation();
        n.dataset.rz = "1";
        var id = n.getAttribute("data-sort");
        var startX = e.clientX, start = n.getBoundingClientRect().width;
        function mv(ev) { ui.widths[id] = Math.max(36, Math.min(200, start + (ev.clientX - startX))); applyWidths(); }
        function up() {
          document.removeEventListener("mousemove", mv); document.removeEventListener("mouseup", up); saveUi();
          setTimeout(function () { delete n.dataset.rz; }, 0);
        }
        document.addEventListener("mousemove", mv); document.addEventListener("mouseup", up);
      };
      n.onclick = function () {
        if (n.dataset.rz) return;
        var k = n.getAttribute("data-sort");
        if (ui.sort !== k) { ui.sort = k; ui.dir = k === "sym" ? 1 : -1; }
        else if (ui.dir < 0) ui.dir = 1;
        else { ui.sort = ""; ui.dir = -1; }
        saveUi(); paint();
      };
    });
  }
  function openSym(s) {
    var q = document.getElementById("q");
    if (!q) { if (window.jhOpenSymbol) window.jhOpenSymbol(s); return; }
    q.value = s;
    q.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    paintCard(s);
  }
  function paintCard(s) {
    var host = document.getElementById("tvcard");
    var stack = document.getElementById("w-stack");
    if (!host && stack) { host = document.createElement("div"); host.id = "tvcard"; stack.insertBefore(host, stack.firstChild); }
    if (!host) return;
    s = s || activeSym();
    var q = quotes[s] || quotes[bare(s)] || {};
    var has = q.last != null && isFinite(q.last);
    var range = "";
    if (has && q.low != null && q.high != null && q.high > q.low) {
      var p = Math.max(0, Math.min(1, (q.last - q.low) / (q.high - q.low))) * 100;
      range = "<div class=rng><i style='left:calc(" + p.toFixed(1) + "% - 4px)'></i></div><div class=rlab><span>" + num(q.low) + "</span><span>Day range</span><span>" + num(q.high) + "</span></div>";
    }
    var keep = null, prevNote = host.querySelector("textarea");
    if (prevNote && document.activeElement === prevNote) keep = prevNote.value;
    var meta = "";
    if (has) meta = "<div class=meta><span>O " + num(q.open) + "</span><span>Prev " + num(q.prev) + "</span>" + (q.h52 != null ? "<span>52w " + num(q.l52) + " \u2013 " + num(q.h52) + "</span>" : "") + "</div>";
    host.innerHTML = "<div class=row1>" + logoHtml(s) + "<div class=wmain><b>" + esc(bare(s) || "\u2014") + "</b><span class=wdesc>" + esc(descOf(s)) + "</span></div></div>" +
      "<div class='px " + cls(q.chg) + "'>" + (has ? num(q.last) : "\u2014") + "</div>" +
      "<div class='" + cls(q.chg) + "'>" + (has ? ((q.chgv >= 0 ? "+" : "") + num(q.chgv) + "   " + pct(q.chg)) : "") + (q.ext == null ? "" : "   <span class='ext " + cls(q.ext) + "'>" + esc(q.extTag || "Ext") + " " + pct(q.ext) + "</span>") + "</div>" +
      range + (q.vol ? "<div class=rlab><span>Volume " + vol(q.vol) + "</span><span>" + (q.avg ? "Avg " + vol(q.avg) : "") + "</span></div>" : "") +
      meta + "<textarea placeholder='Private note \u2014 saved on this browser'>" + esc(noteOf(s)) + "</textarea>";
    var ta = host.querySelector("textarea");
    if (!ta) return;
    if (keep != null) { ta.value = keep; ta.focus(); }
    ta.onchange = function () { setNote(s, ta.value.trim()); };
  }
  function paintButton(L) {
    var btn = document.getElementById("listbtn"); if (!btn || !L) return;
    var name = (ui.alias && ui.alias[L.id]) || L.name || "Watchlist";
    if (ui.starred && ui.starred[L.id]) name = "\u2605 " + name;
    btn.innerHTML = "<i class=tvflag style='background:" + esc(listColorOf(L)) + "'></i><span>" + esc(name) + "</span><em>" + (paintButton._n != null ? paintButton._n : "") + "</em>";
  }
  function paint() {
    if (lock) return;
    var box = document.getElementById("wlist"); if (!box) return;
    var L = findList(ui.active);
    if (!L) {
      var sel = document.getElementById("list");
      if (sel && findList(sel.value)) { ui.active = sel.value; L = findList(ui.active); }
      else {
        var c0 = customLists()[0];
        if (c0) { ui.active = String(c0.id); L = c0; }
      }
    }
    if (!L) return;
    var view = viewOf(L);
    var cur = activeSym();
    paintButton._n = symCount(view.full);
    var sig = [ui.active, ui.sort, ui.dir, ui.table, ui.logo, ui.ticker, ui.desc, ui.flag, JSON.stringify(ui.widths || {}), view.shown.join("|"), cur].join("~");
    if (box.dataset.sig === sig && box.querySelector(".tvmark")) { ensureChrome(); paintButton(L); paintCard(cur); renderAdv(); return; }
    lock = 1;
    var sc = box.scrollTop;
    var html = ["<div class=tvmark hidden></div>"];
    view.shown.forEach(function (s) {
      if (String(s).indexOf("###") === 0) {
        var name = String(s).replace(/^#+/, "");
        var open = !ui.collapsed[L.id + "|" + name];
        html.push("<div class=wsec data-sec='" + esc(name) + "' draggable='true'><span>" + (open ? "\u25be" : "\u25b8") + "</span><span class=sn>" + esc(name) + "</span><span class=sc>" + (view.counts[name] || 0) + "</span></div>");
      } else html.push(rowHtml(s, bare(s) === bare(cur) || s === cur));
    });
    if (view.shown.length === 0) html.push("<div style='padding:16px;color:#787b86'>This list is empty. Use + to add a symbol.</div>");
    box.dataset.sig = sig;
    box.classList.toggle("table", !!ui.table);
    box.innerHTML = html.join("");
    box.scrollTop = sc;
    bindRows(box, L);
    paintHeader();
    paintButton(L);
    paintCard(cur);
    ensureChrome();
    renderAdv();
    lock = 0;
    see();
  }
  function bindRows(box, L) {
    box.querySelectorAll(".wrow").forEach(function (b) {
      var s = b.getAttribute("data-s");
      b.onclick = function (e) {
        if (e.target.closest && e.target.closest(".grip,.tvflag,.wx")) return;
        openSym(s);
      };
      b.oncontextmenu = function (e) { e.preventDefault(); openMenu(e.clientX, e.clientY, s); };
      var flag = b.querySelector(".tvflag");
      if (flag) flag.onclick = function (e) { e.stopPropagation(); openFlags(e.clientX, e.clientY, s); };
      var x = b.querySelector(".wx");
      if (x) x.onclick = function (e) { e.stopPropagation(); removeSym(L, s); };
      b.ondragstart = function (e) { dragId = s; e.dataTransfer.setData("text/plain", s); };
      b.ondragover = function (e) { e.preventDefault(); b.classList.add("drop"); };
      b.ondragleave = function () { b.classList.remove("drop"); };
      b.ondrop = function (e) {
        e.preventDefault(); b.classList.remove("drop");
        var from = e.dataTransfer.getData("text/plain") || dragId;
        moveItem(L, from, s);
      };
    });
    box.querySelectorAll(".wsec").forEach(function (sec) {
      var name = sec.getAttribute("data-sec");
      sec.onclick = function () {
        var k = L.id + "|" + name;
        ui.collapsed[k] = !ui.collapsed[k];
        saveUi(); paint();
      };
      sec.ondblclick = function (e) {
        e.preventDefault();
        var n = prompt("Rename section", name); if (!n) return;
        renameSection(L, name, n);
      };
      sec.draggable = true;
      sec.ondragstart = function (e) { dragId = "###" + name; e.dataTransfer.setData("text/plain", "###" + name); e.stopPropagation(); };
      sec.ondragover = function (e) { e.preventDefault(); };
      sec.ondrop = function (e) {
        e.preventDefault();
        var from = e.dataTransfer.getData("text/plain") || dragId;
        moveItem(L, from, "###" + name);
      };
      sec.oncontextmenu = function (e) { e.preventDefault(); e.stopPropagation(); openSectionMenu(e.clientX, e.clientY, L, name); };
    });
  }
  function baseSeq(L) { return mergeOrder(L.symbols || [], ui.extra[L.id] || [], ui.order[L.id] || [], ui.hide[L.id] || {}); }
  function persistSeq(L, seq) { ui.order[L.id] = seq; saveUi(); paint(); }
  function moveItem(L, from, before) {
    if (!from || from === before) return;
    if (!L || L.virtual) { if (L && L.virtual) toast("This view is read-only. Make a copy to edit it."); return; }
    var seq = baseSeq(L);
    if (String(from).indexOf("###") === 0) {
      var i = seq.indexOf(from); if (i < 0) return;
      var j = i + 1;
      while (j < seq.length && String(seq[j]).indexOf("###") !== 0) j++;
      var block = seq.slice(i, j);
      seq = seq.slice(0, i).concat(seq.slice(j));
      var at = seq.indexOf(before);
      if (at < 0) seq = seq.concat(block); else seq = seq.slice(0, at).concat(block, seq.slice(at));
      ui.sort = ""; persistSeq(L, seq); return;
    }
    seq = seq.filter(function (x) { return x !== from; });
    var at2 = seq.indexOf(before);
    if (at2 < 0) seq.push(from); else seq.splice(at2, 0, from);
    ui.sort = ""; persistSeq(L, seq);
  }
  function removeSym(L, s) {
    if (L.virtual) { toast("This view is read-only. Make a copy to edit it."); return; }
    var own = findCustom(L.id);
    if (own) {
      own.symbols = (own.symbols || []).filter(function (x) { return x !== s; });
      own.n = own.symbols.length;
      saveCustom(customLists().map(function (x) { return String(x.id) === String(L.id) ? own : x; }));
      L.symbols = own.symbols;
    } else {
      ui.hide[L.id] = ui.hide[L.id] || {};
      ui.hide[L.id][s] = 1;
    }
    if (ui.order[L.id]) ui.order[L.id] = ui.order[L.id].filter(function (x) { return x !== s; });
    saveUi(); paint();
  }
  function addSym(L, raw) {
    var s = String(raw || "").trim().toUpperCase();
    if (!s || !L) return;
    if (L.virtual) { toast("This view is read-only. Make a copy to edit it."); return; }
    var own = findCustom(L.id);
    if (own) {
      if ((own.symbols || []).indexOf(s) < 0) own.symbols.push(s);
      own.n = own.symbols.length;
      saveCustom(customLists().map(function (x) { return String(x.id) === String(L.id) ? own : x; }));
      L.symbols = own.symbols;
    } else {
      ui.extra[L.id] = ui.extra[L.id] || [];
      if (ui.extra[L.id].indexOf(s) < 0) ui.extra[L.id].push(s);
      if (ui.hide[L.id]) delete ui.hide[L.id][s];
    }
    var seq = baseSeq(L);
    if (seq.indexOf(s) < 0) seq.push(s);
    ui.order[L.id] = seq;
    saveUi(); paint();
    want(s);
  }
  function addSection(L, name, before) {
    name = String(name || "").trim();
    if (!name || !L || L.virtual) { if (L && L.virtual) toast("This view is read-only. Make a copy to edit it."); return; }
    var seq = baseSeq(L), line = "###" + name, at = before ? seq.indexOf(before) : -1;
    if (at < 0) seq.unshift(line); else seq.splice(at, 0, line);
    ui.sort = ""; persistSeq(L, seq);
  }
  function renameSection(L, oldName, newName) {
    var seq = baseSeq(L).map(function (s) { return s === "###" + oldName ? "###" + newName : s; });
    delete ui.collapsed[L.id + "|" + oldName];
    persistSeq(L, seq);
  }
  function createList(name) {
    name = String(name || "").trim(); if (!name) return;
    var arr = customLists();
    var id = "custom-" + Date.now();
    var color = FLAG_COLORS[arr.length % FLAG_COLORS.length];
    arr.unshift({ id: id, name: name, symbols: [], n: 0, custom: 1, color: color, from: "chart" });
    saveCustom(arr);
    ui.active = id; saveUi(); paint(); closePop();
  }
  function renameList(L) {
    if (!L) return;
    var n = prompt("Rename list", (ui.alias[L.id] || L.name || "")); if (!n) return;
    var own = findCustom(L.id);
    if (own) {
      own.name = n;
      saveCustom(customLists().map(function (x) { return String(x.id) === String(L.id) ? own : x; }));
      L.name = n;
    } else ui.alias[L.id] = n;
    saveUi(); paint();
  }
  function duplicateList(L) {
    if (!L) return;
    var id = "custom-" + Date.now();
    var full = viewOf(L).full.slice();
    var syms = full.filter(function (s) { return String(s).indexOf("###") !== 0; });
    var arr = customLists();
    arr.unshift({ id: id, name: (L.name || "List") + " copy", symbols: syms, n: syms.length, custom: 1, color: L.color || "#2962ff" });
    saveCustom(arr);
    ui.active = id; ui.order[id] = full.slice();
    saveUi(); paint();
  }
  function clearList(L) {
    if (!L || L.virtual) return;
    if (!confirm("Clear " + (L.name || "this list") + "?")) return;
    var own = findCustom(L.id);
    if (own) { own.symbols = []; own.n = 0; saveCustom(customLists().map(function (x) { return String(x.id) === String(L.id) ? own : x; })); L.symbols = []; }
    else {
      ui.hide[L.id] = {};
      (L.symbols || []).forEach(function (s) { ui.hide[L.id][s] = 1; });
    }
    ui.order[L.id] = [];
    saveUi(); paint();
  }
  function restoreList(L) {
    if (!L) return;
    ui.hide[L.id] = {};
    ui.order[L.id] = [];
    saveUi(); paint();
  }
  function exportList(L) {
    if (!L) return;
    var text = exportBody(L);
    var name = (ui.alias && ui.alias[L.id]) || L.name || "watchlist";
    if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).catch(function () {});
    var a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([text], { type: "text/plain" }));
    a.download = String(name).replace(/[^\w\-]+/g, "_").slice(0, 40) + ".txt";
    document.body.appendChild(a); a.click(); a.remove();
    toast("Exported " + name);
  }
  function pop(id) { var n = document.getElementById(id); if (!n) { n = document.createElement("div"); n.id = id; document.body.appendChild(n); } return n; }
  function closePop() { ["tvmenu", "tvflags", "tvimp", "listdrop"].forEach(function (id) { var n = document.getElementById(id); if (n) n.className = ""; }); }
  function openFlags(x, y, s) {
    var box = pop("tvflags");
    box.className = "on";
    box.innerHTML = FLAG_COLORS.map(function (c) { return "<button type=button data-c='" + c + "' style='background:" + c + "'></button>"; }).join("") + "<button type=button data-c='' title='Clear' style='background:#2a2e39;color:#787b86'>\u00d7</button>";
    box.style.left = Math.max(8, x) + "px";
    box.style.top = Math.max(8, y) + "px";
    box.querySelectorAll("button").forEach(function (b) {
      b.onclick = function (e) { e.stopPropagation(); setFlag(s, b.getAttribute("data-c")); closePop(); paint(); };
    });
  }
  function tick(on) { return "<i>" + (on ? "\u2713" : "") + "</i>"; }
  function openMenu(x, y, sym) {
    var L = findList(ui.active); if (!L) return;
    var box = pop("tvmenu");
    box.className = "on";
    var colBtns = MET.filter(function (m) { return !m.table || ui.table; }).map(function (m) {
      return "<button type=button data-col='" + m.id + "'>" + tick(ui.cols[m.id]) + m.label + "</button>";
    }).join("");
    var flagsHtml = FLAG_COLORS.map(function (c) { return "<button type=button data-fc='" + c + "'><i style='background:" + c + ";width:10px;height:10px;border-radius:2px;display:inline-block'></i></button>"; }).join("");
    box.innerHTML = sym
      ? ("<button type=button data-act=copy>Copy " + esc(bare(sym)) + "</button><button type=button data-act=rm>Remove</button><button type=button data-act=sec>Add section above</button><button type=button data-act=fav>Add to favorites</button><button type=button data-act=alert>Add alert</button><div class=lab>FLAG</div>" + flagsHtml + "<button type=button data-fc=''>Clear flag</button>")
      : ("<div class=lab>SYMBOL</div><button type=button data-tog=logo>" + tick(ui.logo) + "Logo</button><button type=button data-tog=ticker>" + tick(ui.ticker) + "Ticker</button><button type=button data-tog=desc>" + tick(ui.desc) + "Description</button><div class=lab>COLUMNS</div>" + colBtns + "<div class=lab>VIEW</div><button type=button data-act=table>" + tick(ui.table) + "Table view</button><button type=button data-act=adv>" + tick(ui.adv) + "Advanced view</button><button type=button data-act=widths>Reset column widths</button><div class=lab>SORT</div><button type=button data-sort=''>" + tick(!ui.sort) + "Custom order</button><button type=button data-sort=sym>" + tick(ui.sort === "sym") + "Symbol</button><button type=button data-sort=last>" + tick(ui.sort === "last") + "Last</button><button type=button data-sort=chgp>" + tick(ui.sort === "chgp") + "Change %</button><button type=button data-sort=vol>" + tick(ui.sort === "vol") + "Volume</button>");
    box.style.left = Math.max(8, Math.min(x, window.innerWidth - 248)) + "px";
    box.style.top = Math.max(8, Math.min(y, window.innerHeight - 80)) + "px";
    box.onclick = function (e) {
      var t = e.target.closest ? e.target.closest("[data-act],[data-col],[data-tog],[data-sort],[data-fc]") : null;
      if (!t) return;
      e.stopPropagation();
      var act = t.getAttribute("data-act"), col = t.getAttribute("data-col"), tog = t.getAttribute("data-tog"), sk = t.getAttribute("data-sort"), fc = t.getAttribute("data-fc");
      if (fc != null && sym) { setFlag(sym, fc); closePop(); paint(); return; }
      if (col) { ui.cols[col] = ui.cols[col] ? 0 : 1; saveUi(); openMenu(x, y, sym); paint(); return; }
      if (tog) { ui[tog] = ui[tog] ? 0 : 1; saveUi(); openMenu(x, y, sym); paint(); return; }
      if (sk != null) { ui.sort = sk; ui.dir = sk === "sym" || !sk ? 1 : -1; saveUi(); closePop(); paint(); return; }
      if (act === "copy" && sym) exportText(sym);
      if (act === "rm" && sym) removeSym(L, sym);
      if (act === "sec") { var nm = prompt("Section name", "Section"); if (nm) addSection(L, nm, sym); }
      if (act === "fav" && sym) toggleFav(sym);
      if (act === "alert") askAlert(sym || "", L.id);
      if (act === "table") { ui.table = ui.table ? 0 : 1; if (ui.table) { ui.cols.w1 = ui.cols.m1 = ui.cols.m3 = ui.cols.ytd = ui.cols.y1 = ui.cols.avg = 1; } qSet = {}; saveUi(); }
      if (act === "adv") { ui.adv = ui.adv ? 0 : 1; if (ui.adv) qSet = {}; saveUi(); }
      if (act === "widths") { ui.widths = {}; saveUi(); }
      closePop(); paint();
    };
  }
  function exportText(s) {
    if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(s).catch(function () {});
  }
  function boxClearQuotes() { qSet = {}; }
  function showAdd() {
    var n = document.getElementById("tvadd");
    var list = document.getElementById("w-list");
    if (!n && list) {
      n = document.createElement("div"); n.id = "tvadd";
      n.innerHTML = "<input placeholder='Add symbol — AAPL, NASDAQ:NVDA, FRED:DGS10' />";
      list.appendChild(n);
      n.querySelector("input").onkeydown = function (e) {
        if (e.key === "Escape") { n.className = ""; return; }
        if (e.key !== "Enter") return;
        addSym(findList(ui.active), n.querySelector("input").value);
        n.querySelector("input").value = "";
      };
    }
    if (!n) return;
    n.className = "on";
    var inp = n.querySelector("input"); if (inp) inp.focus();
  }
  function fillDrop() {
    ensureFoot();
    var d = document.getElementById("listdrop"); if (!d) return;
    d.className = "on";
    var btn = document.getElementById("listbtn");
    if (btn) {
      var r = btn.getBoundingClientRect();
      d.style.left = Math.max(8, r.left) + "px";
      d.style.top = (r.bottom + 4) + "px";
      d.style.width = Math.max(280, r.width) + "px";
    }
    var q = document.getElementById("listq"); if (q) { q.value = ""; q.oninput = function () { renderDrop(q.value); }; q.focus(); }
    renderDrop("");
  }
  function renderDrop(q) {
    var box = document.getElementById("listres"); if (!box) return;
    q = String(q || "").toLowerCase();
    function row(L) {
      var c = listColorOf(L);
      var name = (ui.alias && ui.alias[L.id]) || L.name || L.id;
      var n = symCount(L.symbols || []);
      var star = ui.starred && ui.starred[L.id] ? " on" : "";
      return "<div class='ld-row" + (String(L.id) === String(ui.active) ? " on" : "") + "'><button type=button data-id='" + esc(L.id) + "'><i class=ld-flag style='background:" + esc(c) + "'></i><b>" + esc(name) + "</b><span>" + n + "</span></button><button type=button class='ld-star" + star + "' data-star='" + esc(L.id) + "'>\u2605</button></div>";
    }
    function block(title, rows) {
      var html = "", i, shown = 0;
      for (i = 0; i < rows.length; i++) {
        var L = rows[i]; if (!L) continue;
        var name = ((ui.alias && ui.alias[L.id]) || L.name || "");
        if (q && String(name).toLowerCase().indexOf(q) < 0 && String(L.id).toLowerCase().indexOf(q) < 0) continue;
        if (shown === 0) html += "<div class=ld-h>" + title + "</div>";
        if (shown >= 60 && q === "") break;
        html += row(L); shown++;
      }
      return html;
    }
    var starred = [], mine = customLists().slice(), cats = [];
    Object.keys(catalog).forEach(function (id) { cats.push(catalog[id]); });
    mine.concat(cats).forEach(function (L) { if (L && ui.starred && ui.starred[L.id]) starred.push(L); });
    var fav = findList("favorites");
    var flagsL = [];
    FLAG_COLORS.forEach(function (c) { var L = findList("flag:" + c); if (L && L.symbols.length) flagsL.push(L); });
    var html = "";
    if (fav && (fav.symbols || []).length) html += block("FAVORITES", [fav]);
    html += block("STARRED LISTS", starred);
    html += block("COLOR LISTS", flagsL);
    html += block("MY LISTS", mine.filter(function (L) { return !(ui.starred && ui.starred[L.id]); }));
    html += block("YOUR LISTS", cats.filter(function (L) { return !(ui.starred && ui.starred[L.id]); }));
    box.innerHTML = html || "<div style='padding:12px;color:#787b86'>No lists</div>";
    box.querySelectorAll("[data-id]").forEach(function (b) {
      b.onclick = function (e) { e.stopPropagation(); ui.active = b.getAttribute("data-id"); ui.flag = ""; saveUi(); var ld = document.getElementById("listdrop"); if (ld) ld.className = ""; closePop(); paint(); };
    });
    box.querySelectorAll("[data-star]").forEach(function (b) {
      b.onclick = function (e) { e.stopPropagation(); toggleStar(b.getAttribute("data-star")); };
    });
  }
  function remember(s, prev) {
    var box = document.getElementById("wlist"); if (!box || lock) { advUpdate(s); return; }
    var sel = (window.CSS && CSS.escape) ? CSS.escape(s) : s;
    var row = box.querySelector("[data-s='" + sel + "']");
    var q = quotes[s] || {};
    if (row) ["last", "chg", "chgp", "vol", "ext", "avg", "w1", "m1", "m3", "ytd", "y1", "rsi", "mom"].forEach(function (k) {
      var el = row.querySelector("[data-k='" + k + "']"); if (!el) return;
      el.textContent = fmtCell(k, q);
      el.className = cellClass(k, q);
      if (k === "last" && prev != null && prev !== q.last) el.className += q.last > prev ? " flash-up" : " flash-dn";
    });
    if (activeSym() === s || bare(activeSym()) === bare(s)) paintCard(s);
    advUpdate(s);
  }
  function takeBars(s, bars, rich) {
    if (!bars || bars.length < 2) return;
    var b = bars[bars.length - 1], a = bars[bars.length - 2];
    var last = +b.close, p = +a.close;
    if (!isFinite(last) || !isFinite(p) || !p) return;
    var prev = quotes[s] && quotes[s].last;
    var q = quotes[s] || {};
    q.last = last; q.chg = (last - p) / p; q.chgv = last - p;
    q.vol = +b.value || q.vol; q.high = +b.high; q.low = +b.low; q.open = +b.open; q.prev = p;
    q.flag = flags()[s] || q.flag || "";
    q.span = bars.length;
    var closes = [], vols = [], i, hi = -Infinity, lo = Infinity, n52 = Math.min(bars.length, 252);
    for (i = bars.length - n52; i < bars.length; i++) {
      var hiB = +bars[i].high, loB = +bars[i].low;
      if (isFinite(hiB) && hiB > hi) hi = hiB;
      if (isFinite(loB) && loB > 0 && loB < lo) lo = loB;
    }
    if (bars.length >= 20 && isFinite(hi) && isFinite(lo)) { q.h52 = hi; q.l52 = lo; }
    for (i = 0; i < bars.length; i++) {
      var c = +bars[i].close; if (isFinite(c)) closes.push(c);
      if (i >= bars.length - 10) { var v = +bars[i].value; if (v > 0) vols.push(v); }
    }
    function back(n) { if (closes.length <= n) return null; var x = closes[closes.length - 1 - n]; return x ? last / x - 1 : null; }
    if (closes.length > 5) q.w1 = back(5);
    if (closes.length > 21) q.m1 = back(21);
    if (closes.length > 63) q.m3 = back(63);
    if (closes.length > 252) q.y1 = back(252);
    var y = new Date((b.time > 1e12 ? b.time : b.time * 1000)).getUTCFullYear();
    for (i = 0; i < bars.length; i++) {
      var ts = bars[i].time > 1e12 ? bars[i].time : bars[i].time * 1000;
      if (new Date(ts).getUTCFullYear() === y && +bars[i].close) { q.ytd = last / (+bars[i].close) - 1; break; }
    }
    if (vols.length) q.avg = vols.reduce(function (x, z) { return x + z; }, 0) / vols.length;
    if (closes.length > 15) q.rsi = rsi(closes, 14);
    if (closes.length > 10) q.mom = last - closes[closes.length - 1 - 10];
    quotes[s] = q;
    checkAlerts(s, q);
    remember(s, prev);
  }
  function pump() {
    if (inflight >= 3 || !wait.length) return;
    var s = wait.shift(); inflight++;
    var t = tickerOf(s);
    var days = (ui.table || ui.adv) ? 420 : 12;
    if (!t) { inflight--; pump(); return; }
    fetch("https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=" + encodeURIComponent(t) + "&span=day&mult=1&days=" + days)
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) { takeBars(s, j && j.bars, ui.table); })
      .catch(function () {})
      .then(function () { inflight--; pump(); });
    if (ui.cols.ext || ui.adv) wantExt(s);
  }
  function want(s) {
    var need = (ui.table || ui.adv) ? 420 : 12;
    var hit = quotes[s];
    if (hit && hit.span >= need) { if (ui.cols.ext || ui.adv) wantExt(s); return; }
    var key = s + "@" + need;
    if (qSet[key] || !tickerOf(s)) return;
    qSet[key] = 1; wait.push(s); pump();
  }
  function nyMins(ts) {
    var ms = ts > 1e12 ? ts : ts * 1000;
    var parts = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).formatToParts(new Date(ms));
    var h = 0, m = 0;
    parts.forEach(function (p) { if (p.type === "hour") h = +p.value; if (p.type === "minute") m = +p.value; });
    return h * 60 + m;
  }
  function inRegular(ts) { var m = nyMins(ts); return m >= 9 * 60 + 30 && m < 16 * 60; }
  function takeExt(s, bars) {
    if (!bars || bars.length < 2) return;
    var i, lastReg = null, last = bars[bars.length - 1];
    for (i = bars.length - 1; i >= 0; i--) if (inRegular(bars[i].time)) { lastReg = bars[i]; break; }
    if (!lastReg || inRegular(last.time)) return;
    var base = +lastReg.close, ext = +last.close;
    if (!base || !isFinite(ext)) return;
    if (!quotes[s]) quotes[s] = {};
    var prev = quotes[s].ext;
    quotes[s].extTag = nyMins(last.time) < 9 * 60 + 30 ? "Pre" : "Post";
    quotes[s].ext = ext / base - 1;
    if (prev !== quotes[s].ext) remember(s, quotes[s].last);
  }
  function pumpExt() {
    if (einflight >= 2 || !ewait.length) return;
    var s = ewait.shift(); einflight++;
    var t = tickerOf(s);
    if (!t) { einflight--; pumpExt(); return; }
    fetch("https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=" + encodeURIComponent(t) + "&span=minute&mult=5&days=2")
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) { takeExt(s, j && j.bars); })
      .catch(function () {})
      .then(function () { einflight--; pumpExt(); });
  }
  function wantExt(s) { if (eSet[s] || !tickerOf(s)) return; eSet[s] = 1; ewait.push(s); pumpExt(); }
  function see() {
    var box = document.getElementById("wlist"); if (!box || !window.IntersectionObserver) return;
    if (see.io) see.io.disconnect();
    see.io = new IntersectionObserver(function (ents) {
      ents.forEach(function (en) { if (en.isIntersecting) { var s = en.target.getAttribute("data-s"); if (s) want(s); } });
    }, { root: box, rootMargin: "120px" });
    box.querySelectorAll(".wrow").forEach(function (n) { see.io.observe(n); });
  }
  var NOTES_KEY = "jh-tv-notes", ALERTS_KEY = "jh-tv-alerts";
  var ETF = {SPY:1,QQQ:1,IWM:1,DIA:1,GLD:1,SLV:1,TLT:1,IEF:1,HYG:1,LQD:1,EEM:1,VTI:1,VOO:1,IVV:1,IBIT:1,ETHA:1,BITO:1,SMH:1,XLF:1,XLE:1,XLK:1,XLV:1,XLY:1,XLP:1,XLI:1,XLB:1,XLU:1,XLRE:1,ARKK:1,VNQ:1,BND:1,TIP:1,SHY:1,EFA:1,EWJ:1,FXI:1,USO:1,SOXX:1,GDX:1,JNK:1,AGG:1,BIL:1,SGOV:1,VEA:1,VWO:1,MAGS:1};
  var CRYPTO = {BTC:1,ETH:1,SOL:1,XRP:1,DOGE:1,ADA:1,AVAX:1,LINK:1,BNB:1,LTC:1,DOT:1,BCH:1,UNI:1,ATOM:1,NEAR:1,APT:1,ARB:1,OP:1,SUI:1,TON:1,TRX:1,XLM:1,PEPE:1,SHIB:1};
  var TYPE_COLOR = {Stock:"#2962ff",ETF:"#089981",Crypto:"#f7931a",FX:"#ab47bc",Macro:"#787b86",Index:"#ff6d00",Other:"#434651"};
  var ADV = {price:["last","chg","chgp","vol","avg","ext"],perf:["w1","m1","m3","ytd","y1"],tech:["rsi","mom","chgp"]};

  function colW(id) {
    var w = ui.widths && ui.widths[id];
    if (w) return Math.max(36, Math.min(200, +w)) + "px";
    if (id === "sym") return "minmax(72px,1fr)";
    if (id === "last") return "72px";
    if (id === "vol" || id === "avg") return "64px";
    if (id === "ext") return "74px";
    return "58px";
  }

  function applyWidths() {
    var g = gridCss();
    var cols = document.getElementById("cols");
    if (cols) cols.style.gridTemplateColumns = g;
    document.querySelectorAll("#wlist .wrow").forEach(function (r) { r.style.gridTemplateColumns = g; });
  }

  function kindOf(s) {
    var e = exch(s).toUpperCase(), b = bare(s).toUpperCase(), ck;
    if (/^(FRED|ECB|BOJ|OECD|BIS|NYFED|CENSUS|BEA|BLS|EIA|USDA)$/.test(e)) return "Macro";
    if (/CRYPTO|BINANCE|COINBASE|BITSTAMP|KRAKEN|BYBIT/.test(e) || CRYPTO[b]) return "Crypto";
    for (ck in CRYPTO) { if (b.indexOf(ck) === 0 && b.length > ck.length && b.length <= ck.length + 5) return "Crypto"; }
    if (ETF[b]) return "ETF";
    if (/^[A-Z]{6}$/.test(b) && /^(USD|EUR|JPY|GBP|CHF|AUD|CAD|NZD)(USD|EUR|JPY|GBP|CHF|AUD|CAD|NZD)$/.test(b) && b.slice(0, 3) !== b.slice(3)) return "FX";
    if (/INDEX|TVC/.test(e) || /^(SPX|NDX|DJI|RUT|VIX|DXY)$/.test(b)) return "Index";
    if (e || /^[A-Z][A-Z0-9.\-]{0,9}$/.test(b)) return "Stock";
    return "Other";
  }

  function descOf(s) {
    var e = exch(s), k = kindOf(s);
    if (e && k) return e + " \u00b7 " + k;
    return e || k || "";
  }

  function listColorOf(L) {
    if (!L) return "#787b86";
    if (ui.listColor && ui.listColor[L.id]) return ui.listColor[L.id];
    if (L.color && String(L.color).charAt(0) === "#") return L.color;
    return "#787b86";
  }

  function rsi(closes, n) {
    n = n || 14;
    if (!closes || closes.length < n + 1) return null;
    var ag = 0, al = 0, i, d;
    for (i = 1; i <= n; i++) { d = closes[i] - closes[i - 1]; if (d >= 0) ag += d; else al -= d; }
    ag /= n; al /= n;
    for (i = n + 1; i < closes.length; i++) {
      d = closes[i] - closes[i - 1];
      ag = (ag * (n - 1) + (d > 0 ? d : 0)) / n;
      al = (al * (n - 1) + (d < 0 ? -d : 0)) / n;
    }
    if (!(al > 0)) return ag > 0 ? 100 : null;
    return 100 - 100 / (1 + ag / al);
  }

  function parseImport(text) {
    var seq = [], i, parts = String(text || "").split(/[\n,]+/);
    for (i = 0; i < parts.length; i++) {
      var tok = parts[i].trim();
      if (!tok) continue;
      if (tok.indexOf("###") === 0) seq.push("###" + tok.replace(/^#+/, "").trim());
      else seq.push(tok.toUpperCase());
    }
    return seq;
  }

  function symCount(seq) {
    var n = 0, i;
    for (i = 0; i < seq.length; i++) if (String(seq[i]).indexOf("###") !== 0) n++;
    return n;
  }

  function toast(msg) {
    var n = document.getElementById("tvtoast");
    if (!n) { n = document.createElement("div"); n.id = "tvtoast"; document.body.appendChild(n); }
    n.textContent = msg; n.className = "on";
    clearTimeout(toast._t); toast._t = setTimeout(function () { n.className = ""; }, 6000);
  }

  function noteOf(s) { var n = read(NOTES_KEY, {}) || {}; return n[s] || ""; }

  function setNote(s, t) { var n = read(NOTES_KEY, {}) || {}; if (t) n[s] = t; else delete n[s]; write(NOTES_KEY, n); }

  function checkAlerts(s, q) {
    var arr = read(ALERTS_KEY, []); if (!Array.isArray(arr)) return;
    var changed = false;
    arr.forEach(function (a) {
      if (!a) return;
      var on = a.kind === "listpct" ? a.list === ui.active : a.sym === s;
      if (!on) return;
      if (a.kind === "px") {
        if (q.last == null || a.done) return;
        var side = q.last >= a.value ? "above" : "below";
        if (a.armed == null) { a.armed = side; changed = true; return; }
        if (side !== a.armed && ((a.op === ">" && side === "above") || (a.op === "<" && side === "below"))) {
          a.done = 1; changed = true; toast(bare(s) + " crossed " + a.op + " " + a.value + " (" + num(q.last) + ")");
        }
        a.armed = side;
      } else if (q.chg != null) {
        a.hit = a.hit || {};
        if (a.hit[s]) return;
        if (Math.abs(q.chg) * 100 >= a.value) { a.hit[s] = 1; changed = true; toast(bare(s) + " " + pct(q.chg) + " \u2014 alert"); }
      }
    });
    if (changed) write(ALERTS_KEY, arr);
  }

  function askAlert(sym, listId) {
    var raw = prompt(sym ? "Alert for " + bare(sym) + " \u2014 use > 200, < 50, or % 3" : "Alert when any symbol on this list moves \u2014 use % 2", sym ? "> " : "% 2");
    if (!raw) return;
    raw = String(raw).trim();
    var a = { id: Date.now() }, m;
    if ((m = raw.match(/^%\s*([0-9.]+)/))) { a.kind = sym ? "pct" : "listpct"; a.value = +m[1]; if (sym) a.sym = sym; else a.list = listId; }
    else if ((m = raw.match(/^([<>])\s*([0-9.]+)/))) { if (!sym) { toast("Right-click a symbol for a price alert"); return; } a.kind = "px"; a.op = m[1]; a.value = +m[2]; a.sym = sym; }
    else { toast("Use > 200, < 50, or % 3"); return; }
    var arr = read(ALERTS_KEY, []); if (!Array.isArray(arr)) arr = [];
    arr.push(a); write(ALERTS_KEY, arr); toast("Alert saved on this browser");
  }

  function fmtCell(k, q) {
    q = q || {};
    if (k === "last") return q.last == null ? "\u2014" : num(q.last);
    if (k === "chg") return q.last == null ? "\u2014" : (q.chgv >= 0 ? "+" : "") + num(q.chgv);
    if (k === "chgp") return q.last == null ? "\u2014" : pct(q.chg);
    if (k === "vol") return vol(q.vol);
    if (k === "avg") return vol(q.avg);
    if (k === "rsi") return q.rsi == null ? "\u2014" : Number(q.rsi).toFixed(1);
    if (k === "mom") return q.mom == null ? "\u2014" : num(q.mom);
    if (k === "ext") return q.ext == null ? "\u2014" : ((q.extTag ? q.extTag + " " : "") + pct(q.ext));
    return q[k] == null ? "\u2014" : pct(q[k]);
  }

  function cellClass(k, q) {
    q = q || {};
    if (k === "vol" || k === "avg" || k === "rsi") return "";
    if (k === "last") return q.last == null ? "" : "px " + cls(q.chg);
    if (k === "chg") return q.chgv == null ? "" : cls(q.chgv);
    if (k === "chgp") return q.chg == null ? "" : cls(q.chg);
    if (k === "ext") return q.ext == null ? "" : "ext " + cls(q.ext);
    if (k === "mom") return q.mom == null ? "" : cls(q.mom);
    if (q[k] == null) return "";
    return cls(q[k]);
  }

  function metricNum(s, k) {
    var q = quotes[s] || {};
    if (k === "last") return q.last;
    if (k === "chg") return q.chgv;
    if (k === "chgp") return q.chg;
    if (k === "vol") return q.vol;
    if (k === "avg") return q.avg;
    if (k === "ext") return q.ext;
    if (k === "rsi") return q.rsi;
    if (k === "mom") return q.mom;
    return q[k];
  }

  function deleteSection(L, name) {
    if (!L || L.virtual) return;
    var seq = baseSeq(L).filter(function (x) { return x !== "###" + name; });
    delete ui.collapsed[L.id + "|" + name];
    persistSeq(L, seq);
  }

  function setListColor(L, c) {
    if (!L) return;
    ui.listColor[L.id] = c || "";
    var own = findCustom(L.id);
    if (own) {
      own.color = c || "";
      saveCustom(customLists().map(function (x) { return String(x.id) === String(L.id) ? own : x; }));
      L.color = c;
    }
    saveUi(); paint();
  }

  function toggleStar(id) {
    ui.starred[id] = ui.starred[id] ? 0 : 1;
    saveUi();
    var d = document.getElementById("listdrop");
    if (d && d.classList.contains("on")) renderDrop((document.getElementById("listq") || {}).value || "");
    paintButton(findList(ui.active));
  }

  function toggleFav(s) {
    var fav = read("jh-chart-favs", []);
    if (!Array.isArray(fav)) fav = [];
    var i = fav.indexOf(s);
    if (i >= 0) fav.splice(i, 1); else fav.push(s);
    write("jh-chart-favs", fav);
    toast(i >= 0 ? "Removed from favorites" : "Added to favorites");
  }

  function exportBody(L) {
    var seq = viewOf(L).full, lines = [], buf = [];
    function flush() { if (buf.length) { lines.push(buf.join(",")); buf = []; } }
    seq.forEach(function (x) { if (String(x).indexOf("###") === 0) { flush(); lines.push(x); } else buf.push(x); });
    flush();
    return lines.join("\n");
  }

  function applyImport(seq, asNew) {
    if (!seq || !seq.length) { toast("Nothing to import"); return; }
    if (asNew) createList("Imported");
    var L = findList(ui.active);
    if (!L || L.virtual) { toast("Open a list you can edit, or import as a new list."); return; }
    var syms = seq.filter(function (x) { return String(x).indexOf("###") !== 0; });
    var own = findCustom(L.id);
    if (own) {
      syms.forEach(function (x) { if ((own.symbols || []).indexOf(x) < 0) own.symbols.push(x); });
      own.n = own.symbols.length;
      saveCustom(customLists().map(function (x) { return String(x.id) === String(L.id) ? own : x; }));
      L.symbols = own.symbols;
    } else {
      ui.extra[L.id] = ui.extra[L.id] || [];
      syms.forEach(function (x) {
        if ((L.symbols || []).indexOf(x) < 0 && ui.extra[L.id].indexOf(x) < 0) ui.extra[L.id].push(x);
        if (ui.hide[L.id]) delete ui.hide[L.id][x];
      });
    }
    var order = baseSeq(L);
    seq.forEach(function (x) { if (order.indexOf(x) < 0) order.push(x); });
    ui.order[L.id] = order; ui.sort = "";
    saveUi(); paint();
    toast("Imported " + syms.length + " symbols");
  }

  function openImport() {
    var box = pop("tvimp");
    box.className = "on";
    box.style.left = "16px"; box.style.top = "72px";
    box.innerHTML = "<div class=lab>IMPORT</div><p style='color:#787b86;font-size:12px;margin:6px 0'>Comma or one symbol per line. A ### line is a section. Symbols already saved are kept.</p><textarea id=tvimp-t placeholder='###TECH&#10;NASDAQ:NVDA,NASDAQ:AAPL'></textarea><div class=row><button type=button data-m=add>Add to this list</button><button type=button data-m=new>New list</button><button type=button data-m=file>From file</button></div>";
    box.onclick = function (e) {
      var b = e.target.closest ? e.target.closest("[data-m]") : null;
      if (!b) return;
      e.stopPropagation();
      var mode = b.getAttribute("data-m");
      if (mode === "file") {
        var inp = document.createElement("input"); inp.type = "file"; inp.accept = ".txt,text/plain";
        inp.onchange = function () {
          var f = inp.files && inp.files[0]; if (!f) return;
          var rd = new FileReader();
          rd.onload = function () { applyImport(parseImport(String(rd.result || "")), false); box.className = ""; };
          rd.readAsText(f);
        };
        inp.click(); return;
      }
      applyImport(parseImport(box.querySelector("textarea").value), mode === "new");
      box.className = "";
    };
  }

  function openSectionMenu(x, y, L, name) {
    var box = pop("tvmenu");
    box.className = "on";
    box.style.left = Math.max(8, x) + "px"; box.style.top = Math.max(8, y) + "px";
    box.innerHTML = "<button type=button data-act=ren>Rename section</button><button type=button data-act=del>Remove section</button><button type=button data-act=add>Add symbol</button>";
    box.onclick = function (e) {
      var t = e.target.closest ? e.target.closest("[data-act]") : null;
      if (!t) return;
      e.stopPropagation();
      var act = t.getAttribute("data-act");
      if (act === "ren") { var n = prompt("Rename section", name); if (n) renameSection(L, name, n); }
      if (act === "del") deleteSection(L, name);
      if (act === "add") showAdd();
      closePop();
    };
  }

  function ensureFoot() {
    var d = document.getElementById("listdrop"); if (!d || document.getElementById("ld-foot")) return;
    var f = document.createElement("div"); f.id = "ld-foot";
    f.innerHTML = "<button type=button data-foot=new>Create new list</button><button type=button data-foot=ren>Rename</button><button type=button data-foot=dup>Make a copy</button><button type=button data-foot=sec>Add section</button><button type=button data-foot=clear>Clear list</button><button type=button data-foot=imp>Import</button><button type=button data-foot=exp>Export</button><button type=button data-foot=alert>List alert</button><button type=button data-foot=color>List color</button><button type=button data-foot=star>Star this list</button><button type=button data-foot=restore>Restore order</button>";
    d.appendChild(f);
    f.onclick = function (e) {
      var b = e.target.closest ? e.target.closest("[data-foot]") : null; if (!b) return;
      e.stopPropagation();
      var L = findList(ui.active), act = b.getAttribute("data-foot");
      if (act === "new") { var nn = prompt("New list name", "My list"); if (nn) createList(nn); }
      if (act === "ren") renameList(L);
      if (act === "dup") duplicateList(L);
      if (act === "sec") { var nm = prompt("Section name", "Section"); if (nm) addSection(L, nm); }
      if (act === "clear") clearList(L);
      if (act === "imp") openImport();
      if (act === "exp") exportList(L);
      if (act === "alert") askAlert("", L && L.id);
      if (act === "color") openPalette(e.clientX, e.clientY, function (c) { setListColor(L, c); });
      if (act === "star" && L) toggleStar(L.id);
      if (act === "restore") restoreList(L);
      if (act !== "color" && act !== "star" && act !== "imp") { var ld = document.getElementById("listdrop"); if (ld) ld.className = ""; }
    };
  }

  function openPalette(x, y, cb) {
    var box = pop("tvflags");
    box.className = "on";
    box.innerHTML = FLAG_COLORS.map(function (c) { return "<button type=button data-c='" + c + "' style='background:" + c + "'></button>"; }).join("") + "<button type=button data-c='' title='Clear' style='background:#2a2e39;color:#787b86'>\u00d7</button>";
    box.style.left = Math.max(8, x) + "px"; box.style.top = Math.max(8, y) + "px";
    box.querySelectorAll("button").forEach(function (b) { b.onclick = function (e) { e.stopPropagation(); cb(b.getAttribute("data-c") || ""); closePop(); }; });
  }

  function ensureChrome() {
    var head = document.querySelector("#watch .whead");
    var bar = document.getElementById("tvfbar");
    if (head && !bar) {
      bar = document.createElement("div"); bar.id = "tvfbar";
      head.insertAdjacentElement("afterend", bar);
      bar.onclick = function (e) {
        var b = e.target.closest ? e.target.closest("[data-f]") : null; if (!b) return;
        ui.flag = b.getAttribute("data-f") || ""; saveUi(); paint();
      };
    }
    if (bar) {
      var html = "<button type=button data-f=''>All</button>";
      FLAG_COLORS.forEach(function (c) { html += "<button type=button data-f='" + c + "' title='Color list' style='background:" + c + "'></button>"; });
      if (bar.dataset.h !== html) { bar.dataset.h = html; bar.innerHTML = html; }
      bar.querySelectorAll("[data-f]").forEach(function (b) { b.classList.toggle("on", (b.getAttribute("data-f") || "") === (ui.flag || "")); });
    }
    var ops = document.querySelector("#watch .whead .wops");
    if (ops && !document.getElementById("tv-pie")) {
      var pie = document.createElement("button");
      pie.id = "tv-pie"; pie.type = "button"; pie.title = "Advanced view"; pie.setAttribute("aria-label", "Advanced view"); pie.textContent = "\u25CE";
      pie.onclick = function (e) { e.preventDefault(); e.stopPropagation(); ui.adv = ui.adv ? 0 : 1; if (ui.adv) qSet = {}; saveUi(); paint(); };
      var menu = document.getElementById("w-menu");
      if (menu) ops.insertBefore(pie, menu); else ops.appendChild(pie);
    }
  }

  function advGroups(seq) {
    if (!ui.group || ui.group === "none") return [["", seq.filter(function (x) { return String(x).indexOf("###") !== 0; })]];
    if (ui.group === "section") {
      var out = [], cur = "General", bucket = [];
      function flush() { if (bucket.length) out.push([cur, bucket]); bucket = []; }
      seq.forEach(function (x) { if (String(x).indexOf("###") === 0) { flush(); cur = String(x).replace(/^#+/, "") || "Section"; } else bucket.push(x); });
      flush(); return out.length ? out : [["", []]];
    }
    var map = {}, order = [];
    seq.forEach(function (x) {
      if (String(x).indexOf("###") === 0) return;
      var k = ui.group === "exch" ? (exch(x) || "\u2014") : kindOf(x);
      if (!map[k]) { map[k] = []; order.push(k); }
      map[k].push(x);
    });
    return order.map(function (k) { return [k, map[k]]; });
  }

  function pieStyle(pairs) {
    var tot = 0, i; for (i = 0; i < pairs.length; i++) tot += pairs[i][1];
    if (!tot) return "background:#2a2e39";
    var acc = 0, stops = [];
    pairs.forEach(function (p) { var a = acc; acc += p[1] / tot * 100; stops.push(p[0] + " " + a.toFixed(2) + "% " + acc.toFixed(2) + "%"); });
    return "background:conic-gradient(" + stops.join(",") + ")";
  }

  function fmtSum(k, v) {
    if (v == null || !isFinite(v)) return "\u2014";
    if (k === "vol" || k === "avg") return vol(v);
    if (k === "rsi" || k === "last" || k === "chg" || k === "mom") return k === "rsi" ? v.toFixed(1) : num(v);
    return pct(v);
  }

  function renderAdv() {
    var watch = document.getElementById("watch");
    var host = document.getElementById("tvadv");
    if (!ui.adv) { if (host) host.className = ""; return; }
    if (!host && watch) { host = document.createElement("div"); host.id = "tvadv"; watch.appendChild(host); }
    if (!host) return;
    var L = findList(ui.active);
    host.className = "on";
    if (!L) { host.innerHTML = "<div class=ah><b>Watchlist</b><button type=button id=tvadv-x>Close</button></div>"; return; }
    var full = viewOf(L).full;
    var keys = ADV[ui.advTab] || ADV.price;
    var sig = [L.id, ui.advTab, ui.group, ui.summary ? 1 : 0, full.join("|")].join("~");
    if (host.dataset.sig === sig && host.querySelector("table")) { return; }
    var sc = host.querySelector(".advsc"); var top = sc ? sc.scrollTop : 0;
    var groups = advGroups(full);
    var syms = [];
    groups.forEach(function (g) { g[1].forEach(function (x) { if (String(x).indexOf("###") !== 0) syms.push(x); }); });
    var head = "<th>Symbol</th>" + keys.map(function (k) { var m = MET.filter(function (x) { return x.id === k; })[0]; return "<th>" + (m ? m.label : k) + "</th>"; }).join("");
    var body = "";
    groups.forEach(function (g) {
      if (g[0]) body += "<tr class=gh><td colspan='" + (keys.length + 1) + "'>" + esc(g[0]) + "  " + g[1].length + "</td></tr>";
      g[1].forEach(function (s) {
        if (String(s).indexOf("###") === 0) return;
        var q = quotes[s] || {};
        body += "<tr data-s='" + esc(s) + "'><td>" + (ui.logo ? logoHtml(s) : "") + " " + esc(bare(s)) + "</td>" + keys.map(function (k) { return "<td data-k='" + k + "' class='" + cellClass(k, q) + "'>" + fmtCell(k, q) + "</td>"; }).join("") + "</tr>";
      });
    });
    if (ui.summary) {
      ["min", "max", "avg", "med"].forEach(function (which) {
        body += "<tr class=gh><td>" + which.toUpperCase() + "</td>";
        keys.forEach(function (k) {
          var vals = [];
          syms.forEach(function (s) { var v = metricNum(s, k); if (typeof v === "number" && isFinite(v)) vals.push(v); });
          vals.sort(function (a, b) { return a - b; });
          var v = null;
          if (vals.length) {
            if (which === "min") v = vals[0];
            else if (which === "max") v = vals[vals.length - 1];
            else if (which === "med") v = vals[Math.floor((vals.length - 1) / 2)];
            else { var sum = 0; vals.forEach(function (n) { sum += n; }); v = sum / vals.length; }
          }
          body += "<td>" + fmtSum(k, v) + "</td>";
        });
        body += "</tr>";
      });
    }
    var types = {}, exs = {}, ti;
    syms.forEach(function (s) { var t = kindOf(s); types[t] = (types[t] || 0) + 1; var e = exch(s) || "\u2014"; exs[e] = (exs[e] || 0) + 1; });
    var tp = Object.keys(types).map(function (k) { return [TYPE_COLOR[k] || "#434651", types[k]]; });
    var palette = ["#2962ff", "#089981", "#f23645", "#ff6d00", "#ab47bc", "#fdd835", "#787b86"];
    var ep = Object.keys(exs).slice(0, 8).map(function (k, i) { return [palette[i % palette.length], exs[k]]; });
    var legendT = Object.keys(types).map(function (k) { return k + " " + types[k]; }).join(" \u00b7 ");
    var legendE = Object.keys(exs).slice(0, 8).map(function (k) { return k + " " + exs[k]; }).join(" \u00b7 ");
    var name = (ui.alias && ui.alias[L.id]) || L.name || "Watchlist";
    host.innerHTML = "<div class=ah><b>" + esc(name) + "</b><button type=button id=tvadv-x>Close</button></div>" +
      "<div class=tabs><button type=button data-tab=price>Price</button><button type=button data-tab=perf>Performance</button><button type=button data-tab=tech>Technicals</button></div>" +
      "<div class=tool><label>Group <select id=tvadv-g><option value=none>No group</option><option value=section>Sections</option><option value=exch>Exchange</option><option value=type>Symbol type</option></select></label><label><input id=tvadv-sum type=checkbox> Summary</label><button type=button id=tvadv-exp>Export</button></div>" +
      "<div class=advsc><table><thead><tr>" + head + "</tr></thead><tbody>" + body + "</tbody></table></div>" +
      "<div class=pies><div><div class=pie style='" + pieStyle(tp) + "'></div><div class=plab>Type \u00b7 " + esc(legendT) + "</div></div><div><div class=pie style='" + pieStyle(ep) + "'></div><div class=plab>Exchange \u00b7 " + esc(legendE) + "</div></div></div>" +
      "<div class=note>Built from the daily tape on this chart. EPS, dividends, market cap, and earnings dates are not on this tape, so they are not shown.</div>";
    host.dataset.sig = sig;
    host.querySelectorAll(".tabs button").forEach(function (b) { if (b.getAttribute("data-tab") === ui.advTab) b.classList.add("on"); });
    var sel = host.querySelector("#tvadv-g"); if (sel) sel.value = ui.group || "none";
    var sum = host.querySelector("#tvadv-sum"); if (sum) sum.checked = !!ui.summary;
    var sc2 = host.querySelector(".advsc"); if (sc2) sc2.scrollTop = top;
    host.querySelector("#tvadv-x").onclick = function () { ui.adv = 0; saveUi(); paint(); };
    host.querySelectorAll("[data-tab]").forEach(function (b) { b.onclick = function () { ui.advTab = b.getAttribute("data-tab"); saveUi(); renderAdv(); seeAdv(); }; });
    if (sel) sel.onchange = function () { ui.group = sel.value; saveUi(); renderAdv(); };
    if (sum) sum.onchange = function () { ui.summary = sum.checked ? 1 : 0; saveUi(); renderAdv(); };
    host.querySelector("#tvadv-exp").onclick = function () { exportList(L); };
    host.querySelectorAll("tbody tr[data-s]").forEach(function (tr) { tr.onclick = function () { openSym(tr.getAttribute("data-s")); }; });
    seeAdv();
  }

  function advUpdate(s) {
    var host = document.getElementById("tvadv"); if (!host || !host.classList.contains("on")) return;
    var sel = (window.CSS && CSS.escape) ? CSS.escape(s) : s;
    var row = host.querySelector("tr[data-s='" + sel + "']"); if (!row) return;
    var q = quotes[s] || {};
    row.querySelectorAll("[data-k]").forEach(function (el) {
      var k = el.getAttribute("data-k");
      el.textContent = fmtCell(k, q);
      el.className = cellClass(k, q);
    });
  }

  function seeAdv() {
    var box = document.querySelector("#tvadv .advsc"); if (!box || !window.IntersectionObserver) return;
    if (seeAdv.io) seeAdv.io.disconnect();
    seeAdv.io = new IntersectionObserver(function (ents) {
      ents.forEach(function (en) { if (en.isIntersecting) { var s = en.target.getAttribute("data-s"); if (s) want(s); } });
    }, { root: box, rootMargin: "160px" });
    box.querySelectorAll("tr[data-s]").forEach(function (n) { seeAdv.io.observe(n); });
  }

  function loadCat() {
    var f = flags();
    Object.keys(f).forEach(function (k) { quotes[k] = quotes[k] || {}; quotes[k].flag = f[k]; });
    fetch("/data/tv-watchlists.json?v=tvwatch2").then(function (r) { return r.json(); }).then(function (j) {
      (j.lists || []).forEach(function (L) { if (L && L.name) catalog[String(L.id || L.name)] = L; });
      if (!ui.active) {
        var c0 = customLists()[0];
        ui.active = c0 ? String(c0.id) : (Object.keys(catalog)[0] || "");
        saveUi();
      }
      paint();
    }).catch(function () { paint(); });
  }
  function hook() {
    var box = document.getElementById("wlist");
    var btn = document.getElementById("listbtn");
    if (!box || !btn) return false;
    if (!box.dataset.tvw2) {
      box.dataset.tvw2 = "1";
      new MutationObserver(function () { if (!lock) paint(); }).observe(box, { childList: true });
      btn.onclick = function (e) { e.preventDefault(); e.stopPropagation(); var d = document.getElementById("listdrop"); if (d && d.classList.contains("on")) { d.className = ""; return; } fillDrop(); };
      var menu = document.getElementById("w-menu");
      if (menu) menu.onclick = function (e) { e.preventDefault(); e.stopPropagation(); var r = menu.getBoundingClientRect(); openMenu(r.left, r.bottom + 4, ""); };
      var add = document.getElementById("addsym");
      if (add) add.onclick = function (e) { e.preventDefault(); e.stopPropagation(); showAdd(); };
      var neu = document.getElementById("w-new");
      if (neu) { neu.title = "Add symbol"; neu.setAttribute("aria-label", "Add symbol"); neu.onclick = function (e) { e.preventDefault(); e.stopPropagation(); showAdd(); }; }
      document.addEventListener("click", function (e) {
        if (e.target.closest && e.target.closest("#tvmenu,#tvflags,#tvimp,#listdrop,#listbtn,#w-menu,#tvfbar,#tv-pie,#tvadv")) return;
        closePop();
        var ld = document.getElementById("listdrop"); if (ld) ld.className = "";
      });
    }
    paint();
    return true;
  }
  document.addEventListener("keydown", function (e) {
    var tag = (e.target && e.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
    if (e.key === "Escape") {
      var open = document.querySelector("#tvmenu.on,#tvflags.on,#listdrop.on,#tvimp.on");
      if (open) { closePop(); return; }
      if (ui.adv) { ui.adv = 0; saveUi(); paint(); }
      return;
    }
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp" && e.key !== "Delete" && e.key !== "Backspace") return;
    var w = document.getElementById("watch");
    if (!w || !w.classList.contains("is-open")) return;
    var rows = [].slice.call(document.querySelectorAll("#wlist .wrow"));
    if (!rows.length) return;
    var i = 0, k; for (k = 0; k < rows.length; k++) if (rows[k].classList.contains("on")) i = k;
    if (e.key === "Delete" || e.key === "Backspace") {
      e.preventDefault();
      removeSym(findList(ui.active), rows[i].getAttribute("data-s"));
      return;
    }
    if (e.altKey) {
      e.preventDefault();
      var Lalt = findList(ui.active); if (!Lalt || Lalt.virtual) return;
      var salt = rows[i].getAttribute("data-s");
      var seq = baseSeq(Lalt), at = seq.indexOf(salt), to = e.key === "ArrowUp" ? at - 1 : at + 1;
      if (at < 0 || to < 0 || to >= seq.length) return;
      var tmp = seq[to]; seq[to] = seq[at]; seq[at] = tmp;
      ui.sort = ""; persistSeq(Lalt, seq); return;
    }
    i = e.key === "ArrowDown" ? Math.min(rows.length - 1, i + 1) : Math.max(0, i - 1);
    e.preventDefault();
    rows[i].scrollIntoView({ block: "nearest" });
    openSym(rows[i].getAttribute("data-s"));
  });
  loadCat();
  var n = 0, timer = setInterval(function () { if (hook() || ++n > 50) clearInterval(timer); }, 250);
})();
