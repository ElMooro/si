/* Watch drawer + indicator studio. CSS injected so a stale chart.html cannot ignore it. */
(function () {
  var KEY = "jh-chart-ind-layout-v1";
  var css = document.createElement("style");
  css.id = "jh-studio-css-runtime";
  css.textContent = [
    ":root{--watch-w:320px;--list-h:240px}",
    ".watch,#watch{width:var(--watch-w,320px);min-width:180px;max-width:none;overflow:hidden;display:flex;flex-direction:column}",
    ".watch.hide,#watch.hide,#watch.is-collapsed{width:0!important;min-width:0!important;max-width:0!important;overflow:hidden!important;border:0!important}",
    "#watch.is-open{width:var(--watch-w,320px)!important;min-width:220px!important;max-width:none!important;overflow:hidden!important}",
    "@media(max-width:720px){.watch,#watch.is-open{width:100%!important;max-width:none!important;min-width:0!important}#rrail{display:none!important}}"
  ].join("");
  document.documentElement.appendChild(css);

  function load() { try { return JSON.parse(localStorage.getItem(KEY) || "{}"); } catch (e) { return {}; } }
  function save(st) { try { localStorage.setItem(KEY, JSON.stringify(st)); } catch (e) {} }
  function ensureTapeInds() {
    if (typeof INDS === "undefined") return;
    var extra = [
      { id: "tape", n: "Tape / Wyckoff", c: "#2962ff", on: false, k: "tape", cat: "Tape" },
      { id: "volev", n: "Vol events SC/DIST", c: "#f23645", on: false, k: "volev", cat: "Tape" },
      { id: "rs", n: "RS / beta vs SPY", c: "#089981", on: false, k: "rs", cat: "Tape" },
      { id: "macro", n: "Macro rail tags", c: "#ff9800", on: false, k: "macro", cat: "Tape" }
    ];
    extra.forEach(function (x) {
      if (!INDS.some(function (i) { return i.id === x.id; })) INDS.push(x);
    });
  }
  function applySaved() {
    if (typeof INDS === "undefined") return;
    ensureTapeInds();
    var st = load();
    var first = !Object.keys(st).length;
    INDS.forEach(function (i) {
      if (st[i.id]) {
        if (st[i.id].on != null) i.on = !!st[i.id].on;
        if (st[i.id].c) i.c = st[i.id].c;
      } else if (first) i.on = false;
    });
  }
  function snap() {
    if (typeof INDS === "undefined") return;
    var st = {};
    INDS.forEach(function (i) { st[i.id] = { on: !!i.on, c: i.c }; });
    save(st);
  }
  function tapeOn(id) {
    if (typeof INDS === "undefined") return false;
    var i = INDS.find(function (x) { return x.id === id; });
    return !!(i && i.on);
  }
  var _tr = window.jhTapeRead;
  window.jhTapeRead = function (d) {
    return _tr ? _tr(d) : { markers: [], panel: "" };
  };
  var _ve = window.jhVolEvents;
  window.jhVolEvents = function (d) {
    if (!tapeOn("volev")) return [];
    return _ve ? _ve(d) : [];
  };
  var _rs = window.jhRsReady;
  window.jhRsReady = function (d) {
    if (!tapeOn("rs")) return Promise.resolve([]);
    return _rs ? _rs(d) : Promise.resolve([]);
  };

  function panel() {
    if (typeof INDS === "undefined") return;
    ensureTapeInds();
    var box = document.getElementById("ind-studio");
    if (!box) {
      box = document.createElement("div");
      box.id = "ind-studio";
      box.style.cssText = "position:fixed;left:44px;top:86px;width:260px;max-height:60vh;overflow:auto;background:#131722;color:#d1d4dc;border:1px solid #2a2e39;z-index:50;padding:8px;font:11px/1.35 sans-serif";
      document.body.appendChild(box);
    }
    var html = "<div style=display:flex;justify-content:space-between><b>Indicators</b><button id=ind-x>x</button></div>";
    html += "<div style=margin:6px 0><button id=ind-none>Hide all</button></div>";
    INDS.forEach(function (i) {
      html += "<div style=display:flex;align-items:center;gap:6px>";
      html += "<button data-eye='" + i.id + "'>" + (i.on ? "◉" : "○") + "</button>";
      html += "<input type=color data-col='" + i.id + "' value='" + ((i.c && i.c[0] === "#") ? i.c : "#2962ff") + "' style=width:20px;height:16px;border:0>";
      html += "<span>" + i.n + "</span></div>";
    });
    box.innerHTML = html;
    box.querySelectorAll("[data-eye]").forEach(function (b) {
      b.onclick = function () {
        var i = INDS.find(function (x) { return x.id === b.getAttribute("data-eye"); });
        if (!i) return;
        i.on = !i.on; snap();
        if (window.lastBars && lastBars.length && window.paint) paint(lastBars);
        panel();
      };
    });
    box.querySelectorAll("[data-col]").forEach(function (inp) {
      inp.oninput = function () {
        var i = INDS.find(function (x) { return x.id === inp.getAttribute("data-col"); });
        if (!i) return;
        i.c = inp.value; snap();
        if (window.lastBars && lastBars.length && window.paint) paint(lastBars);
      };
    });
    document.getElementById("ind-none").onclick = function () {
      INDS.forEach(function (i) { i.on = false; }); snap();
      if (window.lastBars && lastBars.length && window.paint) paint(lastBars);
      panel();
    };
    document.getElementById("ind-x").onclick = function () { box.style.display = "none"; };
  }
  function boot() {
    applySaved();
    var fx = document.getElementById("fx");
    if (fx && !fx.dataset.st) {
      fx.dataset.st = "1";
      fx.addEventListener("click", function (e) {
        e.stopPropagation();
        panel();
        var box = document.getElementById("ind-studio");
        box.style.display = box.style.display === "none" ? "block" : (box.style.display ? "none" : "block");
      });
    }
  }
  window.addEventListener("load", function () {
    setTimeout(boot, 400);
    setTimeout(applySaved, 1500);
    setTimeout(function () {
      if (window.lastBars && lastBars.length && window.paint) paint(lastBars);
    }, 1600);
  });
})();
