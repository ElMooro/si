/* TV-style indicator list + watch hover drawer. Does not touch tv-watchlists.json. */
(function () {
  var KEY = "jh-chart-ind-layout-v1";
  function load() {
    try { return JSON.parse(localStorage.getItem(KEY) || "{}"); } catch (e) { return {}; }
  }
  function save(st) {
    try { localStorage.setItem(KEY, JSON.stringify(st)); } catch (e) {}
  }
  function applySaved() {
    if (!window.INDS && !document.getElementById("legend")) return false;
    var st = load();
    if (typeof INDS === "undefined") return false;
    INDS.forEach(function (i) {
      if (st[i.id]) {
        if (st[i.id].on != null) i.on = !!st[i.id].on;
        if (st[i.id].c) i.c = st[i.id].c;
      } else {
        i.on = false;
      }
    });
    if (typeof OSC !== "undefined") {
      OSC.forEach(function (o) {
        if (st["o:" + o.id] && st["o:" + o.id].on != null) o.on = !!st["o:" + o.id].on;
        else o.on = false;
      });
    }
    return true;
  }
  function snapshot() {
    var st = {};
    if (typeof INDS === "undefined") return;
    INDS.forEach(function (i) { st[i.id] = { on: !!i.on, c: i.c }; });
    if (typeof OSC !== "undefined") OSC.forEach(function (o) { st["o:" + o.id] = { on: !!o.on }; });
    save(st);
  }
  function panel() {
    var box = document.getElementById("ind-studio");
    if (!box) {
      box = document.createElement("div");
      box.id = "ind-studio";
      box.style.cssText = "position:fixed;right:12px;bottom:48px;width:280px;max-height:52vh;overflow:auto;background:var(--bg,#131722);border:1px solid var(--bd,#2a2e39);z-index:40;display:none;font-size:11px;padding:8px";
      document.body.appendChild(box);
    }
    if (typeof INDS === "undefined") { box.innerHTML = "engine not ready"; return; }
    var html = "<div style=display:flex;justify-content:space-between;align-items:center><b>Indicators</b><span style=opacity:.6>eye = on chart · color</span></div>";
    html += "<div style=margin:6px 0><button id=ind-none>Hide all</button> <button id=ind-tv>TV set</button></div>";
    INDS.forEach(function (i) {
      html += "<div style=display:flex;align-items:center;gap:6px;padding:2px 0>";
      html += "<button data-eye='" + i.id + "' title='show/hide'>" + (i.on ? "◉" : "○") + "</button>";
      html += "<input type=color data-col='" + i.id + "' value='" + (i.c && i.c[0] === "#" ? i.c : "#2962ff") + "' style=width:22px;height:18px;border:0;background:transparent>";
      html += "<span>" + i.n + "</span></div>";
    });
    if (typeof OSC !== "undefined") {
      html += "<div style=margin-top:8px;opacity:.6>Oscillators</div>";
      OSC.forEach(function (o) {
        html += "<div style=display:flex;gap:6px;align-items:center><button data-oeye='" + o.id + "'>" + (o.on ? "◉" : "○") + "</button><span>" + o.n + "</span></div>";
      });
    }
    box.innerHTML = html;
    box.querySelectorAll("[data-eye]").forEach(function (b) {
      b.onclick = function () {
        var i = INDS.find(function (x) { return x.id === b.getAttribute("data-eye"); });
        if (!i) return;
        i.on = !i.on;
        snapshot();
        if (window.lastBars && lastBars.length) paint(lastBars);
        panel();
      };
    });
    box.querySelectorAll("[data-col]").forEach(function (inp) {
      inp.oninput = function () {
        var i = INDS.find(function (x) { return x.id === inp.getAttribute("data-col"); });
        if (!i) return;
        i.c = inp.value;
        snapshot();
        if (window.lastBars && lastBars.length) paint(lastBars);
      };
    });
    box.querySelectorAll("[data-oeye]").forEach(function (b) {
      b.onclick = function () {
        var o = OSC.find(function (x) { return x.id === b.getAttribute("data-oeye"); });
        if (!o) return;
        o.on = !o.on;
        snapshot();
        if (window.lastBars && lastBars.length) paint(lastBars);
        panel();
      };
    });
    var n = document.getElementById("ind-none");
    if (n) n.onclick = function () {
      INDS.forEach(function (i) { i.on = false; });
      if (typeof OSC !== "undefined") OSC.forEach(function (o) { o.on = false; });
      snapshot();
      if (window.lastBars && lastBars.length) paint(lastBars);
      panel();
    };
    var tv = document.getElementById("ind-tv");
    if (tv) tv.onclick = function () {
      INDS.forEach(function (i) { i.on = /sma20|sma50|sma200|vwap/.test(i.id); });
      snapshot();
      if (window.lastBars && lastBars.length) paint(lastBars);
      panel();
    };
  }
  function boot() {
    applySaved();
    var btn = document.getElementById("fx");
    if (btn && !btn.dataset.studio) {
      btn.dataset.studio = "1";
      btn.addEventListener("click", function () {
        var box = document.getElementById("ind-studio");
        if (!box) panel();
        box = document.getElementById("ind-studio");
        box.style.display = box.style.display === "none" ? "block" : "none";
        if (box.style.display === "block") panel();
      });
    }
    var w = document.getElementById("watch");
    if (w && !w.dataset.drawer) {
      w.dataset.drawer = "1";
      w.title = "Hover edge to open watchlists — lists are not deleted";
    }
  }
  window.addEventListener("load", function () { setTimeout(boot, 600); setTimeout(applySaved, 1200); });
})();
