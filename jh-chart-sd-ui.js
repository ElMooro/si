/* Inject Demand / Supply buttons next to Alert. Self-contained:
   works even if the engine HTML patch is not deployed yet. */
(function (root) {
  if (root.__jhSdUiV3) return;
  root.__jhSdUiV3 = true;

  var SHOW_KEY = "jh-sd-show";

  function css() {
    if (document.getElementById("jh-sd-ui-css")) return;
    var s = document.createElement("style");
    s.id = "jh-sd-ui-css";
    s.textContent =
      "#rrail #btn-demand-rail.on{color:#089981;background:#2a2e39}" +
      "#rrail #btn-supply-rail.on{color:#f23645;background:#2a2e39}" +
      "#tfbar #btn-demand.on,#tfbar #btn-demand2.on{color:#089981}" +
      "#tfbar #btn-supply.on,#tfbar #btn-supply2.on{color:#f23645}" +
      "#tfbar #btn-demand,#tfbar #btn-supply{display:inline-flex!important}";
    document.head.appendChild(s);
  }

  function readShow() {
    var s = root.__jhSdShow;
    if (s && typeof s === "object") return { demand: !!s.demand, supply: !!s.supply };
    try {
      var raw = root.localStorage && root.localStorage.getItem(SHOW_KEY);
      if (raw) {
        var p = JSON.parse(raw);
        if (p && typeof p === "object") return { demand: !!p.demand, supply: !!p.supply };
      }
    } catch (e) {}
    return { demand: false, supply: false };
  }
  function writeShow(next) {
    root.__jhSdShow = { demand: !!next.demand, supply: !!next.supply };
    try { if (root.localStorage) root.localStorage.setItem(SHOW_KEY, JSON.stringify(root.__jhSdShow)); } catch (e) {}
    return root.__jhSdShow;
  }

  function enableStudy() {
    var s = readShow();
    var on = !!(s.demand || s.supply);
    try {
      var inds = root.INDS || (root.window && root.window.INDS);
      if (inds && inds.length) {
        for (var i = 0; i < inds.length; i++) {
          if (inds[i] && inds[i].id === "sdmd") { inds[i].on = on; break; }
        }
      }
    } catch (e) {}
  }

  function sync() {
    var s = readShow();
    ["btn-demand", "btn-demand2", "btn-demand-rail"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.classList.toggle("on", !!s.demand);
    });
    ["btn-supply", "btn-supply2", "btn-supply-rail"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.classList.toggle("on", !!s.supply);
    });
  }

  function toast(msg) {
    try {
      if (typeof root.jhToast === "function") return root.jhToast(msg);
      var el = document.getElementById("toast");
      if (el) { el.textContent = msg; el.className = "on"; setTimeout(function(){ el.className = ""; }, 1600); }
    } catch (e) {}
  }

  function toggle(side) {
    var s = readShow();
    s[side] = !s[side];
    writeShow(s);
    enableStudy();
    sync();
    try {
      var bars = root.lastBars;
      if (bars && bars.length && typeof root.paint === "function") root.paint(bars);
    } catch (e) {}
    toast(side === "demand" ? (s.demand ? "Demand zones on" : "Demand zones off") : (s.supply ? "Supply zones on" : "Supply zones off"));
  }

  function bind(el, side) {
    if (!el || el.__jhSdBound) return;
    el.__jhSdBound = true;
    el.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      if (side === "demand" && typeof root.jhToggleDemand === "function" && !root.jhToggleDemand.__jhSdUi) {
        root.jhToggleDemand();
      } else if (side === "supply" && typeof root.jhToggleSupply === "function" && !root.jhToggleSupply.__jhSdUi) {
        root.jhToggleSupply();
      } else {
        toggle(side);
      }
      sync();
    });
  }

  function mkBtn(id, title, label, extraClass) {
    var b = document.createElement("button");
    b.type = "button";
    b.id = id;
    b.title = title;
    if (extraClass) b.className = extraClass;
    b.innerHTML = label;
    return b;
  }

  function insertBefore(node, ref) {
    if (!node || !ref || !ref.parentNode) return;
    ref.parentNode.insertBefore(node, ref);
  }

  function injectRail() {
    var rail = document.getElementById("rrail");
    if (!rail) return;
    var alertBtn = rail.querySelector('[data-rail="alerts"]') || rail.querySelector('[title="Alerts"]');
    var d = document.getElementById("btn-demand-rail");
    var s = document.getElementById("btn-supply-rail");
    if (!d) {
      d = mkBtn("btn-demand-rail", "Demand zones", "D");
      if (alertBtn) insertBefore(d, alertBtn); else rail.appendChild(d);
    }
    if (!s) {
      s = mkBtn("btn-supply-rail", "Supply zones", "S");
      if (alertBtn) insertBefore(s, alertBtn); else rail.appendChild(s);
    }
    bind(d, "demand");
    bind(s, "supply");
  }

  function injectTfbar() {
    var bar = document.getElementById("tfbar");
    if (!bar) return;
    var alrt = document.getElementById("btn-alrt");
    var al = document.getElementById("btn-al");
    if (alrt && !document.getElementById("btn-demand")) {
      var d = mkBtn("btn-demand", "Demand zones · DBR / RBR", "<span class=g>D</span><span class=l>Demand</span>", "wsico wsdesk");
      var s = mkBtn("btn-supply", "Supply zones · RBD / DBD", "<span class=g>S</span><span class=l>Supply</span>", "wsico wsdesk");
      insertBefore(d, alrt);
      insertBefore(s, alrt);
    }
    if (al && !document.getElementById("btn-demand2")) {
      var d2 = mkBtn("btn-demand2", "Demand zones", "Demand");
      var s2 = mkBtn("btn-supply2", "Supply zones", "Supply");
      insertBefore(d2, al);
      insertBefore(s2, al);
    }
    bind(document.getElementById("btn-demand"), "demand");
    bind(document.getElementById("btn-supply"), "supply");
    bind(document.getElementById("btn-demand2"), "demand");
    bind(document.getElementById("btn-supply2"), "supply");
  }

  function hook() {
    css();
    injectRail();
    injectTfbar();
    enableStudy();
    sync();
  }

  root.jhToggleDemand = root.jhToggleDemand || function () { toggle("demand"); };
  root.jhToggleSupply = root.jhToggleSupply || function () { toggle("supply"); };
  root.jhToggleDemand.__jhSdUi = !root.jhToggleDemand.__jhEngine;
  root.jhToggleSupply.__jhSdUi = !root.jhToggleSupply.__jhEngine;
  root.jhSdUiSync = sync;

  var n = 0;
  function wait() {
    hook();
    if (n++ < 60) setTimeout(wait, 250);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", wait);
  else wait();

  if (typeof MutationObserver !== "undefined") {
    var t = 0;
    try {
      new MutationObserver(function () {
        var now = Date.now();
        if (now - t < 80) return;
        t = now;
        hook();
      }).observe(document.documentElement, { childList: true, subtree: true });
    } catch (e) {}
  }
})(typeof window !== "undefined" ? window : globalThis);
