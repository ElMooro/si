/* Inject Demand / Supply buttons next to Alert. Self-contained:
   works even if the engine HTML patch is not deployed yet.

   2026-11-26, Khalid: "remove the demand, supply icons from there bcs they arent serving no purpose they dont belong
   there" - the two icon buttons (D Demand, S Supply) that sat among the desk buttons on the bar are no longer made.
   The zones are still switched by the Demand and Supply text buttons beside Alert and by the D / S pair on the right
   rail; nothing else changed. To bring the icons back, restore this file and the loader's ?v= from the commit before. */
(function (root) {
  if (root.__jhSdUiV3) return;
  root.__jhSdUiV3 = true;

  var SHOW_KEY = "jh-sd-show";

  function css() {
    if (document.getElementById("jh-sd-ui-css")) return;
    var s = document.createElement("style");
    s.id = "jh-sd-ui-css";
    s.textContent =
      /* 2026-11-26: while a desk is open in the workspace, the chart's own share-count note does not float over it */
      "body:has(#ws-overlay.on) #jh-capital-structure-status{display:none}" +
      "#rrail #btn-demand-rail.on{color:#089981;background:#2a2e39}" +
      "#rrail #btn-supply-rail.on{color:#f23645;background:#2a2e39}" +
      "#tfbar #btn-demand2.on{color:#089981}" +
      "#tfbar #btn-supply2.on{color:#f23645}";
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

  function study() {
    try {
      var inds = root.INDS || (root.window && root.window.INDS);
      if (inds && inds.length) {
        for (var i = 0; i < inds.length; i++) if (inds[i] && inds[i].id === "sdmd") return inds[i];
      }
    } catch (e) {}
    return null;
  }
  /* The Demand and Supply buttons switch the Supply & Demand study on and off. */
  function enableStudy() {
    var s = readShow(), row = study();
    if (row) { row.on = !!(s.demand || s.supply); if (row.on) row.hide = false; }
    linked = !!row;
  }
  /* 2026-11-23: this used to run on every change to the page and push the buttons' state onto the study each time, so the
     cross on the study's legend row put it straight back and the Indicators list could never switch it on. Now the buttons
     set the study once when the page opens and when one is pressed; after that a change made anywhere else (the legend's
     cross, the Indicators list) is what the buttons show. */
  var linked = false;
  function follow() {
    var row = study();
    if (!row) return;
    if (!linked) { enableStudy(); return; }
    var s = readShow(), want = !!(s.demand || s.supply), on = !!row.on;
    if (on === want) return;
    writeShow(on ? { demand: true, supply: true } : { demand: false, supply: false });
  }

  function sync() {
    var s = readShow();
    ["btn-demand2", "btn-demand-rail"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.classList.toggle("on", !!s.demand);
    });
    ["btn-supply2", "btn-supply-rail"].forEach(function (id) {
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
    var al = document.getElementById("btn-al");
    ["btn-demand", "btn-supply"].forEach(function (id) {       /* an icon left by an older copy of this script */
      var old = document.getElementById(id);
      if (old && old.parentNode) old.parentNode.removeChild(old);
    });
    if (al && !document.getElementById("btn-demand2")) {
      var d2 = mkBtn("btn-demand2", "Demand zones", "Demand");
      var s2 = mkBtn("btn-supply2", "Supply zones", "Supply");
      insertBefore(d2, al);
      insertBefore(s2, al);
    }
    bind(document.getElementById("btn-demand2"), "demand");
    bind(document.getElementById("btn-supply2"), "supply");
  }

  function hook() {
    css();
    injectRail();
    injectTfbar();
    follow();
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
