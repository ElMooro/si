/* jh-nav-drawer.js — universal navigation drawer, present on every page.
   Closed by default; pull the edge handle (or press Ctrl/Cmd+B) to search or browse
   sideways across all pages, grouped by category. Picking a page just navigates —
   real <a href> links, no click interception. Degrades silently if the manifest
   can't be fetched. Namespaced (jhnav- prefix, no :root vars) so it can't collide
   with any existing page's own styles. */
(function () {
  "use strict";
  if (window.__jhNavDrawer) return; window.__jhNavDrawer = true;

  /* Amber Terminal theme + sitewide chrome (phase 2). /screener PROTECTED-excluded. */
  try {
    var _themed = location.pathname.indexOf("/screener") !== 0;
    if (_themed && !document.getElementById("jh-theme")) {
      var _jt = document.createElement("link");
      _jt.id = "jh-theme"; _jt.rel = "stylesheet"; _jt.href = "/jh-theme.css";
      (document.head || document.documentElement).prepend(_jt);
    }
    if (_themed && !document.querySelector("[data-jh-no-chrome]") && !document.getElementById("jh-chrome")) {
      var bar = document.createElement("div");
      bar.id = "jh-chrome"; bar.className = "jh-topbar";
      bar.innerHTML = '<a class="jhc-brand" href="/">JUSTHODL·AI</a>'
        + '<div class="jhc-tape" id="jhc-tape"></div>'
        + '<span class="jhc-clock" id="jhc-clock"></span>'
        + '<a class="jhc-cc" href="/">COMMAND CENTER →</a>';
      var mount = function () {
        document.body.insertBefore(bar, document.body.firstChild);
        var ck = document.getElementById("jhc-clock");
        var jt = function () { var dd = new Date();
          ck.textContent = dd.toLocaleTimeString("en-US", {hour12:false, timeZone:"America/New_York"}) + " ET"; };
        jt(); setInterval(jt, 1000);
        fetch("/data/market-tape.json?t=" + Date.now(), {cache:"no-store"})
          .then(function (r) { return r.json(); })
          .then(function (dd) { var tp = document.getElementById("jhc-tape"); if (!tp) return;
            (dd.items || []).forEach(function (it) {
              var sp = document.createElement("span"); sp.className = "jhc-chip";
              var cls = (typeof it.chg_pct === "number") ? (it.chg_pct >= 0 ? "jhc-up" : "jhc-dn") : "";
              var v = (it.display || "") + (typeof it.chg_pct === "number"
                ? ((it.chg_pct >= 0 ? " +" : " ") + it.chg_pct.toFixed(1) + "%") : "");
              sp.setAttribute("data-sym", it.label || "");
              sp.innerHTML = "<b>" + it.label + "</b> <span class=\"" + cls + "\">" + v + "</span>";
              tp.appendChild(sp); }); })
          .catch(function () {}); };
      if (document.body) mount(); else document.addEventListener("DOMContentLoaded", mount);
    }
  } catch (e) {}


  var CSS = ".jhnav-handle{position:fixed;top:50%;left:0;transform:translateY(-50%);width:22px;height:64px;background:#10151f;border:1px solid #1d2636;border-left:none;border-radius:0 10px 10px 0;display:flex;align-items:center;justify-content:center;cursor:pointer;z-index:999997;color:#5d6b82;font-size:15px;transition:left .22s cubic-bezier(.4,0,.2,1),color .15s,border-color .15s;padding:0;font-family:inherit;box-shadow:2px 0 10px rgba(0,0,0,.25)}"
    + ".jhnav-handle:hover{color:#22d3ee;border-color:#22d3ee}"
    + ".jhnav-handle.jhnav-open{left:280px}"
    + ".jhnav-backdrop{position:fixed;inset:0;background:rgba(4,6,10,.55);opacity:0;pointer-events:none;transition:opacity .2s ease;z-index:999996}"
    + ".jhnav-backdrop.jhnav-open{opacity:1;pointer-events:auto}"
    + ".jhnav-drawer{position:fixed;left:0;top:0;bottom:0;width:280px;background:#10151f;border-right:1px solid #1d2636;padding:16px 12px;overflow-y:auto;transform:translateX(-100%);transition:transform .22s cubic-bezier(.4,0,.2,1);z-index:999998;box-shadow:12px 0 34px rgba(0,0,0,.45);font-family:Inter,system-ui,sans-serif;box-sizing:border-box}"
    + ".jhnav-drawer *{box-sizing:border-box}"
    + ".jhnav-drawer.jhnav-open{transform:translateX(0)}"
    + ".jhnav-head{display:flex;align-items:center;gap:8px;margin-bottom:12px;padding:0 2px}"
    + ".jhnav-dot{width:8px;height:8px;background:#26ffaf;border-radius:50%;box-shadow:0 0 8px #26ffaf;flex-shrink:0}"
    + ".jhnav-brand{font-family:'IBM Plex Mono',monospace;font-weight:800;font-size:13px;color:#e8edf5}"
    + ".jhnav-brand b{color:#5d6b82;font-weight:600}"
    + ".jhnav-count{margin-left:auto;font-size:10.5px;color:#5d6b82;font-family:'IBM Plex Mono',monospace}"
    + ".jhnav-search{width:100%;background:#161d2a;border:1px solid #1d2636;border-radius:8px;padding:9px 11px;color:#e8edf5;font-size:12.5px;margin-bottom:12px;font-family:Inter,system-ui,sans-serif}"
    + ".jhnav-search:focus{outline:none;border-color:#22d3ee}"
    + ".jhnav-group{margin-bottom:2px}"
    + ".jhnav-ghead{display:flex;justify-content:space-between;align-items:center;padding:9px 8px;cursor:pointer;border-radius:7px;font-size:12.5px;color:#a8b3c7;user-select:none}"
    + ".jhnav-ghead:hover{background:#161d2a}"
    + ".jhnav-ghead .jhnav-n{color:#5d6b82;font-size:11px}"
    + ".jhnav-chev{transition:transform .15s;color:#5d6b82;font-size:10px;display:inline-block}"
    + ".jhnav-group.jhnav-gopen .jhnav-chev{transform:rotate(90deg)}"
    + ".jhnav-items{display:none;padding:2px 0 8px 14px}"
    + ".jhnav-group.jhnav-gopen .jhnav-items{display:block}"
    + ".jhnav-item{display:block;padding:6px 8px;font-size:12px;color:#5d6b82;border-radius:6px;text-decoration:none}"
    + ".jhnav-item:hover{color:#22d3ee;background:#161d2a}"
    + ".jhnav-item.jhnav-here{color:#26ffaf;font-weight:600}";

  var backdrop, handle, drawer, hereCatIndex = -1;

  function esc(s) { return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;"); }

  function open() {
    drawer.classList.add("jhnav-open"); backdrop.classList.add("jhnav-open");
    handle.classList.add("jhnav-open"); handle.textContent = "\u2039";
    handle.setAttribute("aria-label", "Close navigation");
    setTimeout(function () { var s = document.getElementById("jhnav-search"); if (s) s.focus(); }, 120);
  }
  function close() {
    drawer.classList.remove("jhnav-open"); backdrop.classList.remove("jhnav-open");
    handle.classList.remove("jhnav-open"); handle.textContent = "\u203a";
    handle.setAttribute("aria-label", "Open navigation");
  }
  function toggle() { drawer.classList.contains("jhnav-open") ? close() : open(); }

  var lastM = null;
  function getFavs(){ try { return JSON.parse(localStorage.getItem("jh_favs") || "[]"); } catch(e){ return []; } }
  function setFavs(a){ try { localStorage.setItem("jh_favs", JSON.stringify(a.slice(0, 80))); } catch(e){} }
  function toggleFav(h){ var f = getFavs(); var i = f.indexOf(h); if (i >= 0) f.splice(i, 1); else f.unshift(h); setFavs(f); }

  function render(m) {
    lastM = m;
    var here = (location.pathname || "/").replace(/\/$/, "") || "/";
    var groupsEl = document.getElementById("jhnav-groups");
    var countEl = document.getElementById("jhnav-count");
    if (countEl) countEl.textContent = m.n_pages + " pages";
    var favs = getFavs();
    var titleByHref = {};
    (m.categories || []).forEach(function (c) { (c.pages || []).forEach(function (p) { titleByHref[p.href] = p.title; }); });
    function itemHtml(p) {
      var isHere = p.href === here;
      var on = favs.indexOf(p.href) >= 0;
      return '<a class="jhnav-item' + (isHere ? " jhnav-here" : "") + '" href="' + esc(p.href) + '">'
        + '<span class="jhnav-t">' + esc(p.title) + (isHere ? " \u2014 you\u2019re here" : "") + '</span>'
        + '<span class="jhnav-star' + (on ? " on" : "") + '" data-fav="' + esc(p.href) + '" title="' + (on ? "Unfavorite" : "Favorite") + '">' + (on ? "\u2605" : "\u2606") + '</span></a>';
    }
    var html = "";
    var favPages = favs.filter(function (h) { return titleByHref[h]; }).map(function (h) { return { href: h, title: titleByHref[h] }; });
    if (favPages.length) {
      html += '<div class="jhnav-group jhnav-gopen jhnav-favgroup">'
        + '<div class="jhnav-ghead"><span>\u2b50 Favorites <span class="jhnav-n">' + favPages.length + '</span></span><span class="jhnav-chev">\u25b8</span></div>'
        + '<div class="jhnav-items">' + favPages.map(itemHtml).join("") + '</div></div>';
    }
    (m.categories || []).forEach(function (cat, i) {
      var hasHere = (cat.pages || []).some(function (p) { return p.href === here; });
      if (hasHere) hereCatIndex = i;
      html += '<div class="jhnav-group' + (hasHere ? " jhnav-gopen" : "") + '" data-i="' + i + '">'
        + '<div class="jhnav-ghead"><span>' + esc(String(cat.name).replace(/^[^A-Za-z0-9]+ */,"")) + ' <span class="jhnav-n">' + cat.count + '</span></span><span class="jhnav-chev">\u25b8</span></div>'
        + '<div class="jhnav-items">' + (cat.pages || []).map(itemHtml).join("") + '</div>'
        + '</div>';
    });
    groupsEl.innerHTML = html;
    if (!groupsEl.__jhFavWired) {
      groupsEl.__jhFavWired = true;
      groupsEl.addEventListener("click", function (e) {
        var st = e.target && e.target.classList && e.target.classList.contains("jhnav-star") ? e.target : null;
        if (!st) return;
        e.preventDefault(); e.stopPropagation();
        toggleFav(st.getAttribute("data-fav"));
        if (lastM) render(lastM);
      }, true);
    }
    var groups = groupsEl.querySelectorAll(".jhnav-group");
    for (var gi = 0; gi < groups.length; gi++) {
      (function (g) {
        g.querySelector(".jhnav-ghead").addEventListener("click", function () {
          g.classList.toggle("jhnav-gopen");
        });
      })(groups[gi]);
    }
    var search = document.getElementById("jhnav-search");
    search.addEventListener("input", function () {
      var q = this.value.toLowerCase();
      var gs = groupsEl.querySelectorAll(".jhnav-group");
      for (var i = 0; i < gs.length; i++) {
        var g = gs[i], any = false;
        var items = g.querySelectorAll(".jhnav-item");
        for (var j = 0; j < items.length; j++) {
          var it = items[j];
          var hit = !q || it.textContent.toLowerCase().indexOf(q) !== -1;
          it.style.display = hit ? "" : "none";
          if (hit) any = true;
        }
        g.classList.toggle("jhnav-gopen", q ? any : (g.getAttribute("data-i") == hereCatIndex));
        g.style.display = (q && !any) ? "none" : "";
      }
    });
  }

  function inject() {
    var styleEl = document.createElement("style");
    styleEl.textContent = CSS
      + ".jhnav-item{display:flex;justify-content:space-between;align-items:center;gap:8px}"
      + ".jhnav-item .jhnav-t{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
      + ".jhnav-star{flex:none;color:#5a6675;font-size:13px;padding:0 3px;cursor:pointer}"
      + ".jhnav-star.on{color:#ffd75e}.jhnav-star:hover{color:#ffd75e}"
      + ".jhnav-favgroup .jhnav-ghead{color:#ffd75e}";
    document.head.appendChild(styleEl);

    backdrop = document.createElement("div");
    backdrop.className = "jhnav-backdrop";

    handle = document.createElement("button");
    handle.className = "jhnav-handle";
    handle.type = "button";
    handle.title = "Search & browse all pages (Ctrl/Cmd+B)";
    handle.setAttribute("aria-label", "Open navigation");
    handle.textContent = "\u203a";

    drawer = document.createElement("div");
    drawer.className = "jhnav-drawer";
    drawer.innerHTML =
      '<div class="jhnav-head"><span class="jhnav-dot"></span><span class="jhnav-brand">JustHodl<b>.AI</b></span><span class="jhnav-count" id="jhnav-count"></span></div>'
      + '<input class="jhnav-search" id="jhnav-search" placeholder="Search pages\u2026" autocomplete="off">'
      + '<div id="jhnav-groups"></div>';

    document.body.appendChild(backdrop);
    document.body.appendChild(handle);
    document.body.appendChild(drawer);

    handle.addEventListener("click", toggle);
    backdrop.addEventListener("click", close);
    document.addEventListener("keydown", function (e) {
      if ((e.metaKey || e.ctrlKey) && (e.key === "b" || e.key === "B")) { e.preventDefault(); toggle(); }
      if (e.key === "Escape") close();
    });

    fetch("/nav-manifest.json").then(function (r) { return r.ok ? r.json() : null; })
      .then(function (m) { if (m) render(m); })
      .catch(function () {});
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", inject);
  } else {
    inject();
  }
})();
