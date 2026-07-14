/* jh-nav-drawer.js — universal navigation drawer, present on every page.
   Closed by default; pull the edge handle (or press Ctrl/Cmd+B) to search or browse
   sideways across all pages, grouped by category. Picking a page just navigates —
   real <a href> links, no click interception. Degrades silently if the manifest
   can't be fetched. Namespaced (jhnav- prefix, no :root vars) so it can't collide
   with any existing page's own styles. */
(function () {
  "use strict";
  if (window.__jhNavDrawer) return; window.__jhNavDrawer = true;

  /* JH_FRESH_GUARD (ops 3310) — stale-client self-heal.
     Some clients kept serving old HTML through an outdated service
     worker + browser HTTP cache. One-time per generation: unregister
     every SW, delete every Cache Storage entry, then reload once.
     Loop-guarded via sessionStorage; no-ops forever after. */
  try {
    var GEN = "3310";
    if (localStorage.getItem("jh_sw_gen") !== GEN &&
        sessionStorage.getItem("jh_fresh_try") !== GEN) {
      sessionStorage.setItem("jh_fresh_try", GEN);
      var done = function () {
        try { localStorage.setItem("jh_sw_gen", GEN); } catch (e) {}
        try { location.reload(); } catch (e) {}
      };
      var jobs = [];
      if (navigator.serviceWorker && navigator.serviceWorker.getRegistrations) {
        jobs.push(navigator.serviceWorker.getRegistrations()
          .then(function (rs) { return Promise.all(rs.map(function (r) { return r.unregister(); })); })
          .catch(function () {}));
      }
      if (window.caches && caches.keys) {
        jobs.push(caches.keys()
          .then(function (ks) { return Promise.all(ks.map(function (k) { return caches.delete(k); })); })
          .catch(function () {}));
      }
      if (jobs.length) { Promise.all(jobs).then(done, done); }
      else { try { localStorage.setItem("jh_sw_gen", GEN); } catch (e) {} }
    } else if (localStorage.getItem("jh_sw_gen") !== GEN) {
      /* second pass after the reload: mark generation complete (new SW
         re-registers itself naturally from the page). */
      try { localStorage.setItem("jh_sw_gen", GEN); } catch (e) {}
    }
  } catch (e) {}

  /* JH_USERSYNC_V1 — theme + per-user sync (ops 3157) */
  try {
    var __th = localStorage.getItem("jh_theme");
    if (__th === "light" || __th === "dark") document.documentElement.setAttribute("data-theme", __th);
  } catch (e) {}

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
  function toggleFav(h){ var f = getFavs(); var i = f.indexOf(h); if (i >= 0) f.splice(i, 1); else f.unshift(h); setFavs(f); queueSync(); }


  // ── theme + per-user sync (ops 3157) ────────────────────────────────
  function getTheme(){ try { return localStorage.getItem("jh_theme") || "dark"; } catch(e){ return "dark"; } }
  function setTheme(t, fromServer){
    if (t !== "light" && t !== "dark") return;
    try { localStorage.setItem("jh_theme", t); localStorage.setItem("jh_theme_ts", String(Date.now())); } catch(e){}
    try { document.documentElement.setAttribute("data-theme", t); } catch(e){}
    var b = document.getElementById("jhnav-themebtn");
    if (b) b.textContent = (t === "light" ? "\u263E Dark mode" : "\u2600 Light mode");
    if (!fromServer) queueSync();
  }
  window.__jhTheme = { get: getTheme, set: setTheme };

  function sbToken(){
    try {
      for (var i = 0; i < localStorage.length; i++) {
        var k = localStorage.key(i);
        if (/^sb-.*-auth-token$/.test(k)) {
          var v = JSON.parse(localStorage.getItem(k) || "null");
          if (!v) return null;
          return v.access_token || (v.currentSession && v.currentSession.access_token) || null;
        }
      }
    } catch(e){}
    return null;
  }
  var SYNC_URL = "https://justhodl-data-proxy.raafouis.workers.dev/userdata/self";
  var syncTimer = null;
  function queueSync(){ if (!sbToken()) return; clearTimeout(syncTimer); syncTimer = setTimeout(pushSync, 1500); }
  function pushSync(){
    var t = sbToken(); if (!t) return;
    var blob = { v: 1, favs: getFavs(), theme: getTheme(), updated_at: Date.now() };
    try {
      fetch(SYNC_URL, { method: "PUT",
        headers: { "Content-Type": "application/json", "Authorization": "Bearer " + t },
        body: JSON.stringify(blob) }).catch(function(){});
    } catch(e){}
  }
  function pullSync(){
    var t = sbToken(); if (!t) return;
    try {
      fetch(SYNC_URL, { headers: { "Authorization": "Bearer " + t } })
        .then(function(r){ return r.ok ? r.json() : null; })
        .then(function(b){
          if (!b) return;
          if (b.empty) return pushSync();
          try {
            var local = getFavs();
            var remote = (b.favs || []).filter(function(x){ return typeof x === "string"; });
            var merged = remote.concat(local.filter(function(x){ return remote.indexOf(x) < 0; }));
            setFavs(merged);
            var localTs = 0; try { localTs = +(localStorage.getItem("jh_theme_ts") || 0); } catch(e){}
            if (b.theme && (b.updated_at || 0) > localTs) setTheme(b.theme, true);
            if (lastM) render(lastM);
            if (merged.length !== remote.length) pushSync();
          } catch(e){}
        }).catch(function(){});
    } catch(e){}
  }

  // ── account + favorites-sync surface (ops 3292): favorites are
  //    ACCOUNT-BACKED via /userdata; this makes sign-in reachable from
  //    every page so sync actually happens. Reuses auth.js (no rebuild).
  function jwtEmail(t){
    try { var p = JSON.parse(atob(t.split(".")[1].replace(/-/g,"+").replace(/_/g,"/")));
          return p.email || null; } catch(e){ return null; }
  }
  var authLoading = false;
  function ensureAuth(cb){
    if (window.JustHodlAuth) { try { window.JustHodlAuth.init(); } catch(e){} cb && cb(); return; }
    if (authLoading) { var w=setInterval(function(){ if(window.JustHodlAuth){clearInterval(w); cb&&cb();} },200); return; }
    authLoading = true;
    function load(src, next){ var sc=document.createElement("script"); sc.src=src; sc.onload=next;
      sc.onerror=function(){ authLoading=false; }; document.head.appendChild(sc); }
    load("/auth-config.js", function(){
      load("https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2", function(){
        load("/auth.js", function(){
          try { window.JustHodlAuth.init(); } catch(e){}
          try { window.JustHodlAuth.onChange(function(){ renderAccount(); pullSync(); }); } catch(e){}
          cb && cb();
        });
      });
    });
  }
  function renderAccount(){
    var el = document.getElementById("jhnav-account"); if (!el) return;
    var t = sbToken();
    if (t) {
      var em = jwtEmail(t) || "signed in";
      el.innerHTML = '<div style="display:flex;align-items:center;gap:8px;font-size:11px;color:#8fa0b8;padding:2px 2px">'
        + '<span style="width:7px;height:7px;border-radius:50%;background:#3ddc84;flex:none"></span>'
        + '<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1" title="favorites + theme sync to your account">' + em + '</span>'
        + '<a href="#" id="jhnav-signout" style="color:#5d6b82;text-decoration:none;flex:none">sign out</a></div>';
      var so = document.getElementById("jhnav-signout");
      if (so) so.addEventListener("click", function(e){ e.preventDefault();
        ensureAuth(function(){ window.JustHodlAuth.signOut().then(function(){ renderAccount(); }); }); });
    } else {
      el.innerHTML = '<button id="jhnav-signin" style="width:100%;background:#0d2233;border:1px solid #17435f;border-radius:8px;padding:9px 11px;color:#4fc3f7;font-size:12px;cursor:pointer;font-family:inherit">\u2605 Sign in \u2014 sync favorites across devices</button>';
      var si = document.getElementById("jhnav-signin");
      if (si) si.addEventListener("click", function(){ ensureAuth(function(){ window.JustHodlAuth.openSignIn(); }); });
    }
  }

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
    var favPages = favs.map(function (h) { return { href: h, title: titleByHref[h] || (h.replace(/^\//,"").replace(/\.html$/,"").replace(/-/g," ")) }; }); /* ops 3269: a star never vanishes even if the manifest misses the page */
    if(!favs.length){favPages=[{href:'#',title:'No stars in THIS browser yet — favorites are per-browser; sign in on the browser that has them to sync (ops 3275)'}];}
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
      + '<div id="jhnav-groups"></div>'
      + '<div style="margin-top:12px;border-top:1px solid #1d2636;padding-top:10px">'
      +   '<button id="jhnav-themebtn" style="width:100%;background:#161d2a;border:1px solid #1d2636;border-radius:8px;padding:9px 11px;color:#a8b3c7;font-size:12px;cursor:pointer;font-family:inherit">theme</button>'
      +   '<div id="jhnav-account" style="margin-top:8px"></div>'
      + '</div>';

    document.body.appendChild(backdrop);
    document.body.appendChild(handle);
    document.body.appendChild(drawer);

    try {
      var tb = document.getElementById("jhnav-themebtn");
      if (tb) {
        tb.textContent = (getTheme() === "light" ? "\u263E Dark mode" : "\u2600 Light mode");
        tb.addEventListener("click", function(){ setTheme(getTheme() === "light" ? "dark" : "light"); });
      }
      renderAccount();
      if (sbToken()) { ensureAuth(function(){ setTimeout(function(){ renderAccount(); pullSync(); }, 400); }); }
      pullSync();
    } catch(e){}

    handle.addEventListener("click", toggle);
    backdrop.addEventListener("click", close);
    document.addEventListener("keydown", function (e) {
      if ((e.metaKey || e.ctrlKey) && (e.key === "b" || e.key === "B")) { e.preventDefault(); toggle(); }
      if (e.key === "Escape") close();
    });

    try{if(navigator.serviceWorker&&navigator.serviceWorker.getRegistrations)navigator.serviceWorker.getRegistrations().then(function(rs){rs.forEach(function(r){r.update();});});}catch(e){}
    try{if(!sessionStorage.getItem('jh_diag_3276')){sessionStorage.setItem('jh_diag_3276','1');var fv=0;try{fv=(JSON.parse(localStorage.getItem('jh_favs')||'[]')||[]).length;}catch(e){}var sw='none';try{sw=(navigator.serviceWorker&&navigator.serviceWorker.controller)?'yes':'none';}catch(e){}fetch('https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page='+encodeURIComponent(location.pathname)+'&favs='+fv+'&sw='+sw+'&v=3276',{keepalive:true}).catch(function(){});}}catch(e){}
    fetch("/nav-manifest.json?v="+Math.floor(Date.now()/36e5)) /* ops 3272: hourly bust — stale CDN hid new pages+stars */.then(function (r) { return r.ok ? r.json() : null; })
      .then(function (m) { if (m) render(m); })
      .catch(function () {});
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", inject);
  } else {
    inject();
  }
})();
