/* jh-chart-find: symbol hits paint as each source returns; indicator search ignores the tab; every favorite is on the Indicators menu. */
(function () {
  if (window.__jhFind) return;
  window.__jhFind = 1;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var gen = 0, extra = [], lastQ = "\0", symTimer = 0;

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      if (c === "&") return "&" + "amp;";
      if (c === "<") return "&" + "lt;";
      if (c === ">") return "&" + "gt;";
      if (c === '"') return "&" + "quot;";
      return "&" + "#39;";
    });
  }
  function studies() {
    var a = [];
    (window.INDS || []).forEach(function (i) { a.push({ item: i, osc: false, id: i.id, n: i.n || i.id, cat: i.cat || "Overlay" }); });
    (window.OSC || []).forEach(function (o) { a.push({ item: o, osc: true, id: o.id, n: o.n || o.id, cat: o.cat || "Oscillator" }); });
    return a;
  }
  function findStudy(id) {
    return (window.INDS || []).concat(window.OSC || []).filter(function (x) { return x.id === id; })[0] || null;
  }
  function blob(s) {
    return ((s.n || "") + " " + (s.id || "") + " " + (s.cat || "") + " " + ((s.item && s.item.k) || "")).toLowerCase();
  }
  function matchQ(s, q) {
    q = String(q || "").trim().toLowerCase();
    if (!q) return true;
    var b = blob(s), squish = b.replace(/[^a-z0-9%]+/g, ""), qsquish = q.replace(/[^a-z0-9%]+/g, "");
    if (b.indexOf(q) >= 0 || (qsquish && squish.indexOf(qsquish) >= 0)) return true;
    var parts = q.split(/[^a-z0-9%]+/).filter(Boolean);
    return parts.length > 0 && parts.every(function (p) { return b.indexOf(p) >= 0 || squish.indexOf(p) >= 0; });
  }
  function paintChart(it) {
    try { if (window.jhSaveLay) window.jhSaveLay(); } catch (e) {}
    try { if (window.paint && window.lastBars && window.lastBars.length) window.paint(window.lastBars); } catch (e2) {}
  }
  function toggleStudy(id) {
    var it = findStudy(id);
    if (!it) return null;
    it.on = !it.on;
    if (it.hide) it.hide = false;
    paintChart(it);
    return it;
  }

  function favIds() {
    var list = [];
    try { if (window.jhIndFavList) list = window.jhIndFavList() || []; } catch (e) {}
    if (!list.length) {
      try { var x = JSON.parse(localStorage.getItem("jh-chart-ind-favs") || "null"); if (Array.isArray(x)) list = x; } catch (e2) {}
    }
    return list;
  }

  function rowHtml(id, name, on, starred) {
    return "<div class='irow" + (on ? " on" : "") + "' data-jh-row='" + esc(id) + "'>" +
      "<button type=button class=nm data-jh-tog='" + esc(id) + "'>" + (on ? "✓  " : "") + esc(name || id) + "</button>" +
      "<button type=button class='fv" + (starred ? " on" : "") + "' data-jh-fav='" + esc(id) + "' title='Favorite'>★</button>" +
      "<button type=button class=gr data-jh-gear='" + esc(id) + "' title='Inputs and colors'>⚙</button></div>";
  }
  function bindRows(root) {
    root.querySelectorAll("[data-jh-tog]").forEach(function (b) {
      b.onclick = function (e) {
        e.preventDefault(); e.stopPropagation();
        var id = b.getAttribute("data-jh-tog");
        var it = toggleStudy(id);
        if (!it) return;
        b.textContent = (it.on ? "✓  " : "") + (it.n || id);
        var row = b.closest(".irow");
        if (row) row.classList.toggle("on", !!it.on);
      };
    });
    root.querySelectorAll("[data-jh-fav]").forEach(function (b) {
      b.onclick = function (e) {
        e.preventDefault(); e.stopPropagation();
        var id = b.getAttribute("data-jh-fav");
        if (window.jhIndFavToggle) window.jhIndFavToggle(id);
        var menu = document.getElementById("menu");
        if (menu && menu.classList.contains("on")) enhanceMenu(menu, true);
      };
    });
    root.querySelectorAll("[data-jh-gear]").forEach(function (b) {
      b.onclick = function (e) {
        e.preventDefault(); e.stopPropagation();
        var it = findStudy(b.getAttribute("data-jh-gear"));
        var menu = document.getElementById("menu");
        if (menu) menu.className = "menu";
        if (it && window.jhInduxSet) window.jhInduxSet(it, (window.OSC || []).indexOf(it) >= 0);
      };
    });
  }

  function hideCapped(menu) {
    menu.querySelectorAll(".lab").forEach(function (lab) {
      if (lab.closest("[data-jh-favblock]")) return;
      if ((lab.textContent || "").trim() !== "FAVORITES") return;
      lab.style.display = "none";
      var n = lab.nextElementSibling;
      while (n && !n.classList.contains("lab")) {
        if (n.classList.contains("irow")) { n.setAttribute("data-jh-capped", "1"); n.style.display = "none"; }
        n = n.nextElementSibling;
      }
    });
  }

  function paintFavs(menu) {
    var ids = favIds(), seen = {}, html = "", i, id, it, starred;
    for (i = ids.length - 1; i >= 0; i--) {
      id = ids[i];
      if (!id || seen[id]) continue;
      seen[id] = 1;
      it = findStudy(id);
      starred = window.jhIndFavHas ? !!window.jhIndFavHas(id) : true;
      html += rowHtml(id, (it && it.n) || id, !!(it && it.on), starred);
    }
    var block = menu.querySelector("[data-jh-favblock]");
    if (!block) {
      block = document.createElement("div");
      block.setAttribute("data-jh-favblock", "1");
      var bar = menu.querySelector("[data-jh-findbar]");
      if (bar && bar.nextSibling) menu.insertBefore(block, bar.nextSibling);
      else menu.insertBefore(block, menu.firstChild);
    }
    block.innerHTML = html ? "<div class=lab>FAVORITES</div>" + html : "";
    bindRows(block);
  }

  function applyFilter(menu, q) {
    q = String(q || "");
    var nq = q.trim().toLowerCase();
    menu.querySelectorAll(".irow").forEach(function (row) {
      if (row.getAttribute("data-jh-capped") === "1") { row.style.display = "none"; return; }
      if (row.closest("[data-jh-extra]")) return;
      var id = (row.querySelector("[data-tog], [data-jh-tog]") || {}).getAttribute;
      var tid = "";
      var t = row.querySelector("[data-tog], [data-jh-tog]");
      if (t) tid = t.getAttribute("data-tog") || t.getAttribute("data-jh-tog") || "";
      var fake = { id: tid, n: row.textContent || "", cat: "" };
      row.style.display = !nq || matchQ(fake, nq) ? "" : "none";
    });
    menu.querySelectorAll(".lab").forEach(function (lab) {
      if ((lab.textContent || "").trim() === "FAVORITES" && !lab.closest("[data-jh-favblock]")) { lab.style.display = "none"; return; }
      var n = lab.nextElementSibling, any = false, steps = 0;
      while (n && !n.classList.contains("lab") && steps < 80) {
        if (n.classList.contains("irow") && n.style.display !== "none" && n.getAttribute("data-jh-capped") !== "1") any = true;
        n = n.nextElementSibling; steps++;
      }
      lab.style.display = any || !nq ? "" : "none";
    });
    var old = menu.querySelector("[data-jh-extra]");
    if (old) old.remove();
    if (!nq) return;
    var shown = {};
    menu.querySelectorAll("[data-tog], [data-jh-tog]").forEach(function (b) {
      var row = b.closest(".irow");
      if (row && row.style.display !== "none") shown[b.getAttribute("data-tog") || b.getAttribute("data-jh-tog")] = 1;
    });
    var more = studies().filter(function (s) { return !shown[s.id] && matchQ(s, nq); });
    if (!more.length) {
      var anyRow = false;
      menu.querySelectorAll(".irow").forEach(function (row) { if (row.style.display !== "none") anyRow = true; });
      if (!anyRow) {
        var empty = document.createElement("div");
        empty.setAttribute("data-jh-extra", "1");
        empty.className = "lab";
        empty.textContent = "No match";
        menu.appendChild(empty);
      }
      return;
    }
    var box = document.createElement("div");
    box.setAttribute("data-jh-extra", "1");
    var html = "<div class=lab>MATCHES</div>";
    more.slice(0, 24).forEach(function (s) {
      var starred = window.jhIndFavHas ? !!window.jhIndFavHas(s.id) : false;
      html += rowHtml(s.id, s.n, !!s.item.on, starred);
    });
    box.innerHTML = html;
    menu.appendChild(box);
    bindRows(box);
  }

  function enhanceMenu(menu, keepQ) {
    if (!menu || !menu.querySelector("[data-tog]")) return;
    hideCapped(menu);
    var bar = menu.querySelector("[data-jh-findbar]");
    var prev = "";
    if (!bar) {
      bar = document.createElement("div");
      bar.setAttribute("data-jh-findbar", "1");
      bar.innerHTML = "<input class=jh-q id=jh-indq placeholder='Search indicators' autocomplete=off spellcheck=false>";
      menu.insertBefore(bar, menu.firstChild);
      var inp = bar.querySelector("input");
      inp.oninput = function () { applyFilter(menu, inp.value); };
      inp.onclick = function (e) { e.stopPropagation(); };
      inp.onkeydown = function (e) { e.stopPropagation(); if (e.key === "Escape") { menu.className = "menu"; } };
    } else if (keepQ) prev = (bar.querySelector("input") || {}).value || "";
    paintFavs(menu);
    var inp2 = bar.querySelector("input");
    if (keepQ && inp2) inp2.value = prev;
    applyFilter(menu, inp2 ? inp2.value : "");
    if (inp2 && !keepQ) { try { inp2.focus(); } catch (e) {} }
  }

  function watchMenu() {
    var menu = document.getElementById("menu");
    if (!menu || menu.dataset.jhFindWatch) return;
    menu.dataset.jhFindWatch = "1";
    new MutationObserver(function () {
      if (!menu.classList.contains("on")) return;
      if (!menu.querySelector("[data-tog]")) return;
      if (menu.querySelector("[data-jh-findbar]")) return;
      enhanceMenu(menu, false);
    }).observe(menu, { childList: true, subtree: false });
  }

  function paintDialog(q) {
    var list = document.getElementById("indlist");
    if (!list) return;
    var rows = studies().filter(function (s) { return matchQ(s, q); });
    if (!rows.length) { list.innerHTML = "<div class=irow><span>No match</span></div>"; return; }
    var cats = [];
    rows.forEach(function (s) { if (cats.indexOf(s.cat) < 0) cats.push(s.cat); });
    var html = "";
    cats.forEach(function (c) {
      html += "<div class=icat>" + esc(c).toUpperCase() + "</div>";
      rows.filter(function (s) { return s.cat === c; }).forEach(function (s) {
        var fav = window.jhIndFavHas && window.jhIndFavHas(s.id);
        html += "<button type=button class='irow" + (s.item.on ? " on" : "") + "' data-add='" + esc(s.id) + "' data-osc='" + (s.osc ? "1" : "0") + "'><div><b>" + esc(s.n) + "</b><span>" + esc(s.cat) + (s.osc ? " · pane" : " · overlay") + "</span></div><span class='star" + (fav ? " on" : "") + "' data-star='" + esc(s.id) + "'>" + (fav ? "★" : "☆") + "</span></button>";
      });
    });
    list.innerHTML = html;
    list.querySelectorAll("[data-add]").forEach(function (b) {
      b.onclick = function (e) {
        if (e.target.closest && e.target.closest("[data-star]")) return;
        var id = b.getAttribute("data-add");
        if (id === "vol" || id === "voltape") { if (window.jhSetVol) window.jhSetVol(true); if (id === "vol") { paintDialog(q); return; } }
        var it = findStudy(id);
        if (!it) return;
        it.on = true; it.hide = false;
        paintChart(it);
        paintDialog(q);
      };
    });
    list.querySelectorAll("[data-star]").forEach(function (b) {
      b.onclick = function (e) {
        e.stopPropagation();
        var id = b.getAttribute("data-star");
        if (window.jhIndFavToggle) window.jhIndFavToggle(id);
        paintDialog(q);
        var menu = document.getElementById("menu");
        if (menu && menu.classList.contains("on")) enhanceMenu(menu, true);
      };
    });
  }

  function hookOpen() {
    var open = window.jhInduxOpen;
    if (!open || open.__jhFind) return;
    var wrapped = function () {
      var r = open.apply(this, arguments);
      var inp = document.getElementById("indq2");
      if (!inp) return r;
      var orig = inp.oninput;
      inp.oninput = function () {
        var q = inp.value || "";
        if (!String(q).trim()) { if (orig) orig.call(inp); return; }
        paintDialog(q);
      };
      return r;
    };
    wrapped.__jhFind = 1;
    window.jhInduxOpen = wrapped;
  }

  function rankSym(sym, name, q) {
    var u = String(q || "").trim().toUpperCase();
    if (!u) return 9;
    var s = String(sym || "").toUpperCase();
    var b = s.split(":").pop();
    var n = String(name || "").toUpperCase();
    if (b === u || s === u) return 0;
    if (b.indexOf(u) === 0 || s.indexOf(u) === 0) return 1;
    if (n.indexOf(u) === 0) return 2;
    if (u.length >= 2 && (n.indexOf(u) >= 0 || b.indexOf(u) >= 0)) return 3;
    return 9;
  }
  function addExtra(my, sym, name, ex) {
    if (my !== gen || !sym) return;
    sym = String(sym).trim();
    if (!sym || /sentinel/i.test(sym)) return;
    var q = lastQ;
    var rk = rankSym(sym, name, q);
    if (rk > 3) return;
    var key = sym.toUpperCase();
    if (extra.some(function (r) { return r.k === key; })) return;
    extra.push({ s: sym, n: name || sym, x: ex || "", k: key, rk: rk });
    extra.sort(function (a, b) { return a.rk - b.rk; });
    if (extra.length > 16) extra.length = 16;
    placeHits();
  }
  function fan(q) {
    var my = ++gen;
    extra = [];
    lastQ = q;
    placeHits();
    if (window.JHChartCatalog && window.JHChartCatalog.suggest) {
      try {
        window.JHChartCatalog.suggest(q, 20).forEach(function (a) { addExtra(my, a.s, a.name, a.extra || ""); });
      } catch (e) {}
    }
    function take(url, fn) {
      fetch(url).then(function (r) { return r.json(); }).then(function (j) { if (my === gen) fn(j); }).catch(function () {});
    }
    take(PROXY + "/tv-search?text=" + encodeURIComponent(q), function (j) {
      (j && j.symbols || []).forEach(function (s) { addExtra(my, s.symbol || s.full, s.description || s.name, (s.exchange || "") + " " + (s.type || "")); });
    });
    take("/api/yahoo-search?q=" + encodeURIComponent(q), function (j) {
      (j && j.quotes || []).forEach(function (s) { if (s && s.symbol) addExtra(my, s.symbol, s.shortname || s.longname || s.name, s.exchDisp || s.exchange || s.typeDisp || ""); });
    });
    take(PROXY + "/symsearch?q=" + encodeURIComponent(q) + "&limit=20", function (j) {
      var rows = (j && j.rows) || [];
      rows.forEach(function (row) {
        if (!row) return;
        addExtra(my, row.symbol || row.ticker || row.id, row.name || row.title || "", (row.exchange || row.provider || "") + " " + (row.type || row.kind || ""));
      });
    });
  }
  function placeHits() {
    var box = document.getElementById("ssres");
    var wrap = document.getElementById("symsearch");
    if (!box || !wrap || !wrap.classList.contains("on")) return;
    var st = box.querySelector("[data-search-status]");
    if (st && st !== box.lastElementChild) box.appendChild(st);
    var have = {};
    box.querySelectorAll(".ss-hit .nm").forEach(function (n) {
      if (n.closest("[data-jh-hits]")) return;
      have[String(n.textContent || "").trim().toUpperCase()] = 1;
    });
    var rows = extra.filter(function (r) {
      var b = r.s.toUpperCase().split(":").pop();
      return !have[r.s.toUpperCase()] && !have[b];
    }).slice(0, 12);
    var sig = rows.map(function (r) { return r.s; }).join("|");
    var node = box.querySelector("[data-jh-hits]");
    if (!rows.length) { if (node) node.remove(); return; }
    if (node && node.getAttribute("data-sig") === sig && box.firstElementChild === node) return;
    if (node) node.remove();
    var html = "<div class=ss-sec>MATCHES</div>";
    rows.forEach(function (r) {
      var tick = r.s.split(":").pop();
      html += "<button type=button class=ss-hit data-jh-sym='" + esc(r.s) + "'><i class=ss-logo style=background:#2962ff>" + esc(tick.slice(0, 1)) + "</i><span><span class=nm>" + esc(tick) + "</span><span class=ds>" + esc(r.n) + "</span></span><span class=ss-ex>" + esc(r.x) + "</span><span class=ss-more> </span></button>";
    });
    var host = document.createElement("div");
    host.setAttribute("data-jh-hits", "1");
    host.setAttribute("data-sig", sig);
    host.innerHTML = html;
    box.insertBefore(host, box.firstChild);
    host.querySelectorAll("[data-jh-sym]").forEach(function (b) {
      b.onclick = function () {
        var s = b.getAttribute("data-jh-sym");
        if (window.jhGoSymbol) window.jhGoSymbol(s, "chart");
        else if (window.jhOpenSymbol) window.jhOpenSymbol(s);
      };
    });
  }
  function watchSym() {
    if (symTimer) return;
    var moHold = false;
    function arm() {
      var box = document.getElementById("ssres");
      if (!box || box.dataset.jhFindWatch) return;
      box.dataset.jhFindWatch = "1";
      new MutationObserver(function () {
        if (moHold) return;
        var pop = document.getElementById("symsearch");
        if (!pop || !pop.classList.contains("on")) return;
        moHold = true;
        try { placeHits(); } finally { moHold = false; }
      }).observe(box, { childList: true });
    }
    arm();
    symTimer = setInterval(function () {
      arm();
      hookOpen();
      watchMenu();
      var pop = document.getElementById("symsearch");
      var inp = document.getElementById("ssin");
      if (!pop || !inp || !pop.classList.contains("on")) { if (lastQ) { lastQ = ""; extra = []; } return; }
      var q = String(inp.value || "").trim();
      if (q === lastQ) return;
      if (q.length < 2) { gen++; extra = []; lastQ = q; placeHits(); return; }
      clearTimeout(watchSym._t);
      watchSym._t = setTimeout(function () { fan(q); }, 90);
    }, 200);
  }

  document.addEventListener("keydown", function (e) {
    var menu = document.getElementById("menu");
    if (!menu || !menu.classList.contains("on") || !menu.querySelector("[data-jh-findbar]")) return;
    var inp = document.getElementById("jh-indq");
    if (!inp || e.target === inp) return;
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    if (e.key === "Backspace") {
      e.preventDefault(); e.stopPropagation();
      inp.focus();
      inp.value = inp.value.slice(0, -1);
      applyFilter(menu, inp.value);
      return;
    }
    if (e.key.length === 1) {
      e.preventDefault(); e.stopPropagation();
      inp.focus();
      inp.value += e.key;
      applyFilter(menu, inp.value);
    }
  }, true);

  var css = document.createElement("style");
  css.id = "jh-find-css";
  css.textContent = "#menu .jh-q{display:block;width:calc(100% - 16px);margin:4px 8px 6px;box-sizing:border-box;padding:7px 8px;border:1px solid var(--line,#2a2e39);border-radius:4px;background:transparent;color:inherit;font:13px IBM Plex Sans,sans-serif}#menu [data-jh-favblock]>.lab{color:#f0b429}#ssres [data-jh-hits] .ss-sec{padding:8px 14px 2px;font-size:10px;letter-spacing:1px;color:#787b86}";
  document.head.appendChild(css);
  hookOpen();
  watchMenu();
  watchSym();
})();
