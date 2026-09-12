/* jh-chart-pro-tvux.js
 * Chart-only chrome, paste-import, object tree (hide/show), row alert, tiny sparks.
 * NEVER deletes State.customWatchlists, localStorage lists, or tv-watchlists.json.
 */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  if (window.__jhChartTvux) return;
  window.__jhChartTvux = true;

  function toast(m) { if (window.jhToast) jhToast(m); }

  function parseSymbols(text) {
    var out = [];
    String(text || "").split(/[\s,;]+/).forEach(function (t) {
      t = t.replace(/^\"|\"$/g, "").trim();
      if (t && /^[A-Za-z0-9_.:\-]+$/.test(t) && /[A-Za-z]/.test(t)) out.push(t.toUpperCase());
    });
    return out.filter(function (t, i, a) { return a.indexOf(t) === i; });
  }

  function addListNeverDelete(name, tickers) {
    if (!window.WatchlistManager || !window.State) return 0;
    if (!tickers || !tickers.length) return 0;
    State.customWatchlists = State.customWatchlists || {};
    var id = null;
    Object.keys(State.customWatchlists).forEach(function (k) {
      if (State.customWatchlists[k] && State.customWatchlists[k].name === name) id = k;
    });
    if (!id) id = WatchlistManager.createCustom(name);
    tickers.forEach(function (t) { WatchlistManager.addTicker(id, t); });
    State.activeWatchlistId = id;
    WatchlistManager.saveActiveWl();
    if (window.UI && UI.refreshWatchlist) UI.refreshWatchlist();
    return tickers.length;
  }

  var bar = document.createElement("div");
  bar.id = "jh-tvux";
  bar.style.cssText = "position:fixed;left:48px;top:56px;z-index:42;display:flex;gap:6px;align-items:center;flex-wrap:wrap";
  bar.innerHTML =
    '<button type="button" data-act="chrome" style="background:#0a0d12;color:#22d3ee;border:1px solid #1d2636;border-radius:6px;padding:3px 8px;font:10px IBM Plex Mono,monospace;cursor:pointer">chart only</button>' +
    '<button type="button" data-act="tree" style="background:#0a0d12;color:#22d3ee;border:1px solid #1d2636;border-radius:6px;padding:3px 8px;font:10px IBM Plex Mono,monospace;cursor:pointer">drawings</button>' +
    '<input data-act="paste" placeholder="paste NVDA,AAPL,FRED:DGS10 — new list" style="width:220px;background:#0a0d12;color:#e8edf5;border:1px solid #1d2636;border-radius:6px;padding:3px 6px;font:10px IBM Plex Mono,monospace"/>';
  function mount() { if (!document.getElementById("jh-tvux")) document.body.appendChild(bar); }
  if (document.body) mount(); else document.addEventListener("DOMContentLoaded", mount);

  var style = document.createElement("style");
  style.textContent =
    "body.jh-chart-only #jh-nav-drawer, body.jh-chart-only .jh-nav-drawer, body.jh-chart-only [data-jh-chrome]," +
    "body.jh-chart-only .cmd-center, body.jh-chart-only #command-center { display:none !important; }" +
    "#jh-obj-tree{position:fixed;right:56px;top:88px;z-index:43;width:220px;max-height:50vh;overflow:auto;" +
    "background:rgba(10,13,18,.94);border:1px solid #1d2636;border-radius:10px;padding:10px;color:#a8b3c7;font:12px Inter,sans-serif;display:none}";
  document.head.appendChild(style);

  var tree = document.createElement("div");
  tree.id = "jh-obj-tree";
  if (document.body) document.body.appendChild(tree);
  else document.addEventListener("DOMContentLoaded", function () { document.body.appendChild(tree); });

  function renderTree() {
    var d = (window.State && State.drawings) || {};
    var keys = Object.keys(d);
    tree.innerHTML = '<div style="font:10px IBM Plex Mono,monospace;color:#22d3ee;margin-bottom:6px">DRAWINGS (hide only)</div>';
    if (!keys.length) { tree.innerHTML += '<div>none saved</div>'; return; }
    keys.forEach(function (k) {
      var row = document.createElement("label");
      row.style.cssText = "display:block;margin:4px 0;cursor:pointer";
      var on = d[k] && d[k].hidden ? "" : "checked";
      row.innerHTML = '<input type="checkbox" ' + on + '> ' + k;
      row.querySelector("input").addEventListener("change", function (ev) {
        if (!State.drawings[k]) return;
        State.drawings[k].hidden = !ev.target.checked;
        if (WatchlistManager && WatchlistManager.syncToCloud) WatchlistManager.syncToCloud();
      });
      tree.appendChild(row);
    });
  }

  bar.addEventListener("click", function (e) {
    var act = e.target && e.target.getAttribute("data-act");
    if (act === "chrome") document.body.classList.toggle("jh-chart-only");
    if (act === "tree") {
      tree.style.display = tree.style.display === "block" ? "none" : "block";
      renderTree();
    }
  });
  bar.addEventListener("keydown", function (e) {
    if (e.target.getAttribute("data-act") !== "paste" || e.key !== "Enter") return;
    var syms = parseSymbols(e.target.value);
    if (!syms.length) return;
    var n = addListNeverDelete("Paste " + new Date().toISOString().slice(0, 16), syms);
    toast("added list with " + n + " symbols");
    e.target.value = "";
  });

  var API = "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws";
  function drawSpark(cv, pts) {
    var w = cv.width, h = cv.height, ctx = cv.getContext("2d");
    if (!ctx || !pts.length) return;
    var vals = pts.map(function (p) { return +p[1]; }).filter(function (n) { return isFinite(n); });
    if (vals.length < 2) return;
    var mn = Math.min.apply(null, vals), mx = Math.max.apply(null, vals), span = mx - mn || 1;
    ctx.clearRect(0, 0, w, h);
    ctx.strokeStyle = vals[vals.length - 1] >= vals[0] ? "#26ffaf" : "#ff5577";
    ctx.lineWidth = 1;
    ctx.beginPath();
    vals.forEach(function (v, i) {
      var x = i / (vals.length - 1) * w;
      var y = h - 1 - (v - mn) / span * (h - 2);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }
  function sparks() {
    var rows = document.querySelectorAll("#jhwl-ul .jhwl-s");
    var n = 0;
    Array.prototype.forEach.call(rows, function (row) {
      if (n >= 12) return;
      if (row.querySelector("canvas.jh-spark")) { n++; return; }
      var sym = (row.textContent || "").trim();
      if (!sym) return;
      n++;
      var cv = document.createElement("canvas");
      cv.className = "jh-spark";
      cv.width = 56; cv.height = 16;
      cv.style.cssText = "float:right;margin-left:6px";
      row.appendChild(cv);
      fetch(API + "?sym=" + encodeURIComponent(sym)).then(function (r) { return r.json(); }).then(function (j) {
        if (j && j.points) drawSpark(cv, j.points.slice(-60));
      }).catch(function () {});
    });
  }
  var lastSel = "";
  setInterval(function () {
    var sel = document.getElementById("jhwl-sel");
    var key = sel ? sel.value + ":" + document.querySelectorAll("#jhwl-ul .jhwl-s").length : "";
    if (key && key !== lastSel) { lastSel = key; sparks(); }
  }, 1200);
})();
