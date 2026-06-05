/* cmdk.js — global ⌘K / Ctrl-K command palette for JustHodl.AI.
 *
 * Drop into any page with:
 *   <script src="/cmdk.js"></script>
 * Opens with ⌘K (Mac) / Ctrl-K (Win) or the "/" key (when not typing in a field).
 * Fuzzy-searches all ~225 pages (loaded from /site-catalog.json) plus a curated
 * set of top tools pinned at the top. Arrow keys + Enter to navigate.
 */
(function () {
  if (window.__jhCmdk) return; window.__jhCmdk = true;
  var SITE_CATALOG = "/site-catalog.json";
  // Curated top tools shown first (with icons) — the things people want most.
  var TOP = [
    { u: "/chart-pro.html", t: "Chart Pro", i: "📈", k: "charts trading terminal" },
    { u: "/signal-board.html", t: "Today's Setups / Conviction Board", i: "🎯", k: "setups conviction triple quad threat" },
    { u: "/opportunities.html", t: "Opportunities", i: "💡", k: "value growth screen" },
    { u: "/dislocations.html", t: "Buy the Laggard (Dislocations)", i: "⚖️", k: "cheap relative value laggard" },
    { u: "/compounders.html", t: "Compounders", i: "🌱", k: "quality growth durable" },
    { u: "/capital-flow.html", t: "Capital Flow", i: "💰", k: "institutions 13f etf accumulating" },
    { u: "/my-portfolio.html", t: "My Portfolio (Personal CIO)", i: "🧠", k: "holdings portfolio risk" },
    { u: "/ask.html", t: "Ask JustHodl (AI)", i: "💬", k: "question natural language ai chat" },
    { u: "/bond-vol.html", t: "Bond Vol Regime", i: "📊", k: "move rates volatility macro regime" },
    { u: "/track-public.html", t: "Track Record", i: "✅", k: "proof backtest performance" },
    { u: "/glossary.html", t: "Glossary", i: "📖", k: "definitions terms help learn" },
  ];

  var catalog = null, items = [], sel = 0, open = false;

  function css() {
    if (document.getElementById("cmdk-css")) return;
    var s = document.createElement("style"); s.id = "cmdk-css";
    s.textContent = [
      "#cmdk-ov{position:fixed;inset:0;background:rgba(4,6,10,0.6);backdrop-filter:blur(3px);z-index:99999;display:none;align-items:flex-start;justify-content:center}",
      "#cmdk-ov.on{display:flex}",
      "#cmdk-box{margin-top:11vh;width:min(620px,92vw);background:#0c1018;border:1px solid #2a3550;border-radius:14px;box-shadow:0 24px 80px rgba(0,0,0,0.6);overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif}",
      "#cmdk-in{width:100%;background:transparent;border:none;outline:none;color:#e1e8f4;font-size:17px;padding:18px 20px;border-bottom:1px solid #1c2433}",
      "#cmdk-in::placeholder{color:#6f7b91}",
      "#cmdk-list{max-height:54vh;overflow-y:auto;padding:6px}",
      ".cmdk-it{display:flex;align-items:center;gap:11px;padding:11px 14px;border-radius:8px;cursor:pointer;color:#a8b3c7}",
      ".cmdk-it.sel{background:#131929;color:#fff}",
      ".cmdk-it .ic{width:22px;text-align:center;font-size:15px;flex-shrink:0}",
      ".cmdk-it .tt{flex:1;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}",
      ".cmdk-it .uu{font-family:'IBM Plex Mono',monospace;font-size:10px;color:#6f7b91}",
      ".cmdk-grp{font-family:'IBM Plex Mono',monospace;font-size:9.5px;color:#6f7b91;text-transform:uppercase;letter-spacing:.6px;padding:9px 14px 4px}",
      "#cmdk-foot{display:flex;gap:14px;padding:8px 14px;border-top:1px solid #1c2433;font-family:'IBM Plex Mono',monospace;font-size:9.5px;color:#6f7b91}",
      ".cmdk-empty{padding:24px;text-align:center;color:#6f7b91;font-family:'IBM Plex Mono',monospace;font-size:12px}",
    ].join("");
    document.head.appendChild(s);
  }

  function build() {
    var ov = document.createElement("div"); ov.id = "cmdk-ov";
    ov.innerHTML =
      '<div id="cmdk-box">' +
      '<input id="cmdk-in" placeholder="Search tools, pages, signals…  (try \'capital flow\' or \'glossary\')" autocomplete="off" spellcheck="false">' +
      '<div id="cmdk-list"></div>' +
      '<div id="cmdk-foot"><span>↑↓ navigate</span><span>↵ open</span><span>esc close</span><span style="margin-left:auto">⌘K / Ctrl-K</span></div>' +
      '</div>';
    document.body.appendChild(ov);
    ov.addEventListener("click", function (e) { if (e.target === ov) close_(); });
    document.getElementById("cmdk-in").addEventListener("input", function (e) { render(e.target.value); });
    document.getElementById("cmdk-in").addEventListener("keydown", onKey);
  }

  function esc(s) { return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }

  function score(q, t, k) {
    t = t.toLowerCase(); k = (k || "").toLowerCase(); q = q.toLowerCase();
    if (!q) return 0;
    var hay = t + " " + k;
    if (t.startsWith(q)) return 100;
    if (hay.indexOf(q) >= 0) return 60;
    // subsequence fuzzy
    var i = 0; for (var c = 0; c < hay.length && i < q.length; c++) if (hay[c] === q[i]) i++;
    return i === q.length ? 25 : -1;
  }

  function render(q) {
    q = (q || "").trim();
    var list = document.getElementById("cmdk-list");
    items = [];
    var topScored = TOP.map(function (x) { return { x: x, s: q ? score(q, x.t, x.k) : 50 }; }).filter(function (o) { return o.s >= 0; }).sort(function (a, b) { return b.s - a.s; });
    var allScored = [];
    if (q && catalog) {
      var topUrls = {}; TOP.forEach(function (x) { topUrls[x.u] = 1; });
      allScored = catalog.map(function (x) { return { x: x, s: score(q, x.t, x.u) }; })
        .filter(function (o) { return o.s >= 0 && !topUrls[o.x.u]; })
        .sort(function (a, b) { return b.s - a.s; }).slice(0, 40);
    }
    var html = "";
    if (topScored.length) {
      html += '<div class="cmdk-grp">Top tools</div>';
      topScored.forEach(function (o) { items.push(o.x); html += row(o.x, items.length - 1, o.x.i); });
    }
    if (allScored.length) {
      html += '<div class="cmdk-grp">All pages</div>';
      allScored.forEach(function (o) { items.push(o.x); html += row(o.x, items.length - 1, "›"); });
    }
    if (!items.length) html = '<div class="cmdk-empty">No matches. Try a different term.</div>';
    list.innerHTML = html;
    sel = 0; paint();
    list.querySelectorAll(".cmdk-it").forEach(function (el) { el.addEventListener("click", function () { go(+el.dataset.i); }); });
  }
  function row(x, i, ic) {
    return '<div class="cmdk-it" data-i="' + i + '"><span class="ic">' + esc(ic) + '</span><span class="tt">' + esc(x.t) + '</span><span class="uu">' + esc(x.u) + '</span></div>';
  }
  function paint() {
    var els = document.querySelectorAll(".cmdk-it");
    els.forEach(function (el, i) { el.classList.toggle("sel", i === sel); });
    if (els[sel]) els[sel].scrollIntoView({ block: "nearest" });
  }
  function go(i) { var x = items[i]; if (x) window.location.href = x.u; }
  function onKey(e) {
    if (e.key === "ArrowDown") { e.preventDefault(); sel = Math.min(sel + 1, items.length - 1); paint(); }
    else if (e.key === "ArrowUp") { e.preventDefault(); sel = Math.max(sel - 1, 0); paint(); }
    else if (e.key === "Enter") { e.preventDefault(); go(sel); }
    else if (e.key === "Escape") { close_(); }
  }
  function open_() {
    open = true; document.getElementById("cmdk-ov").classList.add("on");
    var inp = document.getElementById("cmdk-in"); inp.value = ""; render("");
    setTimeout(function () { inp.focus(); }, 30);
    if (!catalog) fetch(SITE_CATALOG).then(function (r) { return r.ok ? r.json() : []; }).then(function (d) { catalog = d; }).catch(function () { catalog = []; });
  }
  function close_() { open = false; document.getElementById("cmdk-ov").classList.remove("on"); }

  function init() {
    css(); build();
    document.addEventListener("keydown", function (e) {
      var k = e.key.toLowerCase();
      if ((e.metaKey || e.ctrlKey) && k === "k") { e.preventDefault(); open ? close_() : open_(); return; }
      // "/" opens too, but not while typing in a field
      var tag = (e.target.tagName || "").toLowerCase();
      if (k === "/" && !open && tag !== "input" && tag !== "textarea" && !e.target.isContentEditable) { e.preventDefault(); open_(); }
    });
    // optional floating hint button (bottom-right)
    var b = document.createElement("div");
    b.innerHTML = "⌘K";
    b.title = "Search (⌘K / Ctrl-K)";
    b.style.cssText = "position:fixed;bottom:16px;right:16px;z-index:9998;background:#0c1018;border:1px solid #2a3550;color:#6f7b91;font-family:'IBM Plex Mono',monospace;font-size:12px;padding:7px 11px;border-radius:8px;cursor:pointer;box-shadow:0 4px 16px rgba(0,0,0,.4)";
    b.onclick = open_;
    b.onmouseenter = function () { b.style.color = "#22d3ee"; b.style.borderColor = "#22d3ee"; };
    b.onmouseleave = function () { b.style.color = "#6f7b91"; b.style.borderColor = "#2a3550"; };
    document.body.appendChild(b);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init); else init();
})();
