/* jh-reskin-skip */
/* Buyback pane. Own study, not the Events pin buyb. RSI was only the pane reference. */
(function () {
  if (window.__jhBuybackIndV1) return;
  window.__jhBuybackIndV1 = true;
  var on = false;
  var pack = null;
  var loading = false;

  function norm(s) {
    s = String(s || "").toUpperCase().trim();
    var i = s.lastIndexOf(":");
    if (i >= 0) s = s.slice(i + 1);
    return s.replace(/[^A-Z0-9.\-]/g, "");
  }
  function currentSym() {
    var onTab = document.querySelector("#tabs .tab.on[data-id], #tabs button.tab.on");
    if (onTab) return norm(onTab.getAttribute("data-id") || onTab.textContent);
    var wm = document.getElementById("wm");
    if (wm && wm.textContent) return norm(wm.textContent);
    var p = new URLSearchParams(location.search);
    return norm(p.get("s") || p.get("symbol") || window.jhSymbol || "");
  }
  function repurchase(m) {
    if (m == null) return null;
    if (typeof m === "number") return -m;
    var v = m.value;
    if (typeof v !== "number" || !isFinite(v)) return null;
    if (m.sign === "negative") return Math.abs(v);
    if (m.sign === "positive") return -Math.abs(v);
    return -v;
  }
  function rowFor(sym) {
    if (!pack) return null;
    var t = pack.tickers || pack;
    return t[sym] || null;
  }
  function points(sym) {
    var row = rowFor(sym);
    if (!row) return [];
    var cap = row.market_cap;
    if (typeof cap !== "number" || !(cap > 0)) return [];
    var obs = (row.measurements && row.measurements.cashflow_observations) || row.cashflow_observations || [];
    var out = [];
    for (var i = 0; i < obs.length; i++) {
      var o = obs[i] || {};
      var met = o.metrics || o;
      var usd = repurchase(met.net_common_repurchases);
      var date = o.date || (o.original && o.original.date);
      if (usd == null || !date) continue;
      out.push({ date: String(date).slice(0, 10), pct: (usd / cap) * 100 });
    }
    out.sort(function (a, b) { return a.date < b.date ? -1 : 1; });
    return out;
  }
  function load() {
    if (pack || loading) return Promise.resolve(pack);
    loading = true;
    return fetch("/data/buyback-engine.json", { cache: "no-store" }).then(function (r) {
      return r.ok ? r.json() : null;
    }).then(function (j) {
      pack = j;
      loading = false;
      return j;
    }).catch(function () { loading = false; return null; });
  }
  function host() {
    var wrap = document.getElementById("oscwrap");
    if (!wrap) return null;
    var n = document.getElementById("jh-buyback-pane");
    if (n) return n;
    n = document.createElement("div");
    n.id = "jh-buyback-pane";
    n.className = "osc";
    n.innerHTML = '<div class="olab" id="jh-buyback-lab">Buyback</div><svg id="jh-buyback-svg" width="100%" height="100%" preserveAspectRatio="none"></svg>';
    wrap.appendChild(n);
    return n;
  }
  function paint() {
    var wrap = document.getElementById("oscwrap");
    var pane = document.getElementById("jh-buyback-pane");
    if (!on) {
      if (pane) pane.style.display = "none";
      return;
    }
    if (wrap) wrap.classList.add("on");
    pane = host();
    if (!pane) return;
    pane.style.display = "block";
    var sym = currentSym();
    var pts = points(sym);
    var lab = document.getElementById("jh-buyback-lab");
    var svg = document.getElementById("jh-buyback-svg");
    if (!pts.length) {
      if (lab) lab.textContent = "Buyback \u00b7 " + (sym || "\u2014") + " \u00b7 not in buyback engine";
      if (svg) svg.innerHTML = "";
      return;
    }
    var last = pts[pts.length - 1];
    if (lab) lab.textContent = "Buyback \u00b7 " + last.pct.toFixed(2) + "% \u00b7 " + last.date;
    var w = pane.clientWidth || 600;
    var h = pane.clientHeight || 96;
    var vals = pts.map(function (p) { return p.pct; });
    var lo = Math.min(0, Math.min.apply(null, vals));
    var hi = Math.max(0, Math.max.apply(null, vals));
    if (hi === lo) hi = lo + 0.1;
    function y(v) { return 8 + (h - 16) * (1 - (v - lo) / (hi - lo)); }
    function x(i) { return 8 + (w - 16) * (pts.length === 1 ? 0.5 : i / (pts.length - 1)); }
    var d = "";
    for (var i = 0; i < pts.length; i++) {
      var x0 = x(i);
      var x1 = i + 1 < pts.length ? x(i + 1) : w - 8;
      var yy = y(pts[i].pct);
      d += (i ? "L" : "M") + x0.toFixed(1) + "," + yy.toFixed(1) + "L" + x1.toFixed(1) + "," + yy.toFixed(1);
    }
    var y0 = y(0);
    svg.setAttribute("viewBox", "0 0 " + w + " " + h);
    svg.innerHTML = '<line x1="8" y1="' + y0.toFixed(1) + '" x2="' + (w - 8) + '" y2="' + y0.toFixed(1) + '" stroke="#787b86" stroke-dasharray="3 3"/>' +
      '<path d="' + d + '" fill="none" stroke="#2962ff" stroke-width="1.6"/>' +
      '<text x="' + (w - 8) + '" y="14" text-anchor="end" fill="#d1d4dc" font-size="11" font-family="IBM Plex Mono,monospace">' + last.pct.toFixed(2) + '%</text>';
  }
  function setOn(next) {
    on = next;
    var btn = document.getElementById("jh-buyback-toggle");
    if (btn) btn.classList.toggle("on", on);
    if (on) load().then(paint);
    else paint();
  }
  function ensureMenu() {
    if (document.getElementById("jh-buyback-toggle")) return;
    var menus = document.querySelectorAll(".menu");
    for (var i = 0; i < menus.length; i++) {
      var b = menus[i].querySelector("button");
      if (!b) continue;
      var txt = menus[i].textContent || "";
      if (txt.indexOf("RSI") < 0 && txt.indexOf("Relative Volume") < 0 && txt.indexOf("Buybacks") < 0) continue;
      var row = document.createElement("button");
      row.id = "jh-buyback-toggle";
      row.type = "button";
      row.textContent = "Buyback";
      row.title = "Quarterly net repurchase as % of latest market cap. Own pane. Not the authorization pin.";
      row.addEventListener("click", function (ev) {
        ev.preventDefault();
        ev.stopPropagation();
        setOn(!on);
      });
      menus[i].insertBefore(row, menus[i].firstChild);
      return;
    }
  }
  setInterval(function () {
    ensureMenu();
    if (on) paint();
  }, 800);
})();
