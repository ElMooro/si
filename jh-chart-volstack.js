/* Persistent Vol stack — glance line under the quote. Click opens Inst vol. */
(function () {
  if (window.__jhVolStack) return;
  window.__jhVolStack = true;
  var lastSym = "";
  var lastAt = 0;
  var cache = {};

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>]/g, function (c) {
      return ({ "&": "&", "<": "<", ">": ">" })[c];
    });
  }
  function el() {
    var n = document.getElementById("volhud");
    if (n) return n;
    var q = document.getElementById("etfhud") || document.getElementById("quote");
    if (!q || !q.parentNode) return null;
    n = document.createElement("div");
    n.id = "volhud";
    q.parentNode.insertBefore(n, q.nextSibling);
    return n;
  }
  function pillCls(h, conf) {
    if (h === "CONFIRMED") return /DIST/.test(String((conf && conf.verdict) || "")) || (conf && conf.a < 0) ? "out" : "in";
    if (h === "DIVERGENT" || h === "NOT SIZE") return "out";
    return "";
  }
  function stShort(s) {
    if (s === "ACCUMULATION") return "ACCUM";
    if (s === "DISTRIBUTION") return "DIST";
    return s || "—";
  }
  function paint(row) {
    var n = el();
    if (!n) return;
    if (!row) { n.className = ""; n.innerHTML = ""; return; }
    var V = window.JHInstVol;
    var h = row.headline || (V && V.headline(row.conf)) || "NO READ";
    var ats = row.ats || {};
    var inst = row.inst || {};
    var vs = row.vs_spy || {};
    var html = "<span class='pill " + pillCls(h, row.conf) + "' title='" + esc((row.conf && row.conf.why) || "") + "'>" + esc(h) + "</span>";
    if (vs.rel != null) html += "<span title='daily RVOL vs SPY daily'>" + Number(vs.rel).toFixed(1) + "× SPY <b>" + esc(vs.read || "") + "</b></span>";
    else if (vs.name_rvol != null) html += "<span>" + Number(vs.name_rvol).toFixed(1) + "×</span>";
    if (ats.dark_pool_pct != null) {
      html += "<span>ATS <b>" + Number(ats.dark_pool_pct).toFixed(0) + "%</b> " + esc(stShort(ats.state)) + "</span>";
    }
    if (row.ping) html += "<span class='pill out'>ping</span>";
    var net = (Number(inst.bought_usd) || 0) - (Number(inst.sold_usd) || 0);
    if (inst && (inst.bought_usd != null || inst.sold_usd != null)) {
      var signed = V && V.fmtUsd ? V.fmtUsd(net) : String(net);
      html += "<span>13F <b class='" + (net >= 0 ? "up" : "dn") + "'>" + esc(signed) + "</b></span>";
    }
    html += "<button type=button id=volhud-open>Inst vol</button>";
    n.className = "on";
    n.innerHTML = html;
    n.title = "Vol stack — labeled clocks, not a blended score. Click for the desk.";
    n.onclick = function (e) {
      if (e.target && e.target.id === "volhud-open") return;
      if (window.jhOpenDataTypePanel) window.jhOpenDataTypePanel("ivol");
    };
    var btn = document.getElementById("volhud-open");
    if (btn) btn.onclick = function (e) {
      e.stopPropagation();
      if (window.jhOpenDataTypePanel) window.jhOpenDataTypePanel("ivol");
    };
  }

  function load(sym) {
    var V = window.JHInstVol;
    if (!V || !V.stack || !sym) return;
    var token = sym;
    V.stack(sym).then(function (row) {
      if (window.jhActive !== token && window.jhActive !== sym) return;
      cache[sym] = row;
      lastAt = Date.now();
      paint(row);
    }).catch(function () { paint({ headline: "NO READ", conf: {} }); });
  }

  function tick() {
    var s = window.jhActive;
    if (!s) return;
    if (s !== lastSym) {
      lastSym = s;
      var n = el();
      if (n) { n.className = "on"; n.innerHTML = "<span class=pill>Vol…</span>"; }
      load(s);
      return;
    }
    if (Date.now() - lastAt > 180000) load(s);
  }

  function boot() {
    tick();
    setInterval(tick, 700);
    window.addEventListener("load", function () { setTimeout(tick, 500); });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
