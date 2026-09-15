/* jh-chart-etf-desk.js — Massive ETF Global on the Superchart.
   HUD chips, creation/redemption markers, flow oscillator, stock look-through. */
(function () {
  if (window.__jhChartEtfDesk) return;
  window.__jhChartEtfDesk = true;
  var F = null;
  var cache = {};
  var lastSym = "";
  var lastPaintAt = 0;

  function fuse() { return window.JHEtfFuse; }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>]/g, function (c) {
      return ({ "&": "&", "<": "<", ">": ">" })[c];
    });
  }
  function hud() {
    var n = document.getElementById("etfhud");
    if (n) return n;
    var q = document.getElementById("quote");
    if (!q || !q.parentNode) return null;
    n = document.createElement("div");
    n.id = "etfhud";
    q.parentNode.insertBefore(n, q.nextSibling);
    return n;
  }
  function clsFlow(v) {
    if (v == null) return "";
    return v > 0 ? "in" : v < 0 ? "out" : "";
  }

  function paintHud(pack) {
    var el = hud();
    if (!el) return;
    if (!pack || !pack.ok) { el.className = ""; el.innerHTML = ""; return; }
    var F = fuse();
    if (!F) return;
    var r = pack.row || {};
    var html = "";
    if (pack.kind === "etf") {
      html += "<span class='pill " + clsFlow(r.flow_1d) + "'>" + esc(r.flow_label || (r.flow_1d > 0 ? "INFLOW" : r.flow_1d < 0 ? "OUTFLOW" : "ETF")) + "</span>";
      html += "<span>AUM <b>" + F.fmtUsd(r.aum) + "</b></span>";
      html += "<span>ER <b>" + F.fmtEr(r.er) + "</b></span>";
      html += "<span>1D <b class='" + (r.flow_1d >= 0 ? "up" : "dn") + "'>" + F.fmtUsd(r.flow_1d) + "</b></span>";
      html += "<span>5D <b class='" + (r.flow_5d >= 0 ? "up" : "dn") + "'>" + F.fmtUsd(r.flow_5d) + "</b></span>";
      html += "<span>21D <b class='" + (r.flow_21d >= 0 ? "up" : "dn") + "'>" + F.fmtUsd(r.flow_21d) + "</b></span>";
      if (r.issuer) html += "<span>" + esc(r.issuer) + "</span>";
      if (r.benchmark) html += "<span title='benchmark'>" + esc(r.benchmark) + "</span>";
      if (r.nav != null) html += "<span>NAV <b>" + Number(r.nav).toFixed(2) + "</b></span>";
      var sec = (r.sector || []).slice(0, 3).map(function (x) {
        return esc(x.k) + " " + ((x.w < 1 ? x.w * 100 : x.w).toFixed(0)) + "%";
      }).join(" · ");
      if (sec) html += "<span>" + sec + "</span>";
      if (r.top && r.top[0]) html += "<span>Top " + esc(r.top[0].t) + " " + F.wgt(r.top[0].w) + "</span>";
      var der = pack.derived || {};
      if (der.hhi != null) html += "<span>HHI <b>" + Number(der.hhi).toFixed(0) + "</b></span>";
      if (der.levered) html += "<span class='pill out'>LEV " + esc(der.leverage_style || (der.levered_amount != null ? ("×" + der.levered_amount) : "2x+")) + "</span>";
      if (der.px_flow) html += "<span class='pill " + (der.px_flow.indexOf("BID") >= 0 || der.px_flow === "ABSORPTION" ? "in" : der.px_flow.indexOf("OFFER") >= 0 || der.px_flow === "DISTRIBUTION" ? "out" : "") + "'>" + esc(der.px_flow.replace(/_/g, " ")) + "</span>";
      if (der.implied_top_1d != null && der.top) html += "<span>Hit " + esc(der.top) + " <b class='" + (der.implied_top_1d >= 0 ? "up" : "dn") + "'>" + F.fmtUsd(der.implied_top_1d) + "</b></span>";
      html += "<button type=button id=etfhud-open>Holdings</button>";
    } else if (pack.kind === "stock") {
      var h = pack.holders || [];
      var dem = pack.demand;
      var derS = pack.derived || {};
      html += "<span class='pill " + clsFlow(dem) + "'>ETF LOOK-THROUGH</span>";
      html += "<span>Implied 1D <b class='" + (dem >= 0 ? "up" : "dn") + "'>" + F.fmtUsd(dem) + "</b></span>";
      if (derS.implied_5d != null) html += "<span>Implied 5D <b class='" + (derS.implied_5d >= 0 ? "up" : "dn") + "'>" + F.fmtUsd(derS.implied_5d) + "</b></span>";
      if (derS.crowding_pct != null) html += "<span>Crowding <b>" + Number(derS.crowding_pct).toFixed(1) + "%</b></span>";
      if (derS.confirmed) html += "<span class='pill in'>CONFIRMED</span>";
      if (derS.disagreed) html += "<span class='pill out'>DISAGREED</span>";
      if (derS.flow_type) html += "<span>" + esc(derS.flow_type.replace(/_/g, " ")) + "</span>";
      html += "<span>Held by " + h.length + " desk fund" + (h.length === 1 ? "" : "s") + "</span>";
      h.slice(0, 6).forEach(function (x) {
        html += "<span><a href='/chart.html?s=" + encodeURIComponent(x.etf) + "'>" + esc(x.etf) + "</a> " +
          F.wgt(x.w) + " <b class='" + (x.flow_1d >= 0 ? "up" : "dn") + "'>" + F.fmtUsd(x.flow_1d) + "</b></span>";
      });
      html += "<button type=button id=etfhud-open>Holders</button>";
    }
    el.className = "on";
    el.innerHTML = html;
    var btn = document.getElementById("etfhud-open");
    if (btn) btn.onclick = function () {
      if (window.jhOpenDataType) window.jhOpenDataType(btn);
      setTimeout(function () {
        var tab = pack.kind === "etf" ? "etf" : "hold";
        var b = document.querySelector("#dttabs [data-t='" + tab + "']");
        if (b) b.click();
      }, 80);
    };
  }

  function store(sym, pack) {
    cache[sym] = pack;
    window.jhEtfPack = pack;
    window.jhEtfFlowSeries = function (bars) {
      var F = fuse();
      if (!F || !pack || pack.kind !== "etf") return [];
      return F.alignHist(pack.row && pack.row.flow_hist, bars || window.lastBars || []);
    };
    window.jhEtfFlowMarks = function (bars) {
      var F = fuse();
      if (!F || !pack || pack.kind !== "etf") return [];
      return F.markers(pack.row && pack.row.flow_hist, bars || window.lastBars || []);
    };
  }

  window.jhEtfFlowReady = function (display) {
    var F = fuse();
    var sym = window.jhActive;
    if (!F || !sym) return Promise.resolve([]);
    var pack = cache[sym];
    if (pack && pack.kind === "etf" && pack.row && pack.row.flow_hist) {
      return Promise.resolve(F.markers(pack.row.flow_hist, display));
    }
    return load(sym).then(function (p) {
      if (!p || p.kind !== "etf") return [];
      return F.markers(p.row && p.row.flow_hist, display);
    });
  };

  function suggestOsc(isEtf) {
    var osc = (window.OSC || []).filter(function (o) { return o.id === "etfflow"; })[0];
    if (!osc) return;
    var dismissed = false;
    try { dismissed = localStorage.getItem("jh-etf-flow-osc") === "off"; } catch (e) {}
    var want = !!(isEtf && !dismissed);
    if (!!osc.on === want) return;
    osc.on = want ? 1 : 0;
    if (window.paint && window.lastBars && window.lastBars.length) {
      try { window.paint(window.lastBars); } catch (e2) {}
    }
  }

  function load(sym) {
    var F = fuse();
    if (!F || !sym) return Promise.resolve(null);
    var t = F.bare(sym);
    return Promise.all([F.of(t), F.live(t), F.reverse(t), F.ofDerived ? F.ofDerived(t) : Promise.resolve(null)]).then(function (pack) {
      var row = F.mergeLive(pack[0], pack[1]);
      var holders = pack[2] || [];
      var kind = F.isFund(row, pack[1]) ? "etf" : (holders.length ? "stock" : "none");
      var out = {
        ok: kind !== "none",
        kind: kind,
        sym: t,
        row: row,
        holders: holders,
        demand: F.impliedDemand(holders),
        derived: pack[3] || null,
        live: pack[1],
        at: Date.now()
      };
      store(sym, out);
      paintHud(out);
      suggestOsc(kind === "etf");
      return out;
    }).catch(function () {
      paintHud(null);
      return null;
    });
  }

  function tick() {
    var s = window.jhActive;
    if (!s) return;
    if (s !== lastSym) {
      lastSym = s;
      load(s);
      return;
    }
    if (Date.now() - lastPaintAt > 15000 && cache[s] && cache[s].at && Date.now() - cache[s].at > 180000) {
      lastPaintAt = Date.now();
      load(s);
    }
  }

  function bindOscOff() {
    document.addEventListener("click", function (e) {
      var t = e.target;
      if (!t || !t.getAttribute) return;
      if (t.getAttribute("data-act") === "x") {
        var pane = t.closest && t.closest(".osc");
        if (pane && pane.getAttribute("data-oid") === "etfflow") {
          try { localStorage.setItem("jh-etf-flow-osc", "off"); } catch (err) {}
        }
      }
    }, true);
  }

  function boot() {
    F = fuse();
    bindOscOff();
    tick();
    setInterval(tick, 700);
    window.addEventListener("load", function () { setTimeout(tick, 400); });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
