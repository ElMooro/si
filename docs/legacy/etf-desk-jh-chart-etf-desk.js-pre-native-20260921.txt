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
      if (r.holdings_n) html += "<span>Holdings <b>" + r.holdings_n + "</b></span>";
      var nHist = r.flow_hist_n || ((r.flow_hist && r.flow_hist.length) || 0);
      if (nHist) html += "<span>Tape <b>" + nHist.toLocaleString() + "</b>" + (r.flow_hist_from ? (" since " + esc(String(r.flow_hist_from).slice(0, 10))) : "") + "</span>";
      var rk = pack.rank && pack.rank.windows;
      if (rk) {
        ["d", "w", "m", "q"].forEach(function (k) {
          var w = rk[k] || {};
          if (w.vs == null && w.rank == null) return;
          var lab = k === "q" ? "3M" : (w.label || k.toUpperCase());
          html += "<span>vs SPX " + lab + " <b class='" + (w.vs >= 0 ? "up" : "dn") + "'>" +
            (w.vs == null ? "—" : ((w.vs >= 0 ? "+" : "") + Number(w.vs).toFixed(2) + "%")) + "</b>" +
            (w.rank != null ? (" #" + w.rank + "/" + w.n) : "") + "</span>";
        });
      }
      var der = pack.derived || {};
      var risk = pack.risk || {};
      if (risk.risk) html += "<span class='pill " + (String(risk.risk).indexOf("ON") >= 0 ? "in" : String(risk.risk).indexOf("OFF") >= 0 ? "out" : "") + "'>" + esc(risk.risk.replace(/_/g, " ")) + "</span>";
      if (risk.fear_greed) html += "<span class='pill " + (/GREED/.test(risk.fear_greed) ? "in" : /FEAR/.test(risk.fear_greed) ? "out" : "") + "'>" + esc(risk.fear_greed.replace(/_/g, " ")) + "</span>";
      if (risk.wrapper) html += "<span class='pill " + (risk.wrapper === "CASHING_IN" ? "in" : risk.wrapper === "CASHING_OUT" ? "out" : "") + "'>" + esc(String(risk.wrapper).replace(/_/g, " ")) + "</span>";
      if (risk.geo) html += "<span class='pill " + (String(risk.geo).indexOf("USA_OVER") >= 0 || String(risk.geo).indexOf("DM_OVER") >= 0 ? "in" : String(risk.geo).indexOf("ABROAD") >= 0 || String(risk.geo).indexOf("EM_OVER") >= 0 || String(risk.geo).indexOf("TRADE_BID") >= 0 ? "out" : "") + "'>" + esc(String(risk.geo).replace(/_/g, " ")) + "</span>";
      if (risk.levered && risk.levered !== "QUIET") html += "<span class='pill " + (/GREED|BULL/.test(risk.levered) ? "in" : /FEAR|BEAR/.test(risk.levered) ? "out" : "") + "'>" + esc(String(risk.levered).replace(/_/g, " ")) + "</span>";
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
      var tab = pack.kind === "etf" ? "etf" : "hold";
      if (window.jhOpenDataTypePanel) window.jhOpenDataTypePanel(tab);
      else if (window.jhOpenDataType) window.jhOpenDataType(btn);
    };
  }

  function hydrateFullHist(sym, pack) {
    var F = fuse();
    if (!F || !F.fullHist || !pack || pack.kind !== "etf") return;
    var t = pack.sym || F.bare(sym);
    var before = (pack.row && pack.row.flow_hist && pack.row.flow_hist.length) || 0;
    F.fullHist(t).then(function (rows) {
      if (!rows || !rows.length) return;
      var cur = cache[sym] || pack;
      if (!cur.row) return;
      var merged = F.mergeHist(cur.row.flow_hist, rows);
      if (merged.length <= before && cur.fullHistAt) return;
      cur.row.flow_hist = merged;
      cur.row.flow_hist_n = merged.length;
      cur.row.flow_hist_from = merged[0] && merged[0].d;
      cur.row.flow_hist_to = merged.length ? merged[merged.length - 1].d : null;
      cur.fullHistAt = Date.now();
      cache[sym] = cur;
      var active = window.jhActive;
      if (active && active !== sym && F.bare(active) !== t) return;
      store(sym, cur);
      paintHud(cur);
      var osc = (window.OSC || []).filter(function (o) { return o.id === "etfflow" && o.on; })[0];
      if (osc && window.paint && window.lastBars && window.lastBars.length && merged.length > before) {
        try { window.paint(window.lastBars); } catch (e) {}
      }
    });
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
    return Promise.all([F.of(t), F.live(t), F.reverse(t), F.ofDerived ? F.ofDerived(t) : Promise.resolve(null), F.derived ? F.derived() : Promise.resolve(null), F.census ? F.census() : Promise.resolve(null)]).then(function (pack) {
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
        risk: (pack[4] && pack[4].verdicts) || {},
        rank: (kind === "etf" && F.rankVs && pack[5]) ? F.rankVs(t, pack[5]) : null,
        live: pack[1],
        at: Date.now()
      };
      store(sym, out);
      paintHud(out);
      suggestOsc(kind === "etf");
      if (kind === "etf") hydrateFullHist(sym, out);
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
