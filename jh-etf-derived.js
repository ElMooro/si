/* jh-etf-derived.js — ten honest products from the $297 ETF Global bundle.
   Fund creations are facts. Name-level $ and CUSIP pressure are inferred. Never "smart money". */
(function (w) {
  "use strict";
  if (w.JHEtfDerived) return;

  var CSS = [
    ".jh-der{font:12.5px/1.45 'IBM Plex Sans',ui-sans-serif,system-ui,sans-serif;color:#d6e0ee;margin:14px 0 22px}",
    ".jh-der *{box-sizing:border-box}",
    ".jh-der a{color:#3fb6dc;text-decoration:none}",
    ".jh-der h2{font:700 11px/1 'IBM Plex Mono',ui-monospace,monospace;letter-spacing:.12em;text-transform:uppercase;color:#7c8aa0;margin:18px 0 8px;padding:0}",
    ".jh-der h2 b{color:#d6e0ee;letter-spacing:0;text-transform:none;font-weight:600;margin-left:8px}",
    ".jh-der .note{color:#7c8aa0;font-size:11.5px;margin:0 0 10px;max-width:920px}",
    ".jh-der .kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin:0 0 12px}",
    ".jh-der .kpi{background:#111722;border:1px solid #1e2836;border-radius:8px;padding:10px 12px}",
    ".jh-der .kpi .l{font:10px 'IBM Plex Mono',ui-monospace,monospace;letter-spacing:.1em;text-transform:uppercase;color:#7c8aa0}",
    ".jh-der .kpi .v{font:700 18px 'IBM Plex Mono',ui-monospace,monospace;margin-top:3px}",
    ".jh-der .kpi .s{font-size:11px;color:#7c8aa0;margin-top:2px}",
    ".jh-der .up{color:#16c784}.jh-der .dn{color:#ea3943}.jh-der .flat{color:#f0a020}",
    ".jh-der .tag{display:inline-block;font:10px/1 'IBM Plex Mono',ui-monospace,monospace;letter-spacing:.06em;padding:3px 7px;border-radius:4px;border:1px solid #1e2836}",
    ".jh-der .tag.ROTATION,.jh-der .tag.RISK_ON,.jh-der .tag.RISK_ON_SOFT,.jh-der .tag.WRAPPER_BID,.jh-der .tag.CONFIRMED_BID,.jh-der .tag.ABSORPTION,.jh-der .tag.GREED,.jh-der .tag.NEUTRAL_GREED,.jh-der .tag.CASHING_IN,.jh-der .tag.SPEC_GREED,.jh-der .tag.BULL_LEVERED_BID,.jh-der .tag.SIZE_ON,.jh-der .tag.STOCKS_OVER_BONDS,.jh-der .tag.CREDIT_OVER_DURATION,.jh-der .tag.USA_OVER_ABROAD,.jh-der .tag.DM_OVER_EM,.jh-der .tag.ACCELERATING,.jh-der .tag.CONFIRMED,.jh-der .tag.TRADE_OFFER_CAPITAL_BID{color:#16c784;border-color:rgba(22,199,132,.35);background:rgba(22,199,132,.08)}",
    ".jh-der .tag.BETA,.jh-der .tag.MIXED,.jh-der .tag.QUIET,.jh-der .tag.NEUTRAL,.jh-der .tag.BALANCED,.jh-der .tag.NO_CLEAN_ROTATION,.jh-der .tag.NO_CLEAN_GEO,.jh-der .tag.STABLE{color:#7c8aa0}",
    ".jh-der .tag.RISK_OFF,.jh-der .tag.RISK_OFF_SOFT,.jh-der .tag.WRAPPER_OFFER,.jh-der .tag.CONFIRMED_OFFER,.jh-der .tag.DISTRIBUTION,.jh-der .tag.EM_STRESS,.jh-der .tag.FEAR,.jh-der .tag.NEUTRAL_FEAR,.jh-der .tag.CASHING_OUT,.jh-der .tag.SPEC_FEAR,.jh-der .tag.BEAR_LEVERED_BID,.jh-der .tag.FLIGHT_TO_MEGA,.jh-der .tag.CREDIT_STRESS,.jh-der .tag.BONDS_OVER_STOCKS,.jh-der .tag.DURATION_OVER_CREDIT,.jh-der .tag.SIZE_OFF,.jh-der .tag.ABROAD_OVER_USA,.jh-der .tag.EM_OVER_DM,.jh-der .tag.SLOWING,.jh-der .tag.DISAGREED,.jh-der .tag.TRADE_BID_CAPITAL_OFFER{color:#ea3943;border-color:rgba(234,57,67,.35);background:rgba(234,57,67,.08)}",
    ".jh-der .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px}",
    ".jh-der .card{background:#111722;border:1px solid #1e2836;border-radius:8px;padding:11px 12px}",
    ".jh-der .card h3{margin:0 0 8px;font:11px 'IBM Plex Mono',ui-monospace,monospace;letter-spacing:.1em;text-transform:uppercase;color:#a8b3c7}",
    ".jh-der table{width:100%;border-collapse:collapse;font:11.5px 'IBM Plex Mono',ui-monospace,monospace}",
    ".jh-der th{text-align:right;color:#7c8aa0;font-weight:500;padding:5px 7px;border-bottom:1px solid #1e2836;font-size:10px;letter-spacing:.06em;text-transform:uppercase}",
    ".jh-der td{padding:5px 7px;border-bottom:1px solid #1e2836;text-align:right;white-space:nowrap}",
    ".jh-der th:first-child,.jh-der td:first-child,.jh-der td.l{text-align:left}",
    ".jh-der .caveat{color:#7c8aa0;font-size:11px;margin-top:12px;border-top:1px dashed #1e2836;padding-top:10px;max-width:920px}"
  ].join("");

  function injectCss() {
    if (document.getElementById("jh-der-css")) return;
    var s = document.createElement("style");
    s.id = "jh-der-css";
    s.textContent = CSS;
    document.head.appendChild(s);
  }
  function F() { return w.JHEtfFuse || {}; }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return "&#" + c.charCodeAt(0) + ";";
    });
  }
  function usd(v) { return F().fmtUsd ? F().fmtUsd(v) : (v == null ? "—" : String(v)); }
  function cls(v) { return v > 0 ? "up" : v < 0 ? "dn" : "flat"; }
  function money(v) { return '<span class="' + cls(v) + '">' + usd(v) + "</span>"; }
  function tag(s) { return s ? '<span class="tag ' + esc(s) + '">' + esc(s.replace(/_/g, " ")) + "</span>" : ""; }
  function tk(t) { return t ? '<a href="/chart.html?s=' + encodeURIComponent(t) + '">' + esc(t) + "</a>" : "—"; }
  function kpi(l, v, s) {
    return '<div class="kpi"><div class="l">' + esc(l) + '</div><div class="v">' + v + '</div><div class="s">' + (s || "") + "</div></div>";
  }
  function tbl(headers, rows) {
    if (!rows || !rows.length) return '<div class="note">none</div>';
    return "<table><thead><tr>" + headers.map(function (h, i) {
      return "<th" + (i === 0 ? " class=l" : "") + ">" + h + "</th>";
    }).join("") + "</tr></thead><tbody>" + rows.join("") + "</tbody></table>";
  }
  function members(sl, n) {
    return ((sl && sl.members) || []).slice(0, n || 8).map(function (m) {
      return "<tr><td class=l>" + tk(m.t) + "</td><td class='" + cls(m.flow_1d) + "'>" + usd(m.flow_1d) +
        "</td><td class='" + cls(m.flow_5d) + "'>" + usd(m.flow_5d) + "</td><td>" + usd(m.aum) + "</td></tr>";
    });
  }
  function names(list) {
    return (list || []).filter(Boolean).slice(0, 12).map(function (r) {
      var t = r.t || r.ticker;
      return "<tr><td class=l>" + tk(t) + "</td><td>" + (r.etf_ownership_pct == null ? "—" : r.etf_ownership_pct.toFixed(1) + "%") +
        "</td><td class='" + cls(r.net_flow_5d_usd) + "'>" + usd(r.net_flow_5d_usd) +
        "</td><td>" + esc(r.flow_type || "") + "</td><td>" + (r.confirmed ? "Y" : r.why ? "DISAGREE" : "—") + "</td></tr>";
    });
  }

  function sectionThematic(d) {
    var v = d.thematic_vs_index || {};
    var s = d.sleeves || {};
    return '<h2>Thematic vs index <b>' + tag(v.verdict) + "</b></h2>" +
      '<p class="note">' + esc(v.note || "") + " Index = SPY/VOO/IVV/VTI/QQQ. Thematic = SMH/SOXX/XBI/ARKK/KRE. Measured fund dollars.</p>" +
      '<div class="kpis">' +
        kpi("Index 5D", money(v.index_5d), "beta wrappers") +
        kpi("Thematic 5D", money(v.thematic_5d), "SMH SOXX XBI ARKK") +
        kpi("Sector 5D", money(v.sector_5d), "XLK…XLC + miners") +
      "</div>" +
      '<div class="grid"><div class="card"><h3>Index members</h3>' + tbl(["Ticker", "1D", "5D", "AUM"], members(s.index_beta)) +
      '</div><div class="card"><h3>Thematic members</h3>' + tbl(["Ticker", "1D", "5D", "AUM"], members(s.thematic)) + "</div></div>";
  }
  function sectionFactor(d) {
    var f = d.factor || {};
    var rows = (f.ranked || []).map(function (r) {
      return "<tr><td class=l>" + esc(r.label) + "</td><td>" + (r.tickers || []).map(tk).join(" ") +
        "</td><td class='" + cls(r.flow_5d) + "'>" + usd(r.flow_5d) + "</td><td>" + (r.pct_aum_5d == null ? "—" : r.pct_aum_5d.toFixed(2) + "%") + "</td></tr>";
    });
    return '<h2>Factor dollars <b>' + tag(f.rotation) + "</b></h2>" +
      '<p class="note">MTUM / VLUE / QUAL / USMV / IWF creations. This is factor rotation in dollars, not a price ratio.</p>' +
      '<div class="card">' + tbl(["Factor", "Vehicles", "Flow 5D", "% AUM"], rows) + "</div>";
  }
  function sectionCredit(d) {
    var c = d.credit_stack || {};
    function sl(x) { return x || {}; }
    return '<h2>Credit / rates / EM <b>' + tag(c.verdict) + "</b></h2>" +
      '<p class="note">' + esc(c.note || "") + " Creations, not OAS. Fallen angels split from HY.</p>" +
      '<div class="kpis">' +
        kpi("HY 5D", money(sl(c.hy).flow_5d), (sl(c.hy).tickers || []).join(" ")) +
        kpi("Fallen 5D", money(sl(c.fallen).flow_5d), "FALN") +
        kpi("IG 5D", money(sl(c.ig).flow_5d), (sl(c.ig).tickers || []).join(" ")) +
        kpi("TLT 5D", money(sl(c.rates_long).flow_5d), "duration") +
        kpi("EM sov 5D", money(sl(c.em_sov).flow_5d), "EMB") +
      "</div>";
  }
  function sectionRisk(d) {
    var x = d.cross_asset || {};
    var w = d.wrapper_intensity || {};
    var L = d.levered_sentiment || {};
    var v = d.verdicts || {};
    function sl(s) { return s || {}; }
    var why = (x.reasons || []).join(" · ");
    var spx = x.spx_family || {};
    var plumbing = spx.plumbing
      ? '<p class="note">' + esc(spx.note || "") + " Gross in " + usd(spx.gross_in) + " / out " + usd(spx.gross_out) + " · net " + usd(spx.net) + ".</p>"
      : "";
    return '<h2>Risk on / off <b>' + tag(v.risk || x.verdict) + "</b> " + tag(v.fear_greed || x.fear_greed) + " " + tag(v.rotation || x.rotation) + " " + tag(v.size || x.size) + "</h2>" +
      '<p class="note">' + esc(x.note || "") + (why ? " · " + esc(why) : "") + "</p>" + plumbing +
      '<div class="kpis">' +
        kpi("Stocks 5D", money(x.equity_5d), "mega + large + small") +
        kpi("Gov 5D", money(x.gov_5d), "Treasuries + T-bills") +
        kpi("Credit 5D", money(x.credit_5d), "IG + junk + fallen") +
        kpi("Mega 5D", money(sl(x.mega).flow_5d), "1D " + usd(sl(x.mega).flow_1d) + " · SPY VOO IVV QQQ") +
        kpi("Large 5D", money(sl(x.large).flow_5d), "1D " + usd(sl(x.large).flow_1d) + " · DIA RSP (not VTI)") +
        kpi("Small 5D", money(sl(x.small).flow_5d), "1D " + usd(sl(x.small).flow_1d) + " · IWM") +
        kpi("Duration 5D", money(sl(x.duration).flow_5d), "TLT IEF GOVT") +
        kpi("T-bills 5D", money(sl(x.t_bills).flow_5d), "BIL SGOV SHY") +
        kpi("Junk 5D", money(sl(x.hy).flow_5d), "HYG JNK USHY") +
        kpi("Fallen 5D", money(sl(x.fallen).flow_5d), "FALN") +
      "</div>" +
      '<p class="note">' + esc(x.rotation_note || "") + "</p>" +
      '<h2>Wrapper bid / offer <b>' + tag(w.verdict) + "</b></h2>" +
      '<p class="note">' + esc(w.note || "") + " Unlevered funds only. Large creations = cashing in. Large redemptions = cashing out.</p>" +
      '<div class="kpis">' +
        kpi("Gross in 1D", money(w.gross_in_1d), "creations") +
        kpi("Gross out 1D", money(w.gross_out_1d), "redemptions") +
        kpi("Net 1D", money(w.net_1d), "unlevered") +
        kpi("Net 5D", money(w.net_5d), usd(w.gross_in_5d) + " in / " + usd(w.gross_out_5d) + " out") +
      "</div>" +
      '<div class="card"><h3>Heavy 1D prints (≥ $1B)</h3>' +
        tbl(["Ticker", "1D", "Label"], (w.heavy_1d || []).map(function (r) {
          return "<tr><td class=l>" + tk(r.t) + "</td><td class='" + cls(r.flow_1d) + "'>" + usd(r.flow_1d) + "</td><td>" + esc(r.flow_label || "") + "</td></tr>";
        })) + "</div>" +
      '<h2>Levered / inverse <b>' + tag(L.verdict) + "</b></h2>" +
      '<p class="note">' + esc(L.note || "") + " " + esc(L.caveat || "") + "</p>" +
      '<div class="kpis">' +
        kpi("Bull 2x/3x 5D", money(sl(L.bull).flow_5d), (sl(L.bull).tickers || []).slice(0, 6).join(" ")) +
        kpi("Bear / inverse 5D", money(sl(L.bear).flow_5d), (sl(L.bear).tickers || []).slice(0, 6).join(" ")) +
        kpi("Vol 5D", money(sl(L.vol).flow_5d), "UVXY SVXY") +
        kpi("Bull − bear 5D", money(L.net_bull_minus_bear_5d), "speculative book") +
      "</div>";
  }
  function sectionGeo(d) {
    var g = d.geo_rotation || {};
    var v = d.verdicts || {};
    var sl = g.sleeves || {};
    function s(k) { return sl[k] || {}; }
    var re = g.real_economy || {};
    function yoy(x) {
      if (!x || x.yoy_pct == null) return "—";
      var n = Number(x.yoy_pct);
      return (n > 0 ? "+" : "") + n.toFixed(1) + "% YoY";
    }
    var rows = (g.countries || []).map(function (c) {
      var w = c.wrapper || {};
      var p = c.ports || {};
      var extra = [];
      if (c.orders && c.orders.yoy_pct != null) extra.push("orders " + yoy(c.orders));
      if (c.exports && c.exports.yoy_pct != null) extra.push("exp " + yoy(c.exports));
      if (c.hot_money && c.hot_money.sum_5d_bn != null) extra.push("board 5d " + c.hot_money.sum_5d_bn + " " + (c.hot_money.unit || ""));
      if (c.copper && c.copper.yoy_pct != null) extra.push("Cu " + yoy(c.copper));
      if (c.trade_vs_capital) extra.push(c.trade_vs_capital.replace(/_/g, " "));
      if (c.bop && c.bop.status === "DEFERRED") extra.push("BOP deferred");
      return "<tr><td class=l>" + esc(c.label) + " " + (c.tickers || []).map(tk).join(" ") +
        "</td><td class='" + cls(w.flow_5d) + "'>" + usd(w.flow_5d) +
        "</td><td>" + (w.pct_aum_5d == null ? "—" : w.pct_aum_5d.toFixed(2) + "%") +
        "</td><td>" + tag(p.verdict) + (p.vs_base_pct == null ? "" : " " + (p.vs_base_pct > 0 ? "+" : "") + p.vs_base_pct + "%") +
        "</td><td>" + tag(c.wrapper_vs_ports || c.trade_vs_capital || "") +
        "</td><td class=l>" + esc(extra.join(" · ")) + "</td></tr>";
    });
    return '<h2>USA / DM / EM / country <b>' + tag(v.geo || g.verdict) + "</b> " + tag(g.specialist || "") + "</h2>" +
      '<p class="note">' + esc(g.note || "") + " " + esc((g.reasons || []).join(" · ")) +
      " Wrapper $ is daily. Ports are AIS. Exports are monthly. BOP is quarterly. Not one number.</p>" +
      '<div class="kpis">' +
        kpi("USA 5D", money(g.usa_5d), "SPY VOO IVV QQQ VTI · 1D " + usd(s("usa").flow_1d)) +
        kpi("Developed 5D", money(g.dm_5d), "EFA IEFA VEA — contains Japan") +
        kpi("Emerging 5D", money(g.em_5d), "EEM VWO") +
        kpi("Japan 5D", money(s("japan").flow_5d), "EWJ") +
        kpi("Korea 5D", money(s("korea").flow_5d), "EWY · exp " + yoy(re.korea_exports)) +
        kpi("Taiwan 5D", money(s("taiwan").flow_5d), "EWT · orders " + yoy(re.taiwan_orders)) +
        kpi("Chile 5D", money(s("chile").flow_5d), "ECH · copper ports") +
        kpi("Peru 5D", money(s("peru").flow_5d), "EPU · Cu " + yoy(re.peru_copper)) +
        kpi("Finland 5D", money(s("finland").flow_5d), "EFNL") +
      "</div>" +
      '<div class="card"><h3>Country tape · wrapper vs ports vs trade</h3>' +
        tbl(["Country", "Wrapper 5D", "% AUM", "Ports", "Agree", "Trade / board"], rows) + "</div>" +
      '<p class="note">' + esc((g.caveats || []).slice(0, 3).join(" ")) +
      ' · <a href="/global-flows.html">BOP</a> · <a href="/portwatch.html">ports</a> · <a href="/hot-money.html">hot money</a> · <a href="/apac.html">APAC board</a> · <a href="/macro-leads.html">Asia leads</a></p>';
  }
  function sectionCrypto(d) {
    var c = d.crypto_wrapper || {};
    var v = (d.verdicts || {}).crypto;
    return '<h2>BTC / ETH wrapper bid <b>' + tag(v) + "</b></h2>" +
      '<p class="note">' + esc(c.note || "") + "</p>" +
      '<div class="kpis">' +
        kpi("BTC spot 5D", money((c.btc_spot || {}).flow_5d), (c.btc_spot && c.btc_spot.tickers || []).join(" ")) +
        kpi("ETH spot 5D", money((c.eth_spot || {}).flow_5d), "ETHA") +
        kpi("BTC futures 5D", money((c.btc_futures || {}).flow_5d), "BITO · not the same as spot") +
        kpi("BTC+ETH 1D", money(c.btc_eth_1d), usd(c.btc_eth_5d) + " 5D") +
      "</div>";
  }
  function sectionLeverage(d) {
    var rows = (d.leverage || []).map(function (r) {
      return "<tr><td class=l>" + tk(r.t) + "</td><td>" + esc(r.leverage_style || "—") + "</td><td>" +
        (r.levered_amount == null ? "—" : r.levered_amount) + "</td><td class='" + cls(r.flow_5d) + "'>" + usd(r.flow_5d) + "</td></tr>";
    });
    return '<h2>Leverage <b>' + ((d.leverage || []).length ? (d.leverage.length + " flagged") : "none on desk") + "</b></h2>" +
      '<p class="note">2x/3x/inverse creations are not 1:1 underlying demand. TBT inflow is not duration bid.</p>' +
      (rows.length ? '<div class="card">' + tbl(["Ticker", "Style", "Amt", "Flow 5D"], rows) + "</div>" : "");
  }
  function sectionHhi(d) {
    var c = d.concentration || {};
    var high = (c.high_hhi || []).slice(0, 10).map(function (r) {
      return "<tr><td class=l>" + tk(r.t) + "</td><td>" + (r.hhi == null ? "—" : r.hhi.toFixed(0)) +
        "</td><td>" + (r.holdings_n || "—") + "</td><td>" + tk(r.top) + "</td><td class='" + cls(r.implied_top_1d) + "'>" + usd(r.implied_top_1d) + "</td></tr>";
    });
    var hit = (c.top_holding_hit || []).slice(0, 10).map(function (r) {
      return "<tr><td class=l>" + tk(r.t) + "</td><td>" + tk(r.top) + "</td><td>" +
        (r.top_w == null ? "—" : (r.top_w * 100).toFixed(1) + "%") + "</td><td class='" + cls(r.implied_top_1d) + "'>" + usd(r.implied_top_1d) + "</td></tr>";
    });
    return '<h2>HHI / concentration</h2><p class="note">' + esc(c.note || "") + "</p>" +
      '<div class="grid"><div class="card"><h3>Highest HHI</h3>' + tbl(["Fund", "HHI", "N", "Top", "Implied 1D"], high) +
      '</div><div class="card"><h3>Hardest hit top holding</h3>' + tbl(["Fund", "Name", "Wgt", "Implied 1D"], hit) + "</div></div>";
  }
  function sectionPx(d) {
    var p = d.price_vs_flow || {};
    function col(k, title) {
      var rows = (p[k] || []).map(function (r) {
        return "<tr><td class=l>" + tk(r.t) + "</td><td class='" + cls(r.flow_5d) + "'>" + usd(r.flow_5d) +
          "</td><td class='" + cls(r.nav_5d_pct) + "'>" + (r.nav_5d_pct == null ? "—" : r.nav_5d_pct.toFixed(2) + "%") + "</td></tr>";
      });
      return '<div class="card"><h3>' + title + "</h3>" + tbl(["Ticker", "Flow 5D", "NAV 5D"], rows) + "</div>";
    }
    return '<h2>Price vs flow</h2><p class="note">' + esc(p.note || "") + "</p>" +
      '<div class="grid">' +
        col("ABSORPTION", "Up on outflow · other bid") +
        col("DISTRIBUTION", "Down on inflow · absorbed") +
        col("CONFIRMED_BID", "Up on inflow") +
        col("CONFIRMED_OFFER", "Down on outflow") +
      "</div>";
  }
  function sectionCrowd(d) {
    return '<h2>Crowding · passive beta</h2>' +
      '<p class="note">% of mcap sitting in the funds we cover. High = most sensitive to SPY/VOO/IVV creations. Inferred from holdings MV.</p>' +
      '<div class="card">' + tbl(["Name", "ETF own %", "Implied 5D", "Type", "Confirm"], names(d.crowding)) + "</div>";
  }
  function sectionConfirm(d) {
    return '<h2>Confirmed vs disagreed</h2>' +
      '<p class="note">Confirmed = flow×weight and reported share-count delta agree. Disagreed = both present and opposite (cash/custom basket or stale file). Still inferred.</p>' +
      '<div class="grid"><div class="card"><h3>Confirmed</h3>' + tbl(["Name", "Own %", "Flow 5D", "Type", "Y"], names(d.confirmed)) +
      '</div><div class="card"><h3>Disagreed</h3>' + tbl(["Name", "Own %", "Flow 5D", "Type", "Flag"], names(d.disagreed)) + "</div></div>";
  }
  function sectionBonds(d) {
    var b = d.bond_lookthrough || {};
    var rows = (b.leaders || []).slice(0, 16).map(function (r) {
      return "<tr><td class=l>" + esc((r.cusip_or_ticker || "").slice(0, 28)) + "</td><td class=l>" +
        esc((r.name || "").slice(0, 28)) + "</td><td>" + tk(r.etf) + "</td><td class='" + cls(r.implied_5d_usd) + "'>" +
        usd(r.implied_5d_usd) + "</td></tr>";
    });
    return '<h2>Bond look-through</h2><p class="note">' + esc(b.note || "") + "</p>" +
      '<div class="card">' + tbl(["CUSIP / ticker", "Name", "Via", "Implied 5D"], rows) + "</div>";
  }

  var VIEWS = {
    all: ["risk", "geo", "thematic", "factor", "credit", "crypto", "leverage", "hhi", "px", "crowd", "confirm", "bonds"],
    etf: ["risk", "geo", "thematic", "factor", "hhi", "leverage", "px"],
    bonds: ["risk", "credit", "bonds", "leverage"],
    crypto: ["crypto", "px"],
    factor: ["factor", "thematic"],
    credit: ["risk", "credit", "bonds"],
    strong: ["risk", "geo", "crowd", "px", "thematic", "crypto"],
    lookthrough: ["crowd", "confirm", "px", "hhi"],
    radar: ["risk", "geo", "thematic", "credit", "crypto", "factor"],
    thematic: ["risk", "geo", "thematic", "hhi", "px"]
  };
  var RENDER = {
    risk: sectionRisk, geo: sectionGeo, thematic: sectionThematic, factor: sectionFactor, credit: sectionCredit,
    crypto: sectionCrypto, leverage: sectionLeverage, hhi: sectionHhi,
    px: sectionPx, crowd: sectionCrowd, confirm: sectionConfirm, bonds: sectionBonds
  };

  function htmlFor(d, view) {
    var keys = VIEWS[view] || VIEWS.all;
    var v = d.verdicts || {};
    var head = '<div class="kpis">' +
      kpi("Risk", tag(v.risk), esc(v.fear_greed || "") + " · " + esc(v.rotation || "")) +
      kpi("Wrapper", tag(v.wrapper), "cashing in / out") +
      kpi("Levered", tag(v.levered), "2x/3x / inverse") +
      kpi("Size", tag(v.size), "mega vs IWM") +
      kpi("Credit", tag(v.credit), "HY vs TLT vs FALN") +
      kpi("BTC wrapper", tag(v.crypto), "IBIT+FBTC+ETHA") +
      kpi("Geo", tag(v.geo), esc(v.geo_specialist || "USA / DM / EM")) +
      "</div>";
    var body = keys.map(function (k) { return RENDER[k] ? RENDER[k](d) : ""; }).join("");
    var cave = '<p class="caveat">Evidence: fund creations = fact. Name/CUSIP dollars = fund flow × weight (inferred). Not institutional volume, not 13F, not a sweep. ' +
      esc((d.generated_at || "").slice(0, 16).replace("T", " ")) + " UTC · v" + esc(d.version || "") +
      ' · <a href="/flow-lookthrough.html">look-through</a> · <a href="/etf.html">ETF desk</a></p>';
    return head + body + cave;
  }

  function mount(el, view) {
    if (!el) return Promise.resolve(null);
    injectCss();
    el.className = (el.className ? el.className + " " : "") + "jh-der";
    el.innerHTML = '<p class="note">Loading ETF Global derived desk…</p>';
    var loader = F().derived ? F().derived() : fetch("/data/etf-derived.json?t=" + Date.now()).then(function (r) { return r.json(); });
    return loader.then(function (d) {
      if (!d || d.status === "EMPTY" || (!d.verdicts && !d.thematic_vs_index)) {
        el.innerHTML = '<p class="note">Derived desk not harvested yet. Next etf-global-desk run writes data/etf-derived.json.</p>';
        return d;
      }
      el.innerHTML = htmlFor(d, view || el.getAttribute("data-etf-derived") || "all");
      return d;
    }).catch(function () {
      el.innerHTML = '<p class="note">Derived desk unavailable.</p>';
      return null;
    });
  }

  function boot() {
    injectCss();
    document.querySelectorAll("[data-etf-derived]").forEach(function (el) {
      mount(el, el.getAttribute("data-etf-derived"));
    });
  }

  w.JHEtfDerived = { mount: mount, htmlFor: htmlFor, boot: boot };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})(window);
