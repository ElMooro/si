/* jh-reskin-skip */
/* symbol.html — TradingView-style overview for a stock or fund (2026-10-06).
 * Every number comes from a named source and is shown with its as-of date; missing data shows "—", never an estimate.
 *   price history    data proxy /ohlc (Polygon, Yahoo supplement)       key stats      /fundamentals, /fmp ratios-ttm, key-metrics-ttm, profile, shares-float
 *   statements       /fmp income/balance/cash-flow (annual, quarterly)  ETF profile    /fmp etf/info, etf/holdings, sector & country weightings
 *   30-day changes   /data/watchlist-insights (ETF constituent snapshots) 13F           /fmp institutional summary (all filers) + 15 tracked managers
 *   ETF holders      /fmp etf/asset-exposure                            ETF flows      /data/etf-flow-hist (ETF Global fund flows)
 */
(function () {
  "use strict";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var qs = new URLSearchParams(location.search), T = String(qs.get("s") || qs.get("symbol") || "AAPL").toUpperCase().replace(/^[A-Z]+:/, "").replace(/[^A-Z0-9.\-]/g, "").slice(0, 12) || "AAPL";
  var $ = function (id) { return document.getElementById(id); };
  var head = $("sp-head"), tabsEl = $("sp-tabs"), body = $("sp-body"), foot = $("sp-foot");
  var C = {}; // fetch cache
  function getJ(u) { if (!C[u]) C[u] = fetch(u).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }); return C[u]; }
  function fmp(ep, extra) { return getJ(PROXY + "/fmp?ep=" + ep + "&symbol=" + encodeURIComponent(T) + (extra || "")).then(function (d) { return d && d.data != null ? d.data : null; }); }
  function first(a) { return Array.isArray(a) ? a[0] || null : a || null; }
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function ok(v) { return v != null && v !== "" && isFinite(+v); }
  function big(v, cur) { if (!ok(v)) return "—"; v = +v; var a = Math.abs(v), s = v < 0 ? "−" : ""; return s + (cur === false ? "" : "$") + (a >= 1e12 ? (a / 1e12).toFixed(2) + "T" : a >= 1e9 ? (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? (a / 1e6).toFixed(2) + "M" : a >= 1e3 ? (a / 1e3).toFixed(1) + "K" : a.toFixed(2)); }
  function n2(v, suf) { return ok(v) ? (+v).toFixed(2) + (suf || "") : "—"; }
  function pctF(v) { return ok(v) ? (v * 100).toFixed(2) + "%" : "—"; } // fraction → %
  function sg(v, txt) { return '<span class="' + (v > 0 ? "up" : v < 0 ? "dn" : "") + '">' + txt + "</span>"; }
  function sp(v, dp) { return ok(v) ? sg(v, (v > 0 ? "+" : "") + (+v).toFixed(dp == null ? 2 : dp) + "%") : "—"; }
  function cnt(v) { return ok(v) ? Math.round(v).toLocaleString("en-US") : "—"; }
  function shardOf(t) { return (t + "__").slice(0, 2).replace(/[^A-Z0-9]/g, "_"); }
  function lastCompleteQuarter(back) {
    var d = new Date(Date.now() - 46 * 864e5), q = Math.floor(d.getUTCMonth() / 3) - 1 - (back || 0), y = d.getUTCFullYear();
    while (q < 0) { q += 4; y--; }
    return { year: y, quarter: q + 1 };
  }

  // ------------------------------------------------------------------ data
  var D = {};
  function core() {
    if (D.core) return D.core;
    D.core = Promise.all([
      getJ(PROXY + "/fundamentals?ticker=" + encodeURIComponent(T)),
      fmp("profile"), fmp("etf/info"),
      getJ(PROXY + "/data/watchlist-insights/etf/" + T + ".json"),
      getJ(PROXY + "/data/etf-flow-hist/" + T + ".json")
    ]).then(function (a) {
      var f = a[0] && !a[0].error ? a[0] : null, p = first(a[1]), e = first(a[2]);
      return { f: f, p: p, etfInfo: e, etfIns: a[3], flows: a[4] && a[4].d ? a[4] : null, isEtf: !!(e && e.holdingsCount != null) || !!a[3] || !!(f && (f.isEtf || f.isFund)) || !!(p && (p.isEtf || p.isFund)) };
    });
    return D.core;
  }

  // ------------------------------------------------------------------ header
  function renderHead(c) {
    var f = c.f || {}, p = c.p || {}, e = c.etfInfo || {};
    var name = p.companyName || f.name || e.name || T, logo = p.image || f.image;
    var chg = ok(p.change) ? +p.change : null, chp = ok(p.changePercentage) ? +p.changePercentage : ok(f.changesPct) ? +f.changesPct : null, px = ok(p.price) ? +p.price : f.price;
    document.title = T + " — " + name + " · JustHodl.AI";
    head.innerHTML =
      '<div class="sp-logo">' + (logo ? '<img src="' + esc(logo) + '" alt="" onerror="this.remove()">' : esc(T.charAt(0))) + "</div>" +
      '<div class="sp-id"><h1>' + esc(name) + '</h1><div class="tk"><b>' + esc(T) + "</b> · " + esc(p.exchangeFullName || f.exchange || e.domicile || "") + (c.isEtf ? " · ETF" + (e.etfCompany ? " · " + esc(e.etfCompany) : "") : (p.sector || f.sector ? " · " + esc(p.sector || f.sector) + (p.industry || f.industry ? " / " + esc(p.industry || f.industry) : "") : "")) + "</div></div>" +
      '<div class="sp-px">' + (ok(px) ? '<span class="p">' + (+px).toLocaleString("en-US", { maximumFractionDigits: 2, minimumFractionDigits: 2 }) + "</span>" + '<span class="c">' + (chg != null ? sg(chg, (chg > 0 ? "+" : "") + chg.toFixed(2)) : "") + " " + (chp != null ? sg(chp, "(" + (chp > 0 ? "+" : "") + chp.toFixed(2) + "%)") : "") + "</span>" : '<span class="mu">No price</span>') +
      '<div class="a">' + esc(p.currency || "USD") + " · FMP quote" + (ok(p.volume) ? " · vol " + big(p.volume, false) : "") + "</div></div>" +
      '<div class="sp-actions"><a class="sp-btn pri" href="/chart.html?s=' + encodeURIComponent(T) + '" style="display:inline-flex;align-items:center;text-decoration:none">Open chart</a>' + (p.website || f.website ? '<a class="sp-btn" href="' + esc(p.website || f.website) + '" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;text-decoration:none">Website ↗</a>' : "") + "</div>";
  }

  // the warehouse quote (the same one the chart tab strip shows) replaces the FMP profile price when it answers in time
  function liveQuote() {
    var ctl = window.AbortController ? new AbortController() : null; setTimeout(function () { if (ctl) ctl.abort(); }, 7000);
    fetch(PROXY + "/quote?ids=" + encodeURIComponent(T), ctl ? { signal: ctl.signal } : undefined).then(function (r) { return r.ok ? r.json() : null; }).then(function (d) {
      var q = d && d.quotes ? d.quotes[T] || d.quotes[Object.keys(d.quotes)[0]] : null, el = head.querySelector(".sp-px");
      if (!q || !q.ok || !ok(q.last) || !el) return;
      el.innerHTML = '<span class="p">' + (+q.last).toLocaleString("en-US", { maximumFractionDigits: 2, minimumFractionDigits: 2 }) + '</span><span class="c">' + (ok(q.chg) ? sg(q.chg, (q.chg > 0 ? "+" : "") + (+q.chg).toFixed(2)) : "") + " " + (ok(q.chg_pct) ? sg(q.chg_pct, "(" + (q.chg_pct > 0 ? "+" : "") + (+q.chg_pct).toFixed(2) + "%)") : "") + '</span><div class="a">' + esc(q.unit || "USD") + " · as of " + esc(q.last_date || "") + (q.prev_date ? " vs " + esc(q.prev_date) : "") + " · JustHodl warehouse quote</div>";
    }).catch(function () {});
  }

  // ------------------------------------------------------------------ price chart (SVG, full history)
  function priceChart(host) {
    host.innerHTML = '<div class="sp-chart"><div class="sp-rng">' + ["1M", "6M", "YTD", "1Y", "5Y", "All"].map(function (r) { return '<button type="button" data-r="' + r + '"' + (r === "1Y" ? ' class="on"' : "") + ">" + r + "</button>"; }).join("") + '<span class="mu" style="margin-left:auto;font-size:12px" data-k="src"></span></div><div data-k="svg" class="mu" style="height:260px">Loading price history…</div><div class="sp-tip"></div></div>';
    getJ(PROXY + "/ohlc?ticker=" + encodeURIComponent(T) + "&tf=1d").then(function (d) {
      var bars = d && Array.isArray(d.bars) ? d.bars.filter(function (b) { return ok(b.close); }) : [];
      var box = host.querySelector('[data-k="svg"]');
      if (!bars.length) { box.textContent = "No price history available for " + T + "."; return; }
      host.querySelector('[data-k="src"]').textContent = new Date(bars[0].time * 1000).toISOString().slice(0, 10) + " → " + new Date(bars[bars.length - 1].time * 1000).toISOString().slice(0, 10) + " · " + bars.length.toLocaleString() + " daily bars";
      function draw(r) {
        var end = bars[bars.length - 1].time, from = r === "All" ? 0 : r === "YTD" ? Date.UTC(new Date(end * 1000).getUTCFullYear(), 0, 1) / 1000 : end - { "1M": 31, "6M": 183, "1Y": 366, "5Y": 1827 }[r] * 86400;
        var b = bars.filter(function (x) { return x.time >= from; }); if (b.length < 2) b = bars.slice(-2);
        var W = 1100, H = 260, lo = Infinity, hi = -Infinity; b.forEach(function (x) { lo = Math.min(lo, x.close); hi = Math.max(hi, x.close); });
        var pad = (hi - lo) * 0.06 || 1; lo -= pad; hi += pad;
        var X = function (i) { return i / (b.length - 1) * (W - 60); }, Y = function (v) { return H - 20 - (v - lo) / (hi - lo) * (H - 30); };
        var up = b[b.length - 1].close >= b[0].close, col = up ? "#089981" : "#f23645";
        var path = b.map(function (x, i) { return (i ? "L" : "M") + X(i).toFixed(1) + " " + Y(x.close).toFixed(1); }).join("");
        var ticks = [0, 0.25, 0.5, 0.75, 1].map(function (k) { var v = lo + (hi - lo) * k; return '<text x="' + (W - 54) + '" y="' + (Y(v) + 4).toFixed(1) + '" fill="#787b86" font-size="11">' + v.toFixed(v > 100 ? 0 : 2) + '</text><line x1="0" x2="' + (W - 60) + '" y1="' + Y(v).toFixed(1) + '" y2="' + Y(v).toFixed(1) + '" stroke="#2a2e39"/>'; }).join("");
        var chg = (b[b.length - 1].close / b[0].close - 1) * 100;
        box.innerHTML = '<svg viewBox="0 0 ' + W + " " + H + '" preserveAspectRatio="none" role="img" aria-label="' + T + " price, " + r + '">' + ticks +
          '<path d="' + path + "L" + X(b.length - 1).toFixed(1) + " " + (H - 20) + "L0 " + (H - 20) + 'Z" fill="' + col + '" fill-opacity=".12"/><path d="' + path + '" fill="none" stroke="' + col + '" stroke-width="1.6"/>' +
          '<text x="4" y="' + (H - 4) + '" fill="#787b86" font-size="11">' + new Date(b[0].time * 1000).toISOString().slice(0, 10) + '</text><text x="' + (W - 130) + '" y="' + (H - 4) + '" fill="#787b86" font-size="11">' + new Date(b[b.length - 1].time * 1000).toISOString().slice(0, 10) + "</text>" +
          '<line data-k="cx" x1="0" x2="0" y1="0" y2="' + (H - 20) + '" stroke="#787b86" stroke-dasharray="3 3" visibility="hidden"/></svg>' +
          '<div class="mu" style="font-size:12px;margin-top:2px">' + r + " change " + sp(chg) + "</div>";
        var svg = box.querySelector("svg"), tip = host.querySelector(".sp-tip"), cx = svg.querySelector('[data-k="cx"]');
        svg.onmousemove = function (ev) {
          var rc = svg.getBoundingClientRect(), fx = (ev.clientX - rc.left) / rc.width * W, i = Math.max(0, Math.min(b.length - 1, Math.round(fx / (W - 60) * (b.length - 1))));
          cx.setAttribute("x1", X(i)); cx.setAttribute("x2", X(i)); cx.setAttribute("visibility", "visible");
          tip.style.display = "block"; tip.style.left = Math.min(rc.width - 150, ev.clientX - rc.left + 12) + "px"; tip.style.top = "40px";
          tip.innerHTML = new Date(b[i].time * 1000).toISOString().slice(0, 10) + " · <b>" + (+b[i].close).toFixed(2) + "</b>";
        };
        svg.onmouseleave = function () { tip.style.display = "none"; cx.setAttribute("visibility", "hidden"); };
      }
      host.querySelector(".sp-rng").onclick = function (e) { var x = e.target.closest("[data-r]"); if (!x) return; host.querySelectorAll(".sp-rng button").forEach(function (y) { y.classList.toggle("on", y === x); }); draw(x.getAttribute("data-r")); };
      draw("1Y");
    });
  }

  // ------------------------------------------------------------------ tabs
  var TABS = [];
  function overview(c) {
    var f = c.f || {}, p = c.p || {}, e = c.etfInfo || {};
    body.innerHTML = '<div data-k="chart"></div><h2>Key statistics <span data-k="asof"></span></h2><div class="sp-grid" data-k="stats"><div class="mu">Loading…</div></div><div data-k="more"></div>';
    priceChart(body.querySelector('[data-k="chart"]'));
    Promise.all([fmp("ratios-ttm"), fmp("key-metrics-ttm"), fmp("shares-float"), c.isEtf ? null : fmp("grades-consensus"), c.isEtf ? fmp("dividends") : null]).then(function (a) {
      var r = first(a[0]) || {}, k = first(a[1]) || {}, sf = first(a[2]) || {}, g = first(a[3]);
      // funds: trailing-12-month distributions / price when the profile carries no yield
      if (c.isEtf && f.dividendYield == null && Array.isArray(a[4]) && a[4].length && ok(p.price || f.price)) {
        var cut = Date.now() - 365 * 864e5, ttm = a[4].filter(function (x) { return Date.parse(x.date) > cut; }).reduce(function (s0, x) { return s0 + (+x.dividend || 0); }, 0);
        if (ttm > 0) f = Object.assign({}, f, { dividendYield: ttm / (p.price || f.price) });
      }
      var rows = c.isEtf ? [
        ["AUM", big(e.assetsUnderManagement)], ["NAV", ok(e.nav) ? (+e.nav).toFixed(2) + " " + esc(e.navCurrency || "") : "—"], ["Expense ratio", ok(e.expenseRatio) ? (+e.expenseRatio).toFixed(2) + "%" : "—"],
        ["Holdings", cnt(e.holdingsCount)], ["Asset class", esc(e.assetClass || "—")], ["Inception", esc(e.inceptionDate || "—")],
        ["Dividend yield (TTM)", pctF(f.dividendYield)], ["P/E (holdings)", n2(f.pe)], ["Beta", n2(p.beta || f.beta)], ["52-week range", esc(p.range || (ok(f.yearLow) ? f.yearLow + "–" + f.yearHigh : "—"))], ["Avg volume", big(e.avgVolume || p.averageVolume, false)], ["Issuer", esc(e.etfCompany || "—")]
      ] : [
        ["Market cap", big(p.marketCap || f.marketCap)], ["P/E (TTM)", n2(r.priceToEarningsRatioTTM != null ? r.priceToEarningsRatioTTM : f.pe)], ["P/S (TTM)", n2(r.priceToSalesRatioTTM != null ? r.priceToSalesRatioTTM : f.ps)],
        ["PEG (TTM)", n2(r.priceToEarningsGrowthRatioTTM != null ? r.priceToEarningsGrowthRatioTTM : f.peg)], ["Forward PEG", n2(r.forwardPriceToEarningsGrowthRatioTTM)], ["Dividend yield", pctF(r.dividendYieldTTM != null ? r.dividendYieldTTM : f.dividendYield)],
        ["P/B", n2(r.priceToBookRatioTTM != null ? r.priceToBookRatioTTM : f.pb)], ["P/FCF", n2(r.priceToFreeCashFlowRatioTTM)], ["EV/EBITDA", n2(k.evToEBITDATTM != null ? k.evToEBITDATTM : f.evToEbitda)],
        ["EV/Sales", n2(k.evToSalesTTM != null ? k.evToSalesTTM : f.evToSales)], ["Enterprise value", big(k.enterpriseValueTTM)], ["EPS (TTM)", n2(r.netIncomePerShareTTM)],
        ["Gross margin", pctF(r.grossProfitMarginTTM != null ? r.grossProfitMarginTTM : f.grossMargin)], ["Operating margin", pctF(r.operatingProfitMarginTTM != null ? r.operatingProfitMarginTTM : f.opMargin)], ["Net margin", pctF(r.netProfitMarginTTM != null ? r.netProfitMarginTTM : f.netMargin)],
        ["ROE", pctF(k.returnOnEquityTTM != null ? k.returnOnEquityTTM : f.roe)], ["ROIC", pctF(k.returnOnInvestedCapitalTTM)], ["FCF yield", pctF(k.freeCashFlowYieldTTM != null ? k.freeCashFlowYieldTTM : f.fcfYield)],
        ["Debt/Equity", n2(r.debtToEquityRatioTTM != null ? r.debtToEquityRatioTTM : f.debtToEquity)], ["Current ratio", n2(r.currentRatioTTM != null ? r.currentRatioTTM : f.currentRatio)], ["Payout ratio", pctF(r.dividendPayoutRatioTTM)],
        ["Beta", n2(p.beta || f.beta)], ["52-week range", esc(p.range || (ok(f.yearLow) ? f.yearLow + "–" + f.yearHigh : "—"))], ["Shares outstanding", big(sf.outstandingShares, false)],
        ["Free float", ok(sf.freeFloat) ? (+sf.freeFloat).toFixed(2) + "%" : "—"], ["Employees", ok(p.fullTimeEmployees || f.employees) ? cnt(+(p.fullTimeEmployees || f.employees)) : "—"], ["IPO date", esc(p.ipoDate || f.ipoDate || "—")]
      ];
      body.querySelector('[data-k="stats"]').innerHTML = rows.map(function (x) { return "<div><small>" + x[0] + "</small><b>" + x[1] + "</b></div>"; }).join("");
      body.querySelector('[data-k="asof"]').textContent = c.isEtf ? "FMP ETF profile · updated " + String(e.updatedAt || "").slice(0, 10) : "TTM ratios and key metrics · Financial Modeling Prep";
      var more = "";
      if (g) {
        var tot = (g.strongBuy || 0) + (g.buy || 0) + (g.hold || 0) + (g.sell || 0) + (g.strongSell || 0);
        more += '<h2>Analyst ratings <span>' + tot + " ratings · consensus " + esc(g.consensus || "—") + (f.analystPT && f.analystPT.avg ? " · avg price target " + n2(f.analystPT.avg) + " (" + f.analystPT.n + ")" : "") + '</span></h2><div class="sp-card">' +
          [["Strong buy", g.strongBuy, "#089981"], ["Buy", g.buy, "#26a69a"], ["Hold", g.hold, "#787b86"], ["Sell", g.sell, "#ef5350"], ["Strong sell", g.strongSell, "#f23645"]].map(function (x) { return '<div class="sp-bar"><span>' + x[0] + '</span><i style="width:' + (tot ? (x[1] || 0) / tot * 60 : 0) + "%;background:" + x[2] + '"></i><em>' + (x[1] || 0) + "</em></div>"; }).join("") + "</div>";
      }
      if (f.estimates && f.estimates.length) more += '<h2>Estimates <span>analyst consensus</span></h2><div class="sp-tw"><table><tr><th>Fiscal year</th><th>Revenue (avg)</th><th>EPS (avg)</th></tr>' + f.estimates.map(function (x) { return "<tr><td>" + esc(x.year) + "</td><td>" + big(x.revenueAvg) + "</td><td>" + n2(x.epsAvg) + "</td></tr>"; }).join("") + "</table></div>";
      var desc = (c.isEtf ? e.description : p.description) || f.description;
      if (desc) more += "<h2>About " + esc(T) + '</h2><p class="sp-desc">' + esc(desc) + "</p>" + (p.ceo || f.ceo ? '<p class="mu">CEO: ' + esc(p.ceo || f.ceo) + (p.city ? " · " + esc(p.city) + ", " + esc(p.state || p.country || "") : "") + "</p>" : "");
      body.querySelector('[data-k="more"]').innerHTML = more;
    });
  }

  // ------------------------------------------------------------------ statements (TradingView "Financials → Statements" layout)
  // rows: [key or [fallback keys], label, children]; values shown with the change vs the prior period beneath
  var ST = {
    income: { ep: "income-statement", label: "Income statement", def: ["revenue", "netIncome"], rows: [
      ["revenue", "Total revenue", [["costOfRevenue", "Cost of goods sold"]]],
      ["grossProfit", "Gross profit"],
      ["operatingExpenses", "Operating expenses (excl. COGS)", [["researchAndDevelopmentExpenses", "Research & development"], ["sellingGeneralAndAdministrativeExpenses", "Selling, general & admin"], ["otherExpenses", "Other operating expenses"]]],
      ["operatingIncome", "Operating income"],
      ["totalOtherIncomeExpensesNet", "Non-operating income, total", [["interestIncome", "Interest income"], ["interestExpense", "Interest expense"], ["nonOperatingIncomeExcludingInterest", "Other non-operating income"]]],
      ["incomeBeforeTax", "Pretax income"],
      ["incomeTaxExpense", "Taxes"],
      ["netIncome", "Net income", [["netIncomeFromContinuingOperations", "From continuing operations"], ["netIncomeFromDiscontinuedOperations", "From discontinued operations"]]],
      ["eps", "Basic EPS", null, "x"], ["epsDiluted", "Diluted EPS", null, "x"],
      ["weightedAverageShsOut", "Average basic shares outstanding"], ["weightedAverageShsOutDil", "Diluted shares outstanding"],
      ["ebitda", "EBITDA"], ["ebit", "EBIT"], ["depreciationAndAmortization", "Depreciation & amortization"]
    ] },
    balance: { ep: "balance-sheet-statement", label: "Balance sheet", def: ["totalAssets", "totalLiabilities"], rows: [
      ["totalAssets", "Total assets", [
        ["totalCurrentAssets", "Total current assets", [["cashAndShortTermInvestments", "Cash and short term investments"], ["netReceivables", "Total receivables, net"], ["inventory", "Total inventory"], ["prepaids", "Prepaid expenses"], ["otherCurrentAssets", "Other current assets"]]],
        ["totalNonCurrentAssets", "Total non-current assets", [["propertyPlantEquipmentNet", "Net property, plant & equipment"], ["longTermInvestments", "Long term investments"], ["goodwillAndIntangibleAssets", "Goodwill & intangibles"], ["taxAssets", "Deferred tax assets"], ["otherNonCurrentAssets", "Other non-current assets"]]]]],
      ["totalLiabilities", "Total liabilities", [
        ["totalCurrentLiabilities", "Total current liabilities", [["accountPayables", "Accounts payable"], ["shortTermDebt", "Short term debt"], ["deferredRevenue", "Deferred revenue"], ["taxPayables", "Income tax payable"], ["otherCurrentLiabilities", "Other current liabilities"]]],
        ["totalNonCurrentLiabilities", "Total non-current liabilities", [["longTermDebt", "Long term debt"], ["deferredRevenueNonCurrent", "Deferred revenue (non-current)"], ["deferredTaxLiabilitiesNonCurrent", "Deferred tax liabilities"], ["otherNonCurrentLiabilities", "Other non-current liabilities"]]]]],
      ["totalEquity", "Total equity", [["commonStock", "Common stock"], ["additionalPaidInCapital", "Additional paid-in capital"], ["retainedEarnings", "Retained earnings"], ["accumulatedOtherComprehensiveIncomeLoss", "Other comprehensive income"], ["treasuryStock", "Treasury stock"], ["minorityInterest", "Minority interest"]]],
      ["totalLiabilitiesAndTotalEquity", "Total liabilities & shareholders' equities"],
      ["totalDebt", "Total debt"], ["netDebt", "Net debt"]
    ] },
    cash: { ep: "cash-flow-statement", label: "Cash flow", def: ["operatingCashFlow", "freeCashFlow"], rows: [
      ["operatingCashFlow", "Cash from operating activities", [["netIncome", "Net income"], ["depreciationAndAmortization", "Depreciation & amortization"], ["stockBasedCompensation", "Stock-based compensation"], ["deferredIncomeTax", "Deferred taxes"], ["changeInWorkingCapital", "Changes in working capital"], ["otherNonCashItems", "Other non-cash items"]]],
      ["netCashProvidedByInvestingActivities", "Cash from investing activities", [["capitalExpenditure", "Capital expenditures"], ["acquisitionsNet", "Acquisitions, net"], ["purchasesOfInvestments", "Purchase of investments"], ["salesMaturitiesOfInvestments", "Sale/maturity of investments"], ["otherInvestingActivities", "Other investing activities"]]],
      ["netCashProvidedByFinancingActivities", "Cash from financing activities", [["netDebtIssuance", "Issuance/retirement of debt, net"], ["commonStockRepurchased", "Repurchase of common stock"], ["commonStockIssuance", "Issuance of common stock"], ["netDividendsPaid", "Dividends paid"], ["otherFinancingActivities", "Other financing activities"]]],
      ["netChangeInCash", "Net change in cash"],
      ["freeCashFlow", "Free cash flow"]
    ] }
  };
  var COLORS = ["#2962ff", "#00bcd4", "#ff9800", "#e91e63", "#4caf50", "#9c27b0", "#ffeb3b", "#795548"];
  var MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  function financials(c) {
    if (c.isEtf) { body.innerHTML = '<p class="mu">Funds do not file company financial statements. See Holdings for what ' + esc(T) + " owns.</p>"; return; }
    var st = "income", per = "annual", open = {}, pick = {}, all = false;
    body.innerHTML = '<div data-k="chart" class="fs-chart"></div>' +
      '<div class="fs-bar"><span class="fs-pills" data-k="st">' + Object.keys(ST).map(function (k) { return '<button type="button" data-v="' + k + '"' + (k === st ? ' class="on"' : "") + ">" + ST[k].label + "</button>"; }).join("") + '</span>' +
      '<label class="mu fs-all"><input type="checkbox" data-k="all"> Every reported field</label><span class="sp-seg fs-per" data-k="per"><button type="button" data-v="annual" class="on">Annual</button><button type="button" data-v="quarter">Quarterly</button></span></div>' +
      '<div data-k="tbl" class="mu">Loading…</div>';
    var cols = [], cur = "USD";
    function fmtV(v, kind) { return kind === "x" ? (ok(v) ? (+v).toFixed(2) : "—") : big(v, false); }
    function head(x) { var d = new Date(String(x.date) + "T12:00:00Z"); return '<th><b>' + esc(per === "annual" ? String(x.fiscalYear || String(x.date).slice(0, 4)) : x.period + " '" + String(x.fiscalYear || "").slice(2)) + "</b><small>" + (isNaN(d) ? esc(x.date) : MON[d.getUTCMonth()] + " " + d.getUTCFullYear()) + "</small></th>"; }
    function flat(rows, depth, parent, out) { (rows || []).forEach(function (r) { out.push({ k: r[0], label: r[1], kids: r[2], kind: r[3], depth: depth, parent: parent }); if (r[2] && open[st + ":" + r[0]]) flat(r[2], depth + 1, r[0], out); }); return out; }
    function present(k) { return cols.some(function (x) { return ok(x[k]); }); }
    function table() {
      var S = ST[st], rows = all ? Object.keys(cols[cols.length - 1] || {}).filter(function (k) { return typeof cols[cols.length - 1][k] === "number"; }).map(function (k) { return { k: k, label: k.replace(/([A-Z])/g, " $1").replace(/^./, function (z) { return z.toUpperCase(); }), depth: 0 }; }) : flat(S.rows, 0, null, []).filter(function (r) { return present(r.k); });
      var sel = pick[st] || (pick[st] = S.def.slice());
      var h = '<div class="sp-tw fs-tw"><table class="fs"><thead><tr><th class="fs-m"><span>Metrics</span><small>Currency: ' + esc(cur) + "</small></th>" + cols.map(head).join("") + "</tr></thead><tbody>";
      rows.forEach(function (r) {
        var si = sel.indexOf(r.k), kidsP = r.kids && r.kids.some(function (z) { return present(z[0]); });
        h += '<tr data-row="' + esc(r.k) + '" class="' + (si >= 0 ? "on" : "") + '"' + (si >= 0 ? ' style="--c:' + COLORS[si % COLORS.length] + '"' : "") + '><td class="fs-m" style="padding-left:' + (14 + r.depth * 18) + 'px">' + (kidsP ? '<button type="button" class="fs-tg" data-tg="' + esc(r.k) + '" aria-expanded="' + !!open[st + ":" + r.k] + '">' + (open[st + ":" + r.k] ? "⌄" : "›") + "</button>" : '<span class="fs-sp"></span>') + esc(r.label) + "</td>" +
          cols.map(function (x, i) { var v = x[r.k], pv = i ? cols[i - 1][r.k] : null, g = ok(v) && ok(pv) && +pv !== 0 ? (v - pv) / Math.abs(pv) * 100 : null; return "<td><b>" + fmtV(v, r.kind) + "</b><small" + (g != null ? ' class="' + (g > 0 ? "up" : g < 0 ? "dn" : "") + '"' : "") + ">" + (g != null ? (g > 0 ? "+" : "") + g.toFixed(2) + "%" : i ? "—" : "") + "</small></td>"; }).join("") + "</tr>";
      });
      h += '</tbody></table></div><p class="sp-note">Reported figures in ' + esc(cur) + ", change versus the prior " + (per === "annual" ? "fiscal year" : "quarter") + ". Click a row to chart it (up to 8), › to expand its components. Source: Financial Modeling Prep (company filings" + (cols.length ? ", latest filed " + esc(cols[cols.length - 1].filingDate || cols[cols.length - 1].date) : "") + ").</p>";
      body.querySelector('[data-k="tbl"]').innerHTML = h;
      chart();
    }
    function chart() {
      var sel = (pick[st] || []).filter(function (k) { return present(k); }), host = body.querySelector('[data-k="chart"]');
      if (!sel.length || !cols.length) { host.innerHTML = ""; return; }
      var W = 1100, H = 240, n = cols.length, mx = 0, mn = 0;
      cols.forEach(function (x) { sel.forEach(function (k) { var v = +x[k] || 0; mx = Math.max(mx, v); mn = Math.min(mn, v); }); });
      var rng = mx - mn || 1, gw = (W - 90) / n, bw = Math.min(26, gw / (sel.length + 1)), Y = function (v) { return 12 + (mx - v) / rng * (H - 44); };
      var g = "", t;
      for (t = 0; t <= 4; t++) { var v = mn + rng * t / 4; g += '<line x1="0" x2="' + (W - 80) + '" y1="' + Y(v).toFixed(1) + '" y2="' + Y(v).toFixed(1) + '" stroke="#2a2e39"/><text x="' + (W - 74) + '" y="' + (Y(v) + 4).toFixed(1) + '" fill="#787b86" font-size="11">' + big(v, false) + "</text>"; }
      cols.forEach(function (x, i) {
        sel.forEach(function (k, j) { var v = +x[k] || 0, y0 = Y(Math.max(0, v)), y1 = Y(Math.min(0, v)); g += '<rect x="' + (10 + i * gw + gw / 2 - bw * sel.length / 2 + j * bw).toFixed(1) + '" y="' + y0.toFixed(1) + '" width="' + (bw - 2).toFixed(1) + '" height="' + Math.max(1, y1 - y0).toFixed(1) + '" fill="' + COLORS[(pick[st] || []).indexOf(k) % COLORS.length] + '" rx="1"><title>' + esc(labelOf(k)) + " " + big(v, false) + "</title></rect>"; });
        g += '<text x="' + (10 + i * gw + gw / 2).toFixed(1) + '" y="' + (H - 8) + '" fill="#787b86" font-size="11" text-anchor="middle">' + esc(per === "annual" ? String(x.fiscalYear || String(x.date).slice(0, 4)) : x.period + " '" + String(x.fiscalYear || "").slice(2)) + "</text>";
      });
      host.innerHTML = '<svg viewBox="0 0 ' + W + " " + H + '" role="img" aria-label="selected statement rows">' + '<line x1="0" x2="' + (W - 80) + '" y1="' + Y(0) + '" y2="' + Y(0) + '" stroke="#434651"/>' + g + '</svg><div class="fs-leg">' + sel.map(function (k) { return '<span><i style="background:' + COLORS[(pick[st] || []).indexOf(k) % COLORS.length] + '"></i>' + esc(labelOf(k)) + "</span>"; }).join("") + "</div>";
    }
    function labelOf(k) { var found = k; (function walk(rows) { (rows || []).forEach(function (r) { if (r[0] === k) found = r[1]; if (r[2]) walk(r[2]); }); })(ST[st].rows); return found; }
    function draw() {
      var S = ST[st], tb = body.querySelector('[data-k="tbl"]'); tb.innerHTML = '<p class="mu">Loading ' + S.label.toLowerCase() + "…</p>";
      fmp(S.ep, "&period=" + per + "&limit=" + (per === "annual" ? 15 : 20)).then(function (d) {
        if (!Array.isArray(d) || !d.length) { tb.innerHTML = '<p class="sp-err">No ' + S.label.toLowerCase() + " returned for " + esc(T) + ".</p>"; body.querySelector('[data-k="chart"]').innerHTML = ""; return; }
        cols = d.slice().reverse(); cur = d[0].reportedCurrency || "USD"; table();
      });
    }
    body.addEventListener("click", function (e) {
      var tg = e.target.closest("[data-tg]");
      if (tg) { var k = st + ":" + tg.getAttribute("data-tg"); open[k] = !open[k]; table(); return; }
      var b = e.target.closest(".fs-pills button, .sp-seg button");
      if (b) {
        var seg = b.parentNode.getAttribute("data-k"); b.parentNode.querySelectorAll("button").forEach(function (x) { x.classList.toggle("on", x === b); });
        if (seg === "st") st = b.getAttribute("data-v"); else per = b.getAttribute("data-v"); draw(); return;
      }
      var tr = e.target.closest("tr[data-row]");
      if (tr) { var rk = tr.getAttribute("data-row"), sel = pick[st] || (pick[st] = []), i = sel.indexOf(rk); if (i >= 0) sel.splice(i, 1); else if (sel.length < 8) sel.push(rk); table(); }
    });
    body.querySelector('[data-k="all"]').onchange = function (e) { all = e.target.checked; table(); };
    draw();
  }
  // ------------------------------------------------------------------ statistics (yearly ratios + TTM)
  var STATS = [
    ["Valuation", [["marketCap", "Market capitalization", "$"], ["enterpriseValue", "Enterprise value", "$"], ["priceToEarningsRatio", "Price to earnings ratio"], ["priceToSalesRatio", "Price to sales ratio"], ["priceToBookRatio", "Price to book ratio"], ["priceToFreeCashFlowRatio", "Price to free cash flow"], ["priceToEarningsGrowthRatio", "PEG ratio"], ["evToEBITDA", "EV / EBITDA"], ["evToSales", "EV / sales"]]],
    ["Profitability", [["grossProfitMargin", "Gross margin", "%"], ["operatingProfitMargin", "Operating margin", "%"], ["ebitdaMargin", "EBITDA margin", "%"], ["netProfitMargin", "Net margin", "%"], ["returnOnEquity", "Return on equity", "%"], ["returnOnAssets", "Return on assets", "%"], ["returnOnInvestedCapital", "Return on invested capital", "%"]]],
    ["Liquidity & solvency", [["currentRatio", "Current ratio"], ["quickRatio", "Quick ratio"], ["cashRatio", "Cash ratio"], ["debtToEquityRatio", "Debt to equity"], ["debtToAssetsRatio", "Debt to assets"], ["interestCoverageRatio", "Interest coverage"], ["netDebtToEBITDA", "Net debt / EBITDA"]]],
    ["Efficiency & per share", [["assetTurnover", "Asset turnover"], ["inventoryTurnover", "Inventory turnover"], ["receivablesTurnover", "Receivables turnover"], ["revenuePerShare", "Revenue per share"], ["bookValuePerShare", "Book value per share"], ["freeCashFlowPerShare", "Free cash flow per share"], ["dividendPayoutRatio", "Dividend payout ratio", "%"], ["dividendYield", "Dividend yield", "%"]]]
  ];
  function statistics(c) {
    var per = "annual";
    body.innerHTML = '<div class="fs-bar"><span class="sp-seg fs-per" data-k="per" style="margin-left:auto"><button type="button" data-v="annual" class="on">Annual</button><button type="button" data-v="quarter">Quarterly</button></span></div><div data-k="tbl" class="mu">Loading…</div>';
    function draw() {
      var lim = per === "annual" ? 10 : 12;
      Promise.all([fmp("ratios", "&period=" + per + "&limit=" + lim), fmp("key-metrics", "&period=" + per + "&limit=" + lim)]).then(function (a) {
        var R = Array.isArray(a[0]) ? a[0] : [], K = Array.isArray(a[1]) ? a[1] : [], byDate = {};
        R.concat(K).forEach(function (x) { var d = x.date; byDate[d] = Object.assign(byDate[d] || {}, x); });
        var cols = Object.keys(byDate).sort().map(function (d) { return byDate[d]; });
        if (!cols.length) { body.querySelector('[data-k="tbl"]').innerHTML = '<p class="sp-err">No statistics returned for ' + esc(T) + ".</p>"; return; }
        function fv(v, u) { if (!ok(v)) return "—"; return u === "%" ? (v * 100).toFixed(2) + "%" : u === "$" ? big(v) : (+v).toFixed(2); }
        var h = "";
        STATS.forEach(function (g) {
          var rows = g[1].filter(function (r) { return cols.some(function (x) { return ok(x[r[0]]); }); }); if (!rows.length) return;
          h += "<h2>" + g[0] + '</h2><div class="sp-tw fs-tw"><table class="fs"><thead><tr><th class="fs-m">Metric</th>' + cols.map(function (x) { var d = new Date(x.date + "T12:00:00Z"); return "<th><b>" + esc(per === "annual" ? String(x.fiscalYear || x.date.slice(0, 4)) : x.period + " '" + String(x.fiscalYear || "").slice(2)) + "</b><small>" + MON[d.getUTCMonth()] + " " + d.getUTCFullYear() + "</small></th>"; }).join("") + "</tr></thead><tbody>" +
            rows.map(function (r) { return '<tr><td class="fs-m" style="padding-left:14px">' + esc(r[1]) + "</td>" + cols.map(function (x) { return "<td><b>" + fv(x[r[0]], r[2]) + "</b></td>"; }).join("") + "</tr>"; }).join("") + "</tbody></table></div>";
        });
        body.querySelector('[data-k="tbl"]').innerHTML = h + '<p class="sp-note">Ratios and key metrics per reported period. Source: Financial Modeling Prep.</p>';
      });
    }
    body.addEventListener("click", function (e) { var b = e.target.closest(".sp-seg button"); if (!b) return; b.parentNode.querySelectorAll("button").forEach(function (x) { x.classList.toggle("on", x === b); }); per = b.getAttribute("data-v"); draw(); });
    draw();
  }
  // ------------------------------------------------------------------ segments (revenue by product and by region)
  function segments(c) {
    var per = "annual";
    body.innerHTML = '<div class="fs-bar"><span class="sp-seg fs-per" data-k="per" style="margin-left:auto"><button type="button" data-v="annual" class="on">Annual</button><button type="button" data-v="quarter">Quarterly</button></span></div><div data-k="tbl" class="mu">Loading…</div>';
    function block(title, d) {
      d = Array.isArray(d) ? d.slice().sort(function (a1, a2) { return String(a1.date).localeCompare(String(a2.date)); }) : [];
      if (!d.length) return "";
      var keys = {}; d.forEach(function (x) { Object.keys(x.data || {}).forEach(function (k) { keys[k] = (keys[k] || 0) + Math.abs(+x.data[k] || 0); }); });
      var ks = Object.keys(keys).sort(function (a1, a2) { return keys[a2] - keys[a1]; });
      var W = 1100, H = 220, n = d.length, mx = 0; d.forEach(function (x) { var s = 0; ks.forEach(function (k) { s += Math.max(0, +((x.data || {})[k]) || 0); }); mx = Math.max(mx, s); });
      var gw = (W - 90) / n, bw = Math.min(46, gw * 0.6), Y = function (v) { return 10 + (1 - v / (mx || 1)) * (H - 40); }, g = "";
      d.forEach(function (x, i) { var acc = 0; ks.forEach(function (k, j) { var v = Math.max(0, +((x.data || {})[k]) || 0); if (!v) return; g += '<rect x="' + (10 + i * gw + gw / 2 - bw / 2).toFixed(1) + '" y="' + Y(acc + v).toFixed(1) + '" width="' + bw.toFixed(1) + '" height="' + (Y(acc) - Y(acc + v)).toFixed(1) + '" fill="' + COLORS[j % COLORS.length] + '"><title>' + esc(k) + " " + big(v) + "</title></rect>"; acc += v; }); g += '<text x="' + (10 + i * gw + gw / 2).toFixed(1) + '" y="' + (H - 8) + '" fill="#787b86" font-size="11" text-anchor="middle">' + esc(per === "annual" ? String(x.fiscalYear || String(x.date).slice(0, 4)) : (x.period || "") + " '" + String(x.fiscalYear || "").slice(2)) + "</text>"; });
      return "<h2>" + title + '</h2><div class="sp-chart"><svg viewBox="0 0 ' + W + " " + H + '">' + g + '</svg><div class="fs-leg">' + ks.map(function (k, j) { return '<span><i style="background:' + COLORS[j % COLORS.length] + '"></i>' + esc(k) + "</span>"; }).join("") + "</div></div>" +
        '<div class="sp-tw fs-tw"><table class="fs"><thead><tr><th class="fs-m">Segment</th>' + d.map(function (x) { return "<th><b>" + esc(per === "annual" ? String(x.fiscalYear || String(x.date).slice(0, 4)) : (x.period || "") + " '" + String(x.fiscalYear || "").slice(2)) + "</b><small>" + esc(x.date) + "</small></th>"; }).join("") + "</tr></thead><tbody>" +
        ks.map(function (k, j) { return '<tr><td class="fs-m" style="padding-left:14px"><i style="display:inline-block;width:8px;height:8px;border-radius:2px;margin-right:8px;background:' + COLORS[j % COLORS.length] + '"></i>' + esc(k) + "</td>" + d.map(function (x, i) { var v = (x.data || {})[k], pv = i ? (d[i - 1].data || {})[k] : null, gg = ok(v) && ok(pv) && +pv ? (v - pv) / Math.abs(pv) * 100 : null; return "<td><b>" + big(v) + "</b><small" + (gg != null ? ' class="' + (gg > 0 ? "up" : "dn") + '"' : "") + ">" + (gg != null ? (gg > 0 ? "+" : "") + gg.toFixed(2) + "%" : "") + "</small></td>"; }).join("") + "</tr>"; }).join("") + "</tbody></table></div>";
    }
    function draw() {
      var lim = per === "annual" ? 8 : 12;
      Promise.all([fmp("revenue-product-segmentation", "&period=" + per + "&limit=" + lim), fmp("revenue-geographic-segmentation", "&period=" + per + "&limit=" + lim)]).then(function (a) {
        var h = block("Revenue by business segment", a[0]) + block("Revenue by region", a[1]);
        body.querySelector('[data-k="tbl"]').innerHTML = h ? h + '<p class="sp-note">Segment revenue as reported in filings. Source: Financial Modeling Prep.</p>' : '<p class="sp-note">No segment breakdown reported for ' + esc(T) + ".</p>";
      });
    }
    body.addEventListener("click", function (e) { var b = e.target.closest(".sp-seg button"); if (!b) return; b.parentNode.querySelectorAll("button").forEach(function (x) { x.classList.toggle("on", x === b); }); per = b.getAttribute("data-v"); draw(); });
    draw();
  }
  function holdings(c) {
    if (!c.isEtf) { body.innerHTML = '<p class="mu">' + esc(T) + " is not a fund. See Ownership for the funds and ETFs that hold it.</p>"; return; }
    body.innerHTML = '<div data-k="chg"></div><div class="sp-two"><div><h2>Sectors</h2><div class="sp-card" data-k="sec"><span class="mu">Loading…</span></div></div><div><h2>Countries</h2><div class="sp-card" data-k="cty"><span class="mu">Loading…</span></div></div></div><h2>All holdings <span data-k="hasof"></span></h2><input class="sp-filter" type="search" placeholder="Filter holdings" aria-label="Filter holdings"><div data-k="tbl" class="mu">Loading…</div>';
    var ins = c.etfIns;
    if (ins) {
      var pe = (ins.prior_effective || [])[0], ef = (ins.effective || [])[0];
      body.querySelector('[data-k="chg"]').innerHTML = "<h2>Composition changes <span>" + esc(pe) + " → " + esc(ef) + ' · constituent snapshots</span></h2><div class="sp-grid"><div><small>Added</small><b class="up">' + (ins.added || []).length + '</b></div><div><small>Removed</small><b class="dn">' + (ins.removed || []).length + "</b></div><div><small>Shares increased</small><b>" + cnt(ins.increased) + "</b></div><div><small>Shares decreased</small><b>" + cnt(ins.decreased) + "</b></div><div><small>Holdings</small><b>" + cnt(ins.constituents) + "</b></div></div>" +
        '<div class="sp-two"><div class="sp-tw"><table><tr><th>Added</th><th>Weight</th></tr>' + ((ins.added || []).map(function (x) { return '<tr><td><a href="?s=' + encodeURIComponent(x[0] || "") + '">' + esc(x[0] || "") + "</a> " + esc(x[1] || "") + "</td><td>" + n2(x[2], "%") + "</td></tr>"; }).join("") || '<tr><td class="mu" colspan="2">None</td></tr>') + '</table></div><div class="sp-tw"><table><tr><th>Removed</th><th>Prior weight</th></tr>' + ((ins.removed || []).map(function (x) { return '<tr><td><a href="?s=' + encodeURIComponent(x[0] || "") + '">' + esc(x[0] || "") + "</a> " + esc(x[1] || "") + "</td><td>" + n2(x[2], "%") + "</td></tr>"; }).join("") || '<tr><td class="mu" colspan="2">None</td></tr>') + "</table></div></div>" +
        '<div class="sp-two"><div class="sp-tw"><table><tr><th>Largest share increases</th><th>Shares</th></tr>' + (ins.top_increases || []).filter(function (x) { return x[0]; }).slice(0, 10).map(function (x) { return "<tr><td>" + esc(x[0]) + " " + esc(x[1] || "") + '</td><td class="up">+' + cnt(x[2]) + "</td></tr>"; }).join("") + '</table></div><div class="sp-tw"><table><tr><th>Largest share decreases</th><th>Shares</th></tr>' + (ins.top_decreases || []).filter(function (x) { return x[0]; }).slice(0, 10).map(function (x) { return "<tr><td>" + esc(x[0]) + " " + esc(x[1] || "") + '</td><td class="dn">' + cnt(x[2]) + "</td></tr>"; }).join("") + "</table></div></div>" +
        '<p class="sp-note">' + esc(ins.note || "") + "</p>";
    } else body.querySelector('[data-k="chg"]').innerHTML = '<p class="sp-note">No dated constituent snapshots for ' + esc(T) + " in the ETF holdings research set (294 ETFs), so 30-day additions and removals are not available.</p>";
    function barsOf(el, rows, nameK, wK) {
      rows = (rows || []).map(function (r) { return [r[nameK], parseFloat(r[wK])]; }).filter(function (r) { return ok(r[1]) && r[1] > 0.005; }).sort(function (a, b) { return b[1] - a[1]; });
      var mx = rows.length ? rows[0][1] : 1;
      el.innerHTML = rows.length ? rows.slice(0, 14).map(function (r) { return '<div class="sp-bar"><span title="' + esc(r[0]) + '">' + esc(r[0]) + '</span><i style="width:' + (r[1] / mx * 55).toFixed(1) + '%"></i><em>' + r[1].toFixed(2) + "%</em></div>"; }).join("") : '<span class="mu">Not reported</span>';
    }
    fmp("etf/sector-weightings").then(function (d) { barsOf(body.querySelector('[data-k="sec"]'), d, "sector", "weightPercentage"); });
    fmp("etf/country-weightings").then(function (d) { barsOf(body.querySelector('[data-k="cty"]'), d, "country", "weightPercentage"); });
    fmp("etf/holdings").then(function (d) {
      var el = body.querySelector('[data-k="tbl"]');
      if (!Array.isArray(d) || !d.length) { el.innerHTML = '<p class="sp-err">No holdings list returned.</p>'; return; }
      d.sort(function (a, b) { return (b.weightPercentage || 0) - (a.weightPercentage || 0); });
      body.querySelector('[data-k="hasof"]').textContent = d.length.toLocaleString() + " positions · updated " + String(d[0].updatedAt || "").slice(0, 10) + " · Financial Modeling Prep";
      function draw(q) {
        q = String(q || "").toUpperCase();
        var rows = d.filter(function (r) { return !q || String(r.asset).toUpperCase().indexOf(q) >= 0 || String(r.name).toUpperCase().indexOf(q) >= 0; }).slice(0, 600);
        el.innerHTML = '<div class="sp-tw"><table><tr><th>Holding</th><th>Weight</th><th>Shares</th><th>Market value</th></tr>' + rows.map(function (r) { return "<tr><td>" + (r.asset ? '<a href="?s=' + encodeURIComponent(r.asset) + '"><b>' + esc(r.asset) + "</b></a> " : "") + esc(r.name || "") + "</td><td>" + n2(r.weightPercentage, "%") + "</td><td>" + cnt(r.sharesNumber) + "</td><td>" + big(r.marketValue) + "</td></tr>"; }).join("") + "</table></div>";
      }
      body.querySelector(".sp-filter").oninput = function (e) { draw(e.target.value); };
      draw("");
    });
  }

  function ownership(c) {
    var q0 = lastCompleteQuarter(0);
    body.innerHTML = '<div data-k="inst"><p class="mu">Loading 13F ownership…</p></div><div data-k="mgr"></div><div data-k="etfh"></div>';
    if (!c.isEtf) {
      Promise.all([0, 1, 2, 3].map(function (b) { var q = lastCompleteQuarter(b); return fmp("institutional-ownership/symbol-positions-summary", "&year=" + q.year + "&quarter=" + q.quarter).then(function (d) { return { q: q, d: first(d) }; }); })).then(function (a) {
        var el = body.querySelector('[data-k="inst"]'), cur = a[0].d;
        if (!cur) { el.innerHTML = '<p class="sp-note">No 13F institutional summary for ' + esc(T) + ".</p>"; return; }
        var shc = cur.lastNumberOf13Fshares ? cur.numberOf13FsharesChange / cur.lastNumberOf13Fshares * 100 : null;
        el.innerHTML = "<h2>Institutional ownership <span>13F, all filers · Q" + q0.quarter + " " + q0.year + " (quarter ended " + esc(cur.date) + ") vs prior quarter</span></h2>" +
          '<div class="sp-grid"><div><small>Shares held by institutions</small><b>' + big(cur.numberOf13Fshares, false) + "</b> " + sp(shc) + "</div><div><small>Ownership of shares</small><b>" + n2(cur.ownershipPercent, "%") + "</b> " + (ok(cur.ownershipPercentChange) ? sg(cur.ownershipPercentChange, (cur.ownershipPercentChange > 0 ? "+" : "") + (+cur.ownershipPercentChange).toFixed(2) + " pp") : "") + "</div><div><small>Holders</small><b>" + cnt(cur.investorsHolding) + "</b> " + sg(cur.investorsHoldingChange, (cur.investorsHoldingChange > 0 ? "+" : "") + cnt(cur.investorsHoldingChange)) + "</div><div><small>Value held</small><b>" + big(cur.totalInvested) + "</b></div>" +
          '<div><small>New positions</small><b class="up">' + cnt(cur.newPositions) + '</b></div><div><small>Added to</small><b class="up">' + cnt(cur.increasedPositions) + '</b></div><div><small>Trimmed</small><b class="dn">' + cnt(cur.reducedPositions) + '</b></div><div><small>Sold out</small><b class="dn">' + cnt(cur.closedPositions) + "</b></div><div><small>Put/call ratio</small><b>" + n2(cur.putCallRatio) + "</b></div></div>" +
          '<div class="sp-tw"><table><tr><th>Quarter</th><th>Holders</th><th>Shares held</th><th>Change</th><th>Ownership</th><th>New</th><th>Added</th><th>Trimmed</th><th>Sold out</th></tr>' + a.filter(function (x) { return x.d; }).map(function (x) { var d = x.d, ch = d.lastNumberOf13Fshares ? d.numberOf13FsharesChange / d.lastNumberOf13Fshares * 100 : null; return "<tr><td>Q" + x.q.quarter + " " + x.q.year + "</td><td>" + cnt(d.investorsHolding) + "</td><td>" + big(d.numberOf13Fshares, false) + "</td><td>" + sp(ch) + "</td><td>" + n2(d.ownershipPercent, "%") + "</td><td>" + cnt(d.newPositions) + "</td><td>" + cnt(d.increasedPositions) + "</td><td>" + cnt(d.reducedPositions) + "</td><td>" + cnt(d.closedPositions) + "</td></tr>"; }).join("") + "</table></div>" +
          '<p class="sp-note">13F reports are filed up to 45 days after each quarter, so "last quarter" is the latest quarter whose filing deadline has passed. 13F covers US-listed long positions of managers above $100M; it is not a 30-day trade feed. Source: Financial Modeling Prep.</p>';
      });
    } else body.querySelector('[data-k="inst"]').innerHTML = "";
    getJ(PROXY + "/data/watchlist-insights/stock/" + shardOf(T) + ".json").then(function (sh) {
      var row = sh && sh.rows ? sh.rows[T] : null, el = body.querySelector('[data-k="mgr"]');
      if (row && row.f13 && row.f13.rows && row.f13.rows.length) {
        var NM = {}; (sh.managers || []).forEach(function (m) { NM[m.fund] = m; });
        var lab = { reported_quantity_increased: ["Increased", "up"], reported_quantity_decreased: ["Decreased", "dn"], reported_quantity_unchanged: ["Unchanged", ""], newly_present_in_public_disclosure: ["New", "up"], not_present_in_current_public_disclosure: ["Exited", "dn"] };
        el.innerHTML = "<h2>Tracked managers <span>" + row.f13.rows.length + " of " + (sh.managers || []).length + " watched funds · period " + esc(row.f13.period) + " vs prior quarter</span></h2>" +
          '<div class="sp-tw"><table><tr><th>Manager</th><th>Action</th><th>Shares</th><th>Change</th><th>Change %</th><th>Value</th></tr>' + row.f13.rows.slice().sort(function (a, b) { return (b[4] || 0) - (a[4] || 0); }).map(function (r) {
            var L = lab[r[1]] || [r[1], ""], pc = r[5] ? r[3] / r[5] * 100 : null;
            return '<tr><td title="' + esc((NM[r[0]] || {}).name || "") + '">' + esc((NM[r[0]] || {}).name || r[0]) + '<br><span class="mu" style="font-size:11px">filed ' + esc((NM[r[0]] || {}).filed || "") + '</span></td><td class="' + L[1] + '">' + L[0] + "</td><td>" + cnt(r[2]) + "</td><td>" + (r[3] ? sg(r[3], (r[3] > 0 ? "+" : "") + cnt(r[3])) : "0") + "</td><td>" + (pc != null ? sp(pc) : r[1].indexOf("newly") === 0 ? '<span class="up">new</span>' : "—") + "</td><td>" + big(r[4]) + "</td></tr>";
          }).join("") + "</table></div>";
      }
      if (row && row.etf && !c.isEtf) {
        var e = row.etf, chg = e.sh_prior > 0 ? (e.sh_cur / e.sh_prior - 1) * 100 : null;
        body.querySelector('[data-k="etfh"]').innerHTML = "<h2>ETF holders — last ~30 days <span>dated constituent snapshots of 294 ETFs</span></h2>" +
          '<div class="sp-grid"><div><small>Tracked ETFs holding</small><b>' + cnt(e.n_held) + "</b></div><div><small>Shares held by them</small><b>" + sp(chg) + '</b></div><div><small>Added by</small><b class="up">' + (e.added || []).length + '</b></div><div><small>Removed by</small><b class="dn">' + (e.removed || []).length + "</b></div><div><small>Raised / cut shares</small><b>" + cnt(e.up) + " / " + cnt(e.down) + "</b></div></div>" +
          ((e.added || []).length ? '<p><span class="up">Added by:</span> ' + e.added.map(function (x) { return '<a href="?s=' + encodeURIComponent(x[0]) + '">' + esc(x[0]) + "</a>" + (x[2] ? ' <span class="mu">(' + esc(x[2]) + ")</span>" : ""); }).join(", ") + "</p>" : "") +
          ((e.removed || []).length ? '<p><span class="dn">Removed by:</span> ' + e.removed.map(function (x) { return '<a href="?s=' + encodeURIComponent(x[0]) + '">' + esc(x[0]) + "</a>"; }).join(", ") + "</p>" : "") +
          '<div data-k="exp"><p class="mu">Loading every ETF that holds ' + esc(T) + "…</p></div>";
        fmp("etf/asset-exposure").then(function (d) {
          var el2 = body.querySelector('[data-k="exp"]'); if (!el2) return;
          if (!Array.isArray(d) || !d.length) { el2.innerHTML = ""; return; }
          // market values are in each fund's own currency, so US-listed funds (USD) are ranked by value and foreign listings by portfolio weight
          var us = d.filter(function (r) { return r.symbol && r.symbol.indexOf(".") < 0; }).sort(function (a, b) { return (b.marketValue || 0) - (a.marketValue || 0); });
          var fx = d.filter(function (r) { return r.symbol && r.symbol.indexOf(".") >= 0; }).sort(function (a, b) { return (b.weightPercentage || 0) - (a.weightPercentage || 0); });
          el2.innerHTML = "<h2>ETFs holding " + esc(T) + " <span>" + d.length.toLocaleString() + " funds worldwide · " + us.length.toLocaleString() + ' US-listed · Financial Modeling Prep</span></h2><div class="sp-two"><div class="sp-tw"><table><tr><th>US-listed ETF</th><th>Weight</th><th>Shares</th><th>Value (USD)</th></tr>' + us.slice(0, 250).map(function (r) { return '<tr><td><a href="?s=' + encodeURIComponent(r.symbol) + '">' + esc(r.symbol) + "</a></td><td>" + n2(r.weightPercentage, "%") + "</td><td>" + cnt(r.sharesNumber) + "</td><td>" + big(r.marketValue) + "</td></tr>"; }).join("") + '</table></div><div class="sp-tw"><table><tr><th>Listed outside the US</th><th>Weight</th><th>Shares</th></tr>' + fx.slice(0, 250).map(function (r) { return "<tr><td>" + esc(r.symbol) + "</td><td>" + n2(r.weightPercentage, "%") + "</td><td>" + cnt(r.sharesNumber) + "</td></tr>"; }).join("") + "</table></div></div>";
        });
      }
      if (!el.innerHTML && !body.querySelector('[data-k="inst"]').innerHTML && !(row && row.etf)) el.innerHTML = '<p class="mu">No ownership data for ' + esc(T) + ".</p>";
    });
  }

  function flows(c) {
    var fh = c.flows;
    if (!fh) { body.innerHTML = '<p class="sp-note">No daily fund-flow history for ' + esc(T) + ". The flows desk covers 117 ETFs.</p>"; return; }
    function sum(days) { var end = Date.parse(fh.d[fh.d.length - 1]), s = 0, n = 0; for (var i = fh.d.length - 1; i >= 0 && Date.parse(fh.d[i]) > end - days * 864e5; i--) if (ok(fh.f[i])) { s += +fh.f[i]; n++; } return n ? s : null; }
    var y = fh.d[fh.d.length - 1].slice(0, 4), ytd = 0; for (var i = fh.d.length - 1; i >= 0 && fh.d[i].slice(0, 4) === y; i--) if (ok(fh.f[i])) ytd += +fh.f[i];
    var P = [["1 day", sum(1)], ["1 week", sum(7)], ["1 month", sum(30)], ["3 months", sum(91)], ["YTD", ytd], ["1 year", sum(365)], ["3 years", sum(1096)]];
    var n = Math.min(fh.d.length, 260), d = fh.d.slice(-n), f = fh.f.slice(-n), cum = [], s = 0; f.forEach(function (v) { s += ok(v) ? +v : 0; cum.push(s); });
    var W = 1100, H = 240, mx = Math.max.apply(null, f.map(function (v) { return Math.abs(+v || 0); })) || 1, cmx = Math.max.apply(null, cum.map(Math.abs)) || 1, bw = (W - 20) / n;
    var bz = f.map(function (v, i) { v = +v || 0; var h = Math.abs(v) / mx * (H / 2 - 20); return '<rect x="' + (10 + i * bw).toFixed(1) + '" y="' + (v >= 0 ? H / 2 - h : H / 2).toFixed(1) + '" width="' + Math.max(1, bw - 1).toFixed(1) + '" height="' + Math.max(0.5, h).toFixed(1) + '" fill="' + (v >= 0 ? "#089981" : "#f23645") + '"><title>' + d[i] + " " + big(v) + "</title></rect>"; }).join("");
    var line = cum.map(function (v, i) { return (i ? "L" : "M") + (10 + i * bw + bw / 2).toFixed(1) + " " + (H / 2 - v / cmx * (H / 2 - 20)).toFixed(1); }).join("");
    body.innerHTML = "<h2>Fund flows <span>as of " + esc(fh.asof || d[d.length - 1]) + " · " + esc(fh.source || "") + " · history from " + esc(fh.from || fh.d[0]) + '</span></h2><div class="sp-grid">' + P.map(function (x) { return "<div><small>" + x[0] + "</small><b>" + (x[1] == null ? "—" : sg(x[1], (x[1] > 0 ? "+" : "") + big(x[1]))) + "</b></div>"; }).join("") + "</div>" +
      '<div class="sp-chart"><div class="mu" style="font-size:12px;margin:2px 4px 4px">Daily net creations/redemptions (bars) and cumulative flow over the last ' + n + ' sessions (line)</div><svg viewBox="0 0 ' + W + " " + H + '" role="img" aria-label="ETF flows"><line x1="0" x2="' + W + '" y1="' + H / 2 + '" y2="' + H / 2 + '" stroke="#363a45"/>' + bz + '<path d="' + line + '" fill="none" stroke="#2962ff" stroke-width="1.6"/></svg></div>' +
      '<div class="sp-tw"><table><tr><th>Date</th><th>Net flow</th></tr>' + fh.d.slice(-30).reverse().map(function (x, i) { var v = fh.f[fh.f.length - 1 - i]; return "<tr><td>" + esc(x) + "</td><td>" + (ok(v) ? sg(+v, (v > 0 ? "+" : "") + big(+v)) : "—") + "</td></tr>"; }).join("") + "</table></div>";
  }

  function dividends() {
    body.innerHTML = '<p class="mu">Loading…</p>';
    fmp("dividends").then(function (d) {
      if (!Array.isArray(d) || !d.length) { body.innerHTML = '<p class="sp-note">No dividends reported for ' + esc(T) + ".</p>"; return; }
      body.innerHTML = "<h2>Dividends <span>" + d.length + " payments · " + esc(d[0].frequency || "") + '</span></h2><div class="sp-tw"><table><tr><th>Ex-date</th><th>Dividend</th><th>Adjusted</th><th>Yield at the time</th><th>Record</th><th>Paid</th><th>Declared</th></tr>' +
        d.slice(0, 80).map(function (r) { return "<tr><td>" + esc(r.date) + "</td><td>" + n2(r.dividend) + "</td><td>" + n2(r.adjDividend) + "</td><td>" + n2(r.yield, "%") + "</td><td>" + esc(r.recordDate || "") + "</td><td>" + esc(r.paymentDate || "") + "</td><td>" + esc(r.declarationDate || "") + "</td></tr>"; }).join("") + "</table></div>";
    });
  }
  function earnings() {
    body.innerHTML = '<p class="mu">Loading…</p>';
    fmp("earnings").then(function (d) {
      if (!Array.isArray(d) || !d.length) { body.innerHTML = '<p class="sp-note">No earnings calendar for ' + esc(T) + ".</p>"; return; }
      body.innerHTML = '<h2>Earnings <span>reported vs estimated</span></h2><div class="sp-tw"><table><tr><th>Date</th><th>EPS</th><th>EPS est.</th><th>Surprise</th><th>Revenue</th><th>Revenue est.</th></tr>' +
        d.map(function (r) { var s = ok(r.epsActual) && ok(r.epsEstimated) && r.epsEstimated ? (r.epsActual / r.epsEstimated - 1) * 100 : null; return "<tr><td>" + esc(r.date) + (r.epsActual == null ? ' <span class="mu">upcoming</span>' : "") + "</td><td>" + n2(r.epsActual) + "</td><td>" + n2(r.epsEstimated) + "</td><td>" + (s == null ? "—" : sp(s, 1)) + "</td><td>" + big(r.revenueActual) + "</td><td>" + big(r.revenueEstimated) + "</td></tr>"; }).join("") + "</table></div>";
    });
  }

  function setTab(id, c) {
    tabsEl.querySelectorAll("button").forEach(function (b) { var on = b.getAttribute("data-t") === id; b.classList.toggle("on", on); b.setAttribute("aria-selected", String(on)); });
    var t = TABS.filter(function (x) { return x[0] === id; })[0] || TABS[0];
    var nb = body.cloneNode(false); body.parentNode.replaceChild(nb, body); body = nb; // drop listeners of the previous tab
    t[2](c);
    try { history.replaceState(null, "", "?s=" + encodeURIComponent(T) + (t[0] !== "overview" ? "&tab=" + t[0] : "")); } catch (e) {}
  }
  core().then(function (c) {
    renderHead(c);
    liveQuote();
    TABS = [["overview", "Overview", overview]];
    if (!c.isEtf) TABS.push(["financials", "Statements", financials], ["statistics", "Statistics", statistics]);
    if (c.isEtf) TABS.push(["holdings", "Holdings", holdings]);
    TABS.push(["ownership", c.isEtf ? "Ownership" : "Ownership & funds", ownership]);
    if (c.flows) TABS.push(["flows", "ETF flows", flows]);
    TABS.push(["dividends", "Dividends", dividends]);
    if (!c.isEtf) TABS.push(["earnings", "Earnings", earnings]);
    if (!c.isEtf) TABS.push(["segments", "Segments", segments]);
    tabsEl.innerHTML = TABS.map(function (t) { return '<button type="button" role="tab" data-t="' + t[0] + '">' + t[1] + "</button>"; }).join("");
    tabsEl.onclick = function (e) { var b = e.target.closest("[data-t]"); if (b) setTab(b.getAttribute("data-t"), c); };
    setTab(qs.get("tab") || "overview", c);
    foot.innerHTML = "Sources: Financial Modeling Prep (quotes, statements, ratios, ETF holdings, 13F summaries), Polygon/Yahoo via the JustHodl data proxy (price history), ETF Global (fund flows, constituent snapshots), SEC 13F filings of tracked managers. Figures are as reported; nothing here is investment advice.";
    if (!c.f && !c.p && !c.etfInfo) head.insertAdjacentHTML("beforeend", '<p class="sp-err" style="width:100%">No company or fund profile found for ' + esc(T) + ". Check the ticker.</p>");
  });
  $("sp-find").onsubmit = function (e) { e.preventDefault(); var v = $("sp-q").value.trim().toUpperCase(); if (v) location.href = "?s=" + encodeURIComponent(v.replace(/^[A-Z]+:/, "")); };
})();
