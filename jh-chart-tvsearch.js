/* jh-reskin-skip: TradingView symbol widget, compare chrome, data-type desk. */
(function () {
  if (window.__jhTvSearch) return;
  window.__jhTvSearch = true;

  var TABS = [
    ["over", "Overview"],
    ["stats", "Statistics"],
    ["val", "Valuation"],
    ["fin", "Financials"],
    ["est", "Estimates"],
    ["div", "Dividends"],
    ["news", "News"],
    ["tech", "Technicals"],
    ["short", "Short"],
    ["opt", "Options"],
    ["etf", "ETF"],
    ["flow", "Flow"],
    ["hold", "Holders"]
  ];
  var tab = "over";
  var cache = {};
  var loading = {};

  function ctx() {
    return {
      active: window.jhActive || window.active,
      quotes: {},
      lastBars: window.lastBars || [],
      finCache: {},
      PROXY: "https://justhodl-data-proxy.raafouis.workers.dev",
      fmt: window.jhFmt,
      fmtBig: window.jhFmtBig,
      numish: window.jhNumish,
      bare: window.jhBare || function (s) { return String(s || "").split(":").pop(); },
      displayTicker: window.jhDisplayTicker || function (s) { return s; },
      UP: "#089981",
      DN: "#f23645"
    };
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&" + "amp;")
      .replace(/</g, "&" + "lt;")
      .replace(/>/g, "&" + "gt;");
  }
  function num(x) {
    if (window.jhNumish) return window.jhNumish(x);
    if (x == null) return null;
    if (typeof x === "number") return x;
    if (typeof x === "object" && x.raw != null) return x.raw;
    var n = Number(x);
    return isFinite(n) ? n : null;
  }
  function fmt(n, d) {
    n = num(n);
    if (n == null) return "—";
    d = d == null ? (Math.abs(n) >= 100 ? 2 : 2) : d;
    return n.toLocaleString(undefined, { maximumFractionDigits: d, minimumFractionDigits: 0 });
  }
  function fmtBig(n) {
    n = num(n);
    if (n == null) return "—";
    var a = Math.abs(n);
    if (a >= 1e12) return (n / 1e12).toFixed(2) + "T";
    if (a >= 1e9) return (n / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return (n / 1e6).toFixed(2) + "M";
    return fmt(n);
  }
  function pct(n, mul) {
    n = num(n);
    if (n == null) return "—";
    if (mul) n = n * 100;
    return (n >= 0 ? "+" : "") + n.toFixed(2) + "%";
  }
  function cls(n) {
    n = num(n);
    if (n == null) return "";
    return n >= 0 ? "up" : "dn";
  }
  function retN(bars, n) {
    if (!bars || bars.length < 2) return null;
    var a = bars[bars.length - 1];
    var b = bars[Math.max(0, bars.length - 1 - n)];
    if (!a || !b || !b.close) return null;
    return (a.close / b.close - 1) * 100;
  }
  function ytd(bars) {
    if (!bars || !bars.length) return null;
    var last = bars[bars.length - 1];
    var y = new Date((last.time || 0) * 1000).getUTCFullYear();
    var i, start = bars[0];
    for (i = 0; i < bars.length; i++) {
      if (new Date(bars[i].time * 1000).getUTCFullYear() === y) { start = bars[i]; break; }
    }
    if (!start || !start.close) return null;
    return (last.close / start.close - 1) * 100;
  }
  function range52(bars) {
    if (!bars || !bars.length) return { hi: null, lo: null, pos: null };
    var last = bars[bars.length - 1];
    var cut = (last.time || 0) - 365 * 86400;
    var hi = -1e99, lo = 1e99, i;
    for (i = 0; i < bars.length; i++) {
      if (bars[i].time < cut) continue;
      if (bars[i].high > hi) hi = bars[i].high;
      if (bars[i].low < lo) lo = bars[i].low;
    }
    if (hi < -1e90) return { hi: null, lo: null, pos: null };
    var pos = hi > lo && last ? ((last.close - lo) / (hi - lo)) * 100 : null;
    return { hi: hi, lo: lo, pos: pos };
  }

  function pick(j) {
    j = j || {};
    var p = j.price || j.quote || {};
    var sd = j.summaryDetail || {};
    var ks = j.defaultKeyStatistics || j.defaultKeyStatistics || {};
    var fd = j.financialData || {};
    var eh = j.earningsHistory || {};
    return { p: p, sd: sd, ks: ks, fd: fd, eh: eh, raw: j };
  }

  function kpi(label, value, extra) {
    return "<div class=k><span>" + esc(label) + "</span><b>" + value + "</b>" + (extra ? "<span>" + extra + "</span>" : "") + "</div>";
  }
  function table(rows) {
    if (!rows || !rows.length) return "";
    return "<table><tbody>" + rows.map(function (r) {
      return "<tr><td>" + esc(r[0]) + "</td><td>" + r[1] + "</td></tr>";
    }).join("") + "</tbody></table>";
  }
  function blk(title, inner) {
    if (!inner) return "";
    return "<div class=blk><h4>" + esc(title) + "</h4>" + inner + "</div>";
  }

  function renderOver(d, bars, q, pack) {
    pack = pack || {};
    var x = pick(d);
    var last = bars && bars.length ? bars[bars.length - 1] : null;
    var px = last ? last.close : num(q && q.last) || num(x.p.regularMarketPrice);
    var r = range52(bars);
    var kpis = [
      kpi("Last", px != null ? fmt(px, 2) : "—"),
      kpi("Mkt cap", fmtBig(num(x.p.marketCap) || num(x.sd.marketCap))),
      kpi("P/E", fmt(num(x.sd.trailingPE) || num(x.ks.trailingPE))),
      kpi("Fwd P/E", fmt(num(x.sd.forwardPE) || num(x.ks.forwardPE))),
      kpi("EPS", fmt(num(x.ks.trailingEps) || num(x.fd.trailingEps))),
      kpi("Div yld", num(x.sd.dividendYield) != null ? (num(x.sd.dividendYield) * 100).toFixed(2) + "%" : "—")
    ].join("");
    var rets = [
      ["1D", retN(bars, 1)],
      ["1W", retN(bars, 5)],
      ["1M", retN(bars, 21)],
      ["3M", retN(bars, 63)],
      ["YTD", ytd(bars)],
      ["1Y", retN(bars, 252)]
    ];
    var retHtml = "<table><thead><tr>" + rets.map(function (r) { return "<th>" + r[0] + "</th>"; }).join("") + "</tr></thead><tbody><tr>" +
      rets.map(function (r) {
        var v = r[1];
        return "<td class='" + cls(v) + "'>" + (v == null ? "—" : (v >= 0 ? "+" : "") + v.toFixed(2) + "%") + "</td>";
      }).join("") + "</tr></tbody></table>";
    var profile = table([
      ["Name", esc(x.p.shortName || x.p.longName || window.jhActive || "—")],
      ["Exchange", esc(x.p.exchangeName || x.p.exchange || "—")],
      ["Currency", esc(x.p.currency || "USD")],
      ["Beta", fmt(num(x.ks.beta))],
      ["52w high", fmt(r.hi || num(x.sd.fiftyTwoWeekHigh))],
      ["52w low", fmt(r.lo || num(x.sd.fiftyTwoWeekLow))],
      ["52w position", r.pos != null ? r.pos.toFixed(1) + "%" : "—"],
      ["Avg volume", fmtBig(num(x.sd.averageVolume) || num(x.p.averageDailyVolume3Month))],
      ["Float", fmtBig(num(x.ks.floatShares))],
      ["Shares out", fmtBig(num(x.ks.sharesOutstanding))]
    ]);
    var etfBit = "";
    if (window.JHEtfFuse && window.JHEtfFuse.isFund(pack.etfRow, pack.polyEtf)) etfBit = renderPolyEtf(pack);
    else if (pack.etfHolders && pack.etfHolders.length) etfBit = renderHold(d, pack);
    return "<div class=kpi>" + kpis + "</div>" + blk("Total return", retHtml) + blk("Profile", profile) + etfBit;
  }

  function renderRelated(pack) {
    var j = pack.related || {};
    var rows = polyRows(j);
    if (!rows.length && j.results && Array.isArray(j.results)) rows = j.results;
    if (!rows.length) return "";
    return blk("Related (Polygon reference)", "<table><thead><tr><th>Ticker</th><th>Name</th></tr></thead><tbody>" +
      rows.slice(0, 12).map(function (r) {
        var t = r.ticker || r.symbol || "";
        return "<tr><td><a href='/chart.html?s=" + encodeURIComponent(t) + "'>" + esc(t) + "</a></td><td>" +
          esc(r.name || r.company_name || "") + "</td></tr>";
      }).join("") + "</tbody></table>");
  }

  function renderStats(d, bars) {
    var x = pick(d);
    var last = bars && bars.length ? bars[bars.length - 1] : null;
    var r = range52(bars);
    return blk("Price & volume", table([
      ["Last", last ? fmt(last.close) : "—"],
      ["Open", last ? fmt(last.open) : "—"],
      ["High", last ? fmt(last.high) : "—"],
      ["Low", last ? fmt(last.low) : "—"],
      ["Volume", last ? fmtBig(last.volume) : "—"],
      ["Avg vol (3m)", fmtBig(num(x.sd.averageVolume) || num(x.p.averageDailyVolume3Month))],
      ["52-week high", fmt(r.hi || num(x.sd.fiftyTwoWeekHigh))],
      ["52-week low", fmt(r.lo || num(x.sd.fiftyTwoWeekLow))],
      ["50d MA", fmt(num(x.sd.fiftyDayAverage))],
      ["200d MA", fmt(num(x.sd.twoHundredDayAverage))]
    ])) + blk("Share statistics", table([
      ["Market cap", fmtBig(num(x.p.marketCap) || num(x.sd.marketCap))],
      ["Enterprise value", fmtBig(num(x.ks.enterpriseValue))],
      ["Shares outstanding", fmtBig(num(x.ks.sharesOutstanding))],
      ["Float", fmtBig(num(x.ks.floatShares))],
      ["Held by insiders", num(x.ks.heldPercentInsiders) != null ? (num(x.ks.heldPercentInsiders) * 100).toFixed(2) + "%" : "—"],
      ["Held by institutions", num(x.ks.heldPercentInstitutions) != null ? (num(x.ks.heldPercentInstitutions) * 100).toFixed(2) + "%" : "—"],
      ["Short % of float", num(x.ks.shortPercentOfFloat) != null ? (num(x.ks.shortPercentOfFloat) * 100).toFixed(2) + "%" : "—"],
      ["Short ratio", fmt(num(x.ks.shortRatio))],
      ["Beta (5y)", fmt(num(x.ks.beta))]
    ]));
  }

  function renderVal(d) {
    var x = pick(d);
    return "<div class=kpi>" + [
      kpi("P/E", fmt(num(x.sd.trailingPE) || num(x.ks.trailingPE))),
      kpi("Fwd P/E", fmt(num(x.sd.forwardPE) || num(x.ks.forwardPE))),
      kpi("PEG", fmt(num(x.ks.pegRatio))),
      kpi("P/S", fmt(num(x.sd.priceToSalesTrailing12Months))),
      kpi("P/B", fmt(num(x.sd.priceToBook) || num(x.ks.priceToBook))),
      kpi("EV/EBITDA", fmt(num(x.ks.enterpriseToEbitda)))
    ].join("") + "</div>" +
      blk("Valuation", table([
        ["Trailing P/E", fmt(num(x.sd.trailingPE) || num(x.ks.trailingPE))],
        ["Forward P/E", fmt(num(x.sd.forwardPE) || num(x.ks.forwardPE))],
        ["PEG ratio", fmt(num(x.ks.pegRatio))],
        ["Price / sales", fmt(num(x.sd.priceToSalesTrailing12Months))],
        ["Price / book", fmt(num(x.sd.priceToBook) || num(x.ks.priceToBook))],
        ["EV / revenue", fmt(num(x.ks.enterpriseToRevenue))],
        ["EV / EBITDA", fmt(num(x.ks.enterpriseToEbitda))],
        ["Enterprise value", fmtBig(num(x.ks.enterpriseValue))]
      ])) +
      blk("Profitability", table([
        ["Profit margin", num(x.fd.profitMargins) != null ? (num(x.fd.profitMargins) * 100).toFixed(2) + "%" : "—"],
        ["Operating margin", num(x.fd.operatingMargins) != null ? (num(x.fd.operatingMargins) * 100).toFixed(2) + "%" : "—"],
        ["Gross margin", num(x.fd.grossMargins) != null ? (num(x.fd.grossMargins) * 100).toFixed(2) + "%" : "—"],
        ["EBITDA", fmtBig(num(x.fd.ebitda))],
        ["ROE", num(x.fd.returnOnEquity) != null ? (num(x.fd.returnOnEquity) * 100).toFixed(2) + "%" : "—"],
        ["ROA", num(x.fd.returnOnAssets) != null ? (num(x.fd.returnOnAssets) * 100).toFixed(2) + "%" : "—"]
      ])) +
      blk("Balance sheet", table([
        ["Total cash", fmtBig(num(x.fd.totalCash))],
        ["Total debt", fmtBig(num(x.fd.totalDebt))],
        ["Debt / equity", fmt(num(x.fd.debtToEquity))],
        ["Current ratio", fmt(num(x.fd.currentRatio))],
        ["Quick ratio", fmt(num(x.fd.quickRatio))],
        ["Book value / sh", fmt(num(x.ks.bookValue))]
      ]));
  }

  function stmtRows(list, fields) {
    list = (list || []).slice(0, 4);
    if (!list.length) return "";
    var head = "<tr><th></th>" + list.map(function (y) {
      var dt = (y.endDate && (y.endDate.fmt || y.endDate)) || y.date || "";
      return "<th>" + esc(String(dt).slice(0, 10)) + "</th>";
    }).join("") + "</tr>";
    var body = fields.map(function (f) {
      return "<tr><td>" + esc(f[0]) + "</td>" + list.map(function (y) {
        return "<td>" + fmtBig(num(y[f[1]])) + "</td>";
      }).join("") + "</tr>";
    }).join("");
    return "<table><thead>" + head + "</thead><tbody>" + body + "</tbody></table>";
  }

  function renderFin(d) {
    var j = d || {};
    var inc = ((j.incomeStatementHistory || {}).incomeStatementHistory) || j.income || [];
    var bal = ((j.balanceSheetHistory || {}).balanceSheetStatements) || j.balance || [];
    var cf = ((j.cashflowStatementHistory || {}).cashflowStatements) || j.cash || [];
    var html = "";
    html += blk("Income statement", stmtRows(inc, [
      ["Revenue", "totalRevenue"],
      ["Gross profit", "grossProfit"],
      ["Operating income", "operatingIncome"],
      ["EBITDA", "ebitda"],
      ["Net income", "netIncome"],
      ["EPS", "dilutedEPS"]
    ]) || "<div class=empty>Income statement not in this feed yet.</div>");
    html += blk("Balance sheet", stmtRows(bal, [
      ["Cash", "cash"],
      ["Total assets", "totalAssets"],
      ["Total liabilities", "totalLiab"],
      ["Stockholder equity", "totalStockholderEquity"],
      ["Long-term debt", "longTermDebt"]
    ]));
    html += blk("Cash flow", stmtRows(cf, [
      ["Operating CF", "totalCashFromOperatingActivities"],
      ["Capex", "capitalExpenditures"],
      ["Free cash flow", "freeCashFlow"],
      ["Dividends", "dividendsPaid"]
    ]));
    return html;
  }

  function renderEst(d) {
    var x = pick(d);
    var tr = ((d || {}).earningsTrend || {}).trend || [];
    var rec = ((d || {}).recommendationTrend || {}).trend || [];
    var tgt = num(x.fd.targetMeanPrice);
    var last = num(x.p.regularMarketPrice);
    var upside = tgt && last ? ((tgt / last - 1) * 100) : null;
    var html = "<div class=kpi>" + [
      kpi("Target", tgt != null ? fmt(tgt) : "—"),
      kpi("Upside", upside != null ? (upside >= 0 ? "+" : "") + upside.toFixed(1) + "%" : "—"),
      kpi("Rec.", esc(x.fd.recommendationKey || "—")),
      kpi("# analysts", fmt(num(x.fd.numberOfAnalystOpinions)))
    ].join("") + "</div>";
    if (tr.length) {
      html += blk("Earnings trend", "<table><thead><tr><th>Period</th><th>EPS est.</th><th>Rev est.</th><th>Growth</th></tr></thead><tbody>" +
        tr.slice(0, 6).map(function (t) {
          return "<tr><td>" + esc(t.period || t.endDate || "") + "</td><td>" +
            fmt(num(t.earningsEstimate && t.earningsEstimate.avg)) + "</td><td>" +
            fmtBig(num(t.revenueEstimate && t.revenueEstimate.avg)) + "</td><td>" +
            (num(t.growth) != null ? (num(t.growth) * 100).toFixed(1) + "%" : "—") + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    if (rec.length) {
      html += blk("Recommendation trend", "<table><thead><tr><th>Period</th><th>Strong buy</th><th>Buy</th><th>Hold</th><th>Sell</th><th>Strong sell</th></tr></thead><tbody>" +
        rec.slice(0, 4).map(function (t) {
          return "<tr><td>" + esc(t.period || "") + "</td><td>" + (t.strongBuy || 0) + "</td><td>" + (t.buy || 0) +
            "</td><td>" + (t.hold || 0) + "</td><td>" + (t.sell || 0) + "</td><td>" + (t.strongSell || 0) + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    if (!tr.length && !rec.length) html += "<div class=empty>No analyst estimates in this feed for this name.</div>";
    return html;
  }

  function renderDiv(d) {
    var x = pick(d);
    return blk("Dividend", table([
      ["Dividend yield", num(x.sd.dividendYield) != null ? (num(x.sd.dividendYield) * 100).toFixed(2) + "%" : "—"],
      ["Dividend rate", fmt(num(x.sd.dividendRate))],
      ["Payout ratio", num(x.sd.payoutRatio) != null ? (num(x.sd.payoutRatio) * 100).toFixed(1) + "%" : "—"],
      ["Ex-dividend", esc((x.sd.exDividendDate && (x.sd.exDividendDate.fmt || x.sd.exDividendDate)) || "—")],
      ["5y avg yield", num(x.sd.fiveYearAvgDividendYield) != null ? num(x.sd.fiveYearAvgDividendYield).toFixed(2) + "%" : "—"],
      ["Trailing annual", fmt(num(x.sd.trailingAnnualDividendRate))]
    ]));
  }

  function renderHold(d, pack) {
    pack = pack || {};
    var j = d || {};
    var mh = j.majorHoldersBreakdown || {};
    var inst = ((j.institutionOwnership || {}).ownershipList) || [];
    var html = blk("Ownership", table([
      ["Insiders", num(mh.insidersPercentHeld) != null ? (num(mh.insidersPercentHeld) * 100).toFixed(2) + "%" : "—"],
      ["Institutions", num(mh.institutionsPercentHeld) != null ? (num(mh.institutionsPercentHeld) * 100).toFixed(2) + "%" : "—"],
      ["Inst. float", num(mh.institutionsFloatPercentHeld) != null ? (num(mh.institutionsFloatPercentHeld) * 100).toFixed(2) + "%" : "—"],
      ["# institutions", fmt(num(mh.institutionsCount))]
    ]));
    if (inst.length) {
      html += blk("Top holders", "<table><thead><tr><th>Holder</th><th>% out</th><th>Shares</th></tr></thead><tbody>" +
        inst.slice(0, 12).map(function (h) {
          return "<tr><td>" + esc(h.organization || h.holder || "") + "</td><td>" +
            (num(h.pctHeld) != null ? (num(h.pctHeld) * 100).toFixed(2) + "%" : "—") + "</td><td>" +
            fmtBig(num(h.position) || num(h.shares)) + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    var etfH = pack.etfHolders || [];
    if (etfH.length) {
      var dem = window.JHEtfFuse ? window.JHEtfFuse.impliedDemand(etfH) : null;
      html += blk("ETF Global look-through (funds that hold this name)", "<div class=kpi>" +
        kpi("Implied 1D", fmtBig(dem)) + kpi("Funds", String(etfH.length)) + "</div>" +
        "<table><thead><tr><th>ETF</th><th>Wgt</th><th>Flow 1D</th><th>Print</th></tr></thead><tbody>" +
        etfH.slice(0, 16).map(function (h) {
          return "<tr><td><a href='/chart.html?s=" + encodeURIComponent(h.etf) + "'>" + esc(h.etf) +
            "</a></td><td>" + (num(h.w) != null ? ((h.w < 1 ? h.w * 100 : h.w).toFixed(2) + "%") : "—") +
            "</td><td class='" + cls(h.flow_1d) + "'>" + fmtBig(h.flow_1d) + "</td><td>" +
            esc(h.flow_label || "") + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    return html;
  }

  function lastVal(results) {
    var rows = results || [];
    if (!rows.length) return null;
    var v = rows[0].value != null ? rows[0].value : (rows[0].values && (rows[0].values.value || rows[0].values));
    return num(v);
  }
  function polyRows(j) {
    if (!j) return [];
    if (Array.isArray(j.results)) return j.results;
    if (j.results && Array.isArray(j.results.values)) return j.results.values;
    return [];
  }
  function renderPolyNews(pack) {
    var rows = polyRows(pack.polyNews) || [];
    if (!rows.length) return "<div class=empty>No Polygon news for this name yet (Stocks Starter /v2/reference/news).</div>";
    return blk("Polygon news", "<table><thead><tr><th>When</th><th>Headline</th><th>Source</th></tr></thead><tbody>" +
      rows.slice(0, 16).map(function (n) {
        var dt = (n.published_utc || n.published || "").slice(0, 16).replace("T", " ");
        var href = n.article_url || n.url || "#";
        return "<tr><td>" + esc(dt) + "</td><td><a href='" + esc(href) + "' target=_blank rel=noopener>" +
          esc(n.title || "") + "</a></td><td>" + esc((n.publisher && n.publisher.name) || n.author || "") + "</td></tr>";
      }).join("") + "</tbody></table>");
  }
  function renderPolyDiv(pack) {
    var rows = polyRows(pack.polyDiv);
    if (!rows.length) return "";
    return blk("Polygon dividends", "<table><thead><tr><th>Ex</th><th>Pay</th><th>Cash</th><th>Freq</th></tr></thead><tbody>" +
      rows.slice(0, 12).map(function (r) {
        return "<tr><td>" + esc(r.ex_dividend_date || "") + "</td><td>" + esc(r.pay_date || "") +
          "</td><td>" + fmt(num(r.cash_amount), 4) + "</td><td>" + esc(r.frequency || "") + "</td></tr>";
      }).join("") + "</tbody></table>");
  }
  function renderPolyTech(pack) {
    var t = pack.polyTech || {};
    var rsi = lastVal(polyRows(t.rsi && (t.rsi.results || t.rsi)));
    if (rsi == null && t.rsi && t.rsi.results && t.rsi.results.values) rsi = lastVal(t.rsi.results.values);
    function ind(j) {
      var r = j && (j.results || j);
      if (r && r.values) return lastVal(r.values);
      return lastVal(r);
    }
    var sma = ind(t.sma), ema = ind(t.ema);
    var macdObj = t.macd && (t.macd.results || t.macd);
    var macdVal = null, signal = null;
    if (macdObj && macdObj.values && macdObj.values[0]) {
      macdVal = num(macdObj.values[0].value);
      signal = num(macdObj.values[0].signal);
    } else if (Array.isArray(macdObj) && macdObj[0]) {
      macdVal = num(macdObj[0].value); signal = num(macdObj[0].signal);
    }
    return blk("Polygon indicators (Stocks Starter /v1/indicators)", table([
      ["RSI 14d", rsi != null ? rsi.toFixed(1) : "—"],
      ["SMA 50d", fmt(sma)],
      ["EMA 20d", fmt(ema)],
      ["MACD", macdVal != null ? macdVal.toFixed(3) : "—"],
      ["MACD signal", signal != null ? signal.toFixed(3) : "—"]
    ])) + "<div class=empty>Native Polygon indicators — not a local SMA on Yahoo bars.</div>";
  }
  function renderPolyShort(pack) {
    var rows = polyRows(pack.polyShort);
    if (!rows.length) return "<div class=empty>No Polygon short-interest print for this name.</div>";
    return blk("Polygon short interest", "<table><thead><tr><th>Settle</th><th>Short</th><th>Days to cover</th><th>Avg vol</th></tr></thead><tbody>" +
      rows.slice(0, 8).map(function (r) {
        return "<tr><td>" + esc(r.settlement_date || r.date || "") + "</td><td>" +
          fmtBig(num(r.short_interest) || num(r.short_interest_shares)) + "</td><td>" +
          fmt(num(r.days_to_cover), 2) + "</td><td>" + fmtBig(num(r.avg_daily_volume)) + "</td></tr>";
      }).join("") + "</tbody></table>");
  }
  function renderPolyOpt(pack) {
    var j = pack.polyOpt || {};
    var snaps = (j.results || []).slice(0, 18);
    if (!snaps.length) return "<div class=empty>No Options Starter snapshot for this underlying.</div>";
    return blk("Options snapshot (Massive Options Starter)", "<table><thead><tr><th>Contract</th><th>OI</th><th>Vol</th><th>IV</th><th>Delta</th><th>Last</th></tr></thead><tbody>" +
      snaps.map(function (r) {
        var d = r.details || r;
        var g = r.greeks || {};
        var day = r.day || {};
        return "<tr><td>" + esc(d.ticker || "") + "</td><td>" + fmtBig(num(r.open_interest)) +
          "</td><td>" + fmtBig(num(day.volume) || num(r.volume)) + "</td><td>" +
          (num(r.implied_volatility) != null ? (num(r.implied_volatility) * 100).toFixed(1) + "%" : "—") +
          "</td><td>" + fmt(num(g.delta), 3) + "</td><td>" + fmt(num(day.close) || num(r.break_even_price)) + "</td></tr>";
      }).join("") + "</tbody></table>");
  }
  function renderPolyEtf(pack) {
    var F = window.JHEtfFuse;
    var j = pack.polyEtf || {};
    var row = (pack.etfRow) || {};
    var flows = (F ? F.polyRows(j.flows) : (j.flows && j.flows.results) || []) || [];
    var prof = ((F ? F.polyRows(j.profile) : (j.profile && j.profile.results) || [])[0]) || {};
    var holds = (F ? F.polyRows(j.holdings) : (j.holdings && j.holdings.results) || []) || [];
    if (row.top && row.top.length && !holds.length) {
      holds = row.top.map(function (h) { return { constituent_ticker: h.t, constituent_name: h.n, weight: h.w, market_value: h.mv }; });
    }
    if (!flows.length && !Object.keys(prof).length && !row.aum && !(pack.etfHolders && pack.etfHolders.length)) {
      return "<div class=empty>ETF Global has no fund print for this ticker — if it is a stock, open Holders for the look-through of funds that own it.</div>";
    }
    var latest = flows[0] || {};
    var aum = num(row.aum) || num(prof.aum);
    var er = num(row.er) || num(prof.net_expense_ratio) || num(prof.expense_ratio);
    var f1 = num(row.flow_1d) != null ? num(row.flow_1d) : num(latest.fund_flow);
    var f5 = num(row.flow_5d);
    var f21 = num(row.flow_21d);
    if (f5 == null && flows.length) {
      f5 = 0; flows.slice(0, 5).forEach(function (r) { f5 += num(r.fund_flow) || 0; });
    }
    if (f21 == null && flows.length) {
      f21 = 0; flows.slice(0, 21).forEach(function (r) { f21 += num(r.fund_flow) || 0; });
    }
    var erTxt = F ? F.fmtEr(er) : (er == null ? "—" : er.toFixed(3) + "%");
    var html = "<div class=kpi>" + [
      kpi("AUM", fmtBig(aum)),
      kpi("ER", erTxt),
      kpi("Flow 1D", fmtBig(f1)),
      kpi("Flow 5D", fmtBig(f5)),
      kpi("Flow 21D", fmtBig(f21)),
      kpi("NAV", fmt(num(row.nav) || num(latest.nav), 3))
    ].join("") + "</div>";
    html += blk("ETF Global profile (paid)", table([
      ["Name", esc(row.name || prof.description || prof.fund_name || "—")],
      ["Issuer", esc(row.issuer || prof.issuer || prof.advisor || "—")],
      ["Asset class", esc(row.asset_class || prof.asset_class || "—")],
      ["Category", esc(row.category || prof.category || prof.focus || "—")],
      ["Benchmark", esc(row.benchmark || prof.primary_benchmark || "—")],
      ["Inception", esc(row.inception || prof.inception_date || "—")],
      ["Shares out", fmtBig(num(row.shares) || num(latest.shares_outstanding))],
      ["Holdings", fmt(row.holdings_n)],
      ["HHI", row.hhi != null ? fmt(row.hhi, 0) : "—"],
      ["Leverage", esc(row.leverage || "—")],
      ["As-of", esc(row.flow_asof || latest.processed_date || "—")]
    ]));
    var sec = row.sector || [];
    if (sec.length) {
      html += blk("Sector exposure", "<table><thead><tr><th>Sector</th><th>Wgt</th></tr></thead><tbody>" +
        sec.map(function (x) {
          var w = num(x.w);
          return "<tr><td>" + esc(x.k) + "</td><td>" + (w == null ? "—" : ((w < 1 ? w * 100 : w).toFixed(1) + "%")) + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    var geo = row.geo || [];
    if (geo.length) {
      html += blk("Geographic exposure", "<table><thead><tr><th>Region</th><th>Wgt</th></tr></thead><tbody>" +
        geo.map(function (x) {
          var w = num(x.w);
          return "<tr><td>" + esc(x.k) + "</td><td>" + (w == null ? "—" : ((w < 1 ? w * 100 : w).toFixed(1) + "%")) + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    if (holds.length) {
      html += blk("Top holdings (constituents)", "<table><thead><tr><th>#</th><th>Ticker</th><th>Name</th><th>Wgt</th></tr></thead><tbody>" +
        holds.slice(0, 16).map(function (h, i) {
          var w = num(h.weight != null ? h.weight : h.w);
          var tk = h.constituent_ticker || h.t || "";
          return "<tr><td>" + (h.constituent_rank || h.rank || (i + 1)) + "</td><td><a href='/chart.html?s=" + encodeURIComponent(tk) + "'>" +
            esc(tk) + "</a></td><td>" + esc(h.constituent_name || h.n || "") + "</td><td>" +
            (w != null ? ((w < 1 && w > -1 ? w * 100 : w).toFixed(2) + "%") : "—") + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    if (flows.length) {
      html += blk("Creation / redemption tape", "<table><thead><tr><th>Date</th><th>Flow</th><th>NAV</th><th>Shares</th></tr></thead><tbody>" +
        flows.slice(0, 16).map(function (r) {
          var f = num(r.fund_flow);
          return "<tr><td>" + esc(r.processed_date || "") + "</td><td class='" + cls(f) + "'>" + fmtBig(f) +
            "</td><td>" + fmt(num(r.nav), 3) + "</td><td>" + fmtBig(num(r.shares_outstanding)) + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    return html;
  }

  function renderDerivedFlow(pack) {
    var F = window.JHEtfFuse;
    var der = pack.derived || {};
    var desk = pack.etfRow || {};
    var html = "";
    if (!der || (!der.px_flow && der.hhi == null && der.crowding_pct == null && !der.flow_1d && der.implied_5d == null && !der.levered)) {
      html += "<div class=empty>No derived ETF Global print for this ticker yet. Fund creations live on the ETF tab; name-level $ is inferred (flow × weight), not a print.</div>";
      return html;
    }
    html += "<div class=kpi>" + [
      kpi("Sleeve", esc(der.sleeve || (desk.asset_class || "—"))),
      kpi("Flow 1D", fmtBig(der.flow_1d != null ? der.flow_1d : desk.flow_1d)),
      kpi("Flow 5D", fmtBig(der.flow_5d != null ? der.flow_5d : desk.flow_5d)),
      kpi("HHI", der.hhi == null ? "—" : Number(der.hhi).toFixed(0)),
      kpi("Px vs flow", esc((der.px_flow || "—").replace(/_/g, " "))),
      kpi("Crowding", der.crowding_pct == null ? "—" : Number(der.crowding_pct).toFixed(1) + "%")
    ].join("") + "</div>";
    html += blk("Honest read", table([
      ["Kind", esc(der.kind || "—")],
      ["Levered", der.levered ? ("YES · " + (der.leverage_style || der.levered_amount || "2x+")) : "no"],
      ["NAV 5D", der.nav_5d_pct == null ? "—" : der.nav_5d_pct.toFixed(2) + "%"],
      ["Top holding", esc(der.top || "—") + (der.top_w ? (" · " + (der.top_w * 100).toFixed(1) + "%") : "")],
      ["Implied hit on top", fmtBig(der.implied_top_1d)],
      ["Confirmed", der.confirmed ? "flow × weight and share delta agree (still inferred)" : "—"],
      ["Disagreed", der.disagreed ? "flow × weight vs share delta opposite — cash/custom basket or stale file" : "—"],
      ["Flow type", esc((der.flow_type || "—").replace(/_/g, " "))],
      ["Implied 5D (name)", fmtBig(der.implied_5d)]
    ]));
    html += "<div class=src>Fund creations = fact. Name-level dollars = fund flow × holdings weight (inferred). Not institutional volume.</div>";
    if (window.JHEtfDerived && pack.derivedDesk) {
      html += "<div id=dt-derived-mini></div>";
    }
    return html;
  }

  function paintBody(pack) {
    var body = document.getElementById("dtbody");
    if (!body) return;
    var d = pack.data || {};
    var bars = pack.bars || [];
    var q = pack.quote || {};
    var html = "";
    if (tab === "over") html = renderOver(d, bars, q, pack) + renderRelated(pack);
    else if (tab === "stats") html = renderStats(d, bars);
    else if (tab === "val") html = renderVal(d);
    else if (tab === "fin") html = renderFin(d);
    else if (tab === "est") html = renderEst(d);
    else if (tab === "div") html = renderDiv(d) + renderPolyDiv(pack);
    else if (tab === "news") html = renderPolyNews(pack);
    else if (tab === "tech") html = renderPolyTech(pack);
    else if (tab === "short") html = renderPolyShort(pack);
    else if (tab === "opt") html = renderPolyOpt(pack);
    else if (tab === "etf") html = renderPolyEtf(pack);
    else if (tab === "flow") html = renderDerivedFlow(pack);
    else html = renderHold(d, pack);
    var src = (pack.src && pack.src.length) ? pack.src.join(" · ") : "computed from chart bars";
    body.innerHTML = html + "<div class=src>" + esc(src) + " · delayed · not advice</div>";
  }

  function paintHead(pack) {
    var name = document.getElementById("dt-name");
    var px = document.getElementById("dt-px");
    var chg = document.getElementById("dt-chg");
    var x = pick(pack.data);
    var bars = pack.bars || [];
    var last = bars.length ? bars[bars.length - 1] : null;
    var prev = bars.length > 1 ? bars[bars.length - 2] : last;
    var price = last ? last.close : num(x.p.regularMarketPrice);
    var dlt = last && prev && prev.close ? (last.close - prev.close) / prev.close : null;
    if (name) name.textContent = (x.p.shortName || x.p.longName || pack.sym || "—") + "  ·  " + (pack.sym || "");
    if (px) px.textContent = price != null ? fmt(price, 2) : "—";
    if (chg) {
      chg.textContent = dlt == null ? "" : ((dlt >= 0 ? "+" : "") + (dlt * 100).toFixed(2) + "%");
      chg.style.color = dlt == null ? "" : (dlt >= 0 ? "#089981" : "#f23645");
    }
  }

  function paintTabs() {
    var el = document.getElementById("dttabs");
    if (!el) return;
    el.innerHTML = TABS.map(function (t) {
      return "<button type=button class='" + (tab === t[0] ? "on" : "") + "' data-t='" + t[0] + "'>" + t[1] + "</button>";
    }).join("");
    el.querySelectorAll("[data-t]").forEach(function (b) {
      b.onclick = function () {
        tab = b.getAttribute("data-t");
        paintTabs();
        var sym = window.jhActive;
        var pack = cache[sym];
        if (pack) paintBody(pack);
        var need = { tech: "polyTech", short: "polyShort", opt: "polyOpt", etf: "polyEtf" };
        var slot = need[tab];
        if (slot && pack && !pack[slot]) {
          loadKind(sym, tab).then(function (j) {
            if (!j || !cache[sym]) return;
            cache[sym][slot] = j;
            if (tab === "tech") cache[sym].src.push("Polygon indicators");
            if (tab === "short") cache[sym].src.push("Polygon short interest");
            if (tab === "opt") cache[sym].src.push("Polygon options");
            if (tab === "etf") cache[sym].src.push("ETF Global");
            if (window.jhActive === sym) paintBody(cache[sym]);
          }).catch(function () {});
        }
      };
    });
  }

  async function loadPack(sym) {
    var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
    var t = (window.jhBare || function (s) { return s; })(sym);
    var pack = { sym: sym, src: [], data: {}, bars: window.lastBars || [], quote: {} };
    if (window.lastBars && window.jhActive === sym) pack.bars = window.lastBars;
    function take(j, label) {
      if (!j || typeof j !== "object") return;
      pack.data = Object.assign(pack.data, j);
      if (j.price || j.summaryDetail || j.defaultKeyStatistics || j.financialData) pack.src.push(label);
    }
    try {
      var r = await fetch("/api/yahoo-fund?ticker=" + encodeURIComponent(t));
      var j = await r.json();
      if (j && (j.ok || j.price || j.summaryDetail)) take(j, "Yahoo fundamentals");
    } catch (e) {}
    try {
      var r2 = await fetch(PROXY + "/yf-quote?symbol=" + encodeURIComponent(t));
      var j2 = await r2.json();
      if (j2) {
        pack.quote = j2.quote || j2;
        if (j2.price) take(j2, "Yahoo quote");
        else pack.src.push("Yahoo quote");
      }
    } catch (e2) {}
    try {
      var r3 = await fetch(PROXY + "/yf-fund?symbol=" + encodeURIComponent(t));
      var j3 = await r3.json();
      if (j3) take(j3, "Yahoo modules");
    } catch (e3) {}
    if ((!pack.bars || pack.bars.length < 10) && window.klines) {
      try { pack.bars = await window.klines(sym, "1d", true); } catch (e4) {}
    }
    if (pack.src.indexOf("chart bars") < 0) pack.src.push("chart bars");
    try {
      var r4 = await fetch(PROXY + "/poly/ref?ticker=" + encodeURIComponent(t));
      var j4 = await r4.json();
      if (j4) {
        pack.polyNews = j4.news;
        pack.polyDiv = j4.dividends;
        pack.tickerDetail = j4.tickerDetail;
        pack.polySplits = j4.splits;
        pack.related = j4.related;
        pack.snapshot = j4.snapshot;
        pack.src.push("Polygon news/div/splits/related");
      }
    } catch (e5) {}
    try {
      var r5 = await fetch(PROXY + "/poly/etf?ticker=" + encodeURIComponent(t));
      var j5 = await r5.json();
      if (j5) {
        pack.polyEtf = j5;
        pack.src.push("ETF Global live");
      }
    } catch (e6) {}
    if (window.JHEtfFuse) {
      try {
        var row = await window.JHEtfFuse.of(t);
        var holders = await window.JHEtfFuse.reverse(t);
        var der = window.JHEtfFuse.ofDerived ? await window.JHEtfFuse.ofDerived(t) : null;
        if (row) pack.etfRow = window.JHEtfFuse.mergeLive(row, pack.polyEtf);
        pack.etfHolders = holders || [];
        pack.derived = der || null;
        if (row || (holders && holders.length) || der) pack.src.push("ETF desk warehouse");
      } catch (e7) {}
    }
    return pack;
  }

  async function loadKind(sym, kind) {
    var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
    var t = (window.jhBare || function (s) { return s; })(sym);
    var map = { tech: "tech", short: "short", opt: "options", etf: "etf" };
    var path = map[kind];
    if (!path) return null;
    var r = await fetch(PROXY + "/poly/" + path + "?ticker=" + encodeURIComponent(t));
    return r.json();
  }

  function openPanel(which) {
    if (which) tab = which;
    var ov = document.getElementById("dtype");
    if (!ov) return;
    ov.className = "on";
    paintTabs();
    var body = document.getElementById("dtbody");
    var sym = window.jhActive || "SPY";
    if (body) body.innerHTML = "<div class=empty>Loading " + esc(sym) + " fundamentals…</div>";
    paintHead({ data: {}, bars: window.lastBars || [], sym: sym });
    var run = function (pack) {
      cache[sym] = pack;
      paintHead(pack);
      paintBody(pack);
    };
    if (cache[sym]) run(cache[sym]);
    if (loading[sym]) return;
    loading[sym] = true;
    loadPack(sym).then(function (pack) {
      loading[sym] = false;
      run(pack);
    }).catch(function () {
      loading[sym] = false;
      if (body) body.innerHTML = "<div class=empty>Could not load fundamentals for " + esc(sym) + ".</div>";
    });
  }

  function closePanel() {
    var ov = document.getElementById("dtype");
    if (ov) ov.className = "";
  }

  window.jhOpenDataType = function (at, info) {
    var m = document.getElementById("menu");
    if (!m) { openPanel("over"); return; }
    m.className = "menu on";
    m.innerHTML =
      "<div class=lab>CHART</div>" +
      "<button data-dt=price>Price</button>" +
      "<div class=lab>DATA</div>" +
      "<button data-dt=over>Overview</button>" +
      "<button data-dt=stats>Statistics</button>" +
      "<button data-dt=val>Valuation / ratios</button>" +
      "<button data-dt=fin>Financials</button>" +
      "<button data-dt=est>Estimates</button>" +
      "<button data-dt=div>Dividends</button>" +
      "<button data-dt=news>News</button>" +
      "<button data-dt=tech>Technicals</button>" +
      "<button data-dt=short>Short interest</button>" +
      "<button data-dt=opt>Options</button>" +
      "<button data-dt=etf>ETF Global</button>" +
      "<button data-dt=hold>Holders</button>";
    if (at && at.getBoundingClientRect) {
      var r = at.getBoundingClientRect();
      m.style.left = Math.min(r.left, window.innerWidth - 230) + "px";
      m.style.top = (r.bottom + 4) + "px";
    }
    m.querySelectorAll("[data-dt]").forEach(function (b) {
      b.onclick = function () {
        m.className = "menu";
        var k = b.getAttribute("data-dt");
        if (k === "price") { closePanel(); return; }
        openPanel(k);
      };
    });
  };

  function bind() {
    var x = document.getElementById("dtx");
    if (x && !x.dataset.bound) { x.dataset.bound = "1"; x.onclick = closePanel; }
    var back = document.getElementById("dt-chart");
    if (back && !back.dataset.bound) { back.dataset.bound = "1"; back.onclick = closePanel; }
    var ov = document.getElementById("dtype");
    if (ov && !ov.dataset.bound) {
      ov.dataset.bound = "1";
      ov.onclick = function (e) { if (e.target === ov) closePanel(); };
    }
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        var d = document.getElementById("dtype");
        if (d && d.classList.contains("on")) closePanel();
      }
    });
  }
  bind();
})();
