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
    ["ident", "Identity"],
    ["chain", "On-chain"],
    ["press", "Pressure"],
    ["revx", "Revisions"],
    ["qual", "Quality"],
    ["pead", "Surprise"],
    ["priced", "Priced-in"],
    ["boom", "Stack"],
    ["sqz", "Squeeze"],
    ["ivol", "Inst vol"],
    ["inst", "13F"],
    ["div", "Dividends"],
    ["news", "News"],
    ["tech", "Technicals"],
    ["short", "Short"],
    ["opt", "Options"],
    ["etf", "Holdings"],
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
  function retCal(bars, days) {
    if (!bars || bars.length < 2 || !days) return null;
    var last = bars[bars.length - 1];
    if (!last || !last.close) return null;
    var t0 = last.time - days * 86400, b = null, i;
    for (i = 0; i < bars.length; i++) if (bars[i].time >= t0) { b = bars[i]; break; }
    if (!b || !b.close) return null;
    return (last.close / b.close - 1) * 100;
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
    var fmp = d && d.fmp && d.fmp.row;
    var kpis = [
      kpi("Last", px != null ? fmt(px, 2) : "—"),
      kpi("Mkt cap", fmtBig(num(x.p.marketCap) || num(x.sd.marketCap) || (fmp && fmp.mkt_cap))),
      kpi("P/E", fmt(num(x.sd.trailingPE) || num(x.ks.trailingPE) || (fmp && fmp.pe))),
      kpi("Fwd P/E", fmt(num(x.sd.forwardPE) || num(x.ks.forwardPE))),
      kpi("EPS", fmt(num(x.ks.trailingEps) || num(x.fd.trailingEps))),
      kpi("Div yld", num(x.sd.dividendYield) != null ? (num(x.sd.dividendYield) * 100).toFixed(2) + "%" : "—")
    ].join("");
    var rets = [
      ["1D", retN(bars, 1)],
      ["1W", retN(bars, 5)],
      ["1M", retCal(bars, 30)],
      ["3M", retCal(bars, 91)],
      ["YTD", ytd(bars)],
      ["1Y", retCal(bars, 365)]
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
    if (window.JHEtfFuse && window.JHEtfFuse.isFund(pack.etfRow, pack.polyEtf)) etfBit = renderPolyEtf(pack, { compact: true });
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
    var fmp = d && d.fmp && d.fmp.row;
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
      ["Market cap", fmtBig(num(x.p.marketCap) || num(x.sd.marketCap) || (fmp && fmp.mkt_cap))],
      ["P/E (TTM)", fmt(num(x.sd.trailingPE) || num(x.ks.trailingPE) || (fmp && fmp.pe))],
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

  // ---- Identity desk: OpenFIGI via the symbology master (data/symbology/master.json). FIGI/shareClassFIGI/type/exch only.
  // CUSIP/ISIN never come from OpenFIGI (licence); when present they are the SEC/13F spine and are labelled so.
  var SYMBOLOGY = null;
  async function symbologyRow(t) {
    try {
      if (!SYMBOLOGY) { var r = await fetch("/data/symbology/master.json", { cache: "no-store" }); SYMBOLOGY = r.ok ? await r.json() : {}; }
    } catch (e) { SYMBOLOGY = {}; }
    var recs = SYMBOLOGY.by_ticker || SYMBOLOGY.tickers || SYMBOLOGY;
    return lookupRow(recs, t);
  }
  function renderIdent(d) {
    var r = d && d.ident;
    if (!r) return "<div class=note>Identity: no symbology-master row for this symbol (figi_status unknown, nothing invented)</div>";
    var mapped = !!r.figi;
    var rows = [
      ["FIGI", mapped ? esc(r.figi) : "figi_status=" + esc(r.figi_status || "no_match")],
      ["Share-class FIGI", esc(r.shareClassFIGI || r.share_class_figi || "—")],
      ["Security type", esc(r.securityType || r.security_type || "—")],
      ["Exchange code", esc(r.exchCode || r.exch_code || "—")],
      ["Name", esc(r.name || "—")]
    ];
    var spine = [];
    if (r.cusip) spine.push(["CUSIP (SEC/13F spine, not OpenFIGI)", esc(r.cusip)]);
    if (r.isin) spine.push(["ISIN (derived from the SEC CUSIP, not OpenFIGI)", esc(r.isin)]);
    if (r.lei) spine.push(["LEI (GLEIF)", esc(r.lei)]);
    return blk("Identity — OpenFIGI (symbology master" + (SYMBOLOGY && SYMBOLOGY.generated_at ? ", " + String(SYMBOLOGY.generated_at).slice(0, 10) : "") + ")", table(rows)) +
      (spine.length ? blk("Filing identifiers — SEC / 13F spine", table(spine)) : "");
  }
  // ---- On-chain desk: CryptoQuant EOD on-chain (BTC/ETH proxies only). Never LIVE, never mixed with equity/ETF flow.
  var CQ_PROXIES = { BTC: 1, ETH: 1, IBIT: 1, FBTC: 1, BITB: 1, ETHA: 1, MSTR: 1, COIN: 1, MARA: 1, RIOT: 1, "BTC-USD": 1, "ETH-USD": 1, BTCUSD: 1, ETHUSD: 1 };
  var CQ_ONCHAIN = null, CQ_SERIES = null;
  async function cqPack(t) {
    t = jhFundTicker(t);
    if (!CQ_PROXIES[t]) return { proxy: false };
    try {
      if (!CQ_ONCHAIN) { var r = await fetch("/data/cryptoquant-onchain.json", { cache: "no-store" }); CQ_ONCHAIN = r.ok ? await r.json() : {}; }
      if (!CQ_SERIES) { var r2 = await fetch("/data/cryptoquant-series.json", { cache: "no-store" }); CQ_SERIES = r2.ok ? await r2.json() : {}; }
    } catch (e) { CQ_ONCHAIN = CQ_ONCHAIN || {}; CQ_SERIES = CQ_SERIES || {}; }
    return { proxy: true, onchain: CQ_ONCHAIN, series: CQ_SERIES };
  }
  function cqMetric(m, keys) {
    for (var i = 0; i < keys.length; i++) { var v = m[keys[i]]; if (v && typeof v === "object") return v; }
    return null;
  }
  function renderChain(d) {
    var c = d && d.chain;
    if (!c || !c.proxy) return "<div class=note>On-chain: no CQ series — this symbol is not a BTC/ETH proxy (IBIT, FBTC, BITB, ETHA, MSTR, COIN, MARA, RIOT, BTC, ETH)</div>";
    var m = (c.onchain && c.onchain.metrics) || {};
    var head = [
      ["MVRV", ["btc_mvrv", "mvrv"]], ["SOPR", ["btc_sopr", "sopr"]], ["MPI", ["btc_mpi", "mpi"]], ["Whale ratio", ["btc_whale_ratio", "whale_ratio", "exchange_whale_ratio"]],
      ["Exchange netflow", ["btc_exch_netflow", "btc_exchange_netflow", "exch_netflow", "netflow"]], ["NUPL", ["btc_nupl", "nupl"]], ["SSR", ["btc_ssr", "ssr", "stablecoin_supply_ratio"]], ["Realized price", ["btc_realized_price", "realized_price"]]
    ];
    var kp = head.map(function (h) {
      var v = cqMetric(m, h[1]);
      var val = v ? (v.value != null ? v.value : (v.latest != null ? v.latest : v.last)) : null;
      return kpi(h[0], fmt(num(val)) + (v && v.as_of ? " <small>" + esc(String(v.as_of).slice(0, 10)) + "</small>" : ""));
    }).join("");
    var label = (c.onchain && c.onchain.label) || "CryptoQuant EOD on-chain";
    var stamp = c.onchain && (c.onchain.generated_at || c.onchain.as_of);
    var ser = (c.series && (c.series.series || c.series)) || {};
    var counts = Object.keys(ser).filter(function (k) { return Array.isArray(ser[k]); }).slice(0, 8).map(function (k) { return [esc(k), ser[k].length + " pts"]; });
    return "<div class=kpi>" + kp + "</div>" +
      blk("On-chain — " + esc(label) + (stamp ? " · as of " + esc(String(stamp).slice(0, 10)) : "") + " (cadence EOD; never LIVE; not blended with ETF or FMP data)",
          counts.length ? table(counts) : "<div class=note>series file present but empty</div>");
  }
  function renderValFmp(f) {
    var r = f.row;
    var label = "FMP EOD/TTM" + (f.key_status === "unauthorized" ? " — KEY REJECTED (stale)" : "") + " · as of " + String(f.as_of || "").slice(0, 10);
    return "<div class=kpi>" + [
      kpi("P/E (TTM)", fmt(num(r.pe))),
      kpi("PEG", fmt(num(r.peg))),
      kpi("P/S (TTM)", fmt(num(r.ps))),
      kpi("P/B (TTM)", fmt(num(r.pb))),
      kpi("EV/EBITDA", fmt(num(r.ev_ebitda))),
      kpi("Div yield %", fmt(num(r.div_yield)))
    ].join("") + "</div>" +
      blk("Valuation — " + label, table([
        ["P/E (TTM)", fmt(num(r.pe))],
        ["P/E 10y low / high", fmt(num(r.pe_low)) + " / " + fmt(num(r.pe_high))],
        ["P/E percentile", fmt(num(r.pe_pctile))],
        ["PEG", fmt(num(r.peg))],
        ["Price / sales (TTM)", fmt(num(r.ps))],
        ["Price / book (TTM)", fmt(num(r.pb))],
        ["EV / EBITDA", fmt(num(r.ev_ebitda))],
        ["Net debt / EBITDA", fmt(num(r.net_debt_ebitda))],
        ["Market cap", fmtBig(num(r.mkt_cap))],
        ["Next earnings", esc(r.next_earnings || "—")]
      ]));
  }
  function fmpMissNote(f) {
    if (f && f.http !== 200) return "<div class=note>FMP harvest HTTP " + esc(String(f.http)) + " — not a missing ticker (the harvest file could not be read)</div>";
    return "<div class=note>Yahoo fallback — no FMP row for this symbol yet (harvest read OK)</div>";
  }
  function renderVal(d) {
    if (d && d.fmp && d.fmp.row) return renderValFmp(d.fmp);
    var x = pick(d);
    return fmpMissNote(d && d.fmp) + "<div class=kpi>" + [
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

  function renderFinFmp(f) {
    var rows = (f.row.financials || []).slice().sort(function (a, b) {
      return Number(b.year) - Number(a.year);
    }).slice(0, 10);
    var hdr = "<tr><th>Year</th><th>Revenue</th><th>Net income</th><th>EPS</th><th>GM %</th><th>OM %</th><th>NM %</th><th>FCF</th><th>FCF %</th></tr>";
    var body = rows.map(function (y) {
      return "<tr><td>" + esc(y.year) + "</td><td>" + fmtBig(num(y.revenue)) + "</td><td>" + fmtBig(num(y.netIncome)) + "</td><td>" + fmt(num(y.eps)) +
        "</td><td>" + fmt(num(y.gm)) + "</td><td>" + fmt(num(y.om)) + "</td><td>" + fmt(num(y.nm)) + "</td><td>" + fmtBig(num(y.fcf)) + "</td><td>" + fmt(num(y.fcfm)) + "</td></tr>";
    }).join("");
    var label = "FMP FILING (annual)" + (f.key_status === "unauthorized" ? " — KEY REJECTED (stale)" : "") + " · as of " + String(f.as_of || "").slice(0, 10);
    return blk("Financials — " + label, "<table><thead>" + hdr + "</thead><tbody>" + (body || "<tr><td colspan=9>no annual rows</td></tr>") + "</tbody></table>") +
      blk("Quality", table([["Gross-margin trend", esc(f.row.gm_trend || "—")], ["Cash conversion", fmt(num(f.row.cash_conv))], ["Accruals", fmt(num(f.row.accruals))],
                            ["Current ratio", fmt(num(f.row.cur_ratio))], ["Interest cover", fmt(num(f.row.int_cov))], ["Share change %", fmt(num(f.row.share_chg_pct))]]));
  }
  function renderFin(d) {
    if (d && d.fmp && d.fmp.row) return renderFinFmp(d.fmp);
    var j = d || {};
    var miss = fmpMissNote(d && d.fmp);
    var inc = ((j.incomeStatementHistory || {}).incomeStatementHistory) || j.income || [];
    var bal = ((j.balanceSheetHistory || {}).balanceSheetStatements) || j.balance || [];
    var cf = ((j.cashflowStatementHistory || {}).cashflowStatements) || j.cash || [];
    var html = miss;
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
  function renderPolyEtf(pack, opts) {
    opts = opts || {};
    var compact = !!opts.compact;
    var F = window.JHEtfFuse;
    var j = pack.polyEtf || {};
    var row = pack.etfRow || {};
    var flows = (F ? F.polyRows(j.flows) : (j.flows && j.flows.results) || []) || [];
    var prof = ((F ? F.polyRows(j.profile) : (j.profile && j.profile.results) || [])[0]) || {};
    var holds = [];
    var book = pack.etfConstituents || {};
    if (book.rows && book.rows.length) {
      holds = book.rows;
    } else {
      holds = (F ? F.polyRows(j.holdings) : (j.holdings && j.holdings.results) || []) || [];
      if (row.top && row.top.length && !holds.length) {
        holds = row.top.map(function (h) { return { t: h.t, n: h.n, w: h.w, mv: h.mv, rank: h.rank, sh: h.sh }; });
      } else {
        holds = holds.map(function (h) {
          return { t: h.constituent_ticker || h.t, n: h.constituent_name || h.n, w: h.weight != null ? h.weight : h.w, mv: h.market_value || h.mv, rank: h.constituent_rank || h.rank, sh: h.shares || h.sh };
        });
      }
    }
    if (!flows.length && !Object.keys(prof).length && !row.aum && !(pack.etfHolders && pack.etfHolders.length) && !holds.length) {
      return "<div class=empty>ETF Global has no fund print for this ticker — if it is a stock, open Holders for the look-through of funds that own it.</div>";
    }
    var latest = flows[0] || {};
    var aum = num(row.aum) || num(prof.aum);
    var er = num(row.er) || num(prof.net_expense_ratio) || num(prof.expense_ratio);
    var f1 = num(row.flow_1d) != null ? num(row.flow_1d) : num(latest.fund_flow);
    var f5 = num(row.flow_5d);
    var f21 = num(row.flow_21d);
    var f63 = num(row.flow_63d);
    if (f5 == null && flows.length) { f5 = 0; flows.slice(0, 5).forEach(function (r) { f5 += num(r.fund_flow) || 0; }); }
    if (f21 == null && flows.length) { f21 = 0; flows.slice(0, 21).forEach(function (r) { f21 += num(r.fund_flow) || 0; }); }
    var erTxt = F ? F.fmtEr(er) : (er == null ? "—" : er.toFixed(3) + "%");
    var nHold = book.n || holds.length || row.holdings_n;
    var holdNote = row.holdings_complete === false
      ? "PARTIAL " + (nHold || "—") + " names (HHI withheld)"
      : ((nHold || "—") + " names");
    var rk = pack.etfRank || {};
    var wins = rk.windows || {};
    function winKpi(key, fallbackLabel) {
      var w = wins[key] || {};
      var vs = num(w.vs);
      var lab = w.label || fallbackLabel;
      var extra = w.rank != null ? ("#" + w.rank + "/" + w.n) : "";
      return kpi("vs SPX " + lab, vs == null ? "—" : ((vs >= 0 ? "+" : "") + vs.toFixed(2) + "%"), extra);
    }
    var html = "<div class=kpi>" + [
      kpi("AUM", fmtBig(aum)),
      kpi("ER", erTxt),
      kpi("Flow 1D", "<span class='" + cls(f1) + "'>" + fmtBig(f1) + "</span>", esc(row.flow_label || "")),
      kpi("Flow 5D", "<span class='" + cls(f5) + "'>" + fmtBig(f5) + "</span>"),
      kpi("Flow 21D", "<span class='" + cls(f21) + "'>" + fmtBig(f21) + "</span>"),
      kpi("Flow 63D", "<span class='" + cls(f63) + "'>" + fmtBig(f63) + "</span>"),
      kpi("Holdings", fmt(nHold), holdNote),
      kpi("HHI", row.hhi != null ? fmt(row.hhi, 0) : "—"),
      winKpi("d", "1D"),
      winKpi("w", "1W"),
      winKpi("m", "1M"),
      winKpi("q", "3M")
    ].join("") + "</div>";

    html += blk("ETF Global profile (paid · creations, not volume)", table([
      ["Name", esc(row.name || prof.description || prof.fund_name || "—")],
      ["Issuer", esc(row.issuer || prof.issuer || prof.advisor || "—")],
      ["Asset class", esc(row.asset_class || prof.asset_class || "—")],
      ["Category", esc(row.category || prof.category || prof.focus || "—")],
      ["Benchmark", esc(row.benchmark || prof.primary_benchmark || "—")],
      ["Inception", esc(row.inception || prof.inception_date || "—")],
      ["Shares out", fmtBig(num(row.shares) || num(latest.shares_outstanding))],
      ["NAV", fmt(num(row.nav) || num(latest.nav), 3)],
      ["Leverage", esc(row.leverage_style || row.leverage || "—") + (row.levered_amount ? (" · ×" + row.levered_amount) : "")],
      ["Flow as-of", esc(row.flow_asof || row.flow_effective || latest.processed_date || "—")],
      ["Beta vs SPY", rk.beta != null ? Number(rk.beta).toFixed(2) : "—"]
    ]));

    html += renderEtfVsSpy(pack);
    html += renderEtfFlows(pack, flows, f1, f5, f21, f63);
    if (!compact) {
      html += renderEtfHoldingsAll(pack, holds, nHold);
      html += renderEtfEmerging13f(pack, holds);
    } else if (holds.length) {
      html += blk("Holdings (top " + Math.min(12, holds.length) + " of " + (nHold || holds.length) + ")",
        "<table class=wide><thead><tr><th>#</th><th>Ticker</th><th>Name</th><th>Wgt</th><th>Mkt val</th></tr></thead><tbody>" +
        holds.slice(0, 12).map(function (h, i) {
          var w = num(h.w);
          var tk = h.t || "";
          return "<tr><td>" + (h.rank || (i + 1)) + "</td><td><a href='/chart.html?s=" + encodeURIComponent(tk) + "'>" +
            esc(tk) + "</a></td><td>" + esc(h.n || "") + "</td><td>" +
            (w != null ? ((w < 1 && w > -1 ? w * 100 : w).toFixed(2) + "%") : "—") + "</td><td>" +
            fmtBig(h.mv) + "</td></tr>";
        }).join("") + "</tbody></table>" +
        "<div class=note>Full book, vs-SPX ranks, inflows/outflows and 13F emerging names are on the Holdings tab.</div>");
    }

    var sec = row.sector || [];
    if (sec.length) {
      html += blk("Sector exposure", "<table><thead><tr><th>Sector</th><th>Wgt</th></tr></thead><tbody>" +
        sec.map(function (x) {
          var w = num(x.w);
          return "<tr><td>" + esc(x.k) + "</td><td>" + (w == null ? "—" : ((w < 1 ? w * 100 : w).toFixed(1) + "%")) + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    var geo = row.geo || [];
    if (geo.length && !compact) {
      html += blk("Geographic exposure", "<table><thead><tr><th>Region</th><th>Wgt</th></tr></thead><tbody>" +
        geo.map(function (x) {
          var w = num(x.w);
          return "<tr><td>" + esc(x.k) + "</td><td>" + (w == null ? "—" : ((w < 1 ? w * 100 : w).toFixed(1) + "%")) + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    return html;
  }

  function renderEtfVsSpy(pack) {
    var rk = pack.etfRank || {};
    var wins = rk.windows || {};
    var bars = pack.bars || [];
    var order = ["d", "w", "m", "q"];
    var tapeN = { d: 1, w: 5, m: 21, q: 63 };
    var tapeLab = { d: "1D", w: "1W", m: "1M", q: "3M" };
    function cell(v, kind) {
      if (v == null || v === "") return "<td>—</td>";
      var n = num(v);
      if (n == null) return "<td>—</td>";
      if (kind === "rank") return "<td>#" + n + "</td>";
      var txt = (n >= 0 ? "+" : "") + n.toFixed(2) + "%";
      return "<td class='" + cls(n) + "'>" + txt + "</td>";
    }
    var head = "<table class=wide><thead><tr><th>Window</th><th>ETF (census)</th><th>SPY</th><th>vs SPY</th><th>Rank vs ETFs</th><th>This tape</th></tr></thead><tbody>";
    var body = order.map(function (k) {
      var w = wins[k] || {};
      var tape = retN(bars, tapeN[k]);
      var rankTxt = (w.rank != null) ? ("#" + w.rank + " / " + w.n) : "—";
      return "<tr><td>" + esc(w.label || tapeLab[k]) + (w.already_vs_spy ? " RS" : "") + "</td>" +
        cell(w.etf) + cell(w.spy) + cell(w.vs) +
        "<td>" + rankTxt + "</td>" + cell(tape) + "</tr>";
    }).join("");
    var stamp = rk.as_of ? String(rk.as_of).slice(0, 10) : "—";
    var note = rk.in_universe
      ? "Census EOD (" + stamp + ") · " + (rk.n_universe || 80) + " liquid wrappers. 1D/1W/1M are total return; 3M is rs_13w already vs SPY (SPY = 0). Rank is among names with a print that window. Tape = this chart's daily bars (not mixed into rank)."
      : "This ticker is not in the 80-name census universe — vs SPY / rank need a census row. Tape return is still from this chart.";
    return blk("Strength vs S&P 500 · rank vs other ETFs", head + body + "</tbody></table><div class=note>" + note + "</div>");
  }

  function renderEtfFlows(pack, flows, f1, f5, f21, f63) {
    var row = pack.etfRow || {};
    var w = row.flow_windows || {};
    function wlab(k, usd) {
      var x = w[k];
      if (!x) return fmtBig(usd);
      var s = fmtBig(x.usd != null ? x.usd : usd);
      if (x.complete === false) s += " · " + (x.n_observed || 0) + "/" + (x.n_requested || "?") + " sess (partial)";
      return s;
    }
    var html = blk("Inflows / outflows — Massive ETF Global creations", table([
      ["Print", esc(row.flow_label || "—") + (row.flow_z != null ? (" · z " + Number(row.flow_z).toFixed(2)) : "")],
      ["1D", "<span class='" + cls(f1) + "'>" + wlab("1d", f1) + "</span>"],
      ["5D", "<span class='" + cls(f5) + "'>" + wlab("5d", f5) + "</span>"],
      ["21D", "<span class='" + cls(f21) + "'>" + wlab("21d", f21) + "</span>"],
      ["63D", "<span class='" + cls(f63) + "'>" + wlab("63d", f63) + "</span>"],
      ["Effective", esc(row.flow_effective || row.flow_asof || "—")]
    ]));
    var hist = (row.flow_hist && row.flow_hist.length) ? row.flow_hist : (flows || []).map(function (r) {
      return { d: r.processed_date || r.effective_date, f: num(r.fund_flow), n: num(r.nav) };
    });
    if (hist.length) {
      html += blk("Creation / redemption tape", "<table class=wide><thead><tr><th>Date</th><th>Flow</th><th>NAV</th></tr></thead><tbody>" +
        hist.slice(0, 18).map(function (r) {
          var f = num(r.f != null ? r.f : r.fund_flow);
          return "<tr><td>" + esc(r.d || r.processed_date || "") + "</td><td class='" + cls(f) + "'>" + fmtBig(f) +
            "</td><td>" + fmt(num(r.n != null ? r.n : r.nav), 3) + "</td></tr>";
        }).join("") + "</tbody></table>");
    }
    return html;
  }

  function instIdx() {
    return (INST13F && INST13F.idx) || {};
  }

  function renderEtfHoldingsAll(pack, holds, nHold) {
    if (!holds || !holds.length) {
      return blk("Holdings", "<div class=empty>No constituents in the harvest yet (desk top + holdings-index + live fill all empty). Nothing invented.</div>");
    }
    var idx = instIdx();
    var rows = holds.map(function (h, i) {
      var tk = h.t || "";
      var inst = idx[jhFundTicker(tk)] || {};
      var neu = num(inst.n_funds_new_position) || 0;
      var add = num(inst.n_funds_adding) || 0;
      var badge = neu > 0 ? "<span class=pill-fresh>NEW 13F ×" + neu + "</span>" : (add > 0 ? "<span class=pill-new>ADD ×" + add + "</span>" : "");
      var w = num(h.w);
      var q = (tk + " " + (h.n || "")).toUpperCase().replace(/"/g, "");
      return "<tr data-q=\"" + esc(q) + "\"><td>" + (h.rank || (i + 1)) + "</td><td><a href='/chart.html?s=" + encodeURIComponent(tk) + "'>" +
        esc(tk) + "</a> " + badge + "</td><td>" + esc(h.n || "") + "</td><td>" +
        (w != null ? ((w < 1 && w > -1 ? w * 100 : w).toFixed(2) + "%") : "—") + "</td><td>" +
        fmtBig(h.mv) + "</td><td>" + (neu || add ? (("+" + add + " new " + neu)) : "—") + "</td><td class='" +
        cls(inst.net_flow_usd) + "'>" + fmtBig(inst.net_flow_usd) + "</td></tr>";
    }).join("");
    var complete = (pack.etfConstituents && pack.etfConstituents.complete === false)
      ? "PARTIAL book"
      : ("showing " + holds.length + (nHold && nHold > holds.length ? " of " + nHold : ""));
    if (!pack._hydrated && nHold && holds.length < nHold) complete += " · loading full book…";
    return blk("Holdings — all constituents",
      "<div class=hold-tools><input id=etf-hold-q class=hold-q placeholder=\"Filter holdings… NVDA, semiconductor, 13F\" autocomplete=off><span id=etf-hold-n>" +
      complete + "</span></div>" +
      "<div class=hold-scroll><table class=wide id=etf-hold-tbl><thead><tr><th>#</th><th>Ticker</th><th>Name</th><th>Wgt</th><th>Mkt val</th><th>13F add/new</th><th>13F net $</th></tr></thead><tbody>" +
      rows + "</tbody></table></div>" +
      "<div class=note>Weights from ETF Global constituents (look-through). 13F columns are quarterly lagged filings, not live prints. Filter is local.</div>");
  }

  function renderEtfEmerging13f(pack, holds) {
    var desk = pack.desk13f || {};
    var ns = desk.new_since || {};
    var idx = instIdx();
    var holdMap = {};
    (holds || []).forEach(function (h) {
      var tk = jhFundTicker(h.t);
      if (tk) holdMap[tk] = h;
    });
    function take(list, extra) {
      var out = [];
      (list || []).forEach(function (r) {
        var tk = jhFundTicker(r.ticker);
        if (!tk || !holdMap[tk]) return;
        out.push(Object.assign({ w: holdMap[tk].w, hold_n: holdMap[tk].n }, r, extra || {}));
      });
      return out;
    }
    var emerging = [];
    Object.keys(holdMap).forEach(function (tk) {
      var r = idx[tk];
      if (!r) return;
      var neu = num(r.n_funds_new_position) || 0;
      if (neu <= 0) return;
      emerging.push(Object.assign({ ticker: tk, w: holdMap[tk].w, hold_n: holdMap[tk].n }, r));
    });
    emerging.sort(function (a, b) {
      return (num(b.n_funds_new_position) || 0) - (num(a.n_funds_new_position) || 0) ||
        (num(b.bought_usd) || 0) - (num(a.bought_usd) || 0);
    });
    var smid = take(ns.new_small_mid_micro, { radar: "small/mid/micro first appearance" });
    var freshNames = take(ns.new_names, { radar: "first seen on 13F ledger" });
    function tbl(list, colsHint) {
      if (!list.length) return "<div class=empty>None of this ETF's holdings match this 13F cut.</div>";
      return "<table class=wide><thead><tr><th>Ticker</th><th>Name</th><th>Wgt</th><th>New funds</th><th>Adding</th><th>Bought $</th><th>Net $</th><th>Cap</th></tr></thead><tbody>" +
        list.slice(0, 40).map(function (r) {
          var tk = r.ticker || "";
          var w = num(r.w);
          var fresh = r.fresh ? " <span class=pill-fresh>FRESH</span>" : "";
          return "<tr><td><a href='/chart.html?s=" + encodeURIComponent(tk) + "'>" + esc(tk) + "</a>" + fresh +
            "</td><td>" + esc(r.name || r.hold_n || "") + "</td><td>" +
            (w != null ? ((w < 1 && w > -1 ? w * 100 : w).toFixed(2) + "%") : "—") + "</td><td>" +
            fmt(r.n_funds_new_position) + "</td><td>" + fmt(r.n_funds_adding) + "</td><td class='" +
            cls(r.bought_usd) + "'>" + fmtBig(r.bought_usd) + "</td><td class='" + cls(r.net_flow_usd) + "'>" +
            fmtBig(r.net_flow_usd) + "</td><td>" + esc(r.cap_tier || "—") + "</td></tr>";
        }).join("") + "</tbody></table>";
    }
    var qtr = desk.as_of_quarter || ns.generated_at || "—";
    var html = blk("13F radar — new buys inside this ETF",
      "<div class=note>Quarter " + esc(String(qtr).slice(0, 10)) + " · lagged ~45d. A name is 'new' when n_funds_new_position > 0 (first appearance in that filer's book this quarter), not a live print. " +
      emerging.length + " holdings newly bought by at least one 13F filer.</div>" + tbl(emerging));
    html += blk("Emerging — small/mid/micro first appearance that this ETF already holds",
      "<div class=note>From 13F desk new_since (72h ledger). Fresh = first seen in the last 72h. Intersection with this ETF's constituents only — empty means the emerging names are not in this book.</div>" +
      tbl(smid));
    if (freshNames.length) {
      html += blk("13F ledger — names first seen recently that sit in this book",
        tbl(freshNames) + "<div class=note>Ledger first_seen is when THIS system first observed the name, not the SEC file date.</div>");
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
      ["Desk risk", esc(((pack.derivedDesk && pack.derivedDesk.verdicts) || pack.risk || {}).risk || "—")],
      ["Fear / greed", esc(((pack.derivedDesk && pack.derivedDesk.verdicts) || pack.risk || {}).fear_greed || "—")],
      ["Wrapper", esc(((pack.derivedDesk && pack.derivedDesk.verdicts) || pack.risk || {}).wrapper || "—")],
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


  function indexByTicker(doc) {
    var m = {};
    if (!doc || typeof doc !== "object") return m;
    function add(row, bucket) {
      if (!row || typeof row !== "object") return;
      var tk = jhFundTicker(row.ticker || row.symbol || row.sym || "");
      if (!tk) return;
      if (!m[tk]) m[tk] = { ticker: tk, buckets: [] };
      if (m[tk].buckets.indexOf(bucket) < 0) m[tk].buckets.push(bucket);
      Object.keys(row).forEach(function (k) { if (m[tk][k] == null) m[tk][k] = row[k]; });
    }
    Object.keys(doc).forEach(function (k) {
      var v = doc[k];
      if (Array.isArray(v)) v.forEach(function (row) { add(row, k); });
      else if (v && typeof v === "object" && (k === "by_ticker" || k === "tickers" || k === "ticker_map" || k === "all_tickers")) {
        if (Array.isArray(v)) v.forEach(function (row) { add(row, k); });
        else Object.keys(v).forEach(function (tk) {
          var row = v[tk];
          if (row && typeof row === "object" && !Array.isArray(row)) add(Object.assign({ ticker: tk }, row), k);
        });
      }
    });
    return m;
  }
  var FLOW_LT = null, REV_DOC = null, QUAL_DOC = null;
  async function loadJsonOnce(holder, url) {
    if (holder.v) return holder.v;
    try {
      var r = await fetch(url, { cache: "no-store" });
      holder.v = r.ok ? await r.json() : {};
    } catch (e) { holder.v = {}; }
    holder.idx = indexByTicker(holder.v);
    return holder.v;
  }
  var FLOW_CF = null, PEAD_DOC = null, GF_DOC = null, BENEISH_DOC = null, BOOM_DOC = null, SQ_DOC = null;
  function pickIdx(holder, t) {
    t = jhFundTicker(t);
    var idx = holder.idx || {};
    var doc = holder.v || {};
    return idx[t] || (doc.by_ticker && (doc.by_ticker[t] || doc.by_ticker[t.replace(".","-")])) ||
      (doc.tickers && (doc.tickers[t] || doc.tickers[t.replace(".","-")])) ||
      (doc.ticker_map && (doc.ticker_map[t] || doc.ticker_map[t.replace(".","-")])) || null;
  }
  async function pressureRow(t) {
    await loadJsonOnce(FLOW_LT || (FLOW_LT = {}), "/data/flow-lookthrough.json");
    var row = pickIdx(FLOW_LT, t);
    return { row: row, doc: FLOW_LT.v || {}, as_of: (FLOW_LT.v || {}).generated_at };
  }
  async function confluenceRow(t) {
    await loadJsonOnce(FLOW_CF || (FLOW_CF = {}), "/data/flow-confluence.json");
    var row = pickIdx(FLOW_CF, t);
    if (row && typeof row === "object") row = Object.assign({ ticker: jhFundTicker(t) }, row);
    return { row: row, doc: FLOW_CF.v || {}, as_of: (FLOW_CF.v || {}).generated_at };
  }
  async function peadRow(t) {
    await loadJsonOnce(PEAD_DOC || (PEAD_DOC = {}), "/data/earnings-pead.json");
    return { row: pickIdx(PEAD_DOC, t), doc: PEAD_DOC.v || {}, as_of: (PEAD_DOC.v || {}).generated_at };
  }
  async function pricedRow(t) {
    await loadJsonOnce(GF_DOC || (GF_DOC = {}), "/data/gf-value.json");
    return { row: pickIdx(GF_DOC, t), doc: GF_DOC.v || {}, as_of: (GF_DOC.v || {}).generated_at };
  }
  async function boomRow(t) {
    await loadJsonOnce(BOOM_DOC || (BOOM_DOC = {}), "/data/boom-radar.json");
    return { row: pickIdx(BOOM_DOC, t), doc: BOOM_DOC.v || {}, as_of: (BOOM_DOC.v || {}).generated_at };
  }
  async function squeezeRow(t) {
    await loadJsonOnce(SQ_DOC || (SQ_DOC = {}), "/data/squeeze-pretrigger.json");
    var row = pickIdx(SQ_DOC, t);
    return { row: row, doc: SQ_DOC.v || {}, as_of: (SQ_DOC.v || {}).as_of || (SQ_DOC.v || {}).generated_at };
  }
  var INST13F = null;
  var DESK13F = null;
  async function inst13fRow(t) {
    await loadJsonOnce(INST13F || (INST13F = {}), "/data/13f-by-ticker.json");
    var row = pickIdx(INST13F, t);
    return { row: row, doc: INST13F.v || {}, as_of: (INST13F.v || {}).generated_at, quarter: (INST13F.v || {}).as_of_quarter };
  }
  async function desk13fDoc() {
    await loadJsonOnce(DESK13F || (DESK13F = {}), "/data/13f-desk.json");
    return DESK13F.v || {};
  }
  function renderInst13f(d) {
    var p = d && d.inst13f || {};
    var r = p.row;
    var note = "<div class=note>SEC 13F is quarterly and lagged. Additions/trims are reported holdings, not live institutional prints. Compact index — full tape is 20MB off this overlay.</div>";
    if (!r) return note + "<div class=empty>No 13F compact row for this symbol yet (needs justhodl-13f-positions to publish data/13f-by-ticker.json). Board: <a href='/13f.html'>13f</a>.</div>";
    return note + "<div class=kpi>" + [
      kpi("Quarter", esc(p.quarter || "—")),
      kpi("Funds holding", fmt(num(r.n_funds_holding))),
      kpi("Adding", fmt(num(r.n_funds_adding))),
      kpi("New", fmt(num(r.n_funds_new_position))),
      kpi("Trimming", fmt(num(r.n_funds_trimming))),
      kpi("Exiting", fmt(num(r.n_funds_exiting)))
    ].join("") + "</div>" +
      blk("13F — " + esc(r.name || r.ticker || ""), table([
        ["Bought $", fmtBig(r.bought_usd)],
        ["Sold $", fmtBig(r.sold_usd)],
        ["Reported value", fmtBig(r.total_value)]
      ]));
  }
  function cadence(s) {
    return "<span class=cadence>" + esc(s) + "</span>";
  }
  function renderInstVol(d, pack) {
    var p = d && d.instvol || pack && pack.instvol || {};
    var ats = p.ats || {};
    var inst = p.inst || {};
    var daily = p.daily || {};
    var meta = p.meta || {};
    var conf = p.conf || {};
    var tape = (pack && pack.tapeVol) || {};
    var note = "<div class=note>Three clocks. Polygon tape is delayed prints. FINRA ATS is weekly, ~" +
      esc(String(meta.ats_lag || 21)) + "d behind (week of " + esc(meta.ats_week || "—") +
      "). 13F is quarterly lagged (" + esc(meta.f13_quarter || "—") +
      "). Never average them into one “institutional volume.” Off-exchange ≠ dark pool — most of it is retail wholesalers.</div>";
    var html = note;
    html += "<div class=kpi>" + [
      kpi("Confluence", esc(conf.verdict || "—")),
      kpi("ATS state", esc(ats.state || "—")),
      kpi("ATS % of tape", num(ats.dark_pool_pct) != null ? num(ats.dark_pool_pct).toFixed(1) + "%" : "—"),
      kpi("Off-ex %", num(ats.offex_pct) != null ? num(ats.offex_pct).toFixed(1) + "%" : "—"),
      kpi("13F net $", inst.bought_usd != null ? fmtBig((num(inst.bought_usd) || 0) - (num(inst.sold_usd) || 0)) : "—"),
      kpi("Fingerprint", esc(ats.venue_fingerprint || (p.ping ? "RETAIL_PING" : "—")))
    ].join("") + "</div>";
    if (conf.why) html += "<div class=note>" + esc(conf.why) + "</div>";
    var rvol = tape.rvol;
    var vs = p.vs_spy || {};
    html += blk("1 · Tape · Polygon / Massive " + cadence("DELAYED · this timeframe"), table([
      ["This bar volume", tape.last != null ? fmtBig(tape.last) : "—"],
      ["RVOL vs 20-bar median", rvol != null ? rvol.toFixed(2) + "x" : "—"],
      ["SPY daily RVOL", vs.spy_rvol != null ? vs.spy_rvol.toFixed(2) + "x" : "—"],
      ["Name RVOL / SPY RVOL", vs.rel != null ? vs.rel.toFixed(2) + "x · " + esc(vs.read || "") : "—"],
      ["Dollar volume this bar", tape.dollar != null ? fmtBig(tape.dollar) : "—"],
      ["Close vs VWAP", tape.vs_vwap_pct != null ? tape.vs_vwap_pct.toFixed(2) + "% · VWAP " + fmt(tape.vwap) : "—"],
      ["VWAP source", esc(tape.vwap_src || (tape.vwap != null ? "bar vw if Polygon daily" : "no vw on these bars"))],
      ["Up-bar share (20)", tape.up_share != null ? (tape.up_share * 100).toFixed(0) + "% of volume on up bars" : "—"],
      ["Float turnover this bar", tape.float_turn_pct != null ? tape.float_turn_pct.toFixed(3) + "%" : "—"],
      ["Effort vs result", esc(tape.absorb || "—")],
      ["Polygon weekly consolidated (ATS week)", p.tape_week != null ? fmtBig(p.tape_week) : "—"],
      ["Note", "Daily RVOL vs SPY uses SPY daily bars, not this timeframe. " + (tape.label || "")]
    ]));
    var sess = p.session || {};
    html += blk("1b · Session split · NY clock " + cadence(sess.status === "OK" ? "THIS SESSION" : "NEEDS MINUTE BARS"), table([
      ["Session day", esc(sess.day || "—")],
      ["Pre-market volume", sess.pre != null ? fmtBig(sess.pre) + " (" + (num(sess.pre_pct) != null ? sess.pre_pct.toFixed(1) + "%" : "") + ")" : "—"],
      ["RTH volume", sess.rth != null ? fmtBig(sess.rth) + " (" + (num(sess.rth_pct) != null ? sess.rth_pct.toFixed(1) + "%" : "") + ")" : "—"],
      ["Post-close volume", sess.post != null ? fmtBig(sess.post) + " (" + (num(sess.post_pct) != null ? sess.post_pct.toFixed(1) + "%" : "") + ")" : "—"],
      ["Read", esc(sess.note || "Polygon Stocks does not publish a named pre/post feed; this is the minute tape bucketed on the NY clock.")]
    ]));
    html += blk("2 · Dark pool · FINRA ATS " + cadence("WEEKLY · lagged " + (meta.ats_age_days != null ? meta.ats_age_days + "d" : "")), table([
      ["Week of", esc(meta.ats_week || "—")],
      ["ATS shares / week", fmtBig(ats.ats_shares_wk)],
      ["Off-exchange shares / week", fmtBig(ats.offex_shares_wk)],
      ["Polygon total volume / week", fmtBig(ats.total_vol_wk || p.tape_week)],
      ["ATS % of Polygon week", num(ats.dark_pool_pct) != null ? num(ats.dark_pool_pct).toFixed(2) + "%" : (num(p.dark_share) != null ? (num(p.dark_share) * 100).toFixed(2) + "%" : "—")],
      ["Off-ex % of Polygon week", num(ats.offex_pct) != null ? num(ats.offex_pct).toFixed(2) + "%" : "—"],
      ["ATS accel vs prior 4w", num(ats.dark_accel) != null ? (num(ats.dark_accel) * 100).toFixed(1) + "%" : "—"],
      ["Avg ATS trade (shares)", fmt(num(ats.ats_avg_trade_size))],
      ["Week return (Polygon)", num(ats.week_return_pct) != null ? num(ats.week_return_pct).toFixed(2) + "%" : "—"],
      ["State", esc(ats.state || "not in 930-name scored book")],
      ["Source", esc(meta.ats_src || "FINRA weeklySummary + Polygon grouped daily")]
    ]));
    html += blk("3 · Daily off-exchange (TRF) · FINRA short file " + cadence("EOD · " + (meta.daily_date || "")), table([
      ["TRF total volume", fmtBig(daily.total_volume)],
      ["Short volume (TRF)", fmtBig(daily.short_volume)],
      ["Short volume %", num(daily.svr_pct) != null ? num(daily.svr_pct).toFixed(1) + "%" : "—"],
      ["20d avg TRF volume", fmtBig(daily.avg_total_volume_20d)],
      ["Short z", fmt(num(daily.z_score), 2)],
      ["Days to cover", fmt(num(daily.days_to_cover), 2)],
      ["Read", "TRF = off-exchange (ATS + wholesalers). Not ATS-only. High SVR is normal for internalized retail."]
    ]));
    html += blk("4 · Confirmation · 13F " + cadence("QUARTERLY · " + (meta.f13_quarter || "lagged")), table([
      ["Name", esc(inst.name || "—")],
      ["Funds holding", fmt(num(inst.n_funds_holding))],
      ["Adding", fmt(num(inst.n_funds_adding))],
      ["New positions", fmt(num(inst.n_funds_new_position))],
      ["Trimming", fmt(num(inst.n_funds_trimming))],
      ["Exiting", fmt(num(inst.n_funds_exiting))],
      ["Bought $", fmtBig(inst.bought_usd)],
      ["Sold $", fmtBig(inst.sold_usd)],
      ["Net $", inst.bought_usd != null ? fmtBig((num(inst.bought_usd) || 0) - (num(inst.sold_usd) || 0)) : "—"],
      ["Reported value", fmtBig(inst.total_value)],
      ["Read", "Holdings change, not prints. A fund can buy on the tape for weeks before this quarter files."]
    ]));
    var dix = p.dix || {};
    html += blk("5 · Market dark index · not this name " + cadence("EOD · SPX"), table([
      ["SqueezeMetrics DIX", num(dix.pct) != null ? num(dix.pct).toFixed(1) + "%" : "—"],
      ["DIX regime", esc(dix.regime || "—")],
      ["GEX ($B)", fmt(num(dix.gex_b), 2)],
      ["GEX regime", esc(dix.gex_regime || "—")],
      ["Combined", esc(dix.combined || "—")],
      ["Own TRF DIX-style (501 names)", num(dix.own_pct) != null ? num(dix.own_pct).toFixed(1) + "% · " + esc(dix.own_read || "") : "—"],
      ["Read", "Market-level dark *buying* share (SqueezeMetrics). Do not paste this onto AAPL. Own proxy = $vol-weighted (1−short%) on FINRA TRF, not the same series."]
    ]));
    var ven = p.venue || {};
    html += blk("6 · Who internalized it · FINRA monthly firms " + cadence("MONTHLY · " + (p.venue_month || "lagged")), table([
      ["Top reporting firm", esc(ven.top_firm || "—")],
      ["That firm's share of off-ex", num(ven.top_pct) != null ? num(ven.top_pct).toFixed(1) + "%" : "—"],
      ["# firms reporting", fmt(num(ven.n_firms))],
      ["Off-ex shares (month)", fmtBig(ven.sh)],
      ["Read", "Wholesaler concentration, not a named institution. Citadel/Goldman here usually means internalization, not Berkshire buying."]
    ]));
    var liq = p.liq || {};
    html += blk("7 · Liquidity capacity " + cadence("EOD · volume only"), table([
      ["ADV $", liq.adv_usd_str || fmtBig(liq.adv_usd)],
      ["Liquidity score", fmt(num(liq.liquidity_score), 1)],
      ["Consistency", num(liq.consistency) != null ? (num(liq.consistency) * 100).toFixed(0) + "%" : "—"],
      ["ATR %", fmt(num(liq.atr_percent), 2)],
      ["Scope", esc(liq.liquidity_score_scope || "80% ADV / 20% consistency — no spread or book depth")]
    ]));
    var sh = p.shares || {};
    html += blk("8 · Corporate supply · not tape volume " + cadence("FILING / TTM"), table([
      ["Shares out", fmtBig(sh.shares_outstanding)],
      ["Share count YoY", num(sh.sh_yoy_pct) != null ? num(sh.sh_yoy_pct).toFixed(2) + "%" : "—"],
      ["Buybacks TTM $", fmtBig(sh.buyback_ttm_usd)],
      ["Issuance TTM $", fmtBig(sh.issuance_ttm_usd)],
      ["Buyback yield", num(sh.buyback_yield_pct) != null ? num(sh.buyback_yield_pct).toFixed(2) + "%" : "—"],
      ["Read", "Issuer shrinking or growing the float. This is supply, not institutional buying on the tape."]
    ]));
    var opt = p.opt || {};
    html += blk("9 · Options overlay " + cadence("EOD · not stock volume"), table([
      ["Posture", esc(opt.posture || "—")],
      ["Score", fmt(num(opt.score), 2)],
      ["Engines", fmt(num(opt.n_engines))],
      ["Tags", esc((opt.tags && opt.tags.join(", ")) || "—")],
      ["Read", "Options prints can lead stock. Unusual options ≠ stock block. Open the Options tab for OI/IV."]
    ]));
    var look = p.look || {};
    html += blk("10 · ETF look-through $ " + cadence("DERIVED · creations × weight"), table([
      ["Net 5d $", fmtBig(look.net_flow_5d_usd)],
      ["ETF ownership %", num(look.etf_ownership_pct) != null ? num(look.etf_ownership_pct).toFixed(2) + "%" : "—"],
      ["# ETFs in map", fmt(num(look.n_etfs))],
      ["Flow type", esc(look.flow_type || "—")],
      ["Confirmed", look.confirmed ? "yes" : (look.confirmed === false ? "no" : "—")],
      ["Read", "Fund creations × holdings weight. Not a print in AAPL. Same number as the Pressure tab."]
    ]));
    var si = p.si || {};
    html += blk("11 · Short interest " + cadence("SETTLEMENT · " + (p.si_asof || "lagged")), table([
      ["Short interest (shares)", fmtBig(si.short_interest)],
      ["Short % of float", num(si.short_float_pct) != null ? num(si.short_float_pct).toFixed(2) + "%" : "—"],
      ["Days to cover", fmt(num(si.days_to_cover), 2)],
      ["SI change %", num(si.si_change_pct) != null ? num(si.si_change_pct).toFixed(1) + "%" : "—"],
      ["Daily short volume % (TRF)", num(si.daily_short_volume_pct) != null ? num(si.daily_short_volume_pct).toFixed(1) + "%" : "—"],
      ["Signal", esc(si.signal || "—")],
      ["SI source", esc(si.short_interest_source || "—")],
      ["Float source", esc(si.short_float_source || "—")],
      ["Read", "SI is a position, not today's volume. Daily short-volume % is TRF (often retail). Do not mix them."]
    ]));
    var cry = p.crypto || {};
    if (cry.show) {
      var mine = cry.etf_mine || {};
      html += blk("12 · Crypto on-chain + spot ETF " + cadence("EOD · " + (cry.netflow_asof || "")), table([
        ["Asset", esc(cry.side)],
        ["Exchange netflow (BTC/ETH)", cry.netflow != null ? fmt(num(cry.netflow), 1) + " · z " + fmt(num(cry.netflow_z), 2) : "—"],
        ["Exchange inflow / outflow", (cry.inflow != null ? fmt(num(cry.inflow), 1) : "—") + " / " + (cry.outflow != null ? fmt(num(cry.outflow), 1) : "—")],
        ["Whale ratio", fmt(num(cry.whale), 3)],
        ["Spot ETF flow today $", fmtBig(cry.etf_today)],
        ["Spot ETF 5d / 30d $", fmtBig(cry.etf_5d) + " / " + fmtBig(cry.etf_30d)],
        ["ETF regime", esc(cry.etf_regime || "—")],
        ["This ticker's ETF print", mine.etf ? (esc(mine.etf) + " " + fmtBig(mine.flow_usd)) : "aggregate only"],
        ["ETF as-of", esc(cry.etf_date || "")],
        ["Read", "Netflow onto exchanges = potential sell-side supply. ETF creations are fund flow, not on-chain. " + esc(cry.interp || "")]
      ]));
    } else {
      html += blk("12 · Crypto on-chain + spot ETF " + cadence("N/A"), "<div class=note>Shown for BTC/ETH and the spot wrappers (IBIT, FBTC, BITB, ARKB, ETHA, …). AAPL has no CryptoQuant series.</div>");
    }
    var cot = p.cot || {};
    var cr = cot.row || {};
    html += blk("13 · CFTC COT " + cadence("WEEKLY · futures"), table([
      ["Contract", esc(cot.key || "—")],
      ["Mapped from", cot.mapped ? esc(p.ticker + " → " + cot.key) : esc(p.ticker || "")],
      ["Smart-money side", esc(cr.smart_money_side || "—")],
      ["Direction", esc(cr.direction || "—")],
      ["Net speculator", fmtBig(cr.net_speculator)],
      ["Net commercial", fmtBig(cr.net_commercial)],
      ["Spec WoW", fmtBig(cr.net_spec_wow)],
      ["As of", esc(cr.as_of || "")],
      ["Read", esc(cot.note || "Positioning, not volume. ES overlay on a single-stock chart is market context only.")]
    ]));
    if (!ats.state && !inst.ticker && !daily.symbol) {
      html += "<div class=empty>No ATS / 13F / TRF row for this symbol. Mega-caps are in the 930-name ATS book and 13F compact; many micro names will not score.</div>";
    }
    return html;
  }
  async function revisionRow(t) {
    await loadJsonOnce(REV_DOC || (REV_DOC = {}), "/data/estimate-revisions.json");
    var tk = jhFundTicker(t);
    var row = pickIdx(REV_DOC, t);
    var dir = ((REV_DOC.v || {}).direction_map || {})[tk];
    if (!row && dir) row = { ticker: tk, direction: dir, buckets: ["direction_map"] };
    return { row: row, doc: REV_DOC.v || {}, as_of: (REV_DOC.v || {}).generated_at };
  }
  async function qualityRow(t) {
    await loadJsonOnce(QUAL_DOC || (QUAL_DOC = {}), "/data/earnings-quality.json");
    await loadJsonOnce(BENEISH_DOC || (BENEISH_DOC = {}), "/data/beneish.json");
    var row = pickIdx(QUAL_DOC, t);
    var ben = pickIdx(BENEISH_DOC, t);
    if (row && ben) row = Object.assign({}, row, { m_score: ben.m_score, beneish_verdict: ben.verdict });
    else if (!row && ben) row = { ticker: jhFundTicker(t), m_score: ben.m_score, beneish_verdict: ben.verdict, buckets: ["beneish"] };
    return { row: row, doc: QUAL_DOC.v || {}, as_of: (QUAL_DOC.v || {}).as_of || (QUAL_DOC.v || {}).generated_at, beneish: ben };
  }

  function renderPead(d) {
    var p = d && d.pead || {};
    var r = p.row;
    var note = "<div class=note>Post-earnings drift vs the print. Graded vs the move after the report, not a live surprise. Board: <a href='/earnings-pead.html'>earnings-pead</a>.</div>";
    if (!r) return note + "<div class=empty>No PEAD row for this symbol in today's qualifying list.</div>";
    return note + "<div class=kpi>" + [
      kpi("PEAD score", fmt(num(r.score))),
      kpi("Tier", esc(r.tier || "—")),
      kpi("Beat streak", fmt(num(r.beat_streak))),
      kpi("Flags", esc((r.flags && (Array.isArray(r.flags) ? r.flags.join(", ") : String(r.flags))) || "—"))
    ].join("") + "</div>";
  }
  function renderPriced(d) {
    var p = d && d.priced || {};
    var r = p.row;
    var note = "<div class=note>How much future growth the multiple already requires (GuruFocus multi-lens). Not a DCF guarantee. Implied growth, not a forecast.</div>";
    if (!r) return note + "<div class=empty>No gf-value row for this symbol (universe is the scored tape, not every listing).</div>";
    return note + "<div class=kpi>" + [
      kpi("GF value", fmt(num(r.gf_value))),
      kpi("MOS %", fmt(num(r.margin_of_safety_pct))),
      kpi("Rating", esc(r.rating || "—")),
      kpi("Lenses", fmt(num(r.n_lenses))),
      kpi("DCF", fmt(num(r.dcf_fair_value))),
      kpi("Graham", fmt(num(r.graham_number)))
    ].join("") + "</div>";
  }
  function renderBoom(d) {
    var p = d && d.boom || {};
    var r = p.row;
    var note = "<div class=note>Independent engines agreeing on the same name. Display only — not a trading signal. Dimensions are BEAT / ANALYST / ESTIMATE (same-fiscal) / FLOW / SQUEEZE / BREAKOUT.</div>";
    if (!r) return note + "<div class=empty>Not on today's boom-radar (needs 2+ independent dims).</div>";
    return note + "<div class=kpi>" + [
      kpi("Boom score", fmt(num(r.boom_score != null ? r.boom_score : r.score))),
      kpi("Convergence", fmt(num(r.convergence))),
      kpi("Dims", esc((r.dimensions && (Array.isArray(r.dimensions) ? r.dimensions.join(" · ") : String(r.dimensions))) || "—"))
    ].join("") + "</div>" +
      blk("Reasons", table((r.reasons || []).slice(0, 8).map(function (x) { return ["", esc(String(x))]; })));
  }
  function renderSqueeze(d) {
    var p = d && d.sqz || {};
    var r = p.row;
    var doc = p.doc || {};
    var note = "<div class=note>Crowding / borrow / short pressure. Estimate of exit difficulty, not a squeeze clock. State: " + esc(doc.state || "—") + "</div>";
    if (!r) return note + "<div class=empty>No squeeze-pretrigger row for this symbol today.</div>";
    return note + "<div class=kpi>" + [
      kpi("Score", fmt(num(r.score))),
      kpi("Ticker", esc(r.ticker || "—")),
      kpi("Posture", esc(r.posture || r.tag || "—"))
    ].join("") + "</div>";
  }

  function renderPressure(d) {
    var p = d && d.pressure || {};
    var r = p.row;
    var doc = p.doc || {};
    var note = "<div class=note>Estimate, not a print. Fund creations = fact. Name-level $ = ETF flow × holdings weight (F08). Not institutional volume. " +
      esc(doc.tier_note || doc.evidence_tier || "") + "</div>";
    if (!r) {
      return note + "<div class=empty>No look-through row for this symbol in today's published lists (engine scored " +
        esc(String(doc.n_names || "—")) + " names across " + esc(String(doc.n_etfs_used || "—")) +
        " ETFs). Leaders live on <a href='/flow-lookthrough.html'>flow-lookthrough</a>.</div>";
    }
    if (r.net_flow_5d_usd == null && (r.posture || r.engines)) {
      return note + "<div class=kpi>" + [
        kpi("Confluence", esc(r.posture || "—")),
        kpi("Engines", esc((r.engines && (Array.isArray(r.engines) ? r.engines.join(" · ") : String(r.engines))) || "—")),
        kpi("Score", fmt(num(r.score)))
      ].join("") + "</div><div class=note>From flow-confluence ticker_map (3,821 names). Look-through dollars publish on the next producer run (by_ticker slice).</div>";
    }
    var drv = (r.drivers || []).slice(0, 8).map(function (x) {
      return [esc(x.etf || x.ticker || ""), fmtBig(x.contrib_5d_usd != null ? x.contrib_5d_usd : x.flow),
              x.weight != null ? (Number(x.weight) * 100).toFixed(2) + "%" : "—"];
    });
    return note + "<div class=kpi>" + [
      kpi("5D attrib. $", fmtBig(r.net_flow_5d_usd)),
      kpi("Daily attrib. $", fmtBig(r.net_flow_daily_usd)),
      kpi("Type", esc((r.flow_type || (r.buckets && r.buckets[0]) || "—").replace(/_/g, " "))),
      kpi("Confirmed", r.confirmed ? "flow × wt AND share Δ" : "—"),
      kpi("ETF own %", r.etf_ownership_pct == null ? "—" : Number(r.etf_ownership_pct).toFixed(2) + "%"),
      kpi("Lists", esc((r.buckets || []).join(", ") || "—"))
    ].join("") + "</div>" +
      blk("Pressure — ETF look-through · " + String(p.as_of || "").slice(0, 10), table([
        ["Ticker", esc(r.ticker)],
        ["Industry", esc(r.industry || "—")],
        ["# ETFs in print", fmt(num(r.n_etfs))],
        ["Broad 5D $", fmtBig(r.broad_flow_5d_usd)],
        ["Thematic 5D $", fmtBig(r.thematic_flow_5d_usd)],
        ["Share Δ $", fmtBig(r.shares_delta_usd)],
        ["Flow bps mcap", fmt(num(r.flow_bps_mcap))]
      ])) +
      (drv.length ? blk("Driver ETFs (inferred)", "<table><thead><tr><th>ETF</th><th>5D $</th><th>Wgt</th></tr></thead><tbody>" +
        drv.map(function (a) { return "<tr><td>" + a[0] + "</td><td>" + a[1] + "</td><td>" + a[2] + "</td></tr>"; }).join("") +
        "</tbody></table>") : "");
  }
  function renderRevisions(d) {
    var p = d && d.revx || {};
    var r = p.row;
    var doc = p.doc || {};
    var split = "<div class=note>Two different numbers. <b>eps_rev_pct</b> = same-fiscal estimate vs our prior snapshot (a revision). " +
      "<b>fwd_eps_growth_pct</b> = FY2 vs FY1 consensus slope (projected growth, NOT a revision). " +
      esc((doc.caveats && doc.caveats[1]) || "") + "</div>";
    if (!r) {
      return split + "<div class=empty>No revision row in today's published leaders. The harvest is a leader list, not the full tape. " +
        "See <a href='/estimate-revisions.html'>estimate-revisions</a>. n_with_history=" + esc(String(doc.n_with_history || "—")) + ".</div>";
    }
    return split + "<div class=kpi>" + [
      kpi("Same-FY rev %", r.eps_rev_pct == null ? "—" : fmt(num(r.eps_rev_pct)) + "%"),
      kpi("FY2 vs FY1 %", r.fwd_eps_growth_pct == null ? "—" : fmt(num(r.fwd_eps_growth_pct)) + "%"),
      kpi("Direction", esc(r.direction || "—")),
      kpi("n snapshots", fmt(num(r.n_obs))),
      kpi("Analysts", fmt(num(r.n_analysts))),
      kpi("Strength", fmt(num(r.estimate_strength)))
    ].join("") + "</div>" +
      blk("Revisions — " + String(p.as_of || "").slice(0, 10), table([
        ["Fiscal", esc(String(r.fiscal_period || "") + " " + String(r.fiscal_year || ""))],
        ["Current EPS est", fmt(num(r.current_eps_est))],
        ["Baseline EPS est", fmt(num(r.baseline_eps_est))],
        ["Baseline date", esc(r.baseline_date || "—")],
        ["Source", esc(r.consensus_source || "—")],
        ["Earnings", esc(r.earnings_date || "—")],
        ["Revenue confirms", r.revenue_confirms ? "yes" : "no"]
      ]));
  }
  function renderQuality(d) {
    var p = d && d.qual || {};
    var r = p.row;
    var doc = p.doc || {};
    var note = "<div class=note>Cash conversion and accruals from statements (FMP). Dilution in share_chg when present. Not a price forecast.</div>";
    if (!r) {
      return note + "<div class=empty>No earnings-quality row for this symbol (universe " +
        esc(String(doc.universe_size || (doc.all_ranked && doc.all_ranked.length) || "—")) +
        "). Board: <a href='/quality-on-sale.html'>quality</a>.</div>";
    }
    return note + "<div class=kpi>" + [
      kpi("Cash conv.", fmt(num(r.cash_conversion_ratio))),
      kpi("Sloan % assets", fmt(num(r.sloan_accruals_pct_assets))),
      kpi("TTM FCF", fmtBig(r.ttm_fcf_usd)),
      kpi("TTM OCF", fmtBig(r.ttm_ocf_usd)),
      kpi("TTM NI", fmtBig(r.ttm_ni_usd)),
      kpi("Beneish DSRI", fmt(num(r.dsri_beneish)))
    ].join("") + "</div>" +
      blk("Cash-flow quality — " + String(p.as_of || "").slice(0, 10), table([
        ["Name", esc(r.name || r.ticker)],
        ["GMI Beneish", fmt(num(r.gmi_beneish))],
        ["P/E", fmt(num(r.pe))]
      ]));
  }

  function paintBody(pack) {
    var body = document.getElementById("dtbody");
    if (!body) return;
    var d = pack.data || {};
    d.fmp = pack.fmp || d.fmp;
    d.ident = pack.ident || d.ident;
    d.chain = pack.chain || d.chain;
    d.pressure = pack.pressure || d.pressure;
    d.revx = pack.revx || d.revx;
    d.qual = pack.qual || d.qual;
    d.pead = pack.pead || d.pead;
    d.priced = pack.priced || d.priced;
    d.boom = pack.boom || d.boom;
    d.sqz = pack.sqz || d.sqz;
    d.confluence = pack.confluence || d.confluence;
    d.inst13f = pack.inst13f || d.inst13f;
    d.instvol = pack.instvol || d.instvol;
    var bars = pack.bars || [];
    var q = pack.quote || {};
    var html = "";
    if (tab === "over") html = renderOver(d, bars, q, pack) + renderRelated(pack);
    else if (tab === "stats") html = renderStats(d, bars);
    else if (tab === "val") html = renderVal(d);
    else if (tab === "fin") html = renderFin(d);
    else if (tab === "ident") html = renderIdent(d);
    else if (tab === "chain") html = renderChain(d);
    else if (tab === "press") html = renderPressure(d);
    else if (tab === "revx") html = renderRevisions(d);
    else if (tab === "qual") html = renderQuality(d);
    else if (tab === "pead") html = renderPead(d);
    else if (tab === "priced") html = renderPriced(d);
    else if (tab === "boom") html = renderBoom(d);
    else if (tab === "sqz") html = renderSqueeze(d);
    else if (tab === "ivol") html = renderInstVol(d, pack);
    else if (tab === "inst") html = renderInst13f(d);
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
    bindHoldFilter();
  }

  function bindHoldFilter() {
    var q = document.getElementById("etf-hold-q");
    var tbl = document.getElementById("etf-hold-tbl");
    if (!q || !tbl) return;
    var lab = document.getElementById("etf-hold-n");
    var rows = tbl.querySelectorAll("tbody tr");
    q.oninput = function () {
      var s = String(q.value || "").trim().toUpperCase();
      var shown = 0;
      for (var i = 0; i < rows.length; i++) {
        var hit = !s || (rows[i].getAttribute("data-q") || "").indexOf(s) >= 0;
        rows[i].style.display = hit ? "" : "none";
        if (hit) shown++;
      }
      if (lab) lab.textContent = shown + " / " + rows.length;
    };
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

  // One normaliser for every fundamentals lookup: "NASDAQ:AAPL" / "US__AAPL" / "US_AAPL" -> "AAPL" (harvest + master + CQ keys)
  function jhFundTicker(s) {
    s = String(s || "").trim().toUpperCase();
    s = s.split(":").pop();
    s = s.replace(/^US__/, "").replace(/^US_/, "");
    return s;
  }
  window.jhFundTicker = jhFundTicker;
  function lookupRow(tickers, t) {
    if (!tickers) return null;
    var k = jhFundTicker(t);
    var cands = [k, k.replace(".", "-"), k.replace("-", "."), String(t || "")];
    for (var i = 0; i < cands.length; i++) { if (cands[i] && tickers[cands[i]]) return tickers[cands[i]]; }
    return null;            // never invent a row
  }
  var FMP_HARVEST = null, FMP_HTTP = null;
  async function fmpRow(t) {
    // a miss is never sticky: re-fetch whenever the cache is empty or the last fetch was not 200
    if (!FMP_HARVEST || FMP_HTTP !== 200 || !FMP_HARVEST.tickers || !Object.keys(FMP_HARVEST.tickers).length) {
      try {
        var r = await fetch("/data/fmp-ratios.json", { cache: "no-store" });
        FMP_HTTP = r.status;
        FMP_HARVEST = r.ok ? await r.json() : null;
      } catch (e) { FMP_HTTP = "network"; FMP_HARVEST = null; }
    }
    if (!FMP_HARVEST) return { http: FMP_HTTP, row: null };
    var row = lookupRow(FMP_HARVEST.tickers, t);
    return row && !row.error ? { http: 200, row: row, as_of: FMP_HARVEST.generated_at, key_status: (FMP_HARVEST.key_status || {}).status }
                             : { http: 200, row: null, as_of: FMP_HARVEST.generated_at };
  }
  async function loadPack(sym) {
    var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
    var t = jhFundTicker(sym);
    var pack = { sym: sym, src: [], data: {}, bars: window.lastBars || [], quote: {} };
    if (window.lastBars && window.jhActive === sym) pack.bars = window.lastBars;
    function take(j, label) {
      if (!j || typeof j !== "object") return;
      pack.data = Object.assign(pack.data, j);
      if (j.price || j.summaryDetail || j.defaultKeyStatistics || j.financialData) pack.src.push(label);
    }
    try {
      pack.fmp = await fmpRow(t);
      pack.ident = await symbologyRow(t);
      pack.chain = await cqPack(t);
      pack.pressure = await pressureRow(t);
      pack.revx = await revisionRow(t);
      pack.qual = await qualityRow(t);
      pack.pead = await peadRow(t);
      pack.priced = await pricedRow(t);
      pack.boom = await boomRow(t);
      pack.sqz = await squeezeRow(t);
      pack.confluence = await confluenceRow(t);
      if (pack.confluence && pack.confluence.row && (!pack.pressure || !pack.pressure.row)) {
        pack.pressure = pack.pressure || {};
        pack.pressure.row = pack.pressure.row || pack.confluence.row;
        pack.pressure.doc = pack.pressure.doc || {};
      }
      if (pack.ident) pack.src.push("OpenFIGI symbology master");
      if (pack.chain && pack.chain.proxy) pack.src.push("CryptoQuant EOD on-chain");
      if (pack.fmp && pack.fmp.row) pack.src.push("FMP EOD/TTM (" + String(pack.fmp.as_of || "").slice(0, 10) + ")");
      if (pack.pressure && pack.pressure.row) pack.src.push("ETF look-through (inferred)");
      if (pack.revx && pack.revx.row) pack.src.push("estimate-revisions");
      if (pack.qual && pack.qual.row) pack.src.push("earnings-quality");
      if (pack.pead && pack.pead.row) pack.src.push("earnings-pead");
      if (pack.priced && pack.priced.row) pack.src.push("gf-value");
      if (pack.boom && pack.boom.row) pack.src.push("boom-radar");
      if (pack.confluence && pack.confluence.row) pack.src.push("flow-confluence");
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
        var F = window.JHEtfFuse;
        var row = await F.of(t);
        if (row) pack.etfRow = F.mergeLive(row, pack.polyEtf);
        var isFund = F.isFund(pack.etfRow, pack.polyEtf);
        if (F.ofDerived) pack.derived = await F.ofDerived(t);
        if (F.derived) {
          pack.derivedDesk = await F.derived();
          pack.risk = (pack.derivedDesk && pack.derivedDesk.verdicts) || {};
        }
        if (isFund) {
          if (F.census) {
            pack.etfCensus = await F.census();
            pack.etfRank = F.rankVs(t, pack.etfCensus);
            if (pack.etfRank && pack.etfRank.in_universe) pack.src.push("ETF census vs SPY (" + String(pack.etfRank.as_of || "").slice(0, 10) + ")");
          }
          pack.src.push("ETF desk warehouse");
        }
      } catch (e7) {}
    }
    hydrateHeavy(sym, pack);
    return pack;
  }

  function hydrateHeavy(sym, pack) {
    if (!pack || pack._hydrating || pack._hydrated) return;
    pack._hydrating = true;
    var t = jhFundTicker(sym);
    var F = window.JHEtfFuse;
    var jobs = [];
    jobs.push(inst13fRow(t).then(function (r) {
      pack.inst13f = r;
      if (r && r.row && pack.src.indexOf("13F quarterly (lagged)") < 0) pack.src.push("13F quarterly (lagged)");
    }).catch(function () {}));
    if (window.JHInstVol && window.JHInstVol.of) {
      jobs.push(window.JHInstVol.of(t).then(function (r) {
        pack.instvol = r;
        var sh = r && r.shares && r.shares.shares_outstanding;
        pack.tapeVol = window.JHInstVol.tapeFromBars(pack.bars || window.lastBars || [], sh);
        if (!window.JHInstVol.enrich) return r;
        return window.JHInstVol.enrich(r, { bars: pack.bars || window.lastBars || [], snapshot: pack.snapshot }).then(function (en) {
          pack.instvol = en;
          pack.tapeVol = en.tape || pack.tapeVol;
          return en;
        });
      }).then(function (r) {
        if (r && pack.src.indexOf("FINRA ATS + Polygon week") < 0) pack.src.push("FINRA ATS weekly + Polygon denominator + 13F confirmation");
      }).catch(function () {}));
    }
    jobs.push(desk13fDoc().then(function (r) { pack.desk13f = r; }).catch(function () {}));
    if (F && F.isFund(pack.etfRow, pack.polyEtf) && F.constituents) {
      jobs.push(F.constituents(t).then(function (c) {
        pack.etfConstituents = c;
        if (c && c.n) pack.src.push("ETF holdings-index (" + c.n + " names)");
      }).catch(function () {}));
    } else if (F && F.reverse) {
      jobs.push(F.reverse(t).then(function (h) { pack.etfHolders = h || []; }).catch(function () {}));
    }
    Promise.all(jobs).then(function () {
      pack._hydrating = false;
      pack._hydrated = true;
      cache[sym] = pack;
      if (window.jhActive === sym) paintBody(pack);
    }).catch(function () { pack._hydrating = false; });
  }

  async function loadKind(sym, kind) {
    var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
    var t = jhFundTicker(sym);
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

  window.jhOpenDataTypePanel = function (which) {
    openPanel(which || "etf");
  };

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
      "<button data-dt=ident>Identity (OpenFIGI)</button>" +
      "<button data-dt=chain>On-chain (CryptoQuant)</button>" +
      "<button data-dt=press>Pressure (ETF look-through)</button>" +
      "<button data-dt=revx>Revisions (same fiscal)</button>" +
      "<button data-dt=qual>Cash-flow quality</button>" +
      "<button data-dt=pead>Surprise vs reaction</button>" +
      "<button data-dt=priced>Growth already priced</button>" +
      "<button data-dt=boom>Stack (boom-radar)</button>" +
      "<button data-dt=sqz>Squeeze / crowded</button>" +
      "<button data-dt=ivol>Inst vol (ATS + tape + 13F)</button>" +
      "<button data-dt=inst>13F (quarterly, lagged)</button>" +
      "<button data-dt=est>Estimates</button>" +
      "<button data-dt=div>Dividends</button>" +
      "<button data-dt=news>News</button>" +
      "<button data-dt=tech>Technicals</button>" +
      "<button data-dt=short>Short interest</button>" +
      "<button data-dt=opt>Options</button>" +
      "<button data-dt=etf>Holdings (ETF book)</button>" +
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
