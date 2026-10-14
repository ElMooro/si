#!/usr/bin/env python3
"""Refresh the Macro desk: drop dead series, add current plumbing, stop the stampede.

Unavailable was a full-history /series burst (429). Outdated cards were
discontinued FRED ids (NAPM, London gold, TED, OECD CPI) still on the board.
Fed assets were labeled millions twice. ETFs now use the quote tape.
"""
from pathlib import Path

PATH = Path("chart-pro.html")

CATALOG = r"""  static CATALOG_REV = 3;
  static CATALOG = [
    { cat: "US Yield Curve", flag: "🇺🇸", items: [
      ["DGS1MO","1M Treasury","%"], ["DGS3MO","3M Treasury","%"], ["DGS6MO","6M Treasury","%"],
      ["DGS1","1Y Treasury","%"], ["DGS2","2Y Treasury","%"], ["DGS5","5Y Treasury","%"],
      ["DGS7","7Y Treasury","%"], ["DGS10","10Y Treasury","%"], ["DGS20","20Y Treasury","%"],
      ["DGS30","30Y Treasury","%"], ["T10Y2Y","10Y-2Y Spread","%"], ["T10Y3M","10Y-3M Spread","%"],
      ["T10YFF","10Y minus Fed Funds","%"],
    ]},
    { cat: "Policy Rates", flag: "🏦", items: [
      ["DFEDTARU","Fed Funds Target Upper","%"], ["DFF","Fed Funds (daily)","%"],
      ["EFFR","Effective Fed Funds","%"], ["SOFR","SOFR","%"],
      ["SOFR30DAYAVG","SOFR 30-day Avg","%"], ["SOFR90DAYAVG","SOFR 90-day Avg","%"],
      ["IORB","Interest on Reserve Balances","%"], ["OBFR","Overnight Bank Funding","%"],
      ["DPRIME","Bank Prime Rate","%"], ["FEDFUNDS","Fed Funds (monthly)","%"],
    ]},
    { cat: "Real Yields & Inflation Expectations", flag: "🔥", items: [
      ["DFII5","5Y TIPS Real Yield","%"], ["DFII10","10Y TIPS Real Yield","%"], ["DFII30","30Y TIPS Real Yield","%"],
      ["T5YIE","5Y Breakeven","%"], ["T10YIE","10Y Breakeven","%"], ["T5YIFR","5Y5Y Forward Inflation","%"],
      ["THREEFYTP10","10Y Term Premium (ACM)","%"], ["MICH","Michigan Inflation Expectation","%"],
    ]},
    { cat: "Fed Balance Sheet & Liquidity", flag: "💧", items: [
      ["WALCL","Fed Total Assets","$M"], ["TREAST","Fed Treasuries","$M"],
      ["WSHOMCB","Fed MBS","$M"], ["WSHOSHO","Fed Securities Held Outright","$M"],
      ["WCURCIR","Currency in Circulation","$M"], ["WRESBAL","Reserve Balances","$M"],
      ["WTREGEN","Treasury General Account","$M"], ["RRPONTSYD","Overnight Reverse Repo","$B"],
      ["TOTRESNS","Total Reserves","$B"], ["BOGMBASE","Monetary Base","$B"],
      ["M2SL","M2 Money Supply","$B"], ["M1SL","M1 Money Supply","$B"], ["WM2NS","M2 (weekly)","$B"],
    ]},
    { cat: "Credit, Banks & Stress", flag: "⚠️", items: [
      ["BAMLH0A0HYM2","High-Yield OAS","%"], ["BAMLH0A1HYBB","BB High-Yield OAS","%"],
      ["BAMLH0A3HYC","CCC High-Yield OAS","%"], ["BAMLH0A0HYM2EY","HY Effective Yield","%"],
      ["BAMLC0A0CM","Investment-Grade OAS","%"], ["BAMLC0A4CBBB","BBB Corporate OAS","%"],
      ["AAA","AAA Corporate Yield","%"], ["BAA","BAA Corporate Yield","%"], ["BAA10Y","BAA minus 10Y","%"],
      ["STLFSI4","St. Louis Fed Financial Stress","Index"], ["NFCI","Chicago Fed NFCI","Index"],
      ["ANFCI","Adjusted NFCI","Index"], ["NFCIRISK","NFCI Risk","Index"],
      ["NFCILEVERAGE","NFCI Leverage","Index"], ["NFCICREDIT","NFCI Credit","Index"],
      ["DRTSCILM","Bank Lending Standards","%"], ["DRCCLACBS","Credit-Card Delinquency","%"],
      ["TOTLL","Bank Loans","$B"], ["DPSACBW027SBOG","Bank Deposits","$B"], ["TOTALSL","Consumer Credit","$M"],
    ]},
    { cat: "Inflation", flag: "📈", items: [
      ["CPIAUCSL","CPI","Index"], ["CPILFESL","Core CPI","Index"],
      ["PCEPI","PCE Price Index","Index"], ["PCEPILFE","Core PCE","Index"],
      ["PPIACO","Producer Price Index","Index"], ["GASREGW","US Regular Gasoline","$/gal"],
    ]},
    { cat: "Labor", flag: "👷", items: [
      ["UNRATE","Unemployment Rate","%"], ["U6RATE","U-6 Underemployment","%"],
      ["PAYEMS","Nonfarm Payrolls","Thousands"], ["MANEMP","Manufacturing Payrolls","Thousands"],
      ["ICSA","Initial Claims","Number"], ["IC4WSA","Claims, 4-week Avg","Number"],
      ["CCSA","Continued Claims","Number"], ["CIVPART","Participation Rate","%"],
      ["EMRATIO","Employment-Population","%"], ["JTSJOL","Job Openings","Thousands"],
      ["JTSQUR","Quits Rate","%"], ["AHETPI","Avg Hourly Earnings, Prod.","$/hr"],
      ["CES0500000003","Avg Hourly Earnings, Total","$/hr"],
    ]},
    { cat: "Growth & Activity", flag: "🏭", items: [
      ["GDPC1","Real GDP","$B"], ["A191RL1Q225SBEA","Real GDP Growth","%"],
      ["INDPRO","Industrial Production","Index"], ["IPMAN","Manufacturing Output","Index"],
      ["TCU","Capacity Utilization","%"], ["CFNAI","Chicago Fed National Activity","Index"],
      ["RSAFS","Retail Sales","$M"], ["UMCSENT","Consumer Sentiment","Index"],
      ["DGORDER","Durable Goods Orders","$M"], ["NEWORDER","Core Capital Goods Orders","$M"],
      ["BUSINV","Business Inventories","$M"], ["USEPUINDXD","Economic Policy Uncertainty","Index"],
    ]},
    { cat: "Housing", flag: "🏠", items: [
      ["HOUST","Housing Starts","Thousands"], ["PERMIT","Building Permits","Thousands"],
      ["HSN1F","New Home Sales","Thousands"], ["EXHOSLUSM495S","Existing Home Sales","Number"],
      ["MSACSR","Months' Supply of New Homes","Months"], ["MSPUS","Median New Home Price","$"],
      ["MORTGAGE30US","30Y Mortgage Rate","%"], ["MORTGAGE15US","15Y Mortgage Rate","%"],
      ["CSUSHPISA","Case-Shiller Home Price","Index"], ["CSUSHPINSA","Case-Shiller (NSA)","Index"],
    ]},
    { cat: "Dollar & FX", flag: "💱", items: [
      ["DTWEXBGS","Broad Dollar","Index"], ["DTWEXAFEGS","Advanced-Economy Dollar","Index"],
      ["DTWEXEMEGS","EM Dollar","Index"], ["DEXUSEU","USD per Euro","Rate"],
      ["DEXUSUK","USD per Pound","Rate"], ["DEXJPUS","Yen per USD","Rate"],
      ["DEXCHUS","Yuan per USD","Rate"], ["DEXCAUS","CAD per USD","Rate"],
      ["DEXMXUS","Peso per USD","Rate"], ["DEXBZUS","Real per USD","Rate"],
      ["DEXKOUS","Won per USD","Rate"], ["DEXSZUS","Franc per USD","Rate"],
    ]},
    { cat: "Europe & Japan", flag: "🇪🇺", items: [
      ["ECBDFR","ECB Deposit Rate","%"], ["ECBMRRFR","ECB Main Refi Rate","%"],
      ["IRLTLT01DEM156N","Germany 10Y","%"], ["IRLTLT01FRM156N","France 10Y","%"],
      ["IRLTLT01ITM156N","Italy 10Y","%"], ["IRLTLT01ESM156N","Spain 10Y","%"],
      ["IRLTLT01GBM156N","UK 10Y","%"], ["IRLTLT01JPM156N","Japan 10Y","%"],
      ["CP0000EZ19M086NEST","Euro Area HICP","Index"], ["LRHUTTTTJPM156S","Japan Unemployment","%"],
      ["CLVMNACSCAB1GQEA19","Euro Area Real GDP","€M"],
    ]},
    { cat: "US Fiscal", flag: "🏛", items: [
      ["GFDEBTN","Federal Debt","$M"], ["GFDEGDQ188S","Debt / GDP","%"],
      ["MTSDS133FMS","Monthly Federal Deficit","$M"], ["FGRECPT","Federal Receipts","$B"],
      ["FGEXPND","Federal Expenditures","$B"],
    ]},
    { cat: "Commodities & Crypto", flag: "🛢️", items: [
      ["DCOILWTICO","WTI Crude","$/bbl"], ["DCOILBRENTEU","Brent Crude","$/bbl"],
      ["DHHNGSP","Henry Hub Gas","$/mmBtu"], ["PCOPPUSDM","Copper","$/mt"],
      ["PWHEAMTUSDM","Wheat","$/mt"], ["PMAIZMTUSDM","Corn","$/mt"],
      ["WPU0911","Lumber PPI","Index"], ["CBBTCUSD","Bitcoin","$"], ["CBETHUSD","Ether","$"],
    ]},
    { cat: "Markets", flag: "📊", items: [
      ["SP500","S&P 500","Index"], ["NASDAQCOM","Nasdaq Composite","Index"], ["VIXCLS","VIX","Index"],
    ]},
    { cat: "Metals & Energy Tape", flag: "🪙", etf: true, items: [
      ["GLD","Gold"], ["SLV","Silver"], ["PPLT","Platinum"], ["CPER","Copper"],
      ["USO","WTI Oil"], ["UNG","Natural Gas"],
    ]},
    { cat: "Global Equity Tape", flag: "🌍", etf: true, items: [
      ["EEM","Emerging Markets"], ["VWO","Emerging Markets Vanguard"], ["FM","Frontier"],
      ["FXI","China Large Cap"], ["MCHI","China"], ["ASHR","China A-shares"],
      ["EWZ","Brazil"], ["INDA","India"], ["EWW","Mexico"], ["EZA","South Africa"],
      ["TUR","Turkey"], ["EWY","Korea"], ["EWT","Taiwan"], ["EIDO","Indonesia"],
      ["VNM","Vietnam"], ["EPHE","Philippines"], ["EWJ","Japan"], ["EWG","Germany"],
      ["EWU","United Kingdom"], ["EWQ","France"], ["EWL","Switzerland"], ["EWI","Italy"],
      ["EWP","Spain"], ["EWC","Canada"], ["EWA","Australia"], ["ILF","Latin America"],
    ]},
  ];"""

LOAD_OLD = """  static load() {
    if (this.state) return this.state;
    try {
      const saved = JSON.parse(localStorage.getItem(this.STORAGE_KEY) || 'null');
      if (saved && Array.isArray(saved.categories)) this.state = saved;
    } catch (e) {}
    if (!this.state) this.state = { version: 2, categories: this.defaults() };
    this.normalize();
    return this.state;
  }"""

LOAD_NEW = """  static load() {
    if (this.state) return this.state;
    try {
      const saved = JSON.parse(localStorage.getItem(this.STORAGE_KEY) || 'null');
      if (saved && Array.isArray(saved.categories)) this.state = saved;
    } catch (e) {}
    if (!this.state || this.state.catalogRev !== this.CATALOG_REV) {
      this.state = { version: 2, catalogRev: this.CATALOG_REV, categories: this.defaults() };
      this.normalize();
      this.save();
      return this.state;
    }
    this.normalize();
    return this.state;
  }"""

NORM_OLD = """    this.state = {
      version: 2,
      categories: cats.map((cat, ci) => ({"""

NORM_NEW = """    const rev = this.state && this.state.catalogRev;
    this.state = {
      version: 2,
      catalogRev: rev === this.CATALOG_REV ? this.CATALOG_REV : undefined,
      categories: cats.map((cat, ci) => ({"""

HYDRATE_OLD_START = "  static hydrateMetrics() {"
PAINT_START = "  static paintMetric(card, metric) {"

HYDRATE_NEW = r"""  static hydrateMetrics() {
    const token = ++this.renderToken;
    this.macroQueue = [];
    this.macroRunning = 0;
    const cards = [...document.querySelectorAll('#macro-body .macro-item[data-symbol]')];
    const enqueue = card => {
      if (!card || card.dataset.metricState) return;
      card.dataset.metricState = 'queued';
      this.macroQueue.push(card);
      this.pumpMetrics(token);
    };
    if (!('IntersectionObserver' in window)) {
      cards.slice(0, 36).forEach(enqueue);
      return;
    }
    if (this.metricObserver) this.metricObserver.disconnect();
    this.metricObserver = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        if (!entry.isIntersecting) return;
        this.metricObserver.unobserve(entry.target);
        enqueue(entry.target);
      });
    }, { root: document.getElementById('macro-body'), rootMargin: '280px 0px', threshold: 0.01 });
    cards.forEach(card => this.metricObserver.observe(card));
  }

  static pumpMetrics(token) {
    if (token !== this.renderToken) return;
    while (this.macroRunning < 3 && this.macroQueue && this.macroQueue.length) {
      const card = this.macroQueue.shift();
      this.macroRunning++;
      this.loadCard(card, token, 0).finally(() => {
        this.macroRunning--;
        this.pumpMetrics(token);
      });
    }
  }

  static async loadCard(card, token, attempt) {
    card.dataset.metricState = 'loading';
    try {
      const metric = await this.fetchMetric(card.dataset.symbol);
      if (token !== this.renderToken || !card.isConnected) return;
      this.paintMetric(card, metric);
      card.dataset.metricState = 'ready';
    } catch (e) {
      const msg = String(e && e.message || e);
      if (attempt < 2 && token === this.renderToken && (msg.indexOf('429') >= 0 || msg.indexOf('503') >= 0)) {
        await new Promise(resolve => setTimeout(resolve, 800 * (attempt + 1)));
        if (token !== this.renderToken || !card.isConnected) return;
        return this.loadCard(card, token, attempt + 1);
      }
      if (!card.isConnected || token !== this.renderToken) return;
      card.classList.remove('loading');
      card.dataset.metricState = 'error';
      card.querySelector('.macro-latest-value').textContent = 'Unavailable';
      card.querySelector('.macro-latest-value').title = msg;
    }
  }

"""

FETCH_OLD = """  static async fetchMetric(symbol) {
    const cached = this.metricCache.get(symbol);"""

FETCH_NEW = r"""  static async fetchMarket(symbol) {
    const cached = this.metricCache.get(symbol);
    if (cached && Date.now() - cached.cachedAt < 10 * 60 * 1000) return cached;
    if (this.metricInflight.has(symbol)) return this.metricInflight.get(symbol);
    const request = (async () => {
      const id = encodeURIComponent(symbol);
      let last = null;
      let day = null;
      let asof = '';
      try {
        const quote = await fetch(PROXY + '/quotes?tickers=' + id);
        if (quote.ok) {
          const payload = await quote.json();
          const row = payload.tickers && payload.tickers[symbol];
          if (row && Number.isFinite(Number(row.price))) {
            last = Number(row.price);
            day = Number(row.changePct);
            asof = new Date().toISOString().slice(0, 10);
          }
        }
      } catch (e) {}
      const pct = (bars, back, current) => {
        const i = bars.length - 1 - back;
        if (i < 0 || current == null) return null;
        const prev = Number(bars[i].close);
        if (!Number.isFinite(prev) || prev === 0) return null;
        return ((current - prev) / Math.abs(prev)) * 100;
      };
      let changes = { D: Number.isFinite(day) ? day : null, W: null, M: null, Q: null, Y: null };
      try {
        const hist = await fetch(PROXY + '/yf-ohlc?symbol=' + id + '&range=1y&interval=1d&nowarehouse=1');
        if (hist.ok) {
          const payload = await hist.json();
          const bars = (payload.bars || []).filter(bar => Number.isFinite(Number(bar.close)));
          if (bars.length) {
            const end = bars[bars.length - 1];
            if (last == null) last = Number(end.close);
            if (!asof && end.time) asof = new Date(Number(end.time) * 1000).toISOString().slice(0, 10);
            changes = {
              D: Number.isFinite(day) ? day : pct(bars, 1, last),
              W: pct(bars, 5, last),
              M: pct(bars, 21, last),
              Q: pct(bars, 63, last),
              Y: pct(bars, Math.min(252, bars.length - 1), last),
            };
          }
        }
      } catch (e) {}
      if (last == null) throw new Error('No market print');
      const metric = { last: last, asof: asof, changes: changes, spacingDays: 1, cachedAt: Date.now() };
      this.metricCache.set(symbol, metric);
      return metric;
    })();
    this.metricInflight.set(symbol, request);
    request.finally(() => this.metricInflight.delete(symbol));
    return request;
  }

  static async fetchMetric(symbol) {
    if (String(symbol).indexOf(':') < 0) return this.fetchMarket(symbol);
    const cached = this.metricCache.get(symbol);"""

PAINT_OLD = """    card.querySelector('.macro-latest-value').textContent = this.formatValue(metric.last);
    card.querySelector('.macro-asof').textContent = metric.asof;"""

PAINT_NEW = """    const shown = this.formatValue(metric.last, item.units);
    card.querySelector('.macro-latest-value').textContent = shown;
    const unitNode = card.querySelector('.macro-latest-unit');
    if (unitNode && /T$/.test(shown) && (item.units === '$M' || item.units === '$B')) unitNode.textContent = 'USD';
    card.querySelector('.macro-asof').textContent = metric.asof;"""

FORMAT_OLD = """  static formatValue(value) {
    if (!Number.isFinite(value)) return '—';
    const abs = Math.abs(value);
    if (abs >= 1e12) return `${(value / 1e12).toFixed(2)}T`;
    if (abs >= 1e9) return `${(value / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `${(value / 1e6).toFixed(2)}M`;
    if (abs >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 1 });
    return value.toLocaleString(undefined, { maximumFractionDigits: abs < 10 ? 3 : 2 });
  }"""

FORMAT_NEW = """  static formatValue(value, units) {
    if (!Number.isFinite(value)) return '—';
    const u = String(units || '');
    if (u === '$M' && Math.abs(value) >= 1e6) return (value / 1e6).toFixed(2) + 'T';
    if (u === '$B' && Math.abs(value) >= 1000) return (value / 1e3).toFixed(2) + 'T';
    const abs = Math.abs(value);
    if (abs >= 1000) return value.toLocaleString('en-US', { maximumFractionDigits: abs >= 100000 ? 0 : 1 });
    return value.toLocaleString('en-US', { maximumFractionDigits: abs < 10 ? 3 : 2 });
  }"""

RESET_OLD = """  static reset() {
    if (!confirm('Restore the original curated macro catalog? Your custom categories, series, and danger rules will be removed.')) return;
    this.state = { version: 2, categories: this.defaults() };"""
RESET_NEW = """  static reset() {
    if (!confirm('Restore the original curated macro catalog? Your custom categories, series, and danger rules will be removed.')) return;
    this.state = { version: 2, catalogRev: this.CATALOG_REV, categories: this.defaults() };"""


def once(text, old, new, label):
    if new in text:
        print("already", label)
        return text
    n = text.count(old)
    if n != 1:
        raise SystemExit(label + " count " + str(n))
    print("patched", label)
    return text.replace(old, new, 1)


def main():
    text = PATH.read_text(encoding="utf-8")
    if "static CATALOG_REV = 3;" in text and "static async fetchMarket(" in text:
        print("already catalog")
    else:
        start = text.find("  static CATALOG = [")
        end = text.find("\n  static state = null;", start)
        if start < 0 or end < 0:
            raise SystemExit("catalog bounds")
        text = text[:start] + CATALOG + text[end:]
        print("patched catalog")
    text = once(text, LOAD_OLD, LOAD_NEW, "load")
    text = once(text, NORM_OLD, NORM_NEW, "normalize")
    if "static pumpMetrics(" in text:
        print("already hydrate")
    else:
        a = text.find(HYDRATE_OLD_START)
        b = text.find(PAINT_START, a)
        if a < 0 or b < 0:
            raise SystemExit("hydrate bounds")
        text = text[:a] + HYDRATE_NEW + text[b:]
        print("patched hydrate")
    text = once(text, FETCH_OLD, FETCH_NEW, "fetch")
    text = once(text, PAINT_OLD, PAINT_NEW, "paint")
    text = once(text, FORMAT_OLD, FORMAT_NEW, "format")
    text = once(text, RESET_OLD, RESET_NEW, "reset")
    if "NAPM" in text or "GOLDAMGBD228NLBM" in text or "TEDRATE" in text:
        raise SystemExit("dead series still present")
    PATH.write_text(text, encoding="utf-8")
    print("items", text.count('["', text.find("static CATALOG = ["), text.find("static load()")))


if __name__ == "__main__":
    main()
