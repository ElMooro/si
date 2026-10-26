#!/usr/bin/env python3
"""Bank-fusion audit: which engines should consume which newly banked datasets (Khalid 2026-10-25: "fuse the new data
in all the engines that need them to improve and enrich those engines").

Static, repo-only (no AWS): for every engine under aws/lambdas/ it reads the module docstring, config description,
provider endpoints (FMP from docs/audit/2026-10-25/fmp-consumer-census.json, the rest by regex), universe hints
(S&P-only / curated list / MAX_TICKERS caps - the per-symbol API cost that the banks remove) and the bank-first shims
present, then scores every bank dataset family against the engine's purpose with the curated rule set below.

Writes docs/audit/<date>/bank-fusion-map.json and .md.  The .md is the program ledger: per dataset the engines, per
engine the datasets with the fusion mode:
    U = universe: the engine caps its universe for API cost -> scan the whole market from the factor book
    F = factors: new whole-market fields the engine never had (scores, consensus, short interest, float, ...)
    H = history: full histories the engine fetched 1-6 periods of (statements since 1985, EOD since 2000)
    E = events/feeds: calendars, filings, insider/13F/political feeds, news
Run: python3 scripts/bank_fusion_audit.py [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAMBDAS = ROOT / "aws" / "lambdas"
CENSUS = ROOT / "docs" / "audit" / "2026-10-25" / "fmp-consumer-census.json"

# ----------------------------------------------------------------------------------------------------------------------
# The newly banked dataset families (today's banks) and the engine purposes they serve.  `core` (specific) and `ctx` (generic) are matched as
# whole words (case-insensitive) against engine name + description + docstring (strong) and source identifiers (weak); `mode` is the fusion mode; `bank` the home bank; `fields` what the factor book carries from it.
# ----------------------------------------------------------------------------------------------------------------------
DATASETS = [
    # --- FMP bulk (whole market, data/warm/fmp-bank) ---
    dict(id="fmp:scores-bulk", bank="fmp-bank", mode="F", fields="altman_z, piotroski, working_capital, retained_earnings, ebit, revenue, market_cap",
         core=r"altman|piotroski|beneish|forensic|distress|bankrupt|zombie|net.net|deep.value|accrual|earnings.quality|credit.risk|default.risk|fortress|quality.on.sale|coffee.can|magic.formula|graham", ctx=r"quality|value|screen|screener|fraud|credit"),
    dict(id="fmp:rating-bulk", bank="fmp-bank", mode="F", fields="rating, overall score, DCF/ROE/ROA/DE/PE/PB component scores",
         core=r"stock.rating|rating.change|ratings?.cluster|starmine|stock.analyzer|stock.picks|quality.screen", ctx=r"rating|grade|screener|research"),
    dict(id="fmp:dcf-bulk", bank="fmp-bank", mode="F", fields="dcf, price, dcf_upside_pct",
         core=r"dcf|intrinsic|gf.value|fair.value|margin.of.safety|stock.valuations|reverse.dcf|owner.earnings", ctx=r"valuation|undervalu|cheap|upside|value"),
    dict(id="fmp:price-target-summary-bulk", bank="fmp-bank", mode="F", fields="pt last month/quarter/year avg-high-low, n analysts, pt_upside_pct",
         core=r"price.target|target.price|sellside|sell.side|analyst.consensus|starmine|estimate.revision|eps.revision|rating.change|analyst", ctx=r"consensus|upside|target|estimate|revision"),
    dict(id="fmp:upgrades-downgrades-consensus-bulk", bank="fmp-bank", mode="F", fields="strongBuy/buy/hold/sell/strongSell, consensus",
         core=r"upgrade|downgrade|rating.change|analyst.consensus|sellside|sell.side|starmine", ctx=r"analyst|consensus|grade"),
    dict(id="fmp:key-metrics-ttm-bulk", bank="fmp-bank", mode="F", fields="~60 TTM metrics: EV/EBITDA, ROIC, FCF yield, earnings yield, net debt/EBITDA, interest coverage, ...",
         core=r"roic|fcf.yield|ev.?/?ebitda|enterprise.value|magic.formula|eva.spread|smart.beta|quality.on.sale|coffee.can|screener|stock.valuations|deep.value|buyback.yield|shareholder.yield|key.metrics", ctx=r"valuation|metric|quality|screen|factor|quant|value|yield"),
    dict(id="fmp:ratios-ttm-bulk", bank="fmp-bank", mode="F", fields="~60 TTM ratios: PE, PB, PS, P/FCF, margins, turnovers, payout, dividend yield, leverage",
         core=r"payout.ratio|dividend.yield|peer.comparison|valuation.table|screener|ratios?.ttm|price.to.book|price.to.sales|p/e|p/b|p/s", ctx=r"valuation|ratio|margin|payout|dividend|screen|peer"),
    dict(id="fmp:statements-bulk", bank="fmp-bank", mode="H", fields="income / balance / cash-flow statements + growth per fiscal year x annual/Q1-Q4 since 1985 (whole market)",
         core=r"income.statement|balance.sheet|cash.flow|capex|repurchase|buyback|accrual|beneish|net.net|deep.value|revenue.acceleration|fundamental.graphs|fundamental.census|eva|earnings.quality|working.capital|inventory.turnover|days.sales|dso|dilution|share.count|capital.allocation", ctx=r"fundamental|statement|revenue|growth|margin|fcf|debt|leverage"),
    dict(id="fmp:earnings-surprises-bulk", bank="fmp-bank", mode="F", fields="per-year EPS actual vs estimate (beat rate, avg surprise %)",
         core=r"earnings.surprise|surprise|pead|post.earnings|beat.rate|beat|miss|whisper|earnings.quality|earnings.tracker|earnings.preview|eps.revision|earnings.iv|earnings.pead", ctx=r"earnings|estimate|eps|guidance"),
    dict(id="fmp:peers-bulk", bank="fmp-bank", mode="F", fields="peer list per symbol",
         core=r"peer|pairs?.trad|pairs.arb|pair.trades|sympath|readthrough|read.through|supply.chain|linkage|competitor|peer.comparison", ctx=r"relative|sector|industry|comparison"),
    dict(id="fmp:etf-holder-bulk", bank="fmp-bank", mode="F", fields="which ETFs hold the symbol and at what weight (passive ownership intensity)",
         core=r"index.inclusion|inclusion|russell|reconstitution|rebalance|passive.ownership|etf.holder|etf.true.flows|etf.census|stock.exposure|index.recon", ctx=r"etf|passive|flows|holder|ownership|concentration"),
    dict(id="fmp:profile-bulk", bank="fmp-bank", mode="U", fields="every listed symbol: name, exchange, sector, industry, country, market cap, beta, ipo date, isEtf, isActivelyTrading",
         core=r"universe.builder|universe.discovery|symbology|symdir|market.map|theme.classifier|theme.tier|sector.heatmap|census|screener|microcap|micro.cap|small.cap|mid.cap|ipo.date|adr\b|company.screener", ctx=r"universe|sector|industry|market.cap|country|classifier|discovery|map"),
    dict(id="fmp:eod-bulk", bank="fmp-bank", mode="H", fields="every listed symbol's close per session since 2000 (global)",
         core=r"backtest|forward.return|signal.backtest|outcome.checker|calibrat|forward.orders|replay|lookback|analog|seasonal|52.?wk|52.?week|spx.history|price.history|historical.price", ctx=r"momentum|breadth|trend|returns|drawdown|volatility|beta|correlation|regime|historical|history"),
    # --- FMP walker (per-symbol histories and feeds, data/warm/fmp) ---
    dict(id="fmp:shares-float-all", bank="fmp-walker", mode="F", fields="free float shares, outstanding, float %",
         core=r"free.float|float.shares|low.float|shares.float|float.pct|float.turnover|squeeze|short.interest|days.to.cover|microcap|micro.cap|pre.pump|pump|ignition|ftd|fails.to.deliver|lockup|lock.up|spac|borrow", ctx=r"short|liquidity|small.cap|float"),
    dict(id="fmp:insider-feeds", bank="fmp-walker", mode="E", fields="insider-trading latest/search/statistics/transaction-type (whole feed, not a watchlist)",
         core=r"insider|form.4|form4|open.market.buy|officer|director.buy|cluster.buy|buy.cluster|insider.own", ctx=r"cluster|buying"),
    dict(id="fmp:institutional-13f", bank="fmp-walker", mode="E", fields="institutional-ownership latest/extracts per filer/holder performance/positions summary/industry breakdown",
         core=r"13f|13.f|institutional|smart.money|whale|hedge.fund|clone|activist|13d|smart.money.cluster", ctx=r"holdings|ownership|fund.flow|holder"),
    dict(id="fmp:political-trades", bank="fmp-walker", mode="E", fields="senate/house trades, latest, by id",
         core=r"senate|house.trades|congress|political|politic|capitol|lawmaker|pelosi|quiver", ctx=r"trades"),
    dict(id="fmp:estimates-targets-grades", bank="fmp-walker", mode="F", fields="analyst-estimates annual/quarter, price-target + consensus + news, grades + historical + news",
         core=r"analyst.estimate|estimate.revision|eps.revision|estimate.revisions|price.target|grades?|starmine|sellside|sell.side|earnings.preview|rating.change|analyst.consensus|revision.velocity", ctx=r"estimate|eps|guidance|target|analyst|consensus|revision"),
    dict(id="fmp:transcripts", bank="fmp-walker", mode="E", fields="earning-call-transcript per quarter + dates + latest",
         core=r"transcript|earnings.call|call.transcript|earnings.nlp|pump.earnings.nlp|management.tone|language", ctx=r"nlp|sentiment|guidance|tone"),
    dict(id="fmp:calendars", bank="fmp-walker", mode="E", fields="earnings / dividends / splits / IPO calendars (+ disclosure, prospectus), economic calendar",
         core=r"earnings.calendar|econ.calendar|economic.calendar|macro.calendar|catalyst.calendar|opex.calendar|dividends?.calendar|splits?.calendar|ipo.calendar|ipo.pipeline|lockup|lock.up|blackout|precatalyst|pre.catalyst|earnings.date|next.earnings|event.driven|catalyst", ctx=r"calendar|event|ipo|spac|fomc|econ"),
    dict(id="fmp:sec-filings-feeds", bank="fmp-walker", mode="E", fields="sec-filings-8k / financials / search-form-type / search-symbol",
         core=r"8.?k\b|form.8|edgar|sec.filing|filings?|10.?k\b|10.?q\b|material.event|edgar.authority|sec.8k", ctx=r"sec\b|disclosure|activist|13d"),
    dict(id="fmp:ma-and-capital-feeds", bank="fmp-walker", mode="E", fields="mergers-acquisitions-latest, crowdfunding, fundraising, symbol-change, delisted",
         core=r"merger|m&a|ma.tracker|ma.target|merger.arb|deal.scanner|acquisition|spinoff|spin.off|delist|symbol.change|corporate.action|fundraising|crowdfunding", ctx=r"deal|arb|target"),
    dict(id="fmp:news-feeds", bank="fmp-walker", mode="E", fields="news stock/general/press-releases/crypto/forex (+ latest), fmp-articles, price-target & grades news",
         core=r"news|headline|press.release|news.wire|newswire|buzz|narrative|media", ctx=r"sentiment|story|wire"),
    dict(id="fmp:macro-rates", bank="fmp-walker", mode="F", fields="treasury-rates history, market-risk-premium by country, economic-calendar",
         core=r"treasury.rate|treasury.yield|yield.curve|risk.premium|market.risk.premium|term.premium|fed.pivot|rates.desk|bond.desk|us.cycle|global.macro|fomc.reaction|eurodollar", ctx=r"treasury|rates|macro|bond|fed|econ|duration|fomc"),
    dict(id="fmp:cot", bank="fmp-walker", mode="H", fields="commitment-of-traders report + analysis per symbol",
         core=r"cot\b|commitment.of.traders|cftc|speculator|commercial.hedger|cot.tracker|positioning.analog|futures.positioning", ctx=r"positioning|futures"),
    dict(id="fmp:sector-industry-pe-perf", bank="fmp-walker", mode="F", fields="sector / industry PE snapshots + history, performance snapshots + history",
         core=r"sector.rotation|rotation|sector.heatmap|heatmap|heat.map|diffusion|sector.earnings|industry.pe|sector.pe|sector.performance|leadership|theme.rotation|group.strength|relative.strength", ctx=r"sector|industry|theme|breadth|relative"),
    dict(id="fmp:market-cap-history", bank="fmp-walker", mode="H", fields="historical-market-capitalization per symbol",
         core=r"market.cap|market.capitalization|concentration|index.weight|top.heavy|mega.cap|cap.weight|concentration.liquidity", ctx=r"weight|breadth|liquidity"),
    dict(id="fmp:revenue-segmentation", bank="fmp-walker", mode="F", fields="revenue by product and by geography (annual/quarter)",
         core=r"segment|segmentation|geograph|revenue.by|china.exposure|tariff|supply.chain|linkage|readthrough|read.through|exposure.map|international.revenue|foreign.revenue|regional.exposure|theme.second.wave|chokepoint|bottleneck", ctx=r"exposure|china|theme|revenue|region|country|emerging"),
    dict(id="fmp:employees", bank="fmp-walker", mode="F", fields="employee-count + historical",
         core=r"employee|headcount|hiring|talent|layoff|workforce|labor.market|hiring.velocity|talent.migration", ctx=r"labor"),
    dict(id="fmp:esg", bank="fmp-walker", mode="F", fields="esg-disclosures / ratings / benchmark",
         core=r"esg|sustainab|carbon|climate", ctx=r"governance"),
    dict(id="fmp:etf-holdings-weightings", bank="fmp-walker", mode="F", fields="etf-holdings, sector/country weightings, asset exposure, info, portfolio dates",
         core=r"etf.holdings|etf.weight|sector.weight|country.weight|asset.exposure|etf.overlap|overlap|constituent|etf.census|etf.true.flows|thematic.etf|cef|closed.end|crypto.etf|etf.arb|stock.exposure", ctx=r"etf|holdings|weighting|exposure|flows|passive"),
    dict(id="fmp:intraday-1min", bank="fmp-walker", mode="H", fields="historical-chart-1min per session for the chart universe",
         core=r"intraday|1.?min|one.minute|minute.bar|vwap|opening.range|gap.fill|gap.metrics|microstructure|live.pulse|intraday.pulse|session.5min|tick.session", ctx=r"gap|session|tape|pulse"),
    dict(id="fmp:movers-snapshots", bank="fmp-walker", mode="E", fields="biggest-gainers / losers / most-actives dated daily",
         core=r"biggest.gainers|gainers|losers|most.active|movers|top.movers|pump|ignition|pre.pump|velocity|buzz.velocity|momentum.leaders|momentum.breakout", ctx=r"mover|active|momentum|retail"),
    dict(id="fmp:dividends-splits", bank="fmp-walker", mode="H", fields="dividends + splits history per symbol",
         core=r"dividend|divcut|div.cut|payout|aristocrat|dividend.growth|ex.div|split", ctx=r"income|yield|cut"),
    dict(id="fmp:governance-execs", bank="fmp-walker", mode="F", fields="key-executives, executive compensation (+ benchmark)",
         core=r"executive|ceo\b|cfo\b|compensation|governance|management.change|key.executives|founder.led", ctx=r"management|insider"),
    dict(id="fmp:fail-to-deliver", bank="fmp-walker", mode="H", fields="fail-to-deliver per symbol",
         core=r"ftd|fails?.to.deliver|cns.fails|settlement.fail|equity.ftd|fails\b", ctx=r"squeeze|short|settlement|plumbing"),
    dict(id="fmp:social-sentiment", bank="fmp-walker", mode="F", fields="historical-social-sentiment, trending, change",
         core=r"social.sentiment|reddit|stocktwits|twitter|wsb|wallstreetbets|meme|retail.sentiment|buzz|crowd|social", ctx=r"retail|sentiment"),
    dict(id="fmp:funds-disclosure", bank="fmp-walker", mode="E", fields="mutual-fund / ETF disclosures per holder (N-PORT)",
         core=r"mutual.fund|fund.holdings|n.port|nport|fund.disclosure|smart.money.holdings|funds.disclosure|fund.flow", ctx=r"fund|holdings|disclosure|flows"),
    dict(id="fmp:company-notes", bank="fmp-walker", mode="F", fields="company-notes (debt instruments outstanding)",
         core=r"company.notes|debt.maturit|maturity.wall|refinanc|credit.risk|cds\b|credit.equity|bond.notes|leveraged.loan|covenant|cds.monitor", ctx=r"credit|debt|bond|leverage|distress|default"),
    # --- Polygon bank (data/warm/polygon-bank) and the per-ticker tape (data/warm/polygon-full/eod) ---
    dict(id="polygon:short-interest-volume", bank="polygon-bank", mode="F", fields="short interest (settlement dates, days to cover), daily short volume ratio",
         core=r"short.interest|short.volume|days.to.cover|squeeze|borrow|ftd|fails\b|crowded.short|short.squeeze|microcap|micro.cap|pre.pump|pump|ignition|free.float|low.float", ctx=r"short|positioning|crowded|float"),
    dict(id="polygon:ticker-events-ipos", bank="polygon-bank", mode="E", fields="ticker events (name/ticker changes), IPOs",
         core=r"ipo\b|ipos\b|ipo.pipeline|lockup|lock.up|spac|symbol.change|ticker.change|rename|delist|corporate.action|new.listing", ctx=r"listing"),
    dict(id="polygon:options-contracts", bank="polygon-bank", mode="F", fields="options contracts (incl. expired) for the chart universe",
         core=r"option|options|gex|gamma|dealer|opex|expiration|expiry|implied.vol|iv.crush|iv.rank|skew|straddle|put.call|options.flow|unusual|convexity|vrp|variance.risk|tail.hedg|hedge.planner|0dte|max.pain|option.chain", ctx=r"chain|flow|hedge"),
    dict(id="polygon:minute-bars", bank="polygon-bank", mode="H", fields="minute bars (730 days) for the chart universe",
         core=r"intraday|minute|1.?min|5.?min|vwap|opening.range|gap.fill|microstructure|live.pulse|tick.session|session.5min|tape.reader|market.tape|intraday.pulse|trade.ticket", ctx=r"gap|session|tape|pulse"),
    dict(id="polygon:etf-global", bank="polygon-bank", mode="F", fields="ETF Global fund flows, constituents, profiles for every US ETF",
         core=r"etf.flow|fund.flow|etf.true.flows|etf.census|inflow|outflow|etf.constituent|constituent|passive.flow|crypto.etf|etf.arb|share.flows|capital.flow|asset.compass|rebalance.radar|etf.flows", ctx=r"etf|flows|passive|rebalance|exposure"),
    dict(id="polygon:grouped-crypto-fx", bank="polygon-bank", mode="H", fields="grouped daily crypto and FX sessions since 2018",
         core=r"crypto|bitcoin|btc|eth\b|ethereum|altcoin|forex|fx.regime|dxy|eurusd|usdjpy|yen|emerging.fx|carry.surface|fx.carry", ctx=r"dollar|currency|fx\b|carry"),
    dict(id="polygon:eod-per-ticker", bank="polygon-eod", mode="H", fields="every US ticker's daily OHLCV/VWAP/trades since 2021-08-26 in one file per ticker",
         core=r"backtest|forward.return|signal.backtest|outcome.checker|calibrat|replay|lookback|seasonal|52.?wk|52.?week|moving.average|rsi\b|relative.strength|chart.pattern|double.top|double.bottom|multi.tf|mean.reversion|trend.engine|spx.ma|price.history|historical.price|daily.bars|ohlc|trend.reversal|failed.pattern|momentum.breakout|momentum.leaders|breadth.divergence|breadth.thrust|beta.laggard|volatility.squeeze", ctx=r"returns|volatility|regime|history|historical|chart|momentum|breadth|trend|reversal|pattern|technical|drawdown|beta|correlation"),
    dict(id="polygon:market-plumbing", bank="polygon-bank", mode="E", fields="market status/holidays, exchanges, conditions, ticker types, whole-market snapshot + gainers/losers",
         core=r"market.holiday|holiday|market.status|trading.calendar|exchange.list|whole.market.snapshot|market.breadth|breadth.thrust|advance.decline|up.down.volume|market.tape|tape.reader|biggest.gainers|biggest.losers", ctx=r"calendar|session|breadth|mover|snapshot|gainers|losers"),
    # --- EIA / CMC / CryptoQuant / TE (data/warm/eia-bank, data/warm/provider-bank) ---
    dict(id="eia:full-catalog", bank="eia-bank", mode="H", fields="whole EIA v2 tree: petroleum stocks/prices/production/imports/refinery runs, natural gas storage/prices, electricity RTO, coal, STEO, nuclear outages",
         core=r"eia\b|crude|wti|brent|refin|gasoline|diesel|distillate|natural.gas|natgas|lng|henry.hub|power|electric|grid|uranium|nuclear|coal|opec|cushing|rig.count|petroleum|energy|inventory.drawdown|oil\b|commodity.curves|metals.miners", ctx=r"inventory|commodity|inflation"),
    dict(id="cmc:crypto-market", bank="provider-bank", mode="F", fields="CMC categories, global metrics history (dominance, total cap), fear & greed history, exchange map, listings",
         core=r"crypto|bitcoin|btc|eth\b|ethereum|altcoin|defi|stablecoin|dominance|fear.and.greed|fear.greed|coin\b|token|crypto.etf|on.chain|onchain|miner", ctx=r"digital"),
    dict(id="cq:onchain-hourly", bank="provider-bank", mode="H", fields="CryptoQuant hourly metrics per exchange / miner (flows, reserves)",
         core=r"on.chain|onchain|cryptoquant|exchange.flow|exchange.reserve|miner|whale.flow|stablecoin.flow|btc|bitcoin|crypto", ctx=r"whale|reserve"),
    dict(id="te:macro-markets", bank="provider-bank", mode="F", fields="Trading Economics calendar, country indicators, markets (bond/commodity/currency/index/crypto), forecasts, ratings (once the 401 is cleared)",
         core=r"trading.economics|tradingeconomics|sovereign|emerging.market|country.risk|global.macro|global.markets|gdp\b|cpi\b|pmi\b|central.bank|hkma|apac|ecb\b|boj\b|boe\b|pboc|global.stress|global.cycle|global.business|asia.leads|apac.flows", ctx=r"macro|country|inflation|rates|bond|yield|calendar|rating|forecast|world|international|europe|asia"),
]

UNIVERSE_HINTS = [
    (r"sp500|s&p ?500|S&P-wide|sp500-constituent|SP500", "S&P 500 only"),
    (r"MAX_TICKERS|MAX_SYMBOLS|MAX_NAMES|max_tickers|\[:\s*\d{2,3}\]", "hard cap on names"),
    (r"curated|hand.picked|watchlist|WATCHLIST|CORE_40|core 40", "curated / watchlist universe"),
    (r"company-screener|screener ladder", "FMP screener ladder (search endpoint, not banked)"),
    (r"data/universe\.json", "unified universe (data/universe.json)"),
    (r"finviz", "Finviz universe"),
]

HOSTS = {
    "polygon": r"api\.polygon\.io/", "fred": r"api\.stlouisfed\.org", "sec": r"sec\.gov", "eia": r"api\.eia\.gov",
    "cmc": r"coinmarketcap\.com", "newsapi": r"newsapi\.org", "te": r"tradingeconomics\.com", "ndl": r"data\.nasdaq\.com",
    "cq": r"cryptoquant\.com", "yahoo": r"query[12]\.finance\.yahoo\.com|yfinance", "finviz": r"finviz\.com",
    "tradingview": r"tradingview\.com", "coingecko": r"coingecko\.com", "cboe": r"cboe\.com", "treasury": r"treasury\.gov|fiscaldata",
    "nyfed": r"newyorkfed\.org", "ecb": r"data-api\.ecb\.europa\.eu|ecb\.europa\.eu", "bls": r"api\.bls\.gov", "bea": r"bea\.gov",
}

WORD = re.compile(r"[A-Za-z][A-Za-z0-9_&-]+")


def read(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:  # noqa: BLE001
        return ""


def docstring(src: str) -> str:
    m = re.match(r'\s*(?:#.*\n)*\s*[rubRUB]*("""|\'\'\')(.*?)\1', src, re.S)
    return (m.group(2) if m else "")[:1500]


def engine_text(name: str, desc: str, doc: str, src: str) -> tuple[str, str]:
    """(strong text, weak text): purpose words vs source identifiers."""
    strong = " ".join([name.replace("justhodl-", "").replace("-", " "), desc, doc]).lower()
    idents = " ".join(sorted(set(WORD.findall(src))))[:200000].lower()
    return strong, idents


def hits(pattern: str, text: str) -> int:
    return len(re.findall(r"(?<![a-z0-9])(?:%s)(?![a-z0-9])" % pattern, text))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-10-26")
    a = ap.parse_args()
    census = json.loads(read(CENSUS) or "{}").get("engines", {}) if CENSUS.exists() else {}
    engines = []
    for d in sorted(p for p in LAMBDAS.iterdir() if p.is_dir()):
        srcp = d / "source" / "lambda_function.py"
        if not srcp.exists():
            continue
        src = read(srcp)
        cfg = {}
        try:
            cfg = json.loads(read(d / "config.json") or "{}")
        except Exception:  # noqa: BLE001
            cfg = {}
        desc = (cfg.get("description") or "")
        doc = docstring(src)
        strong, idents = engine_text(d.name, desc, doc, src)
        providers = sorted(k for k, rx in HOSTS.items() if re.search(rx, src))
        fmp_eps = sorted((census.get(d.name) or {}).get("endpoints") or []) if d.name in census else (["(direct FMP caller)"] if "financialmodelingprep.com" in src else [])
        if fmp_eps:
            providers = sorted(set(providers) | {"fmp"})
        universe = [label for rx, label in UNIVERSE_HINTS if re.search(rx, src)]
        shims = [s for s in ("fmp_bankfirst", "polygon_bankfirst", "fmp_bank", "fmp_book", "bank_enrich") if re.search(r"\b%s\b" % s, src)]
        outputs = sorted(set(re.findall(r'["\'](data/[A-Za-z0-9_./-]+\.json(?:\.gz)?)["\']', src)))[:12]
        scored = []
        for ds in DATASETS:
            s = min(5 * hits(ds["core"], strong), 10) + min(hits(ds["ctx"], strong), 4) + min(2 * hits(ds["core"], idents), 4)
            if s >= 5:
                scored.append((s, ds["id"], ds["mode"]))
        scored.sort(reverse=True)
        capped = bool(universe) and any(p in providers for p in ("fmp", "polygon", "yahoo", "finviz"))
        engines.append(dict(engine=d.name, description=desc[:220], providers=providers, fmp_endpoints=fmp_eps[:40], universe=universe,
                            universe_capped=capped, shims=shims, outputs=outputs, size=len(src),
                            datasets=[dict(id=i, mode=m, score=s) for s, i, m in scored[:12]]))
    by_ds = collections.defaultdict(list)
    for e in engines:
        for x in e["datasets"]:
            by_ds[x["id"]].append((x["score"], e["engine"]))
    out_dir = ROOT / "docs" / "audit" / a.date
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = dict(generated=a.date, engines=len(engines), with_datasets=sum(1 for e in engines if e["datasets"]),
                   universe_capped=sum(1 for e in engines if e["universe_capped"]),
                   datasets=[dict(id=d["id"], bank=d["bank"], mode=d["mode"], fields=d["fields"], engines=len(by_ds[d["id"]])) for d in DATASETS],
                   by_dataset={k: [n for _, n in sorted(v, reverse=True)] for k, v in by_ds.items()}, engine_map=engines)
    (out_dir / "bank-fusion-map.json").write_text(json.dumps(payload, indent=1), encoding="utf-8")
    md = [f"# Bank fusion map — {a.date}", "",
          f"{len(engines)} engines scanned; {payload['with_datasets']} match at least one newly banked dataset family; "
          f"{payload['universe_capped']} cap their universe for API cost (S&P-only / hard caps / curated lists) and can scan the whole market from the factor book.",
          "", "Modes: U universe · F factors · H history · E events/feeds.  Scores are keyword evidence from the engine's name, description and docstring (×3) plus source identifiers; they rank the queue, a human (or the wave review) decides the integration.", "",
          "## Datasets → engines", "", "| dataset | bank | mode | fields | engines |", "|---|---|---|---|---|"]
    for d in DATASETS:
        names = payload["by_dataset"].get(d["id"], [])
        md.append("| `%s` | %s | %s | %s | %d: %s%s |" % (d["id"], d["bank"], d["mode"], d["fields"], len(names), ", ".join(n.replace("justhodl-", "") for n in names[:14]), " …" if len(names) > 14 else ""))
    md += ["", "## Engines → datasets", "", "| engine | providers now | universe | datasets (mode) |", "|---|---|---|---|"]
    for e in sorted(engines, key=lambda e: (-len(e["datasets"]), e["engine"])):
        if not e["datasets"]:
            continue
        md.append("| `%s` | %s | %s | %s |" % (e["engine"], " ".join(e["providers"]) or "—", "; ".join(e["universe"]) or "—",
                                              ", ".join("%s (%s)" % (x["id"], x["mode"]) for x in e["datasets"][:8])))
    (out_dir / "bank-fusion-map.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("engines %d, with datasets %d, universe-capped %d -> %s" % (len(engines), payload["with_datasets"], payload["universe_capped"], out_dir))
    for d in DATASETS:
        print("%-42s %3d" % (d["id"], len(payload["by_dataset"].get(d["id"], []))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
