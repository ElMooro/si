"""Explicit public feed adapters, verified against 2026-10-07 native packets.

Rows are declared paths, never a recursive search for something resembling a
ticker. A context-only adapter is deliberate: ports, currencies and series IDs
must not become equity symbols. Paths are JSON pointers into retained originals.
"""
GROUPS = {
    "macro": ("Economy and industry", "Relate economic observations to industry and earnings exposure.", "macro-nowcast nowcast-desk global-business-cycle physical-economy portwatch industry-rotation earnings-confluence"),
    "funding": ("Liquidity and funding", "Compare liquidity, credit and dollar funding; preserve risk holds.", "global-liquidity liquidity-credit-engine usd-funding eurodollar-plumbing credit-composite"),
    "treasury": ("Treasury and collateral", "Review issuance, settlement, collateral and currency transmission.", "treasury auction-desk settlement-fails treasury-rehypo bond-warroom dollar-radar fx-intelligence"),
    "ownership": ("Ownership and flows", "Distinguish fund flows, holdings changes, insiders and repurchases.", "etf-true-flows global-flow-desk flow-lookthrough holdings-research 13f-positions insider-radar buyback-engine"),
    "fundamentals": ("Fundamentals and accounting", "Compare valuation, estimates, cash quality and accounting warnings.", "fundamental-census-matrix fundamentals stock-valuations estimate-revisions earnings-quality forensic-screen beneish"),
    "catalysts": ("Earnings and catalysts", "Align reported periods, earnings events, language and filings.", "earnings-transcripts earnings-nlp earnings-confluence earnings-pead catalyst-calendar sec-filings-intel"),
    "technical": ("Technical conditions", "Separate structural setups, tape observations and qualified entries.", "bottom katlin phase-detector accumulation-radar tape-reader tape-truth volatility-squeeze"),
    "positioning": ("Options and positioning", "Review derivatives, crowding, catalysts and liquidity constraints.", "options-analytics options-confluence dealer-gex short-interest catalyst-calendar liquidity-capacity"),
    "industry": ("Industry and supply chain", "Trace demand, supplier relationships, industry rotation and innovation.", "forward-orders supply-chain-graph industry-rotation theme-cascade patent-velocity"),
    "crypto": ("Crypto and dollar liquidity", "Compare crypto leverage, basis, stablecoins and flows with dollar funding.", "crypto-confluence crypto-funding crypto-basis stablecoin-flow crypto-stablecoin-peg crypto-exchange-flows crypto-etf-flows dollar-radar usd-funding"),
    "ideas": ("Idea synthesis", "Compare overlapping idea engines without counting duplicates as independent votes.", "best-ideas master-ranker equity-confluence flow-confluence options-confluence khalid katlin ticker-360-native"),
    "challenge": ("Thesis challenge", "Expose opposing observations, missing invalidation rules and unresolved conflicts.", "engine-conflicts devils-advocate-public narrative-vs-tape kill-theses"),
    "portfolio": ("Portfolio context", "Carry public factors and scenarios into the existing private portfolio workflow.", "factor-risk correlation-surface stress-scenarios liquidity-capacity"),
    "evaluation": ("Evaluation and learning", "Keep prospective measurement, genealogy and reliability separate from promotion authority.", "genealogy prospective-outcomes signal-scorecard engine-trust signal-orthogonality alpha-decay"),
    "operations": ("Data and delivery health", "Expose freshness, contract failures and processed publication receipts.", "contract-violations _freshness-monitor event-flow-health"),
    "briefing": ("Unified briefing", "Present the common evidence alongside the authoritative risk decision.", "engine-fusion jh-fusion khalid-risk alpha-council financial-secretary"),
}

# (JSON pointer, symbol field; @key means an explicitly declared symbol map).
ROWS = {
    "industry-rotation": [("/industry_credit", "@key")],
    "earnings-confluence": [("/confluence_book", "ticker")],
    "etf-true-flows": [("/by_etf", "@key")],
    "global-flow-desk": [("/funds", "@key")],
    "flow-lookthrough": [("/funds", "@key")],
    "insider-radar": [("/latest_buys", "ticker"), ("/finviz_sells", "ticker")],
    "buyback-engine": [("/tickers", "@key")],
    "fundamentals": [("/companies", "ticker")],
    "stock-valuations": [("/sp_table", "t")],
    "estimate-revisions": [("/request_records", "ticker")],
    "earnings-quality": [("/issuer_rows", "ticker")],
    "forensic-screen": [("/issuers", "symbol")],
    "beneish": [("/all_tickers", "ticker")],
    "earnings-nlp": [("/by_ticker", "@key")],
    "earnings-pead": [("/request_records", "ticker")],
    "catalyst-calendar": [("/events", "ticker")],
    "sec-filings-intel": [("/all_tickers", "ticker")],
    "bottom": [("/board", "ticker")],
    "phase-detector": [("/tickers", "@key")],
    "accumulation-radar": [("/market_leaders", "ticker"), ("/leaders_fading", "ticker")],
    "katlin": [("/picks", "ticker"), ("/watch", "ticker")],
    "tape-reader": [("/top_loud_tape", "ticker")],
    "tape-truth": [("/symbols", "@key")],
    "volatility-squeeze": [("/request_records", "ticker")],
    "options-analytics": [("/board", "ticker")],
    "options-confluence": [("/ticker_map", "@key")],
    "liquidity-capacity": [("/positions", "symbol")],
    "forward-orders": [("/all_results", "ticker")],
    "supply-chain-graph": [("/nodes", "ticker")],
    "theme-cascade": [("/all_ranked", "ticker")],
    "patent-velocity": [("/all_results", "ticker")],
    "crypto-confluence": [("/confluence_book", "coin")],
    "crypto-funding": [("/by_coin", "@key")],
    "stablecoin-flow": [("/top_stablecoins_by_mcap", "symbol")],
    "crypto-stablecoin-peg": [("/coins", "@key")],
    "best-ideas": [("/stack", "symbol")],
    "master-ranker": [("/top_tickers", "ticker"), ("/unranked_tickers", "ticker")],
    "equity-confluence": [("/xray_map", "@key")],
    "flow-confluence": [("/ticker_map", "@key")],
    "khalid": [("/opportunity_radar", "ticker")],
    "narrative-vs-tape": [("/quiet_accumulation", "ticker"), ("/crowded_fading", "ticker")],
    "kill-theses": [("/theses", "symbol")],
    "factor-risk": [("/risk_contributors", "symbol")],
    "stress-scenarios": [("/asset_impact/all", "ticker")],
}
KEYS = {"treasury": "data/warm/treasury/latest-summary.json",
        "financial-secretary": "data/secretary-latest.json",
        "ticker-360-native": "data/ticker-360.json",
        "genealogy": "data/signal-genealogy-research/current.json"}
CRYPTO = frozenset("crypto-confluence crypto-funding stablecoin-flow crypto-stablecoin-peg".split())
MIXED = frozenset(("bottom", "katlin", "khalid"))
SOURCES = {
    name: {"id": name, "key": KEYS.get(name, "data/" + name + ".json"),
           "rows": ROWS.get(name, []),
           "identity_scope": "crypto" if name in CRYPTO else "explicit" if name in MIXED else "listed_security",
           "groups": [gid for gid, (_, _, names) in GROUPS.items() if name in names.split()],
           # Publication SLA only. It is never substituted for observation age.
           "publication_sla_hours": 48 if name in ("forensic-screen", "fundamental-census-matrix", "holdings-research", "portwatch", "treasury") else 26}
    for name in sorted({s for _, _, names in GROUPS.values() for s in names.split()})
}
SOURCES['earnings-transcripts']['access_status'] = 'public_access_not_verified'

# Research consumers receive these sections, without affecting their scoring.
SUBSCRIPTIONS = {
    "industry-rotation": ("macro", "funding", "industry", "ownership"),
    "earnings-confluence": ("macro", "fundamentals", "catalysts"),
    "fx-intelligence": ("treasury", "funding", "macro"),
    "stock-valuations": ("fundamentals", "catalysts", "challenge"),
    "khalid": ("technical", "positioning", "ideas", "challenge", "briefing"),
    "engine-fusion": ("funding", "treasury", "operations"),
    "khalid-risk": ("funding", "treasury", "positioning", "operations"),
    "jh-fusion": ("ideas", "challenge", "evaluation", "operations"),
    "master-ranker": ("macro", "ownership", "fundamentals", "catalysts", "industry"),
    "alpha-council": tuple(GROUPS),
    "financial-secretary": tuple(GROUPS),
    "portfolio-risk": ("macro", "funding", "treasury", "portfolio", "challenge"),
    "prospective-evaluator": ("evaluation", "operations"),
}
