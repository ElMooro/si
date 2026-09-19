"""Explicit research-view configuration; not a claim of complete market coverage."""

CONTEXT_KEYS = (
    'etf-flows/daily.json', 'data/dark-pool.json', 'data/money-flow-state.json',
    'data/capital-flow-radar.json', 'data/aaii-sentiment.json', 'data/aaii.json',
    'data/stablecoin-flow.json', 'data/sovereign-fiscal.json',
    'data/polygon-fx-regime.json', 'data/capex-pulse.json',
    'data/structural-pre-signals.json', 'data/structural-presignals.json',
)

CTRY = {'MCHI': 'China', 'FXI': 'China', 'ASHR': 'China', 'EWJ': 'Japan', 'EWG': 'Germany', 'EWY': 'South Korea', 'EWT': 'Taiwan', 'EWU': 'UK', 'EWC': 'Canada', 'EWA': 'Australia', 'EWW': 'Mexico', 'EZA': 'South Africa', 'TUR': 'Turkey', 'EPOL': 'Poland', 'ARGT': 'Argentina', 'EIDO': 'Indonesia', 'VNM': 'Vietnam', 'THD': 'Thailand', 'EWQ': 'France', 'EWL': 'Switzerland', 'EWI': 'Italy', 'EWP': 'Spain', 'EWS': 'Singapore', 'INDA': 'India', 'EWZ': 'Brazil', 'EEM': 'EM (broad)', 'VWO': 'EM (broad)', 'EFA': 'DM ex-US', 'VEA': 'DM ex-US'}

CLASSES = {'EQUITY_US': ['BROAD_EQUITY_US'], 'EQUITY_INTL': ['INTERNATIONAL'], 'SECTORS': ['SECTOR_EQUITY'], 'TREASURIES': ['RATES_TREASURIES'], 'CREDIT': ['CREDIT'], 'TIPS': ['TIPS_INFLATION'], 'CRYPTO': ['CRYPTO_ETF', 'CRYPTO'], 'COMMODITIES': ['COMMODITIES'], 'THEMATIC': ['THEMATIC'], 'DIVIDEND_VALUE': ['DIVIDEND_VALUE'], 'GROWTH': ['GROWTH'], 'VOL': ['VOLATILITY']}

SECTORS = ('XLK','XLF','XLE','XLV','XLI','XLY','XLP','XLU','XLB','XLRE','XLC')
EXTRA_GROUPS = {
    'GOLD_WRAPPERS': ('GLD','IAU','GDX'),
    'SILVER_WRAPPERS': ('SLV',),
    'REAL_ESTATE_WRAPPERS': ('XLRE','VNQ','IYR'),
    'SHORT_TREASURY_WRAPPERS': ('BIL','SGOV','SHV'),
}
GROUP_LABELS = {
    'EQUITY_US':'US equity research basket',
    'EQUITY_INTL':'International equity research basket',
    'SECTORS':'Sector research basket',
    'TREASURIES':'Treasury-fund research basket',
    'CREDIT':'Mixed bond research basket (legacy CREDIT group)',
    'TIPS':'Inflation-linked fund research basket',
    'CRYPTO':'Digital-asset fund research basket',
    'COMMODITIES':'Commodity and resource-equity research basket',
    'THEMATIC':'Thematic research basket',
    'DIVIDEND_VALUE':'Dividend/value research basket',
    'GROWTH':'Growth research basket',
    'VOL':'Volatility-product research basket',
    'GOLD_WRAPPERS':'Gold and gold-miner research basket',
    'SILVER_WRAPPERS':'Silver research basket',
    'REAL_ESTATE_WRAPPERS':'Real-estate research basket',
    'SHORT_TREASURY_WRAPPERS':'Short Treasury research basket',
}
CONFIGURED_GROUP_SCOPE = (
    'Preserved configured research baskets, not a verified issuer taxonomy or '
    'complete asset classes. Membership overlaps and mixed mandates remain explicit. '
    'Their subtotals must not be added together as independent market flows.'
)
