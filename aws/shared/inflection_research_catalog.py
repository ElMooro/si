"""Every FRED identity requested by the legacy inflection engine, without substitution."""
SERIES = {
    'WALCL': ('balance_sheet', 'Federal Reserve total assets'),
    'WTREGEN': ('balance_sheet', 'Treasury general account, weekly average'),
    'RRPONTSYD': ('balance_sheet', 'Overnight reverse repurchase agreements'),
    'WRESBAL': ('balance_sheet', 'Reserve balances'),
    'HQMCB10YR': ('credit', '10-year HQM corporate spot yield'),
    'DGS10': ('rates', '10-year Treasury constant maturity yield'),
    'DGS30': ('rates', '30-year Treasury constant maturity yield'),
    'M2SL': ('money', 'M2 money stock'),
    'CPIAUCSL': ('prices', 'Consumer price index'),
    'SOFR': ('funding', 'Secured overnight financing rate'),
    'IORB': ('funding', 'Interest on reserve balances'),
    'DTWEXBGS': ('currency', 'Nominal broad US dollar index'),
    'BAMLH0A0HYM2': ('credit', 'High yield option-adjusted spread'),
    'NFCI': ('conditions', 'National financial conditions index'),
    'SWPT': ('facilities', 'Central bank liquidity swaps'),
    'WLCFLPCL': ('facilities', 'Primary credit'),
    'RPONTSYD': ('facilities', 'Overnight repurchase agreements'),
    'CRDQCNAPABIS': ('china', 'Legacy China credit identity A'),
    'QCNPAM770A': ('china', 'Legacy China credit identity B'),
    'CRDQCNBPABIS': ('china', 'Legacy China credit identity C'),
}
ARCHIVES = ('WALCL', 'WTREGEN', 'RRPONTSYD')
AUXILIARIES = (
    'ecb-hist/excess_liquidity', 'ecb-hist/excess-liquidity', 'stablecoin-flow',
    'auction-crisis', 'spx-history-deep', 'funding-plumbing', 'eurodollar-stress',
    'eurodollar-plumbing', 'move-index', 'global-liquidity', 'credit-stress',
    'global-stress', 'china-liquidity', 'settlement-fails', 'capital-flow',
    'repo-lending', 'dealer-survey', 'ofr-stfm',
)


def extend_catalog(catalog):
    requested = {sid: {'category': cat, 'display_name': label} for sid, (cat, label) in SERIES.items()}
    return {**requested, **catalog}
