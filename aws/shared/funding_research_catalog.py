"""Exact existing Eurodollar FRED identities and their reviewed measurement roles."""

SERIES = {
    'SOFR': ('us_core', 'Percent', 'D', 5, 'Secured overnight rate; volume-weighted median'),
    'SOFR99': ('us_core', 'Percent', 'D', 5, '99th percentile of the SOFR transaction distribution'),
    'IORB': ('us_core', 'Percent', 'D', 5, 'Administered interest rate on reserve balances'),
    'EFFR': ('us_core', 'Percent', 'D', 5, 'Effective overnight federal funds rate'),
    'OBFR': ('us_core', 'Percent', 'D', 5, 'Overnight bank funding rate; includes specified offshore dollar transactions'),
    'RRPONTSYD': ('us_core', 'Billions of US Dollars', 'D', 5, 'Overnight reverse-repo operation amount; not a measured remaining buffer'),
    'WRESBAL': ('us_core', 'Millions of U.S. Dollars', 'W', 14, 'Reported reserve balances; use the native weekly averaging convention'),
    'WTREGEN': ('us_core', 'Millions of U.S. Dollars', 'W', 14, 'Treasury General Account weekly average, not a Wednesday closing balance'),
    'DCPF3M': ('bank_funding', 'Percent', 'D', 5, 'Three-month financial commercial paper quote; not an overnight OIS quote'),
    'DCPN3M': ('bank_funding', 'Percent', 'D', 5, 'Three-month nonfinancial commercial paper quote; not an overnight OIS quote'),
    'DTB3': ('bank_funding', 'Percent', 'D', 5, 'Three-month Treasury bill secondary-market discount-basis quote'),
    'BAMLH0A0HYM2': ('credit', 'Percent', 'D', 5, 'US high-yield option-adjusted spread; broader credit context'),
    'BAMLC0A0CM': ('credit', 'Percent', 'D', 5, 'US corporate option-adjusted spread; broader credit context'),
    'SWPT': ('backstops', 'Millions of U.S. Dollars', 'W', 14, 'Central-bank liquidity swap asset stock; use does not alone prove a shortage'),
    'H41RESPPALGTRFNWW': ('backstops', 'Millions of U.S. Dollars', 'W', 14, 'Foreign-official repurchase-agreement assets at Wednesday'),
    'WORAL': ('backstops', 'Millions of U.S. Dollars', 'W', 14, 'Total repurchase-agreement assets at Wednesday; not an isolated SRF allotment'),
    'DTWEXBGS': ('fx', 'Index Jan 2006=100', 'D', 10, 'Broad nominal dollar index; daily observations may be released in weekly batches'),
    'WMTSECL1': ('fx', 'Millions of U.S. Dollars', 'W', 14, 'Custody holdings of marketable Treasuries; stock changes do not identify sales or motives'),
    'NDFACBW027SBOG': ('fx', 'Billions of U.S. Dollars', 'W', 17, 'Seasonally adjusted net due to related foreign offices; a stock, not measured cash flows'),
}


def extend_catalog(catalog):
    result = dict(catalog)
    for sid, (layer, unit, frequency, age, note) in SERIES.items():
        result.setdefault(sid, {'category': 'funding', 'display_name': sid})
    return result
