"""Existing CB Injection source identities, preserved without substitutions."""
SERIES = {
    'WSHOSHO': ('cb_components', 'Federal Reserve securities held outright'),
    'WLCFLPCL': ('cb_components', 'Federal Reserve primary credit'),
    'SWPT': ('cb_components', 'Central-bank liquidity swaps'),
    'ECBASSETSW': ('cb_assets', 'Eurosystem total assets'),
    'ECBDFR': ('policy_rates', 'ECB deposit facility rate'),
    'JPNASSETS': ('cb_assets', 'Bank of Japan total assets'),
    'IR3TIB01JPM156N': ('interbank_rates', 'Japan monthly three-month interbank rate'),
    'WALCL': ('cb_assets', 'Federal Reserve total assets'),
    'DFEDTARU': ('policy_rates', 'Federal funds target upper limit'),
    'IR3TIB01CHM156N': ('interbank_rates', 'Switzerland monthly three-month interbank rate'),
    'DEXJPUS': ('fx', 'Japanese yen per US dollar'),
    'DEXSZUS': ('fx', 'Swiss francs per US dollar'),
    'DEXUSEU': ('fx', 'US dollars per euro'),
}


def extend_catalog(catalog):
    requested = {sid: {'category': category, 'display_name': label}
                 for sid, (category, label) in SERIES.items()}
    return {**requested, **catalog}
