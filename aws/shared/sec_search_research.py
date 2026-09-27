"""SEC keyword-search candidates do not qualify investment decisions.

Keep the original research packet and legacy source intact. This boundary only
withholds its unverified keyword counts from downstream investment scores.
No output-supplied true flag can qualify a model that has not been validated.
"""
KEY = 'data/sec-filings-intel.json'
PERMISSIONS = {'calls_eligible': False, 'forecast_qualified': False,
               'sizing_eligible': False, 'execution_eligible': False}


def covered(key):
    return key in (KEY, 'sec-filings-intel')


def guard(key, packet):
    return {} if covered(key) else packet


def context():
    return {'source_key': KEY, 'qualification': 'keyword_candidates_only',
            'independent_investment_votes': 0,
            'reason': 'Keyword search does not verify issuer events, ownership concentration or predictive edge.',
            **PERMISSIONS}
