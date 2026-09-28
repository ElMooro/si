"""Current sampled option bars do not qualify an investment vote.

The full native packet remains the research source. No legacy rank row, forged
permission flag or fallback alias bypasses this boundary. Other families are
unchanged and independently unreviewed.
"""
KEY='data/options-flow-scanner.json'
CONTRACT='options-flow-observations.v1'


def guard(key,packet):
    return {} if key==KEY else packet


def context(packet):
    return {'source_key':KEY,'research_page':'/options-scanner.html',
        'reason':'sampled_option_bars_and_finra_flows_have_no_directional_qualification',
        'native_contract_present':isinstance(packet,dict) and packet.get('measurement_contract')==CONTRACT,
        'independent_investment_votes':0,'forecast_qualified':False,'calls_eligible':False,
        'sizing_eligible':False,'execution_eligible':False,'other_inputs_qualified':False}
