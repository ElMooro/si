"""Official measurements and legacy percentiles do not qualify portfolio forecasts."""


def qualified_for_signals(packet):
    # No independently qualified Crisis Plumbing forecasting policy is registered.
    # A packet's own flags or old confidence ladder cannot register one.
    return False


def legacy_signal(name):
    return isinstance(name,str) and (name.startswith('crisis_') or name in ('crisis-plumbing','crisis_plumbing'))


def eligible_weights(mapping):
    return {k:v for k,v in (mapping or {}).items() if not legacy_signal(k)}


def decision_view(packet):
    packet=packet if isinstance(packet,dict) else {}
    return {'contract':'plumbing-decision-view.v1','status':'RESEARCH_ONLY','phase':None,
        'generated_at':packet.get('generated_at'),'native_research_available':packet.get('contract')=='plumbing-research.v1',
        'composite':{'composite_stress_score':None,'consensus_count':None,'agreement_signal':'RESEARCH_ONLY'},
        'crisis_indices':{},'funding_credit_signals':{},'xcc_basis_proxy':{},'mmf_composition':None,
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'forecast_eligible':False,
        'reason':'Native conditions, credit and funding observations are descriptive. Legacy score buckets and rate differences have no qualified crisis or portfolio forecasting authority.'}


def guard(key,packet):
    return decision_view(packet) if key=='data/crisis-plumbing.json' else packet
