"""Portfolio research inputs from a verified sector publication, without prescribed weights."""
from copy import deepcopy
import sector_research_model as sectors
CONTRACT='sector-tilt-native.v1';PREFIX='data/sector-tilt-research/'
CURRENT='data/sector-tilt.json';PRIVATE=sectors.PRIVATE
PERMISSIONS=sectors.PERMISSIONS;clock=sectors.clock;sha=sectors.sha;encoded=sectors.encoded

def build(packet,generated_at,legacy,refs):
    if packet.get('contract')!=sectors.CONTRACT or any(packet.get(k) is not False for k in PERMISSIONS):raise ValueError('Native unqualified sector research required')
    if clock(packet['generated_at'])>clock(generated_at):raise ValueError('Future sector publication')
    # Preserve output/input independence without copying unused source histories
    # and expanded relative-performance trails a second time during replay.
    source={key:deepcopy(packet[key]) for key in ('sectors','risk_sample','quality')};tilts=[]
    for row in source['sectors']:
        tilts.append({'ticker':row['symbol'],'symbol':row['symbol'],'name':row['name'],'reference_date':row['reference_date'],
            'price':row['price'],'comparisons':row['comparisons'],'regime_tilt_score':None,'regime_tilt_label':'UNAVAILABLE',
            'alignment':None,'current_state':None,'implication':'WAIT','urgency':None,'call':None,
            'rationale':'Dated sector price and risk research. No validated macro-to-sector allocation or user portfolio mandate.',**PERMISSIONS})
    risk=source['risk_sample']
    scenario={'status':risk['status'],'symbols':risk.get('symbols',[]),'covariance':risk.get('covariance'),
        'covariance_unit':risk.get('covariance_unit'),'return_rows':risk.get('return_rows',[]),
        'dates':risk.get('dates',[]),'annualization_assumption':risk.get('annualization_assumption'),
        'annualization_scope':risk.get('annualization_scope'),
        'definition':'Signed user exposures only. The retained past price sample supports hypothetical P&L and descriptive covariance under explicitly chosen exposure assumptions, not recommended weights or a forward risk bound.'}
    contexts=[{'source_key':key,'source_generated_at':(legacy.get(key) or {}).get('generated_at') or (legacy.get(key) or {}).get('as_of'),
        'retained':ref,'status':'retained_unqualified_context' if ref else 'missing'} for key,ref in sorted(refs.items())]
    return {'contract':CONTRACT,'generated_at':generated_at,'as_of':generated_at,'source_generated_at':packet['generated_at'],
        'source_valid_until':packet['source_valid_until'],'reference_date':packet['reference_date'],'sector_replay':packet['replay'],
        'tilts':tilts,'scenario_data':scenario,'source_quality':source['quality'],
        'quality':{'status':'source_within_age_ceiling' if clock(generated_at)<clock(packet['source_valid_until']) else 'source_expired',
            'independent_investment_votes':0,'portfolio_positions_supplied':False,'forecast_validation_established':False},
        'regime':None,'regime_raw':None,'regime_rationale':'Macro regime and sector-return edge remain unqualified.',
        'summary':{'n_overweight':0,'n_underweight':0,'n_neutral':0,'n_unavailable':len(tilts),'n_aligned':0,'n_misaligned':0,
            'top_buy_opportunities':[],'top_fade_opportunities':[],'top_confirmed_buys':[]},
        'retained_contexts':contexts,'call':None,'portfolio_action':'WAIT',**PERMISSIONS}
