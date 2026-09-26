"""No allocator forecast or personal mandate is qualified by a heuristic score."""
from copy import deepcopy

CURRENT='data/master-allocation.json'
CONTRACT='master-allocation-research.v1'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','alert_eligible')
REASON=('No independently qualified allocation model and reconciled portfolio mandate are registered. '
        'WAIT means no new model-authorized target; it is not an instruction to liquidate existing holdings.')


def context(packet):
    p=packet if isinstance(packet,dict) else {}
    stamp=p.get('generated_at',p.get('as_of'))
    return {'source_key':CURRENT,'status':'ABSTAIN','reported_generated_at':stamp if isinstance(stamp,str) else None,
        'original_source_replay_performed_by_consumer':False,'independent_investment_votes':0,
        'reason':REASON,**dict.fromkeys(FLAGS,False)}


def decision_view(packet):
    c=context(packet)
    return {'contract':CONTRACT,'as_of':c['reported_generated_at'],'generated_at':c['reported_generated_at'],
        'posture':'WAIT','call':'WAIT','confidence':None,'target_allocation':None,
        'deltas_from_benchmark':None,'summary':None,'contributions':{},'signals_used':{},
        'active_risk_bps':None,'risk_budget_clipped':None,
        'best_asset':{'winner':None,'top3':[],'ranked':[],'risk_override':{'active':False,'reasons':[]}},
        'research_context':c,'rationale':REASON,'qualification_status':'UNQUALIFIED',**dict.fromkeys(FLAGS,False)}


def project(report):
    if not isinstance(report,dict) or not isinstance(report.get('as_of'),str):
        raise ValueError('Complete dated allocator research report required')
    original=deepcopy(report)
    view=decision_view(report)
    view.update(unqualified_projection=original,
        methodology='Retained heuristic allocation and reported closing-price momentum; no qualified forecast, total-return benchmark, covariance risk or personal portfolio target.',
        original_source_replay_verified=False,portfolio_mandate_verified=False,
        historical_calibration_verified=False,held_positions_action='NO_INSTRUCTION')
    return view


def execution_packet(report):
    view=decision_view(report)
    return {'contract':CONTRACT,'as_of':view['as_of'],'target':None,'posture':'WAIT','confidence':None,
        'held_positions_action':'NO_INSTRUCTION','reason':REASON,**dict.fromkeys(FLAGS,False)}


def guard(key,packet):return decision_view(packet) if key==CURRENT else packet
