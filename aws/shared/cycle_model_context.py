"""Synthetic business-cycle/recession models have no qualified decision vote.

Keep the original datasets in their canonical research pages. This consumer
projection records parsed-input identity; it does not attest provider originals,
calibration, independence, or portfolio eligibility.
"""
from datetime import datetime, timezone
import hashlib
import json

BUSINESS='data/global-business-cycle.json'
RECESSION='data/global-recession.json'
CONTRACTS={BUSINESS:'global-business-cycle-research.v1',RECESSION:'global-recession-research.v1'}
FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')
NOTE='Synthetic cycle classifications and inherited recession transforms are unqualified research; no probability, risk vote, asset tilt or position size is established.'


def context(key,packet,at=None):
    if key not in CONTRACTS:raise ValueError('Unreviewed cycle context')
    p=packet if isinstance(packet,dict) else {}
    digest=None
    try:
        digest=hashlib.sha256(json.dumps(p,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()
    except (ValueError,TypeError,OverflowError):pass
    generated=p.get('generated_at');publication_current=False
    try:
        stamp=datetime.fromisoformat(generated.replace('Z','+00:00'))
        now=at or datetime.now(timezone.utc)
        publication_current=stamp.tzinfo is not None and now.tzinfo is not None and 0 <= (now-stamp).total_seconds() <= 26*3600
    except (AttributeError,TypeError,ValueError,OverflowError):pass
    declared=p.get('contract')==CONTRACTS[key] and all(p.get(flag) is False for flag in FLAGS)
    return {'contract':'synthetic-cycle-consumer-context.v1','source_key':key,
            'source_generated_at':generated if isinstance(generated,str) else None,
            'parsed_input_sha256':digest,'identity_basis':'Canonical complete parsed JSON; not original stored bytes.',
            'status':'declared_unqualified_research' if declared else 'unqualified_legacy_or_unavailable',
            'publication_within_26h':publication_current,'observation_freshness_verified':False,
            'original_source_replay_performed_by_consumer':False,'qualified_investment_votes':0,
            'portfolio_action':'WAIT','meaning':'abstain',**dict.fromkeys(FLAGS,False),'note':NOTE}


class _DecisionView(dict):
    """In-process idempotence; serialized self-asserted metadata is never trusted."""


def decision_view(key,packet):
    if key not in CONTRACTS:raise ValueError('Unreviewed cycle context')
    ctx=(dict(packet['research_context']) if isinstance(packet,_DecisionView)
         and packet['research_context']['source_key']==key else context(key,packet))
    return _DecisionView({'research_context':ctx,'generated_at':ctx['source_generated_at'],
            'aggregate':{},'by_country':{},'interpretation':{},'countries':[],
            'phase':None,'cycle_phase':None,'regime':None,'score':None,'global_recession_prob_pct':None,
            'downturn_probability_6m':None,'call':None,'portfolio_action':'WAIT',**dict.fromkeys(FLAGS,False)})


def guard(key,packet):
    return decision_view(key,packet) if key in CONTRACTS else packet
