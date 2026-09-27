"""Shipping research cannot establish an industry, pricing or investment vote.

The full public packet remains at its canonical source. This boundary records
the identity of every parsed input field without claiming original-byte replay.
Self-declared eligibility cannot grant model authority.
"""
from datetime import datetime, timezone
import hashlib,json

SOURCE='data/portwatch.json'
FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')
NOTE=('Vessel counts are not cargo tons, industry revenues or national exports. '
      'A calendar count comparison does not establish pricing power, unpriced demand, '
      'an industry exposure weight or a portfolio action. Shipping contributes no investment vote.')


def context(packet,at=None):
    p=packet if isinstance(packet,dict) else {};digest=None
    try:
        digest=hashlib.sha256(json.dumps(packet,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()
    except (ValueError,TypeError,OverflowError):pass
    generated=p.get('generated_at');current=False
    try:
        stamp=datetime.fromisoformat(generated.replace('Z','+00:00'));now=at or datetime.now(timezone.utc)
        current=stamp.tzinfo is not None and now.tzinfo is not None and 0<=(now-stamp).total_seconds()<=26*3600
    except (ValueError,TypeError,AttributeError,OverflowError):pass
    declared=(p.get('contract')=='portwatch-preserved-calculation.v1'
              and all(p.get(k) is False for k in FLAGS) and p.get('portfolio_action')=='WAIT')
    return {'contract':'shipping-consumer-context.v1','source_key':SOURCE,
        'source_generated_at':generated if isinstance(generated,str) else None,
        'parsed_input_sha256':digest,'identity_basis':'Complete canonical parsed JSON; not original stored bytes.',
        'source_object_valid':isinstance(packet,dict),
        'status':'declared_unqualified_research' if declared else 'unqualified_legacy_or_unavailable',
        'publication_within_26h':current,'observation_freshness_verified':False,
        'original_source_replay_performed_by_consumer':False,'qualified_investment_votes':0,
        'portfolio_action':'WAIT','meaning':'abstain',**dict.fromkeys(FLAGS,False),'note':NOTE}


class _DecisionView(dict):
    pass


def decision_view(packet):
    ctx=dict(packet['research_context']) if isinstance(packet,_DecisionView) else context(packet)
    return _DecisionView({'research_context':ctx,'generated_at':ctx['source_generated_at'],
        'ports':[],'chokepoints':[],'exporters':[],'exporters_slowing':[],'industry_exposure_summary':{},
        'worst':None,'n_disrupted':None,'ports_disrupted':None,'call':None,
        'portfolio_action':'WAIT',**dict.fromkeys(FLAGS,False)})
