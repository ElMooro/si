"""Accounting measurements retain lineage but never become investment votes."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib, json, re

CURRENT='data/forensic-screen.json'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
NOTE=('Normalized provider statements support dated descriptive calculations only. '
      'Provider fields and reconciliations can share a derivation. No fraud finding, '
      'financial-strength grade, directional return, independent vote or size is qualified. '
      'Original SEC filings, historical availability and statement durations remain unverified.')


def context(packet):
    p=packet if isinstance(packet,dict) else {};ref=p.get('replay');available=False
    try:
        stamp=datetime.fromisoformat(p['generated_at'].replace('Z','+00:00'))
        available=(p.get('contract')=='financial-statement-original-research.v2'
            and all(p.get(k) is False for k in FLAGS)
            and all(p.get(k) is None for k in ('call','score','grade','m_score'))
            and p.get('independent_investment_votes')==0
            and p.get('quality',{}).get('original_source_bytes_replayed') is True
            and p.get('quality',{}).get('original_sec_filings_replayed') is False
            and p.get('quality',{}).get('accounting_audit') is False
            and p.get('quality',{}).get('current_sec_index_replayed') is True
            and p.get('quality',{}).get('issuer_conflicts_withheld') is True
            and p.get('quality',{}).get('historical_security_continuity_verified') is False
            and isinstance(p.get('issuers'),list) and len(p['issuers'])==p.get('reported_names')
            and all(isinstance(v,dict) and re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}',str(v.get('symbol',''))) for v in p['issuers'])
            and len({v['symbol'] for v in p['issuers']})==len(p['issuers'])
            and isinstance(ref,dict) and set(ref)=={'manifest_key','output_sha256'}
            and bool(re.fullmatch(r'data/statement-research/runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))))
            and bool(re.fullmatch('[a-f0-9]{64}',str(ref.get('output_sha256',''))))
            and hashlib.sha256(json.dumps({k:v for k,v in p.items() if k!='replay'},sort_keys=True,
                separators=(',',':'),allow_nan=False).encode()).hexdigest()==ref['output_sha256']
            and stamp.tzinfo is not None and stamp<=datetime.now(timezone.utc))
    except (KeyError,ValueError,TypeError,AttributeError,OverflowError):
        available=False
    return {'contract':'financial-statement-context.v2','native_reference_available':bool(available),
        'status':'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
        'canonical':{'key':CURRENT,'replay':deepcopy(ref)} if available else None,
        'generated_at':p.get('generated_at') if available else None,
        'reported_names':p.get('reported_names') if available else None,
        'output_digest_checked':bool(available),'original_provider_replay_performed_by_consumer':False,
        'current_freshness_verified_by_consumer':False,'call':None,'score':None,'grade':None,
        'm_score':None,'independent_investment_votes':0,'portfolio_action':'WAIT','note':NOTE,**dict.fromkeys(FLAGS,False)}


def decision_view(packet):
    return {'research_context':context(packet),'all_results':[],'rows':[],'stocks':[],
        'most_concerning_top_25':[],'cleanest_top_25':[],'fortress_financials':[],'problem_financials':[],
        'sector_valuation_medians':{},'sector_strength_medians':{},'by_ticker':{},'tickers':{},
        'call':None,'score':None,'grade':None,'m_score':None,'independent_investment_votes':0,**dict.fromkeys(FLAGS,False)}


def reported_universe(packet):
    """Identity labels only, explicitly separate from the decision view.

Sector labels retain the original universe row and source clock. They remain
provider classifications, never inferred from CIK or reused as quality scores.
"""
    if not context(packet)['native_reference_available']:return []
    return [{'symbol':v['symbol'],'sector':v.get('universe_record',{}).get('reported_classification',{}).get('sector'),
        'sector_status':'retained_universe_classification_unverified',
        'classification_source':deepcopy(v.get('universe_record')),
        'current_sec_ciks':deepcopy(v.get('current_sec_ciks',[])),
        'historical_security_continuity_verified':False,
        'reported_issuer_roots':deepcopy(v.get('reported_issuer_roots',[])),
        'source_key':CURRENT,'source_generated_at':packet['generated_at'],
        'index_membership_verified':False,'independent_investment_votes':0} for v in packet['issuers']]


def guard(key,packet):
    return decision_view(packet) if key in (CURRENT,'forensic-screen.json') else packet
