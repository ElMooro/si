"""Descriptive FINRA evidence has no qualified vote, ownership or sizing authority."""
from copy import deepcopy
from datetime import datetime,timezone
import hashlib,json,re
CURRENT='data/dark-pool.json'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
NOTE=('FINRA reported executions do not establish beneficial-owner accumulation, buying pressure or a qualified return forecast. '
      'Daily, weekly and monthly views can overlap. Missing or legacy data abstains; no direct-source fallback restores a vote.')

def context(packet):
    p=packet if isinstance(packet,dict) else {};ref=p.get('replay');available=False
    try:
        stamp=datetime.fromisoformat(p['generated_at'].replace('Z','+00:00'))
        available=(p.get('contract')=='offexchange-original-research.v1' and all(p.get(k) is False for k in FLAGS)
            and all(p.get(k) is None for k in ('call','signal','score','state')) and p.get('board')==[]
            and p.get('decision',{}).get('abstain') is True and p.get('decision',{}).get('eligible_votes')==0
            and isinstance(ref,dict) and set(ref)=={'manifest_key','output_sha256'}
            and bool(re.fullmatch(r'data/offexchange-research/runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))))
            and bool(re.fullmatch('[a-f0-9]{64}',str(ref.get('output_sha256',''))))
            and hashlib.sha256(json.dumps({k:v for k,v in p.items() if k!='replay'},sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()==ref['output_sha256']
            and stamp.tzinfo is not None and stamp<=datetime.now(timezone.utc))
    except (KeyError,ValueError,TypeError,AttributeError,OverflowError):available=False
    return {'contract':'offexchange-context.v1','native_reference_available':bool(available),
        'status':'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
        'canonical':{'key':CURRENT,'replay':deepcopy(ref)} if available else None,
        'generated_at':p.get('generated_at') if available else None,'output_digest_checked':bool(available),
        'original_provider_replay_performed_by_consumer':False,'current_freshness_verified_by_consumer':False,
        'call':None,'score':None,'state':None,'independent_investment_votes':0,'portfolio_action':'WAIT','note':NOTE,**dict.fromkeys(FLAGS,False)}

def decision_view(packet):
    return {'research_context':context(packet),'call':None,'score':None,'state':None,'signal':None,
        'board':[],'top_picks':[],'top_accumulation':[],'top_distribution':[],'high_conviction':[],
        'dark_map':{},'dark_share_map':{},'xray_map':{},'distribution':{},'dix':{'own_dix_pct':None},
        'tickets':[],'independent_investment_votes':0,**dict.fromkeys(FLAGS,False)}

def guard(key,packet):return decision_view(packet) if key in (CURRENT,'dark-pool.json') else packet
