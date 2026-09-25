"""Gold/Equity observations cannot become unqualified votes, sizes or narratives."""
from copy import deepcopy
from datetime import datetime,timezone
import hashlib,json,re
CURRENT='data/gold-equity-rotation.json'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
NOTE=('Recorded fund-share prices and provider dividend-adjusted returns are dated descriptive evidence. '
      'UUP is a fund proxy, volume is not AUM or investor cash flow, and price leadership supplies no independently qualified allocation instruction.')

def context(packet):
    p=packet if isinstance(packet,dict) else {};ref=p.get('replay');available=False
    try:
        stamp=datetime.fromisoformat(p['generated_at'].replace('Z','+00:00'))
        available=(p.get('contract')=='gold-rotation-original-research.v1' and all(p.get(k) is False for k in FLAGS)
            and p.get('call') is None and p.get('state') is None and p.get('signal_strength') is None
            and p.get('trade_tickets')==[] and p.get('independent_investment_votes')==0
            and isinstance(ref,dict) and set(ref)=={'manifest_key','output_sha256'}
            and isinstance(ref.get('manifest_key'),str) and bool(re.fullmatch(r'data/gold-rotation-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']))
            and isinstance(ref.get('output_sha256'),str) and bool(re.fullmatch('[a-f0-9]{64}',ref['output_sha256']))
            and hashlib.sha256(json.dumps({k:v for k,v in p.items() if k!='replay'},sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()==ref['output_sha256']
            and stamp.tzinfo is not None and stamp<=datetime.now(timezone.utc))
    except (KeyError,ValueError,TypeError,AttributeError,OverflowError):available=False
    return {'contract':'gold-rotation-context.v1','native_reference_available':bool(available),
        'status':'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
        'canonical':{'key':CURRENT,'replay':deepcopy(ref)} if available else None,
        'generated_at':p.get('generated_at') if available else None,'output_digest_checked':bool(available),
        'original_provider_replay_performed_by_consumer':False,'current_freshness_verified_by_consumer':False,
        'call':None,'score':None,'state':None,'signal_strength':None,'independent_investment_votes':0,
        'trade_tickets':[],'portfolio_action':'WAIT','note':NOTE,**dict.fromkeys(FLAGS,False)}
def decision_view(packet):return {'research_context':context(packet),'state':None,'signal_strength':None,'current_metrics':{},
    'trade_tickets':[],'call':None,'score':None,'independent_investment_votes':0,**dict.fromkeys(FLAGS,False)}
