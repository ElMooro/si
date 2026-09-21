"""Dollar observations cannot supply a forecast, allocation tilt or alert score."""
from copy import deepcopy
from datetime import datetime,timezone
import hashlib,json,re
CURRENT='data/dollar-radar.json'
FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')
NOTE=('Recorded dollar indices and dated currency quotes are descriptive. Fed broad indices, ICE DXY and Bloomberg Dollar Spot '
      'have different definitions. These inputs do not establish a dollar squeeze, liquidity flood, asset-return forecast or allocation tilt.')


def context(packet):
    p=packet if isinstance(packet,dict) else {};ref=p.get('replay');available=False
    try:
        stamp=datetime.fromisoformat(p['generated_at'].replace('Z','+00:00'))
        available=(p.get('contract')=='dollar-original-research.v1' and all(p.get(k) is False for k in FLAGS)
            and all(p.get(k) is None for k in ('call','score','regime','dollar_pressure'))
            and isinstance(ref,dict) and set(ref)=={'manifest_key','output_sha256'}
            and isinstance(ref.get('manifest_key'),str) and bool(re.fullmatch(r'data/dollar-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']))
            and isinstance(ref.get('output_sha256'),str) and bool(re.fullmatch('[a-f0-9]{64}',ref['output_sha256']))
            and hashlib.sha256(json.dumps({k:v for k,v in p.items() if k!='replay'},sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()==ref['output_sha256']
            and stamp.tzinfo is not None and stamp<=datetime.now(timezone.utc))
    except (KeyError,ValueError,TypeError,AttributeError,OverflowError):available=False
    return {'contract':'dollar-original-context.v1','native_reference_available':bool(available),
        'status':'descriptive_native_reference' if available else 'unqualified_legacy_or_unavailable',
        'canonical':{'key':CURRENT,'replay':deepcopy(ref)} if available else None,
        'generated_at':p.get('generated_at') if available else None,'current_freshness_verified_by_consumer':False,
        'output_digest_checked':bool(available),'original_provider_replay_performed_by_consumer':False,
        'call':None,'score':None,'regime':None,'dollar_pressure':None,'portfolio_action':'WAIT',
        'independent_investment_votes':0,**dict.fromkeys(FLAGS,False),'note':NOTE}
def qualified_score(packet):return None
def decision_view(packet):
    return {'research_context':context(packet),'regime':None,'stance':None,'score':None,'composite':None,'dollar_pressure':None,
        'risk_transmission':{'score':None,'verdict':None},'canaries':[],'canaries_pump':None,'canaries_dump':None,
        'bbdxy':{},'technicals':{},'headline':None,'signal':None,'call':None,'portfolio_action':'WAIT',
        'independent_investment_votes':0,**dict.fromkeys(FLAGS,False)}
def guard(key,packet):return decision_view(packet) if key in (CURRENT,'data/dollar.json') else packet
