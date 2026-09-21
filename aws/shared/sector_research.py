"""Descriptive sector research does not supply allocation or signal votes."""
from datetime import datetime,timedelta,timezone
import hashlib,json,re
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
CONTRACTS={'sector-native-research.v1':'sector-research','sector-tilt-native.v1':'sector-tilt-research'}

def context(packet,at=None):
    absent={'available':False,'reason':'verified_sector_research_unavailable',**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract') not in CONTRACTS or any(packet.get(k) is not False for k in PERMISSIONS):return absent
        if packet.get('call') is not None or packet.get('portfolio_action')!='WAIT' or packet.get('quality',{}).get('independent_investment_votes')!=0:return absent
        def clock(s):
            stamp=datetime.fromisoformat(s.replace('Z','+00:00'))
            if stamp.tzinfo is None:raise ValueError('Aware clock required')
            return stamp
        at=at or datetime.now(timezone.utc);source=clock(packet['source_generated_at']);generated=clock(packet['generated_at']);due=clock(packet['source_valid_until'])
        if not source<=generated<=at<due<=source+timedelta(hours=26):return absent
        ref=packet['replay'];body={k:v for k,v in packet.items() if k!='replay'};prefix=CONTRACTS[packet['contract']]
        if not re.fullmatch('data/'+prefix+r'/runs/[a-f0-9]{64}\.json',ref.get('manifest_key','')):return absent
        if hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref.get('output_sha256'):return absent
        return {**absent,'available':True,'reason':'dated_sector_price_research_only','generated_at':packet['generated_at'],
            'reference_date':packet.get('reference_date'),'source_valid_until':packet['source_valid_until'],'replay':ref,
            'note':'Matched-date price performance and past-sample covariance are descriptive; no qualified regime, allocation, directional call or independent evidence vote.'}
    except (KeyError,ValueError,TypeError,AttributeError,ArithmeticError):return absent


def decision_view(packet):
    return {'research_context':context(packet),'sectors':[],'tilts':[],'rankings':[],'leaders':[],'ratios':[],
        'summary':{},'rotation_alerts':{'rotating_in':[],'rotating_out':[]},'risk_appetite':None,
        'macro_context':{'cycle_phase':None,'regime_label':None,'macro_stress_score':None},'market_breadth':None,
        'score':None,'regime':None,'call':None,'portfolio_action':'WAIT',**PERMISSIONS}

def guard(key,packet):return decision_view(packet) if key in ('data/sector-rotation.json','data/sector-tilt.json','data/sector-flow-state.json','data/sector-capital-fusion.json') else packet
