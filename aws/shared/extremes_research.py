"""Cycle research cannot authorize rankings, alerts, allocations or calibration."""
from datetime import datetime,timezone
import hashlib,json,re
CONTRACT='extremes-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
def context(packet,at=None):
    absent={'available':False,'reason':'verified_current_extremes_research_unavailable',**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT or packet.get('engine') not in ('capitulation','market-extremes'):return absent
        if any(packet.get(k) is not False for k in PERMISSIONS) or any(packet.get(k) is not None for k in ('capitulation_score','signal','call','posture','cycle_position')):return absent
        at=at or datetime.now(timezone.utc);generated=datetime.fromisoformat(packet['generated_at']);due=datetime.fromisoformat(packet['freshness']['pipeline_check_due_at'])
        if generated.tzinfo is None or due.tzinfo is None or not generated<=at<due or (due-generated).total_seconds()>26*3600:return absent
        ref=packet['replay'];body={k:v for k,v in packet.items() if k!='replay'}
        if not re.fullmatch(r'data/extremes-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']) or hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref['output_sha256']:return absent
        rows=[r for r in packet['measurements'] if datetime.fromisoformat(r['valid_until'])>at]
        return {**absent,'available':bool(rows),'reason':'dated_research_only','generated_at':packet['generated_at'],'measurements':rows,'replay':ref,
            'independent_investment_votes':0,'note':'No qualified market-turn forecast or portfolio recommendation; WAIT means abstention.'}
    except (KeyError,ValueError,TypeError,ArithmeticError):return absent
def qualified_score(packet):return None
def decision_view(packet):
    return {'research_context':context(packet),'capitulation_score':None,'signal':None,'call':None,'action':None,'stabilising':None,'smart_money_confirm':None,
        'posture':None,'cycle_posture':None,'cycle_position':None,'scores':{},'candidates':[],'signals':[],'items':[],'shopping_list':[],
        'portfolio_action':'WAIT',**PERMISSIONS}
def guard(key,packet):return decision_view(packet) if key in ('data/capitulation.json','data/market-extremes.json') else packet
