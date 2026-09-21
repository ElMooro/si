"""Cycle measurements may inform research, never an unqualified portfolio vote."""
from datetime import datetime,timedelta,timezone
import hashlib,json,re
CONTRACT='cycle-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}

def context(packet,at=None):
    absent={'available':False,'reason':'verified_cycle_research_unavailable',**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT or any(packet.get(k) is not False for k in PERMISSIONS):return absent
        if any(packet.get(k) is not None for k in ('call','score','regime','verdict')) or packet.get('portfolio_action')!='WAIT':return absent
        if packet['synthesis']['score'] is not None or packet['synthesis']['posture']!='WAIT':return absent
        if any(packet['cycle'].get(k) is not None for k in ('phase','headline_phase','quadrant','recession_prob_pct','asset_leadership')):return absent
        def clock(s):
            d=datetime.fromisoformat(s.replace('Z','+00:00'))
            if d.tzinfo is None:raise ValueError('Aware clock required')
            return d
        at=at or datetime.now(timezone.utc);source=clock(packet['source_generated_at']);generated=clock(packet['generated_at']);due=clock(packet['source_valid_until'])
        if not source<=generated<=at<due<=source+timedelta(hours=26):return absent
        ref=packet['replay'];body={k:v for k,v in packet.items() if k!='replay'}
        if not re.fullmatch(r'data/cycle-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        if hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref['output_sha256']:return absent
        if packet['dependency_graph']['independent_investment_votes']!=0 or packet['track_record']['hit_rate'] is not None:return absent
        return {**absent,'available':True,'reason':'dated_cycle_measurements_only','generated_at':packet['generated_at'],
            'source_valid_until':packet['source_valid_until'],'replay':ref,'retained_input_count':packet['dependency_graph']['retained_inputs'],
            'note':'Original-source descriptive research; no qualified investment direction or position size.'}
    except (KeyError,ValueError,TypeError,AttributeError,ArithmeticError):return absent

def decision_view(packet):
    return {'research_context':context(packet),'cycle':{'phase':None,'headline_phase':None,'quadrant':None,'recession_prob_pct':None},
        'synthesis':{'posture':'WAIT','score':None},'verdict':None,'regime':None,'phase':None,'call':None,'score':None,
        'portfolio_action':'WAIT',**PERMISSIONS}

def qualified_score(packet):return None
def guard(key,packet):return decision_view(packet) if key=='data/cycle-clock.json' else packet
