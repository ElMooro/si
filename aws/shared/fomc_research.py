"""Historical event-study context supplies no unqualified policy or regime vote."""
from datetime import datetime,timedelta,timezone
import hashlib,json,re
CONTRACT='fomc-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
def context(packet,at=None):
    absent={'available':False,'reason':'verified_event_history_unavailable',**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT or any(packet.get(k) is not False for k in PERMISSIONS):return absent
        if any(packet.get(k) is not None for k in ('call','score','regime','regime_context')) or packet.get('portfolio_action')!='WAIT':return absent
        def clock(s):
            d=datetime.fromisoformat(s.replace('Z','+00:00'))
            if d.tzinfo is None:raise ValueError('Aware clock required')
            return d
        at=at or datetime.now(timezone.utc);generated=clock(packet['generated_at']);source=clock(packet['source_generated_at']);calendar=clock(packet['calendar']['source_received_at'])
        due=min(source,calendar)+timedelta(hours=26)
        if not max(source,calendar)<=generated<=at<due or clock(packet['freshness']['pipeline_check_due_at'])!=due:return absent
        ref=packet['replay'];body={k:v for k,v in packet.items() if k!='replay'}
        if not re.fullmatch(r'data/fomc-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        if hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref['output_sha256']:return absent
        if packet['surprise']['label'] is not None or packet['reaction_map'] or packet['calibration']['forecast_calibrated'] is not False:return absent
        if packet['self_grading']['directional_accuracy_pct'] is not None or any(s['forecast_probability'] is not None or any(s.get(k) is not False for k in PERMISSIONS) for s in packet['summaries']):return absent
        return {**absent,'available':True,'reason':'historical_event_associations_only','generated_at':packet['generated_at'],
            'historical_scheduled_events':len(packet['events']),'source_valid_until':due.isoformat(),'replay':ref,
            'note':'Exact dated observations and retrospective samples; no identified policy shock, independent vote or calibrated forecast.'}
    except (KeyError,ValueError,TypeError,AttributeError,ArithmeticError):return absent
def decision_view(packet):
    return {'research_context':context(packet),'regime_context':None,'surprise':{'label':None,'basis':'unqualified'},
        'reaction_map':{},'call':None,'score':None,'regime':None,'portfolio_action':'WAIT',**PERMISSIONS}
def guard(key,packet):return decision_view(packet) if key=='data/fomc-reaction.json' else packet
