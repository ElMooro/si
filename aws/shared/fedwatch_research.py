"""Dated federal-funds futures context cannot become an unreviewed policy vote."""
from datetime import datetime,timedelta,timezone
import hashlib,json,re
CONTRACT='fedwatch-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
def context(packet,at=None):
    absent={'available':False,'reason':'verified_futures_research_unavailable','dated_contracts':0,**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT or any(packet.get(k) is not False for k in PERMISSIONS):return absent
        if any(packet.get(k) is not None for k in ('call','score','regime')) or packet.get('portfolio_action')!='WAIT':return absent
        def clock(s):
            d=datetime.fromisoformat(s.replace('Z','+00:00'))
            if d.tzinfo is None:raise ValueError('Aware clock required')
            return d
        at=at or datetime.now(timezone.utc);started=clock(packet['collection']['started_at']);generated=clock(packet['generated_at'])
        due=clock(packet['freshness']['capture_check_due_at']);source=clock(packet['source_generated_at'])
        if not source<=started<=generated<=at<due or due!=started+timedelta(hours=26) or generated-started>timedelta(seconds=150):return absent
        if clock(packet['freshness']['canonical_check_due_at'])!=source+timedelta(hours=26) or at>=source+timedelta(hours=26):return absent
        ref=packet['replay'];body={k:v for k,v in packet.items() if k!='replay'}
        if not re.fullmatch(r'data/fedwatch-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        if hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref['output_sha256']:return absent
        quotes=packet['contracts']
        if len(quotes)!=12 or len({q['symbol'] for q in quotes})!=12:return absent
        n=0
        for q in quotes:
            if not re.fullmatch(r'ZQ[FGHJKMNQUVXZ][0-9]{2}\.CBT',q['symbol']) or any(q.get(k) is not False for k in PERMISSIONS):return absent
            if q.get('meeting_probabilities') is not None or q.get('official_settlement_verified') is not False:return absent
            if q.get('available') is True and at<clock(q['quote_valid_until']):n+=1
        if packet['next_6mo_summary']['scenario'] is not None or any(m['probabilities_pct'] is not None for m in packet['meetings_ahead']):return absent
        return {**absent,'available':n>0,'reason':'dated_provider_contract_context_only','dated_contracts':n,
            'generated_at':packet['generated_at'],'source_valid_until':min(due,source+timedelta(hours=26)).isoformat(),'replay':ref,
            'note':'Contract observations only; no official settlement, synchronized curve, meeting probability, rate-timing or investment vote.'}
    except (KeyError,ValueError,TypeError,AttributeError,ArithmeticError):return absent
def qualified_score(packet):return None
def decision_view(packet):
    return {'research_context':context(packet),'generated_at':packet.get('generated_at') if isinstance(packet,dict) else None,
        'current_fed_funds_range':{'lower':None,'upper':None,'midpoint':None,'as_of':None},'next_meeting':None,
        'meetings_ahead':[],'next_6mo_summary':{'scenario':None,'cumulative_implied_move_bps':None,'n_reliable_meetings':0},
        'n_meetings_with_data':0,'score':None,'regime':None,'call':None,'portfolio_action':'WAIT',**PERMISSIONS}
def guard(key,packet):return decision_view(packet) if key=='data/fedwatch.json' else packet
