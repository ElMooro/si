"""Typed USD-context boundary. Research measurements never authorize a trade."""
from datetime import date,datetime,timezone,timedelta
from decimal import Decimal
import hashlib,json,math,re
CONTRACT='eurodollar-native-research.v1'
PERMISSIONS={'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
CATALOG={'STLFSI4':('Index','W'),'BAMLH0A0HYM2':('Percent','D'),'BAMLC0A0CM':('Percent','D'),
    'VIXCLS':('Index','D'),'DTWEXBGS':('Index Jan 2006=100','D'),'DTB3':('Percent','D'),
    'DGS10':('Percent','D'),'SOFR':('Percent','D'),'DFF':('Percent','D')}
def clock(value):
    out=datetime.fromisoformat(value.replace('Z','+00:00'))
    if out.tzinfo is None:raise ValueError('Aware clock required')
    return out.astimezone(timezone.utc)

def context(packet,at=None):
    absent={'available':False,'reason':'verified_current_usd_context_unavailable','measurements':{},'repo_comparison':None,**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT:return absent
        if any(packet.get(k) is not False for k in PERMISSIONS) or packet.get('call') is not None:return absent
        at=at or datetime.now(timezone.utc);generated=clock(packet['generated_at']);source=clock(packet['source_generated_at'])
        due=clock(packet['freshness']['pipeline_check_due_at'])
        if not source<=generated<=at<due or due!=source+timedelta(hours=26):return absent
        ref=packet['replay']
        if not re.fullmatch(r'data/eurodollar-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        body={k:v for k,v in packet.items() if k!='replay'}
        raw=json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest()!=ref['output_sha256']:return absent
        measurements={}
        for sid,m in packet['measurements'].items():
            if sid not in CATALOG or m.get('series_id')!=sid:return absent
            unit,freq=CATALOG[sid]
            if m['quality']['status']!='within_age_ceiling':continue
            if m['unit']!=unit or m['source_unit']!=unit or m['frequency']!=freq:return absent
            if m['source_url']!='https://fred.stlouisfed.org/series/'+sid or any(m.get(k) is not False for k in PERMISSIONS):return absent
            acquired=clock(m['acquired_at']);observed=date.fromisoformat(m['observation_date'])
            limit=21 if freq=='W' else 10
            if not 0<=(at.date()-observed).days<=limit or not 0<=(at-acquired).total_seconds()<26*3600:continue
            expected=min(source+timedelta(hours=26),acquired+timedelta(hours=26),datetime.combine(observed+timedelta(days=limit+1),datetime.min.time(),timezone.utc))
            if clock(m['source_valid_until'])!=expected or not at<expected:continue
            if m['source_generated_at']!=packet['source_generated_at'] or acquired>source:return absent
            value=m['value'];exact=Decimal(m['exact_value'])
            if type(value) not in (int,float) or not math.isfinite(value) or not exact.is_finite() or float(exact)!=value:return absent
            if unit=='Percent' and (m['value_bps']!=float(exact*100) or Decimal(m['exact_value_bps'])!=exact*100):return absent
            if set(m['evidence'])!={'definition','observations'}:return absent
            for evidence in m['evidence'].values():
                if evidence.get('provider')!='fred' or evidence.get('captured') is not True or not re.fullmatch(r'data/evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz',evidence.get('key','')):return absent
            measurements[sid]={k:m[k] for k in ('series_id','label','value','exact_value','unit','value_bps','observation_date','source_url','interpretation','quality','history_coverage','source_valid_until')}
            measurements[sid].update(PERMISSIONS)
        pair=None;a=measurements.get('SOFR');b=measurements.get('DFF');r=packet['repo_comparison']
        if a and b and r['current_comparison_available'] and a['observation_date']==b['observation_date']==r['observation_date']:
            expected=(Decimal(a['exact_value'])-Decimal(b['exact_value']))*100
            if r['difference_bps']!=float(expected) or Decimal(r['exact_difference_bps'])!=expected:return absent
            pair={k:r[k] for k in ('observation_date','difference_bps','unit','interpretation')}
        return {**absent,'available':bool(measurements),'reason':'descriptive_usd_context_only','measurements':measurements,
            'repo_comparison':pair,'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],'replay':ref,
            'note':'Overlapping canonical macro, credit and volatility observations; zero independent investment votes. WAIT is abstention.'}
    except (KeyError,ValueError,TypeError,ArithmeticError,OverflowError):return absent

def qualified_score(packet):
    # A descriptive contract cannot qualify its own investment score.
    return None

def decision_view(packet):
    return {'research_context':context(packet),'score':None,'composite_score':None,'composite_stress_score':None,
        'stress_score':None,'severity':None,'regime':None,'hot_signals':[],'cold_signals':[],'signals':[],
        'n_signals_used':0,'call':None,'portfolio_action':'WAIT',**PERMISSIONS}

def guard(key,packet):
    return decision_view(packet) if key=='data/eurodollar-stress.json' else packet
