"""VRP research context cannot promote historical volatility into a trading vote."""
from datetime import datetime,date,time,timedelta,timezone
from decimal import Decimal
import hashlib,json,math,re
CONTRACT='vrp-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
SERIES=('SP500','VIXCLS','VXVCLS')
def clock(value):
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('Aware clock required')
    return d.astimezone(timezone.utc)
def context(packet,at=None):
    absent={'available':False,'reason':'verified_current_spx_volatility_context_unavailable','measurements':{},**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT:return absent
        if any(packet.get(k) is not False for k in PERMISSIONS) or any(packet.get(k) is not None for k in ('call','regime','score')):return absent
        at=at or datetime.now(timezone.utc);generated=clock(packet['generated_at']);source=clock(packet['source_generated_at']);due=clock(packet['freshness']['pipeline_check_due_at'])
        if not source<=generated<=at<due or due!=source+timedelta(hours=26):return absent
        ref=packet['replay']
        if not re.fullmatch(r'data/vrp-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        body={k:v for k,v in packet.items() if k!='replay'}
        if hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref['output_sha256']:return absent
        if set(packet['measurements'])!=set(SERIES):return absent
        rows={}
        for sid,m in packet['measurements'].items():
            if m['quality']['status']!='within_age_ceiling':continue
            if m['series_id']!=sid or m['unit']!='Index' or m['source_unit']!='Index' or m['frequency']!='D':return absent
            if m['definition']['id']!=sid or m['definition']['seasonal_adjustment_short']!='NSA':return absent
            if m['source_url']!='https://fred.stlouisfed.org/series/'+sid or any(m.get(k) is not False for k in PERMISSIONS):return absent
            observed=date.fromisoformat(m['observation_date']);acquired=clock(m['acquired_at'])
            if not acquired<=source or m['source_generated_at']!=packet['source_generated_at']:return absent
            valid=min(source+timedelta(hours=26),acquired+timedelta(hours=26),datetime.combine(observed+timedelta(days=11),time.min,timezone.utc))
            if clock(m['source_valid_until'])!=valid or not 0<=(at.date()-observed).days<=10 or not at<valid:continue
            value=m['value'];exact=Decimal(m['exact_value'])
            if type(value) not in (int,float) or not math.isfinite(value) or value<=0 or not exact.is_finite() or float(exact)!=value:return absent
            if set(m['evidence'])!={'definition','observations'}:return absent
            for e in m['evidence'].values():
                if e.get('provider')!='fred' or e.get('captured') is not True or not re.fullmatch(r'data/evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz',e.get('key','')):return absent
            rows[sid]={k:m[k] for k in ('series_id','label','value','exact_value','unit','observation_date','source_url','source_valid_until','history_coverage')}
            rows[sid].update(PERMISSIONS)
        return {**absent,'available':bool(rows),'reason':'descriptive_spx_volatility_context_only','measurements':rows,
            'generated_at':packet['generated_at'],'replay':ref,'note':'SPX implied and realized research shares canonical market roots. Descriptive gaps and ex-post outcomes are not option P&L or independent investment votes.'}
    except (ValueError,TypeError,KeyError,ArithmeticError,OverflowError):return absent
def qualified_score(packet):return None
def decision_view(packet):
    return {'research_context':context(packet),'score':None,'regime':None,'call':None,'portfolio_action':'WAIT',
        'vrp':{'vrp_30d':None,'vrp_30d_percentile_1y':None},'realized':{'rv_21d':None},**PERMISSIONS}
