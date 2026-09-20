"""Typed valuation-input research cannot authorize fair-value or timing claims."""
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal
import hashlib,json,math,re
CONTRACT='valuation-native-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
DEFINITIONS={
 'SP500':('Index','D','NSA'),'GDP':('Billions of Dollars','Q','SAAR'),
 'DGS10':('Percent','D','NSA'),'VIXCLS':('Index','D','NSA'),
 'BAA':('Percent','M','NSA'),'AAA':('Percent','M','NSA'),
 'BAMLH0A0HYM2':('Percent','D','NSA'),'BAMLC0A0CM':('Percent','D','NSA'),
 'T10YIE':('Percent','D','NSA'),'M2SL':('Billions of Dollars','M','SA'),
 'CPIAUCSL':('Index 1982-1984=100','M','SA'),'DTWEXBGS':('Index Jan 2006=100','D','NSA'),
 'WALCL':('Millions of U.S. Dollars','W','NSA'),'DCOILWTICO':('Dollars per Barrel','D','NSA'),
 'DCOILBRENTEU':('Dollars per Barrel','D','NSA'),'DHHNGSP':('Dollars per Million BTU','D','NSA')}
SERIES=(*DEFINITIONS,'CAPE','WILL5000INDFC','GOLDAMGBD228NLBM')
AGE={'D':10,'W':21,'M':100,'Q':200}

def clock(s):
    d=datetime.fromisoformat(s.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('Aware clock required')
    return d.astimezone(timezone.utc)

def context(packet,at=None):
    absent={'available':False,'reason':'verified_dated_valuation_inputs_unavailable','measurements':{},**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT:return absent
        if any(packet.get(k) is not False for k in PERMISSIONS) or any(packet.get(k) is not None for k in ('call','score','regime','cape','buffett_indicator','market_cap_gdp')):return absent
        if packet.get('composite',{}).get('score') is not None or packet.get('composite',{}).get('regime') is not None:return absent
        at=at or datetime.now(timezone.utc);source=clock(packet['source_generated_at']);generated=clock(packet['generated_at'])
        due=clock(packet['freshness']['pipeline_check_due_at'])
        if not source<=generated<=at<due or due!=source+timedelta(hours=26):return absent
        ref=packet['replay'];body={k:v for k,v in packet.items() if k!='replay'}
        if not re.fullmatch(r'data/valuation-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        if hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=ref['output_sha256']:return absent
        if set(packet['measurements'])!=set(SERIES):return absent
        rows={}
        for sid,m in packet['measurements'].items():
            if m['quality']['status']!='within_age_ceiling':continue
            if sid not in DEFINITIONS:return absent
            unit,freq,adj=DEFINITIONS[sid]
            if m['series_id']!=sid or m['unit']!=unit or m['source_unit']!=unit or m['frequency']!=freq or m['seasonal_adjustment']!=adj:return absent
            if m['definition']['id']!=sid or m['definition']['seasonal_adjustment_short']!=adj:return absent
            if m['source_url']!='https://fred.stlouisfed.org/series/'+sid or any(m.get(k) is not False for k in PERMISSIONS):return absent
            day=date.fromisoformat(m['observation_date']);acquired=clock(m['acquired_at'])
            if not acquired<=source or m['source_generated_at']!=packet['source_generated_at']:return absent
            valid=min(source+timedelta(hours=26),acquired+timedelta(hours=26),datetime.combine(day+timedelta(days=AGE[freq]+1),time.min,timezone.utc))
            if clock(m['source_valid_until'])!=valid:return absent
            if not 0<=(at.date()-day).days<=AGE[freq] or not at<valid:continue
            value=m['value'];exact=Decimal(m['exact_value'])
            if type(value) not in (int,float) or not math.isfinite(value) or not exact.is_finite() or float(exact)!=value:return absent
            if sid in ('SP500','GDP','VIXCLS','M2SL','CPIAUCSL','DTWEXBGS','WALCL') and value<=0:return absent
            if set(m['evidence'])!={'definition','observations'}:return absent
            for e in m['evidence'].values():
                if e.get('provider')!='fred' or e.get('captured') is not True or not re.fullmatch(r'data/evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz',e.get('key','')):return absent
            rows[sid]={k:m[k] for k in ('series_id','label','value','exact_value','unit','observation_date','source_url','source_valid_until','history_coverage')}
            rows[sid].update(PERMISSIONS)
        return {**absent,'available':bool(rows),'reason':'dated_inputs_only_not_fair_value','measurements':rows,'replay':ref,
            'generated_at':packet['generated_at'],'note':'Canonical macro and credit roots overlap other engines. No composite valuation, return forecast, price target or independent investment vote.'}
    except (ValueError,KeyError,TypeError,OverflowError,ArithmeticError):return absent

def qualified_score(packet):return None
def decision_view(packet):
    return {'research_context':context(packet),'score':None,'regime':None,'call':None,'cape':None,'CAPE':None,
        'buffett_indicator':None,'market_cap_gdp':None,'composite':{'score':None,'regime':None},'portfolio_action':'WAIT',**PERMISSIONS}

def guard(key,packet):
    return decision_view(packet) if key in ('valuations-data.json','data/valuations-data.json') else packet
