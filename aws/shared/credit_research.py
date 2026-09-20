"""Typed current credit research context; packet fields cannot authorize a trade."""
from datetime import date,datetime,timezone
from decimal import Decimal
import hashlib,json,math,re

CONTRACT='credit-native-research.v1'
PERMISSIONS={'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}


def context(packet,at=None):
    absent={'available':False,'reason':'verified_current_credit_unavailable','measurements':{},'comparisons':{},**PERMISSIONS}
    try:
        if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT or packet.get('call') is not None: return absent
        if any(packet.get(k) is not False for k in PERMISSIONS): return absent
        at=at or datetime.now(timezone.utc)
        generated=datetime.fromisoformat(packet['generated_at'].replace('Z','+00:00'))
        deadline=datetime.fromisoformat(packet['freshness']['pipeline_check_due_at'].replace('Z','+00:00'))
        if generated.tzinfo is None or deadline.tzinfo is None or not generated<=at<deadline or (at-generated).total_seconds()>36*3600:return absent
        ref=packet['replay']
        if not re.fullmatch(r'data/credit-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']):return absent
        raw=json.dumps({k:v for k,v in packet.items() if k!='replay'},sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if hashlib.sha256(raw).hexdigest()!=ref['output_sha256']:return absent
        sources={}
        for source in packet['source_evidence']:
            sid,kind=source['series_id'],source['kind'];digest=source['sha256']
            if not re.fullmatch('[a-f0-9]{64}',digest) or source['key']!='audit-private/20260909-originals/credit-research/'+digest+'.bin' or source.get('provider')!='fred':return absent
            expected='https://api.stlouisfed.org/fred/'+('series?' if kind=='definition' else 'series/observations?')+'series_id='+sid+'&'
            if kind not in ('definition','observations') or not source['request_url'].startswith(expected) or 'api_key' in source['request_url']:return absent
            if (sid,kind) in sources:return absent
            collected=datetime.fromisoformat(source['acquired_at'].replace('Z','+00:00'))
            if collected.tzinfo is None or not generated.timestamp()-240<=collected.timestamp()<=generated.timestamp():return absent
            sources[(sid,kind)]=source
        measurements={}
        for sid,m in packet['measurements'].items():
            if m.get('series_id')!=sid or m.get('quality',{}).get('status')!='within_age_ceiling':continue
            if any((sid,k) not in sources for k in ('definition','observations')):continue
            day=date.fromisoformat(m['observation_date'])
            if not 0<=(at.date()-day).days<=5:continue
            p,b=m['value_pct'],m['value_bps']
            if any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) for v in (p,b)):continue
            if Decimal(str(p))*100!=Decimal(str(b)):continue
            if m['source_unit']!='Percent' or m['source_url']!='https://fred.stlouisfed.org/series/'+sid:continue
            if any(m.get(k) is not False for k in PERMISSIONS):continue
            measurements[sid]={'series_id':sid,'value_pct':p,'value_bps':b,'observation_date':str(day),
                'unit':m['unit'],'definition':m['definition'],'source_url':m['source_url'],
                'previous_observation_date':m['previous_observation_date'],'change_bps':m['change_bps'],
                'descriptive_statistics':m['descriptive_statistics'],**PERMISSIONS}
        comparisons={}
        for name,pair in packet['comparisons'].items():
            a=measurements.get(pair['left_series_id']);b=measurements.get(pair['right_series_id'])
            if not a or not b or not pair['current_comparison_available']:continue
            if a['observation_date']!=b['observation_date'] or pair['observation_date']!=a['observation_date']:continue
            if Decimal(str(a['value_pct']))-Decimal(str(b['value_pct']))!=Decimal(str(pair['value_pp'])):continue
            comparisons[name]={k:pair[k] for k in ('value_pp','value_bps','observation_date','left_series_id','right_series_id')}
        return {**absent,'available':bool(measurements),'reason':'descriptive_credit_only','generated_at':packet['generated_at'],
            'measurements':measurements,'comparisons':comparisons,'replay':ref,
            'interpretation':'Source-defined dated measurements. OAS is not default probability, all-in funding cost, expected return or a trade vote.'}
    except (KeyError,TypeError,ValueError,ArithmeticError,OverflowError):return absent


def describe(doc):
    if not doc.get('available'):return 'Credit: verified current measurements unavailable; research abstains.'
    parts=[]
    for sid,label in (('BAMLH0A0HYM2','US HY OAS'),('BAMLC0A0CM','US IG OAS'),('BAMLEMCBPIOAS','EM corporate plus OAS (mixed ratings)')):
        m=doc['measurements'].get(sid)
        if m:parts.append(f"{label} {m['value_bps']:g} bp ({m['value_pct']:g}%) observed {m['observation_date']}; source {m['source_url']}")
    return 'Credit research: '+'; '.join(parts)+'. Descriptive context; no qualified return forecast, crisis call or target position.'


def qualified_signal(packet):
    # No externally accepted credit forecasting model is registered here.
    return None


def decision_view(packet):
    return {'research_context':context(packet),'call':None,'composite_regime':None,'regimes':{},'metrics':{},**PERMISSIONS}


def morning_fields(packet):
    doc=context(packet);measurements=doc['measurements'];comparisons=doc['comparisons']
    out={'credit_research_context':doc,'credit_composite_signal':describe(doc),
         'credit_hy_z_60d':None,'credit_hy_pct_1y':None,'credit_hy_dod_bps':None,
         'credit_hy_regime':None,'credit_ig_regime':None,'credit_composite_regime':None,
         'credit_generated_at':doc.get('generated_at'),'credit_data_date':(measurements.get('BAMLH0A0HYM2') or {}).get('observation_date')}
    for label,sid in (('hy','BAMLH0A0HYM2'),('ig','BAMLC0A0CM'),('bbb','BAMLC0A4CBBB'),('ccc','BAMLH0A3HYC'),('em_hy','BAMLEMHBHYCRPIOAS')):
        m=measurements.get(sid,{})
        out['credit_'+label+'_oas_pct']=m.get('value_pct');out['credit_'+label+'_observation_date']=m.get('observation_date')
    for name in ('hy_minus_ig','bbb_minus_aaa','ccc_minus_bb','em_hy_minus_us_hy'):
        p=comparisons.get(name,{})
        out['credit_'+name]=p.get('value_pp');out['credit_'+name+'_unit']='percentage_points'
        out['credit_'+name+'_observation_date']=p.get('observation_date')
    curve=measurements.get('T10Y2Y',{})
    out.update(yield_curve_10y_2y=curve.get('value_pct'),yield_curve_10y_2y_unit='percentage_points',yield_curve_10y_2y_observation_date=curve.get('observation_date'))
    return out
