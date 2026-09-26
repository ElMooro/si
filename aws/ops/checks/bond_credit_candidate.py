"""Typed, same-date credit comparisons for the Bond Desk; no recursive search."""
from datetime import datetime,date,timezone
from decimal import Decimal,localcontext
import hashlib,json,math,re

PAIRS={'ccc_minus_bb':('BAMLH0A3HYC','BAMLH0A1HYBB'),
       'hy_minus_ig':('BAMLH0A0HYM2','BAMLC0A0CM'),
       'bbb_minus_aaa':('BAMLC0A4CBBB','BAMLC0A1CAAA'),
       'em_hy_minus_us_hy':('BAMLEMHBHYCRPIOAS','BAMLH0A0HYM2')}
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
encode=lambda x:json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def clock(s):
    out=datetime.fromisoformat(s.replace('Z','+00:00'))
    if out.tzinfo is None:raise ValueError('Aware clock required')
    return out.astimezone(timezone.utc)


def decimal(s):
    if not isinstance(s,str) or len(s)>80 or not re.fullmatch(r'-?\d+(?:\.\d+)?',s):raise ValueError('Exact source decimal required')
    out=Decimal(s)
    if abs(out)>1000:raise ValueError('Reviewed percent range exceeded')
    return out


def number_matches(value,exact):
    return type(value) in (int,float) and math.isfinite(value) and Decimal(str(value))==exact


def project(raw,evaluated_at):
    if not isinstance(raw,bytes) or not 0<len(raw)<=4*1024*1024:raise ValueError('Whole bounded credit packet required')
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('Duplicate credit key')
            out[k]=v
        return out
    def invalid(value):raise ValueError('Nonfinite credit JSON')
    p=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid);encode(p)
    if p.get('contract')!='credit-native-research.v1' or any(p.get(k) is not False for k in FLAGS):raise ValueError('Explicit native credit research required')
    at=clock(evaluated_at);generated=clock(p['generated_at'])
    fresh=0<=(at-generated).total_seconds()<=36*3600 and generated<=at<clock(p['freshness']['valid_until'])
    results={};sources={(r.get('series_id'),r.get('kind')):r for r in p.get('source_evidence',[])}
    if len(sources)!=len(p.get('source_evidence',[])):raise ValueError('Duplicate credit source identity')
    for name,(left,right) in PAIRS.items():
        result={'id':name,'left_series_id':left,'right_series_id':right,'unit':'basis_points','value_decimal':None,
            'value_bps':None,'observation_date':None,'status':'unavailable','reason':'source_or_comparison_unavailable',
            'formula':'100 * (left OAS percent - right OAS percent), same latest provider date',
            'source_path':'/comparisons/'+name,'components':[],**{k:False for k in FLAGS}}
        try:
            pair=p['comparisons'][name];a=p['measurements'][left];b=p['measurements'][right]
            if not fresh:raise ValueError('source_expired_or_future')
            if pair.get('id')!=name or pair.get('unit')!='basis_points' or pair.get('left_series_id')!=left or pair.get('right_series_id')!=right:raise ValueError('comparison_identity_or_unit_differs')
            if any(pair.get(k) is not False for k in FLAGS) or pair.get('current_comparison_available') is not True or pair.get('both_latest_dates_match') is not True:raise ValueError('comparison_not_current_research')
            values=[];components=[];days=[]
            for sid,row,side in ((left,a,'left'),(right,b,'right')):
                if row.get('series_id')!=sid or row.get('source_unit')!='Percent' or row.get('unit')!='percent' or row.get('kind')!='oas' or any(row.get(k) is not False for k in FLAGS):raise ValueError('measurement_identity_or_unit_differs')
                day=date.fromisoformat(row['observation_date']);idx=row['original_row_index'];value=decimal(row['exact']['value_pct'])
                if type(idx) is not int or idx<0 or type(pair[side+'_original_row_index']) is not int or pair[side+'_original_row_index']!=idx:raise ValueError('original_row_differs')
                if row['quality']['status']!='within_age_ceiling' or not 0<=(at.date()-day).days<=5 or clock(row['collected_at'])!=generated or at>=clock(row['source_valid_until']):raise ValueError('observation_expired_or_future')
                if not number_matches(row['value_pct'],value) or not number_matches(row['value_bps'],value*100):raise ValueError('typed_source_value_differs')
                refs={kind:sources[(sid,kind)] for kind in ('definition','observations')}
                for kind,ref in refs.items():
                    digest=ref['sha256']
                    if not re.fullmatch('[a-f0-9]{64}',digest) or ref['key']!='audit-private/20260909-originals/credit-research/'+digest+'.bin' or type(ref['bytes']) is not int or ref['bytes']<=0 or not 0<=(generated-clock(ref['acquired_at'])).total_seconds()<=240:raise ValueError('protected_original_reference_differs')
                values.append(value);days.append(day.isoformat());components.append({'series_id':sid,'value_percent_decimal':format(value,'f'),'unit':'percent','original_row_index':idx,'observation_date':day.isoformat(),'originals':refs})
            if not days[0]==days[1]==pair['observation_date']==pair['left_latest_date']==pair['right_latest_date']:raise ValueError('latest_dates_differ')
            with localcontext() as ctx:
                ctx.prec=100;difference=values[0]-values[1];bps=difference*100
            if not number_matches(pair['value_bps'],bps) or not number_matches(pair['value_pp'],difference):raise ValueError('comparison_arithmetic_differs')
            result.update(value_decimal=format(bps,'f'),value_bps=float(bps),observation_date=days[0],status='dated_descriptive_comparison',reason=None,components=components)
        except (KeyError,ValueError,TypeError,AttributeError) as error:
            result['reason']=str(error) if isinstance(error,ValueError) and re.fullmatch('[a-z_]{1,80}',str(error)) else 'source_or_comparison_unavailable'
        results[name]=result
    return {'contract':'bond-credit-comparisons.v1','evaluated_at':evaluated_at,'comparisons':results,
        'source':{'key':'data/credit-stress.json','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'generated_at':p['generated_at'],'replay':p.get('replay'),'original_replayed_here':False},
        'legacy_fields':{name+'_bps':row['value_bps'] for name,row in results.items()},'independent_votes':0,
        'decision':{'verb':'WAIT','meaning':'abstain'},'credit_stress_score':None,'default_probability':None,
        'definition':'Index OAS differences across different constituent baskets; not executable spreads, pure default premiums or portfolio targets.',
        **{k:False for k in FLAGS}}
