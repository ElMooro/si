"""ECB observations retain their unit scale, period and quality contract."""
import calendar
import csv
import io
import math
import statistics
from datetime import date, datetime, timezone

METHOD = 'ecb-dated-units.v2'
FREQUENCIES = {'D':'daily','B':'daily','W':'weekly','M':'monthly','Q':'quarterly','A':'annual'}
SLA = {'daily':7,'weekly':21,'monthly':100,'quarterly':180,'annual':550}


def period_end(period):
    if '-W' in period:
        y,w=map(int,period.split('-W'));return date.fromisocalendar(y,w,5)
    if '-Q' in period:
        y,q=map(int,period.split('-Q'))
        if not 1<=q<=4:raise ValueError('invalid quarter')
        m=q*3;return date(y,m,calendar.monthrange(y,m)[1])
    if len(period)==7:
        y,m=map(int,period.split('-'));return date(y,m,calendar.monthrange(y,m)[1])
    if len(period)==4:return date(int(period),12,31)
    return date.fromisoformat(period)


def parse_csv(text,flow_key):
    rows={};units=set();multipliers=set();frequencies=set();statuses=set();skipped=0
    for row in csv.DictReader(io.StringIO(text.lstrip('\ufeff'))):
        period=row.get('TIME_PERIOD','').strip();raw=row.get('OBS_VALUE','').strip()
        if not period or not raw:continue
        try:value=float(raw)
        except ValueError:continue
        if not math.isfinite(value):raise ValueError('nonfinite observation')
        status=row.get('OBS_STATUS','').strip();statuses.add(status)
        if status not in ('','A','E','P'):
            skipped+=1;continue
        unit=row.get('UNIT','').strip();mult=row.get('UNIT_MULT','').strip();freq=row.get('FREQ','').strip()
        if not unit or not mult or freq not in FREQUENCIES:raise ValueError('unit scale or frequency missing')
        multiplier=int(mult)
        if not -9<=multiplier<=12:raise ValueError('unsupported scale')
        units.add(unit);multipliers.add(multiplier);frequencies.add(freq)
        if flow_key.startswith('ILM/'):
            if unit!='EUR':raise ValueError('ILM monetary unit is not EUR')
            value*=10.0**(multiplier-9)
        else:value*=10.0**multiplier
        end=period_end(period)
        key=end.isoformat() if '-W' in period else period
        if key in rows and rows[key]!=value:raise ValueError('conflicting duplicate period')
        rows[key]=value
    if not rows:raise ValueError('no usable observations')
    if len(units)!=1 or len(frequencies)!=1:raise ValueError('mixed series units or frequency')
    return sorted([[d,v] for d,v in rows.items()]), {
        'unit':'EUR_bn' if flow_key.startswith('ILM/') else next(iter(units)),
        'source_unit':next(iter(units)),'source_unit_multipliers':sorted(multipliers),
        'frequency':FREQUENCIES[next(iter(frequencies))],
        'conversion':'OBS_VALUE * 10^(UNIT_MULT-9)' if flow_key.startswith('ILM/') else 'OBS_VALUE * 10^UNIT_MULT',
        'observation_statuses':sorted(statuses),'excluded_status_rows':skipped}


def quality(period,freq,today=None):
    today=today or datetime.now(timezone.utc).date()
    try:age=(today-period_end(period)).days
    except (ValueError,TypeError):return {'status':'invalid','reason':'invalid_period','observation_date':period}
    status='invalid' if age<0 else 'stale' if age>SLA[freq] else 'fresh'
    return {'status':status,'age_days':age,'max_age_days':SLA[freq],'observation_date':period,'period_end':period_end(period).isoformat()}


def summarize(sid,label,points,metadata,source,today=None):
    points=sorted(points)
    if not points or any(not math.isfinite(v) for _,v in points):raise ValueError('invalid history')
    vals=[v for _,v in points];q=quality(points[-1][0],metadata['frequency'],today)
    # Historical distribution is descriptive, includes revisions and changing composition.
    sample=vals[-260:];sd=statistics.pstdev(sample) if len(sample)>=26 else 0
    live=q['status']=='fresh'
    return {'id':sid,'label':label,'methodology_version':METHOD,'flow_key':source,
            'source_url':'https://data.ecb.europa.eu/data/datasets/'+source.split('/')[0]+'/'+source.replace('/','.',1) if source.startswith(('ILM/','CISS/','EXR/','STS/','LFSI/','MNA/','BSI/','MIR/','ICP/')) else source,
            'generated_at':datetime.now(timezone.utc).isoformat(),'freq':metadata['frequency'],
            'unit':metadata['unit'],'source_metadata':metadata,'quality':q,
            'latest':round(vals[-1],5) if live else None,'last_observed_value':round(vals[-1],5),
            'latest_date':points[-1][0],'first_date':points[0][0],'n_points':len(points),
            'min':min(vals),'max':max(vals),'points':points,
            'percentile':round(100*sum(v<=vals[-1] for v in vals)/len(vals),1) if live else None,
            'z_score':round((vals[-1]-statistics.mean(sample))/sd,2) if live and sd else None,
            'z_window_observations':len(sample),'stale_days':q.get('age_days'),'discontinued':False,
            'call':None,'execution_eligible':False,'revision_basis':'current-vintage descriptive history',
            'interpretation':'Balance-sheet stocks do not by themselves identify policy injections, dollar shortages or funding demand.'}


def yoy_series(points):
    lookup={d[:7]:v for d,v in points};out=[]
    for d,v in points:
        y,m=map(int,d[:7].split('-'));prior=lookup.get(f'{y-1:04d}-{m:02d}')
        if prior and math.isfinite(prior):out.append([d,round((v/prior-1)*100,4)])
    return out
