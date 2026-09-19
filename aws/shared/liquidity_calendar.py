"""Calendar-defined descriptive liquidity features; no investment authority."""
from datetime import date,timedelta
from decimal import Decimal,localcontext
from math import isfinite
from statistics import mean,pstdev


def friday_samples(history):
    """Retain actual Friday-noon archive evaluations; never invent a missing week."""
    for day in history:
        if not isinstance(day,str) or date.fromisoformat(day).isoformat()!=day:raise ValueError('canonical calendar date required')
    return {day:value for day,value in history.items() if date.fromisoformat(day).weekday()==4}


def calendar_trend(weekly,end,weeks=13):
    endpoint=date.fromisoformat(end)
    if endpoint.weekday()!=4 or weeks!=13:raise ValueError('reviewed Friday / 13-week policy required')
    start=endpoint-timedelta(weeks=weeks)
    days=[(start+timedelta(weeks=i)).isoformat() for i in range(weeks+1)]
    missing=[day for day in days if weekly.get(day) is None]
    result={'status':'incomplete_calendar_window','start':start.isoformat(),'end':end,'calendar_days':91,
        'expected_weekly_samples':14,'present_weekly_samples':14-len(missing),'missing_weeks':missing,
        'slope_usd_mn_per_week':None,'slope_decimal':None,'change_usd_mn_decimal':None,
        'sample_dates':days,'definition':'OLS level slope on elapsed calendar weeks, inclusive 13-week interval; all 14 Friday samples required.'}
    if missing:return result
    if any(not isinstance(weekly[day],str) for day in days):raise ValueError('exact source decimal text required')
    values=[Decimal(weekly[day]) for day in days]
    if any(not v.is_finite() or not isfinite(float(v)) or (v and not float(v)) for v in values):raise ValueError('finite representable source levels required')
    with localcontext() as ctx:
        ctx.prec=28
        xs=[Decimal((date.fromisoformat(day)-start).days)/7 for day in days]
        xbar=sum(xs)/len(xs);ybar=sum(values)/len(values)
        slope=sum((x-xbar)*(y-ybar) for x,y in zip(xs,values))/sum((x-xbar)**2 for x in xs)
        if not isfinite(float(slope)):raise ValueError('calendar slope overflow')
        result.update(status='descriptive',slope_decimal=str(slope),slope_usd_mn_per_week=float(slope),
                      change_usd_mn_decimal=str(values[-1]-values[0]))
    return result


def calendar_features(history):
    weekly=friday_samples(history);ends=sorted(weekly);rows={}
    for end in ends:rows[end]=calendar_trend(weekly,end)
    for end,current in rows.items():
        # Acceleration is explicitly change in two slopes, not the level slope.
        previous=(date.fromisoformat(end)-timedelta(weeks=13)).isoformat();baseline=rows.get(previous)
        current.update(acceleration_usd_mn_per_week2=None,acceleration_baseline_date=previous)
        if current['slope_decimal'] is not None and baseline and baseline['slope_decimal'] is not None:
            with localcontext() as ctx:
                ctx.prec=28
                current['acceleration_usd_mn_per_week2']=float((Decimal(current['slope_decimal'])-Decimal(baseline['slope_decimal']))/13)
        endpoint=date.fromisoformat(end)
        try:cutoff=endpoint.replace(year=endpoint.year-3)
        except ValueError:cutoff=endpoint.replace(year=endpoint.year-3,day=28)
        first=cutoff+timedelta(days=(4-cutoff.weekday())%7)
        required=[];cursor=first
        while cursor<=endpoint:required.append(cursor.isoformat());cursor+=timedelta(days=7)
        missing=[d for d in required if d not in rows or rows[d]['slope_decimal'] is None]
        stat={'status':'incomplete_calendar_window','start':cutoff.isoformat(),'end':end,
            'expected_weekly_slopes':len(required),'missing_weeks':missing,'z':None,
            'definition':'Descriptive population z-score of complete weekly 13-week slopes in the trailing three calendar years; current slope included, no predictive probability.'}
        if not missing:
            values=[rows[d]['slope_usd_mn_per_week'] for d in required];avg=mean(values);sd=pstdev(values)
            stat.update(status='zero_variance' if sd==0 else 'descriptive',population_mean=avg,population_sd=sd,
                        z=None if sd==0 else (current['slope_usd_mn_per_week']-avg)/sd)
        current['z_3y']=stat
    return {'sampling':'Friday 12:00 UTC archive evaluation; existing conservative provider-day availability policy',
        'history':rows,'latest':rows[ends[-1]] if ends else None,'weekly_observations':len(weekly),
        'calls_eligible':False,'sizing_eligible':False,'historical_feature_replay_ready':False,
        'scope':'Descriptive reconstruction from original-bound provider archives; no historical system-possession or investment-edge claim.'}
