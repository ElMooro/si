"""Calendar-identified monthly freight measurements; no composite or forecast.

Definitions reviewed against the FRED source pages on 2026-09-27. A source's
monthly label is not its publication date. NSA six-month growth is not annualized.
"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from copy import deepcopy
import math

CONTRACT='freight-calendar-measurements.v1'
PROFILES={
 'TSIFRGHT':('tsi_freight','Index 2000=100','SA','Freight Transportation Services Index'),
 'FRGSHPUSM649NCIS':('cass_shipments','Index Jan 1990=1','NSA','Cass Freight Index: Shipments'),
 'FRGEXPUSM649NCIS':('cass_expend','Index Jan 1990=1','NSA','Cass Freight Index: Expenditures'),
 'TRUCKD11':('truck_tonnage','Index 2015=100','SA','Truck Tonnage Index'),
 'RAILFRTCARLOADSD11':('rail_carloads','Carloads','SA','Rail Freight Carloads'),
 'RAILFRTINTERMODALD11':('rail_intermodal','Containers and Trailers','SA','Rail Freight Intermodal Traffic'),
}
FLAGS=dict.fromkeys(('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'),False)


def day(value):
    if not isinstance(value,str) or len(value)!=10:raise ValueError('Exact date required')
    result=datetime.strptime(value,'%Y-%m-%d').date()
    if result.isoformat()!=value:raise ValueError('Exact date required')
    return result


def month_shift(value,months):
    index=value.year*12+value.month-1+months
    return value.replace(year=index//12,month=index%12+1,day=1)


def number(value):
    if isinstance(value,bool) or not isinstance(value,(str,int,float)):return None
    try:n=Decimal(str(value))
    except InvalidOperation:return None
    return n if n.is_finite() and 0<=n<=Decimal('1e30') and (not n or float(n)!=0) else None


def change(current,previous):
    if current is None or previous is None:return {'status':'missing_comparison','percent':None}
    if previous==0:return {'status':'zero_denominator','percent':None}
    value=float(100*(current/previous-1))
    return {'status':'measured' if math.isfinite(value) else 'outside_numeric_range','percent':value if math.isfinite(value) else None}


def measure(sid,metadata,packet,at):
    if sid not in PROFILES:raise ValueError('Unreviewed freight series')
    key,unit,seasonal,name=PROFILES[sid];clock=datetime.fromisoformat(at.replace('Z','+00:00'))
    if clock.tzinfo is None:raise ValueError('Aware calculation clock required')
    clock=clock.astimezone(timezone.utc)
    out={'series_id':sid,'key':key,'name':name,'source_url':'https://fred.stlouisfed.org/series/'+sid,
         'definition_status':'unverified','status':'unavailable','unit':None,'frequency':'monthly','seasonal_adjustment':None,
         'observations':[],'returned_rows':0,'latest_date':None,'level':None,'observation_date_kind':'month_start_label',
         'observation_freshness_verified':False,'point_in_time_verified':False,**FLAGS}
    records=metadata.get('seriess') if isinstance(metadata,dict) else None
    if not isinstance(records,list) or len(records)!=1 or not isinstance(records[0],dict) or records[0].get('id')!=sid:
        out['status']='metadata_identity_mismatch';return out
    meta=records[0];out['metadata']={k:deepcopy(meta.get(k)) for k in ('id','title','units','frequency_short','seasonal_adjustment_short','observation_start','observation_end','last_updated','realtime_start','realtime_end')}
    if (meta.get('units'),meta.get('frequency_short'),meta.get('seasonal_adjustment_short'))!=(unit,'M',seasonal):
        out['status']='metadata_definition_changed';return out
    out.update(definition_status='reviewed_source_definition',unit=unit,seasonal_adjustment=seasonal)
    rows=packet.get('observations') if isinstance(packet,dict) else None
    if (not isinstance(rows,list) or type(packet.get('count')) is not int or packet['count']!=len(rows)
        or type(packet.get('offset')) is not int or packet['offset']!=0 or packet.get('units')!='lin' or type(packet.get('output_type')) is not int or packet['output_type']!=1):
        out['status']='incomplete_or_transformed_response';return out
    values={};ambiguous=False
    for i,row in enumerate(rows):
        item={'row':i,'date':row.get('date') if isinstance(row,dict) else None,'provider_value':deepcopy(row.get('value')) if isinstance(row,dict) else None,'value':None,'status':'invalid_date'}
        try:
            date=day(item['date'])
            if date.day!=1 or not day('2015-01-01')<=date<=clock.date():raise ValueError('Outside requested calendar')
            n=number(item['provider_value']);item.update(value=float(n) if n is not None else None,status='observed' if n is not None else 'missing_or_invalid_value')
            if date in values:ambiguous=True;item['status']='duplicate_date'
            values[date]=n
        except (ValueError,TypeError):ambiguous=True
        out['observations'].append(item)
    out['returned_rows']=len(rows)
    if ambiguous:out['status']='ambiguous_observation_identity';return out
    if not values:out['status']='empty_observations';return out
    latest=max(values);current=values[latest];out.update(latest_date=latest.isoformat(),level=float(current) if current is not None else None,
       observation_age_days=(clock.date()-latest).days,status='measured' if current is not None else 'latest_missing')
    for field,shift in [('yoy',-12),('six_month',-6)]:
        previous_date=month_shift(latest,shift);previous=values.get(previous_date)
        comparison=change(current,previous)
        comparison.update(current_date=latest.isoformat(),current_level=float(current) if current is not None else None,
           previous_date=previous_date.isoformat(),previous_level=float(previous) if previous is not None else None,unit='percent')
        if field=='six_month':
            comparison.update(annualized_percent=None,annualization_status='not_seasonally_adjusted' if seasonal=='NSA' else comparison['status'])
            if seasonal=='SA' and comparison['status']=='measured':
                value=float(100*((current/previous)**2-1))
                comparison['annualized_percent']=value if math.isfinite(value) else None
                if not math.isfinite(value):comparison['annualization_status']='outside_numeric_range'
        out[field]=comparison
    dates=[month_shift(latest,i) for i in range(-60,0)];missing=[d.isoformat() for d in dates if values.get(d) is None]
    baseline={'definition':'Prior 60 exact calendar months, excluding current observation; population standard deviation.',
              'start':dates[0].isoformat(),'end':dates[-1].isoformat(),'expected_months':60,'missing_or_invalid_dates':missing,
              'mean':None,'population_sd':None,'z':None,'status':'incomplete_baseline' if missing else 'latest_missing' if current is None else 'measured'}
    if not missing:
        nums=[values[d] for d in dates];mean=sum(nums)/60;variance=sum((n-mean)**2 for n in nums)/60;sd=variance.sqrt()
        baseline.update(mean=float(mean),population_sd=float(sd))
        if sd==0:baseline['status']='zero_variance'
        elif current is not None:
            z=float((current-mean)/sd);baseline['z']=z if math.isfinite(z) else None
            if not math.isfinite(z):baseline['status']='outside_numeric_range'
    out['prior_60_months']=baseline
    return out


def cass_ratio(series):
    a=series.get('FRGEXPUSM649NCIS',{});b=series.get('FRGSHPUSM649NCIS',{})
    out={'status':'unavailable','unit':'dimensionless_index_ratio','current_date':None,'prior_date':None,
         'current_ratio':None,'prior_ratio':None,'yoy_percent':None,'observations':[],
         'meaning':'Ratio of expenditure and shipment indexes with the same January 1990 base. Not dollars per shipment or a pure freight-rate/price index; fuel, mix and timing can matter.',**FLAGS}
    if a.get('status') not in ('measured','latest_missing') or b.get('status') not in ('measured','latest_missing'):return out
    av={r['date']:number(r['provider_value']) for r in a['observations']};bv={r['date']:number(r['provider_value']) for r in b['observations']}
    dates=sorted(set(av)|set(bv));ratios={}
    for date in dates:
        numerator,denominator=av.get(date),bv.get(date);ratio=numerator/denominator if numerator is not None and denominator is not None and denominator>0 else None
        if ratio is not None and not math.isfinite(float(ratio)):ratio=None
        ratios[date]=ratio;out['observations'].append({'date':date,'expenditure_index':float(numerator) if numerator is not None else None,
             'shipment_index':float(denominator) if denominator is not None else None,'ratio':float(ratio) if ratio is not None else None})
    if not dates:return out
    latest=dates[-1];prior=month_shift(day(latest),-12).isoformat();current,previous=ratios[latest],ratios.get(prior)
    out.update(current_date=latest,prior_date=prior,current_ratio=float(current) if current is not None else None,
               prior_ratio=float(previous) if previous is not None else None)
    comparison=change(current,previous);out.update(status=comparison['status'],yoy_percent=comparison['percent']);return out


def build(responses,at):
    rows={sid:measure(sid,responses.get((sid,'series')),responses.get((sid,'series/observations')),at) for sid in PROFILES}
    return {'contract':CONTRACT,'calculation_at':at,'series':rows,'cass_index_ratio':cass_ratio(rows),
            'series_count':len(rows),'complete_requested_population':True,'point_in_time_verified':False,**FLAGS,
            'interpretation':'Descriptive monthly series with exact calendar comparisons. Different source dates, seasonal adjustments and overlapping transport populations cannot form independent forecasts by averaging them.',
            'weekly_distillate_status':'Retained separately in the complete native calculation. Original-source identity and weekly calendar/sector interpretation require their own review; not truck fuel consumption.'}
