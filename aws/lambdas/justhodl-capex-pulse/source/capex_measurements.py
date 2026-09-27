"""Reported cash-flow windows; no inferred durations, currencies or forecast edge."""
from datetime import date, timedelta
import calendar
import json
import math
import re

CONTRACT='capex-accounting-measurements.v1'


def number(value):
    if isinstance(value,bool) or not isinstance(value,(int,float)):return None
    try:return value if math.isfinite(value) else None
    except (OverflowError,ValueError):return None


def decode(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('Duplicate source JSON key')
            out[k]=v
        return out
    def invalid(v):raise ValueError('Nonfinite source JSON number')
    def decimal(v):
        result=float(v)
        if not math.isfinite(result) or result==0 and any(c in '123456789' for c in v.lower().split('e')[0]):invalid(v)
        return result
    if isinstance(raw,bytes):raw=raw.decode('utf-8')
    return json.loads(raw,object_pairs_hook=pairs,parse_float=decimal,parse_constant=invalid)


def day(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):return None
    try:return date.fromisoformat(value)
    except ValueError:return None


def shift(value,months):
    year,month=divmod(value.year*12+value.month-1+months,12);month+=1
    if not 1<=year<=9999:return None
    return date(year,month,min(value.day,calendar.monthrange(year,month)[1]))


def observation(row,index,symbol,as_of):
    result={'source_row':index,'original':row,'issues':[],'eligible':False,'value':None,'date':None}
    if not isinstance(row,dict):result['issues']=['row_not_object'];return result
    end=day(row.get('date'));start=day(row.get('startDate'));unit=row.get('reportedCurrency')
    cik=str(row.get('cik','')).lstrip('0');period=row.get('period');value=number(row.get('capitalExpenditure'))
    if row.get('symbol')!=symbol:result['issues'].append('explicit_symbol_mismatch_or_missing')
    if not cik.isdigit() or not cik or len(cik)>10:result['issues'].append('explicit_cik_missing')
    if not isinstance(unit,str) or not re.fullmatch('[A-Z]{3}',unit):result['issues'].append('explicit_currency_missing')
    if end is None or end>as_of:result['issues'].append('invalid_or_future_end')
    following=shift(start,3) if start else None
    if not (start and start.day==1 and end and following and following-timedelta(days=1)==end):
        result['issues'].append('explicit_calendar_quarter_unavailable')
    if period not in ('Q1','Q2','Q3','Q4'):result['issues'].append('explicit_quarter_label_missing')
    if value is None:result['issues'].append('amount_missing_or_invalid')
    result.update(date=end.isoformat() if end else None,start_date=start.isoformat() if start else None,
                  cik=cik,period=period,unit=unit,value=abs(value) if value is not None else None,eligible=not result['issues'])
    return result


def window(ordered,offset):
    result={'status':'four_explicit_contiguous_quarters_unavailable','source_rows':[],
            'start_date':None,'end_date':None,'unit':None,'cik':None,'value':None,'aligned':False}
    selected=ordered[offset:offset+4]
    if len(selected)!=4 or not all(r['eligible'] for r in selected):return result
    if len({r['unit'] for r in selected})!=1 or len({r['cik'] for r in selected})!=1:
        result['status']='mixed_issuer_or_currency';return result
    if any(sum(other['date']==r['date'] for other in ordered)!=1 for r in selected):
        result['status']='duplicate_or_conflicting_periods';return result
    for newer,older in zip(selected,selected[1:]):
        if day(older['date'])+timedelta(days=1)!=day(newer['start_date']) or int(newer['period'][1])!=int(older['period'][1])%4+1:
            result['status']='noncontiguous_quarters';return result
    value=number(sum(r['value'] for r in selected))
    if value is None:result['status']='amount_overflow';return result
    result.update(status='four_reported_calendar_quarters',source_rows=[r['source_row'] for r in selected],
                  start_date=selected[-1]['start_date'],end_date=selected[0]['date'],unit=selected[0]['unit'],
                  cik=selected[0]['cik'],value=value,aligned=True)
    return result


def dossier(symbol,source,context,as_of):
    today=day(as_of)
    if not today or not isinstance(symbol,str) or not re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}',symbol):raise ValueError('Explicit symbol and date required')
    if not isinstance(source,list):raise ValueError('Complete cash-flow response list required')
    observations=[observation(row,i,symbol,today) for i,row in enumerate(source)]
    # Invalid dated rows cannot disappear in favor of an older artificial window.
    ordered=sorted(observations,key=lambda r:r['date'],reverse=True) if all(r['date'] for r in observations) else []
    current=window(ordered,0);prior=window(ordered,4)
    paired=bool(current['aligned'] and prior['aligned'] and current['unit']==prior['unit'] and current['cik']==prior['cik']
                and shift(day(current['start_date']),-12)==day(prior['start_date'])
                and day(prior['end_date'])+timedelta(days=1)==day(current['start_date']))
    growth=number(100*(current['value']/prior['value']-1)) if paired and prior['value']>0 else None
    # Foreign statement amounts remain visible in their reported currency.
    # Today's FX cannot establish a historical USD flow or aligned market cap.
    usd=current['value'] if current['aligned'] and current['unit']=='USD' else None
    prior_usd=prior['value'] if paired and prior['unit']=='USD' else None
    return {'ticker':symbol,'sector':context.get('sec') if isinstance(context.get('sec'),str) else '?',
        'measurement_contract':CONTRACT,'checked_as_of':as_of,'source_context':context,'provider_response':source,
        'observations':observations,'current_window':current,'prior_window':prior,
        'reported_currency':current['unit'],'reported_window_amount':current['value'],
        'capex_ttm_b':round(usd/1e9,2) if usd is not None else None,
        'yoy_pct':round(growth,1) if growth is not None else None,
        'current_window_usd':usd,'prior_window_usd':prior_usd,
        'current_window_rows':len(current['source_rows']),'prior_window_rows':len(prior['source_rows']),
        'mc_b':number(context.get('mc_b')),'intensity_pct':None,
        'asof':current['end_date'],'reported_calendar_comparison_aligned':paired,
        'annual_comparability_verified':False,'call':None,'calls_eligible':False,'forecast_qualified':False,
        'sizing_eligible':False,'execution_eligible':False,
        'quality':{'status':'partial','original_sec_filings_replayed':False,'provider_originals_replayed':False,
                   'point_in_time_availability_verified':False,'market_cap_currency_and_date_aligned':False},
        'measurement_limits':'Provider calendar durations only; no original SEC or first-release replay. Foreign amounts remain local-currency. No spot-FX flow conversion, market-cap intensity or performance claim.'}


def aggregate(rows):
    eligible=[r for r in rows if number(r.get('current_window_usd')) is not None]
    # Issuer CIKs, not ticker-name resemblance, determine duplicate accounting.
    ciks=[r['current_window']['cik'] for r in eligible]
    unique=[r for r in eligible if ciks.count(r['current_window']['cik'])==1]
    cohorts={}
    for row in unique:
        w=row['current_window'];key=w['start_date']+'/'+w['end_date']
        cohorts.setdefault(key,[]).append(row)
    sums=[]
    for key,group in sorted(cohorts.items()):
        paired=[r for r in group if number(r.get('prior_window_usd')) is not None and r['prior_window_usd']>0]
        total=number(sum(r['current_window_usd'] for r in group))
        current=number(sum(r['current_window_usd'] for r in paired)) if paired else None
        prior=number(sum(r['prior_window_usd'] for r in paired)) if paired else None
        growth=number(100*(current/prior-1)) if current is not None and prior is not None and prior>0 else None
        sums.append({'start_date':key.split('/')[0],'end_date':key.split('/')[1],'unit':'USD','n':len(group),
                     'symbols':[r['ticker'] for r in group],'value':total,'comparison_n':len(paired),
                     'comparison_current_usd':current,'comparison_prior_usd':prior,
                     'yoy_pct':round(growth,1) if growth is not None else None})
    one=sums[0] if len(sums)==1 else None
    return {'capex_ttm_b':round(one['value']/1e9,1) if one and one['value'] is not None else None,
            'yoy_pct':one['yoy_pct'] if one else None,'n':len(rows),'measured_usd_n':len(unique),
            'unmeasured_or_foreign_n':len(rows)-len(eligible),'ambiguous_issuer_n':len(eligible)-len(unique),
            'comparison_n':one['comparison_n'] if one else 0,'comparison_missing_n':len(rows)-(one['comparison_n'] if one else 0),
            'comparison_current_usd':one['comparison_current_usd'] if one else None,
            'comparison_prior_usd':one['comparison_prior_usd'] if one else None,
            'cohorts':sums,'calendar_cohort_count':len(sums),'annual_comparability_verified':False,
            'comparison_scope':'Reported USD quarters grouped by exact window and unambiguous issuer. Different fiscal windows are never combined; incomplete coverage is explicit.'}


def intentions(source,as_of):
    """Exact monthly source pairs; diffusion-index changes are index points."""
    today=day(as_of);rows=source.get('observations',[]) if isinstance(source,dict) else []
    out={'series':'CEFDFSA066MSFRBPHI','original_response':source,'asof':None,'latest':None,
         'avg_3m':None,'delta_12m':None,'delta_unit':'index_points','read':None,'forecast_qualified':False}
    if not today or not isinstance(rows,list) or not rows:return out
    parsed=[]
    for row in rows:
        if not isinstance(row,dict):return out
        d=day(row.get('date'))
        if not d or d.day!=1 or d>today:return out
        try:v=number(decode(row['value'])) if isinstance(row.get('value'),str) else number(row.get('value'))
        except ValueError:v=None
        parsed.append((d,v))
    if len(set(d for d,v in parsed))!=len(parsed):return out
    parsed.sort(reverse=True);latest,value=parsed[0];values=dict(parsed)
    prior=values.get(shift(latest,-12));recent=[values.get(shift(latest,-i)) for i in range(3)]
    avg=number(sum(recent)/3) if all(v is not None for v in recent) else None
    delta=number(value-prior) if value is not None and prior is not None else None
    out.update(asof=latest.isoformat(),latest=value,avg_3m=round(avg,1) if avg is not None else None,
               delta_12m=round(delta,1) if delta is not None else None)
    return out
