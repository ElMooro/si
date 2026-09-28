"""Pure dated volume-window measurements; no acceleration score or confirmation."""
from collections import Counter
from datetime import date,timedelta
from decimal import Decimal,localcontext
import re

CONTRACT='velocity-volume-observations.v1'
BASELINE=20
RECENT=7
LOOKBACK_CALENDAR_DAYS=57
FLAGS=dict.fromkeys(('calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'),False)


def number(value,zero=False):
    if isinstance(value,bool) or not isinstance(value,(int,float,Decimal)):return None
    try:n=Decimal(str(value))
    except (ValueError,ArithmeticError):return None
    return n if n.is_finite() and (n>=0 if zero else n>0) and n<=Decimal('1e20') and (not n or n>=Decimal('1e-20')) else None


def day(value):
    try:return date.fromisoformat(value) if isinstance(value,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',value) else None
    except ValueError:return None


def _operand(row,field):
    value=row['values'][field]
    return {'source_pointer':row['source_pointer']+'/'+field,'source_index':row['source_index'],
            'observation_date':row['date'],'value_exact':str(value) if value is not None else None}


def calculate(doc,ticker,as_of):
    if not isinstance(ticker,str) or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-^]{0,24}',ticker) or type(as_of) is not date:
        raise ValueError('Literal issuer and UTC acquisition date required')
    source=doc if isinstance(doc,list) else doc.get('historical') if isinstance(doc,dict) else None
    root='' if isinstance(doc,list) else '/historical';issues=[];rows=[];dates=[];fatal=False
    reported_count=len(source) if isinstance(source,list) else None
    if not isinstance(source,list):source=[];fatal=True;issues.append({'reason':'unrecognized_shape'})
    if isinstance(doc,dict) and doc.get('symbol') not in (None,ticker):fatal=True;issues.append({'reason':'wrong_wrapper_issuer'})
    for index,r in enumerate(source):
        pointer=root+'/'+str(index)
        if not isinstance(r,dict):fatal=True;issues.append({'source_pointer':pointer,'reason':'non_object_row'});continue
        d=day(r.get('date'))
        if d is None:fatal=True;issues.append({'source_pointer':pointer,'reason':'invalid_date'});continue
        if r.get('symbol')!=ticker:fatal=True;issues.append({'source_pointer':pointer,'reason':'missing_or_wrong_issuer'})
        dates.append(d)
        if d>=as_of or d<as_of-timedelta(days=LOOKBACK_CALENDAR_DAYS):
            issues.append({'source_pointer':pointer,'reason':'outside_declared_completed_date_window'});continue
        values={k:number(r.get(k),zero=k=='volume') for k in ('open','high','low','close','volume')}
        price_fields=('open','high','low','close');price_ok=all(values[k] is not None for k in price_fields)
        if price_ok and not values['low']<=min(values['open'],values['close'])<=max(values['open'],values['close'])<=values['high']:price_ok=False
        if not price_ok:issues.append({'source_pointer':pointer,'reason':'missing_or_invalid_ohlc'})
        if values['volume'] is None:issues.append({'source_pointer':pointer,'reason':'missing_or_invalid_volume'})
        rows.append({'date':d.isoformat(),'source_pointer':pointer,'source_index':index,'values':values,'price_valid':price_ok})
    if any(n>1 for n in Counter(dates).values()):fatal=True;issues.append({'reason':'duplicate_dates'})
    rows.sort(key=lambda r:(r['date'],r['source_index']));window=rows[-(BASELINE+RECENT):]
    complete=not fatal and len(window)==BASELINE+RECENT;baseline_rows=window[:BASELINE] if complete else []
    recent=window[BASELINE:] if complete else [];metrics=[]
    def metric(name,unit,value,reason,operands,definition):
        dates=[o['observation_date'] for o in operands]
        result={'name':name,'unit':unit,'value':float(value) if value is not None else None,
            'value_exact':str(value) if value is not None else None,'status':'measured' if value is not None else 'unavailable',
            'reason':reason if value is None else None,'operands':operands,'definition':definition,
            'start_date':min(dates) if dates else None,'end_date':max(dates) if dates else None,**FLAGS}
        metrics.append(result);return result
    unavailable='source_identity_or_dates_invalid' if fatal else 'insufficient_completed_observations'
    baseline=None
    with localcontext() as ctx:
        ctx.prec=34
        if complete and all(r['values']['volume'] is not None for r in baseline_rows):
            baseline=sum(r['values']['volume'] for r in baseline_rows)/BASELINE
        baseline_reason=unavailable if not complete else 'invalid_baseline_volume'
        baseline_operands=[_operand(r,'volume') for r in baseline_rows]
        metric('baseline_volume_mean_20','reported_volume_units',baseline,baseline_reason,baseline_operands,
               'Mean of exactly twenty reported volumes immediately before the seven-observation recent window, including genuine zeros.')
        per_observation=[];ratios=[];returns=[]
        for index,r in enumerate(recent):
            previous=window[BASELINE+index-1];volume=r['values']['volume']
            ratio=volume/baseline if volume is not None and baseline is not None and baseline>0 else None
            change=r['values']['close']/previous['values']['close']-1 if r['price_valid'] and previous['price_valid'] else None
            ratios.append(ratio);returns.append(change)
            per_observation.append({'observation_date':r['date'],'source_index':r['source_index'],
                'volume_operand':_operand(r,'volume'),'close_operand':_operand(r,'close'),'previous_close_operand':_operand(previous,'close'),
                'relative_volume':float(ratio) if ratio is not None else None,'relative_volume_exact':str(ratio) if ratio is not None else None,
                'close_change_fraction':float(change) if change is not None else None,'close_change_exact':str(change) if change is not None else None,
                'ratio_status':'measured' if ratio is not None else 'zero_or_missing_baseline' if baseline in (None,0) else 'missing_recent_volume',
                'return_status':'measured' if change is not None else 'missing_or_invalid_price_pair',**FLAGS})
        all_ratios=complete and all(v is not None for v in ratios)
        ratio_reason=unavailable if not complete else 'zero_baseline' if baseline==0 else 'invalid_volume_in_exact_window'
        ratio_operands=baseline_operands+[_operand(r,'volume') for r in recent]
        latest=ratios[-1] if ratios else None
        metric('last_relative_volume','multiple_of_prior_baseline',latest,ratio_reason,
               baseline_operands+([_operand(recent[-1],'volume')] if recent else []),
               'Latest selected completed-date volume divided by the preceding baseline; never borrow an earlier current value.')
        slope=None
        if all_ratios:
            mean=sum(ratios)/RECENT
            slope=sum(Decimal(i-3)*(v-mean) for i,v in enumerate(ratios))/28
        metric('relative_volume_slope_7','baseline_multiples_per_reported_observation',slope,ratio_reason,ratio_operands,
               'OLS slope across exactly seven relative-volume observations at indices 0..6; centered-x sum of squares is 28. This is a descriptive slope, not a forecast.')
        signed=None
        if all_ratios and all(v is not None for v in returns) and sum(ratios)>0:
            signed=sum(v*(1 if change>Decimal('.005') else -1 if change<Decimal('-.005') else 0) for v,change in zip(ratios,returns))/sum(ratios)
        price_operands=[_operand(r,'close') for r in window[BASELINE-1:]] if complete else []
        metric('volume_weighted_close_direction_7','unitless_signed_balance',signed,
               unavailable if not complete else 'incomplete_price_volume_window_or_zero_recent_volume',ratio_operands+price_operands,
               'Relative-volume-weighted sign of each close change above +0.5% or below -0.5%, using its actual preceding close; not investor flows or institutional accumulation.')
        floor=None
        if all_ratios and min(ratios[:3])>0:floor=(min(ratios[3:])-min(ratios[:3]))/min(ratios[:3])
        metric('relative_volume_floor_change','fractional_change',floor,ratio_reason if not all_ratios else 'zero_early_floor',ratio_operands,
               'Relative change from minimum of the first three to minimum of the last four observations. A zero early floor is undefined, not replaced with an arbitrary constant.')
    return {'ticker':ticker,'status':'identity_dates_parsed' if not fatal else 'source_identity_or_dates_invalid',
        'source_records':reported_count,'row_issues':issues,'required_observations':BASELINE+RECENT,
        'selected_observations':len(window),'start_date':window[0]['date'] if window else None,'end_date':window[-1]['date'] if window else None,
        'selected_source_indices':[r['source_index'] for r in window],
        'recent_observations':per_observation,'measurements':metrics,'observation_age_calendar_days':(as_of-day(window[-1]['date'])).days if window else None,
        'exchange_sessions_verified':False,'volume_units_verified':False,'corporate_actions_verified':False,
        'reported_source_family':'issuer_ohlcv','independent_roots':None,
        'composite_score':None,'confirmation':None,'direction':None,**FLAGS}
