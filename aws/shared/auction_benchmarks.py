"""Dated nominal Treasury benchmark comparisons, never security returns.

Pure calculations on the complete supplied series. Acquisition/first-release
vintages are not verified here; no forecasting or portfolio authority is granted.
"""
from datetime import date, timedelta
import math
import re
from treasury_instruments import instrument_fields

CONTRACT = 'auction-benchmark-observations.v1'
TENORS = ((31,'DGS1MO'),(95,'DGS3MO'),(190,'DGS6MO'),(370,'DGS1'),(730,'DGS2'),
          (1095,'DGS3'),(1825,'DGS5'),(2555,'DGS7'),(3650,'DGS10'),(7300,'DGS20'),(11000,'DGS30'))


def number(value):
    if type(value) not in (str, int, float): return None
    text = str(value).strip()
    if len(text)>128 or not re.fullmatch(r'[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?',text): return None
    try: result=float(text)
    except (ValueError, OverflowError): return None
    return result if math.isfinite(result) else None


def term_days(term):
    if not isinstance(term,str) or len(term)>80 or term.lstrip().startswith(('-', '+')): return None
    text=term.upper().replace('-',' ').strip()
    pattern=r'(\d+)\s+(DAY|WEEK|MONTH|YEAR)S?\b'
    parts=list(re.finditer(pattern,text))
    if not parts or re.sub(pattern,'',text).strip(): return None
    units=[p.group(2) for p in parts]
    if len(set(units))!=len(units): return None
    days=sum(int(p.group(1))*{'DAY':1,'WEEK':7,'MONTH':30,'YEAR':365}[p.group(2)] for p in parts)
    return days if 0<days<=50*365 else None


def calendar_identity(record):
    """Use explicit provider flags, retaining original fields outside this helper."""
    def value(camel,snake): return record[camel] if camel in record else record.get(snake)
    normalized={'securityType':value('securityType','security_type'),
                'tips':value('tips','inflation_index_security'),
                'floatingRate':value('floatingRate','floating_rate'),
                'originalSecurityTerm':value('originalSecurityTerm','original_security_term')}
    fields=instrument_fields(normalized,'treasurydirect');kind=fields['instrument_kind']
    days=term_days(value('securityTerm','security_term'))
    bucket=('tips' if kind=='TIPS' else 'frn' if kind=='FRN' else 'unknown' if days is None else
            ('bills_lt_90d' if days<90 else 'bills_gte_90d') if kind=='BILL' else
            ('coupons_lt_3y' if days<=1095 else 'coupons_gt_3y') if kind=='NOMINAL_COUPON' else 'unknown')
    return {**fields,'tenor_bucket':bucket,'term_days_approximate':days}


def series_for(row, allow_bill=False):
    kinds=('NOMINAL_COUPON','BILL') if allow_bill else ('NOMINAL_COUPON',)
    if (row.get('instrument_contract')!='treasury-instrument.v1' or
        row.get('instrument_classification_status')!='verified' or row.get('instrument_kind') not in kinds): return None
    if not allow_bill and row.get('quote_basis')!='nominal_yield_pct': return None
    days=term_days(row.get('security_term'))
    if days is None:return None
    return next((series for maximum,series in TENORS if days<=maximum),None)


def day(value):
    if not isinstance(value,str):return None
    try: result=date.fromisoformat(value)
    except ValueError:return None
    return result if result.isoformat()==value else None


def history_rows(values, cutoff):
    if not isinstance(values,dict):return [],'missing_series'
    rows=[]
    for label,value in values.items():
        observed=day(label);measurement=number(value)
        if observed is None or measurement is None:return [],'invalid_series_observation'
        if observed<=cutoff:rows.append((observed,measurement))
    rows.sort()
    return rows, None if rows else 'no_available_observations'


def context(status,series,**extra):
    return {'contract':CONTRACT,'status':status,'series_id':series,'unit':'bp',
            'source_capture_verified':False,'historical_point_in_time_verified':False,
            'forecast_eligible':False,'calls_eligible':False,'sizing_eligible':False,
            'series_mapping':'ceiling bucket of approximate remaining term; not a same-security quote',**extra}


def preauction(upcoming, histories, today):
    out=[]
    for row in upcoming:
        series=series_for(row,allow_bill=True)
        result={**row,'concession_series':series,'concession_5d_bp':None,'concession_1d_bp':None,
                'concession_today_yield':None,'concession_today_date':None,'concession_regime':'NO_DATA',
                'concession_interpretation':'Comparable dated nominal benchmark observations are unavailable.'}
        rows,error=history_rows(histories.get(series),today) if series else ([], 'incomparable_instrument')
        latest=rows[-1] if rows else None
        if latest and (today-latest[0]).days>5:error='stale_latest_observation'
        comparisons={}
        if not error:
            result.update(concession_today_date=latest[0].isoformat(),concession_today_yield=latest[1])
            for steps,max_span in ((1,5),(5,14)):
                previous=rows[-steps-1] if len(rows)>steps else None
                valid=previous is not None and (latest[0]-previous[0]).days<=max_span
                change=round((latest[1]-previous[1])*100,1) if valid else None
                if change is not None and not math.isfinite(change):change=None;valid=False
                comparisons[str(steps)]={'observation_steps':steps,'start_date':previous[0].isoformat() if previous else None,
                    'end_date':latest[0].isoformat(),'start_yield_pct':previous[1] if previous else None,'end_yield_pct':latest[1],
                    'calendar_days':(latest[0]-previous[0]).days if previous else None,
                    'max_calendar_span':max_span,'status':'complete' if valid else 'insufficient_or_gapped_observations','change_bp':change}
                result['concession_'+str(steps)+'d_bp']=change
            change=result['concession_5d_bp']
            if change is not None:
                result['concession_regime']=('HEAVY_CONCESSION' if change>=5 else 'CONCESSION' if change>=2 else
                    'STRONG_RALLY' if change<=-5 else 'RALLY' if change<=-2 else 'FLAT')
            result['concession_interpretation']='Dated nominal benchmark yield changes over available observations; not a same-security concession, investor motive or an auction forecast.'
        status=error or ('complete' if all(result['concession_'+str(n)+'d_bp'] is not None for n in (1,5)) else 'partial')
        result['concession_measurement']=context(status,series,comparisons=comparisons,
            calculation_as_of=today.isoformat(),maximum_latest_age_calendar_days=5,
            formula='100 * (latest benchmark yield percent - prior benchmark yield percent)',
            legacy_key_note='1d and 5d keys are retained; they now denote one and five available observation intervals, with actual dates.')
        out.append(result)
    return out


def postissue(auctions,histories,today,max_lookback_days=60):
    out=[]
    for row in auctions:
        series=series_for(row);issued=day(row.get('issue_date'));auctioned=day(row.get('auction_date'));high=number(row.get('high_rate'))
        result={**row,'postissue_series':series,'postissue_1d_bp':None,'postissue_5d_bp':None,'postissue_30d_bp':None,
                'postissue_classification':'UNAVAILABLE'}
        error=('incomparable_instrument' if not series else 'invalid_auction_or_issue_date' if issued is None or auctioned is None or issued<auctioned
               else 'missing_auction_yield' if high is None else 'issue_pending' if issued>today
               else 'outside_comparison_window' if (today-issued).days>max_lookback_days else None)
        rows,source_error=history_rows(histories.get(series),today) if not error else ([],None)
        error=error or source_error;comparisons={}
        if not error:
            for horizon in (1,5,30):
                target=issued+timedelta(days=horizon)
                observed=next((v for v in rows if target<=v[0]<=target+timedelta(days=5)),None)
                change=round((high-observed[1])*100,1) if observed else None
                if change is not None and not math.isfinite(change):change=None
                status='complete' if change is not None else 'pending' if target>today else 'missing_bounded_endpoint'
                comparisons[str(horizon)]={'target_date':target.isoformat(),'observation_date':observed[0].isoformat() if observed else None,
                    'benchmark_yield_pct':observed[1] if observed else None,'auction_yield_pct':high,
                    'auction_date':auctioned.isoformat(),'issue_date':issued.isoformat(),
                    'maximum_endpoint_delay_calendar_days':5,'status':status,'change_bp':change}
                result['postissue_'+str(horizon)+'d_bp']=change
            result['postissue_classification']='DESCRIPTIVE_ONLY' if any(v['status']=='complete' for v in comparisons.values()) else 'PENDING'
        result['postissue_measurement']=context(error or ('complete' if all(v['status']=='complete' for v in comparisons.values()) else 'partial'),series,
            comparisons=comparisons,calculation_as_of=today.isoformat(),
            formula='100 * (nominal auction yield percent - dated constant-maturity benchmark yield percent)',
            limitations='Different instruments and quote times. This is neither a security return nor a realized investment outcome. Original FRED response/vintage replay remains unverified.')
        out.append(result)
    return out
