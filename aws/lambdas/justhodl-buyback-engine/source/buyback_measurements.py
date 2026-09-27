"""Descriptive provider statement observations. No buyback/forecast score.

Calendar sums require reported start/end dates, four contiguous quarters and
one explicitly reported currency. Unknown durations are never synthetic TTM.
"""
from datetime import date, timedelta
import calendar
import json
import math
import re

CONTRACT='buyback-accounting-measurements.v1'


def decode(raw):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate provider JSON key')
            out[key]=value
        return out
    def invalid(value):raise ValueError('Nonfinite provider JSON')
    def decimal(value):
        result=float(value)
        if not math.isfinite(result) or result==0 and any(c in '123456789' for c in value.lower().split('e')[0]):invalid(value)
        return result
    return json.loads(raw,object_pairs_hook=pairs,parse_float=decimal,parse_constant=invalid)


def number(value):
    if isinstance(value,bool) or not isinstance(value,(int,float)):return None
    try:return value if math.isfinite(value) else None
    except (OverflowError,ValueError):return None


def day(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):return None
    try:return date.fromisoformat(value)
    except ValueError:return None


def shift(value,months):
    year,month=divmod(value.year*12+value.month-1+months,12);month+=1
    if not 1<=year<=9999:return None
    return date(year,month,min(value.day,calendar.monthrange(year,month)[1]))


def reported_number(row,primary,alias=None):
    """Zero is present. A malformed primary cannot be replaced by another field."""
    if row.get(primary) is not None:return number(row[primary]),primary
    if alias and row.get(alias) is not None:return number(row[alias]),alias
    return None,None


def measure_cashflow(row,symbol,index,as_of):
    result={'source_row':index,'original':row,'eligible':False,'issues':[],'metrics':{}}
    if not isinstance(row,dict):result['issues']=['source_row_not_object'];return result
    end=day(row.get('date'));start=day(row.get('startDate'));currency=row.get('reportedCurrency')
    if row.get('symbol')!=symbol:result['issues'].append('explicit_source_symbol_mismatch_or_missing')
    cik=str(row.get('cik','')).lstrip('0')
    if not cik.isdigit() or not cik or len(cik)>10:result['issues'].append('explicit_issuer_cik_missing')
    if end is None or end>as_of:result['issues'].append('invalid_or_future_period_end')
    if not isinstance(currency,str) or not re.fullmatch('[A-Z]{3}',currency):result['issues'].append('reported_currency_missing')
    period=row.get('period')
    if period not in ('Q1','Q2','Q3','Q4'):result['issues'].append('reported_quarter_missing')
    next_start=shift(start,3) if start else None
    aligned=bool(start and start.day==1 and end and next_start and next_start-timedelta(days=1)==end)
    if not aligned:result['issues'].append('reported_quarter_duration_unverified')
    result.update(date=end.isoformat() if end else None,start_date=start.isoformat() if start else None,
                  reported_currency=currency,period=period,cik=cik,eligible=not result['issues'],
                  reported_calendar_duration_aligned=aligned)
    definitions=(('gross_common_repurchases','commonStockRepurchased',None,'magnitude'),
                 ('net_common_repurchases','netCommonStockIssuance',None,'negative'),
                 ('common_issuance','commonStockIssuance',None,'signed'),
                 ('common_dividends','commonDividendsPaid',None,'magnitude'),
                 ('stock_compensation','stockBasedCompensation',None,'signed'),
                 ('net_debt_issuance','netDebtIssuance',None,'signed'),
                 ('operating_cash_flow','operatingCashFlow','netCashProvidedByOperatingActivities','signed'),
                 ('capital_expenditure','capitalExpenditure',None,'magnitude'))
    for key,field,alias,sign in definitions:
        value,source=reported_number(row,field,alias)
        if value is not None:value=abs(value) if sign=='magnitude' else -value if sign=='negative' else value
        result['metrics'][key]={'value':value,'source_field':source,'unit':currency,'sign':sign,
                                'status':'reported_value' if value is not None else 'missing_or_invalid'}
    ocf=result['metrics']['operating_cash_flow']['value'];capex=result['metrics']['capital_expenditure']['value']
    fcf=number(ocf-capex) if ocf is not None and capex is not None else None
    result['metrics']['free_cash_flow']={'value':fcf,'unit':currency,
        'inputs':['operating_cash_flow','capital_expenditure'],'status':'descriptive_difference' if fcf is not None else 'required_input_unavailable'}
    return result


def cashflow_window(observations):
    result={'status':'four_comparable_reported_quarters_unavailable','source_rows':[], 'unit':None,
            'start_date':None,'end_date':None,'metrics':{},'reported_calendar_duration_aligned':False,
            'original_sec_filings_replayed':False}
    # Anchor on the latest received dated observations; do not quietly skip a
    # malformed latest quarter and manufacture a fresh sum from older rows.
    dated=[r for r in observations if r.get('date')]
    if len(dated)!=len(observations):return result
    ordered=sorted(dated,key=lambda r:r['date'],reverse=True)
    if len(ordered)<4:return result
    selected=ordered[:4]
    if len({r['date'] for r in selected})!=4 or any(sum(v['date']==r['date'] for v in ordered)!=1 for r in selected):
        result['status']='duplicate_or_conflicting_periods';return result
    if not all(r['eligible'] for r in selected):return result
    if len({r['reported_currency'] for r in selected})!=1:
        result['status']='mixed_reported_currencies';return result
    if len({r['cik'] for r in selected})!=1:
        result['status']='mixed_reported_issuers';return result
    for newer,older in zip(selected,selected[1:]):
        if (day(older['date'])+timedelta(days=1)!=day(newer['start_date'])
                or int(newer['period'][1])!=int(older['period'][1])%4+1):
            result['status']='noncontiguous_reported_quarters';return result
    result.update(status='four_reported_calendar_quarters',source_rows=[r['source_row'] for r in selected],
                  unit=selected[0]['reported_currency'],start_date=selected[-1]['start_date'],end_date=selected[0]['date'],
                  reported_calendar_duration_aligned=True)
    for key in selected[0]['metrics']:
        values=[r['metrics'][key]['value'] for r in selected]
        try:total=number(sum(values)) if all(v is not None for v in values) else None
        except OverflowError:total=None
        result['metrics'][key]={'value':total,'unit':result['unit'],'source_rows':result['source_rows'],
                                'status':'descriptive_sum' if total is not None else 'required_input_missing_or_invalid'}
    return result


def share_comparison(rows,symbol,as_of):
    result={'status':'exact_annual_share_pair_unavailable','reduction_pct':None,'current':None,'prior':None,
            'split_and_corporate_action_adjusted':False,'dilution_event_verified':False}
    valid=[]
    for i,row in enumerate(rows if isinstance(rows,list) else []):
        if not isinstance(row,dict) or row.get('symbol')!=symbol:continue
        end=day(row.get('date'));value=number(row.get('numberOfShares'))
        if end:valid.append({'source_row':i,'date':end.isoformat(),'value':value if value is not None and value>0 else None,'unit':'reported_shares'})
    if not valid:return result
    latest=max(r['date'] for r in valid);current=[r for r in valid if r['date']==latest]
    if len(current)!=1:result['status']='ambiguous_latest_share_observation';return result
    result['current']=current[0]
    if day(latest)>as_of or current[0]['value'] is None:
        result['status']='latest_share_value_or_date_unavailable';return result
    prior_date=shift(day(latest),-12)
    if prior_date is None:return result
    if day(latest).day==calendar.monthrange(day(latest).year,day(latest).month)[1]:
        prior_date=date(prior_date.year,prior_date.month,calendar.monthrange(prior_date.year,prior_date.month)[1])
    # A leap-day endpoint is paired to the prior February month-end.
    prior=[r for r in valid if r['date']==prior_date.isoformat()]
    if len(prior)!=1 or prior[0]['value'] is None:return result
    value=number((prior[0]['value']-current[0]['value'])/prior[0]['value']*100)
    if value is None:return result
    result.update(status='exact_provider_calendar_year_pair',reduction_pct=round(value,2),prior=prior[0])
    return result


def dossier(symbol,profile_response,cashflow_response,metrics_response,shares_response,as_of):
    today=day(as_of)
    if not today or not re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}',symbol):raise ValueError('Explicit issuer symbol and clock required')
    profiles=[r for r in profile_response if isinstance(r,dict) and r.get('symbol')==symbol] if isinstance(profile_response,list) else []
    profile=profiles[0] if len(profiles)==1 else {}
    cash=cashflow_response if isinstance(cashflow_response,list) else []
    observations=[measure_cashflow(row,symbol,i,today) for i,row in enumerate(cash)]
    window=cashflow_window(observations);shares=share_comparison(shares_response,symbol,today)
    if window['source_rows'] and str(profile.get('cik','')).lstrip('0')!=observations[window['source_rows'][0]]['cik']:
        window=dict(window,status='profile_and_statement_issuer_unaligned',metrics={},reported_calendar_duration_aligned=False)
    metric_rows=[r for r in metrics_response if isinstance(r,dict) and r.get('symbol')==symbol] if isinstance(metrics_response,list) else []
    metric=metric_rows[0] if len(metric_rows)==1 else {}
    market_cap=number(metric.get('marketCap'))
    market_cap=market_cap if market_cap is not None and market_cap>0 else None
    ratio_aligned=bool(window['reported_calendar_duration_aligned'] and market_cap is not None
        and metric.get('date')==window['end_date'] and metric.get('reportedCurrency')==window['unit'])
    def amount(key):return window['metrics'].get(key,{}).get('value')
    def rounded(key):
        value=amount(key);return round(value,0) if value is not None else None
    def ratio(key):
        value=amount(key)
        if value is None or not ratio_aligned:return None
        result=number(value/market_cap*100);return round(result,2) if result is not None else None
    net=ratio('net_common_repurchases');div=ratio('common_dividends')
    sector=profile.get('sector') if isinstance(profile.get('sector'),str) else ''
    industry=profile.get('industry') if isinstance(profile.get('industry'),str) else ''
    financial=('Financial' in sector or any(w in industry for w in ('Bank','Insurance','Capital Markets','Credit','Asset Management','Mortgage','REIT','Healthcare Plans','Financial')))
    last=next((r for r in observations if r['source_row']==window['source_rows'][0]),None) if window['source_rows'] else None
    return {'symbol':symbol,'measurement_contract':CONTRACT,'checked_as_of':as_of,
        'sector':profile.get('sector'),'industry':profile.get('industry'),'company_name':profile.get('companyName'),
        'market_cap':market_cap,'market_cap_asof':metric.get('date'),'market_cap_unit':metric.get('reportedCurrency'),
        'gross_repurchases_ttm':rounded('gross_common_repurchases'),'net_buyback_ttm':rounded('net_common_repurchases'),
        'issuance_ttm':rounded('common_issuance'),'sbc_ttm':rounded('stock_compensation'),
        'gross_buyback_yield':ratio('gross_common_repurchases'),'net_buyback_yield':net,'dividend_yield':div,
        'shareholder_yield':number(round(net+div,2)) if net is not None and div is not None else None,
        'fcf_yield_annualized':None if financial else ratio('free_cash_flow'),'fcf_nm':financial,
        'last_q_repurchase':last['metrics']['gross_common_repurchases']['value'] if last else None,
        'shares_now':(shares['current'] or {}).get('value'),'share_count_reduction_yoy':shares['reduction_pct'],
        'active_execution':None,'debt_funded':None,'net_issuer':None,'extreme':None,
        'call':None,'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False,
        'quality':{'status':'partial','provider_originals_replayed':False,'original_sec_filings_replayed':False,
                   'point_in_time_availability_verified':False,'market_cap_currency_and_date_aligned':ratio_aligned},
        'measurements':{'cashflow_observations':observations,'cashflow_window':window,'reported_shares':shares},
        'provider_responses':{'profile':profile_response,'cash_flow':cashflow_response,'key_metrics':metrics_response,'enterprise_values':shares_response},
        'measurement_limits':'Legacy TTM fields require four reported contiguous calendar-quarter start/end pairs in one currency. Ratios also require explicit matched-date/currency market cap. Provider statements are not original SEC replay. Share changes are not split-adjusted dilution events; cash flows do not prove current execution, debt funding or future performance.'}
