"""Comparable reported accounting windows; no quality rank or investment ticket."""
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
import calendar
import json
import math
import re

CONTRACT = 'earnings-accounting-measurements.v1'
ENDPOINTS = ('income', 'cash_flow', 'balance_sheet')
FIELDS = {'income': ('netIncome','revenue','grossProfit'),
          'cash_flow': ('operatingCashFlow','capitalExpenditure','netCashProvidedByOperatingActivities'),
          'balance_sheet': ('totalAssets','netReceivables')}


def number(value):
    if isinstance(value,bool) or not isinstance(value,(int,float)):return None
    try:return value if math.isfinite(value) else None
    except (ValueError,OverflowError):return None


def decode(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('Duplicate provider key')
            out[k]=v
        return out
    def invalid(v):raise ValueError('Invalid provider number')
    def decimal(v):
        n=float(v)
        if not math.isfinite(n) or n==0 and any(c in '123456789' for c in v.lower().split('e')[0]):invalid(v)
        return n
    if isinstance(raw,bytes):raw=raw.decode('utf-8')
    return json.loads(raw,object_pairs_hook=pairs,parse_float=decimal,parse_constant=invalid)


def day(v):
    if not isinstance(v,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',v):return None
    try:return date.fromisoformat(v)
    except ValueError:return None


def shift(d,months):
    y,m=divmod(d.year*12+d.month-1+months,12);m+=1
    return date(y,m,min(d.day,calendar.monthrange(y,m)[1])) if 1<=y<=9999 else None


def calculated(values,operation):
    if any(number(v) is None for v in values):return None
    try:
        with localcontext() as ctx:
            ctx.prec=40
            result=operation(*[Decimal(str(v)) for v in values])
            value=float(result)
            if not math.isfinite(value) or value==0 and result!=0:return None
            return value
    except (ArithmeticError,ValueError,OverflowError):return None


def observation(symbol,endpoint,row,index,today):
    out={'source_row':index,'original':row,'date':None,'start_date':None,'identity':None,'issues':[],
         'values':{},'duration_verified':False}
    if not isinstance(row,dict):out['issues']=['row_not_object'];return out
    end=day(row.get('date'));start=day(row.get('startDate'));filing=day(row.get('filingDate'))
    cik=str(row.get('cik','')).lstrip('0');unit=row.get('reportedCurrency');period=row.get('period')
    fiscal=str(row.get('fiscalYear',''));accepted=row.get('acceptedDate')
    try:
        accepted_day=datetime.fromisoformat(accepted.replace('Z','+00:00')).date() if isinstance(accepted,str) and len(accepted)>=16 else None
    except ValueError:accepted_day=None
    if row.get('symbol')!=symbol:out['issues'].append('explicit_symbol_mismatch_or_missing')
    if not cik or not cik.isdigit() or len(cik)>10:out['issues'].append('explicit_cik_missing')
    if not isinstance(unit,str) or not re.fullmatch('[A-Z]{3}',unit):out['issues'].append('currency_missing')
    if not end or end>today:out['issues'].append('invalid_or_future_end')
    if not filing or not end or not end<=filing<=today or not accepted_day or not end<=accepted_day<=today:
        out['issues'].append('filing_clock_missing_or_invalid')
    if period not in ('Q1','Q2','Q3','Q4') or not re.fullmatch(r'\d{4}',fiscal):out['issues'].append('fiscal_identity_missing')
    following=shift(start,3) if start else None
    duration=bool(start and start.day==1 and following and end and following-timedelta(days=1)==end)
    if endpoint!='balance_sheet' and not duration:out['issues'].append('explicit_quarter_duration_missing')
    values={key:number(row.get(key)) for key in FIELDS[endpoint]}
    out.update(date=end.isoformat() if end else None,start_date=start.isoformat() if start else None,
               identity={'cik':cik,'currency':unit,'period':period,'fiscal_year':fiscal,
                         'filing_date':row.get('filingDate'),'accepted_at':accepted},
               values=values,duration_verified=duration)
    if endpoint=='cash_flow':
        out['operating_cash_alias_residual']=calculated([values['operatingCashFlow'],values['netCashProvidedByOperatingActivities']],lambda a,b:a-b)
    return out


def ordered(rows):
    if not rows or any(not row['date'] for row in rows):return []
    return sorted(rows,key=lambda row:row['date'],reverse=True)


def window(rows,offset):
    result={'status':'four_comparable_calendar_quarters_unavailable','source_rows':[],
            'start_date':None,'end_date':None,'currency':None,'cik':None,'amounts':{},'aligned':False}
    records=ordered(rows);chosen=records[offset:offset+4]
    if len(chosen)!=4 or any(r['issues'] for r in chosen):return result
    if any(sum(v['date']==r['date'] for v in records)!=1 for r in chosen):result['status']='duplicate_or_conflicting_periods';return result
    ids=[r['identity'] for r in chosen]
    if len({(i['cik'],i['currency']) for i in ids})!=1:result['status']='mixed_issuer_or_currency';return result
    for newer,older in zip(chosen,chosen[1:]):
        a,b=newer['identity'],older['identity'];q=int(b['period'][1])
        if day(older['date'])+timedelta(days=1)!=day(newer['start_date']) or int(a['period'][1])!=q%4+1 or int(a['fiscal_year'])!=int(b['fiscal_year'])+(q==4):
            result['status']='noncontiguous_quarters';return result
    amounts={key:calculated([r['values'][key] for r in chosen],lambda *v:sum(v)) for key in chosen[0]['values']}
    result.update(status='four_explicit_comparable_quarters',source_rows=[r['source_row'] for r in chosen],
                  start_date=chosen[-1]['start_date'],end_date=chosen[0]['date'],currency=ids[0]['currency'],cik=ids[0]['cik'],
                  amounts=amounts,aligned=True)
    return result


def aligned_flows(income,cash,observations):
    if not income['aligned'] or not cash['aligned']:return False
    if any(income[k]!=cash[k] for k in ('start_date','end_date','currency','cik')):return False
    for i,c in zip(income['source_rows'],cash['source_rows']):
        a,b=observations['income'][i],observations['cash_flow'][c]
        if a['date']!=b['date'] or a['start_date']!=b['start_date'] or a['identity']!=b['identity']:return False
    return True


def balance(rows,end,flow,flow_observation=None):
    candidates=[r for r in rows if r['date']==end]
    if len(candidates)!=1:return None
    row=candidates[0];identity=row['identity']
    if row['issues'] or identity['currency']!=flow['currency'] or identity['cik']!=flow['cik']:return None
    if flow_observation and identity!=flow_observation['identity']:return None
    return row


def dossier(symbol,acquisitions,as_of):
    today=day(as_of)
    if not today:raise ValueError('Explicit accounting check date required')
    observations={}
    for endpoint in ENDPOINTS:
        source=acquisitions.get(endpoint,{}).get('response')
        observations[endpoint]=[observation(symbol,endpoint,row,i,today) for i,row in enumerate(source)] if isinstance(source,list) else []
    windows={name:{endpoint:window(observations[endpoint],offset) for endpoint in ('income','cash_flow')} for name,offset in (('current',0),('prior',4))}
    for group in windows.values():group['aligned']=aligned_flows(group['income'],group['cash_flow'],observations)
    current,prior=windows['current'],windows['prior'];a=current['income'];b=current['cash_flow']
    amounts={};metrics={};balances={};unit=a['currency'] if current['aligned'] else None
    if current['aligned']:
        ni=a['amounts']['netIncome'];ocf=b['amounts']['operatingCashFlow'];capex=b['amounts']['capitalExpenditure']
        amounts={'net_income':ni,'revenue':a['amounts']['revenue'],'gross_profit':a['amounts']['grossProfit'],
                 'operating_cash_flow':ocf,'capital_expenditure_reported':capex,
                 'free_cash_flow_derived':calculated([ocf,capex],lambda x,y:x-abs(y)),
                 'earnings_cash_gap':calculated([ni,ocf],lambda x,y:x-y)}
        current_row=observations['income'][a['source_rows'][0]]
        end=balance(observations['balance_sheet'],a['end_date'],a,current_row)
        begin_date=(day(a['start_date'])-timedelta(days=1)).isoformat()
        begin=balance(observations['balance_sheet'],begin_date,a)
        balances={'ending':end['source_row'] if end else None,'beginning':begin['source_row'] if begin else None}
        assets=end['values']['totalAssets'] if end else None
        prior_assets=begin['values']['totalAssets'] if begin else None
        average=calculated([assets,prior_assets],lambda x,y:(x+y)/2) if assets is not None and assets>0 and prior_assets is not None and prior_assets>0 else None
        metrics['cash_conversion_ratio']=calculated([ocf,ni],lambda x,y:x/y) if ni is not None and ni>0 else None
        metrics['earnings_cash_gap_pct_end_assets']=calculated([amounts['earnings_cash_gap'],assets],lambda x,y:100*x/y) if assets is not None and assets>0 else None
        metrics['cash_flow_accruals_pct_average_assets']=calculated([amounts['earnings_cash_gap'],average],lambda x,y:100*x/y) if average is not None and average>0 else None
        old=prior['income']
        annual=bool(prior['aligned'] and a['cik']==old['cik'] and a['currency']==old['currency']
                    and shift(day(a['start_date']),-12)==day(old['start_date'])
                    and day(old['end_date'])+timedelta(days=1)==day(a['start_date']))
        if annual:
            old_row=observations['income'][old['source_rows'][0]]
            old_balance=balance(observations['balance_sheet'],old['end_date'],old,old_row)
            revenue=a['amounts']['revenue'];prev_revenue=old['amounts']['revenue']
            gp=a['amounts']['grossProfit'];prev_gp=old['amounts']['grossProfit']
            ar=end['values']['netReceivables'] if end else None;prev_ar=old_balance['values']['netReceivables'] if old_balance else None
            if revenue is not None and revenue>0 and prev_revenue is not None and prev_revenue>0:
                metrics['dsri_reported']=calculated([ar,revenue,prev_ar,prev_revenue],lambda x,y,u,v:(x/y)/(u/v)) if ar is not None and ar>=0 and prev_ar is not None and prev_ar>0 else None
                metrics['gmi_reported']=calculated([gp,revenue,prev_gp,prev_revenue],lambda x,y,u,v:(u/v)/(x/y)) if gp is not None and gp>0 and prev_gp is not None and prev_gp>=0 else None
        current['annual_prior_aligned']=annual
    quote=acquisitions.get('quote',{}).get('response');q=quote[0] if isinstance(quote,list) and quote and isinstance(quote[0],dict) else {}
    return {'ticker':symbol,'name':q.get('name') if isinstance(q.get('name'),str) else None,
            'measurement_contract':CONTRACT,'checked_as_of':as_of,'acquisitions':acquisitions,
            'statement_observations':observations,'windows':windows,'amounts':amounts,'measurements':metrics,
            'balance_source_rows':balances,'reported_currency':unit,'as_of':a['end_date'] if current['aligned'] else None,
            'status':'aligned_reported_accounting_windows' if current['aligned'] else 'accounting_alignment_unavailable',
            'quality_score':None,'sloan_accruals_pct_assets':None,'cash_conversion_ratio':None,'dsri_beneish':None,'gmi_beneish':None,
            'ttm_ni_usd':None,'ttm_ocf_usd':None,'ttm_fcf_usd':None,'accruals_change_yoy_pct':None,
            'call':None,'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False,
            'limits':'Reported accounting windows only. No original HTTP/SEC replay, first-release reconstruction, accounting audit, Sloan replication, Beneish M-score, quality rank, return forecast or sizing authority. Quote market-cap gate is legacy sampling context; currency/date alignment is unverified.'}
