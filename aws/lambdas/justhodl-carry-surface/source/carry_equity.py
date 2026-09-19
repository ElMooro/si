"""Provider-reported income and split reconciliation, never an expected return."""
from datetime import date,timedelta
from decimal import Decimal,InvalidOperation,localcontext,ROUND_HALF_EVEN
from collections import Counter
import re

def decimal(value,positive=False,nonnegative=False):
    if isinstance(value,bool) or value is None or len(str(value))>60:raise ValueError('finite original number required')
    try:n=Decimal(str(value))
    except InvalidOperation:raise ValueError('invalid original number')
    if not n.is_finite() or abs(n)>Decimal('1e18') or positive and n<=0 or nonnegative and n<0:raise ValueError('original numeric domain differs')
    return n

def day(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError('ISO date required')
    return date.fromisoformat(value)

def records(document,symbol):
    if not isinstance(document,list) or len(document)>20000:raise ValueError('provider array bound')
    if any(not isinstance(r,dict) or r.get('symbol')!=symbol for r in document):raise ValueError('provider security identity differs')
    return document

def unique(rows):
    result={}
    for index,r in enumerate(rows):
        d=day(r['date']).isoformat()
        if d in result:raise ValueError('duplicate provider date')
        result[d]=(index,r)
    return result

def split_factor(splits,start,end):
    factor=Decimal(1);used=[]
    for r in splits:
        if start<r['date']<=end:
            factor*=Decimal(r['numerator_decimal'])/Decimal(r['denominator_decimal']);used.append(r['row_index'])
    return factor,used

def annualized_volatility(sample):
    """Portable arithmetic: no operating-system libm or float summation order."""
    with localcontext() as ctx:
        ctx.prec=42;ctx.rounding=ROUND_HALF_EVEN
        values=[Decimal(r['value_decimal']) for r in sample]
        returns=[(b/a).ln() for a,b in zip(values,values[1:])]
        if len(returns)<2:raise ValueError('sample volatility requires two returns')
        mean=sum(returns)/len(returns)
        variance=sum((r-mean)**2 for r in returns)/(len(returns)-1)
        return format((variance.sqrt()*Decimal(252).sqrt()*100).quantize(Decimal('.00000001')),'f')

def compile_equity(symbol,documents,as_of):
    """Documents must already be independently verified against retained responses.

    `as_of` is a completed US session cutoff. Source retrieval time is not a price date.
    Histories are provider-returned records, not asserted exhaustive corporate-action
    histories or historical point-in-time knowledge. All source row indices survive.
    """
    day(as_of)
    docs={k:records(v,symbol) for k,v in documents.items()}
    profile=docs['profile']
    if len(profile)!=1:raise ValueError('one security profile required')
    profile=profile[0]
    if profile.get('currency')!='USD' or profile.get('isActivelyTrading') is not True:raise ValueError('active USD quotation identity required')
    if not isinstance(profile.get('isEtf'),bool):raise ValueError('security type missing')
    identity={k:profile.get(k) for k in ('symbol','isin','cusip','cik','currency','exchange','country','isEtf','isFund','isActivelyTrading')}
    splits=[]
    for d,(i,r) in sorted(unique(docs['splits']).items()):
        a,b=decimal(r['numerator'],positive=True),decimal(r['denominator'],positive=True)
        if a!=a.to_integral_value() or b!=b.to_integral_value():raise ValueError('integral split ratio required')
        splits.append({'date':d,'numerator_decimal':str(a),'denominator_decimal':str(b),'row_index':i,
                       'status':'effective_by_cutoff' if d<=as_of else 'future_announced'})
    light=unique(docs['historical-price-eod/light']);raw=unique(docs['historical-price-eod/non-split-adjusted'])
    prices=[];future=[];reconciliation=[]
    for d,(i,r) in sorted(raw.items()):
        close=decimal(r['adjClose'],positive=True)
        # The endpoint explicitly says non-split-adjusted despite its adjClose field name.
        factor,split_rows=split_factor(splits,d,as_of);adjusted=close/factor
        match=light.get(d);reported=decimal(match[1]['price'],positive=True) if match else None
        difference=reported-adjusted if reported is not None else None
        agreed=difference is not None and abs(difference)<=Decimal('.005')
        row={'date':d,'value_decimal':str(adjusted),'raw_close_decimal':str(close),'split_factor_decimal':str(factor),
          'split_rows':split_rows,'row_index':i,'light_row_index':match[0] if match else None,
          'provider_adjusted_decimal':str(reported) if reported is not None else None,
          'reconciliation_difference_decimal':str(difference) if difference is not None else None,
          'status':'split_reconciled' if agreed else 'price_adjustment_unresolved'}
        (future if d>as_of else prices).append(row)
        if d<=as_of and not agreed:reconciliation.append(d)
    if not prices:raise ValueError('completed-session price unavailable')
    latest=prices[-1];price=Decimal(latest['value_decimal']);anchor=latest['date'];cutoff=(day(anchor)-timedelta(days=365)).isoformat()
    distributions=[];selected=[];paid=[];unpaid=[];dividend_mismatches=[]
    dividend_dates=Counter(day(r['date']).isoformat() for r in docs['dividends'])
    for i,r in sorted(enumerate(docs['dividends']),key=lambda v:(v[1]['date'],v[0])):
        d=r['date'];duplicate=dividend_dates[d]>1
        declared=decimal(r['dividend'],nonnegative=True);factor,used=split_factor(splits,d,anchor);normalized=declared/factor
        adjusted=decimal(r['adjDividend'],nonnegative=True) if r.get('adjDividend') is not None else None
        # FMP adjusted dividends may round to five decimal places. Retain both numbers.
        tolerance=min(Decimal('.000005'),Decimal(1).scaleb(adjusted.as_tuple().exponent)/2) if adjusted is not None else None
        agreed=adjusted is not None and abs(adjusted-normalized)<=tolerance
        payment=r.get('paymentDate') or None
        if payment is not None:day(payment)
        if payment and payment<d:raise ValueError('payment precedes ex date')
        row={'date':d,'row_index':i,'raw_dividend_decimal':str(declared),'value_decimal':str(normalized),
          'provider_adjusted_decimal':str(adjusted) if adjusted is not None else None,'split_factor_decimal':str(factor),
          'split_rows':used,'payment_date':payment,'record_date':r.get('recordDate') or None,
          'declaration_date':r.get('declarationDate') or None,'frequency':r.get('frequency'),
          'status':'ambiguous_duplicate_ex_date' if duplicate else 'split_reconciled' if agreed else 'distribution_adjustment_unresolved',
          'rounding_tolerance_decimal':str(tolerance) if tolerance is not None else None,
          'payment_status':'date_unavailable' if not payment else 'paid_by_price_date' if payment<=anchor else 'payment_after_price_date'}
        distributions.append(row)
        if cutoff<d<=anchor:
            selected.append(row)
            if not agreed or duplicate:dividend_mismatches.append(d)
            if payment is None or payment>anchor:unpaid.append(d)
        if payment and cutoff<payment<=anchor:paid.append(row)
    coverage=bool(distributions and distributions[0]['date']<=cutoff)
    valid=coverage and bool(selected) and not dividend_mismatches and latest['status']=='split_reconciled'
    total=sum((Decimal(r['value_decimal']) for r in selected),Decimal(0)) if valid else None
    yield_pct=100*total/price if total is not None else None
    paid_valid=coverage and bool(paid) and all(r['status']=='split_reconciled' for r in paid)
    paid_total=sum((Decimal(r['value_decimal']) for r in paid),Decimal(0)) if paid_valid else None
    ratio=docs.get('ratios-ttm',[]);reported_ratio=None
    if len(ratio)>1:raise ValueError('multiple TTM ratio records')
    if ratio and ratio[0].get('dividendYieldTTM') is not None:reported_ratio=decimal(ratio[0]['dividendYieldTTM'],nonnegative=True)
    volatility={}
    for n in (20,60,252):
        sample=prices[-n-1:]
        usable=len(sample)==n+1 and all(r['status']=='split_reconciled' for r in sample) and all(1<=(day(b['date'])-day(a['date'])).days<=6 for a,b in zip(sample,sample[1:]))
        annualized=annualized_volatility(sample) if usable else None
        volatility[str(n)]={'returns':n if usable else 0,'annualized_pct':float(annualized) if annualized is not None else None,
          'annualized_pct_decimal':annualized,'arithmetic':'Decimal ln, sample variance and sqrt at 42 digits; round-half-even to 8 decimal percentage places.',
          'from':sample[0]['date'] if sample else None,'to':sample[-1]['date'] if sample else None,
          'basis':'Sample log-return standard deviation on independently split-adjusted closes; 252-session annualization. Not dividend total return, a Sharpe ratio or a forecast.'}
    return {'symbol':symbol,'identity':identity,'as_of':anchor,'price_decimal':str(price),'price_row_index':latest['row_index'],
      'price_adjustment_status':latest['status'],'prices':prices,'future_or_incomplete_session_prices':future,'splits':splits,'distributions':distributions,
      'trailing_distribution':{'status':'reported_window_comparison' if valid else 'unqualified',
        'window_start_exclusive':cutoff,'window_end_inclusive':anchor,'date_basis':'ex_date_entitlement_not_payment_date',
        'current_share_distribution_decimal':str(total) if total is not None else None,'yield_pct_decimal':str(yield_pct) if yield_pct is not None else None,
        'selected_rows':[r['row_index'] for r in selected],'record_count':len(selected),'endpoint_window_supported':coverage,
        'unpaid_or_undated_selected_ex_dates':unpaid,'adjustment_mismatch_dates':dividend_mismatches,
        'currency_basis':'Provider-reported distributions for the USD-quoted listing; individual distribution records have no currency field. Not independent currency confirmation.',
        'limitation':'Trailing provider-reported ex-date distributions scaled to current shares. Not forecast distributions, realized holding-period income, or a standardized SEC yield.'},
      'paid_distribution_window':{'current_share_distribution_decimal':str(paid_total) if paid_total is not None else None,
        'selected_rows':[r['row_index'] for r in paid],'date_basis':'payment_date; current-share normalization, not actual account receipts'},
      'reported_ttm_ratio':{'decimal_fraction':str(reported_ratio) if reported_ratio is not None else None,
        'percent_decimal':str(100*reported_ratio) if reported_ratio is not None else None,'period_end':None,'status':'provider_reported_period_undated',
        'note':'Explicit zero remains zero. This undated provider ratio is not substituted for a missing verified distribution history.'},
      'price_adjustment_mismatch_dates':reconciliation,'ambiguous_distribution_dates':[d for d,n in dividend_dates.items() if n>1],
      'realized_price_volatility':volatility,
      'history_coverage':'All returned records retained. Provider array has no total-count proof; not an all-time or historical publication-time archive.',
      'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'expected_return':None,'carry_per_vol':None,
      'buyback_yield':None,'buyback_status':'No reviewed dated repurchase numerator/market-cap denominator; original key-metrics response retained separately.'}
