"""Descriptive settlement-position reader; no transport or decision authority."""
from datetime import date
from decimal import Decimal,InvalidOperation
import math,re

FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
TRUSTED_DTC=('matches_reconstructed_rounded_ratio','provider_display_floor_one')
RECONSTRUCTED_DTC=('provider_999_99_convention_unconfirmed','provider_differs_from_reconstructed_ratio')

def decimal_number(value,integer=False,nonnegative=False):
    if isinstance(value,bool) or value is None:return None
    if not isinstance(value,(str,int,float,Decimal)):return None
    try:text=str(value).strip()
    except (ValueError,OverflowError):return None
    if len(text)>80 or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?',text):return None
    try:
        exact=Decimal(text)
        if not exact.is_finite() or nonnegative and exact<0:return None
        if integer and (exact!=exact.to_integral_value() or abs(exact)>9007199254740991):return None
        result=float(exact)
        if not math.isfinite(result) or result==0 and exact!=0:return None
        return exact
    except (InvalidOperation,ValueError,OverflowError,ArithmeticError):return None

def number(value,integer=False,nonnegative=False):
    exact=decimal_number(value,integer=integer,nonnegative=nonnegative)
    return None if exact is None else int(exact) if integer else float(exact)


def calendar(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):return None
    try:return date.fromisoformat(value).isoformat()
    except ValueError:return None

def row_context(symbol,row):
    row=row if isinstance(row,dict) else {}
    status=row.get('dtc_status') if isinstance(row.get('dtc_status'),str) else None
    reported=number(row.get('days_to_cover'),nonnegative=True)
    effective=number(row.get('dtc_effective'),nonnegative=True)
    reconstructed=number(row.get('dtc_reconstructed'),nonnegative=True)
    # A provider display is not an automatic substitute for a reconciled value.
    # Both remain reported descriptive claims, never a covering forecast.
    effective_exact=decimal_number(row.get('dtc_effective'),nonnegative=True)
    if status in TRUSTED_DTC and reported is not None and effective_exact==decimal_number(row.get('days_to_cover'),nonnegative=True):
        selected=effective;basis='producer_reconciled_provider_display'
    elif status in RECONSTRUCTED_DTC and effective is not None and effective_exact==decimal_number(row.get('dtc_reconstructed'),nonnegative=True):
        selected=effective;basis='producer_reconstructed_ratio'
    else:selected=None;basis='ratio_reconciliation_unavailable'
    return {'ticker':symbol,'settlement_date':calendar(row.get('settlement_date')),
        'latest_reported':row.get('latest') if type(row.get('latest')) is bool else None,
        'short_interest_shares':number(row.get('short_interest'),integer=True,nonnegative=True),
        'change_shares':number(row.get('change_shares'),integer=True),'si_change_pct':number(row.get('change_pct')),
        'days_to_cover':selected,'days_to_cover_basis':basis,'reported_days_to_cover':reported,'reported_reconstructed_ratio':reconstructed,'reported_dtc_status':status,
        'si_pct_float':None,'short_utilization':None,'borrow_rate':None,'signal':None,'score':None,
        'observation_freshness_verified':False,'identity_verified':False,**dict.fromkeys(FLAGS,False)}

def descriptive_context(packet):
    out={'contract':'short-position-consumer-context.v1','status':'unavailable','by_ticker':{},'ambiguous_symbols':[],
         'unresolved_occurrences':[],'source_contract':None,'source_occurrences':0,'independent_investment_votes':0,
         'reason':'Reported settlement positions do not establish percentage of float, borrow utilization, covering demand or squeeze probability.',**dict.fromkeys(FLAGS,False)}
    if not isinstance(packet,dict):return out
    out['source_contract']=packet.get('contract') if isinstance(packet.get('contract'),str) else None
    rows=packet.get('by_ticker')
    if out['source_contract']!='short-interest-tickers.v1' or not isinstance(rows,dict):return out
    grouped={}
    for original,row in rows.items():
        out['source_occurrences']+=1
        if not isinstance(original,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.:-]{0,31}',original) or not isinstance(row,dict):
            out['unresolved_occurrences'].append({'reported_key':original if isinstance(original,str) else None,'key_type':type(original).__name__,'reason':'Invalid ticker identity or row shape'});continue
        symbol=original.upper();declared=row.get('ticker')
        if declared is not None and (not isinstance(declared,str) or declared.upper()!=symbol):
            out['unresolved_occurrences'].append({'reported_key':original,'reason':'Ticker identity disagreement'});continue
        context=row_context(symbol,row)
        context['source_row']='/by_ticker/'+original.replace('~','~0').replace('/','~1')
        context['reported_ticker_key']=original
        grouped.setdefault(symbol,[]).append(context)
    for symbol,occurrences in grouped.items():
        if len(occurrences)!=1:
            out['ambiguous_symbols'].append({'ticker':symbol,'occurrences':occurrences});continue
        out['by_ticker'][symbol]=occurrences[0]
    out['status']='descriptive_only'
    return out
