"""Exact annual-estimate observations; no score, event-period join or trade authority."""
from datetime import date, datetime, timezone
import base64
import hashlib
import json
import math
from decimal import Decimal

CONTRACT='estimate-observations.v1'
FIELDS=('epsAvg','epsLow','epsHigh','revenueAvg','revenueLow','revenueHigh','numAnalystsEps','numAnalystsRevenue')


def number(value):
    if type(value) not in (int,float):return None
    try:return value if math.isfinite(value) else None
    except OverflowError:return None


def strict(raw):
    def pairs(values):
        out={}
        for key,value in values:
            if key in out:raise ValueError('Duplicate JSON member')
            out[key]=value
        return out
    def real(value):
        out=float(value)
        if not math.isfinite(out) or (out==0 and Decimal(value)!=0):raise ValueError('Unrepresentable JSON number')
        return out
    def constant(_):raise ValueError('Nonfinite JSON')
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=real,parse_constant=constant)


def day(value):
    try:
        if not isinstance(value,str) or len(value)!=10:return None
        out=date.fromisoformat(value)
        return out if out.isoformat()==value else None
    except ValueError:return None


def clock(value):
    try:
        if not isinstance(value,str):return None
        out=datetime.fromisoformat(value.replace('Z','+00:00'))
        return out.astimezone(timezone.utc) if out.tzinfo else None
    except ValueError:return None


def envelope(raw,received_at):
    parsed=strict(raw)
    if not isinstance(parsed,list) or len(parsed)>6:raise ValueError('Whole requested six-row annual estimate response required')
    if clock(received_at) is None:raise ValueError('Observation clock required')
    return {'status':'received','received_at':received_at,'original_base64':base64.b64encode(raw).decode(),
            'original_sha256':hashlib.sha256(raw).hexdigest(),'original_bytes':len(raw)}


def original(acquisition):
    if acquisition.get('status')!='received':return None
    raw=base64.b64decode(acquisition['original_base64'],validate=True)
    if type(acquisition['original_bytes']) is not int or len(raw)!=acquisition['original_bytes'] or hashlib.sha256(raw).hexdigest()!=acquisition['original_sha256']:
        raise ValueError('Whole original estimate response differs')
    parsed=strict(raw)
    if not isinstance(parsed,list):raise ValueError('Estimate list required')
    return parsed


def projection(symbol,acquisition,checked_as_of):
    today=day(checked_as_of)
    if today is None:raise ValueError('Explicit check date required')
    parsed=original(acquisition)
    if parsed is None:return []
    received=clock(acquisition.get('received_at'))
    if received is None or received.date()>today:raise ValueError('Invalid acquisition date')
    rows=[]
    for index,raw in enumerate(parsed):
        item=raw if isinstance(raw,dict) else {}
        target=day(item.get('date'));matched=item.get('symbol')==symbol
        values={k:number(item.get(k)) for k in FIELDS}
        counts_valid=all(values[k] is None or (values[k]>=0 and int(values[k])==values[k]) for k in ('numAnalystsEps','numAnalystsRevenue'))
        ranges_valid=all(values[lo] is None or values[hi] is None or values[lo]<=values[hi]
                         for lo,hi in (('epsLow','epsHigh'),('revenueLow','revenueHigh')))
        ranges_valid=ranges_valid and all(values[avg] is None or
            ((values[lo] is None or values[lo]<=values[avg]) and (values[hi] is None or values[avg]<=values[hi]))
            for lo,avg,hi in (('epsLow','epsAvg','epsHigh'),('revenueLow','revenueAvg','revenueHigh')))
        currency=item.get('reportedCurrency',item.get('currency'))
        if not isinstance(currency,str) or len(currency)!=3 or not currency.isalpha() or currency!=currency.upper():currency=None
        basis=item.get('epsBasis')
        if not isinstance(basis,str) or not basis.strip():basis=None
        cik=item.get('cik')
        if not isinstance(cik,str) or not cik.isdigit():cik=None
        rows.append({'source_index':index,'ticker':symbol,'raw':raw,'target_period_end':target.isoformat() if target else None,
                     'requested_period':'annual','received_at':acquisition['received_at'],
                     'target_status':'unidentified' if target is None else 'past_target' if target<today else 'current_or_future_target',
                     'symbol_matches':matched,'values':values,'reported_currency':currency,'eps_basis':basis,'reported_cik':cik,
                     'valid_ranges_and_counts':counts_valid and ranges_valid,'estimate_strength':None,
                     'measurement_status':'reported_estimate_observation' if matched and target and counts_valid and ranges_valid else 'unqualified_record',
                     'first_publication_at':None,'source_update_frequency':'weekly_provider_documentation_not_observation_freshness'})
    return rows


def identity(row):
    # Dates are estimate target period ends, never the scheduled earnings event date.
    if (row.get('measurement_status')!='reported_estimate_observation' or not row.get('reported_currency')
        or not row.get('reported_cik') or not row.get('eps_basis')):return None
    return (row['ticker'],row['reported_cik'],row['target_period_end'],row['requested_period'],row['reported_currency'],row['eps_basis'])


def compare(current,previous):
    groups={}
    for old in previous:
        key=identity(old)
        if key:groups.setdefault(key,[]).append(old)
    current_counts={}
    for row in current:
        key=identity(row)
        if key:current_counts[key]=current_counts.get(key,0)+1
    out=[]
    for row in current:
        key=identity(row);matches=groups.get(key,[])
        result={'source_index':row['source_index'],'status':'no_unique_comparable_prior_observation',
                'prior_received_at':None,'eps_change':None,'eps_change_pct_positive_base':None,
                'analyst_count_change':None,'same_analyst_panel_verified':False,'corporate_actions_verified':False}
        if key and current_counts[key]==1 and len(matches)==1:
            prior=matches[0];now_time=clock(row['received_at']);old_time=clock(prior['received_at'])
            if now_time and old_time and old_time<now_time:
                now=row['values']['epsAvg'];old=prior['values']['epsAvg']
                if now is not None and old is not None:
                    delta=number(now-old)
                    result.update(status='observed_same_target_consensus_change',prior_received_at=prior['received_at'],
                                  eps_change=delta,eps_change_pct_positive_base=number(delta/old*100) if delta is not None and old>0 else None)
                    a=row['values']['numAnalystsEps'];b=prior['values']['numAnalystsEps']
                    result['analyst_count_change']=number(a-b) if a is not None and b is not None else None
        out.append(result)
    return out


def dossier(symbol,acquisition,checked_as_of,previous=None):
    rows=projection(symbol,acquisition,checked_as_of)
    old=projection(symbol,previous,checked_as_of) if previous else []
    return {'ticker':symbol,'acquisition':acquisition,'observations':rows,'comparisons':compare(rows,old),
            'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False}
