"""Independent exact rational accounting arithmetic over every original row.

No imports from the measurement/compiler modules. Definitions are written
separately and evaluate original source fields, not displayed input values.
"""
from collections import Counter, defaultdict
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
import hashlib, json, re

I, B, C = 'income-statement', 'balance-sheet-statement', 'cash-flow-statement'
FIELDS = ('symbol','cik','reportedCurrency','date','fiscalYear','period','filingDate','acceptedDate')
# (signed numerator terms, optional positive denominator, multiplier, unit)
FORMULAS = {
    'gross_margin_pct': (((I,'grossProfit',1),),(I,'revenue'),100,'%'),
    'operating_margin_pct': (((I,'operatingIncome',1),),(I,'revenue'),100,'%'),
    'net_margin_pct': (((I,'netIncome',1),),(I,'revenue'),100,'%'),
    'operating_cash_margin_pct': (((C,'operatingCashFlow',1),),(I,'revenue'),100,'%'),
    'cash_conversion_multiple': (((C,'operatingCashFlow',1),),(I,'netIncome'),1,'multiple'),
    'current_ratio': (((B,'totalCurrentAssets',1),),(B,'totalCurrentLiabilities'),1,'multiple'),
    'debt_to_assets_pct': (((B,'totalDebt',1),),(B,'totalAssets'),100,'%'),
    'goodwill_to_assets_pct': (((B,'goodwill',1),),(B,'totalAssets'),100,'%'),
    'net_receivables_to_revenue_pct': (((B,'netReceivables',1),),(I,'revenue'),100,'%'),
    'sga_to_revenue_pct': (((I,'sellingGeneralAndAdministrativeExpenses',1),),(I,'revenue'),100,'%'),
    'reported_fcf_margin_pct': (((C,'freeCashFlow',1),),(I,'revenue'),100,'%'),
    'earnings_cash_gap_to_assets_pct': (((I,'netIncome',1),(C,'operatingCashFlow',-1)),(B,'totalAssets'),100,'%'),
    'net_debt_derived': (((B,'totalDebt',1),(B,'cashAndCashEquivalents',-1)),None,1,'currency'),
    'earnings_cash_gap': (((I,'netIncome',1),(C,'operatingCashFlow',-1)),None,1,'currency'),
    'balance_identity_residual': (((B,'totalAssets',1),(B,'totalLiabilities',-1),(B,'totalEquity',-1)),None,1,'currency'),
    'gross_profit_residual': (((I,'grossProfit',1),(I,'revenue',-1),(I,'costOfRevenue',1)),None,1,'currency'),
    'operating_cash_alias_residual': (((C,'operatingCashFlow',1),(C,'netCashProvidedByOperatingActivities',-1)),None,1,'currency'),
}


def read_original(ref, read):
    raw = read(ref['key'])
    assert len(raw) == ref['bytes'] and hashlib.sha256(raw).hexdigest() == ref['sha256']
    return json.loads(raw, parse_float=Decimal)


def numeric(value):
    if type(value) not in (int, Decimal): return None
    value = Decimal(value)
    if not value.is_finite() or abs(value.adjusted()) > 100 or len(value.as_tuple().digits) > 128: return None
    return Fraction(value)


def rounded(value):
    sign = '-' if value < 0 else ''
    scaled, remainder = divmod(abs(value.numerator) * 10**12, value.denominator)
    if remainder * 2 > value.denominator or (remainder * 2 == value.denominator and scaled % 2): scaled += 1
    if scaled == 0: sign = ''
    return sign + str(scaled // 10**12) + '.' + str(scaled % 10**12).zfill(12)


def valid(row, request, received):
    if any(type(row.get(k)) not in (str,int) for k in FIELDS): return False
    values = {k:str(row[k]) for k in FIELDS}
    if not re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}',values['symbol']) or values['symbol']!=request['symbol']: return False
    if not re.fullmatch('[0-9]{1,10}',values['cik']) or int(values['cik'])==0: return False
    if not re.fullmatch('[A-Z]{3}',values['reportedCurrency']) or not re.fullmatch('[0-9]{4}',values['fiscalYear']): return False
    if values['period'] not in (('FY',) if request['period']=='annual' else ('Q1','Q2','Q3','Q4')): return False
    try:
        end=date.fromisoformat(values['date']); filing=date.fromisoformat(values['filingDate'])
        if end.isoformat()!=values['date'] or filing.isoformat()!=values['filingDate']: return False
        if len(values['acceptedDate'])<16 or values['acceptedDate'][10] not in ('T',' '): return False
        accepted=datetime.fromisoformat(values['acceptedDate']).date()
        limit=datetime.fromisoformat(received).astimezone(timezone.utc).date()+timedelta(days=1)
        return end<=filing and end<=accepted and max(end,filing,accepted)<=limit
    except ValueError:
        return False


def verify(manifest_ref, compiled, read):
    manifest=read_original(manifest_ref,read); originals={}; groups=defaultdict(set)
    for url, ref in manifest['captures'].items():
        cap=read_original(ref,read); rows=read_original(cap['original'],read)
        for index,row in enumerate(rows):
            coord=(ref['sha256'],index); assert coord not in originals
            eligible=valid(row,cap['spec'],cap['received_at'])
            originals[coord]=(cap,row,eligible)
            if eligible:
                ident=tuple(str(row[k]) for k in FIELDS)
                groups[(cap['spec']['symbol'],cap['spec']['period'],ident)].add(coord)
    covered=set(); tested=available=withheld=zeroes=invalid=0; joins=set()
    for symbol,shard in compiled['shards'].items():
        assert shard['requested_symbol']==symbol
        for record in shard['records']:
            coordinates={(v['capture_id'],v['source_row']) for v in record['source_rows']}
            assert coordinates and len(coordinates)==len(record['source_rows']) and not coordinates & covered
            covered.update(coordinates); entries=defaultdict(list)
            for c in record['source_rows']:
                cap,row,eligible=originals[(c['capture_id'],c['source_row'])]
                assert c['source_id']==cap['original']['sha256'] and c['endpoint']==cap['spec']['endpoint']
                assert cap['spec']['symbol']==symbol and c['request_period']==record['request_period']==cap['spec']['period']
                entries[c['endpoint']].append((row,c,eligible))
            if record['measurements'] is None:
                assert len(coordinates)==1 and not next(iter(entries.values()))[0][2]
                invalid+=1;continue
            assert all(row[2] for values in entries.values() for row in values)
            ident=tuple(record['identity'][k] for k in FIELDS)
            key=(symbol,record['request_period'],ident)
            assert key not in joins and groups[key]==coordinates
            joins.add(key)
            unique={endpoint:values[0] for endpoint,values in entries.items() if len(values)==1}
            assert record['duplicate_endpoints']==sorted(endpoint for endpoint,values in entries.items() if len(values)>1)
            out=record['measurements'];assert set(out['metrics'])==set(FORMULAS)
            assert out['complete_three_statement_bundle']==(set(unique)=={I,B,C})
            assert out['m_score'] is None and out['grade'] is None and out['score'] is None
            for name,(terms,denom,scale,unit) in FORMULAS.items():
                metric=out['metrics'][name];refs=[(ep,field) for ep,field,_ in terms]+([denom] if denom else [])
                nums=[]
                assert len(metric['inputs'])==len(refs)
                for (endpoint,field), shown in zip(refs,metric['inputs']):
                    assert shown['endpoint']==endpoint and shown['field']==field
                    row,coord,_=unique.get(endpoint,({}, {}, False));number=numeric(row.get(field));nums.append(number)
                    assert shown['source_id']==coord.get('source_id') and shown['source_row']==coord.get('source_row')
                    assert (shown['reported_value'] is None) == (number is None)
                    if number is not None: assert Fraction(shown['reported_value'])==number
                expected=None
                if all(v is not None for v in nums) and (not denom or nums[-1]>0):
                    value=sum((nums[i]*term[2] for i,term in enumerate(terms)),Fraction())
                    if denom:value=value/nums[-1]*scale
                    expected=rounded(value)
                assert metric['value']==expected,(symbol,record['identity']['date'],name,'arithmetic differs')
                if unique:
                    assert metric['unit']==(record['identity']['reportedCurrency'] if unit=='currency' else unit)
                assert not metric['supports_investment_action'] and not metric['period_annualized']
                if expected is not None:
                    assert metric['status']=='descriptive_calculation';available+=1;zeroes+=Fraction(expected)==0
                else:
                    reason = ('reviewed_statement_records_required' if not unique else
                        'required_statement_record_missing' if any(ep not in unique for ep,field in refs) else
                        'required_provider_field_missing' if any(v is None for v in nums) else 'nonpositive_denominator')
                    assert metric['status']==reason
                    withheld+=1
                tested+=1
            assert all(not out[k] and not record[k] for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'))
    assert covered==set(originals) and joins==set(groups)
    packet=compiled['packet']
    assert packet['provider_rows']==len(originals) and set(compiled['shards'])==set(manifest['reported_symbols'])
    assert packet['reported_names']==len(manifest['reported_symbols']) and packet['independent_investment_votes']==0
    return {'original_rows_checked':len(covered),'exact_identity_groups_checked':len(joins),
        'invalid_identity_rows_checked':invalid,'metric_comparisons':tested,'exact_rational_values_checked':available,
        'unavailable_metrics_checked':withheld,'reported_numeric_zero_results':zeroes,
        'production_measurement_formulas_imported':False,'all_original_rows_conserved':True,
        'original_sec_filings_verified':False,'forecast_qualified':False,'sizing_qualified':False}
