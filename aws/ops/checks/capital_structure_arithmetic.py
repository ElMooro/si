"""Independent source-coordinate, identity and rational capital arithmetic.

No production measurement/compiler imports. Results are recomputed directly
from protected original rows, never from displayed input or output values.
"""
from collections import Counter, defaultdict
from decimal import Decimal
from fractions import Fraction
import json
from statement_research_arithmetic_v2 import read_original, numeric, rounded, valid, current_pairs, corroboration, FIELDS

I, C = 'income-statement', 'cash-flow-statement'
FORMULAS = {
    'cash_repurchase_outflow': (((C,'commonStockRepurchased',-1),),None,((C,'commonStockRepurchased','le'),)),
    'cash_common_stock_issuance': (((C,'commonStockIssuance',1),),None,((C,'commonStockIssuance','ge'),)),
    'cash_common_dividends': (((C,'commonDividendsPaid',-1),),None,((C,'commonDividendsPaid','le'),)),
    'gross_common_cash_distribution': (((C,'commonStockRepurchased',-1),(C,'commonDividendsPaid',-1)),None,
        ((C,'commonStockRepurchased','le'),(C,'commonDividendsPaid','le'))),
    'net_common_cash_return': (((C,'commonStockIssuance',-1),(C,'commonStockRepurchased',-1),(C,'commonDividendsPaid',-1)),None,
        ((C,'commonStockIssuance','ge'),(C,'commonStockRepurchased','le'),(C,'commonDividendsPaid','le'))),
    'common_issuance_cash_residual': (((C,'netCommonStockIssuance',1),(C,'commonStockIssuance',-1),(C,'commonStockRepurchased',-1)),None,()),
    'cash_repurchase_to_operating_cash_flow_pct': (((C,'commonStockRepurchased',-1),),(C,'operatingCashFlow'),((C,'commonStockRepurchased','le'),)),
    'sbc_to_operating_cash_flow_pct': (((C,'stockBasedCompensation',1),),(C,'operatingCashFlow'),()),
    'sbc_to_revenue_pct': (((C,'stockBasedCompensation',1),),(I,'revenue'),()),
    'weighted_diluted_over_basic_pct': (((I,'weightedAverageShsOutDil',1),(I,'weightedAverageShsOut',-1)),(I,'weightedAverageShsOut'),((I,'weightedAverageShsOutDil','gt'),)),
}
FACTS = {
    I: ('weightedAverageShsOut','weightedAverageShsOutDil','netIncome','eps','epsDiluted','revenue'),
    C: ('commonStockIssuance','commonStockIssued','commonStockRepurchased','netCommonStockIssuance','netStockIssuance',
        'commonDividendsPaid','netDividendsPaid','stockBasedCompensation','operatingCashFlow','freeCashFlow'),
    'quote': ('price','marketCap','currency','exchange','timestamp','sharesOutstanding'),
    'shares-float': ('date','floatShares','outstandingShares','freeFloat'),
    'splits': ('date','numerator','denominator','splitType'),
}


def typed(value):
    if isinstance(value,Decimal):return {'type':'decimal','value':str(value)}
    if isinstance(value,list):return {'type':'list','value':[typed(v) for v in value]}
    if isinstance(value,dict):return {'type':'object','value':{k:typed(v) for k,v in value.items()}}
    return {'type':type(value).__name__,'value':value}


def flags(value):
    assert all(value.get(k) is False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'))
    assert value.get('call') is None and value.get('score') is None and value.get('independent_investment_votes')==0


def rational_result(shown,value):
    assert shown['exact']=={'numerator':str(value.numerator),'denominator':str(value.denominator)}
    assert shown['value']==rounded(value)
    assert shown['rounding']=='half_even_12_decimal_places'


def input_field(shown,row,coord,endpoint,field):
    assert shown['endpoint']==endpoint and shown['field']==field
    assert shown['source_id']==coord.get('source_id') and shown['source_row']==coord.get('source_row')
    assert shown['present']==(field in row) and shown['reported']==typed(row.get(field))
    value=numeric(row.get(field))
    assert (shown['numeric_value'] is None)==(value is None)
    if value is not None:assert Fraction(shown['numeric_value'])==value
    return value


def verify(manifest_ref,identity_ref,compiled,read):
    pairs=current_pairs(identity_ref,read);manifest=read_original(manifest_ref,read)
    originals={};groups=defaultdict(set);total_bytes=0
    for url,ref in manifest['captures'].items():
        cap=read_original(ref,read);rows=read_original(cap['original'],read);total_bytes+=cap['original']['bytes']
        assert cap['spec']['url']==url
        shown=compiled['packet']['sources'][ref['sha256']]
        assert shown['request']==cap['spec'] and shown['original_sha256']==cap['original']['sha256']
        assert shown['original_bytes']==cap['original']['bytes'] and shown['rows']==len(rows)
        assert shown['requested_at']==cap['requested_at'] and shown['received_at']==cap['received_at']
        assert shown['original_is_protected'] is True
        for i,row in enumerate(rows):
            coord=(ref['sha256'],i);assert coord not in originals
            statement=cap['spec']['endpoint'] in (I,C)
            eligible=(statement and valid(row,cap['spec'],cap['received_at']) and
                corroboration(row,cap['spec']['symbol'],pairs)=='current_ticker_cik_pair_corroborated')
            originals[coord]=(cap,row,eligible)
            if eligible:
                ident=tuple(str(row[k]).zfill(10) if k=='cik' else str(row[k]) for k in FIELDS)
                groups[(cap['spec']['symbol'],cap['spec']['period'],ident)].add(coord)
    covered=set();joins=set();values=withheld=invalid=snapshots=0;metric_statuses=Counter()
    for symbol,shard in compiled['shards'].items():
        flags(shard);assert shard['requested_symbol']==symbol and shard['current_sec_ciks']==pairs.get(symbol,[])
        for record in shard['records']+shard['snapshots']:
            flags(record)
            coordinates={(v['capture_id'],v['source_row']) for v in record['source_rows']}
            assert coordinates and len(coordinates)==len(record['source_rows']) and not covered&coordinates
            covered.update(coordinates);entries=defaultdict(list)
            for c in record['source_rows']:
                cap,row,eligible=originals[(c['capture_id'],c['source_row'])]
                assert c['source_id']==cap['original']['sha256'] and c['endpoint']==cap['spec']['endpoint']
                assert c['request_period']==cap['spec']['period'] and cap['spec']['symbol']==symbol
                entries[c['endpoint']].append((row,c,eligible))
            reported=record.get('reported_rows',[record.get('reported')])
            assert len(reported)==len(coordinates)
            for evidence in reported:
                c=evidence['coordinate'];cap,row,_=originals[(c['capture_id'],c['source_row'])]
                assert c in record['source_rows'] and evidence['source_received_at']==cap['received_at']
                assert evidence['reported_identity']=={k:typed(row.get(k)) for k in FIELDS}
                assert evidence['identity_fields_present']==[k for k in FIELDS if k in row]
                expected_fields=FACTS[c['endpoint']]
                assert len(evidence['facts'])==len(expected_fields)
                for shown,field in zip(evidence['facts'],expected_fields):input_field(shown,row,c,c['endpoint'],field)
            if 'reported' in record:
                snapshots+=1;assert len(coordinates)==1
                cap,row,_=originals[next(iter(coordinates))];endpoint=cap['spec']['endpoint'];out=record['measurements']
                assert endpoint in ('quote','shares-float','splits') and record['joined_to_cash_flows'] is False
                if endpoint=='quote':assert out is None;continue
                flags(out)
                if endpoint=='shares-float':
                    f,o,p=(numeric(row.get(k)) for k in ('floatShares','outstandingShares','freeFloat'))
                    expected=(f/o*100 if row.get('symbol')==symbol and f is not None and o is not None and o>0 and 0<=f<=o else None)
                    assert out['free_float_change_qualified'] is False and out['current_security_class_verified'] is False
                    if expected is None:assert out['float_of_outstanding_pct'] is None
                    else:rational_result(out['float_of_outstanding_pct'],expected);values+=1
                    if expected is not None and p is not None and 0<=p<=100:
                        rational_result(out['reported_free_float_residual_pp'],p-expected);values+=1
                    else:assert out['reported_free_float_residual_pp'] is None
                else:
                    from datetime import date
                    n,d=numeric(row.get('numerator')),numeric(row.get('denominator'))
                    try:date_ok=isinstance(row.get('date'),str) and date.fromisoformat(row['date']).isoformat()==row['date']
                    except ValueError:date_ok=False
                    assert out['applied_to_statement_shares'] is False
                    if row.get('symbol')==symbol and date_ok and n is not None and d is not None and n>0 and d>0:
                        rational_result(out['reported_numerator_over_denominator'],n/d);values+=1
                    else:assert out['reported_numerator_over_denominator'] is None
                continue
            if record['measurements'] is None:
                assert all(not v[2] for rows in entries.values() for v in rows);invalid+=len(coordinates);continue
            assert all(v[2] for rows in entries.values() for v in rows)
            ident=tuple(record['identity'][k] for k in FIELDS);key=(symbol,record['request_period'],ident)
            assert key not in joins and groups[key]==coordinates;joins.add(key)
            assert record['duplicate_endpoints']==sorted(k for k,v in entries.items() if len(v)>1)
            unique={k:v[0] for k,v in entries.items() if len(v)==1}
            out=record['measurements'];flags(out);assert set(out['metrics'])==set(FORMULAS)
            for name,(terms,denominator,signs) in FORMULAS.items():
                shown=out['metrics'][name];fields=[(e,f) for e,f,_ in terms]
                if denominator and denominator not in fields:fields.append(denominator)
                assert len(shown['inputs'])==len(fields)
                nums={}
                for item,(endpoint,field) in zip(shown['inputs'],fields):
                    row,c,_=unique.get(endpoint,({}, {}, False));nums[(endpoint,field)]=input_field(item,row,c,endpoint,field)
                expected=None;reason=None
                if any(e not in unique for e,f in fields):reason='required_original_statement_row_missing'
                elif any(v is None for v in nums.values()):reason='required_provider_field_unavailable'
                elif any((s=='le' and nums[(e,f)]>0) or (s=='ge' and nums[(e,f)]<0) or (s=='gt' and nums[(e,f)]<=0) for e,f,s in signs):
                    reason='provider_cash_flow_or_share_sign_conflict'
                elif denominator and nums[denominator]<=0:reason='nonpositive_denominator'
                elif name=='weighted_diluted_over_basic_pct' and nums[(I,'weightedAverageShsOutDil')]<nums[(I,'weightedAverageShsOut')]:
                    reason='diluted_denominator_below_basic'
                else:
                    expected=sum((nums[(e,f)]*coefficient for e,f,coefficient in terms),Fraction())
                    if denominator:expected=expected/nums[denominator]*100
                if expected is None:
                    assert shown['value'] is None and shown['exact'] is None and shown['status']==reason;withheld+=1
                else:
                    rational_result(shown,expected);values+=1
                    assert shown['status']=='descriptive_provider_row_calculation'
                assert shown['supports_investment_action'] is False and shown['period_annualized'] is False
                expected_unit='%' if denominator else next((v[0]['reportedCurrency'] for e,v in unique.items() if e in {ep for ep,f in fields}),None)
                assert shown['unit']==expected_unit
                metric_statuses[shown['status']]+=1
    assert covered==set(originals) and joins==set(groups)
    packet=compiled['packet'];flags(packet)
    assert packet['provider_rows']==len(originals) and packet['reported_names']==len(manifest['reported_symbols'])
    assert set(compiled['shards'])==set(manifest['reported_symbols']) and packet['metric_statuses']==dict(metric_statuses)
    assert total_bytes==manifest['counts']['provider_bytes']
    return {'original_rows_checked':len(covered),'exact_identity_groups_checked':len(joins),
        'identity_metadata_rows_checked':len(covered),'invalid_or_uncorroborated_rows_checked':invalid,
        'snapshot_rows_checked':snapshots,'metric_comparisons':sum(metric_statuses.values()),
        'exact_rational_values_checked':values,'unavailable_metrics_checked':withheld,
        'current_sec_pairs_checked':True,'production_measurement_formulas_imported':False,
        'all_original_rows_conserved':True,'forecast_qualified':False,'sizing_qualified':False}
