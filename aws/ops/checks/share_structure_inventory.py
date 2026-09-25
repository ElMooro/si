"""Complete legacy share-structure population and clock diagnostics.

This inventories original packets, not issuer identity, statement comparability,
free float, executed buybacks, investment forecasts or sizing authority.
"""
from collections import Counter, defaultdict
from datetime import date
import ast, re
from financial_statement_inventory import inventory as financial_inventory

CURRENT = 'data/share-flows.json'
UNIVERSE_INPUTS = ('data/phase-detector.json', 'data/insider-clusters.json',
    'data/insider-buys-enriched.json', 'data/opportunities.json', 'data/master-ranker.json',
    'data/industry-rotation.json', 'data/stock-valuations.json', 'data/insider-radar.json')
INPUTS = (CURRENT, *UNIVERSE_INPUTS, 'data/insider-trades.json', 'data/buyback-scanner.json',
    'data/sec-filings-intel.json', 'data/earnings-blackout.json',
    'data/impact/exposure-graph.json', 'data/impact/betas.json')


def inventory(packets, captured_date, legacy_source):
    if set(packets) != set(INPUTS): raise ValueError('Every reviewed source must have an explicit present or missing state')
    day = date.fromisoformat(captured_date)
    current = packets[CURRENT]
    base = financial_inventory(current, 'share_flows')
    roots = current['tickers']
    if not roots: raise ValueError('Whole nonempty predecessor population required')
    refs, issues = defaultdict(list), []

    def add(label, key, path):
        if not isinstance(label, str) or not label:
            issues.append({'source_key':key, 'path':path, 'reason':'missing_or_nonstring_reported_label'})
            return
        refs[label].append({'source_key':key, 'path':path})

    def rows(key, path, values, label_fields=('ticker','symbol')):
        if values is None: return
        if not isinstance(values, list):
            issues.append({'source_key':key,'path':path,'reason':'expected_array'});return
        for index, row in enumerate(values):
            if isinstance(row, str): add(row,key,[*path,index])
            elif isinstance(row, dict):
                field=next((field for field in label_fields if row.get(field)),label_fields[-1])
                add(row.get(field),key,[*path,index,field])
            else: issues.append({'source_key':key,'path':[*path,index],'reason':'unsupported_label_row'})

    for label in roots: add(label,CURRENT,['tickers',label])
    for key in UNIVERSE_INPUTS:
        packet=packets[key]
        if packet is None: continue
        if not isinstance(packet,dict): raise ValueError('Present universe source must be an object')
        if key=='data/phase-detector.json':
            for label in packet.get('tickers') or {}:add(label,key,['tickers',label])
        elif key in ('data/insider-clusters.json','data/insider-buys-enriched.json'):
            for field in ('clusters','rows','buys','strong'):rows(key,[field],packet.get(field))
        elif key=='data/opportunities.json':
            for field in ('all','opportunities','rows'):rows(key,[field],packet.get(field))
        elif key=='data/master-ranker.json':
            for field in ('top_tickers','rows','rankings'):rows(key,[field],packet.get(field))
        elif key=='data/industry-rotation.json':
            for index, row in enumerate(packet.get('leaders') or []):
                if not isinstance(row,dict):raise ValueError('Every industry leader must be retained as an object')
                rows(key,['leaders',index,'holdings_top'],row.get('holdings_top'))
        elif key=='data/stock-valuations.json':
            heatmap=packet.get('heatmap') or {}
            for group in ('sp','hp'):
                values=heatmap.get(group) or {}
                if isinstance(values,dict):
                    for label in values:add(label,key,['heatmap',group,label])
                elif isinstance(values,list):rows(key,['heatmap',group],values,('t',))
                else:raise ValueError('Explicit valuation mapping or reported ticker array required')
        elif key=='data/insider-radar.json':
            for field in ('clusters','latest_buys'):rows(key,[field],packet.get(field))
    # Reproduce the exact shipped selection as a diagnostic only; imports,
    # global Lambda setup and the handler are never executed.
    nodes=[n for n in ast.parse(legacy_source).body if isinstance(n,ast.FunctionDef) and n.name=='build_universe']
    if len(nodes)!=1:raise ValueError('Exact predecessor universe function required')
    def read(key):
        if key not in UNIVERSE_INPUTS:raise ValueError('Unexpected legacy universe dependency')
        return packets[key]
    scope={'s3_json':read}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'retained-universe','exec'),scope)
    legacy=scope['build_universe']([])
    if not isinstance(legacy,list) or len(set(legacy))!=len(legacy):raise ValueError('Unambiguous legacy universe required')
    ages=Counter();old=[];invalid=[];future=[]
    fields=Counter();flags=Counter()
    for label,row in roots.items():
        stamp=row.get('as_of')
        try:
            if not isinstance(stamp,str) or date.fromisoformat(stamp).isoformat()!=stamp:raise ValueError()
            age=(day-date.fromisoformat(stamp)).days
        except (TypeError,ValueError):invalid.append(label)
        else:
            ages[str(age)]+=1
            if age>7:old.append(label)
            if age<0:future.append(label)
        fields.update(row.keys())
        flags.update(v for v in row.get('flags') or [] if isinstance(v,str))
    candidates=sorted(label for label in refs if re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}',label))
    return {'contract':'share-structure-baseline-inventory.v1','captured_date':captured_date,
        'current':base,'per_row_as_of_is_acquisition_not_fiscal_period':True,
        'current_row_age_days':dict(sorted(ages.items())), 'current_rows_older_than_seven_days':sorted(old),
        'invalid_row_dates':sorted(invalid),'future_row_dates':sorted(future),
        'field_population':dict(sorted(fields.items())),'legacy_flag_population':dict(sorted(flags.items())),
        'legacy_requested_labels':legacy,'legacy_request_count':len(legacy),
        'current_labels_not_in_legacy_request':sorted(set(roots)-set(legacy)),
        'requested_labels_missing_current_rows':sorted(set(legacy)-set(roots)),
        'complete_reported_label_occurrences':dict(sorted(refs.items())),
        'complete_reported_label_count':len(refs),'candidate_provider_labels':candidates,
        'candidate_provider_label_count':len(candidates),'candidate_labels_absent_from_legacy_request':sorted(set(candidates)-set(legacy)),
        'reported_labels_not_provider_request_eligible':sorted(set(refs)-set(candidates)),
        'missing_inputs':[key for key in INPUTS if packets[key] is None], 'shape_issues':issues,
        'source_population_selection_not_identity_verification':True,
        'all_current_rows_conserved':len(roots)==base['rows'],'current_sec_identity_verified':False,
        'historical_security_continuity_verified':False,'share_split_comparability_verified':False,
        'statement_duration_verified':False,'free_float_change_qualified':False,
        'buyback_execution_qualified':False,'forecast_qualified':False,'sizing_qualified':False}
