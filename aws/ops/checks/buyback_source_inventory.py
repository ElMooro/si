"""Whole-packet buyback evidence inventory; no ranking or investment inference."""
from collections import Counter, defaultdict
from datetime import date
from urllib.parse import urlsplit
import re

CURRENT='data/buyback-engine.json'
SCANNER='data/buyback-scanner.json'
ATTENTION='data/attention-confluence.json'
INPUTS=(CURRENT,SCANNER,ATTENTION,'data/earnings-blackout.json','data/share-flows.json')


def inventory(packets, captured_date):
    if set(packets)!=set(INPUTS):raise ValueError('Every reviewed source needs an explicit present/missing state')
    today=date.fromisoformat(captured_date)
    if any(value is not None and not isinstance(value,dict) for value in packets.values()):
        raise ValueError('Complete source objects required')
    current=packets[CURRENT]
    if not isinstance(current,dict) or not isinstance(current.get('tickers'),dict):
        raise ValueError('Whole buyback predecessor population required')
    if not all(isinstance(row,dict) for row in current['tickers'].values()):
        raise ValueError('Every buyback row must remain an object')
    refs=defaultdict(list);issues=[];fields=Counter();rows=[];authorizations=[]
    def add(label,key,path):
        if not isinstance(label,str) or not label:
            issues.append({'source_key':key,'path':path,'reason':'missing_or_nonstring_label','reported_label':label});return
        refs[label].append({'source_key':key,'path':path})
    for label,row in current['tickers'].items():
        add(label,CURRENT,['tickers',label]);fields.update(k for k,v in row.items() if v is not None)
        rows.append({'reported_map_key':label,'reported_symbol':row.get('symbol'),'all_reported_fields':sorted(row),
            'non_null_fields':sorted(k for k,v in row.items() if v is not None),
            'source_statement_coordinates_present':isinstance(row.get('source_rows'),list) and bool(row['source_rows']),
            'declared_row_as_of':row.get('as_of'),'declared_period_end':row.get('period_end'),
            'declared_currency':row.get('reportedCurrency',row.get('currency'))})
    scanner=packets[SCANNER] or {};opportunities=scanner.get('top_opportunities',[])
    if not isinstance(opportunities,list) or not all(isinstance(row,dict) for row in opportunities):
        raise ValueError('Every reported authorization row must remain an object')
    auth_labels=set()
    for index,row in enumerate(opportunities):
        label=row.get('ticker') or row.get('symbol');add(label,SCANNER,['top_opportunities',index])
        if isinstance(label,str) and label.strip():auth_labels.add(label.upper().strip())
        stamp=row.get('announcement_date');age=None
        try:
            if not isinstance(stamp,str):raise ValueError()
            day=date.fromisoformat(stamp)
            if day.isoformat()!=stamp:raise ValueError()
            age=(today-day).days
        except ValueError:pass
        url=row.get('filing_url')
        try:parsed=urlsplit(url) if isinstance(url,str) else None
        except ValueError:parsed=None
        exact=bool(parsed and parsed.scheme=='https' and parsed.hostname=='www.sec.gov'
            and re.fullmatch(r'/Archives/edgar/data/\d+/\d+/[^/]+',parsed.path) and not parsed.query and not parsed.fragment)
        authorizations.append({'source_row':index,'reported_label':label,'announcement_date':stamp,'age_days':age,
            'filing_url':url,'exact_sec_archive_url_shape':exact,'filing_accession':row.get('filing_adsh'),
            'authorization_usd':row.get('authorization_usd'),'reported_fields':sorted(row),
            'original_filing_bytes_verified':False,'execution_established':False})
    attention=(packets[ATTENTION] or {}).get('tickers',{})
    if not isinstance(attention,dict):raise ValueError('Explicit attention ticker mapping required')
    attention_labels=set()
    for label in attention:
        add(label,ATTENTION,['tickers',label])
        if isinstance(label,str) and label.strip():attention_labels.add(label.upper().strip())
    # Reproduce the visible lexical 190-name cap for diagnosis, never selection.
    requested=sorted(auth_labels|attention_labels)[:190]
    requested+=sorted(auth_labels-set(requested))
    candidates=sorted(refs)
    return {'contract':'buyback-whole-source-inventory.v1','captured_date':captured_date,
        'packet_clocks':{key:None if value is None else value.get('generated_at') for key,value in packets.items()},
        'missing_inputs':[key for key,value in packets.items() if value is None],
        'current_rows':rows,'current_row_count':len(rows),'field_population':dict(sorted(fields.items())),
        'authorizations':authorizations,'authorization_rows':len(authorizations),
        'reported_label_occurrences':dict(sorted(refs.items())),'complete_reported_labels':candidates,
        'provider_request_eligible_labels':[s for s in candidates if re.fullmatch(r'[A-Z0-9][A-Z0-9.-]{0,19}',s)],
        'reported_labels_not_provider_eligible':[s for s in candidates if not re.fullmatch(r'[A-Z0-9][A-Z0-9.-]{0,19}',s)],
        'legacy_requested_labels':requested,'outside_legacy_request':sorted(set(candidates)-set(requested)),
        'legacy_requests_missing_current_rows':sorted(set(requested)-set(current['tickers'])),
        'shape_issues':issues,'all_current_rows_conserved':True,'all_reported_authorization_rows_conserved':True,
        'unreported_filings_complete':False,'issuer_identity_verified':False,'buyback_execution_qualified':False,
        'ttm_duration_qualified':False,'ownership_dilution_qualified':False,'forecast_qualified':False,'sizing_qualified':False}
