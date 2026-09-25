"""Inspect all original accounting identities against a dated SEC ticker index.

This corroborates current ticker/CIK pairs only. It does not establish past
security continuity, filing values, statement durations or first availability.
"""
from collections import defaultdict,Counter
from datetime import date
import re
import statement_research_source as source
import statement_research_model as model

INDEX_URL='https://www.sec.gov/files/company_tickers.json'


def ticker_index(raw):
    parsed=source.strict(raw)
    if not isinstance(parsed,dict) or not 1000<=len(parsed)<=50000:
        raise ValueError('Complete SEC company ticker index required')
    rows=[];mapping=defaultdict(set)
    for key,value in parsed.items():
        if (not isinstance(key,str) or not key.isdigit() or not isinstance(value,dict)
                or type(value.get('cik_str')) is not int or not 0<value['cik_str']<10**10
                or not isinstance(value.get('ticker'),str) or not value['ticker']
                or not isinstance(value.get('title'),str) or not value['title']):
            raise ValueError('Invalid original SEC index identity')
        row={'source_key':key,'cik':str(value['cik_str']).zfill(10),'ticker':value['ticker'],'title':value['title']}
        rows.append(row);mapping[row['ticker']].add(row['cik'])
    return {'rows':rows,'by_ticker':{key:sorted(values) for key,values in sorted(mapping.items())},
        'repeated_tickers':dict(Counter(v['ticker'] for v in rows)),'historical_security_continuity_verified':False}


def compare(cap,rows,index):
    request=cap['spec'];out=[]
    for i,row in enumerate(rows):
        metadata={key:(None if row.get(key) is None else str(row[key])) for key in ('symbol','cik','reportedCurrency','date','fiscalYear','period','filingDate','acceptedDate')}
        _,problem=model.row_problem(row,request,cap)
        ciks=index['by_ticker'].get(request['symbol'],[])
        reported=metadata['cik'] or ''
        if not ciks:status='not_in_current_sec_ticker_index'
        elif len(ciks)!=1:status='ambiguous_current_sec_ticker_index'
        elif not re.fullmatch('[0-9]{1,10}',reported) or int(reported)==0:status='invalid_provider_cik'
        elif reported.zfill(10)!=ciks[0]:status='provider_cik_differs_from_current_sec_index'
        else:status='current_ticker_cik_pair_corroborated'
        clock_issues=[]
        try:
            end=date.fromisoformat(metadata['date'])
            for field in ('filingDate','acceptedDate'):
                if date.fromisoformat(metadata[field][:10])<end:clock_issues.append(field+'_precedes_period_end')
        except (TypeError,ValueError):clock_issues.append('invalid_provider_date')
        out.append({'requested_symbol':request['symbol'],'endpoint':request['endpoint'],'request_period':request['period'],
            'source_sha256':cap['original']['sha256'],'source_row':i,'reported_identity':metadata,
            'source_received_at':cap['received_at'],'current_sec_ciks':ciks,'current_identity_status':status,
            'statement_identity_problem':problem,'clock_issues':clock_issues,'historical_security_continuity_verified':False})
    return out
