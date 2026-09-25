"""Retain whole SEC submissions behind every existing authorization row.

This is a reconstruction of reported candidates, not a search census or an
authorization/execution classifier. Accession prefixes may name filing agents;
the issuer CIK comes from the preserved row's explicit SEC company link.
"""
from datetime import date
import hashlib,json,re,urllib.error,urllib.parse,urllib.request
from financial_statement_campaign import NoRedirect
from market_runtime_evidence import bounded

BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/buyback-original-research/'
MAX=64*1024*1024
FLAGS={key:False for key in ('authorization_amount_qualified','buyback_execution_qualified',
    'ownership_dilution_qualified','forecast_qualified','sizing_qualified','filing_population_complete')}
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encoded=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def cik(value):
    if type(value) is int:value=str(value)
    if not isinstance(value,str) or not re.fullmatch(r'0*[1-9][0-9]{0,9}',value) or len(value)>10:
        raise ValueError('Exact reported issuer CIK required')
    return str(int(value)).zfill(10)


def spec(issuer,accession):
    issuer=cik(issuer)
    if not isinstance(accession,str) or not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',accession):
        raise ValueError('Exact reported accession required')
    return {'issuer_cik':issuer,'accession':accession,'kind':'complete_sec_submission',
        'url':f'https://www.sec.gov/Archives/edgar/data/{int(issuer)}/{accession.replace("-","")}/{accession}.txt'}


def row_spec(row):
    url=row.get('filing_url');parsed=urllib.parse.urlsplit(url) if isinstance(url,str) else None
    if (not parsed or parsed.scheme!='https' or parsed.netloc!='www.sec.gov'
            or parsed.path!='/cgi-bin/browse-edgar' or parsed.fragment):
        raise ValueError('Reviewed SEC company URL required; do not infer issuer from accession')
    pairs=urllib.parse.parse_qsl(parsed.query,keep_blank_values=True,strict_parsing=True)
    query=dict(pairs)
    if len(pairs)!=len(query) or set(query)-{'action','CIK','type','dateb','owner','count'} or query.get('action')!='getcompany':
        raise ValueError('Unambiguous reported SEC company query required')
    issuer=cik(query.get('CIK'))
    if row.get('cik') is not None and cik(row['cik'])!=issuer:raise ValueError('Reported issuer identities conflict')
    if query.get('type') not in ('8-K','8-K/A'):raise ValueError('Reviewed reported filing form required')
    return spec(issuer,row.get('filing_adsh'))


def plan(packet):
    rows=packet.get('top_opportunities') if isinstance(packet,dict) else None
    if not isinstance(rows,list) or len(rows)>5000 or any(not isinstance(row,dict) for row in rows):
        raise ValueError('Whole reported authorization row population required')
    requests={};coordinates=[];issues=[]
    for index,row in enumerate(rows):
        point={'source_row':index,'source_path':['top_opportunities',index],
            'reported_label':row.get('ticker',row.get('symbol')),'reported_filing_date':row.get('announcement_date'),
            'reported_accession':row.get('filing_adsh'),'reported_filing_url':row.get('filing_url')}
        try:
            request=row_spec(row);point['request_url']=request['url']
            requests.setdefault(request['url'],{'spec':request,'source_rows':[]})['source_rows'].append(index)
        except (ValueError,TypeError) as exc:
            point['issue']=str(exc);issues.append(index)
        coordinates.append(point)
    return {'contract':'buyback-reported-filing-plan.v1','reported_rows':len(rows),
        'rows':coordinates,'requests':[requests[key] for key in sorted(requests)],'unresolved_rows':issues,
        'packet_as_of':packet.get('as_of'),'all_reported_rows_conserved':True,
        'reported_announcement_date_is_verified_event_date':False,**FLAGS}


def _one(pattern,raw):
    found=re.findall(pattern,raw,re.M)
    if len(found)!=1:raise ValueError('Exactly one complete submission metadata field required')
    return found[0].strip()


def inspect(body,request):
    if request!=spec(request.get('issuer_cik'),request.get('accession')):
        raise ValueError('Exact reviewed submission request required')
    if not isinstance(body,bytes) or not 0<len(body)<=MAX:raise ValueError('Whole bounded submission required')
    first=re.match(rb'<SEC-DOCUMENT>[^\r\n]*\r?\n',body)
    if not first or not re.search(rb'(?m)^</SEC-DOCUMENT>\s*\Z',body):
        raise ValueError('Complete SEC submission envelope required')
    header_match=re.search(rb'(?ms)^<SEC-HEADER>[^\r\n]*\r?\n(.*?)^</SEC-HEADER>\s*\r?\n',body)
    if not header_match or body[first.end():header_match.start()].strip():raise ValueError('Complete SEC header required')
    header=header_match[1]
    accession=_one(rb'^ACCESSION NUMBER:\s*([^\r\n]+)',header).decode('ascii')
    form=_one(rb'^CONFORMED SUBMISSION TYPE:\s*([^\r\n]+)',header).decode('ascii')
    filed=_one(rb'^FILED AS OF DATE:\s*([0-9]{8})\s*$',header).decode('ascii')
    filing_date=date(int(filed[:4]),int(filed[4:6]),int(filed[6:])).isoformat()
    public_count=int(_one(rb'^PUBLIC DOCUMENT COUNT:\s*([0-9]+)\s*$',header))
    issuers=sorted({cik(v.decode('ascii')) for v in re.findall(rb'^\s*CENTRAL INDEX KEY:\s*([0-9]+)\s*$',header,re.M)})
    if accession!=request['accession'] or request['issuer_cik'] not in issuers or form not in ('8-K','8-K/A'):
        raise ValueError('Submission header does not match reported issuer/accession/form')
    documents=[];prior=header_match.end();sequences=set();filenames=set()
    for match in re.finditer(rb'(?ms)^<DOCUMENT>\s*\r?\n(.*?)^</DOCUMENT>\s*\r?\n',body):
        if body[prior:match.start()].strip():raise ValueError('Unparsed content between submission documents')
        block=match[1];start=re.search(rb'(?m)^<TEXT>[^\S\r\n]*\r?\n',block)
        end=re.search(rb'(?m)^</TEXT>\s*\Z',block)
        if not start or not end or start.end()>end.start():raise ValueError('Complete embedded document text required')
        metadata=block[:start.start()]
        kind=_one(rb'^<TYPE>([^\r\n]+)',metadata).decode('ascii')
        sequence=_one(rb'^<SEQUENCE>([0-9]+)\s*$',metadata).decode('ascii')
        filename=_one(rb'^<FILENAME>([^\r\n]+)',metadata).decode('ascii')
        if sequence in sequences or filename in filenames or not re.fullmatch(r'[A-Za-z0-9_.-]+',filename) or filename in ('.','..'):
            raise ValueError('Unambiguous safe submission document identity required')
        sequences.add(sequence);filenames.add(filename)
        offset=match.start(1)+start.end();text=body[offset:match.start(1)+end.start()]
        keywords=[{'byte_start':offset+m.start(),'byte_end':offset+m.end(),'reported_text':m[0].decode('ascii')}
            for m in re.finditer(rb'(?i)\brepurchase\b|\bbuyback\b',text)]
        documents.append({'source_document':len(documents),'type':kind,'sequence':sequence,'filename':filename,
            'byte_start':match.start(),'byte_end':match.end(),'bytes':len(match[0]),'sha256':sha(match[0]),
            'text_byte_start':offset,'text_byte_end':offset+len(text),'text_bytes':len(text),'text_sha256':sha(text),
            'literal_keyword_occurrences':keywords,'keyword_matches_establish_authorization':False,
            'document_url':request['url'].rsplit('/',1)[0]+'/'+filename})
        prior=match.end()
    if body[prior:].strip()!=b'</SEC-DOCUMENT>' or not documents or len(documents)!=public_count:
        raise ValueError('Every declared document must be present, parsed and counted')
    if not any(row['type']==form for row in documents):raise ValueError('Declared primary form document absent')
    return {'contract':'buyback-complete-submission-inventory.v1','accession':accession,
        'header_issuer_ciks':issuers,'form':form,'filing_date':filing_date,'reported_document_count':public_count,
        'documents':documents,'all_declared_documents_retained':True,'original_bytes':len(body),'original_sha256':sha(body),
        'literal_keyword_occurrences':sum(len(row['literal_keyword_occurrences']) for row in documents),
        'keyword_search_is_semantic_or_exhaustive':False,'ticker_identity_verified':False,**FLAGS}


def read(client,ref):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',ref.get('sha256',''))
            or ref.get('key')!=PRIVATE+ref['sha256']+'.bin'):
        raise ValueError('Exact private filing reference required')
    body=bounded(client.get_object(Bucket=BUCKET,Key=ref['key'])['Body'],MAX)
    if len(body)!=ref['bytes'] or sha(body)!=ref['sha256']:raise ValueError('Retained filing bytes differ')
    return body


def retain(client,body):
    if not isinstance(body,bytes) or len(body)>MAX:raise ValueError('Whole bounded original required')
    ref={'key':PRIVATE+sha(body)+'.bin','sha256':sha(body),'bytes':len(body)}
    try:client.put_object(Bucket=BUCKET,Key=ref['key'],Body=body,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if read(client,ref)!=body:raise ValueError('Whole filing readback differs')
    return ref


def request_key(request_id,label):
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Durable request identity required')
    return PRIVATE+'requests/'+sha((request_id+':'+label).encode())+'.json'


def journal(client,key,value,claim=False):
    if not re.fullmatch(re.escape(PRIVATE)+r'requests/[a-f0-9]{64}\.json',key):raise ValueError('Reviewed private request key required')
    body=encoded(value)
    if len(body)>MAX:raise ValueError('Whole journal bound exceeded')
    client.put_object(Bucket=BUCKET,Key=key,Body=body,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if bounded(client.get_object(Bucket=BUCKET,Key=key)['Body'],MAX)!=body:raise ValueError('Journal readback differs')


def capture(client,request_id,request,rate,now,transport=None):
    if request!=spec(request.get('issuer_cik'),request.get('accession')):raise ValueError('Exact reviewed submission request required')
    key=request_key(request_id,request['url']);progress={'request_id':request_id,'spec':request,'status':'claimed','claimed_at':now()}
    journal(client,key,progress,True)
    try:
        rate.acquire();progress.update(transport_attempted=True,requested_at=now());journal(client,key,progress)
        http=urllib.request.Request(request['url'],headers={'User-Agent':'JustHodl Research ops@justhodl.ai','Accept-Encoding':'identity'})
        try:response=(transport or urllib.request.build_opener(NoRedirect()).open)(http,timeout=40)
        except urllib.error.HTTPError as exc:response=exc
        code=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in
            ('content-type','content-length','content-encoding','date','etag','last-modified','retry-after')}
        body=bounded(response,MAX);progress.update(status='response_retained',received_at=now(),http_status=code,headers=headers,original=retain(client,body))
        journal(client,key,progress)
        if code!=200 or not body:raise ValueError('SEC response unavailable; never automatically retry')
        if headers.get('content-encoding','identity').lower() not in ('','identity'):raise ValueError('Unexpected content encoding')
        if 'content-length' in headers and int(headers['content-length'])!=len(body):raise ValueError('Transport length differs')
        result={**progress,'inventory':inspect(body,request),'request_status_key':key}
        ref=retain(client,encoded(result));journal(client,key,{**progress,'status':'complete','capture':ref})
        return {**result,'retained_capture':ref}
    except Exception as exc:
        journal(client,key,{**progress,'status':'failed','error_type':type(exc).__name__})
        raise RuntimeError('Filing acquisition failed; inspect retained response and journal before a new reviewed request') from None
