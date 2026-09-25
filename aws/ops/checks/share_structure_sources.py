"""Original capital-structure source capture, before measurement qualification.

Cash amounts, EPS denominators, reported float, quotes and split events are
separate sources. No estimate is substituted for a missing value. No current
engine packet, account, consumer, signal or schedule is accessed here.
"""
from collections import Counter
from decimal import Decimal
from datetime import date
import hashlib,json,re,urllib.request,urllib.error,urllib.parse
from financial_statement_source import strict
from financial_statement_campaign import Rate, NoRedirect
from market_runtime_evidence import bounded

BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/share-structure-research/'
STATEMENTS=('income-statement','cash-flow-statement')
SNAPSHOTS=('quote','shares-float','splits')
IDENTITY=('symbol','cik','reportedCurrency','date','fiscalYear','period','filingDate','acceptedDate','timestamp')
MAX=32*1024*1024
sha=lambda body:hashlib.sha256(body).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def population(labels):
    if not isinstance(labels,(list,tuple,set,frozenset)) or not 1<=len(labels)<=5000:
        raise ValueError('Complete bounded explicit capital-structure population required')
    if any(not isinstance(label,str) or not re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}',label) for label in labels):
        raise ValueError('Exact reported provider labels required; never silently rename a security')
    if len(set(labels))!=len(labels):raise ValueError('Repeated provider label')
    return frozenset(labels)


def spec(symbol,endpoint,period=None):
    population([symbol])
    if endpoint not in STATEMENTS+SNAPSHOTS:raise ValueError('Reviewed provider endpoint required')
    params={'symbol':symbol}
    if endpoint in STATEMENTS:
        if period not in ('annual','quarter'):raise ValueError('Explicit statement period required')
        params.update(period=period,limit=5 if period=='annual' else 13)
    elif period is not None:raise ValueError('Snapshot endpoint cannot claim a fiscal period')
    return {'symbol':symbol,'endpoint':endpoint,'period':period,
        'limit':params.get('limit'),'url':'https://financialmodelingprep.com/stable/'+endpoint+'?'+urllib.parse.urlencode(params)}


def specifications(labels):
    symbols=population(labels)
    return [spec(label,endpoint,period) for label in sorted(symbols)
        for endpoint,period in [(e,p) for p in ('annual','quarter') for e in STATEMENTS]+[(e,None) for e in SNAPSHOTS]]


def typed(value):
    if isinstance(value,Decimal):return {'type':'decimal','value':str(value)}
    if isinstance(value,list):return {'type':'list','value':[typed(v) for v in value]}
    if isinstance(value,dict):return {'type':'object','value':{k:typed(v) for k,v in value.items()}}
    return {'type':type(value).__name__,'value':value}


def inspect(body,request):
    if request!=spec(request.get('symbol'),request.get('endpoint'),request.get('period')):
        raise ValueError('Exact reviewed capital-structure request required')
    rows=strict(body)
    bound=request['limit'] or (10 if request['endpoint']=='quote' else 2000)
    if not isinstance(rows,list) or len(rows)>bound or any(not isinstance(row,dict) for row in rows):
        raise ValueError('Complete bounded provider array required; original is retained before parsing')
    fields,zeros,nulls=Counter(),Counter(),Counter();metadata=[];issues=[]
    for index,row in enumerate(rows):
        fields.update(row.keys());nulls.update(k for k,v in row.items() if v is None)
        zeros.update(k for k,v in row.items() if isinstance(v,(int,Decimal)) and not isinstance(v,bool) and v==0)
        metadata.append({'source_row':index,'metadata':{key:typed(row.get(key)) for key in IDENTITY},
            'metadata_fields_present':[key for key in IDENTITY if key in row]})
        if row.get('symbol')!=request['symbol']:issues.append({'source_row':index,'reason':'reported_symbol_differs_or_missing'})
        if request['endpoint'] in STATEMENTS:
            for field in ('date','filingDate'):
                value=row.get(field)
                try:
                    if not isinstance(value,str) or date.fromisoformat(value).isoformat()!=value:raise ValueError()
                except ValueError:issues.append({'source_row':index,'field':field,'reason':'missing_or_invalid_date'})
            if row.get('period') not in (('FY',) if request['period']=='annual' else ('Q1','Q2','Q3','Q4')):
                issues.append({'source_row':index,'field':'period','reason':'unexpected_fiscal_period'})
            if not isinstance(row.get('reportedCurrency'),str) or not re.fullmatch('[A-Z]{3}',row['reportedCurrency']):
                issues.append({'source_row':index,'field':'reportedCurrency','reason':'currency_unspecified'})
    return {'contract':'capital-structure-source-inventory.v1','rows':len(rows),
        'status':'provider_rows_retained' if rows else 'provider_returned_empty_array',
        'field_counts':dict(sorted(fields.items())),'null_field_counts':dict(sorted(nulls.items())),
        'zero_field_counts':dict(sorted(zeros.items())),'identity_metadata':metadata,'identity_issues':issues,
        'all_original_rows_retained':True,'current_sec_identity_verified':False,
        'share_split_comparability_verified':False,'statement_duration_verified':False,
        'free_float_change_qualified':False,'buyback_execution_qualified':False,
        'currency_alignment_verified':False,'forecast_qualified':False,'sizing_qualified':False}


def read(client,ref):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',ref.get('sha256',''))
            or ref.get('key')!=PRIVATE+ref['sha256']+'.bin'):
        raise ValueError('Exact retained share-structure reference required')
    body=bounded(client.get_object(Bucket=BUCKET,Key=ref['key'])['Body'],MAX)
    if len(body)!=ref['bytes'] or sha(body)!=ref['sha256']:raise ValueError('Original source bytes differ')
    return body


def retain(client,body):
    if not isinstance(body,bytes) or len(body)>MAX:raise ValueError('Whole bounded source required')
    ref={'key':PRIVATE+sha(body)+'.bin','sha256':sha(body),'bytes':len(body)}
    try:client.put_object(Bucket=BUCKET,Key=ref['key'],Body=body,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if read(client,ref)!=body:raise ValueError('Source retention readback differs')
    return ref


def request_key(request_id,label):
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Bounded durable request identity required')
    return PRIVATE+'requests/'+sha((request_id+':'+label).encode())+'.json'


def journal(client,key,value,claim=False):
    if not re.fullmatch(re.escape(PRIVATE)+r'requests/[a-f0-9]{64}\.json',key):raise ValueError('Protected source journal required')
    body=encode(value)
    if len(body)>MAX:raise ValueError('Bounded source journal required')
    client.put_object(Bucket=BUCKET,Key=key,Body=body,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if bounded(client.get_object(Bucket=BUCKET,Key=key)['Body'],MAX)!=body:raise ValueError('Journal readback differs')


def adopt(client,request_id,request,allowed,prior,body,origin,now):
    if (request!=spec(request.get('symbol'),request.get('endpoint'),request.get('period'))
            or request['symbol'] not in population(allowed) or prior.get('spec')!=request
            or prior.get('http_status')!=200 or sha(body)!=prior.get('original',{}).get('sha256')
            or len(body)!=prior.get('original',{}).get('bytes')):
        raise ValueError('Only exact successful original responses may be adopted')
    key=request_key(request_id,request['url'])
    value={'request_id':request_id,'spec':request,'status':'claimed','claimed_at':now(),
        'adopted_from_manifest':origin,'source_request_id':prior.get('request_id'),'transport_attempted':False}
    journal(client,key,value,True)
    try:
        capsule={**value,'original':retain(client,body),'http_status':200,'headers':prior['headers'],
            'requested_at':prior['requested_at'],'received_at':prior['received_at'],
            'inventory':inspect(body,request),'request_status_key':key,'reused_original':True}
        ref=retain(client,encode(capsule));journal(client,key,{**value,'status':'complete','capture':ref})
        return {**capsule,'retained_capture':ref}
    except Exception as exc:
        journal(client,key,{**value,'status':'failed','error_type':type(exc).__name__});raise


def capture(client,request_id,request,allowed,credential,rate,now,transport=None):
    if (request!=spec(request.get('symbol'),request.get('endpoint'),request.get('period'))
            or request['symbol'] not in population(allowed)):
        raise ValueError('Request outside the exact reviewed population')
    if not isinstance(credential,str) or not credential:raise ValueError('Existing managed provider credential required')
    key=request_key(request_id,request['url']);value={'request_id':request_id,'spec':request,'status':'claimed','claimed_at':now()}
    journal(client,key,value,True)
    try:
        rate.acquire()
        value.update(transport_claimed_at=now(),transport_claimed=True);journal(client,key,value)
        http=urllib.request.Request(request['url']+'&apikey='+urllib.parse.quote(credential,safe=''),
            headers={'User-Agent':'JustHodl Research ops@justhodl.ai','Accept':'application/json','Accept-Encoding':'identity'})
        value.update(requested_at=now(),transport_attempted=True)
        try:response=(transport or urllib.request.build_opener(NoRedirect()).open)(http,timeout=25)
        except urllib.error.HTTPError as exc:response=exc
        code=response.status
        headers={k.lower():v for k,v in response.headers.items() if k.lower() in ('content-type','content-length','date','etag','last-modified')}
        body=bounded(response,16*1024*1024)
        value.update(status='response_retained',received_at=now(),http_status=code,headers=headers,original=retain(client,body))
        journal(client,key,value)
        if code!=200 or not body:raise ValueError('Provider unavailable; complete response retained; no retry')
        capsule={**value,'inventory':inspect(body,request),'request_status_key':key}
        ref=retain(client,encode(capsule));journal(client,key,{**value,'status':'complete','capture':ref})
        return {**capsule,'retained_capture':ref}
    except Exception as exc:
        journal(client,key,{**value,'status':'failed','error_type':type(exc).__name__})
        raise RuntimeError('Source acquisition failed; inspect retained response and journal; never blindly retry') from None
