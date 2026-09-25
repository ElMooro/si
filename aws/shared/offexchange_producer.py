"""Scoped native acquisition and conditional publication for FINRA research.

Public FINRA requests only. Durable request identities prevent repeat capture;
failed captures do not relabel old observations as newly acquired measurements.
"""
from datetime import date,datetime,timedelta,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed
import re,time,urllib.request,urllib.error
import offexchange_research_model as model
import offexchange_research_store as store
import offexchange_measurements as measures

MAX=8*1024*1024
HEADERS=('record-total','record-offset','record-limit','total-records-on-page','record-max-limit','finra-api-request-id','content-type','content-length')
def now():return datetime.now(timezone.utc).isoformat()
def missing(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey')
def request_key(request_id):
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Bounded durable request identity required')
    return model.PRIVATE+'requests/'+model.sha(request_id.encode())+'.json'
def raw_read(client,bucket,key):return store.bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
def journal(client,bucket,key,value,claim=False):
    if not re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}\.json',key):raise ValueError('Reviewed request journal required')
    raw=model.encoded(value);client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if raw_read(client,bucket,key)!=raw:raise ValueError('Journal readback differs')
def protect(client,bucket,raw):
    if not isinstance(raw,bytes) or len(raw)>MAX:raise ValueError('Whole bounded original required')
    ref={'key':model.PRIVATE+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)}
    try:client.put_object(Bucket=bucket,Key=ref['key'],Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):raise
    if raw_read(client,bucket,ref['key'])!=raw:raise ValueError('Original readback differs')
    return ref

def advertised(doc,dataset,today):
    fields=['weekStartDate','tierIdentifier'] if dataset=='weeklySummary' else ['monthStartDate','tierIdentifier']
    if not isinstance(doc,dict) or doc.get('partitionFields')!=fields or not isinstance(doc.get('availablePartitions'),list) or len(doc['availablePartitions'])>20000:raise ValueError('Advertised partition schema differs')
    values={};seen=set()
    for row in doc['availablePartitions']:
        part=row.get('partitions') if isinstance(row,dict) else None
        if not isinstance(part,list) or len(part)!=2 or any(not isinstance(v,str) for v in part):raise ValueError('Advertised partition dimensions differ')
        stamp=measures.day(part[0]);key=tuple(part)
        if key in seen:raise ValueError('Duplicate advertised partition')
        seen.add(key)
        if date.fromisoformat(stamp)>today:raise ValueError('Advertised future partition')
        values.setdefault(part[1],set()).add(stamp)
    return values

def plan(today,weekly_doc,monthly_doc):
    if type(today) is not date:raise ValueError('Explicit planning date required')
    weekly=advertised(weekly_doc,'weeklySummary',today);monthly=advertised(monthly_doc,'monthlySummary',today);partitions=[]
    for tier in ('T1','T2'):
        if not weekly.get(tier):raise ValueError('Required weekly reporting tier unavailable')
        period=max(weekly[tier])
        for code in ('ATS_W_SMBL','OTC_W_SMBL'):partitions.append({'dataset':'weeklySummary','code':code,'period':period,'tier':tier})
    months=sorted(monthly.get('NMS',()),reverse=True)[:2]
    if len(months)!=2:raise ValueError('Two advertised NMS monthly periods required')
    for month in months:partitions.append({'dataset':'monthlySummary','code':'OTC_M_SMBL_FIRM','period':month,'tier':'NMS'})
    days=[];cursor=today
    while len(days)<3:
        if cursor.weekday()<5:days.append(cursor.isoformat())
        cursor-=timedelta(days=1)
    return {'planning_date':today.isoformat(),'partitions':partitions,'daily_candidate_dates':days,
        'selection_basis':'Latest provider-advertised T1/T2 periods and two NMS months. Category completeness is checked separately. Daily candidates probe at most three distinct weekdays.'}

def specification(part,offset):
    if type(offset) is not int or not 0<=offset<=500000:raise ValueError('Bounded offset required')
    dataset=part.get('dataset');weekly=dataset=='weeklySummary';period=measures.day(part.get('period'))
    if not (weekly and part.get('code') in measures.WEEKLY_CODES and part.get('tier') in ('T1','T2') or dataset=='monthlySummary' and part.get('code')=='OTC_M_SMBL_FIRM' and part.get('tier')=='NMS'):raise ValueError('Reviewed FINRA partition required')
    field='weekStartDate' if weekly else 'monthStartDate'
    return {'url':'https://api.finra.org/data/group/otcMarket/name/'+dataset,'kind':'probe','body':{'limit':2000,'offset':offset,
        'compareFilters':[{'fieldName':k,'compareType':'EQUAL','fieldValue':v} for k,v in (('summaryTypeCode',part['code']),(field,period),('tierIdentifier',part['tier']))],
        'sortFields':['issueSymbolIdentifier','issueName'] if weekly else ['issueSymbolIdentifier','issueName','firmCRDNumber']}}

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('Unreviewed redirect refused')

def fetch(client,bucket,request_id,label,spec,deadline,transport=None):
    if not (re.fullmatch(r'https://api\.finra\.org/(?:data|partitions)/group/otcMarket/name/(?:weeklySummary|monthlySummary)',spec['url']) or re.fullmatch(r'https://cdn\.finra\.org/equity/regsho/daily/CNMSshvol\d{8}\.txt',spec['url'])):raise ValueError('Reviewed public FINRA endpoint required')
    key=request_key(model.sha((request_id+':'+label).encode()));journal(client,bucket,key,{'request_id':request_id,'source_id':label,'status':'claimed','specification':spec},True)
    requested=now()
    try:
        if time.monotonic()>=deadline:raise TimeoutError('Acquisition deadline')
        opener=transport or urllib.request.build_opener(NoRedirect()).open
        request=urllib.request.Request(spec['url'],data=model.encoded(spec['body']) if spec['body'] is not None else None,
            headers={'User-Agent':'JustHodl-offexchange-research/3.0','Accept':'text/plain' if spec['kind']=='daily_file' else 'application/json','Content-Type':'application/json'})
        try:response=opener(request,timeout=max(1,min(18,deadline-time.monotonic())))
        except urllib.error.HTTPError as exc:response=exc
        code=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in HEADERS}
        raw=store.bounded(response);ref=protect(client,bucket,raw)
        result={**spec,'requested_at':requested,'received_at':now(),'http_status':code,'headers':headers,'original':ref,
            'status':'response_retained' if code==200 and raw else 'empty_http_response' if not raw else 'provider_error_retained'}
        journal(client,bucket,key,{'request_id':request_id,'source_id':label,'status':'complete','capture':result});return result
    except Exception as exc:
        journal(client,bucket,key,{'request_id':request_id,'source_id':label,'status':'failed','error_type':type(exc).__name__,'specification':spec});raise

def collect_partition(client,bucket,request_id,part,deadline,transport=None):
    label='/'.join(part.values());state=request_key(model.sha((request_id+':partition:'+label).encode()));pages=[];offset=0;total=None;seen=set();used=0
    journal(client,bucket,state,{'request_id':request_id,'status':'claimed','partition':part},True)
    try:
        for index in range(40):
            spec=specification(part,offset);capture=fetch(client,bucket,request_id,label+':page:'+str(index),spec,deadline,transport);pages.append(capture)
            journal(client,bucket,state,{'request_id':request_id,'status':'capturing','partition':part,'pages':pages})
            if capture['http_status']!=200 or capture['status']!='response_retained':raise ValueError('Expected partition is unavailable')
            raw=model.original(capture['original'],store.reader(client,bucket));used+=len(raw)
            if used>48*1024*1024:raise ValueError('Partition byte bound exceeded')
            boundary=measures.page(raw,capture['headers'],offset,2000)
            if total is None:total=boundary['reported_total']
            if total!=boundary['reported_total']:raise ValueError('Reported count changed')
            rows=measures.weekly(raw,part['code'],part['period'],part['tier']) if part['dataset']=='weeklySummary' else measures.monthly(raw,part['period'],part['tier'])
            for row in rows:
                identity=(row['symbol'],row['reported_issue_name'],row.get('firm_crd'))
                if identity in seen:raise ValueError('Duplicate reported issue grain')
                seen.add(identity)
            offset=boundary['next_offset']
            if boundary['reported_end_reached']:break
        else:raise ValueError('Incomplete partition at page bound')
        if not total or len(seen)!=total:raise ValueError('Complete nonempty expected partition required')
        recheck=fetch(client,bucket,request_id,label+':first-page-recheck',specification(part,0),deadline,transport);pages.append(recheck)
        if recheck['status']!='response_retained' or recheck['http_status']!=200 or recheck['original']!=pages[0]['original'] or recheck['headers'].get('record-total')!=str(total):raise ValueError('First page changed during capture')
        result={'partition':part,'pages':pages,'rows':len(seen),'reported_total':total,'records_reconciled':True,'snapshot_atomic':False,'first_page_recheck_matched':True}
        journal(client,bucket,state,{'request_id':request_id,'status':'complete','result':result});return result
    except Exception as exc:
        journal(client,bucket,state,{'request_id':request_id,'status':'failed','partition':part,'pages':pages,'error_type':type(exc).__name__});raise

def collect_daily(client,bucket,request_id,dates,deadline,transport=None):
    attempts=[]
    for day in dates:
        spec={'url':'https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+day.replace('-','')+'.txt','body':None,'kind':'daily_file'}
        result=fetch(client,bucket,request_id,'daily:'+day,spec,deadline,transport);attempts.append(result)
        if result['http_status']==200 and result['status']=='response_retained':
            measures.cnms(model.original(result['original'],store.reader(client,bucket)),day)
            return {'daily':result,'daily_date':day,'daily_candidates':attempts}
        if result['http_status'] not in (403,404,204):raise ValueError('Unexpected daily provider failure')
    raise ValueError('No complete CNMS file within the explicit candidate dates')

def not_older(packet,previous):
    old=previous.get('generated_at')
    if old and model.clock(packet['generated_at'])<=model.clock(old):return False
    if previous.get('contract')!=model.CONTRACT:return True
    if packet['daily']['date']<previous['daily']['date']:return False
    latest={}
    for row in packet['coverage']:
        key=(row['dataset'],row['category'],row['tier']);latest[key]=max(latest.get(key,''),row['period_start'])
    return all(latest.get((row['dataset'],row['category'],row['tier']),'')>=row['period_start'] for row in previous['coverage'])

def run(client,bucket,request_id,execution_id,remaining_seconds=300,transport=None):
    if not isinstance(execution_id,str) or not execution_id:raise ValueError('Actual execution identity required')
    if remaining_seconds<160:raise ValueError('Insufficient acquisition and replay budget')
    status=request_key(request_id)
    try:old_status=model.strict(raw_read(client,bucket,status))
    except Exception as exc:
        if not missing(exc):raise
        old_status=None
    if old_status:
        if old_status.get('status')!='complete':raise ValueError('Request already attempted; inspect retained status instead of recollecting')
        return old_status['result']
    journal(client,bucket,status,{'request_id':request_id,'execution_id':execution_id,'status':'claimed','started_at':now()},True)
    progress={}
    try:
        head=client.get_object(Bucket=bucket,Key=model.CURRENT);previous_raw=store.bounded(head['Body']);previous=model.strict(previous_raw);etag=head['ETag']
        predecessor=protect(client,bucket,previous_raw);deadline=time.monotonic()+min(205,remaining_seconds-95)
        discovery={};documents={}
        for dataset in ('weeklySummary','monthlySummary'):
            spec={'url':'https://api.finra.org/partitions/group/otcMarket/name/'+dataset,'kind':'partition_discovery','body':None}
            result=fetch(client,bucket,request_id,'discovery:'+dataset,spec,deadline,transport)
            if result['http_status']!=200 or result['status']!='response_retained':raise ValueError('Partition discovery unavailable')
            discovery[dataset]=result;documents[dataset]=model.strict(model.original(result['original'],store.reader(client,bucket)))
        selection=plan(datetime.now(timezone.utc).date(),documents['weeklySummary'],documents['monthlySummary'])
        progress.update(selection=selection,partition_discovery=discovery,predecessor=predecessor)
        captures={};errors={};daily=None
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs={pool.submit(collect_partition,client,bucket,request_id,part,deadline,transport):'/'.join(part.values()) for part in selection['partitions']}
            jobs[pool.submit(collect_daily,client,bucket,request_id,selection['daily_candidate_dates'],deadline,transport)]='daily'
            for future in as_completed(jobs):
                name=jobs[future]
                try:
                    value=future.result()
                    if name=='daily':daily=value
                    else:captures[name]=value
                except Exception as exc:errors[name]=type(exc).__name__
                progress.update(partitions=captures,daily=daily,errors=errors)
                journal(client,bucket,status,{'request_id':request_id,'execution_id':execution_id,'status':'capturing',**progress})
        if errors or daily is None:raise ValueError('One or more planned sources failed; current head retained')
        inputs={'contract':'offexchange-original-inputs.v1','generated_at':now(),'execution_id':execution_id,'request_id':request_id,
            'selection':selection,'partition_discovery':discovery,'predecessor':predecessor,'partitions':captures,**daily}
        compiled=model.compile_output(inputs,store.reader(client,bucket));ref=store.retain(client,bucket,inputs,compiled)
        expected=model.digest(compiled['packet']);del compiled
        replayed=store.replay(ref,store.reader(client,bucket));packet=replayed['packet'];assert model.digest(packet)==expected
        if not not_older(packet,previous):result={'published':False,'reason':'observation_or_publication_rollback','replay':ref}
        else:
            published={**packet,'replay':ref}
            try:client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(published),ContentType='application/json',CacheControl='no-store',IfMatch=etag)
            except Exception as exc:
                if not store.conflict(exc):raise
                result={'published':False,'reason':'concurrent_publication','replay':ref}
            else:
                if raw_read(client,bucket,model.CURRENT)!=model.encoded(published):raise ValueError('Native publication readback differs')
                result={'published':True,'generated_at':packet['generated_at'],'counts':packet['counts'],'replay':ref}
        journal(client,bucket,status,{'request_id':request_id,'execution_id':execution_id,'status':'complete','result':result});return result
    except Exception as exc:
        journal(client,bucket,status,{'request_id':request_id,'execution_id':execution_id,'status':'failed','error_type':type(exc).__name__,**progress});raise
