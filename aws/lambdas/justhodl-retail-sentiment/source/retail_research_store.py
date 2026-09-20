"""Bounded public attention originals, protected retention, deterministic replay and CAS."""
from datetime import datetime,timezone
from pathlib import Path
import json,re,sys,time,urllib.request,urllib.error,urllib.parse
import retail_research_model as model
PREFIX=model.PREFIX;PRIVATE=model.PRIVATE;CURRENT=model.CURRENT;MAX=64*1024*1024
COMPILERS=(model,sys.modules[__name__])

def now():return datetime.now(timezone.utc).isoformat()
def bounded(stream,limit=MAX):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    if len(raw)>limit:raise ValueError('Research response exceeds byte bound')
    return raw

def error_code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def missing(exc):return error_code(exc) in ('404','NoSuchKey')
def conflict(exc):return error_code(exc) in ('409','412','PreconditionFailed','ConditionalRequestConflict')
def artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin|'+re.escape(PREFIX)+r'(?:inputs|outputs|runs|compilers)/[a-f0-9]{64}\.(?:json|py)',key))
def allowed(key):return key==CURRENT or artifact(key)
def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('Unapproved source path')
        return bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    return read

def immutable(client,bucket,key,raw,kind='application/json'):
    if not artifact(key) or not isinstance(raw,bytes) or len(raw)>MAX:raise ValueError('Bounded immutable artifact required')
    if key.rsplit('/',1)[-1].split('.')[0]!=model.sha(raw):raise ValueError('Content address differs')
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType=kind,IfNoneMatch='*',CacheControl='no-store' if key.startswith(PRIVATE) else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if reader(client,bucket)(key)!=raw:raise ValueError('Immutable readback differs')

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):raise ValueError('Provider redirect refused')

def original(client,bucket,raw):
    key=PRIVATE+model.sha(raw)+'.bin';immutable(client,bucket,key,raw,'application/octet-stream')
    return {'key':key,'sha256':model.sha(raw),'bytes':len(raw)}

def collect(client,bucket,deadline,opener=None):
    opener=opener or urllib.request.build_opener(NoRedirect());pages=[];blocked=set();total=0
    def fetch(kind,identity,page=1):
        nonlocal total
        url=model.source_url(kind,identity,page);item={'kind':kind,'identity':identity,'page':page,'request_url':url,
            'acquired_at':now(),'http_status':None,'status':'time_budget','original':None}
        if kind in blocked:item['status']='provider_access_or_rate_limit';pages.append(item);return None
        if time.monotonic()>=deadline:pages.append(item);return None
        req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0 JustHodl-Retail-Research/2.0','Accept':'application/json','Accept-Encoding':'identity'})
        raw=None
        try:
            response=opener.open(req,timeout=min(10,max(1,deadline-time.monotonic())))
            item['http_status']=int(response.status);raw=bounded(response,model.MAX_BYTES)
        except urllib.error.HTTPError as exc:
            item['http_status']=int(exc.code);raw=bounded(exc,model.MAX_BYTES)
        except (ValueError,OSError,TimeoutError):item['status']='provider_transport_or_body_failure'
        item['acquired_at']=now()
        if item['http_status'] in (401,403,429):blocked.add(kind)
        if raw is not None:
            if total+len(raw)>32*1024*1024:raise ValueError('Original provider total bound exceeded')
            total+=len(raw);item['original']=original(client,bucket,raw);item['status']='received'
        pages.append(item)
        if raw is not None and item['http_status']==200:
            try:return model.decode(raw)
            except (ValueError,UnicodeDecodeError):return None
        return None
    first=None
    for category,n in model.CATEGORIES.items():
        for page in range(1,n+1):
            data=fetch('apewisdom',category,page)
            if category=='all-stocks' and page==1:first=data
    fetch('stocktwits','trending')
    candidates=selected_symbols(first)
    for ticker in candidates:fetch('stocktwits',ticker)
    contexts={}
    for key in model.CONTEXT_KEYS:
        if time.monotonic()>=deadline+25:raise ValueError('Context retention time bound exceeded')
        try:raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        except Exception as exc:
            if not missing(exc):raise
            contexts[key]={'status':'missing','measurement_eligible':False};continue
        contexts[key]={'status':'retained_unqualified_context','original':original(client,bucket,raw),'measurement_eligible':False}
    return {'pages':pages,'context_evidence':contexts,'source_bytes':total}

def selected_symbols(first):
    selected=[]
    for r in (first or {}).get('results',[]) if isinstance((first or {}).get('results'),list) else []:
        ticker=model.symbol(r.get('ticker')) if isinstance(r,dict) else None
        if ticker and ticker not in selected:selected.append(ticker)
        if len(selected)==25:break
    return selected

def verified_original(ref,read):
    digest=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PRIVATE+digest+'.bin':raise ValueError('Original identity differs')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or model.sha(raw)!=digest:raise ValueError('Original bytes differ')
    return raw

def compile_output(inputs,read):
    if inputs.get('contract')!='retail-native-inputs.v1':raise ValueError('Unsupported inputs')
    started=model.clock(inputs['started_at']);completed=model.clock(inputs['generated_at'])
    if not 0<=(completed-started).total_seconds()<=150:raise ValueError('Collection clock exceeds reviewed bound')
    collection=inputs['collection'];items=collection['pages'];contexts=collection['context_evidence']
    if not isinstance(items,list) or not 6<=len(items)<=31 or set(contexts)!=set(model.CONTEXT_KEYS):raise ValueError('Exact reviewed inventory required')
    pages=[];total=0;first=None;expected=[('apewisdom',k,p) for k,n in model.CATEGORIES.items() for p in range(1,n+1)]+[('stocktwits','trending',1)]
    for index,item in enumerate(items):
        if index==6:expected += [('stocktwits',s,1) for s in selected_symbols(first)]
        if index>=len(expected) or (item['kind'],item['identity'],item['page'])!=expected[index]:raise ValueError('Source inventory or order differs')
        if item['request_url']!=model.source_url(*expected[index]) or not started<=model.clock(item['acquired_at'])<=completed:raise ValueError('Source URL or clock differs')
        if item['status'] not in ('received','time_budget','provider_access_or_rate_limit','provider_transport_or_body_failure'):raise ValueError('Unsupported source outcome')
        row=dict(item)
        if item['status']=='received':
            if type(item['http_status']) is not int or not 100<=item['http_status']<=599:raise ValueError('HTTP outcome required')
            raw=verified_original(item['original'],read);total+=len(raw)
            if len(raw)>model.MAX_BYTES:raise ValueError('Provider response bound exceeded')
            row['raw']=raw
            if index==0 and item['http_status']==200:
                try:first=model.decode(raw)
                except (ValueError,UnicodeDecodeError):pass
        elif item.get('original') is not None:raise ValueError('Unreceived source must not have invented original')
        pages.append(row)
    if len(items)==6:expected += [('stocktwits',s,1) for s in selected_symbols(first)]
    if len(items)!=len(expected) or total!=collection['source_bytes'] or total>32*1024*1024:raise ValueError('Collection counts differ')
    for item in contexts.values():
        if item.get('measurement_eligible') is not False:raise ValueError('Context cannot self-qualify')
        if item['status']=='retained_unqualified_context':verified_original(item['original'],read)
        elif item['status']!='missing' or 'original' in item:raise ValueError('Unsupported retained context')
    out=model.compute(pages,contexts,inputs['generated_at']);out['elapsed_s']=round((completed-started).total_seconds(),3)
    out['collection']={'started_at':inputs['started_at'],'completed_at':inputs['generated_at'],'source_bytes':total,'provider_requests':sum(p['status'] not in ('time_budget','provider_access_or_rate_limit') for p in pages),'expected_source_slots':len(pages)}
    return out

def checked(ref, category, read):
    digest = ref.get('sha256', '')
    if not re.fullmatch('[a-f0-9]{64}', digest) or ref.get('key') != PREFIX+category+'/'+digest+'.json':
        raise ValueError('research artifact identity differs')
    raw = read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw) != ref['bytes'] or model.sha(raw) != digest:
        raise ValueError('research artifact bytes differ')
    return json.loads(raw)


def replay(ref, read):
    key = ref.get('manifest_key', '')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json', key): raise ValueError('run identity required')
    raw = read(key); manifest = json.loads(raw)
    if key != PREFIX+'runs/'+model.sha(raw)+'.json' or manifest.get('contract') != 'retail-native-replay.v1':
        raise ValueError('run content identity differs')
    if set(manifest['compilers']) != {m.__name__ for m in COMPILERS}: raise ValueError('compiler inventory differs')
    for module in COMPILERS:
        code = Path(module.__file__).read_bytes(); digest = model.sha(code); compiler = manifest['compilers'][module.__name__]
        if compiler != {'key': PREFIX+'compilers/'+digest+'.py', 'sha256': digest} or read(compiler['key']) != code:
            raise ValueError('matching reviewed compiler release required')
    inputs = checked(manifest['input'], 'inputs', read)
    result = compile_output(inputs, read)
    if (result != checked(manifest['output'], 'outputs', read) or model.sha(model.encoded(result)) != ref.get('output_sha256')
            or manifest['output_sha256'] != ref['output_sha256'] or result['generated_at'] != manifest['generated_at']):
        raise ValueError('native Retail original-source replay differs')
    return result


def retain(client, bucket, inputs, output):
    refs = {}
    for name, doc in (('input', inputs), ('output', output)):
        raw = model.encoded(doc); digest = model.sha(raw); key = PREFIX+name+'s/'+digest+'.json'
        immutable(client, bucket, key, raw); refs[name] = {'key': key, 'sha256': digest, 'bytes': len(raw)}
    compilers = {}
    for module in COMPILERS:
        raw = Path(module.__file__).read_bytes(); digest = model.sha(raw); key = PREFIX+'compilers/'+digest+'.py'
        immutable(client, bucket, key, raw, 'text/x-python'); compilers[module.__name__] = {'key': key, 'sha256': digest}
    manifest = {'contract': 'retail-native-replay.v1', 'generated_at': output['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': refs['output']['sha256'],
        'scope': 'Exact bounded public attention originals; vendor community counts and self-tagged message samples. No flow, population, timing or portfolio qualification.'}
    raw = model.encoded(manifest); key = PREFIX+'runs/'+model.sha(raw)+'.json'; immutable(client, bucket, key, raw)
    ref = {'manifest_key': key, 'output_sha256': refs['output']['sha256']}
    if replay(ref, reader(client, bucket)) != output: raise ValueError('retained replay differs')
    return ref


def publish(client, bucket, packet):
    at = model.clock(packet['generated_at'])
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=CURRENT); raw = bounded(obj['Body']); old = json.loads(raw)
            old_clock = old.get('generated_at') or old.get('as_of')
            if old_clock and model.clock(old_clock) > at: return False
            if old_clock and model.clock(old_clock) == at and old != packet: raise ValueError('conflicting same-clock publication')
            if old.get('contract') == model.CONTRACT and old.get('as_of') and packet.get('as_of') and old['as_of'] > packet['as_of']: return False
            immutable(client, bucket, PRIVATE+model.sha(raw)+'.bin', raw, 'application/octet-stream')
            condition = {'IfMatch': obj['ETag']}
        except Exception as exc:
            if not missing(exc): raise
            condition = {'IfNoneMatch': '*'}
        try:
            client.put_object(Bucket=bucket, Key=CURRENT, Body=model.encoded(packet), ContentType='application/json',
                CacheControl='no-store', **condition)
            live = json.loads(reader(client, bucket)(CURRENT))
            if live != packet and model.clock(live['generated_at']) <= at: raise ValueError('publication readback differs')
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('publication conflict limit; immutable run retained')


def request_key(request_id):
    if not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', request_id):
        raise ValueError('canonical idempotent request id required')
    return PREFIX+'requests/'+model.sha(request_id.encode())+'.json'


def status_write(client, bucket, key, doc, **condition):
    client.put_object(Bucket=bucket, Key=key, Body=model.encoded(doc), ContentType='application/json', CacheControl='no-store', **condition)


def run(client,bucket,request_id,execution_id,remaining_seconds=180):
    started=now();key=request_key(request_id)
    status={'contract':'retail-public-request.v1','request_id':request_id,'execution_id':execution_id,'started_at':started,'status':'running','phase':'collect'}
    try:status_write(client,bucket,key,status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    try:
        collection=collect(client,bucket,time.monotonic()+max(0,min(85,remaining_seconds-85)))
        inputs={'contract':'retail-native-inputs.v1','started_at':started,'generated_at':now(),'collection':collection}
        status['phase']='compile';status_write(client,bucket,key,status)
        output=compile_output(inputs,reader(client,bucket))
        status['phase']='retained_replay';status_write(client,bucket,key,status)
        ref=retain(client,bucket,inputs,output)
        status['phase']='publish';status_write(client,bucket,key,status)
        published=publish(client,bucket,{**output,'replay':ref})
        result={**status,'status':'complete','phase':'complete','completed_at':now(),'published':published,'generated_at':output['generated_at'],'quality':output['quality'],'replay':ref,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0}
        status_write(client,bucket,key,result);return result
    except Exception:
        status_write(client,bucket,key,{**status,'status':'failed','completed_at':now(),'error':'native_collection_replay_or_publication_failed'})
        raise RuntimeError('Native Retail publication failed; inspect reviewed request evidence') from None
