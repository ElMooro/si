"""Bounded option snapshot originals, protected retention, deterministic replay and CAS."""
from datetime import datetime,timezone
from pathlib import Path
import json,re,sys,time,urllib.request,urllib.error,urllib.parse
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from managed_secret import managed_secret
import tail_research_model as model
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
CONTEXT_KEYS=('data/tail-risk.json','data/tail-risk-history.json')
def allowed(key):return key in CONTEXT_KEYS or artifact(key)
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


def collect(client,bucket,started,deadline,opener=None,secret=None):
    secret=secret if secret is not None else managed_secret(('POLYGON_KEY','POLYGON_API_KEY','POLY_KEY'),('/justhodl/polygon/api-key',))
    lock=Lock();total=0
    def chain(symbol):
        nonlocal total
        transport=opener or urllib.request.build_opener(NoRedirect());url=model.initial_url(symbol,started);pages=[];seen=set();stop=None
        for page in range(1,model.MAX_PAGES+1):
            identity=model.sha(url.encode())
            if identity in seen:stop='pagination_cycle';break
            seen.add(identity)
            item={'page':page,'request_identity_sha256':identity,'acquired_at':now(),'status':'time_budget','http_status':None,'original':None}
            if not secret:item['status']='provider_not_configured';pages.append(item);stop=item['status'];break
            if time.monotonic()>=deadline:pages.append(item);stop='time_budget';break
            request=urllib.request.Request(url+'&apiKey='+urllib.parse.quote(secret,safe=''),headers={
                'User-Agent':'JustHodl-Tail-Research/2.0','Accept':'application/json','Accept-Encoding':'identity'})
            raw=None
            try:
                response=transport.open(request,timeout=min(15,max(1,deadline-time.monotonic())))
                item['http_status']=int(response.status);raw=bounded(response,model.MAX_PAGE_BYTES)
            except urllib.error.HTTPError as exc:item['http_status']=int(exc.code);raw=bounded(exc,model.MAX_PAGE_BYTES)
            except (ValueError,OSError,TimeoutError):item['status']='transport_or_body_failure'
            item['acquired_at']=now()
            if raw is not None:
                with lock:
                    total+=len(raw)
                    if total>model.MAX_TOTAL_BYTES:raise ValueError('Total option-original byte bound exceeded')
                item['original']=original(client,bucket,raw);item['status']='received'
            pages.append(item)
            if item['status']!='received':stop=item['status'];break
            if item['http_status']!=200:stop='provider_http_failure';break
            try:
                doc=model.decode(raw)
                if not isinstance(doc,dict) or doc.get('status') not in ('OK','DELAYED') or not isinstance(doc.get('results'),list) or len(doc['results'])>250:
                    stop='invalid_provider_envelope';break
            except (ValueError,UnicodeDecodeError):stop='invalid_provider_envelope';break
            if not doc.get('next_url'):stop='complete';break
            try:
                url=model.next_url(doc['next_url'],symbol)
                if model.sha(url.encode()) in seen:stop='pagination_cycle';break
            except ValueError:stop='invalid_pagination_address';break
        return {'pages':pages,'stop':stop or 'page_limit'}
    with ThreadPoolExecutor(max_workers=3) as pool:
        chains=dict(zip(model.SYMBOLS,pool.map(chain,model.SYMBOLS)))
    contexts={}
    for key in CONTEXT_KEYS:
        try:raw=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        except Exception as exc:
            if not missing(exc):raise
            contexts[key]={'status':'missing','measurement_eligible':False};continue
        contexts[key]={'status':'retained_unqualified_context','original':original(client,bucket,raw),'measurement_eligible':False}
    return {'chains':chains,'context_evidence':contexts,'source_bytes':total}
def verified_original(ref,read):
    digest=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PRIVATE+digest+'.bin':raise ValueError('Original identity differs')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or model.sha(raw)!=digest:raise ValueError('Original bytes differ')
    return raw


def compile_output(inputs,read):
    if inputs.get('contract')!='tail-native-inputs.v1':raise ValueError('Unsupported tail inputs')
    start=model.clock(inputs['started_at']);end=model.clock(inputs['generated_at'])
    if not 0<=(end-start).total_seconds()<=150:raise ValueError('Collection time outside bound')
    collection=inputs['collection'];chains=collection['chains'];contexts=collection['context_evidence'];total=0;requests=0;restored={}
    if set(chains)!=set(model.SYMBOLS) or set(contexts)!=set(CONTEXT_KEYS):raise ValueError('Exact source inventory required')
    for symbol in model.SYMBOLS:
        chain=chains[symbol];pages=chain['pages'];url=model.initial_url(symbol,inputs['started_at']);seen=set();out=[];expected_stop=None;previous=start
        if not isinstance(pages,list) or not 1<=len(pages)<=model.MAX_PAGES:raise ValueError('Page count outside bound')
        for index,item in enumerate(pages):
            identity=model.sha(url.encode());received=model.clock(item['acquired_at'])
            if expected_stop or identity in seen:raise ValueError('Unexpected page after terminal or cyclic cursor')
            if item['page']!=index+1 or type(item['page']) is not int or item['request_identity_sha256']!=identity or not previous<=received<=end:
                raise ValueError('Page identity or receipt clock differs')
            seen.add(identity);previous=received;row=dict(item);row['raw']=None
            if item['status']=='received':
                if type(item['http_status']) is not int or not 100<=item['http_status']<=599:raise ValueError('HTTP outcome required')
                raw=verified_original(item['original'],read);total+=len(raw);requests+=1;row['raw']=raw
                if len(raw)>model.MAX_PAGE_BYTES:raise ValueError('Original page too large')
                if item['http_status']!=200:expected_stop='provider_http_failure'
                else:
                    try:doc=model.decode(raw)
                    except (ValueError,UnicodeDecodeError):doc=None
                    if not isinstance(doc,dict) or doc.get('status') not in ('OK','DELAYED') or not isinstance(doc.get('results'),list) or len(doc['results'])>250:
                        expected_stop='invalid_provider_envelope';row['raw']=None
                    elif not doc.get('next_url'):expected_stop='complete'
                    else:
                        try:
                            url=model.next_url(doc['next_url'],symbol)
                            if model.sha(url.encode()) in seen:expected_stop='pagination_cycle'
                        except ValueError:expected_stop='invalid_pagination_address'
            elif item['status'] in ('time_budget','provider_not_configured','transport_or_body_failure'):
                if item.get('original') is not None:raise ValueError('No invented source original')
                if item['status']!='transport_or_body_failure' and item['http_status'] is not None:raise ValueError('Unrequested page cannot claim HTTP response')
                requests+=int(item['status']=='transport_or_body_failure');expected_stop=item['status']
            else:raise ValueError('Unsupported source outcome')
            out.append(row)
        if expected_stop is None:
            if len(pages)!=model.MAX_PAGES:raise ValueError('Unexplained truncated pagination')
            expected_stop='page_limit'
        if chain['stop']!=expected_stop:raise ValueError('Pagination completeness differs from original')
        restored[symbol]={'pages':out,'stop':expected_stop}
    if total!=collection['source_bytes'] or total>model.MAX_TOTAL_BYTES:raise ValueError('Source byte total differs')
    for item in contexts.values():
        if item.get('measurement_eligible') is not False:raise ValueError('Old output cannot self-qualify')
        if item['status']=='retained_unqualified_context':verified_original(item['original'],read)
        elif item['status']!='missing' or 'original' in item:raise ValueError('Unsupported retained predecessor')
    output=model.compute(restored,inputs['started_at'],inputs['generated_at'],contexts)
    output['collection']={'source_bytes':total,'provider_requests':requests,'started_at':inputs['started_at'],'completed_at':inputs['generated_at']}
    return output
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
    if key != PREFIX+'runs/'+model.sha(raw)+'.json' or manifest.get('contract') != 'tail-native-replay.v1':
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
        raise ValueError('native Tail original-source replay differs')
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
    manifest = {'contract': 'tail-native-replay.v1', 'generated_at': output['generated_at'], **refs,
        'compilers': compilers, 'output_sha256': refs['output']['sha256'],
        'scope': 'Exact bounded option snapshot originals; contract identity, source clocks, pagination and vendor-model comparisons. No density, timing or portfolio qualification.'}
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
    status={'contract':'tail-public-request.v1','request_id':request_id,'execution_id':execution_id,'started_at':started,'status':'running','phase':'collect'}
    try:status_write(client,bucket,key,status,IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):raise
        return json.loads(bounded(client.get_object(Bucket=bucket,Key=key)['Body']))
    try:
        collection=collect(client,bucket,started,time.monotonic()+max(0,min(95,remaining_seconds-75)))
        inputs={'contract':'tail-native-inputs.v1','started_at':started,'generated_at':now(),'collection':collection}
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
        raise RuntimeError('Native Tail publication failed; inspect reviewed request evidence') from None
