"""Complete native acquisition ledger; provider cache clocks are never renewed.

Archives are private. Stored warehouse objects remain derived inputs, not original
Polygon acquisitions or point-in-time evidence. No forecast authority is granted.
"""
from collections import defaultdict,deque
from copy import deepcopy
from datetime import datetime,timezone
from io import BytesIO
from threading import Lock
from types import SimpleNamespace
import json,re,time,urllib.error,urllib.parse,urllib.request
import business_cycle_store as store

FIXED={'data/global-business-cycle.json','data/cycle/features.json.gz','data/portwatch.json'}
WAREHOUSE='data/warm/polygon-full/grouped/'
CONTRACT='business-cycle-native-acquisition.v1'
_CACHE={}
_CACHE_LOCK=Lock()


class AcquisitionError(store.PublicationError):pass


def bounded(body,headers=None):
    chunks=[];size=0
    try:
        while True:
            part=body.read(min(65536,store.LIMIT+1-size))
            if not part:break
            if not isinstance(part,bytes):raise AcquisitionError('Complete byte response required')
            chunks.append(part);size+=len(part)
            if size>store.LIMIT:raise AcquisitionError('Whole acquisition exceeds byte bound')
    finally:body.close()
    raw=b''.join(chunks)
    length=(headers or {}).get('Content-Length')
    if length is not None and (not str(length).isdigit() or int(length)!=len(raw)):
        raise AcquisitionError('Incomplete acquisition response')
    return raw


def request_identity(request,timeout):
    url=request.full_url if hasattr(request,'full_url') else request
    parts=urllib.parse.urlsplit(url);pairs=urllib.parse.parse_qsl(parts.query,keep_blank_values=True)
    query=dict(pairs)
    if len(query)!=len(pairs) or parts.scheme!='https' or parts.username or parts.password or parts.port or parts.fragment:
        raise AcquisitionError('Unreviewed provider identity')
    if getattr(request,'data',None) is not None or (hasattr(request,'get_method') and request.get_method()!='GET'):
        raise AcquisitionError('Only native public GET acquisitions allowed')
    if parts.hostname=='api.stlouisfed.org' and parts.path=='/fred/series/observations':
        if (set(query)!={'series_id','api_key','file_type','sort_order','limit'} or query['file_type']!='json'
                or query['sort_order']!='desc' or not re.fullmatch('[A-Za-z0-9_]+',query['series_id'])
                or not query['limit'].isdigit() or not 1<=int(query['limit'])<=10000):
            raise AcquisitionError('Unreviewed FRED observation request')
        query.pop('api_key')
        provider='FRED'
    elif parts.hostname=='query1.finance.yahoo.com' and parts.path.startswith('/v8/finance/chart/'):
        symbol=urllib.parse.unquote(parts.path.removeprefix('/v8/finance/chart/'))
        if not symbol or len(symbol)>64 or '/' in symbol or set(query)!={'range','interval'} or query['interval']!='1d' or query['range'] not in {'2y','5y'}:
            raise AcquisitionError('Unreviewed Yahoo chart request')
        provider='Yahoo Finance'
    else:raise AcquisitionError('Unreviewed provider host or route')
    if type(timeout) not in (float,int) or not 0<timeout<=30:raise AcquisitionError('Native bounded timeout required')
    return {'provider':provider,'method':'GET','host':parts.hostname,'path':parts.path,'query_without_credentials':query,'timeout_s':timeout}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


class Response(BytesIO):
    def __init__(self,raw,status=200,headers=None):
        super().__init__(raw);self.status=self.code=status;self.headers=headers or {}
    def getcode(self):return self.status


def serializable(value):
    if isinstance(value,datetime):return value.isoformat()
    if isinstance(value,dict):return {k:serializable(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [serializable(v) for v in value]
    return value


class Acquisition:
    def __init__(self,publication,client,bucket,started_at,opener=None,sleep=time.sleep,cache=None):
        self.publication,self.client,self.bucket,self.started_at=publication,client,bucket,started_at
        self.opener=opener or urllib.request.build_opener(NoRedirect()).open
        self.sleep=sleep;self.cache=_CACHE if cache is None else cache
        self.operations=[];self.http_attempts=[];self.clocks=[];self.times=[]
        self.lock=Lock();self.failure=None;self.active_listings=0

    def fail(self,reason):
        self.failure=reason
        raise AcquisitionError(reason)

    def ready(self):
        if self.failure:raise AcquisitionError(self.failure)

    def record(self,row):
        with self.lock:self.operations.append(row)

    def retain(self,raw):
        try:return store.retain(self.client,self.bucket,raw)
        except Exception:self.fail('Complete original retention failed')

    def clock(self,original):
        owner=self
        class Clock(original):
            @classmethod
            def now(cls,tz=None):
                stamp=original.now(timezone.utc)
                owner.clocks.append(stamp.isoformat())
                return stamp.astimezone(tz) if tz else stamp.replace(tzinfo=None)
        return Clock

    def timer(self,original):
        def current():
            value=original.time();self.times.append(value);return value
        return SimpleNamespace(**{**{k:getattr(original,k) for k in dir(original) if not k.startswith('__')},'time':current})

    def get_object(self,**kwargs):
        self.ready();key=kwargs.get('Key')
        if (set(kwargs)!={'Bucket','Key'} or kwargs.get('Bucket')!=self.bucket or not isinstance(key,str)
                or not (key in FIXED or re.fullmatch(re.escape(WAREHOUSE)+r'\d{4}/[^/]+\.json\.gz',key))):
            self.fail('Unreviewed business-cycle object read')
        try:obj=self.client.get_object(**kwargs)
        except Exception as exc:
            code=str(getattr(exc,'response',{}).get('Error',{}).get('Code'))
            if code in ('404','NoSuchKey'):
                self.record({'kind':'s3','key':key,'status':'missing','error_code':code});raise
            self.fail('Native input access failed')
        try:
            raw=bounded(obj['Body'])
            if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or not obj.get('ETag'):
                self.fail('Complete versioned stored input required')
            ref=self.retain(raw)
            meta={k:serializable(obj[k]) for k in ('ETag','ContentLength','LastModified','ContentType','ContentEncoding','VersionId') if k in obj}
            self.record({'kind':'s3','key':key,'status':'retained','original':ref,'metadata':meta,
                         'source_kind':'derived_stored_object','provider_original_verified':False})
            return {**obj,'Body':BytesIO(raw)}
        except Exception:self.fail('Stored input capture incomplete')

    def get_paginator(self,name):
        self.ready()
        if name!='list_objects_v2':self.fail('Unreviewed object listing')
        native=self.client.get_paginator(name);owner=self
        class Paginator:
            def paginate(self,**kwargs):
                owner.ready();prefix=kwargs.get('Prefix')
                if set(kwargs)!={'Bucket','Prefix'} or kwargs.get('Bucket')!=owner.bucket or not isinstance(prefix,str) or not re.fullmatch(re.escape(WAREHOUSE)+r'\d{4}/',prefix):
                    owner.fail('Unreviewed warehouse selection prefix')
                pages=[];complete=False;owner.active_listings+=1
                try:
                    for page in native.paginate(**kwargs):
                        pages.append(owner.retain(store.encode(serializable(page))))
                        yield page
                    complete=True
                except Exception:owner.fail('Incomplete warehouse metadata pagination')
                finally:
                    owner.active_listings-=1
                    if not complete:owner.failure='Incomplete warehouse metadata pagination'
                owner.record({'kind':'listing','prefix':prefix,'pages':pages,'representation':'Complete SDK metadata; datetimes serialized as ISO8601, not original HTTP bytes.'})
        return Paginator()

    def put_object(self,**kwargs):
        self.ready();return self.publication.put_object(**kwargs)

    def network(self,request,timeout,identity):
        self.ready();acquired=datetime.now(timezone.utc).isoformat()
        try:response=self.opener(request,timeout=timeout)
        except urllib.error.HTTPError as error:response=error
        except (urllib.error.URLError,TimeoutError,OSError) as error:
            row={'request':identity,'acquired_at':acquired,'status':'transport_error','error_type':type(error).__name__}
            row['attempt_manifest']=self.retain(store.encode(row))
            self.http_attempts.append(row);return None,row
        try:
            headers=getattr(response,'headers',{}) or {};status=getattr(response,'status',None) or response.getcode()
            if type(status) is not int or not 100<=status<=599:self.fail('HTTP status missing')
            raw=bounded(response,headers)
            safe={k:headers[k] for k in ('Content-Type','Content-Length','Content-Encoding','Date','Last-Modified','ETag','Retry-After') if k in headers}
            row={'request':identity,'acquired_at':acquired,'status':'retained','http_status':status,'headers':safe,'original':self.retain(raw)}
            row['attempt_manifest']=self.retain(store.encode(row))
            self.http_attempts.append(row);return raw,row
        except Exception:self.fail('Complete provider response capture failed')

    def urlopen(self,request,timeout=None):
        self.ready()
        try:identity=request_identity(request,timeout)
        except Exception:self.fail('Unreviewed provider acquisition')
        cache_key=store.sha(store.encode(identity));fred=identity['provider']=='FRED'
        with _CACHE_LOCK:hit=deepcopy(self.cache.get(cache_key)) if fred else None
        now=time.time()
        if hit and 0<=now-hit['cached_at']<1800:
            self.retain(hit['raw'])
            self.record({'kind':'http_result','request':identity,'status':'retained','mode':'warm_cache',
                         'source_acquired_at':hit['attempt']['acquired_at'],'original':hit['attempt']['original'],
                         'original_http_attempt':hit['attempt']['attempt_manifest']})
            return Response(hit['raw'])
        last=None
        for attempt in range(4 if fred else 1):
            raw,last=self.network(request,timeout,identity)
            code=last.get('http_status')
            if raw is not None and 200<=code<300:
                if fred:
                    with _CACHE_LOCK:self.cache[cache_key]={'cached_at':now,'raw':raw,'attempt':last}
                self.record({'kind':'http_result','request':identity,'status':'retained','mode':'native_http',
                             'source_acquired_at':last['acquired_at'],'original':last['original'],'original_http_attempt':last['attempt_manifest']})
                return Response(raw,code,last['headers'])
            if not fred or code is not None and code not in (429,500,502,503,504):break
            self.sleep(min(8,0.6*(2**attempt)))
        if hit and (code is None or code in (429,500,502,503,504)):
            self.retain(hit['raw'])
            self.record({'kind':'http_result','request':identity,'status':'retained','mode':'stale_cache_fallback',
                         'source_acquired_at':hit['attempt']['acquired_at'],'original':hit['attempt']['original'],
                         'original_http_attempt':hit['attempt']['attempt_manifest']})
            return Response(hit['raw'])
        self.record({'kind':'http_result','request':identity,'status':'unavailable','last_attempt':last})
        raise RuntimeError('Retained provider unavailable')

    def finish(self,compilers):
        self.ready()
        if self.active_listings:self.fail('Warehouse listing is not exhausted')
        manifest={'contract':CONTRACT,'started_at':self.started_at,'compiler_sha256':compilers,
                  'operations':self.operations,'http_attempts':self.http_attempts,'processing_clocks':self.clocks,'processing_times':self.times,
                  'complete_native_calculations':{k:self.retain(v['raw']) for k,v in self.publication.pending.items()},
                  'inputs_atomic':False,'original_source_replay_verified':False,'point_in_time_verified':False,'forecast_qualified':False}
        ref=self.retain(store.encode(manifest))
        return {'contract':CONTRACT,'manifest':ref,'operations':len(self.operations),'provider_attempts':len(self.http_attempts),
                'warehouse_objects':sum(r['kind']=='s3' and r['key'].startswith(WAREHOUSE) for r in self.operations),
                'original_source_replay_verified':False,'point_in_time_verified':False}


def replay_native(module,manifest,fetch):
    """No AWS/provider operations: consume the exact retained inputs and clocks."""
    if manifest.get('contract')!=CONTRACT or manifest.get('compiler_sha256')!=store.compiler_hashes():
        raise AcquisitionError('Exact complete compiler required')
    failures=[]
    def retained(ref):
        if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
                or ref.get('key')!=store.PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int or not 0<=ref['bytes']<=store.LIMIT):
            failures.append('Invalid retained identity')
            raise AcquisitionError('Invalid retained identity')
        try:raw=fetch(ref['key'])
        except Exception:
            failures.append('Retained body unavailable')
            raise AcquisitionError('Retained body unavailable')
        if len(raw)!=ref['bytes'] or store.sha(raw)!=ref['sha256']:
            failures.append('Retained bytes differ')
            raise AcquisitionError('Retained bytes differ')
        return raw
    groups=defaultdict(deque)
    for row in manifest['operations']:
        key=(row['kind'],row.get('key') or row.get('prefix') or store.sha(store.encode(row['request'])))
        if row['kind']=='http_result' and row.get('status')=='retained':
            origin=store.strict(retained(row['original_http_attempt']))
            if origin.get('request')!=row['request'] or origin.get('original')!=row['original'] or origin.get('acquired_at')!=row['source_acquired_at']:
                raise AcquisitionError('Original HTTP acquisition identity differs')
        groups[key].append(deepcopy(row))
    for row in manifest['http_attempts']:
        descriptor=store.strict(retained(row['attempt_manifest']))
        if descriptor!={k:v for k,v in row.items() if k!='attempt_manifest'}:raise AcquisitionError('HTTP attempt descriptor differs')
        if 'original' in row:retained(row['original'])
    clocks=deque(manifest['processing_clocks']);times=deque(manifest['processing_times']);pending={};lock=Lock()
    def pop(kind,key):
        with lock:
            rows=groups[(kind,key)]
            if not rows:
                failures.append('Unrecorded native read')
                raise AcquisitionError('Unrecorded native read')
            return rows.popleft()
    class Missing(Exception):
        response={'Error':{'Code':'NoSuchKey'}}
    class Replay:
        def get_object(self,**kw):
            if kw.get('Bucket')!=module.BUCKET:raise AcquisitionError('Unreviewed replay bucket')
            row=pop('s3',kw['Key'])
            if row['status']=='missing':raise Missing()
            meta=deepcopy(row['metadata'])
            if 'LastModified' in meta:meta['LastModified']=datetime.fromisoformat(meta['LastModified'])
            return {**meta,'Body':BytesIO(retained(row['original']))}
        def get_paginator(self,name):
            if name!='list_objects_v2':raise AcquisitionError('Unrecorded paginator')
            return self
        def paginate(self,**kw):
            if kw.get('Bucket')!=module.BUCKET:raise AcquisitionError('Unreviewed replay listing bucket')
            row=pop('listing',kw['Prefix'])
            for ref in row['pages']:yield store.strict(retained(ref))
        def put_object(self,**kw):
            if kw.get('Bucket')!=module.BUCKET or kw.get('Key') not in store.KEYS or kw['Key'] in pending:raise AcquisitionError('Unreviewed replay output')
            pending[kw['Key']]=kw['Body'];return {'publication_state':'offline_not_published'}
        def urlopen(self,request,timeout=None):
            row=pop('http_result',store.sha(store.encode(request_identity(request,timeout))))
            if row['status']=='unavailable':raise RuntimeError('Retained provider unavailable')
            return Response(retained(row['original']))
    originals=(module.S3,module.datetime,module.time,module.urllib.request.urlopen)
    class Clock(originals[1]):
        @classmethod
        def now(cls,tz=None):
            if not clocks:raise AcquisitionError('Unrecorded processing clock')
            stamp=datetime.fromisoformat(clocks.popleft())
            return stamp.astimezone(tz) if tz else stamp.replace(tzinfo=None)
    def timer():
        if not times:raise AcquisitionError('Unrecorded processing time')
        return times.popleft()
    module.S3=Replay();module.datetime=Clock;module.time=SimpleNamespace(time=timer,sleep=lambda seconds:None)
    module.urllib.request.urlopen=module.S3.urlopen
    try:
        result=module._produce(None,None)
        if result.get('statusCode')!=200 or failures or clocks or times or any(groups.values()) or set(pending)!=set(manifest['complete_native_calculations']):
            raise AcquisitionError('Incomplete native replay')
        for key,raw in pending.items():
            if store.strict(raw)!=store.strict(retained(manifest['complete_native_calculations'][key])):
                raise AcquisitionError('Complete native calculation differs')
        return {'status':'complete_native_calculations_replayed','outputs':len(pending),'operations':len(manifest['operations']),
                'comparison':'Every parsed field, including recorded processing clocks; object member order is not a data difference.',
                'native_provider_requests':0,'public_writes':0,'point_in_time_verified':False,'forecast_qualified':False}
    finally:module.S3,module.datetime,module.time,module.urllib.request.urlopen=originals
