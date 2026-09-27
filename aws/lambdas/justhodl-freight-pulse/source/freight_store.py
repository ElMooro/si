"""Complete freight-source capture and reproducible conditional publication.

Legacy native formulas remain separately identified and unqualified. The added
calendar measurements are descriptive, not forecasts or portfolio instructions.
"""
from datetime import datetime, timezone
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from copy import deepcopy
import hashlib, json, math, re, time, urllib.request, urllib.error, urllib.parse
import freight_measurements as measurements
HEAD='data/freight-pulse.json'
GRAPH='data/impact/exposure-graph.json'
BETAS='data/impact/betas.json'
KEYS=(HEAD,GRAPH,BETAS)
ARCHIVE='data/archive/freight-pulse/'
PRIVATE='audit-private/20260909-originals/freight-native-research/'
LIMIT=64*1024*1024
CONTRACT='freight-preserved-calculation.v1'
COMPILERS=('lambda_function.py','freight_store.py','freight_measurements.py','impact_mapper.py','managed_secret.py')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
class CaptureError(ValueError):pass


def strict(raw):
    def pairs(items):
        d={}
        for k,v in items:
            if k in d:raise CaptureError('Duplicate JSON key')
            d[k]=v
        return d
    def invalid(x):raise CaptureError('Nonfinite JSON number')
    def number(x):
        n=float(x)
        if not math.isfinite(n):invalid(x)
        return n
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=invalid)


def whole(stream,length=None):
    chunks=[];size=0
    try:
        while True:
            raw=stream.read(min(65536,LIMIT+1-size))
            if not raw:break
            if not isinstance(raw,bytes):raise CaptureError('Bytes required')
            size+=len(raw);chunks.append(raw)
            if size>LIMIT:raise CaptureError('Whole input exceeds byte bound')
    finally:stream.close()
    raw=b''.join(chunks)
    if length is not None and (not str(length).isdigit() or int(length)!=len(raw)):raise CaptureError('Whole input length differs')
    return raw


def hashes():
    import impact_mapper,managed_secret
    modules={'impact_mapper.py':impact_mapper,'managed_secret.py':managed_secret}
    return {name:sha((Path(modules[name].__file__) if name in modules else Path(__file__).parent/name).read_bytes()) for name in COMPILERS}


def retained(s3,bucket,ref):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
        or ref.get('key')!=PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int or not 0<=ref['bytes']<=LIMIT):
        raise CaptureError('Exact protected identity required')
    obj=s3.get_object(Bucket=bucket,Key=ref['key']);raw=whole(obj['Body'],obj.get('ContentLength'))
    if type(obj.get('ContentLength')) is not int or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise CaptureError('Complete retained body differs')
    return raw


def retain(s3,bucket,raw):
    if not isinstance(raw,bytes) or len(raw)>LIMIT:raise CaptureError('Bounded original required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as e:
        if str(getattr(e,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if retained(s3,bucket,ref)!=raw:raise CaptureError('Retained readback differs')
    return ref


def decode(raw):
    value = strict(raw)
    if not isinstance(value, dict):
        raise CaptureError('Complete JSON object required')
    return value



def stamp(value):
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None or 'T' not in value:raise CaptureError('Aware publication clock required')
    return parsed.astimezone(timezone.utc)


def identity(request,timeout):
    req=urllib.request.Request(request) if isinstance(request,str) else request
    u=urllib.parse.urlsplit(req.full_url);pairs=urllib.parse.parse_qsl(u.query,keep_blank_values=True);params=dict(pairs)
    if (u.scheme!='https' or u.fragment or req.get_method()!='GET' or req.data is not None
        or len(pairs)!=len(params) or type(timeout) not in (int,float) or not 0<timeout<=25):
        raise CaptureError('Unreviewed provider request')
    if u.netloc=='api.stlouisfed.org':
        endpoint=u.path.removeprefix('/fred/')
        if endpoint not in ('series','series/observations') or params.get('series_id') not in measurements.PROFILES or params.get('file_type')!='json':raise CaptureError('Unreviewed FRED identity')
        expected={'series_id','api_key','file_type','observation_start'} if endpoint=='series/observations' else {'series_id','api_key','file_type','realtime_start','realtime_end'}
        if set(params)!=expected or (endpoint=='series/observations' and params['observation_start']!='2015-01-01'):raise CaptureError('Unreviewed FRED window')
    elif u.netloc=='api.eia.gov':
        if u.path!='/v2/seriesid/PET.WDIUPUS2.W' or set(params)!={'api_key'}:raise CaptureError('Unreviewed EIA identity')
    else:raise CaptureError('Unreviewed provider host')
    if not params.get('api_key'):raise CaptureError('Provider credential unavailable')
    return {'method':'GET','endpoint':u.scheme+'://'+u.netloc+u.path,
            'parameters':{k:v for k,v in params.items() if k!='api_key'},'timeout':timeout}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


class Response(BytesIO):
    def __init__(self,raw,code=200,headers=None):
        super().__init__(raw);self.headers=headers or {};self.status=self.code=code
    def getcode(self):return self.code


def archive_population(client,bucket):
    keys=set();token=None;tokens=set()
    for _ in range(100):
        args={'Bucket':bucket,'Prefix':ARCHIVE,'MaxKeys':1000}
        if token:args['ContinuationToken']=token
        page=client.list_objects_v2(**args)
        if type(page.get('IsTruncated')) is not bool or not isinstance(page.get('Contents',[]),list):raise CaptureError('Complete archive membership required')
        for row in page.get('Contents',[]):
            key=row.get('Key')
            if not isinstance(key,str) or not re.fullmatch(re.escape(ARCHIVE)+r'\d{4}-\d{2}-\d{2}\.json',key) or key in keys:raise CaptureError('Unique archive identities required')
            measurements.day(key[len(ARCHIVE):-5]);keys.add(key)
        if not page['IsTruncated']:return sorted(keys)
        token=page.get('NextContinuationToken')
        if not isinstance(token,str) or not token or token in tokens:raise CaptureError('Archive pagination stalled')
        tokens.add(token)
    raise CaptureError('Archive population exceeds bound; not truncated')


class Session:
    def __init__(self,client,bucket,at,eia_key,opener=None,fred_key=None):
        self.client,self.bucket,self.at,self.eia_key=client,bucket,at,eia_key
        self.fred_key=fred_key
        self.started=time.monotonic();self.inputs={};self.pending={};self.reads=[];self.http=[];self.responses={};self.failure=None
        self.archive=ARCHIVE+stamp(at).date().isoformat()+'.json'
        self.opener=opener or urllib.request.build_opener(NoRedirect()).open
        for key in (*KEYS,self.archive):
            try:obj=client.get_object(Bucket=bucket,Key=key)
            except Exception as exc:
                if key==self.archive and str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey'):
                    self.inputs[key]={'status':'missing'};continue
                raise CaptureError('Required predecessor unavailable') from None
            raw=whole(obj['Body']);ref=retain(client,bucket,raw)
            if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or not obj.get('ETag'):raise CaptureError('Whole versioned predecessor required')
            decode(raw);self.inputs[key]={'status':'present','raw':raw,'original':ref,'etag':obj['ETag'],'last_modified':obj['LastModified'].isoformat()}
        if stamp(decode(self.inputs[HEAD]['raw']).get('generated_at'))>=stamp(at):raise CaptureError('Previous publication is malformed or newer')
        if self.inputs[self.archive]['status']!='missing':raise CaptureError('Existing research date preserved; no replacement')
        self.archive_keys=archive_population(client,bucket)
        if self.archive in self.archive_keys or any(measurements.day(k[len(ARCHIVE):-5])>stamp(at).date() for k in self.archive_keys):raise CaptureError('Inconsistent or future archive population')
        self.archive_manifest=retain(client,bucket,encode({'keys':self.archive_keys,'captured_at':at,'snapshot_atomic':False}))

    def ready(self):
        if self.failure:raise CaptureError(self.failure)

    def get_object(self,**kw):
        self.ready();key=kw.get('Key')
        if kw!={'Bucket':self.bucket,'Key':key} or key not in KEYS:self.failure='Undeclared native input';self.ready()
        self.reads.append(key);return {'Body':BytesIO(self.inputs[key]['raw'])}

    def put_object(self,**kw):
        self.ready();key=kw.get('Key');raw=kw.get('Body')
        if kw.get('Bucket')!=self.bucket or key not in (HEAD,self.archive) or key in self.pending or not isinstance(raw,bytes) or len(raw)>LIMIT:
            self.failure='Unexpected native output';self.ready()
        decode(raw);self.pending[key]=raw;return {}

    def list_objects_v2(self,**kw):
        self.ready()
        if kw!={'Bucket':self.bucket,'Prefix':ARCHIVE,'MaxKeys':500} or self.archive not in self.pending:
            self.failure='Unexpected native archive listing';self.ready()
        self.reads.append('archive_membership')
        keys=sorted([*self.archive_keys,self.archive]);return {'KeyCount':min(500,len(keys)),'IsTruncated':len(keys)>500,'Contents':[{'Key':k} for k in keys[:500]]}

    def record_response(self,request,parsed):
        if request['endpoint'].startswith('https://api.stlouisfed.org/'):
            ident=(request['parameters']['series_id'],request['endpoint'].removeprefix('https://api.stlouisfed.org/fred/'))
            if ident in self.responses:raise CaptureError('Duplicate provider identity')
            self.responses[ident]=parsed

    def urlopen(self,req,timeout=None):
        self.ready()
        try:
            request=identity(req,timeout);remaining=120-(time.monotonic()-self.started)
            if remaining<=0 or len(self.http)>=13:raise CaptureError('Original runtime acquisition budget exhausted')
            at=datetime.now(timezone.utc).isoformat()
            try:response=self.opener(req,timeout=min(timeout,remaining))
            except urllib.error.HTTPError as exc:response=exc
            except (urllib.error.URLError,OSError,TimeoutError) as exc:
                row={'request':request,'acquired_at':at,'status':'transport_error','error_type':type(exc).__name__}
                row['attempt_manifest']=retain(self.client,self.bucket,encode(row));self.http.append(row)
                raise CaptureError('Provider transport failed') from None
            headers=getattr(response,'headers',{}) or {};code=response.getcode();raw=whole(response)
            row={'request':request,'acquired_at':at,'status':'http_response','http_status':code,
                 'headers':{k:headers[k] for k in ('Content-Type','Content-Length','Date','Last-Modified','ETag','Retry-After') if k in headers},
                 'original':retain(self.client,self.bucket,raw)}
            row['attempt_manifest']=retain(self.client,self.bucket,encode(row));self.http.append(row)
            length=headers.get('Content-Length')
            if length is not None and (not str(length).isdigit() or int(length)!=len(raw)):raise CaptureError('Provider response length differs')
            parsed=decode(raw)
            if code!=200 or parsed.get('error') or parsed.get('error_code'):raise CaptureError('Provider error retained')
            self.record_response(request,parsed);return Response(raw,code,headers)
        except Exception:
            self.failure='Whole provider acquisition failed';raise


def calculate(module,session):
    original={k:getattr(module,k) for k in ('S3','datetime','urllib','_EIA','FRED_KEY')}
    impact=module.impact_mapper;previous={k:getattr(impact,k) for k in ('_S3','_CACHE','datetime')}
    clock=stamp(session.at)
    class Frozen(original['datetime']):
        @classmethod
        def now(cls,tz=None):return clock.astimezone(tz) if tz else clock.replace(tzinfo=None)
    try:
        module.S3=impact._S3=session;module.datetime=impact.datetime=Frozen;impact._CACHE={};module._EIA={'k':session.eia_key};module.FRED_KEY=session.fred_key
        module.urllib=SimpleNamespace(request=SimpleNamespace(Request=urllib.request.Request,urlopen=session.urlopen))
        for sid in measurements.PROFILES:
            params={'series_id':sid,'api_key':module.FRED_KEY,'file_type':'json','realtime_start':clock.date().isoformat(),'realtime_end':clock.date().isoformat()}
            session.urlopen('https://api.stlouisfed.org/fred/series?'+urllib.parse.urlencode(params),timeout=10).close()
        result=module._legacy_calculation()
        session.ready()
        if set(session.pending)!={HEAD,session.archive}:raise CaptureError('Complete native publication pair required')
        expected={(sid,endpoint) for sid in measurements.PROFILES for endpoint in ('series','series/observations')}
        if set(session.responses)!=expected:raise CaptureError('Every monthly source response required')
        session.measurements=measurements.build(session.responses,session.at)
        return result
    finally:
        for k,v in original.items():setattr(module,k,v)
        for k,v in previous.items():setattr(impact,k,v)


def publication_context(manifest,ref):
    return {'contract':CONTRACT,'manifest':ref,'compiler_sha256':hashes(),'original_source_replay_verified':False,
            'point_in_time_verified':False,'publication_atomic':False,'provider_responses':len(manifest['http_attempts'])}


def projection(calculation,context,review,archive_count):
    out=deepcopy(calculation)
    out.update(contract=CONTRACT,publication_context=context,measurement_review=review,portfolio_action='WAIT',**measurements.FLAGS)
    out['archive_review']={'complete_prior_membership_count':archive_count,'planned_new_date':out['generated_at'][:10],
         'membership_snapshot_atomic':False,'original_daily_records_overwritten':False,'forecast_qualified':False}
    out['research_limits']='The separate measurement_review uses retained FRED definitions and exact calendar observations without early rounding. Inherited top-level composites, inflections, annualizations, rate labels, weekly sector claims and impact models remain unqualified; do not use them as forecasts, independent confirmations or position instructions.'
    return out


def run(module,event=None,context=None,opener=None,at=None):
    client,bucket=module.S3,module.BUCKET;at=at or datetime.now(timezone.utc).isoformat()
    session=Session(client,bucket,at,module._eia_key(),opener,fred_key=module.FRED_KEY)
    result=calculate(module,session)
    inputs={k:{a:b for a,b in value.items() if a!='raw'} for k,value in session.inputs.items()}
    manifest={'contract':CONTRACT,'calculation_at':at,'compiler_sha256':hashes(),'inputs':inputs,'read_order':session.reads,
       'archive_population':session.archive_manifest,'eia_credential_available':bool(session.eia_key),'http_attempts':session.http,
       'native_return':result,'complete_native_outputs':{k:retain(client,bucket,raw) for k,raw in session.pending.items()},
       'inputs_atomic':False,'point_in_time_verified':False}
    ref=retain(client,bucket,encode(manifest));pub_context=publication_context(manifest,ref)
    out=projection(decode(session.pending[HEAD]),pub_context,session.measurements,len(session.archive_keys))
    planned={session.archive:session.pending[session.archive],HEAD:encode(out)}
    retain(client,bucket,encode({'manifest':ref,'publication_order':list(planned),'planned_outputs':{k:retain(client,bucket,raw) for k,raw in planned.items()}}))
    completed=[]
    for key,raw in planned.items():
        try:
            if key==HEAD:client.put_object(Bucket=bucket,Key='data/freight-pulse.json',Body=raw,IfMatch=inputs[HEAD]['etag'],ContentType='application/json',CacheControl='public, max-age=3600')
            else:client.put_object(Bucket=bucket,Key='data/archive/freight-pulse/'+stamp(at).date().isoformat()+'.json',Body=raw,IfNoneMatch='*',ContentType='application/json')
        except Exception as exc:
            if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('409','412','PreconditionFailed','ConditionalRequestConflict'):
                return {'published':False,'state':'concurrent_writer_preserved','completed_paths':completed}
            raise
        completed.append(key)
    return {'published':True,'generated_at':at,'provider_responses':len(session.http),'portfolio_action':'WAIT'}


def replay(module,client,bucket,packet):
    context=packet.get('publication_context') or {};ref=context.get('manifest')
    manifest=decode(retained(client,bucket,ref))
    archive=ARCHIVE+stamp(manifest['calculation_at']).date().isoformat()+'.json'
    if (manifest.get('contract')!=CONTRACT or manifest.get('compiler_sha256')!=hashes() or set(manifest['inputs'])!={*KEYS,archive}
        or context!=publication_context(manifest,ref)):
        raise CaptureError('Exact compiler, complete input identities and context required')
    population=decode(retained(client,bucket,manifest['archive_population']))
    if population.get('snapshot_atomic') is not False or population.get('captured_at')!=manifest['calculation_at'] or not isinstance(population.get('keys'),list):raise CaptureError('Exact archive membership required')
    class Replay:
        def __init__(self):
            self.at=manifest['calculation_at'];self.archive=archive;self.archive_keys=population['keys'];self.bucket=bucket
            self.eia_key='offline-present' if manifest['eia_credential_available'] else '';self.fred_key='offline-present';self.pending={};self.failure=None;self.responses={}
            self.reads=[];self.expected_reads=list(manifest['read_order']);self.http=list(manifest['http_attempts'])
            self.inputs={k:{**v,**({'raw':retained(client,bucket,v['original'])} if v['status']=='present' else {})} for k,v in manifest['inputs'].items()}
        ready=Session.ready;put_object=Session.put_object;record_response=Session.record_response;list_objects_v2=Session.list_objects_v2
        def get_object(self,**kw):return Session.get_object(self,**kw)
        def urlopen(self,req,timeout=None):
            if not self.http:raise CaptureError('Unexpected provider replay')
            row=self.http.pop(0);request=identity(req,timeout)
            if row.get('request')!=request or row.get('status')!='http_response' or row.get('http_status')!=200:raise CaptureError('Original request differs')
            if decode(retained(client,bucket,row['attempt_manifest']))!={k:v for k,v in row.items() if k!='attempt_manifest'}:raise CaptureError('Attempt manifest differs')
            raw=retained(client,bucket,row['original']);self.record_response(request,decode(raw));return Response(raw,200,row['headers'])
    session=Replay();result=calculate(module,session)
    if session.http or session.reads!=session.expected_reads or encode(result)!=encode(manifest['native_return']) or set(session.pending)!=set(manifest['complete_native_outputs']):raise CaptureError('Complete native replay differs')
    for key,raw in session.pending.items():
        if raw!=retained(client,bucket,manifest['complete_native_outputs'][key]):raise CaptureError('Native calculation bytes differ')
    expected=projection(decode(session.pending[HEAD]),context,session.measurements,len(session.archive_keys))
    if encode(packet)!=encode(expected):raise CaptureError('Published measurement projection differs')
    return {'status':'complete_original_calculation_and_calendar_replayed','provider_responses':len(manifest['http_attempts']),
            'monthly_series':len(session.measurements['series']),'monthly_observations':sum(r['returned_rows'] for r in session.measurements['series'].values()),
            'prior_archive_membership':len(session.archive_keys),'complete_stored_inputs':len(session.inputs),
            'provider_requests':0,'public_writes':0,'point_in_time_verified':False,'forecast_qualified':False}
