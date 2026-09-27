"""Whole Asia source retention, conditional research publication and offline replay."""
from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from copy import deepcopy
from types import SimpleNamespace
import contextlib,hashlib,json,math,re,time,urllib.request,urllib.error,urllib.parse
import asia_measurements as measurements
HEAD='data/asia-leads.json'
KEYS=(HEAD,'asia/kr-flash-tape.json','kcs/flash-cache.json','asia/tw-orders-levels.json')
PRIVATE='audit-private/20260909-originals/asia-leads-research/'
CONTRACT='asia-leads-research.v1'
LIMIT=64*1024*1024
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
COMPILERS=('lambda_function.py','asia_store.py','asia_measurements.py','managed_secret.py')
class EvidenceError(ValueError):
    pass


def strict(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise EvidenceError('Duplicate JSON key')
            out[key] = value
        return out
    def invalid(value):
        raise EvidenceError('Nonfinite JSON number')
    def number(value):
        result = float(value)
        if not math.isfinite(result):
            invalid(value)
        return result
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_float=number, parse_constant=invalid)


def whole(stream, length, limit=LIMIT, deadline=None):
    parts = []; size = 0
    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                raise EvidenceError('Complete response deadline exhausted')
            part = stream.read(min(65536, limit + 1 - size))
            if not part:
                break
            if not isinstance(part, bytes):
                raise EvidenceError('Byte stream required')
            parts.append(part); size += len(part)
            if size > limit:
                raise EvidenceError('Complete body exceeds bound; never truncated')
    finally:
        stream.close()
    if length is not None and (not str(length).isdigit() or int(length) != size):
        raise EvidenceError('Whole response length differs')
    return b''.join(parts)


def get(client, bucket, key):
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('404', 'NoSuchKey', 'NotFound'):
            return None
        raise EvidenceError('Object read failed') from None
    raw = whole(obj['Body'], obj.get('ContentLength'))
    if type(obj.get('ContentLength')) is not int or not isinstance(obj.get('ETag'), str) or not obj['ETag']:
        raise EvidenceError('Complete conditional object identity required')
    return {'raw': raw, 'etag': obj['ETag']}


def retained(client, bucket, ref):
    if (not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', str(ref.get('sha256')))
            or ref.get('key') != PRIVATE + ref['sha256'] + '.bin' or type(ref.get('bytes')) is not int or not 0 <= ref['bytes'] <= LIMIT):
        raise EvidenceError('Exact protected original identity required')
    found = get(client, bucket, ref['key'])
    if found is None or len(found['raw']) != ref['bytes'] or sha(found['raw']) != ref['sha256']:
        raise EvidenceError('Retained whole body differs')
    return found['raw']


def retain(client, bucket, raw):
    if not isinstance(raw, bytes) or len(raw) > LIMIT:
        raise EvidenceError('Bounded whole original required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'sha256': sha(raw), 'bytes': len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, IfNoneMatch='*', ContentType='application/octet-stream', CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'PreconditionFailed', 'ConditionalRequestConflict'):
            raise
    if retained(client, bucket, ref) != raw:
        raise EvidenceError('Retained readback differs')
    return ref


class Budget:
    def __init__(self, client, deadline):
        self.client, self.deadline = client, deadline
    def check(self):
        if time.monotonic() + 15 >= self.deadline:
            raise EvidenceError('Acquisition/publication runtime budget exhausted')
    def get_object(self, **kw):
        self.check(); return self.client.get_object(**kw)
    def put_object(self, **kw):
        self.check(); return self.client.put_object(**kw)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def compiler_hashes():
    return {name:sha((Path(__file__).parent/name if (Path(__file__).parent/name).exists()
                     else Path(__file__).resolve().parents[3]/'shared'/name).read_bytes()) for name in COMPILERS}


class Response(BytesIO):
    def __init__(self, raw, headers=None):
        super().__init__(raw); self.headers = headers or {}; self.status = 200
    def getcode(self):
        return 200


def government(url):
    p=urllib.parse.urlsplit(url)
    if p.scheme!='https' or p.netloc not in ('www.customs.go.kr','eng.stat.gov.tw','www.moea.gov.tw') or p.username or p.password or p.fragment:
        raise EvidenceError('Declared HTTPS government source required')
    return p.netloc


def identity(request):
    req=urllib.request.Request(request) if isinstance(request,str) else request
    p=urllib.parse.urlsplit(req.full_url);pairs=urllib.parse.parse_qsl(p.query,keep_blank_values=True);q=dict(pairs)
    if p.scheme!='https' or p.username or p.password or p.fragment or req.get_method()!='GET' or req.data is not None or len(q)!=len(pairs):
        raise EvidenceError('Exact HTTPS public GET required')
    host=p.netloc;source_host=host
    if host=='api.stlouisfed.org':
        if q.get('series_id') not in measurements.PROFILES or q.get('file_type')!='json':raise EvidenceError('Undeclared export series')
        basic={'series_id','api_key','file_type'}
        if p.path=='/fred/series/observations':
            legacy=set(q)==basic|{'observation_start'} and q['observation_start']=='2015-01-01'
            full=(set(q)==basic|{'realtime_start','realtime_end','observation_start','observation_end','sort_order','limit','offset','units','output_type'}
                  and q['observation_start']=='1776-07-04' and q['sort_order']=='asc' and q['limit']=='100000' and q['offset']=='0' and q['units']=='lin' and q['output_type']=='1')
            if not legacy and not full:raise EvidenceError('Undeclared export transformation')
        elif p.path!='/fred/series' or set(q)!=basic|{'realtime_start','realtime_end'}:raise EvidenceError('Undeclared export metadata')
    elif host=='news.google.com':
        if p.path!='/rss/search' or set(q)!={'q','hl','gl','ceid'}:raise EvidenceError('Declared news search required')
    elif host=='newsapi.org':
        if p.path!='/v2/everything' or set(q)!={'q','language','sortBy','pageSize','apiKey'} or q['pageSize']!='50':raise EvidenceError('Declared news request required')
    elif host=='financialmodelingprep.com':
        if p.path!='/stable/news/general-latest' or set(q)!={'page','limit','apikey'} or q['page']!='0' or q['limit']!='120':raise EvidenceError('Declared news page required')
    elif host=='justhodl-data-proxy.raafouis.workers.dev':
        if p.path!='/gov' or set(q)!={'u'}:raise EvidenceError('Declared government proxy required')
        source_host=government(q['u'])
    else:source_host=government(req.full_url)
    return {'method':'GET','endpoint':p.scheme+'://'+p.netloc+p.path,'source_host':source_host,
            'parameters':{k:v for k,v in q.items() if k not in ('api_key','apikey','apiKey')}}


class Missing(Exception):
    response={'Error':{'Code':'NoSuchKey'}}


class Session:
    def __init__(self,client,bucket,at,predecessors,opener=None):
        self.client,self.bucket,self.at,self.predecessors=client,bucket,at,predecessors
        self.opener=opener or urllib.request.build_opener(NoRedirect()).open
        self.http=[];self.pending={};self.reads=[];self.failure=None;self.rate_limited=set()
        self.started=time.monotonic();self.total_bytes=0
    def ready(self):
        if self.failure:raise EvidenceError(self.failure)
    def record(self,row):
        try:row['attempt_manifest']=retain(self.client,self.bucket,encode(row));self.http.append(row)
        except Exception:self.failure='Complete attempt retention failed';self.ready()
    def get_object(self,**kw):
        self.ready();key=kw.get('Key')
        if kw.get('Bucket')!=self.bucket or key not in KEYS[2:]:
            self.failure='Undeclared native cache read';self.ready()
        self.reads.append(key);old=self.predecessors[key]
        if old is None:raise Missing()
        return {'Body':BytesIO(old['raw']),'ContentLength':len(old['raw']),'ETag':old['etag']}
    def put_object(self,**kw):
        self.ready();key=kw.get('Key');raw=kw.get('Body')
        if kw.get('Bucket')!=self.bucket or key not in KEYS or key in self.pending or not isinstance(raw,bytes) or len(raw)>LIMIT:
            self.failure='Unexpected native cache/public writer';self.ready()
        try:p=strict(raw)
        except Exception:self.failure='Invalid native structured publication';self.ready()
        if not isinstance(p,dict) or (key==HEAD and p.get('generated_at')!=self.at):
            self.failure='Complete dated native packet required';self.ready()
        self.pending[key]=raw;return {}
    def urlopen(self,req,timeout=None):
        self.ready()
        try:
            request=identity(req)
            if type(timeout) not in (int,float) or not 0<timeout<=45 or len(self.http)>=64:raise EvidenceError('Request bound differs')
        except Exception:self.failure='Undeclared provider request';self.ready()
        row={'request':request,'requested_at':datetime.now(timezone.utc).isoformat()};host=request['source_host']
        remaining=210-(time.monotonic()-self.started)
        if host in self.rate_limited or remaining<=1:
            row['status']='rate_limit_not_retried' if host in self.rate_limited else 'budget_not_attempted'
            self.record(row);raise EvidenceError(row['status'])
        phase='connect'
        try:
            try:response=self.opener(req,timeout=min(timeout,12,remaining))
            except urllib.error.HTTPError as exc:response=exc
            phase='body';code=response.getcode();headers=response.headers or {};url=req if isinstance(req,str) else req.full_url
            if hasattr(response,'geturl') and response.geturl()!=url:response.close();raise EvidenceError('Redirect refused')
            raw=whole(response,headers.get('Content-Length'),16*1024*1024,self.started+210)
            self.total_bytes+=len(raw)
            if self.total_bytes>LIMIT:raise EvidenceError('Complete response population exceeds bound')
            row.update(status='http_response',http_status=code,
                       headers={k:headers.get(k) for k in ('Content-Type','Content-Length','Date','Last-Modified','ETag','Retry-After')},
                       original=retain(self.client,self.bucket,raw))
        except (OSError,TimeoutError,urllib.error.URLError) as exc:
            if phase!='connect':self.failure='Incomplete source body or failed retention';self.ready()
            row.update(status='transport_error',error_type=type(exc).__name__)
        except Exception:self.failure='Whole source acquisition or retention refused';self.ready()
        self.record(row)
        if row['status']!='http_response' or row['http_status']!=200:
            if row.get('http_status')==429:self.rate_limited.add(host)
            raise EvidenceError('Source unavailable; complete original attempt retained')
        return Response(raw,row['headers'])


class Quiet:
    def write(self,text):return len(text)
    def flush(self):pass


def calculate(module,session,credentials):
    original={k:getattr(module,k) for k in ('s3','datetime','time','urllib','FRED','NEWSAPI_KEY','FMP_KEY')}
    at=measurements.clock(session.at)
    class Frozen(original['datetime']):
        @classmethod
        def now(cls,tz=None):return at.astimezone(tz) if tz else at.replace(tzinfo=None)
    try:
        module.s3=session;module.datetime=Frozen;module.time=SimpleNamespace(time=lambda:at.timestamp())
        module.urllib=SimpleNamespace(parse=urllib.parse,request=SimpleNamespace(Request=urllib.request.Request,urlopen=session.urlopen))
        module.FRED,module.NEWSAPI_KEY,module.FMP_KEY=credentials
        # Legacy diagnostics contain URL prefixes; do not emit credential-bearing text.
        with contextlib.redirect_stdout(Quiet()):result=module._legacy_lambda_handler({},None)
        session.ready()
        if HEAD not in session.pending:raise EvidenceError('Complete native head missing')
        reviews={}
        for sid in measurements.PROFILES:
            params={'series_id':sid,'api_key':credentials[0],'file_type':'json','realtime_start':at.date().isoformat(),'realtime_end':at.date().isoformat()}
            packets=[]
            for endpoint in ('series','series/observations'):
                if endpoint.endswith('observations'):params.update(observation_start='1776-07-04',observation_end=at.date().isoformat(),sort_order='asc',limit='100000',offset='0',units='lin',output_type='1')
                try:
                    with session.urlopen('https://api.stlouisfed.org/fred/'+endpoint+'?'+urllib.parse.urlencode(params),timeout=12) as response:packets.append(strict(response.read()))
                except EvidenceError:session.ready();packets.append(None)
            reviews[sid]=measurements.fred(sid,*packets,session.at)
        session.review={'contract':measurements.CONTRACT,'series':reviews,'calculation_at':session.at,'sources_atomic':False,
                        'original_vintage_verified':False,**measurements.FLAGS}
        return result
    finally:
        for key,value in original.items():setattr(module,key,value)


def overlay(old,new):
    if not isinstance(old,dict) or not isinstance(new,dict):return deepcopy(new)
    result=deepcopy(old)
    for key,value in new.items():result[key]=overlay(result.get(key),value)
    return result


def projection(previous,native,review,at,compilers):
    out=overlay(previous,native)
    out.update(version='1.4.0',contract=CONTRACT,generated_at=at,measurement_review=review,compiler_sha256=compilers,
               legacy_calculation=deepcopy(native),portfolio_action='WAIT',**measurements.FLAGS)
    for sid,row in review['series'].items():
        field=out[row['key']]
        field.update(last_period=row['latest_month']+'-01' if row.get('latest_month') else None,last_value=row.get('level'),
                     yoy_pct=row.get('yoy',{}).get('percent'),chg_3m_pct=row.get('three_month',{}).get('percent'),
                     unit=row['unit'],frequency='M',seasonal_adjustment='NSA',measurement_status=row['status'],
                     definition_status=row['definition_status'],source_id=sid,**measurements.FLAGS)
        field['n_obs']=row['returned_rows']
        field['history_24m_basis']='Preserved legacy last-24-observed-row view; complete exact-month rows are in measurement_review.'
        field['note']='Goods export value, not export orders, physical volume or semiconductor demand. Exact month comparisons; NSA growth is not annualized. Full observations and original values are in measurement_review.'
    for key in ('korea_flash','korea_flash_tape','taiwan_orders'):
        if isinstance(out.get(key),dict):out[key].update(research_status='retained_legacy_extraction_unqualified',**measurements.FLAGS)
    out['methodology']['reads']='The two export-value series have separate source units and calendars. Customs/news/order text extraction remains unqualified. No verified market lead or portfolio instruction is inferred.'
    out['elapsed_s']=None
    out['elapsed_status']='Native calculation uses a frozen reference clock for replay; original HTTP attempts carry actual acquisition clocks.'
    if isinstance(out.get('korea_flash_tape'),dict):
        out['korea_flash_tape']['validated_sample_status']='Legacy field name: an unverified prior-month news candidate, not external validation.'
    out['quality']={'status':'descriptive_measurements' if all(r['status']=='measured' for r in review['series'].values()) else 'partial_measurements',
                    'observation_freshness_verified':False,'legacy_populated_is_not_verified_coverage':True}
    out['research_limits']='Complete acquired responses and compiler are retained. Legacy news title limits, regex signs/periods and Taiwan order extraction are reproducible but unqualified. Full response retention does not make that extraction accurate. Historical vintages, independent evidence, forecasts and sizing remain unverified.'
    return out


def publication_context(ref,count):
    return {'manifest':ref,'original_source_replay_verified':False,'original_vintage_verified':False,
            'sources_atomic':False,'multiple_head_atomic':False,'public_write_count':count}


def publications(predecessors,pending,review,at,compilers):
    out={key:overlay(strict(predecessors[key]['raw']) if predecessors[key] else {},strict(raw)) for key,raw in pending.items()}
    out[HEAD]=projection(strict(predecessors[HEAD]['raw']),strict(pending[HEAD]),review,at,compilers)
    return out


def run(module,event=None,context=None,at=None,opener=None):
    if isinstance(event,dict) and ('httpMethod' in event or 'requestContext' in event):return {'statusCode':409,'body':'Stored research; HTTP does not run the producer.'}
    at=at or datetime.now(timezone.utc).isoformat();measurements.clock(at)
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 300
    client=Budget(module.s3,time.monotonic()+min(285,remaining-10));bucket=module.BUCKET
    predecessors={key:get(client,bucket,key) for key in KEYS}
    if predecessors[HEAD] is None:raise EvidenceError('Existing complete Asia predecessor required')
    previous=strict(predecessors[HEAD]['raw'])
    if not isinstance(previous,dict) or measurements.clock(previous.get('generated_at'))>=measurements.clock(at):raise EvidenceError('Exact older predecessor required')
    originals={key:{'original':retain(client,bucket,row['raw']),'etag':row['etag']} if row else None for key,row in predecessors.items()}
    session=Session(client,bucket,at,predecessors,opener);credentials=(module.FRED,module.NEWSAPI_KEY,module.FMP_KEY)
    result=calculate(module,session,credentials);compilers=compiler_hashes()
    output=publications(predecessors,session.pending,session.review,at,compilers)
    plan={'contract':CONTRACT,'generated_at':at,'predecessors':originals,'compiler_sha256':compilers,'http_attempts':session.http,
          'native_reads':session.reads,'native_return':result,'credential_presence':[bool(c) for c in credentials],
          'native_outputs':{key:retain(client,bucket,raw) for key,raw in session.pending.items()},
          'projections':{key:retain(client,bucket,encode(value)) for key,value in output.items()},'multiple_head_atomic':False}
    ref=retain(client,bucket,encode(plan));output[HEAD]['publication_context']=publication_context(ref,len(output))
    raw_output={key:encode(value) for key,value in output.items()}
    for raw in raw_output.values():retain(client,bucket,raw)
    for key in [k for k in KEYS if k!=HEAD and k in raw_output]+[HEAD]:
        old=predecessors[key];condition={'IfMatch':old['etag']} if old else {'IfNoneMatch':'*'}
        client.put_object(Bucket=bucket,Key=key,Body=raw_output[key],ContentType='application/json',CacheControl='no-store',**condition)
        check=get(client,bucket,key)
        if check is None or check['raw']!=raw_output[key]:raise EvidenceError('Published readback differs; foreign writer is never rolled back')
    return {'published':True,'contract':CONTRACT,'provider_attempts':len(session.http),'portfolio_action':'WAIT','multiple_head_atomic':False}


def replay(module,client,bucket,packet):
    ref=packet.get('publication_context',{}).get('manifest');plan=strict(retained(client,bucket,ref))
    if (plan.get('contract')!=CONTRACT or plan.get('compiler_sha256')!=compiler_hashes() or set(plan.get('predecessors',{}))!=set(KEYS)
            or plan.get('multiple_head_atomic') is not False or not isinstance(plan.get('http_attempts'),list) or not 1<=len(plan['http_attempts'])<=64
            or not isinstance(plan.get('credential_presence'),list) or len(plan['credential_presence'])!=3 or any(type(v)is not bool for v in plan['credential_presence'])
            or HEAD not in plan.get('native_outputs',{}) or not set(plan['native_outputs'])<=set(KEYS) or set(plan.get('projections',{}))!=set(plan['native_outputs'])):
        raise EvidenceError('Exact original compiler and publication required')
    predecessors={key:{'raw':retained(client,bucket,row['original']),'etag':row['etag']} if row else None for key,row in plan['predecessors'].items()}
    if predecessors[HEAD] is None or measurements.clock(strict(predecessors[HEAD]['raw']).get('generated_at'))>=measurements.clock(plan['generated_at']):raise EvidenceError('Exact older predecessor required')
    class Replay:
        def __init__(self):
            self.at=plan['generated_at'];self.bucket=bucket;self.predecessors=predecessors;self.pending={};self.http=[];self.reads=[];self.failure=None
            self.expected=list(plan['http_attempts'])
        ready=Session.ready
        get_object=Session.get_object
        put_object=Session.put_object
        def urlopen(self,req,timeout=None):
            try:
                if not self.expected:raise EvidenceError('Unrecorded request')
                row=self.expected.pop(0);measurements.clock(row['requested_at'])
                if row.get('request')!=identity(req) or strict(retained(client,bucket,row['attempt_manifest']))!={k:v for k,v in row.items() if k!='attempt_manifest'}:
                    raise EvidenceError('Exact source identity differs')
                if row.get('status') not in ('http_response','transport_error','rate_limit_not_retried','budget_not_attempted'):raise EvidenceError('Unknown attempt state')
                raw=retained(client,bucket,row['original']) if row.get('original') else None
                length=row.get('headers',{}).get('Content-Length')
                if (row['status']=='http_response' and raw is None) or (raw is not None and length is not None and (not str(length).isdigit() or int(length)!=len(raw))):raise EvidenceError('Complete source body differs')
            except Exception:self.failure='Original source integrity failed';self.ready()
            self.http.append(row)
            if row['status']!='http_response' or row.get('http_status')!=200:
                if row['status'] in ('rate_limit_not_retried','budget_not_attempted'):raise EvidenceError(row['status'])
                raise EvidenceError('Source unavailable; complete original attempt retained')
            return Response(raw,row.get('headers'))
    session=Replay();result=calculate(module,session,['offline-present' if present else '' for present in plan['credential_presence']])
    if session.expected or session.reads!=plan['native_reads'] or encode(result)!=encode(plan['native_return']) or set(session.pending)!=set(plan['native_outputs']):raise EvidenceError('Complete native execution differs')
    for key,raw in session.pending.items():
        if raw!=retained(client,bucket,plan['native_outputs'][key]):raise EvidenceError('Complete native output differs')
    output=publications(predecessors,session.pending,session.review,plan['generated_at'],plan['compiler_sha256'])
    for key,value in output.items():
        if encode(value)!=retained(client,bucket,plan['projections'][key]):raise EvidenceError('Complete original projection differs')
    output[HEAD]['publication_context']=publication_context(ref,len(output))
    if encode(output[HEAD])!=encode(packet):raise EvidenceError('Whole public packet differs')
    return {'status':'complete_native_sources_and_export_calendars_replayed','provider_attempts':len(session.http),'complete_native_outputs':len(output),
            'export_observations':sum(row['returned_rows'] for row in session.review['series'].values()),'provider_requests':0,'public_writes':0,
            'original_vintage_verified':False,'forecast_qualified':False}
