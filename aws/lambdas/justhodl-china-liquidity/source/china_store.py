"""Whole China source retention, conditional research publication and offline replay."""
from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from copy import deepcopy
from types import SimpleNamespace
import contextlib,hashlib,json,math,re,time,urllib.request,urllib.error,urllib.parse
import china_measurements as measurements
HEAD='data/china-liquidity.json'
KEYS=(HEAD,'data/china-liquidity-history.json','pboc/afre-flow-cache.json')
PRIVATE='audit-private/20260909-originals/china-liquidity-research/'
CONTRACT='china-liquidity-research.v1'
LIMIT=64*1024*1024
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
COMPILERS=('lambda_function.py','china_store.py','china_measurements.py','_fred_shim.py')
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
    if p.scheme not in ('http','https') or p.netloc not in ('www.pbc.gov.cn','pbc.gov.cn') or p.username or p.password or p.fragment:
        raise EvidenceError('Declared PBoC government source required')
    return 'www.pbc.gov.cn'


def normalized(req):
    req=urllib.request.Request(req) if isinstance(req,str) else req
    p=urllib.parse.urlsplit(req.full_url)
    if req.get_method()!='GET' or req.data is not None or req.has_header('Authorization'):
        raise EvidenceError('Public GET required')
    url=req.full_url
    if p.scheme=='http':
        government(url);url=urllib.parse.urlunsplit(('https',p.netloc,p.path,p.query,p.fragment))
    elif p.netloc=='justhodl-data-proxy.raafouis.workers.dev':
        pairs=urllib.parse.parse_qsl(p.query,keep_blank_values=True)
        if p.path!='/gov' or len(pairs)!=1 or pairs[0][0]!='u':raise EvidenceError('Declared PBoC proxy required')
        target=pairs[0][1];government(target);tp=urllib.parse.urlsplit(target)
        target=urllib.parse.urlunsplit(('https',tp.netloc,tp.path,tp.query,tp.fragment))
        url=urllib.parse.urlunsplit((p.scheme,p.netloc,p.path,urllib.parse.urlencode({'u':target}),p.fragment))
    return urllib.request.Request(url,headers=dict(req.header_items()))


def identity(req,monthly_code=None):
    req=normalized(req);p=urllib.parse.urlsplit(req.full_url)
    pairs=urllib.parse.parse_qsl(p.query,keep_blank_values=True);q=dict(pairs)
    if p.scheme!='https' or p.username or p.password or p.fragment or len(q)!=len(pairs):raise EvidenceError('Exact HTTPS public GET required')
    host=p.netloc;source_host=host
    if host=='api.stlouisfed.org':
        if q.get('series_id') not in measurements.PROFILES or q.get('file_type')!='json':raise EvidenceError('Undeclared China series')
        basic={'series_id','api_key','file_type','realtime_start','realtime_end'}
        if q.get('realtime_start')!=q.get('realtime_end') or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',q.get('realtime_start','')):raise EvidenceError('Exact as-of date required')
        if p.path=='/fred/series/observations':
            if (set(q)!=basic|{'observation_start','observation_end','sort_order','limit','offset','units','output_type'}
                    or q['observation_start']!='1776-07-04' or q['observation_end']!=q['realtime_end'] or q['sort_order']!='asc'
                    or q['limit']!='100000' or q['offset']!='0' or q['units']!='lin' or q['output_type']!='1'):raise EvidenceError('Complete untransformed observations required')
        elif p.path!='/fred/series' or set(q)!=basic:raise EvidenceError('Undeclared metadata')
    elif host=='api.db.nomics.world':
        allowed={'/v22/series/NBS/A_A0L08':'30'}
        if monthly_code:allowed['/v22/series/NBS/'+monthly_code]='40'
        if p.path not in allowed or q!={'limit':allowed[p.path],'observations':'1'}:raise EvidenceError('Declared NBS dataset required')
    elif host=='justhodl-data-proxy.raafouis.workers.dev':source_host=government(q['u'])
    else:source_host=government(req.full_url)
    return {'method':'GET','endpoint':p.scheme+'://'+p.netloc+p.path,'source_host':source_host,
            'parameters':{k:v for k,v in q.items() if k!='api_key'}}


class Missing(Exception):
    response={'Error':{'Code':'NoSuchKey'}}


class Session:
    def __init__(self,client,bucket,at,predecessors,opener=None,monthly_code=None):
        self.client,self.bucket,self.at,self.predecessors=client,bucket,at,predecessors
        self.opener=opener or urllib.request.build_opener(NoRedirect()).open
        self.http=[];self.pending={};self.reads=[];self.failure=None;self.rate_limited=set();self.monthly_code=monthly_code
        self.started=time.monotonic();self.total_bytes=0
    def ready(self):
        if self.failure:raise EvidenceError(self.failure)
    def record(self,row):
        try:row['attempt_manifest']=retain(self.client,self.bucket,encode(row));self.http.append(row)
        except Exception:self.failure='Complete attempt retention failed';self.ready()
    def get_object(self,**kw):
        self.ready();key=kw.get('Key')
        if kw.get('Bucket')!=self.bucket or key not in KEYS[1:]:
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
            req=normalized(req);request=identity(req,self.monthly_code)
            if type(timeout) not in (int,float) or not 0<timeout<=45 or len(self.http)>=64:raise EvidenceError('Request bound differs')
        except Exception:self.failure='Undeclared provider request';self.ready()
        row={'request':request,'requested_at':datetime.now(timezone.utc).isoformat()};host=request['source_host']
        remaining=80-(time.monotonic()-self.started)
        if host in self.rate_limited or remaining<=1:
            row['status']='rate_limit_not_retried' if host in self.rate_limited else 'budget_not_attempted'
            self.record(row);raise EvidenceError(row['status'])
        phase='connect'
        try:
            try:response=self.opener(req,timeout=min(timeout,6,remaining))
            except urllib.error.HTTPError as exc:response=exc
            phase='body';code=response.getcode();headers=response.headers or {};url=req if isinstance(req,str) else req.full_url
            if hasattr(response,'geturl') and response.geturl()!=url:response.close();raise EvidenceError('Redirect refused')
            raw=whole(response,headers.get('Content-Length'),16*1024*1024,self.started+80)
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
    original={k:getattr(module,k) for k in ('s3','datetime','time','request','FRED_KEY','os','fred','HISTORY_MAX','maybe_telegram')}
    at=measurements.clock(session.at);reviews={};raw_fred={};session.fred_reads=[];session.notifications_suppressed=0
    class Frozen(original['datetime']):
        @classmethod
        def now(cls,tz=None):return at.astimezone(tz) if tz else at.replace(tzinfo=None)
    def fred_from_original(sid,limit=400):
        if sid not in measurements.PROFILES or limit!=400:raise EvidenceError('Undeclared legacy series view')
        session.fred_reads.append(sid);packet=raw_fred.get(sid)
        rows=packet.get('observations',[]) if isinstance(packet,dict) else []
        # Match the old request's last-400-position window before it skipped nulls.
        rows=sorted(rows,key=lambda r:r.get('date',''),reverse=True)[:400]
        result=[]
        for row in rows:
            if row.get('value') in (None,'.',''):continue
            try:result.append({'date':row.get('date'),'value':float(row['value'])})
            except (ValueError,TypeError):pass
        return result
    def no_notification(*args,**kw):session.notifications_suppressed+=1
    try:
        # One full population per series supplies both the exact review and the
        # retained legacy window; no duplicate latest-400 provider request.
        for sid in measurements.PROFILES:
            params={'series_id':sid,'api_key':credentials[0],'file_type':'json','realtime_start':at.date().isoformat(),'realtime_end':at.date().isoformat()}
            packets=[]
            for endpoint in ('series','series/observations'):
                if endpoint.endswith('observations'):params.update(observation_start='1776-07-04',observation_end=at.date().isoformat(),sort_order='asc',limit='100000',offset='0',units='lin',output_type='1')
                try:
                    with session.urlopen('https://api.stlouisfed.org/fred/'+endpoint+'?'+urllib.parse.urlencode(params),timeout=12) as response:packets.append(strict(response.read()))
                except EvidenceError:session.ready();packets.append(None)
            reviews[sid]=measurements.fred(sid,*packets,session.at);raw_fred[sid]=packets[1]
        module.s3=session;module.datetime=Frozen;module.time=SimpleNamespace(time=lambda:at.timestamp(),sleep=lambda _:None)
        module.request=SimpleNamespace(Request=urllib.request.Request,urlopen=session.urlopen)
        module.FRED_KEY=credentials[0];module.os=SimpleNamespace(environ={'NBS_TSF_MONTHLY':session.monthly_code} if session.monthly_code else {})
        module.fred=fred_from_original;module.maybe_telegram=no_notification
        history=strict(session.predecessors[KEYS[1]]['raw'])
        module.HISTORY_MAX=len(history['snapshots'])+1
        with contextlib.redirect_stdout(Quiet()):result=module._legacy_lambda_handler({},None)
        session.ready()
        if HEAD not in session.pending or KEYS[1] not in session.pending:raise EvidenceError('Complete native head and history required')
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


def current(row,field='level',nested=None):
    if row.get('current_measurement_eligible') is not True:return None
    value=row.get(field)
    return value.get(nested) if nested and isinstance(value,dict) else value


def projection(previous,native,review,at,compilers):
    out=overlay(previous,native);rows=review['series']
    out.update(version='2.0.0',schema_version='2.0',contract=CONTRACT,generated_at=at,measurement_review=review,compiler_sha256=compilers,
               legacy_calculation=deepcopy(native),portfolio_action='WAIT',regime='WAIT',**measurements.FLAGS)
    out['regime_read']='No qualified policy regime or market lead. Separate dated monetary stocks, interest rates and price observations are descriptive research.'
    out['money'].update(m1_yoy_pct=current(rows['MANMM101CNM189S'],'yoy','value'),m2_yoy_pct=current(rows['MYAGM2CNM189N'],'yoy','value'),
                        m1_observation_date=rows['MANMM101CNM189S'].get('latest_date'),m2_observation_date=rows['MYAGM2CNM189N'].get('latest_date'),
                        status='Separate M1 and M2 definitions; M3 is never substituted. Stale series are historical context only.',**measurements.FLAGS)
    out['credit_impulse'].update(value_pp=None,is_proxy=False,signal=None,
                                definition='Unavailable: a TSF credit impulse requires a qualified flow definition, comparison periods and denominator. Money-growth acceleration is a separate descriptive measure.',
                                measurement_status='not_established',**measurements.FLAGS)
    for key in ('impulse_pct','value','latest'):
        if key in out['credit_impulse']:out['credit_impulse'][key]=None
    out['interbank_rate'].update(latest_pct=current(rows['IR3TIB01CNM156N']),change_3m_pp=current(rows['IR3TIB01CNM156N'],'three_month','value'),
                                 observation_date=rows['IR3TIB01CNM156N'].get('latest_date'),source_id='IR3TIB01CNM156N')
    out['currency'].update(usd_cny=current(rows['DEXCHUS']),cny_change_3m_pct=current(rows['DEXCHUS'],'three_month','value'),
                           read='CNY per USD exchange rate; a change does not establish capital inflows or outflows.',observation_date=rows['DEXCHUS'].get('latest_date'))
    out['dr_copper'].update(copper_yoy_pct=current(rows['PCOPPUSDM'],'yoy','value'),copper_gold_ratio=None,
                           read='Nominal USD per metric ton. Price alone does not identify Chinese real demand. Gold export-price index is not bullion.',
                           ratio_status='No contemporaneous compatible bullion-price denominator',observation_date=rows['PCOPPUSDM'].get('latest_date'))
    out['series_resolved'].update(m1='MANMM101CNM189S',m2='MYAGM2CNM189N',interbank='IR3TIB01CNM156N',usdcny='DEXCHUS',copper='PCOPPUSDM',gold=None)
    out['series_resolved_basis']='Declared canonical identities, not availability; every candidate including M3 and retired/unreviewed alternatives remains in measurement_review.'
    out['tsf'].update(research_status='retained_legacy_extraction_unqualified',**measurements.FLAGS)
    out['tsf']['note']='Original NBS/PBoC extraction retained for inspection. Annual composition, cumulative and monthly flows cannot substitute for one another; signs, units and periods are not yet independently qualified.'
    for key in ('pboc_monthly','pboc_cn','monthly'):
        if isinstance(out['tsf'].get(key),dict):out['tsf'][key].update(research_status='retained_legacy_extraction_unqualified',**measurements.FLAGS)
    out['elapsed_s']=None;out['elapsed_status']='Frozen calculation clock; actual acquisition times remain in original manifests.'
    out['quality']={'status':'partial_measurements','current_series':sum(r.get('current_measurement_eligible') is True for r in rows.values()),
                    'total_declared_series':len(rows),'publication_is_not_observation_freshness':True}
    out['research_limits']='Whole acquired responses, compiler and complete predecessors retained. Legacy PBoC/DBnomics truncation, regex signs/periods, historical vintages, independent evidence, forecasts and sizing remain unqualified.'
    return out


def publication_context(ref,count):
    return {'manifest':ref,'original_source_replay_verified':False,'original_vintage_verified':False,
            'sources_atomic':False,'multiple_head_atomic':False,'public_write_count':count}


def validate_predecessors(predecessors,at):
    if predecessors[HEAD] is None or predecessors[KEYS[1]] is None:raise EvidenceError('Complete existing China head and history required')
    previous=strict(predecessors[HEAD]['raw']);history=strict(predecessors[KEYS[1]]['raw'])
    if not isinstance(previous,dict) or measurements.clock(previous.get('generated_at'))>=measurements.clock(at):raise EvidenceError('Exact older predecessor required')
    if not isinstance(history,dict) or not isinstance(history.get('snapshots'),list):raise EvidenceError('Whole original history required')
    if any(not isinstance(r,dict) or measurements.clock(r.get('ts'))>=measurements.clock(at) for r in history['snapshots']):raise EvidenceError('Original history date conflicts')
    cache=predecessors[KEYS[2]]
    if cache is not None:
        value=strict(cache['raw'])
        if not isinstance(value,dict) or not isinstance(value.get('reports'),dict):raise EvidenceError('Whole original AFRE cache required')


def publications(predecessors,pending,review,at,compilers):
    out={key:overlay(strict(predecessors[key]['raw']) if predecessors[key] else {},strict(raw)) for key,raw in pending.items()}
    native=strict(pending[HEAD]);out[HEAD]=projection(strict(predecessors[HEAD]['raw']),native,review,at,compilers)
    old=strict(predecessors[KEYS[1]]['raw'])['snapshots'];new=out[KEYS[1]]['snapshots']
    if len(new)!=len(old)+1 or new[:-1]!=old or new[-1].get('ts')!=at:raise EvidenceError('Every predecessor history row must be preserved')
    row=new[-1];row.update(regime='WAIT',m2_yoy=out[HEAD]['money']['m2_yoy_pct'],credit_impulse=None,
                         copper_yoy=out[HEAD]['dr_copper']['copper_yoy_pct'],contract=CONTRACT,**measurements.FLAGS)
    out[KEYS[1]]['history_basis']='Original rows remain unchanged and unqualified. New rows identify the research contract; no retrospective rewriting.'
    return out


def run(module,event=None,context=None,at=None,opener=None):
    if isinstance(event,dict) and ('httpMethod' in event or 'requestContext' in event):return {'statusCode':409,'body':'Stored research; HTTP does not run the producer.'}
    at=at or datetime.now(timezone.utc).isoformat();measurements.clock(at)
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 120
    client=Budget(module.s3,time.monotonic()+min(110,remaining-5));bucket=module.S3_BUCKET
    predecessors={key:get(client,bucket,key) for key in KEYS}
    validate_predecessors(predecessors,at)
    originals={key:{'original':retain(client,bucket,row['raw']),'etag':row['etag']} if row else None for key,row in predecessors.items()}
    monthly_code=module.os.environ.get('NBS_TSF_MONTHLY')
    if monthly_code and (not re.fullmatch(r'[A-Za-z0-9_/-]{1,120}',monthly_code) or '..' in monthly_code):raise EvidenceError('Public NBS dataset code required')
    if not module.FRED_KEY:raise EvidenceError('Existing FRED authentication unavailable')
    session=Session(client,bucket,at,predecessors,opener,monthly_code);credentials=(module.FRED_KEY,)
    result=calculate(module,session,credentials);compilers=compiler_hashes()
    output=publications(predecessors,session.pending,session.review,at,compilers)
    plan={'contract':CONTRACT,'generated_at':at,'predecessors':originals,'compiler_sha256':compilers,'http_attempts':session.http,
          'native_reads':session.reads,'native_return':result,'credential_presence':[bool(c) for c in credentials],
          'monthly_dataset_code':monthly_code,'legacy_fred_reads':session.fred_reads,'notifications_suppressed':session.notifications_suppressed,
          'native_outputs':{key:retain(client,bucket,raw) for key,raw in session.pending.items()},
          'projections':{key:retain(client,bucket,encode(value)) for key,value in output.items()},'multiple_head_atomic':False}
    ref=retain(client,bucket,encode(plan));output[HEAD]['publication_context']=publication_context(ref,len(output))
    raw_output={key:encode(value) for key,value in output.items()}
    if len(module.FRED_KEY) >= 8 and any(module.FRED_KEY.encode() in raw for raw in raw_output.values()):
        raise EvidenceError('Provider credential echoed in response; public publication refused')
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
            or not isinstance(plan.get('credential_presence'),list) or len(plan['credential_presence'])!=1 or any(type(v)is not bool for v in plan['credential_presence'])
            or HEAD not in plan.get('native_outputs',{}) or not set(plan['native_outputs'])<=set(KEYS) or set(plan.get('projections',{}))!=set(plan['native_outputs'])):
        raise EvidenceError('Exact original compiler and publication required')
    predecessors={key:{'raw':retained(client,bucket,row['original']),'etag':row['etag']} if row else None for key,row in plan['predecessors'].items()}
    validate_predecessors(predecessors,plan['generated_at'])
    code=plan.get('monthly_dataset_code')
    if code and (not isinstance(code,str) or not re.fullmatch(r'[A-Za-z0-9_/-]{1,120}',code) or '..' in code):raise EvidenceError('Invalid retained dataset code')
    class Replay:
        def __init__(self):
            self.at=plan['generated_at'];self.bucket=bucket;self.predecessors=predecessors;self.pending={};self.http=[];self.reads=[];self.failure=None
            self.expected=list(plan['http_attempts']);self.monthly_code=code
        ready=Session.ready
        get_object=Session.get_object
        put_object=Session.put_object
        def urlopen(self,req,timeout=None):
            try:
                if not self.expected:raise EvidenceError('Unrecorded request')
                row=self.expected.pop(0);measurements.clock(row['requested_at'])
                if row.get('request')!=identity(req,self.monthly_code) or strict(retained(client,bucket,row['attempt_manifest']))!={k:v for k,v in row.items() if k!='attempt_manifest'}:
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
    if session.expected or session.reads!=plan['native_reads'] or session.fred_reads!=plan.get('legacy_fred_reads') or session.notifications_suppressed!=plan.get('notifications_suppressed') or encode(result)!=encode(plan['native_return']) or set(session.pending)!=set(plan['native_outputs']):raise EvidenceError('Complete native execution differs')
    for key,raw in session.pending.items():
        if raw!=retained(client,bucket,plan['native_outputs'][key]):raise EvidenceError('Complete native output differs')
    output=publications(predecessors,session.pending,session.review,plan['generated_at'],plan['compiler_sha256'])
    for key,value in output.items():
        if encode(value)!=retained(client,bucket,plan['projections'][key]):raise EvidenceError('Complete original projection differs')
    output[HEAD]['publication_context']=publication_context(ref,len(output))
    if encode(output[HEAD])!=encode(packet):raise EvidenceError('Whole public packet differs')
    return {'status':'complete_native_sources_and_china_calendars_replayed','provider_attempts':len(session.http),'complete_native_outputs':len(output),
            'china_observations':sum(row['returned_rows'] for row in session.review['series'].values()),'provider_requests':0,'public_writes':0,
            'original_vintage_verified':False,'forecast_qualified':False}
