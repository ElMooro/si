"""Complete native inputs/calculations and protected, conditional publication.

This preserves reproducibility without qualifying the legacy economic model.
Original ArcGIS bodies are retained; source pagination/definitions are separate.
"""
from datetime import datetime,timezone
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from copy import deepcopy
import gzip,hashlib,json,math,re,time,urllib.request,urllib.error,urllib.parse

HEAD='data/portwatch.json';HISTORY='data/warm/portwatch/history/daily-rows.json.gz';IMPORT='data/import-canary.json'
KEYS=(HEAD,HISTORY,IMPORT);PRIVATE='audit-private/20260909-originals/portwatch-research/'
LIMIT=64*1024*1024;CONTRACT='portwatch-preserved-calculation.v1'
COMPILERS=('lambda_function.py','portwatch_store.py')
LAYERS=('PortWatch_chokepoints_database','Daily_Chokepoints_Data','Daily_Ports_Data','portwatch_disruptions_database','PortWatch_ports_database')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
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


def hashes():return {name:sha((Path(__file__).parent/name).read_bytes()) for name in COMPILERS}


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


def decode(raw,key):
    p=strict(whole(gzip.GzipFile(fileobj=BytesIO(raw))) if key==HISTORY else raw)
    if not isinstance(p,dict):raise CaptureError('Complete input object required')
    if key==HISTORY:
        for family in ('choke','ports','choke_fallback'):
            rows=p.get(family,{})
            if not isinstance(rows,dict) or any(not isinstance(row,dict) for row in rows.values()):raise CaptureError('Malformed complete history')
    return p


def identity(req,timeout):
    url=req.full_url if hasattr(req,'full_url') else req;parts=urllib.parse.urlsplit(url)
    body=getattr(req,'data',None);method=req.get_method() if hasattr(req,'get_method') else 'GET'
    if parts.scheme!='https' or parts.hostname!='services9.arcgis.com' or parts.port or parts.username or parts.password or parts.fragment:
        raise CaptureError('Unreviewed provider host')
    valid={'/weJ1QsnbMYJlCHdG/arcgis/rest/services/'+x+'/FeatureServer/0/query' for x in LAYERS}
    if parts.path not in valid or method not in ('GET','POST') or (method=='POST' and (parts.query or not isinstance(body,bytes))) or (method=='GET' and body is not None):
        raise CaptureError('Unreviewed provider operation')
    pairs=urllib.parse.parse_qsl(body.decode('utf-8') if body is not None else parts.query,keep_blank_values=True);query=dict(pairs)
    if len(pairs)!=len(query) or set(query)-{'where','outFields','orderByFields','resultRecordCount','resultOffset','f'} or query.get('f')!='json':
        raise CaptureError('Unreviewed provider fields')
    if type(timeout) not in (int,float) or not 0<timeout<=30:raise CaptureError('Bounded native timeout required')
    return {'method':method,'url':url,'body_utf8':body.decode('utf-8') if body is not None else None,'timeout':timeout}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


class Response(BytesIO):
    def __init__(self,raw,code=200,headers=None):super().__init__(raw);self.headers=headers or {};self.code=self.status=code
    def getcode(self):return self.code


class Session:
    def __init__(self,s3,bucket,at,opener=None):
        self.s3,self.bucket,self.at=s3,bucket,at;self.pending={};self.inputs={};self.http=[];self.reads=[];self.failure=None
        self.opener=opener or urllib.request.build_opener(NoRedirect()).open
        # A denied, malformed or truncated predecessor cannot become an empty
        # history inside the old handler's catch-all read fallback.
        for key in KEYS:
            obj=s3.get_object(Bucket=bucket,Key=key);raw=whole(obj['Body'],obj.get('ContentLength'))
            if type(obj.get('ContentLength')) is not int or not obj.get('ETag'):raise CaptureError('Complete versioned predecessor required')
            ref=retain(s3,bucket,raw);decode(raw,key)
            self.inputs[key]={'original':ref,'etag':obj['ETag'],'last_modified':obj['LastModified'].isoformat(),'raw':raw}

    def ready(self):
        if self.failure:raise CaptureError(self.failure)

    def pause(self,n):
        self.ready();time.sleep(n)

    def get_object(self,**kw):
        self.ready();key=kw.get('Key')
        if set(kw)!={'Bucket','Key'} or kw.get('Bucket')!=self.bucket or key not in (HISTORY,IMPORT):raise CaptureError('Unreviewed stored input read')
        self.reads.append(key);row=self.inputs[key]
        return {'Body':BytesIO(row['raw']),'ContentLength':len(row['raw']),'ETag':row['etag']}

    def put_object(self,**kw):
        self.ready();key=kw.get('Key');raw=kw.get('Body')
        if kw.get('Bucket')!=self.bucket or key not in (HEAD,HISTORY) or not isinstance(raw,bytes) or len(raw)>LIMIT or key in self.pending:
            self.failure='Unexpected native output';self.ready()
        self.pending[key]=raw
        return {}

    def urlopen(self,req,timeout=None):
        self.ready()
        try:
            request=identity(req,timeout);at=datetime.now(timezone.utc).isoformat()
            try:response=self.opener(req,timeout=timeout)
            except urllib.error.HTTPError as e:response=e
            except (urllib.error.URLError,OSError,TimeoutError) as e:
                row={'request':request,'acquired_at':at,'status':'transport_error','error_type':type(e).__name__}
                row['attempt_manifest']=retain(self.s3,self.bucket,encode(row));self.http.append(row);raise CaptureError('Provider transport failed')
            headers=getattr(response,'headers',{}) or {};code=response.getcode()
            raw=whole(response,headers.get('Content-Length'))
            row={'request':request,'acquired_at':at,'status':'http_response','http_status':code,
                 'headers':{k:headers[k] for k in ('Content-Type','Content-Length','Content-Encoding','Date','Last-Modified','ETag','Retry-After') if k in headers},
                 'original':retain(self.s3,self.bucket,raw)}
            row['attempt_manifest']=retain(self.s3,self.bucket,encode(row));self.http.append(row)
            if code!=200:raise CaptureError('Provider HTTP failure; no retry publication')
            packet=strict(raw)
            if not isinstance(packet,dict) or packet.get('error'):raise CaptureError('Provider data error')
            return Response(raw,code,headers)
        except Exception:
            self.failure='Complete provider acquisition failed';raise


def calculate(module,session):
    original={k:getattr(module,k) for k in ('S3','datetime','urllib','gzip','time')}
    stamp=datetime.fromisoformat(session.at)
    class Frozen(original['datetime']):
        @classmethod
        def now(cls,tz=None):return stamp.astimezone(tz) if tz else stamp.replace(tzinfo=None)
    def sleep(n):session.pause(n)
    try:
        module.S3=session;module.datetime=Frozen
        module.urllib=SimpleNamespace(parse=urllib.parse,request=SimpleNamespace(Request=urllib.request.Request,urlopen=session.urlopen))
        module.gzip=SimpleNamespace(decompress=gzip.decompress,compress=lambda raw:gzip.compress(raw,mtime=0))
        module.time=SimpleNamespace(sleep=sleep)
        result=module._native_calculation()
        session.ready()
        if set(session.pending)!={HEAD,HISTORY}:raise CaptureError('Complete native outputs required')
        return result
    finally:
        for k,v in original.items():setattr(module,k,v)


def history_preserved(old,new):
    if set(old)-set(new):raise CaptureError('History metadata removed')
    for key,value in old.items():
        if key in ('choke','ports','choke_fallback'):
            if not isinstance(new[key],dict) or set(value)-set(new[key]):raise CaptureError('Stored history rows removed')
        elif new[key]!=value:raise CaptureError('Unreviewed history metadata mutation')


def projection(calculation,context):
    packet=deepcopy(calculation)
    packet.update(contract=CONTRACT,forecast_qualified=False,calls_eligible=False,sizing_eligible=False,execution_eligible=False,
        portfolio_action='WAIT',publication_context=context,
        research_limits='Complete acquisition/calculation retention is not validation of legacy day windows, metric substitution, pagination completeness, disruption labels, exporter inference or industry consequences. These interpretations remain unqualified.')
    return packet


def run(module,event=None,context=None,opener=None,at=None):
    s3=module.S3;bucket=module.BUCKET;at=at or datetime.now(timezone.utc).isoformat()
    session=Session(s3,bucket,at,opener);result=calculate(module,session)
    if session.reads!=[HISTORY,IMPORT]:raise CaptureError('Complete reviewed native read sequence required')
    history=decode(session.pending[HISTORY],HISTORY);previous=decode(session.inputs[HISTORY]['raw'],HISTORY)
    history_preserved(previous,history)
    calculation=decode(session.pending[HEAD],HEAD)
    if calculation.get('ok') is not True or calculation.get('errors') or (calculation.get('industry_exposure_summary') or {}).get('error'):
        raise CaptureError('Incomplete calculation cannot replace previous publication')
    inputs={k:{x:y for x,y in v.items() if x!='raw'} for k,v in session.inputs.items()}
    outputs={k:retain(s3,bucket,v) for k,v in session.pending.items()}
    manifest={'contract':CONTRACT,'compiler_sha256':hashes(),'calculation_at':at,'inputs':inputs,'read_order':session.reads,
              'http_attempts':session.http,'complete_native_outputs':outputs,'native_return':result,'inputs_atomic':False,'point_in_time_verified':False}
    manifest_ref=retain(s3,bucket,encode(manifest))
    ctx={'contract':CONTRACT,'manifest':manifest_ref,'compiler_sha256':hashes(),'original_source_replay_verified':False,
         'publication_atomic':False,'point_in_time_verified':False}
    raw=encode(projection(calculation,ctx));planned={HISTORY:session.pending[HISTORY],HEAD:raw}
    retain(s3,bucket,encode({'manifest':manifest_ref,'publication_order':[HISTORY,HEAD],'planned_outputs':{k:retain(s3,bucket,v) for k,v in planned.items()}}))
    done=[]
    for key,value in planned.items():
        try:s3.put_object(Bucket=bucket,Key=key,Body=value,IfMatch=inputs[key]['etag'],
                          ContentType='application/gzip' if key==HISTORY else 'application/json',CacheControl='no-store' if key==HISTORY else 'public, max-age=900')
        except Exception as e:
            if str(getattr(e,'response',{}).get('Error',{}).get('Code')) in ('409','412','PreconditionFailed','ConditionalRequestConflict'):
                return {'published':False,'state':'concurrent_writer_preserved','completed_paths':done}
            raise
        done.append(key)
    return {'published':True,'generated_at':at,'http_attempts':len(session.http),'portfolio_action':'WAIT'}


def replay(module,s3,bucket,packet):
    ctx=packet.get('publication_context') or {}
    if ctx.get('contract')!=CONTRACT or ctx.get('compiler_sha256')!=hashes() or any(ctx.get(k) is not False for k in ('original_source_replay_verified','publication_atomic','point_in_time_verified')):
        raise CaptureError('Exact reviewed compiler and limitations required')
    m=strict(retained(s3,bucket,ctx['manifest']))
    if m.get('contract')!=CONTRACT or m.get('compiler_sha256')!=hashes() or set(m['inputs'])!=set(KEYS):raise CaptureError('Whole manifest differs')
    class Replay:
        def __init__(self):
            self.at=m['calculation_at'];self.pending={};self.failure=None;self.reads=list(m['read_order']);self.http=list(m['http_attempts'])
            self.inputs={k:{**v,'raw':retained(s3,bucket,v['original'])} for k,v in m['inputs'].items()}
        def ready(self):
            if self.failure:raise CaptureError(self.failure)
        def pause(self,n):raise CaptureError('A successful capture cannot introduce a retry in replay')
        def get_object(self,**kw):
            key=kw.get('Key')
            if kw!={'Bucket':bucket,'Key':key} or not self.reads or self.reads.pop(0)!=key:raise CaptureError('Native read order differs')
            return {'Body':BytesIO(self.inputs[key]['raw'])}
        def put_object(self,**kw):
            key=kw.get('Key')
            if kw.get('Bucket')!=bucket or key not in (HEAD,HISTORY) or key in self.pending:raise CaptureError('Unexpected native replay output')
            self.pending[key]=kw['Body'];return {}
        def urlopen(self,req,timeout=None):
            if not self.http:raise CaptureError('Unexpected provider replay')
            row=self.http.pop(0)
            if row['request']!=identity(req,timeout) or row.get('status')!='http_response' or row.get('http_status')!=200:
                self.failure='Provider replay identity differs';self.ready()
            if strict(retained(s3,bucket,row['attempt_manifest']))!={k:v for k,v in row.items() if k!='attempt_manifest'}:raise CaptureError('Original attempt descriptor differs')
            return Response(retained(s3,bucket,row['original']),200,row['headers'])
    session=Replay();result=calculate(module,session)
    if session.reads or session.http or encode(result)!=encode(m['native_return']):raise CaptureError('Incomplete deterministic replay')
    for key,raw in session.pending.items():
        if raw!=retained(s3,bucket,m['complete_native_outputs'][key]):raise CaptureError('Whole native calculation differs')
    history_preserved(decode(session.inputs[HISTORY]['raw'],HISTORY),decode(session.pending[HISTORY],HISTORY))
    if encode(packet)!=encode(projection(decode(session.pending[HEAD],HEAD),ctx)):raise CaptureError('Final public projection differs')
    return {'status':'whole_native_calculation_replayed','http_attempts':len(m['http_attempts']),
            'history_rows':{k:len(v) for k,v in decode(session.pending[HISTORY],HISTORY).items() if k in ('choke','ports','choke_fallback')},
            'provider_requests':0,'public_writes':0,'point_in_time_verified':False,'model_qualified':False}
