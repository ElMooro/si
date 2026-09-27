"""Whole CAD workbook capture, complete ledger and conditional publication."""
from pathlib import Path
from datetime import datetime,timezone
from copy import deepcopy
from io import BytesIO
import hashlib,json,math,re,time,urllib.request,urllib.error
import air_measurements as measurements
HEAD='data/air-cargo.json'
LEVELS='air/hkia-cargo-levels.json'
PRIVATE='audit-private/20260909-originals/air-cargo-research/'
LIMIT=64*1024*1024
CONTRACT='air-cargo-research.v1'
URL='https://www.cad.gov.hk/english/pdf/Stat%20Webpage.xlsx'
EDGE='https://justhodl-data-proxy.raafouis.workers.dev/gov?u=https%3A%2F%2Fwww.cad.gov.hk%2Fenglish%2F.%2Fpdf%2FStat%20Webpage.xlsx'
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')


class EvidenceError(ValueError):pass

def strict(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise EvidenceError('Duplicate JSON key')
            out[k]=v
        return out
    def invalid(x):raise EvidenceError('Nonfinite JSON number')
    def number(x):
        n=float(x)
        if not math.isfinite(n):invalid(x)
        return n
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=invalid)

def whole(stream,length):
    parts=[];size=0
    try:
        while True:
            raw=stream.read(min(65536,LIMIT+1-size))
            if not raw:break
            if not isinstance(raw,bytes):raise EvidenceError('Byte stream required')
            parts.append(raw);size+=len(raw)
            if size>LIMIT:raise EvidenceError('Complete body exceeds bound')
    finally:stream.close()
    if type(length) is not int or size!=length:raise EvidenceError('Incomplete body')
    return b''.join(parts)

def get(client,bucket,key):
    try:obj=client.get_object(Bucket=bucket,Key=key)
    except Exception as e:
        code=str(getattr(e,'response',{}).get('Error',{}).get('Code'))
        if code in ('NoSuchKey','404','NotFound'):return None
        raise EvidenceError('Object read failed') from None
    raw=whole(obj['Body'],obj.get('ContentLength'))
    etag=obj.get('ETag')
    if not isinstance(etag,str) or not etag:raise EvidenceError('Conditional identity absent')
    return {'raw':raw,'etag':etag}

def retain(client,bucket,raw):
    if not isinstance(raw,bytes) or len(raw)>LIMIT:raise EvidenceError('Bounded whole original required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:client.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as e:
        if str(getattr(e,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if retained(client,bucket,ref)!=raw:raise EvidenceError('Original readback differs')
    return ref

def retained(client,bucket,ref):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
        or ref.get('key')!=PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int or not 0<=ref['bytes']<=LIMIT):raise EvidenceError('Protected identity required')
    got=get(client,bucket,ref['key'])
    if got is None or len(got['raw'])!=ref['bytes'] or sha(got['raw'])!=ref['sha256']:raise EvidenceError('Retained body differs')
    return got['raw']

def stamp(value):
    if not isinstance(value,str) or 'T' not in value:raise EvidenceError('Aware publication clock required')
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:raise EvidenceError('Aware publication clock required')
    return d.astimezone(timezone.utc)


def compiler_hashes():
    return {name:sha((Path(__file__).parent/name).read_bytes()) for name in ('lambda_function.py','air_store.py','air_measurements.py')}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


class Budget:
    def __init__(self,client,deadline):self.client,self.deadline=client,deadline
    def check(self):
        if time.monotonic()+25>=self.deadline:raise EvidenceError('Acquisition/publication budget exhausted')
    def get_object(self,**kw):self.check();return self.client.get_object(**kw)
    def put_object(self,**kw):self.check();return self.client.put_object(**kw)


def http_body(stream):
    chunks=[];size=0;start=time.monotonic()
    try:
        while True:
            if time.monotonic()-start>30:raise EvidenceError('Whole response deadline exceeded')
            chunk=stream.read(min(65536,16*1024*1024+1-size))
            if not chunk:break
            if not isinstance(chunk,bytes):raise EvidenceError('Whole workbook bytes required')
            size+=len(chunk);chunks.append(chunk)
            if size>16*1024*1024:raise EvidenceError('Whole workbook exceeds bound; not truncated')
        raw=b''.join(chunks);length=stream.headers.get('Content-Length')
        if length is not None and (not str(length).isdigit() or int(length)!=len(raw)):raise EvidenceError('Truncated workbook response')
        return raw
    finally:stream.close()


def acquire(client,bucket,opener=None):
    opener=opener or urllib.request.build_opener(NoRedirect()).open;attempts=[]
    for via,url in (('edge',EDGE),('direct',URL)):
        client.check() if isinstance(client,Budget) else None
        req=urllib.request.Request(url,headers={'User-Agent':'JustHodl-CAD-Research/2.1','Accept':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'})
        attempt={'via':via,'request_url':url,'requested_at':datetime.now(timezone.utc).isoformat()}
        try:
            response=opener(req,timeout=25)
            if response.getcode()!=200:raise EvidenceError('Unexpected workbook response status')
            if hasattr(response,'geturl') and response.geturl()!=url:raise EvidenceError('Unexpected workbook redirect')
            headers={k:response.headers.get(k) for k in ('Content-Type','Content-Length','Last-Modified','ETag','Date')}
            raw=http_body(response)
            try:ref=retain(client,bucket,raw)
            except Exception:raise EvidenceError('Complete workbook retention failed') from None
            attempt.update(status=200,received_at=datetime.now(timezone.utc).isoformat(),headers=headers,original=ref)
            attempts.append(attempt);retain(client,bucket,encode({'http_attempts':attempts}))
            if not raw:raise EvidenceError('Empty workbook body')
            return raw,attempts
        except urllib.error.HTTPError as e:
            raw=http_body(e)
            attempt.update(status=e.code,error_type='HTTPError',original=retain(client,bucket,raw));attempts.append(attempt)
            retain(client,bucket,encode({'http_attempts':attempts}))
            if e.code==429:raise EvidenceError('Source rate limited; no immediate fallback') from None
        except EvidenceError:
            # A damaged/oversized/redirected/retention-denied response cannot
            # become a partial workbook or be masked by another transport.
            retain(client,bucket,encode({'http_attempts':attempts,'failed_attempt':attempt,'status':'refused_incomplete_or_unverified_acquisition'}))
            raise
        except Exception as e:
            attempt.update(status=None,error_type=type(e).__name__);attempts.append(attempt)
            retain(client,bucket,encode({'http_attempts':attempts}))
    raise EvidenceError('All existing workbook transports failed')


def predecessors(client,bucket,at):
    found={}
    for key in (HEAD,LEVELS):
        obj=get(client,bucket,key)
        if obj is None:raise EvidenceError('Existing publication/history missing; no empty bootstrap')
        original=retain(client,bucket,obj['raw']);doc=strict(obj['raw'])
        if not isinstance(doc,dict):raise EvidenceError('Whole predecessor object required')
        found[key]={'doc':doc,'etag':obj['etag'],'original':original}
    head=found[HEAD]['doc'];ledger=found[LEVELS]['doc']
    if stamp(head.get('generated_at'))>=stamp(at):raise EvidenceError('Publication predecessor is not older')
    if not isinstance(ledger.get('levels'),dict):raise EvidenceError('Complete prior level population required')
    for month,value in ledger['levels'].items():
        if not re.fullmatch(r'\d{4}-(?:0[1-9]|1[0-2])',month) or month>at[:7]:raise EvidenceError('Invalid predecessor month')
        if value is not None and (type(value) not in (int,float) or not math.isfinite(value) or value<0):raise EvidenceError('Invalid prior level; zero and null remain distinct')
    return found


def projection(previous,ledger,review,at,via,workbook_bytes,compilers):
    rows=review['monthly_observations'];latest=rows[-1];levels=deepcopy(ledger)
    current={r['month']:r['total']/1000 if r['total'] is not None else None for r in rows}
    levels['levels'].update(current)
    levels.update(generated_at=at,contract=CONTRACT,unit='thousand_tonnes',source='HK CAD workbook; freight excludes air mail',
                  research_months=list(current),legacy_only_months=sorted(set(ledger['levels'])-set(current)),
                  original_vintage_verified=False,compiler_sha256=compilers)
    out=deepcopy(previous)
    out.update(ok=latest['total'] is not None,version='2.1.0',contract=CONTRACT,generated_at=at,
        airport='Hong Kong International Airport',errors=[],attribution='Hong Kong Civil Aviation Department; source Airport Authority Hong Kong',
        fetch_via=via,xlsx_bytes=workbook_bytes,tonnes=latest['total'],tonnes_k=latest['total']/1000 if latest['total'] is not None else None,
        month=latest['month'],via='cad_xlsx('+via+')',xlsx_n=len(rows),xlsx_tail=[[r['month'],r['total']/1000 if r['total'] is not None else None] for r in rows[-13:]],
        yoy_pct=latest['reported_yoy_pct'],yoy_basis='Published source change calculated from unrounded figures; independent displayed-level change is separate.',
        read='MONTHLY FREIGHT TONNAGE; cargo value and commodity mix are unavailable',levels_cached=len(levels['levels']),
        measurement_review=review,compiler_sha256=compilers,calls_eligible=False,sizing_eligible=False,execution_eligible=False,forecast_qualified=False,portfolio_action='WAIT')
    return out,levels


def run(client,bucket,event=None,context=None,at=None,opener=None):
    at=at or datetime.now(timezone.utc).isoformat();stamp(at)
    remaining=context.get_remaining_time_in_millis()/1000 if context and hasattr(context,'get_remaining_time_in_millis') else 180
    client=Budget(client,time.monotonic()+min(155,remaining-10))
    before=predecessors(client,bucket,at)
    raw,attempts=acquire(client,bucket,opener)
    review=measurements.measure(raw,at);compilers=compiler_hashes()
    out,ledger=projection(before[HEAD]['doc'],before[LEVELS]['doc'],review,at,attempts[-1]['via'],len(raw),compilers)
    plan={'contract':CONTRACT,'generated_at':at,'inputs':{k:{'original':v['original'],'etag':v['etag']} for k,v in before.items()},
          'http_attempts':attempts,'compiler_sha256':compilers,'projection':retain(client,bucket,encode(out)),
          'ledger':retain(client,bucket,encode(ledger)),'publication_order':[LEVELS,HEAD],'publication_atomic':False}
    ref=retain(client,bucket,encode(plan));out['publication_context']={'manifest':ref,'original_vintage_verified':False,'publication_atomic':False,'replay_scope':'Complete stored workbook, prior ledger and exact monthly projection; no first-release-vintage or forecast qualification.'}
    head_raw=encode(out);retain(client,bucket,head_raw)
    # Check both before starting. Actual writes remain conditional, ordered and
    # explicitly non-atomic; never roll back another writer or remove history.
    for key in (HEAD,LEVELS):
        now=get(client,bucket,key)
        if now is None or now['etag']!=before[key]['etag']:raise EvidenceError('Predecessor changed before publication')
    for key,data in ((LEVELS,encode(ledger)),(HEAD,head_raw)):
        client.put_object(Bucket=bucket,Key=key,Body=data,IfMatch=before[key]['etag'],ContentType='application/json',CacheControl='public, max-age=3600')
        check=get(client,bucket,key)
        if check is None or check['raw']!=data:raise EvidenceError('Publication changed or differs; no rollback')
    return {'ok':out['ok'],'published':True,'contract':CONTRACT,'month':out['month'],'monthly_observations':len(review['monthly_observations']),'levels_preserved':len(ledger['levels']),'portfolio_action':'WAIT'}


def replay(client,bucket,packet):
    plan=strict(retained(client,bucket,packet['publication_context']['manifest']))
    if plan.get('contract')!=CONTRACT or plan.get('compiler_sha256')!=compiler_hashes() or set(plan.get('inputs',{}))!={HEAD,LEVELS}:raise EvidenceError('Exact reviewed compiler and predecessors required')
    originals={key:strict(retained(client,bucket,row['original'])) for key,row in plan['inputs'].items()}
    attempts=plan['http_attempts']
    if not 1<=len(attempts)<=2 or attempts[-1].get('status')!=200:raise EvidenceError('Complete successful workbook acquisition required')
    for i,a in enumerate(attempts):
        if (a.get('via'),a.get('request_url'))!=(('edge',EDGE) if i==0 else ('direct',URL)):raise EvidenceError('Reviewed transport identity differs')
        if a.get('original') is not None:retained(client,bucket,a['original'])
    raw=retained(client,bucket,attempts[-1]['original']);review=measurements.measure(raw,plan['generated_at'])
    out,levels=projection(originals[HEAD],originals[LEVELS],review,plan['generated_at'],attempts[-1]['via'],len(raw),plan['compiler_sha256'])
    if retained(client,bucket,plan['projection'])!=encode(out) or retained(client,bucket,plan['ledger'])!=encode(levels):raise EvidenceError('Whole source/ledger replay differs')
    out['publication_context']={'manifest':packet['publication_context']['manifest'],'original_vintage_verified':False,'publication_atomic':False,'replay_scope':'Complete stored workbook, prior ledger and exact monthly projection; no first-release-vintage or forecast qualification.'}
    if encode(out)!=encode(packet):raise EvidenceError('Whole public output differs')
    return {'status':'matched','http_attempts':len(attempts),'workbook_bytes':len(raw),'monthly_observations':review['monthly_count'],'annual_rows':review['annual_count'],'ledger_months':len(levels['levels']),'provider_requests':0,'original_vintage_verified':False}
