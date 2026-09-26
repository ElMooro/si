"""Retain whole ICI originals, reconstruct every measurement, publish with CAS.

Only scheduled native runs acquire releases. Failed runs leave the public head
and the unvintaged legacy histories untouched. Raw publisher HTML stays private.
"""
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json,re,urllib.request,urllib.error
import ici_research as model
import verify_ici_research as independent
from ici_qualification import QUALIFIED

CURRENT='data/ici-flows.json'
PREFIX='data/ici-research/'
PRIVATE='audit-private/20260909-originals/ici-research/'
ROOT=Path(__file__).resolve().parent
MAX=8*1024*1024
AUTHORITY=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
now=lambda:datetime.now(timezone.utc).isoformat()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
code=lambda exc:str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
conflict=lambda exc:code(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')
missing=lambda exc:code(exc) in ('404','NoSuchKey','NotFound')


def strict(raw):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def invalid(value):raise ValueError('Nonfinite JSON number')
    out=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
    encode(out)
    return out


def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=MAX:raise ValueError('Complete bounded artifact required')
    return raw


def qualified():
    for name,digest in QUALIFIED.items():
        if sha((ROOT/name).read_bytes())!=digest:raise ValueError('Accepted ICI arithmetic differs: '+name)


def compilers():
    return {name:ROOT/name for name in ('ici_research.py','verify_ici_research.py','ici_store.py','ici_qualification.py','lambda_function.py')}


def reader(client,bucket):
    def read(key):
        if not isinstance(key,str) or not (key==CURRENT or re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin',key)
            or re.fullmatch(re.escape(PREFIX)+r'(?:runs|inputs|outputs|proofs|compilers)/[a-f0-9]{64}\.(?:json|py)',key)):
            raise ValueError('Unapproved ICI artifact')
        return bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    return read


def retain(client,bucket,raw,category='inputs',extension='json',private=False):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Complete artifact required')
    if category not in ('runs','inputs','outputs','proofs','compilers') or extension not in ('json','py'):
        raise ValueError('Unapproved artifact type')
    key=PRIVATE+sha(raw)+'.bin' if private else PREFIX+category+'/'+sha(raw)+'.'+extension
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',
        ContentType='application/octet-stream' if private else 'text/plain' if extension=='py' else 'application/json',
        CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if reader(client,bucket)(key)!=raw:raise ValueError('Whole retained artifact differs')
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}


def checked(ref,read,category='inputs',extension='json',private=False):
    if not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256'))):raise ValueError('Exact content identity required')
    key=PRIVATE+ref['sha256']+'.bin' if private else PREFIX+category+'/'+ref['sha256']+'.'+extension
    if ref.get('key')!=key or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX:raise ValueError('Whole bounded reference required')
    raw=read(key)
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Referenced bytes differ')
    return raw if private or extension=='py' else strict(raw)


def compile_inputs(inputs,read):
    qualified()
    if inputs.get('contract')!='ici-inputs.v1' or set(inputs.get('sources',{}))!=set(model.SOURCES):raise ValueError('Both native original inputs required')
    sources={}
    for kind,entry in inputs['sources'].items():
        if entry['url']!=model.SOURCES[kind]['url'] or entry['http_status']!=200:raise ValueError('Successful exact official release required')
        sources[kind]={'raw':checked(entry['original'],read,private=True),'acquired_at':entry['acquired_at']}
    output=model.compile_releases(sources,inputs['generated_at'])
    proof=independent.verify({kind:entry['raw'] for kind,entry in sources.items()},output)
    if proof['reconciliation_issues']:raise ValueError('Original measurements failed reconciliation')
    for ref in inputs['predecessors'].values():checked(ref,read,private=True)
    output.update(source='ICI official weekly releases: MMF assets and combined mutual-fund flows / ETF net issuance',
        method='Every published table cell retained with its unit, observation date and source coordinates; exact-decimal reconciliations and independent rational checks.',
        duration_s=None,original_arithmetic_checks=proof,predecessors=inputs['predecessors'],
        portfolio_consequences={'status':'UNAVAILABLE','target_weights':None},
        dependency_graph={'independent_votes':0,'roots':['ICI:mmf','ICI:combined_flows']})
    return output,proof


def binding(packet,read):
    ref=packet.get('replay',{});key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Exact run identity required')
    raw=read(key);manifest=strict(raw)
    if key!=PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract')!='ici-replay.v1':raise ValueError('Native manifest differs')
    output=checked(manifest['output'],read,'outputs')
    if encode(output)!=encode({k:v for k,v in packet.items() if k!='replay'}) or ref.get('output_sha256')!=manifest['output']['sha256'] or manifest['generated_at']!=output['generated_at']:
        raise ValueError('Published head differs from retained output')
    return manifest


def replay(packet,read):
    manifest=binding(packet,read);paths=compilers()
    if set(manifest['compilers'])!=set(paths):raise ValueError('Complete compiler closure required')
    for name,path in paths.items():
        if checked(manifest['compilers'][name],read,'compilers','py')!=path.read_bytes():raise ValueError('Compiler bytes differ')
    inputs=checked(manifest['input'],read);output,proof=compile_inputs(inputs,read)
    if encode(output)!=encode(checked(manifest['output'],read,'outputs')) or encode(proof)!=encode(checked(manifest['proof'],read,'proofs')):
        raise ValueError('Complete original reconstruction differs')
    return output


def seal(client,bucket,inputs):
    read=reader(client,bucket);output,proof=compile_inputs(inputs,read)
    put=lambda value,category:retain(client,bucket,encode(value),category)
    manifest={'contract':'ici-replay.v1','generated_at':inputs['generated_at'],'input':put(inputs,'inputs'),
        'output':put(output,'outputs'),'proof':put(proof,'proofs'),
        'compilers':{name:retain(client,bucket,path.read_bytes(),'compilers','py') for name,path in compilers().items()}}
    ref=put(manifest,'runs');packet={**output,'replay':{'manifest_key':ref['key'],'output_sha256':manifest['output']['sha256']}}
    replay(packet,read)
    return packet


def head(client,bucket):
    try:
        obj=client.get_object(Bucket=bucket,Key=CURRENT)
        return bounded(obj['Body']),obj['ETag']
    except Exception as exc:
        if missing(exc):return None,None
        raise


def predecessors(client,bucket):
    raw,_=head(client,bucket);out={}
    if raw is not None:
        out['previous_packet']=retain(client,bucket,raw,private=True)
        try:old=strict(raw)
        except (ValueError,UnicodeDecodeError):old={}
        if isinstance(old,dict) and old.get('contract')=='ici-research.v1':
            binding(old,reader(client,bucket))
            # Carry original legacy references, without rewriting/truncating histories.
            return {**old.get('predecessors',{}),'previous_packet':out['previous_packet']}
    for name,key in (('legacy_mmf','data/history/ici-mmf.json'),('legacy_flows','data/history/ici-flows.json')):
        try:body=bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
        except Exception as exc:
            if missing(exc):continue
            raise
        out[name]=retain(client,bucket,body,private=True)
    return out


def publish(client,bucket,packet):
    replay(packet,reader(client,bucket))  # Publication cannot bypass retained originals or authority checks.
    stamp=model.stamp(packet['generated_at']);raw,etag=head(client,bucket)
    if raw is not None:
        try:old=strict(raw)
        except (ValueError,UnicodeDecodeError):old={}
        if not isinstance(old,dict):old={}
        if old.get('generated_at'):
            prior=model.stamp(old['generated_at'])
            if prior>stamp:return False
            if prior==stamp:
                if encode(old)!=encode(packet):raise ValueError('Conflicting same-clock head')
                return True
        if old.get('contract')=='ici-research.v1':
            binding(old,reader(client,bucket))
            for name,before in old['sources'].items():
                after=packet['sources'][name]
                if before['release_date']>after['release_date'] or max(before['observation_dates'])>max(after['observation_dates']) or model.stamp(before['acquired_at'])>model.stamp(after['acquired_at']):return False
        retain(client,bucket,raw,private=True)
    try:client.put_object(Bucket=bucket,Key=CURRENT,Body=encode(packet),ContentType='application/json',CacheControl='no-store',
        **({'IfMatch':etag} if etag is not None else {'IfNoneMatch':'*'}))
    except Exception as exc:
        if conflict(exc):raise RuntimeError('Concurrent head changed; immutable run retained') from exc
        raise
    actual,_=head(client,bucket)
    if actual!=encode(packet):raise ValueError('Public head changed before complete readback')
    return True


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):return None


def acquire(kind):
    url=model.SOURCES[kind]['url']
    request=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0 JustHodl-source-research/1.0','Accept-Encoding':'identity'})
    try:response=urllib.request.build_opener(NoRedirect()).open(request,timeout=30)
    except urllib.error.HTTPError as exc:response=exc
    with response:
        if response.geturl()!=url:raise ValueError('Unreviewed source redirect')
        headers={k:response.headers.get(k) for k in ('Content-Type','Content-Length','ETag','Last-Modified')}
        raw=bounded(response)
        if headers['Content-Length'] is not None and int(headers['Content-Length'])!=len(raw):raise ValueError('Incomplete HTTP response')
    return raw,{'url':url,'http_status':response.status,'acquired_at':now(),'response_headers':headers}


def run(client,bucket,request_id):
    qualified()
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Stable native request identity required')
    key=PRIVATE+'requests/'+sha(('native:'+request_id).encode())+'.json'
    progress={'status':'claimed','started_at':now(),'provider_request_attempts':0,'sources':{}}
    def journal(claim=False):
        raw=encode(progress)
        client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
        if bounded(client.get_object(Bucket=bucket,Key=key)['Body'])!=raw:raise ValueError('Native request journal differs')
    try:journal(True)
    except Exception as exc:
        if conflict(exc):return {'published':False,'status':'duplicate_request','provider_requests':0}
        raise
    try:
        previous=predecessors(client,bucket)
        for kind in model.SOURCES:
            progress.update(status='acquisition_attempted',provider_request_attempts=progress['provider_request_attempts']+1);journal()
            raw,source=acquire(kind)
            source['original']=retain(client,bucket,raw,private=True)
            progress['sources'][kind]=source;journal()
            if source['http_status']!=200:raise ValueError('Official release unavailable; whole response retained')
        inputs={'contract':'ici-inputs.v1','generated_at':now(),'sources':progress['sources'],'predecessors':previous}
        packet=seal(client,bucket,inputs);published=publish(client,bucket,packet)
        result={'published':published,'generated_at':packet['generated_at'],'replay':packet['replay'],
            'original_arithmetic_checks':packet['original_arithmetic_checks'],'provider_requests':progress['provider_request_attempts'],
            'paid_ai_calls':0,'account_reads':0,'notifications_sent':0,'history_writes':0}
        progress.update(status='complete',result=result);journal();return result
    except Exception as exc:
        progress.update(status='failed',error_type=type(exc).__name__);journal();raise
