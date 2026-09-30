"""Native original workbook retention, reproducible research and conditional head."""
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json,re,time,urllib.request,urllib.error
import term_premium_model as model
from term_premium_qualification import QUALIFIED

MAX=64*1024*1024
PRIVATE='audit-private/20260909-originals/term-premium-research/'
ROOT=Path(__file__).resolve().parent
sha=lambda raw:hashlib.sha256(raw).hexdigest()
now=lambda:datetime.now(timezone.utc).isoformat()
error=lambda exc:str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
conflict=lambda exc:error(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')


def strict(raw):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate research JSON key')
            result[key]=value
        return result
    def invalid(value):raise ValueError('Nonfinite research JSON number')
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
    model.encoded(value)
    return value


def same_json(left,right):
    # Keep boolean/integer/float types distinct throughout evidence checks.
    return model.encoded(left)==model.encoded(right)


def bounded(stream, *, expected_length=None):
    """Read every byte to EOF; one read may return only a transport fragment."""
    try:
        if expected_length is not None and (type(expected_length) is not int or not 0<expected_length<=MAX):
            raise ValueError('Complete declared artifact length required')
        chunks=[];size=0
        while True:
            requested=min(1024*1024,MAX+1-size)
            chunk=stream.read(requested)
            if type(chunk) is not bytes or len(chunk)>requested:
                raise ValueError('Exact binary response fragment required')
            if not chunk:break
            size+=len(chunk)
            if size>MAX:raise ValueError('Complete artifact exceeds reviewed byte bound')
            chunks.append(chunk)
        if not size or expected_length is not None and size!=expected_length:
            raise ValueError('Complete artifact differs from declared length')
        return b''.join(chunks)
    finally:
        stream.close()


def stored(response):
    """S3 always declares its body length; reject missing or partial evidence."""
    stream=response['Body'];length=response.get('ContentLength')
    if type(length) is not int or not 0<length<=MAX:
        stream.close()
        raise ValueError('Exact S3 ContentLength required')
    return bounded(stream,expected_length=length)


def qualified_arithmetic():
    for name,digest in QUALIFIED.items():
        if sha((ROOT/name).read_bytes())!=digest:raise ValueError('Accepted ACM compiler differs: '+name)


def compilers():
    return {name:ROOT/name for name in sorted(set(QUALIFIED)|{'term_premium_model.py','term_premium_store.py','term_premium_qualification.py'})}


def allowed(key):
    return key==model.CURRENT or isinstance(key,str) and bool(re.fullmatch(re.escape(model.PREFIX)+
        r'(?:runs|inputs|outputs|tables|views|sources|compilers|originals)/[a-f0-9]{64}\.(?:json|py|xls)',key))


def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('Unapproved ACM research artifact')
        return stored(client.get_object(Bucket=bucket,Key=key))
    return read


def retain_bytes(client,bucket,raw,category,extension='json',private=False):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Complete nonempty artifact required')
    if category not in ('runs','inputs','outputs','tables','views','sources','compilers','originals'):raise ValueError('Unreviewed artifact category')
    if extension not in ('json','py','xls'):raise ValueError('Unreviewed artifact type')
    key=PRIVATE+sha(raw)+'.bin' if private else model.PREFIX+category+'/'+sha(raw)+'.'+extension
    mime='application/octet-stream' if private else {'json':'application/json','py':'text/plain','xls':'application/vnd.ms-excel'}[extension]
    try:client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',ContentType=mime,
        CacheControl='no-store' if private else 'public, max-age=31536000, immutable')
    except Exception as exc:
        if not conflict(exc):raise
    if stored(client.get_object(Bucket=bucket,Key=key))!=raw:raise ValueError('Whole artifact readback differs')
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}


def checked(ref,category,read,extension='json'):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
        or ref.get('key')!=model.PREFIX+category+'/'+ref['sha256']+'.'+extension
        or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):raise ValueError('Exact whole artifact reference required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Artifact bytes differ')
    return strict(raw) if extension=='json' else raw


def compile_output(inputs,read):
    qualified_arithmetic()
    if inputs.get('contract')!='term-premium-inputs.v1':raise ValueError('Explicit native input contract required')
    source=checked(inputs['source'],'sources',read);raw=checked(inputs['workbook'],'originals',read,'xls')
    return model.build(raw,source,inputs['generated_at'],inputs['previous_watermarks'],inputs['predecessors'],inputs['acquisition'])


def binding(packet,read):
    ref=packet.get('replay',{});key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Exact native run required')
    raw=read(key);manifest=strict(raw)
    if key!=model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract')!='term-premium-replay.v1' or manifest.get('output_sha256')!=ref.get('output_sha256'):
        raise ValueError('Native manifest binding differs')
    view=checked(manifest['view'],'views',read)
    if not same_json(view,{k:v for k,v in packet.items() if k!='replay'}) or view['generated_at']!=manifest['generated_at']:
        raise ValueError('Published view differs from retained original')
    return manifest


def replay(packet,read):
    manifest=binding(packet,read);paths=compilers()
    if set(manifest['compilers'])!=set(paths):raise ValueError('Complete compiler closure required')
    for name,path in paths.items():
        archived=checked(manifest['compilers'][name],'compilers',read,'py')
        # This exact retained predecessor differs only in storage/HTTP transport
        # and replay identity handling. All mathematical compilers stay exact.
        # Never execute archived code or accept an arbitrary historical hash.
        compatible_store=(name=='term_premium_store.py' and sha(archived)==
            'def1ed96c885267e1ea17a5761fcbef94a8cfe62a6ce67dddca89f806d350828')
        if archived!=path.read_bytes() and not compatible_store:raise ValueError('Reviewed compiler bytes differ')
    inputs=checked(manifest['input'],'inputs',read);output=compile_output(inputs,read)
    if model.digest(output)!=manifest['output_sha256'] or not same_json(output,checked(manifest['output'],'outputs',read)):
        raise ValueError('Complete original workbook replay differs')
    if set(manifest['tables'])!=set(model.arithmetic.SHEETS):raise ValueError('Both complete table artifacts required')
    for name,table in output['tables'].items():
        if not same_json(checked(manifest['tables'][name],'tables',read),table):raise ValueError('Complete table bytes differ')
    if not same_json(model.compact(output,manifest['tables']),{k:v for k,v in packet.items() if k!='replay'}):raise ValueError('Compact projection differs')
    return output


def retain(client,bucket,inputs,output):
    put=lambda value,category:retain_bytes(client,bucket,model.encoded(value),category)
    tables={name:put(table,'tables') for name,table in output['tables'].items()}
    view=model.compact(output,tables)
    manifest={'contract':'term-premium-replay.v1','generated_at':inputs['generated_at'],
        'input':put(inputs,'inputs'),'output':put(output,'outputs'),'output_sha256':model.digest(output),'tables':tables,
        'view':put(view,'views'),'compilers':{name:retain_bytes(client,bucket,path.read_bytes(),'compilers','py') for name,path in compilers().items()}}
    ref=put(manifest,'runs');packet={**view,'replay':{'manifest_key':ref['key'],'output_sha256':manifest['output_sha256']}}
    if not same_json(replay(packet,reader(client,bucket)),output):raise ValueError('Fresh retained reconstruction differs')
    return packet


def previous_state(client,bucket):
    raw=stored(client.get_object(Bucket=bucket,Key=model.CURRENT))
    ref=retain_bytes(client,bucket,raw,'sources',private=True)
    try:packet=strict(raw)
    except (ValueError,UnicodeDecodeError):packet={}
    if not isinstance(packet,dict):packet={}
    if packet.get('contract')==model.CONTRACT:
        manifest=binding(packet,reader(client,bucket))
        return packet['predecessors'],model.continuity(packet),checked(manifest['input'],'inputs',reader(client,bucket))
    archive=stored(client.get_object(Bucket=bucket,Key='data/history/acm-term-premium.json'))
    return {'packet':ref,'parsed_archive':retain_bytes(client,bucket,archive,'sources',private=True)},None,None


def publish(client,bucket,packet):
    if (packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in model.arithmetic.AUTHORITY)
        or packet.get('signals')!=[] or packet.get('call') is not None or packet.get('decision')!={'verb':'WAIT','meaning':'abstain'}
        or packet.get('portfolio_consequences')!={'status':'UNAVAILABLE','target_weights':None}
        or packet.get('dependency_graph',{}).get('independent_votes')!=0
        or packet.get('view',{}).get('contract')!='term-premium-summary.v1' or len(packet.get('series',{}))!=60):
        raise ValueError('Complete research-only publication required')
    if any(row.get(k) is not False for row in packet['series'].values() for k in model.arithmetic.AUTHORITY):
        raise ValueError('Series acquired investment authority')
    binding(packet,reader(client,bucket));stamp=model.clock(packet['generated_at']);marks=model.continuity(packet)
    for _ in range(4):
        obj=client.get_object(Bucket=bucket,Key=model.CURRENT);raw=stored(obj)
        try:old=strict(raw)
        except (ValueError,UnicodeDecodeError):old={}
        if not isinstance(old,dict):old={}
        if old.get('generated_at'):
            prior=model.clock(old['generated_at'])
            if prior>stamp:return False
            if prior==stamp:
                if not same_json(old,packet):raise ValueError('Conflicting same-clock packet')
                return True
        if old.get('contract')==model.CONTRACT:
            previous=model.continuity(old)
            if model.clock(previous['acquired_at'])>model.clock(marks['acquired_at']):return False
            for name,before in previous['observations'].items():
                if before and (not marks['observations'][name] or before>marks['observations'][name]):return False
            # A concurrent publication may have advanced while this run compiled.
            for row in packet['series'].values():
                if row['current'] is not None and (model.clock(packet['source']['acquired_at'])<model.clock(previous['acquired_at'])
                    or previous['observations'][row['table']] and row['current']['observation_date']<previous['observations'][row['table']]):return False
        retain_bytes(client,bucket,raw,'sources',private=True)
        try:
            client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),ContentType='application/json',CacheControl='no-store',IfMatch=obj['ETag'])
            live=strict(reader(client,bucket)(model.CURRENT))
            if not same_json(live,packet) and model.clock(live['generated_at'])<=stamp:raise ValueError('Published head readback differs')
            return True
        except Exception as exc:
            if not conflict(exc):raise
    raise RuntimeError('Publication conflict ceiling; whole immutable run retained')


def acquire():
    request=urllib.request.Request(model.arithmetic.URL,headers={'User-Agent':'Mozilla/5.0 JustHodl-source-research/1.0'})
    try:response=urllib.request.urlopen(request,timeout=35)
    except urllib.error.HTTPError as exc:
        exc.close()
        raise
    try:
        if response.status!=200 or response.headers.get('Content-Range') is not None:
            raise ValueError('Complete HTTP 200 workbook response required')
        if response.geturl()!=model.arithmetic.URL:raise ValueError('Unreviewed source redirect')
        if response.headers.get('Content-Encoding','identity').strip().lower()!='identity':
            raise ValueError('Unreviewed encoded workbook response')
        lengths=response.headers.get_all('Content-Length',[])
        if len(lengths)>1 or lengths and not re.fullmatch('[0-9]+',lengths[0].strip()):
            raise ValueError('Unambiguous HTTP Content-Length required')
        expected=int(lengths[0].strip()) if lengths else None
        headers={name:response.headers.get(name) for name in ('Content-Type','ETag','Last-Modified')}
    except Exception:
        response.close()
        raise
    raw=bounded(response,expected_length=expected)
    return raw,{'source_url':model.arithmetic.URL,'bytes':len(raw),'sha256':sha(raw),'acquired_at':now(),'response_headers':headers}


def run(client,bucket,request_id):
    qualified_arithmetic()
    if not isinstance(request_id,str) or not 1<=len(request_id)<=200:raise ValueError('Native request identity required')
    key=PRIVATE+'requests/'+sha(('native:'+request_id).encode())+'.json'
    started=time.monotonic();phase_started=started
    progress={'status':'claimed','started_at':now(),'provider_request_attempts':0,'stage':'claimed','stage_timings':[]}
    def journal(claim=False):
        raw=model.encoded(progress);client.put_object(Bucket=bucket,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
        if stored(client.get_object(Bucket=bucket,Key=key))!=raw:raise ValueError('Native claim readback differs')
    def stage(name):
        nonlocal phase_started
        current=time.monotonic()
        progress['stage_timings'].append({'stage':progress['stage'],'elapsed_s':round(current-phase_started,3)})
        phase_started=current;progress.update(stage=name,stage_started_at=now(),elapsed_s=round(current-started,3));journal()
    journal(True)
    try:
        stage('capture_previous')
        predecessors,previous,fallback=previous_state(client,bucket)
        progress.update(status='acquisition_attempted',provider_request_attempts=1);stage('acquire_workbook')
        acquisition={'status':'acquired'}
        try:
            raw,source=acquire();progress['acquired_original']=retain_bytes(client,bucket,raw,'originals',private=True);stage('validate_workbook')
            model.arithmetic.build(raw,source,now())  # Reject schema/identity drift before admitting a public original.
            stage('retain_workbook')
            original=retain_bytes(client,bucket,raw,'originals','xls');receipt=retain_bytes(client,bucket,model.encoded(source),'sources')
        except Exception as exc:
            acquisition={'status':'failed','error_type':type(exc).__name__}
            progress.update(status='acquisition_failed',acquisition=acquisition);journal()
            if fallback is None:raise
            original,receipt=fallback['workbook'],fallback['source']
        inputs={'contract':'term-premium-inputs.v1','generated_at':now(),'workbook':original,'source':receipt,
            'previous_watermarks':previous,'predecessors':predecessors,'acquisition':acquisition}
        stage('compile_and_verify')
        output=compile_output(inputs,reader(client,bucket))
        stage('retain_and_replay')
        packet=retain(client,bucket,inputs,output)
        stage('publish_conditionally')
        published=publish(client,bucket,packet)
        result={'published':published,'generated_at':packet['generated_at'],'quality':packet['quality'],'replay':packet['replay'],
            'current_bytes':len(model.encoded(packet)),'original_cells':packet['original_arithmetic_checks']['original_cells_including_headers'],
            'provider_requests':1,'paid_ai_calls':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0}
        progress.update(status='complete',result=result);stage('complete');return result
    except Exception as exc:
        progress.update(status='failed',error_type=type(exc).__name__,failed_stage=progress['stage'],elapsed_s=round(time.monotonic()-started,3));journal();raise
