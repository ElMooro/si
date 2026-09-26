"""Retained original histories, deterministic replay and conditional publication.

Full source populations and intermediate histories stay behind runner IAM.
Only derived views, reviewed compiler bytes and digest manifests are public.
"""
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
import hashlib,re,time
import sentinel_model as model
import sentinel_sources as sources

MAX=sources.MAX
PRIVATE=sources.PRIVATE
sha=lambda raw:hashlib.sha256(raw).hexdigest()
strict=sources.strict
bounded=sources.bounded
FILES=('lambda_function.py','sentinel_model.py','sentinel_sources.py','sentinel_store.py')


def error(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',''))
def conflict(exc):return error(exc) in ('409','412','ConditionalRequestConflict','PreconditionFailed')
def same(left,right):return model.encoded(left)==model.encoded(right)


def allowed(key):
    return isinstance(key,str) and (key in (model.CURRENT,'data/indicator-bus.json') or bool(re.fullmatch(
        r'(?:audit-private/20260909-originals/us10y-sentinel-research/(?:originals/[a-f0-9]{64}/[a-f0-9]{64}\.json|'
        r'(?:inputs|outputs|snapshots)/[a-f0-9]{64}\.json)|'
        r'data/us10y-sentinel-research/(?:runs|views|compilers)/[a-f0-9]{64}\.(?:json|py))',key)))


def reader(client,bucket):
    def read(key):
        if not allowed(key):raise ValueError('Unapproved Sentinel evidence path')
        return bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
    return read


def retain_bytes(client,bucket,raw,category,public=False):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Whole bounded artifact required')
    if (category not in ('runs','views','compilers') if public else category not in ('inputs','outputs','snapshots')):
        raise ValueError('Unapproved artifact category')
    key=(model.PREFIX if public else PRIVATE)+category+'/'+sha(raw)+('.py' if category=='compilers' else '.json')
    try:
        client.put_object(Bucket=bucket,Key=key,Body=raw,IfNoneMatch='*',
            ContentType='text/plain' if category=='compilers' else 'application/json',
            CacheControl='public, max-age=31536000, immutable' if public else 'no-store')
    except Exception as exc:
        if not conflict(exc):raise
    if reader(client,bucket)(key)!=raw:raise ValueError('Retained artifact readback differs')
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}


def checked_bytes(ref,category,read,public=False):
    prefix=model.PREFIX if public else PRIVATE
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256','')))
        or ref.get('key')!=prefix+category+'/'+ref['sha256']+'.json'
        or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):raise ValueError('Exact artifact reference required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Whole artifact bytes differ')
    return raw


def checked(ref,category,read,public=False):return strict(checked_bytes(ref,category,read,public))


def compact(full):
    out={k:deepcopy(v) for k,v in full.items() if k not in ('histories','original_sources')}
    out['original_sources']={sid:{k:deepcopy(row[k]) for k in ('original_rows','numeric_rows','missing_rows',
        'unit','frequency','first_observation','last_observation','sources','vintage_basis')}
        for sid,row in full['original_sources'].items()}
    pairs=out['correlation_trace'].pop('observations')
    out['correlation_trace'].update(retained_matched_pairs=len(pairs),
        start_date=pairs[0]['start_date'] if pairs else None,end_date=pairs[-1]['end_date'] if pairs else None,
        raw_pair_access='runner_iam')
    out['source_access']={'scope':'Complete source responses and histories retained privately; runner IAM required for full replay.',
        'public_redistribution_qualified':False,'producer_replay_performed':True,'independent_acceptance_in_this_packet':False,
        'licensing_note':'FRED SP500 is a price index excluding dividends; unrestricted redistribution of its history is not established.'}
    return out


def compile_output(inputs,read):
    if inputs.get('contract')!='us10y-sentinel-inputs.v1':raise ValueError('Complete native input contract required')
    stamp=inputs['generated_at'];histories,details=sources.restore(inputs['sources'],stamp,read)
    predecessor=checked(inputs['predecessor'],'snapshots',read)
    if not isinstance(predecessor,dict):raise ValueError('Complete predecessor object required')
    status='missing'
    if inputs['bus'] is not None:
        raw=checked_bytes(inputs['bus'],'snapshots',read)
        try:
            bus=strict(raw);row=bus['indicators']['US10Y']
            if not isinstance(row,dict):raise ValueError('Invalid bus observation')
            status='retained_unqualified_context'
        except (ValueError,KeyError,TypeError):status='malformed_retained_context'
    if status!='retained_unqualified_context':row={}
    context={'marker':'ops4217','us10y_bus':row.get('v'),'asof':row.get('asof'),'src':row.get('src'),
        'status':status,
        'independent_votes':0,'source_independence_verified':False,
        'note':'Reported bus context; source ancestry is not independently verified and cannot add another vote'}
    return model.build(histories,stamp,predecessor.get('tier'),context,details,inputs.get('acquisition_elapsed_s'))


def replay(ref,read):
    key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Exact Sentinel run required')
    raw=read(key);manifest=strict(raw)
    if key!=model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract')!='us10y-sentinel-replay.v1':raise ValueError('Manifest identity differs')
    if not same(ref,{'manifest_key':key,'output_sha256':manifest['output_sha256'],'view_sha256':manifest['view']['sha256']}):
        raise ValueError('Complete replay reference differs')
    if set(manifest['compilers'])!=set(FILES):raise ValueError('Complete reviewed compiler closure required')
    for name in FILES:
        body=(Path(__file__).parent/name).read_bytes()
        expected={'key':model.PREFIX+'compilers/'+sha(body)+'.py','sha256':sha(body),'bytes':len(body)}
        if not same(manifest['compilers'][name],expected) or read(expected['key'])!=body:raise ValueError('Matching reviewed source required: '+name)
    inputs=checked(manifest['input'],'inputs',read);full=compile_output(inputs,read);view=compact(full)
    if (full['generated_at']!=manifest['generated_at'] or model.digest(full)!=manifest['output_sha256']
        or not same(full,checked(manifest['output'],'outputs',read))
        or not same(view,checked(manifest['view'],'views',read,True))):raise ValueError('Whole original-source reconstruction differs')
    return full,view


def retain(client,bucket,inputs):
    read=reader(client,bucket);full=compile_output(inputs,read);view=compact(full)
    compilers={name:retain_bytes(client,bucket,(Path(__file__).parent/name).read_bytes(),'compilers',True) for name in FILES}
    manifest={'contract':'us10y-sentinel-replay.v1','generated_at':inputs['generated_at'],
        'input':retain_bytes(client,bucket,model.encoded(inputs),'inputs'),
        'output':retain_bytes(client,bucket,model.encoded(full),'outputs'),
        'view':retain_bytes(client,bucket,model.encoded(view),'views',True),
        'compilers':compilers,'output_sha256':model.digest(full)}
    ref=retain_bytes(client,bucket,model.encoded(manifest),'runs',True)
    replay_ref={'manifest_key':ref['key'],'output_sha256':manifest['output_sha256'],'view_sha256':manifest['view']['sha256']}
    rebuilt,projected=replay(replay_ref,read)
    if not same(full,rebuilt) or not same(view,projected):raise ValueError('Retained reconstruction differs')
    return {**view,'replay':replay_ref}


def binding(packet,read):
    ref=packet.get('replay') or {};key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Bound publication required')
    raw=read(key);manifest=strict(raw)
    if (key!=model.PREFIX+'runs/'+sha(raw)+'.json' or manifest.get('contract')!='us10y-sentinel-replay.v1'
        or not same(ref,{'manifest_key':key,'output_sha256':manifest['output_sha256'],'view_sha256':manifest['view']['sha256']})
        or packet.get('generated_at')!=manifest['generated_at']
        or not same({k:v for k,v in packet.items() if k!='replay'},checked(manifest['view'],'views',read,True))):
        raise ValueError('Publication differs from retained whole view')


def publish(client,bucket,packet):
    read=reader(client,bucket);binding(packet,read)
    flags=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','alert_sent','alert_eligible','point_in_time_backtest_qualified')
    if (packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in flags)
        or packet.get('call') is not None or not same(packet.get('decision'),{'verb':'WAIT','meaning':'abstain'})):
        raise ValueError('Unqualified Sentinel cannot acquire decision authority')
    stamp=model.clock(packet['generated_at'])
    obj=client.get_object(Bucket=bucket,Key=model.CURRENT);raw=bounded(obj['Body']);previous=strict(raw)
    if not isinstance(previous,dict):raise ValueError('Complete prior publication required')
    if previous.get('generated_at'):
        before=model.clock(previous['generated_at'])
        if before>stamp:return False
        if before==stamp:
            if not same(previous,packet):raise ValueError('Same-clock publication differs')
            return True
    if previous.get('contract')==model.CONTRACT:
        binding(previous,read)
        for sid in model.SERIES:
            for kind in ('definition','observations'):
                old=previous['original_sources'][sid]['sources'][kind]['acquired_at']
                new=packet['original_sources'][sid]['sources'][kind]['acquired_at']
                if model.clock(new)<model.clock(old):return False
    retain_bytes(client,bucket,raw,'snapshots')
    try:client.put_object(Bucket=bucket,Key=model.CURRENT,Body=model.encoded(packet),IfMatch=obj['ETag'],ContentType='application/json',CacheControl='no-store')
    except Exception as exc:
        if conflict(exc):return False
        raise
    live=strict(read(model.CURRENT))
    if not same(live,packet):
        if model.clock(live['generated_at'])>stamp and live.get('contract')==model.CONTRACT:binding(live,read)
        else:raise ValueError('Published head readback differs')
    return True


def run(client,bucket,key,fetch=None):
    started=time.monotonic()
    read=reader(client,bucket)
    predecessor=retain_bytes(client,bucket,read(model.CURRENT),'snapshots')
    try:bus=retain_bytes(client,bucket,read('data/indicator-bus.json'),'snapshots')
    except Exception as exc:
        if error(exc) not in ('404','NoSuchKey'):raise
        bus=None
    originals=sources.acquire(client,bucket,key,**({'fetch':fetch} if fetch is not None else {}))
    inputs={'contract':'us10y-sentinel-inputs.v1','generated_at':datetime.now(timezone.utc).isoformat(),
        'sources':originals,'predecessor':predecessor,'bus':bus,'acquisition_elapsed_s':round(time.monotonic()-started,3)}
    packet=retain(client,bucket,inputs);published=publish(client,bucket,packet)
    return {'contract':model.CONTRACT,'generated_at':packet['generated_at'],'published':published,
        'output_sha256':packet['replay']['output_sha256'],'calls_eligible':False,'sizing_eligible':False,
        'source_responses':6,'original_rows':sum(row['original_rows'] for row in packet['original_sources'].values())}
