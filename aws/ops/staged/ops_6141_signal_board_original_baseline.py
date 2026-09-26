"""Preserve Signal Board code, every registered public sidecar and consumer.

No new provider request, invocation, account access, public write, notification
or schedule change. The baseline is evidence for a future native migration.
"""
from pathlib import Path
from datetime import datetime, timezone
import ast, base64, hashlib, io, json, re, subprocess, sys, urllib.request, urllib.error, zipfile
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import bounded, verified_alias, schedule_evidence
from release_package_evidence import shared_imports
import retained_access_evidence as access
import liquidity_agent_triggers as triggers

BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-signal-board'
PRIVATE = 'audit-private/20260909-originals/signal-board-research/'
REQUEST = 'chatgpt-signal-board-original-baseline-6141'
sha = lambda raw: hashlib.sha256(raw).hexdigest()
encoded = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
now = lambda: datetime.now(timezone.utc).isoformat()
STATUS = PRIVATE + 'requests/' + sha(REQUEST.encode()) + '.json'
PRIVATE_INPUTS = frozenset(('data/pm-decision.json','data/sizing.json'))



def retain(s3, raw, allow_empty=False):
    if not isinstance(raw, bytes) or not (0 if allow_empty else 1) <= len(raw) <= 64 * 1024 * 1024:
        raise ValueError('Complete bounded predecessor required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'sha256': sha(raw), 'bytes': len(raw)}
    try:
        s3.put_object(Bucket=BUCKET, Key=ref['key'], Body=raw, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    assert bounded(s3.get_object(Bucket=BUCKET, Key=ref['key'])['Body']) == raw
    return ref


def journal(s3, value, claim=False):
    raw = encoded(value)
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=raw, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert bounded(s3.get_object(Bucket=BUCKET, Key=STATUS)['Body']) == raw


def feeds(body):
    """Read the literal source registry without importing or executing the engine."""
    tree=ast.parse(body);nodes=[n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='FEEDS' for t in n.targets)]
    if len(nodes)!=1 or not isinstance(nodes[0],ast.List) or not 1<=len(nodes[0].elts)<=300:raise ValueError('Whole explicit source registry required')
    result=[];names=set()
    for item in nodes[0].elts:
        if not isinstance(item,ast.Tuple) or len(item.elts)!=4:raise ValueError('Complete feed tuple required')
        name,category,key=[ast.literal_eval(n) for n in item.elts[:3]];fn=item.elts[3]
        if (not all(isinstance(v,str) and v for v in (name,category,key)) or name in names or
            not re.fullmatch(r'(?:data|screener)/[A-Za-z0-9_-]+[.]json',key) or not isinstance(fn,ast.Name)):
            raise ValueError('Reviewed literal feed identity required')
        names.add(name);result.append({'engine':name,'category':category,'source_key':key,'normalizer':fn.id,
            'capture_scope':'excluded_private_account_input' if key in PRIVATE_INPUTS else 'anonymous_public_sidecar_only'})
    return result


def summarize(raw):
    result={'status':'retained_derived_packet_only','original_provider_verified':False,'forecast_qualified':False,'sizing_qualified':False}
    try:packet=json.loads(raw)
    except (ValueError,UnicodeError):return {**result,'status':'whole_unparsed_response'}
    if not isinstance(packet,dict):return {**result,'status':'whole_nonobject_response'}
    result.update(fields=sorted(packet),contract=packet.get('contract'),generated_at=packet.get('generated_at'),
        permission_declarations={k:packet.get(k) for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')},
        replay_declared=isinstance(packet.get('replay'),dict))
    rows=packet.get('engines')
    if isinstance(rows,list):
        result['engine_rows']=len(rows)
        result['numeric_signal_rows']=sum(isinstance(r,dict) and type(r.get('signal')) in (int,float) for r in rows)
        result['missing_signal_rows']=sum(isinstance(r,dict) and r.get('signal') is None for r in rows)
    return result


def public_response(key,transport=None):
    if key in PRIVATE_INPUTS or not re.fullmatch(r'(?:data|screener)/[A-Za-z0-9_-]+[.]json',key):raise ValueError('Reviewed anonymous public input required')
    request=urllib.request.Request('https://justhodl.ai/'+key+'?exact=1&nogen=1',headers={'User-Agent':'justhodl-verify-release/1.0','Cache-Control':'no-cache'})
    opener=transport or urllib.request.build_opener(access.NoRedirect()).open
    try:response=opener(request,timeout=30)
    except urllib.error.HTTPError as exc:response=exc
    status=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in ('content-type','content-length','cache-control','date','last-modified','etag')}
    raw=bounded(response,64*1024*1024)
    return raw,status,headers


def package_inventory(raw, expected):
    """Preserve every ordered ZIP member; duplicate names are evidence, not proof."""
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        infos=archive.infolist()
        if len(infos)>20000 or sum(x.file_size for x in infos)>128*1024*1024:raise ValueError('Expanded package exceeds bound')
        members=[{'index':i,'name':info.filename,'sha256':sha(archive.read(info)),'bytes':info.file_size}
            for i,info in enumerate(infos)]
    by_name={}
    for member in members:by_name.setdefault(member['name'],[]).append(member)
    differences={}
    for name,body in expected.items():
        variants=by_name.get(name,[]);tracked={'sha256':sha(body),'bytes':len(body)}
        if len(variants)!=1 or any(v['sha256']!=tracked['sha256'] or v['bytes']!=tracked['bytes'] for v in variants):
            differences[name]={'status':'missing_from_package' if not variants else 'duplicate_packaged_members' if len(variants)>1 else 'different_bytes',
                'tracked':tracked,'deployed_members':variants}
    return {'source_files_checked':len(expected),'code_matches_repository':not differences,
        'source_differences':differences,'complete_zip_members':members,
        'duplicate_member_names':sorted(name for name,items in by_name.items() if len(items)>1),
        'additional_packaged_files':sorted(set(by_name)-set(expected)),
        'runtime_member_resolution_verified':False,'release_verification':False,'baseline_only':True}


def runtime(lam,s3,events,scheduler,function):
    """Read the actual predecessor without forcing a deployment to erase drift."""
    source=ROOT/'aws/lambdas'/function/'source';deployed=lam.get_function(FunctionName=function);cfg=deployed['Configuration']
    if cfg['State']!='Active' or cfg['LastUpdateStatus']!='Successful':raise ValueError('Function is not ready')
    raw=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=40))
    if base64.b64encode(hashlib.sha256(raw).digest()).decode()!=cfg['CodeSha256']:raise ValueError('Actual ZIP hash differs')
    paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p.read_bytes() for p in paths}
    expected.update({p.name:p.read_bytes() for p in shared_imports(ROOT,paths) if not (source/p.name).exists()})
    if not expected:raise ValueError('Tracked source is required')
    inventory=package_inventory(raw,expected)
    try:
        receipt_raw=bounded(s3.get_object(Bucket=BUCKET,Key='data/ops/releases/'+function+'.json')['Body'])
        receipt=json.loads(receipt_raw)
        receipt_status={'status':'matched' if receipt.get('code_sha256')==cfg['CodeSha256'] else 'different_code_hash',
            'commit':receipt.get('commit'),'reported_code_sha256':receipt.get('code_sha256'),'whole_receipt':retain(s3,receipt_raw)}
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('NoSuchKey','404'):raise
        receipt_status={'status':'missing_predecessor_receipt'}
    path=source.parent/'config.json';conf=json.loads(path.read_bytes()) if path.exists() else {}
    alias=verified_alias(lam,function,cfg,conf)
    result={**inventory,'code_sha256':cfg['CodeSha256'],'whole_zip':retain(s3,raw),
        'handler_bytes':[m['bytes'] for m in inventory['complete_zip_members'] if m['name']=='lambda_function.py'],
        'timeout':cfg['Timeout'],'memory_mb':cfg['MemorySize'],'receipt':receipt_status,
        'schedules':schedule_evidence(events,scheduler,function,cfg['FunctionArn'],conf,alias),
        'function_name':cfg['FunctionName'],'runtime':cfg['Runtime'],'handler':cfg['Handler'],
        'architectures':cfg['Architectures'],'role':cfg['Role'],'ephemeral_storage_mb':cfg['EphemeralStorage']['Size']}
    if alias:result['active_alias']=alias
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_signal_board_original_baseline.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    registry=feeds((ROOT/'aws/lambdas'/FUNCTION/'source/lambda_function.py').read_text(encoding='utf-8'))
    with report('ops_6141_signal_board_original_baseline') as r:
        progress={'contract':'signal-board-original-baseline.v1','request_id':REQUEST,'status':'claimed','started_at':now(),
            'registry':registry,'captures':{},'repo_predecessors':{},'public_packet_requests':0,'snapshot_atomic':False}
        journal(s3,progress,True)
        try:
            before=runtime(lam,s3,events,scheduler,FUNCTION)
            progress['native_predecessor']={'runtime':before,'whole_zip':before['whole_zip']}
            arn=lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn']
            discovery=triggers.collect(lam,scheduler,s3,arn,BUCKET);progress['trigger_inventory']=discovery
            aliases=[]
            for page in lam.get_paginator('list_aliases').paginate(FunctionName=FUNCTION):
                aliases.extend({'Name':a['Name'],'FunctionVersion':a['FunctionVersion']} for a in page.get('Aliases',[]))
                if len(aliases)>100:raise ValueError('Alias inventory bound exceeded')
            qualifiers={a['Name'] for a in aliases}
            for page in lam.get_paginator('list_versions_by_function').paginate(FunctionName=FUNCTION):
                qualifiers.update(v['Version'] for v in page.get('Versions',[]) if v['Version']!='$LATEST')
                if len(qualifiers)>1000:raise ValueError('Version inventory bound exceeded')
            qualified={r['target_arn'].split(arn+':',1)[1] for r in discovery['matching_schedules'] if r['target_arn'].startswith(arn+':')}
            for qualifier in sorted(qualifiers):
                for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=arn+':'+qualifier):
                    if page.get('RuleNames'):qualified.add(qualifier)
            progress['scheduled_qualified_packages']={}
            for qualifier in sorted(qualified):
                function=lam.get_function(FunctionName=FUNCTION,Qualifier=qualifier)
                body=bounded(urllib.request.urlopen(function['Code']['Location'],timeout=40));digest=base64.b64encode(hashlib.sha256(body).digest()).decode()
                if digest!=function['Configuration']['CodeSha256']:raise ValueError('Qualified code bytes differ')
                progress['scheduled_qualified_packages'][qualifier]={'code_sha256':digest,'whole_zip':retain(s3,body)}
            progress['aliases']=aliases;journal(s3,progress)
            for key in ('data/signal-board.json','data/signal-board.json.prev'):
                try:response=s3.get_object(Bucket=BUCKET,Key=key)
                except Exception as exc:
                    if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('NoSuchKey','404'):raise
                    progress['captures'][key]={'status':'missing','source_key':key};continue
                raw=bounded(response['Body']);progress['captures'][key]={'source_key':key,'original':retain(s3,raw),
                    'etag':response['ETag'],'captured_at':now(),'summary':summarize(raw)}
            for key in sorted({row['source_key'] for row in registry}):
                if key in PRIVATE_INPUTS:
                    progress['captures'][key]={'status':'excluded_private_account_input','source_key':key,'requested':False};continue
                progress['captures'][key]={'status':'attempt_recorded','source_key':key,'requested_at':now()}
                progress['public_packet_requests']+=1;journal(s3,progress)
                try:raw,status,headers=public_response(key)
                except Exception as exc:
                    progress['captures'][key].update(status='transport_or_size_unavailable',error_type=type(exc).__name__,received_at=now())
                else:
                    progress['captures'][key].update(status='public_sidecar_retained' if status==200 else 'whole_http_error_retained',
                        http_status=status,headers=headers,original=retain(s3,raw,allow_empty=True),received_at=now(),summary=summarize(raw))
                journal(s3,progress)
            paths=set(subprocess.check_output(['git','grep','-l','-F','signal-board.json','--','aws/lambdas/*/source/*.py','aws/shared/*.py','*.html','*.js','assets/*.js'],cwd=ROOT,text=True).splitlines())
            paths.update(subprocess.check_output(['git','ls-files','--','aws/lambdas/'+FUNCTION+'/source'],cwd=ROOT,text=True).splitlines())
            paths.update(('aws/lambdas/'+FUNCTION+'/config.json','signal-board.html','jh-fifx-board.js','jh-fifx-research.js'))
            for path in sorted(paths):progress['repo_predecessors'][path]=retain(s3,(ROOT/path).read_bytes(),allow_empty=True)
            progress.update(status='retained',registered_feeds=len(registry),unique_source_keys=len({v['source_key'] for v in registry}),
                private_inputs_excluded=sorted(PRIVATE_INPUTS),original_provider_verified=False,independent_votes_qualified=False)
            journal(s3,progress);manifest=retain(s3,encoded(progress))
            protected={STATUS,manifest['key'],before['whole_zip']['key']}
            if before['receipt'].get('whole_receipt'):protected.add(before['receipt']['whole_receipt']['key'])
            protected.update(v['whole_zip']['key'] for v in progress['scheduled_qualified_packages'].values())
            protected.update(v['key'] for v in progress['repo_predecessors'].values())
            protected.update(v['original']['key'] for v in progress['captures'].values() if 'original' in v)
            outcomes=[access.check(key) for key in sorted(protected)]
            access_ref=retain(s3,encoded({'contract':'signal-board-baseline-access.v1','outcomes':outcomes}));outcomes.append(access.check(access_ref['key']))
            privacy=access.summarize(outcomes);r.kv(access_evidence=access_ref,**privacy)
            assert privacy['all_denied'],'Inspect anonymous-access outcomes before changing the producer'
            assert runtime(lam,s3,events,scheduler,FUNCTION)==before
            assert triggers.collect(lam,scheduler,s3,arn,BUCKET)==discovery
            for qualifier,row in progress['scheduled_qualified_packages'].items():
                assert lam.get_function_configuration(FunctionName=FUNCTION,Qualifier=qualifier)['CodeSha256']==row['code_sha256']
            journal(s3,{**progress,'status':'complete','completed_at':now(),'manifest':manifest,'privacy':privacy})
            r.kv(manifest=manifest,native_predecessor=progress['native_predecessor'],registry=registry,captures=progress['captures'],
                repo_predecessors=progress['repo_predecessors'],trigger_inventory=discovery,aliases=aliases,
                scheduled_qualified_packages=progress['scheduled_qualified_packages'],registered_feeds=len(registry),
                unique_source_keys=progress['unique_source_keys'],private_inputs_excluded=sorted(PRIVATE_INPUTS),
                public_packet_requests=progress['public_packet_requests'],native_package_unchanged=True,
                provider_requests=0,producer_invocations=0,consumer_invocations=0,public_writes=0,
                private_account_reads=0,paid_ai_calls=0,notifications_sent=0,signal_writes=0,schedules_changed=0,
                forecast_qualified=False,sizing_qualified=False,original_provider_verified=False,independent_votes_qualified=False)
        except Exception as exc:
            journal(s3,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
