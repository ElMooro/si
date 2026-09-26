"""Preserve the complete FI/FX migration predecessor and six canonical roots.

No new provider request, invocation, account access, public write, notification
or schedule change. The baseline is evidence for a future native migration.
"""
from pathlib import Path
from datetime import datetime, timezone
import base64, gzip, hashlib, io, json, re, subprocess, sys, urllib.request, zipfile
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import bounded, verified_alias, schedule_evidence
from release_package_evidence import shared_imports
import canonical_fred_replay as canonical
import retained_access_evidence as access
import liquidity_agent_triggers as triggers

BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-fifx-vol-migration'
PRIVATE = 'audit-private/20260909-originals/fifx-vol-research/'
REQUEST = 'chatgpt-fifx-original-baseline-6136'
sha = lambda raw: hashlib.sha256(raw).hexdigest()
encoded = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
now = lambda: datetime.now(timezone.utc).isoformat()
STATUS = PRIVATE + 'requests/' + sha(REQUEST.encode()) + '.json'
INPUTS = ('data/fifx-vol.json', 'data/fifx-vol-history.json', 'data/bond-vol.json', 'data/report-measurements.json')
SERIES = ('VIXCLS', 'DGS10', 'DEXUSEU', 'DEXJPUS', 'DEXUSUK', 'DTWEXBGS')
QUOTE_SYMBOLS = ('^MOVE', '^KS11', '^HSI', '^N225', '^GDAXI', '^FTSE', '^FCHI', '000001.SS', '^BSESN', '^BVSP', '^AXJO', '^VHSI')


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


def summarize(raw):
    """Observe the predecessor without promoting parsed rows to source evidence."""
    result={'original_quote_verified':False,'point_in_time_qualified':False}
    try: packet=json.loads(raw)
    except (ValueError,UnicodeDecodeError):return {**result,'status':'whole_unparsed_predecessor'}
    if not isinstance(packet,dict):return {**result,'status':'whole_unexpected_predecessor'}
    result.update(status='whole_predecessor_packet_only',fields=sorted(packet),generated_at=packet.get('generated_at'))
    rows=packet.get('rows',packet.get('history'))
    if isinstance(rows,list):
        dates=[r.get('date',r.get('d')) for r in rows if isinstance(r,dict) and isinstance(r.get('date',r.get('d')),str)]
        result.update(rows=len(rows),dated_rows=len(dates),unique_dates=len(set(dates)),first_date=min(dates,default=None),last_date=max(dates,default=None))
    result['current_legs']={k:{f:v.get(f) for f in ('measure','level','level_pct','z','pctile')} for k,v in (packet.get('legs') or {}).items() if isinstance(v,dict)} if isinstance(packet.get('legs'),dict) else {}
    move=packet.get('move')
    if isinstance(move,dict):
        result['move']={k:move.get(k) for k in ('source','date','value','level','is_proxy','n_obs')}
        result['move']['original_quote_verified']=False
    return result


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
    subprocess.run([sys.executable, str(ROOT/'tests/test_fifx_original_baseline.py')], cwd=ROOT, check=True)
    s3, lam, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('s3', 'lambda', 'events', 'scheduler'))
    with report('ops_6136_fifx_original_baseline') as r:
        progress = {'contract': 'fifx-source-baseline.v1', 'request_id': REQUEST,
                    'started_at': now(), 'status': 'claimed', 'captures': {}, 'repo_predecessors': {}, 'canonical_originals': {}}
        journal(s3, progress, True)
        try:
            before = runtime(lam, s3, events, scheduler, FUNCTION)
            location = lam.get_function(FunctionName=FUNCTION)['Code']['Location']
            raw = bounded(urllib.request.urlopen(location, timeout=40))
            assert base64.b64encode(hashlib.sha256(raw).digest()).decode() == before['code_sha256']
            progress['native_predecessor'] = {'runtime': before, 'whole_zip': retain(s3, raw)}
            arn=lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn']
            discovery=triggers.collect(lam,scheduler,s3,arn,BUCKET)
            progress['trigger_inventory']=discovery
            # Preserve every actually scheduled qualified code package as well.
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
            progress['aliases']=aliases
            journal(s3, progress)
            packets = {}
            for key in INPUTS:
                try:response = s3.get_object(Bucket=BUCKET, Key=key)
                except Exception as exc:
                    if key=='data/report-measurements.json' or str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('NoSuchKey','404'):raise
                    progress['captures'][key]={'source_key':key,'status':'missing','captured_at':now()};journal(s3,progress);continue
                raw = bounded(response['Body'])
                if key=='data/report-measurements.json': packets[key] = json.loads(raw)
                progress['captures'][key] = {'source_key': key, 'original': retain(s3, raw), 'etag': response['ETag'], 'captured_at': now(), 'summary': summarize(raw)}
                journal(s3, progress)
            cache = {}
            def read(key):
                if not isinstance(key, str) or not re.fullmatch(r'data/(report-research|evidence/fred)/[A-Za-z0-9_./-]+', key) or '..' in key:
                    raise ValueError('Canonical macro replay path required')
                if key not in cache:
                    raw = bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'], canonical.MAX)
                    container = retain(s3, raw)
                    if key.endswith('.gz'):
                        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
                            raw = stream.read(canonical.MAX + 1)
                        if len(raw) > canonical.MAX: raise ValueError('Whole original exceeds replay bound')
                    progress['canonical_originals'][key] = {'source_key': key, 'stored_container': container, 'original': retain(s3, raw)}
                    cache[key] = raw
                return cache[key]
            packet = packets['data/report-measurements.json']
            originals = canonical.restore(packet, SERIES, read)
            summary = {}
            for sid, original in originals.items():
                if original is None:
                    summary[sid] = {'status': 'missing_canonical_series'}
                    continue
                meta = original['definition']['seriess'][0]; rows = original['observations']['observations']
                dates = sorted(row['date'] for row in rows)
                summary[sid] = {'status': 'reconstructed_from_whole_originals', 'native_unit': meta['units'],
                    'frequency': meta['frequency'], 'frequency_short': meta['frequency_short'],
                    'seasonal_adjustment': meta['seasonal_adjustment'], 'source_last_updated': meta.get('last_updated'),
                    'original_row_count': len(rows), 'missing_value_rows': sum(row.get('value') in (None, '.', '') for row in rows),
                    'first_observation_date': dates[0] if dates else None, 'last_observation_date': dates[-1] if dates else None,
                    'acquired_at': original['acquired_at'], 'source_quality': packet['measurements'][sid].get('quality'),
                    'historical_release_availability_verified': False}
            paths = subprocess.check_output(['git', 'grep', '-l', '-F', '-e', 'fifx-vol.json', '-e', 'fifx-vol-history.json', '--',
                'aws/lambdas/*/source/*.py', 'aws/shared/*.py', '*.html', '*.js', 'assets/*.js'], cwd=ROOT, text=True).splitlines()
            paths = set(paths) | {'aws/lambdas/' + FUNCTION + '/source/lambda_function.py', 'aws/shared/canonical_fred_replay.py',
                'aws/shared/report_observations.py', 'aws/shared/research_brief_model.py', 'aws/shared/evidence_store.py',
                'aws/shared/donor_contract.py', 'signal-board.html'}
            paths.update(subprocess.check_output(['git','ls-files','--','aws/lambdas/'+FUNCTION+'/source'],cwd=ROOT,text=True).splitlines())
            config = ROOT / 'aws/lambdas' / FUNCTION / 'config.json'
            progress['tracked_config_present'] = config.exists()
            if config.exists(): paths.add(config.relative_to(ROOT).as_posix())
            for path in sorted(paths): progress['repo_predecessors'][path] = retain(s3, (ROOT / path).read_bytes(), allow_empty=True)
            progress['quote_source_inventory']={sym:{'status':'original_response_not_retained_by_predecessor','original_quote_verified':False} for sym in QUOTE_SYMBOLS}
            progress.update(status='retained', source_replay=packet['replay'], source_generated_at=packet['generated_at'], source_summary=summary, move_original_verified=False, donor_authority_qualified=False)
            journal(s3, progress); manifest = retain(s3, encoded(progress))
            protected = {STATUS, manifest['key'], progress['native_predecessor']['whole_zip']['key']}
            protected.update(row['whole_zip']['key'] for row in progress['scheduled_qualified_packages'].values())
            if before['receipt'].get('whole_receipt'):protected.add(before['receipt']['whole_receipt']['key'])
            protected.update(ref['key'] for ref in progress['repo_predecessors'].values())
            protected.update(row['original']['key'] for row in progress['captures'].values() if 'original' in row)
            for row in progress['canonical_originals'].values(): protected.update((row['stored_container']['key'], row['original']['key']))
            outcomes = [access.check(key) for key in sorted(protected)]
            access_ref = retain(s3, encoded({'contract': 'fifx-baseline-access.v1', 'outcomes': outcomes}))
            outcomes.append(access.check(access_ref['key']))
            privacy = access.summarize(outcomes); r.kv(access_evidence=access_ref, **privacy)
            assert privacy['all_denied'], 'Inspect anonymous-access outcomes before changing the producer'
            assert runtime(lam, s3, events, scheduler, FUNCTION) == before
            assert triggers.collect(lam,scheduler,s3,arn,BUCKET)==discovery
            for qualifier,row in progress['scheduled_qualified_packages'].items():
                assert lam.get_function_configuration(FunctionName=FUNCTION,Qualifier=qualifier)['CodeSha256']==row['code_sha256']
            journal(s3, {**progress, 'status': 'complete', 'completed_at': now(), 'manifest': manifest, 'privacy': privacy})
            r.kv(manifest=manifest, native_predecessor=progress['native_predecessor'], captures=progress['captures'],
                trigger_inventory=discovery,aliases=aliases,scheduled_qualified_packages=progress['scheduled_qualified_packages'],
                source_replay=packet['replay'], source_generated_at=packet['generated_at'], source_summary=summary,
                repo_predecessors=progress['repo_predecessors'], canonical_originals=progress['canonical_originals'], quote_source_inventory=progress['quote_source_inventory'],
                move_original_verified=False, donor_authority_qualified=False, all_predecessor_bytes_preserved=True, canonical_originals_replayed=True, requested_series=len(SERIES), reconstructed_series=sum(v['status']=='reconstructed_from_whole_originals' for v in summary.values()), missing_series=[k for k,v in summary.items() if v['status']=='missing_canonical_series'], native_package_unchanged=True,
                native_formula_changed=False, tracked_config_present=progress['tracked_config_present'], provider_requests=0, producer_invocations=0, consumer_invocations=0,
                public_writes=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0, signal_writes=0, schedules_changed=0,
                forecast_qualified=False, sizing_qualified=False)
        except Exception as exc:
            journal(s3, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
            raise


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
