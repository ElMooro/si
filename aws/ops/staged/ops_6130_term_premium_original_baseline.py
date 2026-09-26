"""Preserve the complete Term Premium engine, parsed archive and all consumers.

No new provider request, invocation, account access, public write, notification
or schedule change. The baseline is evidence for a future native migration.
"""
from pathlib import Path
from datetime import datetime, timezone
import base64, gzip, hashlib, io, json, re, subprocess, sys, urllib.request
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
import retained_access_evidence as access
import liquidity_agent_triggers as triggers

BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-term-premium'
PRIVATE = 'audit-private/20260909-originals/term-premium-research/'
REQUEST = 'chatgpt-term-premium-original-baseline-6130'
sha = lambda raw: hashlib.sha256(raw).hexdigest()
encoded = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
now = lambda: datetime.now(timezone.utc).isoformat()
STATUS = PRIVATE + 'requests/' + sha(REQUEST.encode()) + '.json'
INPUTS = ('data/term-premium.json', 'data/history/acm-term-premium.json')



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
    try:packet=json.loads(raw)
    except (ValueError,UnicodeDecodeError):return {'status':'whole_unparsed_predecessor','original_workbook_verified':False}
    if isinstance(packet,list):
        dates=[r.get('date') for r in packet if isinstance(r,dict) and isinstance(r.get('date'),str)]
        return {'status':'whole_parsed_predecessor_only','rows':len(packet),'dated_rows':len(dates),
            'unique_dates':len(set(dates)),'first_date':min(dates,default=None),'last_date':max(dates,default=None),
            'row_fields':sorted({name for r in packet if isinstance(r,dict) for name in r}),
            'original_workbook_verified':False,'point_in_time_qualified':False}
    if not isinstance(packet,dict):return {'status':'whole_unexpected_predecessor','original_workbook_verified':False}
    latest=packet.get('latest');latest=latest if isinstance(latest,dict) else {}
    return {'status':'whole_predecessor_packet_only','generated_at':packet.get('generated_at'),
        'latest_date':latest.get('date'),'source_status':packet.get('source'),'fields':sorted(packet),
        'original_workbook_verified':False,'point_in_time_qualified':False}


def main():
    subprocess.run([sys.executable, str(ROOT/'tests/test_term_premium_original_baseline.py')], cwd=ROOT, check=True)
    s3, lam, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('s3', 'lambda', 'events', 'scheduler'))
    with report('ops_6130_term_premium_original_baseline') as r:
        progress = {'contract': 'term-premium-source-baseline.v1', 'request_id': REQUEST,
                    'started_at': now(), 'status': 'claimed', 'captures': {}, 'repo_predecessors': {}}
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
            summary = {}
            for key in INPUTS:
                try:response = s3.get_object(Bucket=BUCKET, Key=key)
                except Exception as exc:
                    if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('NoSuchKey','404'):raise
                    progress['captures'][key]={'source_key':key,'status':'missing','captured_at':now()};journal(s3,progress);continue
                raw = bounded(response['Body'])
                progress['captures'][key] = {'source_key': key, 'original': retain(s3, raw), 'etag': response['ETag'], 'captured_at': now()}
                summary[key]=summarize(raw)
                journal(s3, progress)
            paths = subprocess.check_output(['git', 'grep', '-l', '-F', 'term-premium.json', '--',
                'aws/lambdas/*/source/*.py', 'aws/shared/*.py', '*.html', '*.js', 'assets/*.js'], cwd=ROOT, text=True).splitlines()
            paths = set(paths) | {'term-premium.html'}
            paths.update(subprocess.check_output(['git','ls-files','--','aws/lambdas/'+FUNCTION+'/source'],cwd=ROOT,text=True).splitlines())
            config = ROOT / 'aws/lambdas' / FUNCTION / 'config.json'
            progress['tracked_config_present'] = config.exists()
            if config.exists(): paths.add(config.relative_to(ROOT).as_posix())
            for path in sorted(paths): progress['repo_predecessors'][path] = retain(s3, (ROOT / path).read_bytes(), allow_empty=True)
            progress.update(status='retained', source_summary=summary, original_workbook_verified=False)
            journal(s3, progress); manifest = retain(s3, encoded(progress))
            protected = {STATUS, manifest['key'], progress['native_predecessor']['whole_zip']['key']}
            protected.update(row['whole_zip']['key'] for row in progress['scheduled_qualified_packages'].values())
            protected.update(ref['key'] for ref in progress['repo_predecessors'].values())
            protected.update(row['original']['key'] for row in progress['captures'].values() if 'original' in row)
            outcomes = [access.check(key) for key in sorted(protected)]
            access_ref = retain(s3, encoded({'contract': 'term-premium-baseline-access.v1', 'outcomes': outcomes}))
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
                source_summary=summary,repo_predecessors=progress['repo_predecessors'],
                all_predecessor_bytes_preserved=True,original_workbook_verified=False,native_package_unchanged=True,
                native_formula_changed=False, tracked_config_present=progress['tracked_config_present'], provider_requests=0, producer_invocations=0, consumer_invocations=0,
                public_writes=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0, signal_writes=0, schedules_changed=0,
                forecast_qualified=False, sizing_qualified=False)
        except Exception as exc:
            journal(s3, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
            raise


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
