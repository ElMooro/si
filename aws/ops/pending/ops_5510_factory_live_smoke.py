#!/usr/bin/env python3
"""Verify the actual Gear A release without training, orders or new resources."""
import hashlib
import io
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/shared'))
from factory_core import verify_state

PUBLIC = 'justhodl-dashboard-live'
PRIVATE = 'justhodl-ai-857687956942'
REGION = 'us-east-1'
CFG = Config(connect_timeout=5, read_timeout=140, retries={'max_attempts': 1})


def main(rep):
    lam = boto3.client('lambda', region_name=REGION, config=CFG)
    s3 = boto3.client('s3', region_name=REGION, config=CFG)
    iam = boto3.client('iam', config=CFG)
    sch = boto3.client('scheduler', region_name=REGION, config=CFG)
    def read(bucket, key):
        return json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())
    result = {'schema_version':'factory-live-smoke.v1', 'verified_at':datetime.now(timezone.utc).isoformat(), 'functions':{}, 'checks':{}}
    for name in ('justhodl-ai','justhodl-factory-grader','justhodl-student-rsi'):
        # Ops and function deploys share the same push. Wait for this checkout's
        # function sources, using the receipt before downloading the package.
        expected = {p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in (ROOT/'aws/lambdas'/name/'source').glob('*.py')}
        for attempt in range(12):
            fn = lam.get_function(FunctionName=name)
            config = fn['Configuration']
            receipt = read(PUBLIC, 'data/ops/releases/' + name + '.json')
            ready = config.get('State') == 'Active' and config.get('LastUpdateStatus') == 'Successful'
            ready = ready and all(receipt.get('source',{}).get(module,{}).get('sha256') == sha for module,sha in expected.items())
            if ready:break
            if attempt == 11:raise RuntimeError('release_receipt_not_this_checkout:' + name)
            time.sleep(20)
        if config.get('State') != 'Active' or config.get('LastUpdateStatus') != 'Successful':
            raise RuntimeError('function_not_ready:' + name)
        receipt = read(PUBLIC, 'data/ops/releases/' + name + '.json')
        if receipt.get('verified') is not True or receipt['code_sha256'] != config['CodeSha256']:
            raise RuntimeError('live_package_receipt_mismatch:' + name)
        with urllib.request.urlopen(fn['Code']['Location'], timeout=30) as response:
            raw = response.read(60 * 1024 * 1024)
        if hashlib.sha256(raw).hexdigest() != receipt['zip_sha256_hex']:
            raise RuntimeError('downloaded_package_hash_mismatch')
        archive = zipfile.ZipFile(io.BytesIO(raw))
        required = ['factory_core.py', 'factory_store.py']
        required += ['factory_gateway.py'] if name == 'justhodl-ai' else ['student_lambda.py'] if name == 'justhodl-student-rsi' else ['lambda_function.py']
        for module in required:
            if module not in archive.namelist():
                raise RuntimeError('shared_module_missing:' + module)
        for module in ('factory_core.py','factory_store.py'):
            if archive.read(module) != (ROOT/'aws/shared'/module).read_bytes():
                raise RuntimeError('shared_source_not_this_release:' + module)
        if name != 'justhodl-ai' and config['Role'] != 'arn:aws:iam::857687956942:role/' + name + '-role':
            raise RuntimeError('incorrect_factory_execution_role')
        result['functions'][name] = {'arn':config['FunctionArn'],'role':config['Role'],'code_sha256':config['CodeSha256'],
            'source_commit':receipt['commit'],'memory':config['MemorySize'],'timeout':config['Timeout'],'package_parity':True}
    student = result['functions']['justhodl-student-rsi']
    if (student['memory'],student['timeout']) != (512,120):
        raise RuntimeError('student_resource_contract_mismatch')
    schedule = sch.get_schedule(Name='justhodl-student-rsi-1m',GroupName='default')
    if schedule['State'] != 'ENABLED' or schedule['ScheduleExpression'] != 'rate(1 minute)' or schedule['Target']['Arn'] != student['arn']:
        raise RuntimeError('schedule_contract_mismatch')
    result['schedule'] = {k:schedule[k] for k in ('Name','State','ScheduleExpression')}
    result['schedule']['service']='EventBridge Scheduler'
    for attempt in range(4):
        invoked = lam.invoke(FunctionName='justhodl-student-rsi',InvocationType='RequestResponse',Payload=b'{}')
        output = json.loads(invoked['Payload'].read())
        if invoked.get('FunctionError'):
            raise RuntimeError('student_invocation_failed:' + str(output.get('errorMessage',''))[:200])
        if output.get('status') != 'already_running':
            break
        time.sleep(15)
    a, b = read(PUBLIC,'student-state.json'),read(PUBLIC,'data/student-state.json')
    verify_state(a);verify_state(b)
    if a['checksum'] != b['checksum'] or a['gen'] < 1 or len(a['agents']) != 9 or not a['skillbook']:
        raise RuntimeError('state_or_evidence_incomplete')
    if a['health']['errors']:
        raise RuntimeError('factory_health_degraded:' + json.dumps(a['health']['errors'])[:300])
    if a['model']['training_runs'] != 0 or a['budget']['gpu_jobs'] != 0 or a['budget']['paid_model_calls'] != 0:
        raise RuntimeError('unexpected_model_or_resource_operation')
    verdict=read(PRIVATE,'factory/verdicts/code-identity-v1-student.json')
    if verdict['ok'] is not True or verdict['candidate']['n'] != 64 or verdict['candidate']['score'] <= verdict['baseline']['score']:
        raise RuntimeError('no_independent_coding_improvement')
    result['state']={k:a[k] for k in ('gen','state_version','checksum','fit','health','model')}
    result['coding']={'candidate':verdict['candidate'],'baseline':verdict['baseline'],'source_sha256':verdict['source_sha256'],
                     'scope':'one bounded repair family, not model training'}
    role=student['role']
    denied = [
        ('iam:CreateRole','*'),('events:PutRule','*'),('scheduler:CreateSchedule','*'),('sagemaker:CreateEndpoint','*'),
        ('sagemaker:CreateTrainingJob','*'),('lambda:InvokeFunction','arn:aws:lambda:us-east-1:857687956942:function:justhodl-ai'),
        ('s3:GetObject','arn:aws:s3:::'+PRIVATE+'/factory/exams/code-identity-v1.json'),
        ('s3:GetObject','arn:aws:s3:::'+PRIVATE+'/brain/notes.json'),
        ('s3:PutObject','arn:aws:s3:::'+PRIVATE+'/factory/control/policy.json'),
        ('s3:PutObject','arn:aws:s3:::'+PUBLIC+'/data/ai.json'),
        ('s3:PutObject','arn:aws:s3:::'+PUBLIC+'/factory/champions/current.json')]
    decisions=[]
    for action,resource in denied:
        evaluation=iam.simulate_principal_policy(PolicySourceArn=role,ActionNames=[action],ResourceArns=[resource])['EvaluationResults'][0]
        if evaluation['EvalDecision']=='allowed':raise RuntimeError('student_permission_too_broad:'+action)
        decisions.append({'action':action,'resource':resource,'decision':evaluation['EvalDecision']})
    result['permission_denials']=decisions
    brain=read(PUBLIC,'data/ai.json')
    if not {'version','pipeline','scoreboard','inventory_summary'} <= brain.keys():raise RuntimeError('brain_feed_not_preserved')
    result['checks']['brain_feed_preserved']=True
    result['checks']['brain_version']=brain['version']
    result['checks']['brain_endpoints']=brain['inventory_summary'].get('endpoints')
    for key in ('factory/salon/board.json','factory/salon/season.json','factory/scoreboard.json','factory/champions/current.json','factory/invites.json'):
        read(PUBLIC,key)
    for key in ('factory/salon/wall.jsonl','factory/salon/events.jsonl'):
        lines=s3.get_object(Bucket=PUBLIC,Key=key)['Body'].read().splitlines()
        if not lines:raise RuntimeError('wall_evidence_missing')
        for line in lines:json.loads(line)
        result['checks'][key]={'lines':len(lines),'sha256':hashlib.sha256(b'\n'.join(lines)).hexdigest()}
    # Verify the public unauthenticated boundary. No credentials or private data are transmitted.
    request=urllib.request.Request('https://justhodl.ai/api/v1/factory/traces',data=b'{}',method='POST',headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(request,timeout=20) as response:
            status=response.status
    except urllib.error.HTTPError as exc:
        status=exc.code
    if status!=401:raise RuntimeError('public_admission_boundary:'+str(status))
    result['checks']['unauthenticated_trace_status']=status
    # Maintain the owner's requested S3 site copies as well as the Pages release.
    # Archive the previous object before a conditional replacement; never touch data/ai.json.
    mirrored = []
    for asset, content_type in [('ai.html','text/html; charset=utf-8'),('factory-desk.js','text/javascript; charset=utf-8'),('factory-desk.css','text/css; charset=utf-8')]:
        body = (ROOT / asset).read_bytes()
        if asset == 'ai.html' and (b'factory-pane' not in body or b'/data/ai.json' not in body):
            raise RuntimeError('invalid_desk_artifact')
        try:
            prior = s3.get_object(Bucket=PUBLIC,Key=asset)
            old = prior['Body'].read()
            if old == body:
                mirrored.append(asset)
                continue
            backup = 'factory/releases/site-before-' + hashlib.sha256(old).hexdigest() + '-' + asset
            try:
                s3.put_object(Bucket=PRIVATE,Key=backup,Body=old,ContentType=content_type,IfNoneMatch='*',ServerSideEncryption='AES256')
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('PreconditionFailed','412'):raise
            conditional={'IfMatch':prior['ETag']}
        except Exception as exc:
            if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('NoSuchKey','404'):raise
            conditional={'IfNoneMatch':'*'}
        s3.put_object(Bucket=PUBLIC,Key=asset,Body=body,ContentType=content_type,CacheControl='max-age=60, must-revalidate',ServerSideEncryption='AES256',**conditional)
        mirrored.append(asset)
    result['checks']['site_s3_mirrors']=mirrored
    result['ok']=True
    target=Path('aws/ops/reports/5510.json');target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    key='factory/releases/'+result['verified_at'].replace(':','-')+'.json'
    s3.put_object(Bucket=PRIVATE,Key=key,Body=json.dumps(result).encode(),ContentType='application/json',IfNoneMatch='*',ServerSideEncryption='AES256')
    rep.kv(functions=result['functions'],schedule=result['schedule'],generation=a['gen'],permissions_denied=len(decisions),coding=result['coding'])
    rep.ok('Live code, roles, independent result, dual state, schedule, wall and Brain verified. No training or endpoints created.')


if __name__=='__main__':
    try:
        with report('5510_factory_live_smoke') as rep:
            main(rep)
    except Exception as exc:
        print('Factory smoke failed:',type(exc).__name__,str(exc)[:300])
        sys.exit(1)
