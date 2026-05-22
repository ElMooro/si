#!/usr/bin/env python3
"""
ops 1063 — direct-deploy carry-surface + engine-contribution via boto3
=======================================================================
The deploy-lambdas.yml workflow path filter isn't picking up these new dirs.
Bypass it: read source + config from the checked-out repo (this script runs
inside the GH runner with the repo present), zip, lambda.create_function,
wire DLQ + X-Ray + EventBridge target, invoke once to confirm output.
"""
import json, os, time, io, zipfile, boto3
from datetime import datetime, timezone

DLQ_ARN = 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default'
ACCOUNT_ID = '857687956942'
REGION = 'us-east-1'

lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)


def make_zip(source_path):
    with open(source_path, 'rb') as f:
        source = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('lambda_function.py', source)
    return buf.getvalue(), len(source)


def wait_active(fn_name, max_wait=90):
    for _ in range(max_wait):
        try:
            cfg = lam.get_function_configuration(FunctionName=fn_name)
            state = cfg.get('State')
            lu = cfg.get('LastUpdateStatus')
            if state == 'Active' and lu in ('Successful', None):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def deploy_one(fn_name):
    out = {'fn_name': fn_name}
    
    source_path = f"aws/lambdas/{fn_name}/source/lambda_function.py"
    config_path = f"aws/lambdas/{fn_name}/config.json"
    
    if not os.path.exists(source_path):
        out['error'] = f"source not found at {source_path}"
        return out
    
    cfg = json.loads(open(config_path).read())
    zip_bytes, source_size = make_zip(source_path)
    out['fetched'] = {'source_bytes': source_size, 'zip_size': len(zip_bytes)}
    
    common = {
        'FunctionName': fn_name,
        'Timeout': cfg.get('timeout', 60),
        'MemorySize': cfg.get('memory', 512),
        'Environment': {'Variables': cfg.get('env', {})},
    }
    
    try:
        lam.create_function(
            **common,
            Runtime=cfg.get('runtime', 'python3.12'),
            Role=cfg['role'],
            Handler=cfg.get('handler', 'lambda_function.lambda_handler'),
            Code={'ZipFile': zip_bytes},
            Description=cfg.get('description', '')[:256],
            Architectures=cfg.get('architectures', ['x86_64']),
            TracingConfig={'Mode': 'Active'},
            DeadLetterConfig={'TargetArn': DLQ_ARN},
            Publish=False,
        )
        out['action'] = 'created'
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=fn_name, ZipFile=zip_bytes, Publish=False)
        wait_active(fn_name)
        lam.update_function_configuration(
            **common,
            Runtime=cfg.get('runtime', 'python3.12'),
            Role=cfg['role'],
            Handler=cfg.get('handler', 'lambda_function.lambda_handler'),
            Description=cfg.get('description', '')[:256],
            TracingConfig={'Mode': 'Active'},
            DeadLetterConfig={'TargetArn': DLQ_ARN},
        )
        out['action'] = 'updated'
    except Exception as e:
        out['error'] = f"create failed: {type(e).__name__}: {str(e)[:300]}"
        return out
    
    if not wait_active(fn_name):
        out['warn'] = 'did not reach Active state in 90s'
    
    # Wire EB rule
    rule_name = (cfg.get('schedule') or {}).get('rule_name')
    if rule_name:
        fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn_name}"
        try:
            events.put_targets(Rule=rule_name, Targets=[{'Id': 'target1', 'Arn': fn_arn}])
            out['eb_target_wired'] = True
        except Exception as e:
            out['eb_target_error'] = str(e)[:150]
        try:
            lam.add_permission(
                FunctionName=fn_name,
                StatementId=f'eb-{rule_name}'[:64],
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
            )
            out['eb_permission_added'] = True
        except lam.exceptions.ResourceConflictException:
            out['eb_permission_added'] = 'already_exists'
        except Exception as e:
            out['eb_permission_error'] = str(e)[:150]
    
    # Test invoke
    time.sleep(3)
    try:
        inv = lam.invoke(FunctionName=fn_name, InvocationType='RequestResponse', Payload=b'{}')
        out['invoke'] = {
            'status': inv['StatusCode'],
            'fn_error': inv.get('FunctionError', 'none'),
            'response': inv['Payload'].read().decode()[:800],
        }
    except Exception as e:
        out['invoke'] = {'error': str(e)[:300]}
    
    # S3 output check
    out_key_map = {
        'justhodl-carry-surface': 'data/carry-surface.json',
        'justhodl-engine-contribution': 'data/engine-contributions.json',
    }
    out_key = out_key_map.get(fn_name)
    if out_key:
        time.sleep(3)
        try:
            head = s3.head_object(Bucket='justhodl-dashboard-live', Key=out_key)
            age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
            out['s3_output'] = {
                'exists': True,
                'size': head['ContentLength'],
                'age_h': round(age_h, 3),
                'last_modified': head['LastModified'].isoformat(),
            }
        except Exception as e:
            out['s3_output'] = {'exists': False, 'error': str(e)[:150]}
    
    return out


report = {'started_at': datetime.now(timezone.utc).isoformat()}

for fn in ['justhodl-carry-surface', 'justhodl-engine-contribution']:
    print(f"--- deploying {fn} ---")
    try:
        report[fn] = deploy_one(fn)
    except Exception as e:
        report[fn] = {'fatal_error': f"{type(e).__name__}: {str(e)[:400]}"}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1063.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print("\n=== FINAL ===")
print(json.dumps(report, indent=2, default=str))
