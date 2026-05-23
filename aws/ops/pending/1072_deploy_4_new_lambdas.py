#!/usr/bin/env python3
"""
ops 1072 — DIRECT BOTO3 DEPLOY of 4 new Lambdas
=================================================
deploy-lambdas.yml path filter only triggers on file changes inside
already-tracked Lambda dirs. Brand-new dirs require manual boto3 create.

For each new Lambda:
  1. Read config.json + lambda_function.py from aws/lambdas/<fn>/
  2. Zip source → in-memory bytes
  3. Lookup ANTHROPIC_API_KEY from justhodl-ai-chat (existing Claude Lambda)
     to replace PLACEHOLDER in fed-nlp + news-wire env
  4. lam.create_function (with env vars) OR update if exists
  5. Create/update EventBridge rule with schedule
  6. Add EB rule target + Lambda invoke permission
  7. Verify Lambda exists with state=Active
"""
import json, os, io, zipfile, time, boto3
from datetime import datetime, timezone

REGION = 'us-east-1'
ACCT = '857687956942'
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)

# Read Anthropic key from a known Claude-using Lambda
def get_anthropic_key():
    for source_fn in ['justhodl-ai-chat', 'justhodl-morning-intelligence',
                       'justhodl-investor-agents', 'justhodl-earnings-nlp']:
        try:
            cfg = lam.get_function_configuration(FunctionName=source_fn)
            env = (cfg.get('Environment') or {}).get('Variables', {})
            key = env.get('ANTHROPIC_API_KEY')
            if key and len(key) > 20:
                return key, source_fn
        except Exception:
            continue
    return None, None

ANTHROPIC_KEY, anthropic_src = get_anthropic_key()
print(f"[ops1072] ANTHROPIC_API_KEY sourced from: {anthropic_src or 'NONE'} "
      f"(len={len(ANTHROPIC_KEY) if ANTHROPIC_KEY else 0})")

LAMBDAS = [
    'justhodl-macro-calendar',
    'justhodl-fed-nlp',
    'justhodl-news-wire',
    'justhodl-concentration-liquidity',
]

results = {}

for fn_name in LAMBDAS:
    base = f'aws/lambdas/{fn_name}'
    cfg_path = f'{base}/config.json'
    src_path = f'{base}/source/lambda_function.py'
    
    if not os.path.exists(cfg_path) or not os.path.exists(src_path):
        results[fn_name] = {'error': f'missing files: cfg={os.path.exists(cfg_path)} src={os.path.exists(src_path)}'}
        continue
    
    cfg = json.load(open(cfg_path))
    src_code = open(src_path, 'r', encoding='utf-8').read()
    # Force LF line endings (per memory rule: CRLF breaks Lambda)
    src_code = src_code.replace('\r\n', '\n').replace('\r', '\n')
    
    # Zip source
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zi = zipfile.ZipInfo('lambda_function.py')
        zi.external_attr = 0o644 << 16
        zf.writestr(zi, src_code)
    zip_bytes = buf.getvalue()
    
    # Resolve env vars (inject Anthropic key)
    env_vars = dict(cfg.get('env', {}))
    if env_vars.get('ANTHROPIC_API_KEY') == 'PLACEHOLDER_REPLACE_VIA_AWS_CONSOLE_OR_OPS':
        if ANTHROPIC_KEY:
            env_vars['ANTHROPIC_API_KEY'] = ANTHROPIC_KEY
        else:
            del env_vars['ANTHROPIC_API_KEY']
    
    fn_arn = None
    create_attempted = False
    try:
        # Does it already exist?
        existing = lam.get_function_configuration(FunctionName=fn_name)
        fn_arn = existing['FunctionArn']
        # Update code
        lam.update_function_code(FunctionName=fn_name, ZipFile=zip_bytes, Publish=False)
        # Wait for update
        for _ in range(30):
            s = lam.get_function_configuration(FunctionName=fn_name)
            if s.get('LastUpdateStatus') == 'Successful':
                break
            time.sleep(2)
        # Update config
        lam.update_function_configuration(
            FunctionName=fn_name,
            Runtime=cfg.get('runtime', 'python3.12'),
            Handler=cfg.get('handler', 'lambda_function.lambda_handler'),
            Role=cfg['role'],
            Description=cfg.get('description', '')[:255],
            Timeout=int(cfg.get('timeout', 300)),
            MemorySize=int(cfg.get('memory', 512)),
            Environment={'Variables': env_vars},
        )
        results[fn_name] = {'action': 'UPDATED', 'arn': fn_arn}
    except lam.exceptions.ResourceNotFoundException:
        # Create
        create_attempted = True
        try:
            resp = lam.create_function(
                FunctionName=fn_name,
                Runtime=cfg.get('runtime', 'python3.12'),
                Role=cfg['role'],
                Handler=cfg.get('handler', 'lambda_function.lambda_handler'),
                Code={'ZipFile': zip_bytes},
                Description=cfg.get('description', '')[:255],
                Timeout=int(cfg.get('timeout', 300)),
                MemorySize=int(cfg.get('memory', 512)),
                Environment={'Variables': env_vars},
                Publish=True,
                Architectures=cfg.get('architectures', ['x86_64']),
            )
            fn_arn = resp['FunctionArn']
            # Wait Active
            for _ in range(30):
                s = lam.get_function_configuration(FunctionName=fn_name)
                if s.get('State') == 'Active':
                    break
                time.sleep(2)
            results[fn_name] = {'action': 'CREATED', 'arn': fn_arn}
        except Exception as e:
            results[fn_name] = {'action': 'CREATE_FAILED', 'error': str(e)[:300]}
            continue
    except Exception as e:
        results[fn_name] = {'action': 'UPDATE_FAILED', 'error': str(e)[:300]}
        continue
    
    # === Schedule (EventBridge) ===
    schedule = cfg.get('schedule')
    if schedule and fn_arn:
        rule_name = schedule['rule_name']
        try:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression=schedule['cron'],
                State='ENABLED',
                Description=schedule.get('description', '')[:255],
            )
            # Lambda permission for EB to invoke
            stmt_id = f'eb-invoke-{rule_name}'[:64]
            try:
                lam.add_permission(
                    FunctionName=fn_name,
                    StatementId=stmt_id,
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=f'arn:aws:events:{REGION}:{ACCT}:rule/{rule_name}',
                )
            except lam.exceptions.ResourceConflictException:
                pass  # already exists
            # Target
            events.put_targets(
                Rule=rule_name,
                Targets=[{'Id': '1', 'Arn': fn_arn}],
            )
            results[fn_name]['schedule'] = f"{rule_name} → {schedule['cron']}"
        except Exception as e:
            results[fn_name]['schedule_err'] = str(e)[:200]
    
    # Final verify
    try:
        v = lam.get_function_configuration(FunctionName=fn_name)
        results[fn_name]['state'] = v.get('State')
        results[fn_name]['last_update_status'] = v.get('LastUpdateStatus')
        results[fn_name]['code_size'] = v.get('CodeSize')
    except Exception as e:
        results[fn_name]['verify_err'] = str(e)[:200]

report = {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'anthropic_key_sourced_from': anthropic_src,
    'results': results,
}
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1072.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:4000])
