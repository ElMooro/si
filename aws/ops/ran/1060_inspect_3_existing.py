#!/usr/bin/env python3
"""ops 1060 — read full code of 3 existing Lambdas + check their S3 outputs"""
import json, boto3, os, urllib.request, io, zipfile, re
from datetime import datetime, timezone

lam = boto3.client('lambda', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

report = {'started_at': datetime.now(timezone.utc).isoformat()}

for fn in ['justhodl-carry-surface', 'justhodl-engine-contribution', 'justhodl-earnings-nlp']:
    info = {}
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        info['exists'] = True
        info['memory'] = cfg.get('MemorySize')
        info['timeout'] = cfg.get('Timeout')
        info['description'] = cfg.get('Description')
        info['env_keys'] = sorted(list((cfg.get('Environment') or {}).get('Variables', {}).keys()))
        info['last_modified'] = cfg.get('LastModified')
        
        # Full code
        full = lam.get_function(FunctionName=fn)
        code = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(full['Code']['Location'], timeout=30).read())).read('lambda_function.py').decode()
        info['code_size'] = len(code)
        info['code_lines'] = len(code.split('\n'))
        
        # Find OUT_KEY
        m = re.search(r'(OUT_KEY|STATE_KEY)\s*=\s*["\']([^"\']+)["\']', code)
        info['out_key'] = m.group(2) if m else None
        
        # Find imports + top-level structure
        imports = re.findall(r'^(?:from\s+\S+\s+import\s+\S+|import\s+\S+)', code, re.MULTILINE)
        info['imports'] = imports[:10]
        
        # Find functions
        funcs = re.findall(r'^def\s+(\w+)\(', code, re.MULTILINE)
        info['functions'] = funcs[:20]
        
        # Show the lambda_handler
        h_match = re.search(r'def lambda_handler.*?(?=^def |\Z)', code, re.DOTALL | re.MULTILINE)
        info['lambda_handler'] = h_match.group(0)[:2500] if h_match else None
        
        # Recent invocation success
        cw = boto3.client('cloudwatch', region_name='us-east-1')
        end = datetime.now(timezone.utc)
        from datetime import timedelta
        start = end - timedelta(days=7)
        try:
            inv_resp = cw.get_metric_statistics(
                Namespace='AWS/Lambda', MetricName='Invocations',
                Dimensions=[{'Name': 'FunctionName', 'Value': fn}],
                StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
            )
            invocations_7d = sum(p['Sum'] for p in inv_resp.get('Datapoints', []))
            err_resp = cw.get_metric_statistics(
                Namespace='AWS/Lambda', MetricName='Errors',
                Dimensions=[{'Name': 'FunctionName', 'Value': fn}],
                StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
            )
            errors_7d = sum(p['Sum'] for p in err_resp.get('Datapoints', []))
            info['cw_7d'] = {'invocations': int(invocations_7d), 'errors': int(errors_7d)}
        except Exception as e:
            info['cw_7d'] = {'error': str(e)[:100]}
        
        # Live invoke
        try:
            inv = lam.invoke(FunctionName=fn, InvocationType='RequestResponse', Payload=b'{}')
            payload = inv['Payload'].read().decode()
            info['live_invoke'] = {
                'status': inv['StatusCode'],
                'fn_error': inv.get('FunctionError', 'none'),
                'response': payload[:800],
            }
        except Exception as e:
            info['live_invoke'] = {'error': str(e)[:200]}
        
        # Check S3 output if OUT_KEY found
        if info['out_key']:
            try:
                head = s3.head_object(Bucket='justhodl-dashboard-live', Key=info['out_key'])
                age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
                info['s3_output'] = {
                    'key': info['out_key'],
                    'exists': True,
                    'size': head['ContentLength'],
                    'last_modified': head['LastModified'].isoformat(),
                    'age_h': round(age_h, 2),
                }
                # Read content preview
                obj = s3.get_object(Bucket='justhodl-dashboard-live', Key=info['out_key'])
                content = obj['Body'].read().decode()
                try:
                    parsed = json.loads(content)
                    info['s3_content_keys'] = list(parsed.keys()) if isinstance(parsed, dict) else 'not_dict'
                    info['s3_content_preview'] = json.dumps(parsed, default=str, indent=2)[:1500]
                except Exception:
                    info['s3_content_preview'] = content[:500]
            except Exception as e:
                info['s3_output'] = {'key': info['out_key'], 'exists': False, 'error': str(e)[:100]}
    except Exception as e:
        info['exists'] = False
        info['error'] = str(e)[:300]
    
    report[fn] = info

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1060.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
