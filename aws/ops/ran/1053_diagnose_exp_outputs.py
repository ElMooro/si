#!/usr/bin/env python3
"""ops 1053 — diagnose exp Lambda output state (missing vs key-mismatch vs silent-skip)"""
import json, boto3, os, re
from datetime import datetime, timezone

REGION = 'us-east-1'
s3 = boto3.client('s3', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)

EXPECTED = {
    'justhodl-premortem-engine': 'data/kill-theses.json',
    'justhodl-behavior-mirror': 'data/khalid-behavior-mirror.json',
    'justhodl-failure-library': 'data/failure-library.json',
    'justhodl-causality-scanner': 'data/causality-scanner.json',
    'justhodl-convexity-scorer': 'data/convexity-scores.json',
    'justhodl-chart-vision': 'data/chart-vision.json',
    'justhodl-meta-improver': 'data/meta-improver.json',
    'justhodl-signal-halflife': 'data/signal-halflife.json',
}

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'lambdas': {}}

for fn_name, expected_key in EXPECTED.items():
    info = {'expected_key': expected_key}
    
    # Check S3 directly
    try:
        head = s3.head_object(Bucket='justhodl-dashboard-live', Key=expected_key)
        age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
        info['s3'] = {
            'exists': True,
            'size': head['ContentLength'],
            'last_modified': head['LastModified'].isoformat(),
            'age_h': round(age_h, 1),
        }
    except Exception as e:
        info['s3'] = {'exists': False, 'error': str(e)[:120]}
    
    # Pull the deployed Lambda code and find OUT_KEY (or equivalent constants)
    try:
        cfg = lam.get_function(FunctionName=fn_name)
        loc = cfg.get('Code', {}).get('Location')
        if loc:
            import urllib.request, io, zipfile
            zip_bytes = urllib.request.urlopen(loc, timeout=30).read()
            z = zipfile.ZipFile(io.BytesIO(zip_bytes))
            code = z.read('lambda_function.py').decode()
            
            # Search for OUT_KEY assignment or s3.put_object Key=
            constants = re.findall(r'^([A-Z_]+)\s*=\s*["\']data/[^"\']+["\']', code, re.MULTILINE)
            keys_in_puts = re.findall(r'\.put_object\([^)]*Key\s*=\s*["\']?([^,"\'\s)]+)', code)
            
            # Look for OUT_KEY value specifically
            m = re.search(r'OUT_KEY\s*=\s*["\']([^"\']+)["\']', code)
            info['code_OUT_KEY'] = m.group(1) if m else None
            
            info['constants_data'] = constants[:5]
            info['keys_used'] = list(set(keys_in_puts))[:5]
            
            # Check for "early return" patterns (return without put_object before final write)
            handler_start = code.find('def lambda_handler')
            if handler_start > 0:
                handler_body = code[handler_start:]
                # Count returns before any put_object in handler
                first_put_at = handler_body.find('s3.put_object')
                if first_put_at > 0:
                    pre_put = handler_body[:first_put_at]
                    early_returns = pre_put.count('return ')
                    info['early_returns_before_put'] = early_returns
                else:
                    info['early_returns_before_put'] = 'no_put_in_handler'
    except Exception as e:
        info['code_inspect_error'] = str(e)[:200]
    
    report['lambdas'][fn_name] = info
    
    icon = "✅" if info['s3'].get('exists') else "❌"
    code_key = info.get('code_OUT_KEY', '?')
    print(f"  {icon} {fn_name}")
    print(f"     expected: {expected_key}")
    print(f"     OUT_KEY  : {code_key}  match={code_key == expected_key}")
    print(f"     s3       : {info['s3']}")
    print(f"     keys_used: {info.get('keys_used',[])}")
    print(f"     early_returns_before_put: {info.get('early_returns_before_put','?')}")
    print()

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1053.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
