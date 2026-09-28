"""Retain the complete Volatility Squeeze research producer and only their declared public packets.

No native producer import/invocation, provider acquisition, account/configuration
secret read, public overwrite, schedule change or notification. Writes are
immutable private originals and the ordinary operation report only.
"""
from pathlib import Path
from datetime import datetime, timezone
import ast
import hashlib
import json
import subprocess
import sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded
from ops_6204_shipping_consumer_baseline import runtime
import retained_access_evidence as access

BUCKET = 'justhodl-dashboard-live'
PRIVATE = 'audit-private/20260909-originals/volatility-squeeze-research/'
SOURCES = {'justhodl-volatility-squeeze-hunter': ('01b674d9d396b1432be4a3624d606e46e983d326b79b18be68924bcf3db006de', 'data/volatility-squeeze.json'), 'justhodl-compound-aggregator': ('a32bed3d21d7e925b420993f84614996db7815657692f2482cfaa207b78ec340', None), 'justhodl-options-confluence': ('1464796fea7775a4e8be5f8f9c320adeb2cb1d897dd9025800904323e58bd938', None), 'justhodl-master-ranker': ('ec22b21fa85cdd7ff415838d4df35b8ca2beeafc959a1643ab149f42062f3306', None), 'justhodl-katlin': ('ed3bf50aa112b868a22c40a034f9d59d483efa31d16349331cdbe8a5930406f4', None), 'justhodl-ai-infra-stack': ('1f75d968478b2a4729b5df82ddc3b0fc6f24d74a7006d78b466da9de250ff5e4', None), 'justhodl-theme-second-wave': ('9e8cf5fadfccbb29482ffb0dd5b54100ba0dc5bd0a2070e99519b0d682bdf553', None)}
KEYS = tuple(value[1] for value in SOURCES.values() if value[1] is not None)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def source_check(function, raw):
    if function not in SOURCES or sha(raw) != SOURCES[function][0]:
        raise ValueError('Unreviewed whole Volatility Squeeze producer')
    constants = {n.value for n in ast.walk(ast.parse(raw))
                 if isinstance(n, ast.Constant) and isinstance(n.value, str)}
    if SOURCES[function][1] is not None and SOURCES[function][1] not in constants:
        raise ValueError('Pinned source does not declare the research packet')
    return {'bytes': len(raw), 'sha256': sha(raw), 'imported_or_executed': False}


def retain(s3, raw):
    if not isinstance(raw, bytes) or len(raw) > 64 * 1024 * 1024:
        raise ValueError('Complete bounded original required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'bytes': len(raw), 'sha256': sha(raw)}
    try:
        s3.put_object(Bucket=BUCKET, Key=ref['key'], Body=raw, IfNoneMatch='*',
                      ContentType='application/octet-stream', CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in (
                '409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    obj = s3.get_object(Bucket=BUCKET, Key=ref['key'])
    back = bounded(obj['Body'])
    if obj.get('ContentLength') != len(back) or back != raw:
        raise ValueError('Complete immutable readback differs')
    return ref


def capture(s3, key):
    if key not in KEYS:
        raise ValueError('Undeclared Volatility Squeeze research object')
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('404', 'NoSuchKey'):
            return {'status': 'missing'}
        raise
    raw = bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != len(raw) or not obj.get('ETag'):
        raise ValueError('Complete versioned source required')
    return {'status': 'whole_object_retained', 'original': retain(s3, raw),
            'etag': obj['ETag'], 'last_modified': obj['LastModified'].isoformat(),
            'original_provider_verified': False}


def producer_settings(lam):
    # Only these declared non-secret controls leave the runner. The Lambda API
    # includes an environment dictionary; never serialize or retain the rest.
    cfg=lam.get_function_configuration(FunctionName='justhodl-volatility-squeeze-hunter')
    env=(cfg.get('Environment') or {}).get('Variables') or {}
    limits={}
    for name,default in (('N_WORKERS',12),('MAX_TICKERS',600),('TIMEOUT_BUDGET_S',260)):
        value=env.get(name,str(default))
        if not isinstance(value,str) or not value.isascii() or not value.isdecimal() or not 1<=int(value)<=100000:
            raise ValueError('Declared non-secret numeric control invalid')
        limits[name]=int(value)
    if env.get('S3_BUCKET',BUCKET)!=BUCKET or env.get('S3_KEY','data/volatility-squeeze.json')!='data/volatility-squeeze.json':
        raise ValueError('Actual producer output differs from reviewed public ownership')
    return {'request_limits':limits,'public_head':'data/volatility-squeeze.json','bucket':BUCKET}


def main():
    for test in ('tests/ops/test_volatility_squeeze_original_baseline.py', 'tests/test_shipping_consumer_baseline.py'):
        subprocess.run([sys.executable, str(ROOT / test)], cwd=ROOT, check=True)
    checked = {fn: source_check(fn, (ROOT / 'aws/lambdas' / fn / 'source/lambda_function.py').read_bytes())
               for fn in SOURCES}
    clients = {name: boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')}
    with report('ops_6261_volatility_squeeze_original_baseline') as r:
        s3 = clients['s3']
        settings = producer_settings(clients['lambda'])
        producers = {fn: runtime(clients['lambda'], s3, clients['events'], clients['scheduler'], fn) for fn in SOURCES}
        originals = {key: capture(s3, key) for key in KEYS}
        baseline = {'contract': 'volatility-squeeze-original-baseline.v1', 'captured_at': datetime.now(timezone.utc).isoformat(),
                    'snapshot_atomic': False, 'source_checks': checked, 'producers': producers, 'captures': originals, 'producer_settings': settings}
        ref = retain(s3, json.dumps(baseline, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())
        protected = [ref['key']]
        protected.extend(p['whole_zip']['key'] for p in producers.values() if 'whole_zip' in p)
        protected.extend(row['original']['key'] for row in originals.values() if 'original' in row)
        privacy = access.summarize([access.check(key) for key in protected])
        if not privacy['all_denied']:
            raise ValueError('Original code and retained research must remain private')
        summaries = {}
        for fn, actual in producers.items():
            summaries[fn] = {k: v for k, v in actual.items() if k not in ('inventory', 'repository_sources')}
            if 'inventory' in actual:
                summaries[fn].update({k: actual['inventory'][k] for k in ('code_matches_repository', 'source_files_checked', 'source_differences')})
        if producer_settings(clients['lambda']) != settings:raise ValueError('Original acquisition controls changed during capture')
        r.kv(baseline=ref, actual_producers=summaries, producer_settings=settings, source_checks=checked, originals=originals, **privacy,
             native_invocations=0, provider_requests=0, account_reads=0, learning_log_reads=0, notifications_sent=0,
             public_writes=0, history_writes=0, schedule_changes=0,
             scope='Whole deployed producer and six consumer packages; one exact existing public Volatility Squeeze producer research packet only. No consumer output or private state is read. Provider response originals, acquisition completeness, original vintages, donor quality, common input ancestry, category joins, source timing, score calibration and signal performance remain unverified.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
