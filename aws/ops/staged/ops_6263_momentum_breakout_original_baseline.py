"""Retain the complete Momentum Breakout research producer and only their declared public packets.

No native producer import/invocation, provider acquisition, account/configuration
secret read, public overwrite, schedule change or notification. Writes are
immutable private originals and the ordinary operation report only.
"""
from pathlib import Path
from datetime import datetime, timezone
import ast
from decimal import Decimal, InvalidOperation
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
PRIVATE = 'audit-private/20260909-originals/momentum-breakout-research/'
SOURCES = {'justhodl-momentum-breakout': ('cd69689147e8e2c0a697707f17cb32bf7265c304b49e7311cbf63ce5b2cdfdc1', 'data/momentum-breakout.json'), 'justhodl-best-ideas': ('ae7e5218b372208db69c22e8d390ea9ccd70646aeabe1189202e74fbc2fcde6e', None), 'justhodl-convergence-radar': ('8fbb2e5bd2b4fd0bea23718b65ccdac6e984037ecab388935547efdf5289396f', None), 'justhodl-compound-aggregator': ('e8d69925199cda71a11acfc23bede69bc39202453b076c74b4d29b2f959a185b', None), 'justhodl-momentum-leaders': ('460089bf12a7bb0c0ec2ca2cfc9388e91c4587c56f36a7509ffc8681546be1f8', None), 'justhodl-master-ranker': ('e14c1f6cf2a270efeec7113132c8f032f8273644dfef87e7a430029945a2f464', None), 'justhodl-opportunity-screener': ('76b49f3c3a27b08611e44f1c2a6d021b7349381906bec867577c210ff512b541', None), 'justhodl-velocity-acceleration': ('d4a7aeefddd2e2e1ec76ea64c6c1d93fc5f3bd3aef881b67f94e985bf85ba504', None)}
KEYS = tuple(value[1] for value in SOURCES.values() if value[1] is not None)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def source_check(function, raw):
    if function not in SOURCES or sha(raw) != SOURCES[function][0]:
        raise ValueError('Unreviewed whole Momentum Breakout producer')
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
        raise ValueError('Undeclared Momentum Breakout research object')
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
    cfg=lam.get_function_configuration(FunctionName='justhodl-momentum-breakout')
    env=(cfg.get('Environment') or {}).get('Variables') or {}
    limits={}
    for name,default in (('N_WORKERS',12),('MAX_TICKERS',600),('TIMEOUT_BUDGET_S',260)):
        value=env.get(name,str(default))
        if not isinstance(value,str) or not value.isascii() or not value.isdecimal() or not 1<=int(value)<=100000:
            raise ValueError('Declared non-secret numeric control invalid')
        limits[name]=int(value)
    value=env.get('MIN_DOLLAR_VOL','5000000')
    if not isinstance(value,str):raise ValueError('Original nominal acquisition threshold malformed')
    try:threshold=Decimal(value)
    except InvalidOperation:raise ValueError('Original nominal acquisition threshold malformed')
    if not threshold.is_finite() or not 0<=threshold<=Decimal('1e20'):raise ValueError('Original nominal acquisition threshold out of bounds')
    limits['MIN_DOLLAR_VOL']=str(threshold)
    if env.get('S3_BUCKET',BUCKET)!=BUCKET or env.get('S3_KEY','data/momentum-breakout.json')!='data/momentum-breakout.json':
        raise ValueError('Actual producer output differs from reviewed public ownership')
    return {'request_limits':limits,'public_head':'data/momentum-breakout.json','bucket':BUCKET}


def main():
    for test in ('tests/ops/test_momentum_breakout_original_baseline.py', 'tests/test_shipping_consumer_baseline.py'):
        subprocess.run([sys.executable, str(ROOT / test)], cwd=ROOT, check=True)
    checked = {fn: source_check(fn, (ROOT / 'aws/lambdas' / fn / 'source/lambda_function.py').read_bytes())
               for fn in SOURCES}
    clients = {name: boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')}
    with report('ops_6263_momentum_breakout_original_baseline') as r:
        s3 = clients['s3']
        settings = producer_settings(clients['lambda'])
        producers = {fn: runtime(clients['lambda'], s3, clients['events'], clients['scheduler'], fn) for fn in SOURCES}
        originals = {key: capture(s3, key) for key in KEYS}
        baseline = {'contract': 'momentum-breakout-original-baseline.v1', 'captured_at': datetime.now(timezone.utc).isoformat(),
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
             scope='Whole deployed producer and seven consumer packages; one exact existing public Momentum Breakout producer research packet only. No consumer output or private state is read. Provider response originals, acquisition completeness, original vintages, donor quality, common input ancestry, category joins, source timing, score calibration and signal performance remain unverified.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
