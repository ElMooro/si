"""Read-only exact native package, ordinary publication and original replay.

Reads only the public producer and its typed retained evidence. No private
request journal, provider request, native invocation or downstream live head.
"""
from pathlib import Path
import hashlib
import json
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
FN = 'justhodl-fifx-vol-migration'
sys.path[:0] = [str(ROOT/'aws/lambdas'/FN/'source'),
               str(ROOT/'aws/shared'), str(ROOT/'aws/ops/checks'), str(ROOT/'aws/ops')]
import fifx_model as model
import fifx_store as store
from market_runtime_evidence import runtime, bounded

BUCKET = 'justhodl-dashboard-live'
EXPECTED = 'bdfd0c10bcafd6d41e47f59512057c7f928d9b45'
PUBLIC_SHA = 'aabac40452da9461ea80b7edff11550e71b1e6e53a02bec789e11bef767805af'
CODE_SHA = 'tB2v9661hv6YbTU2HKNBDAgsX/A5ZmerqL5miRODwgA='


def check_runtime(value):
    if value['receipt'] != {'status':'matched','commit':EXPECTED} or value['code_sha256'] != CODE_SHA:
        raise ValueError('Exact original native package and receipt required')
    expected = {'source_files_checked':14, 'timeout':900, 'memory_mb':2048,
        'handler_bytes':1496, 'function_name':FN, 'runtime':'python3.12',
        'handler':'lambda_function.lambda_handler', 'architectures':['x86_64'],
        'ephemeral_storage_mb':512}
    if any(value.get(k) != v for k,v in expected.items()):
        raise ValueError('Original FI/FX operating settings differ')
    if value['schedules'] != [{'kind':'EventBridge Scheduler','name':'justhodl-fifx-vol-daily',
        'state':'ENABLED','expression':'cron(20 21 ? * MON-FRI *)','timezone':'UTC',
        'native_targets':1,'group':'default'}]:
        raise ValueError('Original FI/FX schedule differs')


def original_reader(client):
    """No predecessor/private paths or live consumer heads are reachable."""
    read = store.reader(client, BUCKET)
    totals = {'objects':0,'bytes':0}
    def original(key):
        if not re.fullmatch(r'data/(?:evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz|'
            r'report-research/(?:runs|inputs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py)|'
            r'fifx-vol-research/(?:runs|inputs|outputs|compilers|snapshots|series|views|originals|receipts)/[a-f0-9]{64}\.(?:json|py|bin))', key):
            raise ValueError('Unreviewed original replay path')
        raw = read(key)
        totals['objects'] += 1; totals['bytes'] += len(raw)
        return raw
    return original, totals


def publication(raw, read):
    packet = store.strict(raw)
    if hashlib.sha256(raw).hexdigest() != PUBLIC_SHA or packet.get('contract') != model.CONTRACT:
        raise ValueError('Whole reviewed normal public packet required')
    if packet.get('generated_at') != '2026-09-28T21:21:57.335781+00:00':
        raise ValueError('Original 21:20 scheduled publication required')
    proof = store.replay(packet, read)
    return {'status':'complete_native_originals_replayed', 'generated_at':packet['generated_at'],
        'contract':packet['contract'],'version':packet['version'],'bytes':len(raw),
        'sha256':hashlib.sha256(raw).hexdigest(),'quality':packet['quality'],
        'decision':packet['decision'],'acquisition':packet['acquisition'],
        'series_status':{sid:row['quality']['status'] for sid,row in packet['series'].items()},
        'original_replay':proof,
        'all_sources_usable':all(row['current'] is not None for row in packet['series'].values()),
        'forecast_qualified':False,'sizing_qualified':False}


def main():
    import boto3
    from ops_report import report
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    clients = [boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6315_fifx_normal_publication') as r:
        before = runtime(*clients,FN); check_runtime(before)
        obj = clients[1].get_object(Bucket=BUCKET,Key=model.CURRENT)
        raw = bounded(obj['Body'])
        if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != len(raw):
            raise ValueError('Complete public original required')
        read, counts = original_reader(clients[1])
        result = publication(raw, read)
        if runtime(*clients,FN) != before:
            raise ValueError('Actual runtime changed during replay')
        r.kv(expected_commit=EXPECTED,actual_runtime=before,native_publication=result,
            retained_read_counts=counts,native_invocations=0,provider_requests=0,private_journal_reads=0,
            learning_ledger_reads=0,downstream_live_head_reads=0,private_account_reads=0,
            public_writes=0,schedule_changes=0,
            scope='Exact normal native publication and complete original/compiler/arithmetic reproduction. Source availability is reported separately: successful replay cannot qualify absent sources or identity mismatches. No acquisition retry or forced publication.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
