"""Read-only verification of the normal Share Flows publication and all originals."""
from pathlib import Path
from datetime import datetime, timezone
import json, subprocess, sys, time, urllib.request
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import capital_structure_source as source
import capital_structure_store as store
import capital_structure_producer as producer
import capital_structure_research as model
import capital_structure_arithmetic as independent

FUNCTION = 'justhodl-share-flows'
BUCKET = 'justhodl-dashboard-live'
EXPECTED = 'e0abbfe1cdbe0c47c645d257176e2df647fb27c0'
EARLIEST = '2026-09-26T13:35:00+00:00'
# Acquisition claims share this research-only prefix with native journals.
# Bound the initial accepted population plus one complete subsequent cycle.
MAX_REQUESTS = 2*11305+128
MAX_RECENT_REQUESTS = 64


def native_execution(s3, reference):
    matches = []; scanned = 0; recent = 0
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=source.PRIVATE+'requests/'):
        for item in page.get('Contents', []):
            scanned += 1
            if scanned > MAX_REQUESTS: raise ValueError('Research request inventory bound exceeded')
            if item['LastModified'] < source.clock(EARLIEST): continue
            recent += 1
            if recent > MAX_RECENT_REQUESTS: raise ValueError('Recent request read bound exceeded')
            value = source.strict(producer.raw(s3, BUCKET, item['Key']))
            if not value.get('execution_id'): continue  # Runner-only qualification is not a native execution.
            if value.get('status') == 'complete' and value.get('result', {}).get('published') is True and value['result'].get('replay') == reference:
                elapsed = (source.clock(value['finished_at'])-source.clock(value['started_at'])).total_seconds()
                if source.clock(value['started_at']) < source.clock(EARLIEST) or not 0 < elapsed < 900:
                    raise ValueError('Normal native execution clock or runtime differs')
                matches.append({'journal_key': item['Key'], 'execution_id': value['execution_id'],
                    'started_at': value['started_at'], 'finished_at': value['finished_at'], 'elapsed_seconds': elapsed})
    if len(matches) != 1: raise ValueError('Exactly one completed normal native publication required')
    return matches[0]


def main():
    subprocess.run([sys.executable, str(ROOT/'tests/test_share_flows_normal_publication_evidence.py')], cwd=ROOT, check=True)
    subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')], cwd=ROOT, check=True)
    s3 = boto3.client('s3', region_name='us-east-1', config=Config(max_pool_connections=12, retries={'max_attempts': 2}))
    lam, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 'events', 'scheduler'))
    with report('ops_6154_share_flows_complete_normal_acceptance') as r:
        before = runtime(lam, s3, events, scheduler, FUNCTION)
        if before['receipt'] != {'status': 'matched', 'commit': EXPECTED}: raise ValueError('Exact native code receipt required')
        head = s3.get_object(Bucket=BUCKET, Key=producer.CURRENT); raw = store.bounded(head['Body'])
        packet = source.strict(raw)
        if packet.get('contract') != model.CONTRACT: raise ValueError('Normal native publication still pending')
        if head['LastModified'] < source.clock(EARLIEST): raise ValueError('Head predates the expected normal publication')
        ref = packet['replay']
        if not producer.current_matches(packet, ref): raise ValueError('Public output digest differs')
        execution = native_execution(s3, ref)
        started = time.monotonic(); deadline = started+2700
        def budget():
            if time.monotonic() > deadline: raise TimeoutError('Complete read-only replay budget exceeded')
        read = store.reader(s3, BUCKET, before_read=budget)
        compiled = store.replay(ref, read)
        if source.encoded(compiled['packet']) != source.encoded({k:v for k,v in packet.items() if k!='replay'}):
            raise ValueError('Complete native output differs from original reconstruction')
        run = store.verified_run(ref, read); inputs = store.checked(run['input'], 'inputs', read)
        proof = independent.verify(inputs['source_manifest'], inputs['identity_capture'], compiled, read)
        budget()
        if packet['reported_names'] != 1615 or packet['provider_responses'] != 11305 or packet['provider_rows'] != 59256:
            raise ValueError('Accepted initial complete population differs')
        if proof['all_original_rows_conserved'] is not True or proof['original_rows_checked'] != packet['provider_rows']:
            raise ValueError('Independent original coverage differs')
        for flag, expected in model.FLAGS.items():
            if type(packet.get(flag)) is not type(expected) or packet.get(flag) != expected:
                raise ValueError('Research authority differs')
        public = urllib.request.Request('https://justhodl.ai/'+producer.CURRENT+'?exact=1&nogen=1',
            headers={'User-Agent': 'JustHodl-research-acceptance/1.0', 'Cache-Control': 'no-cache'})
        with urllib.request.urlopen(public, timeout=40) as response:
            public_raw = store.bounded(response)
        if public_raw != raw: raise ValueError('Public edge differs from the complete S3 head')
        if producer.raw(s3, BUCKET, producer.CURRENT) != raw: raise ValueError('Head changed during replay; review its newer run')
        after = runtime(lam, s3, events, scheduler, FUNCTION)
        if after != before: raise ValueError('Native package or timing changed during replay')
        r.kv(expected_commit=EXPECTED, actual_runtime=before, execution=execution,
             generated_at=packet['generated_at'], public_head_last_modified=head['LastModified'].isoformat(),
             source_acquisition_started_at=packet['source_acquisition_started_at'],
             source_acquisition_completed_at=packet['source_acquisition_completed_at'],
             public_bytes=len(raw), public_sha256=source.sha(raw), replay=ref,
             counts={k:packet[k] for k in ('reported_names','provider_responses','provider_rows','empty_responses')},
             independent_proof=proof, complete_original_replay=True, public_edge_matches=True,
             elapsed_replay_seconds=round(time.monotonic()-started, 3), native_normal_publication_verified=True,
             producer_invocations=0, consumer_invocations=0, provider_requests=0, private_account_reads=0,
             public_writes=0, ready_writes=0, notifications_sent=0, schedules_changed=0,
             original_sec_filings_verified=False, historical_security_continuity_verified=False,
             forecast_qualified=False, sizing_qualified=False,
             scope='Complete initial vendor/SEC-identity snapshot and normal native publication; recurring refresh and predictive qualification remain separate.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
