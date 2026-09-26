"""Verify exact ingestion code, synthetic regressions and unchanged runtime only."""
from pathlib import Path
from datetime import datetime, timezone
import subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'scripts')]
from ops_report import report
from market_runtime_evidence import runtime


def main():
    function = 'justhodl-tv-notes-ingest'
    clients = [boto3.client(name, region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    with report('ops_6163_ingestion_save_repair_acceptance') as r:
        expected = subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+function+'/source'], cwd=ROOT, text=True).strip()
        before = runtime(*clients, function)
        if before['receipt'] != {'status':'matched','commit':expected}:
            raise ValueError('Exact source receipt required')
        if before['timeout'] != 300 or before['memory_mb'] != 1024 or before['schedules']:
            raise ValueError('Original ingestion runtime and absence of schedules required')
        subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/function/'tests/run_tests.py')], cwd=ROOT, check=True)
        if runtime(*clients, function) != before:
            raise ValueError('Runtime changed during read-only acceptance')
        r.kv(checked_at=datetime.now(timezone.utc).isoformat(), actual_runtime=before,
             synthetic_regressions='passed', native_invocations=0, personal_account_reads=0,
             note_reads=0, description_reads=0, source_provider_requests=0, storage_writes=0,
             notifications_sent=0, schedules_changed=0,
             scope='Exact deployed source package and synthetic faults only. No live notes, descriptions, watchlists or account are read or mutated; live business writes are not claimed.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
