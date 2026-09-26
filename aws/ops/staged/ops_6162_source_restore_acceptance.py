"""Read exact restored ingestion code and normal public research publications."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, subprocess, sys, urllib.request, urllib.error
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops','aws/ops/checks','scripts','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime, bounded


def main():
    clients = [boto3.client(name, region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    with report('ops_6162_source_restore_acceptance') as r:
        results = {}
        for name in ('justhodl-tv-notes-ingest','justhodl-liquidity-pulse','justhodl-us10y-sentinel'):
            expected = subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+name+'/source'], cwd=ROOT, text=True).strip()
            before = runtime(*clients, name)
            if before['receipt'] != {'status':'matched','commit':expected}: raise ValueError('Exact source receipt required')
            if name == 'justhodl-tv-notes-ingest':
                if before['handler_bytes'] != 22974 or before['timeout'] != 300 or before['memory_mb'] != 1024:
                    raise ValueError('Complete restored ingestion reserve required')
                subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/name/'tests/run_tests.py')], cwd=ROOT, check=True)
            results[name] = before
        # Reconstruct existing Pulse public artifacts; this path cannot generate.
        from replay_liquidity_pulse_research import verify, read, store
        pulse = verify(store.strict(read(store.model.CURRENT)))
        if pulse['generated_at'] < '2026-09-26T16:03:00': raise ValueError('New normal Pulse publication required')
        from replay_us10y_sentinel import verify as verify_sentinel
        import sentinel_store
        reader = sentinel_store.reader(clients[1], 'justhodl-dashboard-live')
        raw = reader(sentinel_store.model.CURRENT); packet = sentinel_store.strict(raw)
        if packet.get('contract') != sentinel_store.model.CONTRACT or packet['generated_at'] < '2026-09-26T16:20:00':
            raise ValueError('New normal Sentinel publication required')
        sentinel = verify_sentinel(packet, reader)
        request = urllib.request.Request('https://justhodl.ai/'+sentinel_store.model.CURRENT+'?exact=1&nogen=1',
            headers={'User-Agent':'justhodl-verify-release/1.0','Cache-Control':'no-cache'})
        if bounded(urllib.request.urlopen(request, timeout=40)) != raw: raise ValueError('Anonymous Sentinel differs')
        key = packet['original_sources']['SP500']['sources']['observations']['evidence']['key']
        for origin in ('https://justhodl-data-proxy.raafouis.workers.dev/', 'https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/'):
            try:
                with urllib.request.urlopen(urllib.request.Request(origin+key, method='HEAD'), timeout=30):
                    raise ValueError('Source archive is anonymously readable')
            except urllib.error.HTTPError as error:
                if error.code != 403: raise ValueError('Archive protection not established') from None
        for name, before in results.items():
            if runtime(*clients, name) != before: raise ValueError('Runtime changed during read-only acceptance')
        r.kv(checked_at=datetime.now(timezone.utc).isoformat(), actual_runtimes=results,
             normal_pulse=pulse, normal_sentinel=sentinel, sentinel_public_sha256=hashlib.sha256(raw).hexdigest(),
             archive_protection='anonymous_edge_and_s3_denied', native_invocations=0, consumer_invocations=0, provider_requests=0,
             personal_account_reads=0, public_writes=0, notifications_sent=0, schedules_changed=0,
             scope='Exact code packages and existing research archives only; no TV notes or personal portfolio access.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
