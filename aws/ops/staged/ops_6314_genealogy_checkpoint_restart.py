"""Read approved originals; checkpoint/restart only in isolated runner storage."""
from pathlib import Path
import json
import resource
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate',
    'aws/ops/checks','aws/shared','aws/ops/staged','aws/ops','tests')]


def phase(root, cold):
    import boto3
    from botocore.config import Config
    import genealogy_cache_checkpoint as checkpoint
    from genealogy_public_archive import BUCKET, load_inventory
    from genealogy_revision_cache import OriginalCache, revisions, revision_digest
    from genealogy_streamed_pipeline import collect, verify_files
    from ops_6312_genealogy_revision_cache import ReadCounter
    from test_genealogy_cache_checkpoint import DiskStore
    inventory = load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')
    if {p:i['inventory_sha256'] for p,i in inventory.items()} != {
        'data/research-forecasts/captures/':'be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd',
        'data/research-forecasts/records/':'98f21bcc0afe16801b06648c02c15c08c77a01f0c1ef7a4401e4b694083552f2'}:
        raise ValueError('Reviewed population differs')
    expected = {'inputs':'aa2f9c68d0f3fd9a9e5da65ef465bc2648f61c21b6e258df1e045c3ee313fe9f',
                'output':'6b37529b9ca7d1b04c85fbbf4a3bfe973a50a103de2024e04b9003cd6ddf823f',
                'membership':'a1b1500fa1490c3f671890bbe327b7882ef87855770f1bebfea8e7026a1e3a1d'}
    client = ReadCounter(boto3.client('s3',region_name='us-east-1',config=Config(
        max_pool_connections=8,connect_timeout=10,read_timeout=30,
        retries={'mode':'standard','total_max_attempts':2})))
    started = time.monotonic(); before = revisions(client, inventory)
    store = DiskStore(root/'portable-artifacts')  # Only this local fake receives writes.
    name = 'cold' if cold else 'fresh_process'
    if cold:
        cache = OriginalCache(root/'cold.sqlite'); restoration = None
    else:
        cache, restoration = checkpoint.restore(store, BUCKET, before, root/'fresh.sqlite')
        if restoration['restored'] != len(before) or not restoration['checkpoint_found']:
            raise ValueError('Incomplete restored original population')
    try:
        bound = cache.bind(client, before)
        result = collect(bound, inventory, root/name)
        if any(result['files'][key]['sha256'] != digest for key,digest in expected.items()):
            raise ValueError('Whole accepted input or output differs')
        if revisions(client, inventory) != before:
            raise ValueError('Original revision population changed')
        verify_files(root/name, result)
        if cold:
            cutoff = next(iter(inventory.values()))['cutoff']
            ref = checkpoint.snapshot(store, BUCKET, bound, cutoff)
            if revisions(client, inventory) != before:
                raise ValueError('Original revision population changed before checkpoint acceptance')
            if not checkpoint.publish(store, BUCKET, ref):
                raise ValueError('Isolated checkpoint not accepted')
            checkpoint_doc = checkpoint.validate(store, BUCKET, ref)
        else:
            ref = checkpoint.head(store.path(checkpoint.CURRENT).read_bytes())['snapshot']
            checkpoint_doc = checkpoint.validate(store, BUCKET, ref)
            if client.gets != 0 or cache.stats()['hits'] != len(before):
                raise ValueError('Restart reread or omitted original bytes')
        stats = cache.stats()
    finally:
        cache.close()
    return {'phase':name, 'computation':result, 'aws_body_reads':client.gets,
        'aws_body_bytes':client.bytes, 'cache':stats, 'restoration':restoration,
        'revision_inventory_sha256':revision_digest(before),
        'metadata_reconciled_before_and_after':True, 'seconds':round(time.monotonic()-started,3),
        'peak_runner_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'checkpoint':{'reference':ref,'shards':len(checkpoint_doc['shards']),
            'entries':checkpoint_doc['entries'],'compressed_bytes':checkpoint_doc['compressed_bytes'],
            'transport_bytes':sum(p.stat().st_size for p in store.root.iterdir())},
        'isolated_checkpoint_reads':len(store.reads),'isolated_checkpoint_writes':len(store.writes)}


def main():
    from ops_report import report
    with report('ops_6314_genealogy_checkpoint_restart') as r:
        with tempfile.TemporaryDirectory(prefix='genealogy-checkpoint-') as directory:
            root = Path(directory)
            cold = phase(root, True)
            # A separate interpreter has no surviving SQLite connection, imported
            # byte cache or in-memory originals from the cold computation.
            restarted = subprocess.run([sys.executable, str(Path(__file__).resolve()),
                '--isolated-restart', directory], capture_output=True, text=True, check=True, timeout=300)
            warm = json.loads(restarted.stdout)
            if cold['revision_inventory_sha256'] != warm['revision_inventory_sha256']:
                raise ValueError('Original revisions differ across process restart')
            if cold['computation'] != warm['computation']:
                raise ValueError('Restart changed complete computation')
        r.kv(passes=[cold,warm], separate_interpreter_restart=True,
            actual_aws_writes=0,native_invocations=0,provider_requests=0,learning_ledger_reads=0,
            downstream_output_reads=0,private_account_reads=0,schedule_changes=0,
            scope='Complete reviewed public original corpus; neutral JSON checkpoints use isolated runner file storage. Fresh process restores a new known-schema SQLite cache and reruns all validators and calculations. This verifies portable checkpoint integrity and full-corpus parity, not native cold-start capacity, AWS checkpoint persistence, incremental compilation, or public research publication.')


if __name__ == '__main__':
    try:
        if len(sys.argv) == 3 and sys.argv[1] == '--isolated-restart':
            print(json.dumps(phase(Path(sys.argv[2]), False)))
        else:
            main()
    except Exception:
        sys.exit(1)
