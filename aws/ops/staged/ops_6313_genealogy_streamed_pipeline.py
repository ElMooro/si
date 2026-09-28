"""Read all fixed public originals; verify complete streamed candidate parity."""
from pathlib import Path
import resource
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]


def main():
    sys.path[:0] = [str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate',
        'aws/ops/checks','aws/shared','aws/ops/staged','aws/ops')]
    import boto3
    from botocore.config import Config
    from ops_report import report
    from genealogy_public_archive import load_inventory
    from genealogy_revision_cache import OriginalCache, revisions, revision_digest
    from genealogy_streamed_pipeline import collect, verify_files
    from ops_6312_genealogy_revision_cache import ReadCounter
    inventory = load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')
    if {p:i['inventory_sha256'] for p,i in inventory.items()} != {
        'data/research-forecasts/captures/':'be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd',
        'data/research-forecasts/records/':'98f21bcc0afe16801b06648c02c15c08c77a01f0c1ef7a4401e4b694083552f2'}:
        raise ValueError('Reviewed complete population differs')
    expected = {'inputs':'aa2f9c68d0f3fd9a9e5da65ef465bc2648f61c21b6e258df1e045c3ee313fe9f',
                'output':'6b37529b9ca7d1b04c85fbbf4a3bfe973a50a103de2024e04b9003cd6ddf823f',
                'membership':'a1b1500fa1490c3f671890bbe327b7882ef87855770f1bebfea8e7026a1e3a1d'}
    client = ReadCounter(boto3.client('s3',region_name='us-east-1',config=Config(
        max_pool_connections=8,connect_timeout=10,read_timeout=30,
        retries={'mode':'standard','total_max_attempts':2})))
    with report('ops_6313_genealogy_streamed_pipeline') as r:
        passes = []
        with tempfile.TemporaryDirectory(prefix='genealogy-streamed-pipeline-') as temp:
            root = Path(temp)
            for name in ('cold_originals','reopened_cache'):
                started = time.monotonic(); before_gets = client.gets; before_bytes = client.bytes
                before = revisions(client,inventory)
                cache = OriginalCache(root/'originals.sqlite')
                try:
                    result = collect(cache.bind(client,before),inventory,root/name)
                    if any(result['files'][key]['sha256'] != sha for key,sha in expected.items()):
                        raise ValueError('Whole accepted input or output differs')
                    if revisions(client,inventory) != before:
                        raise ValueError('Original revision population changed during computation')
                    verify_files(root/name,result)
                    stats = cache.stats()
                finally:
                    cache.close()
                if name == 'reopened_cache' and (client.gets-before_gets != 0 or stats['hits'] != len(before)):
                    raise ValueError('Reopened replay reread or omitted originals')
                passes.append({'pass':name,'computation':result,'aws_body_reads':client.gets-before_gets,
                    'aws_body_bytes':client.bytes-before_bytes,'cache':stats,
                    'revision_inventory_sha256':revision_digest(before),'metadata_reconciled_before_and_after':True,
                    'seconds':round(time.monotonic()-started,3),
                    'single_computation_disk_bytes':sum(p.stat().st_size for p in (root/name).iterdir())+(root/'originals.sqlite').stat().st_size})
        if passes[0]['revision_inventory_sha256'] != passes[1]['revision_inventory_sha256']:
            raise ValueError('Revision population changed between passes')
        r.kv(passes=passes,peak_runner_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            actual_aws_writes=0,native_invocations=0,provider_requests=0,learning_ledger_reads=0,
            downstream_output_reads=0,private_account_reads=0,schedule_changes=0,
            scope='Complete fixed public original population. Every accepted input/output byte is reproduced using disk-backed capture history. Record reconciliation and membership summaries remain in memory with explicit artifact limits. No persistent Lambda cache, native rollout, archive-growth qualification or public publication is asserted.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
