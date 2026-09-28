"""Read all fixed public captures; compile using only a temporary runner spool."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import resource,sys,tempfile,time
ROOT=Path(__file__).resolve().parents[3]


def main():
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','aws/shared','aws/ops')]
    import boto3
    from botocore.config import Config
    from ops_report import report
    from genealogy_public_archive import load_inventory,read_object
    from genealogy_capture_timing import capture_context
    from genealogy_spooled_timing import compile_spooled
    inventory=load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')['data/research-forecasts/captures/']
    if inventory['inventory_sha256']!='be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd':raise ValueError('Reviewed population differs')
    client=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=8,connect_timeout=10,
                        read_timeout=30,retries={'mode':'standard','total_max_attempts':2}))
    def read(meta):
        doc,receipt=read_object(client,meta['key'],meta)
        return capture_context(doc,receipt)
    # Bounded batches prevent ThreadPoolExecutor.map from collecting the whole
    # result set while the slower disk writer consumes its first contexts.
    def contexts():
        with ThreadPoolExecutor(max_workers=8) as pool:
            for offset in range(0,len(inventory['objects']),8):
                yield from pool.map(read,inventory['objects'][offset:offset+8])
    with report('ops_6311_genealogy_spooled_timing') as r:
        started=time.monotonic()
        with tempfile.TemporaryDirectory(prefix='genealogy-public-timing-') as directory:
            path=Path(directory)
            result=compile_spooled(contexts(),inventory['cutoff'],path/'spool.sqlite',path/'output.json')
        if result['complete_output_sha256']!='a1b1500fa1490c3f671890bbe327b7882ef87855770f1bebfea8e7026a1e3a1d':raise ValueError('Complete accepted output differs')
        r.kv(result=result,whole_predecessor_output_equal=True,original_bytes=inventory['total_bytes'],
             elapsed_seconds=round(time.monotonic()-started,3),peak_runner_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
             actual_aws_writes=0,native_invocations=0,provider_requests=0,learning_ledger_reads=0,
             downstream_output_reads=0,private_account_reads=0,schedule_changes=0,
             scope='All fixed approved public captures. Temporary SQLite ordering and streaming canonical output reproduce every predecessor value. No native publication, incremental cache, private ledger, account data or portfolio authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
