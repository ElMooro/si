"""Read-only complete public-original replay with a disposable runner cache.

Two full metadata checks per pass. No native invocation, AWS data write,
downstream output, private ledger, account, provider or credential read.
"""
from pathlib import Path
import gc
import hashlib
import resource
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[3]


class ReadCounter:
    def __init__(self, client):
        self.client = client
        self.gets = self.bytes = 0
        self.lock = threading.Lock()

    def get_paginator(self, name):
        return self.client.get_paginator(name)

    def head_object(self, **kwargs):
        return self.client.head_object(**kwargs)

    def get_object(self, **kwargs):
        obj = self.client.get_object(**kwargs)
        with self.lock:
            self.gets += 1
            self.bytes += obj['ContentLength']
        return obj


def main():
    sys.path[:0] = [str(ROOT/p) for p in (
        'aws/ops/checks/genealogy_native_candidate', 'aws/ops/checks', 'aws/shared', 'aws/ops')]
    import boto3
    from botocore.config import Config
    from ops_report import report
    from genealogy_public_archive import load_inventory, canonical
    from genealogy_research_model import collect, summary
    from genealogy_revision_cache import OriginalCache, revisions, revision_digest, CONTRACT
    inventory = load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')
    if {p: i['inventory_sha256'] for p, i in inventory.items()} != {
        'data/research-forecasts/captures/': 'be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd',
        'data/research-forecasts/records/': '98f21bcc0afe16801b06648c02c15c08c77a01f0c1ef7a4401e4b694083552f2'}:
        raise ValueError('Reviewed population differs')
    client = ReadCounter(boto3.client('s3', region_name='us-east-1', config=Config(
        max_pool_connections=8, connect_timeout=10, read_timeout=30,
        retries={'mode': 'standard', 'total_max_attempts': 2})))
    expected_input = 'aa2f9c68d0f3fd9a9e5da65ef465bc2648f61c21b6e258df1e045c3ee313fe9f'
    expected_output = '6b37529b9ca7d1b04c85fbbf4a3bfe973a50a103de2024e04b9003cd6ddf823f'
    with report('ops_6312_genealogy_revision_cache') as r:
        passes = []
        with tempfile.TemporaryDirectory(prefix='genealogy-revision-cache-') as directory:
            path = Path(directory)/'originals.sqlite'
            for name in ('cold_originals', 'reopened_cache'):
                started = time.monotonic(); before_gets = client.gets; before_bytes = client.bytes
                before = revisions(client, inventory)
                cache = OriginalCache(path)
                try:
                    inputs, output = collect(cache.bind(client, before), inventory, workers=8)
                    input_sha = hashlib.sha256(canonical(inputs)).hexdigest()
                    output_sha = hashlib.sha256(canonical(output)).hexdigest()
                    if input_sha != expected_input or output_sha != expected_output:
                        raise ValueError('Complete accepted population or output differs')
                    if revisions(client, inventory) != before:
                        raise ValueError('Original revision set changed during compilation')
                    coverage = summary(output)['coverage']
                    stats = cache.stats()
                    del inputs, output
                    gc.collect()
                finally:
                    cache.close()
                result = {'pass': name, 'complete_input_sha256': input_sha,
                    'complete_output_sha256': output_sha, 'whole_output_equal': True,
                    'revision_inventory_sha256': revision_digest(before), 'original_objects': len(before),
                    'aws_body_reads': client.gets-before_gets, 'aws_body_bytes': client.bytes-before_bytes,
                    'metadata_reconciled_before_and_after': True, 'cache': stats, 'coverage': coverage,
                    'seconds': round(time.monotonic()-started, 3), 'temporary_database_bytes': path.stat().st_size}
                if name == 'reopened_cache' and (result['aws_body_reads'] != 0 or stats['hits'] != len(before)):
                    raise ValueError('Warm replay reread or omitted originals')
                passes.append(result)
        if passes[0]['revision_inventory_sha256'] != passes[1]['revision_inventory_sha256']:
            raise ValueError('Revision population changed between passes')
        r.kv(contract=CONTRACT, passes=passes,
            peak_runner_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            actual_aws_writes=0, native_invocations=0, provider_requests=0, learning_ledger_reads=0,
            downstream_output_reads=0, private_account_reads=0, schedule_changes=0,
            scope='All approved fixed original public-journal objects. Both passes run every current validator and reproduce the entire accepted candidate. Cache exists only in a temporary runner directory; no Lambda cache durability, native resource qualification or public Genealogy deployment is asserted.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
