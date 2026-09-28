"""Replay complete public originals through isolated retained publication.

AWS is read-only. All checkpoint, compiler, artifact and head writes go to
temporary runner files, using the same conditional transport contract.
"""
from pathlib import Path
import resource
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate',
    'aws/ops/checks','aws/shared','aws/ops/staged','aws/ops','tests')]


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import genealogy_native_publication as publication
    import genealogy_cache_checkpoint as checkpoint
    import genealogy_streamed_pipeline as pipeline
    from genealogy_public_archive import BUCKET,load_inventory,canonical,strict_json
    from genealogy_revision_cache import OriginalCache,revisions,revision_digest
    from ops_6312_genealogy_revision_cache import ReadCounter
    from test_genealogy_native_publication import DiskStore
    inventory=load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')
    if {p:i['inventory_sha256'] for p,i in inventory.items()}!={
        'data/research-forecasts/captures/':'be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd',
        'data/research-forecasts/records/':'98f21bcc0afe16801b06648c02c15c08c77a01f0c1ef7a4401e4b694083552f2'}:
        raise ValueError('Reviewed complete original population differs')
    expected={'inputs':'aa2f9c68d0f3fd9a9e5da65ef465bc2648f61c21b6e258df1e045c3ee313fe9f',
        'output':'6b37529b9ca7d1b04c85fbbf4a3bfe973a50a103de2024e04b9003cd6ddf823f',
        'membership':'a1b1500fa1490c3f671890bbe327b7882ef87855770f1bebfea8e7026a1e3a1d',
        'head':'6384acf06c568659ea888fdafb7fccc1df6178902ceab2720fa0a5134a867c48'}
    client=ReadCounter(boto3.client('s3',region_name='us-east-1',config=Config(
        max_pool_connections=8,connect_timeout=10,read_timeout=30,
        retries={'mode':'standard','total_max_attempts':2})))
    with report('ops_6317_genealogy_retained_publication') as r:
        with tempfile.TemporaryDirectory(prefix='genealogy-retained-') as directory:
            root=Path(directory);started=time.monotonic();before=revisions(client,inventory)
            cache=OriginalCache(root/'originals.sqlite');store=DiskStore(root/'artifacts')
            try:
                bound=cache.bind(client,before)
                result=pipeline.collect(bound,inventory,root/'calculated')
                if any(result['files'][name]['sha256']!=digest for name,digest in expected.items()):
                    raise ValueError('Complete accepted calculation differs')
                if revisions(client,inventory)!=before:raise ValueError('Original revisions changed')
                calculated=time.monotonic()
                original_ref=checkpoint.snapshot(store,BUCKET,bound,next(iter(inventory.values()))['cutoff'])
                ref=publication.retain(store,root/'calculated',result,inventory,original_ref)
            finally:cache.close()
            retained=time.monotonic();before_replay_gets=client.gets
            published=publication.publish(store,ref,root/'replayed')
            replayed=time.monotonic()
            expected_head=strict_json((root/'calculated/head.json').read_bytes())
            if not published['published'] or published['packet']!={**expected_head,'replay':ref}:
                raise ValueError('Retained original publication differs')
            if store.path(publication.CURRENT).read_bytes()!=canonical(published['packet']):
                raise ValueError('Isolated head readback differs')
            if client.gets!=before_replay_gets:raise ValueError('Retained replay fetched live originals')
            if revisions(client,inventory)!=before:raise ValueError('Original revisions changed before acceptance')
            manifest=strict_json(publication.read_ref(store,ref,'runs'))
            r.kv(whole_computation=result,retained_manifest=ref,
                original_checkpoint=original_ref,compiler_files=len(manifest['compilers']),
                publication_head_sha256=publication.sha(store.path(publication.CURRENT).read_bytes()),
                coverage=published['packet']['coverage'],comparison_status_counts=published['packet']['comparison_status_counts'],
                original_objects=len(before),original_body_reads=client.gets,original_body_bytes=client.bytes,
                replay_original_body_reads=client.gets-before_replay_gets,
                revision_inventory_sha256=revision_digest(before),metadata_reconciled_before_and_after=True,
                timings_seconds={'collect':round(calculated-started,3),'retain':round(retained-calculated,3),
                    'replay_and_publish':round(replayed-retained,3),'total':round(replayed-started,3)},
                temporary_disk_bytes=sum(p.stat().st_size for p in root.rglob('*') if p.is_file()),
                isolated_artifact_bytes=sum(p.stat().st_size for p in store.root.iterdir()),
                isolated_write_count=len(store.writes),peak_runner_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                actual_aws_writes=0,native_invocations=0,provider_requests=0,learning_ledger_reads=0,
                downstream_output_reads=0,private_account_reads=0,schedule_changes=0,
                scope='Whole reviewed original corpus, all compiler files, complete calculation and conditional head publication in isolated runner storage. No actual AWS persistence/publication or Lambda runtime capacity claim; native integration and future archive growth remain required.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
