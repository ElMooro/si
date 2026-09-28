"""Read-only full native orchestration candidate, twice in isolated storage.

Actual AWS operations are only approved public-original reads and metadata.
No invocation, provider request, private ledger or downstream output is used.
"""
from pathlib import Path
import hashlib,resource,sys,tempfile

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate',
    'aws/ops/checks','aws/shared','aws/ops/staged','aws/ops','tests')]


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import genealogy_native_runtime as native
    import genealogy_native_publication as publication
    import genealogy_research_model as reference
    from genealogy_public_archive import strict_json,canonical
    from test_genealogy_native_publication import DiskStore
    from ops_6312_genealogy_revision_cache import ReadCounter
    client=ReadCounter(boto3.client('s3',region_name='us-east-1',config=Config(
        max_pool_connections=8,connect_timeout=10,read_timeout=30,
        retries={'mode':'standard','total_max_attempts':2})))
    cutoff='2026-09-28T18:48:09.350067+00:00'
    with report('ops_6319_genealogy_native_orchestration') as r:
        with tempfile.TemporaryDirectory(prefix='genealogy-orchestration-') as directory:
            root=Path(directory);store=DiskStore(root/'artifacts')
            cold=native.run(client,store,cutoff,root/'cold');first_gets=client.gets
            if cold['revision_sha256']!='137ff3a3c3920e4ecbbda8c79bf6c80c0f206ef7acaa9e6143fce2815b09f188':
                raise ValueError('Complete reviewed original revision population differs')
            if cold['original_objects']!=3557 or cold['original_bytes']!=129773477:
                raise ValueError('Complete original coverage differs')
            cold_disk=sum(p.stat().st_size for p in (root/'cold').rglob('*') if p.is_file())
            warm=native.run(client,store,cutoff,root/'warm')
            if not cold['published'] or warm['reason']!='identical_head' or cold['packet']!=warm['packet']:
                raise ValueError('Full cold and warm original publication differs')
            if client.gets!=first_gets or warm['cache']['misses']!=0 or warm['restored']['restored']!=3557:
                raise ValueError('Warm run did not exclusively reuse validated original bytes')
            if cold['whole_computation']['files']!=warm['whole_computation']['files']:
                raise ValueError('Complete input/output calculation differs across starts')
            if cold['whole_computation']['files']['membership']['sha256']!='a1b1500fa1490c3f671890bbe327b7882ef87855770f1bebfea8e7026a1e3a1d':
                raise ValueError('Accepted complete chronology changed')
            peak_native=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Independent full frozen compiler comparison after timing/resource
            # capture; diagnostic inventory fields no longer affect identities.
            inputs=strict_json((root/'cold/replayed/inputs.json').read_bytes())
            expected=canonical(reference.compile_frozen(inputs))
            if expected!=(root/'cold/replayed/output.json').read_bytes():
                raise ValueError('Complete independent frozen compiler differs')
            manifest=strict_json(publication.read_ref(store,cold['retained_manifest'],'runs'))
            r.kv(cold=cold,warm={k:v for k,v in warm.items() if k not in ('packet','whole_computation')},
                original_body_reads=first_gets,original_body_bytes=client.bytes,warm_original_body_reads=client.gets-first_gets,
                compiler_files=len(manifest['compilers']),independent_frozen_output_sha256=hashlib.sha256(expected).hexdigest(),
                cold_remaining_scratch_bytes=cold_disk,isolated_artifact_bytes=sum(p.stat().st_size for p in store.root.iterdir()),
                peak_orchestration_runner_rss_kib=peak_native,peak_including_independent_reference_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                actual_aws_writes=0,native_invocations=0,provider_requests=0,learning_ledger_reads=0,
                downstream_output_reads=0,private_account_reads=0,schedule_changes=0,
                scope='Complete fixed originals through fresh listing, warm byte restoration, whole computation, immutable retention, independent replay and final revision reconciliation. All writes isolated to temporary runner files; actual native capacity, durable AWS transport, future growth and page adoption remain unverified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
