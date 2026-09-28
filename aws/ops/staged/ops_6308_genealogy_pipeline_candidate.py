"""Complete original-journal candidate replay; no native invocation or AWS writes."""
from pathlib import Path
import hashlib,io,json,resource,sys,time
ROOT=Path(__file__).resolve().parents[3]


def main():
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks/genealogy_native_candidate','aws/shared','aws/ops')]
    import genealogy_public_archive as archive
    import genealogy_research_model as model
    import genealogy_research_store as store
    import boto3
    from botocore.config import Config
    from ops_report import report
    inventories=archive.load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')
    expected={'data/research-forecasts/captures/':'be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd',
              'data/research-forecasts/records/':'98f21bcc0afe16801b06648c02c15c08c77a01f0c1ef7a4401e4b694083552f2'}
    if {p:v['inventory_sha256'] for p,v in inventories.items()}!=expected:raise ValueError('Reviewed population differs')
    client=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=8,connect_timeout=10,
                        read_timeout=30,retries={'mode':'standard','total_max_attempts':2}))
    with report('ops_6308_genealogy_pipeline_candidate') as r:
        started=time.monotonic();inputs,output=model.collect(client,inventories)
        elapsed=time.monotonic()-started
        actual=hashlib.sha256(archive.canonical(output['membership'])).hexdigest()
        if actual!='a1b1500fa1490c3f671890bbe327b7882ef87855770f1bebfea8e7026a1e3a1d':raise ValueError('Complete accepted membership output differs')
        rows=[{k:v for k,v in row.items() if k!='source_issue'} for row in output['archive']['records']]
        if hashlib.sha256(archive.canonical(rows)).hexdigest()!='43983cf6cb1b77bb66629156d6cb4f637be23a3c404642468283b98201ba9c5d':raise ValueError('Original accepted registration population differs')
        # Exercise retention and publication only against an in-memory fake.
        # The actual IAM client above only reaches the fixed public originals.
        class Memory:
            def __init__(self):self.objects={};self.reads=0;self.writes=0
            def put_object(self,**kw):
                if not store.allowed(kw['Key']):raise ValueError('Fake store path differs')
                if kw.get('IfNoneMatch')!='*':raise ValueError('Fake retention requires first write')
                raw=kw['Body'];key=kw['Key']
                if key in self.objects and self.objects[key]!=raw:raise ValueError('Fake conflict')
                self.objects[key]=raw;self.writes+=1
            def get_object(self,**kw):
                self.reads+=1;raw=self.objects[kw['Key']]
                return {'Body':io.BytesIO(raw),'ContentLength':len(raw)}
        memory=Memory();ref=store.retain(memory,archive.BUCKET,inputs,output)
        replay=store.replay(ref,store.reader(memory,archive.BUCKET))
        if archive.canonical(replay)!=archive.canonical(output):raise ValueError('Complete retained replay differs')
        head={**model.summary(output),'replay':ref}
        peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        r.kv(contract=output['contract'],cutoff=output['generated_at'],coverage=head['coverage'],
             comparison_status_counts=head['comparison_status_counts'],exclusion_counts=head['exclusion_counts'],
             complete_input_sha256=hashlib.sha256(archive.canonical(inputs)).hexdigest(),
             complete_output_sha256=hashlib.sha256(archive.canonical(output)).hexdigest(),
             input_bytes=len(archive.canonical(inputs)),output_bytes=len(archive.canonical(output)),head_bytes=len(archive.canonical(head)),
             compiler_hashes={name:hashlib.sha256(raw).hexdigest() for name,raw in store.compiler_bytes().items()},
             original_collection_seconds=round(elapsed,3),complete_candidate_seconds=round(time.monotonic()-started,3),
             peak_runner_rss_kib=peak,whole_replay_equal=True,fake_retained_artifact_count=len(memory.objects),
             actual_aws_writes=0,native_invocations=0,provider_requests=0,learning_ledger_reads=0,
             downstream_output_reads=0,private_account_reads=0,schedule_changes=0,forecast_qualified=False,
             scope='Complete fixed original public-journal population, compile and in-memory retention/replay. Runner resource measurements are not native Lambda capacity proof. No actual public publication, account data, price request or private learning ledger.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
