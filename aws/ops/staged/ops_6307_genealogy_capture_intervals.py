"""Read every fixed public capture and check collection-order ambiguity offline."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import hashlib,sys,time
ROOT=Path(__file__).resolve().parents[3]
CAPTURE_HASH='be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd'


def main():
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','aws/shared','aws/ops')]
    from genealogy_public_archive import load_inventory,read_object,canonical
    import genealogy_capture_timing as model
    from ops_report import report
    import boto3
    from botocore.config import Config
    inventories=load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')
    inventory=inventories['data/research-forecasts/captures/']
    if inventory['inventory_sha256']!=CAPTURE_HASH:raise ValueError('Reviewed capture population differs')
    client=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=8,connect_timeout=10,
                         read_timeout=30,retries={'mode':'standard','total_max_attempts':2}))
    def read(meta):
        document,evidence=read_object(client,meta['key'],meta)
        return model.capture_context(document,evidence)
    with report('ops_6307_genealogy_capture_intervals') as r:
        started=time.monotonic()
        with ThreadPoolExecutor(max_workers=8) as pool:contexts=list(pool.map(read,inventory['objects']))
        result=model.compile_contexts(contexts,inventory['cutoff'])
        # Store the complete pair comparison table, including unresolved pairs;
        # full context/interval outputs are bound by hashes and reproducible.
        summary={k:v for k,v in result.items() if k not in ('intervals','first_observations')}
        r.kv(result=summary,total_membership_intervals=len(result['intervals']),
             bounded_membership_intervals=sum(v['lower_exclusive_utc'] is not None for v in result['intervals']),
             complete_output_sha256=hashlib.sha256(canonical(result)).hexdigest(),output_bytes=len(canonical(result)),
             source_bytes_verified=inventory['total_bytes'],candidate_sha256=hashlib.sha256(Path(model.__file__).read_bytes()).hexdigest(),
             elapsed_seconds=round(time.monotonic()-started,3),
             native_invocations=0,provider_requests=0,learning_ledger_reads=0,downstream_output_reads=0,
             private_account_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='All 251 fixed approved public capture originals; no engine or evaluator output. This checks selected-membership observation intervals and their unresolved ordering, not economic onset, predictive performance or portfolio authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
