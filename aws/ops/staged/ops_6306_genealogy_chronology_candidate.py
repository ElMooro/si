"""Reconcile all fixed public registrations, then compile an isolated chronology.

The earlier complete capture reconciliation binds every record projection and
body hash. Re-read all 3,305 original record bodies and their storage clocks;
the unchanged complete projection hash proves this is that same population.
No capture sampling, private ledger, source engine, price or consumer is read.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import hashlib,sys,time
ROOT=Path(__file__).resolve().parents[3]
EXPECTED_INVENTORIES={
 'data/research-forecasts/captures/':'be88c4246fb155165ae7c8a778cc734d1552060d987d0b910e0937f14d61e0cd',
 'data/research-forecasts/records/':'98f21bcc0afe16801b06648c02c15c08c77a01f0c1ef7a4401e4b694083552f2'}
EXPECTED_PROJECTION='43983cf6cb1b77bb66629156d6cb4f637be23a3c404642468283b98201ba9c5d'


def reconcile(client,inventories):
    from genealogy_public_archive import read_object,validate_registered_record,validate_inventory,canonical,clock,PREFIX
    validate_inventory(inventories)
    if {p:i['inventory_sha256'] for p,i in inventories.items()}!=EXPECTED_INVENTORIES:raise ValueError('Reviewed inventory changed')
    def read(meta):
        doc,evidence=read_object(client,meta['key'],meta)
        return validate_registered_record(doc,evidence)
    with ThreadPoolExecutor(max_workers=8) as pool:
        records=list(pool.map(read,inventories[PREFIX+'records/']['objects']))
    records.sort(key=lambda r:(clock(r['registered_at']),r['forecast_id']))
    predecessor=[{k:v for k,v in row.items() if k!='source_issue'} for row in records]
    if hashlib.sha256(canonical(predecessor)).hexdigest()!=EXPECTED_PROJECTION:
        raise ValueError('Complete previously reconciled record population differs')
    protocols={}
    for row in records:
        ref=row['protocol_ref'];key=ref['key']
        if key in protocols and protocols[key]!=ref:raise ValueError('Protocol references conflict')
        protocols[key]=ref
    for key,ref in sorted(protocols.items()):
        _,evidence=read_object(client,key)
        if evidence['sha256']!=ref['sha256'] or clock(evidence['last_modified'])!=clock(ref['first_stored_at']):raise ValueError('Protocol body or actual clock changed')
    return {'contract':'genealogy-public-archive-audit.v1','record_body_and_storage_checks_complete':True,
        'cutoff':inventories[PREFIX+'records/']['cutoff'],'records':records,'validated_records':len(records),
        'registration_counts_by_source':dict(Counter(r['source_key'] for r in records)),
        'unreferenced_record_ids':[],'failures':[],'reference_errors':[],'reference_conflicts':[],
        'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,
        'prior_capture_reconciliation':{'operation':6304,'run':36469128376,'record_projection_sha256':EXPECTED_PROJECTION,
            'captures_checked':251,'record_reference_occurrences_checked':24335,'complete_candidate_scans':46,
            'scope':'Every record/body hash and original clock rechecked. Capture-reference closure is inherited only after the complete earlier projection matches byte-for-byte.'}}


def main():
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','aws/shared','aws/ops')]
    from genealogy_public_archive import load_inventory,canonical
    import genealogy_registration_model as model
    from ops_report import report
    import boto3
    from botocore.config import Config
    inv=load_inventory(ROOT/'aws/ops/reports/latest/ops_6303_public_research_archive_inventory.md')
    client=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=8,connect_timeout=10,
                        read_timeout=30,retries={'mode':'standard','total_max_attempts':2}))
    with report('ops_6306_genealogy_chronology_candidate') as r:
        started=time.monotonic();inputs=reconcile(client,inv);output=model.compile_archive(inputs)
        if output['archive_records']!=3305 or output['excluded_records']!=2019 or output['retained_records']!=1286:
            raise ValueError('Reconciled source/identity exclusion counts differ')
        counts={k:output[k] for k in ('archive_records','retained_records','excluded_records','possible_comparisons','compared','uncompared')}
        r.kv(counts=counts,cutoff=inputs['cutoff'],complete_input_sha256=hashlib.sha256(canonical(inputs)).hexdigest(),
             complete_output_sha256=hashlib.sha256(canonical(output)).hexdigest(),output_bytes=len(canonical(output)),
             retained_sources=len({v['source_key'] for v in output['records']}),retained_instruments=len({v['instrument_id'] for v in output['records']}),
             first_registration_groups=len(output['first_registrations']),
             all_pair_summaries=output['pair_summaries'],
             exclusion_counts=dict(Counter(reason for row in output['exclusions'] for reason in row['reasons'])),
             candidate_sha256=hashlib.sha256(Path(model.__file__).read_bytes()).hexdigest(),
             elapsed_seconds=round(time.monotonic()-started,3),
             native_invocations=0,provider_requests=0,learning_ledger_reads=0,downstream_output_reads=0,
             private_account_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             source_replay_verified=False,forecast_qualified=False,calls_eligible=False,sizing_eligible=False,
             scope=output['definition'],population_scope=output['population_scope'])


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
