"""Exact deployed code/schedules and approved public capture; no downstream results."""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
FUNCTIONS=('justhodl-signal-harvester','justhodl-prospective-evaluator')


def main():
    import boto3
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','aws/shared','aws/ops')]
    from market_runtime_evidence import runtime
    from genealogy_public_archive import read_object,validate_capture,validate_registered_record,clock
    from prospective_journal import canonical
    from research_identity import source_selection_policy
    from ops_report import report
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/research_identity.py'],cwd=ROOT,text=True).strip()
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')}
    args=[clients[name] for name in ('lambda','s3','events','scheduler')];s3=clients['s3']
    prior=json.loads((ROOT/'docs/audit/2026-09-26/research-identity-acceptance.json').read_text(encoding='utf-8'))['actual_runtimes']
    with report('ops_6305_research_self_ingestion_acceptance') as r:
        before={fn:runtime(*args,fn) for fn in FUNCTIONS}
        for fn,actual in before.items():
            if actual['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact release receipt differs')
            for field in ('timeout','memory_mb','runtime','handler','architectures','role','ephemeral_storage_mb','schedules'):
                if actual[field]!=prior[fn][field]:raise ValueError('Original runtime or schedule changed: '+field)
        obj=s3.get_object(Bucket='justhodl-dashboard-live',Key='data/prospective-research.json')
        try:raw=obj['Body'].read(1024*1024+1)
        finally:obj['Body'].close()
        if len(raw)>1024*1024:raise ValueError('Capture summary size differs')
        head=json.loads(raw);new=canonical(head.get('source_selection_policy'))==canonical(source_selection_policy())
        verified=None
        if new:
            ref=head['capture'];capture,evidence=read_object(s3,ref['key'])
            if evidence['sha256']!=ref['sha256']:raise ValueError('Capture summary byte reference differs')
            verified,refs=validate_capture(capture,evidence)
            if canonical(capture.get('source_selection_policy'))!=canonical(head['source_selection_policy']):raise ValueError('Capture policy differs')
            if head['generated_at']!=capture['generated_at'] or canonical(head['coverage'])!=canonical(capture['coverage']):raise ValueError('Capture coverage differs')
            if type(head['records_in_capture']) is not int or head['records_in_capture']!=len(refs):raise ValueError('Capture record count differs')
            for ref in refs.values():
                doc,receipt=read_object(s3,ref['key']);record=validate_registered_record(doc,receipt)
                if receipt['sha256']!=ref['sha256'] or record['identity_issue'] or record['source_issue']:raise ValueError('New capture contains unqualified identity or recursive source')
                protocol,proof=read_object(s3,doc['protocol_ref']['key'])
                if proof['sha256']!=doc['protocol_ref']['sha256'] or clock(proof['last_modified'])!=clock(doc['protocol_ref']['first_stored_at']):raise ValueError('Protocol storage clock differs')
        if {fn:runtime(*args,fn) for fn in FUNCTIONS}!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtimes=before,source_selection_policy=source_selection_policy(),
             current_capture_generated_at=head.get('generated_at'),current_capture_bytes=len(raw),current_capture_sha256=hashlib.sha256(raw).hexdigest(),
             new_normal_scheduled_capture_verified=new,verified_capture=verified,
             native_invocations=0,learning_ledger_reads=0,private_account_reads=0,downstream_output_reads=0,
             provider_requests=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Exact two native packages and unchanged operating settings. Only the approved public producer capture is read. Evaluator downstream results are not read; its echo exclusion is tested offline and code verified. Old captures remain pending until an original normal run publishes the new policy.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
