"""Verify deployed cohort package and accepted retained arithmetic, without invoke."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks','aws/shared','aws/lambdas/justhodl-bond-desk/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6168_bond_desk_baseline import retain,PRIVATE,BUCKET,FUNCTION
from ops_6169_bond_flow_candidate import private_read
import bond_flow_store as store
import retained_access_evidence as access

ACCEPTANCE='1711477684c59a88757aa20e0f4cf7f0478c80c0f88fa8aa7fe72eff27c64c79'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6170_bond_flow_native_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source/bond_flow_store.py'],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        if before['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact native cohort receipt required')
        if before['memory_mb']!=256 or before['timeout']!=180 or before['schedules']!=[{'kind':'EventBridge rule','name':'justhodl-bond-desk-daily','state':'ENABLED','expression':'cron(15 15 ? * MON-FRI *)','native_targets':1}]:raise ValueError('Preserved runtime or cadence changed')
        evidence=json.loads(private_read(s3,{'key':PRIVATE+ACCEPTANCE+'.bin','sha256':ACCEPTANCE,'bytes':139099}))
        if evidence['whole_original_replay_passed'] is not True:raise ValueError('Accepted complete original replay required')
        for name,filename in (('bond_flow_candidate','bond_flow.py'),('verify_bond_flow','verify_bond_flow.py')):
            if private_read(s3,evidence['compilers'][name])!=(store.ROOT/filename).read_bytes():raise ValueError('Accepted compiler bytes changed')
        raw=private_read(s3,evidence['source']);view,proof=store.compile_raw(raw,evidence['evaluated_at'],store.reader(s3,BUCKET))
        accepted=private_read(s3,evidence['view'])
        if store.encode({k:v for k,v in view.items() if k not in ('upstream_binding','arithmetic_checks')})!=accepted:raise ValueError('Native arithmetic differs from accepted original candidate')
        if store.encode(proof)!=private_read(s3,evidence['proof']):raise ValueError('Independent native proof differs')
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        ref=retain(s3,store.encode({'contract':'bond-flow-native-acceptance.v1','commit':expected,'candidate_acceptance':ACCEPTANCE,
            'source':evidence['source'],'view_sha256':store.sha(store.encode(view)),'proof':proof,'actual_runtime':before}))
        privacy=access.summarize([access.check(ref['key'])])
        current=store.strict(bounded(s3.get_object(Bucket=BUCKET,Key='data/bond-desk.json')['Body']))
        flows=current.get('regions',{}).get('us',{}).get('flows',{})
        after=runtime(lam,s3,events,scheduler,FUNCTION)
        if before!=after:raise ValueError('Runtime changed during read-only acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,acceptance=ref,candidate_acceptance=ACCEPTANCE,
            source=evidence['source'],checks=proof,accepted_candidate_byte_equal=True,
            public_packet={'generated_at':current.get('generated_at'),'version':current.get('version'),'flow_status':flows.get('flow_status'),'cohort_contract':(flows.get('flow_research') or {}).get('contract')},
            **privacy,normal_new_code_publication_verified=False,provider_requests=0,native_invocations=0,public_writes=0,
            history_writes=0,account_reads=0,notifications_sent=0,schedules_changed=0,
            scope='Exact deployed code and accepted original-bound cohort arithmetic. Normal public publication remains a separate check; no producer invoked.')
        if not privacy['all_denied']:raise ValueError('Acceptance evidence must remain private')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
