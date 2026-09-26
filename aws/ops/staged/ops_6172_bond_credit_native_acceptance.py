"""Accept exact native credit projection bytes without invoking the producer."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks','aws/shared','aws/lambdas/justhodl-bond-desk/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6168_bond_desk_baseline import retain,PRIVATE,BUCKET,FUNCTION
from ops_6171_bond_credit_candidate import baseline_read,original_check
import bond_credit_store as store
import credit_research_store as upstream
import retained_access_evidence as access

ACCEPTANCE='11539530e1215a1023106a991a517bff877f3f1adc63abc0a67d95b112aa1694'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6172_bond_credit_native_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source/bond_credit_store.py'],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        if before['receipt']!={'status':'matched','commit':expected} or before['memory_mb']!=256 or before['timeout']!=180:raise ValueError('Exact native credit package and original runtime required')
        if before['schedules']!=[{'kind':'EventBridge rule','name':'justhodl-bond-desk-daily','state':'ENABLED','expression':'cron(15 15 ? * MON-FRI *)','native_targets':1}]:raise ValueError('Cadence changed')
        evidence=json.loads(baseline_read(s3,{'key':PRIVATE+ACCEPTANCE+'.bin','sha256':ACCEPTANCE,'bytes':14394}))
        if evidence['complete_original_replay_passed'] is not True:raise ValueError('Accepted original qualification required')
        if baseline_read(s3,evidence['candidate_compiler'])!=(store.ROOT/'bond_credit.py').read_bytes():raise ValueError('Accepted source projection changed')
        raw=baseline_read(s3,evidence['source']);view,proof=store.compile_raw(raw,evidence['evaluated_at'],store.reader(s3,BUCKET))
        if store.encode({k:v for k,v in view.items() if k not in ('upstream_binding','arithmetic_checks')})!=baseline_read(s3,evidence['candidate']):raise ValueError('Native comparison differs from accepted candidate')
        original_proof=original_check(json.loads(raw),view,upstream.reader(s3,BUCKET))
        if store.encode(original_proof)!=baseline_read(s3,evidence['independent_proof']):raise ValueError('Original independent proof changed')
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        ref=retain(s3,store.encode({'contract':'bond-credit-native-acceptance.v1','commit':expected,'candidate_acceptance':ACCEPTANCE,'source':evidence['source'],
            'view_sha256':store.sha(store.encode(view)),'proof':proof,'original_proof':original_proof,'runtime':before}))
        privacy=access.summarize([access.check(ref['key'])]);current=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key='data/bond-desk.json')['Body']))
        if runtime(lam,s3,events,scheduler,FUNCTION)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,acceptance=ref,accepted_candidate_byte_equal=True,checks=proof,original_checks=original_proof,
            current_publication={'generated_at':current.get('generated_at'),'version':current.get('version')},**privacy,
            normal_new_code_publication_verified=False,provider_requests=0,native_invocations=0,public_writes=0,history_writes=0,account_reads=0,notifications_sent=0,schedules_changed=0,
            scope='Exact code and native source-bound current comparisons; original/provider historical charts and analogs remain separately unqualified.')
        if not privacy['all_denied']:raise ValueError('Private acceptance reference exposed')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
