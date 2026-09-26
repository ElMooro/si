"""Verify deployed preservation guards; read existing heads, never invoke or write."""
from pathlib import Path
from datetime import datetime,timezone
import subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-bond-desk/source')]
from ops_report import report
from market_runtime_evidence import runtime
import bond_publication as pub

FN='justhodl-bond-desk';BUCKET='justhodl-dashboard-live'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6174_bond_publication_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN+'/source/bond_publication.py'],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FN)
        if before['receipt']!={'status':'matched','commit':expected} or before['memory_mb']!=256 or before['timeout']!=180:raise ValueError('Exact guarded package and original runtime required')
        if before['schedules']!=[{'kind':'EventBridge rule','name':'justhodl-bond-desk-daily','state':'ENABLED','expression':'cron(15 15 ? * MON-FRI *)','native_targets':1}]:raise ValueError('Cadence changed')
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
        state=pub.begin(s3,BUCKET,datetime.now(timezone.utc).isoformat())
        evidence={name:{'bytes':len(v['raw']),'sha256':pub.sha(v['raw'])} if v else {'status':'absent'} for name,v in (('head',state['head']),('history',state['history']))}
        if runtime(lam,s3,events,scheduler,FN)!=before:raise ValueError('Runtime changed during verification')
        head=(state['head'] or {}).get('doc',{})
        r.kv(expected_commit=expected,actual_runtime=before,existing_heads=evidence,
            preserved_history_rows=len((state['history'] or {}).get('doc',{})),
            current_publication={'generated_at':head.get('generated_at'),'version':head.get('version')},
            normal_new_code_publication_verified=False,offline_failure_tests_passed=True,
            provider_requests=0,native_invocations=0,public_writes=0,history_writes=0,account_reads=0,notifications_sent=0,schedules_changed=0,
            scope='Exact deployed preservation/CAS guards and readable complete predecessor; next normal publication remains pending.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
