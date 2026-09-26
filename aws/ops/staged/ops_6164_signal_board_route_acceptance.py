"""Read-only Signal Board package, one public route probe and ICI availability."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, subprocess, sys
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared','aws/lambdas/justhodl-signal-board/source')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
import board_store

FUNCTION='justhodl-signal-board'
BUCKET='justhodl-dashboard-live'
BASELINE='746cfc5ea9abc8dc933c7c1bbefc36c1adebe5db00d78c2f38cfe97031ef35d6'


def error_code(exc):
    return str(getattr(exc,'response',{}).get('Error',{}).get('Code',type(exc).__name__))


def main():
    lam,s3,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler'))
    with report('ops_6164_signal_board_route_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source/board_store.py'],cwd=ROOT,text=True).strip()
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        if before['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact deployed route-repair receipt required')
        raw=bounded(s3.get_object(Bucket=BUCKET,Key='audit-private/20260909-originals/signal-board-research/'+BASELINE+'.bin')['Body'])
        if len(raw)!=192798 or hashlib.sha256(raw).hexdigest()!=BASELINE:raise ValueError('Complete accepted baseline differs')
        baseline=json.loads(raw)
        if before['schedules']!=baseline['native_predecessor']['runtime']['schedules'] or before['memory_mb']!=1024 or before['timeout']!=600:
            raise ValueError('Preserved schedule/runtime differs')
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        raw,status,headers=board_store.acquire('screener/mean-reversion.json')
        if status!=200 or headers.get('x-jh-artifact-key')!='screener/mean-reversion.json':raise ValueError('Exact non-data route did not succeed')
        if not isinstance(json.loads(raw),(dict,list)):raise ValueError('Whole structured public packet required')
        route={'key':'screener/mean-reversion.json','status':status,'artifact_identity':headers['x-jh-artifact-key'],
               'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'complete_public_response_read':True}
        # Metadata only: distinguish a missing public head or function from an
        # inaccessible one. Do not invoke, seed, repair or expose ICI contents.
        ici={}
        try:
            cfg=lam.get_function_configuration(FunctionName='justhodl-ici-flows')
            ici['runtime']={k:cfg.get(k) for k in ('FunctionName','State','LastUpdateStatus','Runtime','Handler','Timeout','MemorySize','LastModified','CodeSha256')}
        except Exception as exc:ici['runtime_read_status']=error_code(exc)
        try:
            obj=s3.head_object(Bucket=BUCKET,Key='data/ici-flows.json')
            ici['public_head']={'status':'present','bytes':obj['ContentLength'],'last_modified':obj['LastModified'].isoformat()}
        except Exception as exc:ici['public_head']={'status':error_code(exc)}
        if runtime(lam,s3,events,scheduler,FUNCTION)!=before:raise ValueError('Runtime changed during read-only acceptance')
        r.kv(checked_at=datetime.now(timezone.utc).isoformat(),expected_commit=expected,actual_runtime=before,
             public_route_probe=route,ici_metadata=ici,synthetic_regressions='passed',
             normal_new_code_publication_verified=False,public_derived_read_attempts=1,provider_requests=0,
             native_invocations=0,consumer_invocations=0,private_account_reads=0,public_writes=0,
             notifications_sent=0,schedules_changed=0,
             scope='Actual deployed code, preserved runtime and one existing public packet; no native publication is forced. ICI diagnostic is metadata only.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
