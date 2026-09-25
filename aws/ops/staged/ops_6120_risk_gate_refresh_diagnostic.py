"""Read-only refresh diagnosis; private log evidence and no native invocation."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, subprocess, sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import bounded
import risk_gate_runtime_diagnostic as diagnostic
import retained_access_evidence as access
BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/risk-gate-runtime/'
REQUEST='chatgpt-risk-gate-refresh-diagnostic-6120'
encoded=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
sha=lambda raw:hashlib.sha256(raw).hexdigest()
STATUS=PRIVATE+'requests/'+sha(REQUEST.encode())+'.json'


def retain(s3,raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=32*1024*1024:raise ValueError('Bounded complete evidence required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','PreconditionFailed','ConditionalRequestConflict'):raise
    assert bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])==raw
    return ref


def main():
    with report('ops_6120_risk_gate_refresh_diagnostic') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_risk_gate_runtime_diagnostic.py')],cwd=ROOT,check=True)
        s3,lam,events,scheduler,logs,metrics=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler','logs','cloudwatch'))
        now=datetime.now(timezone.utc);progress={'request_id':REQUEST,'started_at':now.isoformat(),'status':'claimed'}
        s3.put_object(Bucket=BUCKET,Key=STATUS,Body=encoded(progress),ContentType='application/json',CacheControl='no-store',IfNoneMatch='*')
        try:
            evidence,raw_logs=diagnostic.collect(lam,events,scheduler,logs,metrics,now)
            refs={'raw_log_sample':retain(s3,encoded(raw_logs))};packets={}
            for key in ('data/risk-gate.json','data/ops/releases/justhodl-risk-gate.json'):
                obj=s3.get_object(Bucket=BUCKET,Key=key);raw=bounded(obj['Body']);value=json.loads(raw)
                refs[key]=retain(s3,raw)
                packets[key]={k:value.get(k) for k in ('generated_at','source_generated_at','contract','commit','code_sha256')}
            refs['diagnosis']=retain(s3,encoded(evidence))
            outcomes=[access.check(key) for key in [STATUS]+[v['key'] for v in refs.values()]]
            refs['access_evidence']=retain(s3,encoded(outcomes));outcomes.append(access.check(refs['access_evidence']['key']))
            privacy=access.summarize(outcomes);assert privacy['all_denied']
            latest=lam.get_function_configuration(FunctionName=diagnostic.FUNCTION)
            assert {k:latest.get(k) for k in diagnostic.CONFIG_FIELDS}==evidence['configuration'],'Runtime changed during read-only diagnosis'
            result={'evidence':evidence,'artifacts':refs,'packets':packets,'privacy':privacy,
                'native_invocations':0,'provider_requests':0,'public_writes':0,'schedules_changed':0,
                'private_account_reads':0,'notifications_sent':0,'paid_ai_calls':0}
            s3.put_object(Bucket=BUCKET,Key=STATUS,Body=encoded({**progress,'status':'complete','result':result}),ContentType='application/json',CacheControl='no-store')
            r.kv(**result)
        except Exception as exc:
            s3.put_object(Bucket=BUCKET,Key=STATUS,Body=encoded({**progress,'status':'failed','error_type':type(exc).__name__}),ContentType='application/json',CacheControl='no-store')
            raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
