"""Diagnose stale Liquidity-agent output using read-only AWS operation evidence."""
from pathlib import Path
from datetime import datetime,timezone
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded
import risk_gate_runtime_diagnostic as diagnostic
import liquidity_agent_triggers as triggers
import ops_6121_liquidity_agent_original_baseline as baseline
import retained_access_evidence as access
REQUEST='chatgpt-liquidity-agent-trigger-diagnostic-6126'
STATUS=baseline.PRIVATE+'requests/'+baseline.sha(REQUEST.encode())+'.json'


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_liquidity_agent_triggers.py')],cwd=ROOT,check=True)
    subprocess.run([sys.executable,str(ROOT/'tests/test_risk_gate_runtime_diagnostic.py')],cwd=ROOT,check=True)
    clients=[boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler','logs','cloudwatch')]
    s3,lam,events,scheduler,logs,metrics=clients
    with report('ops_6126_liquidity_agent_trigger_diagnostic') as r:
        now=datetime.now(timezone.utc);progress={'request_id':REQUEST,'started_at':now.isoformat(),'status':'claimed'}
        s3.put_object(Bucket=baseline.BUCKET,Key=STATUS,Body=baseline.encoded(progress),ContentType='application/json',CacheControl='no-store',IfNoneMatch='*')
        try:
            evidence,raw=diagnostic.collect(lam,events,scheduler,logs,metrics,now,baseline.FUNCTION)
            discovery=triggers.collect(lam,scheduler,s3,evidence['configuration']['FunctionArn'],baseline.BUCKET)
            artifacts={'raw_logs':baseline.retain(s3,baseline.encoded(raw)),
                'runtime_diagnostic':baseline.retain(s3,baseline.encoded(evidence)),
                'trigger_inventory':baseline.retain(s3,baseline.encoded(discovery))}
            packets={}
            for key in ('liquidity-data.json','data/ops/releases/'+baseline.FUNCTION+'.json'):
                raw=bounded(s3.get_object(Bucket=baseline.BUCKET,Key=key)['Body']);packet=json.loads(raw)
                artifacts[key]=baseline.retain(s3,raw)
                packets[key]={k:packet.get(k) for k in ('generated_at','contract','commit','code_sha256')}
            outcomes=[access.check(key) for key in [STATUS]+[ref['key'] for ref in artifacts.values()]]
            artifacts['access_evidence']=baseline.retain(s3,baseline.encoded(outcomes));outcomes.append(access.check(artifacts['access_evidence']['key']))
            privacy=access.summarize(outcomes)
            if not privacy['all_denied']:raise ValueError('All diagnostic originals must remain private')
            latest=lam.get_function_configuration(FunctionName=baseline.FUNCTION)
            if {k:latest.get(k) for k in diagnostic.CONFIG_FIELDS}!=evidence['configuration']:
                raise ValueError('Native runtime changed during diagnosis')
            result={'evidence':evidence,'trigger_inventory':discovery,'artifacts':artifacts,'packets':packets,'privacy':privacy,
                'native_invocations':0,'provider_requests':0,'public_writes':0,'schedules_changed':0,
                'private_account_reads':0,'notifications_sent':0,'paid_ai_calls':0}
            s3.put_object(Bucket=baseline.BUCKET,Key=STATUS,Body=baseline.encoded({**progress,'status':'complete','result':result}),ContentType='application/json',CacheControl='no-store')
            r.kv(**result)
        except Exception as exc:
            s3.put_object(Bucket=baseline.BUCKET,Key=STATUS,Body=baseline.encoded({**progress,'status':'failed','error_type':type(exc).__name__}),ContentType='application/json',CacheControl='no-store');raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
