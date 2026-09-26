"""Read-only complete package and existing original-source replay acceptance."""
from pathlib import Path
import json,subprocess,sys
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import liquidity_agent_triggers as triggers

FUNCTIONS={
    'justhodl-liquidity-flow':('liquidity_flow_store.py','replay_liquidity_flow_research.py',
        {('justhodl-liquidity-flow-cadence','cron(5 22 ? * MON-FRI *)','ENABLED','UTC'),
         ('justhodl-liquidity-flow-daily','rate(1 day)','ENABLED','UTC')}),
    'justhodl-bond-vol':('bond_vol_store.py','replay_bond_vol_research.py',
        {('justhodl-bond-vol-hourly','cron(15 * * * ? *)','ENABLED','UTC')})}
BUCKET='justhodl-dashboard-live'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6153_liquidity_bond_typed_binding_verify') as r:
        rows=[]
        for function,(module,script,expected_clocks) in FUNCTIONS.items():
            subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/function/'tests/run_tests.py')],cwd=ROOT,check=True)
            expected=subprocess.check_output(['git','log','-1','--format=%H','--',
                'aws/lambdas/'+function+'/source/'+module],cwd=ROOT,text=True).strip()
            if len(expected)!=40:raise ValueError('Full intended source commit required')
            before=runtime(lam,s3,events,scheduler,function)
            if before['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact source receipt required')
            config=json.loads((ROOT/'aws/lambdas'/function/'config.json').read_bytes())
            if before['memory_mb']!=config['memory'] or before['timeout']!=config['timeout']:raise ValueError('Runtime reserve differs')
            arn=lam.get_function_configuration(FunctionName=function)['FunctionArn']
            discovered=triggers.collect(lam,scheduler,s3,arn,BUCKET)
            clocks={(row['name'],row['expression'],row['state'],row.get('timezone','UTC')) for row in before['schedules']}
            clocks.update((row['Name'],row['ScheduleExpression'],row['State'],row['ScheduleExpressionTimezone']) for row in discovered['matching_schedules'])
            if clocks!=expected_clocks or discovered['event_source_mappings'] or discovered['direct_bucket_notifications']:
                raise ValueError('Reviewed normal direct publication bindings differ')
            proof=json.loads(subprocess.check_output([sys.executable,str(ROOT/'scripts'/script)],cwd=ROOT,text=True))
            if proof.get('replayed',proof.get('full_replay')) is not True or proof.get('calls_eligible') is not False or proof.get('sizing_eligible') is not False:
                raise ValueError('Complete research-only existing publication replay required')
            after=runtime(lam,s3,events,scheduler,function)
            if before!=after:raise ValueError('Package or timing changed during acceptance')
            rows.append({'function':function,'expected_commit':expected,'runtime':before,'trigger_inventory':discovered,
                'current_public_output_replayed':proof})
        r.kv(packages=rows,code_and_receipt_verified=True,complete_original_replay=True,
            producer_invocations=0,consumer_invocations=0,provider_requests=0,private_account_reads=0,
            public_writes=0,notifications_sent=0,schedules_changed=0,normal_publication_under_new_code_verified=False,
            scope='Exact deployed source and complete current original-source replay; existing publications may predate this storage repair. Normal new-code publications remain separate evidence.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
