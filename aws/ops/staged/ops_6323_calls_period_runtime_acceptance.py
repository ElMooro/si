"""Read only exact Calls compiler packages, release receipts and existing cadence.

No engine invocation, current consumer output, private account, learning ledger,
research outcome, provider request or schedule mutation is permitted.
"""
from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET

SCHEDULES={
 'justhodl-ai-brief':{'justhodl-ai-brief-4h':'cron(5 0,4,8,12,16,20 * * ? *)'},
 'justhodl-calls-research-audit':{'justhodl-calls-research-audit-15m':'rate(15 minutes)'},
 'justhodl-signal-harvester':{'signal-harvester-daily':'cron(15 23 * * ? *)','justhodl-prospective-capture-hourly':'rate(1 hour)'},
 'justhodl-prospective-evaluator':{'justhodl-prospective-evaluator-hourly':'rate(1 hour)'},
}


class ReceiptsOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**kwargs):
        if set(kwargs)!={'Bucket','Key'} or kwargs['Bucket']!=BUCKET or kwargs['Key'] not in {
                'data/ops/releases/'+fn+'.json' for fn in SCHEDULES}:
            raise ValueError('Only the four exact release receipts may be read')
        return self.client.get_object(**kwargs)


def validate(actual,fn,expected):
    if actual['function_name']!=fn or actual['receipt']!={'status':'matched','commit':expected}:
        raise ValueError('Exact function and release commit required')
    if actual['source_files_checked']<3:raise ValueError('Whole compiler closure is required')
    for name,expression in SCHEDULES[fn].items():
        rows=[r for r in actual['schedules'] if r['name']==name]
        if len(rows)!=1 or rows[0]['state']!='ENABLED' or rows[0]['expression']!=expression or rows[0]['native_targets']!=1:
            raise ValueError('Declared original cadence differs: '+name)


def main():
    import boto3
    from ops_report import report
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/shared/calls_free_brief.py','aws/shared/calls_research_replay.py'],cwd=ROOT,text=True).strip()
    lam,storage,events,scheduler=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptsOnly(storage),events,scheduler)
    with report('ops_6323_calls_period_runtime_acceptance') as r:
        actual={fn:runtime(*clients,fn) for fn in SCHEDULES}
        r.kv(actual_runtimes=actual,expected_commit=expected)
        for fn,value in actual.items():validate(value,fn,expected)
        after={fn:runtime(*clients,fn) for fn in SCHEDULES}
        if actual!=after:raise ValueError('Runtime changed during read-only acceptance')
        r.kv(exact_compiler_packages_accepted=True,declared_schedules_present=True,
            native_publication_verified=False,current_consumer_reads=0,private_account_reads=0,
            learning_ledger_reads=0,provider_requests=0,native_invocations=0,
            public_writes=0,history_writes=0,schedule_changes=0,
            scope='Whole actual ZIP source closure and exact release receipts only. All current consumer and outcome packets are excluded. No native execution or new publication is claimed.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
