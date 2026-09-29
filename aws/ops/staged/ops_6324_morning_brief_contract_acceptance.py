"""Read-only actual Morning package and release; no invocation or message access."""
from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET
FUNCTION='justhodl-morning-intelligence'


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**kwargs):
        if kwargs!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FUNCTION+'.json'}:
            raise ValueError('Only the exact Morning release receipt may be read')
        return self.client.get_object(**kwargs)


def validate(actual,expected):
    if actual['function_name']!=FUNCTION or actual['receipt']!={'status':'matched','commit':expected}:
        raise ValueError('Exact release and function required')
    if actual['source_files_checked']<2:raise ValueError('Whole shared compiler closure required')
    rows=[r for r in actual['schedules'] if r['name']=='justhodl-morning-brief-daily']
    if len(rows)!=1 or rows[0]['state']!='ENABLED' or rows[0]['native_targets']!=1 or not rows[0]['expression']:
        raise ValueError('Declared existing daily schedule must remain bound and enabled')


def main():
    import boto3
    from ops_report import report
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/shared/morning_free_research.py'],cwd=ROOT,text=True).strip()
    lam,storage,events,scheduler=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptOnly(storage),events,scheduler)
    with report('ops_6324_morning_brief_contract_acceptance') as r:
        actual=runtime(*clients,FUNCTION);r.kv(actual_runtime=actual,expected_commit=expected)
        validate(actual,expected)
        if runtime(*clients,FUNCTION)!=actual:raise ValueError('Runtime changed during acceptance')
        r.kv(exact_package_accepted=True,native_publication_verified=False,
            current_consumer_reads=0,recipient_reads=0,private_account_reads=0,
            learning_ledger_reads=0,provider_requests=0,native_invocations=0,
            messages_sent=0,public_writes=0,schedule_changes=0,
            scope='Actual whole ZIP, exact release receipt and stable existing runtime only. No delivery or native publication is claimed.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
