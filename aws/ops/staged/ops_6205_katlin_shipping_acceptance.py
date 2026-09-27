"""Exact deployed shipping boundary; no account-output access or native invocation."""
from pathlib import Path
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6194_cycle_consumer_acceptance import unchanged
import shipping_model_context as shipping
BUCKET='justhodl-dashboard-live';FN='justhodl-katlin'
BASELINE='eb90d7d59b0c3125a8c6e781def7f1670b0bb42d8d19ecdf0421f6e778008fa9'
PRIVATE='audit-private/20260909-originals/shipping-consumer-research/'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6205_katlin_shipping_acceptance') as r:
        obj=s3.get_object(Bucket=BUCKET,Key=PRIVATE+BASELINE+'.bin');raw=bounded(obj['Body'])
        if obj['ContentLength']!=len(raw) or len(raw)!=60976 or hashlib.sha256(raw).hexdigest()!=BASELINE:
            raise ValueError('Whole protected baseline differs')
        baseline=json.loads(raw)
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        actual=runtime(lam,s3,events,scheduler,FN)
        if actual['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact release receipt required')
        unchanged(baseline['consumers'][FN],actual)
        subprocess.run([sys.executable,str(ROOT/'tests/shipping_consumer_tests.py')],cwd=ROOT,check=True)
        obj=s3.get_object(Bucket=BUCKET,Key=shipping.SOURCE);source=bounded(obj['Body'])
        if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(source):
            raise ValueError('Complete public source required')
        view=shipping.decision_view(json.loads(source))
        if view['ports'] or view['exporters'] or any(view[k] is not False for k in shipping.FLAGS):
            raise ValueError('Unqualified source became investment evidence')
        if runtime(lam,s3,events,scheduler,FN)!=actual:raise ValueError('Actual package changed during acceptance')
        r.kv(expected_commit=expected,baseline_sha256=BASELINE,actual_runtime=actual,
            public_source={'key':shipping.SOURCE,'bytes':len(source),'sha256':hashlib.sha256(source).hexdigest(),
                           'consumer_context':view['research_context']},
            qualified_shipping_investment_votes=0,native_invocations=0,account_reads=0,recipient_reads=0,
            learning_log_reads=0,notifications_sent=0,provider_requests=0,public_writes=0,history_writes=0,schedules_changed=0,
            scope='Exact code/alias and isolated stock/country-ETF scoring only. Native private research/account outputs are neither invoked nor read. Other ranking models remain outside this qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
