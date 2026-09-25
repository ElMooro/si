"""Retain four complete downstream packages before capital-structure guards.

Read only against Lambda, schedules and release receipts. No engine invocations,
provider credentials, account datasets, signals, notifications or public writes.
"""
from pathlib import Path
import base64,hashlib,json,sys,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_5998_option_population_retained_acceptance import denied_with_retry
import share_structure_sources as source

REQUEST='chatgpt-capital-structure-remaining-predecessors-6095'
STATUS=source.request_key(REQUEST,'baseline')
CONSUMERS=('justhodl-industry-boom','justhodl-impact-graph','justhodl-deal-scanner','justhodl-spx-beaters')


def main():
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6095_capital_structure_remaining_predecessors') as r:
        source.journal(s3,STATUS,{'request_id':REQUEST,'status':'claimed','functions':list(CONSUMERS)},True)
        predecessors={}
        for function in CONSUMERS:
            evidence=runtime(lam,s3,events,scheduler,function)
            location=lam.get_function(FunctionName=function)['Code']['Location']
            raw=bounded(urllib.request.urlopen(location,timeout=40))
            assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==evidence['code_sha256'], 'Actual package digest differs: '+function
            predecessors[function]={'runtime':evidence,'whole_zip':source.retain(s3,raw)}
            source.journal(s3,STATUS,{'request_id':REQUEST,'status':'retaining','consumer_predecessors':predecessors})
        protected={STATUS,*(v['whole_zip']['key'] for v in predecessors.values())}
        for key in protected:
            assert denied_with_retry('https://justhodl.ai/'+key), 'Private predecessor exposed at site'
            assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key), 'Private predecessor exposed at origin'
        for function,prior in predecessors.items():
            assert runtime(lam,s3,events,scheduler,function)==prior['runtime'], 'Native package changed during preservation: '+function
        document={'request_id':REQUEST,'status':'complete','consumer_predecessors':predecessors}
        source.journal(s3,STATUS,document)
        r.kv(**document,protected_artifacts_checked=len(protected),producer_invocations=0,
            consumer_invocations=0,provider_requests=0,private_account_reads=0,public_writes=0,
            signal_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
