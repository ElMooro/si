"""Retain both complete explanation producers without running either producer.

Only code, runtime, alias and scheduling metadata. No page/account/learning-log
packets, model requests, native invocations, public writes or cadence changes.
"""
from pathlib import Path
from datetime import datetime,timezone
import ast,hashlib,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from ops_6204_shipping_consumer_baseline import runtime,retain,encode
import retained_access_evidence as access
PINS={
 'justhodl-page-ai':'c01267c87d5607f57001676f9ddb6eb74f691805bc5b40426223a1c5b7cd3da8',
 'justhodl-page-ai-commentary':'40d7312e88234218a76b475d3ca5bcdbcf29f6998562ca358d6ed6681df24e72',
}


def source_check(function,raw):
    digest=hashlib.sha256(raw).hexdigest()
    if function not in PINS or digest!=PINS[function]:raise ValueError('Complete original explanation producer differs')
    ast.parse(raw.decode('utf-8'))
    return {'bytes':len(raw),'sha256':digest,'imported_or_executed':False}


def package_summary(row):
    summary={k:v for k,v in row.items() if k not in ('inventory','repository_sources')}
    if row.get('status')!='whole_actual_package_retained':raise ValueError('Both actual original producers must be retained')
    inv=row.get('inventory') or {}
    summary.update(code_matches_repository=inv.get('code_matches_repository'),source_files_checked=inv.get('source_files_checked'),source_differences=inv.get('source_differences'))
    return summary


def main():
    for name in ('test_shipping_consumer_baseline.py','test_page_explanation_original_baseline.py'):
        subprocess.run([sys.executable,str(ROOT/'tests'/name)],cwd=ROOT,check=True)
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    with report('ops_6216_page_explanation_original_baseline') as r:
        baseline={'contract':'page-explanation-original-baseline.v1','captured_at':datetime.now(timezone.utc).isoformat(),
                  'snapshot_atomic':False,'source_checks':{},'producers':{},'input_packets_read':[]}
        for function in PINS:
            raw=(ROOT/'aws/lambdas'/function/'source/lambda_function.py').read_bytes()
            baseline['source_checks'][function]=source_check(function,raw)
            baseline['producers'][function]=runtime(clients['lambda'],clients['s3'],clients['events'],clients['scheduler'],function)
        ref=retain(clients['s3'],encode(baseline))
        summaries={fn:package_summary(row) for fn,row in baseline['producers'].items()}
        privacy=access.summarize([access.check(k) for k in [ref['key'],*[row['whole_zip']['key'] for row in baseline['producers'].values()]]])
        r.kv(baseline=ref,producer_runtimes=summaries,whole_source_pins=baseline['source_checks'],**privacy,
             native_invocations=0,provider_requests=0,model_requests=0,account_reads=0,learning_log_reads=0,
             page_output_reads=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Both entire original packages and runtime/cadence metadata only. Source-context completeness, scorecard matching, research permissions and model provenance remain unqualified. No cached page output or customer context was read.')
        if not privacy['all_denied']:raise ValueError('Original code must remain private')
        if not all(row['code_matches_repository'] is True for row in summaries.values()):raise ValueError('Actual source differs from repository; reconcile before editing')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
