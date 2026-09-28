"""Observe the actual credit schedule after the strict 6297 baseline refused it.

Read-only package, release receipt and bound schedule metadata. This does not
accept the mismatched baseline or read licensed originals/private account data.
"""
from pathlib import Path
import runpy,sys

ROOT=Path(__file__).resolve().parents[3]


def summarize(actual,baseline):
    if actual['receipt']!={'status':'matched','commit':baseline['EXPECTED']}:
        raise ValueError('Exact predecessor code receipt required')
    reason=None
    try:baseline['validate_runtime'](actual)
    except ValueError as error:reason=str(error)
    return {'actual_runtime':actual,'declared_schedule':baseline['CRON'],
        'strict_baseline_matches':reason is None,'strict_baseline_refusal':reason,
        'baseline_accepted':False,'retained_originals_replayed':False,
        'scope':'Runtime/schedule observation only. The strict acceptance requirement remains unchanged.'}


def main():
    import boto3
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks')]
    from ops_report import report
    from market_runtime_evidence import runtime
    path=Path(__file__).with_name('ops_6297_credit_cadence_baseline.py')
    baseline=runpy.run_path(str(path))
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')}
    args=[clients[name] for name in ('lambda','s3','events','scheduler')]
    with report('ops_6298_credit_schedule_diagnostic') as r:
        before=runtime(*args,baseline['FN']);evidence=summarize(before,baseline)
        r.kv(**evidence,native_invocations=0,provider_requests=0,credential_reads=0,private_account_reads=0,
             licensed_original_reads=0,public_writes=0,history_writes=0,schedule_changes=0)
        if runtime(*args,baseline['FN'])!=before:raise ValueError('Runtime changed during diagnostic')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
