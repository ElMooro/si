"""Complete acceptance of the already-observed Scheduler collection; never recollect."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
               str(ROOT/'aws/lambdas/justhodl-13f-positions/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
import holdings_native as model
import holdings_store as store
from replay_holdings_research import verify_current

def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method='HEAD',
                headers={'User-Agent': 'justhodl-verify-release/1.0'}), timeout=30) as response:
            return response.status in (401, 403, 404)
    except urllib.error.HTTPError as exc:
        return exc.code in (401, 403, 404)


def main():
    fn, bucket = 'justhodl-13f-positions', 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=30, tcp_keepalive=True, retries={'max_attempts': 0}))
    raw = store.reader(s3, bucket)
    def read(key):
        return json.loads(raw(key))
    with report('ops_5879_holdings_scheduled_receipt_acceptance') as r:
        policy = json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy'])
        deny = next(v for v in policy['Statement'] if v.get('Sid') == 'Audit20260909ImmutableOriginalBackups')
        assert deny['Effect'] == 'Deny' and deny['Principal'] == '*'
        assert {'s3:GetObject', 's3:GetObjectVersion'} <= set(deny['Action'])
        assert deny['Condition'] == {'StringNotEquals': {'aws:PrincipalAccount': '857687956942'}}
        assert 'arn:aws:s3:::' + bucket + '/audit-private/20260909-originals/*' in deny['Resource']
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + fn], text=True).strip()
        deadline = time.monotonic() + 2400
        while True:
            receipt = read('data/ops/releases/' + fn + '.json')
            if receipt.get('commit') == expected:
                break
            assert time.monotonic() < deadline, 'Exact holdings runtime wait expired'
            time.sleep(15)
        conf = lam.get_function_configuration(FunctionName=fn)
        assert conf['CodeSha256'] == receipt['code_sha256']
        r.kv(runtime={'commit': expected, 'code_sha256': conf['CodeSha256']}, action='holdings_research_collect',
             legacy_collector_invoked=False, invocation_origin='AWS EventBridge Scheduler')
        before = {}
        for key in store.LEGACY_KEYS:
            try:
                before[key] = hashlib.sha256(raw(key)).hexdigest()
            except Exception as exc:
                if not store.missing(exc):
                    raise
                before[key] = None
        scheduler = boto3.client('scheduler', region_name='us-east-1')
        # The code receipt is written before schedule reconciliation. Wait only
        # for an absent schedule; a present but different target still fails.
        deadline = time.monotonic() + 600
        while True:
            try:
                schedule = scheduler.get_schedule(Name='justhodl-holdings-originals-research', GroupName='default')
                break
            except Exception as exc:
                if getattr(exc, 'response', {}).get('Error', {}).get('Code') != 'ResourceNotFoundException':
                    raise
                assert time.monotonic() < deadline, 'Deployed Scheduler binding was not observed'
                time.sleep(15)
        assert schedule['State'] == 'ENABLED' and schedule['ScheduleExpression'] == 'cron(40 0/2 * * ? *)'
        assert schedule['ScheduleExpressionTimezone'] == 'UTC'
        assert schedule['Target']['Arn'] == conf['FunctionArn']
        assert schedule['Target']['RoleArn'] == 'arn:aws:iam::857687956942:role/justhodl-scheduler-role'
        assert json.loads(schedule['Target']['Input']) == {'action': 'holdings_research_collect', 'notify': False}
        # The previous run observed and replayed a real Scheduler collection.
        # Only its final read-only Lambda request timed out; CloudWatch showed
        # one completed collector and no second execution. Reuse that evidence.
        request_id = 'holdings-42b290f582b4a71b-35472690408'
        when = datetime.fromisoformat('2026-09-19T22:15:30.469712+00:00')
        prior_report = ROOT/'aws/ops/reports/latest/ops_5877_holdings_recurring_acceptance.md'
        report_bytes = prior_report.read_bytes()
        report_text = report_bytes.decode()
        assert request_id in report_text and when.isoformat() in report_text
        assert 'scheduled_publication' in report_text and 'ReadTimeoutError' in report_text
        packet = read(model.CURRENT)
        assert packet.get('collection_request_id') == request_id, 'Current collection advanced; review that run separately'
        assert packet['replay']['manifest_key'].endswith('e5f89192a9caf1ebc4b7398d4aa7a1e1232068859c0ff6181a24fccc0a6f2ba8.json')
        r.kv(reused_scheduler_evidence={'run_id': '35472690408', 'request_id': request_id,
            'report_sha256': hashlib.sha256(report_bytes).hexdigest(), 'new_collector_invocations': 0})
        assert model.clock(packet['collection_started_at']) >= when - timedelta(seconds=5)
        assert packet['acquisition_mode'] == 'official_sec_collection'
        assert model.clock(packet['source_generated_at']) >= model.clock(packet['collection_started_at'])
        assert model.clock(packet['generated_at']) >= model.clock(packet['source_generated_at'])
        r.kv(scheduled_publication={'generated_at': packet['generated_at'], 'source_generated_at': packet['source_generated_at'],
                                  'request_id': request_id, 'replay': packet['replay']})
        output = verify_current(raw); packet = read(model.CURRENT)
        assert output['fund_count'] == 18 and output['current_cohort_count'] == 15
        assert output['collection_request_id'] == request_id
        assert model.clock(output['source_generated_at']) >= when
        assert all(output[k] == v for k, v in model.PERMISSION.items())
        originals, native_rows, comparable_rows = 0, 0, 0
        for name, fund in output['funds'].items():
            detail = read(fund['detail']['key'])
            for accession, ref in detail['filings'].items():
                filing = read(ref['key']); originals += 1; native_rows += len(filing['rows'])
                assert filing['reconciliation'] == {'row_count_exact': True, 'value_sum_exact': True}
            comparable_rows += len(detail['comparison']['rows'])
        assert originals == 37 and native_rows == 132975
        berkshire = read(output['funds']['BERKSHIRE']['detail']['key'])
        ko = next(v for v in berkshire['comparison']['rows'] if v['identity']['cusip'] == '191216100')
        assert ko['status'] == 'reported_quantity_unchanged' and ko['reported_quantity_change'] == '0'
        assert ko['reported_value_change_usd'] == '2088000000' and ko['inferred_purchase_usd'] is None
        citadel = read(output['funds']['CITADEL']['detail']['key'])
        assert citadel['periods']['2026-06-30']['effective_accessions'] == ['0001104659-26-104387']
        assert citadel['periods']['2026-06-30']['effective_native_rows'] == 16122
        pershing = output['funds']['PERSHING']
        assert pershing['latest_submission']['form'] == '13F-NT' and pershing['current_cohort_eligible'] is False
        baupost = read(output['funds']['BAUPOST']['detail']['key'])
        assert baupost['periods']['2026-06-30']['reported_value_usd'] == '5415853'
        assert baupost['periods']['2026-06-30']['valuation_reviews']
        snapshots = read(output['legacy_snapshot']['key'])['objects']; preserved = []
        for item in snapshots:
            if item['status'] == 'absent_at_snapshot':
                assert before[item['source']] is None
                continue
            key = store.PRIVATE + item['sha256'] + '.bin'
            body = store.bounded(s3.get_object(Bucket=bucket, Key=key)['Body'])
            assert hashlib.sha256(body).hexdigest() == item['sha256'] and len(body) == item['bytes']
            assert denied('https://' + bucket + '.s3.amazonaws.com/' + key) and denied('https://justhodl.ai/' + key)
            preserved.append(item['source'])
        for key, sha in before.items():
            if sha is not None:
                assert hashlib.sha256(raw(key)).hexdigest() == sha, 'Legacy product changed during controlled research: ' + key
        response, _ = invoke_when_available(lam, dict(FunctionName=fn, InvocationType='RequestResponse',
            Payload=model.encoded({'action': 'holdings_research_read'})))
        result = json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result['statusCode'] == 200
        assert json.loads(result['body']) == packet and read(model.CURRENT) == packet
        current_schedule = scheduler.get_schedule(Name=schedule['Name'], GroupName='default')
        assert all(current_schedule[k] == schedule[k] for k in ('ScheduleExpression', 'State', 'Target'))
        assert read('data/ops/releases/' + fn + '.json')['commit'] == expected
        assert lam.get_function_configuration(FunctionName=fn)['CodeSha256'] == conf['CodeSha256']
        proof = {'contract': 'holdings-native-verification.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
                 'commit': expected, 'code_sha256': conf['CodeSha256'], 'research_generated_at': output['generated_at'],
                 'replay': packet['replay'], 'output_sha256': model.digest(output), 'replay_reproduced': True,
                 'collection_request_id': request_id, 'source_generated_at': output['source_generated_at'], 'filings': originals, 'native_rows': native_rows, 'comparison_rows': comparable_rows,
                 'funds': 18, 'current_period_funds': 15, 'protected_legacy_verified': preserved,
                 'legacy_products_unchanged': True, 'legacy_collector_invoked': False,
                 'unchanged_quantity_regression_verified': True, 'restatement_regression_verified': True,
                 'read_action_did_not_recollect': True, 'paid_ai_calls': 0, 'notifications_sent': 0,
                 'private_account_reads': 0, 'portfolio_writes': 0, **model.PERMISSION,
                 'schedule': {'name': schedule['Name'], 'state': schedule['State'], 'expression': schedule['ScheduleExpression'], 'timezone': schedule['ScheduleExpressionTimezone']},
                 'scheduler_execution_verified': True, 'direct_collector_invokes': 0,
                 'scheduler_evidence_run': '35472690408', 'new_scheduler_jobs': 0,
                 'scheduler_report_sha256': hashlib.sha256(report_bytes).hexdigest(),
                 'remaining': 'Legacy producer and downstream consumer migration are still outstanding.'}
        s3.put_object(Bucket=bucket, Key='data/holdings-research-verification.json', Body=model.encoded(proof),
                      ContentType='application/json', CacheControl='no-store')
        assert read('data/holdings-research-verification.json') == proof
        r.kv(acceptance=proof)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('Holdings source acceptance failed; inspect sanitized committed report.')
        sys.exit(1)
