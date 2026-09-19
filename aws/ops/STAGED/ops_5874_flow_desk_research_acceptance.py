"""Accept exact flow synthesis runtime, complete replay and descriptive boundaries."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
               str(ROOT/'aws/lambdas/justhodl-global-flow-desk/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
import flow_desk_research as model
import flow_desk_store as store
from replay_flow_desk_research import verify_current


def denied(url):
    try:
        request = urllib.request.Request(url, method='HEAD', headers={'User-Agent': 'justhodl-verify-release/1.0'})
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status in (401, 403, 404)
    except urllib.error.HTTPError as exc:
        return exc.code in (401, 403, 404)


def main():
    fn, consumer, bucket = 'justhodl-global-flow-desk', 'justhodl-signal-board', 'justhodl-dashboard-live'
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=340, retries={'max_attempts': 0}))
    raw = store.reader(s3, bucket)
    def read(key):
        return json.loads(raw(key))
    with report('ops_5874_flow_desk_research_acceptance') as r:
        policy = json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy'])
        deny = next(v for v in policy['Statement'] if v.get('Sid') == 'Audit20260909ImmutableOriginalBackups')
        assert deny['Effect'] == 'Deny' and deny['Principal'] == '*'
        assert {'s3:GetObject', 's3:GetObjectVersion'} <= set(deny['Action'])
        assert deny['Condition'] == {'StringNotEquals': {'aws:PrincipalAccount': '857687956942'}}
        assert 'arn:aws:s3:::' + bucket + '/audit-private/20260909-originals/*' in deny['Resource']
        runtimes = {}
        for name in (fn, consumer):
            expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + name], text=True).strip()
            deadline = time.monotonic() + 2400
            while True:
                try:
                    receipt = read('data/ops/releases/' + name + '.json')
                except Exception as exc:
                    if not store.missing(exc):
                        raise
                    receipt = {}
                if receipt.get('commit') == expected:
                    break
                assert time.monotonic() < deadline, 'Exact runtime wait expired: ' + name
                time.sleep(15)
            conf = lam.get_function_configuration(FunctionName=name)
            assert conf['CodeSha256'] == receipt['code_sha256']
            runtimes[name] = {'commit': expected, 'code_sha256': conf['CodeSha256']}
            if name == fn:
                assert conf['Timeout'] == 300 and conf['MemorySize'] == 1024
        r.kv(runtimes=runtimes, signal_board_invoked=False)
        before_history = raw(store.LEGACY_HISTORY)
        source_before = read('data/etf-true-flows.json')['replay']
        started = datetime.now(timezone.utc).isoformat()
        response, waits = invoke_when_available(lam, dict(FunctionName=fn, InvocationType='RequestResponse',
                    Payload=model.encoded({'acceptance': 'ops5874', 'notify': False})))
        result = json.loads(response['Payload'].read())
        assert not response.get('FunctionError'), result
        assert result['statusCode'] == 200, result
        result = json.loads(result['body'])
        assert result['published'] is True and result['generated_at'] >= started
        for key in ('signals_emitted', 'paid_ai_calls', 'notifications_sent', 'private_account_reads', 'portfolio_writes'):
            assert result[key] == 0
        r.kv(invocation=result, concurrent_waits=waits)
        packet = read(model.CURRENT)
        output = verify_current(raw)
        assert output['generated_at'] == result['generated_at'] and output['contract'] == model.CONTRACT
        assert output['call'] is None and output['portfolio_impact'] is None
        assert all(output[k] is False for k in ('calls_eligible', 'sizing_eligible', 'execution_eligible'))
        assert output['additional_independent_votes'] == 0
        q = output['quality']
        assert q['status'] == 'partial' and q['configured_funds'] == 128
        assert q['source_histories'] == 81 and q['aligned_funds'] == 81 and q['issuer_classified_funds'] == 49
        assert len(q['unavailable_funds']) == 47 and len(output['groups']) == 64
        assert output['period'] == {'start_date': '2026-09-10', 'end_date': '2026-09-17', 'issuer_observations': 5}
        assert output['inst_vs_retail']['institutional'] is None and output['inst_vs_retail']['retail'] is None
        assert output['hot_money']['n_scored'] == 0 and output['hot_money']['countries'] == {}
        assert output['funds']['AGG']['issuer_classification']['sub_asset_class'] == 'Multi Sectors'
        assert output['tic_context']['source_status'] == 'verified_descriptive_snapshot'
        assert output['tic_context']['calls_eligible'] is False
        assert len(output['retained_contexts']) == 12
        rows = 0
        for group in output['groups'].values():
            history = read(group['history']['key'])
            assert len(history['rows']) == group['history']['rows'] == 6628
            assert len({v['date'] for v in history['rows']}) == 6628
            assert group['calls_eligible'] is False and group['is_national_capital_flow'] is False
            if not group['coverage_count']:
                assert group['net_flow_5d_usd'] is None and group['direction'] is None
            rows += len(history['rows'])
        assert rows == 424192
        marker = read(model.PREFIX + 'migration.json')
        backups = []
        for item in marker['objects']:
            assert item['protected_backup'] is True
            key = store.PRIVATE + item['sha256'] + '.bin'
            body = s3.get_object(Bucket=bucket, Key=key)['Body'].read(item['bytes'] + 1)
            assert len(body) == item['bytes'] and hashlib.sha256(body).hexdigest() == item['sha256']
            assert denied('https://' + bucket + '.s3.amazonaws.com/' + key) and denied('https://justhodl.ai/' + key)
            backups.append(item['source'])
        assert set(backups) == {model.CURRENT, store.LEGACY_HISTORY}
        assert raw(store.LEGACY_HISTORY) == before_history
        assert read('data/etf-true-flows.json')['replay'] == source_before
        response, _ = invoke_when_available(lam, dict(FunctionName=fn, InvocationType='RequestResponse',
                    Payload=model.encoded({'requestContext': {'http': {'method': 'GET'}}})))
        result = json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result['statusCode'] == 200
        assert json.loads(result['body']) == packet and read(model.CURRENT) == packet
        events = boto3.client('events', region_name='us-east-1')
        rule = events.describe_rule(Name='justhodl-global-flow-desk-daily')
        assert rule['ScheduleExpression'] == 'cron(50 22 ? * MON-FRI *)' and rule['State'] == 'ENABLED'
        targets = events.list_targets_by_rule(Rule=rule['Name'])
        assert not targets.get('NextToken') and len(targets['Targets']) == 1
        assert targets['Targets'][0]['Arn'].split(':function:')[-1] in (fn, fn + ':$LATEST')
        for name, expected in runtimes.items():
            receipt = read('data/ops/releases/' + name + '.json')
            assert all(receipt[k] == expected[k] for k in ('commit', 'code_sha256'))
            assert lam.get_function_configuration(FunctionName=name)['CodeSha256'] == expected['code_sha256']
        proof = {'contract': 'flow-desk-research-verification.v1', 'generated_at': datetime.now(timezone.utc).isoformat(),
                 **runtimes[fn], 'runtimes': runtimes, 'research_generated_at': output['generated_at'],
                 'replay': packet['replay'], 'output_sha256': model.digest(output), 'quality': q,
                 'groups': 64, 'complete_history_rows': rows, 'replay_reproduced': True,
                 'protected_legacy_verified': backups, 'legacy_history_unchanged': True,
                 'upstream_etf_unchanged': True, 'http_reads_did_not_recollect': True,
                 'retained_context_statuses': {k: v['source_status'] for k, v in output['retained_contexts'].items()},
                 'schedule': {'kind': 'events', 'name': rule['Name'], 'cron': rule['ScheduleExpression'], 'state': rule['State']},
                 'signal_board_invoked': False, 'paid_ai_calls': 0, 'notifications_sent': 0,
                 'private_account_reads': 0, 'portfolio_writes': 0, 'calls_eligible': False, 'sizing_eligible': False,
                 'acceptance_scope': 'Reproduced descriptive fund comparisons and preserved source context. Signal Board runtime verified; its complete strategy was not invoked or qualified.'}
        s3.put_object(Bucket=bucket, Key='data/flow-desk-research-verification.json', Body=model.encoded(proof),
                      ContentType='application/json', CacheControl='no-store')
        r.kv(**proof)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('Flow desk acceptance failed; inspect committed runner report.')
        sys.exit(1)
