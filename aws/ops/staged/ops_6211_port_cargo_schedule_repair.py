"""Quarantine only the extra classic rule from deploy 36301127898.

Preserve the original Scheduler job and every input. No producer invocation.
The full extra-rule configuration remains retained and the rule stays present.
"""
from pathlib import Path
from copy import deepcopy
from datetime import datetime
import json, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-port-cargo/source', 'aws/shared', 'scripts')]
from ops_report import report
from market_runtime_evidence import runtime
from normalize_lambda_config import normalize_config
import cargo_store as store
import lambda_function as native
FN = 'justhodl-port-cargo'; BUCKET = 'justhodl-dashboard-live'
NAME = 'justhodl-port-cargo-daily'
ARN = 'arn:aws:lambda:us-east-1:857687956942:function:' + FN
RULE_ARN = 'arn:aws:events:us-east-1:857687956942:rule/' + NAME
EXPRESSION = 'cron(40 12 * * ? *)'
DESCRIPTION = '12:40 UTC daily — after PortWatch upstream refresh'


def configuration(value):
    return {key: item for key, item in value.items() if key != 'ResponseMetadata'}


def verify_extra(rule, targets):
    expected = {'Name': NAME, 'Arn': RULE_ARN, 'ScheduleExpression': EXPRESSION,
                'State': rule.get('State'), 'Description': DESCRIPTION, 'EventBusName': 'default'}
    if 'CreatedBy' in rule: expected['CreatedBy'] = '857687956942'
    if configuration(rule) != expected or rule.get('State') not in ('ENABLED', 'DISABLED'):
        raise ValueError('Extra rule does not match the exact deploy-created binding')
    if configuration(targets) != {'Targets': [{'Id': 'audit-'+FN, 'Arn': ARN}]}:
        raise ValueError('Extra rule has different, additional or paginated targets')


def verify_baseline(actual, baseline, state):
    extra = {'kind': 'EventBridge rule', 'name': NAME, 'state': state, 'expression': EXPRESSION, 'native_targets': 1}
    schedules = list(actual['schedules'])
    if schedules.count(extra) != 1: raise ValueError('Exactly one known extra rule required')
    schedules.remove(extra)
    if schedules != baseline['schedules']: raise ValueError('Original native schedules differ')
    for old, new in {'Runtime': 'runtime', 'Handler': 'handler', 'MemorySize': 'memory_mb', 'Timeout': 'timeout', 'Architectures': 'architectures', 'Role': 'role'}.items():
        if baseline['runtime'][old] != actual[new]: raise ValueError('Original runtime differs: '+old)
    if baseline['runtime']['EphemeralStorage']['Size'] != actual['ephemeral_storage_mb']:
        raise ValueError('Original ephemeral storage differs')


def quarantine(events, scheduler, retain):
    original = scheduler.get_schedule(Name=NAME, GroupName='default')
    if (original.get('Name') != NAME or original.get('GroupName', 'default') != 'default'
            or original.get('Target', {}).get('Arn') != ARN or original.get('State') != 'ENABLED'
            or original.get('ScheduleExpression') != EXPRESSION or original.get('ScheduleExpressionTimezone') != 'UTC'):
        raise ValueError('Original enabled Scheduler job must remain intact')
    rule = events.describe_rule(Name=NAME)
    targets = events.list_targets_by_rule(Rule=NAME)
    verify_extra(rule, targets)
    saved = retain({'rule': configuration(rule), 'targets': configuration(targets),
                    'original_scheduler': configuration(original), 'cause_run': 36301127898})
    # Recheck identities after retention and immediately before the sole mutation.
    if configuration(events.describe_rule(Name=NAME)) != configuration(rule) or configuration(events.list_targets_by_rule(Rule=NAME)) != configuration(targets):
        raise ValueError('Extra rule changed during retention')
    if configuration(scheduler.get_schedule(Name=NAME, GroupName='default')) != configuration(original):
        raise ValueError('Original schedule changed before quarantine')
    changed = rule['State'] == 'ENABLED'
    if changed: events.disable_rule(Name=NAME)
    after = events.describe_rule(Name=NAME)
    expected = {**configuration(rule), 'State': 'DISABLED'}
    if configuration(after) != expected or configuration(events.list_targets_by_rule(Rule=NAME)) != configuration(targets):
        raise ValueError('Extra-rule quarantine readback differs')
    if configuration(scheduler.get_schedule(Name=NAME, GroupName='default')) != configuration(original):
        raise ValueError('Original Scheduler payload changed')
    return {'duplicate_rule_disabled': True, 'changed_this_run': changed, 'complete_predecessors': saved,
            'original_scheduler_configuration_identical': True, 'extra_rule_deleted': False}


def main():
    lam, s3, events, scheduler = (boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6211_port_cargo_schedule_repair') as r:
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+FN], cwd=ROOT, text=True).strip()
        before = runtime(lam, s3, events, scheduler, FN)
        r.kv(actual_runtime_before=before)
        if before['receipt'] != {'status': 'matched', 'commit': expected}: raise ValueError('Exact source/config release required')
        config = normalize_config(json.loads((ROOT/'aws/lambdas'/FN/'config.json').read_bytes()))
        if 'schedule' in config or config['release_schedule_note']['status'] != 'EXISTING_SCHEDULER_REFERENCE':
            raise ValueError('Configuration must stop managing the extra classic rule')
        baseline = json.loads((ROOT/'docs/audit/2026-09-27/shipping-consumer-baseline.json').read_bytes())['consumer_runtimes'][FN]
        state = events.describe_rule(Name=NAME)['State']; verify_baseline(before, baseline, state)
        def retain(value):
            raw = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                             allow_nan=False, default=lambda v: v.isoformat() if isinstance(v, datetime) else (_ for _ in ()).throw(TypeError())).encode('utf-8')
            return store.retain(s3, BUCKET, raw)
        result = quarantine(events, scheduler, retain)
        after = runtime(lam, s3, events, scheduler, FN); verify_baseline(after, baseline, 'DISABLED')
        equivalent = deepcopy(after)
        for entry in equivalent['schedules']:
            if entry['kind'] == 'EventBridge rule' and entry['name'] == NAME: entry['state'] = state
        if equivalent != before: raise ValueError('Unrelated runtime state changed')
        subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')], cwd=ROOT, check=True)
        obj = s3.get_object(Bucket=BUCKET, Key=store.HEAD); raw = store.whole(obj['Body'], obj.get('ContentLength')); packet = store.decode(raw)
        publication = {'status': 'pending_original_daily_1240_publication', 'generated_at': packet.get('generated_at'),
                       'version': packet.get('version'), 'whole_bytes': len(raw), 'whole_sha256': store.sha(raw)}
        if packet.get('contract') == store.CONTRACT:
            publication.update(status='whole_native_source_replayed', replay=store.replay(native, s3, BUCKET, packet))
        r.kv(expected_commit=expected, actual_runtime=after, repair=result, native_publication=publication,
             compiler_sha256=store.hashes(), original_schedules_changed=0, extra_classic_rules_disabled=int(result['changed_this_run']),
             native_invocations=0, provider_requests=0, public_writes=0, account_reads=0,
             scope='Exact package and original native cadence; one erroneous additional classic rule remains disabled and retained. Native source replay still requires normal publication.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
