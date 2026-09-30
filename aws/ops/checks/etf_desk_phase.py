"""Reviewed single-rule phase migration. No invocations or target modifications.

The manifest is edited by one-entry compare-and-swap, never broadly applied.
An immutable nonsecret reversal record is required before either configuration
write. EventBridge has no conditional PutRule: rechecks narrow, not eliminate,
the read/write race; independent schedule writers must remain serialized.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FN = 'justhodl-etf-global-desk'
NAME = FN + '-daily'
ARN = 'arn:aws:lambda:us-east-1:857687956942:function:' + FN
RULE_ARN = 'arn:aws:events:us-east-1:857687956942:rule/' + NAME
BUCKET = 'justhodl-dashboard-live'
MANIFEST = 'config/schedule-manifest.json'
ARCHIVE = 'data/ops/etf-desk-phase-v1/before.json'
OLD = 'cron(20 22 * * ? *)'
NEW = 'cron(5 23 * * ? *)'
OLD_DESCRIPTION = '18:20 ET daily — after ETF Global EOD window, before constituents pressure'
NEW_DESCRIPTION = '23:05 UTC daily — after canonical ETF holdings; unchanged once-daily collection scope'
BEFORE_RULE = {'Name': NAME, 'Arn': RULE_ARN, 'ScheduleExpression': OLD,
               'State': 'ENABLED', 'Description': OLD_DESCRIPTION, 'EventBusName': 'default'}
AFTER_RULE = {**BEFORE_RULE, 'ScheduleExpression': NEW, 'Description': NEW_DESCRIPTION}
TARGETS = [{'Id': 'audit-' + FN, 'Arn': ARN}]
BOUND = 4 * 1024 * 1024


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def code(exc):
    return getattr(exc, 'response', {}).get('Error', {}).get('Code', '')


def entry(expression):
    return {'kind': 'events', 'name': NAME, 'expr': expression, 'state': 'ENABLED',
            'targets': [{'id': TARGETS[0]['Id'], 'arn': ARN, 'input': None, 'path': None}]}


def selected(doc):
    if not isinstance(doc, dict) or not isinstance(doc.get('rules'), list) or not isinstance(doc.get('schedules'), list):
        raise ValueError('manifest_shape_changed')
    matches = [r for r in doc['rules'] if r.get('name') == NAME]
    if len(matches) > 1 or any(s.get('name') == NAME for s in doc['schedules']):
        raise ValueError('ambiguous_manifest_binding')
    return matches[0] if matches else None


def replace_entry(doc, desired):
    selected(doc)
    out = deepcopy(doc)
    out['rules'] = [r for r in out['rules'] if r.get('name') != NAME]
    if desired is not None:
        # Preserve every unrelated entry and ordering, including metadata.
        original_index = next((i for i, r in enumerate(doc['rules']) if r.get('name') == NAME), len(out['rules']))
        out['rules'].insert(original_index, deepcopy(desired))
    return out


def get_json(s3, key, optional=False):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except Exception as exc:
        if optional and code(exc) in ('NoSuchKey', '404'):
            return None, None
        raise ValueError('required_evidence_read_failed:' + key) from None
    stream = obj['Body']
    try:
        raw = stream.read(BOUND + 1)
    finally:
        stream.close()
    if len(raw) > BOUND:
        raise ValueError('evidence_byte_bound')
    return json.loads(raw), obj.get('ETag')


def binding_rule(rule):
    if not isinstance(rule, dict):
        raise ValueError('invalid_rule_record')
    if 'CreatedBy' in rule and rule['CreatedBy'] != '857687956942':
        raise ValueError('rule_creator_changed')
    return {k:v for k,v in rule.items() if k != 'CreatedBy'}


def read_rule(events):
    rule = events.describe_rule(Name=NAME)
    rule = {k:v for k,v in rule.items() if k != 'ResponseMetadata'}
    targets = events.list_targets_by_rule(Rule=NAME)
    if targets.get('NextToken') or targets.get('Targets') != TARGETS:
        raise ValueError('exact_target_binding_changed')
    if binding_rule(rule) not in (BEFORE_RULE, AFTER_RULE):
        raise ValueError('unreviewed_rule_state')
    return rule


def dependencies(scheduler, events, lam):
    try:
        scheduler.get_schedule(Name=NAME, GroupName='default')
    except Exception as exc:
        if code(exc) != 'ResourceNotFoundException':
            raise ValueError('scheduler_read_denied_or_failed') from None
    else:
        raise ValueError('unexpected_scheduler_binding')
    upstream = events.describe_rule(Name='justhodl-etf-constituents-daily')
    if upstream.get('ScheduleExpression') != 'cron(45 22 * * ? *)' or upstream.get('State') != 'ENABLED':
        raise ValueError('upstream_phase_changed')
    targets = events.list_targets_by_rule(Rule='justhodl-etf-constituents-daily')
    expected = [{'Id':'1','Arn':'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-constituents'}]
    if targets.get('NextToken') or targets.get('Targets') != expected:
        raise ValueError('upstream_binding_changed')
    for fn, timeout, memory in ((FN,900,4096), ('justhodl-etf-constituents',840,3072)):
        cfg = lam.get_function_configuration(FunctionName=fn)
        if (cfg.get('State'), cfg.get('LastUpdateStatus'), cfg.get('Runtime'), cfg.get('Timeout'), cfg.get('MemorySize')) != ('Active','Successful','python3.12',timeout,memory):
            raise ValueError('reviewed_runtime_changed')
        # Never serialize environment, role or other configuration fields.


def repo_ready(root, desired_rule, desired_entry):
    config = json.loads((root / 'aws/lambdas' / FN / 'config.json').read_text(encoding='utf-8'))
    schedule = config.get('schedule') or {}
    if schedule.get('name') != NAME or schedule.get('expression') != desired_rule['ScheduleExpression'] or schedule.get('description') != desired_rule['Description'] or config.get('eventbridge_scheduler'):
        raise ValueError('repository_phase_not_prepared')
    for path in ('config/schedule-manifest.json', 'aws/ops/audit/schedule-manifest.json'):
        if selected(json.loads((root / path).read_text(encoding='utf-8'))) != desired_entry:
            raise ValueError('repository_manifest_not_prepared')


def safe_clock(clock):
    at = clock()
    if at.tzinfo is None or at.astimezone(timezone.utc).hour >= 22:
        raise ValueError('outside_safe_utc_window_before_2200')


def migrate(events, scheduler, lam, s3, *, rollback=False, root=ROOT,
            clock=lambda: datetime.now(timezone.utc), revision='unrecorded'):
    dependencies(scheduler, events, lam)
    rule = read_rule(events)
    manifest, etag = get_json(s3, MANIFEST)
    current_entry = selected(manifest)
    if current_entry not in (None, entry(OLD), entry(NEW)) or not etag:
        raise ValueError('unreviewed_manifest_entry')
    archive, _ = get_json(s3, ARCHIVE, optional=True)
    if archive is None:
        if rollback or binding_rule(rule) != BEFORE_RULE or current_entry not in (None, entry(OLD)):
            raise ValueError('reversal_record_required')
        safe_clock(clock)
        # Refuse even an archival write if the repository is not migration-ready.
        repo_ready(root, AFTER_RULE, entry(NEW))
        archive = {'contract':'etf-desk-phase-reversal.v1', 'rule':rule, 'targets':TARGETS,
                   'manifest_entry':current_entry, 'manifest_etag':etag,
                   'manifest_sha256':hashlib.sha256(encoded(manifest)).hexdigest(),
                   'recorded_at':clock().isoformat(), 'revision':revision,
                   'scope':'Exact existing rule and its manifest entry only; UTC classic rule; no Scheduler flexible window exists.'}
        try:
            s3.put_object(Bucket=BUCKET, Key=ARCHIVE, Body=encoded(archive), ContentType='application/json', IfNoneMatch='*')
        except Exception:
            raise ValueError('reversal_archive_write_failed_retry_after_inspection') from None
        saved, _ = get_json(s3, ARCHIVE)
        if saved != archive:
            raise ValueError('reversal_archive_readback_failed')
    if (archive.get('contract') != 'etf-desk-phase-reversal.v1' or binding_rule(archive.get('rule')) != BEFORE_RULE
            or archive.get('targets') != TARGETS or archive.get('manifest_entry') not in (None, entry(OLD))):
        raise ValueError('reversal_record_differs')
    before_entry = archive['manifest_entry']
    if current_entry not in (before_entry, entry(NEW)):
        raise ValueError('manifest_changed_since_archive')
    desired_rule = deepcopy(archive['rule'])
    if not rollback:
        desired_rule.update(ScheduleExpression=NEW, Description=NEW_DESCRIPTION)
    desired_entry = before_entry if rollback else entry(NEW)
    repo_ready(root, desired_rule, desired_entry)
    if rule == desired_rule and current_entry == desired_entry:
        return {'status':'already_restored' if rollback else 'already_migrated', 'aws_configuration_writes':0, 'rollback_key':ARCHIVE}
    safe_clock(clock)
    # Guard both snapshots immediately before the first configuration write.
    if read_rule(events) != rule:
        raise ValueError('rule_changed_before_write')
    latest, latest_etag = get_json(s3, MANIFEST)
    if latest != manifest or latest_etag != etag:
        raise ValueError('manifest_changed_before_write')
    writes = 0
    if current_entry != desired_entry:
        updated = replace_entry(manifest, desired_entry)
        try:
            s3.put_object(Bucket=BUCKET, Key=MANIFEST, Body=encoded(updated), ContentType='application/json', IfMatch=etag)
        except Exception:
            raise ValueError('manifest_compare_and_swap_failed') from None
        confirmed, _ = get_json(s3, MANIFEST)
        if confirmed != updated:
            raise ValueError('manifest_readback_failed')
        writes += 1
    safe_clock(clock)
    if read_rule(events) != rule:
        raise ValueError('rule_changed_before_put_rule')
    if rule != desired_rule:
        request = {k:v for k,v in desired_rule.items() if k not in ('Arn', 'CreatedBy')}
        events.put_rule(**request)
        writes += 1
    confirmed, _ = get_json(s3, MANIFEST)
    if read_rule(events) != desired_rule or selected(confirmed) != desired_entry:
        raise ValueError('migration_readback_failed')
    return {'status':'restored' if rollback else 'migrated', 'aws_configuration_writes':writes,
            'rule_arn':RULE_ARN, 'expression':desired_rule['ScheduleExpression'], 'rollback_key':ARCHIVE,
            'target_changed':False, 'native_invocations':0, 'provider_requests':0}


def run_operation(rollback=False, report_name=None):
    import os
    import subprocess
    import sys
    if os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('GITHUB_REF') != 'refs/heads/main':
        raise SystemExit('Run only through a reviewed GitHub Actions dispatch on main.')
    import boto3
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    revision = subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip()
    name = 'etf_desk_phase_rollback' if rollback else 'etf_desk_phase_migration'
    with report(report_name or name) as r:
        clients = {n:boto3.client(n,region_name='us-east-1') for n in ('events','scheduler','lambda','s3')}
        try:
            result = migrate(clients['events'],clients['scheduler'],clients['lambda'],clients['s3'],rollback=rollback,revision=revision)
        except ValueError as exc:
            r.fail(str(exc)); raise
        except Exception:
            raise RuntimeError('AWS operation failed; inspect retained reversal and exact binding before retry') from None
        r.kv(**result, revision=revision, daily_frequency=1, fleet_apply=False, paid_provider_calls=0)
