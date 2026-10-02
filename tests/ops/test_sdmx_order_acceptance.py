"""Invented SDK/package fixtures for the runner-only technical acceptance."""
import base64
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import types
import tempfile
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'aws/ops/staged/ops_6442_sdmx_order_acceptance.py'
spec = importlib.util.spec_from_file_location('sdmx_acceptance', PATH)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def fixture(candidate=True, alter=None, alter_receipt=None, initial_ephemeral=None, missing_retry=False):
    expected = b'candidate fixture: intentionally distinct bytes\n'
    predecessor = b'predecessor fixture\n'
    temporary = tempfile.TemporaryDirectory(prefix='sdmx-probe-')
    source = Path(temporary.name) / 'aws/lambdas/justhodl-sdmx-walker/source'
    source.mkdir(parents=True)
    (source / 'lambda_function.py').write_bytes(expected)
    actual = expected if candidate else predecessor
    package = io.BytesIO()
    with zipfile.ZipFile(package, 'w') as archive:
        archive.writestr('lambda_function.py', actual)
    raw = package.getvalue()
    code_sha = base64.b64encode(hashlib.sha256(raw).digest()).decode()
    config = json.loads((ROOT / 'aws/lambdas/justhodl-sdmx-walker/config.json').read_bytes())
    (source.parent / 'config.json').write_text(json.dumps(config), encoding='utf-8')
    arn = 'arn:aws:lambda:us-east-1:857687956942:function:' + probe.FUNCTION
    live = {'FunctionName': probe.FUNCTION, 'State': 'Active', 'LastUpdateStatus': 'Successful',
            'CodeSha256': code_sha, 'FunctionArn': arn, 'Runtime': config['runtime'],
            'Handler': config['handler'], 'MemorySize': config['memory'], 'Timeout': config['timeout'],
            'EphemeralStorage': {'Size': config['ephemeral_mb'] if initial_ephemeral is None else initial_ephemeral}, 'Role': config['role'],
            'Environment': {'Variables': {**config['env'], 'PRIVATE_CANARY': 'DO_NOT_PUBLISH'}},
            'TracingConfig': {'Mode': 'Active'}, 'DeadLetterConfig': {
                'TargetArn': 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default'}, 'Architectures': ['x86_64']}
    def get_function(**kw):
        return {'Configuration': live, 'Code': {'Location': 'PRIVATE_SIGNED_URL'}}
    lam = types.SimpleNamespace(get_function=get_function,
        get_function_concurrency=lambda **kw: {})
    def get_object(**kw):
        receipt = {'function': probe.FUNCTION, 'verified': True, 'code_sha256': code_sha,
                   'commit': 'f' * 40, 'deployed_at': '2026-10-02T00:00:00Z',
                   'source': {'lambda_function.py': {'sha256': hashlib.sha256(expected).hexdigest()}}}
        if alter_receipt:
            alter_receipt(receipt)
        return {'Body': io.BytesIO(json.dumps(receipt).encode())} if candidate else None
    s3 = types.SimpleNamespace(get_object=get_object,
        head_object=lambda **kw: {'LastModified': '2026-10-02T00:01:00Z', 'ContentLength': 200})
    events = types.SimpleNamespace(describe_rule=lambda **kw: {'Name': kw['Name'], 'State': 'ENABLED',
        'ScheduleExpression': 'rate(5 minutes)'}, list_targets_by_rule=lambda **kw: {'Targets': [
            {'Id': '1', 'Arn': arn, 'Input': 'PRIVATE_TARGET_PAYLOAD', 'RoleArn': 'PRIVATE_ROLE'}]})
    class Missing(Exception):
        response = {'Error': {'Code': 'ResourceNotFoundException'}}
    def get_schedule(**kwargs):
        if missing_retry and kwargs['Name'] == 'justhodl-eurostat-retry-30min':
            raise Missing('PRIVATE_MISSING_MESSAGE')
        return {'Name': kwargs['Name'], 'State': 'ENABLED',
            'ScheduleExpression': 'rate(1 hour)', 'Target': {'Arn': arn, 'Input': 'PRIVATE_TARGET_PAYLOAD',
            'RoleArn': 'PRIVATE_ROLE'}, 'FlexibleTimeWindow': {'Mode': 'OFF'}}
    scheduler = types.SimpleNamespace(get_schedule=get_schedule)
    def git_show(command, **kwargs):
        return predecessor if command[2].startswith('ab0a502c') else expected
    if candidate:
        # Capture a true predecessor baseline before testing candidate acceptance.
        prior_package = io.BytesIO()
        with zipfile.ZipFile(prior_package, 'w') as archive:
            archive.writestr('lambda_function.py', predecessor)
        prior_raw = prior_package.getvalue()
        live['CodeSha256'] = base64.b64encode(hashlib.sha256(prior_raw).digest()).decode()
        original_get_object = s3.get_object
        s3.get_object = lambda **kw: None
        baseline_path = Path(temporary.name) / 'baseline.json'
        with patch.object(probe, 'ROOT', Path(temporary.name)), \
             patch.object(probe, 'BASELINE', baseline_path), \
             patch.object(probe.subprocess, 'check_output', git_show):
            baseline = probe.inspect(lam, s3, events, scheduler, probe.Reader(),
                                     opener=lambda *a, **k: io.BytesIO(prior_raw))
        baseline_path.write_text(json.dumps(baseline), encoding='utf-8')
        live['CodeSha256'] = code_sha
        s3.get_object = original_get_object
    if alter:
        alter(live)
    return (lam, s3, events, scheduler), raw, git_show, temporary


def test_package_controls_schedules_and_private_projection():
    for candidate in (False, True):
        clients, raw, git_show, temporary = fixture(candidate)
        with temporary, patch.object(probe.subprocess, 'check_output', git_show), \
             patch.object(probe, 'ROOT', Path(temporary.name)), \
             patch.object(probe, 'BASELINE', Path(temporary.name) / 'baseline.json'):
            result = probe.inspect(*clients, probe.Reader(), opener=lambda *a, **k: io.BytesIO(raw))
        assert result['source_phase'] == ('candidate' if candidate else 'predecessor')
        assert result['aws_read_calls'] == 19
        assert result['named_bindings_checked'] == 10
        assert result['aws_writes'] == result['producer_invokes'] == 0
        text = json.dumps(result)
        assert all(word not in text for word in ('PRIVATE_CANARY', 'DO_NOT_PUBLISH', 'PRIVATE_TARGET_PAYLOAD', 'PRIVATE_ROLE', 'PRIVATE_SIGNED_URL'))


def test_invalid_package_or_changed_control_stops():
    for alter, bad_package in ((lambda live: live.update(Timeout=1), False), (None, True)):
        clients, raw, git_show, temporary = fixture(alter=alter)
        with temporary, patch.object(probe.subprocess, 'check_output', git_show), \
             patch.object(probe, 'ROOT', Path(temporary.name)), \
             patch.object(probe, 'BASELINE', Path(temporary.name) / 'baseline.json'):
            try:
                probe.inspect(*clients, probe.Reader(), opener=lambda *a, **k: io.BytesIO(b'bad' if bad_package else raw))
            except probe.Stop as error:
                assert str(error) in ('release_control_mismatch:timeout_matches', 'package_hash_mismatch')
            else:
                raise AssertionError('bad controls/package accepted')


def test_candidate_receipt_validation_and_source_failure_are_sanitized():
    cases = [(lambda r: r.update(commit='PRIVATE_BAD_GIT_ARGUMENT'), 'receipt_commit_invalid'),
             (lambda r: r.update(commit={'PRIVATE': True}), 'receipt_commit_invalid'),
             (lambda r: r.update(deployed_at='PRIVATE_CLOCK'), 'receipt_clock_invalid'),
             (lambda r: r.update(verified=False), 'candidate_receipt_mismatch')]
    for alter, reason in cases:
        clients, raw, git_show, temporary = fixture(alter_receipt=alter)
        with temporary, patch.object(probe.subprocess, 'check_output', git_show), \
             patch.object(probe, 'ROOT', Path(temporary.name)), \
             patch.object(probe, 'BASELINE', Path(temporary.name) / 'baseline.json'):
            try:
                probe.inspect(*clients, probe.Reader(), opener=lambda *a, **k: io.BytesIO(raw))
            except probe.Stop as error:
                assert str(error) == reason
            else:
                raise AssertionError('invalid receipt accepted')
    for remove_baseline in (False, True):
        clients, raw, git_show, temporary = fixture()
        baseline_path = Path(temporary.name) / 'baseline.json'
        if remove_baseline:
            baseline_path.unlink()
        else:
            baseline_path.write_text(json.dumps({'operating_fingerprint': 'changed'}), encoding='utf-8')
        with temporary, patch.object(probe.subprocess, 'check_output', git_show), \
             patch.object(probe, 'ROOT', Path(temporary.name)), patch.object(probe, 'BASELINE', baseline_path):
            try:
                probe.inspect(*clients, probe.Reader(), opener=lambda *a, **k: io.BytesIO(raw))
            except probe.Stop as error:
                assert str(error) == ('candidate_baseline_missing' if remove_baseline else 'operating_controls_changed')
            else:
                raise AssertionError('missing/changed baseline accepted')
    with patch.object(probe.subprocess, 'check_output', side_effect=RuntimeError('PRIVATE_GIT_ERROR')):
        try:
            probe.commit_source('f' * 40, ROOT / 'source.py')
        except probe.Stop as error:
            assert str(error) == 'commit_source_unavailable'
        else:
            raise AssertionError('Git failure was not sanitized')


def test_read_bound_and_access_denied_withhold_details():
    reader = probe.Reader()
    reader.calls = probe.MAX_CALLS
    try:
        reader.read(lambda: None)
    except probe.Stop as error:
        assert str(error) == 'api_bound_reached'
    else:
        raise AssertionError('bound ignored')
    class Denied(Exception):
        response = {'Error': {'Code': 'AccessDenied'}}
    def denied():
        raise Denied('PRIVATE_ERROR')
    try:
        probe.Reader().read(denied)
    except probe.Stop as error:
        assert str(error) == 'access_denied_stop'
    else:
        raise AssertionError('denial ignored')


def test_legacy_ephemeral_metadata_does_not_override_live_setting():
    clients, raw, git_show, temporary = fixture(initial_ephemeral=512)
    with temporary, patch.object(probe.subprocess, 'check_output', git_show), \
         patch.object(probe, 'ROOT', Path(temporary.name)), \
         patch.object(probe, 'BASELINE', Path(temporary.name) / 'baseline.json'):
        result = probe.inspect(*clients, probe.Reader(), opener=lambda *a, **k: io.BytesIO(raw))
        assert result['operating_controls']['ephemeral_storage_mb'] == 512
        assert result['baseline_compared'] is True
        clients[0].get_function()['Configuration']['EphemeralStorage']['Size'] = 10240
        try:
            probe.inspect(*clients, probe.Reader(), opener=lambda *a, **k: io.BytesIO(raw))
        except probe.Stop as error:
            assert str(error) == 'operating_controls_changed'
        else:
            raise AssertionError('live ephemeral change was accepted')


def test_optional_retry_absence_is_preserved_without_ignoring_other_errors():
    clients, raw, git_show, temporary = fixture(missing_retry=True)
    with temporary, patch.object(probe.subprocess, 'check_output', git_show), \
         patch.object(probe, 'ROOT', Path(temporary.name)), \
         patch.object(probe, 'BASELINE', Path(temporary.name) / 'baseline.json'):
        result = probe.inspect(*clients, probe.Reader(), opener=lambda *a, **k: io.BytesIO(raw))
        assert result['optional_missing_bindings'] == ['justhodl-eurostat-retry-30min']
        assert 'PRIVATE_MISSING_MESSAGE' not in json.dumps(result)
        def get_schedule(**kwargs):
            return {'Name': kwargs['Name'], 'State': 'ENABLED', 'Target': clients[2].list_targets_by_rule()['Targets'][0]}
        clients[3].get_schedule = get_schedule
        try:
            probe.inspect(*clients, probe.Reader(), opener=lambda *a, **k: io.BytesIO(raw))
        except probe.Stop as error:
            assert str(error) == 'operating_controls_changed'
        else:
            raise AssertionError('new optional binding was accepted')
    class Missing(Exception):
        response = {'Error': {'Code': 'ResourceNotFoundException'}}
    def get_schedule(**kwargs):
        raise Missing('PRIVATE_MISSING_MESSAGE')
    for name in ('fleet-error-monitor-sched', 'unknown'):
        try:
            probe.Reader().read(get_schedule, Name=name)
        except probe.Stop as error:
            assert str(error) == 'aws_read_failed_details_withheld'
        else:
            raise AssertionError('protected/unknown missing binding was ignored')
