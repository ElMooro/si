"""Independent, bounded Gear A checker. Never executes submitted source code.

Hidden exam seed and outcome writes are available to this role only. The
student cannot change either. Every code family gets one held-out submission;
repeat calls return the immutable verdict, never fresh feedback for tuning.
Official market outcomes remain pending until verified source prints exist.
"""
import hashlib
import json
import os
import random
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3
from botocore.config import Config

from factory_core import Invalid, canonical, digest, finite, identifier, iso, labels_from_prices, promotion_decision, score_prediction, timestamp
from factory_repair import DEFAULT_HANDLER, DEFAULT_ROLE, PROBLEM, execute, render, validate_program
from factory_store import Conflict, Store

PRIVATE = os.environ.get('FACTORY_PRIVATE_BUCKET', 'justhodl-ai-857687956942')
PUBLIC = os.environ.get('FACTORY_PUBLIC_BUCKET', 'justhodl-dashboard-live')
CFG = Config(connect_timeout=3, read_timeout=10, retries={'max_attempts': 2})


def heldout_cases(exam):
    """Independent fixtures; hidden values generated only inside the grader role."""
    rng = random.Random(exam['seed'])
    cases = []
    for index in range(exam['cases']):
        suffix = '%012x' % rng.getrandbits(48)
        role = 'arn:aws:iam::857687956942:role/factory-test/' + suffix
        handler = 'module_' + suffix + '.handle_event'
        variant = index % 8
        if variant == 0:
            cases.append(({}, {'role': DEFAULT_ROLE, 'handler': DEFAULT_HANDLER}))
        elif variant == 1:
            cases.append(({'role': role}, {'role': role, 'handler': DEFAULT_HANDLER}))
        elif variant == 2:
            cases.append(({'handler': handler}, {'role': DEFAULT_ROLE, 'handler': handler}))
        elif variant == 3:
            cases.append(({'role': role, 'handler': handler, 'timeout': rng.randint(1, 120)}, {'role': role, 'handler': handler}))
        elif variant == 4:
            cases.append(({'role': 'arn:aws:iam::111111111111:role/' + suffix}, 'reject'))
        elif variant == 5:
            cases.append(({'handler': ['bad', '', 'module;command', None][rng.randrange(4)]}, 'reject'))
        elif variant == 6:
            cases.append(({'role': [None, '', 42, 'https://example.invalid/' + suffix][rng.randrange(4)]}, 'reject'))
        else:
            cases.append(([suffix], 'reject'))
    return cases


def measure(program, cases, evaluation_id):
    passed = 0
    for inputs, expected in cases:
        try:
            actual = execute(program, inputs)
        except (ValueError, TypeError):
            actual = 'reject'
        passed += actual == expected
    return {'evaluation_id': evaluation_id, 'independent': True, 'held_out': True,
            'n': len(cases), 'passed': passed, 'score': passed / len(cases), 'critical_failures': len(cases) - passed}


def quota(store, policy):
    day = store.clock().date().isoformat()
    key = 'factory/grader/daily/' + day + '.json'
    for _ in range(3):
        current, etag = store.read(store.private, key)
        count = (current or {}).get('count', 0)
        if count >= min(50, int(policy.get('max_grades_per_day', 0))):
            raise Invalid('grader_daily_limit')
        try:
            store.put(store.private, key, {'count': count + 1, 'day': day}, etag=etag, absent=etag is None)
            return
        except Conflict:
            continue
    raise Conflict('grader_quota_busy')


def grade_code(store, experiment_id):
    candidate, _ = store.read(store.private, 'factory/experiments/' + experiment_id + '.json')
    if not candidate:
        raise Invalid('experiment_missing')
    program = validate_program(candidate.get('program'))
    if candidate.get('program_hash') != digest(program) or candidate.get('source_sha256') != hashlib.sha256(render(program).encode()).hexdigest():
        raise Invalid('candidate_artifact_mismatch')
    actor = identifier(candidate.get('agent', 'student'))
    # One locked submission per agent/problem, regardless of caller-chosen experiment ID.
    key = 'factory/verdicts/code-identity-v1-' + actor + '.json'
    prior, _ = store.read(store.private, key)
    if prior:
        if prior['program_hash'] != digest(program):
            raise Invalid('heldout_family_already_consumed')
        return prior
    exam, _ = store.read(store.private, 'factory/exams/code-identity-v1.json')
    if not exam or exam.get('frozen') is not True or not 30 <= exam.get('cases', 0) <= 128:
        raise Invalid('protected_exam_missing')
    evaluation_id = exam['id'] + '-' + digest(exam)[:16]
    cases = heldout_cases(exam)
    score = measure(program, cases, evaluation_id)
    baseline = measure({'problem': PROBLEM, 'role': 'default', 'handler': 'default', 'validate': False}, cases, evaluation_id)
    verdict = {'schema_version': 'factory-verdict.v1', 'kind': 'code', 'id': 'code-identity-v1-' + actor,
        'experiment_id': experiment_id, 'agent': actor, 'graded_at': iso(store.clock()),
        'program_hash': digest(program), 'source_sha256': candidate['source_sha256'], 'candidate': score, 'baseline': baseline,
        'promotion': promotion_decision(score, baseline), 'ok': score['critical_failures'] == 0,
        'scope': 'typed deployment identity repair; no arbitrary code execution; no model training',
        'training_eligible': score['critical_failures'] == 0, 'hidden_answers_disclosed': False}
    store.immutable(store.private, key, verdict)
    return verdict


def grade_trace(store, trace_id):
    envelope, _ = store.read(store.private, 'factory/quarantine/' + trace_id + '.json')
    if not envelope:
        raise Invalid('trace_missing')
    key = 'factory/verdicts/trace-' + trace_id + '.json'
    prior, _ = store.read(store.private, key)
    if prior:
        return prior
    trace = envelope['trace']
    reason, ok = 'unsupported_verifiable_task', False
    # These descriptors are data, not instructions. Arbitrary scripts, tool calls,
    # IAM/paid-API requests and self-attested passes have no executable surface.
    if trace.get('domain') == 'math' and set(trace.get('task', {})) == {'operation', 'a', 'b', 'answer'}:
        task = trace['task']
        try:
            a, b, answer = [Decimal(str(task[k])) for k in ('a', 'b', 'answer')]
            if any(not v.is_finite() or abs(v) > Decimal('1e12') or abs(v.as_tuple().exponent) > 12 for v in (a, b, answer)):
                raise Invalid('math_bounds')
            operation = task['operation']
            expected = {'add': lambda: a + b, 'subtract': lambda: a - b, 'multiply': lambda: a * b}.get(operation)
            if expected:
                ok = expected() == answer
                reason = 'exact_decimal_check_passed' if ok else 'wrong_answer'
        except (InvalidOperation, ValueError, TypeError, ArithmeticError):
            reason = 'invalid_math_input'
    verdict = {'schema_version': 'factory-verdict.v1', 'kind': 'trace', 'id': 'trace-' + trace_id,
        'agent': envelope['agent'], 'trace_id': trace_id, 'graded_at': iso(store.clock()),
        'ok': ok, 'training_eligible': ok, 'reason': reason, 'held_out': False, 'elo_eligible': False,
        'checker': 'bounded-decimal-v1', 'trace_hash': digest(trace)}
    store.immutable(store.private, key, verdict)
    return verdict


def grade_market(store, prediction_id):
    prediction, _ = store.read(store.private, 'factory/salon/accepted/' + prediction_id + '.json')
    if not prediction:
        raise Invalid('prediction_missing')
    key = 'factory/salon/results/' + prediction_id + '.json'
    prior, _ = store.read(store.private, key)
    if prior:
        return prior
    season, _ = store.read(store.private, 'factory/control/season.json')
    if season['policy_hash'] != prediction['policy_hash']:
        raise Invalid('season_mismatch')
    if store.clock() < timestamp(prediction['window']['grade_after']):
        return {'ok': False, 'status': 'pending', 'reason': 'week_not_closed'}
    prints, _ = store.read(store.private, 'factory/official-prints/' + prediction['week'] + '/' + prediction['symbol'] + '.json')
    if not prints:
        return {'ok': False, 'status': 'pending', 'reason': 'official_prints_unavailable'}
    required = {'source', 'source_url', 'raw_sha256', 'verified_by', 'window', 'opening', 'closes', 'sessions', 'corporate_action', 'available_at'}
    if not required <= prints.keys() or prints['verified_by'] != 'owner_runner' or prints['source'] != prediction['price_source']:
        raise Invalid('unverified_official_prints')
    if prints['window'] != prediction['window'] or prints['sessions'] != prediction['window']['sessions'] or len(prints['closes']) != len(prints['sessions']):
        raise Invalid('official_window_incomplete')
    if timestamp(prints['available_at']) > store.clock() or len(prints['raw_sha256']) != 64 or not prints['source_url'].startswith('https://'):
        raise Invalid('official_provenance_invalid')
    if prints['corporate_action'] is True:
        result = {'status': 'void', 'reason': 'corporate_action_week', 'ok': False}
    elif prints['corporate_action'] is not False:
        return {'ok': False, 'status': 'pending', 'reason': 'corporate_action_check_missing'}
    else:
        labels = labels_from_prices(prediction['symbol'], prints['opening'], prints['closes'], season)
        result = {'status': 'graded', 'ok': True, 'labels': labels, 'metrics': score_prediction(prediction, labels, season)}
    result.update(schema_version='factory-market-result.v1', id=prediction_id, prediction_id=prediction_id,
        agent=prediction['agent'], week=prediction['week'], symbol=prediction['symbol'], held_out=True,
        graded_at=iso(store.clock()), policy_hash=season['policy_hash'], evidence_hash=digest(prints))
    store.immutable(store.private, key, result)
    return result


def handle(event, store):
    if not isinstance(event, dict) or set(event) != {'kind', 'id'} or event['kind'] not in ('code', 'trace', 'market'):
        raise Invalid('grader_action_not_allowed')
    identifier(event['id'])
    policy, _ = store.read(store.private, 'factory/control/policy.json')
    if not policy or policy.get('enabled') is not True:
        return {'ok': False, 'status': 'paused'}
    quota(store, policy)
    return {'code': grade_code, 'trace': grade_trace, 'market': grade_market}[event['kind']](store, event['id'])


def lambda_handler(event, context):
    store = Store(boto3.client('s3', region_name='us-east-1', config=CFG), PRIVATE, PUBLIC, lambda: datetime.now(timezone.utc))
    try:
        return handle(event, store)
    except (Invalid, Conflict) as exc:
        return {'ok': False, 'status': 'rejected', 'reason': str(exc)}
