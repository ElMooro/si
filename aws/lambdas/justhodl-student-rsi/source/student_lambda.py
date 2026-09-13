"""Gear A: sense, independently grade, retain passes, and heal durable state.

This is a bounded experiment controller. It does not claim recursive model
training, execute arbitrary guest code, acquire resources, or release changes.
The deployed program-search generator can solve one real repair family; an
unavailable generative model is reported as unavailable, never simulated.
"""
import copy
import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.config import Config

from factory_core import (AGENTS, DIRECTIONS, Invalid, NY, REGIMES, SCHEMA, SYMBOLS, VERSION, WORKERS,
    canonical, digest, iso, retain_fact, timestamp, validate_prediction, validate_task, week_window)
import factory_discipline
from factory_repair import propose
from factory_sources import sense
from factory_store import Busy, Conflict, Store, code

PUBLIC = os.environ.get('FACTORY_PUBLIC_BUCKET', 'justhodl-dashboard-live')
PRIVATE = os.environ.get('FACTORY_PRIVATE_BUCKET', 'justhodl-ai-857687956942')
GRADER = 'justhodl-factory-grader'
CFG = Config(connect_timeout=3, read_timeout=10, retries={'max_attempts': 2})


def replace_view(store, key, value):
    store.assert_owned()
    old, etag = store.recoverable(store.public, key)
    if old == value:
        return
    store.put(store.public, key, value, etag=etag, absent=etag is None, public=True)


def public_projection(state):
    season = state.get("season") or {}
    model = state.get("model") or {}
    health = state.get("health") or {}
    return {
        "schema_version": "student-state.v1",
        "projection": "public-factory.v1",
        "generated_at": state.get("generated_at"),
        "state_version": int(state.get("state_version") or 0),
        "gen": int(state.get("gen") or 0),
        "objective": state.get("objective"),
        "agents": state.get("agents") or [],
        "skillbook": state.get("skillbook") or [],
        "wall": state.get("wall") or {},
        "season": {
            "id": season.get("id"),
            "weeks": season.get("weeks"),
            "starts_on": season.get("starts_on"),
            "crisis_definition": season.get("crisis_definition"),
            "price_sources": season.get("price_sources") or {},
        },
        "fit": state.get("fit"),
        "model": {"status": model.get("status"), "training_runs": 0},
        "health": {
            "status": health.get("status") or "live",
            "last_tick_at": health.get("last_tick_at"),
            "errors": health.get("errors") or [],
        },
        "outer_status": state.get("outer_status") or {},
        "budget": state.get("budget"),
        "ranks": factory_discipline.projection(state.get("ranks")),
        "discipline": {"last_run_at": (state.get("discipline") or {}).get("last_run_at"),
                       "changes": (state.get("discipline") or {}).get("changes") or [],
                       "doctrine": "factory-doctrine.v1"},
        "checksum": "public-projection",
    }



def keys(store, prefix, *, limit=1000):
    result, token = [], None
    while True:
        args = {'Bucket': store.private, 'Prefix': prefix, 'MaxKeys': min(1000, limit - len(result))}
        if token:
            args['ContinuationToken'] = token
        page = store.s3.list_objects_v2(**args)
        result.extend(v['Key'] for v in page.get('Contents', []))
        token = page.get('NextContinuationToken')
        if not token:
            return result
        if len(result) >= limit:
            raise Invalid('bounded_scan_limit; pagination_checkpoint_required')


def grade(lam, kind, item_id):
    response = lam.invoke(FunctionName=GRADER, InvocationType='RequestResponse', Payload=canonical({'kind': kind, 'id': item_id}))
    raw = response['Payload'].read(65537)
    if response.get('FunctionError') or len(raw) > 65536:
        raise Invalid('independent_grader_failed')
    verdict = json.loads(raw)
    if not isinstance(verdict, dict):
        raise Invalid('independent_grader_response_invalid')
    return verdict


def initial_state(now, season, policy):
    return {'schema_version': SCHEMA, 'version': VERSION, 'state_version': 0, 'generated_at': iso(now),
        'gen': 0, 'fit': None, 'skillbook': [], 'outer_status': {}, 'season': season,
        'health': {'status': 'starting', 'errors': []}, 'agents': [], 'experiments': {}, 'processed_traces': [],
        'wall': {'last_checked_at': None, 'pending': 0, 'graded': 0, 'independent_weeks': 0},
        'model': {'status': policy.get('generative_status'), 'adapter': None, 'training_runs': 0},
        'budget': {'day': now.date().isoformat(), 'experiments': 0, 'gpu_jobs': 0, 'paid_model_calls': 0},
        'objective': policy['objective'], 'deployment': {'mode': 'queue_only', 'automatic_release': False}}


def coding_battery(store, lam, state, policy):
    family = 'deployment-create-identity-v1'
    if family in state['experiments']:
        return
    if state['budget']['experiments'] >= min(1, int(policy.get('max_experiments_per_day', 0))):
        return
    candidate = propose()
    candidate.update(id=family, agent='student', source_sha256=hashlib.sha256(candidate['source'].encode()).hexdigest())
    # Stable artifact excludes a changing timestamp so retries are idempotent.
    store.immutable(store.private, 'factory/experiments/' + family + '.json', candidate)
    state['budget']['experiments'] += 1
    verdict = grade(lam, 'code', family)
    # Trust the grader's protected stored object, never a caller-supplied ok:true.
    stored, _ = store.read(store.private, 'factory/verdicts/code-identity-v1-student.json')
    if stored != verdict or verdict.get('program_hash') != candidate['program_hash']:
        raise Invalid('protected_verdict_mismatch')
    passed = verdict.get('ok') is True
    trace = {'id': family, 'domain': 'code', 'candidate': candidate, 'verdict': verdict, 'ok': passed,
             'training_eligible': passed, 'license': 'original', 'source_repository': 'https://github.com/ElMooro/si',
             'parent_commit': 'a12ce0808276b5e0fa3453ddfcda950a83dc6188'}
    store.immutable(store.private, 'factory/traces/' + ('code/' if passed else '_reject/') + family + '.json', trace)
    state['experiments'][family] = {'id': family, 'status': 'passed' if passed else 'rejected', 'verdict_id': verdict['id'],
        'source_sha256': candidate['source_sha256'], 'candidate_score': verdict['candidate']['score'],
        'baseline_score': verdict['baseline']['score'], 'held_out_cases': verdict['candidate']['n']}
    state['gen'] += 1
    if passed:
        skill = {'id': family, 'summary': 'Honor and validate the configured Lambda role and handler before creation',
                 'scope': 'bounded program search', 'evidence': '/factory/traces/code/' + family + '.json',
                 'candidate_score': verdict['candidate']['score'], 'baseline_score': verdict['baseline']['score'],
                 'independent_cases': verdict['candidate']['n'], 'model_training': False}
        store.immutable(store.private, 'factory/skillbook/' + family + '.json', skill)
        store.immutable(store.public, 'factory/traces/code/' + family + '.json', trace, public=True)
        state['skillbook'].append(skill)
        state['fit'] = {'domain': 'code', 'score': verdict['candidate']['score'], 'baseline': verdict['baseline']['score'],
                        'n': verdict['candidate']['n'], 'independent': True, 'market_score': None}
        # A candidate can enter the queue; the student cannot deploy it or name itself champion.
        if verdict['promotion']['eligible']:
            store.immutable(store.private, 'factory/queue/' + family + '.json', {'id': family, 'status': 'awaiting_owner_release',
                'source_sha256': candidate['source_sha256'], 'source': candidate['source'], 'verdict_id': verdict['id'],
                'files': ['scripts/lambda_identity.py', 'scripts/deploy_lambdas.sh']})
    event = store.append_event('code-grade', family, {'agent': 'student', 'ok': passed, 'verdict': verdict,
        'scope': 'one bounded repair family; no self-training claim'}, public=True)
    store.jsonl_view('factory/salon/events.jsonl', [event])
    store.jsonl_view('factory/salon/wall.jsonl', [event])


def guest_traces(store, lam, state, deadline):
    # A bounded lexicographic cursor cycles through the add-only queue. New keys
    # before the cursor are visited on the next cycle. Terminal receipts make
    # repeated scans and a state-write failure idempotent without a growing ID set.
    args = {'Bucket': store.private, 'Prefix': 'factory/quarantine/', 'MaxKeys': 10}
    if state.get('guest_cursor'):
        args['StartAfter'] = state['guest_cursor']
    page = store.s3.list_objects_v2(**args)
    rows = page.get('Contents', [])
    if not rows:
        state['guest_cursor'] = None
        return
    for item in rows:
        if time.monotonic() >= deadline:
            break
        key = item['Key']
        item_id = key.rsplit('/', 1)[-1].removesuffix('.json')
        done_key = 'factory/events/trace-kept-' + item_id + '.json'
        done, _ = store.read(store.private, done_key)
        if not done:
            verdict = grade(lam, 'trace', item_id)
            saved, _ = store.read(store.private, 'factory/verdicts/trace-' + item_id + '.json')
            if saved != verdict:
                raise Invalid('protected_guest_verdict_missing')
            envelope, _ = store.read(store.private, key)
            passed = verdict.get('ok') is True
            target = 'factory/traces/' + (envelope['trace']['domain'] + '/' if passed else '_reject/') + item_id + '.json'
            store.immutable(store.private, target, {'submission': envelope, 'verdict': verdict, 'ok': passed,
                                                 'training_eligible': passed})
            store.immutable(store.private, done_key, {'id': item_id, 'verdict_id': verdict['id'],
                                                    'ok': passed, 'retained_key': target})
        state['guest_cursor'] = key


def student_forecasts(store, state, now):
    season = state['season']
    monday = now.astimezone(NY).date()
    monday -= timedelta(days=monday.weekday())
    if not date.fromisoformat(season['starts_on']) <= monday < date.fromisoformat(season['starts_on']) + timedelta(weeks=season['weeks']):
        return
    window = week_window(monday.isoformat(), season)
    if not timestamp(window['opens_at']) <= now < timestamp(window['locks_at']):
        return
    for symbol in SYMBOLS:
        fact = state['outer_status'].get('tape', {}).get(symbol, {})
        history = fact.get('bars', [])
        if fact.get('stale', True) or fact.get('data_unavailable', True) or len(history) < 6:
            continue
        entry_id = monday.isoformat() + '-student-' + symbol
        existing, _ = store.read(store.private, 'factory/salon/accepted/' + entry_id + '.json')
        if existing:
            continue
        move = history[-1]['close'] / history[-6]['close'] - 1
        flat = season['flat_thresholds'][symbol]
        direction = 'UP' if move > flat else 'DOWN' if move < -flat else 'FLAT'
        regime = 'RANGE' if direction == 'FLAT' else 'TREND'
        prediction = {'id': entry_id, 'week': monday.isoformat(), 'symbol': symbol, 'direction': direction, 'regime': regime,
            'crisis_probability': .1, 'direction_probabilities': {d: .5 if d == direction else .25 for d in DIRECTIONS},
            'regime_probabilities': {r: .5 if r == regime else .25 for r in REGIMES},
            'price_source': season['price_sources'][symbol], 'data_cutoff': fact['observed_at'],
            'model_revision': 'transparent-five-session-momentum-baseline.v1'}
        accepted = validate_prediction(prediction, season, now, 'student')
        accepted['research_source_hash'] = fact['raw_sha256']
        accepted['forecast_method'] = 'fixed research baseline; not a trained model; crisis probability is an uncalibrated baseline'
        store.immutable(store.private, 'factory/salon/accepted/' + entry_id + '.json', accepted)


LEDGER_KEY = 'factory/runtime/wall-ledger.json'
TERMINAL = ('graded', 'void')
POLL_BACKOFF_S = 3600


def wall_ledger(store, season):
    """Per-season memo of what the wall already knows. It is a cache of immutable objects, never an
    authority: losing it costs one re-scan, and every event/result object stays where it was."""
    ledger, etag = store.recoverable(store.private, LEDGER_KEY)
    if not isinstance(ledger, dict) or ledger.get('season') != season['id'] or ledger.get('schema_version') != 'factory-wall-ledger.v1':
        ledger = {'schema_version': 'factory-wall-ledger.v1', 'season': season['id'], 'entries': {}, 'final_weeks': [],
                  'polls': {}, 'updated_at': None}
    return ledger, etag


def wall_scores(ledger, season):
    graded = [dict(r, id=eid) for eid, r in ledger['entries'].items() if r.get('status') == 'graded' and r.get('metrics')]
    scores = {}
    for r in graded:
        agent = r['agent']
        row = scores.setdefault(agent, {'agent': agent, 'graded_predictions': 0, 'weeks': set(), 'sum': 0,
              'crisis_brier_sum': 0, 'no_crisis_baseline_brier_sum': 0, 'missed_crises': 0, 'false_alarms': 0, 'elo': 1000.0})
        m = r['metrics']
        row['graded_predictions'] += 1
        row['weeks'].add(r['week'])
        row['sum'] += m['score']
        row['crisis_brier_sum'] += m['crisis_brier']
        row['no_crisis_baseline_brier_sum'] += m['no_crisis_baseline_brier']
        row['missed_crises'] += int(m['missed_crisis'])
        row['false_alarms'] += int(m['false_alarm'])
    # Elo moves once per independent week, against a fixed 1000 reference (display only).
    for agent, row in scores.items():
        for week in sorted(row['weeks']):
            weekly = [r['metrics']['score'] for r in graded if r['agent'] == agent and r['week'] == week]
            expected = 1 / (1 + 10 ** ((1000 - row['elo']) / 400))
            row['elo'] += season['elo_k'] * (sum(weekly) / len(weekly) - expected)
        row.update(independent_weeks=len(row.pop('weeks')), score=row.pop('sum') / row['graded_predictions'],
                   promotion_eligible=False, elo_use='display_only')
    rows = sorted(scores.values(), key=lambda row: (-row['score'], row['agent']))[:50]
    return rows, len({r['week'] for r in graded})


def market_wall(store, lam, state, now, deadline):
    """One bounded pass. Costs scale with UNFINALIZED entries, not with the season: finalized weeks are
    never listed again, first-seen entries are the only ones read, the independent grader is asked only
    when official prints exist (or once an hour when their existence cannot be checked), and the pass
    stops at the tick deadline instead of running into the Lambda timeout."""
    season = state['season']
    ledger, ledger_etag = wall_ledger(store, season)
    entries, final_weeks, polls = ledger['entries'], set(ledger['final_weeks']), ledger['polls']
    events, changed, complete, prints_seen, grader_calls = [], False, True, {}, 0
    first = date.fromisoformat(season['starts_on'])
    for week_i in range(season['weeks']):
        week = (first + timedelta(weeks=week_i)).isoformat()
        if week in final_weeks:
            continue
        window = week_window(week, season)
        if timestamp(window['opens_at']) > now:
            break  # future weeks hold nothing yet
        if time.monotonic() >= deadline:
            complete = False
            break
        week_keys = keys(store, 'factory/salon/accepted/' + week + '-')
        for key in week_keys:
            if time.monotonic() >= deadline:
                complete = False
                break
            eid = key.rsplit('/', 1)[-1].removesuffix('.json')
            rec = entries.get(eid)
            if rec is None:
                entry, _ = store.read(store.private, key)
                events.append(store.append_event('prediction', entry['id'], entry, public=True))
                rec = {'agent': entry['agent'], 'week': entry['week'], 'symbol': entry['symbol'], 'status': 'pending',
                       'direction': entry['direction'], 'regime': entry['regime'], 'crisis_probability': entry['crisis_probability'],
                       'grade_after': entry['window']['grade_after'], 'metrics': None}
                entries[eid] = rec
                changed = True
            if rec['status'] in TERMINAL or timestamp(rec['grade_after']) > now:
                continue
            pk = rec['week'] + '/' + rec['symbol']
            if pk not in prints_seen:
                try:
                    prints_seen[pk] = store.read(store.private, 'factory/official-prints/' + pk + '.json')[0] is not None
                except Exception:  # read not granted yet -> unknown; poll the grader at most hourly instead
                    prints_seen[pk] = None
            may_ask = prints_seen[pk] is True or (prints_seen[pk] is None and (
                not polls.get(pk) or (now - timestamp(polls[pk])).total_seconds() >= POLL_BACKOFF_S))
            result = None
            if prints_seen[pk] is not False:
                result, _ = store.read(store.private, 'factory/salon/results/' + eid + '.json')
            if not result and may_ask:
                outcome = grade(lam, 'market', eid)
                grader_calls += 1
                if outcome.get('status') in TERMINAL:
                    prints_seen[pk] = True       # the grader found verified prints: grade the rest of this week/symbol now
                elif prints_seen[pk] is None:
                    prints_seen[pk] = False      # one probe per week/symbol per hour is enough while prints are absent
                    polls[pk] = iso(now)
                    changed = True
                result, _ = store.read(store.private, 'factory/salon/results/' + eid + '.json')
                if result and outcome != result:
                    raise Invalid('protected_market_verdict_mismatch')
            if result and result.get('status') in TERMINAL:
                events.append(store.append_event('market-grade', 'grade-' + eid, result, public=True))
                metrics = result.get('metrics') or {}
                rec.update(status=result['status'], reason=result.get('reason'), graded_at=result.get('graded_at'),
                           metrics={k: metrics[k] for k in ('score', 'crisis_brier', 'no_crisis_baseline_brier', 'missed_crisis', 'false_alarm')} if metrics else None)
                changed = True
        else:
            # Entries are only accepted inside the 5-minute open window; ten minutes after lock the set is fixed.
            settled = timestamp(window['locks_at']) + timedelta(minutes=10) <= now
            ids = [k.rsplit('/', 1)[-1].removesuffix('.json') for k in week_keys]
            if settled and all(entries.get(i, {}).get('status') in TERMINAL for i in ids):
                final_weeks.add(week)
                changed = True
            continue
        break  # inner loop hit the deadline
    if changed:
        ledger.update(entries=entries, final_weeks=sorted(final_weeks), polls=polls, updated_at=iso(now))
        try:
            store.put(store.private, LEDGER_KEY, ledger, etag=ledger_etag, absent=ledger_etag is None)
        except Exception as exc:  # a lost memo costs one re-scan; the immutable objects remain the authority
            state['health']['errors'].append({'phase': 'wall-ledger', 'error': code(exc)})
    rows, independent_weeks = wall_scores(ledger, season)
    pending = sum(1 for r in entries.values() if r['status'] not in TERMINAL)
    summaries = sorted(({'id': eid, **{k: r.get(k) for k in ('agent', 'week', 'symbol', 'direction', 'regime', 'crisis_probability', 'status')}}
                        for eid, r in entries.items()), key=lambda r: (r['week'], r['agent'], r['symbol']))
    invitations, _ = store.read(store.private, 'factory/control/invites.json')
    invited = [{'agent': r['agent'], 'enabled': r['enabled']} for r in invitations['allowlist']]
    replace_view(store, 'factory/invites.json', {'capacity': 10, 'invited': invited, 'worldwide_after_completed_seasons': 3,
        'want_ads': [{'task': 'verified-math-traces', 'status': 'invite_only'}, {'task': 'weekly-market-forecasts', 'status': 'invite_only'}]})
    replace_view(store, 'factory/salon/board.json', {'schema_version': 'factory-board.v1', 'season': season['id'],
        'generated_at': iso(now), 'top50': rows, 'invited': invited, 'entries': summaries[-100:], 'pending': pending,
        'grade_status': 'official_prints_required', 'trace_passes_affect_elo': False})
    replace_view(store, 'factory/scoreboard.json', {'schema_version': 'factory-scoreboard.v1', 'season': season['id'],
        'generated_at': iso(now), 'rows': rows, 'independent_weeks': independent_weeks,
        'promotion_status': 'insufficient_protected_market_history', 'crisis_metric': season['crisis_definition']})
    for key in ('factory/salon/wall.jsonl', 'factory/salon/events.jsonl'):
        if events:
            store.jsonl_view(key, events)
    state['wall'] = {'last_checked_at': iso(now), 'pending': pending, 'graded': len(entries) - pending,
                     'independent_weeks': independent_weeks, 'entries': len(entries), 'final_weeks': len(final_weeks),
                     'pass_complete': complete, 'grader_calls': grader_calls, 'new_events': len(events),
                     'official_data_status': 'runner_written_prints_required'}


def tick(event, store, lam):
    started, now = time.monotonic(), store.clock()
    if event not in ({}, None):
        validate_task(event)
    try:
        store.acquire()
    except Busy:
        return {'ok': True, 'status': 'already_running'}
    try:
        policy, _ = store.read(store.private, 'factory/control/policy.json')
        season, _ = store.read(store.private, 'factory/control/season.json')
        if not policy or not season:
            raise Invalid('owner_bootstrap_required')
        clean = dict(season); frozen_hash = clean.pop('policy_hash', None)
        if digest(clean) != frozen_hash:
            raise Invalid('frozen_season_hash_mismatch')
        previous, etag = store.load_state()
        state = copy.deepcopy(previous) if previous else initial_state(now, season, policy)
        if previous:
            store.publish_mirrors(previous)
        state['health'] = {'status': 'running' if policy['enabled'] else 'paused', 'errors': []}
        state['season'] = season
        if state['budget']['day'] != now.date().isoformat():
            state['budget'] = {'day': now.date().isoformat(), 'experiments': 0, 'gpu_jobs': 0, 'paid_model_calls': 0}
        deadline = started + min(40, int(policy.get('max_tick_seconds', 0)))
        if policy['enabled']:
            # Fleet counters (factory/fleet/meta.json) are owned by the gateway (doctrine: compute cap
            # owned by factory_gateway). The old drain here reused the state ETag variable and made every
            # commit raise Conflict('state_changed'); it is gone. Recruits are materialized in `discipline`.
            outer = state['outer_status']
            if not outer.get('observed_at') or (now - timestamp(outer['observed_at'])).total_seconds() >= max(900, policy.get('sense_interval_seconds', 900)):
                try:
                    state['outer_status'] = sense(store, outer, now)
                except Exception as exc:
                    state['health']['errors'].append({'phase': 'sense', 'error': code(exc)})
            # Each phase records failures; a source outage does not erase durable skills.
            for label, function in (
                ('coding', lambda: coding_battery(store, lam, state, policy)),
                ('guests', lambda: guest_traces(store, lam, state, deadline)),
                ('forecast', lambda: student_forecasts(store, state, now))):
                if time.monotonic() >= deadline:
                    break
                try:
                    function()
                except Exception as exc:
                    state['health']['errors'].append({'phase': label, 'error': code(exc), 'detail': str(exc)[:120]})
            checked = state['wall'].get('last_checked_at')
            if time.monotonic() < deadline and (not checked or (now - timestamp(checked)).total_seconds() >= 300):
                try:
                    market_wall(store, lam, state, now, deadline)
                except Exception as exc:
                    state['health']['errors'].append({'phase': 'wall', 'error': code(exc), 'detail': str(exc)[:120]})
            # Chain of command: warehouse-graded windows -> promote / hold / retire (factory-doctrine.v1).
            ran = (state.get('discipline') or {}).get('last_run_at')
            if time.monotonic() < deadline and (not ran or (now - timestamp(ran)).total_seconds() >= 900):
                try:
                    created = factory_discipline.materialize(store, state, now)
                    changes = factory_discipline.apply_verdicts(store, state, now)
                    prior = (state.get('discipline') or {}).get('changes') or []
                    state['discipline'] = {'last_run_at': iso(now), 'materialized': len(created),
                                           'changes': (prior + changes)[-20:], 'doctrine': 'factory-doctrine.v1',
                                           'cards_active': factory_discipline.active_count(state['ranks'])}
                except Exception as exc:
                    state['health']['errors'].append({'phase': 'discipline', 'error': code(exc), 'detail': str(exc)[:120]})
        state['agents'] = [{'id': aid, 'name': name, 'purpose': purpose,
            'status': 'principle_card' if aid in ('livermore', 'wyckoff', 'soros', 'druckenmiller') else
                      'queue_only' if aid == 'deployer' else 'bounded_program_search' if aid == 'coder' else
                      'research_baseline' if aid == 'investor' else 'paused' if not policy['enabled'] else 'active'}
            for aid, name, purpose in AGENTS]
        for agent in state['agents'][5:]:
            store.immutable(store.public, 'factory/teachers/' + agent['id'] + '.json',
                {**agent, 'content_type': 'original_principle_summary', 'persona_simulation': False}, public=True)
        state['budget'].update(max_experiments_per_day=min(1, policy['max_experiments_per_day']),
            max_tick_seconds=min(40, policy['max_tick_seconds']), max_gpu_jobs_per_day=0,
            scope='work-unit limits; AWS service charges still apply')
        state['health'].update(lease_fence=store.lease['fence'], last_tick_at=iso(now),
            elapsed_seconds=round(time.monotonic() - started, 3), state_authority='private_conditional_s3',
            protected_exam='independent_grader_role', general_coding='blocked_no_verified_generative_model',
            discipline='factory-doctrine.v1')
        if state['health']['errors']:
            state['health']['status'] = 'degraded'
        state['state_version'] += 1
        state['generated_at'] = iso(now)
        sealed = store.commit_state(state, etag)
        projection = public_projection(sealed)
        for key in ('data/ai-factory.json', 'data/factory-public.json'):
            try:
                replace_view(store, key, projection)
            except Exception as exc:  # the private authority is committed; a denied mirror is reported, not fatal
                state['health']['errors'].append({'phase': 'projection', 'error': code(exc), 'key': key})
        return {'ok': True, 'version': VERSION, 'status': state['health']['status'], 'state_version': sealed['state_version'],
                'gen': state['gen'], 'checksum': sealed['checksum'], 'errors': state['health']['errors']}
    finally:
        store.release()


def lambda_handler(event, context):
    store = Store(boto3.client('s3', region_name='us-east-1', config=CFG), PRIVATE, PUBLIC, lambda: datetime.now(timezone.utc))
    return tick(event, store, boto3.client('lambda', region_name='us-east-1', config=CFG))
