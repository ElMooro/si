"""Serialized complete source refresh; only independently qualified readiness advances.

Six separate runner jobs respect the existing 0.5/sec provider limit. Durable
phase claims survive runner loss; retries cannot repeat an ambiguous acquisition.
Neither source collection nor qualification invokes the native publisher.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import json, re, time, urllib.request
import share_structure_sources as capture
import share_structure_campaign as campaign
import capital_structure_source as source
import capital_structure_store as store
import capital_structure_producer as producer
import capital_structure_readiness as readiness
import statement_identity_capture as identity_capture
import financial_statement_campaign as accounting
import retained_access_evidence as access

CONTROL = source.PRIVATE + 'refresh-control.json'
CONTRACT = 'capital-structure-recurring-refresh.v1'
ACCEPTED_REQUEST = 'chatgpt-capital-structure-research-candidate-6090'
ACCEPTED_STATUS = capture.request_key(ACCEPTED_REQUEST, 'candidate')
NAMES, REQUESTS, PARTS = 1615, 11305, 6
INTERVAL = 2.0
BUCKET = capture.BUCKET


def now():
    return datetime.now(timezone.utc).isoformat()


def request_id(run):
    if not isinstance(run, str) or not re.fullmatch('[0-9]{1,20}', run):
        raise ValueError('Actual GitHub workflow run identity required')
    return 'scheduled-capital-structure:' + run


def load_control(client):
    try:
        obj = client.get_object(Bucket=BUCKET, Key=CONTROL)
        raw = store.bounded(obj['Body'])
        source.strict(raw)  # Reject duplicate keys and nonfinite JSON first.
        # Control timing fields are operational metadata, not original financial
        # decimals. Preserve their JSON types for the following conditional write.
        return json.loads(raw), obj['ETag'], raw
    except Exception as exc:
        if not producer.missing(exc): raise
        return None, None, None


def save_control(client, state, etag):
    body = source.encoded(state)
    if len(body) > source.MAX: raise ValueError('Bounded refresh control required')
    client.put_object(Bucket=BUCKET, Key=CONTROL, Body=body, ContentType='application/json', CacheControl='no-store',
                      **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
    current, new_etag, actual = load_control(client)
    if actual != body: raise ValueError('Refresh control readback differs')
    return new_etag


def baseline_plan(client, clock):
    accepted = campaign.read_journal(client, ACCEPTED_STATUS)
    if (accepted.get('request_id') != ACCEPTED_REQUEST or accepted.get('status') != 'complete'
            or accepted.get('counts', {}).get('reported_names') != NAMES
            or accepted.get('counts', {}).get('provider_responses') != REQUESTS
            or accepted.get('qualification', {}).get('all_original_rows_conserved') is not True):
        raise ValueError('Accepted complete predecessor population required')
    manifest = json.loads(capture.read(client, accepted['source_manifest']))
    old = json.loads(capture.read(client, manifest['plan']))
    baseline = json.loads(capture.read(client, old['baseline']))
    inventory = json.loads(capture.read(client, baseline['inventory']))
    labels = inventory.get('candidate_provider_labels')
    if (baseline.get('status') != 'complete' or inventory.get('all_current_rows_conserved') is not True
            or inventory.get('candidate_provider_label_count') != NAMES
            or labels != old.get('reported_symbols') or labels != manifest.get('reported_symbols')
            or len(capture.population(labels)) != NAMES):
        raise ValueError('Complete accepted label population must be conserved')
    document = campaign.plan(labels, old['baseline'], old['probe'], old['accounting'], clock)
    if document['planned_sources'] != REQUESTS or document['batches'] != PARTS:
        raise ValueError('Exact complete seven-source population required')
    return document


def private_access(client, keys, phase, check=access.check):
    keys = sorted(set(keys))
    if not keys or len(keys) > 8000: raise ValueError('Bounded complete private-access population required')
    for key in keys: access.urls(key)
    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(check, keys))
    evidence = capture.retain(client, source.encoded({'contract': 'capital-structure-refresh-access.v1',
                                                     'phase': phase, 'outcomes': outcomes}))
    outcomes.append(check(evidence['key']))
    summary = access.summarize(outcomes)
    if not summary['all_denied']: raise ValueError('Private access unverified; inspect retained evidence')
    return {'evidence': evidence, 'summary': summary}


def protected_batch(client, manifest_ref):
    manifest = json.loads(capture.read(client, manifest_ref))
    keys = {CONTROL, manifest_ref['key'], manifest['plan']['key'],
            capture.request_key(manifest['request_id'], 'batch:'+str(manifest['part']))}
    for ref in manifest['captures'].values():
        cap = json.loads(capture.read(client, ref))
        keys.update((ref['key'], cap['original']['key'], cap['request_status_key']))
    return keys


def transport():
    # Keep the independently reviewed campaign/compiler unchanged. This stricter
    # outer limiter serializes actual request starts across its three workers.
    limiter = capture.Rate(interval=INTERVAL)
    opener = urllib.request.build_opener(capture.NoRedirect())
    def once(request, timeout):
        limiter.acquire()
        return opener.open(request, timeout=timeout)
    return once


def run(client, run_id, phase, credential=None, clock=now, audit=private_access,
        identity_fetch=None, source_transport=None, qualification=readiness.run):
    request = request_id(run_id)
    phases = ['plan'] + ['part-'+str(i) for i in range(1, PARTS+1)] + ['qualify']
    if phase not in phases: raise ValueError('Exact refresh phase required')
    state, etag, previous_bytes = load_control(client)
    if phase == 'plan':
        if state and state.get('request_id') == request:
            raise ValueError('Refresh already attempted; do not repeat a workflow run')
        if state and (state.get('contract') != CONTRACT or state.get('status') != 'complete'):
            raise ValueError('Previous refresh incomplete or failed; explicit review is required')
        if state and source.clock(state['started_at']).date() >= source.clock(clock()).date():
            raise ValueError('At most one complete source sweep per UTC day')
        previous = capture.retain(client, previous_bytes) if previous_bytes else None
        state = {'contract': CONTRACT, 'request_id': request, 'run_id': run_id,
                 'status': 'running', 'started_at': clock(), 'completed_phases': [], 'parts': [],
                 'previous_cycle': previous, 'request_interval_seconds': INTERVAL,
                 'planned_sources': REQUESTS, 'reported_names': NAMES}
    elif not state or state.get('contract') != CONTRACT or state.get('request_id') != request or state.get('status') != 'running':
        raise ValueError('This exact active refresh must precede its next phase')
    if state.get('active_phase') or state.get('completed_phases') != phases[:phases.index(phase)]:
        raise ValueError('Phase already attempted or predecessor phase not complete')
    if not 0 <= (source.clock(clock())-source.clock(state['started_at'])).total_seconds() < 18*3600:
        raise ValueError('Refresh exceeded its whole-population acquisition window')
    phase_key = capture.request_key(request, 'phase:'+phase)
    # First CAS wins even if two dispatches bypass the Actions concurrency group.
    state.update(active_phase=phase, phase_started_at=clock())
    etag = save_control(client, state, etag)
    capture.journal(client, phase_key, {'request_id': request, 'phase': phase, 'status': 'claimed', 'started_at': clock()}, True)
    started = time.monotonic()
    try:
        if phase == 'plan':
            document = baseline_plan(client, clock)
            state['plan'] = capture.retain(client, source.encoded(document))
            state['identity'] = (identity_fetch or (lambda: identity_capture.capture(client, request, clock)))()
            cap = json.loads(accounting.read(client, state['identity']))
            keys = {CONTROL, phase_key, state['plan']['key'], state['identity']['key'], cap['original']['key'],
                    accounting.request_key(request, 'sec-identity')}
            result = {'plan': state['plan'], 'identity': state['identity'], 'sec_identity_requests': 1}
        elif phase.startswith('part-'):
            part = int(phase.removeprefix('part-'))
            if not isinstance(credential, str) or not credential:
                raise ValueError('Existing managed source credential required')
            ref, made = campaign.run_batch(client, request, state['plan'], part, credential, clock,
                                           lambda spec: None, remaining_seconds=5100,
                                           transport=source_transport or transport())
            if not made: raise ValueError('Unexpected previously executed batch; review retained state')
            state['parts'].append(ref)
            result = {'part': part, 'manifest': ref, 'counts': json.loads(capture.read(client, ref))['counts']}
            keys = protected_batch(client, ref) | {phase_key}
        else:
            # Reparse every disjoint whole response before independent arithmetic.
            complete = campaign.complete_population(client, state['plan'], state['parts'])
            if len(complete['reported_symbols']) != NAMES or complete['counts']['complete_sources'] != REQUESTS:
                raise ValueError('Complete accepted population required before qualification')
            manifest = capture.retain(client, source.encoded(complete))
            state['source_manifest'] = manifest
            keys = {CONTROL, phase_key, manifest['key'], producer.READY,
                    producer.request_key('qualification:'+request)}
            # Verify these protected paths before advancing private readiness.
            result = {'source_manifest': manifest, 'counts': complete['counts']}
        privacy = audit(client, keys, phase)
        if privacy.get('summary', {}).get('all_denied') is not True:
            raise ValueError('Every private-access outcome must pass')
        if phase == 'qualify':
            qualified = qualification(client, BUCKET, request, manifest, state['identity'], remaining_seconds=4500, clock=clock)
            if not qualified.get('ready_advanced') or qualified.get('reason') != 'qualified':
                raise ValueError('Complete qualified readiness did not advance; inspect retained result')
            result['qualification'] = qualified
        result.update(phase=phase, elapsed_seconds=round(time.monotonic()-started, 3), anonymous_access=privacy,
                      producer_invocations=0, consumer_invocations=0, current_head_writes=0,
                      private_account_reads=0, paid_ai_calls=0, notifications_sent=0)
        capture.journal(client, phase_key, {'request_id': request, 'phase': phase, 'status': 'complete', 'result': result})
        state['completed_phases'].append(phase); state.pop('active_phase')
        state.update(last_result=result, status='complete' if phase == 'qualify' else 'running', updated_at=clock())
        save_control(client, state, etag)
        return result
    except Exception as exc:
        # Failed control remains a stop for both workflow retries and future days.
        state.update(status='failed', error_type=type(exc).__name__, failed_at=clock())
        try: save_control(client, state, etag)
        except Exception: pass  # Preserve the earlier durable active-phase claim.
        raise
