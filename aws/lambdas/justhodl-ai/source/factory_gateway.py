"""Authenticated factory admission; guests never receive the Brain action surface.

Called only after private_http_denied has verified the existing service token.
The Worker supplies an identity derived from a verified bearer token. Request
bodies cannot choose that identity, grant roles, schedule work or release code.
"""
import hashlib
import json
import os
import re
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from factory_core import Invalid, canonical, digest, identifier, iso, validate_prediction, verify_state
from factory_store import Conflict, Store

CFG = Config(connect_timeout=3, read_timeout=10, retries={'max_attempts': 2})


def admit_quota(store, agent, maximum):
    key = 'factory/admission/daily/' + store.clock().date().isoformat() + '-' + agent + '.json'
    for _ in range(3):
        row, etag = store.read(store.private, key)
        count = (row or {}).get('count', 0)
        if count >= maximum:
            raise Invalid('daily_submission_limit')
        try:
            store.put(store.private, key, {'count': count + 1}, etag=etag, absent=etag is None)
            return
        except Conflict:
            pass
    raise Conflict('admission_busy')


def actor(event, invites):
    headers = {k.lower(): str(v) for k, v in (event.get('headers') or {}).items()}
    uid, role = headers.get('x-jh-factory-uid', ''), headers.get('x-jh-factory-role', '')
    if not uid or len(uid) > 160 or role not in ('owner', 'user'):
        raise Invalid('verified_factory_identity_required')
    if role == 'owner':
        return 'owner', True
    for invitation in invites.get('allowlist', []):
        if invitation.get('uid') == uid and invitation.get('enabled') is True:
            return identifier(invitation['agent']), False
    raise Invalid('invitation_required')



ROSTER = ("student", "coder", "researcher", "investor", "deployer", "livermore", "wyckoff", "soros", "druckenmiller")
COMPUTE_INFLIGHT = 8
MATERIALIZE_PER_TICK = 100
DECLARE_CAP = 1000000
CHAT_KEEP = 80


def _workers(store):
    row, etag = store.read(store.private, "factory/salon/workers.json")
    if not isinstance(row, dict):
        row = {"schema_version": "factory-workers.v1", "active": [], "queued": [], "retired": 0}
    return row, etag


def chat_snapshot(store, agent, owner):
    log, _ = store.read(store.private, "factory/salon/chat/" + agent + ".json")
    workers, _ = _workers(store)
    return {
        "ok": True,
        "agent": agent,
        "owner": owner,
        "roster": list(ROSTER),
        "messages": (log or {}).get("messages", [])[-CHAT_KEEP:],
        "workers": {"active": len((workers or {}).get("active") or []), "queued": len((workers or {}).get("queued") or []),
                    "retired": (workers or {}).get("retired") or 0, "cap": ACTIVE_WORKER_CAP},
    }



def _fleet_meta(store):
    row, etag = store.read(store.private, "factory/fleet/meta.json")
    if not isinstance(row, dict):
        row = {"schema_version": "factory-fleet.v1", "declared": 0, "materialized": 0, "inflight": 0,
               "queued": 0, "retired": 0, "learn_bytes": 0, "compute_inflight_cap": COMPUTE_INFLIGHT}
    return row, etag


def chat_snapshot(store, agent, owner):
    log, _ = store.read(store.private, "factory/salon/chat/" + agent + ".json")
    meta, _ = _fleet_meta(store)
    return {
        "ok": True, "agent": agent, "owner": owner, "roster": list(ROSTER),
        "messages": (log or {}).get("messages", [])[-CHAT_KEEP:],
        "workers": {
            "active": int(meta.get("inflight") or 0),
            "queued": int(meta.get("queued") or 0),
            "declared": int(meta.get("declared") or 0),
            "materialized": int(meta.get("materialized") or 0),
            "retired": int(meta.get("retired") or 0),
            "cap": COMPUTE_INFLIGHT,
            "note": "Same 8 compute slots at 1 or 1,000,000 agents. Extra cards queue. Learning log expands.",
        },
    }


def spawn_workers(store, agent, body, policy):
    task = str(body.get("task") or body.get("text") or "").strip()
    role = identifier(str(body.get("role") or "researcher"))[:40]
    try:
        count = int(body.get("count") or 0)
    except (TypeError, ValueError):
        count = 0
    if count < 0:
        raise Invalid("spawn_count_required")
    count = min(count, DECLARE_CAP)
    if not task or len(task) > 2000:
        raise Invalid("spawn_task_required")
    if role in ("owner",) or role.startswith("teacher-"):
        raise Invalid("reserved_agent_name")
    now = iso(store.clock())
    batch_id = "batch-" + digest({"agent": agent, "task": task, "at": now, "n": count})[:16]
    batch = {"schema_version": "factory-fleet-batch.v1", "id": batch_id, "role": role, "task": task[:500],
             "requested": count, "remaining": count, "materialized": 0, "spawned_by": agent, "spawned_at": now}
    store.immutable(store.private, "factory/fleet/batches/" + batch_id + ".json", batch)
    store.immutable(store.private, "factory/fleet/inbox/" + batch_id + ".json", {"batch": batch_id, "at": now})
    meta, etag = _fleet_meta(store)
    meta = {**meta, "declared": int(meta.get("declared") or 0) + count,
            "queued": int(meta.get("queued") or 0) + count, "updated_at": now, "last_batch": batch_id}
    store.put(store.private, "factory/fleet/meta.json", meta, etag=etag, absent=etag is None)
    return {"ok": True, "created": count, "batch": batch_id, "active": int(meta.get("inflight") or 0),
            "queued": int(meta.get("queued") or 0), "declared": meta["declared"], "cap": COMPUTE_INFLIGHT,
            "note": "Accepted %s %s cards. Compute stays %s live slots. The tick materializes 100/min so the control plane never changes." % (count, role, COMPUTE_INFLIGHT)}


def _fallback_reply(target, text, state, spawned):
    bits = [
        "I am %s on Khalid's factory desk." % target,
        "Same infrastructure at any fleet size: 8 live compute slots, a queue, an expanding learning log.",
        "I will not place orders, touch IAM, or train weights.",
    ]
    low = text.lower()
    if any(w in low for w in ("chart", "volume", "qr", "tape", "pepe")):
        bits.append("Coder: chart v12 is live on /chart.html (QR tape, warehouse volume, VP). Tell me the next repair.")
    if any(w in low for w in ("spy", "qqq", "market", "wall", "predict", "crisis")):
        bits.append("Investor: CLUB WALL locks Monday 09:30 ET. I can draft SPY/QQQ/IWM/TLT/GLD/BTC from here.")
    if spawned:
        bits.append("Fleet batch accepted: %s cards. They queue; they do not create new Lambdas." % spawned)
    if "billion" in low or "million" in low:
        bits.append("A million cards is a counter + queue. Learning storage grows. Compute does not.")
    bits.append("Heard: %s" % text[:280])
    return " ".join(bits)


def chat_post(store, agent, owner, body, policy):
    allowed = {"text", "to", "spawn", "role", "task"}
    if set(body) - allowed:
        raise Invalid("chat_schema_required")
    text = body.get("text")
    if not isinstance(text, str) or not text.strip() or len(text) > 4000:
        raise Invalid("chat_text_required")
    try:
        target = identifier(str(body.get("to") or "student"))
    except Invalid:
        target = "student"
    if target not in ROSTER:
        target = "student"
    spawn_n = 0
    if "spawn" in body and body.get("spawn") not in (None, "", 0, "0"):
        try:
            spawn_n = max(0, int(body.get("spawn") or 0))
        except (TypeError, ValueError):
            raise Invalid("spawn_count_required")
    spawned = None
    want = spawn_n
    low = text.lower()
    if owner and not want and any(w in low for w in ("spawn", "create agents", "create workers", "hire", "million", "billion")):
        want = 100
    if want:
        if not owner:
            raise Invalid("owner_invitation_required")
        spawned = spawn_workers(store, agent, {"count": want, "role": body.get("role") or "researcher", "task": body.get("task") or text}, policy)
    state, _ = store.read(store.public, "data/student-state.json")
    reply = _fallback_reply(target, text, state, (spawned or {}).get("created"))
    now = iso(store.clock())
    key = "factory/salon/chat/" + agent + ".json"
    log, etag = store.read(store.private, key)
    messages = list((log or {}).get("messages") or [])
    user_msg = {"id": "u-" + digest(text + now)[:12], "at": now, "from": agent, "to": target, "role": "owner" if owner else "guest", "text": text.strip()}
    bot_msg = {"id": "a-" + digest(reply + now)[:12], "at": now, "from": target, "to": agent, "role": "agent", "text": reply, "spawn": (spawned or {}).get("created")}
    messages = (messages + [user_msg, bot_msg])[-CHAT_KEEP:]
    store.put(store.private, key, {"schema_version": "factory-chat.v1", "agent": agent, "messages": messages, "updated_at": now},
              etag=etag, absent=etag is None)
    store.immutable(store.private, "factory/fleet/learn/chat/" + user_msg["id"] + ".json",
                    {"kind": "chat", "at": now, "from": agent, "to": target, "text": text.strip()[:500]})
    return {"ok": True, "to": target, "reply": reply, "messages": messages[-12:], "workers": spawned or chat_snapshot(store, agent, owner).get("workers")}



def handle(event, method, path, body, store):
    invites, invite_etag = store.read(store.private, 'factory/control/invites.json')
    if not invites:
        raise Invalid('factory_not_initialized')
    agent, owner = actor(event, invites)
    policy, policy_etag = store.read(store.private, 'factory/control/policy.json')
    if not isinstance(body, dict) or len(canonical(body)) > 16384:
        raise Invalid('invalid_or_oversized_body')
    action = path.removeprefix('/factory/')
    if method == 'GET' and action == 'view':
        if set(body) - {'kind', 'id'}:
            raise Invalid('unknown_view_argument')
        kind = body.get('kind')
        mapping = {'state': 'data/student-state.json', 'mirror': 'student-state.json',
            'board': 'factory/salon/board.json', 'season': 'factory/salon/season.json',
            'scoreboard': 'factory/scoreboard.json', 'exams': 'factory/exams/index.json',
            'wall': 'factory/salon/wall.jsonl'}
        if kind == 'event':
            key = 'factory/salon/events/' + identifier(body.get('id')) + '.json'
        elif kind == 'trace':
            key = 'factory/traces/code/' + identifier(body.get('id')) + '.json'
        elif kind in mapping:
            key = mapping[kind]
        else:
            raise Invalid('view_not_allowed')
        if kind == 'wall':
            raw = store.s3.get_object(Bucket=store.public, Key=key)['Body'].read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise Invalid('wall_archive_view_required')
            return {'ok': True, 'raw': raw.decode(), 'content_type': 'application/x-ndjson'}
        value, _ = store.read(store.public, key)
        if value is None:
            raise Invalid('factory_view_not_ready')
        if kind in ('state', 'mirror'):
            verify_state(value)
        return {'ok': True, 'raw': canonical(value).decode(), 'content_type': 'application/json'}
    if method == 'GET' and action == 'sandbox':
        state, _ = store.read(store.public, 'data/student-state.json')
        verify_state(state)
        outer = state['outer_status']
        return {'ok': True, 'agent': agent, 'owner': owner, 'sandbox': {'funding': outer.get('funding', {}),
                'tape': outer.get('tape', {}), 'research_only': True}, 'policy': {'enabled': policy['enabled'],
                'gear_b_enabled': False, 'max_traces_per_day': policy['max_guest_traces_per_day']}}
    if method == 'GET' and action == 'chat':
        return chat_snapshot(store, agent, owner)
    if method != 'POST':
        raise Invalid('factory_action_not_allowed')
    if action == 'chat':
        return chat_post(store, agent, owner, body, policy)
    if action == 'spawn':
        if not owner:
            raise Invalid('owner_invitation_required')
        return spawn_workers(store, agent, body, policy)
    if action == 'control':
        if not owner or set(body) != {'enabled'} or type(body['enabled']) is not bool:
            raise Invalid('owner_pause_control_required')
        policy = {**policy, 'enabled': body['enabled'], 'changed_at': iso(store.clock()), 'changed_by': agent}
        store.put(store.private, 'factory/control/policy.json', policy, etag=policy_etag)
        return {'ok': True, 'enabled': policy['enabled']}
    if action == 'invites':
        if not owner or set(body) != {'uid', 'agent', 'enabled'} or type(body['enabled']) is not bool:
            raise Invalid('owner_invitation_required')
        name = identifier(body['agent'])
        if len(name) > 40:
            raise Invalid('agent_name_maximum_40_characters')
        if name in ('owner', 'student') or name.startswith('teacher-'):
            raise Invalid('reserved_agent_name')
        uid = body['uid']
        if not isinstance(uid, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', uid):
            raise Invalid('verified_user_id_required')
        rows = list(invites['allowlist'])
        if any(r['agent'] == name and r['uid'] != uid for r in rows):
            raise Invalid('agent_name_taken')
        rows = [r for r in rows if r['uid'] != uid]
        rows.append({'uid': uid, 'agent': name, 'enabled': body['enabled'], 'invited_at': iso(store.clock())})
        if len(rows) > min(10, invites.get('capacity', 10)):
            raise Invalid('initial_invitation_cap')
        store.put(store.private, 'factory/control/invites.json', {**invites, 'allowlist': rows}, etag=invite_etag)
        return {'ok': True, 'agent': name, 'enabled': body['enabled'], 'message_sent': False}
    if policy.get('enabled') is not True:
        raise Invalid('factory_paused')
    if action == 'predictions':
        season, _ = store.read(store.private, 'factory/control/season.json')
        if season.get('calendar_review_required') is not False:
            raise Invalid('season_calendar_not_frozen')
        prediction = validate_prediction(body, season, store.clock(), agent)
        if prediction['price_source'] != season['price_sources'][prediction['symbol']]:
            raise Invalid('season_price_source_required')
        # Exactly one immutable entry per agent/week/symbol, irrespective of a supplied ID.
        event_id = prediction['week'] + '-' + agent + '-' + prediction['symbol']
        prediction['submitted_id'] = prediction['id']
        prediction['id'] = event_id
        store.immutable(store.private, 'factory/salon/accepted/' + event_id + '.json', prediction)
        return {'ok': True, 'id': event_id, 'status': 'locked', 'permalink': '/ai.html#factory-event=' + event_id}
    if action == 'traces':
        if set(body) != {'domain', 'task', 'provenance', 'solution_notes'}:
            raise Invalid('trace_schema_required')
        if body['domain'] not in ('code', 'math', 'tape', 'sec') or not isinstance(body['task'], dict):
            raise Invalid('trace_domain_required')
        provenance = body['provenance']
        if not isinstance(provenance, dict) or set(provenance) != {'license', 'source'}:
            raise Invalid('trace_provenance_required')
        if provenance['license'] not in ('CC0-1.0', 'MIT', 'Apache-2.0', 'BSD-3-Clause', 'original'):
            raise Invalid('trace_license_not_allowed')
        if not isinstance(provenance['source'], str) or len(provenance['source']) > 512:
            raise Invalid('trace_source_required')
        if not isinstance(body['solution_notes'], str) or len(body['solution_notes']) > 4000:
            raise Invalid('short_solution_notes_required')
        text = canonical(body).decode().lower()
        if any(x in text for x in ('iam:', 'putrule', 'createrole', 'api.openai.com', 'api.anthropic.com', 'access_key', 'secret_key')):
            raise Invalid('void_trace_prohibited_capability')
        trace_id = agent + '-' + digest(body)[:32]
        previous, _ = store.read(store.private, 'factory/quarantine/' + trace_id + '.json')
        if previous:
            return {'ok': True, 'id': trace_id, 'status': 'already_received'}
        admit_quota(store, agent, min(10, int(policy.get('max_guest_traces_per_day', 0))))
        store.immutable(store.private, 'factory/quarantine/' + trace_id + '.json', {'schema_version': 'factory-submission.v1',
            'id': trace_id, 'agent': agent, 'received_at': iso(store.clock()), 'trace': body})
        return {'ok': True, 'id': trace_id, 'status': 'quarantined_for_independent_check'}
    raise Invalid('factory_action_not_allowed')


def route(event, method, path, body):
    store = Store(boto3.client('s3', region_name='us-east-1', config=CFG),
        os.environ.get('AI_PRIVATE_BUCKET', 'justhodl-ai-857687956942'),
        os.environ.get('AI_PUBLIC_BUCKET', 'justhodl-dashboard-live'), lambda: datetime.now(timezone.utc))
    try:
        return 200, handle(event, method, path, body, store)
    except Invalid as exc:
        reason = str(exc)
        status = 403 if reason in ('invitation_required', 'verified_factory_identity_required', 'owner_pause_control_required', 'owner_invitation_required') else 429 if 'limit' in reason else 400
        return status, {'ok': False, 'error': reason}
    except Conflict:
        return 409, {'ok': False, 'error': 'immutable_entry_or_concurrent_update_conflict'}
    except Exception as exc:
        return 500, {'ok': False, 'error': 'factory_internal', 'detail': type(exc).__name__ + ':' + str(exc)[:160]}
