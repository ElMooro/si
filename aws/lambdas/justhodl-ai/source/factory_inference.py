"""Owned-model inference for the factory chat (2026-09-14).

The owner's Qwen2.5-Coder-7B-Instruct answers ordinary and coding questions through a SageMaker ASYNC endpoint
that scales to zero (no always-on GPU). A chat turn submits a request object, gets back a request id, and the
answer is appended to the chat log when its output object appears (the desk polls). Every answer names its
origin: owned:<model_id>@<revision>[+adapter gen-N]. When the endpoint control is absent or disabled, the route
says so; it never substitutes a canned answer for the model.

Control: factory/control/inference.json  {enabled, endpoint_name, model_id, revision, adapter_generation, max_new_tokens}
Objects:  factory/inference/requests/<id>.json  (immutable input)
          factory/inference/outputs/<id>.out    (written by SageMaker on completion)
          factory/inference/outputs/<id>.failure (written by SageMaker on failure)
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone

from factory_core import digest, identifier, iso

CONTROL_KEY = "factory/control/inference.json"
REQ_PREFIX = "factory/inference/requests/"
OUT_PREFIX = "factory/inference/outputs/"
SYSTEM = ("You are JustHodl's owned coding model, running inside Khalid's own AWS account. Answer directly and concretely. "
          "When asked for code, write complete, runnable Python with a short explanation. Never claim to have run code, "
          "trained, or accessed anything you did not; the factory verifies claims separately.")


def qwen_chat_prompt(system, turns, user_text):
    """Qwen2.5 chat template (ChatML). `turns` are prior (role, text) pairs, owner turns only."""
    parts = ["<|im_start|>system\n%s<|im_end|>" % system]
    for role, text in turns[-6:]:
        parts.append("<|im_start|>%s\n%s<|im_end|>" % ("user" if role == "owner" else "assistant", text))
    parts.append("<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n" % user_text)
    return "\n".join(parts)


def load_control(store):
    doc, _ = store.read(store.private, CONTROL_KEY)
    return doc if isinstance(doc, dict) else None


def origin(control):
    if not control:
        return "owned:absent"
    label = "owned:%s@%s" % (control.get("model_id"), str(control.get("revision") or "")[:12])
    if control.get("adapter_generation") not in (None, "", "base"):
        label += "+adapter " + str(control["adapter_generation"])
    return label


PENDING_PREFIX = "factory/inference/pending/"
META_PREFIX = "factory/inference/meta/"
EXPIRE_S = 30 * 60           # ONE lifecycle number (B12): queue TTL == application deadline; invocation allowance below it
INVOKE_TIMEOUT_S = 900


def _idempotency_key(agent, text, history):
    last = (history or [])[-1].get("id") if history else ""
    return digest({"agent": agent, "text": " ".join(text.split()), "after": last})[:24]


STATUS_MODELS = ("owned:queued", "owned:unavailable", "owned:not-connected", "owned:failed", "owned:expired", "owned:malformed")


def conversation_turns(history, limit=8):
    """Semantic turns only (B08): every owner turn (pending metadata is operational, not semantic) and the owned model's
    real answers; queue/status/error events and non-owned voices never enter the model's context."""
    turns = []
    for m in (history or [])[-limit * 2:]:
        role, model = m.get("role"), str(m.get("model") or "")
        if role == "owner":
            turns.append(("owner", str(m.get("text") or "")[:1200]))
        elif model.startswith("owned:") and model not in STATUS_MODELS and m.get("request"):
            turns.append(("agent", str(m.get("text") or "")[:1200]))
    return turns[-limit:]


def pending_key(agent, ikey):
    return PENDING_PREFIX + identifier(agent) + "/" + ikey + ".json"


def submit(store, sm_runtime, control, agent, text, history):
    """Submit one async request. Order (B09): claim a durable pending record FIRST, then invoke; a provider error after the
    claim leaves the record `unknown` (recoverable), never lost and never duplicated. The object at InputLocation is the
    serving payload exactly ({inputs, parameters}); audit metadata is a separate object."""
    now = iso(store.clock())
    ikey = _idempotency_key(agent, text, history)
    pkey = pending_key(agent, ikey)
    existing, petag = store.read(store.private, pkey)
    if isinstance(existing, dict) and existing.get("state") not in ("done", "failed", "expired", "malformed") and not existing.get("delivered"):
        return dict(existing, replay=True)          # a double submit rides the request already claimed/in flight
    rid = "req-" + ikey
    prompt = qwen_chat_prompt(SYSTEM, conversation_turns(history), text[:6000])
    payload = {"inputs": prompt, "parameters": {"max_new_tokens": int(control.get("max_new_tokens") or 700), "temperature": 0.2, "top_p": 0.9,
                                                "stop": ["<|im_end|>", "<|endoftext|>"]}}
    key = REQ_PREFIX + rid + ".json"
    store.immutable(store.private, key, payload)                                   # what the model receives, byte for byte
    store.immutable(store.private, META_PREFIX + rid + ".json", {"schema_version": "factory-inference-request.v1", "id": rid, "agent": agent, "at": now,
                                                                  "endpoint": control.get("endpoint_name"), "origin": origin(control), "idempotency_key": ikey,
                                                                  "input_key": key, "input_sha256": digest(payload), "text_sha256": digest(text),
                                                                  "turn_after": (history or [])[-1].get("id") if history else None})
    pending = {"schema_version": "factory-inference-pending.v1", "id": rid, "state": "claimed", "origin": origin(control), "agent": agent,
               "output_location": None, "failure_location": None, "submitted_at": now, "input_key": key, "delivered": False,
               "expires_at": iso(store.clock() + __import__("datetime").timedelta(seconds=EXPIRE_S))}
    store.put(store.private, pkey, pending, etag=petag, absent=petag is None)     # the durable claim
    try:
        resp = sm_runtime.invoke_endpoint_async(EndpointName=control["endpoint_name"], InputLocation="s3://%s/%s" % (store.private, key),
                                                ContentType="application/json", Accept="application/json", InferenceId=rid,
                                                InvocationTimeoutSeconds=INVOKE_TIMEOUT_S, RequestTTLSeconds=EXPIRE_S)
    except Exception as exc:  # noqa: BLE001 -- the provider may or may not have accepted it: recoverable unknown, not a retry
        pending.update(state="unknown", error=type(exc).__name__ + ":" + str(exc)[:200])
        _, petag2 = store.read(store.private, pkey)
        store.put(store.private, pkey, pending, etag=petag2, absent=False)
        raise
    pending.update(state="queued", output_location=resp.get("OutputLocation"), failure_location=resp.get("FailureLocation"))
    _, petag2 = store.read(store.private, pkey)
    store.put(store.private, pkey, pending, etag=petag2, absent=False)
    return pending


def submit_task(store, sm_runtime, control, agent, system, text, max_new_tokens=1400, temperature=0.2, meta=None):
    """One async generation for an engine task (2026-09-17: the market read's owned voice). Same durable order as
    submit(): claim the pending record first, then invoke. The caller owns the system prompt and the full text (bounded
    at 60k chars ~ 20k tokens, inside Qwen2.5's context); `meta` rides on the pending record so the settle step knows
    what the answer is for. Returns the pending record (state queued|unknown)."""
    now = iso(store.clock())
    ikey = _idempotency_key(agent, (system or "") + "\n" + text, None)
    pkey = pending_key(agent, ikey)
    existing, petag = store.read(store.private, pkey)
    if isinstance(existing, dict) and existing.get("state") not in ("done", "failed", "expired", "malformed") and not existing.get("delivered"):
        return dict(existing, replay=True)
    rid = "req-" + ikey
    prompt = qwen_chat_prompt(system or SYSTEM, [], text[:60000])
    payload = {"inputs": prompt, "parameters": {"max_new_tokens": int(max_new_tokens), "temperature": float(temperature), "top_p": 0.9,
                                                "stop": ["<|im_end|>", "<|endoftext|>"]}}
    key = REQ_PREFIX + rid + ".json"
    store.immutable(store.private, key, payload)
    store.immutable(store.private, META_PREFIX + rid + ".json", {"schema_version": "factory-inference-request.v1", "id": rid, "agent": agent, "at": now,
                                                                  "endpoint": control.get("endpoint_name"), "origin": origin(control), "idempotency_key": ikey,
                                                                  "input_key": key, "input_sha256": digest(payload), "text_sha256": digest(text), "kind": "task"})
    pending = {"schema_version": "factory-inference-pending.v1", "id": rid, "state": "claimed", "origin": origin(control), "agent": agent, "kind": "task",
               "output_location": None, "failure_location": None, "submitted_at": now, "input_key": key, "delivered": False, "meta": meta or {},
               "expires_at": iso(store.clock() + __import__("datetime").timedelta(seconds=EXPIRE_S))}
    store.put(store.private, pkey, pending, etag=petag, absent=petag is None)
    try:
        resp = sm_runtime.invoke_endpoint_async(EndpointName=control["endpoint_name"], InputLocation="s3://%s/%s" % (store.private, key),
                                                ContentType="application/json", Accept="application/json", InferenceId=rid,
                                                InvocationTimeoutSeconds=INVOKE_TIMEOUT_S, RequestTTLSeconds=EXPIRE_S)
    except Exception as exc:  # noqa: BLE001
        pending.update(state="unknown", error=type(exc).__name__ + ":" + str(exc)[:200])
        _, petag2 = store.read(store.private, pkey)
        store.put(store.private, pkey, pending, etag=petag2, absent=False)
        return pending
    pending.update(state="queued", output_location=resp.get("OutputLocation"), failure_location=resp.get("FailureLocation"))
    _, petag2 = store.read(store.private, pkey)
    store.put(store.private, pkey, pending, etag=petag2, absent=False)
    return pending


def _key_from_location(store, location):
    if not location:
        return None
    prefix = "s3://%s/" % store.private
    return location[len(prefix):] if str(location).startswith(prefix) else None


def resolve(store, pending):
    """(state, text_or_reason) for a pending request. Terminal states: done | failed | expired | malformed."""
    out_key = _key_from_location(store, pending.get("output_location"))
    fail_key = _key_from_location(store, pending.get("failure_location"))
    if out_key:
        try:
            doc, _ = store.read(store.private, out_key)
        except Exception as exc:  # noqa: BLE001  -- a non-JSON or oversize body is malformed, not "running"
            return "malformed", "output object unreadable: " + type(exc).__name__
        if doc is not None:
            if isinstance(doc, list) and doc and isinstance(doc[0], dict):
                doc = doc[0]
            if not isinstance(doc, dict):
                return "malformed", "unexpected response shape: " + type(doc).__name__
            details = doc.get("details") if isinstance(doc.get("details"), dict) else {}
            if doc.get("error") or str(doc.get("finish_reason") or "").lower() == "error" or str(details.get("finish_reason") or "").lower() == "error":
                return "failed", str(doc.get("error") or doc.get("details") or "model error")[:400]
            text = re.sub(r"<\|im_end\|>.*$", "", str(doc.get("generated_text") or ""), flags=re.S).strip()
            if not text:
                return "failed", "empty completion"
            reason = str(details.get("finish_reason") or doc.get("finish_reason") or "")
            if reason == "length":
                text += "\n\n[truncated at max_new_tokens]"
            return "done", text
    if fail_key:
        try:
            doc, _ = store.read(store.private, fail_key)
        except Exception as exc:  # noqa: BLE001 -- a non-JSON failure body is still a failure
            return "failed", "failure object unreadable: " + type(exc).__name__
        if doc is not None:
            return "failed", str(doc)[:500]
    if pending.get("state") == "unknown" and not out_key:
        submitted = pending.get("submitted_at")
        age = (store.clock() - datetime.fromisoformat(submitted)).total_seconds() if submitted else 0
        return ("expired", "provider never acknowledged the request") if age > EXPIRE_S else ("unknown", "provider acknowledgement lost; waiting for an output or expiry")
    submitted = pending.get("submitted_at")
    age = (store.clock() - datetime.fromisoformat(submitted)).total_seconds() if submitted else 0
    if age > EXPIRE_S:
        return "expired", "no output within %d minutes (queue TTL/invocation timeout)" % (EXPIRE_S // 60)
    return ("running" if age > 20 else "queued"), "cold start can take a few minutes when the endpoint is scaled to zero"


def list_pending(store, agent, cap=50):
    """Unsettled requests for this agent from its own pending prefix (B10: delivered records are moved out, so a live
    request can never be starved by history). Listing errors are explicit."""
    out = []
    prefix = PENDING_PREFIX + identifier(agent) + "/"
    token = None
    while True:
        kw = {"Bucket": store.private, "Prefix": prefix, "MaxKeys": 200}
        if token:
            kw["ContinuationToken"] = token
        resp = store.s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            doc, etag = store.read(store.private, o["Key"])
            if isinstance(doc, dict) and not doc.get("delivered"):
                out.append((o["Key"], doc, etag))
                if len(out) >= cap:
                    return out
        token = resp.get("NextContinuationToken")
        if not resp.get("IsTruncated") or not token:
            return out


def archive_delivered(store, pkey, pending):
    """Move a delivered record to factory/inference/delivered/ (same relative key); the pending prefix stays small."""
    dest = pkey.replace(PENDING_PREFIX, "factory/inference/delivered/", 1)
    try:
        store.immutable(store.private, dest, pending)                # first archive wins; a re-archive of the same id is a no-op
    except Exception:  # noqa: BLE001
        pass
    try:
        store.s3.delete_object(Bucket=store.private, Key=pkey)
    except Exception:  # noqa: BLE001 -- a leftover pending record is delivered=True and simply skipped
        pass


def is_status_question(text):
    from factory_status import is_status_request
    low = " ".join(text.lower().split())
    return low in ("status", "status?") or is_status_request(text) and not any(w in low for w in ("can you code", "do you code", "can you program", "write", "implement"))
