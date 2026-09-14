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

from factory_core import digest, iso

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
EXPIRE_S = 30 * 60           # queue TTL + invocation timeout; after this a request is `expired`, never "running" forever


def _idempotency_key(agent, text, history):
    last = (history or [])[-1].get("id") if history else ""
    return digest({"agent": agent, "text": " ".join(text.split()), "after": last})[:24]


def submit(store, sm_runtime, control, agent, text, history):
    """Submit one async request. The object at InputLocation is the SERVING payload exactly ({inputs, parameters});
    audit metadata lives in a separate object; request state lives in its own pending index (not the chat log)."""
    now = iso(store.clock())
    ikey = _idempotency_key(agent, text, history)
    existing, _ = store.read(store.private, PENDING_PREFIX + ikey + ".json")
    if isinstance(existing, dict) and existing.get("state") not in ("done", "failed", "expired", "malformed"):
        return dict(existing, replay=True)          # a double submit rides the request already in flight
    rid = "req-" + ikey
    turns = [(m.get("role"), str(m.get("text") or "")[:1200]) for m in (history or [])[-8:]
             if (m.get("role") == "owner" and not m.get("pending")) or str(m.get("model") or "").startswith("owned:")]
    prompt = qwen_chat_prompt(SYSTEM, turns, text[:6000])
    payload = {"inputs": prompt, "parameters": {"max_new_tokens": int(control.get("max_new_tokens") or 700), "temperature": 0.2, "top_p": 0.9,
                                                "stop": ["<|im_end|>", "<|endoftext|>"]}}
    key = REQ_PREFIX + rid + ".json"
    store.immutable(store.private, key, payload)                                   # what the model receives, byte for byte
    store.immutable(store.private, META_PREFIX + rid + ".json", {"schema_version": "factory-inference-request.v1", "id": rid, "agent": agent, "at": now,
                                                                  "endpoint": control.get("endpoint_name"), "origin": origin(control), "idempotency_key": ikey,
                                                                  "input_key": key, "input_sha256": digest(payload), "turn_after": (history or [])[-1].get("id") if history else None})
    resp = sm_runtime.invoke_endpoint_async(EndpointName=control["endpoint_name"], InputLocation="s3://%s/%s" % (store.private, key),
                                            ContentType="application/json", Accept="application/json", InferenceId=rid,
                                            InvocationTimeoutSeconds=900)
    pending = {"schema_version": "factory-inference-pending.v1", "id": rid, "state": "queued", "origin": origin(control), "agent": agent,
               "output_location": resp.get("OutputLocation"), "failure_location": resp.get("FailureLocation"), "submitted_at": now,
               "expires_at": iso(store.clock() + __import__("datetime").timedelta(seconds=EXPIRE_S)), "input_key": key, "delivered": False}
    store.put(store.private, PENDING_PREFIX + ikey + ".json", pending, etag=None, absent=True)
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
            if doc.get("error") or str(doc.get("finish_reason") or "").lower() == "error" or str((doc.get("details") or {}).get("finish_reason") or "").lower() == "error":
                return "failed", str(doc.get("error") or doc.get("details") or "model error")[:400]
            text = re.sub(r"<\|im_end\|>.*$", "", str(doc.get("generated_text") or ""), flags=re.S).strip()
            if not text:
                return "failed", "empty completion"
            reason = str((doc.get("details") or {}).get("finish_reason") or doc.get("finish_reason") or "")
            if reason == "length":
                text += "\n\n[truncated at max_new_tokens]"
            return "done", text
    if fail_key:
        doc, _ = store.read(store.private, fail_key)
        if doc is not None:
            return "failed", str(doc)[:500]
    submitted = pending.get("submitted_at")
    age = (store.clock() - datetime.fromisoformat(submitted)).total_seconds() if submitted else 0
    if age > EXPIRE_S:
        return "expired", "no output within %d minutes (queue TTL/invocation timeout)" % (EXPIRE_S // 60)
    return ("running" if age > 20 else "queued"), "cold start can take a few minutes when the endpoint is scaled to zero"


def list_pending(store, agent, cap=50):
    """Unsettled requests for this agent from the pending index (independent of the chat log)."""
    out = []
    try:
        resp = store.s3.list_objects_v2(Bucket=store.private, Prefix=PENDING_PREFIX, MaxKeys=500)
    except Exception:  # noqa: BLE001
        return out
    for o in resp.get("Contents", []):
        doc, etag = store.read(store.private, o["Key"])
        if isinstance(doc, dict) and doc.get("agent") == agent and not doc.get("delivered"):
            out.append((o["Key"], doc, etag))
            if len(out) >= cap:
                break
    return out


def is_status_question(text):
    from factory_status import is_status_request
    low = " ".join(text.lower().split())
    return low in ("status", "status?") or is_status_request(text) and not any(w in low for w in ("can you code", "do you code", "can you program", "write", "implement"))
