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


def submit(store, sm_runtime, control, agent, text, history):
    """Submit one async request; returns the chat-facing record (never the answer)."""
    now = iso(store.clock())
    rid = "req-" + digest(agent + text + now)[:16]
    # prior context = the owner's own turns plus the owned model's own prior answers (never Brain/status text)
    turns = [(m.get("role"), str(m.get("text") or "")[:1200]) for m in (history or [])[-8:]
             if m.get("role") == "owner" or str(m.get("model") or "").startswith("owned:")]
    prompt = qwen_chat_prompt(SYSTEM, turns, text[:6000])
    body = {"inputs": prompt, "parameters": {"max_new_tokens": int(control.get("max_new_tokens") or 700), "temperature": 0.2, "top_p": 0.9,
                                             "stop": ["<|im_end|>", "<|endoftext|>"]}}
    key = REQ_PREFIX + rid + ".json"
    store.immutable(store.private, key, {"schema_version": "factory-inference-request.v1", "id": rid, "agent": agent, "at": now,
                                          "endpoint": control.get("endpoint_name"), "origin": origin(control), "body": body})
    resp = sm_runtime.invoke_endpoint_async(EndpointName=control["endpoint_name"], InputLocation="s3://%s/%s" % (store.private, key),
                                            ContentType="application/json", Accept="application/json", InferenceId=rid,
                                            InvocationTimeoutSeconds=900)
    return {"id": rid, "state": "queued", "origin": origin(control), "output_location": resp.get("OutputLocation"),
            "failure_location": resp.get("FailureLocation"), "submitted_at": now}


def _key_from_location(store, location):
    if not location:
        return None
    prefix = "s3://%s/" % store.private
    return location[len(prefix):] if str(location).startswith(prefix) else None


def resolve(store, pending):
    """Look for the answer of a pending request. Returns (state, text_or_reason)."""
    out_key = _key_from_location(store, pending.get("output_location"))
    fail_key = _key_from_location(store, pending.get("failure_location"))
    if out_key:
        doc, _ = store.read(store.private, out_key)
        if doc is not None:
            text = doc.get("generated_text") if isinstance(doc, dict) else (doc[0].get("generated_text") if isinstance(doc, list) and doc else str(doc))
            text = re.sub(r"<\|im_end\|>.*$", "", str(text or ""), flags=re.S).strip()
            return "done", text or "(empty completion)"
    if fail_key:
        doc, _ = store.read(store.private, fail_key)
        if doc is not None:
            return "failed", str(doc)[:500]
    age = (store.clock() - datetime.fromisoformat(pending["submitted_at"])).total_seconds() if pending.get("submitted_at") else 0
    return ("running" if age > 20 else "queued"), "cold start can take a few minutes when the endpoint is scaled to zero"


def is_status_question(text):
    from factory_status import is_status_request
    low = " ".join(text.lower().split())
    return low in ("status", "status?") or is_status_request(text) and not any(w in low for w in ("can you code", "do you code", "can you program", "write", "implement"))
