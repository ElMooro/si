"""
chain_guard — bounded-lineage self-chaining for walk engines (ops 5260).

THE PROBLEM IT CLOSES
  AWS Lambda tracks a Lineage counter in the X-Ray trace header across every
  Lambda→Lambda Invoke. When one originating event reaches a function for the
  17th time, Lambda DROPS that invocation, emits RecursiveInvocationsDropped
  and mails an AWS Health "recursive loop detected" event (2026-08-01 census,
  2026-08-31 + 2026-09-07 worldbank-full). Twelve JustHodl walk engines
  self-chain with their own caps of 30–220 hops, i.e. every one of them is
  silently cut at hop 16 and restarted only by its next schedule tick:
  bounded work presented as finished work, plus a scary mail each time.

THE CONTRACT
  Engines keep their own protocol (chain_depth / cursor / phase). They only
  stop calling lambda.invoke(self, Event, ...) directly and call

      chain_guard.chain_invoke(payload, event=event)      # in the handler
  or  chain_guard.begin(event) at handler start, then chain_invoke(payload)

  chain_invoke reads the incoming event's `_lineage_hop`, and
    * hop < MAX_HOPS (12, well under AWS's 16)  → self-invoke with the hop
      stamped into the payload; returns {"chained": True, "hop": n}
    * hop >= MAX_HOPS → PARK: write the resume payload to
      data/_state/chain-parked/<function>/<digest>.json and return
      {"chained": False, "parked": True}. justhodl-chain-resumer (EventBridge
      Scheduler, rate(5 minutes)) re-invokes the engine from that ticket.
      A Scheduler-origin invoke is a NEW lineage, so the walk continues at
      full duty cycle and AWS's breaker is never reached.
  AWS's recursion detection stays ON (Terminate) as the safety net for a
  real bug — this guard only keeps intentional walks under its limit.

  Nothing here raises into the engine: a parking failure degrades to a
  direct self-invoke ONLY if the hop is still under AWS's hard limit; past
  that it returns parked=False and the engine's own schedule resumes it.
"""
import hashlib
import json
import os
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
HOP_KEY = "_lineage_hop"
MAX_HOPS = int(os.environ.get("CHAIN_GUARD_MAX_HOPS", "12"))
AWS_HARD_LIMIT = 16
BUCKET = os.environ.get("CHAIN_GUARD_BUCKET") or os.environ.get("S3_BUCKET") or "justhodl-dashboard-live"
PARK_PREFIX = os.environ.get("CHAIN_GUARD_PARK_PREFIX", "data/_state/chain-parked/")
REGION = os.environ.get("AWS_REGION", "us-east-1")

_state = {"hop": 0, "begun": False}
_lam = None
_s3 = None


def _clients():
    global _lam, _s3
    if _lam is None:
        _lam = boto3.client("lambda", region_name=REGION)
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=REGION)
    return _lam, _s3


def hop_of(event):
    try:
        return max(0, int((event or {}).get(HOP_KEY) or 0))
    except Exception:  # noqa: BLE001
        return 0


def begin(event):
    """Call at the top of lambda_handler when the chain site has no `event` in scope."""
    _state["hop"] = hop_of(event)
    _state["begun"] = True
    return _state["hop"]


def function_name(context=None):
    fn = getattr(context, "function_name", None)
    return fn or os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or "unknown-function"


def ticket_key(fn, payload):
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:12]
    return "%s%s/%s.json" % (PARK_PREFIX, fn, digest)


def park(fn, payload, hop, reason="max_hops", s3=None):
    """Write the resume ticket. The resumer invokes `fn` with `payload` (hop reset)."""
    if s3 is None:
        _, s3 = _clients()
    body = dict(payload or {})
    body[HOP_KEY] = 0
    doc = {"version": VERSION, "function": fn, "payload": body, "hops_walked": hop,
           "reason": reason, "parked_at": datetime.now(timezone.utc).isoformat(),
           "ttl_epoch": int(time.time()) + 7 * 86400}
    key = ticket_key(fn, body)
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return key


def chain_invoke(payload, event=None, context=None, *, max_hops=None, lam=None, s3=None):
    """Continue a walk: self-invoke while the lineage is short, park when it is not."""
    cap = int(max_hops if max_hops is not None else MAX_HOPS)
    hop = hop_of(event) if event is not None else _state["hop"]
    nxt = hop + 1
    fn = function_name(context)
    body = dict(payload or {})
    if nxt <= cap:
        body[HOP_KEY] = nxt
        if lam is None:
            lam, _ = _clients()
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=json.dumps(body, default=str).encode())
        return {"chained": True, "hop": nxt, "parked": False}
    try:
        key = park(fn, body, hop, s3=s3)
        print("[chain_guard] %s parked at hop %d -> %s (resumer picks it up within 5 min)" % (fn, hop, key))
        return {"chained": False, "hop": hop, "parked": True, "ticket": key}
    except Exception as e:  # noqa: BLE001
        print("[chain_guard] park failed (%s)" % str(e)[:120])
        if nxt < AWS_HARD_LIMIT:
            body[HOP_KEY] = nxt
            if lam is None:
                lam, _ = _clients()
            lam.invoke(FunctionName=fn, InvocationType="Event",
                       Payload=json.dumps(body, default=str).encode())
            return {"chained": True, "hop": nxt, "parked": False, "park_error": str(e)[:80]}
        return {"chained": False, "hop": hop, "parked": False, "park_error": str(e)[:80]}
