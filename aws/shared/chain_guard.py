"""
chain_guard v2.0.0 - the fleet-wide trigger cap (Khalid, 2026-11-02): no engine may start other engines, or itself,
past a hard limit; past it the engines involved are SHUT DOWN (reserved concurrency 0 under the spend governor's
manual-release stage CAP) and only an operator's {"action":"release_cap"} turns them back on.  This sits on top of the
existing brakes (the deploy-time loop gate, the coordinator's hourly breaker, the spend governor's fuses and caps).

WHERE IT RUNS - every place one engine starts another, through one module:
  * system_events.publish / publish_many   - stamps the publisher's lineage into the event; refuses at the depth cap
  * justhodl-event-coordinator             - admit() before every routed invoke; stamps the lineage into the payload
  * chain_guard.lambda_client().invoke()    - the guarded Lambda client every engine uses for a direct invoke (the
                                             deploy gate tests/deployment/test_trigger_cap.py forbids a raw one)
  * chain_guard.chain_invoke()              - the walkers' self-continuation (v1 contract kept: parks at the hop cap)

LINEAGE  Every triggered run carries `_jh_chain` (top level of a direct payload, inside `detail` of a bus event, and
inside the coordinator's `trigger_detail`): {id, root, hop, path, self_hop, n}.  `hop` = cross-engine hand-offs taken
so far, `path` = the engines the chain has visited, `self_hop` = self-continuations of the current engine.  begin(event)
at handler start reads it; a run with no lineage is a root (a schedule, a user, an operator).  A new invocation of a
warm container is detected by the runtime's per-invocation trace id, so stale state never leaks between runs.

THE RULES (config/trigger-limits.json == LIMITS below; the deploy gate keeps them equal)
  hand-off (async Event invoke or bus event - the cascade class):
    depth      hop + 1 <= max_hops                                       breach: hold the source
    loop       the target is not already in the chain's path (the pairs in repeat_allowed_pairs - the allowlisted
               invoke cycles - may revisit each other; depth still binds)  breach: hold source AND target
    fan-out    distinct engines in one chain <= max_engines_per_chain     breach: hold the source
    volume     hand-offs in one chain <= max_handoffs_per_chain           breach: hold the source
    budget     hand-offs a target receives per UTC day <= daily.triggered breach: hold source AND target
    budget     hand-offs a source sends per UTC day <= daily.triggers_out breach: hold the source
  call (synchronous RequestResponse - the caller waits, so no multiplication, but still a start):
    depth, loop as above; calls per chain <= max_calls_per_chain; calls a target receives per day <= daily.called
  self-continuation (an engine re-invoking itself - the walkers):
    self_hop + 1 <= max_self_hops  -> otherwise PARK (v1 behaviour: the chain-resumer restarts it, a new lineage)
    self-starts per chain <= max_self_per_chain; self-starts per engine per day <= daily.self   breach: hold the engine
  The counters live in DynamoDB (table justhodl-event-breaker, TTL).  If the table cannot answer, the start is
  REFUSED and nobody is held (fail-closed, no evidence = no shutdown).  A refused start is never retried by this code.

THE SHUTDOWN  record_breach() writes one breach record per (day, source, target, rule) - the first writer wins - and
asks the governor {"action":"trigger_breach","breach_id":...}; the governor holds the listed engines, records the
event (GitHub issue + e-mail through governor-alerts) and marks the record applied.  If the governor is busy, its next
hourly tick sweeps pending records.  Either way the start that breached was refused on the spot.
"""
import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3

try:
    from botocore.config import Config as _BotoConfig
    from botocore.exceptions import ClientError
except Exception:  # noqa: BLE001 - tests stub boto3 without botocore
    _BotoConfig = None

    class ClientError(Exception):  # type: ignore[no-redef]
        response = {}

VERSION = "2.0.0"
HOP_KEY = "_lineage_hop"                       # v1 self-hop stamp, still honoured
LINEAGE_KEY = "_jh_chain"
MAX_HOPS = int(os.environ.get("CHAIN_GUARD_MAX_HOPS", "12"))   # v1 self-continuation cap (park past it)
AWS_HARD_LIMIT = 16
BUCKET = os.environ.get("CHAIN_GUARD_BUCKET") or os.environ.get("S3_BUCKET") or "justhodl-dashboard-live"
PARK_PREFIX = os.environ.get("CHAIN_GUARD_PARK_PREFIX", "data/_state/chain-parked/")
REGION = os.environ.get("AWS_REGION", "us-east-1")
TABLE = os.environ.get("TRIGGER_LEDGER_TABLE", "justhodl-event-breaker")
GOVERNOR = os.environ.get("SPEND_GOVERNOR_FUNCTION", "justhodl-spend-governor")
COUNTER_TTL_S = 3 * 86400
BREACH_TTL_S = 30 * 86400

# == config/trigger-limits.json (tests/deployment/test_trigger_cap.py fails the deploy if the two differ) ==
LIMITS = {
    "version": 1,
    "handoff": {
        "max_hops": 3,
        "max_engines_per_chain": 8,
        "max_handoffs_per_chain": 12,
        "max_handoffs_per_chain_by_source": {
            "justhodl-equity-prewarm": 1500,
            "justhodl-streaming-fanout": 100,
            "justhodl-a2a-bus": 50,
            "justhodl-backend-agent": 50
        },
        "repeat_allowed_pairs": [
            [
                "justhodl-a2a-bus",
                "justhodl-backend-agent"
            ]
        ]
    },
    "call": {
        "max_calls_per_chain": 3000
    },
    "self": {
        "max_self_hops": 12,
        "max_self_per_chain": 512
    },
    "daily": {
        "triggered_default": 200,
        "triggered": {
            "justhodl-research-critique": 1500,
            "justhodl-a2a-bus": 2000,
            "justhodl-backend-agent": 250,
            "justhodl-fred-catalog": 500,
            "justhodl-census-us": 500,
            "openbb-websocket-broadcast": 300
        },
        "called_default": 400,
        "called": {
            "justhodl-tv-bars": 20000,
            "justhodl-fundamental-graphs": 8000,
            "justhodl-a2a-bus": 2000,
            "justhodl-factory-grader": 3000,
            "treasury-api": 250
        },
        "triggers_out_default": 200,
        "triggers_out": {
            "justhodl-equity-prewarm": 1500,
            "justhodl-streaming-fanout": 1000,
            "justhodl-backend-agent": 2000,
            "justhodl-a2a-bus": 250,
            "justhodl-import-sentinel": 500
        },
        "self_default": 400,
        "self": {
            "justhodl-sdmx-walker": 4000,
            "justhodl-boj-full": 2500,
            "justhodl-equity-research": 6000,
            "justhodl-symdir": 1000,
            "justhodl-tv-bars": 500,
            "justhodl-gdelt-full": 600,
            "justhodl-census-us": 500,
            "justhodl-fred-catalog": 500
        }
    },
    "hold": {
        "loop": [
            "source",
            "target"
        ],
        "max_hops": [
            "source"
        ],
        "max_engines_per_chain": [
            "source"
        ],
        "max_handoffs_per_chain": [
            "source"
        ],
        "max_calls_per_chain": [
            "source"
        ],
        "daily_triggered": [
            "source",
            "target"
        ],
        "daily_called": [
            "source",
            "target"
        ],
        "daily_triggers_out": [
            "source"
        ],
        "max_self_per_chain": [
            "source"
        ],
        "daily_self": [
            "source"
        ]
    }
}
NOT_BREACHES = ("max_self_hops", "ledger_unavailable")


class TriggerRefused(RuntimeError):
    """A start this engine asked for was refused by the trigger cap.  .verdict carries reason, limit, lineage, hold."""

    def __init__(self, verdict):
        super().__init__("trigger refused: %s (%s -> %s)" % (verdict.get("reason"), verdict.get("source"), verdict.get("target")))
        self.verdict = verdict


_state = {"hop": 0, "begun": False}            # v1 compatibility (chain_invoke reads _state["hop"])
_ctx = {"trace": None, "request_id": None, "lineage": None, "fn": None}
_lam = None
_s3 = None
_ddb = None
_gov = None


# ---------------------------------------------------------------------------------------------------- clients --
def _clients():
    global _lam, _s3
    if _lam is None:
        _lam = boto3.client("lambda", region_name=REGION)
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=REGION)
    return _lam, _s3


def _ledger():
    global _ddb
    if _ddb is None:
        _ddb = boto3.client("dynamodb", region_name=REGION)
    return _ddb


def _governor_client():
    global _gov
    if _gov is None:
        kw = {}
        if _BotoConfig is not None:
            kw["config"] = _BotoConfig(retries={"max_attempts": 1}, read_timeout=90, connect_timeout=5)
        _gov = boto3.client("lambda", region_name=REGION, **kw)
    return _gov


def function_name(context=None):
    fn = getattr(context, "function_name", None)
    return fn or os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or "unknown-function"


def fn_of(name):
    """'arn:aws:lambda:...:function:justhodl-x:live' / 'justhodl-x:live' / 'justhodl-x' -> 'justhodl-x'."""
    s = str(name or "")
    if s.startswith("arn:"):
        s = s.split(":function:", 1)[-1]
    return s.split(":", 1)[0]


# ---------------------------------------------------------------------------------------------------- lineage --
def hop_of(event):
    try:
        return max(0, int((event or {}).get(HOP_KEY) or 0))
    except Exception:  # noqa: BLE001
        return 0


def fresh(fn):
    return {"id": "%s-%s" % (fn.replace("justhodl-", "")[:24], uuid.uuid4().hex[:10]), "root": fn, "hop": 0, "path": [fn], "self_hop": 0, "n": 0}


def _valid(lin):
    return isinstance(lin, dict) and isinstance(lin.get("id"), str) and isinstance(lin.get("path"), list) and lin.get("path")


def extract(event):
    """The lineage carried by an incoming event, wherever the sender put it; None for a root run."""
    if not isinstance(event, dict):
        return None
    for holder in (event, event.get("detail"), event.get("trigger_detail")):
        if isinstance(holder, dict) and _valid(holder.get(LINEAGE_KEY)):
            lin = dict(holder[LINEAGE_KEY])
            lin["path"] = list(lin["path"])
            lin["hop"] = int(lin.get("hop") or 0)
            lin["self_hop"] = int(lin.get("self_hop") or 0)
            lin["n"] = int(lin.get("n") or 0)
            return lin
    return None


def _invocation_marker(context=None):
    rid = getattr(context, "aws_request_id", None)
    return (os.environ.get("_X_AMZN_TRACE_ID"), rid)


def begin(event, context=None):
    """Call at the top of lambda_handler.  Reads the incoming lineage (or starts a root one) and remembers it for every
    publish / invoke of this run.  Returns the self-hop (v1 callers used the return value as the hop count)."""
    fn = function_name(context)
    lin = extract(event)
    if lin is None:
        lin = fresh(fn)
        legacy = hop_of(event)
        if legacy:
            lin["self_hop"] = legacy
    elif fn not in lin["path"]:
        lin["path"] = lin["path"] + [fn]           # a v1 sender stamped the chain but not the target
    trace, rid = _invocation_marker(context)
    _ctx.update({"trace": trace, "request_id": rid, "lineage": lin, "fn": fn})
    _state["hop"] = lin["self_hop"]
    _state["begun"] = True
    return lin["self_hop"]


def current(context=None):
    """This run's lineage.  A new invocation of a warm container (new trace id / request id) that never called begin()
    gets a fresh root lineage - stale chains never leak between runs."""
    trace, rid = _invocation_marker(context)
    stale = _ctx["lineage"] is None or (trace and _ctx["trace"] and trace != _ctx["trace"]) or (rid and _ctx["request_id"] and rid != _ctx["request_id"])
    if stale:
        fn = function_name(context)
        _ctx.update({"trace": trace, "request_id": rid, "lineage": fresh(fn), "fn": fn})
        _state["hop"] = 0
    return _ctx["lineage"]


def next_lineage(lin, target, kind):
    nxt = {"id": lin["id"], "root": lin["root"], "hop": lin["hop"], "path": list(lin["path"]), "self_hop": lin["self_hop"], "n": int(lin.get("n") or 0) + 1}
    if kind == "self":
        nxt["self_hop"] = lin["self_hop"] + 1
    else:
        nxt["hop"] = lin["hop"] + 1
        nxt["self_hop"] = 0
        if target not in nxt["path"]:
            nxt["path"].append(target)
    return nxt


def stamp(payload, lineage):
    """Put the lineage into a JSON-object payload (dict in, dict out).  Non-object payloads are left alone."""
    if isinstance(payload, dict):
        payload[LINEAGE_KEY] = lineage
        payload[HOP_KEY] = lineage.get("self_hop", 0)
    return payload


# ------------------------------------------------------------------------------------------------------ ledger --
def _day(now=None):
    return (now or datetime.now(timezone.utc)).strftime("%Y%m%d")


def _take(key, limit, now=None):
    """Atomically take one unit of `key`'s budget: ok | open | unavailable:<code>."""
    exp = int(time.time()) + COUNTER_TTL_S
    try:
        _ledger().update_item(
            TableName=TABLE, Key={"pk": {"S": key}},
            UpdateExpression="SET expires_at = if_not_exists(expires_at, :exp) ADD n :one",
            ConditionExpression="attribute_not_exists(n) OR n < :limit",
            ExpressionAttributeValues={":one": {"N": "1"}, ":limit": {"N": str(int(limit))}, ":exp": {"N": str(exp)}})
        return "ok"
    except ClientError as e:
        code = (getattr(e, "response", {}) or {}).get("Error", {}).get("Code", "ClientError")
        return "open" if code == "ConditionalCheckFailedException" else "unavailable:" + code
    except Exception as e:  # noqa: BLE001
        return "unavailable:" + type(e).__name__


def _add_engine(chain_id, target, limit):
    """Add `target` to the chain's engine set unless the set is full: ok | open | unavailable:<code>."""
    exp = int(time.time()) + COUNTER_TTL_S
    try:
        _ledger().update_item(
            TableName=TABLE, Key={"pk": {"S": "chain#%s#engines" % chain_id}},
            UpdateExpression="SET expires_at = if_not_exists(expires_at, :exp) ADD engines :e",
            ConditionExpression="attribute_not_exists(engines) OR contains(engines, :t) OR size(engines) < :limit",
            ExpressionAttributeValues={":e": {"SS": [target]}, ":t": {"S": target}, ":limit": {"N": str(int(limit))}, ":exp": {"N": str(exp)}})
        return "ok"
    except ClientError as e:
        code = (getattr(e, "response", {}) or {}).get("Error", {}).get("Code", "ClientError")
        return "open" if code == "ConditionalCheckFailedException" else "unavailable:" + code
    except Exception as e:  # noqa: BLE001
        return "unavailable:" + type(e).__name__


def _repeat_allowed(source, target):
    """A revisit inside one chain is allowed only for an allowlisted pair (config/loop-allowlist.json); depth still binds."""
    return any(source in pair and target in pair and source != target for pair in LIMITS["handoff"].get("repeat_allowed_pairs", []))


def budget(section, fn):
    d = LIMITS["daily"]
    return int((d.get(section) or {}).get(fn, d[section + "_default"]))


def _refusal(reason, source, target, lineage, kind, limit=None, observed=None, state=None):
    hold = [] if reason in NOT_BREACHES else [source if who == "source" else target for who in LIMITS["hold"].get(reason, ["source"])]
    hold = sorted(set(h for h in hold if h))
    return {"allow": False, "reason": reason, "source": source, "target": target, "kind": kind, "limit": limit, "observed": observed,
            "state": state, "lineage": lineage, "hold": hold, "breach": reason not in NOT_BREACHES}


def admit(source, target, lineage, kind="handoff", now=None, report=True):
    """May `source` start `target` as `kind` (handoff | call | self) given this run's lineage?
    -> {"allow": True, "lineage": <lineage for the target>} or a refusal (see _refusal)."""
    now = now or datetime.now(timezone.utc)
    day = _day(now)
    lin = lineage
    if kind == "self":
        if lin["self_hop"] + 1 > LIMITS["self"]["max_self_hops"]:
            return _refusal("max_self_hops", source, target, lin, kind, LIMITS["self"]["max_self_hops"], lin["self_hop"] + 1)
        checks = (("chain#%s#self" % lin["id"], LIMITS["self"]["max_self_per_chain"], "max_self_per_chain"),
                  ("self#%s#%s" % (source, day), budget("self", source), "daily_self"))
    else:
        if target in lin["path"] and not _repeat_allowed(source, target):
            return _finish(_refusal("loop", source, target, lin, kind, 1, 2), report)
        if lin["hop"] + 1 > LIMITS["handoff"]["max_hops"]:
            return _finish(_refusal("max_hops", source, target, lin, kind, LIMITS["handoff"]["max_hops"], lin["hop"] + 1), report)
        st = _add_engine(lin["id"], target, LIMITS["handoff"]["max_engines_per_chain"] - 1)   # the root engine is the first
        if st != "ok":
            if st == "open":
                return _finish(_refusal("max_engines_per_chain", source, target, lin, kind, LIMITS["handoff"]["max_engines_per_chain"], len(lin["path"]) + 1), report)
            return _refusal("ledger_unavailable", source, target, lin, kind, state=st)
        if kind == "call":
            checks = (("chain#%s#calls" % lin["id"], LIMITS["call"]["max_calls_per_chain"], "max_calls_per_chain"),
                      ("called#%s#%s" % (target, day), budget("called", target), "daily_called"))
        else:
            per_chain = int(LIMITS["handoff"].get("max_handoffs_per_chain_by_source", {}).get(source, LIMITS["handoff"]["max_handoffs_per_chain"]))
            checks = (("chain#%s" % lin["id"], per_chain, "max_handoffs_per_chain"),
                      ("out#%s#%s" % (source, day), budget("triggers_out", source), "daily_triggers_out"),
                      ("in#%s#%s" % (target, day), budget("triggered", target), "daily_triggered"))
    for key, limit, reason in checks:
        st = _take(key, limit, now)
        if st == "ok":
            continue
        if st == "open":
            return _finish(_refusal(reason, source, target, lin, kind, limit, limit + 1), report)
        return _refusal("ledger_unavailable", source, target, lin, kind, state=st)
    return {"allow": True, "lineage": next_lineage(lin, target, kind), "kind": kind, "source": source, "target": target}


def _finish(verdict, report):
    if report and verdict.get("breach"):
        try:
            verdict["breach_id"] = record_breach(verdict)
        except Exception as e:  # noqa: BLE001 - the refusal stands whatever happens to the report
            verdict["breach_error"] = "%s: %s" % (type(e).__name__, str(e)[:120])
    print("[chain_guard] REFUSED %s %s -> %s (%s, limit %s): hold %s" % (verdict["kind"], verdict["source"], verdict["target"],
                                                                        verdict["reason"], verdict.get("limit"), verdict.get("hold")))
    return verdict


def record_breach(verdict, now=None):
    """One record per (day, source, target, rule); the first writer asks the governor to hold.  Returns the breach id."""
    now = now or datetime.now(timezone.utc)
    bid = "breach#%s#%s#%s#%s" % (_day(now), verdict["source"], verdict["target"], verdict["reason"])
    doc = {"id": bid, "at": now.isoformat(), "source": verdict["source"], "target": verdict["target"], "kind": verdict["kind"],
           "reason": verdict["reason"], "limit": verdict.get("limit"), "observed": verdict.get("observed"), "hold": verdict["hold"],
           "lineage": verdict["lineage"], "function": function_name(), "guard_version": VERSION}
    try:
        _ledger().put_item(TableName=TABLE, Item={"pk": {"S": bid}, "kind": {"S": "breach"}, "status": {"S": "pending"},
                                                   "doc": {"S": json.dumps(doc, default=str)},
                                                   "expires_at": {"N": str(int(time.time()) + BREACH_TTL_S)}},
                           ConditionExpression="attribute_not_exists(pk)")
    except ClientError as e:
        if (getattr(e, "response", {}) or {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return bid                                   # already reported today by another copy
        raise
    try:
        resp = _governor_client().invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse",
                                         Payload=json.dumps({"action": "trigger_breach", "breach_id": bid}).encode())
        body = resp["Payload"].read()[:300]
        print("[chain_guard] governor trigger_breach %s -> %s %s" % (bid, resp.get("StatusCode"), body))
    except Exception as e:  # noqa: BLE001 - the governor's hourly sweep applies pending records
        print("[chain_guard] governor busy (%s); %s stays pending for the hourly sweep" % (type(e).__name__, bid))
    return bid


# ------------------------------------------------------------------------------------------- the guarded client --
class GuardedLambda:
    """boto3 Lambda client whose invoke() goes through the trigger cap.  Everything else passes through."""

    def __init__(self, client, context=None, soft=False):
        self._client, self._context, self._soft = client, context, soft

    def __getattr__(self, name):
        return getattr(self._client, name)

    def invoke(self, FunctionName, InvocationType="RequestResponse", Payload=b"{}", **kw):
        me = function_name(self._context)
        target = fn_of(FunctionName)
        lin = current(self._context)
        try:
            obj = json.loads(Payload.decode("utf-8") if isinstance(Payload, (bytes, bytearray)) else Payload) if Payload else {}
        except Exception:  # noqa: BLE001
            obj = None
        if target == me:
            kind = "self"
        elif InvocationType == "Event":
            kind = "handoff"
        else:
            kind = "call"
        verdict = admit(me, target, lin, kind)
        if not verdict["allow"]:
            if self._soft:
                return {"StatusCode": 0, "TriggerRefused": verdict}
            raise TriggerRefused(verdict)
        if isinstance(obj, dict):
            stamp(obj, verdict["lineage"])
            Payload = json.dumps(obj, default=str).encode("utf-8")
        return self._client.invoke(FunctionName=FunctionName, InvocationType=InvocationType, Payload=Payload, **kw)


def lambda_client(event=None, context=None, *, soft=False, client=None, **boto_kw):
    """The Lambda client engines use to start other engines (a drop-in for boto3.client("lambda", ...): botocore
    kwargs such as config= and region_name= pass through).  Pass the handler's event (and context) when you can: that
    also begins this run's lineage.  soft=True returns {"StatusCode": 0, "TriggerRefused": ...} instead of raising."""
    if event is not None:
        begin(event, context)
    if client is None:
        if boto_kw:
            boto_kw.setdefault("region_name", REGION)
            client = boto3.client("lambda", **boto_kw)
        else:
            client, _ = _clients()
    return GuardedLambda(client, context, soft)


# ------------------------------------------------------------------------------------------- self-continuation --
def ticket_key(fn, payload):
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:12]
    return "%s%s/%s.json" % (PARK_PREFIX, fn, digest)


def park(fn, payload, hop, reason="max_hops", s3=None):
    """Write the resume ticket. The resumer invokes `fn` with `payload` (hop reset)."""
    if s3 is None:
        _, s3 = _clients()
    body = dict(payload or {})
    body[HOP_KEY] = 0
    body.pop(LINEAGE_KEY, None)
    doc = {"version": VERSION, "function": fn, "payload": body, "hops_walked": hop,
           "reason": reason, "parked_at": datetime.now(timezone.utc).isoformat(),
           "ttl_epoch": int(time.time()) + 7 * 86400}
    key = ticket_key(fn, body)
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return key


def chain_invoke(payload, event=None, context=None, *, max_hops=None, lam=None, s3=None):
    """Continue a walk: self-invoke while the lineage is short, park when it is not (v1 contract), and since v2 count
    the self-start against the chain and the day - a breach refuses (the engine is being held) and never raises."""
    cap = int(max_hops if max_hops is not None else MAX_HOPS)
    fn = function_name(context)
    if event is not None:
        begin(event, context)
    lin = current(context)
    hop = lin["self_hop"]
    nxt = hop + 1
    body = dict(payload or {})
    if nxt <= cap:
        verdict = admit(fn, fn, lin, "self")
        if not verdict["allow"]:
            if verdict["reason"] == "max_self_hops":
                pass                                       # the park path below handles the walker's own cap
            else:
                print("[chain_guard] %s self-continuation refused: %s" % (fn, verdict["reason"]))
                return {"chained": False, "hop": hop, "parked": False, "refused": verdict["reason"], "hold": verdict.get("hold")}
        else:
            stamp(body, verdict["lineage"])
            if lam is None:
                lam, _ = _clients()
            lam.invoke(FunctionName=fn, InvocationType="Event", Payload=json.dumps(body, default=str).encode())
            return {"chained": True, "hop": nxt, "parked": False}
    try:
        key = park(fn, body, hop, s3=s3)
        print("[chain_guard] %s parked at hop %d -> %s (resumer picks it up within 5 min)" % (fn, hop, key))
        return {"chained": False, "hop": hop, "parked": True, "ticket": key}
    except Exception as e:  # noqa: BLE001
        print("[chain_guard] park failed (%s)" % str(e)[:120])
        return {"chained": False, "hop": hop, "parked": False, "park_error": str(e)[:80]}
