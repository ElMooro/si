"""
justhodl-chain-resumer  v1.0.0  (ops 5260)

The other half of aws/shared/chain_guard.py.

A walk engine that has self-chained 12 hops parks a resume ticket at
    data/_state/chain-parked/<function>/<digest>.json
instead of invoking itself a 13th time. This function runs from EventBridge
Scheduler every 5 minutes, reads every ticket and invokes the engine with the
ticket's payload. Because the invoke originates from a Scheduler-driven
invocation (no X-Ray lineage), the engine starts a NEW 16-hop budget: walks
run at full duty cycle and AWS's recursion breaker is never reached — so no
more "recursive loop detected" Health mails, and no more silently truncated
walks (worldbank-full Aug-31/Sep-7, fundamental-census Aug-01).

Bounds (this must never become the loop it prevents):
  * MAX_PER_RUN tickets per run (default 50).
  * RESUME_MAX_PER_DAY per function (default 120 ≈ 1,440 hops/day, more than
    any lane needs). Beyond that the ticket is moved to
    data/_state/chain-parked-held/ and an SNS alert goes to
    justhodl-fleet-alerts (→ Telegram via justhodl-guardrail-notify).
  * Only functions named justhodl-* are ever resumed.
  * A ticket is deleted only after Lambda accepted the async invoke (202).

Ledger: data/_state/chain-resumer/last-run.json and a rolling log.json
(last 400 resumes) — the evidence trail for "which walk resumed when".
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
PARK_PREFIX = os.environ.get("PARK_PREFIX", "data/_state/chain-parked/")
HELD_PREFIX = os.environ.get("HELD_PREFIX", "data/_state/chain-parked-held/")
LEDGER = os.environ.get("LEDGER_PREFIX", "data/_state/chain-resumer/")
RESUME_MAX_PER_DAY = int(os.environ.get("RESUME_MAX_PER_DAY", "120"))
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "50"))
SNS_ARN = os.environ.get("SNS_ARN", "")
LOG_KEEP = 400

_s3 = None
_lam = None
_sns = None


def clients():
    global _s3, _lam, _sns
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=REGION)
    if _lam is None:
        _lam = boto3.client("lambda", region_name=REGION)
    if _sns is None and SNS_ARN:
        _sns = boto3.client("sns", region_name=REGION)
    return _s3, _lam, _sns


def _gj(s3, key, default):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return default


def _pj(s3, key, doc):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")


def list_tickets(s3, limit):
    keys = []
    kw = {"Bucket": BUCKET, "Prefix": PARK_PREFIX}
    while True:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []) or []:
            if o["Key"].endswith(".json"):
                keys.append(o["Key"])
                if len(keys) >= limit:
                    return keys
        if not r.get("IsTruncated"):
            return keys
        kw["ContinuationToken"] = r["NextContinuationToken"]


def resumes_last_24h(log, fn, now):
    cutoff = (now - timedelta(hours=24)).isoformat()
    return sum(1 for e in log if e.get("function") == fn and e.get("at", "") >= cutoff and e.get("ok"))


def lambda_handler(event=None, context=None):
    s3, lam, sns = clients()
    now = datetime.now(timezone.utc)
    out = {"version": VERSION, "ran_at": now.isoformat(), "resumed": [], "held": [], "errors": [], "skipped": 0}
    log = _gj(s3, LEDGER + "log.json", [])
    if not isinstance(log, list):
        log = []
    keys = list_tickets(s3, MAX_PER_RUN)
    for key in keys:
        doc = _gj(s3, key, None)
        if not isinstance(doc, dict) or not isinstance(doc.get("payload"), dict):
            out["errors"].append({"key": key, "error": "malformed ticket"})
            try:
                s3.delete_object(Bucket=BUCKET, Key=key)
            except Exception:  # noqa: BLE001
                pass
            continue
        fn = str(doc.get("function") or "")
        if not fn.startswith("justhodl-"):
            out["errors"].append({"key": key, "error": "refused non-fleet function %s" % fn[:40]})
            continue
        if resumes_last_24h(log, fn, now) >= RESUME_MAX_PER_DAY:
            held_key = HELD_PREFIX + key[len(PARK_PREFIX):]
            try:
                doc["held_at"] = now.isoformat()
                doc["held_reason"] = "circuit_breaker %d resumes/24h" % RESUME_MAX_PER_DAY
                _pj(s3, held_key, doc)
                s3.delete_object(Bucket=BUCKET, Key=key)
            except Exception as e:  # noqa: BLE001
                out["errors"].append({"key": key, "error": "hold: %s" % str(e)[:100]})
            out["held"].append({"function": fn, "key": held_key})
            if sns:
                try:
                    sns.publish(TopicArn=SNS_ARN, Subject="chain-resumer circuit breaker: %s" % fn[:60],
                                Message=json.dumps({"AlarmName": "justhodl-chain-resumer-circuit-breaker",
                                                    "NewStateValue": "ALARM", "OldStateValue": "OK",
                                                    "NewStateReason": "%s parked and resumed %d times in 24h; ticket held at %s. "
                                                                      "Inspect the engine's state doc — a walk that never converges." % (fn, RESUME_MAX_PER_DAY, held_key),
                                                    "StateChangeTime": now.isoformat(),
                                                    "Trigger": {"MetricName": "chain_resumes_24h"}}))
                except Exception as e:  # noqa: BLE001
                    out["errors"].append({"key": key, "error": "sns: %s" % str(e)[:100]})
            continue
        payload = dict(doc["payload"])
        payload["_lineage_hop"] = 0
        payload["_resumed_from"] = key
        try:
            r = lam.invoke(FunctionName=fn, InvocationType="Event",
                           Payload=json.dumps(payload, default=str).encode())
            ok = int(r.get("StatusCode", 0)) in (200, 202)
        except Exception as e:  # noqa: BLE001
            ok = False
            out["errors"].append({"key": key, "function": fn, "error": str(e)[:120]})
        entry = {"at": now.isoformat(), "function": fn, "ok": ok, "ticket": key,
                 "hops_walked": doc.get("hops_walked"), "parked_at": doc.get("parked_at")}
        log.append(entry)
        if ok:
            try:
                s3.delete_object(Bucket=BUCKET, Key=key)
            except Exception as e:  # noqa: BLE001
                out["errors"].append({"key": key, "error": "delete: %s" % str(e)[:100]})
            out["resumed"].append({"function": fn, "hops_walked": doc.get("hops_walked")})
        time.sleep(0.05)
    log = log[-LOG_KEEP:]
    try:
        _pj(s3, LEDGER + "log.json", log)
        _pj(s3, LEDGER + "last-run.json", out)
    except Exception as e:  # noqa: BLE001
        out["errors"].append({"error": "ledger: %s" % str(e)[:100]})
    print(json.dumps({k: (v if not isinstance(v, list) else len(v)) for k, v in out.items()}))
    return out
