"""justhodl-import-sentinel v1.0 — the import pipelines' pulse monitor

ops 4576 (Khalid: "data import on data.html has been hitting so many
hiccups — monitor it continuously and fix bugs"). Every 10 minutes this
engine reads the ground truth of every import pipeline, classifies it,
heals the classes that are SAFE to heal, and publishes one honest
payload the Data hub can show.

CLASSIFICATION (per pipeline)
  OK / RUNNING            fresh progress or complete
  STALLED                 walking but no state movement beyond budget
  WEDGED                  lease held in the future yet state stale past
                          budget — the holder crashed (pre-v2.1 class)
  BLOCKED / KEY_INVALID   403 / dead key — NEVER auto-kicked (the
                          dual-crawler 403 incident rule)
  ACTION_REQUIRED         needs Khalid (key rotation, vendor block)

REMEDIATION ALLOWLIST — only what is provably safe:
  * FRED scoped import: clear a wedged lease, then ONE async kick
    (InvocationType=Event queues cleanly even when the single
    concurrency slot is busy — the 4575 lesson: a RequestResponse
    TooManyRequests means "already running", which is the single-flight
    system WORKING, not an error)
  * FRED expansion: when scoped status is COMPLETE*, key healthy, no
    403, throttles quiet → set /justhodl/fred/expand-all=1 exactly once
    (Khalid's standing order: most popular first, then the rest). The
    same AIMD/lease machinery carries the wider scope.
Everything else is report-only in v1 — SDMX walkers and the catalog are
classified and surfaced, never blind-kicked.

OUTPUT data/import-health.json — pipelines, velocity/ETA for the FRED
drain, last 100 incidents, actions taken this sweep.
"""
import json
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/import-health.json"
FRED_FN = "justhodl-fred-catalog"
FRED_STATE = "data/_state/fred-scoped-import.json"
FRED_BUDGET_S = 780

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=20, retries={"max_attempts": 1}))
ssm = boto3.client("ssm", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)

SDMX = ("eurostat", "oecd", "statcan", "bis", "ecb")


def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def age_min(iso):
    if not iso:
        return None
    try:
        d = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - d).total_seconds() / 60, 1)
    except Exception:
        return None


def knob(name, default=None):
    try:
        return ssm.get_parameter(Name=name)["Parameter"]["Value"]
    except Exception:
        return default


def cw_sum(metric, minutes=15):
    try:
        end = datetime.now(timezone.utc)
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName=metric,
            Dimensions=[{"Name": "FunctionName", "Value": FRED_FN}],
            StartTime=end - timedelta(minutes=minutes), EndTime=end,
            Period=60, Statistics=["Sum"])
        return int(sum(p["Sum"] for p in r.get("Datapoints", [])))
    except Exception:
        return None


def classify_fred(st, throttles):
    if not st:
        return "NO_STATE", "state file absent"
    status = str(st.get("status") or "")
    a = age_min(st.get("updated_at"))
    lease = st.get("lease_until") or 0
    now = time.time()
    if status == "KEY_INVALID":
        return "ACTION_REQUIRED", ("FRED key dead — drop the rotated key "
                                   "into SSM /justhodl/fred-api-key; the "
                                   "walk self-heals on rotation")
    if st.get("blocked_at") and (age_min(st.get("blocked_ts")) or 9e9) < 240:
        return "BLOCKED_403", ("FRED 403 within 4h — hands off per the "
                               "dual-crawler incident rule: %s"
                               % str(st.get("blocked_at"))[:80])
    if status.startswith("COMPLETE"):
        return "COMPLETE", status
    if status == "walking":
        if a is not None and a <= 15:
            return "RUNNING", "checkpoint %.0f min ago" % a
        if lease > now:
            if a is not None and a > (FRED_BUDGET_S / 60 + 8):
                return "WEDGED", ("lease held %.0fs into the future but "
                                  "state %.0f min stale — holder crashed"
                                  % (lease - now, a))
            return "RUNNING", "lease live, awaiting next checkpoint"
        if a is not None and a > 20:
            return "STALLED", "lease free, state %.0f min stale" % a
        return "RUNNING", "recent"
    return status or "UNKNOWN", "status=%s age=%s" % (status, a)


def classify_sdmx(name, st):
    if not st:
        return "NO_STATE", "state file absent"
    status = str(st.get("status") or st.get("phase") or "")
    a = age_min(st.get("updated_at") or st.get("as_of"))
    n_fail = len(st.get("failures") or {})
    done = st.get("n_done") or st.get("done") or st.get("n_imported")
    total = st.get("n_total")
    if "COMPLETE" in status.upper() or (
            total and done and done >= total):
        return "COMPLETE", "%s/%s%s" % (done, total,
                                        (", %d source-side failures"
                                         % n_fail) if n_fail else "")
    if a is not None and a > 24 * 60:
        return "STALLED", "no movement in %.0f h (%s/%s)" % (a / 60, done, total)
    if status:
        return "RUNNING", "%s — %s/%s" % (status, done, total)
    return "UNKNOWN", "unrecognized state shape"


def lambda_handler(event=None, context=None):
    t0 = time.time()
    prev = read_json(OUT_KEY) or {}
    incidents = (prev.get("incidents") or [])[:100]
    prev_fred = (prev.get("velocity") or {})
    actions = []
    pipelines = []
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def incident(pipe, kind, detail):
        incidents.insert(0, {"at": now_iso, "pipeline": pipe,
                             "kind": kind, "detail": detail[:200]})

    # ── FRED ──────────────────────────────────────────────────────────
    st = read_json(FRED_STATE)
    throttles = cw_sum("Throttles")
    errors = cw_sum("Errors")
    status, detail = classify_fred(st, throttles)
    fred = {"name": "fred", "status": status, "detail": detail,
            "age_min": age_min((st or {}).get("updated_at")),
            "scope": (st or {}).get("import_scope") or "scoped_7_roots",
            "engine_version": (st or {}).get("engine_version"),
            "imported": (st or {}).get("series_imported"),
            "queue_total": (st or {}).get("queue_total"),
            "queue_cursor": (st or {}).get("queue_cursor"),
            "rate_rpm": (st or {}).get("rate_rpm"),
            "throttles_15m": throttles, "errors_15m": errors,
            "last_crash": next((k + ": " + v for k, v in reversed(list(
                ((st or {}).get("errors") or {}).items()))
                if k.startswith("_crash_")), None)}

    # velocity + ETA from the previous sweep's snapshot
    velocity = {"at": now_iso, "imported": fred["imported"],
                "cursor": fred["queue_cursor"]}
    try:
        dt_h = (age_min(prev_fred.get("at")) or 0) / 60.0
        d_imp = (fred["imported"] or 0) - (prev_fred.get("imported") or 0)
        if dt_h > 0.05 and d_imp >= 0:
            rate_h = d_imp / dt_h
            fred["series_per_hour"] = round(rate_h, 1)
            remaining = ((fred["queue_total"] or 0)
                         - (fred["queue_cursor"] or 0))
            if rate_h > 1 and remaining > 0:
                fred["eta_hours"] = round(remaining / rate_h, 1)
    except Exception:
        pass

    # ── remediation (allowlisted) ─────────────────────────────────────
    if status == "WEDGED" and st is not None:
        st["lease_until"] = 0
        try:
            s3.put_object(Bucket=BUCKET, Key=FRED_STATE,
                          Body=json.dumps(st, default=str).encode(),
                          ContentType="application/json")
            actions.append("fred: cleared wedged lease")
            incident("fred", "auto_heal", "wedged lease cleared")
            status = "STALLED"
        except Exception as e:
            incident("fred", "heal_failed", "lease clear: %s" % e)
    if status == "STALLED":
        try:
            lam.invoke(FunctionName=FRED_FN, InvocationType="Event",
                       Payload=json.dumps({"phase": "scoped_import",
                                           "kicked_by": "import-sentinel"}
                                          ).encode())
            actions.append("fred: async kick queued (Event invoke — "
                           "queues cleanly even when the slot is busy)")
            incident("fred", "auto_heal", "stalled — async kick queued")
        except Exception as e:
            incident("fred", "heal_failed", "kick: %s" % str(e)[:120])

    # ── expansion trigger (Khalid's standing order) ───────────────────
    expand = knob("/justhodl/fred/expand-all", "0")
    fred["expand_all_knob"] = expand
    if (status == "COMPLETE" and expand != "1"
            and fred["scope"] == "scoped_7_roots"
            and (throttles or 0) < 5
            and not str((st or {}).get("status")) == "KEY_INVALID"):
        try:
            ssm.put_parameter(Name="/justhodl/fred/expand-all",
                              Value="1", Type="String", Overwrite=True)
            lam.invoke(FunctionName=FRED_FN, InvocationType="Event",
                       Payload=json.dumps({"phase": "scoped_import",
                                           "kicked_by":
                                               "sentinel-expansion"}
                                          ).encode())
            actions.append("fred: scoped set COMPLETE — expand-all=1 set, "
                           "full-catalog walk kicked (popularity-desc, "
                           "same rate discipline)")
            incident("fred", "expansion",
                     "scoped COMPLETE → full catalog started")
        except Exception as e:
            incident("fred", "expansion_failed", str(e)[:150])
    pipelines.append(fred)

    # ── SDMX walkers + phase-1 tree + catalog freshness ───────────────
    for name in SDMX:
        stx = read_json("data/_state/sdmx-walk-%s.json" % name)
        stt, det = classify_sdmx(name, stx)
        if name == "ecb" and stt in ("NO_STATE", "STALLED", "UNKNOWN"):
            stt = "BLOCKED"
            det = (det + " — ECB API 406 content-negotiation block "
                   "(known; needs Accept-header adapter fix)")[:180]
        pipelines.append({"name": "sdmx-" + name, "status": stt,
                          "detail": det,
                          "age_min": age_min((stx or {}).get("updated_at")
                                             if stx else None)})
    cat = read_json("data/provider-catalog.json")
    pipelines.append({"name": "provider-catalog",
                      "status": ("OK" if cat and (age_min(
                          cat.get("as_of") or cat.get("generated_at"))
                          or 9e9) < 26 * 60 else "STALE"),
                      "detail": "hub index freshness",
                      "age_min": age_min((cat or {}).get("as_of")
                                         or (cat or {}).get("generated_at"))})

    order = {"ACTION_REQUIRED": 0, "BLOCKED_403": 1, "BLOCKED": 1,
             "KEY_INVALID": 0, "WEDGED": 2, "STALLED": 3, "STALE": 4,
             "NO_STATE": 5, "UNKNOWN": 6, "RUNNING": 7, "COMPLETE": 8,
             "OK": 9}
    worst = min(pipelines, key=lambda p: order.get(p["status"], 6))
    overall = ("HEALTHY" if order.get(worst["status"], 6) >= 7
               else ("DEGRADED" if order.get(worst["status"], 6) >= 2
                     else "ACTION_REQUIRED"))

    out = {"engine": "import-sentinel", "version": "1.0",
           "generated_at": now_iso,
           "duration_s": round(time.time() - t0, 1),
           "overall": overall, "worst": worst["name"],
           "pipelines": pipelines,
           "velocity": velocity,
           "actions_this_sweep": actions,
           "incidents": incidents[:100],
           "remediation_allowlist": [
               "fred: wedged-lease clear", "fred: stalled async kick",
               "fred: expansion flip on scoped COMPLETE"],
           "rule": ("403/KEY_INVALID are never auto-kicked; Event "
                    "invokes only (queue cleanly at RC=1); every action "
                    "logged as an incident")}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=60")
    print("[sentinel] %s worst=%s actions=%d fred=%s(%s) %.1fs"
          % (overall, worst["name"], len(actions), fred["status"],
             fred.get("detail", "")[:60], time.time() - t0))
    return {"statusCode": 200,
            "body": json.dumps({"overall": overall,
                                "fred": fred["status"],
                                "actions": actions})}
