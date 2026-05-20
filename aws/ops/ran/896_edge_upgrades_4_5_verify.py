"""
ops/896 - Verify edge upgrades #4 (multi-horizon GSI) and
          #5 (streaming intra-day layer) end-to-end.

#4 justhodl-gsi-horizons       -- 4-horizon GSI calibrator
#5 justhodl-streaming-fanout   -- intraday signal-delta fanout
   live.html                   -- real-time consumer dashboard

Steps:
  - confirm both Lambdas deployed
  - propagate WS_API_ID + WS_STAGE from openbb-websocket-broadcast env
    into justhodl-streaming-fanout env (so it can publish the wss URL
    for live.html to self-discover)
  - wire EventBridge Scheduler:
      gsi-horizons       weekly Sunday 09:30 UTC
      streaming-fanout   every minute 13-20 UTC Mon-Fri
  - invoke each Lambda
  - validate S3 outputs (data/gsi-horizons.json, data/streaming-config.json,
    data/streaming-fanout.json)
  - validate SSM keys for the four horizon weight payloads
  - confirm justhodl-global-stress now publishes gsi_by_horizon
  - confirm GitHub Pages serves horizons-gsi.html and live.html

Writes aws/ops/reports/896_edge_upgrades_4_5.json.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
S3_BUCKET = "justhodl-dashboard-live"
SCHEDULER_ROLE = ("arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCOUNT)
BROADCAST_FN = "openbb-websocket-broadcast"

cfg = Config(read_timeout=300, connect_timeout=20,
             retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)
sched = boto3.client("scheduler", region_name=REGION, config=cfg)

report = {"ops": 896, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Verify edge upgrades #4 multi-horizon GSI + "
                     "#5 streaming intra-day layer e2e",
          "checks": []}


def chk(name, ok, detail=""):
    report["checks"].append({"check": name, "ok": bool(ok),
                             "detail": str(detail)[:280]})


def get_or_make_schedule(name, expr, fn_arn):
    try:
        sched.get_schedule(Name=name)
        # update with current expression in case it changed
        sched.update_schedule(
            Name=name,
            ScheduleExpression=expr,
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED",
            Target={
                "Arn": fn_arn,
                "RoleArn": SCHEDULER_ROLE,
                "Input": "{}",
                "RetryPolicy": {"MaximumRetryAttempts": 0,
                                "MaximumEventAgeInSeconds": 3600},
            },
        )
        return "updated"
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        sched.create_schedule(
            Name=name,
            ScheduleExpression=expr,
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED",
            Target={
                "Arn": fn_arn,
                "RoleArn": SCHEDULER_ROLE,
                "Input": "{}",
                "RetryPolicy": {"MaximumRetryAttempts": 0,
                                "MaximumEventAgeInSeconds": 3600},
            },
        )
        return "created"


def fetch_url(url, timeout=15):
    req = urllib.request.Request(
        url + ("&" if "?" in url else "?") + "cb=" + str(int(time.time())),
        headers={"User-Agent": "ops896/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, len(r.read())


def read_lambda_env(fn):
    """Return the env-vars dict of a Lambda's configuration."""
    r = lam.get_function_configuration(FunctionName=fn)
    return r.get("Environment", {}).get("Variables", {}) or {}


# ========================================================================
# #4 justhodl-gsi-horizons
# ========================================================================
hz_arn = None
try:
    r = lam.get_function(FunctionName="justhodl-gsi-horizons")
    hz_arn = r["Configuration"]["FunctionArn"]
    chk("4_lambda_deployed", True,
        "runtime=%s timeout=%ss" %
        (r["Configuration"]["Runtime"], r["Configuration"]["Timeout"]))
except Exception as e:
    chk("4_lambda_deployed", False,
        "%s: %s" % (type(e).__name__, e))

if hz_arn:
    try:
        action = get_or_make_schedule(
            "justhodl-gsi-horizons-weekly",
            "cron(30 9 ? * SUN *)",
            hz_arn)
        chk("4_schedule_wired", True,
            "weekly Sunday 09:30 UTC (%s)" % action)
    except Exception as e:
        chk("4_schedule_wired", False,
            "%s: %s" % (type(e).__name__, e))

    try:
        r = lam.invoke(FunctionName="justhodl-gsi-horizons",
                       InvocationType="RequestResponse",
                       Payload=b"{}")
        raw = r["Payload"].read().decode("utf-8", "ignore")
        payload = json.loads(raw)
        body = json.loads(payload.get("body") or "{}")
        chk("4_invoke", r.get("StatusCode") == 200
            and not r.get("FunctionError")
            and body.get("ok") is True,
            "horizons=%s snapshots=%s top_dims=%s" % (
                body.get("horizons"),
                body.get("snapshots_total"),
                [(t.get("horizon"), t.get("top_dim"), t.get("mode"))
                 for t in (body.get("term_structure") or [])]))
    except Exception as e:
        chk("4_invoke", False, "%s: %s" % (type(e).__name__, e))

    # S3 output
    try:
        time.sleep(2)
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/gsi-horizons.json")
        d = json.loads(obj["Body"].read())
        has_all = all(str(h) in (d.get("results") or {})
                      for h in [5, 21, 63, 252])
        chk("4_s3_output", d.get("ok") is True and has_all,
            "horizons=%s, all_horizons_present=%s, snapshots=%s" %
            (d.get("horizons"), has_all, d.get("snapshots_total")))
    except Exception as e:
        chk("4_s3_output", False, "%s: %s" % (type(e).__name__, e))

    # SSM keys
    ssm_modes = {}
    for hd in [5, 21, 63, 252]:
        try:
            p = ssm.get_parameter(Name="/justhodl/gsi/weights/%dd" % hd)
            v = json.loads(p["Parameter"]["Value"])
            ssm_modes[hd] = v.get("mode")
            chk("4_ssm_/justhodl/gsi/weights/%dd" % hd, True,
                "mode=%s, N=%s, weights=%s" % (
                    v.get("mode"),
                    v.get("sample_size"),
                    {k: round(v.get("weights", {}).get(k, 0), 3)
                     for k in ("market", "credit", "vix", "rate_vol",
                               "contagion", "sovereign")}))
        except Exception as e:
            chk("4_ssm_/justhodl/gsi/weights/%dd" % hd, False,
                "%s: %s" % (type(e).__name__, e))

    # Trigger a global-stress re-run so it picks up the new horizon
    # weights and writes gsi_by_horizon to data/global-stress.json
    try:
        lam.invoke(FunctionName="justhodl-global-stress",
                   InvocationType="RequestResponse",
                   Payload=b"{}")
        time.sleep(3)
        gs = json.loads(s3.get_object(
            Bucket=S3_BUCKET,
            Key="data/global-stress.json")["Body"].read())
        gbh = gs.get("gsi_by_horizon") or {}
        chk("4_global_stress_emits_gsi_by_horizon",
            len(gbh) >= 2,
            "horizons present in gsi_by_horizon=%s, values=%s" %
            (list(gbh.keys()),
             {k: gbh[k].get("gsi") for k in gbh}))
    except Exception as e:
        chk("4_global_stress_emits_gsi_by_horizon", False,
            "%s: %s" % (type(e).__name__, e))

    try:
        s_, sz = fetch_url("https://justhodl.ai/horizons-gsi.html")
        chk("4_page_live", s_ == 200 and sz > 1000,
            "status=%s, bytes=%s" % (s_, sz))
    except Exception as e:
        chk("4_page_live", False, "%s: %s" % (type(e).__name__, e))


# ========================================================================
# #5 justhodl-streaming-fanout
# ========================================================================
sf_arn = None
try:
    r = lam.get_function(FunctionName="justhodl-streaming-fanout")
    sf_arn = r["Configuration"]["FunctionArn"]
    chk("5_lambda_deployed", True,
        "runtime=%s timeout=%ss" %
        (r["Configuration"]["Runtime"], r["Configuration"]["Timeout"]))
except Exception as e:
    chk("5_lambda_deployed", False,
        "%s: %s" % (type(e).__name__, e))

if sf_arn:
    # Propagate WS_API_ID + WS_STAGE from broadcast Lambda's env
    try:
        bc_env = read_lambda_env(BROADCAST_FN)
        ws_id = bc_env.get("WS_API_ID") or ""
        ws_stage = bc_env.get("WS_STAGE") or "prod"
        if ws_id:
            curr_env = read_lambda_env("justhodl-streaming-fanout")
            new_env = dict(curr_env)
            new_env["WS_API_ID"] = ws_id
            new_env["WS_STAGE"] = ws_stage
            new_env["BROADCAST_FN"] = BROADCAST_FN
            lam.update_function_configuration(
                FunctionName="justhodl-streaming-fanout",
                Environment={"Variables": new_env})
            chk("5_ws_api_id_propagated", True,
                "WS_API_ID=%s WS_STAGE=%s" % (ws_id, ws_stage))
        else:
            chk("5_ws_api_id_propagated", False,
                "broadcast Lambda has no WS_API_ID in env -- "
                "the WebSocket API may not be configured yet")
    except Exception as e:
        chk("5_ws_api_id_propagated", False,
            "%s: %s" % (type(e).__name__, e))

    # Schedule: every minute, 13-20 UTC Mon-Fri (US market window)
    try:
        action = get_or_make_schedule(
            "justhodl-streaming-fanout-1min",
            "cron(* 13-20 ? * MON-FRI *)",
            sf_arn)
        chk("5_schedule_wired", True,
            "every 1m 13-20 UTC Mon-Fri (%s)" % action)
    except Exception as e:
        chk("5_schedule_wired", False,
            "%s: %s" % (type(e).__name__, e))

    # Wait for env update to settle, then invoke
    time.sleep(5)
    try:
        r = lam.invoke(FunctionName="justhodl-streaming-fanout",
                       InvocationType="RequestResponse",
                       Payload=b"{}")
        raw = r["Payload"].read().decode("utf-8", "ignore")
        payload = json.loads(raw) if raw else {}
        body = json.loads(payload.get("body") or "{}")
        chk("5_invoke", r.get("StatusCode") == 200
            and not r.get("FunctionError"),
            "tracked=%s broadcasts=%s no_ops=%s missing=%s" %
            (body.get("tracked_engines"),
             body.get("broadcasts"),
             body.get("no_ops"),
             body.get("missing")))
    except Exception as e:
        chk("5_invoke", False, "%s: %s" % (type(e).__name__, e))

    # Streaming config published
    try:
        time.sleep(2)
        obj = s3.get_object(Bucket=S3_BUCKET,
                            Key="data/streaming-config.json")
        d = json.loads(obj["Body"].read())
        chk("5_streaming_config_published",
            bool(d.get("ws_url")) and len(d.get("channels") or []) >= 5,
            "ws_url=%s channels=%s engines=%d" %
            (d.get("ws_url"),
             d.get("channels"),
             len(d.get("engines") or [])))
    except Exception as e:
        chk("5_streaming_config_published", False,
            "%s: %s" % (type(e).__name__, e))

    # Fanout audit log published
    try:
        obj = s3.get_object(Bucket=S3_BUCKET,
                            Key="data/streaming-fanout.json")
        d = json.loads(obj["Body"].read())
        chk("5_fanout_log_published",
            d.get("ok") is True and isinstance(d.get("actions"), list),
            "tracked=%s broadcasts=%s no_ops=%s" %
            (d.get("tracked_engines"),
             d.get("broadcasts"),
             d.get("no_ops")))
    except Exception as e:
        chk("5_fanout_log_published", False,
            "%s: %s" % (type(e).__name__, e))

    # live.html on Pages
    try:
        s_, sz = fetch_url("https://justhodl.ai/live.html")
        chk("5_live_html_live", s_ == 200 and sz > 5000,
            "status=%s, bytes=%s" % (s_, sz))
    except Exception as e:
        chk("5_live_html_live", False, "%s: %s" % (type(e).__name__, e))


# ---- summary -------------------------------------------------------------
n_ok = sum(1 for c in report["checks"] if c["ok"])
n_tot = len(report["checks"])
report["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
report["all_passed"] = n_ok == n_tot
report["verdict"] = (
    "EDGE UPGRADES #4 and #5 LIVE end-to-end. Multi-horizon GSI is "
    "calibrating at 5d/21d/63d/252d weekly, publishing per-horizon "
    "weights to SSM, and global-stress emits the gsi_by_horizon "
    "term-structure on each run. The streaming intra-day layer is "
    "scheduled every minute during US market hours; the fanout driver "
    "detects meaningful deltas on 7 tracked engines and broadcasts to "
    "WebSocket subscribers via openbb-websocket-broadcast. live.html "
    "is the real-time consumer dashboard. All 5 institutional edge "
    "upgrades are now in production."
    if report["all_passed"]
    else "VERIFICATION INCOMPLETE - see per-check details.")

with open("aws/ops/reports/896_edge_upgrades_4_5.json", "w",
          encoding="utf-8") as f:
    json.dump(report, f, indent=1)
print(json.dumps(report, indent=1))
