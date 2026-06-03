"""1227 — Verify adaptive cascade consumers + calibration panel deployment."""
import json
import time
import urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1227_adaptive_cascade_verify.json"
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# 1. Verify Lambda code was updated (check for get_active_cascade in deployed source)
print("[1227] 1. Verify Lambdas redeployed with adaptive cascade helper")
out["lambdas"] = {}
for L in ["justhodl-trade-tickets", "justhodl-prepump-alerts-router"]:
    try:
        cfg_info = lam.get_function_configuration(FunctionName=L)
        out["lambdas"][L] = {
            "state": cfg_info.get("State"),
            "last_modified": cfg_info.get("LastModified")[:19],
            "code_sha": cfg_info.get("CodeSha256")[:16],
        }
        print(f"  {L}: state={cfg_info.get('State')} modified={cfg_info.get('LastModified')[:19]}")
    except Exception as e:
        out["lambdas"][L] = {"error": str(e)[:200]}

# 2. Invoke both Lambdas to test the adaptive cascade behavior
print(f"\n[1227] 2. Test invoke trade-tickets (should pick adaptive cascade)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-trade-tickets",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    out["trade_tickets_invoke"] = {
        "status": resp.get("StatusCode"),
        "elapsed_s": elapsed,
        "function_error": resp.get("FunctionError"),
        "body": resp.get("Payload").read().decode()[:1500],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {out['trade_tickets_invoke']['body'][:400]}")
except Exception as e:
    out["trade_tickets_invoke"] = {"error": str(e)[:200]}

print(f"\n[1227] 3. Test invoke prepump-alerts-router (should pick adaptive cascade)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-prepump-alerts-router",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    out["prepump_router_invoke"] = {
        "status": resp.get("StatusCode"),
        "elapsed_s": elapsed,
        "function_error": resp.get("FunctionError"),
        "body": resp.get("Payload").read().decode()[:1500],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {out['prepump_router_invoke']['body'][:400]}")
except Exception as e:
    out["prepump_router_invoke"] = {"error": str(e)[:200]}

# 3. Fetch CloudWatch logs to confirm adaptive-cascade messages
print(f"\n[1227] 4. Inspect CloudWatch logs for adaptive-cascade behavior")
logs = boto3.client("logs", region_name=REGION, config=cfg)
out["log_messages"] = {}
for L in ["justhodl-trade-tickets", "justhodl-prepump-alerts-router"]:
    try:
        log_group = f"/aws/lambda/{L}"
        # Find latest log stream
        streams_resp = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=1,
        )
        if streams_resp.get("logStreams"):
            stream_name = streams_resp["logStreams"][0]["logStreamName"]
            events_resp = logs.get_log_events(
                logGroupName=log_group, logStreamName=stream_name,
                startFromHead=False, limit=15,
            )
            messages = [e.get("message", "")[:200] for e in events_resp.get("events", [])]
            adaptive_msgs = [m for m in messages if "adaptive-cascade" in m]
            out["log_messages"][L] = {"adaptive_msgs": adaptive_msgs[:3],
                                       "stream": stream_name[:40]}
            print(f"  {L}:")
            for m in adaptive_msgs[:3]:
                print(f"    {m.strip()}")
    except Exception as e:
        out["log_messages"][L] = {"error": str(e)[:150]}

# 4. Verify pre-pump-radar.html has calibration panel deployed
print(f"\n[1227] 5. Verify pre-pump-radar.html deployed with calibration panel")
try:
    req = urllib.request.Request("https://justhodl.ai/pre-pump-radar.html",
                                    headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=15) as r:
        html = r.read().decode()
    out["html_check"] = {
        "size_kb": round(len(html) / 1024, 1),
        "has_ai_brief": "ai-brief-panel" in html,
        "has_cal_panel": 'id="cal-panel"' in html,
        "has_cal_fetch": "cascade-recalibration-audit.json" in html,
        "has_cal_weights_list": 'id="cal-weights"' in html,
        "has_cal_movers_list": 'id="cal-movers"' in html,
    }
    print(f"  ✓ size: {out['html_check']['size_kb']} KB")
    print(f"    AI brief: {out['html_check']['has_ai_brief']}")
    print(f"    Calibration panel: {out['html_check']['has_cal_panel']}")
    print(f"    Calibration fetch: {out['html_check']['has_cal_fetch']}")
    print(f"    Weights list: {out['html_check']['has_cal_weights_list']}")
    print(f"    Movers list: {out['html_check']['has_cal_movers_list']}")
except Exception as e:
    out["html_check"] = {"error": str(e)[:200]}

# 5. Trigger recalibrator one more time to ensure latest audit doc available
print(f"\n[1227] 6. Re-invoke recalibrator to refresh audit for panel display")
try:
    resp = lam.invoke(FunctionName="justhodl-cascade-recalibrator",
                       InvocationType="RequestResponse", Payload=b"{}")
    out["recal_refresh"] = {"status": resp.get("StatusCode"),
                              "body": resp.get("Payload").read().decode()[:500]}
    print(f"  status={resp.get('StatusCode')}")
except Exception as e:
    out["recal_refresh"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1227] DONE")
