#!/usr/bin/env python3
"""Step 418 — Verify just-crossed.json wrote + page deployed + Top events."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/418_jc_final.json"
NAME = "justhodl-tmp-jc-final"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def fetch(url, t=15):
    req = urllib.request.Request(url, headers={"User-Agent":"JH/1.0"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read().decode("utf-8", errors="replace"), r.status

def lambda_handler(event, context):
    out = {}

    # 1. List all snapshots
    try:
        listing = s3.list_objects_v2(Bucket="justhodl-dashboard-live",
                                       Prefix="screener/snapshots/")
        snaps = listing.get("Contents") or []
        out["snapshots"] = [{"key": o["Key"], "size_kb": round(o["Size"]/1024,1),
                              "modified": str(o["LastModified"])}
                              for o in sorted(snaps, key=lambda x: x["Key"], reverse=True)[:5]]
    except Exception as e:
        out["snap_err"] = str(e)[:200]

    # 2. Read just-crossed.json
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="screener/just-crossed.json")
        body = obj["Body"].read()
        jc = json.loads(body)
        out["just_crossed"] = {
            "size_kb": round(len(body)/1024, 1),
            "generated_at": jc.get("generated_at"),
            "comparison": jc.get("comparison"),
            "n_events": jc.get("n_events"),
            "type_counts": jc.get("type_counts"),
        }
        events = jc.get("events") or []
        out["top_events"] = [{"type": e["type"], "symbol": e["symbol"],
                                "name": (e.get("name") or "")[:25],
                                "sector": (e.get("sector") or "")[:20],
                                "from": e.get("from"), "to": e.get("to"),
                                "score": e.get("stealScore"),
                                "delta": e.get("delta"),
                                "sig": e.get("significance")}
                               for e in events[:25]]
    except Exception as e:
        out["jc_err"] = str(e)[:300]

    # 3. Page checks
    try:
        page, status = fetch("https://justhodl.ai/screener/?cb=" + str(int(time.time())))
        out["page"] = {
            "status": status, "size": len(page),
            "has_just_crossed_tab":   "JUST CROSSED" in page,
            "has_jc_feed":            "justCrossedFeed" in page,
            "has_jc_events":          "jcEvents" in page,
            "has_jc_filter":          "jc-filter-btn" in page,
            "has_load_fn":            "function loadJustCrossed" in page,
            "has_render_fn":          "function renderJustCrossed" in page,
            "has_event_renderer":     "function renderJustCrossedEvent" in page,
            "has_filter_map":         "JC_FILTER_MAP" in page,
            "has_apply_ui_mode":      "applyTabUIMode" in page,
        }
    except Exception as e:
        out["page_err"] = str(e)[:200]

    # 4. Lambda log tail to confirm just-crossed wrote
    try:
        lg = "/aws/lambda/justhodl-stock-screener"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=40)
            for e in ev.get("events", []):
                msg = e["message"].strip()
                if "just-crossed" in msg.lower() or "snapshot" in msg.lower() or "events written" in msg.lower():
                    lines.append((e["timestamp"], msg))
        lines.sort()
        out["log_jc"] = [{"ts": ts, "msg": m[:200]} for ts, m in lines[-20:]]
    except Exception as e:
        out["log_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
