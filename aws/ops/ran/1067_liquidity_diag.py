#!/usr/bin/env python3
"""1067 — diagnose liquidity-agent (homepage tile shows $0.0T net liquidity).

Phases:
  1. Read data/liquidity-data.json from S3 — inspect structure + freshness
  2. Check EventBridge schedule for the Lambda
  3. Pull CloudWatch logs (last 24h)
  4. Test FRED API directly (does the key still work? are WALCL/WTREGEN/RRPONTSYD reachable?)
  5. Sync-invoke the Lambda and capture result
"""
import json, os, pathlib, time, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1067_liquidity_diag.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam    = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=180))
events = boto3.client("events", region_name=REGION)
logs   = boto3.client("logs", region_name=REGION)
s3     = boto3.client("s3", region_name=REGION)

FRED_KEY = "2f057499936072679d8843d7fce99989"


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: Read live data file
    print("[1067] phase 1: read liquidity-data.json…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="liquidity-data.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["s3_file"] = {
            "size_bytes":    len(body),
            "last_modified": obj["LastModified"].isoformat(),
        }
        out["meta"] = d.get("meta", {})
        core = d.get("core", {})
        out["core_keys"] = list(core.keys())[:30]
        # Extract net liquidity + the 3 core metrics
        nl = core.get("net_liquidity") or {}
        out["net_liquidity"] = {
            "value_bn":   nl.get("value_bn"),
            "label":      nl.get("label"),
            "components": nl.get("components", {}),
        }
        # Probe the 3 displayed metrics
        for sid in ["walcl", "wtregen", "rrpontsyd"]:
            entry = core.get(sid) or {}
            if isinstance(entry, dict):
                out[f"core_{sid}"] = {
                    "latest":         entry.get("latest"),
                    "latest_value":   entry.get("latest_value"),
                    "value_bn":       entry.get("value_bn"),
                    "n_observations": len(entry.get("observations") or []),
                }
        # Or look for the displayed labels
        for label_key in ["fed_balance_sheet", "tga", "rrp"]:
            entry = core.get(label_key) or {}
            if isinstance(entry, dict):
                out[f"core_{label_key}"] = {
                    k: entry.get(k)
                    for k in ["latest_value", "value_bn", "as_of", "n_obs", "trend"]
                    if entry.get(k) is not None
                }
    except Exception as e:
        out["s3_read_err"] = str(e)[:300]
    
    # Phase 2: EventBridge schedule
    print("[1067] phase 2: check schedule…")
    try:
        # Common rule name patterns
        for rule_candidate in ["liquidity-agent-schedule", "liquidity-agent-daily",
                                  "justhodl-liquidity-agent", "tga-fed-liquidity"]:
            try:
                r = events.describe_rule(Name=rule_candidate)
                out["schedule"] = {
                    "rule_name": rule_candidate,
                    "schedule":  r.get("ScheduleExpression"),
                    "state":     r.get("State"),
                }
                break
            except events.exceptions.ResourceNotFoundException:
                continue
        if "schedule" not in out:
            # List all rules and find any matching liquidity
            r = events.list_rule_names_by_target(
                TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:justhodl-liquidity-agent"
            )
            out["schedule_rules"] = r.get("RuleNames", [])
    except Exception as e:
        out["schedule_err"] = str(e)[:200]
    
    # Phase 3: CloudWatch logs (last 24h)
    print("[1067] phase 3: pull recent logs…")
    log_group = "/aws/lambda/justhodl-liquidity-agent"
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = end_time - (24 * 60 * 60 * 1000)
    try:
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=5,
        )
        out["recent_streams"] = [
            {"stream": s["logStreamName"],
             "last_event": s.get("lastEventTimestamp")}
            for s in streams.get("logStreams", [])
        ]
        out["log_events"] = []
        for s in streams.get("logStreams", [])[:3]:
            try:
                evt = logs.get_log_events(
                    logGroupName=log_group, logStreamName=s["logStreamName"],
                    startTime=start_time, limit=80,
                )
                for e in evt.get("events", []):
                    msg = e["message"].strip()
                    if msg and not msg.startswith(("START ", "END ", "INIT_START")):
                        out["log_events"].append({
                            "ts": e["timestamp"],
                            "msg": msg[:280],
                        })
            except Exception:
                pass
    except Exception as e:
        out["logs_err"] = str(e)[:200]
    
    # Phase 4: Test FRED API directly
    print("[1067] phase 4: direct FRED probe…")
    out["fred_probes"] = {}
    for series_id in ["WALCL", "WTREGEN", "RRPONTSYD"]:
        url = (f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series_id}&api_key={FRED_KEY}"
                f"&file_type=json&limit=5&sort_order=desc")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Diag/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                body = r.read()
                fred_data = json.loads(body)
                obs = fred_data.get("observations") or []
                out["fred_probes"][series_id] = {
                    "status":    r.status,
                    "n_obs":     len(obs),
                    "latest_5":  [(o.get("date"), o.get("value")) for o in obs[:5]],
                }
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8")[:200]
            except Exception:
                err_body = ""
            out["fred_probes"][series_id] = {"err": f"HTTP {e.code}",
                                                "body": err_body}
        except Exception as e:
            out["fred_probes"][series_id] = {"err": f"{type(e).__name__}: {str(e)[:120]}"}
        time.sleep(0.3)
    
    # Phase 5: Sync-invoke
    print("[1067] phase 5: sync-invoke liquidity-agent…")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName="justhodl-liquidity-agent",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out["invoke"] = {
            "elapsed_s":  round(time.time() - t0, 1),
            "status":     r.get("StatusCode"),
            "raw":        body[:600],
        }
        try:
            p = json.loads(body)
            out["invoke"]["parsed"] = p
        except Exception:
            pass
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    # Re-read after invoke
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="liquidity-data.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["after_invoke"] = {
            "size_bytes":    len(body),
            "last_modified": obj["LastModified"].isoformat(),
            "meta_generated_at": (d.get("meta") or {}).get("generated_at"),
            "core_walcl_value": ((d.get("core") or {}).get("walcl") or {}).get("latest_value"),
            "core_wtregen_value": ((d.get("core") or {}).get("wtregen") or {}).get("latest_value"),
            "core_rrpontsyd_value": ((d.get("core") or {}).get("rrpontsyd") or {}).get("latest_value"),
        }
    except Exception as e:
        out["after_invoke_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1067] DONE → {REPORT}")


if __name__ == "__main__":
    main()
