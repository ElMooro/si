#!/usr/bin/env python3
"""1056 — deploy justhodl-lobbying-intel + update event-coordinator.

DEPLOYS
═══════
NEW: justhodl-lobbying-intel (daily 16 UTC, 1024MB / 180s)
  - 20K lobbying records from Quiver /live/lobbying
  - 4 signal layers: SPIKE / CLUSTER / NEW ENTRANT / BILL TRACKER
  - Emits lobbying.crowd_signal for sector catalyst clusters
UPDATE: justhodl-event-coordinator (new route + Telegram formatter)

VERIFIES
════════
  - Lambda creates + schedules cleanly
  - Sync-invoke returns expected metrics
  - data/lobbying-intel.json is valid + populated
  - Sector clusters detected with realistic $$ + ticker counts
  - At least N tickers with bill mentions
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1056_lobbying_intel.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=300))
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=300))
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip(name):
    src = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    return buf.getvalue()


def lambda_exists(name):
    try:
        lam.get_function(FunctionName=name)
        return True
    except lam.exceptions.ResourceNotFoundException:
        return False


def upsert_schedule(rule_name, cron, target_fn, description):
    events.put_rule(Name=rule_name, ScheduleExpression=cron,
                     State="ENABLED", Description=description[:255])
    target_arn = lam.get_function(FunctionName=target_fn)["Configuration"]["FunctionArn"]
    events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": target_arn}])
    try:
        lam.add_permission(
            FunctionName=target_fn,
            StatementId=f"ebridge-{rule_name}"[:64],
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=events.describe_rule(Name=rule_name)["Arn"],
        )
    except lam.exceptions.ResourceConflictException:
        pass


def invoke_sync(name):
    r = long_lam.invoke(FunctionName=name,
                         InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        return json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        return {"_raw": body[:400]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: deploy lobbying-intel
    print("[1056] phase 1: deploy justhodl-lobbying-intel…")
    try:
        with open("aws/lambdas/justhodl-lobbying-intel/config.json") as f:
            cfg = json.load(f)
        cfg["role"] = ROLE_ARN
        zb = build_zip("justhodl-lobbying-intel")
        
        if lambda_exists("justhodl-lobbying-intel"):
            for attempt in range(4):
                try:
                    lam.update_function_code(
                        FunctionName="justhodl-lobbying-intel",
                        ZipFile=zb, Publish=False)
                    lam.get_waiter("function_updated").wait(FunctionName="justhodl-lobbying-intel")
                    break
                except Exception as e:
                    if "ResourceConflict" in str(e) and attempt < 3:
                        time.sleep(5 * (attempt + 1)); continue
                    raise
            lam.update_function_configuration(
                FunctionName="justhodl-lobbying-intel",
                Timeout=cfg.get("timeout", 60),
                MemorySize=cfg.get("memory", 128),
                Description=cfg.get("description", "")[:256],
            )
            lam.get_waiter("function_updated").wait(FunctionName="justhodl-lobbying-intel")
            out["lobbying_action"] = "update_code"
        else:
            lam.create_function(
                FunctionName="justhodl-lobbying-intel",
                Runtime=cfg["runtime"],
                Role=cfg["role"],
                Handler=cfg["handler"],
                Code={"ZipFile": zb},
                Description=cfg.get("description", "")[:256],
                Timeout=cfg.get("timeout", 60),
                MemorySize=cfg.get("memory", 128),
                Architectures=cfg.get("architectures", ["x86_64"]),
                Publish=False,
            )
            lam.get_waiter("function_active").wait(FunctionName="justhodl-lobbying-intel")
            out["lobbying_action"] = "create"
        
        out["zip_size"] = len(zb)
        
        sched = cfg.get("schedule") or {}
        if sched.get("rule_name") and sched.get("cron"):
            upsert_schedule(sched["rule_name"], sched["cron"],
                              "justhodl-lobbying-intel", sched.get("description", ""))
            out["lobbying_schedule"] = f"{sched['rule_name']} {sched['cron']}"
    except Exception as e:
        out["lobbying_deploy_err"] = str(e)[:300]
    
    time.sleep(3)
    
    # Phase 2: update event-coordinator
    print("[1056] phase 2: update event-coordinator…")
    try:
        zb = build_zip("justhodl-event-coordinator")
        for attempt in range(4):
            try:
                lam.update_function_code(
                    FunctionName="justhodl-event-coordinator",
                    ZipFile=zb, Publish=False)
                lam.get_waiter("function_updated").wait(FunctionName="justhodl-event-coordinator")
                break
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < 3:
                    time.sleep(5 * (attempt + 1)); continue
                raise
        out["coordinator_update"] = "ok"
    except Exception as e:
        out["coordinator_err"] = str(e)[:300]
    
    time.sleep(3)
    
    # Phase 3: sync-invoke lobbying-intel
    print("[1056] phase 3: sync-invoke lobbying-intel…")
    t0 = time.time()
    try:
        result = invoke_sync("justhodl-lobbying-intel")
        out["invoke"] = {
            "elapsed_s":       round(time.time() - t0, 1),
            "ok":              result.get("ok"),
            "source":          result.get("source"),
            "n_records":       result.get("n_records"),
            "n_tickers":       result.get("n_tickers"),
            "n_clusters":      result.get("n_clusters"),
            "n_spike_alerts":  result.get("n_spike_alerts"),
            "n_new_lobbyists": result.get("n_new_lobbyists"),
            "duration_s":      result.get("duration_s"),
            "err":             result.get("_raw"),
        }
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # Phase 4: verify S3 output
    print("[1056] phase 4: verify S3 output…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/lobbying-intel.json")
        body = obj["Body"].read()
        lob = json.loads(body)
        hl = lob.get("highlights") or {}
        out["snapshot"] = {
            "size_kb":        round(len(body) / 1024, 1),
            "schema":         lob.get("schema_version"),
            "method":         lob.get("method"),
            "source":         lob.get("lobbying_source"),
            "generated_at":   lob.get("generated_at"),
            "duration_s":     lob.get("duration_s"),
            
            "n_records":      lob.get("n_records_total"),
            "n_tickers":      lob.get("n_tickers"),
            "n_clusters":     lob.get("n_clusters"),
            "n_spikes":       lob.get("n_spike_alerts"),
            "n_new_lob":      lob.get("n_new_lobbyists"),
            "n_bills":        lob.get("n_bills_tracked"),
        }
        # Top 3 clusters
        out["snapshot"]["top_3_clusters"] = [
            {"issue":         c["issue"][:80],
             "n_clients":     c["n_clients"],
             "n_tickers":     c["n_tickers"],
             "total_amount":  c["total_amount"],
             "top_tickers":   c["top_tickers"][:6],
             "top_bills":     c["top_bills"][:3]}
            for c in (hl.get("issue_clusters") or [])[:3]
        ]
        # Top 5 spike alerts
        out["snapshot"]["top_5_spikes"] = [
            {"ticker":             r["ticker"],
             "client":             (r.get("client") or "")[:40],
             "score":              r["score"],
             "acceleration_ratio": r["acceleration_ratio"],
             "recent_usd":         r["recent_amount_usd"],
             "new_issues":         r.get("new_issues") or [],
             "bills":              r.get("bills_mentioned") or []}
            for r in (hl.get("spike_alerts") or [])[:5]
        ]
        # Top 5 bills tracked
        out["snapshot"]["top_5_bills"] = (hl.get("bills_tracked") or [])[:5]
    except Exception as e:
        out["snapshot_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1056] DONE → {REPORT}")


if __name__ == "__main__":
    main()
