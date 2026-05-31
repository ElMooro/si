#!/usr/bin/env python3
"""Step 1024 — Extend event-bus coverage + capture broken-engine traces.

PHASE 1: Wire 2 more engines to emit events
  - justhodl-signal-scorecard → signal.promoted + signal.deprecated
  - justhodl-cross-asset-regime → regime.changed

PHASE 2: Verify by invoking + reading the audit log

PHASE 3: Async-invoke the 2 broken engines so we capture their engine.error
events without blocking on a synchronous boto3 client (which timed out at
60s last time). After 90s wait, read audit log for engine.error entries.

PHASE 4: Re-apply memory updates that the workflow may have reverted again,
+ verify configs are sticky now that config.json uses 'memory' field.
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1024_extend_event_coverage.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

# Default 60s client timeout is too short for crisis-plumbing / liquidity-credit-engine.
from botocore.config import Config
lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)


def redeploy_code(fn_name, retry=4):
    src_dir = pathlib.Path(f"aws/lambdas/{fn_name}/source")
    if not src_dir.exists():
        return {"err": "source dir not found"}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    zb = buf.getvalue()
    
    for attempt in range(retry):
        try:
            lam.update_function_code(FunctionName=fn_name, ZipFile=zb, Publish=False)
            lam.get_waiter("function_updated").wait(FunctionName=fn_name)
            return {"action": "updated", "zip_size": len(zb)}
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < retry - 1:
                time.sleep(5 * (attempt + 1))
                continue
            return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def update_memory(fn_name, new_memory_mb, retry=4):
    for attempt in range(retry):
        try:
            current = lam.get_function_configuration(FunctionName=fn_name)
            if current.get("MemorySize") == new_memory_mb:
                return {"action": "no_change", "memory_mb": new_memory_mb}
            lam.update_function_configuration(
                FunctionName=fn_name, MemorySize=new_memory_mb,
            )
            lam.get_waiter("function_updated").wait(FunctionName=fn_name)
            return {
                "action":     "updated",
                "from_memory": current.get("MemorySize"),
                "to_memory":   new_memory_mb,
            }
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < retry - 1:
                time.sleep(5 * (attempt + 1))
                continue
            return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def invoke_sync(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out = {"status": r.get("StatusCode"), "fn_err": r.get("FunctionError")}
        try:
            p = json.loads(body)
            out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["raw"] = body[:400]
        return out
    except Exception as e:
        return {"fail": f"{type(e).__name__}: {str(e)[:200]}"}


def invoke_async(fn):
    """Fire-and-forget invoke. Lambda runs to completion regardless of
    client lifetime. We won't see the result but we'll see audit events
    + CloudWatch logs."""
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        return {"async": True, "status": r.get("StatusCode")}
    except Exception as e:
        return {"fail": f"{type(e).__name__}: {str(e)[:200]}"}


def read_audit_log_today(limit=30):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"system-events/audit/{today}.jsonl"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8")
        lines = [l for l in body.split("\n") if l.strip()]
        return {
            "key": key, "n_events": len(lines),
            "entries": [json.loads(l) for l in lines[-limit:]],
        }
    except s3.exceptions.NoSuchKey:
        return {"missing": True}
    except Exception as e:
        return {"err": str(e)[:200]}


def get_function_config(fn):
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        return {
            "memory_mb":   cfg.get("MemorySize"),
            "timeout_s":   cfg.get("Timeout"),
            "last_modified": cfg.get("LastModified"),
            "code_size_kb": round(cfg.get("CodeSize", 0) / 1024, 1),
        }
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # ─── Phase 1: deploy 2 new event-emitting engines ───────────────────
    print("[1024] phase 1: deploy signal-scorecard + cross-asset-regime…")
    out["redeploys"] = {}
    for fn in ("justhodl-signal-scorecard", "justhodl-cross-asset-regime"):
        out["redeploys"][fn] = redeploy_code(fn)
        time.sleep(3)
    
    # ─── Phase 2: re-apply memory bumps (config.json now uses 'memory') ─
    print("[1024] phase 2: re-apply memory bumps…")
    out["memory"] = {}
    out["memory"]["justhodl-outcome-checker"] = update_memory("justhodl-outcome-checker", 1024)
    time.sleep(2)
    out["memory"]["justhodl-crisis-plumbing"] = update_memory("justhodl-crisis-plumbing", 768)
    time.sleep(2)
    
    # ─── Phase 3: invoke the 2 new event-emitting engines ───────────────
    print("[1024] phase 3: invoke signal-scorecard + cross-asset-regime…")
    out["invokes"] = {}
    out["invokes"]["justhodl-signal-scorecard"] = invoke_sync("justhodl-signal-scorecard")
    time.sleep(8)
    out["invokes"]["justhodl-cross-asset-regime"] = invoke_sync("justhodl-cross-asset-regime")
    time.sleep(10)
    
    # ─── Phase 4: async-invoke the 2 broken engines ─────────────────────
    # These run 60-120s. We can't block on them. Fire async, then wait,
    # then check the audit log for engine.error events.
    print("[1024] phase 4: async-invoke 2 broken engines + wait 90s…")
    out["async_invokes"] = {}
    out["async_invokes"]["justhodl-crisis-plumbing"] = invoke_async("justhodl-crisis-plumbing")
    time.sleep(3)
    out["async_invokes"]["justhodl-liquidity-credit-engine"] = invoke_async("justhodl-liquidity-credit-engine")
    
    print("[1024] waiting 90s for engines to complete + events to flow…")
    time.sleep(90)
    
    # ─── Phase 5: read audit log ────────────────────────────────────────
    print("[1024] phase 5: read audit log for new events…")
    out["audit_log"] = read_audit_log_today(limit=40)
    
    # ─── Phase 6: verify final config ───────────────────────────────────
    print("[1024] phase 6: verify final config state…")
    out["final_config"] = {}
    for fn in ("justhodl-outcome-checker", "justhodl-crisis-plumbing",
                "justhodl-liquidity-credit-engine",
                "justhodl-signal-scorecard", "justhodl-cross-asset-regime"):
        out["final_config"][fn] = get_function_config(fn)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
