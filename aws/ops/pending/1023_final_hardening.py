#!/usr/bin/env python3
"""Step 1023 — Final hardening pass.

  1. Right-size outcome-checker memory: 256 → 1024 MB (62s avg → expect ~25-30s)
  2. Right-size crisis-plumbing memory: 512 → 768 MB (gives more headroom)
  3. Redeploy crisis-plumbing + liquidity-credit-engine with engine.error
     emission wrappers
  4. Redeploy event-coordinator with expanded CRITICAL_ENGINES set
  5. Invoke crisis-plumbing + liquidity-credit-engine to verify they still work
     (the wrapper shouldn't break them; if they fail now, engine.error event
     should fire visible in the audit log)
  6. Read final audit log state to confirm everything is wired
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1023_final_hardening.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def update_memory(fn_name, new_memory_mb, retry=4):
    """Update only the memory configuration of a Lambda."""
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


def redeploy_code(fn_name, retry=4):
    """Build zip from source/ and update function code."""
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


def invoke_once(fn):
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


def read_audit_log_today(limit=20):
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


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # ─── Phase 1: memory right-sizing ───────────────────────────────────
    print("[1023] phase 1: memory right-sizing…")
    out["memory_updates"] = {}
    out["memory_updates"]["justhodl-outcome-checker"] = update_memory("justhodl-outcome-checker", 1024)
    time.sleep(2)
    out["memory_updates"]["justhodl-crisis-plumbing"] = update_memory("justhodl-crisis-plumbing", 768)
    time.sleep(2)
    
    # ─── Phase 2: code redeploys with engine.error wrapping ─────────────
    print("[1023] phase 2: code redeploys (engine.error emission)…")
    out["code_redeploys"] = {}
    for fn in ("justhodl-crisis-plumbing", "justhodl-liquidity-credit-engine",
                "justhodl-event-coordinator"):
        out["code_redeploys"][fn] = redeploy_code(fn)
        time.sleep(3)
    
    # ─── Phase 3: invoke broken engines to verify they still work ───────
    # If they error now, the engine.error event will fire and we'll see it
    # in the audit log + Telegram alert.
    print("[1023] phase 3: invoke broken engines to verify the wrap doesn't break them…")
    out["broken_engine_invokes"] = {}
    out["broken_engine_invokes"]["justhodl-crisis-plumbing"] = invoke_once("justhodl-crisis-plumbing")
    time.sleep(5)
    out["broken_engine_invokes"]["justhodl-liquidity-credit-engine"] = invoke_once("justhodl-liquidity-credit-engine")
    time.sleep(8)   # event propagation
    
    # ─── Phase 4: read audit log to see if engine.error events fired ────
    print("[1023] phase 4: check audit log for engine.error events…")
    out["audit_log"] = read_audit_log_today()
    
    # ─── Phase 5: verify config changes are persisted ───────────────────
    print("[1023] phase 5: verify final memory + code state…")
    out["final_config"] = {}
    for fn in ("justhodl-outcome-checker", "justhodl-crisis-plumbing",
                "justhodl-liquidity-credit-engine", "justhodl-event-coordinator"):
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            out["final_config"][fn] = {
                "memory_mb":   cfg.get("MemorySize"),
                "timeout_s":   cfg.get("Timeout"),
                "last_modified": cfg.get("LastModified"),
                "code_size_kb": round(cfg.get("CodeSize", 0) / 1024, 1),
                "state":        cfg.get("State"),
            }
        except Exception as e:
            out["final_config"][fn] = {"err": str(e)[:120]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
