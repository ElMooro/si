"""1189 — Verify regime-tagged backtest end-to-end.

Workflow:
  1. Re-invoke equity-research on 1 ticker to seed a regime-stamped doc
  2. Re-invoke critique on same ticker to test critique stamping
  3. Re-invoke backtest to compute regime_attribution
  4. Read backtest output, show regime breakdown
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1189_regime_attribution_verify.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}

TEST_TICKER = "AAPL"


# Step 1: re-invoke research on AAPL to write a regime-stamped doc
print(f"[1189] 1. Re-invoke equity-research on {TEST_TICKER} (force fresh write)")
try:
    t0 = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-equity-research",
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "queryStringParameters": {"ticker": TEST_TICKER, "force": "true"}
        }).encode(),
    )
    elapsed = round(time.time() - t0, 1)
    body = resp.get("Payload").read().decode()
    out["steps"]["research_invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body_size": len(body),
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s body_len={len(body)}")
    if resp.get("FunctionError"):
        print(f"  ⚠ {body[:400]}")
except Exception as e:
    out["steps"]["research_invoke"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

# Step 2: read the research doc and check the stamp
print(f"\n[1189] 2. Inspect equity-research/{TEST_TICKER}.json for regime stamp")
try:
    doc = json.loads(s3.get_object(
        Bucket=BUCKET, Key=f"equity-research/{TEST_TICKER}.json"
    )["Body"].read())
    stamp = doc.get("regime_at_generation") or {}
    out["steps"]["research_stamp"] = {
        "schema_version": doc.get("schema_version"),
        "generated_at": doc.get("generated_at"),
        "regime": stamp.get("regime"),
        "confidence": stamp.get("confidence"),
        "reasoning": stamp.get("reasoning"),
        "sub_regimes": stamp.get("sub_regimes"),
        "macro_generated_at": stamp.get("macro_generated_at"),
    }
    print(f"  ✓ schema={doc.get('schema_version')} regime={stamp.get('regime')}")
    print(f"  reasoning: {stamp.get('reasoning')}")
except Exception as e:
    out["steps"]["research_stamp"] = {"error": str(e)[:300]}

# Step 3: re-invoke critique to test critique stamp
print(f"\n[1189] 3. Re-invoke critique on {TEST_TICKER}")
try:
    t0 = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-research-critique",
        InvocationType="RequestResponse",
        Payload=json.dumps({"ticker": TEST_TICKER}).encode(),
    )
    elapsed = round(time.time() - t0, 1)
    body = resp.get("Payload").read().decode()
    out["steps"]["critique_invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body_preview": body[:400],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {body[:400]}")
except Exception as e:
    out["steps"]["critique_invoke"] = {"error": str(e)[:300]}

# Verify critique stamp
try:
    cdoc = json.loads(s3.get_object(
        Bucket=BUCKET, Key=f"equity-critique/{TEST_TICKER}.json"
    )["Body"].read())
    cstamp = cdoc.get("regime_at_generation") or {}
    out["steps"]["critique_stamp"] = {
        "generated_at": cdoc.get("generated_at"),
        "regime": cstamp.get("regime"),
        "confidence": cstamp.get("confidence"),
        "sub_regimes_count": len(cstamp.get("sub_regimes") or {}),
    }
    print(f"  ✓ critique stamp regime={cstamp.get('regime')}")
except Exception as e:
    out["steps"]["critique_stamp"] = {"error": str(e)[:300]}

# Step 4: re-invoke backtest with new code
print(f"\n[1189] 4. Re-invoke backtest (async; poll for output)")
try:
    invoke_t0 = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-research-backtest",
        InvocationType="Event",
        Payload=b"{}",
    )
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    print(f"  async invoke {resp.get('StatusCode')}; polling backtest/report.json...")
    for i in range(60):  # ~5 min
        time.sleep(5)
        try:
            head = s3.head_object(Bucket=BUCKET, Key="backtest/report.json")
            if head["LastModified"] > invoke_dt:
                elapsed = round(time.time() - invoke_t0, 1)
                doc = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="backtest/report.json"
                )["Body"].read())
                ra = doc.get("regime_attribution") or {}
                out["steps"]["backtest_invoke"] = {
                    "elapsed_s": elapsed,
                    "generated_at": doc.get("generated_at"),
                    "n_research_files": doc.get("n_research_files"),
                    "n_calls_with_returns": doc.get("n_calls_with_returns"),
                    "n_calls_with_alpha": doc.get("n_calls_with_alpha"),
                    "avg_days_held": doc.get("avg_days_held"),
                    "caveats": doc.get("caveats", []),
                }
                out["steps"]["backtest_regime"] = {
                    "coverage": ra.get("regime_coverage"),
                    "by_regime": ra.get("by_regime", []),
                    "by_rating_regime": ra.get("by_rating_regime", []),
                }
                print(f"  ✓ backtest done in {elapsed}s")
                print(f"  ✓ regime coverage: {ra.get('regime_coverage')}")
                break
        except Exception:
            pass
    else:
        out["steps"]["backtest_invoke"] = {"error": "poll timeout"}
except Exception as e:
    out["steps"]["backtest_invoke"] = {"error": str(e)[:300]}

# Step 5: check how many research files have stamps now
print(f"\n[1189] 5. Sample 10 research files to check stamp coverage")
try:
    pag = s3.get_paginator("list_objects_v2")
    sample_keys = []
    for page in pag.paginate(Bucket=BUCKET, Prefix="equity-research/"):
        for obj in (page.get("Contents") or []):
            if obj["Key"].endswith(".json") and not obj["Key"].endswith("manifest.json"):
                sample_keys.append(obj["Key"])
                if len(sample_keys) >= 60:
                    break
        if len(sample_keys) >= 60:
            break
    stamps = []
    for k in sample_keys[:60]:
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
            stamps.append({
                "ticker": d.get("ticker"),
                "has_stamp": bool(d.get("regime_at_generation")),
                "stamp_regime": (d.get("regime_at_generation") or {}).get("regime"),
                "schema": d.get("schema_version"),
            })
        except Exception:
            continue
    n_stamped = sum(1 for s in stamps if s["has_stamp"])
    out["steps"]["stamp_coverage"] = {
        "n_inspected": len(stamps),
        "n_stamped": n_stamped,
        "pct_stamped": round(100 * n_stamped / max(len(stamps), 1), 1),
        "sample": stamps[:15],
    }
    print(f"  ✓ {n_stamped}/{len(stamps)} files stamped")
except Exception as e:
    out["steps"]["stamp_coverage"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1189] DONE")
