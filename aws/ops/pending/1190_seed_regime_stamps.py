"""1190 — Force-refresh 8 high-value tickers with regime stamps, then
re-run backtest to populate the regime_attribution table.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1190_seed_regime_stamps.json"
BUCKET = "justhodl-dashboard-live"

# Tickers across multiple sectors for diverse regime-tagged sample
SEED_TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "JPM", "XOM", "WMT"]

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {"seed": {}}}


# Step 1: force-refresh each ticker
print(f"[1190] 1. Force-refresh {len(SEED_TICKERS)} tickers (Lambda async)")
invoke_t0 = time.time()
for t in SEED_TICKERS:
    try:
        resp = lam.invoke(
            FunctionName="justhodl-equity-research",
            InvocationType="Event",  # async
            Payload=json.dumps({
                "queryStringParameters": {"ticker": t, "refresh": "1"}
            }).encode(),
        )
        out["steps"]["seed"][t] = {"status": resp.get("StatusCode")}
        print(f"  ✓ async invoked {t}")
    except Exception as e:
        out["steps"]["seed"][t] = {"error": str(e)[:150]}

# Step 2: wait for writes (research takes ~60-90s each, parallel so total ~3min)
print(f"\n[1190] 2. Wait for research writes (sleep 240s)")
time.sleep(240)

# Step 3: check stamps
print(f"\n[1190] 3. Check regime stamps on seeded tickers")
stamp_results = {}
for t in SEED_TICKERS:
    try:
        doc = json.loads(s3.get_object(
            Bucket=BUCKET, Key=f"equity-research/{t}.json"
        )["Body"].read())
        stamp = doc.get("regime_at_generation") or {}
        stamp_results[t] = {
            "schema": doc.get("schema_version"),
            "generated_at": doc.get("generated_at"),
            "regime": stamp.get("regime"),
            "confidence": stamp.get("confidence"),
            "is_recent": (doc.get("generated_at") or "")[:10] == datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }
        ok = "✓" if stamp.get("regime") else "✗"
        print(f"  {ok} {t:6s} schema={doc.get('schema_version')} regime={stamp.get('regime') or '—'} generated={doc.get('generated_at','')[:16]}")
    except Exception as e:
        stamp_results[t] = {"error": str(e)[:150]}
out["steps"]["stamp_check"] = stamp_results

# Step 4: re-invoke backtest with longer wait
print(f"\n[1190] 4. Re-invoke backtest (longer poll)")
try:
    t0 = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-research-backtest",
        InvocationType="Event",
        Payload=b"{}",
    )
    invoke_dt = datetime.fromtimestamp(t0, timezone.utc)
    print(f"  async invoke {resp.get('StatusCode')}; polling up to 7 min...")
    for i in range(85):  # ~7 min
        time.sleep(5)
        try:
            head = s3.head_object(Bucket=BUCKET, Key="backtest/report.json")
            if head["LastModified"] > invoke_dt:
                elapsed = round(time.time() - t0, 1)
                doc = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="backtest/report.json"
                )["Body"].read())
                ra = doc.get("regime_attribution") or {}
                out["steps"]["backtest"] = {
                    "elapsed_s": elapsed,
                    "n_calls_with_alpha": doc.get("n_calls_with_alpha"),
                    "regime_coverage": ra.get("regime_coverage"),
                    "by_regime": ra.get("by_regime", []),
                    "by_rating_regime": ra.get("by_rating_regime", []),
                }
                cov = ra.get("regime_coverage") or {}
                print(f"  ✓ done in {elapsed}s; coverage {cov.get('pct_coverage')}% ({cov.get('n_calls_with_regime_tag')}/{cov.get('n_calls_with_alpha')})")
                break
        except Exception:
            pass
    else:
        out["steps"]["backtest"] = {"error": "poll timeout"}
        print(f"  ⚠ poll timeout")
except Exception as e:
    out["steps"]["backtest"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1190] DONE")
