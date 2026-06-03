"""1236 — Deploy news/earnings/GDELT integration + earnings catalyst overlay.

Updates:
  - justhodl-prediction-snapshotter (reads news/earnings/gdelt)
  - justhodl-self-improvement (NEW tiers: NEWS_SURGE, EARNINGS_FRESH)
  - justhodl-trade-tickets (earnings calendar overlay)
  - justhodl-prepump-alerts-router (earnings warning in Telegram)

Invokes snapshotter + trade-tickets to verify earnings overlay populates.
"""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1236_news_earnings_gdelt_rollout.json"
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(source_dir):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, source_dir)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# 1. Update 4 Lambdas
LAMBDAS = [
    ("justhodl-prediction-snapshotter", "aws/lambdas/justhodl-prediction-snapshotter/source"),
    ("justhodl-self-improvement", "aws/lambdas/justhodl-self-improvement/source"),
    ("justhodl-trade-tickets", "aws/lambdas/justhodl-trade-tickets/source"),
    ("justhodl-prepump-alerts-router", "aws/lambdas/justhodl-prepump-alerts-router/source"),
]
print("[1236] 1. Update 4 Lambdas")
out["updates"] = {}
for name, src in LAMBDAS:
    try:
        zip_bytes = build_zip(src)
        lam.update_function_code(FunctionName=name, ZipFile=zip_bytes)
        for _ in range(15):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("LastUpdateStatus") == "Successful":
                break
        out["updates"][name] = {"sha": c.get("CodeSha256")[:16],
                                  "state": c.get("State")}
        print(f"  ✓ {name}: sha={c.get('CodeSha256')[:16]}")
    except Exception as e:
        out["updates"][name] = {"error": str(e)[:200]}

# 2. Check source data exists
print("\n[1236] 2. Check source data files exist")
sources = ["sentiment/data.json", "screener/earnings-sentiment.json",
            "data/gdelt-financial-sentiment.json", "data/retail-sentiment.json"]
out["source_files"] = {}
for key in sources:
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        out["source_files"][key] = {"size_kb": round(head["ContentLength"]/1024, 1),
                                      "modified": head["LastModified"].isoformat()[:19]}
        print(f"  ✓ {key}: {round(head['ContentLength']/1024,1)} KB")
    except Exception as e:
        out["source_files"][key] = {"missing": True}
        print(f"  ✗ {key}: missing (graceful fallback OK)")

# 3. Invoke snapshotter
print("\n[1236] 3. Invoke prediction-snapshotter (news/earnings/GDELT enrichment)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-prediction-snapshotter",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["snap_invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                            "function_error": resp.get("FunctionError"),
                            "body": payload[:1500]}
    if not resp.get("FunctionError"):
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  ✓ status=200, predictions={inner.get('n_predictions')}, alerts={inner.get('alert_distribution')}")
        except: pass
except Exception as e:
    out["snap_invoke"] = {"error": str(e)[:200]}

# 4. Verify new alerts/features appeared
print("\n[1236] 4. Verify NEW_SURGE/EARNINGS_FRESH tiers + news/earnings features")
try:
    snap = json.loads(s3.get_object(Bucket=BUCKET, Key="data/predictions-snapshots/latest.json")["Body"].read())
    preds = snap.get("predictions") or []
    
    news_surge = [p for p in preds if "NEWS_SURGE_BULLISH" in (p.get("alerts") or [])]
    earnings_fresh = [p for p in preds if "EARNINGS_FRESH" in (p.get("alerts") or [])]
    with_news = [p for p in preds if (p.get("features") or {}).get("news_score") is not None]
    with_earnings = [p for p in preds if (p.get("features") or {}).get("earnings_score") is not None]
    with_gdelt = [p for p in preds if (p.get("features") or {}).get("gdelt_tone") is not None]
    
    out["new_tiers"] = {
        "n_news_surge": len(news_surge),
        "n_earnings_fresh": len(earnings_fresh),
        "n_with_news_features": len(with_news),
        "n_with_earnings_features": len(with_earnings),
        "n_with_gdelt_features": len(with_gdelt),
        "news_surge_sample": [p.get("ticker") for p in news_surge[:5]],
        "earnings_fresh_sample": [p.get("ticker") for p in earnings_fresh[:5]],
    }
    print(f"  NEWS_SURGE_BULLISH tier: {len(news_surge)} tickers")
    print(f"  EARNINGS_FRESH tier: {len(earnings_fresh)} tickers")
    print(f"  Preds enriched with news: {len(with_news)}")
    print(f"  Preds enriched with earnings: {len(with_earnings)}")
    print(f"  Preds enriched with GDELT: {len(with_gdelt)}")
except Exception as e:
    out["new_tiers"] = {"error": str(e)[:200]}

# 5. Invoke trade-tickets to test earnings overlay
print("\n[1236] 5. Invoke trade-tickets (earnings catalyst overlay)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-trade-tickets",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["tickets_invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                               "function_error": resp.get("FunctionError"),
                               "body": payload[:800]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
except Exception as e:
    out["tickets_invoke"] = {"error": str(e)[:200]}

# 6. Check earnings warnings in tickets
print("\n[1236] 6. Check earnings warnings populated in tickets")
try:
    tickets_doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-tickets.json")["Body"].read())
    tickets = (tickets_doc.get("tickets") or [])
    in_window = [t for t in tickets if t.get("earnings_in_window")]
    out["earnings_overlay"] = {
        "n_tickets_total": len(tickets),
        "n_with_earnings_in_window": len(in_window),
        "sample": [{"ticker": t.get("ticker"), "date": t.get("earnings_date"),
                     "days_until": t.get("earnings_days_until"),
                     "source": t.get("earnings_source"),
                     "warning": t.get("earnings_warning")}
                    for t in in_window[:5]],
    }
    print(f"  ✓ {len(in_window)}/{len(tickets)} tickets have earnings_in_window=True")
    for t in in_window[:3]:
        print(f"    {t.get('ticker')}: {t.get('earnings_warning')}")
except Exception as e:
    out["earnings_overlay"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print("\n[1236] DONE")
