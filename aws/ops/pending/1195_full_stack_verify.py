"""1195 — Final verification of the expanded institutional stack.

Steps:
  1. Re-invoke constituents Lambda (now covers all 84 ETFs)
  2. Read constituent-pressure.json + stock-exposure-lookup.json
  3. Show top stocks by aggregate cross-ETF flow exposure (the new full view)
  4. Re-invoke flows-ai-analysis (now sees constituent + macro + flows)
  5. Show top calls — should reflect the now-complete cross-feed picture
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1195_full_stack_verify.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


# Step 1: Re-invoke constituents (will take longer with 84 ETFs)
print("[1195] 1. Sync invoke constituents (84 ETFs × FMP holdings)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-etf-constituents",
                      InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    body = resp.get("Payload").read().decode()
    out["steps"]["constituents_invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body_preview": body[:800],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {body[:500]}")
except Exception as e:
    out["steps"]["constituents_invoke"] = {"error": str(e)[:300]}

# Step 2: Read constituent-pressure.json
print(f"\n[1195] 2. Read constituent outputs")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/constituent-pressure.json")["Body"].read())
    out["steps"]["constituent_doc"] = {
        "generated_at": doc.get("generated_at"),
        "mode": doc.get("mode"),
        "n_etfs_total": doc.get("n_etfs_total"),
        "n_etfs_high_z": doc.get("n_etfs_high_z"),
        "n_etfs_fetched": doc.get("n_etfs_fetched"),
        "n_stocks_with_exposure": doc.get("n_stocks_with_exposure"),
        "n_top_constituents_by_pressure": len(doc.get("top_constituents_by_pressure", [])),
        "n_top_aggregate_exposure": len(doc.get("top_aggregate_exposure", [])),
        "top_25_aggregate_exposure": [
            {
                "stock": s.get("stock"),
                "name": s.get("name"),
                "n_etfs_holding": s.get("n_etfs_holding"),
                "cumulative_weight_pct": s.get("cumulative_weight_pct"),
                "agg_5d_usd": s.get("total_aggregate_flow_5d_usd"),
                "agg_21d_usd": s.get("total_aggregate_flow_21d_usd"),
                "top_3_etfs": [
                    {"etf": e.get("etf"),
                     "wt": e.get("weight_pct"),
                     "z": e.get("etf_zscore"),
                     "flow_5d": e.get("etf_flow_5d_usd")}
                    for e in (s.get("top_holding_etfs") or [])[:3]
                ],
            }
            for s in (doc.get("top_aggregate_exposure") or [])[:25]
        ],
    }
    print(f"  ✓ {doc.get('n_etfs_fetched')}/{doc.get('n_etfs_total')} ETFs fetched")
    print(f"  ✓ {doc.get('n_stocks_with_exposure')} stocks have cross-ETF exposure")
except Exception as e:
    out["steps"]["constituent_doc"] = {"error": str(e)[:300]}

# Step 3: Check the slim lookup file
print(f"\n[1195] 3. Check stock-exposure-lookup.json")
try:
    lookup = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/stock-exposure-lookup.json")["Body"].read())
    sample_lookups = {}
    for ticker in ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "JPM", "XOM", "WMT"]:
        l = lookup.get(ticker)
        if l:
            sample_lookups[ticker] = {
                "n_etfs_holding": l.get("n_etfs_holding"),
                "cum_wt": l.get("cumulative_weight_pct"),
                "agg_5d_M": round((l.get("total_aggregate_flow_5d_usd") or 0) / 1e6, 1),
                "agg_21d_M": round((l.get("total_aggregate_flow_21d_usd") or 0) / 1e6, 1),
                "top_3_etfs": [
                    {"etf": e.get("etf"), "wt": e.get("weight_pct"), "z": e.get("etf_zscore")}
                    for e in (l.get("top_etfs") or [])[:3]
                ],
            }
    out["steps"]["lookup_sample"] = {
        "total_stocks_in_lookup": len(lookup),
        "samples": sample_lookups,
    }
    print(f"  ✓ lookup has {len(lookup)} stocks")
    for t, info in sample_lookups.items():
        print(f"    {t}: n_etfs={info['n_etfs_holding']}, cum_wt={info['cum_wt']}%, 5d_flow=${info['agg_5d_M']}M, 21d=${info['agg_21d_M']}M")
except Exception as e:
    out["steps"]["lookup_sample"] = {"error": str(e)[:300]}

# Step 4: Re-invoke flows-ai-analysis (now with constituent pressure context)
print(f"\n[1195] 4. Re-invoke flows-ai-analysis with full context")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-flows-ai-analysis",
                      InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    body = resp.get("Payload").read().decode()
    out["steps"]["ai_invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body_preview": body[:400],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {body[:500]}")
    # Read AI output
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/ai-analysis.json")["Body"].read())
        a = doc.get("analysis", {})
        out["steps"]["ai_output"] = {
            "generated_at": doc.get("generated_at"),
            "claude_elapsed_s": doc.get("claude_elapsed_s"),
            "usage": doc.get("usage"),
            "regime_call": a.get("regime_call"),
            "macro_narrative_preview": (a.get("macro_narrative") or "")[:600],
            "n_divergences": len(a.get("key_divergences", []) or []),
            "first_divergence": (a.get("key_divergences") or [{}])[0],
            "n_ticker_calls": len(a.get("ticker_calls", []) or []),
            "ticker_calls": [
                {
                    "t": c.get("ticker"), "call": c.get("call"),
                    "conv": c.get("conviction"), "tf": c.get("timeframe_days"),
                    "n_aligned": (c.get("signal_alignment") or {}).get("n_signals_aligned"),
                    "thesis": (c.get("thesis_1liner") or "")[:120],
                }
                for c in (a.get("ticker_calls") or [])
            ],
            "regime_alpha_note": (a.get("regime_alpha_note") or "")[:400],
        }
        print(f"  ✓ regime={(a.get('regime_call') or {}).get('regime')} · {len(a.get('ticker_calls') or [])} calls")
    except Exception as e:
        out["steps"]["ai_output"] = {"error": str(e)[:300]}
except Exception as e:
    out["steps"]["ai_invoke"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1195] DONE")
