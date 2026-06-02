"""1188 — Re-invoke macro-regime (now with ETF proxies) + AI flow analysis
(now with regime injected) + show full institutional output.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1188_phase2_verify.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


# Step 1: re-invoke macro-regime (sync, get full output)
print("[1188] 1. Sync invoke macro-regime")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-macro-regime",
                      InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["steps"]["macro_invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body_preview": payload[:1200],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    # Read full output
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key="macro/regime.json")["Body"].read())
        tl = doc.get("top_level_regime", {}) or {}
        subs = doc.get("sub_regimes", {}) or {}
        # Get representative assets
        am = doc.get("asset_metrics", []) or []
        sample = sorted(am, key=lambda m: m.get("zscore_90d") or 0, reverse=True)[:5] if am else []
        worst = sorted([m for m in am if m.get("zscore_90d") is not None],
                       key=lambda m: m.get("zscore_90d") or 0)[:5]
        out["steps"]["macro_output"] = {
            "top_level": tl,
            "sub_regimes": {k: {"label": v.get("label"), "score": v.get("score")}
                            for k, v in subs.items()},
            "n_ok": doc.get("n_ok"),
            "universe_size": doc.get("universe_size"),
            "best_5_by_zscore": [
                {k: v for k, v in m.items() if k in
                  ["ticker","name","role","feed","latest_close","ret_5d_pct","ret_21d_pct","zscore_90d"]}
                for m in sample
            ],
            "worst_5_by_zscore": [
                {k: v for k, v in m.items() if k in
                  ["ticker","name","role","feed","latest_close","ret_5d_pct","ret_21d_pct","zscore_90d"]}
                for m in worst
            ],
            "errors": [m for m in am if m.get("error")][:5],
        }
        print(f"  ✓ regime={tl.get('regime')} confidence={tl.get('confidence')}")
        print(f"  ✓ {doc.get('n_ok')}/{doc.get('universe_size')} assets fetched")
    except Exception as e:
        out["steps"]["macro_output"] = {"error": str(e)[:300]}
except Exception as e:
    out["steps"]["macro_invoke"] = {"error": str(e)[:300]}

# Step 2: re-invoke AI flow analysis (now will see macro/regime.json)
print(f"\n[1188] 2. Sync invoke flows-ai-analysis with macro regime")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-flows-ai-analysis",
                      InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["steps"]["ai_invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body_preview": payload[:600],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    # Read AI output and compare to prior
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/ai-analysis.json")["Body"].read())
        a = doc.get("analysis", {})
        out["steps"]["ai_output"] = {
            "generated_at": doc.get("generated_at"),
            "claude_elapsed_s": doc.get("claude_elapsed_s"),
            "usage": doc.get("usage"),
            "input_summary": doc.get("input_summary"),
            "regime_call": a.get("regime_call"),
            "macro_narrative_preview": (a.get("macro_narrative") or "")[:600],
            "n_divergences": len(a.get("key_divergences", []) or []),
            "first_divergence": (a.get("key_divergences") or [{}])[0] if a.get("key_divergences") else None,
            "n_ticker_calls": len(a.get("ticker_calls", []) or []),
            "ticker_calls": [
                {
                    "t": c.get("ticker"), "call": c.get("call"),
                    "conv": c.get("conviction"), "tf": c.get("timeframe_days"),
                    "size": c.get("position_size_pct"),
                    "stop": c.get("stop_loss_pct"), "target": c.get("target_21d_pct"),
                    "n_aligned": (c.get("signal_alignment") or {}).get("n_signals_aligned"),
                    "thesis": (c.get("thesis_1liner") or "")[:140],
                }
                for c in (a.get("ticker_calls") or [])
            ],
            "n_pair_trades": len(a.get("pair_trades", []) or []),
            "pair_trades_summary": [
                {"long": p.get("long"), "short": p.get("short"),
                 "conv": p.get("conviction"), "tf": p.get("timeframe_days"),
                 "thesis": (p.get("thesis") or "")[:160]}
                for p in (a.get("pair_trades") or [])
            ],
            "watchlist": a.get("watchlist"),
            "regime_alpha_note": (a.get("regime_alpha_note") or "")[:500],
        }
        rc = a.get("regime_call", {}) or {}
        print(f"  ✓ regime={rc.get('regime')} · {len(a.get('ticker_calls') or [])} calls")
    except Exception as e:
        out["steps"]["ai_output"] = {"error": str(e)[:300]}
except Exception as e:
    out["steps"]["ai_invoke"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1188] DONE")
