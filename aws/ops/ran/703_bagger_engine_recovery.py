"""ops/703 — verify bagger-engine recovered after env-merge workflow fix.

Checks:
  1. env now has FMP_KEY (inherited from sector-heatmap via inherit_env)
  2. full invoke succeeds and scores the universe
  3. sidecar has real tiers + top names with pillar breakdowns
"""
import json, os, time, base64
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def fetch_sidecar(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:200]}"}


def force_invoke(fname, payload=b"{}"):
    try:
        r = lam.invoke(FunctionName=fname, InvocationType="RequestResponse",
                        Payload=payload, LogType="Tail")
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response": body[:700], "log_tail": log[-2500:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. env check
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-bagger-engine")
        env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
        report["config"] = {
            "last_modified": cfg.get("LastModified"),
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "state": cfg.get("State"),
            "env_keys": sorted(env.keys()),
            "has_fmp_key": "FMP_KEY" in env and len(env.get("FMP_KEY", "")) > 10,
            "has_telegram": "TELEGRAM_TOKEN" in env and "TELEGRAM_CHAT_ID" in env,
        }
    except Exception as e:
        report["config"] = {"err": str(e)[:200]}

    # 2. full invoke (720s timeout on the function; this ops Lambda must allow it)
    print("Invoking bagger-engine (full universe)...")
    report["invoke"] = force_invoke("justhodl-bagger-engine")

    # 3. sidecar
    sc = fetch_sidecar("data/bagger-engine.json")
    if isinstance(sc, dict) and "_error" not in sc:
        tiers = sc.get("tiers", {})
        report["sidecar"] = {
            "size_kb": round(len(json.dumps(sc, default=str)) / 1024, 1),
            "generated_at": sc.get("generated_at"),
            "elapsed_s": sc.get("elapsed_s"),
            "universe_size": sc.get("universe_size"),
            "candidates_in_range": sc.get("candidates_in_range"),
            "n_scored": sc.get("n_scored"),
            "n_errors": sc.get("n_errors"),
            "tier_counts": sc.get("tier_counts"),
            "top_15": [
                {"rank": r.get("rank"), "symbol": r.get("symbol"),
                 "name": (r.get("name") or "")[:26],
                 "score": r.get("bagger_score"),
                 "cap_bucket": r.get("cap_bucket"),
                 "cls": (r.get("twin_engine") or {}).get("classification"),
                 "rev_cagr": (r.get("key_stats") or {}).get("revenue_cagr_pct"),
                 "roic": (r.get("key_stats") or {}).get("roic_pct"),
                 "intrinsic": (r.get("key_stats") or {}).get("intrinsic_compounding_pct")}
                for r in (sc.get("top_100") or [])[:15]
            ],
            "potential_100x": [
                {"symbol": r.get("symbol"), "name": (r.get("name") or "")[:26],
                 "score": r.get("bagger_score"),
                 "yr20_rerated": (r.get("twin_engine") or {}).get("yr20", {}).get("with_rerating_x"),
                 "years_to_100x": (r.get("twin_engine") or {}).get("years_to_100x")}
                for r in tiers.get("potential_100x", [])[:10]
            ],
            "potential_25x_sample": [
                {"symbol": r.get("symbol"), "score": r.get("bagger_score"),
                 "rev_cagr": (r.get("key_stats") or {}).get("revenue_cagr_pct")}
                for r in tiers.get("potential_25x", [])[:8]
            ],
        }
        # Pillar breakdown of #1
        top = (sc.get("top_100") or [{}])[0]
        report["sidecar"]["rank1_pillars"] = top.get("pillars")
        report["sidecar"]["rank1_twin_engine"] = top.get("twin_engine")
        report["sidecar"]["rank1_thesis"] = top.get("thesis")
    else:
        report["sidecar"] = sc

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/703_bagger_engine_recovery.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 703_bagger_engine_recovery.json")


if __name__ == "__main__":
    main()
