#!/usr/bin/env python3
"""1068 — check FRED cache freshness + how many other Lambdas are hammering FRED."""
import json, os, pathlib, time
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/1068_fred_cache.json"

s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
lam    = boto3.client("lambda", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: check FRED cache files
    print("[1068] phase 1: check FRED cache files…")
    out["cache_files"] = {}
    for key in ["data/fred-cache.json", "data/fred-cache-secretary.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            d = json.loads(body) if body else {}
            out["cache_files"][key] = {
                "size_kb":       round(len(body) / 1024, 1),
                "last_modified": obj["LastModified"].isoformat(),
                "top_keys":      list(d.keys())[:20] if isinstance(d, dict) else None,
                "n_series":      len(d) if isinstance(d, dict) else None,
            }
            # Check for WALCL, WTREGEN, RRPONTSYD specifically
            for sid in ["WALCL", "WTREGEN", "RRPONTSYD"]:
                if isinstance(d, dict) and sid in d:
                    entry = d[sid]
                    obs = entry.get("observations") if isinstance(entry, dict) else None
                    out["cache_files"][key][f"has_{sid}"] = {
                        "n_obs": len(obs) if isinstance(obs, list) else None,
                        "latest_obs": obs[-1] if isinstance(obs, list) and obs else None,
                        "cached_at": entry.get("cached_at") if isinstance(entry, dict) else None,
                    }
        except s3.exceptions.NoSuchKey:
            out["cache_files"][key] = {"err": "NoSuchKey"}
        except Exception as e:
            out["cache_files"][key] = {"err": str(e)[:200]}
    
    # Phase 2: count how many Lambdas have FRED in their env
    print("[1068] phase 2: enumerate FRED consumers…")
    out["fred_consumers"] = []
    paginator = lam.get_paginator("list_functions")
    n_total = 0
    n_with_fred = 0
    for page in paginator.paginate():
        for f in page["Functions"]:
            n_total += 1
            try:
                cfg = lam.get_function_configuration(FunctionName=f["FunctionName"])
                env_vars = (cfg.get("Environment") or {}).get("Variables") or {}
                if "FRED_API_KEY" in env_vars or "FRED_KEY" in env_vars:
                    n_with_fred += 1
                    out["fred_consumers"].append({
                        "name": f["FunctionName"],
                        "last_modified": cfg.get("LastModified"),
                    })
                if n_with_fred >= 30:
                    break
            except Exception:
                continue
        if n_with_fred >= 30:
            break
    out["n_total_lambdas"] = n_total
    out["n_lambdas_with_fred"] = n_with_fred
    
    # Phase 3: count EventBridge rules that fire FRED-using Lambdas
    print("[1068] phase 3: schedule density…")
    rules = events.list_rules()["Rules"]
    out["n_rules_total"] = len(rules)
    
    # Phase 4: re-test FRED rate limit NOW (45s after last test)
    print("[1068] phase 4: re-test FRED single-call after wait…")
    time.sleep(20)
    import urllib.request, urllib.error
    url = ("https://api.stlouisfed.org/fred/series/observations"
            "?series_id=WALCL&api_key=2f057499936072679d8843d7fce99989"
            "&file_type=json&limit=3&sort_order=desc")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Diag/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read()
            d = json.loads(body)
            obs = d.get("observations") or []
            out["fred_retest"] = {
                "status": r.status,
                "n_obs":  len(obs),
                "latest": obs[0] if obs else None,
            }
    except urllib.error.HTTPError as e:
        out["fred_retest"] = {"err": f"HTTP {e.code}"}
    except Exception as e:
        out["fred_retest"] = {"err": str(e)[:200]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1068] DONE → {REPORT}")


if __name__ == "__main__":
    main()
