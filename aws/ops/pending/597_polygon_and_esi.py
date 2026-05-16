"""ops/597 — finalize Bloomberg-10 deployment:
1. Patch POLYGON_KEY into seasonality + liquidity-profile
2. Re-invoke both, confirm sidecars
3. Read macro-surprise to understand its structure, then patch ESI if its
   event-reading logic uses the wrong key name
"""
import json, os, time, base64
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_param(name):
    try:
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def fetch_sidecar(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:200]}"}


def force_invoke(fname):
    try:
        r = lam.invoke(FunctionName=fname, InvocationType="RequestResponse",
                        Payload=b"{}", LogType="Tail")
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response_preview": body[:400], "log_tail": log[-2000:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def patch_env(fname, extra):
    """Merge extra into env without replacing existing keys."""
    cfg = lam.get_function_configuration(FunctionName=fname)
    env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
    env.update(extra)
    lam.update_function_configuration(FunctionName=fname, Environment={"Variables": env})
    time.sleep(2)
    # wait for config consistency
    for _ in range(20):
        s = lam.get_function_configuration(FunctionName=fname)
        if s.get("LastUpdateStatus") != "InProgress":
            return {"keys_now": sorted(env.keys()), "status": s.get("LastUpdateStatus")}
        time.sleep(1)
    return {"keys_now": sorted(env.keys()), "status": "timeout"}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. POLYGON_KEY patching
    poly = get_param("/justhodl/polygon-key") or "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
    a = {}
    for fname in ("justhodl-seasonality", "justhodl-liquidity-profile"):
        a[fname] = {}
        a[fname]["patch"] = patch_env(fname, {"POLYGON_KEY": poly})
        time.sleep(3)
        a[fname]["invoke"] = force_invoke(fname)
        sk = {"justhodl-seasonality": "data/seasonality.json",
              "justhodl-liquidity-profile": "data/liquidity-profile.json"}[fname]
        a[fname]["sidecar"] = fetch_sidecar(sk)
        sc = a[fname]["sidecar"]
        if isinstance(sc, dict) and "_error" not in sc:
            a[fname]["sidecar_size_bytes"] = len(json.dumps(sc, default=str))
            a[fname]["sidecar_keys"] = list(sc.keys())[:18]
    report["polygon_patch"] = a

    # 2. macro-surprise structure
    ms = fetch_sidecar("data/macro-surprise.json")
    if isinstance(ms, dict) and "_error" not in ms:
        report["macro_surprise"] = {
            "size_bytes": len(json.dumps(ms, default=str)),
            "top_keys": list(ms.keys())[:25],
            "n_events_field": {
                "events": len(ms.get("events", [])) if isinstance(ms.get("events"), list) else "n/a",
                "releases": len(ms.get("releases", [])) if isinstance(ms.get("releases"), list) else "n/a",
                "recent_releases": len(ms.get("recent_releases", [])) if isinstance(ms.get("recent_releases"), list) else "n/a",
                "past_release_events": len(ms.get("past_release_events", [])) if isinstance(ms.get("past_release_events"), list) else "n/a",
                "history": len(ms.get("history", [])) if isinstance(ms.get("history"), list) else "n/a",
            },
        }
        # sample first event from any of the list keys
        for k in ("events","releases","recent_releases","past_release_events","history"):
            v = ms.get(k)
            if isinstance(v, list) and v:
                report["macro_surprise"]["sample_event_key"] = k
                report["macro_surprise"]["sample_event"] = v[0] if isinstance(v[0], dict) else str(v[0])[:200]
                break
    else:
        report["macro_surprise"] = ms

    # 3. ESI re-invoke (now that we know more)
    b = {}
    b["invoke"] = force_invoke("justhodl-esi")
    b["sidecar"] = fetch_sidecar("data/esi.json")
    report["esi"] = b

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/597_polygon_and_esi.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nDONE - wrote 597_polygon_and_esi.json")
    return report


if __name__ == "__main__":
    main()
