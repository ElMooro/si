"""ops/705 — final verification of the 100x Bagger Expansion Pack.

  1. re-invoke global-liquidity, confirm the unit fix landed:
       BOJ ~ 3500-5500 USD bn, Fed net liquidity ~ 4500-6800 USD bn,
       GLI ~ 16000-20000 USD bn
  2. confirm baggers.html is live on GitHub Pages and references the 5 sidecars
  3. freshness check on all 5 sidecars
"""
import json, os, time, base64, urllib.request
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def invoke(fn, payload=b"{}"):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                        Payload=payload, LogType="Tail")
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", "replace") if r.get("LogResult") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response": body[:500], "log_tail": log[-1200:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def sidecar(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:160]}"}


def fetch_url(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Verify/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)[:200]


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. global-liquidity re-invoke + unit sanity
    print("re-invoking global-liquidity...")
    inv = invoke("justhodl-global-liquidity")
    gl = sidecar("data/global-liquidity.json")
    gcheck = {"invoke": inv}
    if "_error" not in gl:
        g = gl.get("global_liquidity_index", {})
        nl = gl.get("fed_net_liquidity", {})
        comp = g.get("components_usd_bn", {})
        boj = comp.get("BOJ")
        gli = g.get("total_usd_bn")
        netliq = nl.get("value_usd_bn")
        gcheck.update({
            "regime": gl.get("regime"),
            "fred_failed": gl.get("fred_failed"),
            "components_usd_bn": comp,
            "gli_total_usd_bn": gli,
            "gli_trillions": g.get("total_usd_trillions"),
            "fed_net_liquidity_bn": netliq,
            "fed_net_liquidity_trillions": nl.get("value_usd_trillions"),
            "m2_yoy": (gl.get("broad_money") or {}).get("us_m2_yoy_pct"),
            "checks": {
                "boj_sane": boj is not None and 2500 <= boj <= 7000,
                "gli_sane": gli is not None and 14000 <= gli <= 22000,
                "netliq_sane": netliq is not None and 3000 <= netliq <= 8000,
            },
        })
    else:
        gcheck["sidecar"] = gl
    report["global_liquidity"] = gcheck

    # 2. baggers.html live on GitHub Pages
    print("checking baggers.html...")
    code, html = fetch_url("https://justhodl.ai/baggers.html")
    markers = ["100x Bagger Engine", "bagger-engine.json", "hiring-velocity.json",
               "global-liquidity.json", "insider-aggregate.json", "coffee-can.json",
               "Multibagger Candidates", "Coffee-Can Portfolio"]
    report["baggers_page"] = {
        "http_status": code,
        "size_bytes": len(html) if isinstance(html, str) else None,
        "markers_found": {m: (m in html) for m in markers} if isinstance(html, str) and code == 200 else {},
        "all_markers_ok": (isinstance(html, str) and code == 200
                            and all(m in html for m in markers)),
    }

    # 3. freshness of all 5 sidecars
    print("freshness check...")
    fresh = {}
    for key in ["bagger-engine.json", "coffee-can.json", "hiring-velocity.json",
                "global-liquidity.json", "insider-aggregate.json"]:
        d = sidecar("data/" + key)
        if "_error" in d:
            fresh[key] = {"status": "MISSING", "err": d["_error"]}
            continue
        gen = d.get("generated_at", "")
        age_h = None
        try:
            age_h = round((datetime.now(timezone.utc)
                           - datetime.fromisoformat(gen)).total_seconds() / 3600, 1)
        except Exception:
            pass
        fresh[key] = {
            "status": "OK", "generated_at": gen, "age_hours": age_h,
            "size_kb": round(len(json.dumps(d, default=str)) / 1024, 1),
        }
    report["sidecar_freshness"] = fresh

    # summary
    report["summary"] = {
        "global_liquidity_units_fixed": all(
            (report["global_liquidity"].get("checks") or {}).values()),
        "baggers_page_live": report["baggers_page"]["all_markers_ok"],
        "all_sidecars_present": all(v["status"] == "OK" for v in fresh.values()),
    }
    report["finished"] = datetime.now(timezone.utc).isoformat()

    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/705_bagger_pack_final.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 705_bagger_pack_final.json :: " + json.dumps(report["summary"]))


if __name__ == "__main__":
    main()
