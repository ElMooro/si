"""ops/715 — verify leading-markets v2: 19 markets, 5 buckets, relative
strength, and that crisis-composite still consumes it cleanly."""
import json, os, time, urllib.request
import boto3
from datetime import datetime, timezone

BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response": (r["Payload"].read().decode("utf-8", "replace")[:500]
                             if r.get("Payload") else "")}
    except Exception as e:
        return {"status": "error", "err": str(e)[:250]}


def sidecar(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:160]}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    report["leading_markets"] = {"invoke": invoke("justhodl-leading-markets")}
    lm = sidecar("data/leading-markets.json")
    if "_error" not in lm:
        report["leading_markets"]["sidecar"] = {
            "schema_version": lm.get("schema_version"),
            "n_markets": lm.get("n_markets"),
            "turning_point_signal": lm.get("turning_point_signal"),
            "flashing_buckets": lm.get("flashing_buckets"),
            "leading_score": lm.get("leading_score"),
            "risk_score": lm.get("risk_score"),
            "expansion_breadth_pct": lm.get("expansion_breadth_pct"),
            "benchmark": lm.get("benchmark"),
            "fmp_failed": lm.get("fmp_failed"),
            "buckets": {k: {"health": v.get("health"),
                            "dominant": v.get("dominant_regime"),
                            "flashing": v.get("flashing"),
                            "n_lagging": v.get("n_lagging_vs_acwi")}
                        for k, v in (lm.get("buckets") or {}).items()},
            "markets": [{"etf": m.get("etf"), "mkt": m.get("market"),
                         "bucket": m.get("bucket"), "regime": m.get("regime"),
                         "r3m": m.get("ret_3m_pct"), "rs": m.get("rs_3m_pct"),
                         "rs_state": m.get("rs_state")}
                        for m in (lm.get("markets") or [])],
        }
    else:
        report["leading_markets"]["sidecar"] = lm

    # crisis-composite still consumes it
    time.sleep(2)
    report["crisis_composite"] = {"invoke": invoke("justhodl-crisis-composite")}
    cc = sidecar("data/crisis-composite.json")
    if "_error" not in cc:
        lead = next((c for c in (cc.get("components") or [])
                     if "leading" in (c.get("source") or "")), None)
        report["crisis_composite"]["check"] = {
            "n_components": len(cc.get("components") or []),
            "leading_component": lead,
            "master_crisis_score": cc.get("master_crisis_score"),
            "defcon_level": cc.get("defcon_level"),
        }

    # defcon radar live with bucket view
    try:
        ts = int(time.time())
        req = urllib.request.Request(f"https://justhodl.ai/defcon.html?cb={ts}",
                                      headers={"User-Agent": "JustHodl-Verify/1.0",
                                               "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            code, html = r.getcode(), r.read().decode("utf-8", "replace")
    except Exception as e:
        code, html = None, str(e)[:160]
    report["defcon_page"] = {
        "http_status": code,
        "bucket_view_live": isinstance(html, str) and code == 200
                            and "Canary Buckets" in html,
    }

    lmsc = report["leading_markets"].get("sidecar", {})
    report["summary"] = {
        "leading_markets_v2_ok": (lmsc.get("schema_version") == "2.0"
                                   and lmsc.get("n_markets", 0) >= 17),
        "n_markets": lmsc.get("n_markets"),
        "n_buckets": len(lmsc.get("buckets", {})),
        "fmp_failed": lmsc.get("fmp_failed"),
        "crisis_composite_ok": report.get("crisis_composite", {}).get("check", {}).get("leading_component") is not None,
        "defcon_bucket_view": report["defcon_page"].get("bucket_view_live"),
    }
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/715_leading_v2_verify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 715_leading_v2_verify.json :: " + json.dumps(report["summary"]))


if __name__ == "__main__":
    main()
