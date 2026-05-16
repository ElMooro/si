"""ops/713 — verify the leading-markets engine and its wiring into
crisis-composite (risk) and the Khalid Index (bloomberg-v8 report.json),
plus the defcon.html radar section.
"""
import json, os, urllib.request, time
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


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

    # 1. leading-markets engine
    print("leading-markets...")
    report["leading_markets"] = {"invoke": invoke("justhodl-leading-markets")}
    lm = sidecar("data/leading-markets.json")
    if "_error" not in lm:
        report["leading_markets"]["sidecar"] = {
            "generated_at": lm.get("generated_at"),
            "turning_point_signal": lm.get("turning_point_signal"),
            "leading_score": lm.get("leading_score"),
            "risk_score": lm.get("risk_score"),
            "expansion_breadth_pct": lm.get("expansion_breadth_pct"),
            "regime_counts": lm.get("regime_counts"),
            "risk_appetite": lm.get("risk_appetite"),
            "n_markets": lm.get("n_markets"),
            "fmp_failed": lm.get("fmp_failed"),
            "markets": [{"etf": m.get("etf"), "mkt": m.get("market"),
                         "regime": m.get("regime"), "r3m": m.get("ret_3m_pct")}
                        for m in (lm.get("markets") or [])],
        }
    else:
        report["leading_markets"]["sidecar"] = lm

    # 2. crisis-composite now fuses leading-markets
    print("crisis-composite...")
    report["crisis_composite"] = {"invoke": invoke("justhodl-crisis-composite")}
    cc = sidecar("data/crisis-composite.json")
    if "_error" not in cc:
        comps = cc.get("components") or []
        lead = next((c for c in comps if "leading" in (c.get("source") or "")), None)
        report["crisis_composite"]["check"] = {
            "n_components": len(comps),
            "master_crisis_score": cc.get("master_crisis_score"),
            "defcon_level": cc.get("defcon_level"),
            "leading_markets_component": lead,
            "leading_wired": lead is not None,
        }
    else:
        report["crisis_composite"]["check"] = cc

    # 3. Khalid Index (bloomberg-v8 -> report.json)
    print("bloomberg-v8 (Khalid Index)...")
    report["khalid_index"] = {"invoke": invoke("justhodl-bloomberg-v8")}
    time.sleep(3)
    rep = sidecar("data/report.json")
    if "_error" not in rep:
        khalid = rep.get("khalid_index") or rep.get("khalid") or {}
        comps = khalid.get("components") or {}
        lm_comp = comps.get("leading_markets")
        report["khalid_index"]["check"] = {
            "score": khalid.get("score"),
            "regime": khalid.get("regime"),
            "n_components": len(comps),
            "leading_markets_component": lm_comp,
            "leading_wired": lm_comp is not None,
        }
    else:
        report["khalid_index"]["check"] = rep

    # 4. defcon.html radar section live
    try:
        ts = int(time.time())
        req = urllib.request.Request(f"https://justhodl.ai/defcon.html?cb={ts}",
                                      headers={"User-Agent": "JustHodl-Verify/1.0",
                                               "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            code, html = r.getcode(), r.read().decode("utf-8", "replace")
    except Exception as e:
        code, html = None, str(e)[:160]
    markers = ["Macro Turning-Point Radar", "leading-markets.json", "Canary Markets",
               "Turning-Point Signal"]
    report["defcon_page"] = {
        "http_status": code,
        "radar_live": isinstance(html, str) and code == 200 and all(m in html for m in markers),
        "missing": [m for m in markers if isinstance(html, str) and m not in html],
    }

    report["summary"] = {
        "leading_markets_ok": ("_error" not in report["leading_markets"].get("sidecar", {})
                                and report["leading_markets"]["invoke"].get("fn_error") is None),
        "crisis_composite_wired": report["crisis_composite"].get("check", {}).get("leading_wired"),
        "khalid_index_wired": report["khalid_index"].get("check", {}).get("leading_wired"),
        "defcon_radar_live": report["defcon_page"].get("radar_live"),
    }
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/713_leading_markets_verify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 713_leading_markets_verify.json :: " + json.dumps(report["summary"]))


if __name__ == "__main__":
    main()
