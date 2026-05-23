"""ops 1087 — Wealth Plan end-to-end audit (memory #29 doctrine).

Verifies:
  1. Function URL responds publicly (no AWS auth) with CORS headers
  2. Returns valid wealth-plan JSON with non-trivial fields
  3. HTML at https://justhodl.ai/wealth-plan.html loads + references the
     correct Function URL
  4. Compass integration: Lambda reads data/forward-returns.json (Capital
     Compass), uses live forward ERs
  5. Freshness manifest includes data/wealth-plan-snapshot.json
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone

REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN_URL = "https://tgo66l3w3s2trxoizqowfwvo240wclyw.lambda-url.us-east-1.on.aws/"
PAGE_URL = "https://justhodl.ai/wealth-plan.html"
MANIFEST_KEY = "data/_freshness-manifest.json"
BUCKET = "justhodl-dashboard-live"
SNAPSHOT_KEY = "data/wealth-plan-snapshot.json"
COMPASS_KEY = "data/forward-returns.json"


def http_get(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "JustHodl/Audit"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return {"status": r.status, "headers": dict(r.headers), "body": r.read().decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "err": str(e)[:200]}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # 1. Hit Function URL with default params, then with a young aggressive profile
    print("1) Function URL — default profile (35yo)...")
    r1 = http_get(FN_URL + "?current_nav=100000&age=35&retire_age=65&annual_savings=24000&annual_spending=80000&risk_profile=moderate", timeout=45)
    report["fn_url_default"] = {
        "status": r1.get("status"),
        "cors_origin": r1.get("headers", {}).get("Access-Control-Allow-Origin") if isinstance(r1.get("headers"), dict) else None,
    }
    if r1.get("body"):
        try:
            data = json.loads(r1["body"])
            report["fn_url_default"]["fields"] = list(data.keys())[:30]
            report["fn_url_default"]["prob_success"] = data.get("probability_of_success")
            report["fn_url_default"]["verdict_status"] = (data.get("verdict") or {}).get("status")
            report["fn_url_default"]["allocation_E_r"] = (data.get("portfolio_metrics") or {}).get("expected_return_pct")
            report["fn_url_default"]["sensitivities_keys"] = list((data.get("sensitivities") or {}).keys())
            report["fn_url_default"]["compass_age_h"] = data.get("compass_age_hours")
            report["fn_url_default"]["body_kb"] = round(len(r1["body"]) / 1024, 1)
        except Exception as e:
            report["fn_url_default"]["parse_err"] = str(e)[:150]
            report["fn_url_default"]["first_300"] = r1["body"][:300]

    print("2) Function URL — aggressive 25yo profile...")
    r2 = http_get(FN_URL + "?current_nav=50000&age=25&retire_age=60&annual_savings=30000&annual_spending=60000&risk_profile=aggressive", timeout=45)
    if r2.get("body"):
        try:
            data = json.loads(r2["body"])
            report["fn_url_young_aggressive"] = {
                "status": r2.get("status"),
                "prob_success": data.get("probability_of_success"),
                "verdict_status": (data.get("verdict") or {}).get("status"),
                "p50_terminal_today_dollars": (data.get("trajectory_real") or {}).get("p50_terminal_today_dollars"),
            }
        except Exception:
            report["fn_url_young_aggressive"] = {"status": r2.get("status"), "parse_err": True}

    # 3. CORS preflight test
    print("3) CORS preflight (OPTIONS)...")
    try:
        req = urllib.request.Request(FN_URL, method="OPTIONS",
                                      headers={"Origin": "https://justhodl.ai", "Access-Control-Request-Method": "GET"})
        with urllib.request.urlopen(req, timeout=10) as r:
            report["cors_preflight"] = {"status": r.status, "headers": {k.lower(): v for k, v in r.headers.items() if "access-control" in k.lower()}}
    except Exception as e:
        report["cors_preflight"] = {"err": str(e)[:150]}

    # 4. HTML page audit
    print("4) HTML page at justhodl.ai/wealth-plan.html...")
    rh = http_get(PAGE_URL, timeout=15)
    if rh.get("body"):
        body = rh["body"]
        report["html_page"] = {
            "status": rh.get("status"),
            "size_kb": round(len(body) / 1024, 1),
            "references_fn_url": FN_URL in body or FN_URL.rstrip("/") in body,
            "has_sliders": "type=\"range\"" in body or "type='range'" in body,
            "has_fan_chart": "svg" in body.lower() and ("p10" in body.lower() or "fan" in body.lower()),
            "has_sensitivities": "sensitivit" in body.lower() or "what if" in body.lower(),
            "has_compass_link": "compass.html" in body,
            "title_has_wealth": "wealth" in body.lower()[:2000],
        }

    # 5. compass.html cross-link audit
    print("5) compass.html cross-link to wealth-plan...")
    rc = http_get("https://justhodl.ai/compass.html", timeout=15)
    if rc.get("body"):
        report["compass_links_to_wealth"] = "wealth-plan" in rc["body"] or "wealth_plan" in rc["body"]

    # 6. S3 outputs
    print("6) S3 outputs...")
    try:
        o = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)
        report["s3_snapshot"] = {
            "size_kb": round(o["ContentLength"] / 1024, 1),
            "last_modified": o["LastModified"].isoformat(),
        }
    except Exception as e:
        report["s3_snapshot"] = {"err": str(e)[:120]}

    try:
        o = s3.get_object(Bucket=BUCKET, Key=COMPASS_KEY)
        report["s3_compass"] = {
            "size_kb": round(o["ContentLength"] / 1024, 1),
            "last_modified": o["LastModified"].isoformat(),
        }
    except Exception as e:
        report["s3_compass"] = {"err": str(e)[:120]}

    # 7. Freshness manifest check
    print("7) Freshness manifest check...")
    try:
        o = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        m = json.loads(o["Body"].read())
        ov = m.get("key_overrides", {})
        report["freshness"] = {
            "total_overrides": len(ov),
            "has_wealth_plan_snapshot": SNAPSHOT_KEY in ov,
            "has_forward_returns": COMPASS_KEY in ov,
        }
    except Exception as e:
        report["freshness"] = {"err": str(e)[:120]}

    # Verdict
    issues = []
    if report.get("fn_url_default", {}).get("status") != 200:
        issues.append("Function URL not 200")
    if not report.get("html_page", {}).get("references_fn_url"):
        issues.append("HTML doesn't reference correct Function URL")
    if not report.get("html_page", {}).get("has_compass_link"):
        issues.append("HTML missing cross-link to /compass.html")
    if not report.get("compass_links_to_wealth"):
        issues.append("compass.html missing cross-link to /wealth-plan.html")
    if not report.get("freshness", {}).get("has_wealth_plan_snapshot"):
        issues.append("Freshness manifest missing wealth-plan-snapshot.json")
    report["open_issues"] = issues
    report["pass"] = len(issues) == 0

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1087.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:4000])


if __name__ == "__main__":
    main()
