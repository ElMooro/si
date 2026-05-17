"""ops/737 — verify the two new net-new engines end-to-end.

Invokes justhodl-construction-housing and justhodl-crypto-narratives,
reads their sidecars, confirms the data populated, checks both pages.
"""
import json, os, time, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=200, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 737, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "construction-housing + crypto-narratives verify"}


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response": body[:300]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:200]}


def read_s3(key):
    try:
        o = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(o["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:160]}


def page(url, marker):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/737"})
        with urllib.request.urlopen(req, timeout=25) as r:
            html = r.read().decode("utf-8", "replace")
            return {"status": r.status, "marker": marker in html}
    except Exception as e:
        return {"status": "error", "err": str(e)[:140]}


# ── construction-housing ──
report["invoke_housing"] = invoke("justhodl-construction-housing")
report["invoke_crypto"] = invoke("justhodl-crypto-narratives")
time.sleep(5)

ch = read_s3("data/construction-housing.json")
cn = read_s3("data/crypto-narratives.json")

if "_error" not in ch:
    report["housing"] = {
        "schema": ch.get("schema_version"), "regime": ch.get("regime"),
        "cycle_score": ch.get("cycle_score"),
        "n_resolved": ch.get("n_resolved"), "n_series": ch.get("n_series"),
        "n_signals": len(ch.get("signals", []))}
else:
    report["housing_error"] = ch["_error"]

if "_error" not in cn:
    report["crypto"] = {
        "schema": cn.get("schema_version"), "stance": cn.get("stance"),
        "breadth": cn.get("narrative_breadth_pct"),
        "n_categories": cn.get("n_categories"),
        "n_hot": len(cn.get("hot", [])), "n_cold": len(cn.get("cold", [])),
        "fg": cn.get("fear_greed"),
        "top_narrative": (cn.get("hot") or [{}])[0].get("name")}
else:
    report["crypto_error"] = cn["_error"]

report["page_housing"] = page("https://justhodl.ai/construction-housing.html",
                               "Housing &amp; Construction Cycle")
report["page_crypto"] = page("https://justhodl.ai/crypto-narratives.html",
                             "Crypto Narratives")

checks = {
    "housing_invoke_ok": report["invoke_housing"].get("status") == 200
                         and report["invoke_housing"].get("fn_error") is None,
    "housing_data_ok": "_error" not in ch and ch.get("schema_version") == "1.0"
                       and (ch.get("n_resolved") or 0) >= 6,
    "housing_page_live": report["page_housing"].get("marker") is True,
    "crypto_invoke_ok": report["invoke_crypto"].get("status") == 200
                        and report["invoke_crypto"].get("fn_error") is None,
    "crypto_data_ok": "_error" not in cn and cn.get("schema_version") == "1.0"
                      and (cn.get("n_categories") or 0) >= 10,
    "crypto_page_live": report["page_crypto"].get("marker") is True,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "VERIFIED — Housing Cycle + Crypto Narratives both live and populated"
    if report["all_pass"] else "REVIEW — see checks")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/737_new_engines_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/737_new_engines_verify.json")
