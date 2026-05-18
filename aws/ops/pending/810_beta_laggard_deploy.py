"""ops/810 — deploy + verify justhodl-beta-laggard AND probe FMP endpoints
for the next two builds (merger-arbitrage, ETF/CEF catch-up).

Audit-before-build: merger-arb needs an offer-price-per-share; ETF catch-up
needs holdings. This probe confirms exactly what FMP returns so the next
engines are built against real field names, not assumptions.
"""
import io, json, os, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-beta-laggard"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"

report = {"ops": 810, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy beta-laggard + probe FMP M&A / ETF endpoints"}

# ── deploy beta-laggard ──
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zb = buf.getvalue()
try:
    try:
        lam.get_function(FunctionName=FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zb)
        for _ in range(30):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Description=CONF["description"][:255])
        report["deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"][:255], Code={"ZipFile": zb})
        report["deploy"] = "created"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get(
                "LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

sch = CONF["schedule"]
try:
    events.put_rule(Name=sch["rule_name"], ScheduleExpression=sch["cron"],
                    State="ENABLED", Description=sch["description"])
    arn = events.describe_rule(Name=sch["rule_name"])["Arn"]
    try:
        lam.add_permission(FunctionName=FN,
                           StatementId=f"{sch['rule_name']}-invoke",
                           Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=arn)
    except lam.exceptions.ResourceConflictException:
        pass
    fa = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    events.put_targets(Rule=sch["rule_name"],
                       Targets=[{"Id": "1", "Arn": fa}])
    report["schedule"] = "wired"
except Exception as e:
    report["schedule"] = f"err {str(e)[:140]}"

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(
                            r["Payload"].read() or b"{}").get("body")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)
ob = {}
try:
    ob = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/beta-laggards.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:160]
report["beta_laggard"] = {
    "ok": ob.get("ok"), "headline": ob.get("headline"),
    "n_working_sectors": ob.get("n_working_sectors"),
    "n_candidates": ob.get("n_candidates"),
    "universe": ob.get("universe_with_returns"),
    "top3": [{"sym": c.get("symbol"), "sector": c.get("sector"),
              "leader": c.get("leader_symbol"),
              "gap": c.get("gap_vs_leader_pp"),
              "score": c.get("catch_up_score"),
              "upside": c.get("upside_pct")}
             for c in (ob.get("top_candidates") or [])[:3]],
}


# ── probe FMP endpoints for the next two engines ──
def probe(label, url):
    try:
        with urllib.request.urlopen(url + f"&apikey={FMP}"
                                    if "?" in url else url + f"?apikey={FMP}",
                                    timeout=20) as r:
            d = json.loads(r.read())
        sample = d[0] if isinstance(d, list) and d else d
        return {"ok": True, "n": len(d) if isinstance(d, list) else 1,
                "keys": sorted(sample.keys())[:40]
                if isinstance(sample, dict) else None,
                "sample": {k: sample[k] for k in list(sample)[:12]}
                if isinstance(sample, dict) else str(sample)[:200]}
    except Exception as e:
        return {"ok": False, "err": f"{type(e).__name__}: {str(e)[:160]}"}


report["fmp_probes"] = {
    "mergers_acquisitions_latest": probe(
        "ma", f"{BASE}/mergers-acquisitions-latest?page=0&limit=5"),
    "ma_search": probe(
        "ma2", f"{BASE}/mergers-acquisitions-search?name=corp"),
    "etf_holdings_XLK": probe(
        "etf", f"{BASE}/etf/holdings?symbol=XLK"),
    "etf_info_XLK": probe("etfi", f"{BASE}/etf/info?symbol=XLK"),
}

checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(
        ("created", "updated")),
    "schedule_wired": report.get("schedule") == "wired",
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "beta_laggard_output_ok": ob.get("ok") is True,
    "beta_laggard_has_candidates": (ob.get("n_candidates") or 0) >= 1,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"BETA-LAGGARD LIVE — {ob.get('n_candidates')} catch-up candidates "
    f"across {ob.get('n_working_sectors')} working sectors. FMP probes "
    "captured for the merger-arb + ETF builds (see fmp_probes)."
    if checks["deploy_ok"] and checks["invoke_ok"]
    else "REVIEW — see checks[] / beta_laggard")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/810_beta_laggard_deploy.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/810_beta_laggard_deploy.json")
