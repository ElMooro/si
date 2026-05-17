"""ops/761 — redeploy + reverify the Canary Grid after two fixes:
swiss_unemp now tries a fallback list of FRED series ids, and dbnomics.py
now sends a User-Agent header. Confirms 9/9 signals and a working fetcher.
"""
import io, json, os, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

report = {"ops": 761, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "redeploy + reverify Canary Grid (swiss_unemp + dbnomics fixes)"}

FN = "justhodl-canary-grid"
SRC_DIR = f"aws/lambdas/{FN}/source"
conf = json.load(open(f"aws/lambdas/{FN}/config.json"))

# ── rebuild + redeploy code ──
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for fn in sorted(os.listdir(SRC_DIR)):
        if fn.endswith(".py"):
            zi = zipfile.ZipInfo(fn)
            zi.external_attr = 0o644 << 16
            z.writestr(zi, open(f"{SRC_DIR}/{fn}", "r", encoding="utf-8").read())
try:
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    for _ in range(30):
        g = lam.get_function(FunctionName=FN)["Configuration"]
        if g.get("LastUpdateStatus") != "InProgress":
            break
        time.sleep(2)
    report["deploy"] = {"action": "updated"}
except Exception as e:
    report["deploy"] = {"err": str(e)[:280]}

# ── invoke ──
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": (r["Payload"].read().decode()[:280]
                                 if r.get("Payload") else "")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

# ── read output ──
data = None
try:
    data = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/canary-grid.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:240]

breakdown = []
if data:
    for s in data.get("signals", []):
        if s.get("available"):
            breakdown.append({"key": s["key"], "ok": True,
                              "fred": s.get("fred_series"), "stress": s.get("stress"),
                              "value": s.get("value"), "as_of": s.get("as_of")})
        else:
            breakdown.append({"key": s["key"], "ok": False,
                              "reason": s.get("reason")})
    report["output"] = {
        "early_warning_level": data.get("early_warning_level"),
        "band": data.get("band"), "headline": data.get("headline"),
        "n_available": data.get("n_available"), "n_total": data.get("n_total"),
        "sub_grids": {g: v.get("score")
                      for g, v in (data.get("sub_grids") or {}).items()},
    }
    report["signal_breakdown"] = breakdown

# ── dbnomics: function test + raw connectivity probe ──
db = {}
try:
    sys.path.insert(0, SRC_DIR)
    import dbnomics
    pts = dbnomics.fetch_series("AMECO/ZUTN/EA19.1.0.0.0.ZUTN")
    db["fetch_series_points"] = len(pts)
    db["sample"] = pts[-1] if pts else None
except Exception as e:
    db["fetch_err"] = str(e)[:240]
try:
    req = urllib.request.Request("https://api.db.nomics.world/v22/providers",
                                 headers={"User-Agent": "justhodl-ops/761"})
    with urllib.request.urlopen(req, timeout=25) as r:
        pj = json.loads(r.read())
    db["providers_probe"] = {"http": 200,
                             "n_providers": len((pj.get("providers") or {}).get("docs", []))}
except Exception as e:
    db["providers_probe"] = {"err": str(e)[:200]}
report["dbnomics"] = db

checks = {
    "redeploy_ok": "err" not in report.get("deploy", {}),
    "engine_runs_ok": report.get("invoke", {}).get("status") == 200
                      and not report.get("invoke", {}).get("fn_error"),
    "all_9_signals_live": bool(data) and data.get("n_available") == data.get("n_total")
                          and (data.get("n_total") or 0) == 9,
    "swiss_unemp_resolved": any(b["key"] == "swiss_unemp" and b.get("ok")
                                for b in breakdown),
    "composite_in_range": bool(data) and data.get("early_warning_level") is not None
                          and 0 <= data.get("early_warning_level", -1) <= 100,
    "dbnomics_fetcher_works": db.get("fetch_series_points", 0) > 10,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CANARY GRID PHASE 1 COMPLETE — {(data or {}).get('n_available')}/"
    f"{(data or {}).get('n_total')} signals live, level "
    f"{(data or {}).get('early_warning_level')} ({(data or {}).get('band')}), "
    f"dbnomics fetcher verified working."
    if report["all_pass"]
    else "REVIEW — see checks[] / signal_breakdown / dbnomics")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/761_canary_grid_reverify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/761_canary_grid_reverify.json")
