"""ops/760 — deploy + verify the Canary Grid (Global Early-Warning Grid).

Deploys justhodl-canary-grid directly (create-if-missing), wires its daily
schedule, invokes it, then bug-checks the output: composite in range, all
four sub-grids present, every signal well-formed, and enough FRED series
resolved (a wrong series id shows as an unavailable signal here). Also
smoke-tests the bundled dbnomics.py fetcher against a known series.
"""
import io, json, os, sys, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
sts = boto3.client("sts", region_name="us-east-1", config=cfg)

report = {"ops": 760, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "deploy + verify Canary Grid (Global Early-Warning Grid)"}

FN = "justhodl-canary-grid"
SRC_DIR = f"aws/lambdas/{FN}/source"
conf = json.load(open(f"aws/lambdas/{FN}/config.json"))
acct = sts.get_caller_identity()["Account"]

# ── build zip with every .py in source/ (lambda_function.py + dbnomics.py) ──
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for fn in sorted(os.listdir(SRC_DIR)):
        if fn.endswith(".py"):
            zi = zipfile.ZipInfo(fn)
            zi.external_attr = 0o644 << 16
            z.writestr(zi, open(f"{SRC_DIR}/{fn}", "r", encoding="utf-8").read())
zip_bytes = buf.getvalue()
report["zip_bytes"] = len(zip_bytes)


def wait_active(name, tries=30):
    for _ in range(tries):
        try:
            g = lam.get_function(FunctionName=name)["Configuration"]
            if g.get("State") == "Active" and g.get("LastUpdateStatus") != "InProgress":
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


exists = True
try:
    lam.get_function(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException:
    exists = False
try:
    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        wait_active(FN)
        lam.update_function_configuration(
            FunctionName=FN, Runtime=conf["runtime"], Handler=conf["handler"],
            Timeout=conf["timeout"], MemorySize=conf["memory"],
            Environment={"Variables": conf.get("environment", {})},
            Description=conf["description"])
        report["deploy"] = {"action": "updated"}
    else:
        lam.create_function(
            FunctionName=FN, Runtime=conf["runtime"], Role=conf["role"],
            Handler=conf["handler"], Code={"ZipFile": zip_bytes},
            Timeout=conf["timeout"], MemorySize=conf["memory"],
            Architectures=conf.get("architectures", ["x86_64"]),
            Environment={"Variables": conf.get("environment", {})},
            Description=conf["description"], Publish=True)
        report["deploy"] = {"action": "created"}
    wait_active(FN)
except Exception as e:
    report["deploy"] = {"err": str(e)[:300]}

fn_arn = f"arn:aws:lambda:us-east-1:{acct}:function:{FN}"

# ── schedule ──
sch = conf.get("schedule", {})
try:
    rule_arn = ev.put_rule(Name=sch["rule_name"], ScheduleExpression=sch["cron"],
                           State="ENABLED",
                           Description=sch.get("description", ""))["RuleArn"]
    try:
        lam.add_permission(FunctionName=FN, StatementId="evb-canary-grid-daily",
                           Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=rule_arn)
    except lam.exceptions.ResourceConflictException:
        pass
    ev.put_targets(Rule=sch["rule_name"], Targets=[{"Id": "1", "Arn": fn_arn}])
    report["schedule"] = {"rule": sch["rule_name"], "cron": sch["cron"],
                          "wired": True}
except Exception as e:
    report["schedule"] = {"err": str(e)[:240]}

# ── invoke ──
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": (r["Payload"].read().decode()[:300]
                                 if r.get("Payload") else "")}
except Exception as e:
    report["invoke"] = {"err": str(e)[:240]}

# ── read + bug-check output ──
data = None
try:
    data = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/canary-grid.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:240]

signal_breakdown, bug_notes = [], []
if data:
    sigs = data.get("signals", [])
    for s in sigs:
        row = {"key": s.get("key"), "grid": s.get("sub_grid"),
               "available": s.get("available")}
        if s.get("available"):
            row["stress"] = s.get("stress")
            row["value"] = s.get("value")
            row["as_of"] = s.get("as_of")
            st = s.get("stress")
            if not isinstance(st, (int, float)) or not (0 <= st <= 100):
                bug_notes.append(f"{s.get('key')}: stress out of range ({st})")
        else:
            row["reason"] = s.get("reason")
        if not s.get("key") or not s.get("sub_grid"):
            bug_notes.append(f"signal missing key/sub_grid: {s}")
        signal_breakdown.append(row)
    lvl = data.get("early_warning_level")
    if lvl is not None and not (0 <= lvl <= 100):
        bug_notes.append(f"early_warning_level out of range ({lvl})")
    report["output"] = {
        "schema_version": data.get("schema_version"),
        "early_warning_level": lvl, "band": data.get("band"),
        "headline": data.get("headline"),
        "n_available": data.get("n_available"), "n_total": data.get("n_total"),
        "sub_grids": {g: {"score": v.get("score"), "band": v.get("band"),
                          "n": v.get("n_signals")}
                      for g, v in (data.get("sub_grids") or {}).items()},
        "top_deteriorating": [t.get("key") for t in
                              data.get("top_deteriorating", [])],
    }
    report["signal_breakdown"] = signal_breakdown
report["bug_notes"] = bug_notes

# ── dbnomics.py smoke test (known series from DBnomics docs) ──
try:
    sys.path.insert(0, SRC_DIR)
    import dbnomics
    pts = dbnomics.fetch_series("AMECO/ZUTN/EA19.1.0.0.0.ZUTN")
    report["dbnomics_smoke"] = {"series": "AMECO/ZUTN/EA19.1.0.0.0.ZUTN",
                                "points": len(pts),
                                "sample": pts[-1] if pts else None}
except Exception as e:
    report["dbnomics_smoke"] = {"err": str(e)[:240]}

sg = (data or {}).get("sub_grids") or {}
checks = {
    "canary_deployed": "err" not in report.get("deploy", {}),
    "schedule_wired": report.get("schedule", {}).get("wired") is True,
    "engine_runs_ok": report.get("invoke", {}).get("status") == 200
                      and not report.get("invoke", {}).get("fn_error"),
    "output_schema_ok": bool(data) and data.get("schema_version") == "1.0"
                        and bool(data.get("signals")),
    "composite_in_range": bool(data) and data.get("early_warning_level") is not None
                          and 0 <= data.get("early_warning_level", -1) <= 100,
    "all_4_subgrids_present": len(sg) == 4,
    "enough_signals_live": (data or {}).get("n_available", 0) >= 6,
    "no_bugs_found": len(bug_notes) == 0,
    "dbnomics_fetcher_works": report.get("dbnomics_smoke", {}).get("points", 0) > 10,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CANARY GRID LIVE — early-warning level {(data or {}).get('early_warning_level')} "
    f"({(data or {}).get('band')}), {(data or {}).get('n_available')}/"
    f"{(data or {}).get('n_total')} signals live, 4 sub-grids, dbnomics fetcher "
    f"verified. No bugs found."
    if report["all_pass"]
    else "REVIEW — see checks[] / bug_notes / signal_breakdown")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/760_canary_grid_deploy_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/760_canary_grid_deploy_verify.json")
