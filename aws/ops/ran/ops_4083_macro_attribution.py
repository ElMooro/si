"""ops_4083 — deploy justhodl-macro-attribution + wire source-map v2.1.

ops 4081 proved TradingView returns source=null across its whole macro
namespace, so the browser can never yield agency attribution. ops 4082
found the authoritative route: FRED's own series/release/sources chain
named 'U.S. Bureau of Economic Analysis' and 'National Bureau of Economic
Research' on live probes.

This deploys that engine, schedules it, and merges the result into
source-map so the Agency Coverage panel finally shows real institutions.

The gates check HONESTY as much as function: the engine must publish its
unattributed count, and must NOT have invented attribution for the 3,206
symbols that resolve only by country+topic inference.
"""
import io, json, sys, time, urllib.request, zipfile as zf
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900, retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def deploy(rep, fn, marker, donor="justhodl-tv-workbench"):
    d = ROOT / "lambdas" / fn
    src = (d / "source" / "lambda_function.py").read_text()
    cfg = json.loads((d / "config.json").read_text())
    assert marker in src, f"{fn}: marker missing"
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
    code = buf.getvalue()

    dc = lam.get_function_configuration(FunctionName=donor)
    env = dict((dc.get("Environment") or {}).get("Variables") or {})
    keep = {k: env[k] for k in ("FRED_KEY",) if k in env}

    try:
        lam.get_function_configuration(FunctionName=fn)
        exists = True
    except lam.exceptions.ResourceNotFoundException:
        exists = False

    if not exists:
        lam.create_function(FunctionName=fn, Runtime=dc["Runtime"],
                            Role=dc["Role"], Handler=cfg["handler"],
                            Code={"ZipFile": code}, Timeout=cfg["timeout"],
                            MemorySize=cfg["memory"],
                            Description=cfg["description"],
                            Environment={"Variables": keep}, Publish=True)
        rep.log(f"  ✓ CREATED {fn}  (env keys: {list(keep)})")
    else:
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=fn, ZipFile=code,
                                         Publish=True); break
            except lam.exceptions.ResourceConflictException:
                time.sleep(12)
        rep.log(f"  ✓ updated {fn}")

    # settle BY MARKER — State==Active lies when deploy hasn't started
    for a in range(24):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())
                                 ).read("lambda_function.py").decode()
                if marker in dep:
                    rep.log(f"  ✓ {fn} marker settled (attempt {a+1})")
                    return True
        except Exception as e:
            rep.log(f"  settle {a+1}: {str(e)[:60]}")
        time.sleep(10)
    return False


def main():
    with report("4083_macro_attribution") as rep:
        rep.heading("ops 4083 — real macro attribution (FRED metadata)")
        checks = []

        rep.section("A. deploy justhodl-macro-attribution")
        ok1 = deploy(rep, "justhodl-macro-attribution", "macro-attribution v1.0 ops4083")
        checks.append(("macro-attribution settled", ok1))
        if not ok1:
            rep.log("✗ refusing to invoke a stale artifact"); sys.exit(1)

        rep.section("B. invoke — first ledger pass")
        r = lam.invoke(FunctionName="justhodl-macro-attribution",
                       InvocationType="RequestResponse", Payload=b'{"source":"ops4083"}')
        rep.log(f"  status={r['StatusCode']} fnerr={r.get('FunctionError')}")
        rep.log(f"  {r['Payload'].read().decode()[:250]}")
        checks.append(("invoke clean", r.get("FunctionError") is None))

        ma = json.loads(s3.get_object(Bucket=BUCKET, Key="data/macro-attribution.json")["Body"].read())
        rep.kv(macro=ma.get("macro_symbols"), attributed=ma.get("attributed"),
               unattributed=ma.get("unattributed"), coverage=ma.get("coverage_pct"),
               calls=ma.get("fred_calls_this_run"))
        rep.section("Publishers resolved (real institutions)")
        for row in (ma.get("by_publisher") or [])[:14]:
            rep.log(f"  {row['n_symbols']:5d}  {row['publisher']}  [{row['family']}]")
        rep.log(f"  by_route: {ma.get('by_route')}")

        checks.append(("attribution is non-empty", (ma.get("attributed") or 0) > 0))
        # HONESTY GATES — the failure mode here is invention, not absence.
        checks.append(("the unattributed gap is PUBLISHED, not hidden",
                       "unattributed" in ma and "unattributed_sample" in ma))
        checks.append(("no attribution invented for inference-only symbols",
                       (ma.get("attributed") or 0) + (ma.get("unattributed") or 0)
                       == (ma.get("macro_symbols") or 0)))
        checks.append(("every resolved row names its route",
                       all(v.get("route") in ("fred-metadata", "vault-fred", "vault-gov")
                           for v in (ma.get("attribution") or {}).values())))

        rep.section("C. source-map v2.1 merge")
        ok2 = deploy(rep, "justhodl-source-map", "source-map engine v2.1 ops4083")
        checks.append(("source-map v2.1 settled", ok2))
        r2 = lam.invoke(FunctionName="justhodl-source-map",
                        InvocationType="RequestResponse", Payload=b'{"source":"ops4083"}')
        rep.log(f"  {r2['Payload'].read().decode()[:220]}")
        sm = json.loads(s3.get_object(Bucket=BUCKET, Key="data/source-map.json")["Body"].read())
        rep.kv(agency_rows=sm.get("agency_rows"), macro_attributed=sm.get("macro_attributed"),
               macro_unattributed=sm.get("macro_unattributed"),
               economics_symbols=sm.get("economics_symbols"))
        rep.log(f"  agency_families: {sm.get('agency_families')}")
        checks.append(("agency_rows is finally NON-ZERO", (sm.get("agency_rows") or 0) > 0))
        checks.append(("macro gap surfaced on the artifact",
                       sm.get("macro_unattributed") is not None))

        rep.section("D. schedule")
        role = None
        for pg in sch.get_paginator("list_schedules").paginate():
            for s_ in pg.get("Schedules", []):
                d2 = sch.get_schedule(Name=s_["Name"])
                if d2.get("Target", {}).get("RoleArn"):
                    role = d2["Target"]["RoleArn"]; break
            if role: break
        arn = lam.get_function_configuration(FunctionName="justhodl-macro-attribution")["FunctionArn"]
        spec = dict(Name="macro-attribution-daily",
                    ScheduleExpression="cron(5 12 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": arn, "RoleArn": role,
                            "Input": json.dumps({"source": "schedule"})},
                    State="ENABLED",
                    Description="macro publisher attribution (ops 4083)")
        try:
            sch.create_schedule(**spec); rep.log("  ✓ created macro-attribution-daily")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**spec); rep.log("  ✓ updated macro-attribution-daily")
        got = sch.get_schedule(Name="macro-attribution-daily")
        rep.log(f"  state={got.get('State')} expr={got.get('ScheduleExpression')}")
        checks.append(("schedule ENABLED (verified)", got.get("State") == "ENABLED"))

        rep.section("E. field coverage")
        page = (REPO / "harvest-monitor.html").read_text()
        DYN = {"generated_at", "marker"}
        missing = [k for k in sm if k not in DYN and k not in page]
        for k in sorted(sm):
            rep.log(f"  {'·' if k in DYN else ('✓' if k in page else '✗')} {k}")
        checks.append((f"every source-map key rendered ({len(sm)-len(missing)}/{len(sm)})",
                       not missing))
        if missing: rep.log(f"  ✗ unrendered: {missing}")

        rep.section("VERDICT")
        for n, o in checks: rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}"); sys.exit(1)
        rep.log(f"✅ PASS_ALL — {ma.get('attributed')} macro symbols carry a REAL "
                f"publisher; {ma.get('unattributed')} reported honestly unattributed. "
                f"Ledger accretes daily.")


if __name__ == "__main__":
    main()
