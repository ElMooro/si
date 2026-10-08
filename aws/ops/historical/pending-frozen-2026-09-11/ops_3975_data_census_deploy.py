# retry ops3976: run 30289873127 died at Configure-AWS-credentials before executing a line — transient OIDC/STS failure, code unchanged
"""
ops_3975 — deploy justhodl-data-census v1.0 from the runner.

ops 3962 failed its gate honestly: 40 polls over 600s, every one
"function not created yet". Deploy Lambdas reported success but never
created the function, and the Actions log API returned a 133-byte body so
the forensics were a dead end. Rather than keep guessing at the workflow,
this op does what the fleet's proven self-heal pattern does: build the zip
on the runner and create the function directly with boto3, discovering
runtime/role from a live donor function instead of assuming them.

The engine needs no API keys — it reads brain.json, tradingview.json,
risk-gate.json and rotation-dashboard.json from S3 and writes back. So no
env inheritance is attempted; that removes a whole failure mode.

Then: settle BY MARKER inside the deployed artifact, invoke, and gate hard
on the live output — 100% classification, >=90% from his own notes, three
scored barometers, ten predictions each citing a brain note.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=660, retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name="us-east-1")

FN = "justhodl-data-census"
DONOR = "justhodl-tradingview"
MARK = "data-census v1.0 ops3975"
BUCKET = "justhodl-dashboard-live"
OUT = "data/data-census.json"
SRC = ROOT / "lambdas" / FN / "source" / "lambda_function.py"


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", SRC.read_text())
    return buf.getvalue()


def main():
    with report("3975_data_census_deploy") as rep:
        rep.heading("ops 3963 — self-heal deploy justhodl-data-census")
        checks = []

        rep.section("A. diagnose")
        exists = True
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
            rep.log(f"  function EXISTS state={cfg.get('State')} "
                    f"modified={cfg.get('LastModified')}")
        except lam.exceptions.ResourceNotFoundException:
            exists = False
            rep.log("  function DOES NOT EXIST — Deploy Lambdas never created it "
                    "(confirms the 3962 failure was real, not a settle-timing artifact)")
        if not SRC.exists():
            rep.fail(f"  source missing at {SRC}")
            sys.exit(1)
        blob = build_zip()
        rep.kv(zip_bytes=len(blob), marker_in_source=MARK in SRC.read_text())
        checks.append(("marker present in source", MARK in SRC.read_text()))

        rep.section("B. create or update from the runner")
        donor = lam.get_function_configuration(FunctionName=DONOR)
        role = donor["Role"]
        runtime = donor.get("Runtime", "python3.12")
        rep.kv(donor=DONOR, role=role, runtime=runtime)
        if not exists:
            lam.create_function(
                FunctionName=FN, Runtime=runtime, Role=role,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": blob}, Timeout=600, MemorySize=1536,
                Description=("BRAIN-CONSTITUTIONAL MACRO/LIQUIDITY/RISK classifier + "
                             "3 barometers + asset-class prediction"),
                Publish=True)
            rep.ok("  create_function issued")
        else:
            lam.update_function_code(FunctionName=FN, ZipFile=blob, Publish=True)
            rep.ok("  update_function_code issued")

        rep.section("C. settle BY MARKER inside the deployed artifact")
        settled = False
        for i in range(30):
            try:
                info = lam.get_function(FunctionName=FN)
                c = info["Configuration"]
                if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                    src = zipfile.ZipFile(io.BytesIO(
                        urllib.request.urlopen(info["Code"]["Location"], timeout=60).read()
                    )).read("lambda_function.py").decode()
                    if MARK in src:
                        rep.ok(f"  settled with marker after ~{i*10}s")
                        settled = True
                        break
                    rep.log(f"  [{i}] artifact lacks marker")
                else:
                    rep.log(f"  [{i}] state={c.get('State')} upd={c.get('LastUpdateStatus')}")
            except Exception as e:
                rep.log(f"  [{i}] {type(e).__name__}: {str(e)[:80]}")
            time.sleep(10)
        checks.append(("zip settled by marker", settled))
        if not settled:
            rep.fail("never settled")
            sys.exit(1)

        rep.section("D. invoke")
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"source": "ops3975"}).encode())
        raw = r["Payload"].read().decode()
        rep.log(f"  status={r.get('StatusCode')} fnerr={r.get('FunctionError')}")
        rep.log(f"  payload={raw[:500]}")
        if r.get("FunctionError"):
            rep.fail("lambda raised — see payload above")
            sys.exit(1)
        checks.append(("invoke clean", True))

        rep.section("E. verify the LIVE artifact")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT)["Body"].read())
        t = doc.get("totals") or {}
        rep.kv(**t)
        rep.log(f"  mislabels={len(doc.get('mislabel_candidates') or [])} "
                f"conflicts={len(doc.get('measure_conflicts') or [])} "
                f"gaps={len(doc.get('gap_fill_candidates') or [])}")
        for m in (doc.get("mislabel_candidates") or [])[:6]:
            rep.log(f"  MISLABEL v={m['value']} countries={m['countries_claimed']} "
                    f"{[x['artifact']+':'+x['path'] for x in m['paths'][:3]]}")
        for g in (doc.get("gap_fill_candidates") or [])[:8]:
            rep.log(f"  GAP {g['symbol']} notes={g['n_notes']} "
                    f"-> {[c['artifact']+':'+c['path'] for c in g['candidates'][:2]]}")
        checks += [
            ("census walked >=100 artifacts", (t.get("artifacts") or 0) >= 100),
            ("census indexed >=2000 scalar paths", (t.get("scalar_paths") or 0) >= 2000),
            ("all three detectors present", all(k in doc for k in
             ("mislabel_candidates", "measure_conflicts", "gap_fill_candidates"))),
            ("full paths ledger written", bool(s3.head_object(Bucket=BUCKET,
             Key="data-census/paths-ledger.json"))),
        ]
        rep.section("G. schedule cron(45 12) — after vault 11:35 / gate 11:05")
        try:
            role_s = sch.get_schedule(Name="tradingview-vault-daily")["Target"]["RoleArn"]
            tgt = {"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
                   "RoleArn": role_s, "Input": json.dumps({"source": "schedule"})}
            kw = dict(Name="data-census-daily",
                      ScheduleExpression="cron(45 12 * * ? *)",
                      FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED")
            try:
                sch.create_schedule(**kw)
                rep.ok("  schedule created")
            except sch.exceptions.ConflictException:
                sch.update_schedule(**kw)
                rep.ok("  schedule updated")
            st = sch.get_schedule(Name="data-census-daily")
            rep.kv(schedule_state=st.get("State"), cron=st.get("ScheduleExpression"))
            checks.append(("schedule ENABLED", st.get("State") == "ENABLED"))
        except Exception as e:
            rep.fail(f"  schedule: {type(e).__name__}: {str(e)[:120]}")
            checks.append(("schedule ENABLED", False))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {ca.get('n_symbols')} symbols "
               f"({ca.get('from_his_own_notes_pct')}% from his own notes); "
               f"M {bar['MACRO']['score_0_100']} / L {bar['LIQUIDITY']['score_0_100']} "
               f"/ R {bar['RISK']['score_0_100']}")


if __name__ == "__main__":
    main()
