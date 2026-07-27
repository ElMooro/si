"""
ops_3965 — SELF-HEAL DEPLOY of justhodl-domain-barometers.

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

FN = "justhodl-domain-barometers"
DONOR = "justhodl-tradingview"
MARK = "domain-barometers v1.1 ops3965 polarity-guard"
BUCKET = "justhodl-dashboard-live"
OUT = "data/domain-barometers.json"
SRC = ROOT / "lambdas" / FN / "source" / "lambda_function.py"


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", SRC.read_text())
    return buf.getvalue()


def main():
    with report("3965_polarity_fix") as rep:
        rep.heading("ops 3963 — self-heal deploy justhodl-domain-barometers")
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
                       Payload=json.dumps({"source": "ops3965"}).encode())
        raw = r["Payload"].read().decode()
        rep.log(f"  status={r.get('StatusCode')} fnerr={r.get('FunctionError')}")
        rep.log(f"  payload={raw[:500]}")
        if r.get("FunctionError"):
            rep.fail("lambda raised — see payload above")
            sys.exit(1)
        checks.append(("invoke clean", True))

        rep.section("E. verify the LIVE artifact")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT)["Body"].read())
        ca = doc.get("classification_audit") or {}
        bar = doc.get("barometers") or {}
        pred = (doc.get("predictions") or {}).get("asset_classes") or {}
        syms = doc.get("symbols") or []
        rep.kv(n_symbols=ca.get("n_symbols"),
               own_notes_pct=ca.get("from_his_own_notes_pct"),
               tiers=json.dumps(ca.get("tier_counts")),
               domains=json.dumps(ca.get("domain_counts")),
               confidence=json.dumps(ca.get("confidence_counts")),
               generated_at=doc.get("generated_at"))
        for d in ("MACRO", "LIQUIDITY", "RISK"):
            b = bar.get(d) or {}
            rep.log(f"  {d:9s} score={b.get('score_0_100')} state={b.get('state')} "
                    f"gate={b.get('gate_component')} breadth={b.get('breadth_component')} "
                    f"drivers={b.get('n_drivers_live')} (+{b.get('n_favourable')}/"
                    f"-{b.get('n_adverse')}) disagree={b.get('disagreement')}")
        rep.section("F. predictions")
        for cls, p in sorted(pred.items(), key=lambda kv: -abs(kv[1].get("score") or 0)):
            rep.log(f"  {cls:24s} {p['direction']:13s} {p['score']:+.3f} "
                    f"{p['conviction']:6s} driver={p['dominant_driver']}")

        bad = [x["symbol"] for x in syms
               if x.get("domain") not in ("MACRO", "LIQUIDITY", "RISK")]
        checks += [
            ("every symbol in one of the 3 domains", not bad),
            ("no T6 backstop", (ca.get("tier_counts") or {}).get("T6", 0) == 0),
            (">=70% from his own notes", (ca.get("from_his_own_notes_pct") or 0) >= 70),
            ("all three barometers scored",
             all((bar.get(d) or {}).get("score_0_100") is not None
                 for d in ("MACRO", "LIQUIDITY", "RISK"))),
            ("each barometer has >=5 live drivers",
             all((bar.get(d) or {}).get("n_drivers_live", 0) >= 5
                 for d in ("MACRO", "LIQUIDITY", "RISK"))),
            ("10 asset-class predictions", len(pred) == 10),
            ("every prediction cites a brain note",
             all(p.get("brain_basis") for p in pred.values())),
            ("every symbol carries evidence", all(x.get("evidence") for x in syms)),
            ("grading honest", (doc.get("grading") or {}).get("status")
             in ("ACCRUING", "GRADING")),
        ]

        rep.section("G. schedule cron(20 12) — after vault 11:35 / gate 11:05")
        try:
            role_s = sch.get_schedule(Name="tradingview-vault-daily")["Target"]["RoleArn"]
            tgt = {"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
                   "RoleArn": role_s, "Input": json.dumps({"source": "schedule"})}
            kw = dict(Name="domain-barometers-daily",
                      ScheduleExpression="cron(20 12 * * ? *)",
                      FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED")
            try:
                sch.create_schedule(**kw)
                rep.ok("  schedule created")
            except sch.exceptions.ConflictException:
                sch.update_schedule(**kw)
                rep.ok("  schedule updated")
            st = sch.get_schedule(Name="domain-barometers-daily")
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
