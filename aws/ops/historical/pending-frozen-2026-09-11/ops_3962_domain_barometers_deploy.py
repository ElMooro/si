"""
ops_3962 — ship justhodl-domain-barometers v1.0.

Khalid: sort the vault by macro / liquidity / risk, a barometer for each, and
a prediction per asset class — with the classification coming from his brain
notes, and every indicator covered.

Probes 3959-3961 established the method empirically (100% coverage, 95.9%
from his own notes, evidence terms that are real doctrine vocabulary). This
op deploys the engine, settles the zip BY MARKER (State==Active returns
instantly when deploy-lambdas has not started — that false-pass burned ops
3830), invokes, and gates hard on the live artifact.

Also schedules it at cron(20 12) — after the vault (11:35) and the gate
(11:05) so both inputs are fresh when it reads them.
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
MARK = "domain-barometers v1.0 ops3962"
BUCKET = "justhodl-dashboard-live"
OUT = "data/domain-barometers.json"


def settle_by_marker(rep, tries=40):
    for i in range(tries):
        try:
            info = lam.get_function(FunctionName=FN)
            cfg = info["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                url = info["Code"]["Location"]
                blob = urllib.request.urlopen(url, timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(blob)).read("lambda_function.py").decode()
                if MARK in src:
                    rep.ok(f"  zip settled with marker after ~{i*15}s")
                    return True
                rep.log(f"  [{i}] deployed artifact lacks marker — waiting")
        except lam.exceptions.ResourceNotFoundException:
            rep.log(f"  [{i}] function not created yet")
        except Exception as e:
            rep.log(f"  [{i}] {type(e).__name__}: {str(e)[:90]}")
        time.sleep(15)
    return False


def main():
    with report("3962_domain_barometers_deploy") as rep:
        rep.heading("ops 3962 — justhodl-domain-barometers v1.0")
        checks = []

        rep.section("A. zip-settle by marker (never trust State==Active alone)")
        if not settle_by_marker(rep):
            rep.fail("zip never settled with the new marker")
            sys.exit(1)
        checks.append(("zip settled by marker", True))

        rep.section("B. invoke")
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"source": "ops3962"}).encode())
        raw = r["Payload"].read().decode()
        rep.log(f"  status={r.get('StatusCode')} fnerr={r.get('FunctionError')}")
        rep.log(f"  payload={raw[:400]}")
        checks.append(("invoke clean", not r.get("FunctionError")))
        if r.get("FunctionError"):
            rep.fail("lambda raised")
            sys.exit(1)

        rep.section("C. verify the LIVE artifact")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT)["Body"].read())
        ca = doc.get("classification_audit") or {}
        bar = doc.get("barometers") or {}
        pred = (doc.get("predictions") or {}).get("asset_classes") or {}
        syms = doc.get("symbols") or []

        rep.kv(n_symbols=ca.get("n_symbols"),
               own_notes_pct=ca.get("from_his_own_notes_pct"),
               tiers=json.dumps(ca.get("tier_counts")),
               domains=json.dumps(ca.get("domain_counts")),
               confidence=json.dumps(ca.get("confidence_counts")))
        rep.log(f"  learned category priors: {json.dumps(ca.get('learned_category_priors'))}")

        for d in ("MACRO", "LIQUIDITY", "RISK"):
            b = bar.get(d) or {}
            rep.log(f"  {d:9s} score={b.get('score_0_100')} state={b.get('state')} "
                    f"gate={b.get('gate_component')} breadth={b.get('breadth_component')} "
                    f"drivers={b.get('n_drivers_live')} (+{b.get('n_favourable')}/"
                    f"-{b.get('n_adverse')}) disagree={b.get('disagreement')}")
            for m in (b.get("worst_movers") or [])[:2]:
                rep.log(f"      worst: {m['symbol']} {m['chg_pct']}% pol={m['polarity']} "
                        f"({m['polarity_basis']})")

        rep.section("D. predictions")
        for cls, p in sorted(pred.items(), key=lambda kv: -abs(kv[1].get("score") or 0)):
            rep.log(f"  {cls:24s} {p['direction']:13s} score={p['score']:+.3f} "
                    f"conv={p['conviction']:6s} driver={p['dominant_driver']}")

        rep.section("E. gates")
        unclassified = [x["symbol"] for x in syms if x.get("domain") not in
                        ("MACRO", "LIQUIDITY", "RISK")]
        t6 = (ca.get("tier_counts") or {}).get("T6", 0)
        checks.append(("every symbol classified into one of the 3 domains", not unclassified))
        checks.append(("no T6 backstop", t6 == 0))
        checks.append((">=90% classified from his own notes",
                       (ca.get("from_his_own_notes_pct") or 0) >= 90))
        checks.append(("all three barometers scored",
                       all((bar.get(d) or {}).get("score_0_100") is not None
                           for d in ("MACRO", "LIQUIDITY", "RISK"))))
        checks.append(("each barometer has live drivers",
                       all((bar.get(d) or {}).get("n_drivers_live", 0) >= 5
                           for d in ("MACRO", "LIQUIDITY", "RISK"))))
        checks.append(("predictions cover all 10 asset classes", len(pred) == 10))
        checks.append(("every prediction cites a brain note",
                       all(p.get("brain_basis") for p in pred.values())))
        checks.append(("every symbol carries evidence",
                       all(x.get("evidence") for x in syms)))
        checks.append(("grading is honest on day one",
                       (doc.get("grading") or {}).get("status") in ("ACCRUING", "GRADING")))
        if unclassified:
            rep.log(f"  unclassified: {unclassified[:20]}")

        rep.section("F. schedule cron(20 12) — after vault 11:35 and gate 11:05")
        try:
            role = sch.get_schedule(Name="tradingview-vault-daily")["Target"]["RoleArn"]
            try:
                sch.create_schedule(
                    Name="domain-barometers-daily",
                    ScheduleExpression="cron(20 12 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
                            "RoleArn": role,
                            "Input": json.dumps({"source": "schedule"})},
                    State="ENABLED")
                rep.ok("  schedule created")
            except sch.exceptions.ConflictException:
                sch.update_schedule(
                    Name="domain-barometers-daily",
                    ScheduleExpression="cron(20 12 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
                            "RoleArn": role,
                            "Input": json.dumps({"source": "schedule"})},
                    State="ENABLED")
                rep.ok("  schedule updated")
            st = sch.get_schedule(Name="domain-barometers-daily")
            rep.kv(schedule_state=st.get("State"), cron=st.get("ScheduleExpression"))
            checks.append(("schedule ENABLED", st.get("State") == "ENABLED"))
        except Exception as e:
            rep.fail(f"  schedule failed: {type(e).__name__}: {str(e)[:120]}")
            checks.append(("schedule ENABLED", False))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {ca.get('n_symbols')} symbols classified "
               f"({ca.get('from_his_own_notes_pct')}% from his own notes); "
               f"MACRO {bar['MACRO']['score_0_100']} / LIQUIDITY "
               f"{bar['LIQUIDITY']['score_0_100']} / RISK {bar['RISK']['score_0_100']}")


if __name__ == "__main__":
    main()
