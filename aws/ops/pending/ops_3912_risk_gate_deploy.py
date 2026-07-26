"""
ops_3912 — DEPLOY justhodl-risk-gate v1.0 (BRAIN-CONSTITUTIONAL Master Risk
Gate) and validate it the way the brain demands: event-study, with the
October-2025 RRP-drain window replayed explicitly. Also ensures the daily
Scheduler schedule (classic EventBridge cap is saturated fleet-wide).

Gates:
  1 deploy settles (marker in live zip) and invoke succeeds
  2 output carries a valid posture + sizing_multiplier + brain_constitution
    block with note-ID citations (traceability = Khalid's directive)
  3 event study ran on real replayed history with >= 2 flips detected
  4 October 2025 window: reports the RRP minimum (should be near zero per
    his call) and the posture day-counts through the window
  5 Scheduler schedule exists/created
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

FN = "justhodl-risk-gate"
BUCKET = "justhodl-dashboard-live"
KEY = "data/risk-gate.json"
MARKER = "risk-gate v1.0 BRAIN-CONSTITUTIONAL"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=580, retries={"max_attempts": 0}))
sched = boto3.client("scheduler", region_name="us-east-1")


def main():
    with report("3912_risk_gate_deploy") as rep:
        rep.heading("ops 3912 — Master Risk Gate deploy + brain-doctrine event-study validation")
        checks = []

        rep.section("1. zip-settle by marker (deploy-lambdas runs on this same push)")
        settled = False
        for attempt in range(1, 41):
            try:
                fn = lam.get_function(FunctionName=FN)
                loc = fn["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if MARKER in src:
                    settled = True
                    rep.ok(f"  marker live on attempt {attempt}")
                    break
            except lam.exceptions.ResourceNotFoundException:
                rep.log(f"  attempt {attempt}: function not created yet")
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:80]}")
            time.sleep(15)
        checks.append(("deploy settled with new-code marker", settled))
        if not settled:
            rep.fail("deploy never landed — cannot proceed")
            sys.exit(1)

        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(20):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(8)
            cfg = lam.get_function_configuration(FunctionName=FN)
        rep.kv(state=cfg.get("State"), last_update=cfg.get("LastUpdateStatus"),
               timeout=cfg.get("Timeout"), memory=cfg.get("MemorySize"),
               has_fred_key="FRED_KEY" in ((cfg.get("Environment") or {}).get("Variables") or {}))

        rep.section("2. invoke (17 FRED series + full 2023+ replay)")
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = json.loads(resp["Payload"].read())
        body = json.loads(raw["body"]) if isinstance(raw, dict) and "body" in raw else raw
        rep.log(f"  invoke body: {json.dumps(body, default=str)[:400]}")
        checks.append(("invoke succeeded", not resp.get("FunctionError")))
        if resp.get("FunctionError"):
            rep.fail(f"  FunctionError: {json.dumps(raw, default=str)[:600]}")
            sys.exit(1)

        rep.section("3. read live output — posture + brain constitution + legs")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        posture = doc.get("posture")
        legs = doc.get("legs") or {}
        bc = doc.get("brain_constitution") or {}
        rep.kv(posture=posture, composite=doc.get("composite"),
               sizing_multiplier=doc.get("sizing_multiplier"),
               n_legs=len(legs))
        for name, leg in legs.items():
            rep.log(f"  {name}: score={leg.get('score')} why={leg.get('why')}")
        checks.append(("valid posture", posture in ("RISK_ON", "NEUTRAL", "RISK_OFF", "SEVERE")))
        checks.append(("sizing multiplier present",
                       isinstance(doc.get("sizing_multiplier"), (int, float))))
        checks.append(("all six legs present",
                       set(legs.keys()) == {"funding", "credit", "dollar", "carry",
                                            "growth", "structure"}))
        blob = json.dumps(doc)
        cited = sum(1 for nid in ("nmq5x1e4os92j", "tv-8711fbee989cf1eb",
                                  "nmq5x00zhe98n", "nmq5x00zh27pq") if nid in blob)
        checks.append((f"brain note-ID citations present in live output ({cited}/4 sampled)",
                       cited >= 3))

        rep.section("4. event study — the brain's grading methodology")
        es = doc.get("event_study") or {}
        rep.kv(n_flips=es.get("n_flips_to_risk_off_or_worse"),
               baseline_fwd21=es.get("spx_baseline_fwd_21d_pct"),
               fwd21_while_risk_off=es.get("avg_spx_fwd_21d_while_risk_off_pct"))
        for f in (es.get("flips") or []):
            rep.log(f"  flip {f.get('date')} -> {f.get('posture')}: "
                    f"SPX fwd21={f.get('spx_fwd_21d_pct')}% fwd63={f.get('spx_fwd_63d_pct')}%")
        checks.append(("event study ran with >= 2 real flips",
                       (es.get("n_flips_to_risk_off_or_worse") or 0) >= 2))

        rep.section("5. OCTOBER 2025 REPLAY — Khalid's call")
        oct_replay = es.get("october_2025_replay") or {}
        rep.log(f"  {json.dumps(oct_replay, default=str)}")
        rrp_min = oct_replay.get("rrp_min_in_window_bn")
        checks.append(("October window replayed with real RRP data",
                       isinstance(rrp_min, (int, float))))
        if isinstance(rrp_min, (int, float)):
            rep.kv(october_rrp_min_bn=rrp_min,
                   near_zero_as_khalid_said=(rrp_min < 50))
        bad_days = sum(v for k, v in (oct_replay.get("posture_day_counts") or {}).items()
                       if k in ("RISK_OFF", "SEVERE"))
        rep.kv(october_risk_off_or_severe_days=bad_days)

        rep.section("6. ensure daily Scheduler schedule")
        sched_ok = False
        try:
            sched.create_schedule(
                Name="risk-gate-daily",
                ScheduleExpression="cron(5 11 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={
                    "Arn": cfg["FunctionArn"],
                    "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                    "Input": "{}",
                },
                State="ENABLED",
                Description="Master Risk Gate daily 11:05 UTC pre-market",
            )
            sched_ok = True
            rep.ok("  Scheduler created: risk-gate-daily cron(5 11 * * ? *)")
        except sched.exceptions.ConflictException:
            sched_ok = True
            rep.ok("  Scheduler already exists")
        except Exception as e:
            rep.fail(f"  Scheduler create failed: {str(e)[:200]}")
        checks.append(("daily schedule armed", sched_ok))

        rep.section("verdict")
        failed = [l for l, ok in checks if not ok]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — Master Risk Gate live: {posture} "
               f"(sizing x{doc.get('sizing_multiplier')}), event-study graded, "
               f"October replayed, schedule armed")


if __name__ == "__main__":
    main()
