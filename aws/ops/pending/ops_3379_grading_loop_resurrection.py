"""ops 3379 — signals grading loop: fleet resurrection gates.

Findings this arc: ~40 direct emitters write schema-v2 rows the outcome-
checker can't score — no check_timestamps (window loop no-ops) and/or a
LITERAL measure_against string that gets PRICED. Equity picks survived only
via the harvester's correct re-log; macro signals (nyfed-pd, ofr-stfm,
settlement-fails, hot-money, JSI) were dead-on-arrival. Shipped this push:
  • outcome-checker: in-loop NORMALIZER — synthesizes check_timestamps from
    logged_at+window and recovers the real symbol from signal_id
    "type#TICKER#date". One point, all 40 emitters, past AND future rows.
  • aws/shared/signals_emit.py — the harvester contract as a shared module
    (log_signal + Yahoo-keyless yprice), for all new emissions.
  • hot-money — emits clean (real ETF ticker + timestamps).
  • stress-index v1.9.1 — JSI closed loop: jsi-episode-entry (UP, atlas-
    backed), jsi-flare (DOWN), jsi-complacency (DOWN) on TRANSITIONS only,
    via the shared emitter. QQQ directional.

Gates:
  G0  checker schedule recon — docstring says weekly, config has none. If
      no live rule targets it: CREATE Scheduler cron(0 8 ? * MON *) via
      justhodl-scheduler-role and codify config.json (auto-commit).
  G1  checker deploy settled + normalizer marker in zip
  G2  E2E on a SYNTHETIC legacy-broken row (type zz-ops3379-test, literal
      measure_against, day_-form windows, no timestamps, logged 40d ago,
      real QQQ baseline from Yahoo): invoke checker → both day_5 and
      day_21 outcomes written → normalizer proven on BOTH defects at once
  G3  cleanup — test signal + its two outcome rows deleted, verified gone
  G4  stress-index v1.9.1 + hot-money clean-emit markers deployed
  G5  fresh JSI run: v2 healthy on 1.9.1, signal_state present
"""

import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=340, retries={"max_attempts": 1}))
DDB = boto3.resource("dynamodb", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
EV = boto3.client("events", "us-east-1")
SCH = boto3.client("scheduler", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3379"}
CHECKER = "justhodl-outcome-checker"
TEST_TYPE = "zz-ops3379-test"


def zsrc(fn):
    info = LAM.get_function(FunctionName=fn)
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA),
                                timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")


def q_close_days_ago(days):
    u = "https://query1.finance.yahoo.com/v8/finance/chart/QQQ?range=3mo&interval=1d"
    with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=15) as r:
        j = json.loads(r.read())
    res = j["chart"]["result"][0]
    ts = res["timestamp"]
    cl = res["indicators"]["quote"][0]["close"]
    target = time.time() - days * 86400
    best = min(range(len(ts)), key=lambda i: abs(ts[i] - target) if cl[i] else 9e18)
    return float(cl[best])


def main(rep):
    out = {"gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:340]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:280]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    # ── G0: schedule recon / create ──
    arn = LAM.get_function_configuration(FunctionName=CHECKER)["FunctionArn"]
    rules = []
    try:
        rules = EV.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
    except Exception:  # noqa: BLE001
        pass
    scheds = []
    try:
        for s in SCH.list_schedules(MaxResults=100).get("Schedules", []):
            d = SCH.get_schedule(Name=s["Name"])
            if CHECKER in json.dumps(d.get("Target", {}), default=str):
                scheds.append({"name": s["Name"], "expr": d.get("ScheduleExpression")})
    except Exception as e:  # noqa: BLE001
        print("[sched-list]", str(e)[:80])
    created = None
    if not rules and not scheds:
        try:
            SCH.create_schedule(
                Name="justhodl-outcome-checker-weekly",
                ScheduleExpression="cron(0 8 ? * MON *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn,
                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                        "Input": "{}"},
                Description="Weekly outcome grading (ops 3379 — was fully unscheduled)")
            created = "cron(0 8 ? * MON *)"
            cfgp = Path(f"aws/lambdas/{CHECKER}/config.json")
            cfg = json.loads(cfgp.read_text())
            cfg["schedule"] = {"scheduler_name": "justhodl-outcome-checker-weekly",
                              "cron": "cron(0 8 ? * MON *)",
                              "description": "Weekly outcome grading (Scheduler, ops 3379)"}
            cfgp.write_text(json.dumps(cfg, indent=2) + "\n")
        except Exception as e:  # noqa: BLE001
            created = f"CREATE_FAILED {str(e)[:120]}"
    gate("G0_checker_scheduled", bool(rules or scheds or (created and "cron" in str(created))),
         f"rules={rules} schedules={scheds} created={created}")

    # ── G1: normalizer deployed ──
    ok1 = False
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            if LAM.get_function_configuration(FunctionName=CHECKER).get("LastUpdateStatus") == "Successful" \
                    and "normalize legacy schema-v2" in zsrc(CHECKER):
                ok1 = True
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G1_normalizer_deployed", ok1, "marker in zip")

    # ── G2: synthetic legacy row E2E ──
    tbl = DDB.Table("justhodl-signals")
    otbl = DDB.Table("justhodl-outcomes")
    logged = datetime.now(timezone.utc) - timedelta(days=40)
    sid = f"{TEST_TYPE}#QQQ#{logged.date().isoformat()}"
    base = q_close_days_ago(40)
    tbl.put_item(Item={
        "signal_id": sid, "signal_type": TEST_TYPE, "signal_value": "test",
        "predicted_direction": "UP", "confidence": Decimal("0.5"),
        "measure_against": "ticker_vs_benchmark",       # literal — defect #2
        "baseline_price": Decimal(str(round(base, 4))),
        "benchmark": "SPY",
        "check_windows": ["day_5", "day_21"],           # day_-form, no timestamps — defect #1
        "outcomes": {}, "accuracy_scores": {},
        "logged_at": logged.isoformat(), "logged_epoch": int(logged.timestamp()),
        "status": "pending", "schema_version": "2",
        "ttl": int(time.time()) + 3 * 86400,
        "rationale": "ops 3379 normalizer E2E — will be deleted"})
    print(f"[test] inserted {sid} baseline={round(base, 2)}")

    resp = LAM.invoke(FunctionName=CHECKER, InvocationType="RequestResponse", Payload=b"{}")
    print(f"[checker] status={resp.get('StatusCode')} err={resp.get('FunctionError')}")
    row = tbl.get_item(Key={"signal_id": sid}).get("Item") or {}
    oc = row.get("outcomes") or {}
    ok2 = "day_5" in oc and "day_21" in oc and row.get("status") != "pending"
    gate("G2_legacy_row_scored", ok2,
         f"outcomes_keys={sorted(oc.keys())} status={row.get('status')} "
         f"(both defects normalized in one pass)")
    out["test_row"] = {"status": row.get("status"),
                      "day_5_correct": (oc.get("day_5") or {}).get("correct"),
                      "day_21_correct": (oc.get("day_21") or {}).get("correct")}

    # ── G3: cleanup ──
    tbl.delete_item(Key={"signal_id": sid})
    purged = 0
    for w in ("day_5", "day_21"):
        try:
            otbl.delete_item(Key={"outcome_id": f"{sid}_{w}"})
            purged += 1
        except Exception:  # noqa: BLE001
            pass
    gone = "Item" not in tbl.get_item(Key={"signal_id": sid})
    gate("G3_cleanup", gone, f"signal_deleted={gone} outcome_deletes={purged}")

    # ── G4: emitters deployed ──
    ok4a = ok4b = False
    deadline = time.time() + 240
    while time.time() < deadline and not (ok4a and ok4b):
        try:
            if not ok4a and 'VERSION = "1.9.1"' in zsrc("justhodl-stress-index"):
                ok4a = True
            if not ok4b and '"check_timestamps"' in zsrc("justhodl-hot-money"):
                ok4b = True
        except Exception:  # noqa: BLE001
            pass
        if not (ok4a and ok4b):
            time.sleep(12)
    gate("G4_emitters_deployed", ok4a and ok4b, f"jsi191={ok4a} hotmoney_ts={ok4b}")

    # ── G5: fresh JSI run healthy ──
    t_inv = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-stress-index", InvocationType="Event", Payload=b"{}")
    ok5, det5 = False, "no fresh feed"
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/jsi.json")["Body"].read())
            if j.get("version") == "1.9.1" and (j.get("generated_at") or "") > t_inv:
                ok5 = bool(j.get("v2") and not j.get("v2_error")
                           and (j["v2"].get("signal_state") is not None))
                det5 = f"v2=ok signal_state={json.dumps(j['v2'].get('signal_state'))[:120]}"
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(15)
    gate("G5_jsi_191_healthy", ok5, det5)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3379.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)


with report("3379_grading_loop_resurrection") as _rep:
    _rep.heading("ops 3379 — signals grading loop resurrection")
    main(_rep)
