"""ops 3380 — grading-loop E2E, throttle-resilient (3379 rerun + findings).

3379 died on account-level Lambda TooManyRequests at Invoke (retries=1 → fatal)
after proving G0/G1. Findings absorbed: the checker IS live-scheduled twice
(-4h + -daily) — config.json just drifted (cron None) — so the deployed
normalizer auto-resurrects the whole stuck backlog within 4h regardless of
this test. Here: clean 3379's stray test row, redo the synthetic-legacy E2E
with backoff-resilient invokes, and codify the real schedules into config.
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=340, retries={"max_attempts": 2}))
DDB = boto3.resource("dynamodb", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
EV = boto3.client("events", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3380)"}
CHECKER = "justhodl-outcome-checker"
TEST_TYPE = "zz-ops3379-test"

def invoke_resilient(fn, itype="Event", payload=b"{}", tries=6):
    for k in range(tries):
        try:
            return LAM.invoke(FunctionName=fn, InvocationType=itype, Payload=payload)
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e):
                w = 15 * (k + 1)
                print(f"[invoke] throttled ({fn}), retry in {w}s"); time.sleep(w); continue
            raise
    raise RuntimeError(f"invoke {fn}: still throttled after {tries} tries")

def q_close_days_ago(days):
    u = "https://query1.finance.yahoo.com/v8/finance/chart/QQQ?range=3mo&interval=1d"
    with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=15) as r:
        j = json.loads(r.read())
    res = j["chart"]["result"][0]; ts = res["timestamp"]; cl = res["indicators"]["quote"][0]["close"]
    target = time.time() - days * 86400
    best = min(range(len(ts)), key=lambda i: abs(ts[i] - target) if cl[i] else 9e18)
    return float(cl[best])

with report("3380_grading_e2e_resilient") as rep:
    rep.heading("ops 3380 — grading E2E, throttle-resilient")
    out = {"gates": {}}; fails = []
    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:320]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:270]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    tbl = DDB.Table("justhodl-signals"); otbl = DDB.Table("justhodl-outcomes")

    # G0 — sweep 3379's stray test row(s)
    stray = tbl.get_item(Key={"signal_id": f"{TEST_TYPE}#QQQ#2026-06-07"}).get("Item")
    if stray: tbl.delete_item(Key={"signal_id": stray["signal_id"]})
    gate("G0_stray_cleaned", True, f"3379 stray removed={bool(stray)}")

    # G0b — codify LIVE checker schedules into config.json (drift: cron was None)
    arn = LAM.get_function_configuration(FunctionName=CHECKER)["FunctionArn"]
    rules = EV.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
    live = [{"rule_name": r, "cron": EV.describe_rule(Name=r).get("ScheduleExpression")} for r in rules]
    cfgp = Path(f"aws/lambdas/{CHECKER}/config.json"); cfg = json.loads(cfgp.read_text())
    if live and not (cfg.get("schedule") or {}).get("cron"):
        cfg["schedule"] = dict(live[0], description="codified from live (ops 3380); also live: " +
                               ", ".join(x["rule_name"] for x in live[1:]))
        cfgp.write_text(json.dumps(cfg, indent=2) + "\n")
    gate("G0b_schedules_codified", bool(live), json.dumps(live))

    # G2 — synthetic legacy-broken row, resilient E2E
    logged = datetime.now(timezone.utc) - timedelta(days=40)
    sid = f"{TEST_TYPE}#QQQ#{logged.date().isoformat()}"
    base = q_close_days_ago(40)
    tbl.put_item(Item={"signal_id": sid, "signal_type": TEST_TYPE, "signal_value": "test",
        "predicted_direction": "UP", "confidence": Decimal("0.5"),
        "measure_against": "ticker_vs_benchmark", "baseline_price": Decimal(str(round(base, 4))),
        "benchmark": "SPY", "check_windows": ["day_5", "day_21"], "outcomes": {},
        "accuracy_scores": {}, "logged_at": logged.isoformat(),
        "logged_epoch": int(logged.timestamp()), "status": "pending", "schema_version": "2",
        "ttl": int(time.time()) + 3 * 86400, "rationale": "ops 3380 normalizer E2E — deletes itself"})
    print(f"[test] inserted {sid} baseline={round(base, 2)}")
    invoke_resilient(CHECKER, "Event")
    ok2, row = False, {}
    deadline = time.time() + 480
    while time.time() < deadline:
        row = tbl.get_item(Key={"signal_id": sid}).get("Item") or {}
        oc = row.get("outcomes") or {}
        if "day_5" in oc and "day_21" in oc:
            ok2 = True; break
        time.sleep(20)
    oc = row.get("outcomes") or {}
    gate("G2_legacy_row_scored", ok2 and row.get("status") != "pending",
         f"outcomes={sorted(oc.keys())} status={row.get('status')}")
    out["test_row"] = {"status": row.get("status"),
                      "day_5": {k: str(v) for k, v in (oc.get("day_5") or {}).items() if k in ("correct", "return_pct")},
                      "day_21": {k: str(v) for k, v in (oc.get("day_21") or {}).items() if k in ("correct", "return_pct")}}

    # G3 — cleanup
    tbl.delete_item(Key={"signal_id": sid})
    for w in ("day_5", "day_21"):
        try: otbl.delete_item(Key={"outcome_id": f"{sid}_{w}"})
        except Exception: pass
    gone = "Item" not in tbl.get_item(Key={"signal_id": sid})
    gate("G3_cleanup", gone, f"deleted={gone}")

    # G4 — emitters deployed (zip markers)
    def zsrc(fn):
        info = LAM.get_function(FunctionName=fn)
        with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
            return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
    ok4 = ('VERSION = "1.9.1"' in zsrc("justhodl-stress-index")) and ('"check_timestamps"' in zsrc("justhodl-hot-money"))
    gate("G4_emitters_deployed", ok4, "jsi 1.9.1 + hot-money ts markers")

    # G5 — fresh JSI healthy on 1.9.1 (resilient invoke)
    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient("justhodl-stress-index", "Event")
    ok5, det5 = False, "no fresh feed"
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/jsi.json")["Body"].read())
            if j.get("version") == "1.9.1" and (j.get("generated_at") or "") > t_inv:
                ok5 = bool(j.get("v2") and not j.get("v2_error") and j["v2"].get("signal_state") is not None)
                det5 = f"signal_state={json.dumps(j['v2'].get('signal_state'))[:110]}"
                break
        except Exception: pass
        time.sleep(15)
    gate("G5_jsi_191_healthy", ok5, det5)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3380.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
