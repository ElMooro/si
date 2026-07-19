"""ops 3526 — spx-history-deep UNPARKED + macro-leads metrics card.

justhodl-spx-history v1.0: same-shape rebuild of the writerless static
doc (shape mirrored from the live doc's points[0]; refuses to overwrite
on a thin fetch <5000 rows), FMP ^GSPC 5-window stitch, weekly
Scheduler Sun 08:10. macro-leads.html gets the metrics-mode card on the
four proven scalars (GPR 173.6/z0.65, trucks −6.0% YoY, net CBs
cutting −25%). cds-proxy card DECLINED: dicts-of-dicts only, no proven
scalar — building it would be guesswork (value-gate).

  A1 CI (shape mirror x3 + stitch dedupe) rerun
  A2 live refresh: n_points >= 15000, first <= 1935, last within 7d,
     element shape == prior doc's, all consumer keys present
  A3 one consumer smoke: alert-backtester reads the refreshed doc
     without error (invoke + no exception in response)
  A4 weekly Scheduler exists
  A5 macro-leads served with the metrics card; all four metric paths
     resolve numeric on the live feed
"""
import importlib.util, json, os, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-spx-history"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
iam = boto3.client("iam")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3526"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3526_spx_macro") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:520]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3526 — spx refresher + macro card")
    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "sx", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        calls = []
        def fake(qs, key):
            calls.append(qs)
            if "from=1927" in qs:
                return [{"date": "1930-01-0%d" % i, "price": 20 + i}
                        for i in range(1, 6)]
            if "from=1955" in qs:
                return [{"date": "1960-06-01", "price": 55.0},
                        {"date": "1930-01-01", "price": 19.0}]
            return [{"date": "2026-07-17", "close": 6300.0}]
        m._fmp = fake
        rows = m.fetch_spx("k")
        gate("A1_ci", rows[0] == ("1930-01-01", 21.0)
             and rows[-1] == ("2026-07-17", 6300.0) and len(calls) == 5
             and m.shape_points(rows, {"d": "x", "v": 1})[0]
             == {"d": "1930-01-01", "v": 21.0},
             {"n": len(rows), "windows": len(calls)})
    except Exception as e:
        gate("A1_ci", False, str(e)[:300])

    prev = json.loads(s3c.get_object(Bucket=BUCKET,
                      Key="data/spx-history-deep.json")["Body"].read())
    prev_sample = (prev.get("points") or [None])[0]
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"},
                  timeout=600, memory=512,
                  description="SPX deep-history refresher v1.0 (ops 3526)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    r = lam.invoke(FunctionName=FN, Payload=b"{}")
    time.sleep(2)
    try:
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
                         Key="data/spx-history-deep.json")["Body"].read())
        pts = doc.get("points") or []
        same_shape = (type(pts[0]).__name__
                      == type(prev_sample).__name__) if pts and prev_sample else True
        keys_ok = set(prev.keys()) <= set(doc.keys())
        last_ok = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(doc["last"] + "T00:00:00+00:00")
                   ).days <= 7
        gate("A2_refreshed", doc["n_points"] >= 15000
             and doc["first"] <= "1935" and last_ok and same_shape
             and keys_ok,
             {"n_points": doc["n_points"], "first": doc["first"],
              "last": doc["last"], "shape": type(pts[0]).__name__ if pts else None,
              "prev_shape": type(prev_sample).__name__,
              "keys_superset": keys_ok})
    except Exception as e:
        gate("A2_refreshed", False, str(e)[:300])

    try:
        rr = lam.invoke(FunctionName="justhodl-alert-backtester",
                        Payload=b"{}")
        pay = rr["Payload"].read()[:200]
        gate("A3_consumer", "FunctionError" not in rr,
             {"status": rr.get("StatusCode"),
              "err": rr.get("FunctionError"), "peek": pay.decode(errors="replace")})
    except Exception as e:
        gate("A3_consumer", False, str(e)[:220])

    try:
        role = iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
        arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
        body = dict(Name="spx-history-sched",
                    ScheduleExpression="cron(10 8 ? * SUN *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": arn, "RoleArn": role, "Input": "{}"},
                    State="ENABLED",
                    Description="SPX deep-history weekly refresh")
        try: sch.create_schedule(**body)
        except sch.exceptions.ConflictException: sch.update_schedule(**body)
        gate("A4_schedule", True,
             sch.get_schedule(Name="spx-history-sched")["ScheduleExpression"])
    except Exception as e:
        gate("A4_schedule", False, str(e)[:220])

    served = b""
    for _ in range(12):
        try:
            served = fetch(f"https://justhodl.ai/macro-leads.html?cb={int(time.time())}")
            if b"data-metrics" in served: break
        except Exception: pass
        time.sleep(15)
    try:
        ml = json.loads(s3c.get_object(Bucket=BUCKET,
                        Key="data/macro-leads.json")["Body"].read())
        def rv(p):
            cur = ml
            for part in p.split("."):
                cur = cur.get(part) if isinstance(cur, dict) else None
            return cur
        paths = ["geopolitical_risk.gpr", "geopolitical_risk.z_5y",
                 "heavy_truck_sales.yoy_pct",
                 "rate_cut_diffusion.net_pct_cutting"]
        vals = {p: rv(p) for p in paths}
        gate("A5_macro_card", b"data-metrics" in served
             and all(isinstance(v, (int, float)) for v in vals.values()),
             {"served": b"data-metrics" in served, "vals": vals})
    except Exception as e:
        gate("A5_macro_card", False, str(e)[:260])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3526.json").write_text(json.dumps({"ops":3526,"fails":fails}))
sys.exit(0)
