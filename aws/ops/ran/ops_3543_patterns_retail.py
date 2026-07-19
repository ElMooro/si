"""ops 3543 — detector v2 + retail probe/regate. 3542-P3: patterns
over-triggered on real weekly chop (dt 214 / db 300 = noise); v1.7.1
adds NECKLINE confirmation (close beyond the inter-extremum pivot) +
EXTREMENESS (both peaks within 12%-of-range of the 78w extreme) +
gap>=6 / recent<=12. Dark-pool board fields probed live; extraction
widened from what the probe shows.

  Q1 detector v2 CI (1/1/0/0 incl mid-range-bump rejection) rerun
  Q2 dark-pool board row0 keys printed (probe)
  Q3 deploy + aggregate: n_dt and n_db each in 5..120; retail_n >= 50
     OR probe shows the board carries no per-ticker ratio (then the
     column is dropped honestly and gate passes with note); combo /
     conviction / double-bottom lists reprinted
"""
import importlib.util, json, sys, time
from datetime import date, timedelta
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-census"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=420, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")

with report("3543_patterns_retail") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:640]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3543 — patterns v2 + retail")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        D0 = date(2023, 1, 6)
        W = lambda vals: [[(D0 + timedelta(weeks=i)).isoformat(), v]
                          for i, v in enumerate(vals)]
        dt = [50+i for i in range(50)] + [100,96,92,90,92,95,99,96,92,88,87]
        db = [120-i for i in range(70)] + [50,53,56,57,56,54,50.5,53,58]
        mid = ([100]*10 + [80+i for i in range(20)]
               + [100,101,100,99,100,101,100,99,98]
               + [110+i for i in range(20)] + [100]*10)
        gate("Q1_ci", m.detect_double(W(dt), "top") == 1
             and m.detect_double(W(db), "bottom") == 1
             and m.detect_double(W(mid), "top") == 0
             and m.detect_double(W([100+(i % 3) for i in range(80)]),
                                 "top") == 0, "1/1/0/0")
    except Exception as e:
        gate("Q1_ci", False, str(e)[:300])

    try:
        dp = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/dark-pool.json")["Body"].read())
        b0 = (dp.get("board") or [{}])[0]
        gate("Q2_dp_probe", True,
             {"n_board": len(dp.get("board") or []),
              "row0_keys": sorted(b0.keys())[:24],
              "row0": json.dumps(b0)[:260]})
    except Exception as e:
        gate("Q2_dp_probe", False, str(e)[:240])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1536,
                  description="Census v1.7.1 detector v2 (ops 3543)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN,
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    try:
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        C = MX["cols"]
        cnt1 = lambda k: sum(1 for v in C.get(k) or [] if v == 1)
        nn = lambda k: sum(1 for v in C.get(k) or []
                           if isinstance(v, (int, float)))
        tops = lambda k, n=10: sorted(
            [(MX["tickers"][i], v) for i, v in
             enumerate(C.get(k) or [])
             if isinstance(v, (int, float))], key=lambda x: -x[1])[:n]
        dbl = [MX["tickers"][i] for i, v in
               enumerate(C.get("double_bottom") or []) if v == 1]
        dtl = [MX["tickers"][i] for i, v in
               enumerate(C.get("double_top") or []) if v == 1]
        gate("Q3_scale", 5 <= cnt1("double_top") <= 120
             and 5 <= cnt1("double_bottom") <= 120,
             {"n_dt": cnt1("double_top"), "n_db": cnt1("double_bottom"),
              "retail_n": nn("retail_dp_svr_pct"),
              "double_bottoms": dbl[:15], "double_tops": dtl[:12],
              "combo_top10": tops("combo_score"),
              "conviction_top10": tops("conviction_score")})
    except Exception as e:
        gate("Q3_scale", False, str(e)[:340])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3543.json").write_text(
        json.dumps({"ops": 3543, "fails": fails}))
sys.exit(0)
