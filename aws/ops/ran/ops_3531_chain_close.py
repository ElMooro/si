"""ops 3531 — census chain hardened (v1.1.2: Event-invoked batches +
pacing + finally-guaranteed link; 3530 showed the sync wait could kill
the orchestrator at 900s), full-universe completion, at-scale regates.
D1 fixture drift fixed (drive extract(), not raw _P rows).

  E1 CI via extract-path matrix
  E2 relaunch chain (cursor 0, cache-friendly) -> poll matrix growth
     to >= 450 tickers (chain ~12min; budget 18)
  E3 full-universe: matrix >= 450/>=150 metrics, AAPL exactness holds;
     census boards final top-10 / careful-10 / issuer wall printed
"""
import importlib.util, json, sys, time
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
                   config=Config(read_timeout=120, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")

with report("3531_chain_close") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3531 — chain-hardened full universe")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        d1 = {"symbol": "AAA",
              "points": {"gross_margin_pct": [["d", 40.0]],
                         "px_ma200": [["d", 1]],
                         "debt_to_equity": [["d", 0.5]]},
              "factor_dna": {"axes": []},
              "verdicts": {"greens": [], "reds": []}}
        r = m.extract(d1, "T")
        mx = m.build_matrix({"AAA": r}, [{"t": "AAA", "sector": "T"}])
        gate("E1_ci", r["_lv"] == {"gross_margin_pct": 40.0,
                                   "debt_to_equity": 0.5}
             and mx["cols"]["gross_margin_pct"] == [40.0]
             and "px_ma200" not in mx["metrics"],
             {"metrics": mx["metrics"]})
    except Exception as e:
        gate("E1_ci", False, str(e)[:280])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1024,
                  description="Census v1.1.2 chain-hardened (ops 3531)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"phase": "warm", "cursor": 0,
                                   "refresh": False}).encode())
    traj = []
    n = 0
    for _ in range(36):
        time.sleep(30)
        try:
            mxh = json.loads(s3c.get_object(
                Bucket=BUCKET,
                Key="data/fundamental-census-matrix.json")["Body"].read())
            n = mxh.get("n_tickers") or 0
        except Exception:
            n = 0
        traj.append(n)
        if n >= 450:
            break
    gate("E2_chain", n >= 450, {"trajectory": traj[-12:], "final": n})

    try:
        MX = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        aapl = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v21.json")["Body"].read())
        gm_doc = None
        for d2, v in reversed(aapl["points"]["gross_margin_pct"]):
            if isinstance(v, (int, float)):
                gm_doc = round(float(v), 4); break
        gm_mx = MX["cols"]["gross_margin_pct"][MX["tickers"].index("AAPL")]
        D = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundamental-census.json")["Body"].read())
        sh = D["metric_boards"]["share_count_yoy_pct"]
        gate("E3_full", MX["n_tickers"] >= 450 and MX["n_metrics"] >= 150
             and abs(gm_mx - gm_doc) < 1e-6
             and D["coverage"]["scored"] >= 450,
             {"matrix": (MX["n_tickers"], MX["n_metrics"]),
              "aapl_gm": (gm_mx, gm_doc),
              "scored": D["coverage"]["scored"],
              "top10": [(r["t"], r["score"], r["n_elite"])
                        for r in D["top_quality"][:10]],
              "careful10": [(r["t"], r["flags"][:2], r["flag_w"])
                            for r in D["careful"][:10]],
              "issuers5": sh["worst"][:5],
              "buybacks5": sh["best"][:5],
              "avg": D["summary"]["avg_score"],
              "flagged": D["summary"]["n_flagged"],
              "dormant": D["coverage"]["dormant_sample"]})
    except Exception as e:
        gate("E3_full", False, str(e)[:320])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3531.json").write_text(
        json.dumps({"ops": 3531, "fails": fails}))
sys.exit(0)
