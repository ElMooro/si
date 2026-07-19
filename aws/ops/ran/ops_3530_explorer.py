"""ops 3530 — (rerun of 3529 after the v1.1.0 OOM: aggregate retained full _P per doc → ~1.5GB at 496 names → Lambda SIGKILL → ConnectionClosed. v1.1.1 keeps only the latest-value map.)

Original spec: ops 3529 — Metric Explorer on the Fundamental Census (Khalid spec):
sort by ANY metric asc/desc, stack unlimited metrics into a Σ of
cross-sectional percentiles (per-chip ↑H/↓L direction toggles,
LOW-better auto-detected for debt/days/dilution/valuation families),
live ticker filter, and DOUBLE-CLICK → deep-chart modal riding the
real FGChart core (selected metrics + add-any-metric from the doc's
catalog + price overlay + 5Y/10Y/MAX) with a hand-off link into the
full comparator. Engine v1.1.0 publishes the columnar latest-value
MATRIX (every fundamentals metric in >=50% of docs; tech/price/
estimate keys excluded) as the sorting substrate.

10-behavior jsdom harness (census2.js) PASS pre-push: exact percentile
Σ ordering incl null handling, LOW auto-flip, chip flip re-rank, asc,
filter, modal open, FGChart series set, price own-scale, add-metric,
range shrink.

  D1 matrix CI (alignment/nulls/exclusions/50%-boundary) rerun
  D2 deploy + aggregate on the now-complete chain: matrix tickers
     >= 450, metrics >= 120, AAPL gross_margin in matrix == doc latest
     (cross-source exactness)
  D3 census full-universe boards printed (the real final top-10 +
     careful-10)
  D4 page served with explorer + modal + core includes, OPS3529 node
  D5 FB doc contract from the runner: MSFT ?symbol&period returns
     points.revenue with >=150 rows
"""
import importlib.util, json, re, subprocess, sys, tempfile, time, urllib.request
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
                   config=Config(read_timeout=900, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url, t=40):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3529"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


with report("3530_explorer") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3529 — Metric Explorer + matrix")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        P1 = {"gross_margin_pct": [["d", 40.0]],
              "debt_to_equity": [["d", 0.5]],
              "px_ma200": [["d", 1]], "est_rev": [["d", 2]],
              "only_a": [["d", 7]]}
        P2 = {"gross_margin_pct": [["d", 20.0]],
              "debt_to_equity": [["d", 2.0]]}
        row = lambda t, s2, P: {"t": t, "sector": s2, "metrics": {},
                                "_P": P}
        uni = [{"t": "AAA", "sector": "T"}, {"t": "BBB", "sector": "E"},
               {"t": "MISS", "sector": "T"}]
        mx = m.build_matrix({"AAA": row("AAA", "T", P1),
                             "BBB": row("BBB", "E", P2)}, uni)
        gate("D1_matrix_ci",
             mx["tickers"] == ["AAA", "BBB"]
             and set(mx["metrics"]) == {"gross_margin_pct",
                                        "debt_to_equity", "only_a"}
             and mx["cols"]["only_a"] == [7, None]
             and all(not k.startswith(("px_", "est_"))
                     for k in mx["metrics"]),
             {"metrics": mx["metrics"]})
    except Exception as e:
        gate("D1_matrix_ci", False, str(e)[:300])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1024,
                  description="Census v1.1.0 matrix (ops 3529)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"phase": "aggregate"}).encode())
    for _ in range(24):
        time.sleep(10)
        try:
            _h = s3c.head_object(Bucket=BUCKET,
                Key="data/fundamental-census-matrix.json")
            import datetime as _dt
            if (_dt.datetime.now(_dt.timezone.utc)
                    - _h["LastModified"]).total_seconds() < 120:
                break
        except Exception:
            pass
    try:
        MX = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        aapl_doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v21.json")["Body"].read())
        gm_doc = None
        for d2, v in reversed(aapl_doc["points"]["gross_margin_pct"]):
            if isinstance(v, (int, float)):
                gm_doc = round(float(v), 4); break
        i = MX["tickers"].index("AAPL")
        gm_mx = MX["cols"]["gross_margin_pct"][i]
        gate("D2_matrix_live",
             MX["n_tickers"] >= 450 and MX["n_metrics"] >= 120
             and abs((gm_mx or 0) - (gm_doc or -1)) < 1e-6
             and len(MX["cols"]["gross_margin_pct"]) == MX["n_tickers"],
             {"n_tickers": MX["n_tickers"], "n_metrics": MX["n_metrics"],
              "aapl_gm": (gm_mx, gm_doc),
              "sample_metrics": MX["metrics"][:8]})
    except Exception as e:
        gate("D2_matrix_live", False, str(e)[:320])

    try:
        D = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundamental-census.json")["Body"].read())
        gate("D3_full_boards", D["coverage"]["scored"] >= 450,
             {"scored": D["coverage"]["scored"],
              "top10": [(r["t"], r["score"], r["n_elite"])
                        for r in D["top_quality"][:10]],
              "careful10": [(r["t"], r["flags"][:2], r["flag_w"])
                            for r in D["careful"][:10]],
              "avg": D["summary"]["avg_score"],
              "flagged": D["summary"]["n_flagged"],
              "issuers": D["metric_boards"]["share_count_yoy_pct"]
              ["worst"][:5]})
    except Exception as e:
        gate("D3_full_boards", False, str(e)[:300])

    pa = b""
    for _ in range(15):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()))
            if b"OPS3529" in pa: break
        except Exception: pass
        time.sleep(20)
    mm = re.search(rb'<script id="OPS3529">\n([\s\S]*?)</script>', pa)
    ok_n = False
    if mm:
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(mm.group(1)); pth = f.name
        ok_n = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    gate("D4_page", all(k in pa for k in
                        [b"OPS3529", b"fxModal", b"fxSvg",
                         b"fg-chart.js", b"fg-catalog.js"]) and ok_n,
         {"node": ok_n})

    try:
        d5 = json.loads(fetch(
            "https://fqb6ztg7v6ax4qzylimqjiezmq0kqyyy.lambda-url."
            "us-east-1.on.aws/?symbol=MSFT&period=quarter", t=120))
        rev = (d5.get("points") or {}).get("revenue") or []
        gate("D5_fb_contract", len(rev) >= 150,
             {"n_revenue": len(rev), "oldest": rev[0][0] if rev else None})
    except Exception as e:
        gate("D5_fb_contract", False, str(e)[:240])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3529.json").write_text(
        json.dumps({"ops": 3530, "fails": fails}))
sys.exit(0)
