"""ops 3536 — Explorer sector + momentum filters & board
personalization (Khalid spec). Engine v1.4.0 derives price momentum
per name from the cached doc's weekly price series — mom_6m_pct and
mom_12_1_pct (12-1 convention: t-52w..t-4w) — flowing into the matrix
as first-class sortable columns. Explorer: sector select (only that
industry) + momentum select (High = top global tercile of 12-1 / Low =
bottom tercile), with Σ percentiles recomputed WITHIN the filtered
peer set (sector-relative ranking). Metric leaders & laggards cells:
★ favorite (auto-float to top), 4-color tag cycle, ◀▶ reorder +
drag-and-drop — persisted in localStorage (jh_census_boards_v1).
17-behavior jsdom harness PASS pre-push (subset-percentile exactness,
tercile boundaries, fav/reorder/tag persistence).

  K1 momentum CI exact (6m 19.55 / 12-1 44.86 on synthetic weekly)
  K2 deploy + aggregate: matrix has mom_12_1_pct n>=430 and the AAPL
     value == recompute from the cached doc price (exactness); tercile
     spread printed (lo/hi + 3 high-mom names)
  K3 page served with fxSec/fxMom + OPS3536 block; both scripts node
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
                   config=Config(read_timeout=420, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3536"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3536_filters_boards") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3536 — filters + board personalization")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        price = [["d", 100 + i] for i in range(60)]
        gate("K1_ci", m.momentum(price, 26) == round((159/133-1)*100, 2)
             and m.mom_12_1(price) == round((155/107-1)*100, 2)
             and m.momentum(price[:20], 26) is None,
             {"m6": m.momentum(price, 26), "m121": m.mom_12_1(price)})
    except Exception as e:
        gate("K1_ci", False, str(e)[:280])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1024,
                  description="Census v1.4.0 momentum cols (ops 3536)",
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
        col = MX["cols"].get("mom_12_1_pct") or []
        n_mom = sum(1 for v in col if isinstance(v, (int, float)))
        aapl_doc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v21.json")["Body"].read())
        exp = m.mom_12_1(aapl_doc.get("price") or [])
        got = col[MX["tickers"].index("AAPL")]
        xs = sorted(v for v in col if isinstance(v, (int, float)))
        lo, hi = xs[len(xs)//3], xs[2*len(xs)//3]
        high3 = sorted([(MX["tickers"][i], v) for i, v in enumerate(col)
                        if isinstance(v, (int, float))],
                       key=lambda x: -x[1])[:3]
        gate("K2_matrix_mom", n_mom >= 430 and exp is not None
             and abs(got - exp) < 1e-6,
             {"n_mom": n_mom, "aapl": (got, exp),
              "terciles": (lo, hi), "top_momentum": high3})
    except Exception as e:
        gate("K2_matrix_mom", False, str(e)[:320])

    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()))
            if b"OPS3536" in pa and b"fxMom" in pa: break
        except Exception: pass
        time.sleep(20)
    okn = []
    for tag in (b"OPS3529", b"OPS3536"):
        mm = re.search(rb'<script id="' + tag + rb'">\n([\s\S]*?)</script>', pa)
        if mm:
            with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                             delete=False) as f:
                f.write(mm.group(1)); pth = f.name
            okn.append(subprocess.run(["node", "--check", pth],
                                      capture_output=True).returncode == 0)
        else:
            okn.append(False)
    gate("K3_page", b"fxSec" in pa and b"fxMom" in pa
         and b"jh_census_boards_v1" in pa and all(okn),
         {"node": okn})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3536.json").write_text(
        json.dumps({"ops": 3536, "fails": fails}))
sys.exit(0)
