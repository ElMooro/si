"""ops 3541 — whale-join regate: 13F per-ticker schema is COMPACT (wn = whale_bought−sold USD); 3539 walker guessed verbose names → 0. v1.6.1 reads wn. Also the empty-file lesson: sed>redirect created a 0-byte 3540 that 'ran' green.

Original spec: institutional layer 2 (the "what else would a hedge fund
add" build). Engine v1.6.0 matrix gains: FIVE factor composites as
first-class sortable columns (Value/Quality/Momentum/Growth/Safety —
mean of direction-adjusted cross-sectional percentiles over their
input sets), 🐳 whale 13F net $M join (data/13f-flows-by-ticker t-map,
tolerant field walk), days-to-earnings join (benzinga calendar
walker), and long/short book membership. Page: whales-buying + no-
earnings-≤7d toggles, 📗/📕 chips on tickers, pretty labels for the
synthetic columns, 💾 saved screens (named, localStorage) + 🔗
shareable links (state in URL hash, restored on load). 31-behavior
harness PASS pre-push.

  N1 factor CI exact rerun (cross_pct ties/low, composite means,
     absent-input skip)
  N2 deploy + aggregate: matrix has all 5 factor cols n>=430 each,
     whale col n>=250, earnings col n>=60, in_long_book sum == book
     size, factor_quality top-5 + whales-buying∩quality>=15 sample
     printed
  N3 page served with toggles + save/share + hash logic + node
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3539"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3541_whale_regate") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3541 — factors + joins + saved screens")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        ok1 = (m.cross_pct([10, 20, 30]) == [16.67, 50.0, 83.33]
               and m.cross_pct([10, 20, 30], low=True)
               == [83.33, 50.0, 16.67]
               and m.cross_pct([5, None, 5]) == [50.0, None, 50.0])
        cols = {"pe_ttm": [10, 20, 40], "fcf_yield_pct": [8, 4, 2],
                "ps_ttm": [2, 4, 8], "mom_12_1_pct": [5, 10, 15],
                "mom_6m_pct": [1, 2, 3]}
        m.add_factors(cols, 3)
        gate("N1_ci", ok1 and cols["factor_value"] == [83.3, 50.0, 16.7]
             and cols["factor_momentum"] == [16.7, 50.0, 83.3]
             and "factor_quality" not in cols, "exact")
    except Exception as e:
        gate("N1_ci", False, str(e)[:300])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1024,
                  description="Census v1.6.0 factors+joins (ops 3541)",
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
        book = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/proven-portfolio.json")["Body"].read()).get("book") or []
        nn = lambda k: sum(1 for v in MX["cols"].get(k) or []
                           if isinstance(v, (int, float)))
        f_ok = all(nn(f) >= 430 for f in
                   ("factor_value", "factor_quality", "factor_momentum",
                    "factor_growth", "factor_safety"))
        fq = MX["cols"]["factor_quality"]
        top5 = sorted([(MX["tickers"][i], v) for i, v in enumerate(fq)
                       if isinstance(v, (int, float))],
                      key=lambda x: -x[1])[:5]
        wh = MX["cols"].get("whale_net_usd_m") or []
        q = MX.get("quality") or []
        wq = [(MX["tickers"][i], wh[i], q[i]) for i in range(len(wh))
              if isinstance(wh[i], (int, float)) and wh[i] > 0
              and isinstance(q[i], (int, float)) and q[i] >= 15][:6]
        in_lb = sum(MX["cols"].get("in_long_book") or [])
        book_in_uni = sum(1 for r in book
                          if str(r.get("ticker", "")).upper()
                          in set(MX["tickers"]))
        gate("N2_live", f_ok and nn("whale_net_usd_m") >= 250
             and nn("earnings_in_days") >= 60
             and in_lb == book_in_uni,
             {"factor_cov": {f: nn(f) for f in
                             ("factor_value", "factor_quality",
                              "factor_momentum", "factor_safety")},
              "fq_top5": top5, "whale_n": nn("whale_net_usd_m"),
              "earn_n": nn("earnings_in_days"),
              "in_long_book": (in_lb, book_in_uni),
              "whales_and_quality": wq})
    except Exception as e:
        gate("N2_live", False, str(e)[:340])

    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()))
            if b"fxWhale" in pa and b"fxShare" in pa: break
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
    gate("N3_page", all(k in pa for k in
                        [b"fxWhale", b"fxNoEarn", b"fxSave", b"fxLoad",
                         b"fxShare", b"stateSet", b"in_long_book"])
         and ok_n, {"node": ok_n})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3541.json").write_text(
        json.dumps({"ops": 3541, "fails": fails}))
sys.exit(0)
