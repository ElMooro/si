"""ops 3537 — INDUSTRY dropdown + institutional pack (Khalid spec +
hedge-fund additions). Engine v1.5.0: matrix gains aligned
industries[] (from fundgraph profile — semis, software, banks...),
quality[] (census score), turn[] (full turnaround map), flagged[]
(careful set); mcap flows as a column. Explorer adds: Industry select,
market-cap buckets (mega/large/mid/small), min-quality select,
hide-🚩 and inflecting-≥75 toggles, Q + Turn columns, ⬇CSV export of
the filtered view, and four one-click preset screens (Quality
compounders / Cash-flow value / Momentum turnarounds / Fortress
balance sheets). 23-behavior harness PASS pre-push.

  L1 v1.5.0 CI (industry/quality/turn/flagged alignment, mcap flow)
  L2 deploy + aggregate: industries populated (>=40 distinct,
     coverage >=430), AAPL industry printed, turn coverage >=300,
     flagged sum ~= careful size, mcap n>=450; semis membership sample
  L3 page served with all new controls + presets + node
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3537"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3537_industry_pack") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3537 — industry + institutional pack")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        d = {"symbol": "A",
             "points": {"gross_margin_pct": [["d", 40.0]],
                        "mcap": [["d", 2.5e11]]},
             "price": [["d", 100 + i] for i in range(60)],
             "profile": {"industry": "Semiconductors"},
             "factor_dna": {"axes": []},
             "verdicts": {"greens": [{"k": "g", "sev": 1, "elite": True,
                                      "basis": "fund"}], "reds": []}}
        r = m.extract(d, "Tech")
        mx = m.build_matrix({"A": r}, [{"t": "A", "sector": "Tech"}],
                            {"A": 88.5}, {"A"})
        gate("L1_ci", r["industry"] == "Semiconductors"
             and mx["industries"] == ["Semiconductors"]
             and mx["quality"] == [3] and mx["turn"] == [88.5]
             and mx["flagged"] == [1]
             and mx["cols"]["mcap"] == [2.5e11], "aligned")
    except Exception as e:
        gate("L1_ci", False, str(e)[:300])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1024,
                  description="Census v1.5.0 industry pack (ops 3537)",
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
            Key="data/fundamental-census.json".replace(
                "census.json", "census-matrix.json"))["Body"].read())
        D = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fundamental-census.json")["Body"].read())
        inds = MX.get("industries") or []
        distinct = sorted({x for x in inds if x})
        n_ind = sum(1 for x in inds if x)
        i_aapl = MX["tickers"].index("AAPL")
        semis = [MX["tickers"][i] for i, x in enumerate(inds)
                 if x and "semiconductor" in x.lower()][:8]
        n_turn = sum(1 for v in MX.get("turn") or []
                     if isinstance(v, (int, float)))
        n_flag = sum(MX.get("flagged") or [])
        n_mcap = sum(1 for v in MX["cols"].get("mcap") or []
                     if isinstance(v, (int, float)))
        gate("L2_live", len(distinct) >= 40 and n_ind >= 430
             and n_turn >= 300 and n_mcap >= 450
             and abs(n_flag - len(D["careful"])) <= 2
             and len(MX["industries"]) == MX["n_tickers"],
             {"distinct_industries": len(distinct), "n_ind": n_ind,
              "aapl_industry": inds[i_aapl], "semis_sample": semis,
              "n_turn": n_turn, "n_flagged": n_flag,
              "careful": len(D["careful"]), "n_mcap": n_mcap})
    except Exception as e:
        gate("L2_live", False, str(e)[:340])

    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()))
            if b"fxInd" in pa and b"fxCsv" in pa: break
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
    gate("L3_page", all(k in pa for k in
                        [b"fxInd", b"fxCap", b"fxQmin", b"fxNoFlag",
                         b"fxTurn", b"fxCsv", b'data-p="qc"',
                         b'data-p="bs"']) and ok_n, {"node": ok_n})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3537.json").write_text(
        json.dumps({"ops": 3537, "fails": fails}))
sys.exit(0)
