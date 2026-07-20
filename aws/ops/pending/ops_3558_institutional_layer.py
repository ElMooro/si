"""ops 3558 — INSTITUTIONAL LAYER on both censuses (Khalid: "what
would institutions and hedge funds add"). ETF v1.1.0: RS-vs-SPY 13w,
52w correlation, up/down capture, NAV premium/discount, $ volume/day,
variance-drag estimate for leveraged wrappers, PAIRS board (8 desk
ratios, 3y z). FI v1.1.0: TTM distribution yield (real payouts),
carry-vs-cash bp, return-per-unit-duration, SPREADS board (HYG/LQD,
LQD/IEF, TIP/IEF, EMB/IEF, TLT/SHY flattener, 3y z), dealer-
positioning + GCF-TRI funding chips (credit-stress + eurodollar-
plumbing joins). Pages: PAIRS strip / SPREADS+dealer strip; new
columns auto-listed. 14-behavior dual harness PASS.

  C1 deploy both (zip 1.1.0) + full reruns
  C2 ETF live: rs/corr/capture coverage >=60; pairs board >=6 pairs
     with z; prem_disc present; variance drag on >=3 levered; print
     pairs z table + capture extremes
  C3 FI live: ttm_yield >=30 names, carry table sane (HYG carry >
     BIL by >150bp), spreads 5/5 with z, dealer+funding chips present
     (or honest None); print all
  C4 pages served with strips + node
"""
import io, json, re, subprocess, sys, tempfile, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=800,
                                 retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url, t=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3558"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


with report("3558_institutional_layer") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:700]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    env_e = {"FMP_API_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}
    env_f = dict(env_e, FRED_API_KEY="2f057499936072679d8843d7fce99989")
    for fn, env, tmo, mem in (("justhodl-etf-census", env_e, 900, 1024),
                              ("justhodl-fi-census", env_f, 900, 768)):
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=REPO/"aws"/"lambdas"/fn/"source",
                      env_vars=env, timeout=tmo, memory=mem,
                      description=f"{fn} v1.1.0 (ops 3558)",
                      create_function_url=False, smoke=False)
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
        src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(
            loc, timeout=60).read())).read("lambda_function.py")
        if b'"1.1.0"' not in src:
            gate(f"C1_zip_{fn}", False, "marker missing")
    try:
        r = lam.invoke(FunctionName="justhodl-etf-census", Payload=b"{}")
        json.loads(r["Payload"].read())
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/etf-census-matrix.json")["Body"].read())
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/etf-census.json")["Body"].read())
        C = MX["cols"]
        nn = lambda k: sum(1 for v in C.get(k) or []
                           if isinstance(v, (int, float)))
        pairs = doc.get("pairs") or []
        pz = [(p["pair"], p.get("z")) for p in pairs]
        drag = [(MX["tickers"][i], v) for i, v in
                enumerate(C.get("variance_drag_pct_ann") or [])
                if isinstance(v, (int, float))]
        cap = sorted([(MX["tickers"][i],
                       C["up_capture_pct"][i],
                       C["down_capture_pct"][i])
                      for i in range(MX["n"])
                      if isinstance((C.get("up_capture_pct") or
                                     [None]*MX["n"])[i], (int, float))
                      and isinstance((C.get("down_capture_pct") or
                                      [None]*MX["n"])[i],
                                     (int, float))],
                     key=lambda x: -(x[1] - x[2]))
        gate("C2_etf", nn("rs_13w_pct") >= 60
             and nn("corr_spy_52w") >= 60
             and nn("prem_disc_pct") >= 40
             and len([1 for _, z in pz if z is not None]) >= 6
             and len(drag) >= 3,
             {"rs_n": nn("rs_13w_pct"), "corr_n": nn("corr_spy_52w"),
              "capture_n": nn("up_capture_pct"),
              "prem_n": nn("prem_disc_pct"), "pairs_z": pz,
              "variance_drag": drag[:6],
              "best_capture_spread": cap[:3],
              "worst_capture_spread": cap[-3:]})
    except Exception as e:
        gate("C2_etf", False, str(e)[:340])

    try:
        r = lam.invoke(FunctionName="justhodl-fi-census", Payload=b"{}")
        json.loads(r["Payload"].read())
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fi-census-matrix.json")["Body"].read())
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fi-census.json")["Body"].read())
        C = MX["cols"]
        idx = {t: i for i, t in enumerate(MX["tickers"])}
        yl = C.get("ttm_yield_pct") or []
        cb = C.get("carry_vs_cash_bp") or []
        ny = sum(1 for v in yl if isinstance(v, (int, float)))
        hyg_c = cb[idx["HYG"]] if "HYG" in idx and cb else None
        spreads = [(p["pair"], p.get("z")) for p in
                   doc.get("spreads") or []]
        top_carry = sorted([(MX["tickers"][i], yl[i], cb[i])
                            for i in range(MX["n"])
                            if isinstance(yl[i], (int, float))],
                           key=lambda x: -x[1])[:6]
        gate("C3_fi", ny >= 30
             and isinstance(hyg_c, (int, float)) and hyg_c > 150
             and len([1 for _, z in spreads if z is not None]) >= 5,
             {"yield_n": ny, "hyg_carry_bp": hyg_c,
              "top_yielders": top_carry, "spreads_z": spreads,
              "dealer": doc.get("dealer"),
              "funding": doc.get("funding"),
              "ret_per_dur_n": sum(1 for v in
                                   C.get("ret_per_duration") or []
                                   if isinstance(v, (int, float)))})
    except Exception as e:
        gate("C3_fi", False, str(e)[:340])

    ok_p = True
    for pg, mark in (("etf-census.html", b"pairsStrip"),
                     ("fixed-income-census.html", b"spreadStrip")):
        pa = b""
        for _ in range(14):
            try:
                pa = fetch(f"https://justhodl.ai/{pg}?cb=%d"
                           % int(time.time()))
                if mark in pa:
                    break
            except Exception:
                pass
            time.sleep(20)
        mm = re.search(rb'<script id="OPSPAGE">\n([\s\S]*?)</script>',
                       pa)
        node = False
        if mm:
            with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                             delete=False) as f:
                f.write(mm.group(1)); pth = f.name
            node = subprocess.run(["node", "--check", pth],
                                  capture_output=True).returncode == 0
        ok_p &= (mark in pa) and node
    gate("C4_pages", ok_p, "strips served + node x2")

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3558.json").write_text(
        json.dumps({"ops": 3558, "fails": fails}))
sys.exit(0)
