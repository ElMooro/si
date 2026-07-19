"""ops 3548 — RISK LAYER (Khalid spec): two dropdowns. Stock risk =
mean of risk-direction percentiles over vol_52w / beta / D-E /
short-float (high=risky) + Altman / interest-coverage / dist-vs-52w-hi
(low=risky), +8 if on the careful board, 0-100. Industry risk = mean
member risk (n>=4 industries), inherited by every member. Both as
matrix columns (sortable/exportable) + tercile dropdowns (Low/Med/
High). 36-behavior harness PASS pre-push.

  T1 risk CI rerun (SAFE 8.3 < RISK 99.7 w/ +8 flag; Util inherited;
     small-industry None)
  T2 deploy (ZIP-MARKER: b"industry_risk_score" present) + aggregate:
     risk_score n>=430, industry_risk n>=350; tercile bounds printed;
     5 safest + 5 riskiest stocks; 5 safest + 5 riskiest industries
  T3 page served with both dropdowns + node
"""
import importlib.util, io, json, re, subprocess, sys, tempfile, time, urllib.request, zipfile
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


def fetch(url, t=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3548"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


with report("3548_risk_layer") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:640]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3548 — risk layer")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        def row(t, ind, lv):
            return {"t": t, "sector": "X", "industry": ind, "score": 5,
                    "n_elite": 0, "n_green": 1, "n_red": 0,
                    "sev_sum": 0, "top_elites": [], "red3": [],
                    "flags": [], "flag_w": 0, "dilution_yoy": None,
                    "metrics": {}, "_lv": lv, "_tr": {},
                    "vintage_days": 1}
        LV = lambda vol, beta, de, sf, az, ic, dh: {
            "vol_52w_pct": vol, "beta_2y": beta, "debt_to_equity": de,
            "short_float_pct": sf, "altman_z": az,
            "interest_coverage_ttm": ic, "dist_52w_high_pct": dh}
        rows = {"SAFE": row("SAFE", "Util", LV(10, .5, .2, 1, 9, 50, -2)),
                "MID1": row("MID1", "Util", LV(20, .9, .6, 3, 5, 20, -8)),
                "MID2": row("MID2", "Util", LV(25, 1, .8, 4, 4, 15, -10)),
                "MID3": row("MID3", "Util", LV(22, .95, .7, 3.5, 4.5,
                                               18, -9)),
                "RISK": row("RISK", "Meme", LV(80, 2.5, 4, 25, 1, 1.5,
                                               -55)),
                "RSK2": row("RSK2", "Meme", LV(70, 2.2, 3.5, 20, 1.2,
                                               2, -50))}
        uni = [{"t": k, "sector": "X"} for k in rows]
        mx = m.build_matrix(rows, uni, {}, {"RISK"})
        rk = dict(zip(mx["tickers"], mx["cols"]["risk_score"]))
        ir = dict(zip(mx["tickers"],
                      mx["cols"]["industry_risk_score"]))
        gate("T1_ci", rk["SAFE"] < 15 < rk["RSK2"] < rk["RISK"]
             and 99 <= rk["RISK"] <= 100
             and ir["SAFE"] == ir["MID1"] and ir["RISK"] is None,
             {"safe": rk["SAFE"], "risk": rk["RISK"],
              "util_ind": ir["SAFE"]})
    except Exception as e:
        gate("T1_ci", False, str(e)[:320])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1536,
                  description="Census v1.8.0 risk layer (ops 3548)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    try:
        loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
        src = zipfile.ZipFile(io.BytesIO(
            urllib.request.urlopen(loc, timeout=60).read())
            ).read("lambda_function.py")
        zok = b"industry_risk_score" in src and b'"1.8.0"' in src
    except Exception:
        zok = False
    lam.invoke(FunctionName=FN,
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    try:
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        C = MX["cols"]
        rk = C.get("risk_score") or []
        irk = C.get("industry_risk_score") or []
        nn = lambda a: sum(1 for v in a if isinstance(v, (int, float)))
        pairs = [(MX["tickers"][i], v) for i, v in enumerate(rk)
                 if isinstance(v, (int, float))]
        xs = sorted(v for _, v in pairs)
        lo, hi = xs[len(xs)//3], xs[2*len(xs)//3]
        safest = sorted(pairs, key=lambda x: x[1])[:5]
        riskiest = sorted(pairs, key=lambda x: -x[1])[:5]
        ind_pairs = {}
        for i, v in enumerate(irk):
            if isinstance(v, (int, float)):
                ind_pairs[MX["industries"][i]] = v
        inds = sorted(ind_pairs.items(), key=lambda x: x[1])
        gate("T2_live", zok and nn(rk) >= 430 and nn(irk) >= 350,
             {"zip_marker": zok, "risk_n": nn(rk),
              "ind_risk_n": nn(irk), "terciles": (lo, hi),
              "safest5": safest, "riskiest5": riskiest,
              "safest_industries": inds[:5],
              "riskiest_industries": inds[-5:][::-1]})
    except Exception as e:
        gate("T2_live", False, str(e)[:340])

    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()))
            if b"fxRisk" in pa and b"fxIndRisk" in pa: break
        except Exception: pass
        time.sleep(20)
    mm = re.search(rb'<script id="OPS3529">\n([\s\S]*?)</script>', pa)
    ok_n = False
    if mm:
        src2 = mm.group(1).replace(b"__BT_URL__", b"https://x")
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(src2); pth = f.name
        ok_n = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    gate("T3_page", b"fxRisk" in pa and b"fxIndRisk" in pa and ok_n,
         {"node": ok_n})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3548.json").write_text(
        json.dumps({"ops": 3548, "fails": fails}))
sys.exit(0)
