"""ops 3559 — RISK/REWARD LAYER across the census family (Khalid
spec): upside_pct / downside_pct / rr_ratio on all three engines,
component math (52w-high recapture + fundamental/tech extension +
pattern measured-moves + FI carry ⇢ upside; risk-scaled vol +
room-above-support + pattern/flag/decay penalties ⇢ downside), fxRR
quick-sort dropdown on all three pages. CI: HERO rr 6.03 vs TRAP
0.26. Harnesses 39 + 16 PASS.

  D1 deploy x3 (zip: b"rr_ratio")
  D2 flagship aggregate: rr coverage >=430; top-10 R/R + top upside +
     lowest downside printed; sanity rr in (0, 30]
  D3 ETF rerun: rr coverage >=70; best/worst R/R printed (levered
     wrappers must sit at the bottom)
  D4 FI rerun: rr coverage >=40; carry-driven upside check (SRLN
     upside > BIL upside); best R/R printed
  D5 pages x3 served with fxRR + node
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3559"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


def rrtable(mxkey):
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
                                   Key=mxkey)["Body"].read())
    C = MX["cols"]
    rows = [(MX["tickers"][i], C["rr_ratio"][i], C["upside_pct"][i],
             C["downside_pct"][i]) for i in range(MX["n"])
            if isinstance((C.get("rr_ratio") or [None]*MX["n"])[i],
                          (int, float))]
    rows.sort(key=lambda x: -x[1])
    return MX, rows


with report("3559_risk_reward") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:700]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    env_e = {"FMP_API_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}
    env_f = dict(env_e, FRED_API_KEY="2f057499936072679d8843d7fce99989")
    for fn, env, tmo, mem in (
            ("justhodl-fundamental-census", {}, 900, 1536),
            ("justhodl-etf-census", env_e, 900, 1024),
            ("justhodl-fi-census", env_f, 900, 768)):
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=REPO/"aws"/"lambdas"/fn/"source",
                      env_vars=env, timeout=tmo, memory=mem,
                      description=f"{fn} rr-layer (ops 3559)",
                      create_function_url=False, smoke=False)
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
        src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(
            loc, timeout=60).read())).read("lambda_function.py")
        if b"rr_ratio" not in src:
            gate(f"D1_zip_{fn}", False, "marker missing")

    try:
        lam.invoke(FunctionName="justhodl-fundamental-census",
                   Payload=json.dumps({"phase": "aggregate"}).encode())
        time.sleep(3)
        MX, rows = rrtable("data/fundamental-census-matrix.json")
        C = MX["cols"]
        upl = sorted([(MX["tickers"][i], C["upside_pct"][i]) for i in
                      range(MX["n"]) if isinstance(
                          (C.get("upside_pct") or [None]*MX["n"])[i],
                          (int, float))], key=lambda x: -x[1])[:6]
        dnl = sorted([(MX["tickers"][i], C["downside_pct"][i]) for i in
                      range(MX["n"]) if isinstance(
                          (C.get("downside_pct") or
                           [None]*MX["n"])[i], (int, float))],
                     key=lambda x: x[1])[:6]
        gate("D2_flagship", len(rows) >= 430
             and all(0 < r[1] <= 30 for r in rows),
             {"n": len(rows), "best_rr": rows[:10],
              "worst_rr": rows[-5:], "top_upside": upl,
              "lowest_downside": dnl})
    except Exception as e:
        gate("D2_flagship", False, str(e)[:340])

    try:
        r = lam.invoke(FunctionName="justhodl-etf-census", Payload=b"{}")
        json.loads(r["Payload"].read())
        MX, rows = rrtable("data/etf-census-matrix.json")
        lev = dict(zip(MX["tickers"], MX["cols"].get("leveraged")
                       or []))
        bottom = rows[-8:]
        lev_bottom = sum(1 for t, *_ in bottom if lev.get(t) == 1)
        gate("D3_etf", len(rows) >= 70 and lev_bottom >= 4,
             {"n": len(rows), "best_rr": rows[:8],
              "worst_rr": bottom, "lev_in_bottom8": lev_bottom})
    except Exception as e:
        gate("D3_etf", False, str(e)[:340])

    try:
        r = lam.invoke(FunctionName="justhodl-fi-census", Payload=b"{}")
        json.loads(r["Payload"].read())
        MX, rows = rrtable("data/fi-census-matrix.json")
        idx = {t: i for i, t in enumerate(MX["tickers"])}
        up = MX["cols"]["upside_pct"]
        srln_gt = (isinstance(up[idx["SRLN"]], (int, float))
                   and isinstance(up[idx["BIL"]], (int, float))
                   and up[idx["SRLN"]] > up[idx["BIL"]])
        gate("D4_fi", len(rows) >= 40 and srln_gt,
             {"n": len(rows), "best_rr": rows[:8],
              "srln_up": up[idx["SRLN"]], "bil_up": up[idx["BIL"]]})
    except Exception as e:
        gate("D4_fi", False, str(e)[:340])

    ok_p = True
    for pg, sid in (("fundamental-census.html", "OPS3529"),
                    ("etf-census.html", "OPSPAGE"),
                    ("fixed-income-census.html", "OPSPAGE")):
        pa = b""
        for _ in range(14):
            try:
                pa = fetch(f"https://justhodl.ai/{pg}?cb=%d"
                           % int(time.time()))
                if b"fxRR" in pa:
                    break
            except Exception:
                pass
            time.sleep(20)
        mm = re.search(rb'<script id="' + sid.encode()
                       + rb'">\n([\s\S]*?)</script>', pa)
        node = False
        if mm:
            with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                             delete=False) as f:
                f.write(mm.group(1).replace(b"__BT_URL__",
                                            b"https://x"))
                pth = f.name
            node = subprocess.run(["node", "--check", pth],
                                  capture_output=True).returncode == 0
        ok_p &= (b"fxRR" in pa) and node
    gate("D5_pages", ok_p, "fxRR served + node x3")

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3559.json").write_text(
        json.dumps({"ops": 3559, "fails": fails}))
sys.exit(0)
