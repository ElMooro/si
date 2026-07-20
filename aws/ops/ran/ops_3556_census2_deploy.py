"""ops 3556 — ETF CENSUS + FIXED-INCOME CENSUS go live (Khalid spec:
same engine, same capabilities, for ETFs and for fixed income).
Shared kernel aws/shared/census_lib.py (extracted verbatim from the
flagship, identity-CI'd) bundled into two NEW engines:
justhodl-etf-census (flow-feed universe ∪ 66 core wrappers; FMP
info + weekly price; beta_spy; leveraged decay guard +12 risk) and
justhodl-fi-census (46 curated bond ETFs / 15 segments; beta_tlt
duration proxy; FRED curve + cds-proxy credit blocks). Pages
etf-census.html + fixed-income-census.html (explorer: Σ chips,
adder-facets, risk terciles, patterns, presets, CSV, chart-pro
links, FI credit strip) — 12-behavior dual harness PASS. Biweekly
Schedulers 2nd & 16th.

  A1 deploy both (zip markers: census_lib import + VERSION)
  A2 full first runs (sync): ETF n>=110, beta_spy n>=90, leveraged
     >=8, flow vocab printed; FI n>=40, LIVE duration ladder ordered
     EDV>TLT>...>BIL, curve 2s10s + HY OAS printed
  A3 Schedulers created (cron 2,16 monthly)
  A4 both pages served + node; FORCE pins on the SERVED manifest
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
sch = boto3.client("scheduler", region_name="us-east-1")
ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"


def fetch(url, t=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3556"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


def zip_marker(fn, mark):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    zf = zipfile.ZipFile(io.BytesIO(
        urllib.request.urlopen(loc, timeout=60).read()))
    names = zf.namelist()
    src = zf.read("lambda_function.py")
    return (mark in src) and ("census_lib.py" in names)


with report("3556_census2_deploy") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:660]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3556 — ETF + FI census live")
    env = {"FMP_API_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
           "FRED_API_KEY": "2f057499936072679d8843d7fce99989"}
    marks = {}
    for fn, tmo, mem in (("justhodl-etf-census", 900, 1024),
                         ("justhodl-fi-census", 600, 768)):
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=REPO/"aws"/"lambdas"/fn/"source",
                      env_vars=env, timeout=tmo, memory=mem,
                      description=f"{fn} v1.0.0 (ops 3556)",
                      create_function_url=False, smoke=False)
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        marks[fn] = zip_marker(fn, b'VERSION = "1.0.0"')
    gate("A1_zip", all(marks.values()), marks)

    try:
        r = lam.invoke(FunctionName="justhodl-etf-census",
                       Payload=b"{}")
        json.loads(r["Payload"].read())
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/etf-census-matrix.json")["Body"].read())
        C = MX["cols"]
        nn = lambda k: sum(1 for v in C.get(k) or []
                           if isinstance(v, (int, float)))
        lev = sum(1 for v in C.get("leveraged") or [] if v == 1)
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/etf-census.json")["Body"].read())
        tt = sorted([(MX["tickers"][i], v) for i, v in
                     enumerate(C.get("tech_score") or [])
                     if isinstance(v, (int, float))],
                    key=lambda x: -x[1])[:6]
        gate("A2_etf", MX["n"] >= 110 and nn("beta_spy") >= 90
             and lev >= 8 and nn("tech_score") >= 100,
             {"n": MX["n"], "beta_n": nn("beta_spy"),
              "leveraged_n": lev,
              "flow_cols": doc["coverage"]["flow_cols"][:8],
              "tech_top": tt,
              "decay_board_n": len(doc["boards"]["decay_careful"]),
              "dbl_bottoms": doc["boards"]["double_bottoms"][:8]})
    except Exception as e:
        gate("A2_etf", False, str(e)[:320])

    try:
        r = lam.invoke(FunctionName="justhodl-fi-census", Payload=b"{}")
        json.loads(r["Payload"].read())
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fi-census-matrix.json")["Body"].read())
        bt = dict(zip(MX["tickers"], MX["cols"].get("beta_tlt") or []))
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fi-census.json")["Body"].read())
        lad = doc["boards"]["duration_ladder"]
        order_ok = (isinstance(bt.get("EDV"), (int, float))
                    and isinstance(bt.get("TLT"), (int, float))
                    and isinstance(bt.get("SHY"), (int, float))
                    and isinstance(bt.get("BIL"), (int, float))
                    and bt["EDV"] > bt["TLT"] > bt["SHY"] > bt["BIL"])
        gate("A2_fi", MX["n"] >= 40 and order_ok
             and doc["curve"].get("curve_2s10s_bp") is not None
             and (doc["credit"]["corporate"].get("hy_oas") or {})
             .get("oas_bp") is not None,
             {"n": MX["n"],
              "ladder": {k: bt.get(k) for k in
                         ("EDV", "TLT", "IEF", "SHY", "BIL")},
              "ladder_top4": lad[:4],
              "curve_2s10s_bp": doc["curve"].get("curve_2s10s_bp"),
              "y10": doc["curve"].get("y10"),
              "hy_oas_bp": doc["credit"]["corporate"]["hy_oas"]
              .get("oas_bp"),
              "regime": doc["credit"].get("regime")})
    except Exception as e:
        gate("A2_fi", False, str(e)[:320])

    ok_s = True
    for name, fn, cron in (("etf-census-sched", "justhodl-etf-census",
                            "cron(30 6 2,16 * ? *)"),
                           ("fi-census-sched", "justhodl-fi-census",
                            "cron(0 7 2,16 * ? *)")):
        try:
            arn = lam.get_function(FunctionName=fn)["Configuration"
                                                    ]["FunctionArn"]
            try:
                sch.get_schedule(Name=name)
            except Exception:
                sch.create_schedule(
                    Name=name, ScheduleExpression=cron,
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": arn, "RoleArn": ROLE,
                            "Input": "{}"})
        except Exception as e:
            ok_s = False
            print("[sched]", name, str(e)[:120])
    gate("A3_sched", ok_s, "2nd & 16th monthly")

    pages_ok, node_ok, pins_ok = True, True, False
    for pg in ("etf-census.html", "fixed-income-census.html"):
        pa = b""
        for _ in range(14):
            try:
                pa = fetch(f"https://justhodl.ai/{pg}?cb=%d"
                           % int(time.time()))
                if b"OPSPAGE" in pa:
                    break
            except Exception:
                pass
            time.sleep(20)
        mm = re.search(rb'<script id="OPSPAGE">\n([\s\S]*?)</script>',
                       pa)
        this_ok = False
        if mm:
            with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                             delete=False) as f:
                f.write(mm.group(1)); pth = f.name
            this_ok = subprocess.run(["node", "--check", pth],
                                     capture_output=True
                                     ).returncode == 0
        pages_ok &= (b"OPSPAGE" in pa)
        node_ok &= this_ok
    try:
        man = json.loads(fetch("https://justhodl.ai/nav-manifest.json?cb=%d"
                               % int(time.time())))
        cats = {c["name"]: [p["href"] for p in c.get("pages") or []]
                for c in man.get("categories") or []}
        rt = cats.get("Research & Tools") or []
        pins_ok = any("etf-census" in h for h in rt) and \
            any("fixed-income-census" in h for h in rt)
    except Exception as e:
        print("[manifest]", str(e)[:120])
    gate("A4_pages", pages_ok and node_ok and pins_ok,
         {"node": node_ok, "pins": pins_ok})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3556.json").write_text(
        json.dumps({"ops": 3556, "fails": fails}))
sys.exit(0)
