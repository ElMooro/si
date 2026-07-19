"""ops 3527 — FUNDAMENTAL CENSUS: the S&P-500-wide fundamental sweep
(Khalid flagship). New justhodl-fundamental-census orchestrates warm
batches of 25 through the EXISTING fundamental-graphs verdict/elite
machinery (extend-don't-rebuild) and aggregates every cached doc into
hedge-fund boards: top/worst quality (2x elite + green − sev-weighted
red, tech excluded), 10 direction-aware metric leader/laggard boards,
the CAREFUL board (dilution >=4/8%/yr, integrity/accrual percentile
floors, sev-3 reds), sector strip, honest coverage. Biweekly Scheduler
(1st + 15th, 06:00 UTC). Flagship page pinned Research & Tools.

Gates:
  B1 CI battery (real verdict shape): score 10 exact w/ tech-exclusion,
     DILUTION_SEVERE+INTEGRITY flag_w=8, boards direction-true,
     dormancy named — rerun in CI
  B2 pilot: sync fundgraph warm x2 (50 universe names, cache-friendly)
     -> census aggregate -> doc: scored>=40, top board has known
     quality names w/ n_elite>=3, metric boards populated (n>=30 each),
     careful board real, coverage honest
  B3 boards sanity on REAL data: share-count board best is negative
     (buybacks) and worst positive (issuers); gross-margin best > worst
  B4 Scheduler cron(0 6 1,15 * ? *) phase-warm refresh
  B5 page served + node + pinned on SERVED manifest
  Final: fire the FULL warm chain async (cursor 0, refresh) so the
     complete universe populates in the background today
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
FG = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900, connect_timeout=10,
                                 retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
iam = boto3.client("iam")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3527"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3527_fundamental_census") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3527 — Fundamental Census (S&P sweep)")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        def doc(t, greens, reds, sy, fd=None, gm=40.0):
            return {"symbol": t,
                    "points": {"share_count_yoy_pct":
                               [["2026-03-31", sy]] if sy is not None else [],
                               "gross_margin_pct": [["2026-03-31", gm]]},
                    "factor_dna": {"axes": fd or []},
                    "verdicts": {"greens": greens, "reds": reds}}
        G = lambda k, e=False, b="fund": {"k": k, "sev": 1, "elite": e,
                                          "basis": b}
        R = lambda k, s, b="fund": {"k": k, "sev": s, "basis": b}
        r1 = m.extract(doc("ELITECO",
                           [G("roe", True), G("fcf", True), G("gm", True),
                            G("x"), G("y"), G("px", b="tech")],
                           [R("dio", 1)], -3.0), "Tech")
        r2 = m.extract(doc("DILUTER", [G("a")], [R("death", 3)], 9.0,
                           fd=[{"k": "beneish_m", "pct": 5.0}], gm=10.0),
                       "Energy")
        uni = [{"t": "ELITECO", "sector": "Tech"},
               {"t": "DILUTER", "sector": "Energy"},
               {"t": "MISSING", "sector": "Tech"}]
        c = m.build_census({"ELITECO": r1, "DILUTER": r2}, uni)
        gate("B1_ci", r1["score"] == 10 and r1["n_green"] == 5
             and r2["flags"] == ["DILUTION_SEVERE",
                                 "EARNINGS_INTEGRITY_LOW"]
             and r2["flag_w"] == 8
             and c["top_quality"][0]["t"] == "ELITECO"
             and c["careful"][0]["t"] == "DILUTER"
             and c["metric_boards"]["share_count_yoy_pct"]["best"][0]["v"]
             == -3.0
             and c["coverage"]["dormant_sample"] == ["MISSING"],
             {"r1_score": r1["score"], "r2": (r2["flags"], r2["flag_w"])})
    except Exception as e:
        gate("B1_ci", False, str(e)[:340])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1024,
                  description="Fundamental Census v1.0 (ops 3527)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c2 = lam.get_function_configuration(FunctionName=FN)
        if c2.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)

    uni = json.loads(s3c.get_object(
        Bucket=BUCKET, Key="data/forensic-screen.json")["Body"].read())
    tick = []
    seen = set()
    for r in uni.get("all_results") or []:
        t = r.get("ticker")
        if t and t not in seen:
            seen.add(t); tick.append(t)
    pilot = tick[:50]
    for i in range(0, 50, 25):
        lam.invoke(FunctionName=FG, Payload=json.dumps(
            {"warm": pilot[i:i+25], "periods": ["quarter"]}).encode())
    lam.invoke(FunctionName=FN,
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    try:
        D = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundamental-census.json")["Body"].read())
        cov = D["coverage"]; top = D["top_quality"]
        mb = D["metric_boards"]
        gate("B2_pilot", cov["scored"] >= 40
             and top and top[0]["n_elite"] >= 3
             and all(b["n"] >= 30 for b in mb.values())
             and len(D["careful"]) >= 1
             and cov["universe"] >= 450,
             {"scored": cov["scored"], "universe": cov["universe"],
              "top5": [(r["t"], r["score"], r["n_elite"])
                       for r in top[:5]],
              "bottom3": [(r["t"], r["score"]) for r in
                          D["bottom_quality"][:3]],
              "careful3": [(r["t"], r["flags"], r["flag_w"])
                           for r in D["careful"][:3]],
              "avg": D["summary"]["avg_score"],
              "n_flagged": D["summary"]["n_flagged"]})
        sh = mb["share_count_yoy_pct"]; gmb = mb["gross_margin_pct"]
        gate("B3_boards_real",
             sh["best"][0]["v"] < 0 and sh["worst"][0]["v"] > 0
             and gmb["best"][0]["v"] > gmb["worst"][0]["v"],
             {"buyback_top": sh["best"][:3],
              "issuer_top": sh["worst"][:3],
              "gm_best": gmb["best"][0], "gm_worst": gmb["worst"][0]})
    except Exception as e:
        gate("B2_pilot", False, str(e)[:340])
        gate("B3_boards_real", False, "pilot failed")

    try:
        role = iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
        arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
        body = dict(Name="fundamental-census-sched",
                    ScheduleExpression="cron(0 6 1,15 * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": arn, "RoleArn": role,
                            "Input": json.dumps({"phase": "warm",
                                                 "cursor": 0,
                                                 "refresh": True})},
                    State="ENABLED",
                    Description="Fundamental Census — biweekly full sweep")
        try: sch.create_schedule(**body)
        except sch.exceptions.ConflictException: sch.update_schedule(**body)
        gate("B4_schedule", True, sch.get_schedule(
            Name="fundamental-census-sched")["ScheduleExpression"])
    except Exception as e:
        gate("B4_schedule", False, str(e)[:240])

    pa, nav = b"", {}
    for _ in range(15):
        try:
            cb = int(time.time())
            pa = fetch(f"https://justhodl.ai/fundamental-census.html?cb={cb}")
            nav = json.loads(fetch(f"https://justhodl.ai/nav-manifest.json?cb={cb}"))
            if b"Fundamental Census" in pa: break
        except Exception: pass
        time.sleep(20)
    scr = re.findall(rb"<script>\n('use strict[\s\S]*?)</script>", pa)
    ok_n = False
    if scr:
        with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
            f.write(scr[0]); pth = f.name
        ok_n = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    pinned = any(c3.get("name") == "Research & Tools"
                 and any(p.get("href") == "/fundamental-census.html"
                         for p in c3.get("pages") or [])
                 for c3 in (nav.get("categories") or []))
    gate("B5_page", b"Fundamental Census" in pa and ok_n and pinned,
         {"served": b"Fundamental Census" in pa, "node": ok_n,
          "pinned": pinned})

    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"phase": "warm", "cursor": 0,
                                   "refresh": False}).encode())
    print("FULL-CHAIN FIRED async (cursor 0)")

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3527.json").write_text(
        json.dumps({"ops": 3527, "fails": fails}))
sys.exit(0)
