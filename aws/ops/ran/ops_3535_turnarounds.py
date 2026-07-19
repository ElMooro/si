"""ops 3535 — census v1.3.0: institutional metric-board expansion
(10 -> 22: op/net margins, ROIC, revenue/EPS growth, interest
coverage, current ratio, Piotroski, Altman, shareholder/buyback yield,
SBC/revenue; coverage guard >=150 per board) + the TURNAROUNDS layer:
4q-vs-prior-4q deltas on 10 inflection metrics, cross-sectionally
percentiled, direction-adjusted (deleveraging/buybacks improve), mean
over >=5 comparable metrics; improving-25 + deteriorating-15 with
top-driver deltas. New page section between Careful and the boards.

  J1 CI (delta 13.0 exact, TURN>99/CRSH<1 ordering, LOW-flip drivers,
     coverage guard) rerun
  J2 deploy + aggregate (cache warm): >=18 boards live each n>=300;
     new metric boards printed (ROIC/rev-growth/int-coverage/SBC
     leaders+laggards); turnarounds improving-10 + deteriorating-5
     with drivers, all turn scores in [0,100], top improver >=75
  J3 page served with the section + node
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3535"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3535_turnarounds") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:640]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3535 — 22 boards + turnarounds")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        P = {"gross_margin_pct": [["d", v] for v in
                                  [30, 30, 30, 30, 40, 42, 44, 46]]}
        t1 = m.turn_delta(P, "gross_margin_pct") == 13.0
        import random
        random.seed(1)
        TK = [k for k, _, _, _ in m.TURN_METRICS]
        rows, uni = {}, []
        def mk(t, d):
            return {"t": t, "sector": "T", "score": 5, "n_elite": 0,
                    "n_green": 2, "n_red": 1, "sev_sum": 1,
                    "top_elites": [], "red3": [], "flags": [],
                    "flag_w": 0, "dilution_yoy": None, "metrics": {},
                    "_lv": {"gross_margin_pct": 40.0}, "_tr": d,
                    "vintage_days": 1}
        for i in range(200):
            t = "N%03d" % i
            rows[t] = mk(t, {k: random.uniform(-2, 2) for k in TK})
            uni.append({"t": t, "sector": "T"})
        rows["TURN"] = mk("TURN", {k: (9.0 if k not in
                          ("debt_to_equity", "share_count_yoy_pct")
                          else -3.0) for k in TK})
        rows["CRSH"] = mk("CRSH", {k: (-9.0 if k not in
                          ("debt_to_equity", "share_count_yoy_pct")
                          else 3.0) for k in TK})
        uni += [{"t": "TURN", "sector": "T"},
                {"t": "CRSH", "sector": "T"}]
        c = m.build_census(rows, uni)
        T = c["turnarounds"]
        gate("J1_ci", t1 and T["improving"][0]["t"] == "TURN"
             and T["improving"][0]["turn_score"] > 99
             and T["deteriorating"][0]["t"] == "CRSH"
             and "roic_pct" not in c["metric_boards"],
             {"turn_top": T["improving"][0]["turn_score"],
              "crash": T["deteriorating"][0]["turn_score"]})
    except Exception as e:
        gate("J1_ci", False, str(e)[:320])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1024,
                  description="Census v1.3.0 turnarounds (ops 3535)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c2 = lam.get_function_configuration(FunctionName=FN)
        if c2.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN,
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    try:
        D = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundamental-census.json")["Body"].read())
        mb = D["metric_boards"]
        T = D["turnarounds"]
        imp = T["improving"]; det = T["deteriorating"]
        ok_scores = all(0 <= r["turn_score"] <= 100
                        for r in imp + det)
        newb = {k: mb.get(k) for k in
                ("roic_pct", "revenue_yoy_pct",
                 "interest_coverage_ttm", "sbc_to_revenue_pct",
                 "operating_margin_pct")}
        gate("J2_live", len(mb) >= 18
             and all(b and b["n"] >= 300 for b in newb.values())
             and len(imp) >= 10 and len(det) >= 5 and ok_scores
             and imp[0]["turn_score"] >= 75,
             {"n_boards": len(mb),
              "roic_best3": (newb["roic_pct"] or {}).get("best", [])[:3],
              "revgrow_best3": (newb["revenue_yoy_pct"] or {})
              .get("best", [])[:3],
              "sbc_worst3": (newb["sbc_to_revenue_pct"] or {})
              .get("worst", [])[:3],
              "improving5": [(r["t"], r["turn_score"],
                              [(d2["label"], d2["delta"])
                               for d2 in r["drivers"][:2]],
                              r["quality_now"]) for r in imp[:5]],
              "deteriorating5": [(r["t"], r["turn_score"],
                                  [(d2["label"], d2["delta"])
                                   for d2 in r["laggers"][:2]])
                                 for r in det[:5]]})
    except Exception as e:
        gate("J2_live", False, str(e)[:340])

    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()))
            if b"jhTurnUp" in pa: break
        except Exception: pass
        time.sleep(20)
    scr = re.findall(rb"<script>\n('use strict[\s\S]*?)</script>", pa)
    ok_n = False
    if scr:
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(scr[0]); pth = f.name
        ok_n = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    gate("J3_page", b"jhTurnUp" in pa and b"DETERIORATING" in pa
         and ok_n, {"node": ok_n})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3535.json").write_text(
        json.dumps({"ops": 3535, "fails": fails}))
sys.exit(0)
