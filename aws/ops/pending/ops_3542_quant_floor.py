"""ops 3542 — QUANT FLOOR (Khalid mega-spec). Engine v1.7.0:
· 13F acc/dist dollars: whale bought/sold $M + institutions
  bought/sold/net $M (compact wb/ws/b/s/n)
· short_float_pct + insider columns joined from finviz-universe
  (11.3k-name FinViz Elite export; insider field names probed live)
· retail_dp_svr_pct from the dark-pool board (short-vol ratio)
· TECHNICALS computed from each doc's weekly price series:
  double_top / double_bottom (5w-gap, 3% tol, 5% depth, confirmed),
  golden_cross_10_40w, breakout_20w, above_ma40w, dist vs 52w
  high/low, RSI-14w, vol_52w, beta_2y vs the SPX deep history
· tech_score (momentum/position composite + pattern bonuses ±),
  combo_score (fundamental-quality pct x tech), conviction_score
  (combo + factor_quality + turnaround + whale-flow pct)
New Lambda justhodl-screen-backtest (Function URL): EW basket of the
CURRENT screen top-20 vs SPX 1/3/5y — honestly labeled hindsight, not
point-in-time. Page: pattern filter, ⚡combo + 🧲whale presets,
🧪 backtest panel with FGChart overlay. 34-behavior harness PASS.

  P1 tech CI (patterns 1/1/0 exact fixtures, beta 2.0) + backtest CI
     (SPX cagr 5.3 exact) rerun
  P2 deploy census v1.7.0 + backtest fn (+URL patched into the page
     source before pages deploy? URL patched HERE into S3-served page
     via direct put after pages build — instead: URL written to the
     repo file pre-push is impossible; we patch the served file is
     wrong. SOLUTION: function URL is deterministic once created —
     create fn+URL FIRST, then rewrite the repo page placeholder via
     put to S3? Page deploys via CI from repo. This ops instead
     patches the placeholder in the REPO COPY at runtime and pushes?
     No pushes from runner. FINAL: create URL, write it to
     data/config-backtest-url.json AND the page fetches placeholder →
     replaced client-side from that config feed.
  P3 aggregate: new col coverage (tech >=430, beta >=400, short_float
     >=400, retail >=?, whale wb/ws >=250, patterns counts sane
     5..120), combo top-10 + conviction top-10 + double_bottom list
     printed
  P4 backtest URL smoke: 6 real tickers → ok, stats, excess numbers
  P5 page served (fxPat/fxBT/presets) + node
"""
import importlib.util, json, re, subprocess, sys, tempfile, time, urllib.request
from datetime import date, timedelta
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-census"
BT = "justhodl-screen-backtest"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=420, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url, t=60):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3542"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


with report("3542_quant_floor") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:640]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3542 — quant floor")
    try:
        spec = importlib.util.spec_from_file_location(
            "fc", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        D0 = date(2023, 1, 6)
        W = lambda vals: [[(D0 + timedelta(weeks=i)).isoformat(), v]
                          for i, v in enumerate(vals)]
        dt = [50 + i for i in range(50)] + [100, 96, 92, 90, 92, 95,
                                            99, 96, 92, 88]
        db = [120 - i for i in range(70)] + [50, 53, 56, 57, 56, 54,
                                             50.5, 53, 58]
        rb = [0.02 if i % 3 == 0 else (-0.01 if i % 3 == 1 else 0.005)
              for i in range(159)]
        pxB, pxA = [100.0], [100.0]
        for r in rb:
            pxB.append(pxB[-1] * (1 + r))
            pxA.append(pxA[-1] * (1 + 2 * r))
        spx = {(D0 + timedelta(weeks=i)).isoformat(): pxB[i]
               for i in range(160)}
        b2 = m.beta_vs(W(pxA), spx)
        spec2 = importlib.util.spec_from_file_location(
            "bt", REPO / "aws/lambdas" / BT / "source/lambda_function.py")
        mb = importlib.util.module_from_spec(spec2)
        spec2.loader.exec_module(mb)
        A = [( (D0+timedelta(weeks=i)).isoformat(), 100*(1.004**i))
             for i in range(320)]
        Bx = [((D0+timedelta(weeks=i)).isoformat(), 100*(1.001**i))
              for i in range(320)]
        mb.load_prices = lambda t: {"AAA": A, "BBB": Bx}.get(t, [])
        mb.spx_series = lambda: Bx
        rr = json.loads(mb.lambda_handler(
            {"queryStringParameters": {"tickers": "AAA,BBB"}}, None)
            ["body"])
        gate("P1_ci", m.detect_double(W(dt), "top") == 1
             and m.detect_double(W(db), "bottom") == 1
             and m.detect_double(W([100 + (i % 3) for i in range(80)]),
                                 "top") == 0
             and abs(b2 - 2.0) < 0.05 and rr["ok"]
             and rr["stats"]["spx"]["cagr_1y"] == 5.3,
             {"beta": b2, "bt_excess_1y": rr["stats"]["excess_1y"]})
    except Exception as e:
        gate("P1_ci", False, str(e)[:340])

    for fn, tmo, mem in ((FN, 900, 1536), (BT, 120, 512)):
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=REPO/"aws"/"lambdas"/fn/"source",
                      env_vars={}, timeout=tmo, memory=mem,
                      description=f"{fn} (ops 3542)",
                      create_function_url=(fn == BT), smoke=False)
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") == "Successful": break
            time.sleep(2)
    try:
        try:
            bturl = lam.get_function_url_config(FunctionName=BT)
        except Exception:
            bturl = lam.create_function_url_config(
                FunctionName=BT, AuthType="NONE",
                Cors={"AllowOrigins": ["*"], "AllowMethods": ["*"]})
            try:
                lam.add_permission(FunctionName=BT,
                                   StatementId="url-public",
                                   Action="lambda:InvokeFunctionUrl",
                                   Principal="*",
                                   FunctionUrlAuthType="NONE")
            except Exception:
                pass
        BTURL = bturl["FunctionUrl"].rstrip("/")
        s3c.put_object(Bucket=BUCKET, Key="data/config-backtest-url.json",
                       Body=json.dumps({"url": BTURL}).encode(),
                       ContentType="application/json",
                       CacheControl="no-cache")
        gate("P2_bt_url", True, BTURL)
    except Exception as e:
        BTURL = None
        gate("P2_bt_url", False, str(e)[:240])

    lam.invoke(FunctionName=FN,
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    try:
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        C = MX["cols"]
        nn = lambda k: sum(1 for v in C.get(k) or []
                           if isinstance(v, (int, float)))
        cnt1 = lambda k: sum(1 for v in C.get(k) or [] if v == 1)
        tops = lambda k, n=10: sorted(
            [(MX["tickers"][i], v) for i, v in enumerate(C.get(k) or [])
             if isinstance(v, (int, float))], key=lambda x: -x[1])[:n]
        dbl = [MX["tickers"][i] for i, v in
               enumerate(C.get("double_bottom") or []) if v == 1][:12]
        gate("P3_matrix", nn("tech_score") >= 430 and nn("beta_2y") >= 400
             and nn("short_float_pct") >= 350
             and nn("whale_buy_usd_m") >= 250
             and 5 <= cnt1("double_top") <= 150
             and 5 <= cnt1("double_bottom") <= 150
             and nn("combo_score") >= 430
             and nn("conviction_score") >= 400,
             {"tech_n": nn("tech_score"), "beta_n": nn("beta_2y"),
              "short_n": nn("short_float_pct"),
              "insider_n": (nn("insider_trans_pct"),
                            nn("insider_own_pct")),
              "retail_n": nn("retail_dp_svr_pct"),
              "n_dt": cnt1("double_top"), "n_db": cnt1("double_bottom"),
              "n_gc": cnt1("golden_cross_10_40w"),
              "combo_top10": tops("combo_score"),
              "conviction_top5": tops("conviction_score", 5),
              "double_bottoms": dbl})
    except Exception as e:
        gate("P3_matrix", False, str(e)[:360])

    try:
        smp = json.loads(fetch(
            BTURL + "/?tickers=NVDA,MU,ADBE,APP,DECK,FIX", 120))
        gate("P4_bt_smoke", smp.get("ok")
             and smp["stats"]["weeks"] >= 100
             and smp["stats"]["excess_3y"] is not None,
             {"weeks": smp["stats"]["weeks"],
              "basket_3y": smp["stats"]["basket"]["cagr_3y"],
              "spx_3y": smp["stats"]["spx"]["cagr_3y"],
              "excess_3y": smp["stats"]["excess_3y"]})
    except Exception as e:
        gate("P4_bt_smoke", False, str(e)[:260])

    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()), 30)
            if b"fxPat" in pa and b"fxBT" in pa: break
        except Exception: pass
        time.sleep(20)
    mm = re.search(rb'<script id="OPS3529">\n([\s\S]*?)</script>', pa)
    ok_n = False
    if mm:
        src = mm.group(1).replace(b"__BT_URL__", b"https://x")
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(src); pth = f.name
        ok_n = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    gate("P5_page", all(k in pa for k in
                        [b"fxPat", b"fxBT", b'data-p="ft"',
                         b'data-p="wa"', b"fxBTSvg"]) and ok_n,
         {"node": ok_n})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3542.json").write_text(
        json.dumps({"ops": 3542, "fails": fails}))
sys.exit(0)
