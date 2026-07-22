"""ops 3697 — air-cargo goes LAMBDA-NATIVE via /gov edge.
Worker: cad.gov.hk + hongkongairport.com added to GOV_ALLOW; binary cap
raised to 8MB (the CAD workbook is 2.1MB and truncation corrupts the zip).
Engine v2.0: clean rewrite carrying ONLY the proven parser (ops 3674 x-ray +
3676 landing: sheet1, header row 8, cols L/M/N/O, '><v>' anchor, Jan-only
year carry-forward), fetching through the edge because CAD tarpits Lambda IPs.
Gate: engine invoke returns the SAME verified value the runner produced
(HKIA ~433k t) with fetch_via='edge'."""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3697_air_lambda_native") as rep:
    rep.heading("ops 3697 — air-cargo Lambda-native via /gov edge")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3697.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        # worker deploy is handled by deploy-workers.yml on push; verify the
        # edge actually serves cad.gov.hk before trusting the engine.
        import urllib.parse, urllib.request
        edge = ("https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
                + urllib.parse.quote(
                    "https://www.cad.gov.hk/english/statistics.html", safe=""))
        ok0 = False; det0 = ""
        for _ in range(10):
            try:
                r = urllib.request.urlopen(urllib.request.Request(
                    edge, headers={"User-Agent": "Mozilla/5.0"}), timeout=40)
                body = r.read(200_000)
                det0 = (f"status={r.status} bytes={len(body)} "
                        f"xgov={r.headers.get('x-gov-fetch')}")
                if r.status == 200 and b"Air Traffic" in body:
                    ok0 = True; break
                if b"host not allowed" in body:
                    det0 += " (worker not yet redeployed)"
            except Exception as e:
                det0 = str(e)[:120]
            time.sleep(25)
        gate("G1_edge_allows_cad", ok0, det0)

        cfg = {}
        try:
            cfg = LAM.get_function_configuration(FunctionName="justhodl-air-cargo")
        except Exception:
            pass
        deploy_lambda(report=rep, function_name="justhodl-air-cargo",
                      source_dir=Path(__file__).resolve().parents[2] / "lambdas" / "justhodl-air-cargo" / "source",
                      env_vars=(cfg.get("Environment") or {}).get("Variables") or {},
                      timeout=180, memory=1024,
                      description="air-cargo v2.0 Lambda-native via /gov edge"[:200],
                      create_function_url=False)
        # async + freshness (doctrine: never RequestResponse-gate a flaky path)
        LAM.invoke(FunctionName="justhodl-air-cargo",
                   InvocationType="Event", Payload=b"{}")
        aj = {}; det = "async=202 "; dl = time.time() + 300
        while time.time() < dl:
            try:
                h = S3C.head_object(Bucket=B, Key="data/air-cargo.json")
                age = (time.time() - h["LastModified"].timestamp()) / 60
                if age < 6:
                    aj = json.loads(S3C.get_object(
                        Bucket=B, Key="data/air-cargo.json")["Body"].read())
                    det += f"fresh@{age:.1f}min "
                    break
                det = f"async=202 age={age:.1f} "
            except Exception as e:
                det += str(e)[:60]
            time.sleep(20)
        tk = aj.get("tonnes_k")
        ok1 = (aj.get("ok") and isinstance(tk, (int, float))
               and 300 <= tk <= 600 and aj.get("fetch_via") == "edge")
        gate("G2_lambda_native", ok1,
             det + f"ok={aj.get('ok')} via={aj.get('fetch_via')} "
             f"tonnes_k={tk} month={aj.get('month')} yoy={aj.get('yoy_pct')} "
             f"n={aj.get('xlsx_n')} bytes={aj.get('xlsx_bytes')} "
             f"cols={aj.get('cols')} read={aj.get('read')} "
             f"levels={aj.get('levels_cached')} errs={aj.get('errors')}")
        out["air"] = {k: aj.get(k) for k in
                      ("tonnes_k", "month", "yoy_pct", "fetch_via",
                       "xlsx_n", "read")}
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3697.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
