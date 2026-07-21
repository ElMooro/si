"""ops 3671 — air v1.4 verdict via ASYNC pattern (3669/3670 died on invoke
transport: ConnectionClosed then ReadTimeout — doctrine: Event invoke + S3
freshness gate, never RequestResponse on flaky paths). Deploy, async invoke,
wait for data/air-cargo.json fresh, gate VALUE (tonnes) with xlsx forensics."""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=60, retries={"max_attempts": 1}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3671_air_async") as rep:
    rep.heading("ops 3671 — air v1.4 async landing")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3671.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:820]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:780]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        cfg = {}
        try:
            cfg = LAM.get_function_configuration(FunctionName="justhodl-air-cargo")
        except Exception:
            pass
        deploy_lambda(report=rep, function_name="justhodl-air-cargo",
                      source_dir=Path(__file__).resolve().parents[2] / "lambdas" / "justhodl-air-cargo" / "source",
                      env_vars=(cfg.get("Environment") or {}).get("Variables") or {},
                      timeout=120, memory=512,
                      description="air-cargo v1.4 CAD xlsx"[:200],
                      create_function_url=False)
        LAM.invoke(FunctionName="justhodl-air-cargo",
                   InvocationType="Event", Payload=b"{}")
        aj = {}
        det = "async=202 "
        dl = time.time() + 300
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
        got = aj.get("ok") and isinstance(aj.get("tonnes_k"), (int, float))
        gate("G1_air", bool(got),
             det + f"ok={aj.get('ok')} via={aj.get('via')} "
             f"tonnes_k={aj.get('tonnes_k')} month={aj.get('month')} "
             f"yoy={aj.get('yoy_pct')} row={str(aj.get('xlsx_row'))[:80]} "
             f"tail={aj.get('xlsx_tail')} n={aj.get('xlsx_n')} "
             f"xp={str(aj.get('xlsx_probe'))[:220]} errs={aj.get('errors')}")
        out["air"] = {k: aj.get(k) for k in ("ok", "tonnes_k", "month",
                                              "yoy_pct", "via", "read",
                                              "xlsx_n", "xlsx_tail")}
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3671.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
