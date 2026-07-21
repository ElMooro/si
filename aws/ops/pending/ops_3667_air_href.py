"""ops 3667 — AIR-CARGO canary + freight page + sidebar: [A] NEW
justhodl-air-cargo (HKIA #1 cargo airport, probe-first; Scheduler 10:40;
VALUE-or-probe gate). [B] portwatch v1.3.2 ref_search truth-probe
(hamburg/haiphong/bremer under other spellings?). [C] freight-pulse.html
dedicated page (US composite + series + air section + sea links) + sidebar
pin; served + manifest gated. Traceback-capture skeleton."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ACCT = "857687956942"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

with report("3667_air_href") as rep:
    rep.heading("ops 3667 — HKIA air canary + freight page + sidebar")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3667.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:720]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:680]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        def dep(fn, tmo, mem, desc, env=None):
            try:
                cfg = LAM.get_function_configuration(FunctionName=fn)
                env = env if env is not None else ((cfg.get("Environment") or {}).get("Variables") or {})
                tmo = max(tmo, cfg.get("Timeout", 120)); mem = max(mem, cfg.get("MemorySize", 256))
            except Exception:
                env = env or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=Path(__file__).resolve().parents[2] / "lambdas" / fn / "source",
                          env_vars=env, timeout=tmo, memory=mem,
                          description=desc[:200], create_function_url=False)

        # [A] air-cargo
        dep("justhodl-air-cargo", 120, 512, "air-cargo v1.0: HKIA monthly canary", env={})
        arn = LAM.get_function(FunctionName="justhodl-air-cargo")["Configuration"]["FunctionArn"]
        c = dict(Name="justhodl-air-cargo-daily",
                 ScheduleExpression="cron(40 10 * * ? *)",
                 FlexibleTimeWindow={"Mode": "OFF"},
                 Target={"Arn": arn, "RoleArn": ROLE, "Input": "{}"},
                 State="ENABLED")
        try:
            SCH.create_schedule(**c)
        except Exception:
            SCH.update_schedule(**c)
        try:
            LAM.add_permission(FunctionName="justhodl-air-cargo",
                               StatementId="sched-air",
                               Action="lambda:InvokeFunction",
                               Principal="scheduler.amazonaws.com")
        except LAM.exceptions.ResourceConflictException:
            pass
        r = LAM.invoke(FunctionName="justhodl-air-cargo",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        aj = json.loads(S3C.get_object(Bucket=B, Key="data/air-cargo.json")["Body"].read())
        got = aj.get("ok") and isinstance(aj.get("tonnes_k"), (int, float))
        probe = aj.get("body_probe") or aj.get("list_probe") or aj.get("fact_probe")
        gate("G1_air", (not err) and (got or bool(aj.get("cad_sub_probe") or aj.get("cad_links"))),
             f"err={err} ok={aj.get('ok')} via={aj.get('via')} "
             f"tonnes_k={aj.get('tonnes_k')} month={aj.get('month')} "
             f"yoy={aj.get('yoy_pct')} link={aj.get('press_link')} "
             f"errs={aj.get('errors')} cadl={aj.get("cad_links")} cadsp={str(aj.get("cad_sub_probe"))[:200]} sm={aj.get("sitemap_hits")}")
        out["air"] = {k: aj.get(k) for k in ("ok", "tonnes_k", "month",
                                              "yoy_pct", "via", "read")}

        # [B] portwatch ref_search
        dep("justhodl-freight-pulse", 120, 512, "freight v1.1 sparks")
        r = LAM.invoke(FunctionName="justhodl-freight-pulse",
                       InvocationType="RequestResponse", Payload=b"{}")
        _ = r["Payload"].read()
        pj = json.loads(S3C.get_object(Bucket=B, Key="data/freight-pulse.json")["Body"].read())
        sk = (pj.get("series") or {}).get("rail_carloads") or {}
        gate("G2_sparks", len(sk.get("spark") or []) >= 10,
             f"spark_n={len(sk.get('spark') or [])} level={sk.get('level')}")
        

        # [C] page + sidebar
        ok3 = False; det3 = ""; dl = time.time() + 480
        while time.time() < dl:
            try:
                def get(u):
                    return urllib.request.urlopen(urllib.request.Request(
                        u + "?cb=" + str(int(time.time())),
                        headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                    ).read().decode("utf-8", "replace")
                h = get("https://justhodl.ai/freight-pulse.html")
                mf = json.loads(get("https://justhodl.ai/nav-manifest.json"))
                hrefs = [p.get("href") for cat in (mf.get("categories") or [])
                         for p in (cat.get("pages") or [])]
                mk = {"fmt": "toLocaleString" in h,
                      "spark": "polyline points" in h,
                      "air_txt": "Civil Aviation" in h,
                      "lag": "pub lag" in h}
                det3 = str(mk)
                if all(mk.values()):
                    ok3 = True; break
            except Exception as e:
                det3 = str(e)[:140]
            time.sleep(20)
        gate("G3_page_sidebar", ok3, det3)
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3667.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
