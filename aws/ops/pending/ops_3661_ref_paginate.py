"""ops 3661 — exporters full coverage (Khalid: port calls in ALL major
exporting countries). portwatch v1.3.1 (ref paginated — ArcGIS-1000 struck on ports_ref): name variants (pusan/hai phong/yangshan/
bremen/kobe...) + fullname match + PORT_NATION fallback (ref country field
unreliable) + ref_misses forensics. Gate: >=13 export nations live incl
China/Korea/Japan/Singapore, misses listed honestly."""
import json, sys
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3661_ref_paginate") as rep:
    rep.heading("ops 3661 — exporters full-coverage v1.3")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3661.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:700]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:660]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        cfg = LAM.get_function_configuration(FunctionName="justhodl-portwatch")
        deploy_lambda(report=rep, function_name="justhodl-portwatch",
                      source_dir=Path(__file__).resolve().parents[2] / "lambdas" / "justhodl-portwatch" / "source",
                      env_vars=(cfg.get("Environment") or {}).get("Variables") or {},
                      timeout=cfg.get("Timeout", 300), memory=cfg.get("MemorySize", 768),
                      description="portwatch v1.3.1 (ref paginated — ArcGIS-1000 struck on ports_ref): full exporter coverage"[:200],
                      create_function_url=False)
        r = LAM.invoke(FunctionName="justhodl-portwatch",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/portwatch.json")["Body"].read())
        ex = j.get("exporters") or []
        nations = {e["country"] for e in ex}
        need = {"China", "Korea", "Japan", "Singapore"}
        ok1 = (not err and len(ex) >= 13 and need <= nations)
        gate("G1_full", ok1,
             f"err={err} ref_total={j.get('ports_ref_total')} ports_n={len(j.get('ports') or [])} nations={len(ex)} "
             f"have={sorted(nations)} slowing={j.get('exporters_slowing')} "
             f"board={[(e['country'], e['n_ports'], e['avg_vs_baseline_pct'], e['verdict']) for e in ex[:12]]} "
             f"misses={j.get('ref_misses')}")
        out["board"] = ex
        out["misses"] = j.get("ref_misses")
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3661.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
