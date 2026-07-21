"""ops 3614 — root cause was my own len(_wn)>=3 guard skipping the only
2-asset style; legacy '60_40' already exists → annotate it, drop the dup.
PASS = every style numeric (60_40 legacy carries the note), clones intact."""
import json, sys
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
FN = "justhodl-forward-returns"

with report("3614_sixty_fix") as rep:
    rep.heading("ops 3614 — 2-asset style guard fix + 60/40 dedupe")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:600]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-forward-returns" / "source",
                      env_vars=env, timeout=max(300, cfg.get("Timeout", 120)),
                      memory=max(768, cfg.get("MemorySize", 512)),
                      description=(cfg.get("Description") or "forward-returns")[:200],
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                      Key="data/forward-returns.json")["Body"].read())
        P = j.get("benchmark_portfolios") or {}
        styles = ("60_40", "permanent", "golden_butterfly", "swensen",
                  "ivy5", "bogle3", "dalio_growth", "risk_parity")
        ers = {k: (P.get(k) or {}).get("forward_er_pct") for k in styles}
        ok1 = (all(isinstance(v, (int, float)) for v in ers.values())
               and "sixty_forty" not in P
               and (P.get("60_40") or {}).get("updates")
               and any(k.startswith("clone_") for k in P)
               and isinstance(pl, dict) and not pl.get("errorMessage"))
        gate("G1_all_styles", ok1,
             f"ers={ers} dedup={'sixty_forty' not in P} "
             f"60_40_note={(P.get('60_40') or {}).get('updates')} "
             f"bw_clone_er={(P.get('clone_bridgewater') or {}).get('forward_er_pct')} "
             f"bw_as_of={(P.get('clone_bridgewater') or {}).get('as_of')} n={len(P)}")
        out["final_portfolios"] = {k: (P[k] or {}).get("forward_er_pct") for k in sorted(P)}
    except Exception as e:
        gate("G1_all_styles", False, str(e)[:340])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3614.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
