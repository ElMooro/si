"""ops 3613 — sixty_forty forensic: print the stored entry verbatim + full
benchmark key list; engine backstop self-heals any None ER and logs MISSING.
PASS = all 7 styles numeric + rp + clones intact + entry dump recorded."""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
FN = "justhodl-forward-returns"

with report("3613_sixty_forensic") as rep:
    rep.heading("ops 3613 — sixty_forty forensic + backstop")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
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
        logs = ""
        try:
            import base64
            r2 = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                            LogType="Tail", Payload=b"{}")
            logs = base64.b64decode(r2.get("LogResult", "")).decode("utf-8", "replace")[-1200:]
        except Exception:
            pass
        j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                      Key="data/forward-returns.json")["Body"].read())
        P = j.get("benchmark_portfolios") or {}
        entry = P.get("sixty_forty")
        styles = ("sixty_forty", "permanent", "golden_butterfly", "swensen",
                  "ivy5", "bogle3", "risk_parity")
        ers = {k: (P.get(k) or {}).get("forward_er_pct") for k in styles}
        ok1 = all(isinstance(v, (int, float)) for v in ers.values()) and len(P) >= 12
        gate("G1_styles", ok1,
             f"n={len(P)} keys={sorted(P.keys())} ers={ers} "
             f"sixty_entry={json.dumps(entry)[:220]} "
             f"port_logs={[l for l in logs.splitlines() if 'portfolios]' in l][:4]}")
        out["sixty_entry"] = entry
        out["all_keys"] = sorted(P.keys())
    except Exception as e:
        gate("G1_styles", False, str(e)[:340])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3613.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
