"""ops 3263 — the PLAYBOOK ENGINE: Khalid's years of tested notes turned
into an extracted, evaluated rulebook. New lambda (env copied from the
donor justhodl-notes-intel per new-fn doctrine; no EB schedule v1 —
rule cap saturated; the brain changes slowly and any ops can invoke).
Deploy → invoke → prove: n_rules by family, six sample rules verbatim,
and the flagship yield-curve rule evaluated on live FRED data (inversion
onset, months elapsed, his 30-month marker date)."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
FN = "justhodl-playbook-engine"
DONOR = "justhodl-notes-intel"
AWS_DIR = Path(__file__).resolve().parents[2]

with report("3263_playbook_engine") as rep:
    fails, warns = [], []
    rep.heading("ops 3263 — his playbook, extracted and evaluated")

    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json")
                     .read_text())
    donor_env = (LAM.get_function_configuration(FunctionName=DONOR)
                 .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=donor_env, eb_rule_name=None,
                      eb_schedule=None,
                      timeout=cfg.get("timeout", 300),
                      memory=cfg.get("memory", 1024),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 40})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:90]}")

    if not fails:
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        for _ in range(30):
            time.sleep(8)
            try:
                d = json.loads(S3.get_object(
                    Bucket=BUCKET, Key="data/playbook-rules.json")
                    ["Body"].read())
            except Exception:
                d = {}
            if str(d.get("generated_at", "")) > mark:
                doc = d
                break
        if not doc:
            fails.append("playbook feed not fresh in window")
        else:
            rep.section("The rulebook")
            rep.kv(source_notes=doc.get("source_notes"),
                   n_rules=doc.get("n_rules"),
                   **{f"fam_{k}": v
                      for k, v in (doc.get("families") or {}).items()})
            for r in (doc.get("rules") or [])[:6]:
                rep.log(f"  [{r['family']}] {r['symbol'][:14]:<14} "
                        f"{r['text'][:96]}")
            fy = (doc.get("flagship") or {}).get("yield_curve") or {}
            rep.section("Flagship — his yield-curve timing rule, live")
            for k in ("series", "most_recent_inversion_onset",
                      "months_elapsed", "khalid_lag_months",
                      "lag_marker_date"):
                rep.log(f"  {k}: {fy.get(k)}")
            if doc.get("n_rules", 0) >= 30 and \
                    fy.get("months_elapsed") is not None:
                rep.ok(f"{doc['n_rules']} tested rules extracted; "
                       "flagship evaluated on live FRED data")
            else:
                warns.append("thin extraction or flagship eval "
                             "incomplete — inspect")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
