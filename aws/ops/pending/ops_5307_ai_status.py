"""ops_5307 -- AI status probe: print the live pipeline state (stage, endpoints, classifier metrics, errors with container log
lines), the current market read (llm_path / raw so an Anthropic 400 body is visible), and the ledger performance. Read-only except
one bounded market read when the pipeline is done and the last read was empty (to capture the API's own error text)."""
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-ai"
REGION = "us-east-1"
PUBLIC = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=60)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=910, connect_timeout=20, retries={"max_attempts": 0}))
FAILS, WARNS = [], []


def invoke(mode, body=None):
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"mode": mode, "body": body or {}}).encode())
    out = json.loads(r["Payload"].read() or b"{}")
    if r.get("FunctionError"):
        raise RuntimeError("%s: %s" % (mode, json.dumps(out)[:400]))
    if out.get("ok") is False:
        raise RuntimeError("%s refused: %s" % (mode, out.get("error")))
    return out.get("result", out)


with report("ops_5307_ai_status") as R:
    R.heading("ops 5307 -- AI status probe (pipeline, market read, ledger)")
    R.section("1. pipeline")
    p = invoke("pipeline/tick", {})            # a tick is the freshest view and advances the machine if it is due
    R.log("   %s: status %s stage %s (%s/%s) since %s | attempts %s" % (p.get("pipeline_id"), p.get("status"), p.get("stage"), (p.get("stage_index") or 0) + 1, len(p.get("stages") or []), p.get("stage_since"), json.dumps(p.get("attempts"))[:200]))
    R.log("   dataset %s %s" % (p.get("dataset_id"), json.dumps(p.get("dataset"))[:300]))
    R.log("   embedding %s realtime=%s deploy %s progress %s" % (p.get("embedding_endpoint"), p.get("embedding_realtime"), json.dumps(p.get("embedding_deploy"))[:200], json.dumps(p.get("embed_progress"))))
    R.log("   classifier job %s metrics %s billable %ss endpoint %s" % (p.get("classifier_job"), json.dumps(p.get("classifier_metrics")), p.get("classifier_billable_s"), p.get("classifier_endpoint")))
    R.log("   infer proof %s" % json.dumps(p.get("infer_proof"))[:400])
    R.log("   retrieval %s progress %s" % (p.get("retrieval_endpoint"), json.dumps(p.get("retrieval_progress"))))
    R.log("   market read (pipeline) %s" % json.dumps(p.get("market_read"))[:400])
    for h in (p.get("history") or [])[-12:]:
        R.log("      %s %s: %s" % (str(h.get("at"))[11:19], h.get("stage"), str(h.get("note"))[:200]))
    for e in (p.get("errors") or [])[-4:]:
        R.log("   error: %s" % str(e.get("error"))[:300])
        for line in (e.get("log") or [])[-8:]:
            R.log("      log: %s" % str(line)[:220])
    for w in (p.get("warnings") or []):
        WARNS.append("pipeline: %s" % str(w)[:200])
    if p.get("status") == "failed":
        FAILS.append("pipeline failed at %s: %s" % (p.get("stage"), p.get("error")))

    R.section("2. market read")
    rd = None
    try:
        rd = invoke("read")
    except Exception as e:
        R.log("   no read yet: %s" % str(e)[:200])
    if rd:
        read = rd.get("read") or {}
        R.log("   read %s at %s | playbook %s | llm_path %s | empty %s | parse_error %s" % (rd.get("read_id"), rd.get("generated_at"), (rd.get("playbook") or {}).get("available"), read.get("llm_path"), read.get("empty"), read.get("parse_error")))
        R.log("   raw[:400]: %s" % str(read.get("raw") or "")[:400])
        R.log("   stances %s" % json.dumps({k: (read.get(k) or {}).get("stance") for k in ("stocks", "bonds", "metals", "crypto")}))
        R.log("   overall: %s" % str(read.get("overall") or "")[:700])
        R.log("   opportunities: %s" % ", ".join("%s %s" % (o.get("ticker"), o.get("side")) for o in (read.get("best_opportunities") or [])))
        R.log("   calls: %s" % json.dumps([(c.get("ticker"), c.get("direction"), c.get("logged"), c.get("why")) for c in (rd.get("calls_logged") or [])])[:400])
        perf = rd.get("performance") or {}
        R.log("   performance: %s" % json.dumps({k: perf.get(k) for k in ("n_calls", "by_window", "error")})[:300])
        if (read.get("parse_error") or read.get("empty")) and p.get("status") in ("done", "failed", "idle"):
            R.log("   re-running one bounded read to capture the LLM path/body …")
            try:
                rd2 = invoke("market-read", {"force": True})
                r2 = rd2.get("read") or {}
                R.log("   -> llm_path %s | empty %s | raw[:400] %s" % (r2.get("llm_path"), r2.get("empty"), str(r2.get("raw") or "")[:400]))
                R.log("   -> stances %s overall %s" % (json.dumps({k: (r2.get(k) or {}).get("stance") for k in ("stocks", "bonds", "metals", "crypto")}), str(r2.get("overall") or "")[:300]))
                if r2.get("parse_error"):
                    FAILS.append("market read still empty: %s" % r2.get("llm_path"))
            except Exception as e:
                FAILS.append("market read: %s" % str(e)[:300])
    for w in WARNS:
        R.warn("   " + w)
    for f in FAILS:
        R.fail("   " + f)
    sys.exit(1 if FAILS else 0)
