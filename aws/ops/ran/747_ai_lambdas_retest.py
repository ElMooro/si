"""ops/747 — re-test AI Lambdas after Anthropic credits were topped up.

The decisive proof is nobrainer-rationale's own counter: n_claude_ok
should jump from 0 to ~all-theses once credits are available. Also
re-invokes investor-agents and morning-intelligence.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=320, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

report = {"ops": 747, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "AI Lambdas re-test post credit top-up"}


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        return {"status_code": r.get("StatusCode"),
                "function_error": r.get("FunctionError"),
                "response": body}
    except Exception as e:
        return {"status_code": "error", "err": str(e)[:240]}


results = {}

# ── nobrainer-rationale — the decisive test (explicit ok/fail counter) ──
nb = invoke("justhodl-nobrainer-rationale")
n_ok = n_fail = None
try:
    inner = json.loads(json.loads(nb["response"])["body"])
    n_ok, n_fail = inner.get("n_claude_ok"), inner.get("n_claude_fail")
except Exception:
    pass
nb["n_claude_ok"] = n_ok
nb["n_claude_fail"] = n_fail
nb["response"] = nb.get("response", "")[:300]
nb["credits_ok"] = (n_ok or 0) > 0 and "credit balance" not in str(nb)
results["justhodl-nobrainer-rationale"] = nb

# ── investor-agents ──
ia = invoke("justhodl-investor-agents")
ia["credit_error_present"] = "credit balance" in (ia.get("response") or "")
ia["response"] = ia.get("response", "")[:300]
results["justhodl-investor-agents"] = ia

# ── morning-intelligence ──
mi = invoke("justhodl-morning-intelligence")
mi["credit_error_present"] = "credit balance" in (mi.get("response") or "")
mi["response"] = mi.get("response", "")[:300]
results["justhodl-morning-intelligence"] = mi

report["results"] = results

checks = {
    "nobrainer_claude_working": bool(results["justhodl-nobrainer-rationale"]
                                     .get("credits_ok")),
    "nobrainer_no_fails": (results["justhodl-nobrainer-rationale"]
                           .get("n_claude_fail") == 0),
    "investor_agents_ok": results["justhodl-investor-agents"].get("status_code") == 200
        and not results["justhodl-investor-agents"].get("credit_error_present"),
    "morning_intel_ok": results["justhodl-morning-intelligence"].get("status_code") == 200
        and not results["justhodl-morning-intelligence"].get("credit_error_present"),
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "AI RESTORED — credits live; nobrainer-rationale generating theses, "
    "no credit errors across AI Lambdas"
    if report["all_pass"]
    else "REVIEW — see checks (credits may still be propagating, or a "
         "non-credit issue remains)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/747_ai_lambdas_retest.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/747_ai_lambdas_retest.json")
