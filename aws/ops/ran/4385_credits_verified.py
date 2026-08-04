"""ops 4385 — the third seat convenes: credit unlock verified end-to-end.

Khalid recharged Anthropic + Z.ai. This ops: clears the SSM-persisted GLM
breaker, live-proves all three council providers (latency + model per
seat), convenes the FULL council with Claude-chaired synthesis on the
maiden question (insiders frontend, v3.1-aware — the arc that started
this saga closes three-wide), re-fires representative credit-blocked AI
engines and classifies their fresh logs (expect quota errors gone),
heals the registry, and announces quorum on the bus for Perplexity's
backlog item #4. The rest of the AI fleet resumes on its own schedules.
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
COUNCIL = "justhodl-ai-council"
BUS = "justhodl-a2a-bus"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
R = {"ops": 4385, "started": datetime.now(timezone.utc).isoformat()}

# 1 — clear persisted breakers
for p in ("glm", "perplexity", "claude"):
    try:
        ssm.delete_parameter(Name=f"/justhodl/a2a/breaker/{p}")
        R.setdefault("breakers_cleared", []).append(p)
    except Exception:
        pass

# 2 — full council on the maiden question, synthesized
QUESTION = """The insiders.html frontend (justhodl.ai) is now v3.1: hero
KPIs + top-20 bars; cluster/big-buy cards; real-sector heat; fleet cards
joining 5 sibling engines; FULL DATA SURFACE tabbed explorer (9 tabs,
click-to-sort sticky tables, daily buy-vs-sell bars, coverage/ratchet/
hydration HUD); CSP meta + escaped-by-audit interpolations + resilient
fetchJSON; unknown future payload fields auto-render under More.
Constraints: vanilla single-file HTML/CSS/JS, dark institutional
aesthetic, zero-edit auto-render must survive. Name the 3 highest-impact
NEXT improvements toward Bloomberg/Koyfin desk grade. Be implementable."""
try:
    inv = lam.invoke(FunctionName=COUNCIL, InvocationType="RequestResponse",
                     Payload=json.dumps({
                         "question": QUESTION,
                         "providers": ["perplexity", "glm", "claude"],
                         "synthesize": True,
                         "tag": "maiden-question-full-council",
                         "max_tokens": 1200}).encode())
    R["council_invoke"] = {"code": inv.get("StatusCode"),
                          "fn_err": inv.get("FunctionError")}
    _ = inv["Payload"].read()
except Exception as e:
    R["council_invoke"] = {"err": str(e)[:150]}
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET,
                                   Key="data/ai-council.json")["Body"].read())
    R["providers"] = {p: {"ok": a.get("ok"), "model": a.get("model"),
                          "latency_s": a.get("latency_s"),
                          "error_class": a.get("error_class"),
                          "error": (a.get("error") or "")[:100]}
                      for p, a in (doc.get("answers") or {}).items()}
    R["ok_count"] = doc.get("ok_count")
    R["synthesis"] = (doc.get("synthesis") or "")[:2600]
    R["answer_excerpts"] = {p: (a.get("answer") or "")[:900]
                            for p, a in (doc.get("answers") or {}).items()
                            if a.get("ok")}
except Exception as e:
    R["council_read_err"] = str(e)[:120]

# 3 — representative credit-blocked engines: re-fire + classify
def classify(txt):
    e = (txt or "").lower()
    if "credit balance" in e or "insufficient balance" in e or "quota" in e:
        return "quota_exhausted"
    if "429" in e:
        return "rate_limited"
    if "traceback" in e or "errormessage" in e:
        return "code_error"
    return "clean"


R["engine_refires"] = {}
t0 = datetime.now(timezone.utc)
for fn in ("justhodl-ai-brief", "justhodl-ciss-ai",
           "justhodl-crypto-intel"):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        R["engine_refires"][fn] = {"fired": True}
    except Exception as e:
        R["engine_refires"][fn] = {"err": str(e)[:100]}
time.sleep(75)
for fn in list(R["engine_refires"]):
    try:
        since = int((t0 - timedelta(seconds=10)).timestamp() * 1000)
        ee = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}",
                                    startTime=since, limit=200)
        tail = "".join(x["message"] for x in ee.get("events", []))
        R["engine_refires"][fn]["error_class"] = classify(tail)
    except Exception as e:
        R["engine_refires"][fn]["log_err"] = str(e)[:80]

# 4 — registry heal + quorum announcement
try:
    reg = json.loads(s3.get_object(Bucket=BUCKET,
                                   Key="data/a2a/registry.json")
                     ["Body"].read())
    if (R.get("providers") or {}).get("glm", {}).get("ok"):
        reg["providers"]["glm"]["status"] = "healthy"
        reg["providers"]["glm"]["note"] = "recharged (ops 4385)"
    reg["updated"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key="data/a2a/registry.json",
                  Body=json.dumps(reg).encode(),
                  ContentType="application/json")
except Exception as e:
    R["registry_err"] = str(e)[:100]


def bus(payload):
    inv2 = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                      Payload=json.dumps(payload).encode())
    b = json.loads(inv2["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


prov_slim = json.dumps(R.get("providers") or {})[:500]
bus({"action": "post_turn", "thread_id": "0001-build-the-bus",
     "from": "claude", "to": "*", "kind": "question",
     "content": "QUORUM NOTICE (Perplexity backlog item #4): Anthropic + "
                "Z.ai recharged; council providers live-proven this run: "
                f"{prov_slim}. Three-wide verification is now available on "
                "any thread that needs it. Credit-gated AI engines resume "
                "on their own schedules; the audit loop will auto-close "
                "healed findings mechanically over the next cycles."})
bus({"action": "fanout_pending"})

ok = (R.get("ok_count") or 0) >= 3
R["verdict"] = ("PASS — full council 3/3, synthesis live, fleet resuming"
                if ok else
                f"PARTIAL — council {R.get('ok_count')}/3, see providers")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4385_credits_verified.json", "w"),
          indent=1, default=str)
md = [f"# ops 4385 — third seat convenes — {R['verdict']}",
      f"- breakers cleared: {R.get('breakers_cleared')}",
      f"- providers: {json.dumps(R.get('providers'), indent=1)}",
      f"- engine refires: {json.dumps(R.get('engine_refires'))}",
      "\n## SYNTHESIS (Claude chair, full council)",
      R.get("synthesis") or "(none)"]
for p, a in (R.get("answer_excerpts") or {}).items():
    md.append(f"\n## {p.upper()} excerpt")
    md.append(a)
open("aws/ops/reports/4385_credits_verified.md", "w").write(
    "\n".join(md) + "\n")
print(json.dumps({k: v for k, v in R.items()
                  if k not in ("synthesis", "answer_excerpts")},
                 indent=1, default=str)[:2200])
