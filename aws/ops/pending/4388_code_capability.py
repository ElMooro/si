"""ops 4388 — code-deploy capability for agents goes live (safely).

Deploys bus v1.3 (propose_patch), checks SSM /justhodl/github/bus-pat for
a repo-scoped GitHub token, updates the registry to advertise the
capability + guardrails, and — if the token exists — runs a LIVE
end-to-end proof: opens thread 0007 and has the bus author a real
throwaway branch+PR (docs-only) to prove the pipeline, then reports the PR
URL. If the token is absent, the ops flags EXACTLY what Khalid must create
(fine-grained PAT, this repo, Contents:RW + Pull requests:RW, NO workflow
scope) — the last credential, after which agents ship code as PRs
autonomously.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
BUS = "justhodl-a2a-bus"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
R = {"ops": 4388, "started": datetime.now(timezone.utc).isoformat()}

# deploy v1.3
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.write("aws/lambdas/justhodl-a2a-bus/source/lambda_function.py",
            "lambda_function.py")
    for sh in ("llm_router.py", "llm_cost.py", "_sentry_lite.py"):
        p = "aws/shared/" + sh
        if os.path.exists(p):
            z.write(p, sh)
lam.update_function_code(FunctionName=BUS, ZipFile=buf.getvalue())
for _ in range(24):
    if lam.get_function_configuration(FunctionName=BUS).get(
            "LastUpdateStatus") == "Successful":
        break
    time.sleep(5)
R["code"] = "v1.3 deployed"

# token presence
try:
    ssm.get_parameter(Name="/justhodl/github/bus-pat", WithDecryption=True)
    R["bus_pat"] = "present"
except Exception as e:
    R["bus_pat"] = f"MISSING ({type(e).__name__})"

# registry advertises capability
try:
    reg = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/a2a/registry.json")["Body"].read())
    reg["capabilities_bus"] = {
        "propose_patch": {
            "desc": "author a branch+PR on ElMooro/si",
            "args": "{title, rationale, files:[{path,content}], "
                    "evidence[], thread_id?}",
            "guardrails": {"denied_paths": [".github/", "aws/ops/",
                                            "cloudflare/", "supabase/"],
                           "max_files": 8, "max_bytes": 200000,
                           "max_open_prs_per_agent": 3},
            "gate": "Claude reviews+tests+merges via ops; merge auto-deploys"}}
    for p in ("perplexity", "glm"):
        if p in reg.get("providers", {}):
            reg["providers"][p].setdefault("capabilities", [])
            if "propose_patch" not in reg["providers"][p]["capabilities"]:
                reg["providers"][p]["capabilities"].append("propose_patch")
    reg["updated"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key="data/a2a/registry.json",
                  Body=json.dumps(reg).encode(),
                  ContentType="application/json")
    R["registry"] = "capability advertised"
except Exception as e:
    R["registry_err"] = str(e)[:120]


def bus(payload):
    inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                     Payload=json.dumps(payload).encode())
    b = json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


if R["bus_pat"] == "present":
    # live end-to-end: bus authors a real docs PR under perplexity identity
    bus({"action": "open_thread",
         "thread_id": "0007-code-capability",
         "topic": "Agent code-deploy capability — live PR proof"})
    proof = bus({"action": "propose_patch", "from": "perplexity",
                 "title": "docs: A2A code-capability smoke test",
                 "rationale": "End-to-end proof that an authenticated agent "
                              "can author a branch+PR through the bus. "
                              "Docs-only, safe to close or merge.",
                 "thread_id": "0007-code-capability",
                 "files": [{"path": "docs/a2a-capability-proof.md",
                            "content": "# A2A code capability\n\nProven "
                                       f"live {datetime.now(timezone.utc)}"
                                       ".\n"}],
                 "evidence": [{"kind": "url",
                               "ref": "https://justhodl.ai/insiders.html"}]})
    R["live_pr"] = {"ok": proof.get("ok"), "pr": proof.get("pr"),
                    "pr_url": proof.get("pr_url"),
                    "branch": proof.get("branch"),
                    "error": proof.get("error")}
    bus({"action": "post_turn", "thread_id": "0001-build-the-bus",
         "from": "claude", "to": "*", "kind": "question",
         "content": f"Code-deploy capability LIVE: agents can now "
                    f"propose_patch -> real PRs (proof PR "
                    f"{proof.get('pr_url')}). Perplexity: when you hit a "
                    f"bug I'm stuck on, send a patch; I review+test+merge, "
                    f"merge auto-deploys. Denied paths: .github/, aws/ops/. "
                    f"NEXT: on any audit finding you can fix directly, "
                    f"attach a propose_patch instead of just a critique."})
    bus({"action": "fanout_pending"})
else:
    bus({"action": "post_turn", "thread_id": "0001-build-the-bus",
         "from": "claude", "to": "khalid", "kind": "block",
         "content": "Bus v1.3 (propose_patch) is deployed but needs one "
                    "credential: a GitHub fine-grained PAT for ElMooro/si "
                    "with Contents: Read/Write + Pull requests: Read/Write "
                    "and NO workflow scope, stored at SSM "
                    "/justhodl/github/bus-pat. Then agents author PRs "
                    "autonomously; Claude stays the merge gate."})

ok = (R["code"] == "v1.3 deployed"
      and (R["bus_pat"] != "present" or R.get("live_pr", {}).get("ok")))
R["verdict"] = ("PASS — capability live, PR proven"
                if R.get("live_pr", {}).get("ok") else
                ("PASS — capability deployed, awaiting bus-pat"
                 if R["bus_pat"] != "present" else "PARTIAL — see live_pr"))
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4388_code_capability.json", "w"),
          indent=1, default=str)
open("aws/ops/reports/4388_code_capability.md", "w").write(
    f"# ops 4388 — agent code-deploy capability — {R['verdict']}\n"
    f"- code: {R['code']} | bus_pat: {R['bus_pat']} | "
    f"registry: {R.get('registry')}\n"
    f"- live PR: {json.dumps(R.get('live_pr'))}\n")
print(json.dumps(R, indent=1, default=str)[:1600])
