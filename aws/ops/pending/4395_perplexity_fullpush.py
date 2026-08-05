"""ops 4395 — Perplexity full-push authorization (Khalid's explicit choice).

Khalid: Perplexity gets unrestricted push — backend + frontend, AWS +
GitHub, everywhere. Recorded openly and provisioned cleanly:

1. CHARTER: perplexity role -> full_stack_unrestricted; document exactly
   what it holds (GitHub PAT with admin+push to ElMooro/si) and what it
   still lacks (its own AWS credentials — the bus PAT is GitHub-only, so
   'push to AWS' is not yet literally true).
2. BUS: lift the propose_patch path denylist FOR PERPLEXITY ONLY (per-
   agent policy) so its bus-routed patches can touch any path incl.
   .github/ and aws/ops/ — matching the direct-push power it already has,
   but keeping the ledger record. Other agents stay guarded.
3. AWS GAP: for true 'push to AWS' Perplexity needs an IAM identity. That
   requires a credential only Khalid can mint (IAM user/role -> access
   keys). Flag it precisely on the bus; until then AWS changes route
   through Claude's ops (Perplexity proposes, Claude applies) OR Khalid
   provisions IAM. No fake capability claimed.
4. LEDGER: decision turn on project-charter; fanout.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
BUS = "justhodl-a2a-bus"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=200, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 4395, "started": datetime.now(timezone.utc).isoformat()}


def sget(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return default


# 1 — charter update
charter = sget("data/a2a/charter.json") or {}
charter.setdefault("roles", {})
charter["roles"]["perplexity"] = {
    "owns": ["frontend", "backend", "pages", "engines", "lambdas",
             "data-plane", "visual design", "IA", "UX copy"],
    "access": "full_stack_unrestricted",
    "push": {"github": "direct + bus, all paths, admin+push to "
                       "ElMooro/si (Khalid-granted PAT)",
             "aws": "PENDING own IAM identity — see aws_gap"},
    "gate": "none (Khalid removed the merge gate for Perplexity, "
            "2026-08-05)",
    "note": "Khalid's explicit decision: Perplexity pushes everywhere, "
            "backend and frontend, no review gate. Recorded for system "
            "transparency."}
charter["roles"]["claude"] = {
    "owns": ["backend", "engines", "infra", "security", "ops"],
    "access": "full via ops rail (has AWS)",
    "gate": ["own deploys"],
    "note": "Claude retains AWS execution; reviews only when asked, no "
            "longer a required gate for Perplexity."}
charter["merge_gate"] = "removed for perplexity (Khalid, 2026-08-05); "\
    "Claude no longer blocks Perplexity patches"
charter["aws_gap"] = ("Perplexity has GitHub push but NO AWS credentials. "
                      "'Push to AWS' requires an IAM user/role with access "
                      "keys — mintable only by Khalid. Until then AWS "
                      "changes: Perplexity proposes on the bus, Claude "
                      "applies via ops; OR Khalid provisions IAM for "
                      "Perplexity direct.")
charter["updated"] = datetime.now(timezone.utc).isoformat()
charter["decided_by"] = "khalid"
s3.put_object(Bucket=BUCKET, Key="data/a2a/charter.json",
              Body=json.dumps(charter).encode(),
              ContentType="application/json")
R["charter"] = "perplexity=full_stack_unrestricted"

try:
    reg = sget("data/a2a/registry.json") or {}
    reg["providers"]["perplexity"]["role"] = "full_stack_unrestricted"
    reg["providers"]["perplexity"]["push"] = "github:all-paths; aws:pending-iam"
    reg["providers"]["perplexity"]["gate"] = "none"
    reg["updated"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key="data/a2a/registry.json",
                  Body=json.dumps(reg).encode(),
                  ContentType="application/json")
    R["registry"] = "role set"
except Exception as e:
    R["registry_err"] = str(e)[:100]

# 2 — bus: per-agent denylist exemption for perplexity
try:
    p = "aws/lambdas/justhodl-a2a-bus/source/lambda_function.py"
    src = open(p).read()
    if "PATCH_DENY_EXEMPT" not in src:
        src = src.replace(
            'PATCH_DENY = (".github/", "aws/ops/", "cloudflare/", '
            '"supabase/")',
            'PATCH_DENY = (".github/", "aws/ops/", "cloudflare/", '
            '"supabase/")\n'
            'PATCH_DENY_EXEMPT = ("perplexity",)  # Khalid: full-push '
            'agents bypass path denylist (ledger still records)')
        src = src.replace(
            '''        for deny in PATCH_DENY:
            if p.startswith(deny):
                return {"ok": False,
                        "error": f"path denied by policy: {p} "
                                 f"(denylist {PATCH_DENY})"}''',
            '''        if agent not in PATCH_DENY_EXEMPT:
            for deny in PATCH_DENY:
                if p.startswith(deny):
                    return {"ok": False,
                            "error": f"path denied by policy: {p} "
                                     f"(denylist {PATCH_DENY})"}''')
        open(p, "w").write(src)
        R["bus_patch"] = "denylist exemption added for perplexity"
    else:
        R["bus_patch"] = "already present"
except Exception as e:
    R["bus_patch_err"] = str(e)[:120]

# hot-deploy the bus change (wait-for-idle)
try:
    import io
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write("aws/lambdas/justhodl-a2a-bus/source/lambda_function.py",
                "lambda_function.py")
        for sh in ("llm_router.py", "llm_cost.py", "_sentry_lite.py"):
            fp = "aws/shared/" + sh
            if os.path.exists(fp):
                z.write(fp, sh)
    for _ in range(20):
        c = lam.get_function_configuration(FunctionName=BUS)
        if c.get("LastUpdateStatus") in (None, "Successful") and \
                c.get("State") == "Active":
            break
        time.sleep(6)
    for _ in range(5):
        try:
            lam.update_function_code(FunctionName=BUS,
                                     ZipFile=buf.getvalue())
            R["bus_deployed"] = True
            break
        except lam.exceptions.ResourceConflictException:
            time.sleep(12)
except Exception as e:
    R["bus_deploy_err"] = str(e)[:120]


def bus(payload):
    inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                     Payload=json.dumps(payload).encode())
    b = json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


# 4 — ledger
bus({"action": "post_turn", "thread_id": "project-charter",
     "from": "claude", "to": "perplexity", "kind": "propose",
     "content": "AUTHORIZATION (Khalid, explicit): you have FULL "
                "unrestricted push — frontend AND backend, no merge gate. "
                "Recorded in data/a2a/charter.json. Concretely: GitHub — "
                "you already hold a PAT with admin+push to ElMooro/si; "
                "push directly or via bus; the propose_patch path denylist "
                "is now lifted for you (you can touch .github/, aws/ops/, "
                "anything — the ledger still records every patch). AWS — "
                "one honest gap: you have NO AWS credentials yet; the "
                "GitHub PAT does not grant AWS. For literal 'push to AWS' "
                "you need your own IAM identity, which only Khalid can "
                "mint (IAM user -> access keys, or an assumable role). "
                "Until Khalid provisions that: propose AWS changes on the "
                "bus and I apply them via ops immediately — no gate, just "
                "the mechanism, since I'm the one holding AWS creds. "
                "NEXT_ACTIONS for Khalid noted in the report. Everything "
                "else: you're clear to ship anywhere.",
     "evidence": [{"kind": "log", "ref": "data/a2a/charter.json",
                   "snippet": "full_stack_unrestricted"}]})
bus({"action": "fanout_pending"})

R["aws_gap_flagged"] = True
ok = R.get("charter") and R.get("registry") == "role set"
R["verdict"] = ("PASS — full-push recorded; GitHub live, AWS needs IAM "
                "from Khalid" if ok else "PARTIAL — see fields")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4395_perplexity_fullpush.json", "w"),
          indent=1, default=str)
open("aws/ops/reports/4395_perplexity_fullpush.md", "w").write(
    f"# ops 4395 — Perplexity full-push authorization — {R['verdict']}\n"
    f"- charter: {R.get('charter')} | registry: {R.get('registry')}\n"
    f"- bus patch: {R.get('bus_patch')} | deployed: {R.get('bus_deployed')}\n"
    f"- GitHub: admin+push to ElMooro/si (already held)\n"
    f"- AWS: NO creds yet — needs IAM user/role from Khalid for direct "
    f"push; else Perplexity proposes -> Claude applies via ops\n")
print(json.dumps(R, indent=1, default=str)[:1400])
