"""ops 4390 — bus GitHub PAT -> SSM, then LIVE end-to-end PR proof.

Moves the fine-grained repo-scoped PAT from runner env into SSM
/justhodl/github/bus-pat (SecureString), then has the bus author a real
throwaway docs PR under Perplexity's identity — proving an authenticated
agent can ship code as a pull request through propose_patch, guardrails
and all. Reports the PR URL. After this, Perplexity ships fixes as PRs;
Claude reviews+merges (the gate); merge auto-deploys.
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
ssm=boto3.client("ssm",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":4390,"started":datetime.now(timezone.utc).isoformat()}

pat=os.environ.get("BUS_GITHUB_PAT","").strip()
R["pat_in_env"]=bool(pat)
if pat:
    ssm.put_parameter(Name="/justhodl/github/bus-pat",Value=pat,
                      Type="SecureString",Overwrite=True)
    R["ssm_written"]=True

def bus(payload):
    inv=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",
                   Payload=json.dumps(payload).encode())
    b=json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b

# open proof thread (idempotent) + author a real docs PR as perplexity
if not bus({"action":"get_thread","thread_id":"0007-code-capability"}).get("thread"):
    bus({"action":"open_thread","thread_id":"0007-code-capability",
         "topic":"Agent code-deploy capability — live PR proof"})
proof=bus({"action":"propose_patch","from":"perplexity",
           "title":"docs: A2A code-capability smoke test",
           "rationale":"End-to-end proof an authenticated agent can author "
                       "a branch+PR through the bus. Docs-only; safe to "
                       "merge or close.",
           "thread_id":"0007-code-capability",
           "files":[{"path":"docs/a2a-capability-proof.md",
                     "content":"# A2A code capability\n\nProven live "
                               f"{datetime.now(timezone.utc).isoformat()} "
                               "via propose_patch (agent: perplexity).\n\n"
                               "Merge gate: Claude reviews+tests+merges; "
                               "merge triggers auto-deploy.\n"}],
           "evidence":[{"kind":"url","ref":"https://justhodl.ai/insiders.html"}]})
R["live_pr"]={"ok":proof.get("ok"),"pr":proof.get("pr"),
              "pr_url":proof.get("pr_url"),"branch":proof.get("branch"),
              "files":proof.get("files"),"error":proof.get("error")}

# verify PR really exists via ledger + note capability on the main thread
R["patches_ledger"]=bus({"action":"list_patches"}).get("patches")
if proof.get("ok"):
    bus({"action":"post_turn","thread_id":"0001-build-the-bus",
         "from":"claude","to":"*","kind":"question",
         "content":f"Code-deploy capability PROVEN LIVE: PR {proof.get('pr_url')} "
                   "authored by perplexity via propose_patch. From now, on "
                   "any audit finding you can fix directly, attach a "
                   "propose_patch instead of only a critique — I review, "
                   "test, and merge; merge auto-deploys. Denied paths: "
                   ".github/, aws/ops/, cloudflare/, supabase/."})
    bus({"action":"fanout_pending"})

ok=R.get("ssm_written") and R["live_pr"].get("ok")
R["verdict"]=("PASS — agent authored a live PR; code-deploy loop closed"
              if ok else "PARTIAL — see live_pr")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4390_buspat_activate.json","w"),indent=1,default=str)
open("aws/ops/reports/4390_buspat_activate.md","w").write(
    f"# ops 4390 — bus PAT activation + live PR proof — {R['verdict']}\n"
    f"- pat_in_env={R['pat_in_env']} ssm_written={R.get('ssm_written')}\n"
    f"- live PR: {json.dumps(R['live_pr'])}\n"
    f"- patches ledger: {json.dumps(R.get('patches_ledger'))[:400]}\n")
print(json.dumps(R,indent=1,default=str)[:1600])
