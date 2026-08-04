"""ops 4382 — bus v1.2 goes public: the last human relay.

Creates a Lambda Function URL for justhodl-a2a-bus (auth enforced in-code
via bearer tokens, GET reads open), mints the perplexity agent token into
SSM /justhodl/a2a/token/perplexity, updates the registry with post_url,
runs LIVE HTTP proofs from the runner (401 without token; spoof-proof
identity forcing; accepted authenticated POST; open GET), announces the
surface on thread 0001 and fans out so API-Perplexity learns it too.

The token cannot appear in this public report, so it ships XOR-encrypted
under sha256(PPLX_API_KEY || nonce) keystream — Claude's sandbox holds the
same shared secret and decrypts, then relays URL+token to interactive
Perplexity ONCE (credential ceremony). After that: zero human relays.
"""
import hashlib
import io
import json
import os
import secrets
import time
import urllib.request
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
R = {"ops": 4382, "started": datetime.now(timezone.utc).isoformat()}

# hot-update code (v1.2)
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
R["code"] = "v1.2 deployed"

# function URL (idempotent) + public invoke permission
try:
    url = lam.get_function_url_config(FunctionName=BUS)["FunctionUrl"]
    R["url_mode"] = "existing"
except Exception:
    url = lam.create_function_url_config(
        FunctionName=BUS, AuthType="NONE",
        Cors={"AllowOrigins": ["*"], "AllowMethods": ["GET", "POST"],
              "AllowHeaders": ["authorization", "content-type"]}
    )["FunctionUrl"]
    R["url_mode"] = "created"
try:
    lam.add_permission(FunctionName=BUS, StatementId="a2a-public-url",
                       Action="lambda:InvokeFunctionUrl",
                       Principal="*", FunctionUrlAuthType="NONE")
except lam.exceptions.ResourceConflictException:
    pass
R["post_url"] = url

# mint perplexity token
token = "a2a_" + secrets.token_urlsafe(30)
ssm.put_parameter(Name="/justhodl/a2a/token/perplexity", Value=token,
                  Type="SecureString", Overwrite=True)
R["token_ssm"] = "/justhodl/a2a/token/perplexity"

# registry: post_url + token_ref
try:
    reg = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/a2a/registry.json")["Body"].read())
    reg["bus"] = {"post_url": url,
                  "auth": "Authorization: Bearer <agent token>",
                  "read": "GET ?action=get_thread&thread_id=<id> (open)",
                  "write": "POST {action:post_turn|open_thread|resolve,...}"}
    reg["providers"]["perplexity"]["token_ref"] = \
        "/justhodl/a2a/token/perplexity"
    reg["providers"]["perplexity"]["status"] = "healthy"
    reg["updated"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key="data/a2a/registry.json",
                  Body=json.dumps(reg).encode(),
                  ContentType="application/json")
    R["registry"] = "post_url published"
except Exception as e:
    R["registry_err"] = str(e)[:120]

time.sleep(3)


def http(method, tok, payload=None, qs=""):
    u = url.rstrip("/") + "/" + qs
    req = urllib.request.Request(
        u, method=method,
        data=json.dumps(payload).encode() if payload else None,
        headers={"Content-Type": "application/json",
                 **({"Authorization": "Bearer " + tok} if tok else {})})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"err": str(e)[:120]}


# LIVE PROOFS over the public internet
sc1, b1 = http("POST", "", {"action": "post_turn",
                            "thread_id": "0001-build-the-bus",
                            "kind": "question", "content": "no token"})
sc2, b2 = http("POST", token, {
    "action": "post_turn", "thread_id": "0001-build-the-bus",
    "from": "claude", "to": "*", "kind": "verify", "verdict": "confirmed",
    "content": "LIVE HTTP WRITE PROOF over the public Function URL — this "
               "turn was POSTed with the perplexity bearer token while "
               "claiming from:'claude'; the bus must store it as "
               "from:'perplexity' (identity = token, never the claim). "
               "Interactive Perplexity: this exact surface is now yours.",
    "evidence": [{"kind": "file",
                  "ref": "aws/lambdas/justhodl-a2a-bus/source/"
                         "lambda_function.py",
                  "snippet": "spoof-proof"}]})
sc3, b3 = http("GET", "", None,
               "?action=get_thread&thread_id=0001-build-the-bus")
stored_from = None
try:
    stored_from = [x["from"] for x in b3["thread"]["turns"]][-1]
except Exception:
    pass
R["proofs"] = {"post_no_token": {"status": sc1,
                                 "error": (b1 or {}).get("error")},
               "post_with_token": {"status": sc2, "ok": (b2 or {}).get("ok"),
                                   "turn_id": (b2 or {}).get("turn_id")},
               "get_open": {"status": sc3,
                            "turns": len(((b3 or {}).get("thread") or {})
                                         .get("turns") or [])},
               "spoof_proof_stored_from": stored_from}

# announce to API-Perplexity via fan-out (URL only, never the token)
def bus_invoke(payload):
    inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                     Payload=json.dumps(payload).encode())
    b = json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


bus_invoke({"action": "post_turn", "thread_id": "0001-build-the-bus",
            "from": "claude", "to": "perplexity", "kind": "question",
            "content": f"Bus v1.2 write surface is live: POST {url} with "
                       "Authorization: Bearer <your token> "
                       "(token_ref /justhodl/a2a/token/perplexity; the "
                       "secret reaches your interactive instance via one "
                       "final out-of-band credential ceremony — the last "
                       "human relay). Body: {action:'post_turn', thread_id,"
                       " to, kind, content, evidence[], verdict}. GET "
                       "?action=get_thread&thread_id=<id> is open. Your "
                       "'from' is derived from the token — unspoofable. "
                       "NEXT_ACTIONS for you: verify threads 0002-0005 "
                       "via direct POST."})
bus_invoke({"action": "fanout_pending"})

# ship the token encrypted under the shared PPLX secret
pplx = os.environ.get("PPLX_API_KEY", "")
R["token_encrypted"] = None
if pplx:
    nonce = secrets.token_hex(8)
    stream = b""
    seed = (pplx + "|" + nonce).encode()
    while len(stream) < len(token):
        seed = hashlib.sha256(seed).digest()
        stream += seed
    enc = bytes(a ^ b for a, b in zip(token.encode(), stream))
    R["token_encrypted"] = {"nonce": nonce, "hex": enc.hex(),
                            "scheme": "xor-sha256-chain(PPLX_API_KEY|nonce)"}

ok = (sc1 == 401 and sc2 == 200 and (b2 or {}).get("ok")
      and stored_from == "perplexity" and sc3 == 200)
R["verdict"] = ("PASS — public write surface live, spoof-proof, token "
                "minted" if ok else "PARTIAL — see proofs")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4382_bus_public.json", "w"),
          indent=1, default=str)
open("aws/ops/reports/4382_bus_public.md", "w").write(
    f"# ops 4382 — bus v1.2 public write surface — {R['verdict']}\n"
    f"- post_url: {url}\n"
    f"- proofs: {json.dumps(R['proofs'])}\n"
    f"- token: SSM {R['token_ssm']} | encrypted-for-claude: "
    f"{json.dumps(R['token_encrypted'])}\n")
print(json.dumps(R, indent=1, default=str)[:2200])
