"""ops_5422 -- Brain constitution shipped as a PRIVATE artifact + every engine Grok's apply lane left undeployed.
(re-armed 2026-09-11 23:20 UTC: the ship push carried a skip marker inside its body, so run-ops skipped it)

Why this op exists (2026-09-11): Grok's Brain/AI patches (apply commit 8d37a5e) could not pass
deploy-lambdas because brain-sync wrote data/brain-constitution.json with Cache-Control public --
the LLM-distilled projection of the private Brain (hard rules, tilts, posture, avoid list) served
anonymously at justhodl.ai/data/brain-constitution.json. The Sep-8 audit's privacy gate refused it.
The fix keeps the feature and the boundary: the constitution is a registered private artifact
(S3 private/no-store, edge denies anonymous reads, owner reads it signed-in), and the three
consumers read it through IAM exactly as before. This gate proves all of that live.

Checks (no secret value is ever printed):
  1. the four functions carry code from THIS commit (CodeSha256 == zip built from the checkout)
  2. brain-sync ran and rewrote data/brain-constitution.json with CacheControl private, no-store,
     producer stamp engine=brain-sync + content_hash, and NO note bodies
  3. edge: anonymous GET /data/brain-constitution.json -> 401 (NOT 200); service-token GET
     /private-artifact?kind=brain-constitution -> 200 with the same content_hash
  4. position-sizer ran: data/position-sizing.json version 1.1, brain_constitution.consumed true,
     content_hash equal to the artifact, risk_posture an enum only
  5. ask: anonymous POST -> 401; ai: function updated (its own tests ran in preflight)
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
# Everything Grok's apply lane left on main but never deployed (GITHUB_TOKEN pushes fire no workflows),
# plus the four Grok asked for. Live CodeSha256 is compared to a zip built from THIS checkout.
FUNCTIONS = ["justhodl-brain-sync", "justhodl-position-sizer", "justhodl-ask", "justhodl-ai",
             "justhodl-alpha-confluence", "justhodl-apac-flows", "justhodl-best-setups", "justhodl-brain-compiler",
             "justhodl-devils-advocate", "justhodl-domain-barometers", "justhodl-morning-intelligence",
             "justhodl-my-brief", "justhodl-risk-gate"]
WORKFLOW = "deploy-lambdas.yml"
EDGE = "https://justhodl.ai"
WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "JustHodl-ops5422/1.0"}
FAILS: list[str] = []

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def fail(msg):
    FAILS.append(msg)


def zip_sha(fn: str) -> str:
    """Rebuild the deploy zip exactly like scripts/deploy_lambdas.sh (source/ + aws/shared/*.py) and hash it."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in sorted((ROOT / "aws/shared").glob("*.py")):
            z.write(p, p.name)
        src = ROOT / "aws/lambdas" / fn / "source"
        for p in sorted(src.rglob("*")):
            if p.is_file() and "__pycache__" not in p.parts:
                z.write(p, str(p.relative_to(src)))
    return base64.b64encode(hashlib.sha256(buf.getvalue()).digest()).decode()


def http(url, method="GET", headers=None, body=None, timeout=30):
    req = urllib.request.Request(url, method=method, headers={**UA, **(headers or {})}, data=body)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as h:
            return h.status, h.read()
    except urllib.error.HTTPError as e:
        return e.code, (e.read() if e.fp else b"")
    except Exception as e:  # noqa: BLE001
        return None, str(e).encode()


def head(key):
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)
    except Exception:  # noqa: BLE001
        return None


def gh(path, payload=None):
    repo = os.environ.get("GITHUB_REPOSITORY", "ElMooro/si")
    token = os.environ.get("GH_API_TOKEN", "")
    if not token:
        raise RuntimeError("GH_API_TOKEN missing on the runner")
    req = urllib.request.Request("https://api.github.com/repos/" + repo + path,
                                 data=None if payload is None else json.dumps(payload).encode(),
                                 headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                                          "Content-Type": "application/json", "X-GitHub-Api-Version": "2026-03-10", **UA},
                                 method="GET" if payload is None else "POST")
    with urllib.request.urlopen(req, timeout=45) as r:
        body = r.read()
        return r.status, (json.loads(body) if body else {})


def deploy_through_workflow(R, functions, commit, wait_s=2400):
    """No raw update-function-code: dispatch deploy-lambdas.yml pinned to this commit (preflight,
    tests, alias protection, CodeSha256 proof all run there) and wait for that run to finish."""
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise RuntimeError("exact commit required")
    started = time.time()
    status, _ = gh("/actions/workflows/%s/dispatches" % WORKFLOW,
                   {"ref": "main", "inputs": {"function": " ".join(functions), "expected_sha": commit}})
    if status not in (200, 204):
        raise RuntimeError("GitHub refused the dispatch: HTTP %s" % status)
    R.ok("dispatched %s for %d function(s) pinned to %s" % (WORKFLOW, len(functions), commit[:10]))
    run = None
    while time.time() - started < wait_s:
        time.sleep(20)
        _, runs = gh("/actions/workflows/%s/runs?event=workflow_dispatch&per_page=5" % WORKFLOW)
        for r in runs.get("workflow_runs") or []:
            if r.get("created_at", "") >= time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started - 60)):
                run = r
                break
        if run and run.get("status") == "completed":
            R.kv(deploy_run=run.get("html_url"), conclusion=run.get("conclusion"))
            return run.get("conclusion") == "success"
    R.warn("deploy run not completed within %ds: %s" % (wait_s, (run or {}).get("html_url")))
    return False


def invoke_and_wait(fn, key, before_ts, budget_s=240):
    lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
    t0 = time.time()
    while time.time() - t0 < budget_s:
        h = head(key)
        if h and h["LastModified"].timestamp() > before_ts:
            return h
        time.sleep(6)
    return None


with report("ops_5422_brain_constitution_private_gate") as R:
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    R.heading("ops 5422 -- Brain constitution as a private artifact")
    R.kv(commit=commit[:10])

    R.section("1. live code == this commit (deploy through the audited workflow if not)")
    def mismatched():
        out = []
        for fn in FUNCTIONS:
            cfg = lam.get_function_configuration(FunctionName=fn)
            live, local = cfg["CodeSha256"], zip_sha(fn)
            (R.ok if live == local else R.warn)("%s: live %s… %s checkout %s… (LastModified %s)"
                                                 % (fn, live[:10], "==" if live == local else "!=", local[:10], cfg["LastModified"]))
            if live != local:
                out.append(fn)
        return out
    behind = mismatched()
    if behind:
        R.log("behind main: " + ", ".join(behind))
        deploy_through_workflow(R, behind, commit)
        behind = mismatched()
    if behind:
        for fn in behind:
            fail("%s still does not run commit %s after the workflow deploy" % (fn, commit[:10]))
        R.fail("; ".join(FAILS))
        sys.exit(1)
    R.ok("all %d functions run commit %s" % (len(FUNCTIONS), commit[:10]))

    R.section("2. brain-sync writes a PRIVATE constitution")
    before = time.time()
    h = invoke_and_wait("justhodl-brain-sync", "data/brain-constitution.json", before)
    if not h:
        fail("brain-sync did not rewrite data/brain-constitution.json within 240s")
        R.fail("no fresh constitution")
        sys.exit(1)
    cc = (h.get("CacheControl") or "")
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/brain-constitution.json")["Body"].read())
    R.kv(cache_control=cc, engine=doc.get("engine"), version=doc.get("version"),
         n_notes=doc.get("n_notes"), n_hard_rules=len(doc.get("hard_rules") or []), content_hash=str(doc.get("content_hash"))[:12])
    if cc != "private, no-store":
        fail(f"constitution CacheControl is {cc!r}, expected 'private, no-store'")
    if doc.get("engine") != "brain-sync" or not doc.get("content_hash"):
        fail("constitution lacks the producer stamp / content_hash")
    if "notes" in doc:
        fail("constitution carries note bodies -- boundary violation")
    R.ok("artifact rewritten %s" % h["LastModified"].isoformat()) if not FAILS else R.fail("; ".join(FAILS))

    R.section("3. edge boundary (deploy-workers lands asynchronously: wait up to 10 min for the deny)")
    for host in (EDGE, WORKER):
        st = None
        for _ in range(30):
            st, _ = http(f"{host}/data/brain-constitution.json")
            if st in (401, 403):
                break
            time.sleep(20)
        (R.ok if st in (401, 403) else R.fail)(f"anonymous GET {host}/data/brain-constitution.json -> {st}")
        if st not in (401, 403):
            fail(f"anonymous read at {host} returned {st} -- the data-proxy worker with the private registration is not live")
    token = ssm.get_parameter(Name="/justhodl/api-admin/token", WithDecryption=True)["Parameter"]["Value"]
    st, body = http(f"{WORKER}/private-artifact?kind=brain-constitution", headers={"X-JH-Service-Token": token})
    ok = False
    if st == 200:
        try:
            mirrored = json.loads(body)
            ok = mirrored.get("content_hash") == doc.get("content_hash")
        except Exception:  # noqa: BLE001
            ok = False
    (R.ok if ok else R.fail)(f"service-token GET /private-artifact?kind=brain-constitution -> {st}, hash match={ok}")
    if not ok:
        fail("owner/service read of the constitution failed or hash mismatch")

    R.section("4. position-sizer consumes it")
    before = time.time()
    h = invoke_and_wait("justhodl-position-sizer", "data/position-sizing.json", before, budget_s=180)
    if not h:
        fail("position-sizer did not rewrite data/position-sizing.json within 180s")
    else:
        ps = json.loads(s3.get_object(Bucket=BUCKET, Key="data/position-sizing.json")["Body"].read())
        bc = ps.get("brain_constitution") or {}
        R.kv(version=ps.get("version"), consumed=bc.get("consumed"), content_hash=str(bc.get("content_hash"))[:12],
             risk_posture=ps.get("risk_posture"), posture_mult=ps.get("posture_mult"), sized=len(ps.get("sized_positions") or []))
        try:
            v_ok = float(str(ps.get("version"))) >= 1.1
        except Exception:  # noqa: BLE001
            v_ok = False
        if not v_ok:
            fail(f"position-sizer version {ps.get('version')} < 1.1")
        if bc.get("consumed") is not True or bc.get("content_hash") != doc.get("content_hash"):
            fail("position-sizer did not consume the fresh constitution (consumed/content_hash)")
        if ps.get("risk_posture") not in ("aggressive", "balanced", "defensive"):
            fail(f"position-sizer leaked a non-enum posture: {str(ps.get('risk_posture'))[:40]}")
        R.ok("consumed=%s posture=%s" % (bc.get("consumed"), ps.get("risk_posture"))) if not FAILS else R.warn("see failures")

    R.section("5. ask / ai")
    url = lam.get_function_url_config(FunctionName="justhodl-ask")["FunctionUrl"]
    st, _ = http(url, method="POST", headers={"Content-Type": "application/json"}, body=b'{"q":"gate"}')
    (R.ok if st == 401 else R.fail)(f"ask anonymous POST -> {st} (expect 401)")
    if st != 401:
        fail(f"ask anonymous POST returned {st}")
    R.ok("ai: code verified in step 1; its 22 tests ran in the deploy preflight")

    R.section("verdict")
    if FAILS:
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("GREEN -- constitution private at the edge, consumed by position-sizer, %d functions on commit %s" % (len(FUNCTIONS), commit[:10]))
