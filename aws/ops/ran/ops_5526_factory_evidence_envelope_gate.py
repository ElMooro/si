"""ops 5526 -- evidence envelope on wall entries, proven live (Claude, 2026-09-13).

Push 099f465c: /factory/predictions accepts an optional factory-evidence.v1 envelope (identity, window,
checker and grade window are fixed by the server; the holdout manifest hash must match the frozen private
manifest when one exists), stores it create-if-absent under factory/evidence/market/<id>.json and links
evidence_id/evidence_hash on the accepted entry. /factory/sandbox publishes the contract and the manifest
digest, never the manifest. Entries without an envelope are still graded on the wall but never feed the
skillbook ("no evidence, no learn").

Proof:
  1. justhodl-ai receipt: source + shared identical to HEAD, code_sha256 == live;
  2. the live zip's factory_gateway.py carries attach_evidence + evidence_contract, no provider bypass;
  3. IAM invocation of the HTTP handler (the transport ops 5510 used): missing service identity -> 401;
     owner GET /factory/sandbox -> 200 with evidence_contract.schema_version == factory-evidence.v1,
     holdout_manifest_hash None or 64-hex, and no holdout block names in the body;
  4. anonymous factory/* still denied.
No entry is submitted (week 1 has not opened), nothing is written to S3 by this op.
"""
from __future__ import annotations

import io
import json
import re
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
PUB = "justhodl-dashboard-live"
FN = "justhodl-ai"
UA = {"User-Agent": "JustHodl-ops-5526 (+https://justhodl.ai)"}


def _same_source(commit):
    paths = ["aws/lambdas/%s/source" % FN, "aws/lambdas/%s/config.json" % FN, "aws/shared"]
    if subprocess.run(["git", "cat-file", "-e", commit + "^{commit}"], cwd=REPO, capture_output=True).returncode != 0:
        subprocess.run(["git", "fetch", "--quiet", "--depth=200", "origin", "main"], cwd=REPO, capture_output=True)
    return subprocess.run(["git", "diff", "--quiet", commit, "HEAD", "--"] + paths, cwd=REPO, capture_output=True).returncode == 0


def _status(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001
        return "ERR:" + type(e).__name__


def main() -> int:
    red = []
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=140, connect_timeout=5, retries={"max_attempts": 0}))
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    with report("ops_5526_factory_evidence_envelope_gate") as R:
        R.heading("ops 5526 -- evidence envelope on wall entries: receipt, live bytes, owner sandbox probe, boundaries")
        R.kv(head=head[:10])
        cfg = lam.get_function(FunctionName=FN)
        live = cfg["Configuration"]["CodeSha256"]
        rc = {}
        try:
            rc = json.loads(s3.get_object(Bucket=PUB, Key="data/ops/releases/%s.json" % FN)["Body"].read())
        except Exception as e:  # noqa: BLE001
            R.fail("receipt unreadable %s" % str(e)[:120])
        commit = str(rc.get("commit") or "")
        ok = bool(commit) and _same_source(commit) and rc.get("code_sha256") == live
        (R.ok if ok else R.fail)("%s receipt commit=%s source_identical_to_HEAD=%s sha_match=%s run=%s" % (
            FN, commit[:7], _same_source(commit) if commit else False, rc.get("code_sha256") == live, rc.get("run_id")))
        if not ok:
            red.append("receipt")
        try:
            zb = urllib.request.urlopen(cfg["Code"]["Location"], timeout=120).read()
            src = zipfile.ZipFile(io.BytesIO(zb)).read("factory_gateway.py").decode()
            has = all(n in src for n in ("def attach_evidence(", "def evidence_contract(", "factory/evidence/market/"))
            clean = all(n not in src for n in ("api.z.ai", "ZAI_BASE_URL", "_zai_key"))
            (R.ok if has and clean else R.fail)("live zip: envelope code=%s provider bypass absent=%s" % (has, clean))
            if not (has and clean):
                red.append("zip")
        except Exception as e:  # noqa: BLE001
            red.append("zip"); R.fail("zip inspect failed %s" % str(e)[:120])

        token = cfg["Configuration"].get("Environment", {}).get("Variables", {}).get("JH_SERVICE_TOKEN")

        def call(headers):
            event = {"version": "2.0", "rawPath": "/factory/sandbox", "rawQueryString": "",
                     "requestContext": {"http": {"method": "GET", "path": "/factory/sandbox"}}, "headers": headers}
            out = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(event).encode())
            return json.loads(out["Payload"].read() or b"{}")

        denied = call({})
        (R.ok if denied.get("statusCode") == 401 else R.fail)("missing service identity -> %s (want 401)" % denied.get("statusCode"))
        if denied.get("statusCode") != 401:
            red.append("service-identity")
        if not token:
            red.append("no-service-token"); R.fail("JH_SERVICE_TOKEN not configured on %s; owner probe impossible" % FN)
        else:
            owner = call({"x-jh-service-token": token, "x-jh-factory-role": "owner", "x-jh-factory-uid": "ops-5526-owner-probe"})
            body = owner.get("body")
            try:
                doc = json.loads(body) if isinstance(body, str) else (body or {})
            except ValueError:
                doc = {}
            contract = doc.get("evidence_contract") or {}
            h = contract.get("holdout_manifest_hash")
            hash_ok = h is None or (isinstance(h, str) and re.fullmatch(r"[0-9a-f]{64}", h) is not None)
            leak = "holdout_blocks" in json.dumps(doc) or "train_blocks" in json.dumps(doc)
            ok = owner.get("statusCode") == 200 and contract.get("schema_version") == "factory-evidence.v1" and hash_ok and not leak
            (R.ok if ok else R.fail)("owner GET /factory/sandbox -> %s contract=%s holdout_hash=%s leak=%s" % (
                owner.get("statusCode"), contract.get("schema_version"), (h[:12] + "…") if isinstance(h, str) else h, leak))
            if not ok:
                red.append("sandbox-contract")
        st = _status("https://justhodl.ai/factory/salon/season.json")
        (R.ok if st in (401, 403) else R.fail)("anonymous factory/salon/season.json -> %s" % st)
        if st not in (401, 403):
            red.append("boundary")
        if red:
            R.fail("RED -- %s" % ", ".join(red))
            return 1
        R.ok("GREEN -- evidence envelope live: contract + manifest digest on the sandbox, envelope code in the live zip, identity still server-side")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
