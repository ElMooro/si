"""ops/4900 -- AI retrieval playbook: permanent + edge-verified.

Khalid: a file at the bottom of the data page telling every AI exactly
how to retrieve ECB and FRED data -- all the small details, so nobody
ever struggles with the 406 (or the FRED traps) again.

Shipped in this push: docs/AI_DATA_RETRIEVAL_PLAYBOOK.md (single
source of truth) + the full text embedded in a <details> block at the
bottom of data.html (marker: AI-RETRIEVAL-PLAYBOOK ops4900) + an
AUTONOMY.md pointer so every future session bootstraps into it.

This op: (G1) mirror the playbook to the deny-Delete'd prefix --
data/warm/playbooks/ecb-fred-retrieval.md + a machine-readable .json
twin -- and read both back byte-exact (sha256); (G2) edge-verify the
data.html marker on the live site (Cloudflare edge, cache-busted;
GitHub Pages origin fallback; PENDING on stubborn cache, honest);
(G3) informational: ecb-deep convergence snapshot.
"""
import gzip
import hashlib
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
MD = ROOT / "docs" / "AI_DATA_RETRIEVAL_PLAYBOOK.md"
K_MD = "data/warm/playbooks/ecb-fred-retrieval.md"
K_JS = "data/warm/playbooks/ecb-fred-retrieval.json"
MARKER = "AI-RETRIEVAL-PLAYBOOK ops4900"
s3 = boto3.client("s3", region_name=REGION)


def g_bytes(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw


def fetch(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl ops4900 edge-verify",
            "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, r.read()
    except Exception as e:
        return type(e).__name__, b""


def main():
    verdict = {"ops": 4900, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4900 -- ai retrieval playbook") as rep:
        rep.heading("ops 4900 — playbook permanent + edge-verified")

        # ── G1 permanent S3 mirror, byte-exact read-back ────────────
        body = MD.read_bytes()
        sha = hashlib.sha256(body).hexdigest()
        ok1 = False
        try:
            s3.put_object(Bucket=B, Key=K_MD, Body=body,
                          ContentType="text/markdown; charset=utf-8",
                          CacheControl="no-cache",
                          Metadata={"ops": "4900", "sha256": sha})
            s3.put_object(
                Bucket=B, Key=K_JS,
                Body=json.dumps({
                    "as_of": datetime.now(timezone.utc).isoformat(
                        timespec="seconds"),
                    "version": "v1", "ops": 4900,
                    "sha256_md": sha,
                    "source": "docs/AI_DATA_RETRIEVAL_PLAYBOOK.md",
                    "surfaces": ["data.html#ai-playbook", K_MD],
                    "text": body.decode("utf-8")}).encode(),
                ContentType="application/json",
                CacheControl="no-cache")
            rb = g_bytes(K_MD)
            ok1 = hashlib.sha256(rb).hexdigest() == sha
            js = json.loads(g_bytes(K_JS))
            ok1 = ok1 and js.get("sha256_md") == sha \
                and "406" in js.get("text", "")
            rep.kv(stage="s3-mirror", md_bytes=len(body),
                   sha256=sha[:16], readback_exact=ok1)
        except Exception as e:
            rep.kv(stage="s3-mirror", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:150]}")
        verdict["gates"]["s3_playbook_permanent"] = (
            "PASS" if ok1 else "FAIL")

        # ── G2 edge verification of the data.html embed ─────────────
        found, where = False, None
        deadline = time.time() + 480
        cb = int(time.time())
        urls = [f"https://justhodl.ai/data.html?opscheck={cb}",
                f"https://elmooro.github.io/si/data.html?o={cb}"]
        while time.time() < deadline and not found:
            for u in urls:
                st, bb = fetch(u)
                if st == 200 and MARKER.encode() in bb:
                    found, where = True, u.split("?")[0]
                    break
            if not found:
                time.sleep(30)
        rep.kv(stage="edge-verify", marker_found=found, where=where)
        verdict["gates"]["edge_marker"] = ("PASS" if found
                                           else "PENDING")

        # ── G3 deep convergence snapshot (informational) ────────────
        try:
            st = json.loads(g_bytes("data/_state/ecb-deep.json"))
            cov = json.loads(g_bytes("data/warm/ecb/coverage.json"))
            rep.kv(stage="deep-snapshot",
                   n_complete=st.get("n_complete"),
                   n_flows=st.get("n_flows"), mode=st.get("mode"),
                   cov_fast=cov.get("n_fast"),
                   cov_deep_complete=cov.get("n_deep_complete"))
            verdict["deep"] = {"n_complete": st.get("n_complete"),
                               "n_flows": st.get("n_flows"),
                               "mode": st.get("mode")}
        except Exception as e:
            rep.kv(stage="deep-snapshot",
                   err=f"{type(e).__name__}")

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4900.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4900.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
