"""Runner-only: verify deployed source, refresh only OFR, prove the hot feed."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import urllib.request
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "aws/lambdas/justhodl-warm-bridge/source"
sys.path.insert(0, str(ROOT / "aws/ops"))
sys.path.insert(0, str(SOURCE))
from ops_report import report
from ofr_funding import FIELDS, build_funding

BUCKET = "justhodl-dashboard-live"
FUNCTION = "justhodl-warm-bridge"
HOT_KEY = "data/ofr-funding.json"


def summary(doc):
    return {field: {key: doc.get(field, {}).get(key)
                    for key in ("value", "unit", "as_of", "data_unavailable", "stale", "series")}
            for field in FIELDS}


def verify_values(actual, expected):
    if actual.get("schema_version") != "ofr-funding.v2":
        raise RuntimeError("Expected funding schema is not published")
    if actual.get("available_fields") != len(FIELDS):
        raise RuntimeError("Some OFR fields are still unavailable")
    for field in FIELDS:
        value, wanted = actual[field], expected[field]
        for key in ("value", "unit", "as_of", "data_unavailable", "stale", "series",
                    "raw_snapshot_key", "fetched_at"):
            if value.get(key) != wanted.get(key):
                raise RuntimeError(f"Warehouse mismatch: {field}.{key}")
        if value.get("source") != wanted.get("source"):
            raise RuntimeError(f"Warehouse provenance mismatch: {field}")


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("This verification must run in GitHub Actions")
    import boto3
    from botocore.config import Config
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1",
                       config=Config(read_timeout=330, retries={"max_attempts": 1}))

    def get(key):
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if key.endswith(".gz") else raw)

    with report("ops_5442_verify_ofr_funding_repair") as r:
        r.heading("OFR deployed source and warehouse-to-hot verification")
        receipt = get(f"data/ops/releases/{FUNCTION}.json")
        cfg = lam.get_function_configuration(FunctionName=FUNCTION)
        if not receipt.get("verified") or receipt.get("code_sha256") != cfg.get("CodeSha256"):
            raise RuntimeError("Live Lambda code does not match its verified release receipt")
        for name in ("lambda_function.py", "ofr_funding.py"):
            digest = hashlib.sha256((SOURCE / name).read_bytes()).hexdigest()
            if receipt.get("source", {}).get(name, {}).get("sha256") != digest:
                raise RuntimeError(f"Deployed source differs from reviewed checkout: {name}")
        r.kv(release_commit=receipt.get("commit"), deploy_run=receipt.get("run_id"),
             code_sha256=cfg.get("CodeSha256"))
        before = get(HOT_KEY)
        expected = build_funding(get, datetime.now(timezone.utc).isoformat(timespec="seconds"))
        if expected["available_fields"] != len(FIELDS):
            r.log("candidate_summary=" + json.dumps(summary(expected)))
            raise RuntimeError("Warehouse precheck did not recover all five fields; no invoke performed")
        for name, value in summary(expected).items():
            r.kv(field=name, **value)
        # Inspect an existing primary-source SOFR archive if the OFR copy is
        # stale. This does not change the selected source or fetch vendor data.
        if expected["sofr"].get("stale"):
            try:
                nyfed_key = "data/warm/nyfed/sofr.json.gz"
                nyfed = get(nyfed_key)
                rows = sorted(nyfed.get("observations") or [], key=lambda x: str(x.get("date")))
                r.log("nyfed_warehouse_sofr=" + json.dumps({
                    "key": nyfed_key, "as_of": nyfed.get("as_of"),
                    "latest": [{k: row.get(k) for k in ("date", "rate")} for row in rows[-3:]]}))
            except Exception as exc:
                r.warn("Existing NY Fed SOFR archive inspection: " + type(exc).__name__)
        started = datetime.now(timezone.utc)
        result = lam.invoke(FunctionName=FUNCTION, InvocationType="RequestResponse",
                            Payload=json.dumps({"feed": "ofr"}).encode())
        if result.get("FunctionError"):
            raise RuntimeError("OFR-only invocation returned a Lambda FunctionError")
        response = json.loads(result["Payload"].read())
        body = json.loads(response.get("body", "{}"))
        if response.get("statusCode") != 200 or body.get("wrapped", {}).get("ofr") != 5:
            raise RuntimeError("OFR-only invocation did not confirm five available fields")
        after = get(HOT_KEY)
        verify_values(after, expected)
        generated = datetime.fromisoformat(after["as_of"].replace("Z", "+00:00"))
        if generated < started.replace(microsecond=0):
            raise RuntimeError("Hot object predates the verification invocation")
        public_proofs = []
        for origin in ("https://justhodl.ai", "https://justhodl-data-proxy.raafouis.workers.dev"):
            # Worker cache keys strip query parameters; wait for the existing
            # five-minute cache TTL instead of claiming query busting works.
            deadline = time.monotonic() + 360
            while True:
                request = urllib.request.Request(f"{origin}/{HOT_KEY}", headers={
                    "Cache-Control": "no-cache", "Accept-Encoding": "identity",
                    "User-Agent": "JustHodl-OFR-verification"})
                try:
                    with urllib.request.urlopen(request, timeout=45) as res:
                        served = json.loads(res.read())
                    verify_values(served, expected)
                    if served.get("as_of") != after["as_of"]:
                        raise RuntimeError("Public route serves an older publication")
                    break
                except Exception as exc:
                    if time.monotonic() >= deadline:
                        raise
                    r.log("Waiting for public cache expiry: " + type(exc).__name__)
                    time.sleep(15)
            public_proofs.append(origin + "/" + HOT_KEY)
            r.ok("Verified public feed: " + public_proofs[-1])
        proof = {
            "status": "PASS", "verified_at": datetime.now(timezone.utc).isoformat(),
            "function": FUNCTION, "release_commit": receipt.get("commit"),
            "deployment_run": receipt.get("run_id"), "code_sha256": cfg.get("CodeSha256"),
            "before": summary(before), "after": summary(after),
            "public_routes": public_proofs, "published_at": after["as_of"],
        }
        target = ROOT / "aws/ops/reports/5442_ofr_funding_verify.json"
        target.write_text(json.dumps(proof, indent=2) + "\n")
        r.ok("PASS: five warehouse observations and provenance verified in S3 and both public routes")


if __name__ == "__main__":
    main()
