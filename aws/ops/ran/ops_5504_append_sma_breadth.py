"""Runner-only SMA merge: signed Finviz percentage distances, independent 80% gates.

Deploy the brief compiler first. This op verifies its package, then uses the
same pure merge against two S3 keys. No compiler invoke or other field refresh.
"""
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import urllib.parse
import urllib.request
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/ops"))
from ops_report import report

BUCKET = "justhodl-dashboard-live"
KEY = "data/jh-internals.json"
SOURCE_KEY = "data/finviz-universe.json"
FUNCTION = "justhodl-brief-compiler"
RECEIPT = ROOT / "aws/ops/reports/5504_sma_breadth_receipt.json"
OWNED_FIELDS = {"pct_above_50", "pct_above_200", "n_above_50", "n_sma50",
                "n_above_200", "n_sma200"}
REQUIRED_FIELDS = {"twos_tens", "liq_proxy_bn", "nfci", "ad_breadth",
                   "n_up", "n_down", "n_univ"}


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        sys.exit(1)
    import boto3
    from botocore.httpsession import URLLib3Session

    proof = {"op": 5504, "started_at": datetime.now(timezone.utc).isoformat(),
             "source_mode": "data/finviz-universe.json only; no vendor HTTP",
             "mutated_keys": [], "transport": {"s3_requests": [], "other_http_attempts": 0}}
    with report("ops_5504_append_sma_breadth") as r:
        try:
            s3 = boto3.client("s3", region_name="us-east-1")
            lam = boto3.client("lambda", region_name="us-east-1")
            package = lam.get_function(FunctionName=FUNCTION)
            with urllib.request.urlopen(package["Code"]["Location"], timeout=60) as response:
                archive = zipfile.ZipFile(io.BytesIO(response.read()))
            hashes = {}
            members = ("compile_jh_internals.py", "internals_warehouse.py")
            for name in members:
                deployed = archive.read(name)
                reviewed = ROOT / "aws/lambdas" / FUNCTION / "source" / name
                if deployed != reviewed.read_bytes():
                    raise ValueError("Deploy reviewed compiler before op 5504: " + name)
                hashes[name] = hashlib.sha256(deployed).hexdigest()
            proof["deployed_package"] = {
                "files": hashes, "last_modified": package["Configuration"]["LastModified"],
                "code_sha256": package["Configuration"]["CodeSha256"],
            }

            original_send = URLLib3Session.send
            allowed_hosts = {f"{BUCKET}.s3.us-east-1.amazonaws.com",
                             f"{BUCKET}.s3.amazonaws.com", "s3.us-east-1.amazonaws.com",
                             "s3.amazonaws.com"}

            def forbidden(*args, **kwargs):
                proof["transport"]["other_http_attempts"] += 1
                raise RuntimeError("Non-S3 HTTP forbidden during SMA merge")

            def guarded_send(session, request, **kwargs):
                parsed = urllib.parse.urlsplit(request.url)
                path = urllib.parse.unquote(parsed.path).lstrip("/")
                if path.startswith(BUCKET + "/"):
                    path = path[len(BUCKET) + 1:]
                allowed = (request.method == "GET" and path in (KEY, SOURCE_KEY)) or (
                    request.method == "PUT" and path == KEY)
                if parsed.hostname not in allowed_hosts or not allowed:
                    return forbidden()
                proof["transport"]["s3_requests"].append({"method": request.method, "key": path})
                return original_send(session, request, **kwargs)

            with tempfile.TemporaryDirectory(prefix="jh-sma-5504-") as tmp:
                for name in members:
                    (Path(tmp) / name).write_bytes(archive.read(name))
                sys.path.insert(0, tmp)
                spec = importlib.util.spec_from_file_location("deployed_sma", Path(tmp) / "internals_warehouse.py")
                compiler = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(compiler)
                with patch.object(URLLib3Session, "send", guarded_send), patch(
                        "urllib.request.urlopen", forbidden):
                    before, before_obj = compiler.read(s3, KEY)
                    universe, source_obj = compiler.read(s3, SOURCE_KEY)
                    if not REQUIRED_FIELDS.issubset(before.get("fields", {})):
                        raise ValueError("Existing core and A-D fields required before SMA append")
                    if not isinstance(before.get("warehouse"), dict) or not before["warehouse"]:
                        raise ValueError("Existing warehouse proof required before SMA append")
                    proof["before"] = {"last_modified": before_obj["LastModified"].isoformat(),
                                       "fields": before["fields"]}
                    proof["source"] = {"key": SOURCE_KEY, "etag": source_obj["ETag"],
                                       "last_modified": source_obj["LastModified"].isoformat(),
                                       "as_of": universe.get("generated_at"),
                                       "n_tickers": universe.get("n_tickers")}
                    out = compiler.merge_sma(before, universe, source_obj["LastModified"].isoformat())
                    untouched = set(before["fields"]) - OWNED_FIELDS
                    if out.get("schema_version") != 1 or any(
                            out["fields"].get(k) != before["fields"][k] for k in untouched):
                        raise ValueError("SMA merge changed an unrelated field or schema")
                    if any(out.get(k) != v for k, v in before.items() if k not in ("fields", "sma_breadth")):
                        raise ValueError("SMA merge changed unrelated document metadata")
                    for window in (50, 200):
                        evidence = out["sma_breadth"]["windows"][str(window)]
                        pct = out["fields"].get(f"pct_above_{window}")
                        if evidence["coverage"] < .8:
                            if pct is not None or not evidence.get("skip_reason"):
                                raise ValueError("SMA coverage skip contract failed")
                        elif pct is None or not 0 <= pct <= 1:
                            raise ValueError("SMA fraction missing or out of range")
                        elif pct != out["fields"][f"n_above_{window}"] / out["fields"][f"n_sma{window}"]:
                            raise ValueError("SMA fraction does not match uncapped counts")
                    proof["preserved_fields"] = {k: before["fields"][k] for k in sorted(untouched)}
                    proof["warehouse_before_sha256"] = hashlib.sha256(
                        json.dumps(before["warehouse"], sort_keys=True).encode()).hexdigest()
                    body = (json.dumps(out, indent=2, allow_nan=False) + "\n").encode()
                    proof["body_sha256"] = hashlib.sha256(body).hexdigest()
                    s3.put_object(Bucket=BUCKET, Key=KEY, Body=body, ContentType="application/json",
                                  CacheControl="public, max-age=60", IfMatch=before_obj["ETag"])
                    proof["mutated_keys"].append(KEY)
                    live, live_obj = compiler.read(s3, KEY)
                    if live != out:
                        raise ValueError("S3 readback differs from merged internals")
                    proof["warehouse_preserved"] = live["warehouse"] == before["warehouse"]
                    proof["warehouse_after_sha256"] = hashlib.sha256(
                        json.dumps(live["warehouse"], sort_keys=True).encode()).hexdigest()
                    proof["live"] = {"key": KEY, "last_modified": live_obj["LastModified"].isoformat(),
                                     "schema_version": live["schema_version"], "fields": live["fields"],
                                     "sma_breadth": live["sma_breadth"], "warehouse": live["warehouse"]}
            proof["status"] = "PASS"
            r.ok("LastModified=%s fields=%s" % (proof["live"]["last_modified"], json.dumps(live["fields"])))
            r.ok("SMA coverage=" + json.dumps(live["sma_breadth"]["windows"]))
            r.ok("Existing fields preserved; vendor HTTP attempts=0")
        except Exception as exc:
            proof["status"] = "FAIL"
            proof["error"] = {"type": type(exc).__name__, "message": str(exc)[:300]}
            r.fail(json.dumps(proof["error"]))
            RECEIPT.write_text(json.dumps(proof, indent=2, default=str) + "\n")
            sys.exit(1)
        proof["finished_at"] = datetime.now(timezone.utc).isoformat()
        RECEIPT.write_text(json.dumps(proof, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
