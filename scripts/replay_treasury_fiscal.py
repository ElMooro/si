#!/usr/bin/env python3
"""Replay public Treasury fiscal inputs using the reviewed local compiler only."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
from treasury_fiscal_model import DATASETS, build, digest

MAX_BYTES = 48 * 1024 * 1024


def public_bytes(key):
    if not isinstance(key, str) or not re.fullmatch(r"data/[A-Za-z0-9_./-]+", key) or ".." in key:
        raise ValueError("public data key required")
    req = urllib.request.Request("https://justhodl.ai/" + key, headers={"User-Agent": "justhodl-verify-release/1.0"})
    with urllib.request.urlopen(req, timeout=60) as response:
        raw = response.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES: raise ValueError("download exceeds replay bound")
    if key.endswith(".gz"):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream: raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES: raise ValueError("decoded source exceeds replay bound")
    return raw


def checked(receipt, read):
    raw = read(receipt["key"])
    if len(raw) != receipt["bytes"] or hashlib.sha256(raw).hexdigest() != receipt["sha256"]:
        raise ValueError("retained source bytes differ")
    return json.loads(raw)


def replay(manifest, read=public_bytes):
    if manifest.get("contract") != "treasury-fiscal-replay.v1": raise ValueError("wrong replay contract")
    source = (ROOT / "aws/shared/treasury_fiscal_model.py").read_bytes()
    if hashlib.sha256(source).hexdigest() != manifest["compiler"]["sha256"]:
        raise ValueError("reviewed local compiler differs; check out the matching release, never execute downloaded code")
    previous = checked(manifest["previous_evidence"], read) if manifest.get("previous_evidence") else None
    pages = [{"document": checked(p["receipt"], read), "receipt": p["receipt"], "acquired_at": p["acquired_at"]}
             for p in manifest["pages"]]
    out = build(manifest["dataset"], previous, pages, manifest["generated_at"], manifest.get("previous_evidence"))
    if digest(out) != manifest["output_sha256"]: raise ValueError("deterministic output differs")
    # Every retained row (including older history) must map back to its original
    # response, not merely to a hash of the previous derived warehouse.
    originals = {key: checked(receipt, read) for key, receipt in out["sources"].items()}
    for item in out["records"]:
        if originals[item["source_key"]]["data"][item["row_index"]] != item["row"]:
            raise ValueError("retained row differs from original provider row")
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=list(DATASETS))
    args = parser.parse_args()
    summary = json.loads(public_bytes("data/warm/treasury/latest-summary.json"))
    for ds in ([args.dataset] if args.dataset else DATASETS):
        key = summary["datasets"][ds]["replay"]["manifest_key"]
        manifest = json.loads(public_bytes(key))
        out = replay(manifest)
        print("REPRODUCED", ds, out["n_records"], "original rows", manifest["output_sha256"])


if __name__ == "__main__": main()
