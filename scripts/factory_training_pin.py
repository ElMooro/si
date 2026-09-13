#!/usr/bin/env python3
"""Pin the owned training lane: bundle factory/training/ into a sha-named tarball in the private bucket and write
factory/training/current.json (factory-training-pin.v1) with the pinned training image.

The bundle is create-if-absent (content-addressed by sha256); current.json is the only mutable pointer and it
always names a bundle that exists. Image: an ECR uri, digest-pinned when the mirror workflow has resolved it.

Usage: factory_training_pin.py --image <ecr uri[@sha256:...]> [--dry-run] [--require-digest]
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import tarfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "factory" / "training"
PRIVATE = os.environ.get("FACTORY_PRIVATE_BUCKET", "justhodl-ai-857687956942")
REGION = os.environ.get("AWS_REGION", "us-east-1")
PIN_KEY = "factory/training/current.json"
FILES = ("train_qlora.py", "requirements.txt")


def bundle_bytes() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", format=tarfile.GNU_FORMAT) as tar:
        for name in FILES:
            p = SRC / name
            info = tarfile.TarInfo(name)
            data = p.read_bytes()
            info.size = len(data); info.mtime = 0; info.mode = 0o644
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def build_pin(image: str, bundle_uri: str, bundle_sha: str, require_digest: bool) -> dict:
    return {"schema_version": "factory-training-pin.v1", "training_image": image, "require_digest": bool(require_digest),
            "bundle_uri": bundle_uri, "bundle_sha256": bundle_sha, "program": "train_qlora.py",
            "requirements_sha256": hashlib.sha256((SRC / "requirements.txt").read_bytes()).hexdigest(),
            "default_instance": "ml.g5.2xlarge", "instances": ["ml.g5.2xlarge", "ml.g5.4xlarge", "ml.g5.12xlarge"],
            "max_steps": 400, "lora_r": 16, "learning_rate": "2e-4", "max_seq_len": 2048, "epochs": 1,
            "pinned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--require-digest", action="store_true")
    args = ap.parse_args(argv)
    data = bundle_bytes()
    sha = hashlib.sha256(data).hexdigest()
    key = "factory/training/bundles/train-%s.tar.gz" % sha[:16]
    pin = build_pin(args.image, "s3://%s/%s" % (PRIVATE, key), sha, args.require_digest)
    if args.dry_run:
        print(json.dumps({"dry_run": True, "bundle_key": key, "bundle_sha256": sha, "pin": pin}, indent=2)); return 0
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.put_object(Bucket=PRIVATE, Key=key, Body=data, ContentType="application/gzip", IfNoneMatch="*")
        state = "written"
    except Exception as exc:  # noqa: BLE001
        if "PreconditionFailed" in str(exc) or "412" in str(exc):
            state = "exists"
        else:
            raise
    s3.put_object(Bucket=PRIVATE, Key=PIN_KEY, Body=json.dumps(pin, indent=2).encode(), ContentType="application/json")
    print(json.dumps({"bundle": state, "bundle_key": key, "bundle_sha256": sha, "pin_key": PIN_KEY, "image": args.image}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
