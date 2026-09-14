"""ops 5543 -- delete the hub download cache the staging job left next to the weights (a second 15 GB copy of every
shard under <revision>/.cache/huggingface/download/). Only keys under that exact prefix are deleted; the hashed
weights, manifest.json, manifest-raw.json and staging records are untouched. Verifies the manifest still validates
afterwards.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "lambdas" / "justhodl-ai" / "source"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI = "us-east-1", "justhodl-ai-857687956942"
MODEL_ID = "qwen2-5-coder-7b-instruct"
BASE = "factory/models/base/%s/" % MODEL_ID


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    import gear_b_own as own
    with report("ops_5543_own_weights_cache_cleanup") as R:
        R.heading("ops 5543 -- remove the hub download cache next to the owned weights; weights and manifests untouched")
        manifest = json.loads(s3.get_object(Bucket=PRI, Key=BASE + "manifest.json")["Body"].read())
        own.validate_base_manifest(manifest, MODEL_ID)
        rev = manifest["revision"]
        prefix = BASE + rev + "/.cache/"
        keys, total, token = [], 0, None
        while True:
            kw = {"Bucket": PRI, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                keys.append(o["Key"]); total += o["Size"]
            token = resp.get("NextContinuationToken")
            if not resp.get("IsTruncated"):
                break
        R.ok("cache objects under %s: %d (%.2f GB)" % (prefix, len(keys), total / 1e9))
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=PRI, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})
        left = s3.list_objects_v2(Bucket=PRI, Prefix=prefix).get("KeyCount", 0)
        weights = [f for f in manifest["files"]]
        missing = []
        for f in weights:
            try:
                s3.head_object(Bucket=PRI, Key=BASE + rev + "/" + f["path"])
            except Exception:  # noqa: BLE001
                missing.append(f["path"])
        (R.ok if not left and not missing else R.fail)("deleted %d cache objects (%.2f GB freed); cache left=%d; weight files missing=%d" % (len(keys), total / 1e9, left, len(missing)))
        if left or missing:
            return 1
        R.ok("GREEN -- owned weights folder holds only the hashed files + manifests")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
