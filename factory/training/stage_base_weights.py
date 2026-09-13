#!/usr/bin/env python3
"""Stage a base model's weights into Khalid's own bucket (SageMaker Processing job, CPU, one-time per revision).

Downloads a pinned revision of a permissive-licensed repo from the Hugging Face hub into the job's output
directory (SageMaker uploads it to s3://<private>/factory/models/base/<model_id>/<revision>/), hashes every
file, reads the license from the model card, and writes manifest.json (factory-base-weights.v1) next to the
weights. The manifest is what gear_b_own.own_spec() trusts; nothing else about the download is trusted.

Environment (set by the launching op): HF_REPO, HF_REVISION (commit sha or tag), MODEL_ID (slug),
EXPECTED_LICENSE (e.g. apache-2.0), S3_PREFIX (the destination prefix, for the manifest), MAX_GB (refuse above).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

OUT = Path(os.environ.get("OUTPUT_DIR", "/opt/ml/processing/output"))
LICENSE_RX = re.compile(r"^license:\s*([A-Za-z0-9.\-+]+)", re.M)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    repo, revision, model_id = os.environ["HF_REPO"], os.environ["HF_REVISION"], os.environ["MODEL_ID"]
    expected = os.environ.get("EXPECTED_LICENSE", "apache-2.0").lower()
    max_bytes = int(float(os.environ.get("MAX_GB", "20")) * (1 << 30))
    from huggingface_hub import HfApi, snapshot_download  # inside the job only
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        revision = HfApi().model_info(repo, revision=revision).sha   # a tag/branch is resolved to the exact commit
    dest = OUT / model_id / revision
    dest.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    snapshot_download(repo_id=repo, revision=revision, local_dir=str(dest),
                      allow_patterns=["*.json", "*.safetensors", "*.txt", "*.model", "*.tiktoken", "LICENSE*", "README.md", "*.py"])
    card = (dest / "README.md").read_text(errors="ignore") if (dest / "README.md").exists() else ""
    m = LICENSE_RX.search(card)
    license_found = (m.group(1).lower() if m else "unknown")
    if license_found == "unknown":
        for name in ("LICENSE", "LICENSE.txt", "LICENSE.md"):
            if (dest / name).exists():
                head = (dest / name).read_text(errors="ignore")[:600].lower()
                if "apache license" in head and "2.0" in head:
                    license_found = "apache-2.0"
                elif "mit license" in head:
                    license_found = "mit"
                break
    print(json.dumps({"stage": "downloaded", "license": license_found, "files": sum(1 for _ in dest.rglob("*") if _.is_file()), "seconds": round(time.time() - t0, 1)}))
    files, total = [], 0
    for p in sorted(dest.rglob("*")):
        if p.is_file():
            size = p.stat().st_size
            total += size
            files.append({"path": p.relative_to(dest).as_posix(), "sha256": sha256(p), "bytes": size})
    manifest = {"schema_version": "factory-base-weights.v1", "model_id": model_id, "repo": repo, "revision": revision,
                "license": license_found, "expected_license": expected, "files": files, "total_bytes": total,
                "s3_prefix": os.environ.get("S3_PREFIX", ""), "staged_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "seconds": round(time.time() - t0, 1), "allow_patterns": "json/safetensors/tokenizer/license/readme"}
    status = "staged"
    if license_found != expected:
        status = "refused_license"
    if total > max_bytes:
        status = "refused_size"
    if not any(f["path"] == "config.json" for f in files) or not any(f["path"].endswith(".safetensors") for f in files):
        status = "refused_incomplete"
    manifest["status"] = status
    body = json.dumps(manifest, indent=2, sort_keys=True)
    manifest["manifest_sha256"] = hashlib.sha256(body.encode()).hexdigest()
    (OUT / model_id / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(json.dumps({k: manifest[k] for k in ("status", "license", "total_bytes", "revision", "seconds")}, indent=None))
    if status != "staged":
        # keep the evidence, delete the weights: a refused license or size never sits in the bucket as trainable input
        for f in files:
            (dest / f["path"]).unlink(missing_ok=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
