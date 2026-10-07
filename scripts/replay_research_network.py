"""Offline replay of complete native source captures; never contacts a provider.

python scripts/replay_research_network.py --snapshots /path --output /path
Input directory: inventory.json from the audit plus native body files.
Output directory must be outside the repository. Retained originals are copied
into that local directory, so the dossier can be inspected without live calls.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from research_network_store import publish_network, KEY
from research_network_consumer import consume
from research_network_registry import SUBSCRIPTIONS


class Missing(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class Collision(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class LocalStore:
    def __init__(self, snapshots, output):
        self.output = output
        self.inventory = json.loads((snapshots / "inventory.json").read_text(encoding="utf-8"))
        self.sources = {r["key"]: snapshots / r["file"] for r in self.inventory if r["status"] == "captured"}
        for row in self.inventory:
            if row["status"] == "captured" and hashlib.sha256(self.sources[row["key"]].read_bytes()).hexdigest() != row["sha256"]:
                raise ValueError("audit original digest mismatch: " + row["key"])

    def get_object(self, *, Key, **kwargs):
        path = self.output / Key
        if not path.is_file():
            path = self.sources.get(Key)
        if path is None or not path.is_file():
            raise Missing(Key)
        raw = path.read_bytes()
        return {"Body": io.BytesIO(raw), "ContentLength": len(raw), "ETag": hashlib.sha256(raw).hexdigest()}

    def put_object(self, *, Key, Body, **kwargs):
        path = self.output / Key
        exists = path.is_file()
        if kwargs.get("IfNoneMatch") and exists:
            raise Collision(Key)
        if kwargs.get("IfMatch") and (not exists or hashlib.sha256(path.read_bytes()).hexdigest() != kwargs["IfMatch"]):
            raise Collision(Key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(Body)
        return {"ETag": hashlib.sha256(Body).hexdigest()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshots", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--now", default="2026-10-07T17:00:00+00:00")
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(ROOT):
        raise ValueError("replay output must be outside the repository")
    store = LocalStore(args.snapshots, args.output)
    now = datetime.fromisoformat(args.now).astimezone(timezone.utc)
    started = time.monotonic()
    manifest = publish_network(store, "local-only", now=now)
    from research_network_registry import SOURCES
    failed_captures = [sid for sid, spec in SOURCES.items() if spec['key'] in store.sources
                       and not spec.get('access_status') and not manifest['sources'][sid]['research_available']]
    if failed_captures:
        raise ValueError('captured sources failed replay: ' + ', '.join(failed_captures))
    receipts = {name: consume(store, "local-only", name, now=now,
                              **({'positions': []} if name == 'portfolio-risk' else {})) for name in SUBSCRIPTIONS}
    result = {"mode": "offline_native_replay", "sources": manifest["source_count"],
              "available": manifest["available_sources"], "entities": manifest["entity_count"],
              "specialists": len(manifest["specialists"]), "seconds": round(time.monotonic() - started, 3),
              "publication_id": manifest["publication_id"],
              "source_states": {k: {f: s.get(f) for f in ("status", "records", "unresolved_rows", "adapter_error", "error_type", "publication_freshness")}
                                for k, s in manifest["sources"].items()},
              "consumers": {k: {f: v.get(f) for f in ("status", "reason", "publication_id")} for k, v in receipts.items()},
              "live_deployment_verified": False}
    (args.output / "replay-results.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "source_states"}, indent=2))
    issues = {k: s for k, s in result["source_states"].items() if s["status"] in ("adapter_mismatch", "unavailable") or s.get("unresolved_rows")}
    print(json.dumps({"adapter_issues": issues}, indent=2))
    if any(v["status"] != "available" for v in receipts.values()):
        raise ValueError("consumer replay failed")


if __name__ == "__main__":
    main()
