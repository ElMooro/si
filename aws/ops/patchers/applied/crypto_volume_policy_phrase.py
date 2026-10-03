#!/usr/bin/env python3
"""Keep the reviewed 'remain unverified' sentence on the crypto join policy.

The volume-unit rule stays. The worker route test still requires that phrase.
"""
import hashlib
import json
from pathlib import Path

OLD = "Equities are not joined here."
NEW = "Equities are not joined here. Units and price adjustments remain unverified."


def root():
    if Path("jh-chart-engine.js").exists():
        return Path(".")
    return Path(__file__).resolve().parents[3]


def main():
    base = root()
    path = base / "cloudflare/workers/justhodl-data-proxy/src/index.js"
    fixture = base / "tests/fixtures/worker-crypto-source/transition.json"
    pred = (base / "tests/fixtures/worker-crypto-source/predecessor/index.js").read_text(encoding="utf-8")
    text = path.read_text(encoding="utf-8")
    trans = json.loads(fixture.read_text(encoding="utf-8"))
    if NEW in text and NEW in trans["after"]:
        print("policy phrase already present")
        return
    if text.count(OLD) != 1 or trans["after"].count(OLD) != 1:
        raise SystemExit("policy sentence is not unique")
    text = text.replace(OLD, NEW, 1)
    trans["after"] = trans["after"].replace(OLD, NEW, 1)
    if text.count(trans["after"]) != 1:
        raise SystemExit("updated after-block is not in index.js once")
    reversed_src = text.replace(trans["after"], trans["before"], 1)
    if reversed_src != pred:
        raise SystemExit("preservation reverse failed")
    source = hashlib.sha256(reversed_src.encode("utf-8")).hexdigest()
    if source != trans["source_sha256"]:
        raise SystemExit("predecessor hash drifted")
    trans["candidate_sha256"] = hashlib.sha256(text.encode("utf-8")).hexdigest()
    path.write_text(text, encoding="utf-8")
    fixture.write_text(json.dumps(trans, indent=2) + "\n", encoding="utf-8")
    print("policy phrase restored", trans["candidate_sha256"])


if __name__ == "__main__":
    main()
