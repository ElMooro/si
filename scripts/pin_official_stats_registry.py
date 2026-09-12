#!/usr/bin/env python3
"""Append official_stats_brief to the three engine-registry copies. Idempotent."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRAG = ROOT / "config" / "brief-engine-official-stats.json"
PATHS = [
    ROOT / "config" / "engine-registry.v1.json",
    ROOT / "aws" / "lambdas" / "justhodl-jhsignal-bridge" / "source" / "engine-registry.v1.json",
    ROOT / "aws" / "lambdas" / "justhodl-jh-fusion" / "source" / "engine-registry.v1.json",
]


def main() -> None:
    frag = json.loads(FRAG.read_text())
    gold_path = PATHS[0]
    doc = json.loads(gold_path.read_text())
    ids = [e["engine_id"] for e in doc["engines"]]
    if "official_stats_brief" not in ids:
        doc["engines"].append(frag)
    text = json.dumps(doc, indent=2) + "\n"
    parsed = json.loads(text)
    assert parsed["engines"][-1]["engine_id"] == "official_stats_brief"
    assert any(e["engine_id"] == "official_stats_brief" for e in parsed["engines"])
    for p in PATHS:
        p.write_text(text)
    print("engines", len(parsed["engines"]), "bytes", len(text))


if __name__ == "__main__":
    main()
