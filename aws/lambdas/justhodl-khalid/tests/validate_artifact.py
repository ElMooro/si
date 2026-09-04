"""Validate a deployed Khalid artifact during release promotion."""
from __future__ import annotations

import json
import sys
from pathlib import Path


with Path(sys.argv[1]).open(encoding="utf-8") as handle:
    data = json.load(handle)

required = {
    "engine",
    "schema_version",
    "generated_at",
    "status",
    "score",
    "stance",
    "risk_score",
    "confidence",
    "coverage",
    "domains",
    "asset_views",
    "inputs",
    "methodology",
    "panels",
}
missing = sorted(required - set(data))
if missing:
    raise SystemExit(f"Khalid artifact missing keys: {missing}")
if data["engine"] != "justhodl-khalid":
    raise SystemExit(f"Unexpected engine identity: {data['engine']}")
if data["status"] not in {"OK", "DEGRADED", "NO_DATA"}:
    raise SystemExit(f"Unexpected Khalid status: {data['status']}")
if not isinstance(data["panels"], dict) or "overview" not in data["panels"]:
    raise SystemExit("Khalid homepage panel is missing")
print("Khalid candidate artifact contract passed")
