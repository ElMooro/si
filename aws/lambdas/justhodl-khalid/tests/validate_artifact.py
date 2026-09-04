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
    "biggest_opportunities",
    "opportunity_radar",
    "opportunity_changes",
    "risk_board",
    "breadth_clusters",
    "queues",
    "near_misses",
    "inputs",
    "methodology",
    "panels",
}
missing = sorted(required - set(data))
if missing:
    raise SystemExit(f"Khalid artifact missing keys: {missing}")
if data["engine"] != "justhodl-khalid":
    raise SystemExit(f"Unexpected engine identity: {data['engine']}")
if data.get("schema_version") != "3.0.0" or data.get("version") != "3.0.0":
    raise SystemExit("Khalid schema/version must both be 3.0.0")
if data["status"] not in {"OK", "DEGRADED", "NO_DATA"}:
    raise SystemExit(f"Unexpected Khalid status: {data['status']}")
if not isinstance(data["panels"], dict) or "overview" not in data["panels"]:
    raise SystemExit("Khalid homepage panel is missing")
radar = data["opportunity_radar"]
if not isinstance(radar, list):
    raise SystemExit("Khalid opportunity radar is not a list")
ids = [(row.get("lifecycle") or {}).get("opportunity_id") for row in radar]
if any(not value for value in ids) or len(ids) != len(set(ids)):
    raise SystemExit("Khalid opportunity IDs must be present and unique")
if (data.get("decision") or {}).get("opportunities_tracked") != len(radar):
    raise SystemExit("Khalid tracked count does not reconcile")
for row in radar:
    stable = {"industry", "sector", "category", "market_cap", "cap_bucket", "momentum", "criteria", "gates", "dump_risk", "risk_reward"}
    if stable - set(row):
        raise SystemExit("Khalid opportunity row schema is unstable")
    if row.get("discovery_stage") == "ENTRY_READY":
        if row.get("action") != "READY_TO_SNIPE" or (row.get("entry_trigger") or {}).get("state") != "TRIGGERED":
            raise SystemExit("Khalid ENTRY_READY row lacks an observed trigger")
    if row.get("discovery_stage") == "EVIDENCE_HOLD":
        if row.get("action") != "TRACKING" or row.get("confidence") != 0:
            raise SystemExit("Khalid EVIDENCE_HOLD row must suspend conviction and execution")
        if (row.get("entry_trigger") or {}).get("state") != "WAIT":
            raise SystemExit("Khalid EVIDENCE_HOLD row must wait for feed recovery")
board = data.get("risk_board") or {}
if not isinstance(board.get("domains"), list) or not board.get("capital_decision"):
    raise SystemExit("Khalid risk board is incomplete")
if (data.get("decision") or {}).get("exposure_cap_pct") != board.get("exposure_cap_pct"):
    raise SystemExit("Khalid risk-board exposure cap does not reconcile")
print("Khalid candidate artifact contract passed")
