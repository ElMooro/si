"""Offline training-label eligibility audit; never imported by a producer.

Consumes explicitly supplied evidence only. It does not reconstruct availability
from session dates, fetch data, fit a strategy, or certify source provenance.
Run: python scripts/research/katlin_label_boundary.py evidence.json
"""
from collections import Counter
from datetime import datetime, timezone
import argparse
import json
import math
from pathlib import Path

LIMITATIONS = [
    "Supplied availability and source references require independent verification.",
    "Current-universe selection is not point-in-time membership; survivorship remains unresolved.",
    "Historical flows, fundamentals, holdings and catalysts are not established by price labels.",
    "No portfolio performance, execution costs, capacity or strategy validation is established.",
]


def clock(value):
    if not isinstance(value, str) or "T" not in value:
        raise ValueError("explicit timestamp required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone required")
    return parsed.astimezone(timezone.utc)


def index(value):
    if type(value) is not int or value < 0:
        raise ValueError("nonnegative integer session index required")
    return value


def training_audit(observations, *, horizon, test_start_index, test_start_at):
    """Accept only labels fully known strictly before a supplied fold boundary.

    Indexes refer to one caller-verified session calendar. Fixed-horizon labels
    only: early terminal/delisting labels need a separate reviewed contract.
    Source IDs are references, not verification that their timestamps are true.
    """
    if type(horizon) is not int or horizon <= 0:
        raise ValueError("positive integer horizon required")
    boundary_index = index(test_start_index)
    boundary = clock(test_start_at)
    if not isinstance(observations, list):
        raise ValueError("observation array required")
    ids = [row.get("id") if isinstance(row, dict) else None for row in observations]
    if any(not isinstance(x, str) or not x.strip() for x in ids) or len(set(ids)) != len(ids):
        raise ValueError("unique nonempty observation IDs required")
    accepted, excluded = [], []
    for row in observations:
        reasons = []
        try:
            entry = index(row.get("entry_index"))
            entered = clock(row.get("entry_at"))
            features_at = clock(row.get("features_available_at"))
            if entry >= boundary_index or entered >= boundary:
                reasons.append("entry_not_before_test")
            if features_at > entered:
                reasons.append("features_unavailable_at_entry")
            label = row.get("labels", {}).get(str(horizon))
            if not isinstance(label, dict):
                reasons.append("missing_horizon_label")
            else:
                endpoint = index(label.get("endpoint_index"))
                ended = clock(label.get("endpoint_at"))
                available = clock(label.get("available_at"))
                if endpoint != entry + horizon:
                    reasons.append("wrong_horizon_endpoint")
                if not entered < ended <= available:
                    reasons.append("inconsistent_label_clocks")
                if endpoint >= boundary_index or ended >= boundary:
                    reasons.append("label_endpoint_not_before_test")
                if available >= boundary:
                    reasons.append("label_unavailable_before_test")
                reference = label.get("source_record_id")
                if not isinstance(reference, str) or not reference.strip():
                    reasons.append("missing_availability_evidence_reference")
                value = label.get("excess_return_pct")
                if type(value) not in (int, float) or not math.isfinite(value):
                    reasons.append("invalid_label_value")
        except (ValueError, TypeError, AttributeError, OverflowError):
            reasons.append("missing_or_invalid_boundary_evidence")
        if reasons:
            excluded.append({"id": row["id"], "reasons": sorted(set(reasons))})
        else:
            accepted.append(row["id"])
    counts = Counter(reason for row in excluded for reason in row["reasons"])
    return {
        "contract": "katlin-offline-label-boundary.v1",
        "status": "eligible_evidence_present" if accepted else "no_eligible_training_evidence",
        "research_only": True,
        "validated_strategy": False,
        "decision_eligible": False,
        "horizon_sessions": horizon,
        "fold": {"test_start_index": boundary_index, "test_start_at": boundary.isoformat(),
                 "rule": "Entry, label endpoint and label availability strictly precede test start"},
        "input_count": len(observations), "accepted_count": len(accepted),
        "excluded_count": len(excluded), "accepted_ids": accepted, "excluded": excluded,
        "exclusion_reason_counts": dict(sorted(counts.items())),
        "limitations": LIMITATIONS[:],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    doc = json.loads(args.evidence.read_text(encoding="utf-8"))
    result = training_audit(doc["observations"], horizon=doc["horizon"],
                            test_start_index=doc["test_start_index"],
                            test_start_at=doc["test_start_at"])
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
