"""Companion annotator for justhodl-bottom.

Imported by tests and (optionally) lambda_function.py. Safe no-op if the
handler never calls it — bottom.json v1.2.0 schema stays intact.
"""
from wyckoff_campaign import (  # shared/ is zipped next to source/ on deploy
    CAMPAIGN_STATES,
    HISTORICAL,
    annotate,
    campaign_of,
    hinge_of,
    project_board,
    proofs_of,
    pump_start_of,
)

__all__ = [
    "CAMPAIGN_STATES",
    "HISTORICAL",
    "annotate",
    "campaign_of",
    "hinge_of",
    "project_board",
    "proofs_of",
    "pump_start_of",
]
