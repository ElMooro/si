#!/usr/bin/env python3
"""Add-only JustHodl internals. Runner/Codex: read warehouse, write data/jh-internals.json.
Never call licensed PMI. Never delete watchlists.
"""
from __future__ import annotations
import json
from datetime import datetime, timezone

# FRED ids already banked. Values filled by the runner from S3/FRED.
FRED_LEGS = {
    "dgs10": "DGS10",
    "dgs2": "DGS2",
    "walcl": "WALCL",
    "tga": "WTREGEN",
    "rrp": "RRPONTSYD",
    "nfci": "NFCI",
}


def compute(legs: dict) -> dict:
    out = {
        "schema_version": 1,
        "source": "compile_jh_internals",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fields": {},
        "skipped": ["ECONOMICS:* licensed PMI", "USI:TICKBA.* intraday"],
    }
    d10 = legs.get("dgs10")
    d2 = legs.get("dgs2")
    if d10 is not None and d2 is not None:
        out["fields"]["twos_tens"] = round(float(d10) - float(d2), 4)
    w, t, r = legs.get("walcl"), legs.get("tga"), legs.get("rrp")
    if None not in (w, t, r):
        out["fields"]["liq_proxy_bn"] = round((float(w) - float(t) - float(r)) / 1000.0, 1)
    if legs.get("nfci") is not None:
        out["fields"]["nfci"] = float(legs["nfci"])
    # Breadth keys only if the runner supplied uncapped counts (never 100/100 cap).
    acc, dist, n = legs.get("n_up"), legs.get("n_down"), legs.get("n_univ")
    if acc is not None and dist is not None and n:
        out["fields"]["ad_breadth"] = round((float(acc) - float(dist)) / float(n), 4)
    return out

