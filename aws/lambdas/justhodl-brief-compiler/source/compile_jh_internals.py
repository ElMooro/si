#!/usr/bin/env python3
"""Add-only JustHodl internals. Warehouse only."""
from __future__ import annotations
import json
from datetime import datetime, timezone

FRED_LEGS = {
    "dgs10": "DGS10", "dgs2": "DGS2", "walcl": "WALCL",
    "tga": "WTREGEN", "rrp": "RRPONTSYD", "nfci": "NFCI",
}


def compute(legs: dict) -> dict:
    out = {
        "schema_version": 1,
        "source": "compile_jh_internals",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fields": {},
        "skipped": ["ECONOMICS:* licensed PMI", "USI:TICKBA.* intraday"],
    }
    d10, d2 = legs.get("dgs10"), legs.get("dgs2")
    if d10 is not None and d2 is not None:
        out["fields"]["twos_tens"] = round(float(d10) - float(d2), 4)
    w, t, r = legs.get("walcl"), legs.get("tga"), legs.get("rrp")
    if None not in (w, t, r):
        out["fields"]["liq_proxy_bn"] = round((float(w) - float(t) - float(r)) / 1000.0, 1)
    if legs.get("nfci") is not None:
        out["fields"]["nfci"] = float(legs["nfci"])
    acc, dist, n = legs.get("n_up"), legs.get("n_down"), legs.get("n_univ")
    if acc is not None and dist is not None and n:
        out["fields"]["ad_breadth"] = round((float(acc) - float(dist)) / float(n), 4)
    a50, n50 = legs.get("n_above_50"), legs.get("n_sma50")
    if a50 is not None and n50:
        out["fields"]["pct_above_50"] = round(float(a50) / float(n50), 4)
    a200, n200 = legs.get("n_above_200"), legs.get("n_sma200")
    if a200 is not None and n200:
        out["fields"]["pct_above_200"] = round(float(a200) / float(n200), 4)
    nh, nl = legs.get("n_new_high"), legs.get("n_new_low")
    if nh is not None and nl is not None:
        out["fields"]["n_new_high"] = int(nh)
        out["fields"]["n_new_low"] = int(nl)
        out["fields"]["nh_nl"] = int(nh) - int(nl)
    return out

