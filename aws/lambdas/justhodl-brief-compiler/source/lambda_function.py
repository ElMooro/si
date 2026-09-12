"""justhodl-brief-compiler -- dispatch on event['mode']."""
from __future__ import annotations

import json

import boto3
from brief_compiler import MODES, run


def lambda_handler(event, context):
    event = event or {}
    mode = event.get("mode") or (event.get("detail") or {}).get("mode")
    if mode == "all" or not mode:
        modes = list(MODES)
    else:
        modes = [mode]
    s3 = boto3.client("s3", region_name="us-east-1")
    out = {"ok": True, "results": {}}
    for m in modes:
        try:
            doc = run(s3, m, source="justhodl-brief-compiler")
            if m == "verdict":
                out["results"][m] = {
                    "coverage": doc.get("coverage"),
                    "score": doc.get("score"),
                    "missing": doc.get("missing_families"),
                    "as_of": doc.get("as_of"),
                }
            else:
                out["results"][m] = {
                    "status": doc.get("status"),
                    "why": doc.get("why"),
                    "held_reason": doc.get("held_reason"),
                }
        except Exception as e:
            out["ok"] = False
            out["results"][m] = {"error": str(e)[:200]}
    return out
