"""Retain and replay original ECB CISS and CLIFS measurements.

Original CSVs, exact definitions, source clocks and frozen compiler receipts are
retained before publication. These measurements do not authorize a trade or size.
"""
import json
import os
import sys
import boto3
import ciss_source_store as store
from ciss_source_store import run

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))     # sovereign_desk.py sits beside this file, also when a test loads it by path
import sovereign_desk  # noqa: E402

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ciss-stress.json"
DESK_NEEDS = 150      # seconds the sovereign-stress desk document needs after the warehouse; with less it waits for the next run


def sovereign(context):
    """The sovereign-stress desk document (data/ciss-sovereign.json) stands on the warehouse that was just published.

    It is compiled from the same retained originals plus the ECB's yield curve, 10-year yields, TARGET balances and
    government debt.  Whatever happens here, the warehouse run above has already succeeded and stays published.
    """
    left = context.get_remaining_time_in_millis()/1000-30 if context else 600
    if left < DESK_NEEDS:
        return {"published": False, "reason": "not enough time left after the warehouse run; the last document stays"}
    try:
        return sovereign_desk.run(S3, BUCKET, store, budget_seconds=left-90)
    except Exception as exc:  # noqa: BLE001 - reported, never raised into the warehouse run
        print("[sovereign-desk] not published: %s: %s" % (type(exc).__name__, str(exc)[:200]))
        return {"published": False, "error": type(exc).__name__}


def lambda_handler(event, context):
    budget = min(480, max(5, context.get_remaining_time_in_millis()/1000-90)) if context else 480
    result = run(S3, BUCKET, budget_seconds=budget)
    result["sovereign"] = sovereign(context)
    print("[sovereign-desk] %s" % json.dumps(result["sovereign"], allow_nan=False)[:400])
    return {"statusCode": 200, "body": json.dumps(result, allow_nan=False)}
