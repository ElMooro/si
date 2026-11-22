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
import world_history  # noqa: E402
import dollar_shortage  # noqa: E402

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ciss-stress.json"
DESK_NEEDS = 150      # seconds the sovereign-stress desk document needs after the warehouse; with less it waits for the next run
WORLD_NEEDS = 45      # seconds the World tab's histories need at the least
DOLLAR_NEEDS = 30     # seconds the dollar-shortage document needs at the least: three retained responses read twice


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


def dollar(context):
    """The dollar-shortage document (data/ciss-dollar-shortage.json): the euro's reference rate and the Eurosystem's two
    foreign-currency claims with their whole histories, read again from the ECB responses the desk document names (no
    request to the ECB), beside the Federal Reserve's dollar series read from the site's own warehouse, each at most
    once a day.  A failure leaves the last document in place and touches nothing else."""
    left = context.get_remaining_time_in_millis()/1000-20 if context else 300
    if left < DOLLAR_NEEDS:
        return {"published": False, "reason": "not enough time left; the last document stays"}
    try:
        return dollar_shortage.run(S3, BUCKET, store, budget_seconds=max(10, min(150, left-WORLD_NEEDS-30)))
    except Exception as exc:  # noqa: BLE001 - reported, never raised into the runs before it
        print("[dollar-shortage] not published: %s: %s" % (type(exc).__name__, str(exc)[:200]))
        return {"published": False, "error": type(exc).__name__}


def world(context):
    """The histories behind the desk's World tab (data/ciss-sovereign-world.json): the site's own stored series, no ECB request.

    It reads the desk document published just above, the CDS desk's measured prints, the warehouse's monthly yields and
    the world table's past publications.  A failure here leaves the last document in place and touches nothing else.
    """
    left = context.get_remaining_time_in_millis()/1000-20 if context else 300
    if left < WORLD_NEEDS:
        return {"published": False, "reason": "not enough time left; the last document stays"}
    try:
        return world_history.run(S3, BUCKET, budget_seconds=min(240, left-20))
    except Exception as exc:  # noqa: BLE001 - reported, never raised into the runs before it
        print("[sovereign-world] not published: %s: %s" % (type(exc).__name__, str(exc)[:200]))
        return {"published": False, "error": type(exc).__name__}


def lambda_handler(event, context):
    if isinstance(event, dict) and event.get("dollar_only"):
        # Only the dollar-shortage document, from the desk document already published and the ECB responses it kept.
        result = {"dollar": dollar(context)}
        print("[dollar-shortage] %s" % json.dumps(result["dollar"], allow_nan=False)[:400])
        return {"statusCode": 200, "body": json.dumps(result, allow_nan=False)}
    if isinstance(event, dict) and event.get("world_only"):
        # Only the World tab's histories: no ECB request, the warehouse and the desk document are not compiled again.
        result = {"world": world(context)}
        print("[sovereign-world] %s" % json.dumps(result["world"], allow_nan=False)[:400])
        return {"statusCode": 200, "body": json.dumps(result, allow_nan=False)}
    if isinstance(event, dict) and event.get("sovereign_only"):
        # Recompile the desk document from the warehouse that is already published; the warehouse itself is not collected again.
        result = {"sovereign": sovereign(context)}
        print("[sovereign-desk] %s" % json.dumps(result["sovereign"], allow_nan=False)[:400])
        result["dollar"] = dollar(context)
        result["world"] = world(context)
        return {"statusCode": 200, "body": json.dumps(result, allow_nan=False)}
    budget = min(480, max(5, context.get_remaining_time_in_millis()/1000-90)) if context else 480
    result = run(S3, BUCKET, budget_seconds=budget)
    result["sovereign"] = sovereign(context)
    print("[sovereign-desk] %s" % json.dumps(result["sovereign"], allow_nan=False)[:400])
    result["dollar"] = dollar(context)
    print("[dollar-shortage] %s" % json.dumps(result["dollar"], allow_nan=False)[:400])
    result["world"] = world(context)
    print("[sovereign-world] %s" % json.dumps(result["world"], allow_nan=False)[:400])
    return {"statusCode": 200, "body": json.dumps(result, allow_nan=False)}
