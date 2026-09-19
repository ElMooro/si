"""Retain and replay original ECB CISS and CLIFS measurements.

Original CSVs, exact definitions, source clocks and frozen compiler receipts are
retained before publication. These measurements do not authorize a trade or size.
"""
import json
import boto3
from ciss_source_store import run

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ciss-stress.json"


def lambda_handler(event, context):
    budget = min(480, max(5, context.get_remaining_time_in_millis()/1000-90)) if context else 480
    result = run(S3, BUCKET, budget_seconds=budget)
    return {"statusCode": 200, "body": json.dumps(result, allow_nan=False)}
