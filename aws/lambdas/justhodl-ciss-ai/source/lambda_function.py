"""Publish source-bound deterministic CISS commentary without an AI API.

Original CSVs, exact definitions, source clocks and frozen compiler receipts are
retained before publication. These measurements do not authorize a trade or size.
"""
import json
import boto3
from ciss_source_store import run_commentary

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ciss-ai.json"


def lambda_handler(event, context):
    result = run_commentary(S3, BUCKET)
    return {"statusCode": 200, "body": json.dumps(result, allow_nan=False)}
