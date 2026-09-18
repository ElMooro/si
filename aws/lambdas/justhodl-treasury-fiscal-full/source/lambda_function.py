"""FiscalData v2: preserve dimensional originals and merge history conditionally."""
import json
import os
import boto3
from raw_snapshot import snapshot  # Declares the transitive capture deploy dependency.
from treasury_fiscal_model import DATASETS
from treasury_fiscal_store import acquire, merge, summary

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")


def lambda_handler(event, context):
    errors = {}
    counts = {}
    for dataset in DATASETS:
        try:
            page = acquire(dataset, BUCKET)
            if not page["document"]["data"]:
                raise ValueError("empty current provider page")
            out = merge(s3, BUCKET, dataset, [page])
            counts[dataset] = out["n_records"]
        except Exception as exc:
            # Provider bodies and credentials never enter the public summary.
            errors[dataset] = type(exc).__name__
    packet = summary(s3, BUCKET, errors)
    result = {"ok": not errors, "version": "2.0.0", "loaded": counts,
              "failed": errors, "generated_at": packet["generated_at"]}
    return {"statusCode": 200 if not errors else 503, "body": json.dumps(result)}
