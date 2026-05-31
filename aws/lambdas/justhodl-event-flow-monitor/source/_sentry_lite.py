"""sentry_lite — minimal, self-hosted error tracking for AWS Lambdas.

A 30-line dependency-free alternative to the paid Sentry SaaS that:

  1. Wraps a Lambda handler with @track_errors decorator
  2. On unhandled exception, writes a structured JSON error record to:
       s3://<BUCKET>/errors/<lambda_name>/<iso_timestamp>.json   (detail)
       s3://<BUCKET>/errors/recent.json                          (rolling 200)
  3. Re-raises the exception so AWS still records the Lambda failure

USAGE
-----

  from _sentry_lite import track_errors

  @track_errors
  def lambda_handler(event, context):
      ...

The decorator is idempotent (safe to apply multiple times).

CONFIGURATION
-------------

Reads these env vars:
  SENTRY_LITE_BUCKET    — S3 bucket (default: justhodl-dashboard-live)
  SENTRY_LITE_PREFIX    — S3 prefix (default: errors)
  SENTRY_LITE_DISABLE   — set to "1" to disable (default off)

The Lambda IAM role must allow s3:PutObject + s3:GetObject on
<BUCKET>/<PREFIX>/*. Both are typically already granted.

DESIGN NOTES
------------

  - Detail records are write-only (one per error).
  - The recent.json rolling tail is read-modify-write; safe under low
    concurrency (last-write-wins is fine for an audit trail). Lambdas
    rarely fire more than 1 error per second.
  - If the S3 write itself fails, we print and swallow — never block
    the original failure. Re-raising the underlying exception is
    always the contract.
"""
from __future__ import annotations
import functools
import json
import os
import traceback
from datetime import datetime, timezone


def _get_s3_client():
    """Lazy boto3 client — avoids importing if Lambda doesn't already have it."""
    import boto3
    return boto3.client("s3", region_name="us-east-1")


def track_errors(handler):
    """Decorator: wrap a Lambda handler with self-hosted error tracking."""
    if os.environ.get("SENTRY_LITE_DISABLE") == "1":
        return handler
    if getattr(handler, "_sentry_wrapped", False):
        return handler  # idempotent

    @functools.wraps(handler)
    def wrapped(event=None, context=None):
        try:
            return handler(event, context)
        except Exception as e:
            try:
                _record_error(e, event, context)
            except Exception as track_err:
                print(f"[sentry-lite] failed to record error: {track_err}")
            # Re-raise so AWS still records the Lambda failure metric
            raise

    wrapped._sentry_wrapped = True
    return wrapped


def _record_error(exc, event, context):
    """Write detail file + update rolling tail. Best-effort; never raises."""
    bucket = os.environ.get("SENTRY_LITE_BUCKET", "justhodl-dashboard-live")
    prefix = os.environ.get("SENTRY_LITE_PREFIX", "errors")
    lambda_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")
    now = datetime.now(timezone.utc)
    iso = now.isoformat(timespec="seconds")

    err = {
        "timestamp": iso,
        "lambda": lambda_name,
        "exception_type": type(exc).__name__,
        "message": str(exc)[:1000],
        "traceback": traceback.format_exc()[:6000],
        "request_id": getattr(context, "aws_request_id", None) if context else None,
        "log_stream": os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME"),
        "log_group": os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME"),
        "remaining_ms": (context.get_remaining_time_in_millis() if context
                         and hasattr(context, "get_remaining_time_in_millis")
                         else None),
        # Lightweight event shape (don't dump full event — may contain PII or huge bodies)
        "event_shape": {
            "type": type(event).__name__,
            "keys": list(event.keys())[:30] if isinstance(event, dict) else None,
        },
    }

    s3 = _get_s3_client()

    # Detail file — partitioned by Lambda + timestamp for easy CloudWatch-like browsing
    safe_iso = iso.replace(":", "-")
    detail_key = f"{prefix}/{lambda_name}/{safe_iso}.json"
    s3.put_object(
        Bucket=bucket, Key=detail_key,
        Body=json.dumps(err, default=str).encode(),
        ContentType="application/json",
    )

    # Rolling tail — last 200 errors across all Lambdas, summary only
    summary = {k: v for k, v in err.items() if k != "traceback"}
    summary["traceback_excerpt"] = (err["traceback"] or "").split("\n")[-3:]

    tail_key = f"{prefix}/recent.json"
    try:
        body = s3.get_object(Bucket=bucket, Key=tail_key)["Body"].read()
        recent = json.loads(body) if body else {"errors": []}
    except Exception:
        recent = {"errors": []}

    if not isinstance(recent.get("errors"), list):
        recent["errors"] = []
    recent["errors"].append(summary)
    recent["errors"] = recent["errors"][-200:]
    recent["last_update"] = iso
    recent["n_errors"] = len(recent["errors"])

    s3.put_object(
        Bucket=bucket, Key=tail_key,
        Body=json.dumps(recent, default=str).encode(),
        ContentType="application/json",
        CacheControl="no-cache",
    )

    print(f"[sentry-lite] recorded {type(exc).__name__} in {lambda_name} → {detail_key}")
