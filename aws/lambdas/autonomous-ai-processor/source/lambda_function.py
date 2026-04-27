"""
autonomous-ai-processor — stub handler

History
-------
The previous source file was a 20-byte literal "400: Invalid request" string,
written by the 2026-04-25 backfill_lambda_sources.py script which captured
the AWS GetFunction error response into the source on disk. That source has
been deployed via CI/CD on every commit since, causing every scheduled
invocation (rate=5min, ~288/day) to fail.

This stub stops the failure cascade and returns a clean 200. It performs no
work — when the original purpose is reidentified, replace the body of
lambda_handler.

Last broken commit: 2c826a3 (auto-commit from run 24919175427)
"""
import json
from datetime import datetime, timezone


def lambda_handler(event, context):
    """No-op handler that returns a structured 200 so EB invocations stop erroring."""
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({
            "service": "autonomous-ai-processor",
            "status": "stub",
            "note": "handler is a no-op stub awaiting reimplementation",
            "invoked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "request_id": getattr(context, "aws_request_id", None),
            "trigger": (
                event.get("source")
                or event.get("requestContext", {}).get("http", {}).get("path")
                or "unknown"
            ),
        }),
    }
