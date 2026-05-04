"""
Probe FINRA fetch from inside the Lambda runtime — find why n_finra=0.

Theory: Lambda VPC doesn't have egress to cdn.finra.org, or the urllib
fetch silently fails. Check Lambda logs after triggering a test invoke.
"""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-short-interest"

lam = boto3.client("lambda", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def main():
    with report("probe_finra_in_lambda") as r:
        r.heading("Probe FINRA fetch from inside Lambda")

        r.section("1. Read recent Lambda logs")
        try:
            log_group = f"/aws/lambda/{LAMBDA_NAME}"
            streams = logs.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=3,
            ).get("logStreams", [])
            for s in streams[:2]:
                r.log(f"  stream: {s['logStreamName']}")
                events = logs.get_log_events(
                    logGroupName=log_group,
                    logStreamName=s["logStreamName"],
                    limit=80,
                    startFromHead=False,
                ).get("events", [])
                for ev in events[-50:]:
                    msg = ev["message"].rstrip()
                    if msg.startswith(("START", "END", "REPORT")):
                        continue
                    r.log(f"    {msg[:300]}")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        r.section("2. Inline test of urlopen via a one-shot Lambda invoke")
        try:
            # Build small inline tester: invoke our Lambda with a payload
            # that, if recognized, triggers a debug branch that just hits
            # FINRA and returns the response. But since the Lambda doesn't
            # know about that yet, we'll just do a raw test from this op
            # context (which has the same network as Actions runners).
            import urllib.request
            url = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260501.txt"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)",
            })
            t0 = time.time()
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = resp.read()
                r.log(f"  ✓ from runner: status={resp.status} size={len(body):,} duration={time.time()-t0:.2f}s")
        except Exception as e:
            r.fail(f"  ✗ runner: {e}")


if __name__ == "__main__":
    main()
