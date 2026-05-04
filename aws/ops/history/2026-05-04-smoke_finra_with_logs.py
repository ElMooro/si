"""Smoke + read logs to see exact FINRA failure mode."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-short-interest"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def main():
    with report("smoke_finra_with_logs") as r:
        r.heading("Smoke + read logs")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            r.log(f"  status: {inv['StatusCode']} duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {inv['Payload'].read().decode()[:300]}")
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        # Wait for logs to flush
        time.sleep(3)
        r.section("Latest Lambda logs")
        try:
            log_group = f"/aws/lambda/{LAMBDA_NAME}"
            streams = logs.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=1,
            ).get("logStreams", [])
            if streams:
                events = logs.get_log_events(
                    logGroupName=log_group,
                    logStreamName=streams[0]["logStreamName"],
                    limit=100,
                    startFromHead=False,
                ).get("events", [])
                for ev in events[-50:]:
                    msg = ev["message"].rstrip()
                    if msg.startswith(("START", "END", "REPORT", "INIT_START")):
                        continue
                    r.log(f"    {msg[:300]}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
