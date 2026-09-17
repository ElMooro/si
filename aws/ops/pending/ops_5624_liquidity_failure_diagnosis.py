"""Read only: locate the liquidity runtime failure without publishing raw provider diagnostics."""
import sys
import time
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report


def main():
    with report("ops_5624_liquidity_failure_diagnosis") as r:
        r.heading("Liquidity runtime failure location")
        logs = boto3.client("logs", region_name="us-east-1")
        result = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-liquidity-agent",
                                       startTime=int((time.time()-3600)*1000), limit=300)
        for event in result.get("events", []):
            msg = event.get("message", "")
            if "[ERROR]" in msg or "Traceback" in msg:
                # Never persist provider URLs, tokens, or arbitrary message values.
                error = msg.split("[ERROR]", 1)[-1].split(":", 1)[0].strip()
                frames = [line.strip() for line in msg.splitlines() if line.lstrip().startswith('File "/var/task/')]
                r.kv(error_type=error[:100], frames=frames)
        r.ok("Read-only error locations collected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
