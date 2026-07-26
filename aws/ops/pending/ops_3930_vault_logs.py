"""ops_3930 — read the crashed v3.0 run's CloudWatch traceback."""
import sys
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

logs = boto3.client("logs", region_name="us-east-1")


def main():
    with report("3930_vault_logs") as rep:
        rep.heading("ops 3930 — justhodl-tradingview crash logs")
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-tradingview",
            orderBy="LastEventTime", descending=True, limit=2)["logStreams"]
        for st in streams:
            evs = logs.get_log_events(logGroupName="/aws/lambda/justhodl-tradingview",
                                      logStreamName=st["logStreamName"],
                                      limit=60)["events"]
            rep.section(st["logStreamName"][-30:])
            for e in evs[-45:]:
                rep.log("  " + e["message"].rstrip()[:220])
        rep.ok("LOGS DUMPED")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
