"""Pull the most recent ai-brief CloudWatch log stream + scan for telegram lines."""
import boto3
from ops_report import report

REGION = "us-east-1"
LOG_GROUP = "/aws/lambda/justhodl-ai-brief"


def main():
    with report("probe_brief_logs") as r:
        logs = boto3.client("logs", region_name=REGION)

        r.heading("1) List recent log streams")
        try:
            streams = logs.describe_log_streams(
                logGroupName=LOG_GROUP,
                orderBy="LastEventTime",
                descending=True,
                limit=3,
            )
            for s in streams.get("logStreams", []):
                r.log(f"  {s['logStreamName']}  last_event={s.get('lastEventTimestamp')}")
            stream_name = streams["logStreams"][0]["logStreamName"]
        except Exception as e:
            r.log(f"  ✗ {e}")
            return

        r.heading(f"2) Pull events from latest stream: {stream_name}")
        try:
            ev = logs.get_log_events(
                logGroupName=LOG_GROUP,
                logStreamName=stream_name,
                startFromHead=False,
                limit=300,
            )
            messages = [e["message"].rstrip() for e in ev.get("events", [])]
            r.log(f"  total events: {len(messages)}")
            r.log("")
            r.log("  Lines mentioning telegram, decisive-call, or sources:")
            for m in messages:
                lo = m.lower()
                if any(kw in lo for kw in ["telegram", "decisive-call", "tg digest", "digest"]):
                    r.log(f"    {m[:300]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("3) All log lines from latest stream (truncated to 200)")
        try:
            for m in messages[-200:]:
                if m.strip():
                    r.log(f"    {m[:250]}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
