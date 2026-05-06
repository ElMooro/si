"""Invoke wave-signal-logger now and dump full handler-by-handler counts."""
import json
import boto3
import time
from ops_report import report

lam = boto3.client("lambda", region_name="us-east-1")


def main():
    with report("invoke_logger_and_dump") as r:
        r.heading("Invoke justhodl-wave-signal-logger")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-wave-signal-logger", InvocationType="RequestResponse", LogType="Tail")
        dt = time.time() - t0
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {dt:.1f}s")
        r.log(f"  resp: {body[:600]}")

        # Decode log tail (last ~4KB of CloudWatch logs)
        import base64
        log_tail = base64.b64decode(resp.get("LogResult", "").encode()).decode()
        r.heading("CloudWatch log tail")
        for line in log_tail.split("\n")[-60:]:
            if line.strip():
                r.log(f"  {line[:200]}")


if __name__ == "__main__":
    main()
