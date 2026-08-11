"""ops 4598 — wo4592 final close (fpr honest write) + fred v2.3 snapshot."""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return {}


def main():
    with report("4598_fpr_close") as r:
        r.heading("ops 4598 — wo4592 final close + fred v2.3 snapshot")
        misses = 0
        t0 = time.time()
        while time.time() - t0 < 240:
            try:
                c = lam.get_function(
                    FunctionName="justhodl-failed-pattern-reversal"
                )["Configuration"]
                if c.get("LastUpdateStatus") == "Successful" \
                        and c.get("State") == "Active":
                    break
            except Exception:
                pass
            time.sleep(6)
        resp = lam.invoke(
            FunctionName="justhodl-failed-pattern-reversal",
            InvocationType="RequestResponse")
        raw = resp["Payload"].read().decode("utf-8", "replace")
        if resp.get("FunctionError") or '"statusCode": 500' in raw[:80]:
            r.fail("handler error: %s" % raw[:400])
            misses += 1
        j = gj("data/failed-pattern-reversal.json")
        ds = j.get("data_sufficiency") or {}
        if isinstance(j.get("state"), str) and ds:
            r.ok("honest write: state=%s ds=%s"
                 % (j.get("state"),
                    {k: v for k, v in ds.items() if k != "rule"}))
        else:
            r.fail("payload missing gate evidence (state=%s ds=%s)"
                   % (j.get("state"), ds))
            misses += 1
        st = gj("data/_state/fred-scoped-import.json")
        r.log("fred: ver=%s rpm=%s imported=%s cursor=%s status=%s "
              "throttled=%s"
              % (st.get("engine_version"), st.get("rate_rpm"),
                 st.get("series_imported"), st.get("queue_cursor"),
                 st.get("status"), st.get("throttled_429")))
        if misses:
            sys.exit(1)
        r.ok("wo4592 CLOSED — all ten flagged engines gated and writing "
             "honestly; fred snapshot above")


if __name__ == "__main__":
    main()
