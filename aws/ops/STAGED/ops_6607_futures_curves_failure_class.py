"""ops 6607 - READ-ONLY: why does justhodl-polygon-futures-curves fail every tick? (issue #122)

ops 6606 found: 196 invocations / 98 errors in 24 h (one failure + one async retry per 5-minute lane tick) and BOTH declared
outputs (data/futures-research.json, data/polygon-futures-curves.json) last written 2026-10-10 13:20 UTC - the feed is NOT
current. The engine hides the cause (`raise RuntimeError(...) from None`) but records `failure_class` and the phase reached in
its private request record (audit-private/20260909-originals/futures-research/requests/<sha>.json). This op lists the newest
request records and prints status / phase / failure_class / error / timestamps, plus the per-dataset checkpoints of the newest
failed request. Nothing is written, nothing invoked.
"""
from datetime import datetime, timezone
import json
import sys

import boto3
from botocore.config import Config
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
PREFIX = "audit-private/20260909-originals/futures-research/requests/"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60)


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6607_futures_curves_failure_class") as r:
        r.heading("ops 6607 - futures-curves request records (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))
        objs = []
        tok = None
        while True:
            kw = {"Bucket": LIVE, "Prefix": PREFIX, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            res = s3.list_objects_v2(**kw)
            for o in res.get("Contents") or []:
                if o["Key"].count("/") == PREFIX.count("/"):       # top-level request records only (not /datasets/)
                    objs.append((o["LastModified"], o["Key"], o["Size"]))
            if not res.get("IsTruncated") or len(objs) > 20000:
                break
            tok = res.get("NextContinuationToken")
        objs.sort()
        r.log("request records: %d; oldest %s, newest %s" % (len(objs), objs[0][0].strftime("%Y-%m-%d %H:%M") if objs else None,
                                                               objs[-1][0].strftime("%Y-%m-%d %H:%M") if objs else None))
        by_day = {}
        for lm, _, _ in objs:
            by_day[lm.strftime("%Y-%m-%d")] = by_day.get(lm.strftime("%Y-%m-%d"), 0) + 1
        r.log("records per day (last 14): %s" % json.dumps(sorted(by_day.items())[-14:]))
        classes = {}
        newest_failed = None
        last_complete = None
        for lm, key, size in objs[-40:][::-1]:
            try:
                rec = json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
            except Exception as ex:  # noqa: BLE001
                r.log("%s %s: unreadable %s" % (lm.strftime("%m-%d %H:%M:%S"), key[-20:], type(ex).__name__))
                continue
            line = "%s status=%s phase=%s failure_class=%s error=%s published=%s request_id=%s started=%s completed=%s" % (
                lm.strftime("%m-%d %H:%M:%S"), rec.get("status"), rec.get("phase"), rec.get("failure_class"), rec.get("error"),
                rec.get("published"), str(rec.get("request_id"))[:48], rec.get("started_at"), rec.get("completed_at"))
            r.log(line)
            k = "%s/%s/%s" % (rec.get("status"), rec.get("phase"), rec.get("failure_class"))
            classes[k] = classes.get(k, 0) + 1
            if rec.get("status") == "failed" and newest_failed is None:
                newest_failed = (key, rec)
        r.log("classes over the newest 40: %s" % json.dumps(classes))
        # the most recent complete record anywhere (walk back further, cheaply, up to 400 records)
        for lm, key, size in objs[-400:-40][::-1]:
            try:
                rec = json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
            except Exception:  # noqa: BLE001
                continue
            if rec.get("status") == "complete":
                last_complete = (lm, rec)
                break
        if last_complete:
            r.log("last COMPLETE record: %s published=%s generated_at=%s quality=%s" % (
                last_complete[0].strftime("%Y-%m-%d %H:%M:%S"), last_complete[1].get("published"), last_complete[1].get("generated_at"),
                json.dumps(last_complete[1].get("quality"), default=str)[:300]))
        else:
            r.log("no COMPLETE record among the newest 400")
        if newest_failed:
            key, rec = newest_failed
            r.section("newest failed request: %s" % key.rsplit("/", 1)[-1])
            r.log(json.dumps(rec, default=str)[:3000])
            ds_prefix = key[:-5] + "/datasets/"
            res = s3.list_objects_v2(Bucket=LIVE, Prefix=ds_prefix, MaxKeys=200)
            ds = res.get("Contents") or []
            r.log("dataset checkpoints: %d" % len(ds))
            for o in ds[:12]:
                try:
                    v = json.loads(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read())
                    r.log("  %s: %s" % (o["Key"].rsplit("/", 1)[-1][:16], json.dumps(v, default=str)[:400]))
                except Exception as ex:  # noqa: BLE001
                    r.log("  %s: %s" % (o["Key"].rsplit("/", 1)[-1][:16], type(ex).__name__))
        r.kv(records=len(objs), newest_failed_class=(newest_failed[1].get("failure_class") if newest_failed else None),
             newest_failed_phase=(newest_failed[1].get("phase") if newest_failed else None))
        r.ok("read-only; nothing written or invoked")
    sys.exit(0)


if __name__ == "__main__":
    main()
