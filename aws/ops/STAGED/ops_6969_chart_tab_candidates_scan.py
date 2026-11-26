"""ops 6969 - read-only: how fresh every published document is and how every engine ran this week.

Khalid 2026-11-26: "Scan my entire system and give me list of engines i should add a tab to chart.html suggestions
only". A page is only worth a button on the chart if its engine is alive, so this op gathers the two facts the
repository cannot say: the age of every document under data/ (one listing, no object is read) and each Lambda's
invocations and errors over seven days (CloudWatch metrics). Both are attached for the scan, which is done off line.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6969_chart_tab_candidates_scan"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    with report(NAME) as r:
        r.heading("6969 - ages of every published document and each engine's week (read-only), %s" % now.isoformat(timespec="minutes"))

        r.section("1. Every document directly under data/")
        docs, folders = {}, []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/", Delimiter="/"):
            for o in page.get("Contents", []):
                docs[o["Key"]] = [round((now - o["LastModified"]).total_seconds() / 3600.0, 1), o["Size"]]
            folders.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
        ages = [a for a, _ in docs.values()]
        r.log("documents %d, folders %d | written in the last 30 h: %d, 30 h to 7 d: %d, older than 7 d: %d, older than 30 d: %d" % (
            len(docs), len(folders), sum(a < 30 for a in ages), sum(30 <= a < 168 for a in ages), sum(a >= 168 for a in ages), sum(a >= 720 for a in ages)))

        r.section("2. Each Lambda's invocations and errors, seven days")
        week = {}
        try:
            lam = boto3.client("lambda", region_name=REGION, config=cfg)
            names = [f["FunctionName"] for page in lam.get_paginator("list_functions").paginate() for f in page["Functions"]]
            cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
            queries = []
            for i, n in enumerate(names):
                for m, tag in (("Invocations", "i"), ("Errors", "e")):
                    queries.append({"Id": "%s%d" % (tag, i), "ReturnData": True, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m, "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 604800, "Stat": "Sum"}})
            got = {}
            for k in range(0, len(queries), 500):
                token = None
                while True:
                    kw = {"MetricDataQueries": queries[k:k + 500], "StartTime": now - timedelta(days=7), "EndTime": now}
                    if token:
                        kw["NextToken"] = token
                    resp = cw.get_metric_data(**kw)
                    for x in resp["MetricDataResults"]:
                        got[x["Id"]] = got.get(x["Id"], 0.0) + sum(x.get("Values") or [])
                    token = resp.get("NextToken")
                    if not token:
                        break
            for i, n in enumerate(names):
                week[n] = [int(got.get("i%d" % i, 0)), int(got.get("e%d" % i, 0))]
            ran = [n for n, v in week.items() if v[0]]
            r.log("functions %d | ran this week %d | with an error %d | every run an error: %s" % (len(names), len(ran), sum(1 for v in week.values() if v[1]), ", ".join(sorted(n for n, v in week.items() if v[0] and v[1] >= v[0]))[:900]))
        except Exception as exc:  # noqa: BLE001
            r.log("metrics not read: %s: %s" % (type(exc).__name__, str(exc)[:200]))

        body = gzip.compress(json.dumps({"at": now.isoformat(timespec="seconds"), "docs": docs, "folders": folders, "week": week}, separators=(",", ":")).encode(), 9)
        b64 = base64.b64encode(body).decode("ascii")
        r.section("attachment")
        r.lines.append("<!-- FIXTURE scan %d bytes" % len(body))
        r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
        r.lines.append("FIXTURE-END -->")
        r.kv(documents=len(docs), folders=len(folders), functions=len(week))
        r.ok("gathered; nothing written")


if __name__ == "__main__":
    main()
