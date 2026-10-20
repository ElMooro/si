"""ops 6573 - READ-ONLY: where the 1,820 GB of old versions sit (from the ops 6570 census JSON), and the shape of the
keys that carry more than 10 old versions (daily overwrites vs. anything that could be history-by-versions)."""
from pathlib import Path
import json
import re
import sys

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PRIVATE = "justhodl-ops-private-857687956942"
SENSITIVE = re.compile(r"private|secret|credential|token|password", re.I)


def label(p):
    return "WITHHELD" if SENSITIVE.search(p) else (p or "(root files)")


def gb(b):
    return round(b / 1024 ** 3, 2)


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=PRIVATE, Prefix="census/").get("Contents", []))
    doc = json.loads(s3.get_object(Bucket=PRIVATE, Key=keys[-1])["Body"].read())
    rows = doc["prefixes"]
    with report("ops_6573_census_old_versions") as r:
        r.heading("ops 6573 - census %s: old versions by prefix (read-only)" % keys[-1].split("/")[-1])
        by_old = sorted(rows, key=lambda x: -x["noncurrent_bytes"])[:15]
        r.log("prefix | old versions | old GB | current objects | current GB | max versions on a key | keys > 10 | truncated")
        for x in by_old:
            r.log("- %s | %d | %s | %d | %s | %d | %d | %s" % (label(x["prefix"]), x["noncurrent"], gb(x["noncurrent_bytes"]), x["current"],
                                                        gb(x["current_bytes"]), x["max_versions_key"], x["keys_over_10"], x.get("truncated", False)))
        r.log("keys with > 10 old versions, per prefix (basename, versions):")
        for x in sorted(rows, key=lambda x: -x["keys_over_10"]):
            if x["keys_over_10"]:
                r.log("- %s: %d keys; e.g. %s" % (label(x["prefix"]), x["keys_over_10"],
                                                  [(re.sub(r"[0-9a-f]{16,}", "<hash>", k.split("/")[-1]), n) for k, n in x["sample_over_10"]]))
        tot_old = sum(x["noncurrent_bytes"] for x in rows)
        in_data = sum(x["noncurrent_bytes"] for x in rows if x["prefix"] == "data/" or x["prefix"].startswith("data/"))
        r.kv(old_gb_total=gb(tot_old), old_gb_under_data=gb(in_data), old_gb_outside_data=gb(tot_old - in_data),
             old_versions_total=sum(x["noncurrent"] for x in rows))
        r.ok("done")


if __name__ == "__main__":
    main()
