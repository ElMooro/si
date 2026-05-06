"""Count non-legacy outcomes."""
import boto3
from boto3.dynamodb.conditions import Attr
from collections import Counter
from ops_report import report

ddb = boto3.resource("dynamodb", region_name="us-east-1")


def main():
    with report("count_nonlegacy_outcomes") as r:
        r.heading("Count non-legacy outcomes by signal_type")
        tbl = ddb.Table("justhodl-outcomes")

        counts = Counter()
        legacy_counts = Counter()
        last_key = None
        pages = 0
        total = 0
        while True:
            kw = {"Limit": 1000}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            items = resp.get("Items", [])
            total += len(items)
            for it in items:
                st = it.get("signal_type", "?")
                if it.get("is_legacy") is True:
                    legacy_counts[st] += 1
                else:
                    counts[st] += 1
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 12:
                break

        r.log(f"  total scanned: {total}")
        r.log(f"  non-legacy total: {sum(counts.values())}")
        r.log(f"  legacy total:     {sum(legacy_counts.values())}")
        r.log("")
        r.log("  non-legacy top by signal_type:")
        for st, n in counts.most_common(20):
            r.log(f"    {st:35s} n={n}")


if __name__ == "__main__":
    main()
