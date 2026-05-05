"""Census of signal vs outcome counts to identify truly dormant signals."""
import json
import boto3
from boto3.dynamodb.conditions import Attr
from collections import defaultdict
from ops_report import report

ddb = boto3.resource("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

sigs_tbl = ddb.Table("justhodl-signals")
out_tbl = ddb.Table("justhodl-outcomes")


def main():
    with report("census_dormant_signals") as r:
        r.heading("1) All logged signal types in justhodl-signals")
        sig_counts = defaultdict(int)
        last_key = None
        pages = 0
        while True:
            kw = {"Limit": 1000}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = sigs_tbl.scan(**kw)
            for item in resp.get("Items", []):
                sig_counts[item.get("signal_type", "?")] += 1
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 25:
                break
        r.log(f"  scanned {sum(sig_counts.values())} signals across {pages} pages, {len(sig_counts)} types:")
        for st in sorted(sig_counts.keys()):
            r.log(f"    {st:40s}  n={sig_counts[st]}")

        r.heading("2) Outcome counts per signal type (scored, non-legacy)")
        out_counts = defaultdict(int)
        last_key = None
        pages = 0
        while True:
            kw = {
                "Limit": 1000,
                "FilterExpression": (Attr("correct").eq(True) | Attr("correct").eq(False))
                                    & Attr("is_legacy").ne(True),
            }
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = out_tbl.scan(**kw)
            for item in resp.get("Items", []):
                out_counts[item.get("signal_type", "?")] += 1
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 30:
                break
        r.log(f"  scanned {sum(out_counts.values())} scored outcomes across {pages} pages, {len(out_counts)} types:")
        for st in sorted(out_counts.keys()):
            r.log(f"    {st:40s}  n={out_counts[st]}")

        r.heading("3) DORMANT — logged but never scored")
        any_dormant = False
        for st in sorted(sig_counts.keys()):
            n_sig = sig_counts[st]
            n_out = out_counts.get(st, 0)
            if n_out == 0 and n_sig > 0:
                r.log(f"  ⚠ {st:40s}  signals={n_sig}, outcomes=0")
                any_dormant = True
        if not any_dormant:
            r.log("  ✓ no dormant signal types — all logged signals have at least one scored outcome")

        r.heading("4) UNDER-CALIBRATED — scored but n<5 (falls back to default weight)")
        for st in sorted(out_counts.keys()):
            if 0 < out_counts[st] < 5:
                r.log(f"  ⚠ {st:40s}  outcomes={out_counts[st]} (need ≥5 for calibration)")

        r.heading("5) Calibration weight registry (from calibration/latest.json)")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            d = json.loads(obj["Body"].read())
            weights = d.get("weights") or {}
            r.log(f"  signals with calibrated weights: {len(weights)}")
            r.log("  Signals in calibration but NOT in DDB outcomes (zombie weights?):")
            for st in sorted(weights.keys()):
                if out_counts.get(st, 0) == 0:
                    r.log(f"    ⚠ {st:40s}  weight={weights[st]:.3f}, no outcomes")
            r.log("  Signals in DDB outcomes but NOT in calibration weights (missing weight?):")
            for st in sorted(out_counts.keys()):
                if st not in weights:
                    r.log(f"    ⚠ {st:40s}  n={out_counts[st]}, no weight")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
