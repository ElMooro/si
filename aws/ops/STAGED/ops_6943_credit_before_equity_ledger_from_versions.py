"""ops 6943 - the board's own ledger, filled from the bond desk's earlier documents the bucket still holds.

Credit Before Equity 2.0.0 compares a company's spread with the same bonds a week, a month and three months earlier out of
its own ledger (credit/cbe-spreads/<holdings day>.json.gz: every bond's spread that day). The ledger would begin empty and the
one-week comparison would not exist for a week. The bucket keeps the corporate bonds desk's earlier documents as object
versions (ops 6942 counted 13, back to 2026-11-06), each with the holdings of its own day. This writes the ledger day of
every such version by the engine's own arithmetic (cbe_core.bond_points on that day's bonds and that day's Treasury curve).

WRITES: one small file a holdings day under credit/cbe-spreads/, only where no file exists yet. Nothing is invoked, nothing
else is touched, and a day already in the ledger is never overwritten. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source")]
LIVE = "justhodl-dashboard-live"
MIN_BONDS = 8000


def main():
    import boto3
    from ops_report import report
    import cbe_core as C
    import lambda_function as L
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    def parse(raw):
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    with report("ops_6943_credit_before_equity_ledger_from_versions") as r:
        r.heading("ops 6943 - the board's ledger from the bond desk's stored versions, %s" % now.isoformat(timespec="minutes"))
        par_rows = parse(s3.get_object(Bucket=LIVE, Key=L.PAR_KEY)["Body"].read()).get("rows") or {}
        have = L.ledger_days(s3)
        r.log("ledger days before: %s" % sorted(have))
        vers, token = [], {}
        while True:
            page = s3.list_object_versions(Bucket=LIVE, Prefix=L.BONDS_KEY, **token)
            vers += [v for v in page.get("Versions") or [] if v["Key"] == L.BONDS_KEY]
            if not page.get("IsTruncated"):
                break
            token = {"KeyMarker": page["NextKeyMarker"], "VersionIdMarker": page["NextVersionIdMarker"]}
        vers.sort(key=lambda v: v["LastModified"])
        r.log("bond desk document: %d versions, %s .. %s" % (len(vers), vers[0]["LastModified"].isoformat()[:16] if vers else None, vers[-1]["LastModified"].isoformat()[:16] if vers else None))
        by_day = {}
        for v in vers:                                     # the last version of a holdings day is that day's document
            try:
                doc = parse(s3.get_object(Bucket=LIVE, Key=L.BONDS_KEY, VersionId=v["VersionId"])["Body"].read())
            except Exception as e:  # noqa: BLE001
                r.log("version of %s: could not be read (%s)" % (v["LastModified"].isoformat()[:16], type(e).__name__))
                continue
            issues = doc.get("issues") or {}
            day = issues.get("as_of")
            if not day or not issues.get("bonds"):
                r.log("version of %s: no holdings date or no bonds (desk %s) - left out" % (v["LastModified"].isoformat()[:16], doc.get("version")))
                continue
            by_day[day] = (v["LastModified"].isoformat()[:16], doc.get("version"), issues["bonds"])
        wrote, kept, refused = [], [], []
        for day in sorted(by_day):
            stamp, version, bonds = by_day[day]
            points, counts, par_days = C.bond_points(bonds, day, par_rows, {})
            snap = C.snapshot_of(points, day)
            line = "holdings day %s (desk %s, written %s): %d bonds read, %d with a spread, curve of %s" % (day, version, stamp, counts["read"], snap["n"], par_days.get("as_of"))
            if day in have:
                kept.append(day)
                r.log(line + " - already in the ledger, not overwritten")
                continue
            if snap["n"] < MIN_BONDS or par_days.get("as_of") is None:
                refused.append(day)
                r.log(line + " - NOT WRITTEN: under %d bonds or no curve" % MIN_BONDS)
                continue
            snap["from_version_written"] = stamp
            key = L.LEDGER_ROOT + day + ".json.gz"
            size = L.put_json(s3, key, snap, gz=True)
            back = parse(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
            ok = back.get("as_of") == day and len(back.get("bonds") or {}) == snap["n"] and back.get("derived_from") is None
            wrote.append(day)
            r.log(line + " - written, %d bytes, read back %s" % (size, "the same" if ok else "DIFFERENT"))
            if not ok:
                refused.append(day)
        after = sorted(L.ledger_days(s3))
        r.log("ledger days after: %s" % after)
        newest = max(by_day) if by_day else None
        if newest:
            r.log("windows the engine would pick for %s: %s" % (newest, C.pick_windows(newest, [d for d in after if d != newest])))
        r.kv(versions=len(vers), holdings_days=len(by_day), written=len(wrote), already_there=len(kept), refused=len(refused))
        if refused:
            r.fail("LEDGER: %d days were not written or did not read back: %s" % (len(refused), refused))
        else:
            r.ok("LEDGER FILLED: %d days written, %d already there; nothing else was touched" % (len(wrote), len(kept)))


if __name__ == "__main__":
    main()
