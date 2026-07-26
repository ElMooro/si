"""
ops_3899 — PROBE (writes no code): two SEPARATE n=0 bugs, two separate root
causes, now precisely scoped by reading both engines' full source:

  signal-backtest's n_observations=0 comes ONLY from
  data/track-record/snapshots/{date}.json via list_snapshots() + reading
  snap["picks"][ticker]["p"] (entry price) — a DIFFERENT data source than
  factor-ic.json (which the SAME lambda computes from a separate panel
  source and which IS mature/healthy). So the bug is isolated to this one
  snapshot pathway, not a fleet-wide validation blindness.

  alpha-calibrator's n=0 comes from data/trade-journal.json via a DynamoDB
  scan (table justhodl-trades) — again a totally separate data source.

This checks both real, live: does data/track-record/snapshots/ actually
have dated files, in the shape list_snapshots()+the picks-reading code
expects; and does the trade-journal / DDB table actually have real,
evaluated entries.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3899_two_n0_bugs_diagnosed") as rep:
        rep.heading("ops 3899 — diagnose signal-backtest's snapshot bug + alpha-calibrator's trade-journal bug")
        failures = []

        rep.section("1. data/track-record/snapshots/ — does it actually have dated files")
        try:
            paginator = s3.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=BUCKET, Prefix="data/track-record/snapshots/"):
                for o in page.get("Contents", []) or []:
                    keys.append((o["Key"], o["LastModified"], o["Size"]))
        except Exception as e:
            rep.fail(f"  list failed: {str(e)[:200]}")
            failures.append("list-snapshots")
            keys = []
        rep.kv(n_snapshot_files=len(keys))
        if keys:
            keys.sort()
            rep.log(f"  earliest: {keys[0][0]} ({keys[0][1]})")
            rep.log(f"  latest:   {keys[-1][0]} ({keys[-1][1]})")
            # check the AGE gate specifically: signal-backtest requires age>=7 days
            today = datetime.now(timezone.utc).date()
            aged = []
            for k, lm, sz in keys:
                d = k.split("/")[-1].replace(".json", "")
                try:
                    from datetime import date as _date
                    age = (today - _date.fromisoformat(d)).days
                    aged.append((d, age, sz))
                except Exception:
                    rep.log(f"  UNPARSEABLE date in filename: {k}")
            n_aged_7plus = sum(1 for _, age, _ in aged if age >= 7)
            rep.kv(n_parseable_dates=len(aged), n_aged_7_plus_days=n_aged_7plus)
            rep.log(f"  age distribution (date, age_days, size_bytes): {aged[:10]}")

            rep.section("2. shape check — does the OLDEST aged-7+ snapshot actually have a real 'picks' dict")
            aged7 = [(d, age) for d, age, _ in aged if age >= 7]
            if aged7:
                target_date = aged7[0][0]
                target_key = f"data/track-record/snapshots/{target_date}.json"
                try:
                    o = s3.get_object(Bucket=BUCKET, Key=target_key)
                    snap = json.loads(o["Body"].read())
                    rep.ok(f"  {target_key} readable, top-level keys: {sorted(snap.keys())}")
                    picks = snap.get("picks")
                    rep.kv(has_picks_key=picks is not None,
                           picks_type=type(picks).__name__ if picks is not None else None,
                           n_picks=len(picks) if isinstance(picks, dict) else
                                   (len(picks) if isinstance(picks, list) else None))
                    if isinstance(picks, dict):
                        k0 = next(iter(picks))
                        rep.log(f"  sample pick [{k0}]: {json.dumps(picks[k0], default=str)[:400]}")
                        rep.kv(sample_has_p_field="p" in picks[k0] if isinstance(picks[k0], dict) else None)
                    elif isinstance(picks, list) and picks:
                        rep.log(f"  picks is a LIST not a dict — signal-backtest's code does "
                                f"`.items()` on it, which would crash or silently skip everything "
                                f"if it's actually a list: sample={json.dumps(picks[0], default=str)[:400]}")
                    elif picks is None:
                        rep.fail(f"  NO 'picks' key at all — real top-level keys are {sorted(snap.keys())}, "
                                 f"signal-backtest reads snap.get('picks') which returns None -> "
                                 f"the whole snapshot silently contributes ZERO records")
                except Exception as e:
                    rep.fail(f"  {target_key} unreadable: {str(e)[:200]}")
            else:
                rep.fail("  no snapshot is >=7 days old yet — this alone would fully explain "
                         "n_observations=0 (genuine bootstrapping, not a bug)")
        else:
            rep.fail("  ZERO files under data/track-record/snapshots/ — either the writer "
                     "engine (opportunity-engine) isn't running, or it writes somewhere else")
            failures.append("no-snapshots-at-all")

        rep.section("3. data/trade-journal.json — does alpha-calibrator's OWN journal read have real data")
        try:
            tj = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-journal.json")["Body"].read())
            rep.ok(f"  top-level keys: {sorted(tj.keys()) if isinstance(tj, dict) else type(tj)}")
            rep.log(f"  FULL DOC (first 1500 chars): {json.dumps(tj, default=str)[:1500]}")
        except Exception as e:
            rep.fail(f"  data/trade-journal.json unreadable: {str(e)[:200]}")
            failures.append("trade-journal-s3")

        rep.section("4. justhodl-trades DynamoDB table — the REAL source alpha-calibrator scans")
        try:
            ddb = boto3.resource("dynamodb", region_name="us-east-1")
            table = ddb.Table("justhodl-trades")
            desc = table.meta.client.describe_table(TableName="justhodl-trades")["Table"]
            rep.kv(table_status=desc.get("TableStatus"),
                   item_count_approx=desc.get("ItemCount"),
                   table_size_bytes=desc.get("TableSizeBytes"))
            scan = table.scan(Limit=5)
            items = scan.get("Items", [])
            rep.log(f"  sample of {len(items)} items: {json.dumps(items, default=str)[:1200]}")
            if items:
                has_outcome = sum(1 for it in items if "outcome_30d" in it or "return_pct" in it)
                rep.kv(sample_items_with_outcome_field=f"{has_outcome}/{len(items)}")
        except Exception as e:
            rep.fail(f"  DynamoDB scan failed: {str(e)[:250]}")
            failures.append("ddb-trades")

        rep.section("verdict")
        rep.kv(failures=str(failures))
        if len(failures) >= 3:
            rep.fail(f"most core reads failed: {failures}")
            sys.exit(1)
        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
