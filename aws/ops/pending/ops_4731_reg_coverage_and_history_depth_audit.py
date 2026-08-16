"""
ops/4731 -- two-part read-only audit, both directly requested by Khalid:

  (A) "I want all the data thats imported in my system to be on data.html
      for easy tracking." -- data.html only shows what's in provider-catalog's
      REG dict. Find every real S3 data producer that ISN'T covered by REG
      (peru-copper, taiwan-moea, te-feed, and the whole physical-economy/boom
      fleet were already confirmed missing from REG's 42 slugs by manual
      inspection -- this does the same check exhaustively over all of S3
      instead of by hand).

  (B) "I want all the data that we get to get all its history for easy
      model training." -- freight-pulse's own source comment already
      admits one leg "accrues from today; nothing backfilled." Before
      building anything else, find out how deep every physical-economy/
      boom engine's actual banked history goes, generically, by scanning
      each output doc for ISO date strings and reporting the real min/max
      found -- not the lookback window the code computes over, the actual
      dates present in the data.

Read-only: S3 reads only. No lambda deploys, no writes to production data.
"""
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
DATE_RE = re.compile(r"\b(19[8-9]\d|20[0-4]\d)-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b")

s3 = boto3.client("s3", region_name=REGION)

# Candidate output keys for the physical-economy/boom fleet + everything
# touched in this build (peru-copper, taiwan-moea, TE engines). Some of
# these may not exist under this exact name -- that's itself a finding.
CANDIDATE_KEYS = [
    "data/import-canary.json", "data/port-cargo.json", "data/portwatch.json",
    "data/freight-pulse.json", "data/boom-radar.json", "data/boom-stage.json",
    "data/trade-nowcast.json", "data/air-cargo.json", "data/bis-crossborder.json",
    "data/geo-risk.json", "data/geopolitical-risk.json",
    "data/peru-copper.json", "data/taiwan-moea.json", "data/te-feed.json",
    "data/te-mirror-status.json", "data/boj-detail.json",
]


def date_scan(raw_text):
    found = sorted(set(DATE_RE.findall(raw_text)))
    if not found:
        return None
    dates = [f"{y}-{m}-{d}" for (y, m, d) in found]
    dates.sort()
    return {"n_distinct_dates": len(dates), "earliest": dates[0], "latest": dates[-1]}


def main():
    with report("4731_reg_coverage_and_history_depth_audit") as rep:
        rep.heading("ops 4731 -- REG coverage gap + historical depth audit (read-only)")

        rep.section("Part A -- REG coverage: what's producing real data but invisible on data.html")
        src = open(ROOT / "aws/lambdas/justhodl-provider-catalog/source/lambda_function.py").read()
        reg_block_match = re.search(r"^REG = \{(.*?)\n\}\n", src, re.S | re.M)
        reg_block = reg_block_match.group(1) if reg_block_match else ""
        registered_paths = set(re.findall(r'"(data/[a-zA-Z0-9_/.\-]+)"', reg_block))
        rep.kv(check="reg_registered_slugs", value=len(re.findall(r'^\s*"([a-z0-9_-]+)":\s*\{', src, re.M)))
        rep.kv(check="reg_registered_data_paths", value=len(registered_paths))

        # Top-level data/*.json files
        top_level = []
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/", Delimiter="/")
        for o in resp.get("Contents", []):
            top_level.append(o["Key"])
        rep.log(f"Top-level data/*.json objects found: {len(top_level)}")
        orphaned_top = [k for k in top_level if k not in registered_paths]
        rep.kv(check="orphaned_top_level_json_files", value=len(orphaned_top))
        for k in sorted(orphaned_top):
            rep.log(f"  ORPHAN (no REG entry references this path): {k}")

        # data/warm/*/ prefixes
        warm_prefixes = []
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/warm/", Delimiter="/")
        for p in resp.get("CommonPrefixes", []):
            warm_prefixes.append(p["Prefix"])
        rep.log(f"data/warm/*/ prefixes found: {len(warm_prefixes)}")
        orphaned_warm = [p for p in warm_prefixes
                          if not any(p.startswith(rp) or rp.startswith(p) for rp in registered_paths)]
        rep.kv(check="orphaned_warm_prefixes", value=len(orphaned_warm))
        for p in sorted(orphaned_warm):
            rep.log(f"  ORPHAN (no REG entry references this prefix): {p}")

        rep.section("Part B -- actual historical depth per engine (real dates found in the data, not the lookback window)")
        for key in CANDIDATE_KEYS:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                raw = obj["Body"].read()
                age_h = round((time.time() - obj["LastModified"].timestamp()) / 3600, 1)
                scan = date_scan(raw.decode("utf-8", "replace"))
                if scan is None:
                    rep.warn(f"{key}: exists ({len(raw)}b, {age_h}h old) but NO ISO dates found in it at all "
                             "-- likely a pure current-snapshot doc with no timestamped history")
                    rep.kv(engine=key, exists=True, age_hours=age_h, n_distinct_dates=0,
                           earliest=None, latest=None)
                else:
                    span_note = ""
                    rep.ok(f"{key}: {scan['n_distinct_dates']} distinct dates, "
                            f"{scan['earliest']} -> {scan['latest']}{span_note}")
                    rep.kv(engine=key, exists=True, age_hours=age_h,
                           n_distinct_dates=scan["n_distinct_dates"],
                           earliest=scan["earliest"], latest=scan["latest"])
            except s3.exceptions.NoSuchKey:
                rep.log(f"{key}: does not exist at this path")
            except Exception as e:
                rep.warn(f"{key}: {type(e).__name__} -- {str(e)[:100]}")

        rep.section("Summary")
        rep.log("Read-only audit only -- nothing written to production. "
                 "Part A's orphan list is exactly what needs a REG entry added "
                 "(no rebuild needed, just registration) to show up on data.html. "
                 "Part B's zero-date / single-date engines are exactly what needs "
                 "real backfill work before they're usable for model training -- "
                 "current lookback-window code doesn't equal banked history.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("AUDIT ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
