"""
ops_3862 — PROBE: is the master-ranker sector map actually resolving? WRITES NO CODE.

Memory carried this as OPEN ("master-ranker 1/25 = EFFECTIVELY NOT WIRED"), but
a repo audit shows ops 3832 already shipped a fix: _harvest_sectors() walks five
sector-bearing feeds and _ticker_sector() falls back to it. Doctrine says don't
rebuild what exists and don't trust a remembered pointer — so measure.

Decisive numbers this probe produces:
  1. live coverage — of the ranked rows, how many carry a non-neutral
     rotation_mult / risk_regime_mult (i.e. the overlay actually found a sector),
  2. independent rebuild — harvest the SAME five donor feeds here and compute the
     TRUE overlap against the live ranked tickers, so a low number can be blamed
     on the right thing (dead donor vs missing ticker vs stale run),
  3. per-donor attribution — which feed supplies how many pairs, and whether any
     donor is stale or empty (a dead donor silently shrinks the map).
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
cw = boto3.client("logs", region_name="us-east-1")

DONORS = ["screener/data.json", "data/capital-flow-radar.json", "data/deep-value.json",
          "data/accumulation-radar.json", "data/asymmetric-scorer.json"]


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def harvest(doc):
    m = {}

    def walk(o):
        if isinstance(o, dict):
            t = o.get("ticker") or o.get("symbol") or o.get("t")
            sec = o.get("sector") or o.get("sectorName")
            if isinstance(t, str) and isinstance(sec, str) and t and sec:
                m.setdefault(t.upper(), sec)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(doc)
    return m


def main():
    with report("3863_ranker_sector_probe") as rep:
        rep.heading("ops 3863 — PROBE: master-ranker sector resolution, live (no code written)")

        rep.section("1. live master-ranker output")
        try:
            mr, lm = get("data/master-ranker.json")
        except Exception as e:
            rep.fail(f"  master-ranker.json unreadable: {str(e)[:180]}")
            sys.exit(1)
        age_h = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
        # ops 3862 read the wrong container (fell back to feed_freshness) and
        # reported 0/0 — the same producer/consumer key mismatch this ops exists
        # to catch. The scored board is top_tickers; assert it rather than guess.
        rows = mr.get("top_tickers") or []
        rep.log(f"  list-bearing keys: {sorted(k for k, v in mr.items() if isinstance(v, list))}")
        if not rows:
            rep.fail("  top_tickers absent or empty — engine contract changed")
            sys.exit(1)
        rep.log(f"  row keys: {sorted(rows[0].keys())}")
        rep.kv(age_hours=round(age_h, 1), generated_at=str(mr.get("generated_at")),
               n_rows=len(rows))

        rep.section("2. overlay coverage — how many rows actually got a sector")
        def moved(r, k):
            v = r.get(k)
            return v is not None and abs(float(v) - 1.0) > 1e-9
        cov = {k: sum(1 for r in rows if moved(r, k)) for k in
               ("rotation_mult", "risk_regime_mult", "liquidity_regime_mult", "nowcast_regime_mult")}
        n_note = sum(1 for r in rows if r.get("rotation_note"))
        for k, v in cov.items():
            (rep.ok if v > len(rows) * 0.5 else rep.fail)(f"  {k:<24} non-neutral on {v}/{len(rows)}")
        rep.log(f"  rotation_note present on {n_note}/{len(rows)}")

        rep.section("3. rebuild the harvest from the same donors — per-donor attribution")
        tick = [str(r.get("ticker", "")).upper() for r in rows if r.get("ticker")]
        combined, per_donor = {}, {}
        for key in DONORS:
            try:
                doc, dlm = get(key)
            except Exception as e:
                rep.fail(f"  {key:<34} UNREADABLE — {str(e)[:70]}")
                per_donor[key] = 0
                continue
            m = harvest(doc)
            per_donor[key] = len(m)
            hit = sum(1 for t in tick if t in m)
            age = (datetime.now(timezone.utc) - dlm).total_seconds() / 3600
            (rep.ok if m else rep.fail)(
                f"  {key:<34} {len(m):>6} pairs · covers {hit:>2}/{len(tick)} ranked · {age:.0f}h old")
            for k2, v2 in m.items():
                combined.setdefault(k2, v2)

        rep.section("4. TRUE overlap — the number that decides the next move")
        resolved = [t for t in tick if t in combined]
        unresolved = [t for t in tick if t not in combined]
        rep.kv(map_size=len(combined), ranked=len(tick),
               resolved=len(resolved), unresolved=len(unresolved))
        rep.log(f"  unresolved tickers: {unresolved[:25]}")
        (rep.ok if len(resolved) >= len(tick) * 0.8 else rep.fail)(
            f"  {len(resolved)}/{len(tick)} ranked tickers resolvable to a sector")

        rep.section("5. did the deployed engine log its map size on the last run")
        try:
            streams = cw.describe_log_streams(
                logGroupName="/aws/lambda/justhodl-master-ranker",
                orderBy="LastEventTime", descending=True, limit=3)["logStreams"]
            found = False
            for st in streams:
                ev = cw.get_log_events(logGroupName="/aws/lambda/justhodl-master-ranker",
                                       logStreamName=st["logStreamName"], limit=300)["events"]
                for e in ev:
                    if "[sector-map]" in e["message"] or "[census-overlay]" in e["message"]:
                        rep.ok(f"  {e['message'].strip()[:150]}")
                        found = True
            if not found:
                rep.fail("  no [sector-map] line in recent logs — engine may predate ops 3832")
        except Exception as e:
            rep.log(f"  log read skipped: {str(e)[:120]}")

        rep.section("6. verdict")
        if len(resolved) >= len(tick) * 0.8 and cov["rotation_mult"] >= len(rows) * 0.5:
            rep.ok("PASS — sector map resolves and the rotation overlay is live on the majority")
            return
        rep.fail(f"OPEN — resolvable {len(resolved)}/{len(tick)}, "
                 f"rotation non-neutral {cov['rotation_mult']}/{len(rows)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
