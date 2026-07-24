"""
ops_3829 — PROBE ONLY (writes no engine code)

Forecasts the rotation-dashboard -> best-setups / master-ranker join BEFORE any
splice. The recurring defect class here has cost 4+ ops: a reader keyed on a
vocabulary the producer never writes, joining zero rows while looking healthy.

Dumps:
  (a) the ACTUAL sector strings on live best-setups top_setups + master-ranker,
      with counts — the join is sector -> SPDR ETF, so unmapped strings are
      silent zeroes;
  (b) the sector ETFs present in rotation-dashboard with quadrant / gate /
      confluence, i.e. what a joined row would actually receive;
  (c) the TRUE forecast overlap: how many of the 50 setups and 25 ranker rows
      would receive a tilt, and which sector strings would be DROPPED.

A wire ops follows only if the forecast overlap is materially non-zero.
"""
import json
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

SECTOR_ETF = {
    "technology": "XLK", "information technology": "XLK",
    "financial services": "XLF", "financials": "XLF", "financial": "XLF",
    "healthcare": "XLV", "health care": "XLV",
    "energy": "XLE",
    "basic materials": "XLB", "materials": "XLB",
    "industrials": "XLI", "industrial": "XLI",
    "consumer cyclical": "XLY", "consumer discretionary": "XLY",
    "consumer defensive": "XLP", "consumer staples": "XLP",
    "utilities": "XLU",
    "real estate": "XLRE",
    "communication services": "XLC", "communications": "XLC",
}


def load(k):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())


def main():
    with report("3829_probe_rotation_join") as rep:
        rep.heading("ops 3829 — PROBE: rotation -> setups/ranker join forecast")

        rep.section("A. rotation-dashboard sector ETFs — what a row would receive")
        rd = load("data/rotation-dashboard.json")
        rot = {}
        for a in rd.get("assets", []):
            if a["ticker"] in set(SECTOR_ETF.values()):
                rot[a["ticker"]] = a
                rep.log(f"    {a['ticker']:<5} {a['rrg']['quadrant']:<10} "
                        f"gate={'PASS' if a['trend_gate']['eligible'] else 'FAIL':<4} "
                        f"conf={a['confluence_score']:>6} rank #{a['rank']}")
        rep.ok(f"  {len(rot)}/11 sector ETFs present in rotation-dashboard")
        rep.log(f"  regime={(rd['layer1_regime'].get('quadrant') or {}).get('regime')} "
                f"generated_at={rd.get('generated_at')}")

        results = {}
        for label, key, listkey, n_expect in (
                ("best-setups", "data/best-setups.json", "top_setups", 50),
                ("master-ranker", "data/master-ranker.json", None, 25)):
            rep.section(f"B. {label} — live sector vocabulary")
            try:
                doc = load(key)
            except Exception as e:
                rep.warn(f"  unreadable: {str(e)[:90]}")
                continue
            rows = None
            if listkey and isinstance(doc.get(listkey), list):
                rows = doc[listkey]
                rep.ok(f"  container '{listkey}' -> {len(rows)} rows")
            else:
                for k, v in doc.items():
                    if isinstance(v, list) and v and isinstance(v[0], dict) \
                            and any("ticker" in x or "symbol" in x for x in v[:3]):
                        rows = v
                        rep.ok(f"  container '{k}' -> {len(v)} rows")
                        break
            if not rows:
                rep.warn(f"  no ticker-bearing list found; keys={list(doc)[:14]}")
                continue

            rep.log(f"  row keys: {list(rows[0])[:22]}")
            secs = Counter()
            for r in rows:
                s_ = r.get("sector") or r.get("industry") or "<none>"
                secs[str(s_)] += 1
            mapped = unmapped = 0
            drop = []
            for s_, c in secs.most_common():
                etf = SECTOR_ETF.get(s_.strip().lower())
                hit = bool(etf and etf in rot)
                if hit:
                    mapped += c
                else:
                    unmapped += c
                    drop.append(f"{s_}({c})")
                rep.log(f"    {s_:<26} n={c:<3} -> {etf or 'UNMAPPED':<6} "
                        f"{'✓' if hit else '✗'}")
            pct = round(100 * mapped / max(1, mapped + unmapped), 1)
            results[label] = (mapped, mapped + unmapped, pct)
            (rep.ok if pct >= 60 else rep.warn)(
                f"  FORECAST JOIN: {mapped}/{mapped+unmapped} rows = {pct}%")
            if drop:
                rep.warn(f"  WOULD BE DROPPED: {drop[:10]}")

        rep.kv(**{f"{k}_forecast": f"{v[0]}/{v[1]} ({v[2]}%)"
                  for k, v in results.items()},
               sector_etfs_available=len(rot))

        if not results or all(v[2] < 40 for v in results.values()):
            rep.fail("forecast join <40% everywhere — vocabulary mismatch, DO NOT WIRE")
            sys.exit(1)
        rep.ok("PROBE COMPLETE — no code written; forecast supports wiring")


if __name__ == "__main__":
    main()
