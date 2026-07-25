"""
ops_3857 — PROBE: real row shape of constituent-pressure.json. WRITES NO CODE.

ops 3856 found flows.html reading four keys the producer never writes
(top_constituents_by_pressure / all_constituents / n_high_z_etfs / threshold_z
vs top_aggregate_exposure / per_stock_exposure / n_etfs_high_z / -), so the
renderer silently returns and 2.4 MB of constituent pressure stays invisible.

Fixing that means mapping the page onto REAL fields. Typing those field names
from memory is precisely the failure this whole thread came from, so this probe
dumps the actual shape first and writes nothing.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

CDN = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def main():
    with report("3857_constituent_shape") as rep:
        rep.heading("ops 3857 — PROBE: constituent-pressure.json true shape (no code written)")
        req = urllib.request.Request(
            f"{CDN}/etf-flows/constituent-pressure.json?v={int(time.time())}",
            headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=60) as r:
            doc = json.loads(r.read())

        rep.section("1. top-level scalars")
        for k, v in doc.items():
            if not isinstance(v, (list, dict)):
                rep.log(f"  {k:<32} = {v!r}")
            else:
                rep.log(f"  {k:<32} = {type(v).__name__}[{len(v)}]")

        rep.section("2. top_aggregate_exposure — the board the page needs")
        top = doc.get("top_aggregate_exposure") or []
        rep.log(f"  container type={type(top).__name__} n={len(top)}")
        rows = list(top.values()) if isinstance(top, dict) else top
        if rows:
            rep.log(f"  row keys: {sorted(rows[0].keys()) if isinstance(rows[0], dict) else type(rows[0])}")
            for r0 in rows[:3]:
                rep.log(f"  sample: {json.dumps(r0)[:420]}")

        rep.section("3. per_stock_exposure — the universe behind it")
        per = doc.get("per_stock_exposure") or {}
        rep.log(f"  container type={type(per).__name__} n={len(per)}")
        if isinstance(per, dict):
            k0 = next(iter(per))
            rep.log(f"  keyed by: {k0!r}")
            rep.log(f"  value keys: {sorted(per[k0].keys()) if isinstance(per[k0], dict) else type(per[k0])}")
            rep.log(f"  sample: {json.dumps({k0: per[k0]})[:420]}")
        elif per:
            rep.log(f"  row keys: {sorted(per[0].keys())}")
            rep.log(f"  sample: {json.dumps(per[0])[:420]}")

        rep.section("4. nested contributor list (page wants ETF + weight + z per stock)")
        probe = rows[0] if rows else None
        if isinstance(probe, dict):
            for k, v in probe.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    rep.ok(f"  contributor list at '{k}' — item keys {sorted(v[0].keys())}")
                    rep.log(f"    sample: {json.dumps(v[0])[:300]}")

        rep.kv(n_top=len(rows), n_per_stock=len(per),
               top_container=type(top).__name__, per_container=type(per).__name__)
        rep.ok("PROBE COMPLETE — shape captured; page mapping follows in the fix ops")


if __name__ == "__main__":
    main()
