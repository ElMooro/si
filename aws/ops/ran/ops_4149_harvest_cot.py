"""ops_4149 — harvest re-audit + COT discovery: his symbol shapes,
the fleet CFTC store anatomy, join overlap."""
import json
import re
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def j(key):
    return json.loads(s3.get_object(Bucket=BUCKET,
                                    Key=key)["Body"].read())


def main():
    with report("4149_harvest_cot") as rep:
        rep.heading("ops 4149 — harvest re-audit + COT discovery")

        rep.section("A. harvest v1.8.3 — done?")
        d = j("data/tv-sources.json")
        rows = (d.get("sources") or d.get("symbols")
                or d.get("by_symbol") or d)
        items = list(rows.items()) if isinstance(rows, dict) else [
            (r.get("symbol"), r) for r in rows]
        n = len(items)
        rep.kv(entries=n, delta_since_last_audit=n - 2019,
               generated_at=d.get("generated_at") or d.get("updated_at"))
        wl = j("data/tv-watchlists.json")
        uni = set()
        for l in (wl.get("lists") or []):
            uni.update(map(str, l.get("symbols") or []))
        econ = sum(1 for sy in uni if sy.startswith("ECONOMICS:"))
        harvestable = len(uni) - econ
        rep.kv(union=len(uni), econ_null_by_design=econ,
               harvestable=harvestable,
               pct_of_harvestable=round(100 * n / max(1, harvestable), 1))

        rep.section("B. his COT symbols, verbatim")
        cots = sorted(sy for sy in uni
                      if sy.split(":")[0] in ("COT", "COT3"))
        rep.kv(cot_watched=len(cots))
        for sy in cots[:14]:
            rep.log("  " + sy)

        rep.section("C. fleet CFTC store anatomy")
        for key in ("data/cftc-cot.json", "data/cot-index.json",
                    "data/cftc.json", "data/cot.json"):
            try:
                c = j(key)
            except Exception:
                continue
            rep.kv(store=key,
                   top_keys=json.dumps(list(c)[:8])[:200])
            inner = None
            for k2 in ("by_market", "markets", "data", "series",
                       "by_code", "contracts"):
                if isinstance(c.get(k2), dict):
                    inner = c[k2]
                    rep.kv(container=k2, n_codes=len(inner))
                    break
            if inner:
                ks = list(inner)[:10]
                rep.log("  code sample: " + json.dumps(ks)[:250])
                v0 = inner[ks[0]]
                rep.log("  record sample: "
                        + json.dumps(v0)[:260])
                bare = {sy.split(":", 1)[1] for sy in cots}
                hits = sum(1 for k3 in inner
                           if k3 in bare or str(k3).upper() in bare)
                rep.kv(direct_key_overlap=hits)
            break
        else:
            rep.log("  no cftc store found at guessed keys")
        rep.ok(f"DONE — harvest {n}, COT watched {len(cots)}")


if __name__ == "__main__":
    main()
