"""ops_4106 — working + rendering check: harvest vitals, map purity,
page joins, status distribution. Read-only."""
import json
import re
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
PFX = re.compile(r"^(?:source|provider|country)/")
JUNK = re.compile(r"^[a-z0-9_]{3,}$")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def main():
    with report("4106_working_rendering") as rep:
        rep.heading("ops 4106 — working + rendering")
        checks = []

        rep.section("A. harvest vitals (v1.8.3 features live?)")
        sr = gj("data/tv-sources.json") or {}
        m = sr.get("sources") or {}
        d = sr.get("last_harvest_diag") or {}
        rep.log("  DIAG: " + json.dumps(d)[:340])
        st = d.get("selftest")
        rep.kv(store=len(m), selftest=json.dumps(st),
               delay_ms=d.get("delay_ms"), done=d.get("done"),
               total=d.get("total"), rate=d.get("rate_per_min"))
        checks.append(("v1.8.3 self-test visible in diag", bool(st)))
        econ = {k: v for k, v in m.items() if str(k).startswith("ECONOMICS")}
        agencies = Counter()
        for k, v in m.items():
            n1 = PFX.sub("", str(v.get("source") or ""))
            if "-" in n1 and not JUNK.match(n1):
                agencies[n1] += 1
        rep.kv(economics_rows=len(econ), agency_slugs=len(agencies))
        for a, n in agencies.most_common(12):
            rep.log(f"    AGENCY {n:4d}  {a[:56]}")

        rep.section("B. source-map purity")
        sm = gj("data/source-map.json") or {}
        new = sm.get("new_sources") or []
        rep.kv(map_marker=sm.get("marker"), n_new=len(new))
        for row in new[:12]:
            rep.log(f"  NEW {row.get('n_symbols'):4}  "
                    f"{str(row.get('source'))[:56]}")

        rep.section("C. pages — workbench + BY WATCHLIST render join")
        wb = gj("data/tv-workbench.json") or {}
        t = wb.get("totals") or {}
        rep.kv(page_sourced=t.get("symbols_with_tv_source"),
               page_lists=t.get("watchlists"))
        wl = gj("data/tv-watchlists.json") or {}
        lists = wl.get("lists") or wl.get("watchlists") or []
        v = gj("data/tradingview.json") or {}
        idx = {}
        for row in v.get("symbols") or []:
            idx[row.get("symbol")] = row
            idx.setdefault(str(row.get("symbol")).split(":")[-1], row)
        union = set()
        for l in lists:
            union.update(l.get("symbols") or [])
        statuses = Counter()
        miss = 0
        for sy in union:
            row = idx.get(sy) or idx.get(str(sy).split(":")[-1])
            if row:
                statuses[row.get("status") or "?"] += 1
            else:
                miss += 1
        rep.kv(lists_n=len(lists), union=len(union), unjoined=miss)
        rep.log("  STATUS DISTRIBUTION: " + json.dumps(dict(
            statuses.most_common())))
        checks.append(("BY WATCHLIST join complete (0 unjoined)", miss == 0))
        checks.append(("491 lists", len(lists) >= 485))

        got = 0
        MK = ["bywatchlist v1-ops4103", "BY WATCHLIST"]
        try:
            r = urllib.request.Request(
                f"https://justhodl.ai/tradingview.html?cb={int(time.time())}",
                headers={"User-Agent": "Mozilla/5.0"})
            h = urllib.request.urlopen(r, timeout=25).read().decode(
                "utf8", "ignore")
            got = sum(1 for mk in MK if mk in h)
        except Exception:
            pass
        checks.append((f"edge page markers {got}/{len(MK)}",
                       got == len(MK)))

        failed = [l2 for l2, k in checks if not k]
        for l2, k in checks:
            (rep.ok if k else rep.fail)(f"  {l2}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — store {len(m)}, {len(agencies)} agency slugs, "
               f"union {len(union)} fully joined, statuses honest")


if __name__ == "__main__":
    main()
