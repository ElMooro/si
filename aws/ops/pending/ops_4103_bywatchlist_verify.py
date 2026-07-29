"""ops_4103 — verify the By-Watchlist mirror at the edge + join sanity."""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
MK = ["bywatchlist v1-ops4103", "BY WATCHLIST", "tv-watchlists.json"]


def main():
    with report("4103_bywatchlist_verify") as rep:
        rep.heading("ops 4103 — By-Watchlist verify")
        checks = []
        got, size = 0, 0
        for i in range(12):
            try:
                r = urllib.request.Request(
                    f"https://justhodl.ai/tradingview.html?cb={int(time.time())}",
                    headers={"User-Agent": "Mozilla/5.0",
                             "Cache-Control": "no-cache"})
                h = urllib.request.urlopen(r, timeout=25).read().decode(
                    "utf8", "ignore")
                size = len(h)
                got = sum(1 for m in MK if m in h)
                if got == len(MK):
                    rep.ok(f"  edge serving new page after ~{i*20}s "
                           f"({size}B, {got}/{len(MK)} markers)")
                    break
            except Exception:
                pass
            time.sleep(20)
        checks.append((f"page at edge {got}/{len(MK)} markers",
                       got == len(MK)))

        wl = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-watchlists.json")["Body"].read())
        lists = wl.get("lists") or wl.get("watchlists") or []
        v = json.loads(s3.get_object(Bucket=BUCKET,
                                     Key="data/tradingview.json")["Body"].read())
        idx = {}
        for row in v.get("symbols") or []:
            idx[row.get("symbol")] = row
            idx.setdefault(str(row.get("symbol")).split(":")[-1], row)
        union, hits, live = set(), 0, 0
        for l in lists:
            for sy in l.get("symbols") or []:
                union.add(sy)
        for sy in union:
            row = idx.get(sy) or idx.get(str(sy).split(":")[-1])
            if row:
                hits += 1
                if row.get("status") == "LIVE":
                    live += 1
        pct = round(hits * 100 / max(1, len(union)), 1)
        rep.kv(lists=len(lists), union_symbols=len(union),
               vault_hits=hits, hit_pct=pct, live_in_union=live)
        checks.append(("join covers >=95% of watchlist symbols", pct >= 95))
        checks.append(("491 lists present", len(lists) >= 485))

        failed = [l2 for l2, k in checks if not k]
        for l2, k in checks:
            (rep.ok if k else rep.fail)(f"  {l2}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {len(lists)} lists x {len(union)} symbols "
               f"mirrored; {hits} joined ({pct}%), {live} LIVE")


if __name__ == "__main__":
    main()
