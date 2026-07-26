"""
ops_3909 — PROBE: verify the served live pages that render signal-backtest's
by_verdict data (the conviction-inversion finding) actually serve, contain
the render path, and that the feed behind them is the fresh post-fix copy
(59,021 obs). Claude's sandbox gets 403 on justhodl.ai (registry-only
egress), so this runs from the GitHub runner which has real internet —
the proven pattern all session. Writes no code.
"""
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
CDN = "https://justhodl-data-proxy.raafouis.workers.dev"
PAGES = {
    "scorecard.html": ["by_verdict", "ai_analysis"],
    "proof.html": ["by_verdict"],
    "track-public.html": ["by_verdict"],
}


def get(url):
    req = urllib.request.Request(f"{url}{'&' if '?' in url else '?'}v={int(time.time())}",
                                  headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def main():
    with report("3909_verify_verdict_pages_live") as rep:
        rep.heading("ops 3909 — verify the served pages behind the conviction-inversion finding")
        checks = []

        rep.section("1. the feed itself — is the SERVED signal-backtest.json the fresh post-fix copy")
        try:
            feed = json.loads(get(f"{CDN}/data/signal-backtest.json"))
            n_obs = feed.get("n_observations")
            headline = (feed.get("ai_analysis") or {}).get("headline", "")
            rep.kv(served_n_observations=n_obs, maturity=feed.get("maturity"),
                   headline=headline[:160])
            checks.append(("served feed carries the real 59k observations", (n_obs or 0) > 50000))
            checks.append(("served feed carries the inversion headline", "inverted" in headline.lower()))
        except Exception as e:
            rep.fail(f"  feed unreadable: {str(e)[:200]}")
            checks.append(("served feed readable", False))

        rep.section("2. each page — serves, and contains its render path")
        for page, markers in PAGES.items():
            try:
                html = get(f"https://justhodl.ai/{page}").decode("utf-8", "ignore")
                found = {m: html.count(m) for m in markers}
                ok = all(v > 0 for v in found.values())
                (rep.ok if ok else rep.fail)(
                    f"  {page}: {len(html):,} bytes, markers={found}")
                checks.append((f"{page} serves with render path", ok))
            except Exception as e:
                rep.fail(f"  {page}: {str(e)[:150]}")
                checks.append((f"{page} serves", False))

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok("PASS_ALL — pages live, render paths present, feed is the fresh post-fix copy")


if __name__ == "__main__":
    main()
