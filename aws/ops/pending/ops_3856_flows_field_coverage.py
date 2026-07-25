"""
ops_3856 — PROBE: field-coverage audit, etf-flows/* vs the SERVED flows.html.
WRITES NO CODE. Feeds one consolidated page commit.

Doctrine (AUTONOMY.md, POST-DEPLOY FIELD-COVERAGE AUDIT): an engine whose data
no human can see is half-built. So: dump every key the LIVE artifact publishes
(top-level AND the union across rows), grep the SERVED page for each, and treat
any key with no render path as an open bug — surface it or record why not.

Known-live context from ops 3852/3855: daily.json = 300 rows, 296 z-scored,
edge byte-matches S3, all four previously-dead sections now render.

Two things this run must separate honestly:
  - REAL gaps: a field the engine computes that the page never shows.
  - DYNAMIC containers: keys only reachable through Object.entries / data-driven
    loops, which can never appear as source literals. Blindly counting those as
    gaps is what made a previous audit wrong (rotation arc), so they are
    classified, not waived silently.

Also verifies the stale-literal defect I spotted: the page hardcodes "84 ETFs"
in the heading and methodology while universe_size is now 300.
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

PAGE = "https://justhodl.ai/flows.html"
CDN = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

FEEDS = {
    "daily": "etf-flows/daily.json",
    "composite": "etf-flows/composite.json",
    "rotation": "etf-flows/rotation.json",
    "ai": "etf-flows/ai-analysis.json",
    "constituent": "etf-flows/constituent-pressure.json",
}
# keys whose only render path is a data-driven loop (Object.entries / map over
# values) — cannot be source literals, so absence of the literal is not a gap
DYNAMIC_OK = {"components", "scores", "by_category", "by_sector", "metrics",
              "history", "raw_sample"}


def get(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(
                f"{url}{'&' if '?' in url else '?'}v={int(time.time())}-{i}",
                headers={"User-Agent": UA, "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception:
            time.sleep(5)
    return None


def main():
    with report("3856_flows_field_coverage") as rep:
        rep.heading("ops 3856 — PROBE: does flows.html render everything etf-flows publishes")
        gaps, dynamic, missing_feeds = [], [], []

        rep.section("1. pull the SERVED page (repo copy is not what users get)")
        html = get(PAGE)
        if not html:
            rep.fail("  could not fetch the served page")
            sys.exit(1)
        rep.ok(f"  {len(html):,} bytes served")

        rep.section("2. pull every etf-flows artifact the page could consume")
        data = {}
        for name, key in FEEDS.items():
            raw = get(f"{CDN}/{key}")
            if raw is None:
                rep.fail(f"  {key}: unreachable")
                missing_feeds.append(key)
                continue
            try:
                data[name] = json.loads(raw)
                rep.ok(f"  {key}: {len(raw):,} bytes")
            except Exception as e:
                rep.fail(f"  {key}: not JSON ({str(e)[:80]})")
                missing_feeds.append(key)

        rep.section("3. daily.json — union of per-row keys vs page render paths")
        rows = (data.get("daily") or {}).get("metrics") or []
        union = sorted({k for r in rows for k in r.keys()})
        rep.log(f"  {len(rows)} rows, {len(union)} distinct row keys")
        for k in union:
            n = sum(1 for r in rows if r.get(k) is not None)
            rendered = k in html
            if rendered:
                rep.ok(f"  {k:<24} {n:>4}/{len(rows)} populated · rendered")
            elif k in DYNAMIC_OK:
                rep.log(f"  {k:<24} {n:>4}/{len(rows)} populated · DYNAMIC container")
                dynamic.append(k)
            else:
                rep.fail(f"  {k:<24} {n:>4}/{len(rows)} populated · NO RENDER PATH")
                gaps.append(f"daily.metrics[].{k}")

        rep.section("4. top-level keys of each artifact vs the page")
        for name, d in data.items():
            if not isinstance(d, dict):
                continue
            for k in sorted(d.keys()):
                if k in html or k in DYNAMIC_OK:
                    continue
                v = d[k]
                size = len(v) if isinstance(v, (list, dict)) else 1
                rep.fail(f"  {name}.{k:<28} (size {size}) · NO RENDER PATH")
                gaps.append(f"{name}.{k}")

        rep.section("5. stale literals — hardcoded counts vs live universe_size")
        usz = (data.get("daily") or {}).get("universe_size")
        n_ok = (data.get("daily") or {}).get("n_ok")
        hard = re.findall(r"(\d{2,4})\s*ETFs?", html)
        rep.kv(live_universe_size=usz, live_n_ok=n_ok, hardcoded_counts=str(sorted(set(hard))))
        stale = [h for h in set(hard) if usz and int(h) != int(usz)]
        for h in sorted(stale):
            rep.fail(f"  page says '{h} ETFs' — live universe_size is {usz}")
        if stale:
            gaps.append(f"stale_literal_{'_'.join(sorted(stale))}_ETFs")

        rep.section("6. verdict")
        rep.kv(n_row_keys=len(union), n_gaps=len(gaps), n_dynamic=len(dynamic),
               missing_feeds=str(missing_feeds), gaps=str(gaps[:14]))
        if gaps:
            rep.fail(f"OPEN BUGS {len(gaps)}: {gaps}")
            sys.exit(1)
        rep.ok("PASS — every published field has a render path")


if __name__ == "__main__":
    main()
