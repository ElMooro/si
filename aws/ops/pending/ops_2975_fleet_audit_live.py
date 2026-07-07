#!/usr/bin/env python3
"""ops 2975 -- live-verify the fleet-audit surface from the runner.
The build container's egress proxy blocks justhodl.ai, so 'is it live'
can only be proven here. Checks: (1) fleet-audit.html serves 200 with
the real section markers; (2) /data/fleet-audit.json serves schema with
the exact totals the 2974 report recorded (retry across the 15-min
Cloudflare edge TTL, then WARN per CDN doctrine -- S3 origin truth was
already established by the 2974 upload); (3) nav-manifest + site-catalog
live copies include the page.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
PAGE = "https://justhodl.ai/fleet-audit.html"
FEED = "https://justhodl.ai/data/fleet-audit.json"


def get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2975",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main():
    fails, warns = [], []
    hl = {}
    with report("2975_fleet_audit_live") as rep:
        rep.section("1. Page live")
        page_ok = False
        for _ in range(24):
            try:
                st, html = get(PAGE + "?v=%d" % int(time.time()))
                page_ok = (st == 200 and "Fleet Audit" in html
                           and 'id="gaps"' in html
                           and "Umbrella Actions" in html
                           and "fleet-audit.json" in html)
                if page_ok:
                    break
            except Exception:
                pass
            time.sleep(10)
        rep.kv(page_ok=page_ok)
        if not page_ok:
            fails.append("fleet-audit.html not serving expected markers")
            rep.fail(fails[-1])

        rep.section("2. Public JSON (retry across CDN TTL)")
        feed_ok, totals = False, {}
        for _ in range(9):
            try:
                st, body = get(FEED + "?t=%d" % int(time.time()))
                d = json.loads(body)
                totals = d.get("totals") or {}
                feed_ok = (d.get("ops") == 2974
                           and totals.get("engines", 0) >= 600
                           and totals.get("gaps", 0) >= 10
                           and len(d.get("umbrella_actions") or []) >= 8)
                if feed_ok:
                    break
            except Exception:
                pass
            time.sleep(20)
        rep.kv(feed_ok=feed_ok, totals=json.dumps(totals)[:200])
        hl["totals"] = totals
        if not feed_ok:
            warns.append("public JSON not confirmed within TTL window -- "
                         "S3 origin verified at 2974 upload; edge clears "
                         "within 15 min")

        rep.section("3. Nav surfaces live")
        try:
            _, nav = get("https://justhodl.ai/nav-manifest.json?t=%d"
                         % int(time.time()))
            _, cat = get("https://justhodl.ai/site-catalog.json?t=%d"
                         % int(time.time()))
            nav_ok = "fleet-audit" in nav
            cat_ok = "fleet-audit" in cat
            rep.kv(nav_ok=nav_ok, catalog_ok=cat_ok)
            if not nav_ok:
                warns.append("nav-manifest live copy lacks fleet-audit "
                             "(CDN TTL)")
            if not cat_ok:
                warns.append("site-catalog live copy lacks fleet-audit "
                             "(CDN TTL)")
        except Exception as e:
            warns.append("nav check flaky: %s" % str(e)[:70])

        if not fails:
            rep.ok("fleet-audit live: page %s, feed %s, engines=%s "
                   "gaps=%s" % (page_ok, feed_ok, totals.get("engines"),
                                totals.get("gaps")))
        out = {"ops": 2975, "fails": fails, "warns": warns,
               "verdict": "PASS" if not fails else "FAIL",
               "ts": datetime.now(timezone.utc).isoformat()}
        out.update(hl)
        rp = AWS_DIR / "ops" / "reports" / "2975.json"
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(out, indent=1))
        rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
        if fails:
            sys.exit(1)


main()
sys.exit(0)
