#!/usr/bin/env python3
"""ops 3061 -- external-audit fix verification (live, post-CDN).
Fixes shipped this push: 12 dead homepage routes rewired to real
pages; downloads de-Bloomberg'd + .vbs retired; ofr/ny-fed
de-OpenBB'd + $42 stripped; errors.html sensitive strings scrubbed;
5 ops dashboards noindex + de-listed from nav; disclaimers on
crypto-opportunities + tax-plan; terms refund aligned to pricing's
7-day guarantee; emoji decimal entities -> glyphs on 27 pages;
bake_seo.py in pages.yml (canonical/OG/description + fresh sitemap
every deploy). lce {{TOTAL}} = repo-clean, verify-only."""
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
import json

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3061"}


def get(path):
    req = urllib.request.Request(
        "https://justhodl.ai/%s?cb=%d" % (path, time.time()),
        headers=UA)
    return urllib.request.urlopen(req, timeout=25).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3061_audit_fixes") as rep:
        rep.section("1. Wait for pages deploy (poll marker)")
        marker_ok = False
        for i in range(24):                       # up to ~8 min
            try:
                if 'href="/options.html"' in get("index.html"):
                    marker_ok = True
                    rep.kv(pages_live_after_s=i * 20)
                    break
            except Exception:
                pass
            time.sleep(20)
        if not marker_ok:
            fails.append("pages deploy never propagated (gex->options"
                         " marker absent after 8min)")
            _fin(rep, fails, warns)
            sys.exit(1)

        rep.section("2. Assert every fix live")
        idx = get("index.html")
        import re
        dead = re.findall(r'href="/(alpha|anomaly|calibration|catalyst'
                          r'|crypto|debate|gex|intel|retail|rotation'
                          r'|short|trades)/"', idx)
        rep.kv(dead_routes_remaining=json.dumps(sorted(set(dead))))
        if dead:
            fails.append("dead routes still linked: %s"
                         % sorted(set(dead)))
        dl = get("downloads.html")
        rep.kv(downloads_bloomberg="Bloomberg" in dl,
               downloads_vbs=".vbs" in dl)
        if "Bloomberg" in dl:
            fails.append("Bloomberg still on downloads")
        if ".vbs" in dl:
            fails.append(".vbs still on downloads")
        for pg in ("ofr.html", "ny-fed.html"):
            x = get(pg)
            if "OpenBB" in x:
                fails.append("OpenBB still on %s" % pg)
            if "$42" in x:
                fails.append("$42 still on %s" % pg)
        er = get("errors.html")
        rep.kv(errors_s3="s3://" in er,
               errors_noindex='name="robots"' in er)
        if "s3://" in er:
            fails.append("errors.html still leaks s3 path")
        for pg in ("observability.html", "system.html",
                   "dep-graph.html", "fleet-health.html"):
            if 'content="noindex' not in get(pg):
                fails.append("%s missing noindex" % pg)
        for pg in ("crypto-opportunities.html", "tax-plan.html"):
            if "not investment advice" not in get(pg):
                fails.append("%s missing disclaimer" % pg)
        tm = get("terms.html")
        if "7-day money-back" not in tm:
            fails.append("terms not aligned")
        lce = get("lce.html")
        rep.kv(lce_template_var="{{TOTAL}}" in lce)
        if "{{TOTAL}}" in lce:
            fails.append("lce still renders {{TOTAL}}")
        mani = json.loads(get("nav-manifest.json"))
        listed = [p["href"] for c in mani["categories"]
                  for p in c["pages"]]
        for op in ("/errors.html", "/observability.html",
                   "/system.html", "/dep-graph.html",
                   "/fleet-health.html"):
            if op in listed:
                fails.append("%s still in nav manifest" % op)

        rep.section("3. SEO layer (canonical/OG + fresh sitemap)")
        seo_ok = 0
        for pg in ("industry-rotation.html", "whales.html",
                   "phases.html"):
            x = get(pg)
            if 'rel="canonical"' in x and 'property="og:title"' in x:
                seo_ok += 1
        sm = get("sitemap.xml")
        n_loc = sm.count("<loc>")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        rep.kv(seo_pages_ok=seo_ok, sitemap_urls=n_loc,
               sitemap_fresh=today in sm)
        if seo_ok < 3:
            fails.append("canonical/OG on %d/3 sampled" % seo_ok)
        if n_loc < 330:
            fails.append("sitemap urls=%d (<330)" % n_loc)
        if today not in sm:
            fails.append("sitemap not regenerated today")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- external audit fixes live")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3061.json").write_text(json.dumps(
        {"ops": 3061, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
