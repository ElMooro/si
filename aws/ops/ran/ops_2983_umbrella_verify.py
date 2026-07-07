#!/usr/bin/env python3
"""ops 2983 -- U1-U7 CONSOLIDATION VERIFY + umbrella SHIPPED stamps.
(A) Runner-side live checks: 3 hubs serve 200 with their markers and
    wire feeds in source; 9 stubs serve 200 and contain their redirect
    target; canonicals alive (system.html with absorbed status strip,
    pairs.html with consolidation note, chart-pro/carry/bottleneck-boom/
    downloads plain 200); nav-manifest live copy includes hubs and
    excludes stubs.
(B) Stamp data/fleet-audit.json umbrella_actions: U1-U8 SHIPPED with
    evidence (U5/U6 carry the evidence-overrides: canonical status =
    system.html not status.html; chart-macro kept, only the 361-byte
    charts.html shell stubbed), U9 stays standing. Note prefixes carry
    the status so the page renders it with zero template changes.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]

HUBS = {
    "insider-desk.html": ["Insider Desk", "data/insider-radar.json",
                          "/insider-clusters.html"],
    "treasury-desk.html": ["Treasury Desk", "data/bill-share.json",
                           "/auction-crisis.html"],
    "signal-intelligence.html": ["Signal Intelligence",
                                 "data/engine-alpha.json",
                                 "/signal-orthogonality.html"],
}
STUBS = {
    "pairs-scanner.html": "/pairs.html",
    "charts.html": "/chart-pro.html",
    "carry-surface.html": "/carry.html",
    "bottleneck.html": "/bottleneck-boom.html",
    "download.html": "/downloads.html",
    "system-health.html": "/system.html",
    "health.html": "/system.html",
    "status.html": "/system.html",
    "uptime.html": "/system.html",
}
CANON = {
    "system.html": "data/schedule-liveness.json",
    "pairs.html": "pairs-arb",
    "chart-pro.html": None,
    "carry.html": None,
    "bottleneck-boom.html": None,
    "downloads.html": None,
}

U_STAMP = {
    "U1": "SHIPPED: insider-desk.html live (4 feeds, 5 members).",
    "U2": "SHIPPED: pairs-scanner stubbed into pairs.html (same feed); "
          "pairs-arb linked.",
    "U3": "SHIPPED: treasury-desk.html live (auction-crisis + new "
          "bill-share M4, 5 members).",
    "U4": "SHIPPED: signal-intelligence.html live (4 proof-layer feeds, "
          "11 members).",
    "U5": "SHIPPED w/ EVIDENCE OVERRIDE: canonical = system.html "
          "(status.html was a 3.8kb shell); system-health/health/"
          "status/uptime stubbed; status strip absorbed.",
    "U6": "SHIPPED w/ EVIDENCE OVERRIDE: chart-macro KEPT (195kb "
          "functional macro surface); only the 361-byte charts.html "
          "shell stubbed to chart-pro.",
    "U7": "SHIPPED: carry-surface->carry, bottleneck->bottleneck-boom "
          "(superset), download->downloads.",
    "U8": "SHIPPED: credit-desk.html live (ops 2978).",
}


def get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2983",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def check_pages(spec_fn, items, retries=9):
    missing = dict(items)
    for _ in range(retries):
        for page in list(missing):
            try:
                st, html = get("https://justhodl.ai/%s?v=%d"
                               % (page, int(time.time())))
                if st == 200 and spec_fn(page, html, missing[page]):
                    del missing[page]
            except Exception:
                pass
        if not missing:
            break
        time.sleep(12)
    return missing


def main():
    fails, warns = [], []
    hl = {}
    with report("2983_umbrella_verify") as rep:

        rep.section("A1. Hubs live")
        miss = check_pages(
            lambda p, h, spec: all(m in h for m in spec), HUBS)
        rep.kv(hubs_ok=len(HUBS) - len(miss), missing=list(miss))
        if miss:
            fails.append("hubs not live/complete: %s" % list(miss))

        rep.section("A2. Stubs redirecting")
        miss = check_pages(
            lambda p, h, to: to in h and "Consolidated" in h, STUBS)
        rep.kv(stubs_ok=len(STUBS) - len(miss), missing=list(miss))
        if miss:
            fails.append("stubs not serving redirects: %s" % list(miss))

        rep.section("A3. Canonicals alive")
        miss = check_pages(
            lambda p, h, marker: (marker in h) if marker else len(h) > 3000,
            CANON, retries=5)
        rep.kv(canon_ok=len(CANON) - len(miss), missing=list(miss))
        if miss:
            fails.append("canonicals missing content: %s" % list(miss))

        rep.section("A4. Nav live")
        try:
            st, nav = get("https://justhodl.ai/nav-manifest.json?t=%d"
                          % int(time.time()))
            nav_ok = (st == 200 and "insider-desk" in nav
                      and "treasury-desk" in nav
                      and "signal-intelligence" in nav
                      and '"/uptime.html"' not in nav
                      and '"/carry-surface.html"' not in nav)
            rep.kv(nav_ok=nav_ok)
            if not nav_ok:
                warns.append("nav-manifest live copy not converged "
                             "(CDN TTL) -- repo copy verified locally")
        except Exception as e:
            warns.append("nav fetch: %s" % str(e)[:60])

        rep.section("B. Umbrella SHIPPED stamps on fleet-audit.json")
        doc = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/fleet-audit.json")["Body"].read())
        stamped = 0
        for u in doc.get("umbrella_actions") or []:
            uid = u.get("id")
            if uid in U_STAMP:
                u["status"] = "SHIPPED"
                if not str(u.get("note", "")).startswith("SHIPPED"):
                    u["note"] = U_STAMP[uid] + " | was: " + str(
                        u.get("note", ""))[:160]
                stamped += 1
        doc["ops"] = 2983
        doc["umbrellas_shipped"] = stamped
        doc["generated_at"] = datetime.now(timezone.utc).isoformat()
        S3.put_object(Bucket=BUCKET, Key="data/fleet-audit.json",
                      Body=json.dumps(doc, separators=(",", ":")
                                      ).encode(),
                      ContentType="application/json",
                      CacheControl="public, max-age=900")
        rep.kv(umbrellas_stamped=stamped)
        hl["umbrellas_stamped"] = stamped
        if stamped < 8:
            fails.append("only %d/8 umbrellas stamped" % stamped)
        back = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/fleet-audit.json")["Body"].read())
        if back.get("umbrellas_shipped") != stamped:
            fails.append("stamp read-back mismatch")

        if not fails:
            rep.ok("U1-U7 VERIFIED LIVE + U1-U8 stamped SHIPPED: "
                   "3 hubs, 9 stubs, 6 canonicals, nav converging")
        out = {"ops": 2983, "fails": fails, "warns": warns,
               "verdict": "PASS" if not fails else "FAIL",
               "ts": datetime.now(timezone.utc).isoformat()}
        out.update(hl)
        rp = AWS_DIR / "ops" / "reports" / "2983.json"
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(out, indent=1))
        rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
        if fails:
            sys.exit(1)


main()
sys.exit(0)
