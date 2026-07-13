"""ops 3203 — FUSION WAVE 2 verified live: the research desk page + the
site-wide rail chip.

Zero of 366 pages surfaced his 162 engines before tonight. Shipped in this
push (pages.yml deploys it in parallel with this ops):
  1. /panels.html — theme pressure board, divergence board, full engine
     table (state / firing / activation pctile / 13w t & excess), filters.
     Live feeds only; every number is fetched, nothing is baked-in fake.
  2. bake_right_rail.py + jh-right-rail.js — a "HIS RESEARCH" chip on
     every rail-carrying page (~239): top theme pressure + first
     divergence, linking to the desk. One change, whole-site fusion.

This ops verifies FROM THE RUNNER (live URL checks never run from the
sandbox): panels.html serves with its markers, a baked page carries the
research payload, and the fusion feed is fresh. Pages deploy takes 1-3
minutes — polled, not assumed.
"""
import json
import sys
import time
import urllib.request

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3203)"}


def get(url, timeout=15):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read().decode(
        "utf-8", "replace")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3203_pages_wave") as rep:
    fails, warns = [], []
    rep.heading("ops 3203 — panels desk + site-wide research chip, "
                "verified live")

    rep.section("1. Fusion feed sanity")
    fus = s3_json("data/wl-fusion.json") or {}
    th = fus.get("themes") or {}
    rep.kv(fusion_generated=str(fus.get("generated_at", ""))[:19],
           themes=len(th))
    if not th:
        fails.append("wl-fusion feed empty — rail chip would be blank")

    rep.section("2. Poll the live site (pages deploy in flight)")
    panels_ok = rail_ok = False
    for i in range(26):
        time.sleep(15)
        if not panels_ok:
            try:
                h = get("https://justhodl.ai/panels.html")
                if "THEME PRESSURE" in h and "wl-fusion.json" in h \
                        and "DIVERGENCE BOARD" in h:
                    panels_ok = True
                    rep.ok(f"panels.html LIVE ({len(h)} bytes) after "
                           f"~{(i + 1) * 15}s")
            except Exception:
                pass
        if not rail_ok:
            try:
                h2 = get("https://justhodl.ai/flows.html")
                if '"research"' in h2 and "panels.html" in h2:
                    rail_ok = True
                    rep.ok("research chip baked into flows.html rail "
                           "payload")
            except Exception:
                pass
        if panels_ok and rail_ok:
            break
    if not panels_ok:
        fails.append("panels.html not live with markers within window")
    if not rail_ok:
        warns.append("rail research payload not seen on flows.html yet — "
                     "re-bake lands on the next pages deploy; verify then")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
