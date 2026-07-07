#!/usr/bin/env python3
"""ops 2970 -- ship the Asset Compass surface: asset-compass.html (macro
forward strip, ER / asymmetry / breakout boards, data-fit betas, monthly
discovery candidates, full universe table) + the Industry Compass
renderer inside why.html (perf-gap table, stock GK components, laggard-
catchup verdict badge, rate read). This script runs AFTER the pages
deploy fires on the same push and verifies the LIVE site:

  (0) poll https://justhodl.ai/asset-compass.html until it serves the
      new page (marker: the data/asset-compass.json fetch), and
      why.html until it carries renderIndustryCompass, up to ~6 min;
  (1) verify the public /data/ proxy serves data/asset-compass.json
      fresh (<36h, schema 1.0, boards populated) and
      data/asset-discovery.json (warn-only if absent -- the page
      degrades gracefully and ops 2969 owns that engine's verdict);
  (2) confirm the why.html live payload carries the verdict-badge CSS
      so the renderer isn't a dead reference.

Registry note: both engines flip to "wired" on the next ops-2876
compile because the page references their data keys.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
SITE = "https://justhodl.ai"
PAGE = SITE + "/asset-compass.html"
WHY = SITE + "/why.html"


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2970",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def fail(rep, fails, msg):
    fails.append(msg)
    rep.fail(msg)


def main():
    fails, warns = [], []
    hl = {}
    with report("2970_asset_compass_page") as rep:

        rep.section("0. Wait for the pages deploy (live-site polling)")
        page_ok = why_ok = False
        page_html = why_html = ""
        for i in range(36):
            bust = "?v=%d" % int(time.time())
            if not page_ok:
                try:
                    st, page_html = http_get(PAGE + bust)
                    page_ok = (st == 200 and
                               "data/asset-compass.json" in page_html)
                except Exception:
                    pass
            if not why_ok:
                try:
                    st, why_html = http_get(WHY + bust)
                    why_ok = (st == 200 and
                              "renderIndustryCompass" in why_html)
                except Exception:
                    pass
            if page_ok and why_ok:
                break
            time.sleep(10)
        rep.kv(page_live=page_ok, why_live=why_ok,
               waited_s=(i + 1) * 10)
        if not page_ok:
            fail(rep, fails, "asset-compass.html never went live with the "
                 "data marker (pages deploy failed or blocked?)")
        if not why_ok:
            fail(rep, fails, "live why.html lacks renderIndustryCompass")
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("1. Public /data/ proxy serves both engine JSONs")
        try:
            st, body = http_get(SITE + "/data/asset-compass.json?t=%d"
                                % int(time.time()))
            d = json.loads(body)
            age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(
                d.get("generated_at", "").replace("Z", "+00:00"))
            ).total_seconds() / 3600.0
            boards = d.get("boards") or {}
            hl["compass_age_h"] = round(age_h, 2)
            hl["er_board_n"] = len(boards.get("er_ranking") or [])
            hl["asym_board_n"] = len(boards.get("asymmetry_ranking") or [])
            rep.kv(compass_status=st, schema=d.get("schema_version"),
                   age_h=hl["compass_age_h"], er_n=hl["er_board_n"],
                   asym_n=hl["asym_board_n"])
            if d.get("schema_version") != "1.0":
                fail(rep, fails, "compass schema %r"
                     % d.get("schema_version"))
            if age_h > 36:
                fail(rep, fails, "compass json stale via public path: "
                     "%.1fh" % age_h)
            if hl["er_board_n"] < 9 or hl["asym_board_n"] < 5:
                fail(rep, fails, "boards thin via public path: er=%d "
                     "asym=%d" % (hl["er_board_n"], hl["asym_board_n"]))
        except Exception as e:
            fail(rep, fails, "public data/asset-compass.json unreadable: %s"
                 % str(e)[:120])
        try:
            st, body = http_get(SITE + "/data/asset-discovery.json?t=%d"
                                % int(time.time()))
            dd = json.loads(body)
            hl["discovery_month"] = dd.get("month")
            hl["discovery_llm"] = dd.get("llm_status")
            hl["discovery_n"] = len(dd.get("candidates") or [])
            rep.kv(discovery_status=st, month=hl["discovery_month"],
                   llm=hl["discovery_llm"], candidates=hl["discovery_n"])
        except Exception as e:
            warns.append("public data/asset-discovery.json not readable "
                         "yet (%s) -- page degrades gracefully; ops 2969 "
                         "owns that engine" % str(e)[:80])

        rep.section("2. Renderer wiring sanity in the live payloads")
        if ".icverdict" not in why_html:
            fail(rep, fails, "why.html live payload missing the "
                 "industry-compass CSS (partial deploy?)")
        for marker in ("Asymmetry Ranking", "Monthly Discovery",
                       "SURVIVAL_GATE", "data/asset-discovery.json"):
            if marker not in page_html:
                fail(rep, fails, "asset-compass.html missing section "
                     "marker %r" % marker)

        if not fails:
            rep.ok("live: %s (compass %.1fh old, er board %d, asym %d; "
                   "discovery %s/%s) + why.html Industry Compass renderer"
                   % (PAGE, hl.get("compass_age_h") or -1,
                      hl.get("er_board_n") or 0, hl.get("asym_board_n")
                      or 0, hl.get("discovery_month"),
                      hl.get("discovery_llm")))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2970, "page": "asset-compass.html", "fails": fails,
           "warns": warns, "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2970.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    rep.log("report written: %s" % rp)
    if fails:
        sys.exit(1)   # run-ops keys on the exit code (2966 convention)


main()
sys.exit(0)
