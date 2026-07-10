#!/usr/bin/env python3
"""ops 3078 -- reproduce Khalid's exact view: fetch accumulation.html
+ its JSON the way a BROWSER does (plain URL, no cache-buster, no
no-cache header), log cf-cache-status/age, and poll until the edge
serves the reversal build (GH Pages max-age=600 -> edge revalidates
within ~10 min; our fetch also warms the fresh object for him).
Render already proven clean by local replay; live-HTML markers proven
by 3077 (cache-busted). This closes the last hop."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]


def get(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    r = urllib.request.urlopen(req, timeout=25)
    return r.read().decode("utf-8", "replace"), dict(r.headers)


def main():
    fails, warns = [], []
    with report("3078_browser_view") as rep:
        rep.section("1. Plain-URL page (browser-style)")
        ok, hd = False, {}
        for i in range(24):
            pg, hd = get("https://justhodl.ai/accumulation.html")
            if "200DMA \u2191" in pg and 'id="rev-bottoms"' in pg:
                ok = True
                rep.kv(edge_fresh_after_s=i * 30,
                       cf_cache=hd.get("cf-cache-status"),
                       age=hd.get("age"))
                break
            rep.log("edge still stale (cf=%s age=%s) -- waiting"
                    % (hd.get("cf-cache-status"), hd.get("age")))
            time.sleep(30)
        if not ok:
            fails.append("plain URL still pre-reversal after 12min "
                         "(cf=%s)" % hd.get("cf-cache-status"))

        rep.section("2. Plain-URL data JSON")
        dj, jh = get("https://justhodl.ai/data/"
                     "accumulation-radar.json")
        try:
            dd = json.loads(dj)
            rep.kv(json_version=dd.get("version"),
                   json_cf=jh.get("cf-cache-status"),
                   has_reversals=bool(dd.get("reversals")),
                   n_tops=len((dd.get("reversals") or {})
                              .get("tops") or []))
            if dd.get("version") != "1.4.1" \
                    or not dd.get("reversals"):
                warns.append("plain-URL JSON still %s -- edge will "
                             "revalidate on TTL; HTML renders "
                             "empty-state frames meanwhile"
                             % dd.get("version"))
        except Exception as e:
            fails.append("json: %s" % str(e)[:80])

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- browser-path serves the reversal build")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3078.json").write_text(json.dumps(
        {"ops": 3078, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
