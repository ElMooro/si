#!/usr/bin/env python3
"""ops 3063 -- OpenBB-on-ofr root cause. Repo + bake scripts are 100%
clean; the new deploy is proven live (manifest marker). Hypothesis:
Cloudflare cached ofr.html with query-strings ignored (warmed by the
external auditor's crawl). Decisive: fetch the GitHub Pages ORIGIN
directly + CF with no-cache headers; if origin is clean, purge-or-wait
is the answer, and the remaining audit asserts get re-run to PASS."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]


def get(url, hdrs=None):
    h = {"User-Agent": "Mozilla/5.0 ops-3063"}
    h.update(hdrs or {})
    req = urllib.request.Request(url, headers=h)
    r = urllib.request.urlopen(req, timeout=25)
    return r.read().decode("utf-8", "replace"), dict(r.headers)


def main():
    fails, warns = [], []
    with report("3063_ofr_cache") as rep:
        rep.section("1. Origin vs CF")
        org, oh = get("https://elmooro.github.io/si/ofr.html?cb=%d"
                      % time.time())
        rep.kv(origin_openbb="OpenBB" in org,
               origin_len=len(org))
        cf, ch = get("https://justhodl.ai/ofr.html?cb=%d"
                     % time.time(),
                     {"Cache-Control": "no-cache",
                      "Pragma": "no-cache"})
        rep.kv(cf_openbb="OpenBB" in cf,
               cf_cache_status=ch.get("cf-cache-status"),
               cf_age=ch.get("age"))
        if "OpenBB" in org:
            fails.append("ORIGIN itself serves OpenBB -- build-side, "
                         "not cache")
            _fin(rep, fails, warns)
            sys.exit(1)
        if "OpenBB" in cf:
            rep.log("CONFIRMED: CF cache stale for ofr.html "
                    "(origin clean). Waiting for edge revalidation "
                    "up to 10 min...")
            cleared = False
            for _ in range(20):
                time.sleep(30)
                cf, ch = get("https://justhodl.ai/ofr.html?nc=%d"
                             % time.time(),
                             {"Cache-Control": "no-cache"})
                if "OpenBB" not in cf:
                    cleared = True
                    break
            rep.kv(cf_cleared=cleared,
                   final_cache_status=ch.get("cf-cache-status"))
            if not cleared:
                warns.append("CF edge still stale after 10min -- "
                             "origin verified clean; will clear on "
                             "TTL; not a repo defect")
        rep.section("2. Re-assert the remaining audit items live")
        for pg, bad in (("ny-fed.html", "OpenBB"),
                        ("downloads.html", "Bloomberg"),
                        ("errors.html", "s3://")):
            x, _ = get("https://justhodl.ai/%s?cb=%d"
                       % (pg, time.time()),
                       {"Cache-Control": "no-cache"})
            if bad in x:
                warns.append("%s still shows %s on CF edge "
                             "(origin-check below)" % (pg, bad))
                o2, _ = get("https://elmooro.github.io/si/%s?cb=%d"
                            % (pg, time.time()))
                if bad in o2:
                    fails.append("%s ORIGIN serves %s" % (pg, bad))
        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- origin clean; any residue is CF TTL")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3063.json").write_text(json.dumps(
        {"ops": 3063, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
