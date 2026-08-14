"""ops 4655 — catalyst.html desk page: class-census bar filters,
macro badges, score leaderboard with evidence+source, why-links,
?-defs, full sorting. Edge-gated (purge if token present).
"""
import json
import os
import sys
import time
import urllib.request

from ops_report import report


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url,
                                 headers={"User-Agent": "ops-4655"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4655_catalyst_page") as r:
        r.heading("ops 4655 — catalyst desk page")

        r.section("cloudflare purge (page + payload)")
        tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
        if not tok:
            r.warn("CLOUDFLARE_API_TOKEN absent — probing "
                   "without purge")
        else:
            try:
                zid = os.environ.get("CLOUDFLARE_ZONE_ID", "")
                body = json.dumps({"files": [
                    "https://justhodl.ai/catalyst.html",
                    "https://justhodl.ai/data/catalyst.json"]}
                ).encode()
                req = urllib.request.Request(
                    "https://api.cloudflare.com/client/v4/"
                    "zones/%s/purge_cache" % zid, data=body,
                    headers={"Authorization": "Bearer " + tok,
                             "Content-Type":
                                 "application/json"},
                    method="POST")
                urllib.request.urlopen(req, timeout=30)
                r.log("purged")
            except Exception as e:
                r.warn("purge: %s" % str(e)[:90])

        r.section("edge verification")
        ok = struct = False
        for att in range(10):
            try:
                pg = http_get("https://justhodl.ai/"
                              "catalyst.html?cb=%d"
                              % time.time()).decode(
                    "utf-8", "replace")
                struct = "data/catalyst.json" in pg \
                    and "REVALUATION" in pg \
                    and "why.html?ticker=" in pg
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/catalyst.json"
                    "?cb=%d" % time.time()))
                ok = struct and (jd.get("n_tickers") or 0) > 0
                if ok:
                    r.kv(n_tickers=jd.get("n_tickers"),
                         classes=len(jd.get("class_census")
                                     or {}),
                         macro=json.dumps(jd.get("macro")
                                          or {})[:100])
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "page_ok", struct,
                           "structural: payload path + doctrine"
                           " + why-links in served HTML")
        misses += contract(r, "payload", ok,
                           "catalyst payload live at edge with "
                           "tickers")

        r.section("verdict")
        if misses:
            r.fail("catalyst page: %d red" % misses)
            sys.exit(1)
        r.ok("CATALYST PAGE LIVE — "
             "https://justhodl.ai/catalyst.html")


if __name__ == "__main__":
    main()
