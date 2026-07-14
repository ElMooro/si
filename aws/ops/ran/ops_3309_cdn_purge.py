"""ops 3309 — CDN STALENESS: Khalid sees old ofr.html /
primary-dealers.html. Prior ops verified with cache-busting query
params (bypass CF); bare URLs can sit in the edge cache.
[A] Diagnose: fetch both pages PLAIN, capture cf-cache-status / age /
    cache-control + marker booleans.
[B] Purge the zone via the CF API (token now injected into run-ops
    env); log the API verdict verbatim (permission ground truth).
[C] Re-fetch plain until markers present.
Prevention shipped alongside: pages.yml now purges CF after every
deploy (continue-on-error so it can never block)."""
import json
import os
import sys
import time
import urllib.request

from ops_report import report

PAGES = {
    "https://justhodl.ai/ofr.html":
        ("data-tab=\"stfm\"", "stfmFsiChart"),
    "https://justhodl.ai/primary-dealers.html":
        ("jhUsd", "SF.signal"),
}


def fetch(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 justhodl-ops-3309",
            "Cache-Control": "no-transform"})
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode("utf-8", "replace")
            h = {k.lower(): v for k, v in r.headers.items()}
        return body, h
    except Exception as e:
        return "", {"error": str(e)[:120]}


def cf(path, method="GET", data=None):
    tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not tok:
        return None, "no CLOUDFLARE_API_TOKEN in env"
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4" + path,
        data=(json.dumps(data).encode() if data else None),
        method=method,
        headers={"Authorization": "Bearer " + tok,
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read()), None
    except urllib.error.HTTPError as e:
        return None, "%s %s" % (e.code, e.read().decode()[:220])
    except Exception as e:
        return None, str(e)[:200]


with report("3309_cdn_purge") as rep:
    fails, warns = [], []

    rep.section("A. Plain-URL diagnosis (what Khalid sees)")
    stale = []
    for url, marks in PAGES.items():
        body, h = fetch(url)
        ok = all(m in body for m in marks)
        rep.kv(**{url.split("/")[-1].replace(".", "_"): {
            "markers_ok": ok,
            "cf_cache_status": h.get("cf-cache-status"),
            "age": h.get("age"),
            "cache_control": h.get("cache-control"),
            "len": len(body)}})
        if not ok:
            stale.append(url)
    rep.kv(stale_urls=stale)

    rep.section("B. Cloudflare purge")
    zj, err = cf("/zones?name=justhodl.ai")
    if err:
        rep.log("zone lookup: %s" % err)
    zid = ((zj or {}).get("result") or [{}])[0].get("id") if zj else None
    rep.kv(zone_id_found=bool(zid))
    purged = False
    if zid:
        pj, perr = cf("/zones/%s/purge_cache" % zid, "POST",
                      {"purge_everything": True})
        rep.kv(purge_success=(pj or {}).get("success"),
               purge_err=perr,
               purge_errors=(pj or {}).get("errors"))
        purged = bool((pj or {}).get("success"))
    if stale and not purged:
        warns.append("could not purge (token permission?) — pages "
                     "will refresh on origin TTL; grant the CF token "
                     "'Zone > Cache Purge' + 'Zone > Zone:Read'")

    rep.section("C. Re-verify plain URLs")
    if purged:
        time.sleep(8)
    still = []
    for url, marks in PAGES.items():
        ok = False
        for _ in range(10):
            body, h = fetch(url)
            if all(m in body for m in marks):
                ok = True
                break
            time.sleep(20)
        rep.kv(**{"final_" + url.split("/")[-1].replace(".", "_"): {
            "markers_ok": ok,
            "cf_cache_status": h.get("cf-cache-status")}})
        if not ok:
            still.append(url)
    if still:
        fails.append("still stale after purge attempt: %s" % still)

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3309 PASS — bare URLs now serve the latest HTML; "
            "pages.yml self-purges CF on every deploy going forward.")
sys.exit(0)
