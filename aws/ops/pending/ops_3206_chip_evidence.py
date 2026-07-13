"""ops 3206 — the chip, with EVIDENCE this time. 3205 proved absence but
not the failure mode. This ops extracts the actual __jhRail payload from
the live page into the report (payload missing research vs page missing
rail vs CDN staleness are three different bugs), proves the S3-direct
fallback the bake now uses is reachable from a runner, and passes only
when "research" is inside the live payload."""
import json
import re
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3206)"}


def get(url):
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=15).read().decode(
        "utf-8", "replace")


with report("3206_chip_evidence") as rep:
    fails, warns = [], []
    rep.heading("ops 3206 — rail chip with evidence")

    rep.section("1. Can a runner reach the fusion feed both ways?")
    for u in ("https://justhodl.ai/data/wl-fusion.json",
              "https://s3.amazonaws.com/justhodl-dashboard-live"
              "/data/wl-fusion.json"):
        try:
            d = json.loads(get(u + f"?t={int(time.time())}"))
            rep.log(f"  ✓ {u.split('/')[2]}: {len(d.get('themes') or {})} "
                    "themes")
        except Exception as e:
            rep.log(f"  ✗ {u.split('/')[2]}: {str(e)[:70]}")

    rep.section("2. Live payload evidence (waits out the pages deploy)")
    ok, payload = False, ""
    for i in range(26):
        time.sleep(15)
        try:
            h = get(f"https://justhodl.ai/flows.html?t={int(time.time())}")
        except Exception:
            continue
        m = re.search(r"__jhRail=(\{.*?\});</script>", h)
        payload = m.group(1)[:420] if m else "(no __jhRail in page)"
        if m and '"research"' in m.group(1):
            ok = True
            rep.ok(f"research IN live payload after ~{(i + 1) * 15}s")
            break
    rep.log("  payload: " + payload[:380])
    if not ok:
        fails.append("live payload lacks research — the payload dump above "
                     "names the failure mode")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
