"""ops 3369 — drawer cache-pickup fix: gate what CLIENTS actually load.

3368's gate fetched /jh-nav-drawer.js with its own cache-buster and passed —
while every real client kept loading ?v=3335: the committed HTML carried a
frozen literal stamp and the pages.yml stamper only rewrote the UNVERSIONED
src (index-only), so it no-oped for 372 pages. Color tags were live at the
file but unreachable by browsers. Fix in this push: source normalized to
unversioned src (screener/ untouched — protected; the deploy step covers its
artifact), stamper now regex-stamps ALL _site pages and tolerates any
pre-existing ?v=. Gate doctrine upgrade: always gate the URL the page
references, not a synthetic busted one.

Gates (poll ≤300s for pages deploy):
  G1  live why.html + index.html + capital-flow.html reference
      jh-nav-drawer.js?v=<8-hex> and NOT v=3335 / v=3407
  G2  fetching that EXACT stamped URL yields GEN "3368" + tag markers
  G3  stamped hash == md5-8 of jh-nav-drawer.js at executing SHA
"""

import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from ops_report import report

SITE = "https://justhodl.ai"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3369"}
PAGES = ["/why.html", "/index.html", "/capital-flow.html"]


def req(url, timeout=25):
    r = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return -1, str(e)[:200]


def main(rep):
    out = {"gates": {}, "stamps": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:300]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:250]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    want = hashlib.md5(open("jh-nav-drawer.js", "rb").read()).hexdigest()[:8]
    pat = re.compile(r"/jh-nav-drawer\.js\?v=([A-Za-z0-9]+)")

    deadline = time.time() + 300
    stamps = {}
    while time.time() < deadline:
        stamps = {}
        for pg in PAGES:
            st, body = req(SITE + pg)
            m = pat.search(body) if st == 200 else None
            stamps[pg] = m.group(1) if m else f"none(http {st})"
        good = all(re.fullmatch(r"[0-9a-f]{8}", v or "") and v == want
                   for v in stamps.values())
        if good:
            break
        time.sleep(15)
    out["stamps"] = stamps
    frozen = [p for p, v in stamps.items() if v in ("3335", "3407")]
    gate("G1_pages_hash_stamped",
         all(re.fullmatch(r"[0-9a-f]{8}", v or "") for v in stamps.values()) and not frozen,
         f"stamps={stamps}")

    v0 = stamps[PAGES[0]]
    st, body = req(SITE + "/jh-nav-drawer.js?v=" + str(v0))
    markers = ['GEN = "3368"', "jhnav-tagbtn", "jhtag-pop", "jhtag-chip", "jh_tags"]
    missing = [m for m in markers if m not in body] if st == 200 else markers
    gate("G2_client_url_serves_tags", not missing, f"http {st} v={v0} missing={missing}")

    gate("G3_stamp_matches_head", v0 == want, f"live={v0} head_md5={want}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3369.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3369_drawer_cache_pickup") as _rep:
    _rep.heading("ops 3369 — drawer client cache-pickup gates")
    main(_rep)
