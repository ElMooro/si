"""ops 6905 - READ-ONLY, real browser on the live site: /primary-dealers.html on the dealer engine's 4.1.0 snapshot.

ops 6903 published the week of 2026-11-11, acquired in five original responses.  The page must load that snapshot
(it checks the retained output's hash itself), show the week, link all five responses, say which response holds a
selected series, and show Treasury repo with the figure the engine published.  Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome"
READ = r"""() => { const text = (id) => { const n = document.getElementById(id); return n ? n.innerText.replace(/\s+/g, ' ').trim() : ''; };
  const links = Array.from(document.querySelectorAll('#detail a')).filter(a => /Original observations/.test(a.textContent)).map(a => [a.textContent.trim(), a.getAttribute('href')]);
  return { asof: text('asof'), detail: text('detail').slice(0, 700), links: links, coverage: text('coverage').slice(0, 300), selection: document.getElementById('selection') ? document.getElementById('selection').value : null }; }"""


def fetch(path, timeout=60):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def number(v):
    """The page's print of an integer (toLocaleString en-US)."""
    return "{:,}".format(int(v))


def selftest():
    assert number(3398380) == "3,398,380" and number(-7404) == "-7,404"


def main():
    selftest()
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    with report("ops_6905_dealer_page_live_proof") as r:
        r.heading("ops 6905 - /primary-dealers.html on the 4.1.0 snapshot, real browser (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        fails = []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("  FAILED: %s" % msg)
            return cond

        served = False
        for attempt in range(30):
            try:
                served = b"Original observations" in fetch("/jh-dealer-research.js?probe=%d" % (stamp + attempt))
            except Exception:  # noqa: BLE001
                served = False
            if served:
                break
            time.sleep(20)
        if not ck(served, "the live site does not serve the new dealer page script"):
            r.fail("nothing was judged")
            sys.exit(1)
        doc = json.loads(fetch("/data/nyfed-primary-dealer.json?probe=%d" % stamp))
        groups, ser = doc.get("groups") or {}, doc.get("native_series") or {}
        repo = groups.get("treasury_repo") or {}
        r.log("document: version %s, week %s, generated %s | Treasury repo %s USD mn (%s) | sources %s" % (
            doc.get("version"), doc.get("as_of"), doc.get("generated_at"), repo.get("usd_mn"), (repo.get("quality") or {}).get("status"), sorted(k for k in doc.get("sources") or {} if k.startswith("observations"))))
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1280, "height": 900}, device_scale_factor=1, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/primary-dealers.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded")
            try:
                page.wait_for_function("() => /Original observations 5 of 5/.test(document.body.innerText)", timeout=90000)
            except Exception as e:  # noqa: BLE001
                r.log("wait: %s | body: %s" % (str(e)[:100], page.evaluate("() => document.body.innerText.replace(/\\s+/g, ' ').slice(0, 300)")))
            d = page.evaluate(READ)
            r.log("as-of line: %s" % d["asof"])
            r.log("default selection %s | links %s" % (d["selection"], json.dumps(d["links"])[:500]))
            ck(doc.get("generated_at") in d["asof"] and len(d["links"]) == 5 and all((h or "").startswith("/data/evidence/fr2004/") for _, h in d["links"])
               and [t for t, _ in d["links"]] == ["Original observations %d of 5" % i for i in range(1, 6)], "the default view: %s | %s" % (d["asof"], d["links"]))
            # Treasury repo: the figure the engine published
            page.select_option("#selection", "group:treasury_repo")
            page.wait_for_timeout(600)
            g = page.evaluate(READ)
            r.log("group:treasury_repo -> %s" % g["detail"][:330])
            ck(number(repo.get("usd_mn")) + " USD millions" in g["detail"] and "report date %s" % repo.get("as_of") in g["detail"] and "Current under the source policy" in g["detail"],
               "Treasury repo on the page: %s" % g["detail"][:200])
            # the side the New York Fed withheld: no figure, and the page says why
            page.select_option("#selection", "group:treasury_reverse_repo")
            page.wait_for_timeout(600)
            w = page.evaluate(READ)
            r.log("group:treasury_reverse_repo -> %s" % w["detail"][:260])
            ck("— USD millions" in w["detail"] and "incomplete" in w["detail"].lower(), "the withheld side: %s" % w["detail"][:200])
            # one series: which response holds it
            key = "PDSORA-UTSETTOT"
            page.select_option("#selection", "series:" + key)
            page.wait_for_timeout(600)
            s = page.evaluate(READ)
            own = (ser.get(key) or {}).get("original_response") or ""
            n = own.split("_")[-1]
            marked = [t for t, _ in s["links"] if "(this series)" in t]
            r.log("series:%s (engine says %s) -> %s | marked link %s" % (key, own, s["detail"][:260], marked))
            ck(marked == ["Original observations %s of 5 (this series)" % n] and "of original response %s, catalog row" % n in s["detail"] and number(ser[key]["reported_value"]) + " USD millions" in s["detail"],
               "the series view: %s | %s" % (marked, s["detail"][:200]))
            raw = fetch(s["links"][int(n) - 1][1])
            ck(len(raw) > 10000 and raw[:2] == b"\x1f\x8b", "the retained original response is not served (%d bytes)" % len(raw))
            r.log("retained original response %s: %d bytes, gzip" % (n, len(raw)))
            ck(not errors, "page errors: %s" % errors[:3])
            ctx.close()
            browser.close()
        r.kv(week=doc.get("as_of"), version=doc.get("version"), original_responses_linked=len(d["links"]), failed_checks=len(fails))
        if fails:
            r.fail("%d checks failed: %s" % (len(fails), " | ".join(fails)[:1500]))
            sys.exit(1)
        r.ok("the dealer page shows the week of %s from five original responses; no page errors" % doc.get("as_of"))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
