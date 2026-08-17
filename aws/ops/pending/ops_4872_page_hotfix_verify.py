"""ops/4872 -- foreign-flows.html hotfix verify.
Burn: cross-session rebase reordered the shared script -- const
acc used before declaration (TDZ) killed the whole IIFE; JS-style
escapes leaked into raw HTML.  Fix: full script rewrite, helpers
first, every section in try/catch armor, node --check local gate.
 (1) committed: helper-before-use order, 10 S_( sections, zero
     literal escape sequences outside script, pulse container
     bound.
 (2) served: body carries the armored script (S_ marker + acc
     helper BEFORE the IIFE) and no literal escapes.
"""
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PAGE = Path(__file__).resolve().parents[3] / "foreign-flows.html"
URL = "https://justhodl.ai/foreign-flows.html"
FAILED = []


def main():
    with report("ops 4872 -- page hotfix verify") as rep:
        rep.heading("1. committed")
        html = PAGE.read_text(encoding="utf-8")
        pre, rest = html.split("<script>", 1)
        script, post = rest.split("</script>", 1)
        checks = {
            "acc declared before IIFE":
                script.index("const acc=")
                < script.index("(async function()"),
            "10 armored sections": script.count("S_(") >= 10,
            "no literal escapes in HTML":
                "\\u00f7" not in pre + post
                and "\\u2014" not in pre + post,
            "pulse container bound":
                'getElementById("pulse")' in script
                and 'id="pulse"' in pre,
            "releases + absorption + auctions + tables ids":
                all(x in pre for x in ('id="rel"', 'id="abstab"',
                                       'id="auct"', 'id="ctry"',
                                       'id="ctryeq"',
                                       'id="hist"',
                                       'id="splits"'))}
        for name, ok in checks.items():
            (rep.ok if ok else rep.fail)("  " + name)
            if not ok:
                FAILED.append(name)
        if FAILED:
            sys.exit(1)

        rep.heading("2. served (<=8 min)")
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "%s?t=%d" % (URL, int(time.time())),
                    headers={"User-Agent": "ops-4872",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) as r:
                    body = r.read().decode("utf-8", "replace")
                sc = body.split("<script>", 1)[-1]
                ok_srv = ("S_(" in sc and "const acc=" in sc
                          and sc.index("const acc=")
                          < sc.index("(async function()")
                          and "\\u00f7" not in
                          body.split("<script>")[0])
                if ok_srv:
                    rep.ok("armored script SERVED (%ds)"
                           % int(time.time() - t0))
                    break
                rep.log("  settling...")
            except Exception as e:  # noqa: BLE001
                rep.log("  fetch: %s" % str(e)[:60])
            time.sleep(30)
        else:
            rep.fail("not served in 8 min")
            sys.exit(1)

        rep.heading("3. verdict")
        rep.ok("desk un-blanked: helpers-first order restored, "
               "sections independently armored")


if __name__ == "__main__":
    main()
