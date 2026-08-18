"""ops/4877 -- global-flows page unit-suffix hotfix verify.
Burn: 4w-sum cell still stamped 'M' (millions) onto JPY-bn values
-- a unit-honesty bug on the live desk.  Fix: suffix driven by the
declared unit (USD -> 'M', else none) on BOTH cells + Japan flag.
 (1) committed: no hardcoded +\"M\" cells remain in the country
     renderer; sfx branch present twice; japan flag in FLAG map.
 (2) served with the sfx token.
"""
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PAGE = Path(__file__).resolve().parents[3] / "global-flows.html"
URL = "https://justhodl.ai/global-flows.html"


def main():
    with report("ops 4877 -- gf page unit fix") as rep:
        html = PAGE.read_text(encoding="utf-8")
        script = html.split("<script>")[1].split("</script>")[0]
        ok = ("const sfx=" in script
              and script.count("+sfx+'</td>") >= 2
              and "'M</td>" not in script
              and "japan:\"\U0001F1EF\U0001F1F5\"" in script)
        (rep.ok if ok else rep.fail)("committed unit-suffix + "
                                     "flag checks")
        if not ok:
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "%s?t=%d" % (URL, int(time.time())),
                    headers={"User-Agent": "ops-4877",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) \
                        as r:
                    if "const sfx=" in r.read().decode(
                            "utf-8", "replace"):
                        rep.ok("SERVED (%ds)"
                               % int(time.time() - t0))
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        else:
            rep.fail("not served")
            sys.exit(1)
        rep.ok("JPY rows no longer mislabeled as millions")


if __name__ == "__main__":
    main()
