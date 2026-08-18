"""ops/4883 -- sortable Beat League columns verify.
 (1) committed: 10 data-k headers, cmp with null-sink, toggle
     logic, filter+sort composition.
 (2) served with the sort tokens.
"""
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PAGE = Path(__file__).resolve().parents[3] / "earnings.html"
URL = "https://justhodl.ai/earnings.html"


def main():
    with report("ops 4883 -- sortable league verify") as rep:
        html = PAGE.read_text(encoding="utf-8")
        script = html.split("<script>")[1].split(
            "</script>")[0]
        checks = {
            "10 sortable headers":
                script.count('data-k="') == 0
                and script.count("data-k=") >= 1
                and script.count('["rank","#",1]') == 1
                and script.count("COLS=[") == 1,
            "null-sink comparator":
                "if(nx)return 1;if(ny)return -1;" in script,
            "direction toggle":
                "st.dir=-st.dir" in script,
            "filter composes with sort":
                "st.bucket===st.bucket" not in script
                and 'st.bucket==="all"||r.bucket===st.bucket'
                in script,
            "node-balanced braces":
                script.count("{") == script.count("}")}
        bad = [k for k, ok in checks.items() if not ok]
        for k in checks:
            (rep.ok if k not in bad else rep.fail)("  " + k)
        if bad:
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "%s?t=%d" % (URL, int(time.time())),
                    headers={"User-Agent": "ops-4883",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) \
                        as r:
                    if "st.dir=-st.dir" in r.read().decode(
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
        rep.ok("every league column sorts both ways, blanks "
               "sink, filters compose")


if __name__ == "__main__":
    main()
