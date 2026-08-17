"""ops/4870 -- TPEx VALUE-aggregate micro-probe (report-only).
Burn 4869: the per-security Difference columns are SHARE counts;
summing shares as NT$ would be fake data.  Find the daily
three-institutional VALUE aggregate:
 (a) openapi catalog: list endpoint names containing insti/qfii;
 (b) fetch candidates and dump heads;
 (c) the zh aggregate page endpoint (bigd/3itrade values).
Hard-fail only if nothing value-denominated is found (fallback:
wire shares with explicit 'shares' unit).
"""

import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 justhodl-ops-4870",
      "Accept": "application/json,text/*;q=0.8"}


def get(url, timeout=50):
    with urllib.request.urlopen(
            urllib.request.Request(url, headers=UA),
            timeout=timeout) as r:
        return r.status, r.read()


def main():
    hits = [0]
    with report("ops 4870 -- tpex value probe") as rep:
        rep.heading("a. openapi catalog")
        names = []
        for cat in ("https://www.tpex.org.tw/openapi/",
                    "https://www.tpex.org.tw/openapi/v1/"
                    "swagger.json",
                    "https://www.tpex.org.tw/openapi/"
                    "swagger.json"):
            try:
                st, raw = get(cat)
                txt = raw.decode("utf-8", "replace")
                rep.ok("  %s HTTP %d bytes=%d"
                       % (cat.split(".tw")[1], st, len(raw)))
                import re
                names = sorted(set(re.findall(
                    r"/v1/(tpex_[A-Za-z0-9_]*"
                    r"(?:insti|3insti|qfii)[A-Za-z0-9_]*)",
                    txt)))
                if names:
                    rep.log("  insti endpoints: %s" % names[:12])
                    break
            except Exception as e:  # noqa: BLE001
                rep.log("  %s died: %s" % (cat.split(".tw")[1],
                                           str(e)[:50]))
            time.sleep(0.4)

        rep.heading("b. candidates")
        cands = ["tpex_" + x for x in (
            "3insti_trading_vol", "3insti_summary",
            "3insti_trading_summary", "qfii_trading_vol",
            "3insti_daily_summary")]
        for n in dict.fromkeys((names or []) + cands):
            u = "https://www.tpex.org.tw/openapi/v1/" + n
            try:
                st, raw = get(u)
                head = raw[:300].decode("utf-8", "replace")
                rep.ok("  %s HTTP %d bytes=%d head=%s"
                       % (n, st, len(raw), head[:180]))
                hits[0] += 1
            except Exception as e:  # noqa: BLE001
                rep.log("  %s died: %s" % (n, str(e)[:50]))
            time.sleep(0.5)

        rep.heading("c. zh aggregate value endpoint")
        for u in ("https://www.tpex.org.tw/www/zh-tw/insti/"
                  "3insti?type=Daily&response=json",
                  "https://www.tpex.org.tw/web/stock/3insti/"
                  "3insti_summary/3itrade_sum_result.php"
                  "?l=en-us&t=D&o=json"):
            try:
                st, raw = get(u)
                rep.ok("  %s HTTP %d bytes=%d head=%s"
                       % (u.split(".tw")[1][:46], st, len(raw),
                          raw[:220].decode("utf-8",
                                           "replace")[:200]))
                hits[0] += 1
            except Exception as e:  # noqa: BLE001
                rep.log("  %s died: %s"
                        % (u.split(".tw")[1][:40], str(e)[:50]))
            time.sleep(0.5)

        rep.heading("verdict")
        if hits[0] == 0:
            rep.fail("no candidate endpoint answered -- TPEx "
                     "value lane blocked")
            sys.exit(1)
        rep.ok("value-endpoint evidence above (%d answers) -- "
               "wire binds only a NT$-denominated field, else "
               "shares w/ unit" % hits[0])


if __name__ == "__main__":
    main()
