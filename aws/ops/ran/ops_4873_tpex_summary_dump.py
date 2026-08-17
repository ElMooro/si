"""ops/4873 -- tpex_3insti_summary FULL dump (report-only).
The only real JSON among seven candidates (probe 4870, 1014
bytes).  Dump it VERBATIM, identify: date field (ROC-year?),
the foreign-investor row/field, and the denomination (NT$ vs
NT$-thousands vs shares).  The hot-money wire binds only what
prints here.  Hard-fail if the payload is not a value table.
"""
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

URL = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_summary"
UA = {"User-Agent": "Mozilla/5.0 justhodl-ops-4873",
      "Accept": "application/json"}


def main():
    with report("ops 4873 -- tpex summary dump") as rep:
        try:
            req = urllib.request.Request(URL, headers=UA)
            with urllib.request.urlopen(req, timeout=50) as r:
                raw = r.read()
            j = json.loads(raw)
        except Exception as e:  # noqa: BLE001
            rep.fail("fetch died: %s" % str(e)[:100])
            sys.exit(1)
        rep.ok("HTTP ok bytes=%d rows=%d"
               % (len(raw), len(j) if isinstance(j, list)
                  else -1))
        rep.log("VERBATIM:")
        rep.log(json.dumps(j, ensure_ascii=False, indent=1)
                [:2400])
        rows = j if isinstance(j, list) else []
        fkeys = sorted({k for r in rows for k in r})
        rep.log("all keys: %s" % fkeys)
        foreign_rows = [r for r in rows
                        if any("foreign" in str(v).lower()
                               or "\u5916\u8cc7" in str(v)
                               for v in r.values())]
        rep.log("foreign-tagged rows: %d" % len(foreign_rows))
        for r in foreign_rows[:3]:
            rep.log("  %s" % json.dumps(r,
                                        ensure_ascii=False)[:300])
        looks_value = any(
            "amount" in k.lower() or "\u91d1\u984d" in k
            or "value" in k.lower()
            for k in fkeys) or any(
            abs(float(str(v).replace(",", ""))) > 1e8
            for r in foreign_rows for v in r.values()
            if str(v).replace(",", "").replace("-", "")
            .replace(".", "").isdigit())
        if not foreign_rows or not looks_value:
            rep.fail("not a value table (foreign=%d value=%s)"
                     % (len(foreign_rows), looks_value))
            sys.exit(1)
        rep.ok("value table confirmed -- wire binds the fields "
               "above")


if __name__ == "__main__":
    main()
