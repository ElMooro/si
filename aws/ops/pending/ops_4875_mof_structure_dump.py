"""ops/4875 -- Japan MOF weekly CSV structure dump (report-only).
254KB Shift-JIS file proven alive (4858).  Dump, decoded cp932:
header block lines 1-14 VERBATIM (unit line!), the first 2 data
rows, the LAST 4 rows (latest weeks), total line count, and a
comma-census of one data row so the wire binds exact column
positions.  Hard-fail if decode or shape fails.
"""
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

URL = ("https://www.mof.go.jp/policy/international_policy/"
       "reference/itn_transactions_in_securities/week.csv")


def main():
    with report("ops 4875 -- mof weekly structure dump") as rep:
        try:
            req = urllib.request.Request(
                URL, headers={"User-Agent":
                              "Mozilla/5.0 justhodl-ops-4875"})
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
            txt = raw.decode("cp932", "replace")
        except Exception as e:  # noqa: BLE001
            rep.fail("fetch/decode died: %s" % str(e)[:90])
            sys.exit(1)
        lines = txt.splitlines()
        rep.ok("bytes=%d lines=%d" % (len(raw), len(lines)))
        rep.heading("header block (1-14)")
        for i, ln in enumerate(lines[:14]):
            rep.log("  L%02d| %s" % (i + 1, ln[:170]))
        data = [ln for ln in lines
                if ln.count(",") >= 6
                and any(c.isdigit() for c in ln[:14])]
        rep.heading("first 2 data-ish rows")
        for ln in data[:2]:
            rep.log("  %s" % ln[:220])
        rep.heading("last 4 rows of file")
        for ln in lines[-4:]:
            rep.log("  %s" % ln[:220])
        if data:
            cells = data[-1].split(",")
            rep.heading("comma census of latest data row")
            rep.log("  n_cells=%d" % len(cells))
            for i, c in enumerate(cells[:20]):
                rep.log("  c%02d=%r" % (i, c.strip()[:26]))
        if not data:
            rep.fail("no data rows detected")
            sys.exit(1)
        rep.ok("structure on record -- wire binds these columns")


if __name__ == "__main__":
    main()
