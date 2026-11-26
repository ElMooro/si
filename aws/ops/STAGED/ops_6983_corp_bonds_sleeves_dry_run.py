"""ops 6983 - dry run on the runner: the corporate bonds engine's ICE sleeves read from the warehouse. Nothing is written.

Found 2026-11-26 (ops 6982): the 24 ICE BofA sleeves on the Corp bonds desk came from FRED's graph file, which timed
out for every series on every kept run from 2026-11-11, so the sleeves were empty for at least two weeks. The engine
now asks the site's own resolver first (the one that serves the ICE BofA desk), one series at a time, and FRED itself
only for a series the warehouse lacks.

This op runs the engine's own new function from the repository - no Lambda is invoked, no object is written - and
checks that the sleeves come back, are current, and read in the right unit. It also asks FRED's graph file for one
series from the runner, as information on why the old path failed. Safe to run again.
"""
from datetime import date, datetime, timezone
from pathlib import Path
import importlib.util
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
NAME = "ops_6983_corp_bonds_sleeves_dry_run"
SRC = ROOT / "aws" / "lambdas" / "justhodl-corp-bonds" / "source"


def main():
    from ops_report import report
    fails = []
    now = datetime.now(timezone.utc)
    with report(NAME) as r:
        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)

        r.heading("6983 - Corp bonds: the ICE sleeves from the warehouse, dry run on the runner (nothing written), %s" % now.isoformat(timespec="minutes"))
        sys.path.insert(0, str(SRC))
        spec = importlib.util.spec_from_file_location("eng_corp_bonds", str(SRC / "lambda_function.py"))
        L = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(L)
        puts = []

        class NoWrite:
            def __getattr__(self, name):
                def call(**kw):
                    puts.append(name)
                    raise RuntimeError("the dry run touches no object")
                return call
        L.s3 = NoWrite()

        r.section("1. The engine's own function")
        t0 = time.time()
        rows, status, via = L.fetch_sleeves(date.today().isoformat())
        took = time.time() - t0
        from_bank = sum(1 for v in via.values() if v == "warehouse")
        from_fred = sum(1 for v in via.values() if str(v).startswith("FRED"))
        r.log("%d of %d sleeves in %.1f s | %d from the warehouse, %d from FRED | not read: %s" % (
            len(rows), len(L.core.SLEEVES), took, from_bank, from_fred, json.dumps({k: v for k, v in status.items() if v != "live"})))
        for row in rows:
            r.log("  %-20s %-10s %9s %-3s as of %s | 1d %s, 5d %s, 21d %s | %s" % (row["label"], row["fred"][:10], row["level"], row["unit"], row["as_of"], row.get("chg_1d"), row.get("chg_5d"), row.get("chg_21d"), row.get("via")))
        ck(not puts, "nothing was written or read from the bucket by the sleeves (%s)" % (puts or "no call"))
        ck(len(rows) >= 20 and sum(1 for v in status.values() if v == "live") == len(rows), "the sleeves are back: %d of 24, each with the status the page reads as printed" % len(rows))
        ck(took < 120, "inside the 150 seconds set aside (%.1f s), one request at a time" % took)
        by = {x["id"]: x for x in rows}
        ig, hy, ccc = by.get("us_ig"), by.get("us_hy"), by.get("us_ccc")
        ck(bool(ig and hy and ccc) and 30 < ig["level"] < 400 and 150 < hy["level"] < 2000 and ig["level"] < hy["level"] < ccc["level"], "the unit is right: US IG %s bp under US HY %s bp under CCC %s bp" % (ig and ig["level"], hy and hy["level"], ccc and ccc["level"]))
        yl = by.get("us_hy_yield")
        ck(bool(yl) and 3 < yl["level"] < 25 and yl["unit"] == "pct", "a yield sleeve stays in percent (US HY yield %s)" % (yl and yl["level"]))
        old = [x["label"] + " " + x["as_of"] for x in rows if (date.today() - date.fromisoformat(x["as_of"])).days > 7]
        ck(len(old) <= 2, "the sleeves are current: none more than a week old but %s" % (old or "none"))
        r.log("newest day across the sleeves: %s | oldest: %s" % (max(x["as_of"] for x in rows) if rows else None, min(x["as_of"] for x in rows) if rows else None))

        r.section("2. FRED's graph file, asked once from the runner (information)")
        t1 = time.time()
        try:
            req = urllib.request.Request(L.FRED % "BAMLH0A0HYM2", headers={"User-Agent": L.UA, "Accept": "text/csv,text/html,*/*"})
            with urllib.request.urlopen(req, timeout=12) as resp:
                text = resp.read().decode("utf-8", "replace")
            pts = L.core.parse_fred_csv(text)
            r.log("answered in %.1f s: %d observations, %s to %s" % (time.time() - t1, len(pts), pts[0][0] if pts else None, pts[-1] if pts else None))
        except Exception as exc:  # noqa: BLE001
            r.log("did not answer in %.1f s: %s %s" % (time.time() - t1, type(exc).__name__, str(exc)[:100]))
        r.kv(failed_checks=len(fails), sleeves=len(rows), seconds=round(took, 1), from_warehouse=from_bank, from_fred=from_fred)
        if fails:
            r.fail("; ".join(fails)[:600])
            sys.exit(1)
        r.ok("the sleeves read from the warehouse")


if __name__ == "__main__":
    main()
