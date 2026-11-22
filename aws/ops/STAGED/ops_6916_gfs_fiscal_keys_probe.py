"""ops 6916 - READ-ONLY: which ECB government-finance series carry the deficit and the interest bill, in percent of GDP.

Khalid 2026-11-22 11:12 ET on the Sovereign desk: "if you have other ways to add even more data and improve it further do".
The desk has debt to GDP from the ECB's GFS flow; a government's deficit and what it pays in interest are the two
figures beside it.  One request to the ECB Data Portal for Germany's quarterly general-government series (last
observation only) lists the candidates; then one request per candidate across all countries says how many report.
Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import csv
import io
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
ECB = "https://data-api.ecb.europa.eu/service/data/"
CLIENT = {"User-Agent": "JustHodl-SourceEvidence/1.0", "Accept": "text/csv"}
DIMS = 18          # GFS key: FREQ ADJUSTMENT REF_AREA COUNTERPART_AREA REF_SECTOR COUNTERPART_SECTOR CONSOLIDATION ACCOUNTING_ENTRY STO INSTR_ASSET MATURITY EXPENDITURE UNIT_MEASURE CURRENCY_DENOM VALUATION PRICES TRANSFORMATION CUST_BREAKDOWN
WORDS = ("deficit", "surplus", "net lending", "interest", "primary")


def path(fixed):
    """A GFS path with the given dimensions fixed (1-based position -> code) and the rest open."""
    return "GFS/" + ".".join(fixed.get(i, "") for i in range(1, DIMS + 1))


def ask(p, query, timeout=120):
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(ECB + p + "?format=csvdata" + query, headers=CLIENT), timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, len(raw), time.time() - t0, list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
    except urllib.error.HTTPError as e:
        return e.code, 0, time.time() - t0, []
    except Exception as e:  # noqa: BLE001
        return type(e).__name__, 0, time.time() - t0, []


def selftest():
    assert path({1: "Q", 3: "DE", 5: "S13"}) == "GFS/Q..DE..S13............." and path({}).count(".") == DIMS - 1


def main():
    selftest()
    from ops_report import report
    with report("ops_6916_gfs_fiscal_keys_probe") as r:
        r.heading("ops 6916 - the ECB's deficit and interest series, percent of GDP (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        status, size, secs, rows = ask(path({1: "Q", 3: "DE", 5: "S13"}), "&lastNObservations=1")
        r.log("Germany, quarterly, general government: %s, %d bytes, %.1fs, %d series" % (status, size, secs, len(rows)))
        units = {}
        for x in rows:
            units[x.get("UNIT_MEASURE")] = units.get(x.get("UNIT_MEASURE"), 0) + 1
        r.log("units: %s" % units)
        picked = []
        for x in rows:
            title = (x.get("TITLE") or x.get("TITLE_COMPL") or "")
            if "B1GQ" in (x.get("UNIT_MEASURE") or "") and any(w in title.lower() for w in WORDS):
                picked.append(x)
                r.log("  %s | %s | %s | %s | %s" % (x["KEY"], x.get("TIME_PERIOD"), x.get("OBS_VALUE"), x.get("UNIT_MEASURE"), title[:150]))
        r.log("%d candidates in percent of GDP" % len(picked))
        seen = set()
        for x in picked:
            parts = x["KEY"].split(".")[1:]
            parts[2] = ""                                    # every country
            p = "GFS/" + ".".join(parts)
            if p in seen:
                continue
            seen.add(p)
            time.sleep(2)
            for which in ("lastNObservations", "firstNObservations"):
                s2, size2, secs2, rows2 = ask(p, "&%s=1" % which)
                periods = sorted({y.get("TIME_PERIOD", "") for y in rows2})
                r.log("  ALL %s %s=1 -> %s, %d bytes, %.1fs, %d areas, periods %s .. %s | %s" % (p[:84], which[:5], s2, size2, secs2, len({y.get('REF_AREA') for y in rows2}), periods[0] if periods else "-", periods[-1] if periods else "-",
                                                                                              " ".join("%s %s" % (y.get("REF_AREA"), y.get("OBS_VALUE")) for y in rows2 if y.get("REF_AREA") in ("DE", "FR", "IT", "GR", "I9", "U2"))[:160]))
                time.sleep(1)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
