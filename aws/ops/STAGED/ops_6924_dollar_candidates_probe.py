"""ops 6924 - READ ONLY: which further dollar-shortage series the site's warehouse can serve.

Khalid 2026-11-22 16:20 ET: "... add whatever dollar shortage metrics such as dxy or other".

ops 6923 confirmed 24 Federal Reserve series.  Before the dollar-shortage document is extended, this asks the site's
own series resolver for the few more that would complete it: the rate on excess reserves that preceded IORB (so that
SOFR's premium can be shown through September 2019 and March 2020), and the H.8 "net due to related foreign offices"
series the site's dollar-funding page already names.  One request at a time with a pause.  Nothing is written.
"""
from datetime import datetime, timezone
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops")]
RESOLVER = "https://justhodl-data-proxy.raafouis.workers.dev/series?id="
ASK = ("IOER", "IORB", "SOFR", "SOFR99", "OBFR", "NDFACBW027SBOG", "NDFACBW027NBOG", "NDFFRIW027SBOG", "NDFFRIW027NBOG", "H8B3092NFRD", "H8B3092NFRA", "TEDRATE", "WDFOL")


def summary(obs):
    """(count, first date, last date, last value, largest, smallest) of [[date, value], ...] with unusable rows dropped."""
    rows = [(d, float(v)) for d, v in obs if isinstance(d, str) and isinstance(v, (int, float)) and not isinstance(v, bool)]
    if not rows:
        return None
    hi, lo = max(rows, key=lambda p: p[1]), min(rows, key=lambda p: p[1])
    return len(rows), rows[0][0], rows[-1][0], rows[-1][1], hi, lo


def selftest():
    assert summary([["2020-01-01", 1.0], ["2020-01-02", None], ["2020-01-03", 3.5]]) == (2, "2020-01-01", "2020-01-03", 3.5, ("2020-01-03", 3.5), ("2020-01-01", 1.0))
    assert summary([]) is None


def main():
    selftest()
    from ops_report import report
    with report("ops_6924_dollar_candidates_probe") as r:
        r.heading("ops 6924 - further dollar-shortage series in the warehouse (read only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        held = []
        for i, sid in enumerate(ASK):
            if i:
                time.sleep(1.5)
            try:
                req = urllib.request.Request(RESOLVER + urllib.parse.quote("fred:" + sid, safe=""), headers={"User-Agent": "jh-ops-probe"})
                with urllib.request.urlopen(req, timeout=45) as resp:
                    payload = json.loads(resp.read())
                got = summary(payload.get("obs") or [])
                if not got:
                    r.log("  %-16s no observations | %s" % (sid, str(payload)[:160]))
                    continue
                held.append(sid)
                r.log("  %-16s %5d obs %s .. %s last %s | high %s on %s, low %s on %s | %s" % (
                    sid, got[0], got[1], got[2], got[3], got[4][1], got[4][0], got[5][1], got[5][0], str(payload.get("source"))[:70]))
                rows = [(d, v) for d, v in payload["obs"] if isinstance(v, (int, float))]
                for a, b in (("2008-09-01", "2008-12-31"), ("2019-09-10", "2019-09-25"), ("2020-03-01", "2020-04-30")):
                    inside = [p for p in rows if a <= p[0] <= b]
                    if inside:
                        r.log("      %s .. %s: %d obs, from %s to %s, highest %s on %s" % (a, b, len(inside), inside[0][1], inside[-1][1], max(inside, key=lambda p: p[1])[1], max(inside, key=lambda p: p[1])[0]))
            except Exception as e:  # noqa: BLE001
                r.log("  %-16s %s: %s" % (sid, type(e).__name__, str(e)[:140]))
        r.kv(asked=len(ASK), held=len(held), held_ids=",".join(held))
        r.ok("read only: %d of %d series are served by the warehouse" % (len(held), len(ASK)))


if __name__ == "__main__":
    main()
