"""ops 6933 - chart.html: the studies the live sweeps flagged, swept again with pictures (read-only, re-runnable).

First use (21:22 UTC): the five oscillator windows ops 6932 flagged plus three neighbours.  Its side-by-side
pictures named the cause: an alert toast of the chart's own at the bottom centre ("SSO: big weekly outflow ...")
had landed in the pictures.  The proof no longer draws those toasts in its browser.

Second use: the nine studies on the price chart that ops 6930 flagged for other reasons - the tape family
(Livermore pivots, Accumulation, Distribution, VSA Tape, Tape / Wyckoff, Vol events: shown again, 4 px apart from
the picture before hiding), RS / beta vs SPY (its markers stayed when hidden; repaired in the engine), Earnings and
Opening Range (they add dates right of the last bar).  Pairs of pictures a few pixels apart are located and
embedded enlarged twelve times, so the 4 px can be seen and named.

Khalid 2026-11-23: "my indicators in my chart.html the indicators are good but some of them wont close and remove out
of the chart and visible and invisible doesnt always work".

Each oscillator in the Indicators list is switched on from the list, hidden with its eye, shown again and closed with
its cross, with a real pointer, on https://justhodl.ai/chart.html (NVDA, daily, a fresh browser with auto refresh off).
After each step the chart column is photographed: hidden, the window must be folded to its title row; shown, it must be
the picture it was; closed, the chart must be the chart before, pixel for pixel - the volume profile on the left
included, which used to stay squashed; adding or closing must not move the zoom.  Buyback (a text section with its own
Close) is checked on its own.  Nothing on the site changes.
"""
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6930", str(Path(__file__).resolve().parent / "ops_6930_chart_indicator_controls_live_proof.py"))
P = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(P)


def main():
    from ops_report import report
    with report("ops_6933_chart_indicator_controls_recheck") as r:
        r.heading("ops 6933 - chart.html: the studies the live sweeps flagged, swept again with pictures; real browser on the live site (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        fails = []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.log("FAIL: " + msg)
            return bool(cond)

        r.section("0. The live site serves the new build")
        served = "function legendFirst(){" in P.fetch(P.BASE + "/jh-chart-engine.js?nocache=6933c") and "data-act=eye title=\'Visibility\'" in P.fetch(P.BASE + "/jh-chart-evr.js?nocache=6933")
        r.log("engine and Effort & Spread file are the new build: %s" % served)
        if not served:
            r.fail("the served site is not the new build")
            raise SystemExit(1)
        sync_playwright = P.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = P.launch(p, r.log)
            shots = {}
            rows = P.prove(r, ck, browser, P.BASE, part="sweep", shots=shots, only=["livermore", "accum", "distrib", "vsa", "tape", "volev", "rs", "earn", "or15", "sma20", "vwap"])
            browser.close()
        r.section("pictures of each step that differed (what it should be | what it was), jpeg, base64")
        import base64
        for k, img in shots.items():
            r.log("%s: %d bytes" % (k, len(img)))
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        good = sum(1 for x in rows if not x["verdict"])
        r.kv(studies=len(rows), work_at_every_step=good, failed_checks=len(fails))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("LIVE: %d of %d flagged studies switch on, hide, show and close with the chart returning to the same picture" % (good, len(rows)))


if __name__ == "__main__":
    main()
    sys.exit(0)
