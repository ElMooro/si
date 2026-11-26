"""ops 6981 - read-only: the breadth engine's and the on-chain engine's OWN runs after the second pass, and their pages.

Deployed 2026-11-26 about 17:50 UTC and not run by hand (one run a day each):
  justhodl-breadth-thrust 1.1   own run 22:00 UTC   -> counts on both bases, day-over-day reading, McClellan, preliminary
  justhodl-onchain-ratios       own run 15:31 UTC   -> cycle_bands.history_weekly (the price on its log trend)

For each: was the live document written by the new code on the engine's own schedule, is it sane, and does the page
draw it (real browser, live site)? An engine that has not run yet is reported as pending, not as a failure.
Nothing is written and no engine is invoked. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import importlib.util
import json
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6981_breadth_onchain_own_runs"
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
DEPLOYED = "2026-11-26T17:50"
READ = r"""() => { const h = document.querySelector('[data-glance]'), b = h.querySelector('[data-body]');
  return { st: h.querySelector('[data-st]').textContent, charts: h.querySelectorAll('[data-chart]').length, lines: h.querySelectorAll('path.jgl-ln').length, bands: h.querySelectorAll('path.jgl-ln.band').length,
    tiles: Array.from(h.querySelectorAll('.jgl-tile')).map(t => [t.querySelector('.jgl-k').textContent, t.querySelector('.jgl-v').textContent]), text: b.innerText.replace(/\s+/g, ' ').slice(0, 1500),
    bad: /NaN|undefined|\[object/.test(b.innerText), step: (h.querySelector('.jgl-steps li.on b') || {}).textContent || null }; }"""


def main():
    import boto3
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    s3 = boto3.client("s3", region_name="us-east-1")
    logs = boto3.client("logs", region_name="us-east-1")
    fails, pending, shots = [], [], {}

    def kept(key):
        return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())

    def runs(fn):
        """Start times of the function's runs since the deploy, from its log group (REPORT lines)."""
        try:
            start = int(datetime(2026, 11, 26, 17, 50, tzinfo=timezone.utc).timestamp() * 1000)
            ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=start, filterPattern="REPORT", limit=50).get("events", [])
            return [(datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M"), e["message"].split("Duration: ")[1].split(" ms")[0] if "Duration: " in e["message"] else "?") for e in ev]
        except Exception as exc:  # noqa: BLE001
            return [("log read failed", str(exc)[:80])]

    with report(NAME) as r:
        r.heading("6981 - breadth 1.1 and the on-chain weekly closes: the engines' own runs, and their pages (read-only)")

        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)
            return cond

        r.section("1. Breadth engine")
        b, h = kept("data/breadth-thrust.json"), kept("data/breadth-history.json")
        rows = sorted(h.get("history") or [], key=lambda x: x["date"])
        own = runs("justhodl-breadth-thrust")
        r.log("runs since the deploy (UTC, ms): %s" % own)
        r.log("document: version %s, written %s, state %s | cache %d sessions %s to %s, not sessions %s" % (b.get("version"), b.get("as_of"), b.get("state"), len(rows), rows[0]["date"], rows[-1]["date"], h.get("not_sessions")))
        breadth_live = b.get("version") == "1.1" and str(b.get("as_of", "")) > DEPLOYED
        if not breadth_live:
            pending.append("breadth (its own run is 22:00 UTC)")
            r.log("PENDING  the breadth engine has not run on its own since the deploy")
        else:
            cr = b["current_readings"]
            ck(len(own) <= 2, "it ran on its schedule, not more (%d runs since the deploy)" % len(own))
            ck(110 <= len(rows) <= 130 and all("adv" in x for x in rows) and sum(1 for x in rows if x.get("ratio_dod") is not None) >= len(rows) - 2, "the cache holds %d sessions, each with its counts and day-over-day reading" % len(rows))
            ck(all(datetime.strptime(x["date"], "%Y-%m-%d").weekday() < 5 and x["n_stocks"] >= 1000 for x in rows), "no weekend or stray file is among the sessions (fewest issues %d)" % min(x["n_stocks"] for x in rows))
            ck(cr.get("zweig_sessions_read") == 60 and (cr.get("bank") or {}).get("error") is None, "the state read 60 sessions; the bank was read without error (%s)" % json.dumps(cr.get("bank")))
            ck((b.get("day_over_day") or {}).get("available") and (b.get("mcclellan") or {}).get("available") and "OPEN-TO-CLOSE" in b.get("breadth_basis", ""), "day-over-day reading %s (would read %s), McClellan %s, basis stated" % ((b.get("day_over_day") or {}).get("ema_10d"), (b.get("day_over_day") or {}).get("would_read"), (b.get("mcclellan") or {}).get("oscillator")))
            r.log("the last settled session: " + json.dumps(b.get("breadth_today")))
            r.log("preliminary: " + json.dumps(b.get("preliminary")))
            r.log("state %s, 10-session average %s, window low %s | run took %s s" % (b.get("state"), cr.get("zweig_10d_ema"), cr.get("zweig_window_min"), b.get("run_seconds")))

        r.section("2. On-chain engine")
        o = kept("data/onchain-ratios.json")
        hw = (o.get("cycle_bands") or {}).get("history_weekly") or []
        own = runs("justhodl-onchain-ratios")
        r.log("runs since the deploy (UTC, ms): %s" % own)
        r.log("document: written %s, weekly closes %d%s" % (o.get("generated_at"), len(hw), (" from %s to %s" % (hw[0][0], hw[-1][0])) if hw else ""))
        onchain_live = len(hw) > 0
        if not onchain_live:
            pending.append("on-chain (its own run is 15:31 UTC)")
            r.log("PENDING  the on-chain engine has not run on its own since the deploy")
        else:
            cb = o["cycle_bands"]
            ck(len(own) <= 2, "it ran on its schedule, not more (%d runs since the deploy)" % len(own))
            ck(600 < len(hw) < 1100 and hw[-1][0] == cb["as_of"] and abs(hw[-1][1] - cb["price"]) < 0.02 and all(a[0] < c[0] for a, c in zip(hw, hw[1:])), "%d weekly closes in order, ending on the document's own close (%s, $%s)" % (len(hw), hw[-1][0], format(hw[-1][1], ",.2f")))
            ck("cryptoquant" in o and (o.get("btc") or o.get("cycle_bands")), "the rest of the document is as before (CryptoQuant block present)")

        r.section("3. The two pages, live")
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            page = browser.new_context(viewport={"width": 1400, "height": 1000}, user_agent=UA).new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            for path, live in (("/breadth-thrust.html", breadth_live), ("/btc-cycle.html", onchain_live)):
                try:
                    page.goto("%s%s?nocache=%d" % (BASE, path, int(time.time())), wait_until="domcontentloaded")
                    page.wait_for_selector("[data-glance-on] [data-body] > *", timeout=90000)
                    page.wait_for_timeout(1500)
                    g = page.evaluate(READ)
                    r.log("%s: %s | %d charts, %d lines (%d bands) | tiles %s" % (path, g["st"], g["charts"], g["lines"], g["bands"], json.dumps(g["tiles"][:12], ensure_ascii=False)))
                    ck(not g["bad"], "%s: nothing broken in the picture" % path)
                    if path == "/breadth-thrust.html" and live:
                        names = [t[0] for t in g["tiles"]]
                        ck(g["charts"] == 2 and "Advanced / declined" in names and "McClellan oscillator" in names and "Day-over-day average" in names, "the breadth page draws the sessions chart and the McClellan chart, with the counts on both bases")
                        ck(("%.4f" % b["current_readings"]["zweig_10d_ema"]) in g["text"] and "the same" in g["text"], "the 10-session average on the page is the document's (%.4f), recomputed there to the same figure" % b["current_readings"]["zweig_10d_ema"])
                    if path == "/btc-cycle.html" and live:
                        ck(g["charts"] == 1 and g["bands"] == 7 and g["lines"] == 8, "the crypto page draws the price on its log trend with the seven bands")
                    page.evaluate("() => document.querySelector('[data-glance]').scrollIntoView()")
                    page.wait_for_timeout(300)
                    shots[path.strip("/").split(".")[0]] = page.screenshot(type="jpeg", quality=55, full_page=False)
                except Exception as exc:  # noqa: BLE001
                    ck(False, "%s raised %s: %s" % (path, type(exc).__name__, str(exc)[:200]))
            ck(not errors, "the pages threw no error (%s)" % (errors[:2] or "none"))
            browser.close()
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(failed_checks=len(fails), pending=len(pending))
        if fails:
            r.fail("; ".join(fails)[:700])
            sys.exit(1)
        r.ok("own runs confirmed" if not pending else "nothing wrong; still pending: " + ", ".join(pending))


if __name__ == "__main__":
    main()
