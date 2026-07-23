#!/usr/bin/env python3
"""ops 3781 — capture-gap tile in the why.html Vitals strip.

Completes the wiring set: engine (3765-3771) -> page (3772-3775) -> best-setups
(3779) -> master-ranker (3780) -> per-stock research view (this).

why.html is the per-ticker research page. Its Vitals strip (ops 3299) already
shows P/E, PEG, P/S, P/B, EV/EBITDA, FCF yield, shareholder yield, shares out
and a flashing DILUTION tile — all valuation context. capture_gap belongs
exactly there: it is the one valuation number that asks whether the market pays
this company for how indispensable it is to its industry.

TILES ADDED (only when the ticker is in the capture ledger — 1,771 names, so
many small caps will legitimately show nothing rather than a fake zero):
  CAPTURE GAP   within-industry pp, sub-line = cross-industry pp
  CATCH-UP      % move to the industry MEDIAN multiple, sub-line = basis
                + an explicit "not a target" qualifier in the sub-line, because
                this is the number most likely to be misread as a forecast.

PAGE-PUSH RULE (learned the hard way this arc, cost 6 ops): root *.html edits
must be committed as a NORMAL push from the sandbox. Ops scripts that write
pages land via the runner's `[skip-deploy]` auto-commit, which SUPPRESSES
pages.yml — the file changes in git and the edge keeps serving the old copy.
So this ops does NOT write the page. The page is edited and pushed separately;
this ops only VERIFIES the served result.
"""
import sys, time, json, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
URL = "https://justhodl.ai/why.html"
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def fetch(url, attempt=0):
    u = url + ("&" if "?" in url else "?") + "v=%d%d" % (int(time.time()), attempt)
    req = urllib.request.Request(u, headers={
        "User-Agent": UA, "Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main():
    with report("3781_why_vitals_capture_gap") as rep:
        rep.heading("ops 3781 — verify capture-gap tiles in why.html Vitals")

        rep.section("Feed precondition")
        ck = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = ck.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        by_t = {r.get("ticker"): r for r in rows}
        gate(rep, "FEED.rows", len(rows) > 500, "capture ledger n=%d" % len(rows))
        gate(rep, "FEED.note", bool(cap.get("catchup_note")),
             "not-a-target note present in feed")

        rep.section("Sample tickers a user would actually open")
        for t in ("NVDA", "TSM", "ASML", "AMD", "GILD", "DBX", "MSFT"):
            r = by_t.get(t)
            if r:
                rep.log("  %-6s gap=%+6.1fpp global=%+6.1fpp catchup=%7s%% (%s) tier=%s" % (
                    t, r.get("capture_gap") or 0, r.get("global_capture_gap") or 0,
                    ("%.0f" % r["catchup_pct"]) if r.get("catchup_pct") is not None else "—",
                    r.get("catchup_basis") or "-", r.get("tier")))
            else:
                rep.log("  %-6s not in ledger (honest blank, not a zero)" % t)

        rep.section("Served page — markers unique to this change")
        MARKERS = {
            "fetch_call": "data/chokepoint.json",
            "tile_gap": "CAPTURE GAP",
            "tile_catchup": "CATCH-UP",
            "honesty": "not a target",
            "fn": "jhCaptureTiles",
        }
        body = ""
        for attempt in range(1, 9):
            try:
                st, body = fetch(URL, attempt)
            except Exception as e:
                rep.warn("attempt %d: %s" % (attempt, str(e)[:110]))
                time.sleep(25)
                continue
            hits = sum(1 for m in MARKERS.values() if m in body)
            rep.log("attempt %d: HTTP %s · %d bytes · %d/%d markers" % (
                attempt, st, len(body), hits, len(MARKERS)))
            if hits == len(MARKERS):
                break
            time.sleep(25)

        for name, m in MARKERS.items():
            gate(rep, f"SERVED.{name}", m in body, "present in served why.html")

        rep.section("Additive — existing Vitals tiles must survive")
        for m in ("P/E TTM", "PEG", "P/S", "EV/EBITDA", "SHARES OUT", "DILUTION",
                  "renderJHVitals", "fillJHVitals"):
            gate(rep, f"KEPT.{m.replace('/', '_').replace(' ', '_')}", m in body, "intact")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — capture gap now visible on the per-stock research page")
        rep.log("Wiring set complete: engine -> page -> best-setups -> ranker -> why.html")


if __name__ == "__main__":
    main()
