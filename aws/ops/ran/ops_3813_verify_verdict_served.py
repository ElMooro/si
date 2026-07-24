#!/usr/bin/env python3
"""ops 3813 — verify the mispricing verdict is SERVED and self-explaining.

v5.0.1 classifies every scored name but the classification was invisible: the
page rendered none of mispricing_verdict / verdict_confirms / mispriced_book /
value_trap_book. An engine whose judgement no human can see is half-built.

Page v12 adds, above the leaderboard:
  Mispriced book   — gap + structural importance + >=2 confirmations, no disqualifier
  Value Trap book  — wide gap contradicted by the evidence, with the reason per row
plus a Verdict column and filter on the leaderboard, glossary entries for
verdict / industry_regime / gap_days_open, and provenance naming the five feeds.

The Value Trap table matters more than the Mispriced one: those names carry the
WIDEST gaps on the board (CDRO +70.6, NWE +68.5, TGS +64.5) and a pure value
screen would rank them first. Showing why they fail is the point.

Verified from the edge — repo state is not proof of live, and [skip-deploy]
auto-commits have silently withheld page work in this arc before.
"""
import sys, json, time, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)


def fetch(u, a=0):
    x = u + ("&" if "?" in u else "?") + "v=%d%d" % (int(time.time()), a)
    req = urllib.request.Request(x, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def main():
    with report("3813_verify_verdict_served") as rep:
        rep.heading("ops 3813 — verdict visible on the page")

        rep.section("Feed precondition")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        st = cap.get("stats") or {}
        vc = st.get("verdict_counts") or {}
        gate(rep, "FEED.v5", str(d.get("version", "")).startswith("5."), "v%s" % d.get("version"))
        gate(rep, "FEED.classified", sum(v for v in vc.values() if v) > 0,
             "verdict counts: %s" % json.dumps(vc))
        gate(rep, "FEED.books", isinstance(cap.get("mispriced_book"), list)
             and isinstance(cap.get("value_trap_book"), list),
             "mispriced=%d traps=%d" % (len(cap.get("mispriced_book") or []),
                                        len(cap.get("value_trap_book") or [])))

        rep.section("Served page v12")
        M = {"stamp": "v12-ops3813", "vdt_fn": "function vdt(",
             "mispriced_div": 'id="mispriced"', "traps_div": 'id="traps"',
             "verdict_filter": 'id="ldVer"', "verdict_col": "'Verdict'",
             "gloss": "mispricing_verdict:", "book_key": "mispriced_book",
             "trap_key": "value_trap_book", "regime": "industry_regime"}
        body = ""
        for a in range(1, 11):
            try:
                body = fetch("https://justhodl.ai/capture-gap.html", a)
            except Exception as e:
                rep.warn(str(e)[:80]); time.sleep(25); continue
            h = sum(1 for m in M.values() if m in body)
            rep.log("attempt %d: %d bytes · %d/%d" % (a, len(body), h, len(M)))
            if h == len(M):
                break
            time.sleep(25)
        for k, m in M.items():
            gate(rep, f"SERVED.{k}", m in body, "present")

        rep.section("Honesty copy must survive")
        for phrase in ("does <b>not</b> say the market is wrong",
                       "the more useful of",
                       "research shortlist, not a buy list"):
            gate(rep, "COPY." + phrase.split()[0][:14], phrase in body, "present")

        rep.section("Provenance names the new feeds")
        for e in ("estimate-revisions", "dark-pool", "finra-short",
                  "earnings-pead", "industry-boom"):
            gate(rep, "PROV." + e.replace("-", "_"), e in body, "cited")

        rep.section("Additive — v11 surfaces intact")
        for k in ("Most Undervalued", "By Industry", "Full Ledger",
                  "How crucial", "function si(", "Default rank is blended"):
            gate(rep, "KEPT." + k.replace(" ", "_")[:18], k in body, "intact")

        rep.section("What a reader now sees")
        for x in (cap.get("mispriced_book") or [])[:5]:
            rep.log("  MISPRICED  %-6s gap=%+5.1f%% SI=%4.1f :: %s" % (
                x.get("ticker"), x.get("capture_gap") or 0,
                x.get("structural_importance") or 0,
                "; ".join(x.get("verdict_confirms") or [])[:46]))
        for x in (cap.get("value_trap_book") or [])[:5]:
            rep.log("  TRAP       %-6s gap=%+5.1f%% :: %s" % (
                x.get("ticker"), x.get("capture_gap") or 0,
                "; ".join(x.get("verdict_disqualifiers") or [])[:46]))

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — the engine's judgement is now visible and explained")


if __name__ == "__main__":
    main()
