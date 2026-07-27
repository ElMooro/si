"""
ops_3969 — PROBE (read-only): which of the vault's dead symbols can the
gov-sources registry actually resolve?

ops 3967 found 92 classified indicators with no live value, so they cannot
vote in any barometer. Khalid: "add the missing ones to tradingview.html
from gov-sources.html."

Before wiring anything I need the honest addressable set, because a large
share of those 92 are CME/CBOT futures (GE1!, USW1!, I1!, YIT1!, MME1!)
that NO government agency publishes — those are not a gov-sources gap, they
are a genuinely unavailable instrument class, and pretending otherwise
would waste ops the way guessing at BOJ URLs did in the 3936-3955 arc.

This op:
  A. lists every non-LIVE vault symbol with its status, note count and the
     domain the classifier assigned it;
  B. reads the gov-sources registry — each agency's spec, probe result and
     stated universe;
  C. routes each dead symbol to the agency whose remit covers it (by
     country/series family inferred from the symbol itself), and separates
     ADDRESSABLE from STRUCTURALLY-UNAVAILABLE;
  D. ranks the addressable set by n_notes so the wiring order follows how
     much he actually wrote about each one;
  E. re-verifies tradingview.html at the edge for the v3 coverage marker.

Writes nothing. Evidence for the wiring op that follows.
"""
import json
import re
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

# country/family prefix -> the agency in the registry whose remit covers it
ROUTE = [
    (re.compile(r"^JP|^JGB|^TOPIX"), "boj / mof_japan"),
    (re.compile(r"^GB|^UK"), "boe / ons"),
    (re.compile(r"^CH0|^CHIR|^CHF"), "snb"),
    (re.compile(r"^NO"), "norges"),
    (re.compile(r"^CL|^PE"), "bcch / bcrp"),
    (re.compile(r"^EU|^DE|^IT|^ES|^FI|^FR|^AEX|^SX"), "ecb / eurostat"),
    (re.compile(r"^US|^DGS|^DTB|^TB\d"), "us_treasury / fred"),
    (re.compile(r"^CN|^TW|^KR|^HK"), "imf / national — no registry entry"),
]
FUTURES = re.compile(r"!$")


def route_for(sym):
    if FUTURES.search(sym):
        return None
    for rx, agency in ROUTE:
        if rx.search(sym):
            return agency
    return None


def main():
    with report("3969_gov_gap_probe") as rep:
        rep.heading("ops 3969 — which dead vault symbols can gov-sources actually serve?")
        checks = []

        rep.section("A. the dead set")
        vault = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key="data/tradingview.json")["Body"].read())
        bar = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/domain-barometers.json")["Body"].read())
        dom = {x["symbol"]: x for x in bar.get("symbols") or []}
        rows = vault.get("symbols") or []
        dead = [r for r in rows if r.get("status") != "LIVE"]
        rep.kv(vault_symbols=len(rows), live=vault.get("n_live"), dead=len(dead),
               status_counts=json.dumps(vault.get("status_counts")))
        checks.append(("vault + barometers loaded", bool(rows) and bool(dom)))

        rep.section("B. the registry")
        try:
            gov = json.loads(s3.get_object(Bucket=BUCKET,
                                           Key="data/gov-sources.json")["Body"].read())
        except Exception as e:
            rep.fail(f"gov-sources.json unreadable: {e}")
            sys.exit(1)
        ags = gov.get("agencies") or []
        rep.kv(n_agencies=len(ags), generated_at=gov.get("generated_at"))
        for a in ags:
            pr = a.get("probe") or {}
            rep.log(f"  {str(a.get('id')):14s} probe={str(pr.get('status')):8s} "
                    f"vault_join={a.get('vault_symbols_n', a.get('vault_symbols'))} "
                    f"| {str(a.get('universe') or '')[:80]}")

        rep.section("C. route each dead symbol")
        addressable, futures, unrouted = [], [], []
        for r in dead:
            s = r["symbol"]
            rec = {"symbol": s, "status": r.get("status"),
                   "n_notes": r.get("n_notes") or 0,
                   "domain": (dom.get(s) or {}).get("domain"),
                   "category": r.get("category"),
                   "note": str(r.get("resolution_note") or "")[:70]}
            if FUTURES.search(s):
                futures.append(rec)
                continue
            ag = route_for(s)
            if ag:
                rec["agency"] = ag
                addressable.append(rec)
            else:
                unrouted.append(rec)
        rep.kv(addressable=len(addressable), futures_no_gov_source=len(futures),
               unrouted=len(unrouted))

        rep.section("D. ADDRESSABLE — ranked by how much he wrote about them")
        for r in sorted(addressable, key=lambda x: -x["n_notes"])[:40]:
            rep.log(f"  {r['symbol']:12s} notes={r['n_notes']:<4} {str(r['domain']):9s} "
                    f"{r['status']:16s} → {r['agency']}")
        by_ag = Counter(r["agency"] for r in addressable)
        rep.log(f"  by agency: {json.dumps(dict(by_ag))}")
        notes_addressable = sum(r["n_notes"] for r in addressable)
        rep.kv(notes_behind_addressable=notes_addressable)

        rep.section("E. STRUCTURALLY UNAVAILABLE — futures, no agency publishes these")
        for r in sorted(futures, key=lambda x: -x["n_notes"])[:20]:
            rep.log(f"  {r['symbol']:12s} notes={r['n_notes']:<4} {r['status']:16s} "
                    f"{r['note']}")
        rep.kv(notes_behind_futures=sum(r["n_notes"] for r in futures))

        rep.section("F. UNROUTED — no prefix match, needs a manual look")
        for r in sorted(unrouted, key=lambda x: -x["n_notes"])[:25]:
            rep.log(f"  {r['symbol']:12s} notes={r['n_notes']:<4} cat={str(r['category']):10s} "
                    f"{r['status']:16s} {r['note']}")

        rep.section("G. page at the edge (v3 coverage denominator)")
        html, got = "", 0
        MARK = ["v3-ops3969", "function cov(", "classified indicators voted",
                "no direction stated in your notes"]
        for i in range(10):
            try:
                req = urllib.request.Request(
                    f"https://justhodl.ai/tradingview.html?cb={int(time.time())}",
                    headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
                html = urllib.request.urlopen(req, timeout=30).read().decode("utf8", "ignore")
                got = sum(1 for m in MARK if m in html)
                rep.log(f"  [{i}] {len(html)}B {got}/{len(MARK)}")
                if got == len(MARK):
                    break
            except Exception as e:
                rep.log(f"  [{i}] {type(e).__name__}")
            time.sleep(20)
        checks.append(("page v3 live at edge", got == len(MARK)))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PROBE DONE — {len(addressable)} addressable from official APIs "
               f"({notes_addressable} notes behind them), {len(futures)} futures with no "
               f"government source, {len(unrouted)} unrouted")


if __name__ == "__main__":
    main()
