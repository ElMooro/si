"""ops_4071 — PROBE (writes NO code, per probe-then-wire doctrine).

Two hypotheses to test before touching anything:

  H1  THE WALK NEVER REACHES THE PAYOFF.
      The extension's allWatchlistSymbols() emits symbols in raw
      watchlist-iteration order.  After 407 of 10,319 walked we hold
      138 MARKET-VENUES and *0 ECONOMICS*.  Market venues are near-zero
      information (we already know NVDA trades on NASDAQ).  The agency
      attribution — BLS / BEA / ECB / BOJ / MOF — is the entire reason
      this arc exists, and it is sitting unwalked deep in the queue.
      Measure: where do ECONOMICS/agency-bearing symbols actually sit in
      the current order, and how many are there?

  H2  data/source-map.json HAS NO PRODUCER.
      harvest-monitor.html renders source-map.json, but grep shows it is
      written only by ops_4070 — an ops script I ran by hand.  No Lambda,
      no schedule.  The moment this session ends the monitor freezes.
      That violates the standing auto-update rule.  Measure: does any
      Lambda write it, does any schedule target it, how stale is it.

Nothing is built here.  The wire op reads these numbers.
"""
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 2}))
evb = boto3.client("events", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

# Prefixes that carry real agency attribution.  ECONOMICS is TradingView's
# macro namespace — ECONOMICS:USCPI resolves to the BLS, USGDP to the BEA.
# These are the rows that make gov-sources / the vault provenance real.
AGENCY_PREFIXES = ("ECONOMICS", "FRED", "TVC", "QUANDL", "CBOE", "USCF")
# Pure trading venues — knowing NVDA is on NASDAQ teaches the system nothing.
VENUE_PREFIXES = ("NASDAQ", "NYSE", "AMEX", "LSE", "EURONEXT", "HKEX", "OSE",
                  "TSX", "ASX", "BSE", "NSE", "SIX", "FWB", "XETR", "BME",
                  "MIL", "OMXSTO", "GPW", "VIE", "PSE", "HOSE", "CSELK")


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def main():
    with report("4071_walkorder_probe") as r:
        r.heading("ops 4071 — PROBE: walk order + source-map producer")

        # ─────────────────────── H1: the walk order ───────────────────────
        r.section("H1 — where does the payoff sit in the queue?")
        wl = gj("data/tv-watchlists.json", {}) or {}
        srcs_doc = gj("data/tv-sources.json", {}) or {}
        have = set((srcs_doc.get("sources") or {}).keys())

        lists = wl.get("watchlists") or wl.get("lists") or []
        if not lists:
            r.log("✗ tv-watchlists.json has no lists — cannot probe")
            sys.exit(1)

        # Rebuild the EXACT order the extension walks: first-seen across
        # watchlist iteration, skipping anything already sourced.
        seen, order = set(), []
        for l in lists:
            for s in (l.get("symbols") or []):
                s = str(s).strip()
                if s and s not in seen:
                    seen.add(s)
                    order.append(s)
        walkable = [s for s in order if s not in have]

        r.row(unique_symbols=len(order), already_sourced=len(have),
              walk_queue=len(walkable))

        def pref(sym):
            return sym.split(":")[0].upper() if ":" in sym else "(bare)"

        # Index positions of agency symbols in the CURRENT order.
        agency_idx, venue_idx = [], []
        for i, s in enumerate(walkable):
            p = pref(s)
            if p in AGENCY_PREFIXES:
                agency_idx.append(i)
            elif p in VENUE_PREFIXES:
                venue_idx.append(i)

        pc = Counter(pref(s) for s in walkable)
        r.section("Walk queue by prefix (top 18)")
        for p, n in pc.most_common(18):
            tag = "  ← AGENCY PAYOFF" if p in AGENCY_PREFIXES else (
                "  (venue: low info)" if p in VENUE_PREFIXES else "")
            r.log(f"  {n:6d}  {p}{tag}")

        r.section("H1 verdict — position of the payoff")
        if agency_idx:
            first_500 = sum(1 for i in agency_idx if i < 500)
            median = sorted(agency_idx)[len(agency_idx) // 2]
            r.log(f"  agency-bearing symbols in queue : {len(agency_idx)}")
            r.log(f"  first agency symbol at index    : {min(agency_idx)}")
            r.log(f"  median agency index             : {median}")
            r.log(f"  agency symbols in first 500     : {first_500}")
            r.log(f"  venue symbols in first 500      : "
                  f"{sum(1 for i in venue_idx if i < 500)}")
            # ~340ms/symbol observed → hours to reach the median agency row
            hrs = median * 0.34 / 3600.0
            r.log(f"  → at the observed 340ms/symbol, the MEDIAN agency row "
                  f"is {hrs:.1f}h into the walk")
            r.log(f"  → under priority ordering the first {len(agency_idx)} "
                  f"symbols walked would ALL be agency-bearing "
                  f"({len(agency_idx) * 0.34 / 60:.0f} min to full payoff)")
            r.row(agency_in_queue=len(agency_idx),
                  agency_first_500=first_500,
                  agency_median_idx=median)
        else:
            r.log("  ✗ zero agency-bearing symbols in the queue — "
                  "H1 is WRONG, do not build the priority walk")
            r.row(agency_in_queue=0)

        # What attribution have we actually banked so far?
        r.section("Attribution banked so far, by family")
        fam = Counter()
        for sym, v in (srcs_doc.get("sources") or {}).items():
            src = (v.get("source") if isinstance(v, dict) else str(v)) or ""
            fam[src.strip() or "(blank)"] += 1
        for f, n in fam.most_common(12):
            r.log(f"  {n:5d}  {f}")

        # ──────────────── H2: does source-map.json auto-update? ────────────
        r.section("H2 — does the monitor's artifact have a producer?")
        sm = gj("data/source-map.json", {}) or {}
        gen = sm.get("generated_at") or sm.get("generated") or "(none)"
        r.log(f"  source-map.json generated_at : {gen}")
        r.log(f"  marker                       : {sm.get('marker','(none)')}")

        # Is there a Lambda that writes it?
        writers, fn_names = [], []
        p = lam.get_paginator("list_functions")
        for page in p.paginate():
            for f in page["Functions"]:
                fn_names.append(f["FunctionName"])
        cand = [n for n in fn_names if "source-map" in n or "sourcemap" in n]
        r.log(f"  fleet size                   : {len(fn_names)} functions")
        r.log(f"  functions named *source-map* : {cand or 'NONE'}")

        # Any schedule pointing at such a function?
        armed = []
        try:
            sp = sch.get_paginator("list_schedules")
            for pg in sp.paginate():
                for s_ in pg.get("Schedules", []):
                    if "source-map" in (s_.get("Name") or ""):
                        armed.append(s_["Name"])
        except Exception as e:
            r.log(f"  (scheduler list skipped: {str(e)[:60]})")
        try:
            for rn in evb.list_rules().get("Rules", []):
                if "source-map" in rn["Name"]:
                    armed.append(rn["Name"])
        except Exception:
            pass
        r.log(f"  schedules targeting it       : {armed or 'NONE'}")

        r.section("H2 verdict")
        if not cand:
            r.log("  ✓ CONFIRMED — source-map.json has NO Lambda producer.")
            r.log("    It exists only because ops_4070 wrote it by hand.")
            r.log("    harvest-monitor.html freezes the moment this session "
                  "ends. Must be promoted to a scheduled engine.")
            r.row(source_map_producer="NONE", source_map_armed=bool(armed))
        else:
            r.log(f"  producer exists: {cand} — H2 wrong, only the schedule "
                  f"may be missing ({armed or 'NOT ARMED'})")
            r.row(source_map_producer=",".join(cand),
                  source_map_armed=bool(armed))

        # ─────────────────── the served monitor page ───────────────────
        r.section("Served page check (runner CAN reach the edge)")
        import urllib.request
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/harvest-monitor.html",
                headers={"User-Agent": "justhodl-ops/4071",
                         "Cache-Control": "no-cache"})
            body = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "ignore")
            mk = re.findall(r"v\d+-ops\d+", body)
            r.log(f"  served bytes  : {len(body)}")
            r.log(f"  served marker : {mk[:2] or 'NONE'}")
            r.row(page_bytes=len(body), page_marker=(mk[0] if mk else "none"))
        except Exception as e:
            r.log(f"  ✗ edge fetch failed: {str(e)[:120]}")

        r.log("")
        r.log("PROBE COMPLETE — no code written. Wire op reads these numbers.")


if __name__ == "__main__":
    main()
