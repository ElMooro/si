"""ops_4072 — SHIP extension v1.7.8: the PRIORITY WALK.

ops 4071 proved the walk order was burning the arc's whole value:

    first 500 symbols walked → 278 venues, 117 agency
    4,598 agency rows at a MEDIAN index of 4,985
    407 symbols walked → 0 agency rows, 100% venue junk banked

v1.7.8 reorders allWatchlistSymbols() into tiers (agency → provider →
venue), demotes the 100%-failing symbol_search route to a 1-in-200
canary, tightens the step to 240ms (2 reqs/symbol now instead of 3, so
the real request RATE falls), and adds wall-clock rate telemetry because
observed throughput (~6/min) was nowhere near the 340ms timer and we
should measure that rather than guess at it.

GATE DISCIPLINE: this op FORECASTS the new order against the live
watchlist artifact BEFORE it ships anything.  If reordering does not
actually pull agency rows to the front, the zip is never written.  A
reorder that silently no-ops is exactly the bug class that has cost this
fleet ops before.
"""
import io
import json
import sys
import urllib.request
import zipfile as zf
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ZIP_KEY = "tools/jh-tv-extension.zip"

# MUST mirror content.js v1.7.8 exactly, or the forecast is a lie.
TIER1 = ["ECONOMICS", "FRED", "TVC", "COT", "COT3", "CBOE",
         "QUANDL", "USCF", "USI", "EIA", "BLS", "BEA"]
TIER2 = ["GLASSNODE", "INTOTHEBLOCK", "CRYPTOCAP", "FX_IDC",
         "INDEX", "DJ", "SPCFD"]


def tier_of(s):
    p = s.split(":")[0].upper() if ":" in s else ""
    if p in TIER1:
        return 0
    if p in TIER2:
        return 1
    return 2


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def main():
    with report("4072_ship_v178_priority") as rep:
        rep.heading("ops 4072 — v1.7.8 PRIORITY WALK (forecast-gated)")
        checks = []

        # ═══════════ A. FORECAST GATE — no ship unless it moves ═══════════
        rep.section("A. FORECAST the new order on live data (pre-ship gate)")
        wl = gj("data/tv-watchlists.json", {}) or {}
        srcs = gj("data/tv-sources.json", {}) or {}
        have = set((srcs.get("sources") or {}).keys())
        lists = wl.get("watchlists") or wl.get("lists") or []
        if not lists:
            rep.log("✗ no watchlists — cannot forecast, refusing to ship")
            sys.exit(1)

        seen, old_order = set(), []
        for l in lists:
            for x in (l.get("symbols") or []):
                x = str(x).strip()
                if x and x not in seen and x not in have:
                    seen.add(x)
                    old_order.append(x)

        buckets = [[], [], []]
        for x in old_order:
            buckets[tier_of(x)].append(x)
        new_order = buckets[0] + buckets[1] + buckets[2]

        assert len(new_order) == len(old_order), "reorder LOST symbols"
        assert set(new_order) == set(old_order), "reorder CHANGED the set"
        checks.append(("reorder is a pure permutation — nothing dropped", True))

        def agency_in(seq, n):
            return sum(1 for x in seq[:n] if tier_of(x) == 0)

        old500, new500 = agency_in(old_order, 500), agency_in(new_order, 500)
        old2k, new2k = agency_in(old_order, 2000), agency_in(new_order, 2000)
        t1 = len(buckets[0])

        rep.kv(queue=len(old_order), tier1_total=t1,
               agency_first500_old=old500, agency_first500_new=new500,
               agency_first2000_old=old2k, agency_first2000_new=new2k)
        rep.log(f"  first 500  : {old500} agency  →  {new500} agency")
        rep.log(f"  first 2000 : {old2k} agency  →  {new2k} agency")
        rep.log(f"  tier1 total: {t1}   tier2: {len(buckets[1])}   "
                f"venue/other: {len(buckets[2])}")

        # The gate. A no-op reorder must not reach the zip.
        checks.append(("first-500 agency yield strictly improves",
                       new500 > old500))
        checks.append(("first 500 is now ~pure agency (>=450/500)",
                       new500 >= min(450, t1)))
        checks.append(("payoff no longer buried past index 5000",
                       new2k >= min(2000, t1)))
        if not (new500 > old500 and new500 >= min(450, t1)):
            rep.log("✗ FORECAST FAILED — reorder does not move the payoff "
                    "forward. Refusing to ship the zip.")
            for n, ok in checks:
                rep.log(f"  {'✓' if ok else '✗'} {n}")
            sys.exit(1)
        rep.log("  ✓ forecast passes — proceeding to ship")

        rep.section("What the first 300 walked will now be (top prefixes)")
        for p, n in Counter(x.split(":")[0] for x in new_order[:300]
                            if ":" in x).most_common(8):
            rep.log(f"  {n:5d}  {p}")

        # ═══════════ B. rebuild + upload the zip Khalid installs ═══════════
        rep.section("B. rebuild + upload the extension zip")
        try:
            old = s3.get_object(Bucket=BUCKET, Key=ZIP_KEY)["Body"].read()
            names = zf.ZipFile(io.BytesIO(old)).namelist()[:4]
            rooted = not any(n.startswith("chrome-extension/") for n in names)
            rep.kv(old_bytes=len(old), files_at_root=rooted)
        except Exception:
            rooted = True
            rep.log("  no existing zip readable — defaulting to files-at-root")

        buf = io.BytesIO()
        src = REPO / "chrome-extension"
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            for f in sorted(src.rglob("*")):
                if f.is_file():
                    z.write(f, str(f.relative_to(src if rooted else REPO)))
        data = buf.getvalue()
        s3.put_object(Bucket=BUCKET, Key=ZIP_KEY, Body=data,
                      ContentType="application/zip", CacheControl="max-age=300")

        # Read BACK from S3 — never trust the local buffer as proof.
        chk = zf.ZipFile(io.BytesIO(
            s3.get_object(Bucket=BUCKET, Key=ZIP_KEY)["Body"].read()))
        pre = "" if rooted else "chrome-extension/"
        man = json.loads(chk.read(pre + "manifest.json"))
        cjs = chk.read(pre + "content.js").decode()
        rep.kv(new_bytes=len(data), version=man.get("version"))

        checks.append(("zip carries v1.7.8", man.get("version") == "1.7.8"))
        checks.append(("zip content.js carries the PRIORITY WALK",
                       "PRIORITY WALK" in cjs and "b[tierOf(s2)].push" in cjs))
        checks.append(("zip carries the tier lists",
                       "ECONOMICS" in cjs and "TIER1" in cjs))
        checks.append(("symsearch demoted to 1-in-200 canary",
                       "i % 200 === 0" in cjs))
        checks.append(("step tightened to 240ms",
                       "setTimeout(step, 240)" in cjs))
        checks.append(("rate telemetry present",
                       "rate_per_min" in cjs and "tier1_done" in cjs))
        # The harvester itself must still be intact — a reorder must not
        # have cost us the thing doing the walking.
        checks.append(("scanner route still intact",
                       "scanner.tradingview.com/symbol" in cjs
                       and "sc2_ok" in cjs))
        checks.append(("autonomy preserved (auto-start + auto-sync)",
                       "autoStart" in cjs and "autoSync" in cjs))

        # ═══════════ C. edge serves the new zip ═══════════
        rep.section("C. edge download serves the new bytes")
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/" + ZIP_KEY,
                headers={"User-Agent": "justhodl-ops/4072",
                         "Cache-Control": "no-cache"})
            got = urllib.request.urlopen(req, timeout=40).read()
            rep.kv(edge_bytes=len(got))
            gman = json.loads(zf.ZipFile(io.BytesIO(got)).read(
                pre + "manifest.json"))
            rep.log(f"  edge zip version: {gman.get('version')}")
            checks.append(("edge serves v1.7.8",
                           gman.get("version") == "1.7.8"))
        except Exception as e:
            rep.log(f"  edge fetch: {str(e)[:110]} "
                    f"(CDN may still be purging — S3 is authoritative)")
            checks.append(("edge serves v1.7.8", None))

        # ═══════════ verdict ═══════════
        rep.section("VERDICT")
        hard = [(n, o) for n, o in checks if o is not None]
        for n, o in checks:
            rep.log(f"  {'✓' if o else ('~' if o is None else '✗')} {n}")
        bad = [n for n, o in hard if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}")
            sys.exit(1)
        rep.log(f"✅ PASS_ALL — v1.7.8 shipped. First 500 walked goes "
                f"{old500} → {new500} agency rows.")
        rep.log("   Khalid must reload the extension to pick up v1.7.8; the "
                "walk is resume-aware, so the 188 already-sourced symbols "
                "are skipped and the agency tier starts immediately.")


if __name__ == "__main__":
    main()
