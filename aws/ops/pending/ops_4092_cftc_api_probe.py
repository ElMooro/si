"""ops_4092 — PROBE (no code): can we actually fetch Khalid's COT tickers?

ops 4091 proved the fleet's cftc-all-cache (29 short-name contracts) has
ZERO overlap with his 340 COT/COT3 tickers, which are 6-digit CFTC market
codes with report suffixes:

    COT3:020601_F_AMP_SPREAD
          ^^^^^^ market code
                 ^ F = futures only, FO = futures + options
                   ^^^ trader class
                       ^^^^^^ position type

So step 3 is a real integration against the CFTC's own publication. Three
things must be true before a single alias is written, and I am measuring
all three rather than reasoning about them:

  H1 REACHABILITY — is publicreporting.cftc.gov servable from our egress?
  H2 THE RIGHT DATASET — which Socrata dataset actually contains market
     code 020601? Legacy, TFF and Disaggregated carry different trader
     taxonomies, and the suffixes above (DP/AMP/LMP/ORP) look like the
     TFF classes (Dealer / Asset Manager / Leveraged Money / Other
     Reportables) rather than Legacy's commercial/non-commercial split.
     Guessing the wrong dataset silently returns the wrong numbers.
  H3 THE COLUMN MAP — do real column names exist that correspond to the
     trader-class + position-type suffixes? If not, the suffix decode is
     my invention and must not ship.
"""
import json, sys, urllib.parse, urllib.request
from collections import Counter
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

# Candidate CFTC Socrata datasets. Named, not guessed at runtime.
DATASETS = {
    "legacy_futures_only":   "6dca-aqww",
    "tff_futures_only":      "gpe5-46if",
    "disaggregated_fut":     "72hh-3qpy",
    "tff_combined":          "yw9f-hn96",
    "legacy_combined":       "jun7-fc8e",
}
BASE = "https://publicreporting.cftc.gov/resource/{}.json"


def get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/4092",
                                               "Accept": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def main():
    with report("4092_cftc_api_probe") as rep:
        rep.heading("ops 4092 — PROBE: CFTC API for the 340 COT tickers")

        rep.section("A. the codes we must serve")
        wl = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-watchlists.json")["Body"].read())
        cot = sorted({str(x).strip().upper()
                      for l in (wl.get("watchlists") or wl.get("lists") or [])
                      for x in (l.get("symbols") or [])
                      if str(x).strip().upper().startswith(("COT:", "COT3:"))})
        codes = sorted({x.split(":", 1)[1].split("_")[0] for x in cot})
        suffix = Counter("_".join(x.split(":", 1)[1].split("_")[1:]) for x in cot)
        rep.log(f"  tickers {len(cot)} · distinct market codes {len(codes)}")
        rep.log(f"  codes sample: {codes[:14]}")
        rep.log("  suffix patterns:")
        for s_, n in suffix.most_common(18):
            rep.log(f"    {n:4d}  {s_}")
        rep.kv(cot_tickers=len(cot), market_codes=len(codes))

        rep.section("B. H1 — reachability + H2 — which dataset has 020601?")
        probe_code = codes[0] if codes else "020601"
        rep.log(f"  probing market code: {probe_code}")
        found = {}
        for name, did in DATASETS.items():
            url = (BASE.format(did) + "?" + urllib.parse.urlencode(
                {"cftc_contract_market_code": probe_code, "$limit": 1}))
            try:
                rows = get(url)
                ok = bool(rows)
                found[name] = (did, ok, rows[0] if rows else None)
                rep.log(f"  {name:22} {did}  rows={len(rows)}  {'✓ HAS IT' if ok else '—'}")
            except Exception as e:
                found[name] = (did, False, None)
                rep.log(f"  {name:22} {did}  ✗ {str(e)[:70]}")

        hit = [(n, v) for n, v in found.items() if v[1]]
        if not hit:
            rep.log("  ✗ no dataset returned this code — do NOT build on a guess")
            rep.kv(dataset_found=False)
            return

        rep.section("C. H3 — do real columns match the suffixes?")
        for name, (did, _, row) in hit:
            cols = sorted(row.keys())
            rep.log(f"  ── {name} ({did}) · {len(cols)} columns")
            rep.log(f"     market: {row.get('market_and_exchange_names')}")
            rep.log(f"     date  : {row.get('report_date_as_yyyy_mm_dd')}")
            for grp, frag in (("dealer", "dealer"), ("asset mgr", "asset_mgr"),
                              ("lev money", "lev_money"), ("other rept", "other_rept"),
                              ("spread", "spread"), ("commercial", "commercial")):
                m = [c for c in cols if frag in c]
                if m:
                    rep.log(f"     {grp:10}: {m[:5]}")
            rep.log(f"     all columns: {cols[:40]}")

        rep.section("D. coverage — how many of his codes does it serve?")
        best = hit[0]
        did = best[1][0]
        served = 0
        for c in codes[:25]:
            try:
                r = get(BASE.format(did) + "?" + urllib.parse.urlencode(
                    {"cftc_contract_market_code": c, "$limit": 1}))
                if r:
                    served += 1
            except Exception:
                pass
        rep.log(f"  sampled 25 codes against {best[0]}: {served} served")
        rep.kv(dataset=best[0], dataset_id=did, sampled=25, served=served)

        rep.section("VERDICT")
        rep.log(f"  dataset to build on: {best[0]} ({did})")
        rep.log("  suffix decode must be derived from the COLUMN NAMES above,")
        rep.log("  not from my assumption about what DP/AMP/LMP mean. If a")
        rep.log("  suffix has no matching column, that ticker stays unrouted.")
        rep.log("  PROBE ONLY — no engine code written.")


if __name__ == "__main__":
    main()
