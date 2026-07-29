"""ops_4091 — PROBE (no code) for STEP 3: the 340 COT/COT3 tickers.

ops 4084 counted 229 COT3 + 111 COT tickers carrying Khalid's notes with
no fetch route. The fleet already ingests CFTC data into
data/cftc-all-cache.json, so this should be a wiring job rather than a
new integration — but only if TradingView's COT symbol codes actually
correspond to the contract keys in that cache.

I do not know that they do. TradingView COT symbols look like
COT:088691_F_ALL_NT (a CFTC contract market code plus report flags),
while the cache may be keyed by short names or by a different code form.
Assuming a join that does not exist is how you ship 340 dead aliases, so
this measures the real overlap first and writes nothing.
"""
import json, sys
from collections import Counter
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

def gj(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception as e:
        print(f"miss {k}: {e}"); return d

def main():
    with report("4091_cot_probe") as rep:
        rep.heading("ops 4091 — PROBE: can COT tickers join the CFTC cache?")

        rep.section("A. the COT tickers we owe a route")
        wl = gj("data/tv-watchlists.json", {}) or {}
        cot = []
        for l in (wl.get("watchlists") or wl.get("lists") or []):
            for x in (l.get("symbols") or []):
                x = str(x).strip().upper()
                if x.startswith(("COT:", "COT3:")):
                    cot.append(x)
        cot = sorted(set(cot))
        rep.log(f"  COT/COT3 tickers: {len(cot)}")
        for x in cot[:20]:
            rep.log(f"    {x}")
        pat = Counter()
        for x in cot:
            body = x.split(":",1)[1]
            pat["digits_"+str(len(body.split('_')[0]))] += 1 if body.split('_')[0].isdigit() else 0
            pat["parts_"+str(len(body.split('_')))] += 1
        rep.log(f"  shape histogram: {dict(pat)}")
        rep.kv(cot_tickers=len(cot))

        rep.section("B. what the CFTC cache is actually keyed by")
        cache = gj("data/cftc-all-cache.json", {}) or {}
        rep.log(f"  top-level keys: {sorted(map(str, cache.keys()))[:16]}")
        # mirror justhodl-cftc-deep-view's shape-aware selection exactly
        contracts = None
        for k in ("contracts", "contracts_data", "by_contract", "data", "series"):
            v = cache.get(k)
            if isinstance(v, dict) and v:
                contracts = v; rep.log(f"  contracts container: '{k}'"); break
        if contracts is None:
            META = {"meta","contract_metadata","metadata","generated_at","engine",
                    "version","methodology","academic_basis","smart_money_map",
                    "duration_seconds","n_contracts","contracts","updated_at","as_of"}
            contracts = {k: v for k, v in cache.items()
                         if isinstance(v, (list, dict)) and str(k) not in META}
            rep.log("  contracts container: top-level fallback")
        ckeys = sorted(map(str, contracts.keys()))
        rep.log(f"  contracts in cache: {len(ckeys)}")
        rep.log(f"  sample keys: {ckeys[:18]}")
        meta = None
        for k in ("contract_metadata","meta","metadata"):
            if isinstance(cache.get(k), dict): meta = cache[k]; break
        if meta:
            rep.log(f"  metadata sample: {list(meta.items())[:3]}")
        rep.kv(cache_contracts=len(ckeys))

        rep.section("C. THE JOIN — does it exist?")
        cset = set(ckeys)
        direct = [x for x in cot if x.split(":",1)[1] in cset]
        # CFTC market codes are the leading digit block of the TV symbol
        codes = {x: x.split(":",1)[1].split("_")[0] for x in cot}
        bycode = [x for x, c in codes.items() if c in cset]
        rep.log(f"  exact body match      : {len(direct)}")
        rep.log(f"  leading-code match    : {len(bycode)}")
        if bycode[:8]:
            rep.log(f"  e.g. {[(x, codes[x]) for x in bycode[:6]]}")
        rep.kv(join_exact=len(direct), join_bycode=len(bycode))

        rep.section("VERDICT")
        if len(direct) + len(bycode) == 0:
            rep.log("  ✗ NO JOIN. The cache is not keyed by anything the TV COT")
            rep.log("    symbols carry. Step 3 is therefore NOT a wiring job —")
            rep.log("    it needs a code mapping (CFTC market code -> cache key)")
            rep.log("    built from the CFTC metadata, or a direct pull from the")
            rep.log("    CFTC's own API. Do NOT emit aliases on this basis.")
        else:
            rep.log(f"  ✓ join viable on "
                    f"{'exact body' if direct else 'leading market code'} — "
                    f"{max(len(direct), len(bycode))} of {len(cot)} tickers")
        rep.log("  PROBE ONLY — no engine code written.")

if __name__ == "__main__":
    main()
