"""ops_4161 — THE DECOMPOSITION: every non-LIVE row, named and counted.
The answer to 'why not 100%' in one table."""
import json
import re
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

US = {"AMEX", "NASDAQ", "NYSE", "OTC", "BATS", "CBOE", "ARCA"}
FUT = {"CME", "CME_MINI", "CBOT", "NYMEX", "COMEX", "ICEUS", "ICEEUR",
       "EUREX", "CBOT_MINI", "COMEX_MINI"}
CRYP = {"BINANCE", "COINBASE", "BITSTAMP", "KRAKEN", "BYBIT", "OKX",
        "BITFINEX", "GEMINI", "CRYPTO", "CRYPTOCAP"}
IDX = {"FTSE", "DJ", "HSI", "USI", "SPCFD", "INDEX", "SP", "SSE",
       "NASDAQ100", "TVC"}
CHAIN = {"GLASSNODE", "INTOTHEBLOCK", "COINMETRICS", "CRYPTOQUANT"}


def cls_of(full):
    if ":" not in full:
        return "bare-registry"
    ex = full.split(":")[0]
    if ex == "ECONOMICS":
        return "economics"
    if ex == "FRED":
        return "fred"
    if ex in US:
        return "us-equity"
    if ex in FUT:
        return "futures"
    if ex in CRYP:
        return "crypto"
    if ex in CHAIN:
        return "onchain-paywalled"
    if ex in IDX:
        return "index-family"
    if ex in ("COT", "COT3"):
        return "cot"
    if ex == "OANDA":
        return "fx"
    return "intl-equity-or-other"


def main():
    with report("4161_decomposition") as rep:
        rep.heading("ops 4161 — the decomposition")
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        rep.kv(generated_at=str(v.get("generated_at"))[:19],
               marker=str(v.get("marker"))[:44])
        idx = {}
        for r in v.get("symbols") or []:
            idx[str(r.get("symbol"))] = r
        wl = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tv-watchlists.json")["Body"].read())
        uni = set()
        for l in (wl.get("lists") or []):
            uni.update(map(str, l.get("symbols") or []))

        live = 0
        buckets = Counter()
        live_b = Counter()
        econ_suffix = Counter()
        for full in uni:
            bare = full.split(":", 1)[1] if ":" in full else full
            r = idx.get(full) or idx.get(bare) or {}
            st = r.get("status")
            c = cls_of(full)
            if st == "LIVE":
                live += 1
                live_b[c] += 1
            else:
                buckets[(c, st or "ABSENT")] += 1
                if c == "economics":
                    m = re.match(r"([A-Z]{2})([A-Z0-9]{1,12})$", bare)
                    if m:
                        econ_suffix[m.group(2)] += 1
        rep.kv(union=len(uni), live_joined=live,
               pct=round(100 * live / len(uni), 1))

        rep.section("LIVE by class")
        for c, n in live_b.most_common():
            rep.log(f"  {n:5d}  {c}")

        rep.section("NON-LIVE buckets (class, status)")
        for (c, st), n in buckets.most_common(24):
            rep.log(f"  {n:5d}  {c:24s} {st}")

        rep.section("residual ECONOMICS family suffixes (top 40)")
        for sfx, n in econ_suffix.most_common(40):
            rep.log(f"  {n:4d}  {sfx}")

        rep.ok(f"DECOMPOSED — {live}/{len(uni)} LIVE; "
               f"top gap classes above")


if __name__ == "__main__":
    main()
