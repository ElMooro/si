"""ops_4054 — SOURCE DELTA: which agencies does TradingView cite that the
system doesn't have? (Khalid: "add whatever data sources we don't have.")
Audit-first: if the harvest upload hasn't landed, say so precisely; if it
has, compute the full delta here — every distinct TV source, matched
against census families + gov-registry agencies + vault adapters, NEW ones
ranked by how many of HIS indicators depend on them."""
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

KNOWN = {
    "FRED": ("federal reserve", "fred", "st. louis"),
    "US-TREASURY": ("u.s. department of the treasury", "treasury"),
    "BLS": ("bureau of labor",),
    "BEA": ("bureau of economic analysis",),
    "CENSUS-US": ("u.s. census bureau", "united states census"),
    "ECB": ("european central bank",),
    "EUROSTAT": ("eurostat",),
    "BOJ": ("bank of japan",),
    "MOF-JAPAN": ("ministry of finance (japan", "japan ministry of finance",
                  "ministry of finance, japan"),
    "ESTAT-JAPAN": ("statistics bureau of japan", "e-stat"),
    "BOE": ("bank of england",),
    "SNB": ("swiss national bank",),
    "NORGES": ("norges bank",),
    "BCRP-PERU": ("banco central de reserva",),
    "BCB-BRAZIL": ("banco central do brasil", "central bank of brazil"),
    "PBOC": ("people's bank of china",),
    "MOEA-TAIWAN": ("ministry of economic affairs",),
    "CFTC": ("commodity futures",),
    "SEC-EDGAR": ("securities and exchange",),
    "OFR": ("office of financial research",),
    "IMF": ("international monetary fund",),
    "HKMA": ("hong kong monetary",),
    "OECD": ("oecd",),
    "WORLD-BANK": ("world bank",),
    "COINMETRICS": ("coin metrics", "coinmetrics"),
    "COINGECKO": ("coingecko",),
    "MARKET-VENUES": ("nasdaq", "nyse", "cboe", "cme", "ice ", "eurex",
                      "tradingview", "arca", "amex", "otc", "lse ", "tsx",
                      "borsa", "euronext", "xetra", "b3 ", "bmv", "hkex",
                      "krx", "twse", "sse", "szse", "asx", "moex", "bist",
                      "forex", "fx ", "binance", "coinbase", "kraken",
                      "bitstamp", "bybit", "okx", "bitfinex"),
}


def fam_of(src):
    t = src.lower()
    for fam, keys in KNOWN.items():
        if any(k in t for k in keys):
            return fam
    return None


def main():
    with report("4054_birth_watch") as rep:
        rep.heading("ops 4043 — TV source delta vs the system")
        import time as _t
        sr = None
        for _i in range(26):
            try:
                sr = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="data/tv-sources.json")["Body"].read())
                rep.ok(f"  sources BORN after ~{_i*30}s of watching")
                break
            except Exception:
                if _i % 4 == 0:
                    rep.log(f"  [{_i}] waiting for the first sync…")
                _t.sleep(30)
        if not sr:
            rep.fail("SOURCES NOT LANDED within 13 min — retrigger later")
            sys.exit(1)
        m = sr.get("sources") or {}
        diag = sr.get("last_harvest_diag") or {}
        rep.kv(n_symbols_with_source=len(m),
               generated_at=sr.get("generated_at"))
        rep.section("DIAG — the telemetry that ends screenshot debugging")
        rep.kv(diag=json.dumps(diag)[:300])
        if not m and diag:
            rep.ok(f"FINGERPRINT CAPTURED — zero sources but diag names it: "
                   f"first_err={diag.get('first_err')!r} "
                   f"ss={diag.get('ss_ok')}/{diag.get('ss_err')} "
                   f"html={diag.get('html_ok')}/{diag.get('html_err')} "
                   f"matched={diag.get('matched')}")
            return

        by_src = Counter()
        examples = defaultdict(list)
        for sym, v in m.items():
            src = (v.get("source") or "").strip()
            if not src:
                continue
            by_src[src] += 1
            if len(examples[src]) < 4:
                examples[src].append(sym)
        rep.kv(distinct_sources=len(by_src))

        rep.section("A. KNOWN — already in the system")
        known_ct = Counter()
        new_rows = []
        for src, n in by_src.most_common():
            fam = fam_of(src)
            if fam:
                known_ct[fam] += n
            else:
                new_rows.append((src, n, examples[src]))
        for fam, n in known_ct.most_common(20):
            rep.log(f"  {n:5d}  {fam}")

        rep.section("B. NEW — sources the system does NOT have (ranked by "
                    "how many of Khalid's indicators depend on them)")
        for src, n, ex in new_rows[:45]:
            rep.log(f"  {n:5d}  {src[:70]}")
            rep.log(f"          e.g. {', '.join(ex)}")
        rep.kv(n_new_sources=len(new_rows),
               symbols_on_new=sum(n for _, n, _ in new_rows))
        out = {"generated_at": datetime.now(timezone.utc).isoformat(),
               "marker": "source-map v1.0 ops4044",
               "n_symbols_with_source": len(m),
               "distinct_sources": len(by_src),
               "known_families": dict(known_ct),
               "new_sources": [{"source": s2, "n_symbols": n,
                                "examples": ex}
                               for s2, n, ex in new_rows]}
        s3.put_object(Bucket=BUCKET, Key="data/source-map.json",
                      Body=json.dumps(out), ContentType="application/json",
                      CacheControl="max-age=300")
        rep.ok(f"DELTA DONE + data/source-map.json written — "
               f"{len(new_rows)} new sources across "
               f"{sum(n for _, n, _ in new_rows)} of his indicators")


if __name__ == "__main__":
    main()
