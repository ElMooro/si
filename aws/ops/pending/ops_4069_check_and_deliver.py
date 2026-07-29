"""ops_4069 — CHECK AND DELIVER: diag verdict; if real attribution flows,
purge junk remnant + run the delta + rejoin the page — the payoff op."""
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
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"
JUNK_RX = re.compile(r"^[a-z0-9_]{3,}$")

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
    with report("4069_check_and_deliver") as rep:
        rep.heading("ops 4069 — check and deliver")
        sr = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-sources.json")["Body"].read())
        m = sr.get("sources") or {}
        diag = sr.get("last_harvest_diag") or {}
        rep.kv(store=len(m), generated=sr.get("generated_at"))
        rep.log("  DIAG: " + json.dumps(diag)[:300])
        real = {k: v for k, v in m.items()
                if not JUNK_RX.match(str(v.get("source") or ""))}
        junk = len(m) - len(real)
        rep.kv(real=len(real), junk=junk)
        for k, v in list(real.items())[:12]:
            rep.log(f"    {k}: {str(v.get('source'))[:56]}")

        if len(real) < 100:
            rep.fail(f"only {len(real)} real yet — diag names the wall: "
                     f"{diag.get('first_err')!r} "
                     f"(sc {diag.get('sc_ok')}/{diag.get('sc_err')}, "
                     f"sc2 {diag.get('sc2_ok')}/{diag.get('sc2_err')})")
            sys.exit(1)

        rep.section("DELIVER — purge remnant, write the delta")
        sr["sources"] = real
        sr["n_symbols"] = len(real)
        s3.put_object(Bucket=BUCKET, Key="data/tv-sources.json",
                      Body=json.dumps(sr), ContentType="application/json",
                      CacheControl="max-age=120")
        by, ex = Counter(), defaultdict(list)
        for sym, v in real.items():
            src = str(v.get("source"))
            by[src] += 1
            if len(ex[src]) < 3:
                ex[src].append(sym)
        known_ct, new_rows = Counter(), []
        for src, n in by.most_common():
            fam = fam_of(src)
            (known_ct.__setitem__(fam, known_ct[fam] + n) if fam
             else new_rows.append((src, n, ex[src])))
        rep.section("KNOWN families covered by his indicators")
        for fam, n in known_ct.most_common(20):
            rep.log(f"  {n:5d}  {fam}")
        rep.section("NEW — the sources the system does NOT have")
        for src, n, e2 in new_rows[:45]:
            rep.log(f"  {n:5d}  {src[:66]}   e.g. {', '.join(e2)[:56]}")
        s3.put_object(Bucket=BUCKET, Key="data/source-map.json",
                      Body=json.dumps({
                          "generated_at": datetime.now(timezone.utc).isoformat(),
                          "marker": "source-map v1.2 ops4069",
                          "symbols_with_source": len(real),
                          "distinct_sources": len(by),
                          "known_families": dict(known_ct),
                          "new_sources": [{"source": a, "n_symbols": b,
                                           "examples": c}
                                          for a, b, c in new_rows]}),
                      ContentType="application/json", CacheControl="max-age=120")
        r = lam.invoke(FunctionName="justhodl-tv-workbench",
                       InvocationType="RequestResponse",
                       Payload=b'{"source": "ops4069"}')
        wb = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-workbench.json")["Body"].read())
        t = wb.get("totals") or {}
        rep.kv(fnerr=r.get("FunctionError"), **t)
        rep.ok(f"DELIVERED — {len(real)} real / {len(new_rows)} NEW sources / "
               f"page {t.get('symbols_with_tv_source')} sourced")


if __name__ == "__main__":
    main()
