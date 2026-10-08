"""ops_4063 — THE DELTA, on real attribution only; junk quantified honestly;
workbench v1.2 rejoined so the page shows only truth."""
import io
import json
import re
import sys
import time
import urllib.request
import zipfile as zf
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
WMARK = "tv-workbench v1.2 ops4063 junk-filter"

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
    with report("4063_real_delta") as rep:
        rep.heading("ops 4063 — the delta on REAL attribution")
        sr = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-sources.json")["Body"].read())
        m = sr.get("sources") or {}
        real, junk = {}, 0
        for k, v in m.items():
            src = (v.get("source") or "").strip()
            if not src:
                continue
            if JUNK_RX.match(src):
                junk += 1
            else:
                real[k] = src
        rep.kv(total=len(m), real=len(real), junk_filtered=junk)

        by_src, examples = Counter(), defaultdict(list)
        for sym, src in real.items():
            by_src[src] += 1
            if len(examples[src]) < 3:
                examples[src].append(sym)
        known_ct, new_rows = Counter(), []
        for src, n in by_src.most_common():
            fam = fam_of(src)
            (known_ct.__setitem__(fam, known_ct[fam] + n) if fam
             else new_rows.append((src, n, examples[src])))
        rep.section("KNOWN families covered")
        for fam, n in known_ct.most_common(18):
            rep.log(f"  {n:5d}  {fam}")
        rep.section("NEW — sources the system does NOT have")
        for src, n, ex in new_rows[:45]:
            rep.log(f"  {n:5d}  {src[:66]}   e.g. {', '.join(ex)[:56]}")
        s3.put_object(Bucket=BUCKET, Key="data/source-map.json",
                      Body=json.dumps({
                          "generated_at": datetime.now(timezone.utc).isoformat(),
                          "marker": "source-map v1.1 ops4063 real-only",
                          "symbols_with_source": len(real),
                          "junk_filtered": junk,
                          "distinct_sources": len(by_src),
                          "known_families": dict(known_ct),
                          "new_sources": [{"source": a, "n_symbols": b,
                                           "examples": c}
                                          for a, b, c in new_rows]}),
                      ContentType="application/json", CacheControl="max-age=120")
        rep.ok("  data/source-map.json v1.1 written (real only)")

        rep.section("workbench v1.2 settle + rejoin")
        srct = (ROOT / "lambdas" / "justhodl-tv-workbench" / "source" /
                "lambda_function.py").read_text()
        assert WMARK in srct
        b2 = io.BytesIO()
        with zf.ZipFile(b2, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", srct)
        ok = False
        for _ in range(24):
            try:
                lam.update_function_code(FunctionName="justhodl-tv-workbench",
                                         ZipFile=b2.getvalue(), Publish=True)
            except Exception:
                pass
            c = lam.get_function_configuration(
                FunctionName="justhodl-tv-workbench")
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") != "InProgress":
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName="justhodl-tv-workbench")
                    ["Code"]["Location"], timeout=60).read())) \
                    .read("lambda_function.py").decode()
                if WMARK in dep:
                    ok = True
                    break
            time.sleep(8)
        r = lam.invoke(FunctionName="justhodl-tv-workbench",
                       InvocationType="RequestResponse",
                       Payload=b'{"source": "ops4063"}')
        wb = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-workbench.json")["Body"].read())
        t = wb.get("totals") or {}
        rep.kv(settled=ok, fnerr=r.get("FunctionError"), **t)
        checks = [("real >= 200", len(real) >= 200),
                  ("delta has >= 3 NEW sources", len(new_rows) >= 3),
                  ("workbench v1.2 settled+clean",
                   ok and not r.get("FunctionError")),
                  ("page joins real attribution (>=100 sourced)",
                   (t.get("symbols_with_tv_source") or 0) >= 100)]
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {len(real)} real / {junk} junk / "
               f"{len(new_rows)} NEW sources / page "
               f"{t.get('symbols_with_tv_source')} sourced")


if __name__ == "__main__":
    main()
