"""ops_4049 — FULL HARVEST CHECK: sources landed? delta computed, workbench
rejoined, note-fidelity re-proven — every standing gate in one pass."""
import json
import random
import re
import sys
import time
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
TV_RX = re.compile(r"\[TV:([A-Z0-9_:!\.\-]+)\]")

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


def gj(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    with report("4049_full_harvest_check") as rep:
        rep.heading("ops 4049 — full harvest check")
        checks = []

        rep.section("A. SOURCES — the moment of truth")
        sr = None
        try:
            sr = gj("data/tv-sources.json")
        except Exception:
            pass
        if not sr:
            rep.fail("data/tv-sources.json STILL NOT BORN — either the "
                     "harvest hasn't reached a 15-min sync yet, or capture "
                     "finds no source fields. Badge sources-count decides.")
            sys.exit(1)
        m = sr.get("sources") or {}
        rep.kv(symbols_with_source=len(m), generated_at=sr.get("generated_at"))
        checks.append(("sources landed (>=200 symbols)", len(m) >= 200))

        rep.section("B. DELTA — sources the system does not have")
        by_src, examples = Counter(), defaultdict(list)
        for sym, v in m.items():
            src = (v.get("source") or "").strip()
            if src:
                by_src[src] += 1
                if len(examples[src]) < 3:
                    examples[src].append(sym)
        known_ct, new_rows = Counter(), []
        for src, n in by_src.most_common():
            fam = fam_of(src)
            (known_ct.__setitem__(fam, known_ct[fam] + n) if fam
             else new_rows.append((src, n, examples[src])))
        rep.kv(distinct_sources=len(by_src), known_families=len(known_ct),
               new_sources=len(new_rows),
               symbols_on_new=sum(n for _, n, _ in new_rows))
        for src, n, ex in new_rows[:40]:
            rep.log(f"  {n:5d}  {src[:64]}   e.g. {', '.join(ex)[:60]}")
        s3.put_object(Bucket=BUCKET, Key="data/source-map.json",
                      Body=json.dumps({
                          "generated_at": datetime.now(timezone.utc).isoformat(),
                          "marker": "source-map v1.0 ops4049",
                          "symbols_with_source": len(m),
                          "distinct_sources": len(by_src),
                          "known_families": dict(known_ct),
                          "new_sources": [{"source": a, "n_symbols": b,
                                           "examples": c}
                                          for a, b, c in new_rows]}),
                      ContentType="application/json", CacheControl="max-age=300")
        rep.ok("  data/source-map.json written")

        rep.section("C. workbench rebuild — attribution joined")
        r = lam.invoke(FunctionName="justhodl-tv-workbench",
                       InvocationType="RequestResponse",
                       Payload=b'{"source": "ops4049"}')
        checks.append(("workbench invoke clean", not r.get("FunctionError")))
        wb = gj("data/tv-workbench.json")
        t = wb.get("totals") or {}
        rep.kv(**t)
        checks += [("workbench current 491/10k",
                    (t.get("watchlists") or 0) >= 485
                    and (t.get("unique_symbols") or 0) >= 9000),
                   ("attribution ON THE PAGE (>=150 sourced)",
                    (t.get("symbols_with_tv_source") or 0) >= 150)]

        rep.section("D. note fidelity — standing gate")
        brain = gj("data/brain.json")
        syms = wb.get("symbols") or {}
        in_wl = {}
        for k, v in syms.items():
            in_wl[k] = v
            in_wl.setdefault(v.get("bare"), v)
        tagged = defaultdict(list)
        for n in brain.get("notes", []):
            txt = n.get("text") or ""
            mm = TV_RX.match(txt)
            if mm:
                tagged[mm.group(1)].append(txt[mm.end():].strip())
        mism = 0
        pool = []
        for sym, notes in tagged.items():
            row = in_wl.get(sym) or in_wl.get(sym.split(":")[-1])
            if not row:
                continue
            if (row.get("n_notes") or 0) < len(notes):
                mism += 1
            pool += [(sym, x) for x in notes]
        random.seed(4049)
        bad = 0
        for sym, txt in random.sample(pool, min(30, len(pool))):
            row = in_wl.get(sym) or in_wl.get(sym.split(":")[-1])
            if txt not in [x.get("t") for x in row.get("notes") or []]:
                bad += 1
        rep.kv(count_mismatches=mism, verbatim_bad=bad)
        checks += [("every watchlisted symbol carries all notes", mism == 0),
                   ("30/30 verbatim", bad == 0)]

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {len(m)} sourced / {len(new_rows)} NEW sources "
               f"({sum(n for _, n, _ in new_rows)} of his indicators) / "
               f"fidelity intact / page live")


if __name__ == "__main__":
    main()
