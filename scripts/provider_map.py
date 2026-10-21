#!/usr/bin/env python3
"""scripts/provider_map.py - which external data provider each engine pulls, from its source (read-only, deterministic).

Hostnames in every `aws/lambdas/<fn>/source/*.py` plus the `aws/shared` modules it imports, mapped to named providers.
Used by the data-flow audit (docs/audit/2026-10-21/data-flow-audit.md), the provider-alignment pass of
scripts/pipeline_order.py (a provider is a pinned "writer" that fires at its publication time) and the freshness census
(ops 6587).  `python3 scripts/provider_map.py` prints the per-provider counts; `--json` prints {engine: [providers]}.
"""
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
HOST_RE = re.compile(r"https?://([a-z0-9.-]+\.[a-z]{2,})", re.I)
IMPORT_RE = re.compile(r"^\s*(?:from\s+([A-Za-z_][\w]*)\s+import|import\s+([A-Za-z_][\w]*))", re.M)
PROVIDERS = [
    ("Polygon/Massive", r"polygon\.io|massive\.com"),
    ("FMP", r"financialmodelingprep\.com"),
    ("FRED", r"stlouisfed\.org"),
    ("SEC EDGAR", r"sec\.gov"),
    ("BLS", r"bls\.gov"),
    ("BEA", r"bea\.gov"),
    ("Census", r"census\.gov"),
    ("ECB", r"ecb\.europa\.eu"),
    ("Eurostat", r"ec\.europa\.eu|eurostat"),
    ("NY Fed", r"newyorkfed\.org"),
    ("Treasury", r"fiscaldata\.treasury\.gov|treasurydirect\.gov|treasury\.gov/resource-center|home\.treasury\.gov|ticdata\.treasury\.gov"),
    ("OFR", r"financialresearch\.gov"),
    ("CFTC", r"cftc\.gov"),
    ("FINRA", r"finra\.org"),
    ("BIS/IMF/WB/OECD", r"bis\.org|imf\.org|worldbank\.org|oecd\.org|db\.nomics|dbnomics"),
    ("Cboe", r"cboe\.com"),
    ("CryptoQuant", r"cryptoquant\.com"),
    ("Crypto exchanges", r"coinbase\.com|coingecko\.com|deribit\.com|coinmarketcap\.com|binance|kraken|coincap\.io|coinpaprika|coinmetrics"),
    ("EIA", r"eia\.gov"),
    ("Trading Economics", r"tradingeconomics\.com"),
    ("Quiver", r"quiverquant\.com"),
    ("News", r"newsapi\.org|gdeltproject\.org|news\.google|benzinga\.com|trends\.google"),
    ("Nasdaq Data Link", r"data\.nasdaq\.com|quandl"),
    ("OpenFIGI", r"openfigi\.com"),
    ("Alpha Vantage", r"alphavantage\.co"),
    ("Yahoo", r"finance\.yahoo\.com"),
    ("TradingView", r"tradingview\.com"),
    ("BoJ", r"boj\.or\.jp|stat-search\.boj"),
    ("LLM", r"anthropic\.com|bigmodel\.cn|z\.ai|x\.ai|perplexity\.ai|openai\.com"),
    ("Telegram", r"telegram\.org"),
    ("own edge", r"justhodl\.ai|justhodl-data-proxy"),
]
NON_DATA = {"LLM", "Telegram", "own edge"}


def provider_of(host):
    for name, rx in PROVIDERS:
        if re.search(rx, host, re.I):
            return name
    if "amazonaws.com" in host or "cloudflare" in host or host.endswith((".local", ".invalid", ".example")):
        return None
    return "other"


def build(repo=ROOT):
    shared = {}
    for p in sorted((repo / "aws/shared").glob("*.py")):
        shared[p.stem] = set(HOST_RE.findall(p.read_text(encoding="utf-8", errors="ignore")))
    out = {}
    for d in sorted((repo / "aws/lambdas").iterdir()):
        src = d / "source"
        if not src.is_dir():
            continue
        try:
            fn = json.loads((d / "config.json").read_text(encoding="utf-8")).get("function_name") or d.name
        except (OSError, ValueError):
            fn = d.name
        hosts, imports = set(), set()
        for p in sorted(src.rglob("*.py")):
            t = p.read_text(encoding="utf-8", errors="ignore")
            hosts |= set(HOST_RE.findall(t))
            for a, b in IMPORT_RE.findall(t):
                imports.add(a or b)
        for m in imports:
            hosts |= shared.get(m, set())
        provs = sorted({pv for pv in (provider_of(h) for h in hosts) if pv})
        out[fn] = provs
    return out


def data_providers(provs):
    return [p for p in provs if p not in NON_DATA and p != "other"]


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    m = build()
    if "--json" in argv:
        print(json.dumps(m, indent=1, sort_keys=True))
        return 0
    from collections import Counter
    c = Counter(p for provs in m.values() for p in provs)
    for p, n in sorted(c.items(), key=lambda kv: -kv[1]):
        print("%-20s %d" % (p, n))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
