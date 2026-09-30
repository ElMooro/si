#!/usr/bin/env python3
"""gen_nav_manifest.py — ops 3269. The drawer's page list regenerates
from the ACTUAL repo pages on every deploy (it froze at 2026-07-05 and
silently hid every page added since, which also filtered their stars
out of the FAVORITES section). Known hrefs keep their existing
category; new pages are keyword-classified; redirect stubs are
skipped."""
import json
import os
import re
import tempfile
from html import unescape
from html.parser import HTMLParser
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CUR = ROOT / "nav-manifest.json"
RULES = [
    ("Crypto & Digital", r"crypto|btc|bitcoin|coin|stablecoin|dex|"
     r"onchain|altseason|eth"),
    ("Vol & Sentiment", r"vix|vol-|volatil|options|opex|sentiment|"
     r"gamma|rv-iv|buzz"),
    ("Portfolio & Execution", r"portfolio|position|sizer|allocator|"
     r"book|pnl|journal|wealth|tax|paper|merger|deal"),
    ("Risk & Crisis", r"risk|crisis|stress|defcon|hedge|tail|"
     r"monitor|early-warning|systemic|drawdown"),
    ("Macro & Liquidity", r"macro|liquidity|dealer|fed|ecb|boj|snb|dollar|"
     r"yield|bond|rates|cycle|nowcast|gdp|inflation|employment|bls|"
     r"eia|treasury|auction|plumbing|eurodollar|repo"),
    ("Research & Tools", r"research|why|panels|ask|brain|notes|"
     r"compare|ticker|chart|glossary|directory|proof|backtest|"
     r"methodology"),
    ("System & Meta", r"audit|engine-|llm|api|account|settings|"
     r"pricing|terms|privacy|contact|about|status|feedback|"
     r"downloads|notifications"),
]


class TitleParser(HTMLParser):
    # Title text is RCDATA: collect markup-like text intact, then decode once.
    CDATA_CONTENT_ELEMENTS = ("script", "style", "title")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.active = False
        self.parts = []
        self.titles = []

    def handle_starttag(self, tag, attrs):
        if tag == "title":
            self.active = True
            self.parts = []

    def handle_data(self, text):
        if self.active:
            self.parts.append(text)

    def handle_endtag(self, tag):
        if tag == "title" and self.active:
            self.titles.append(unescape("".join(self.parts)))
            self.active = False


def title_of(p):
    parser = TitleParser()
    parser.feed(p.read_text(encoding="utf-8"))
    parser.close()
    if parser.active or len(parser.titles) > 1:
        raise ValueError("One complete page title required: " + p.name)
    if not parser.titles:
        return None
    t = re.sub(r"\s+", " ", parser.titles[0]).strip()
    t = re.sub(r"\s*[|·—-]\s*JustHodl.*$", "", t, flags=re.I).strip()
    return t or None


def write_manifest(path, value):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n",
                dir=path.parent, prefix=path.name + ".", suffix=".tmp", delete=False) as target:
            temporary = Path(target.name)
            target.write(json.dumps(value, ensure_ascii=False))
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


FORCE = {  # ops 3302
    "/fx-research.html": "Macro & Liquidity",
    "/futures-research.html": "Macro & Liquidity",
    "/market-evidence.html": "Research & Tools",
    "/khalid.html": "System & Meta",           # Khalid cross-asset risk and opportunity engine
    "/fusion.html": "System & Meta",           # ops 5215 JustHodl Intelligence Network fusion desk
    "/sp500.html": "Research & Tools",         # ops 4809 index-as-a-stock
    "/spx-beaters.html": "Portfolio & Execution",  # ops 4811 weekly beat-SPX league
    "/data.html": "System & Meta",           # ops 4506 Data hub
    "/provider.html": "System & Meta",       # ops 4506 provider template: explicit category pins (beat keyword collisions)
    "/readthrough.html": "Portfolio & Execution",   # ops 3699 read-through radar
    "etf-census.html": "Research & Tools",
    "fixed-income-census.html": "Research & Tools",
    "/proven-alpha.html": "Portfolio & Execution",     # ops 3519
    "/alpha-families.html": "Portfolio & Execution",   # ops 3459
    "/proven-portfolio.html": "Portfolio & Execution",  # ops 3459
    "/short-book.html": "Portfolio & Execution",        # ops 3459
    "/political.html": "Research & Tools",              # ops 3459
    "/fundamental-census.html": "Research & Tools",    # ops 3527
    "/fundamental-graphs.html": "Research & Tools",     # ops 3462
    "/rotation-dashboard.html": "Portfolio & Execution",  # ops 3819
    "/global-recession.html": "Macro & Liquidity",         # ops 3825
    "/physical-trade.html": "Macro & Liquidity",           # ops 3844
    "/btc-cycle.html": "Crypto & Digital",                 # ops 3825
    "/primary-dealers.html": "Macro & Liquidity",
    "/jsi.html": "Risk & Crisis",
    "/sovereign-stress.html": "Risk & Crisis",
    "/global-sovereign.html": "Macro & Liquidity",
    "/geo-risk.html": "Risk & Crisis",              # ops 3653
    "/portwatch.html": "Macro & Liquidity",          # ops 3653
    "/bis-crossborder.html": "Macro & Liquidity",    # ops 3653
    "/freight-pulse.html": "Macro & Liquidity",      # ops 3662
    "/boom-stage.html": "Macro & Liquidity",         # ops 3678
    "/import-canary.html": "Macro & Liquidity",      # ops 3732
    "/capture-gap.html": "Research & Tools",         # ops 3767
    "/grid-queue.html": "Macro & Liquidity",         # ops 3739
    "/insider-industry-cluster.html": "Research & Tools",  # ops 3745
    "/credit-before-equity.html": "Macro & Liquidity",     # ops 3756
    "/ppi-acceleration.html": "Macro & Liquidity",        # ops 3760
    "/port-cargo.html": "Macro & Liquidity",              # ops 4559: daily tonnage fast layer, sits with portwatch/freight
    "/accum-composite.html": "Equity Signals",            # ops 4559: evidence-tiered accumulation board
    "/flows.html": "Equity Signals",                      # ops 4559: ETF true flows at NAV
    "/dark-pool.html": "Equity Signals",                  # ops 4559: FINRA ATS share-of-volume
    "/flow-lookthrough.html": "Equity Signals",           # ops 4559: tier-A mechanical constituent flow
    "/fortress.html": "Equity Signals",   # ops 5081: Fortress Coil dump-resilient accumulation radar
    "/katlin.html": "Equity Signals",     # ops 5203: KATLIN buy desk (war-room posture + asymmetric bottoms across stocks/ETFs/crypto)
    "/bottom.html": "Equity Signals",
    "/ai.html": "System & Meta",       # ops 5300: AI -- the SageMaker front window (inventory, JumpStart financial models, Brain training pipeline, cost guard)     # ops 5290: BOTTOM Wyckoff bottom desk (climax -> rally -> secondary test -> trigger, every asset class)
    "/floor.html": "Risk & Crisis",   # ops 4919: asset-floor auditor —
    # "auditor" in the title otherwise collides with the System & Meta
    # "audit" keyword and files a risk desk under settings/legal.
}


def classify(href, title):
    hay = (href + " " + title).lower()
    for name, rx in RULES:
        if re.search(rx, hay):
            return name
    return "Equity Signals"


def main():
    cur = json.loads(CUR.read_text(encoding="utf-8")) if CUR.exists() else {}
    order = [c["name"] for c in cur.get("categories") or []]
    for extra in ("Equity Signals", "Research & Tools",
                  "System & Meta"):
        if extra not in order:
            order.append(extra)
    known = {}
    for c in cur.get("categories") or []:
        for pg in c.get("pages") or []:
            known[pg["href"]] = c["name"]
    cats = {n: [] for n in order}
    n = 0
    for p in sorted(ROOT.glob("*.html")):
        href = "/" + p.name
        t = title_of(p)
        if not t or t.lower() in ("redirecting", "redirect"):
            continue
        cat = FORCE.get(href) or known.get(href) or classify(href, t)
        cats.setdefault(cat, []).append({"href": href, "title": t})
        n += 1
    out = {"generated_at": date.today().isoformat(), "n_pages": n,
           "title_encoding": "unicode_text",
           "categories": [{"name": k, "count": len(v), "pages": v}
                          for k, v in cats.items() if v]}
    write_manifest(CUR, out)
    print(f"[nav] {n} pages across {sum(1 for v in cats.values() if v)}"
          " categories")


if __name__ == "__main__":
    main()
