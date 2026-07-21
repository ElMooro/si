#!/usr/bin/env python3
"""gen_nav_manifest.py — ops 3269. The drawer's page list regenerates
from the ACTUAL repo pages on every deploy (it froze at 2026-07-05 and
silently hid every page added since, which also filtered their stars
out of the FAVORITES section). Known hrefs keep their existing
category; new pages are keyword-classified; redirect stubs are
skipped."""
import json
import re
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


def title_of(p):
    try:
        head = p.read_text(encoding="utf-8", errors="replace")[:4000]
    except Exception:
        return None
    m = re.search(r"<title>(.*?)</title>", head, re.S | re.I)
    if not m:
        return None
    t = re.sub(r"\s+", " ", m.group(1)).strip()
    t = re.sub(r"\s*[|·—-]\s*JustHodl.*$", "", t, flags=re.I).strip()
    return t or None


FORCE = {  # ops 3302: explicit category pins (beat keyword collisions)
    "etf-census.html": "Research & Tools",
    "fixed-income-census.html": "Research & Tools",
    "/proven-alpha.html": "Portfolio & Execution",     # ops 3519
    "/alpha-families.html": "Portfolio & Execution",   # ops 3459
    "/proven-portfolio.html": "Portfolio & Execution",  # ops 3459
    "/short-book.html": "Portfolio & Execution",        # ops 3459
    "/political.html": "Research & Tools",              # ops 3459
    "/fundamental-census.html": "Research & Tools",    # ops 3527
    "/fundamental-graphs.html": "Research & Tools",     # ops 3462
    "/primary-dealers.html": "Macro & Liquidity",
    "/jsi.html": "Risk & Crisis",
    "/sovereign-stress.html": "Risk & Crisis",
    "/global-sovereign.html": "Macro & Liquidity",
    "/geo-risk.html": "Risk & Crisis",              # ops 3653
    "/portwatch.html": "Macro & Liquidity",          # ops 3653
    "/bis-crossborder.html": "Macro & Liquidity",    # ops 3653
    "/freight-pulse.html": "Macro & Liquidity",      # ops 3662
}


def classify(href, title):
    hay = (href + " " + title).lower()
    for name, rx in RULES:
        if re.search(rx, hay):
            return name
    return "Equity Signals"


def main():
    cur = json.loads(CUR.read_text()) if CUR.exists() else {}
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
           "categories": [{"name": k, "count": len(v), "pages": v}
                          for k, v in cats.items() if v]}
    CUR.write_text(json.dumps(out, ensure_ascii=False))
    print(f"[nav] {n} pages across {sum(1 for v in cats.values() if v)}"
          " categories")


if __name__ == "__main__":
    main()
