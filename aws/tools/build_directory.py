"""build_directory.py — generate directory.html, the organised index of
every page on justhodl.ai. Re-run whenever pages are added.

Reads every root-level *.html in the repo, buckets it into curated
categories (uncategorised pages fall into 'More Tools & Data'), and writes
a single house-style directory.html. No network — pure file scan.
"""
import glob
import json
import os
import re
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))  # aws/tools/ -> repo root

# ── curated categories: (emoji, title, blurb, [pages]) ──
# pages are basenames without .html; sub-apps end with '/'.
CATEGORIES = [
    ("🏦", "Central Banks, Liquidity & Sovereign Stress",
     "Policy stance, balance sheets, the carry trade and euro-area "
     "fragmentation — the macro plumbing.",
     ["cb-injection", "ecb-detail", "ecb", "boj-detail", "snb-detail",
      "systemic-stress", "euro-fragmentation", "yen-carry", "carry",
      "liquidity", "lce", "plumbing", "repo", "ny-fed", "dealer-survey",
      "ofr", "cb/", "funding/"]),
    ("🌪", "Stress, Crisis & Risk",
     "Systemic-stress composites, crisis triggers and the risk dashboard.",
     ["crisis", "stress", "cds-monitor", "defcon", "risk", "auction-crisis",
      "compound-signals", "fleet-health", "audit", "anomaly/"]),
    ("📈", "Macro & Economy",
     "Per-country regimes, the global cycle and the raw macro feeds.",
     ["global-macro", "global-cycle", "global-cycle/", "macro-data",
      "regime", "fred", "bls", "eia", "census", "consumer-pulse",
      "construction-housing", "supply-inflection", "divergence-v2",
      "nasdaq-datalink", "gdelt", "fed-speak", "global/"]),
    ("📊", "Rates, Bonds & Credit",
     "The curve, sovereign and corporate bonds, credit spreads, the dollar.",
     ["bonds", "yield-curve", "dxy", "cross-asset", "cross-asset-rv",
      "treasury-auctions", "auctions", "credit/"]),
    ("💹", "Equities, Screening & Value",
     "Stock screening, fundamentals, baggers and valuation work.",
     ["boom-board", "dividend-growth", "capital-return", "metals-miners", "screener/", "stock/", "stocks/", "deep-value", "fundamentals",
      "baggers", "valuations", "master-rank", "eps-velocity",
      "pead-signals", "earnings-whisper", "earnings/", "sectors",
      "sector-tilt", "themes", "theme-tiers", "ath", "momentum",
      "short-pressure", "short/", "supply-inflection"]),
    ("🎯", "Signals & Alpha",
     "The signal stack, conviction calls, scoreboards and backtests.",
     ["signal-board", "signals", "signal-scorecard", "trading-signals",
      "alpha-scoreboard", "alpha/", "nobrainers", "conviction",
      "conviction/", "best-ideas", "pairs-arb", "trend-engine",
      "risk-radar", "merger-arb", "spinoff-desk", "index-recon",
      "edge", "opportunities",
      "ml-predictions",
      "ai_predictions", "implied-prob", "catalyst-calendar", "catalyst/",
      "event-study", "analogs", "backtest", "accuracy", "composite/"]),
    ("💼", "Insiders, Flow & Smart Money",
     "Insider clusters, 13F holdings, options flow and positioning.",
     ["insiders", "insider", "insider-clusters", "insider-drawdown",
      "smart-money", "smart-money/", "13f", "options-scanner", "flow",
      "pairs", "pairs-scanner", "cot-extremes", "positioning/", "gex/",
      "0dte/", "dix/"]),
    ("🧮", "Portfolio, Risk & Execution",
     "The book, position sizing, multi-strategy capital allocation and "
     "the trade journal.",
     ["portfolio", "portfolio-manager", "portfolio/", "pm-decision",
      "firm-book", "risk-monitor", "factor-risk", "liquidity-capacity",
      "firm-stress", "merger-arb-risk", "pnl-attribution", "desk-allocator",
      "firm-risk-board", "allocator", "position-sizer", "sizing", "sizing/",
      "trade-journal", "trades/", "watchlist"]),
    ("📉", "Volatility & Market Internals",
     "Vol regime, the VIX curve and market-internals breadth.",
     ["vol", "vol-regime", "volatility", "vix-curve", "vix/",
      "market-internals", "tape-reader"]),
    ("🪙", "Crypto",
     "Crypto regime, narratives and the DEX scanner.",
     ["dex", "crypto-narratives", "crypto/"]),
    ("📰", "News, Sentiment & Intelligence",
     "News velocity, sentiment, narratives and the AI intelligence layer.",
     ["news", "sentiment", "narrative", "benzinga", "calls", "intel/",
      "intelligence", "intelligence/", "news-velocity/", "debate/",
      "research", "horizons"]),
    ("🛠", "Reports, System & Ops",
     "Daily briefs, platform health, performance and operational pages.",
     ["today", "brief", "reports", "performance", "system", "health",
      "errors", "downloads", "feedback", "notifications", "alerts",
      "weights", "why", "read", "accuracy", "calibration/"]),
]

# nicer labels for pages whose filename is not self-explanatory
LABELS = {
    "boom-board": "Boom Board",
    "capital-return": "Capital-Return Cannibals",
    "catch-up": "Catch-Up Radar",
    "best-ideas": "Best Ideas \u2014 Confluence Board",
    "pairs-arb": "Pairs Desk \u2014 Statistical Arbitrage",
    "trend-engine": "Systematic Trend Desk",
    "risk-radar": "Risk Radar \u2014 Deterioration Scan",
    "merger-arb": "Merger-Arbitrage Desk",
    "spinoff-desk": "Spin-Off & Special-Situations Desk",
    "desk-allocator": "Desk Allocator \u2014 Multi-Strategy Capital",
    "risk-monitor": "Risk Monitor \u2014 Firm Mandate",
    "factor-risk": "Factor Risk Model \u2014 Barra Decomposition",
    "liquidity-capacity": "Liquidity & Capacity Monitor",
    "firm-stress": "Stress Desk \u2014 Scenario P&L",
    "merger-arb-risk": "Merger-Arb Book Risk \u2014 Deal-Break Stress",
    "pnl-attribution": "P&L Attribution \u2014 Performance Desk",
    "firm-risk-board": "Firm Risk Board \u2014 CRO Synthesis",
    "dividend-growth": "Dividend Compounders",
    "metals-miners": "Metals & Miners",
    "cb-injection": "CB Injection & Carry", "ecb-detail": "ECB Detail",
    "ecb": "ECB Data", "boj-detail": "BOJ & Yen Carry",
    "snb-detail": "SNB & Swiss Franc", "systemic-stress": "Systemic Stress",
    "euro-fragmentation": "Euro Fragmentation", "yen-carry": "Yen Carry",
    "cds-monitor": "Credit Default Monitor",
    "lce": "Liquidity & Credit Engine", "ny-fed": "NY Fed",
    "ofr": "OFR Stress", "13f": "13F Holdings", "ath": "All-Time Highs",
    "dxy": "Dollar Index", "pm-decision": "PM Decision",
    "ai_predictions": "AI Predictions", "gdelt": "GDELT Events",
    "bls": "BLS Labor", "eia": "EIA Energy", "cot-extremes": "COT Extremes",
    "pead-signals": "PEAD Signals", "master-rank": "Master Rank",
    "eps-velocity": "EPS Velocity", "cross-asset-rv": "Cross-Asset RV",
}


def label(page):
    base = page.rstrip("/")
    if base in LABELS:
        return LABELS[base]
    return base.replace("-", " ").replace("_", " ").title()


def href(page):
    return "/" + page if page.endswith("/") else "/" + page + ".html"


def build():
    root_pages = sorted(
        os.path.basename(p)[:-5]
        for p in glob.glob(os.path.join(ROOT, "*.html")))
    subapps = sorted(
        os.path.basename(os.path.dirname(p)) + "/"
        for p in glob.glob(os.path.join(ROOT, "*", "index.html"))
        if not os.path.dirname(p).endswith(("_partials", "archive", "web")))

    assigned = set()
    cat_html = []
    for emoji, title, blurb, pages in CATEGORIES:
        items = []
        for pg in pages:
            exists = (pg in root_pages) or (pg.endswith("/")
                                            and pg in subapps)
            if not exists or pg in assigned:
                continue
            assigned.add(pg)
            items.append((pg, label(pg)))
        if not items:
            continue
        links = "\n".join(
            f'        <a class="d-link" href="{href(pg)}">{lab}</a>'
            for pg, lab in items)
        cat_html.append(
            f'''    <section class="cat">
      <div class="cat-h"><span class="cat-e">{emoji}</span>{title}</div>
      <div class="cat-b">{blurb}</div>
      <div class="d-grid">
{links}
      </div>
    </section>''')

    # everything not curated -> More Tools & Data
    leftover = [p for p in root_pages
                if p not in assigned and p not in ("index", "directory")]
    leftover += [s for s in subapps if s not in assigned]
    if leftover:
        links = "\n".join(
            f'        <a class="d-link" href="{href(pg)}">{label(pg)}</a>'
            for pg in sorted(leftover))
        cat_html.append(
            f'''    <section class="cat">
      <div class="cat-h"><span class="cat-e">🗂</span>More Tools &amp; Data</div>
      <div class="cat-b">Every other page on the platform, A-Z.</div>
      <div class="d-grid">
{links}
      </div>
    </section>''')

    total = len(root_pages) + len(subapps)
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return PAGE.replace("{{CATS}}", "\n".join(cat_html)) \
               .replace("{{TOTAL}}", str(total)) \
               .replace("{{GEN}}", gen)


PAGE = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Directory · JustHodl.AI</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='75' font-size='80'>📁</text></svg>">
<style>
:root{
  --bg:#0a0e14;--panel:#0f131a;--line:#1c2433;
  --mono:'IBM Plex Mono',ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
  --txt:#e6eaf2;--mute:#a8b3c7;--dim:#6f7b91;--cyan:#00d4ff;--green:#26ffaf;
}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--txt);font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;margin:0;font-size:14px;line-height:1.5}
.container{max-width:1180px;margin:0 auto;padding:24px}
header{border-bottom:1px solid var(--line);padding-bottom:16px;margin-bottom:8px}
h1{font-family:var(--mono);font-size:21px;letter-spacing:1px;text-transform:uppercase;margin:0 0 6px;color:var(--cyan)}
.subtitle{color:var(--mute);font-size:12.5px;margin:0;max-width:1000px}
nav.crumbs{margin-top:10px}
nav.crumbs a{color:var(--mute);text-decoration:none;font-family:var(--mono);font-size:11px;letter-spacing:1px;text-transform:uppercase;border-bottom:1px dashed var(--line);margin-right:14px}
nav.crumbs a:hover{color:var(--cyan);border-bottom-color:var(--cyan)}
#search{width:100%;background:var(--panel);border:1px solid var(--line);border-radius:8px;color:var(--txt);font-family:var(--mono);font-size:13px;padding:11px 14px;margin:18px 0 6px}
#search:focus{outline:none;border-color:var(--cyan)}
.cat{margin:22px 0}
.cat-h{font-family:var(--mono);font-size:13px;font-weight:700;letter-spacing:.6px;text-transform:uppercase;color:var(--txt)}
.cat-e{margin-right:8px}
.cat-b{color:var(--dim);font-size:11.5px;margin:3px 0 10px}
.d-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px}
.d-link{display:block;background:var(--panel);border:1px solid var(--line);border-left-width:3px;border-left-color:var(--cyan);border-radius:6px;padding:9px 12px;color:var(--txt);text-decoration:none;font-family:var(--mono);font-size:12px;transition:.12s}
.d-link:hover{border-left-color:var(--green);color:var(--cyan);transform:translateX(2px)}
.empty{color:var(--dim);font-family:var(--mono);font-size:11px;padding:20px 0}
.foot{color:var(--dim);font-size:11px;font-family:var(--mono);margin-top:24px;border-top:1px solid var(--line);padding-top:12px}
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>📁 JustHodl.AI · Full Directory</h1>
    <p class="subtitle">Every page, tool and data view on the platform — {{TOTAL}} in all — organised by desk. Use the filter to jump straight to anything.</p>
    <nav class="crumbs"><a href="/index.html">← desk home</a><a href="/intel/">intel terminal</a></nav>
  </header>
  <input id="search" type="text" placeholder="filter — type to find any tool or page…" autocomplete="off">
  <div id="cats">
{{CATS}}
  </div>
  <div id="noresults" class="empty" style="display:none">no pages match that filter.</div>
  <div class="foot">Generated {{GEN}} · regenerate with aws/tools/build_directory.py</div>
</div>
<script>
const q=document.getElementById("search");
const cats=[...document.querySelectorAll(".cat")];
const nores=document.getElementById("noresults");
q.addEventListener("input",()=>{
  const t=q.value.trim().toLowerCase();
  let any=false;
  cats.forEach(c=>{
    let shown=0;
    c.querySelectorAll(".d-link").forEach(a=>{
      const hit=!t||a.textContent.toLowerCase().includes(t)||a.getAttribute("href").toLowerCase().includes(t);
      a.style.display=hit?"block":"none";
      if(hit)shown++;
    });
    c.style.display=shown?"block":"none";
    if(shown)any=true;
  });
  nores.style.display=any?"none":"block";
});
</script>
</body>
</html>
'''


if __name__ == "__main__":
    html = build()
    out = os.path.join(ROOT, "directory.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[ok] wrote {out} ({len(html)} bytes)")
