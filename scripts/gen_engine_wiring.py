#!/usr/bin/env python3
"""scripts/gen_engine_wiring.py — the canonical engine->page wiring layer.

Source of truth for WHERE each previously-orphaned engine feed is displayed.
Emits data/engine-wiring.json and idempotently injects one jh-wire.js line per
page (replacing any prior line). Feed paths are embedded verbatim in each page
so the engine-directory audit's exact-containment check flips them to WIRED.
Re-run any time assignments change. DEAD feeds are never wired (nothing real to
render); INTERNAL feeds (caches/KBs consumed by other engines) are classified,
not displayed. STALE feeds ARE wired — the card shows the feed's own timestamp,
which is the honest treatment.
"""
import json, re, sys, os

os.chdir(os.path.join(os.path.dirname(__file__), ".."))

# page -> [(feed, engine, title, class)]
ASSIGN = {
 "fed-speak.html":[("data/fed-nlp.json","justhodl-fed-nlp","FED SPEECH NLP","FRESH")],
 "fomc.html":[("data/fedwatch.json","justhodl-fedwatch-rate-probability","FEDWATCH RATE PROBABILITIES","FRESH")],
 "econ-calendar.html":[("data/macro-calendar.json","justhodl-macro-calendar","MACRO CALENDAR (companion feed)","FRESH")],
 "global-macro.html":[
   ("data/cb-stance.json","justhodl-cb-stance","GLOBAL CENTRAL-BANK STANCE","FRESH"),
   ("data/china-liquidity-history.json","justhodl-china-liquidity","CHINA LIQUIDITY HISTORY","FRESH"),
   ("data/global-markets.json","justhodl-global-markets","GLOBAL MARKETS BOARD","FRESH")],
 "apac.html":[
   ("data/hkma.json","justhodl-hkma-monitor","HKMA MONITOR (Hong Kong)","FRESH"),
   ("data/taiwan-moea.json","justhodl-taiwan-moea","TAIWAN MOEA ORDERS","FRESH"),
   ("data/singapore-nodx.json","justhodl-singapore-nodx","SINGAPORE NODX","FRESH")],
 "metals-miners.html":[("data/peru-copper.json","justhodl-peru-copper","PERU COPPER PRODUCTION","FRESH")],
 "dollar.html":[("data/fx-intelligence.json","justhodl-fx-intelligence","FX INTELLIGENCE","FRESH")],
 "macro-leads.html":[("data/leading-markets-history.json","justhodl-leading-markets","LEADING MARKETS HISTORY","FRESH")],
 "liquidity.html":[("data/liquidity-profile.json","justhodl-liquidity-profile","LIQUIDITY PROFILE","FRESH")],
 "bond-desk.html":[("data/bond-trace.json","justhodl-bond-trace","TRACE BOND TAPE","FRESH")],
 "ny-fed.html":[("data/nyfed-primary-dealer.json","justhodl-nyfed-pd","PRIMARY DEALER POSITIONS","STALE")],
 "sentiment.html":[("data/naaim.json","justhodl-naaim","NAAIM MANAGER EXPOSURE","STALE")],
 "carry.html":[("data/polygon-futures-curves.json","justhodl-polygon-futures-curves","FUTURES CURVES (Polygon)","FRESH")],
 "cross-asset-rv.html":[("data/cross-asset-rv-history.json","justhodl-cross-asset-rv","CROSS-ASSET RV HISTORY","FRESH")],
 "cot-extremes.html":[("data/cftc-all-cache.json","cftc-futures-positioning-agent + justhodl-cftc-deep-view","CFTC 29-CONTRACT FULL CACHE","FRESH")],
 "crypto-liquidity.html":[
   ("data/crypto-etf-flows-history.json","justhodl-crypto-etf-flows","CRYPTO ETF FLOWS","FRESH"),
   ("data/coinbase-premium-history.json","justhodl-coinbase-premium","COINBASE PREMIUM","FRESH"),
   ("data/crypto-basis-history.json","justhodl-crypto-basis","FUTURES BASIS","FRESH"),
   ("data/hyperliquid-perps-history.json","justhodl-hyperliquid-perps","HYPERLIQUID PERPS","FRESH")],
 "crypto-risk.html":[
   ("data/crypto-dvol.json","justhodl-crypto-dvol","DERIBIT DVOL","FRESH"),
   ("data/crypto-gex.json","justhodl-crypto-gex","CRYPTO GEX","FRESH"),
   ("data/crypto-funding.json","justhodl-crypto-funding","PERP FUNDING","FRESH"),
   ("data/crypto-options-history.json","justhodl-crypto-options","OPTIONS FLOW HISTORY","FRESH"),
   ("data/crypto-options-surface-history.json","justhodl-crypto-options-surface","OPTIONS SURFACE HISTORY","FRESH"),
   ("data/crypto-cot-history.json","justhodl-crypto-cot","CRYPTO COT HISTORY","FRESH")],
 "crypto-confluence.html":[("data/crypto-engine-trust.json","justhodl-crypto-scorecard","CRYPTO ENGINE TRUST","FRESH")],
 "activist-13d.html":[("data/activist-filings-state.json","justhodl-activist-filings-scanner","ACTIVIST FILINGS STATE","FRESH")],
 "ark.html":[("data/ark-holdings-prev.json","justhodl-ark-holdings","ARK HOLDINGS (prior snapshot / diff base)","FRESH")],
 "benzinga.html":[("data/benzinga-earnings-calendar.json","justhodl-benzinga-earnings","BENZINGA EARNINGS CALENDAR","FRESH")],
 "compounders.html":[("data/coffee-can-holdings.json","justhodl-coffee-can","COFFEE-CAN HOLDINGS","FRESH")],
 "earnings-whisper.html":[
   ("data/earnings-confluence.json","justhodl-earnings-confluence","EARNINGS CONFLUENCE","FRESH"),
   ("data/earnings-tone-velocity.json","justhodl-earnings-tone-velocity","EARNINGS TONE VELOCITY","FRESH")],
 "pead-signals.html":[("data/earnings-pead.json","justhodl-earnings-pead","PEAD SIGNALS FEED","FRESH")],
 "index-recon.html":[("data/finviz-index-membership.json","justhodl-index-inclusion","INDEX MEMBERSHIP (FinViz)","FRESH")],
 "compare.html":[("data/peer-comparison.json","justhodl-peer-comparison","PEER COMPARISON","FRESH")],
 "analyst-actions.html":[("data/sellside-views.json","justhodl-sellside-views","SELLSIDE VIEWS","FRESH")],
 "attention.html":[
   ("data/google-trends.json","justhodl-google-trends","GOOGLE TRENDS","FRESH"),
   ("data/ticker-trends.json","justhodl-ticker-trends","TICKER SEARCH TRENDS","FRESH"),
   ("data/ai-infra-stack.json","justhodl-stocktwits","RETAIL ATTENTION · STOCKTWITS","FRESH")],
 "gdelt.html":[("data/gdelt-buzz.json","justhodl-gdelt-buzz","GDELT NEWS BUZZ","FRESH")],
 "news.html":[("data/news-wire-state.json","justhodl-news-wire","NEWS WIRE STATE","FRESH")],
 "themes.html":[
   ("data/themes-detected.json","justhodl-theme-detector","THEMES DETECTED","FRESH"),
   ("data/theme-rotation-state.json","justhodl-theme-rotation-engine","THEME ROTATION STATE","FRESH")],
 "patterns.html":[("data/seasonality.json","justhodl-seasonality","SEASONALITY MAP","FRESH")],
 "supply-chain.html":[("data/supply-chain-linkage.json","justhodl-supply-chain-linkage","SUPPLY-CHAIN LINKAGE","FRESH")],
 "market-extremes.html":[("data/magnitude-distributions.json","justhodl-magnitude-distributions","MOVE MAGNITUDE DISTRIBUTIONS","FRESH")],
 "dark-pool.html":[("data/dix-history.json","justhodl-dix","DIX HISTORY (dark-pool index)","FRESH")],
 "regime.html":[("data/regime-anomaly.json","justhodl-regime-anomaly","REGIME ANOMALY","FRESH")],
 "political.html":[("data/congress-party-map.json","justhodl-political-stocks","CONGRESS PARTY MAP","STALE")],
 "stress.html":[("data/stress-factor-loadings.json","justhodl-stress-loadings","STRESS FACTOR LOADINGS","STALE")],
 "trade-journal.html":[
   ("data/trade-journal.json","justhodl-trade-evaluator","TRADE JOURNAL (evaluator)","FRESH"),
   ("data/user-trades-stats.json","justhodl-trade-journal","USER TRADE STATS","STALE"),
   ("data/behavior-mirror.json","justhodl-behavior-mirror","BEHAVIOR MIRROR","STALE")],
 "edge-discovery.html":[("data/causality-discoveries.json","justhodl-causality-scanner","CAUSALITY DISCOVERIES","STALE")],
 "pnl-attribution.html":[("data/engine-contributions.json","justhodl-engine-contribution","ENGINE P&L CONTRIBUTIONS","STALE")],
 "feedback.html":[("data/feedback-summary.json","justhodl-feedback","FEEDBACK SUMMARY","STALE")],
 "alerts.html":[("data/alert-backtests.json","justhodl-alert-backtester","ALERT BACKTESTS","FRESH")],
 "calibration-fleet.html":[("data/calibration-history.json","justhodl-alpha-calibrator","CALIBRATION HISTORY","FRESH")],
 "dep-graph.html":[("data/engine-signal-map.json","justhodl-engine-signal-map","ENGINE→SIGNAL MAP","FRESH")],
 "system-health.html":[
   ("data/event-flow-health.json","justhodl-event-flow-monitor","EVENT-FLOW HEALTH","FRESH"),
   ("data/dr-snapshot-latest.json","justhodl-dr-snapshot","DISASTER-RECOVERY SNAPSHOT","FRESH")],
 "api-docs.html":[("data/feed-catalog.json","justhodl-feed-catalog","FEED CATALOG","FRESH")],
 "uptime.html":[("data/schedule-liveness.json","justhodl-schedule-liveness","SCHEDULE LIVENESS","FRESH")],
 "llm-cost.html":[("data/cost-anomaly.json","justhodl-cost-anomaly","COST ANOMALY","FRESH")],
}

INTERNAL = [  # cache / knowledge-base outputs consumed by other engines — display would be noise
 ("justhodl-liquidity-agent","data/fred-cache.json"),
 ("justhodl-bond-regime-detector","data/fred-cache-secretary.json"),
 ("justhodl-flows-ai-analysis","data/crisis-knowledge-base.json"),
 ("justhodl-universe-builder","data/universe.json"),
 ("justhodl-factor-decomposition","data/factor-data-cache.json"),
]
DEAD = [  # output key exists in code but object absent/never written — fix engine first, nothing to render
 ("justhodl-news-velocity","data/news-velocity-history.json"),
 ("justhodl-engine-robustness","data/engine-robustness.json"),
 ("justhodl-commodity-curves","data/commodity-curves-history.json"),
 ("justhodl-transcript-indexer","data/transcripts-index.json"),
 ("justhodl-transcript-query","data/transcripts-index.json"),
 ("justhodl-eurostat-history","data/ecb-confidence.json"),
 ("justhodl-analyst-consensus","data/analyst-consensus-history.json"),
 ("justhodl-kill-switch","data/kill-switch-state.json"),
]

def main():
    problems, patched, wired = [], [], []
    for page, feeds in sorted(ASSIGN.items()):
        if not os.path.exists(page):
            problems.append(f"MISSING PAGE {page}"); continue
        src = open(page, encoding="utf-8", errors="replace").read()
        if len(src) < 1500 or 'http-equiv="refresh"' in src:
            problems.append(f"STUB/REDIRECT {page} ({len(src)}b) — refusing to wire"); continue
        spec = ";".join(f"{f}|{e}|{t}" for f, e, t, _ in feeds)
        line = f'<script src="/jh-wire.js" defer data-feeds="{spec}"></script>'
        src = re.sub(r'\n?<script src="/jh-wire\.js"[^>]*></script>', "", src)
        m = list(re.finditer(r"</body>", src, re.I))
        if not m:
            problems.append(f"NO </body> {page}"); continue
        i = m[-1].start()
        open(page, "w", encoding="utf-8").write(src[:i] + line + "\n" + src[i:])
        patched.append(page)
        for f, e, t, cls in feeds:
            wired.append({"engine": e, "feed": f, "page": page, "title": t, "freshness_at_wiring": cls, "via": "jh-wire.js"})
    manifest = {
        "v": 1, "generated_by": "scripts/gen_engine_wiring.py",
        "note": "Canonical engine→page wiring for previously-orphaned feeds (ops 2944/2945 audit, corrected matcher). WIRED = displayed via jh-wire.js on that page. INTERNAL = feeds other engines by design. DEAD = engine output missing in S3; fix engine, then assign.",
        "wired": wired,
        "internal": [{"engine": e, "feed": f, "class": "INTERNAL-BY-DESIGN"} for e, f in INTERNAL],
        "dead": [{"engine": e, "feed": f, "class": "DEAD-FEED"} for e, f in DEAD],
    }
    os.makedirs("data", exist_ok=True)
    json.dump(manifest, open("data/engine-wiring.json", "w"), indent=1)
    print(f"pages patched: {len(patched)}  feeds wired: {len(wired)}  internal: {len(INTERNAL)}  dead: {len(DEAD)}")
    for p in problems: print("PROBLEM:", p)
    if problems: sys.exit(1)

if __name__ == "__main__":
    main()
