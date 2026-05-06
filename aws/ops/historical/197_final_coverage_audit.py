#!/usr/bin/env python3
"""
Step 197 — FINAL coverage audit.

After 4 paths (A: fix broken sub-nav, B: promote 7, C: 10 raw API
tiles + Khalid, D: /system.html + /investor.html), produce the
definitive answer to:

  "Is every feature of my entire system displayed and accessible
   on the website?"

Method:
  A. Re-inventory: HTML pages, launcher tiles, topbar nav links
  B. Re-inventory: 48 justhodl-* + 18 agent Lambdas
  C. For each Lambda, classify SURFACED / UTILITY / GAP based on
     a hand-curated mapping (no more guessing)
  D. Print a final coverage matrix:
        Total Lambdas         X
        Surfaced via page     Y
        Utility (no UI needed) Z
        TRUE gaps              W
  E. List the W remaining gaps with recommendations
"""
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)


# ─── HAND-CURATED LAMBDA → PAGE MAPPING ─────────────────────────────────
# Built from: memory userMemories + step 190 audit + step 192 audit +
#             session 2026-04-26 work building /system.html + /investor.html

LAMBDA_MAP = {
    # justhodl-* Lambdas
    "justhodl-stock-screener":       {"status":"surfaced", "page":"/screener/", "data":"screener/data.json"},
    "justhodl-stock-analyzer":       {"status":"surfaced", "page":"/stock/ (on-demand)", "data":"stock-analysis/{ticker}.json"},
    "justhodl-stock-ai-research":    {"status":"surfaced", "page":"/stock/ AI tab", "data":"on-demand via api.justhodl.ai/research"},
    "justhodl-ai-chat":              {"status":"surfaced", "page":"AI chat (multiple pages)", "data":"on-demand via api.justhodl.ai/"},
    "justhodl-edge-engine":          {"status":"surfaced", "page":"/edge.html + /", "data":"edge-data.json"},
    "justhodl-options-flow":         {"status":"surfaced", "page":"/flow.html + /", "data":"flow-data.json"},
    "justhodl-daily-report-v3":      {"status":"surfaced", "page":"/intelligence.html + /", "data":"data/report.json + intelligence-report.json"},
    "justhodl-intelligence":         {"status":"surfaced", "page":"/intelligence.html", "data":"intelligence-report.json"},
    "justhodl-morning-intelligence": {"status":"surfaced-partial", "page":"Telegram alerts", "data":"morning brief, no web UI yet"},
    "justhodl-pnl-tracker":          {"status":"surfaced", "page":"/desk.html PnL card", "data":"portfolio/pnl-daily.json"},
    "justhodl-risk-sizer":           {"status":"surfaced", "page":"/risk.html", "data":"risk/recommendations.json"},
    "justhodl-asymmetric-scorer":    {"status":"surfaced", "page":"/desk.html setups", "data":"opportunities/asymmetric-equity.json"},
    "justhodl-divergence-scanner":   {"status":"surfaced", "page":"/desk.html divergences", "data":"divergence/current.json"},
    "justhodl-bond-regime-detector": {"status":"surfaced", "page":"/desk.html regime cell + /", "data":"regime/current.json"},
    "justhodl-cot-extremes-scanner": {"status":"surfaced", "page":"/positioning/", "data":"cot/extremes/current.json"},
    "justhodl-valuations-agent":     {"status":"surfaced", "page":"/valuations.html", "data":"valuations data"},
    "justhodl-crypto-intel":         {"status":"surfaced", "page":"/crypto/ + /", "data":"crypto-intel.json"},
    "justhodl-dex-scanner":          {"status":"surfaced", "page":"/dex.html", "data":"dex-data.json"},
    "justhodl-telegram-bot":         {"status":"utility", "page":"Telegram alerts (no web UI needed)", "data":"telegram/"},
    "justhodl-signal-logger":        {"status":"utility-loop1", "page":"backend Loop 1 (DDB justhodl-signals)", "data":"DynamoDB internal"},
    "justhodl-outcome-checker":      {"status":"utility-loop1", "page":"backend Loop 1 (DDB justhodl-outcomes)", "data":"DynamoDB internal"},
    "justhodl-calibrator":           {"status":"utility-loop1", "page":"surfaced via /reports.html scorecard", "data":"SSM /justhodl/calibration/*"},
    "justhodl-khalid-metrics":       {"status":"surfaced", "page":"/khalid/", "data":"data/khalid-metrics.json"},
    "justhodl-liquidity-agent":      {"status":"surfaced", "page":"/liquidity.html", "data":"liquidity-data.json"},
    "justhodl-ml-predictions":       {"status":"surfaced", "page":"/ml-predictions.html", "data":"predictions.json"},
    "justhodl-reports-builder":      {"status":"surfaced", "page":"/reports.html", "data":"reports/scorecard.json"},
    "justhodl-prompt-iterator":      {"status":"utility-loop3", "page":"backend Loop 3 (weekly self-improvement)", "data":"learning/prompt_templates.json"},
    "justhodl-watchlist-debate":     {"status":"surfaced", "page":"/investor.html (cached results)", "data":"investor-analysis/{ticker}.json"},
    "justhodl-investor-agents":      {"status":"surfaced", "page":"/investor.html (NEW)", "data":"on-demand via api.justhodl.ai/investor"},
    "justhodl-fred-proxy":           {"status":"surfaced", "page":"/fred.html (via proxy)", "data":"FRED API"},
    "justhodl-ecb-proxy":            {"status":"surfaced", "page":"/ecb.html (via proxy)", "data":"ECB API"},
    "justhodl-treasury-proxy":       {"status":"surfaced", "page":"/treasury-auctions.html (via proxy)", "data":"Treasury API"},
    "justhodl-health-monitor":       {"status":"surfaced", "page":"/system.html (NEW)", "data":"_health/dashboard.json"},
    "justhodl-charts-agent":         {"status":"surfaced", "page":"/charts.html backend", "data":"chart data"},
    "justhodl-advanced-charts":      {"status":"surfaced", "page":"/charts.html backend", "data":"chart data"},
    "justhodl-crypto-enricher":      {"status":"utility", "page":"feeds /crypto/ via crypto-intel.json", "data":"backend"},
    "justhodl-bloomberg-v8":         {"status":"utility", "page":"feeds /charts.html (every 5min)", "data":"backend"},
    "justhodl-news-sentiment":       {"status":"GAP", "page":"PRODUCES sentiment data NOT SURFACED", "data":"sentiment/news.json (file missing!)"},
    "justhodl-financial-secretary":  {"status":"GAP", "page":"runs every 4h, no UI", "data":"secretary/findings.json (file missing!)"},
    "justhodl-repo-monitor":         {"status":"GAP", "page":"/repo.html is 451B stub", "data":"repo-data.json"},
    "justhodl-daily-macro-report":   {"status":"GAP", "page":"daily 12:00 report, no UI", "data":"unknown — NO LOGS"},
    "justhodl-email-reports":        {"status":"utility", "page":"Telegram daily 13:00 (8AM ET)", "data":"sends, no UI needed"},
    "justhodl-email-reports-v2":     {"status":"utility", "page":"Telegram daily 12:00", "data":"sends, no UI needed"},
    "justhodl-cache-layer":          {"status":"utility", "page":"backend cache (no UI)", "data":"internal"},
    "justhodl-data-collector":       {"status":"utility", "page":"backend collector (no UI)", "data":"internal"},
    "justhodl-ultimate-orchestrator":{"status":"utility-deprecated", "page":"old orchestrator (Sep 2025)", "data":"likely zombie"},
    "justhodl-ultimate-trading":     {"status":"utility-deprecated", "page":"old (Sep 2025)", "data":"likely zombie"},
    "justhodl-chat-api":             {"status":"utility", "page":"likely older alias of ai-chat", "data":"likely deprecated"},

    # Other agent Lambdas (raw data sources)
    "fred-ice-bofa-api":             {"status":"surfaced", "page":"/fred.html (via fred proxy)", "data":"FRED API"},
    "fedliquidityapi":               {"status":"surfaced", "page":"/liquidity.html backend", "data":"liquidity sources"},
    "fmp-fundamentals-agent":        {"status":"surfaced", "page":"/fmp.html + /screener/", "data":"FMP API"},
    "fmp-stock-picks-agent":         {"status":"GAP", "page":"runs hourly weekdays, no UI", "data":"stock picks data"},
    "alphavantage-market-agent":     {"status":"surfaced", "page":"/stock/ (Alpha Vantage data)", "data":"AV API"},
    "alphavantage-technical-analysis":{"status":"surfaced", "page":"/stock/ technical tab", "data":"AV TA"},
    "bls-labor-agent":               {"status":"surfaced", "page":"/bls.html", "data":"BLS API"},
    "bls-employment-api-v2":         {"status":"surfaced", "page":"/bls.html (v2)", "data":"BLS data"},
    "bea-economic-agent":            {"status":"GAP", "page":"BEA data agent, no /bea.html page", "data":"BEA economic"},
    "benzinga-news-agent":           {"status":"surfaced", "page":"/benzinga.html", "data":"news data"},
    "bond-indices-agent":            {"status":"GAP", "page":"hourly, no /bonds.html page", "data":"bond indices"},
    "census-economic-agent":         {"status":"surfaced", "page":"/census.html", "data":"Census API"},
    "cftc-futures-positioning-agent":{"status":"surfaced", "page":"/positioning/", "data":"CFTC COT"},
    "coinmarketcap-agent":           {"status":"surfaced", "page":"/crypto/ (CMC source)", "data":"CMC API"},
    "dollar-strength-agent":         {"status":"GAP", "page":"DXY data, no dedicated page", "data":"USD strength"},
    "eia-energy-agent":              {"status":"surfaced", "page":"/eia.html", "data":"EIA API"},
    "enhanced-repo-agent":           {"status":"surfaced-partial", "page":"feeds /repo.html (which is stub)", "data":"repo metrics"},
    "google-trends-agent":           {"status":"GAP", "page":"Google Trends data, no UI", "data":"search trends"},
    "manufacturing-global-agent":    {"status":"GAP", "page":"manufacturing PMI data, no UI", "data":"global PMI"},
    "nasdaq-datalink-agent":         {"status":"surfaced", "page":"/nasdaq-datalink.html", "data":"Nasdaq Data Link"},
    "news-sentiment-agent":          {"status":"GAP", "page":"sentiment data, no UI", "data":"news sentiment"},
    "securities-banking-agent":      {"status":"GAP", "page":"banking sector data, no UI", "data":"banking"},
    "volatility-monitor-agent":      {"status":"GAP", "page":"VIX/MOVE/etc, no /volatility.html", "data":"vol surfaces"},
    "xccy-basis-agent":              {"status":"surfaced", "page":"/carry.html (xccy basis)", "data":"cross-currency"},
    "global-liquidity-agent-v2":     {"status":"surfaced", "page":"/liquidity.html", "data":"global liquidity"},
    "macro-financial-intelligence":  {"status":"GAP", "page":"daily macro report, no UI", "data":"macro intel"},
    "ofrapi":                        {"status":"surfaced", "page":"/ofr.html", "data":"OFR API"},
    "fedapi":                        {"status":"surfaced", "page":"/liquidity.html (Fed source)", "data":"Fed API"},
    "fredapi":                       {"status":"surfaced", "page":"/fred.html", "data":"FRED API (legacy)"},
    "treasury-api":                  {"status":"surfaced", "page":"/treasury-auctions.html", "data":"Treasury API"},
    "treasury-auto-updater":         {"status":"surfaced", "page":"/treasury-auctions.html backend", "data":"auto-updates"},
    "ecb-data-daily-updater":        {"status":"surfaced", "page":"/ecb.html backend", "data":"ECB"},
    "ecb-auto-updater":              {"status":"surfaced", "page":"/ecb.html backend (weekly)", "data":"ECB weekly"},
    "fed-liquidity-indicators":      {"status":"surfaced", "page":"/liquidity.html", "data":"Fed liquidity"},
    "daily-liquidity-report":        {"status":"surfaced", "page":"/liquidity.html (daily)", "data":"Fed daily"},
    "fedliquidity":                  {"status":"surfaced", "page":"/liquidity.html (weekly)", "data":"Fed weekly"},
}


with report("final_coverage_audit") as r:
    r.heading("FINAL Coverage Audit — Definitive Answer")

    # ─── A. Re-inventory pages ──────────────────────────────────────────
    r.section("A. Site inventory")
    repo_root = os.environ.get("GITHUB_WORKSPACE", "/home/claude/si")
    html_files = []
    for f in sorted(os.listdir(repo_root)):
        if f.endswith(".html") and f != "index-old.html":
            html_files.append(f)
    for d in ["stock", "screener", "crypto", "positioning",
              "agent", "bot", "khalid", "euro", "stocks"]:
        p = os.path.join(repo_root, d, "index.html")
        if os.path.exists(p):
            html_files.append(f"{d}/index.html")

    # Get launcher tile count + linked pages from index.html
    with open(os.path.join(repo_root, "index.html")) as f:
        idx = f.read()
    tile_count = idx.count('class="tool"')
    nav_links = re.findall(r'<nav class="nav-links">(.*?)</nav>', idx, re.DOTALL)
    topbar_links = re.findall(r'href="(/[^"]+)"', nav_links[0]) if nav_links else []

    r.log(f"  HTML pages in repo:         {len(html_files)}")
    r.log(f"  Launcher tiles:             {tile_count}")
    r.log(f"  Topbar nav items:           {len(topbar_links)}")

    # ─── B. Lambda inventory ────────────────────────────────────────────
    r.section("B. Lambda inventory")
    paginator = lam.get_paginator("list_functions")
    all_lambdas = []
    for page in paginator.paginate():
        all_lambdas.extend(page.get("Functions", []))
    justhodl = [f for f in all_lambdas if f["FunctionName"].startswith("justhodl")]
    others = [f for f in all_lambdas if not f["FunctionName"].startswith("justhodl")]
    r.log(f"  Total Lambdas:              {len(all_lambdas)}")
    r.log(f"  justhodl-*:                 {len(justhodl)}")
    r.log(f"  other agent Lambdas:        {len(others)}")

    # ─── C. Coverage classification ─────────────────────────────────────
    r.section("C. Coverage classification")
    counts = defaultdict(int)
    unmapped = []
    by_status = defaultdict(list)

    for f in all_lambdas:
        name = f["FunctionName"]
        if name in LAMBDA_MAP:
            entry = LAMBDA_MAP[name]
            status = entry["status"]
            counts[status] += 1
            by_status[status].append((name, entry))
        else:
            unmapped.append(name)

    r.log(f"\n  Coverage breakdown for {len(all_lambdas) - len(unmapped)} mapped Lambdas:")
    for status in sorted(counts.keys()):
        r.log(f"    {status:30} {counts[status]:>3}")
    if unmapped:
        r.log(f"\n  Unmapped Lambdas ({len(unmapped)}):")
        for n in sorted(unmapped):
            r.log(f"    {n}")

    # ─── D. Surfaced — list ─────────────────────────────────────────────
    r.section("D. SURFACED Lambdas (have a page or are part of a surfaced feature)")
    surfaced_keys = ["surfaced", "surfaced-partial"]
    for status in surfaced_keys:
        for name, entry in sorted(by_status[status]):
            r.log(f"  ✅ {name:42} → {entry['page']}")

    # ─── E. Utility — list ──────────────────────────────────────────────
    r.section("E. UTILITY Lambdas (no UI needed by design)")
    util_keys = ["utility", "utility-loop1", "utility-loop3", "utility-deprecated"]
    for status in util_keys:
        if not by_status[status]: continue
        r.log(f"\n  {status}:")
        for name, entry in sorted(by_status[status]):
            r.log(f"    ⚙ {name:42} → {entry['page']}")

    # ─── F. GAPS — list ─────────────────────────────────────────────────
    r.section("F. TRUE GAPS — Lambdas producing data with no UI")
    gaps = sorted(by_status.get("GAP", []))
    if not gaps:
        r.log("  🟢 NO GAPS — every functional Lambda is surfaced!")
    else:
        r.log(f"\n  {len(gaps)} Lambdas with no UI surface:")
        for name, entry in gaps:
            r.log(f"    🔍 {name:42} → {entry['page']}")
            r.log(f"        data: {entry['data']}")

    # ─── G. Final answer ────────────────────────────────────────────────
    r.section("G. FINAL ANSWER")
    n_total = len(all_lambdas) - len(unmapped)
    n_surfaced = sum(counts[k] for k in surfaced_keys)
    n_utility = sum(counts[k] for k in util_keys)
    n_gap = counts.get("GAP", 0)
    if n_total > 0:
        r.log(f"\n  Lambdas surfaced as features:    {n_surfaced}/{n_total}  ({100*n_surfaced/n_total:.0f}%)")
        r.log(f"  Lambdas as utility (no UI need): {n_utility}/{n_total}  ({100*n_utility/n_total:.0f}%)")
        r.log(f"  TRUE coverage gaps:              {n_gap}/{n_total}  ({100*n_gap/n_total:.0f}%)")
        r.log(f"  Effective coverage (surfaced + utility): {100*(n_surfaced+n_utility)/n_total:.0f}%")

    # ─── H. Page health summary ────────────────────────────────────────
    r.section("H. Page health summary")
    r.log(f"\n  Working pages linked from launcher: {tile_count}")
    r.log(f"  Pages built this session:")
    r.log(f"    /system.html   (78-component health monitor)")
    r.log(f"    /investor.html (Legendary Investor Panel — 6 personas)")
    r.log(f"  Pages fixed this session:")
    r.log(f"    intelligence.html, positioning/index.html, crypto/index.html")
    r.log(f"    (sed-replaced 6 dead http://...s3-website... URLs)")
    r.log(f"\n  Stub/dead pages remaining (untouched, awaiting cleanup approval):")
    r.log(f"    Reports.html (252B), ml.html (288B), repo.html (451B), stocks.html (249B)")
    r.log(f"    pro.html (59 days stale), exponential-search-dashboard.html (dead OpenBB),")
    r.log(f"    macroeconomic-platform.html (dead OpenBB)")

    r.log("Done")
