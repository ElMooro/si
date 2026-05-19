"""
One-shot: add every shipped product page that is missing from the
landing page (index.html) as a .tool card, grouped into three new
sections, inserted just before the RAW DATA APIS block.

Idempotent: if a page is already linked anywhere in index.html it is
skipped, so re-running never duplicates a card.
"""
import re

INDEX = "index.html"

# (href, icon, name, description) -- grouped
SECTIONS = [
    ("FIRM RISK DESKS",
     "Firm-wide risk capstones &middot; exposure, factor, stress &amp; "
     "attribution", "#ff5577", [
        ("/factor-risk.html", "\u25a4", "FACTOR RISK MODEL",
         "Multi-factor decomposition of the book &middot; market, size, "
         "value, momentum &amp; quality betas &middot; contribution to "
         "variance &middot; daily"),
        ("/firm-book.html", "\u25a3", "FIRM BOOK",
         "Consolidated firm-wide position book &middot; net &amp; gross "
         "exposure, sector &amp; asset weights &middot; the single source "
         "of truth for what the firm holds"),
        ("/firm-stress.html", "\u25c8", "STRESS DESK",
         "15-scenario firm P&amp;L stress test &middot; historical &amp; "
         "hypothetical shocks &middot; worst-case loss per scenario "
         "&middot; daily"),
        ("/liquidity-capacity.html", "\u25d2", "LIQUIDITY MONITOR",
         "Book liquidation capacity &middot; days-to-liquidate per "
         "position &middot; portfolio-level liquidity score &middot; "
         "daily"),
        ("/merger-arb-risk.html", "\u26a0", "MERGER-ARB BOOK RISK",
         "Deal-break stress on the merger-arb book &middot; spread risk, "
         "financing &amp; regulatory break scenarios &middot; daily"),
        ("/pnl-attribution.html", "\u25f0", "P&amp;L ATTRIBUTION",
         "Return decomposition across desks, factors &amp; positions "
         "&middot; what actually drove the book's P&amp;L &middot; daily"),
        ("/risk-monitor.html", "\u25c9", "RISK MONITOR",
         "Live firm risk monitor &middot; VaR, exposure &amp; limit "
         "usage &middot; continuous"),
        ("/risk-radar.html", "\u25ce", "RISK RADAR",
         "Cross-engine risk synthesis radar &middot; early-warning read "
         "across every desk"),
     ]),
    ("STRATEGY DESKS &amp; SCANNERS",
     "Live trading desks &amp; universe scanners", "#00d4ff", [
        ("/chart-patterns.html", "\u25b3", "CHART PATTERN SCANNER",
         "200-DMA cross-ups &amp; cross-downs + double tops &amp; double "
         "bottoms across the S&amp;P 500 &middot; each with its chart "
         "&middot; daily"),
        ("/merger-arb.html", "\u2702", "MERGER-ARBITRAGE DESK",
         "Live M&amp;A deal screen &middot; annualised spread, completion "
         "odds &amp; expected return per deal"),
        ("/pairs-arb.html", "\u2261", "PAIRS DESK",
         "Statistical-arbitrage pairs book &middot; cointegration, "
         "z-score entries &amp; live pair P&amp;L"),
        ("/pairs-scanner.html", "\u229c", "PAIRS SCANNER",
         "Scans the universe for cointegrated, mean-reverting pairs "
         "&middot; ranked by edge"),
        ("/spinoff-desk.html", "\u2702", "SPIN-OFF DESK",
         "Corporate spin-off tracker &middot; post-spin opportunity "
         "screen &amp; forced-selling setups"),
        ("/index-recon.html", "\u21c4", "INDEX RECONSTITUTION DESK",
         "Index add / delete forecasts &middot; positioning ahead of the "
         "rebalance flow"),
        ("/dividend-growth.html", "\u25b2", "DIVIDEND COMPOUNDERS",
         "Dividend-growth screen &middot; payout durability, growth "
         "streak &amp; yield-on-cost"),
        ("/conviction.html", "\u2605", "CONVICTION ENGINE",
         "Multi-signal conviction scoring across the equity universe "
         "&middot; the highest-conviction names"),
        ("/options-scanner.html", "\u25c9", "OPTIONS FLOW SCANNER",
         "Unusual options activity &middot; large prints, sweeps &amp; "
         "implied directional bets"),
        ("/signal-scorecard.html", "\u2713", "SIGNAL SCORECARD",
         "Track record of every signal &middot; hit rate, edge &amp; "
         "calibration over time"),
        ("/portfolio.html", "\u25d3", "PORTFOLIO",
         "Personal portfolio book &middot; positions, P&amp;L &amp; risk"),
     ]),
    ("MACRO, INTEL &amp; DATA ENGINES",
     "Macro regime, narrative intelligence &amp; data feeds", "#a78bfa", [
        ("/cross-asset.html", "\u25c8", "CROSS-ASSET REGIME",
         "Cross-asset regime classifier &middot; equities, rates, "
         "credit, FX &amp; commodities in one read"),
        ("/analogs.html", "\u29d6", "HISTORICAL ANALOGS",
         "Finds the closest historical market regimes to today &middot; "
         "and what happened next"),
        ("/yen-carry.html", "\u00a5", "YEN CARRY &amp; BOJ LIQUIDITY",
         "Yen carry-trade stress monitor &middot; BOJ liquidity, USDJPY "
         "&amp; unwind risk"),
        ("/cot-extremes.html", "\u25ed", "COT EXTREMES",
         "CFTC Commitments-of-Traders positioning extremes &middot; "
         "crowded longs &amp; shorts"),
        ("/activity-nowcast.html", "\u25f7", "ACTIVITY NOWCAST",
         "Real-time US economic activity nowcast &middot; GDP-tracking "
         "from high-frequency data"),
        ("/consumer-pulse.html", "\u25d4", "CONSUMER &amp; LABOUR PULSE",
         "Consumer spending &amp; labour-market health monitor"),
        ("/dealer-survey.html", "\u25a6", "NY FED DEALER SURVEY",
         "Primary-dealer survey signals &middot; positioning &amp; rate "
         "expectations"),
        ("/narrative.html", "\u25a9", "NARRATIVE DENSITY",
         "Tracks which market narratives are intensifying across the "
         "news flow"),
        ("/gdelt.html", "\u25c9", "GDELT SENTIMENT",
         "Global news-event sentiment from the GDELT feed"),
        ("/fleet-health.html", "\u2699", "FLEET HEALTH",
         "System-wide engine health monitor &middot; data freshness "
         "&amp; pipeline status"),
        ("/benzinga.html", "\u25c8", "BENZINGA",
         "Benzinga news &amp; analyst-action feed"),
        ("/eia.html", "\u25b0", "EIA ENERGY",
         "US Energy Information Administration data &middot; "
         "inventories, production &amp; prices"),
        ("/nasdaq-datalink.html", "\u2207", "NASDAQ DATA LINK",
         "NASDAQ Data Link economic &amp; financial datasets"),
     ]),
]

html = open(INDEX, encoding="utf-8").read()

blocks = []
added, skipped = [], []
for title, subtitle, accent, cards in SECTIONS:
    rows = []
    for i, (href, icon, name, desc) in enumerate(cards):
        page = href.strip("/")
        if page in html:               # already linked anywhere -> skip
            skipped.append(page)
            continue
        added.append(page)
        if i == 0 or len(rows) == 0:
            opena = (' style="border-left:3px solid %s"' % accent)
            icona = (' style="color:%s"' % accent)
        else:
            opena, icona = "", ""
        rows.append(
            '          <a class="tool" href="%s"%s>\n'
            '            <div class="tool-icon"%s>%s</div>\n'
            '            <div class="tool-name">%s</div>\n'
            '            <div class="tool-desc">%s</div>\n'
            '          </a>' % (href, opena, icona, icon, name, desc))
    if not rows:
        continue
    blocks.append(
        '      <div style="margin-top:28px;padding-top:18px;'
        'border-top:1px solid var(--border)">\n'
        '        <div class="card-title" style="margin-bottom:6px">%s'
        '</div>\n'
        '        <div class="card-subtitle" style="margin-bottom:14px">%s'
        '</div>\n'
        '        <div class="tools">\n%s\n        </div>\n'
        '      </div>\n\n' % (title, subtitle, "\n".join(rows)))

if not blocks:
    print("Nothing to add -- every product page is already linked.")
    raise SystemExit(0)

anchor = ('      <div style="margin-top:28px;padding-top:18px;'
          'border-top:1px solid var(--border)">\n'
          '        <div class="card-title" style="margin-bottom:6px">'
          'RAW DATA APIS</div>')
if anchor not in html:
    raise SystemExit("ERROR: RAW DATA APIS anchor not found -- aborting.")

html = html.replace(anchor, "".join(blocks) + anchor, 1)
open(INDEX, "w", encoding="utf-8").write(html)

print("Added %d product cards across %d new sections."
      % (len(added), len(blocks)))
print("Added:", ", ".join(added))
if skipped:
    print("Skipped (already linked):", ", ".join(skipped))
