#!/usr/bin/env python3
"""Regenerate directory.html, sitemap.xml, robots.txt, and nav-manifest.json from the
repo's actual pages. Rerunnable any time pages are added — nothing can drift again.

v2: 8-category taxonomy (was 9 — Europe & Global folded into Macro & Liquidity).
Categorization is keyword-blob matching for the ~220 unambiguous pages, plus an
explicit filename OVERRIDES dict for every page that was previously falling into
"Misc" (or worse — silently mis-bucketed via an accidental filename keyword match,
e.g. catalysts.html's dead redirect stub was captured by "catalyst" into Equity
Signals). Pages whose <title> is literally "Redirecting" (dead stubs left over from
consolidations) are hard-excluded from every output — they are not real destinations.
"""
import glob, re, html, json
from datetime import date

EXCLUDE_FILES = {"directory.html", "google", "404.html"}
EXCLUDE_TITLES = {"redirecting"}   # dead one-line "Moved here" stubs — never list these

CATS = [
 ("🧭 Macro & Liquidity", ["regime","us-cycle","liquidity","inflection","eurodollar","tide","liq",
   "fed","fomc","treasury","auction","bonds","yield","rates","dollar","fx","boj","china","m2",
   "money","tga","rrp","macro","cycle","nowcast","activity","gdp","cpi","inflation","bls",
   "employment","claims","credit-imp","ecb","eu-","eurozone","europe","target2","ciss","bund",
   "systemic"]),
 ("🐤 Risk & Crisis", ["canar","crisis","stress","risk","tail","hedge","drawdown","vol-target",
   "unwind","capitulat","warning","watchtower","cro","escalation","fragil","contagion","bank"]),
 ("⚡ Equity Signals", ["ignition","bottleneck","ma-reversion","signal","alpha","edge",
   "screener","scanner","setups","baggers","best-ideas","boom","breakout","momentum","squeeze",
   "insider","13f","13d","ark","political","lobby","congress","buyback","earnings","revision",
   "shelf","inclusion","ipo","gap","swing","calls","master","ranker","scorecard","dossier",
   "why-now","opportunity","frontrun","sniper","pre-pump","activist","whisper","catalyst"]),
 ("📊 Vol & Sentiment", ["vol","vix","skew","gamma","opex","options","put-call",
   "sentiment","fear","greed","buzz","narrat","crowd","positioning","cftc","cot","breadth",
   "internals","mcclellan","thrust"]),
 ("🔬 Research & Tools", ["chart","analytics","backtest","analog","peer","factor","screen",
   "transcript","sec-","filing","fundamental","dcf","beneish","piotroski","altman","quality",
   "compare","primer","fedwatch","supply-chain","decomposition","research","workbench","ask",
   "brain","kb","knowledge","replay","time-machine","accuracy","calibration","skill","audit",
   "confluence","fade","ledger"]),
 ("💼 Portfolio & Execution", ["portfolio","pm-","position","sizing","book","allocator","trade",
   "execution","journal","pnl","performance","holdings","watchlist","alerts"]),
 ("₿ Crypto & Digital", ["crypto","btc","bitcoin","eth-","coin","stablecoin","defi"]),
 ("🛰 System & Meta", []),
]
CATNAMES = [n for n, _ in CATS]

OVERRIDES = {
 "capital-flow-radar.html":"🧭 Macro & Liquidity","capital-flow.html":"🧭 Macro & Liquidity",
 "capital-inflows.html":"🧭 Macro & Liquidity","carry-surface.html":"🧭 Macro & Liquidity",
 "carry.html":"🧭 Macro & Liquidity","cb-injection.html":"🧭 Macro & Liquidity",
 "compass.html":"🧭 Macro & Liquidity","construction-housing.html":"🧭 Macro & Liquidity",
 "consumer-pulse.html":"🧭 Macro & Liquidity","econ-calendar.html":"🧭 Macro & Liquidity",
 "eia.html":"🧭 Macro & Liquidity","euro-fragmentation.html":"🧭 Macro & Liquidity",
 "flows.html":"🧭 Macro & Liquidity","fred.html":"🧭 Macro & Liquidity",
 "funding-plumbing.html":"🧭 Macro & Liquidity","horizons-gsi.html":"🧭 Macro & Liquidity",
 "horizons.html":"🧭 Macro & Liquidity","sovereign.html":"🧭 Macro & Liquidity",
 "sector-emergence.html":"🧭 Macro & Liquidity",
 "cds-monitor.html":"🐤 Risk & Crisis","correlation.html":"🐤 Risk & Crisis",
 "ofr.html":"🐤 Risk & Crisis","episode-compass.html":"🐤 Risk & Crisis",
 "accumulation.html":"⚡ Equity Signals","ai-rerating.html":"⚡ Equity Signals",
 "analyst-actions.html":"⚡ Equity Signals","ath.html":"⚡ Equity Signals",
 "backlog.html":"⚡ Equity Signals","capital-return.html":"⚡ Equity Signals",
 "catch-up.html":"⚡ Equity Signals","conviction.html":"⚡ Equity Signals",
 "deep-value.html":"⚡ Equity Signals","dislocations.html":"⚡ Equity Signals",
 "dividend-growth.html":"⚡ Equity Signals","equity-chokepoint.html":"⚡ Equity Signals",
 "equity-cyclical-bagger.html":"⚡ Equity Signals","eps-velocity.html":"⚡ Equity Signals",
 "eva.html":"⚡ Equity Signals","flow-lookthrough.html":"⚡ Equity Signals",
 "forward-orders.html":"⚡ Equity Signals","gf-value.html":"⚡ Equity Signals",
 "heatmap.html":"⚡ Equity Signals","index-recon.html":"⚡ Equity Signals",
 "investor.html":"⚡ Equity Signals","ma200-radar.html":"⚡ Equity Signals",
 "magic-formula.html":"⚡ Equity Signals","merger-arb.html":"⚡ Equity Signals",
 "metals-miners.html":"⚡ Equity Signals","ml-predictions.html":"⚡ Equity Signals",
 "opportunities.html":"⚡ Equity Signals","pairs-arb.html":"⚡ Equity Signals",
 "patent-velocity.html":"⚡ Equity Signals","predictability.html":"⚡ Equity Signals",
 "resilience.html":"⚡ Equity Signals","rotation-chains.html":"⚡ Equity Signals",
 "rotation-radar.html":"⚡ Equity Signals","russell-recon.html":"⚡ Equity Signals",
 "scarcity-radar.html":"⚡ Equity Signals","sector-tilt.html":"⚡ Equity Signals",
 "sectors.html":"⚡ Equity Signals","short-pressure.html":"⚡ Equity Signals",
 "smart-beta.html":"⚡ Equity Signals","spinoff-desk.html":"⚡ Equity Signals",
 "tape-reader.html":"⚡ Equity Signals","targets.html":"⚡ Equity Signals",
 "theme-tiers.html":"⚡ Equity Signals","track-public.html":"⚡ Equity Signals",
 "trend-engine.html":"⚡ Equity Signals","valuations.html":"⚡ Equity Signals",
 "implied-prob.html":"📊 Vol & Sentiment","digest-trends.html":"📊 Vol & Sentiment",
 "ai_predictions.html":"🔬 Research & Tools","apex.html":"🔬 Research & Tools",
 "benzinga.html":"🔬 Research & Tools","brief.html":"🔬 Research & Tools",
 "digest-archive.html":"🔬 Research & Tools","event-study.html":"🔬 Research & Tools",
 "future-intel.html":"🔬 Research & Tools","glossary.html":"🔬 Research & Tools",
 "kill-theses.html":"🔬 Research & Tools","live-pulse.html":"🔬 Research & Tools",
 "live.html":"🔬 Research & Tools","nasdaq-datalink.html":"🔬 Research & Tools",
 "news.html":"🔬 Research & Tools","read.html":"🔬 Research & Tools",
 "strategist.html":"🔬 Research & Tools","today.html":"🔬 Research & Tools",
 "ticker.html":"🔬 Research & Tools",
 "tax-plan.html":"💼 Portfolio & Execution","wealth-plan.html":"💼 Portfolio & Execution",
 "weights.html":"💼 Portfolio & Execution",
 "altseason.html":"₿ Crypto & Digital","dex.html":"₿ Crypto & Digital",
 "crypto-liquidity.html":"₿ Crypto & Digital","crypto-risk.html":"₿ Crypto & Digital",
 "crypto-opportunities.html":"₿ Crypto & Digital","crypto-narratives.html":"₿ Crypto & Digital",
 "crypto-confluence.html":"₿ Crypto & Digital","crypto-emergence.html":"₿ Crypto & Digital",
 "about.html":"🛰 System & Meta","api-docs.html":"🛰 System & Meta",
 "contact.html":"🛰 System & Meta","dep-graph.html":"🛰 System & Meta",
 "desk-v2.html":"🛰 System & Meta","desk.html":"🛰 System & Meta",
 "download.html":"🛰 System & Meta","downloads.html":"🛰 System & Meta",
 "engine.html":"🛰 System & Meta","engines.html":"🛰 System & Meta",
 "errors.html":"🛰 System & Meta","feedback.html":"🛰 System & Meta",
 "fleet-health.html":"🛰 System & Meta","health.html":"🛰 System & Meta",
 "notifications.html":"🛰 System & Meta","observability.html":"🛰 System & Meta",
 "pricing.html":"🛰 System & Meta","privacy.html":"🛰 System & Meta",
 "settings.html":"🛰 System & Meta","status.html":"🛰 System & Meta",
 "system-health.html":"🛰 System & Meta","system.html":"🛰 System & Meta",
 "terms.html":"🛰 System & Meta","uptime.html":"🛰 System & Meta",
 "cockpit.html":"🛰 System & Meta",
}


def title_of(f):
    s = open(f, encoding="utf-8", errors="ignore").read(4000)
    m = re.search(r"<title>(.*?)</title>", s, re.S)
    t = html.unescape(m.group(1)).strip() if m else f
    return re.sub(r"\s*[|·–-]\s*JustHodl.*$", "", t, flags=re.I)[:64] or f


def cat_of(f, t):
    if f in OVERRIDES:
        return OVERRIDES[f]
    key = (f + " " + t).lower()
    for name, kws in CATS[:-1]:
        if any(k in key for k in kws):
            return name
    return CATS[-1][0]


all_files = sorted(p for p in glob.glob("*.html") if p not in EXCLUDE_FILES)
pages, dead = [], []
for f in all_files:
    t = title_of(f)
    if t.strip().lower() in EXCLUDE_TITLES:
        dead.append(f)
    else:
        pages.append((f, t))

groups = {name: [] for name in CATNAMES}
for f, t in pages:
    groups[cat_of(f, t)].append((f, t))

today = date.today().isoformat()

urls = ["https://justhodl.ai/"] + [f"https://justhodl.ai/{f}" for f, _ in pages]
sm = ['<?xml version="1.0" encoding="UTF-8"?>',
      '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
for u in urls:
    sm.append(f"  <url><loc>{u}</loc><lastmod>{today}</lastmod>"
              f"<changefreq>daily</changefreq></url>")
sm.append("</urlset>")
open("sitemap.xml", "w").write("\n".join(sm))
open("robots.txt", "w").write(
    "User-agent: *\nAllow: /\nDisallow: /archive/\nDisallow: /data/_\n\n"
    "Sitemap: https://justhodl.ai/sitemap.xml\n")

manifest = {
    "generated_at": today,
    "n_pages": len(pages),
    "categories": [
        {"name": name, "count": len(groups[name]),
         "pages": [{"href": "/" + f, "title": t} for f, t in sorted(groups[name], key=lambda x: x[1].lower())]}
        for name in CATNAMES if groups[name]
    ],
}
open("nav-manifest.json", "w").write(json.dumps(manifest, ensure_ascii=False, separators=(",", ":")))

sec_html = ""
for name in CATNAMES:
    items = groups[name]
    if not items:
        continue
    items = sorted(items, key=lambda x: x[1].lower())
    rows = "".join(
        f'<a class="it" href="/{f}" data-s="{html.escape((f+" "+t).lower())}">'
        f'<b>{html.escape(t)}</b><span>/{f}</span></a>' for f, t in items)
    sec_html += (f'<div class="cat"><h2>{name} <em>{len(items)}</em></h2>'
                 f'<div class="grid">{rows}</div></div>')
page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Directory — every page · JustHodl.AI</title>
<style>:root{{--bg:#0a0e14;--p:#10151f;--l:#1d2636;--fg:#e8edf5;--fg2:#a8b3c7;--fg3:#5d6b82;--c:#22d3ee;--mono:'IBM Plex Mono',monospace}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--fg);font-family:Inter,system-ui,sans-serif}}
header{{padding:14px 22px;border-bottom:1px solid var(--l);display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;background:rgba(10,14,20,.95);z-index:9}}
.brand{{font-family:var(--mono);font-weight:800}}.brand i{{display:inline-block;width:8px;height:8px;background:var(--c);border-radius:50%;margin-right:7px}}
nav a{{color:var(--fg2);font-size:12.5px;margin-left:14px;text-decoration:none}}
.wrap{{max-width:1200px;margin:0 auto;padding:22px}}
h1{{font-size:24px;margin:8px 0 4px}}.sub{{color:var(--fg2);font-size:13px;margin-bottom:14px}}
#q{{width:100%;max-width:480px;background:var(--p);border:1px solid var(--l);border-radius:10px;color:var(--fg);padding:11px 14px;font-family:var(--mono);font-size:13px;margin-bottom:8px}}
.cat h2{{font-size:15px;margin:26px 0 10px;font-family:var(--mono)}}.cat h2 em{{color:var(--fg3);font-style:normal;font-size:11px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(255px,1fr));gap:8px}}
.it{{background:var(--p);border:1px solid var(--l);border-radius:10px;padding:10px 12px;text-decoration:none;display:block}}
.it:hover{{border-color:var(--c)}}.it b{{color:var(--fg);font-size:12.5px;display:block;font-weight:600}}
.it span{{color:var(--fg3);font-family:var(--mono);font-size:10px}}
footer{{color:var(--fg3);font-size:11px;padding:26px 22px;border-top:1px solid var(--l);margin-top:30px}}</style></head><body>
<header><div class="brand"><i></i>JustHodl<span style="color:var(--fg3)">.AI</span></div>
<nav><a href="/">Desk</a><a href="/methodology.html">Methodology</a><a href="/skill.html">Skill</a><a href="/regime.html">Regime</a></nav></header>
<div class="wrap"><h1>📁 Directory — all {len(pages)} pages</h1>
<div class="sub">Auto-generated from the deployed system ({today}). Every tool, every radar, every lab — grouped by workflow. Type to filter.</div>
<input id="q" placeholder="filter… e.g. liquidity, vol, ECB, screener">
{sec_html}</div>
<footer>Generated by ops/tools/gen_site_architecture.py · sitemap at /sitemap.xml · Research, not investment advice.</footer>
<script>document.getElementById('q').addEventListener('input',e=>{{const v=e.target.value.toLowerCase();
document.querySelectorAll('.it').forEach(x=>x.style.display=x.dataset.s.includes(v)?'':'none');
document.querySelectorAll('.cat').forEach(c=>c.style.display=[...c.querySelectorAll('.it')].some(x=>x.style.display!=='none')?'':'none');}});</script>
</body></html>"""
open("directory.html", "w").write(page)

print(f"directory.html: {len(pages)} pages in {sum(1 for n in CATNAMES if groups[n])} groups")
print(f"nav-manifest.json: {len(pages)} pages · dead stubs excluded: {dead}")
for name in CATNAMES:
    print(f"  {name}: {len(groups[name])}")
print(f"sitemap {len(urls)} urls · robots.txt OK")
