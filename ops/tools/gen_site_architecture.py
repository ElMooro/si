#!/usr/bin/env python3
"""Regenerate directory.html, sitemap.xml, robots.txt from the repo's actual pages.
Rerunnable any time pages are added — the directory can never drift again."""
import glob, re, html
from datetime import date

EXCLUDE = {"directory.html", "google", "404.html"}
CATS = [
 ("🧭 Macro & Liquidity", ["regime","us-cycle","liquidity","inflection","eurodollar","tide","liq",
   "fed","fomc","treasury","auction","bonds","yield","rates","dollar","fx","boj","china","m2",
   "money","tga","rrp","macro","cycle","nowcast","activity","gdp","cpi","inflation","bls",
   "employment","claims","credit-imp"]),
 ("🇪🇺 Europe & Global", ["ecb","eu-","eurozone","europe","target2","ciss","bund","systemic"]),
 ("🐤 Crisis & Risk Radar", ["canar","crisis","stress","risk","tail","hedge","drawdown","vol-target",
   "unwind","capitulat","warning","watchtower","cro","escalation","fragil","contagion","bank"]),
 ("⚡ Equity Alpha & Signals", ["ignition","bottleneck","ma-reversion","signal","alpha","edge",
   "screener","scanner","setups","baggers","best-ideas","boom","breakout","momentum","squeeze",
   "insider","13f","13d","ark","political","lobby","congress","buyback","earnings","revision",
   "shelf","inclusion","ipo","gap","swing","calls","master","ranker","scorecard","dossier",
   "why-now","opportunity","frontrun","sniper","pre-pump","activist","whisper","catalyst"]),
 ("📊 Vol, Options & Sentiment", ["vol","vix","skew","gamma","opex","options","put-call",
   "sentiment","fear","greed","buzz","narrat","crowd","positioning","cftc","cot","breadth",
   "internals","mcclellan","thrust"]),
 ("🔬 Research & Tools", ["chart","analytics","backtest","analog","peer","factor","screen",
   "transcript","sec-","filing","fundamental","dcf","beneish","piotroski","altman","quality",
   "compare","primer","fedwatch","supply-chain","decomposition","research","workbench","ask",
   "brain","kb","knowledge","replay","time-machine","accuracy","calibration","skill","audit",
   "confluence","fade","ledger"]),
 ("💼 Portfolio & Execution", ["portfolio","pm-","position","sizing","book","allocator","trade",
   "execution","journal","pnl","performance","holdings","watchlist","alerts"]),
 ("₿ Crypto & Digital", ["crypto","btc","bitcoin","eth","coin","stablecoin","defi"]),
 ("🛰 Meta, Status & Misc", []),
]

def title_of(f):
    s = open(f, encoding="utf-8", errors="ignore").read(4000)
    m = re.search(r"<title>(.*?)</title>", s, re.S)
    t = html.unescape(m.group(1)).strip() if m else f
    return re.sub(r"\s*[|·–-]\s*JustHodl.*$", "", t, flags=re.I)[:64] or f

def cat_of(f, t):
    key = (f + " " + t).lower()
    for name, kws in CATS[:-1]:
        if any(k in key for k in kws):
            return name
    return CATS[-1][0]

pages = sorted(p for p in glob.glob("*.html") if p not in EXCLUDE)
groups = {name: [] for name, _ in CATS}
for f in pages:
    t = title_of(f)
    groups[cat_of(f, t)].append((f, t))

today = date.today().isoformat()
# sitemap
urls = ["https://justhodl.ai/"] + [f"https://justhodl.ai/{f}" for f in pages]
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

# directory.html
sec_html = ""
for name, _ in CATS:
    items = groups[name]
    if not items:
        continue
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
print(f"directory.html: {len(pages)} pages in {sum(1 for n,_ in CATS if groups[n])} groups · sitemap {len(urls)} urls · robots.txt ✓")
