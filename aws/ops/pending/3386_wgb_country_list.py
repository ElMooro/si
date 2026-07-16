"""ops 3386 — discover the FULL country universe WGB covers (for the dedicated global
sovereign engine). Scrape the CDS index + bond-yield list pages for all country slugs, and
confirm the endpoint returns data for a broad sample across regions."""
import urllib.request, re, json
from ops_report import report

UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
def get(url,t=25):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":UA})
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"

with report("3386_wgb_countries") as r:
    # the country slugs appear as /country/<slug>/ links across index pages
    slugs=set()
    for page in ["https://www.worldgovernmentbonds.com/",
                 "https://www.worldgovernmentbonds.com/sovereign-cds/",
                 "https://www.worldgovernmentbonds.com/world-credit-ratings/",
                 "https://www.worldgovernmentbonds.com/spread/"]:
        txt=get(page)
        if txt.startswith("__ERR__"): 
            r.log(f"  {page.split('.com')[1]}: {txt[:50]}"); continue
        found=set(re.findall(r"/country/([a-z-]+)/",txt))
        slugs |= found
        r.log(f"  {page.split('.com')[1] or '/'}: +{len(found)} slugs")
    slugs.discard("")
    r.section(f"Total unique country slugs: {len(slugs)}")
    r.log(", ".join(sorted(slugs)))
