"""ops 3387 — find how WGB structures country links (the /country/slug/ pattern found 0 —
maybe it's /bond-historical/ or a different path, or JS-rendered). Inspect the actual link
patterns + the CDS table structure."""
import urllib.request, re
from ops_report import report
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
def get(url,t=25):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":UA})
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"
with report("3387_wgb_links") as r:
    txt=get("https://www.worldgovernmentbonds.com/sovereign-cds/")
    r.section("All internal href patterns on the CDS page")
    hrefs=re.findall(r'href=["\'](/[a-z0-9/-]+/)["\']',txt)
    from collections import Counter
    # bucket by first path segment
    seg=Counter(h.split("/")[1] for h in hrefs if len(h.split("/"))>1)
    r.log(f"  path segments: {dict(seg.most_common(15))}")
    r.section("Sample country-ish links")
    country_links=[h for h in set(hrefs) if any(k in h for k in ("country","bond","cds","germany","japan","united"))]
    for h in sorted(country_links)[:25]:
        r.log(f"  {h}")
    r.section("Does the CDS table have flag/country cells?")
    for m in list(re.finditer(r'(country|bond)/([a-z-]+)/',txt))[:20]:
        r.log(f"  {m.group(0)}")
