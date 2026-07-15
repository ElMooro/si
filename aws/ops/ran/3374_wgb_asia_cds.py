"""ops 3374 — verify worldgovernmentbonds.com has REAL current CDS + 10Y yield for
Singapore, Hong Kong, Taiwan (you already scrape this site for sovereign CDS). This is the
paid/reliable source the fleet already uses. Confirm data exists before wiring in."""
import urllib.request, re
from ops_report import report

def get_text(url, t=20):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (compatible; justhodl/1.0)"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {type(e).__name__} {str(e)[:50]}"

with report("3374_wgb_asia_cds") as r:
    r.section("Country pages exist? (10Y yield + CDS)")
    slugs = {"singapore":"singapore","hong-kong":"hong-kong","taiwan":"taiwan",
             "south-korea":"south-korea"}
    for name,slug in slugs.items():
        txt = get_text(f"https://www.worldgovernmentbonds.com/country/{slug}/")
        if txt.startswith("__ERR__"):
            r.log(f"  {name}: {txt}"); continue
        # 10Y yield pattern
        y = re.search(r"10\s*years?.*?(\d{1,2}\.\d{2,3})\s*%", txt, re.I|re.S)
        # CDS pattern
        cds = re.search(r"CDS.*?(\d{1,4}(?:\.\d+)?)", txt, re.I|re.S)
        rating = re.search(r"Rating\s*[:\-]?\s*([A-D][A-Za-z+\-0-9 ]{1,6})", txt)
        r.log(f"  {name}: page_len={len(txt)} | 10Y≈{y.group(1)+'%' if y else '?'} | CDS≈{cds.group(1) if cds else '?'} | rating={rating.group(1).strip() if rating else '?'}")

    r.section("Sovereign-CDS index page — does it list the Asian ones?")
    base = get_text("https://www.worldgovernmentbonds.com/sovereign-cds/")
    if not base.startswith("__ERR__"):
        for name in ["Singapore","Hong Kong","Taiwan","South Korea"]:
            m = re.search(rf"{re.escape(name)}.*?(\d{{1,4}}(?:\.\d+)?)", base, re.I|re.S)
            r.log(f"  {name} in CDS index: {'CDS≈'+m.group(1) if m else 'not found'}")
    else:
        r.log(f"  CDS index: {base}")
