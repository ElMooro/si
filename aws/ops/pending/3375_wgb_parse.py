"""ops 3375 — extract the REAL parse pattern from worldgovernmentbonds.com Singapore page:
find where the 10Y yield and CDS actually live in the HTML so we build a correct scraper."""
import urllib.request, re
from ops_report import report

def get_text(url,t=20):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (compatible; justhodl/1.0)"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"

with report("3375_wgb_parse") as r:
    txt = get_text("https://www.worldgovernmentbonds.com/country/singapore/")
    r.section("Singapore page — locate yield + CDS in HTML")
    # find all "X.XX%" occurrences with ~40 chars of context
    r.log("-- percentage values with context --")
    for m in list(re.finditer(r".{35}(\d{1,2}\.\d{2,3})\s*%", txt))[:10]:
        ctx=re.sub(r"<[^>]+>"," ",m.group(0)); ctx=re.sub(r"\s+"," ",ctx).strip()
        r.log(f"   {ctx}")
    r.log("-- lines mentioning 'CDS' --")
    for m in list(re.finditer(r".{10}CDS.{60}", txt, re.I))[:6]:
        ctx=re.sub(r"<[^>]+>"," ",m.group(0)); ctx=re.sub(r"\s+"," ",ctx).strip()
        r.log(f"   {ctx}")
    r.log("-- lines mentioning '10 year' / '10Y' --")
    for m in list(re.finditer(r".{5}10\s*[Yy]ear.{70}", txt))[:4]:
        ctx=re.sub(r"<[^>]+>"," ",m.group(0)); ctx=re.sub(r"\s+"," ",ctx).strip()
        r.log(f"   {ctx}")
    # rating
    for m in list(re.finditer(r".{5}[Rr]ating.{50}", txt))[:3]:
        ctx=re.sub(r"<[^>]+>"," ",m.group(0)); ctx=re.sub(r"\s+"," ",ctx).strip()
        r.log(f"   RATING: {ctx}")
