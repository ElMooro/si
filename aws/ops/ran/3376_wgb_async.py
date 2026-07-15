"""ops 3376 — WGB loads yield/CDS via JS async. Find the data endpoint (data-async-base-hint
or the JS fetch URL) that serves the actual numbers for Singapore/HK/Taiwan."""
import urllib.request, re
from ops_report import report

def get_text(url,t=20):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (compatible; justhodl/1.0)"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"

with report("3376_wgb_async") as r:
    txt = get_text("https://www.worldgovernmentbonds.com/country/singapore/")
    r.section("async data-base-hint / endpoint hints")
    for attr in ["data-async-base-hint","data-async-base","data-async-url","async-hint"]:
        for m in list(re.finditer(rf"{attr}\s*=\s*['\"]([^'\"]+)['\"]", txt))[:5]:
            r.log(f"  {attr}: {m.group(1)[:120]}")
    # any .json or api-ish url in the page
    r.section("candidate data URLs in page")
    urls=set(re.findall(r"['\"](/[a-zA-Z0-9/_.-]*(?:async|data|api|result|json)[a-zA-Z0-9/_.-]*)['\"]", txt, re.I))
    for u in list(urls)[:15]:
        r.log(f"  {u}")
    # look for the async loader script src + a global config
    r.section("script srcs + async config")
    for m in list(re.finditer(r"<script[^>]+src=['\"]([^'\"]+)['\"]", txt))[:12]:
        if any(k in m.group(1).lower() for k in ("async","data","result","app","main","bond")):
            r.log(f"  script: {m.group(1)}")
    # the toggle base
    for m in list(re.finditer(r"data-async-base-hint=['\"]([^'\"]+)['\"]", txt))[:3]:
        r.log(f"  base-hint: {m.group(1)}")
    # try the known WGB async pattern
    r.section("probe known WGB async endpoint patterns")
    for u in ["https://www.worldgovernmentbonds.com/async/country/singapore/",
              "https://www.worldgovernmentbonds.com/data/country/singapore/",
              "https://www.worldgovernmentbonds.com/bond-async/singapore/"]:
        t=get_text(u)
        r.log(f"  {u.split('.com')[1]}: {('OK '+t[:80]) if not t.startswith('__ERR__') else t[:60]}")
