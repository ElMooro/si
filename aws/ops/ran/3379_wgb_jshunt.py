"""ops 3379 — find WGB's real data source. The page uses data-async-toggle attributes and
loads numbers via JS. Read myLayout.js (their main script) to find the fetch/ajax URL that
serves the actual yield/CDS values, and inspect the data-async attributes for the endpoint."""
import urllib.request, re
from ops_report import report

def get_text(url,t=25):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (compatible; justhodl/1.0)"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"

with report("3379_wgb_jshunt") as r:
    # 1. Read myLayout.js — the async loader
    r.section("myLayout.js — find fetch/ajax URLs")
    js = get_text("https://www.worldgovernmentbonds.com/wp-content/themes/acgpt-child/js/myLayout.js?ver=2.2.11")
    if not js.startswith("__ERR__"):
        r.log(f"  js length: {len(js)}")
        # find URLs, ajax, fetch, admin-ajax
        for pat,label in [(r"admin-ajax\.php","admin-ajax"),(r"fetch\(['\"]([^'\"]+)","fetch"),
                          (r"\.get\(['\"]([^'\"]+)",".get"),(r"url\s*:\s*['\"]([^'\"]+)","url:"),
                          (r"action\s*[=:]\s*['\"]([a-z_]+)","action"),(r"async[A-Za-z]*\s*[=:]\s*['\"]([^'\"]+)","async")]:
            hits=re.findall(pat,js,re.I)
            if hits: r.log(f"  {label}: {list(set(hits))[:6]}")
        # any wp-json or rest routes
        for m in re.findall(r"['\"](/wp-json/[^'\"]+|/wp-admin/admin-ajax\.php[^'\"]*)['\"]",js):
            r.log(f"  route: {m}")
    else:
        r.log(f"  myLayout.js: {js}")

    # 2. Inspect the data-async attributes fully on the country page
    r.section("data-async-* attributes on Singapore page")
    txt=get_text("https://www.worldgovernmentbonds.com/country/singapore/")
    for m in list(re.finditer(r"data-async-[a-z-]+=['\"][^'\"]{3,80}['\"]",txt))[:12]:
        r.log(f"  {m.group(0)[:110]}")
    # the base-hint often IS the endpoint
    for m in re.findall(r"data-async-base-hint=['\"]([^'\"]+)['\"]",txt)[:3]:
        r.log(f"  BASE-HINT: {m}")

    # 3. WP admin-ajax is the usual WordPress data endpoint — probe it
    r.section("probe admin-ajax + common WP data routes")
    for u in ["https://www.worldgovernmentbonds.com/wp-admin/admin-ajax.php?action=get_bond_data&country=singapore",
              "https://www.worldgovernmentbonds.com/wp-json/",
              "https://www.worldgovernmentbonds.com/world-government-bonds-async/"]:
        t=get_text(u)
        r.log(f"  {u.split('.com')[1][:55]}: {(t[:100]) if not t.startswith('__ERR__') else t[:55]}")
