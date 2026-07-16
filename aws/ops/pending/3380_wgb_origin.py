"""ops 3380 — reverse-engineer WGB's data pipeline. (1) Extract the exact admin-ajax action
+ params from myLayout.js that loads jsGlobalResult. (2) Hunt in their JS/HTML for the
UPSTREAM source they pull from (attribution, data-provider hints, cdn, source URLs) so we
can go to the origin directly."""
import urllib.request, re
from ops_report import report

def get_text(url,t=25,hdr=None):
    try:
        h={"User-Agent":"Mozilla/5.0 (compatible; justhodl/1.0)"}
        if hdr: h.update(hdr)
        req=urllib.request.Request(url,headers=h)
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"

with report("3380_wgb_origin") as r:
    js = get_text("https://www.worldgovernmentbonds.com/wp-content/themes/acgpt-child/js/myLayout.js?ver=2.2.11")
    # also grab the other two scripts
    toc = get_text("https://www.worldgovernmentbonds.com/wp-content/themes/acgpt-child/js/toc.js")
    alljs = js + "\n" + toc

    r.section("1. admin-ajax action + params for jsGlobalResult")
    # find how jsGlobalResult is populated
    for pat,label in [(r"jsGlobalResult","jsGlobalResult refs"),
                      (r"action['\"]?\s*[:=]\s*['\"]([a-zA-Z_]+)","action"),
                      (r"admin-ajax[^'\"]*","admin-ajax url"),
                      (r"ajaxurl\s*[=:]\s*['\"]([^'\"]+)","ajaxurl"),
                      (r"data\s*:\s*\{([^}]{0,120})","POST data"),
                      (r"\$\.(?:get|post|ajax)\(([^)]{0,140})","jquery ajax")]:
        hits=re.findall(pat,alljs,re.I)
        if hits:
            uniq=list(dict.fromkeys([str(h)[:100] for h in hits]))[:5]
            r.log(f"  {label}: {uniq}")

    r.section("2. UPSTREAM source hints (where WGB gets ITS data)")
    home = get_text("https://www.worldgovernmentbonds.com/")
    blob = alljs + home
    # look for external data providers / attribution
    for pat,label in [(r"(investing\.com|tradingeconomics|marketwatch|cbonds|refinitiv|bloomberg| investing|boerse|stooq|finra|ice\.com)","provider names"),
                      (r"source[:\s]{1,3}([A-Za-z .]{4,30})","'source:' text"),
                      (r"https?://(?!www\.worldgovernmentbonds)([a-z0-9.-]+\.(?:com|org|io|net))","external domains"),
                      (r"data.provider['\"]?\s*[:=]\s*['\"]([^'\"]+)","data-provider attr")]:
        hits=re.findall(pat,blob,re.I)
        if hits:
            from collections import Counter
            c=Counter(str(h).lower() for h in hits)
            r.log(f"  {label}: {dict(c.most_common(8))}")

    r.section("3. try the admin-ajax with likely actions")
    for action in ["get_async_data","async_data","get_country_data","wgb_async","get_bond","country_data"]:
        u=f"https://www.worldgovernmentbonds.com/wp-admin/admin-ajax.php?action={action}&country=singapore&slug=singapore"
        t=get_text(u)
        status = "EMPTY/0" if t.strip() in ("0","") else (t[:90] if not t.startswith("__ERR__") else t[:50])
        r.log(f"  action={action}: {status}")
