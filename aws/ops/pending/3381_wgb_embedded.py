"""ops 3381 — two-pronged: (A) is jsGlobalResult embedded as a JSON blob in the page's own
HTML/inline <script>? (that's the cleanest pull). (B) probe investing.com's data endpoint
(WGB's upstream) for Singapore/HK/Taiwan 10Y."""
import urllib.request, re, json
from ops_report import report

def get_text(url,t=25,hdr=None):
    try:
        h={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
        if hdr: h.update(hdr)
        req=urllib.request.Request(url,headers=h)
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"

with report("3381_wgb_embedded") as r:
    r.section("A. Is jsGlobalResult / data embedded inline in the page?")
    txt=get_text("https://www.worldgovernmentbonds.com/country/singapore/")
    # search inline scripts for the data object
    for pat,label in [(r"jsGlobalResult\s*=\s*(\{.{0,200})","jsGlobalResult assign"),
                      (r"var\s+jsGlobal[A-Za-z]*\s*=","jsGlobal var decl"),
                      (r"bond10y['\"]?\s*[:=]\s*['\"]?([\d.]+)","bond10y value"),
                      (r"lastCds['\"]?\s*[:=]\s*['\"]?([\d.]+)","lastCds value"),
                      (r"(\{[^{}]*bond10y[^{}]*\})","obj with bond10y")]:
        hits=re.findall(pat,txt,re.I|re.S)
        if hits: r.log(f"  {label}: {[str(h)[:120] for h in hits[:2]]}")
    # count inline <script> blocks and scan the biggest for numbers
    scripts=re.findall(r"<script[^>]*>(.*?)</script>",txt,re.S)
    r.log(f"  inline scripts: {len(scripts)}; sizes: {sorted([len(s) for s in scripts],reverse=True)[:5]}")
    for s in scripts:
        if "bond10y" in s or "jsGlobal" in s or "lastCds" in s:
            snippet=re.sub(r"\s+"," ",s)[:300]
            r.log(f"  DATA SCRIPT: {snippet}")

    r.section("B. investing.com upstream — Singapore/HK/Taiwan 10Y")
    # investing.com country bond pages
    for name,slug in [("Singapore","singapore"),("Hong Kong","hong-kong"),("Taiwan","taiwan")]:
        t=get_text(f"https://www.investing.com/rates-bonds/{slug}-10-year-bond-yield")
        if t.startswith("__ERR__"):
            r.log(f"  investing {name}: {t[:60]}")
        else:
            y=re.search(r"data-test=['\"]instrument-price-last['\"][^>]*>([\d.]+)",t) or re.search(r'"last"[:\s]*"?([\d.]{3,6})',t)
            r.log(f"  investing {name}: page_len={len(t)} yield={y.group(1) if y else '?'}")
