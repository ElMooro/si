"""ops 2669 — discover the real ERCOT large-load report by searching its catalog by name
(robust, not a guessed ID), and check real WARN Act sources."""
import urllib.request, json

def get(url, timeout=20):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent":"JustHodl.AI research"}), timeout=timeout)
        return r.status, r.read()
    except Exception as e:
        return None, str(e)[:200]

print("=== ERCOT: search MIS report catalog for 'large load' / 'interconnection' by name ===")
# ERCOT's report list search endpoint (proven format from earlier probe)
for term in ["large%20load", "GIS%20Report", "interconnection"]:
    s, b = get(f"https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=&keyword={term}")
    print(f"  keyword={term}: status={s}", str(b)[:200] if s else b)

# ERCOT keyword search UI (different pattern)
s2, b2 = get("https://www.ercot.com/mp/data-products/data-product-list?type=all")
print("\n  data-product-list page status:", s2, len(b2) if s2 else b2)
if s2 and b2:
    txt = b2.decode("utf-8","ignore")
    import re
    hits = re.findall(r'href="[^"]*id=(NP[\w-]+)"[^>]*>([^<]{0,80}[Ll]arge[^<]{0,80})<', txt)
    print("  large-load product matches:", hits[:10])

print("\n=== WARN Act: try Texas Workforce Commission direct data ===")
s3, b3 = get("https://www.twc.texas.gov/news/reports-and-data-mass-layoffs")
print("  TWC WARN page:", s3, len(b3) if s3 else b3)

print("\n=== WARN Act: try a national CSV aggregator (data.gov style) ===")
s4, b4 = get("https://layoffs.fyi/wp-json/wp/v2/posts")
print("  layoffs.fyi wp-json:", s4, str(b4)[:150] if s4 else b4)

print("DONE 2669")
