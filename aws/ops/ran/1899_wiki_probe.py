import urllib.request, time
def g(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"JustHodl/1.0 (research@justhodl.ai)"}),timeout=15)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e: return e.code, e.read().decode("utf-8","ignore")[:160]
    except Exception as e: return 0,str(e)[:120]
arts=["Artificial_intelligence","Data_center","High_Bandwidth_Memory","Graphics_processing_unit","Small_modular_reactor"]
print("WIKIMEDIA PAGEVIEWS probe (free attention proxy):")
for a in arts:
    u="https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/%s/daily/2026050100/2026061900"%a
    c,b=g(u)
    n=b.count('"views"') if c==200 else 0
    # quick recent-vs-prior
    import re
    vals=[int(x) for x in re.findall(r'"views":(\d+)',b)] if c==200 else []
    rec=sum(vals[-7:])/7 if len(vals)>=7 else None
    pri=sum(vals[-21:-7])/14 if len(vals)>=21 else None
    trend = round((rec/pri-1)*100,1) if (rec and pri) else None
    print("  [%s] %-26s HTTP %s  days=%d  recent7=%s prior14avg=%s trend=%s%%"%("OK " if c==200 else "ERR",a,c,n,round(rec) if rec else None,round(pri) if pri else None,trend))
    time.sleep(0.5)
