"""1306 — verify charts.html key removed + chart-pro overlay present."""
import json, urllib.request
out={}
def get(p):
    try:
        req=urllib.request.Request("https://justhodl.ai"+p,headers={"User-Agent":"Mozilla/5.0"})
        return urllib.request.urlopen(req,timeout=20).read().decode("utf-8","replace")
    except Exception as e: return "ERR:"+str(e)[:50]
ch=get("/charts.html")
out["charts_html"]={"key_exposed": "zvEY" in ch or "apiKey=" in ch, "is_redirect": "chart-pro.html" in ch and "Redirecting" in ch, "bytes": len(ch)}
cp=get("/chart-pro.html")
out["chart_pro"]={"has_signal_overlay": "SignalOverlay" in cp, "has_signals_btn": "signal-layers-btn" in cp, "has_deeplink": "URLSearchParams" in cp}
open("aws/ops/reports/1306_overlay.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
