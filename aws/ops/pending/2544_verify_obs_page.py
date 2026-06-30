"""ops 2544 — verify observability.html deployed with the LLM Providers panel."""
import urllib.request, time
time.sleep(60)  # let Cloudflare Pages propagate
url = "https://justhodl.ai/observability.html"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
try:
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
    checks = {
        "LLM Providers panel": "LLM Providers" in html,
        "fetches llm-health.json": "data/llm-health.json" in html,
        "renders billing_action_needed": "billing_action_needed" in html,
        "renders redundancy": "Redundancy" in html,
        "llm-body element": 'id="llm-body"' in html,
    }
    for k, v in checks.items():
        print(f"  {'✅' if v else '❌'} {k}")
    print("ALL PRESENT:", all(checks.values()))
except Exception as e:
    print("fetch err:", str(e)[:120])
print("DONE 2544")
