"""
ops/883 - confirm the live Global Stress page serves the Rate
Volatility panel.

ops/882 verified the engine 9/10; the one miss was the page check,
which ran before GitHub Pages finished publishing the commit. This
re-fetches the live page now that Pages has deployed and confirms the
new Rate Volatility panel is present.

Writes aws/ops/reports/883_global_stress_page.json.
"""
import json
import urllib.request
from datetime import datetime, timezone

PAGE_URL = "https://justhodl.ai/global-stress.html"

rep = {"ops": 883, "ts": datetime.now(timezone.utc).isoformat(),
       "subject": "Confirm the live Global Stress page renders the Rate "
                  "Volatility panel", "checks": []}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})


try:
    req = urllib.request.Request(
        PAGE_URL + "?cb=" + datetime.now().strftime("%H%M%S"),
        headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as resp:
        st, page = resp.status, resp.read().decode("utf-8", "ignore")
    check("page_http_200", st == 200, "HTTP %s" % st)
    check("rate_volatility_panel_rendered",
          "Rate Volatility" in page and "d.rates" in page,
          "Rate Volatility panel present" if "Rate Volatility" in page
          else "still MISSING -- Pages may need another minute")
    check("expanded_subtitle_live",
          "Thirteen equity and bond markets" in page,
          "13-market subtitle live" if "Thirteen equity" in page
          else "old subtitle still cached")
except Exception as e:
    check("page_http_200", False, f"{type(e).__name__}: {e}")

n_ok = sum(1 for c in rep["checks"] if c["ok"])
rep["summary"] = "%d/%d checks passed" % (n_ok, len(rep["checks"]))
rep["all_passed"] = n_ok == len(rep["checks"])
rep["verdict"] = ("Global Stress page fully live with the Rate Volatility "
                  "panel." if rep["all_passed"]
                  else "Page check still failing -- see checks.")

with open("aws/ops/reports/883_global_stress_page.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
