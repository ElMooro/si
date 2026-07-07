#!/usr/bin/env python3
"""ops 2951 — probe every dead feed from 2950 on BOTH channels (worker /data/
vs S3-direct) + the legacy agent API. Pure recon, always exits 0."""
import time, urllib.request
from ops_report import report

BASE = "https://justhodl.ai/data"
S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com"
NESTED = ["cot/extremes/current.json", "divergence/current.json",
          "investor-debate/_index.json", "opportunities/asymmetric-equity.json",
          "portfolio/pnl-daily.json", "portfolio/signal-portfolio-state.json",
          "regime/current.json", "risk/recommendations.json"]
LEGACY = ["crypto-intel.json", "edgar_insiders.json", "equity_research.json",
          "research_critique.json", "intelligence-report.json", "ici-flows.json"]

def code(url):
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"{url}?_={int(time.time()*1000)}",
            headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=12)
        lm = r.headers.get("Last-Modified", "?")
        return f"200 lm={lm}"
    except urllib.error.HTTPError as e:
        return str(e.code)
    except Exception as e:
        return type(e).__name__

def main():
    with report("2951_dead_feed_probe") as rep:
        for k in NESTED:
            line = f"NESTED {k}: worker={code(f'{BASE}/{k}')} s3={code(f'{S3}/{k}')}"
            print(line); rep.log(line)
        for k in LEGACY:
            line = f"LEGACY {k}: worker={code(f'{BASE}/{k}')} s3={code(f'{S3}/{k}')}"
            print(line); rep.log(line)
        line = "AGENT https://api.justhodl.ai/agent/secretary: " + code("https://api.justhodl.ai/agent/secretary")
        print(line); rep.log(line)
        rep.ok("probe complete")

if __name__ == "__main__":
    main()
