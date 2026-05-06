#!/usr/bin/env python3
"""Step 275 — Verify macro-data.html on github-pages has v2.1 chart wiring."""
import json
import os
import urllib.request
from datetime import datetime, timezone

REPORT_PATH = "aws/ops/reports/275_verify_nowcast_chart_live.json"
PAGE_URL = "https://justhodl.ai/macro-data.html"
DATA_URL = "https://justhodl-dashboard-live.s3.amazonaws.com/data/macro-nowcast.json"

def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "verify-275/1.0",
                                                "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()

out = {"probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}

# 1. The page itself
try:
    page_html = fetch(PAGE_URL + "?_=" + str(int(__import__('time').time())))
    out["page_size_bytes"] = len(page_html)
    out["page_has_renderNowcastHistChart"] = "renderNowcastHistChart" in page_html
    out["page_has_nowcastHistChart_div"] = 'id="nowcastHistChart"' in page_html
    out["page_has_historical_scores_ref"] = "historical_scores" in page_html
    out["page_has_loadNowcast_call"] = "loadNowcast()" in page_html
except Exception as e:
    out["page_err"] = str(e)[:200]

# 2. The data file in S3
try:
    data = json.loads(fetch(DATA_URL + "?_=" + str(int(__import__('time').time()))))
    hist = data.get("historical_scores") or []
    summary = data.get("historical_summary") or {}
    out["data_version"] = data.get("v")
    out["data_n_historical_months"] = len(hist)
    out["data_first_date"] = hist[0]["date"] if hist else None
    out["data_last_date"] = hist[-1]["date"] if hist else None
    out["data_min_score"] = min((h["score"] for h in hist), default=None)
    out["data_max_score"] = max((h["score"] for h in hist), default=None)
    out["data_current_regime"] = data.get("regime")
    out["data_current_score"] = data.get("normalized_score")
    out["data_current_pctile"] = summary.get("current_score_percentile")
    out["data_regime_distribution"] = summary.get("regime_distribution")
except Exception as e:
    out["data_err"] = str(e)[:200]

os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
with open(REPORT_PATH, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(json.dumps(out, indent=2, default=str))
