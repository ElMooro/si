"""
justhodl-uspto-patents -- Corporate Patent Filing Tracker
==========================================================

Tracks granted-US-patent counts per major assignee (company) over the last
5 years plus the most recent week's grants, giving a proxy for R&D
output / innovation momentum per ticker.

DATA SOURCE
-----------
USPTO PatentsView "PatentSearch" API (free; requires a free API key obtained
via the PatentsView service desk):
    https://search.patentsview.org/api/v1/patent/

NOTE on the old endpoint: the legacy api.patentsview.org/patents/query API
was discontinued 2025-05-01 (410 Gone / redirects to the USPTO transition
guide), so this pipeline targets the replacement PatentSearch API. Without
an API key the pipeline still runs but writes a degraded output that says
exactly what is needed (store the key at /justhodl/uspto/api-key).

STRATEGY (pragmatic -- bulk data is terabytes)
---------------------------------------------
~40 major tech/pharma/industrial tickers with a hardcoded assignee->ticker
map. For each assignee, one count-only query per year (5y history) plus one
for the most recent 7-day window. patent_date matching in the new query
language is token-based, so counts are approximate (subsidiaries / spelling
variants may shift totals); the YEAR-OVER-YEAR TREND is the signal that
matters, not the absolute number.

OUTPUT
------
data/uspto-patents.json
{
  generated_at, status, source,
  by_ticker: {TICKER: {assignee, patents_by_year: {2021: n, ...},
                       recent_grants_7d: n, total_5y: n, trend: rising|flat|declining}},
  top_filers: [{ticker, assignee, grants_latest_year, total_5y, trend}, ...],
  coverage: {tickers_attempted, tickers_with_data, years, queries_made, partial}
}
"""
import datetime as dt
import json
import os
import time
import traceback
import urllib.parse
import urllib.request

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/uspto-patents.json"
USPTO_API_KEY = os.environ.get("USPTO_API_KEY", "").strip()
BASE_URL = "https://search.patentsview.org/api/v1/patent/"
UA = "JustHodlAI-USPTOPatents/1.0 (contact: justhodl.ai; pipeline: justhodl-uspto-patents)"

# Budget guard: never spend more than ~12 min fetching (timeout is 900s).
FETCH_BUDGET_S = 720
# PatentSearch API rate limit is 45 req/min -> ~1.4s between calls is polite.
PACING_S = 1.4

# ticker -> assignee organization search string (as filed at USPTO; matching is
# token-based so minor variants are fine, trend is what matters)
ASSIGNEE_MAP = {
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "GOOGL": "Google LLC",
    "IBM": "International Business Machines Corporation",
    "INTC": "Intel Corporation",
    "QCOM": "QUALCOMM Incorporated",
    "NVDA": "Nvidia Corporation",
    "AMD": "Advanced Micro Devices, Inc.",
    "MU": "Micron Technology, Inc.",
    "TXN": "Texas Instruments Incorporated",
    "AMAT": "Applied Materials, Inc.",
    "PFE": "Pfizer Inc.",
    "JNJ": "Johnson & Johnson",
    "MRK": "Merck & Co., Inc.",
    "LLY": "Eli Lilly and Company",
    "BMY": "Bristol-Myers Squibb Company",
    "GILD": "Gilead Sciences, Inc.",
    "AMGN": "Amgen Inc.",
    "REGN": "Regeneron Pharmaceuticals, Inc.",
    "MRNA": "Moderna, Inc.",
    "F": "Ford Motor Company",
    "GM": "General Motors LLC",
    "TSLA": "Tesla, Inc.",
    "BA": "The Boeing Company",
    "LMT": "Lockheed Martin Corporation",
    "RTX": "Raytheon Technologies Corporation",
    "GE": "General Electric Company",
    "HON": "Honeywell International Inc.",
    "MMM": "3M Company",
    "CAT": "Caterpillar Inc.",
    "DE": "Deere & Company",
    "CSCO": "Cisco Technology, Inc.",
    "ORCL": "Oracle America, Inc.",
    "ADBE": "Adobe Inc.",
    "CRM": "salesforce.com, inc.",
    "META": "Meta Platforms, Inc.",
    "AMZN": "Amazon.com, Inc.",
    "AVGO": "Broadcom Inc.",
    "NFLX": "Netflix, Inc.",
    "DIS": "Disney Enterprises, Inc.",
    "NKE": "Nike, Inc.",
}


def http_get_json(url, headers, timeout=30):
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", errors="ignore")), r.status
    except Exception as e:
        return {"_error": str(e)}, None


def patent_count(query, api_key):
    """Count-only query against the PatentSearch patent endpoint.

    Returns (count:int|None, error:str|None).
    """
    params = {
        "q": json.dumps(query),
        "f": json.dumps(["patent_id"]),
        "o": json.dumps({"size": 1}),
    }
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    headers = {"User-Agent": UA, "Accept": "application/json",
               "X-Api-Key": api_key}
    data, status = http_get_json(url, headers)
    if status != 200 or not isinstance(data, dict) or "_error" in data:
        err = data.get("_error") if isinstance(data, dict) else "bad status"
        return None, f"http_status={status} err={err}"
    for k in ("total_hits", "total_patent_count", "count"):
        v = data.get(k)
        if isinstance(v, int):
            return v, None
    recs = data.get("patents")
    if isinstance(recs, list):
        return len(recs), None
    return None, "no count field in response"


def trend_label(year_counts):
    """Simple slope-based trend over the 5 annual counts."""
    ys = [float(year_counts[y]) for y in sorted(year_counts)]
    if len(ys) < 3 or sum(ys) == 0:
        return "flat"
    n = len(ys)
    xs = list(range(n))
    mx, my = sum(xs) / n, sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom if denom else 0
    rel = slope / my if my else 0
    if rel > 0.08:
        return "rising"
    if rel < -0.08:
        return "declining"
    return "flat"


def lambda_handler(event, context):
    started = time.time()
    s3 = boto3.client("s3")
    today = dt.datetime.now(dt.timezone.utc).date()
    years = [today.year - i for i in range(4, -1, -1)]  # last 5 calendar years
    week_start = (today - dt.timedelta(days=7)).isoformat()

    output = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "status": "ok",
        "source": "USPTO PatentsView PatentSearch API (search.patentsview.org)",
        "by_ticker": {},
        "top_filers": [],
        "coverage": {
            "tickers_attempted": len(ASSIGNEE_MAP),
            "tickers_with_data": 0,
            "years": years,
            "queries_made": 0,
            "partial": False,
        },
        "notes": [],
    }

    try:
        if not USPTO_API_KEY:
            output["status"] = "degraded"
            output["notes"].append(
                "No USPTO API key configured. The legacy free keyless PatentsView API "
                "was discontinued 2025-05-01; the replacement PatentSearch API needs a "
                "free key from the PatentsView service desk "
                "(https://patentsview-support.atlassian.net/servicedesk/customer/portals). "
                "Store it as env USPTO_API_KEY (or SSM /justhodl/uspto/api-key mapped to it) "
                "and this pipeline will populate on the next run."
            )
        else:
            # Probe once: if the endpoint is unreachable/unauthorized, degrade
            # immediately instead of burning ~240 doomed queries.
            probe_q = {"patent_year": str(years[-1])}
            probe_n, probe_err = patent_count(probe_q, USPTO_API_KEY)
            output["coverage"]["queries_made"] += 1
            if probe_n is None:
                output["status"] = "degraded"
                output["notes"].append(
                    f"PatentSearch API probe failed ({probe_err}); writing empty "
                    "output. Check key validity / endpoint reachability."
                )
            else:
                deadline = started + FETCH_BUDGET_S
                for ticker, assignee in ASSIGNEE_MAP.items():
                    if time.time() > deadline:
                        output["coverage"]["partial"] = True
                        output["notes"].append(
                            f"Hit {FETCH_BUDGET_S}s fetch budget; stopped early "
                            f"({ticker} onward skipped)."
                        )
                        break
                    year_counts = {}
                    ok = True
                    for y in years:
                        q = {"_and": [
                            {"assignees.assignee_organization": assignee},
                            {"patent_year": str(y)},
                        ]}
                        n, err = patent_count(q, USPTO_API_KEY)
                        output["coverage"]["queries_made"] += 1
                        time.sleep(PACING_S)
                        if n is None:
                            ok = False
                            output["notes"].append(f"{ticker}: year {y} failed ({err})")
                            break
                        year_counts[str(y)] = n
                    if not ok:
                        continue
                    # most recent 7-day window
                    q_week = {"_and": [
                        {"assignees.assignee_organization": assignee},
                        {"patent_date": {"gte": week_start,
                                         "lte": today.isoformat()}},
                    ]}
                    recent, err = patent_count(q_week, USPTO_API_KEY)
                    output["coverage"]["queries_made"] += 1
                    time.sleep(PACING_S)
                    if recent is None:
                        recent = 0
                        output["notes"].append(f"{ticker}: recent-week query failed ({err})")
                    total_5y = sum(year_counts.values())
                    output["by_ticker"][ticker] = {
                        "assignee": assignee,
                        "patents_by_year": year_counts,
                        "recent_grants_7d": recent,
                        "total_5y": total_5y,
                        "trend": trend_label(year_counts),
                    }
                output["coverage"]["tickers_with_data"] = len(output["by_ticker"])
                output["top_filers"] = [
                    {"ticker": t,
                     "assignee": d["assignee"],
                     "grants_latest_year": d["patents_by_year"].get(str(years[-1]), 0),
                     "total_5y": d["total_5y"],
                     "trend": d["trend"]}
                    for t, d in sorted(output["by_ticker"].items(),
                                       key=lambda kv: kv[1]["total_5y"],
                                       reverse=True)[:15]
                ]
                if not output["by_ticker"]:
                    output["status"] = "degraded"
                    output["notes"].append("All assignee queries failed; check API key / endpoint.")
        output["run_duration_seconds"] = round(time.time() - started, 1)
        output["notes"].append(
            "Assignee matching is token-based and approximate (subsidiaries/spelling "
            "variants); year-over-year trend is the signal, not the absolute count."
        )

        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(output, default=str).encode("utf-8"),
                      ContentType="application/json",
                      CacheControl="public, max-age=21600")
        print(f"wrote {S3_KEY}: status={output['status']} "
              f"tickers={len(output['by_ticker'])} "
              f"queries={output['coverage']['queries_made']}")
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "status": output["status"],
                                    "tickers": len(output["by_ticker"]),
                                    "queries": output["coverage"]["queries_made"],
                                    "duration_s": round(time.time() - started, 1)})}
    except Exception as e:
        # FAIL-SOFT: never crash the handler; always write *something*.
        try:
            output["status"] = "error"
            output["notes"].append(f"handler exception: {e}")
            output["run_duration_seconds"] = round(time.time() - started, 1)
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(output, default=str).encode("utf-8"),
                          ContentType="application/json",
                          CacheControl="public, max-age=21600")
        except Exception as e2:
            print(f"failed to write fallback output: {e2}")
        print(traceback.format_exc()[:1500])
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": str(e)})}
