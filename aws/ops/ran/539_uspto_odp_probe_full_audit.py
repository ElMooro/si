#!/usr/bin/env python3
"""539 — Probe data.uspto.gov ODP (new endpoint as of 2026-03-20) for BUILD 12
USPTO patents + comprehensive sidecar audit across all 14 shipped builds."""
import io, json, os, urllib.request, urllib.error, time
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/539_uspto_odp_probe_full_audit.json"
s3 = boto3.client("s3", region_name="us-east-1")


def http_probe(url, headers=None, timeout=10, method="GET", body=None):
    h = headers or {"User-Agent": "Mozilla/5.0 (compatible; JustHodlBot/1.0)"}
    try:
        t0 = time.time()
        if method == "POST" and body:
            req = urllib.request.Request(url, data=body, headers=h, method="POST")
        else:
            req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
            return {
                "status": r.status,
                "bytes": len(data),
                "preview": data[:200].decode("utf-8", "replace"),
                "content_type": r.headers.get("Content-Type", ""),
                "elapsed_ms": int((time.time() - t0) * 1000),
            }
    except urllib.error.HTTPError as e:
        body = b""
        try: body = e.read()[:300]
        except: pass
        return {"status": e.code, "err_body": body.decode("utf-8", "replace"), "elapsed_ms": -1}
    except Exception as e:
        return {"err": str(e)[:200], "elapsed_ms": -1}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── BUILD 12 final probe — data.uspto.gov ODP ───
    out["odp_probes"] = {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    last_tue = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

    probes = {
        # Open Data Portal landing
        "odp_landing": "https://data.uspto.gov",
        "odp_api_docs": "https://data.uspto.gov/apis",
        # OpenAPI patent search endpoint candidates
        "odp_search_open": "https://api.uspto.gov/api/v1/patent/applications/search?searchText=Apple&rows=2",
        "odp_search_v2": "https://api.uspto.gov/openapi/v1/patent/applications",
        "odp_grants_recent": f"https://api.uspto.gov/openapi/v1/patent/grants?fromDate={last_tue}&toDate={today}",
        # Bulk download manifest
        "bulk_xml_landing": "https://bulkdata.uspto.gov/data/patent/grant/redbook/2026/",
        "bulk_xml_index": "https://bulkdata.uspto.gov/data/patent/grant/2026/",
        # PEDS (Patent Examination Data System)
        "peds_status": "https://ped.uspto.gov/api/queries",
        # Google Patents (alternative)
        "google_patents_search": "https://patents.google.com/xhr/query?url=q%3DApple%26assignee%3DApple%26after%3Dpriority%3A20260501",
    }

    for k, url in probes.items():
        out["odp_probes"][k] = {"url": url, **http_probe(url, timeout=8)}
        if out["odp_probes"][k].get("status") == 200:
            out["odp_probes"][k]["WORKS"] = True

    # ─── Comprehensive sidecar audit across all 14 shipped builds ───
    sidecars = [
        ("data/dealer-gex.json",         "BUILD 1+13 · Dealer GEX + 0DTE"),
        ("data/finra-short.json",        "BUILD 2 · FINRA Short"),
        ("data/13f-positions.json",      "BUILD 3 · 13F Smart Money"),
        ("data/dix-history.json",        "BUILD 4 · DIX (Squeezemetrics)"),
        ("data/vix-term-structure.json", "BUILD 5 · VIX Term v2"),
        ("data/crypto-funding.json",     "BUILD 6 · Crypto Funding"),
        ("data/earnings-nlp.json",       "BUILD 7 · Earnings NLP"),
        ("data/credit-stress.json",      "BUILD 8 · Credit Stress"),
        ("data/retail-sentiment.json",   "BUILD 9 · Retail Sentiment"),
        ("data/news-velocity.json",      "BUILD 10 · News Velocity"),
        ("data/cb-stance.json",          "BUILD 11 · CB Stance"),
        ("data/global-markets.json",     "BUILD 14 · Global Markets"),
        ("data/commodity-curves.json",   "BUILD 15 · Commodity Curves"),
        # Bonus
        ("data/options-flow-scanner.json", "BONUS · Options Flow Scanner"),
        ("data/insider-transactions.json", "BONUS · Insider Transactions"),
    ]

    out["sidecar_audit"] = {}
    now_utc = datetime.now(timezone.utc)
    for key, label in sidecars:
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            modified = obj["LastModified"]
            age_min = (now_utc - modified).total_seconds() / 60
            full = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = full["Body"].read()
            p = {}
            try: p = json.loads(body)
            except: pass
            info = {
                "label": label,
                "size_kb": round(len(body) / 1024, 1),
                "modified": modified.isoformat()[:19],
                "age_min": round(age_min, 1),
                "fresh": age_min < (24 * 60),
                "version": p.get("version"),
                "composite_regime": p.get("composite_regime"),
            }
            # Pull key signal field
            if "spy_20d" in p: info["spy_20d"] = p.get("spy_20d")
            if "composite" in p and isinstance(p["composite"], dict):
                comp = p["composite"]
                if "ranked_20d" in comp:
                    info["top_3"] = comp.get("top_3_by_20d", [])[:3]
            if "fed" in p and isinstance(p["fed"], dict):
                info["fed_regime"] = p["fed"].get("regime")
                info["fed_hawkish_score"] = (p["fed"].get("latest_statement") or {}).get("hawkish_score")
            out["sidecar_audit"][key] = info
        except s3.exceptions.NoSuchKey:
            out["sidecar_audit"][key] = {"label": label, "exists": False}
        except Exception as e:
            out["sidecar_audit"][key] = {"label": label, "err": str(e)[:150]}

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
