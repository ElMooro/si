#!/usr/bin/env python3
"""540 — Full Google Patents JSON endpoint probe for BUILD 12.

The endpoint patents.google.com/xhr/query returns JSON for patent searches.
Probe variations to understand assignee → patent count, recent grants, etc."""
import json, os, urllib.request, urllib.error, urllib.parse, time
from datetime import datetime, timezone, timedelta

REPORT = "aws/ops/reports/540_google_patents_probe.json"


def gp_query(params, label):
    """Run a Google Patents XHR query. params is dict of search filters."""
    # Format: ?url=q%3D{q}%26... — the "url" param is URL-encoded query string
    qs_parts = []
    for k, v in params.items():
        if isinstance(v, list):
            for vv in v: qs_parts.append(f"{k}={urllib.parse.quote(str(vv))}")
        else: qs_parts.append(f"{k}={urllib.parse.quote(str(v))}")
    inner_qs = "&".join(qs_parts)
    url = f"https://patents.google.com/xhr/query?url={urllib.parse.quote(inner_qs)}&exp="

    try:
        t0 = time.time()
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            elapsed = int((time.time() - t0) * 1000)
            try:
                data = json.loads(raw)
            except:
                return {"label": label, "url": url[:200], "status": r.status, "bytes": len(raw),
                          "elapsed_ms": elapsed, "preview": raw[:300].decode("utf-8", "replace"),
                          "parse_err": "non-json"}
            # Inspect structure
            results = (data.get("results") or {})
            cluster = (results.get("cluster") or [])
            total = (results.get("total_num_results") or 0)
            patents = []
            if cluster:
                for c in cluster[:1]:
                    for r in (c.get("result") or [])[:3]:
                        patent = r.get("patent") or {}
                        patents.append({
                            "publication_number": patent.get("publication_number"),
                            "title": patent.get("title"),
                            "assignee": patent.get("assignee"),
                            "inventor": patent.get("inventor"),
                            "filing_date": patent.get("filing_date"),
                            "publication_date": patent.get("publication_date"),
                            "snippet": (patent.get("snippet") or "")[:200],
                        })
            return {
                "label": label,
                "url_short": url[:120] + "...",
                "status": r.status,
                "bytes": len(raw),
                "elapsed_ms": elapsed,
                "total_num_results": total,
                "n_clusters": len(cluster),
                "sample_patents": patents,
                "top_level_keys": list(data.keys()),
                "results_keys": list(results.keys()) if isinstance(results, dict) else None,
            }
    except urllib.error.HTTPError as e:
        return {"label": label, "url": url[:200], "status": e.code,
                  "err_body": e.read()[:300].decode("utf-8", "replace") if e.fp else ""}
    except Exception as e:
        return {"label": label, "err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    cutoff_30d = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y%m%d")
    cutoff_7d = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y%m%d")
    today = datetime.now(timezone.utc).strftime("%Y%m%d")

    # ─── Variations to find the magic query format ───
    queries = {
        # Simple keyword
        "simple_apple": {"q": "Apple"},
        # Assignee filter (modern PG syntax)
        "assignee_apple_corp": {"q": "Apple", "assignee": "Apple Inc"},
        # CPC-style filtered
        "assignee_strict": {"assignee": "Apple+Inc"},
        # Date range + type=PATENT (granted)
        "apple_grants_30d": {"assignee": "Apple Inc", "after": f"priority:{cutoff_30d}", "type": "PATENT"},
        "nvidia_grants_30d": {"assignee": "NVIDIA Corp", "after": f"priority:{cutoff_30d}", "type": "PATENT"},
        # Publication date filter
        "apple_pub_30d": {"assignee": "Apple Inc", "after": f"publication:{cutoff_30d}"},
        # Status: GRANTED
        "apple_granted": {"assignee": "Apple Inc", "status": "GRANT"},
        # Use full Apple Inc query
        "msft_30d": {"assignee": "Microsoft Technology Licensing LLC", "after": f"publication:{cutoff_30d}"},
        "google_30d": {"assignee": "Google LLC", "after": f"publication:{cutoff_30d}"},
        # Wildcard / total count baseline
        "total_recent_grants": {"after": f"publication:{cutoff_7d}", "type": "PATENT", "status": "GRANT"},
    }

    out["queries"] = {}
    for label, params in queries.items():
        out["queries"][label] = gp_query(params, label)
        time.sleep(0.5)  # polite

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
