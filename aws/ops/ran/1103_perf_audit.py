"""1103 — performance audit of justhodl.ai homepage.

Measures:
1. Page HTML download (TTFB + total + size)
2. Each linked JS file (size, timing, cache headers)
3. The signal-board.json fetch (the only data dependency)
4. Font URL timings (Google Fonts)
5. Cache-Control headers everywhere
6. Cold vs warm comparison

Output: full per-asset timing breakdown so we know what to fix.
"""
import json, pathlib, time, urllib.request
from datetime import datetime, timezone


REPORT = "aws/ops/reports/1103_perf_audit.json"


def timed_fetch(url, follow_redirects=True):
    """Fetch a URL and return timing + size + headers."""
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (perf-audit/1103)",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
        })
        with urllib.request.urlopen(req, timeout=30) as r:
            headers = dict(r.headers)
            t_first_byte = time.time() - t0
            body = r.read()
            t_total = time.time() - t0
            return {
                "url":             url,
                "status":          r.status,
                "ttfb_ms":         round(t_first_byte * 1000, 1),
                "total_ms":        round(t_total * 1000, 1),
                "size_bytes":      len(body),
                "size_kb":         round(len(body) / 1024, 1),
                "content_type":    headers.get("Content-Type", ""),
                "cache_control":   headers.get("Cache-Control", "(none)"),
                "etag":            headers.get("ETag", "(none)"),
                "last_modified":   headers.get("Last-Modified", "(none)"),
                "server":          headers.get("Server", "(none)"),
                "via":             headers.get("Via", ""),
                "cf_cache_status": headers.get("Cf-Cache-Status", "(N/A)"),
                "age":             headers.get("Age", "(none)"),
                "content_encoding": headers.get("Content-Encoding", "(none)"),
            }
    except Exception as e:
        return {"url": url, "err": str(e)[:200],
                 "total_ms": round((time.time() - t0) * 1000, 1)}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. The homepage itself
    print("[1103] auditing homepage…")
    page_url = "https://justhodl.ai/"
    out["homepage_cold"] = timed_fetch(page_url)
    time.sleep(1)
    out["homepage_warm"] = timed_fetch(page_url)
    
    # 2. All scripts referenced
    print("[1103] auditing scripts…")
    scripts = [
        "wss-client.js",
        "tenor-signals.js",
        "liquidity-credit.js",
        "liquidity-pulse.js",
        "ai-synthesis-kit.js",
        "ai-frontrun-kit.js",
        "ai-frontrun-history-kit.js",
    ]
    out["scripts"] = []
    for s in scripts:
        result = timed_fetch(f"https://justhodl.ai/{s}")
        out["scripts"].append({"name": s, **result})
    
    # 3. Data endpoint
    print("[1103] auditing data endpoint…")
    out["data_endpoint"] = {
        "via_cdn_cold": timed_fetch("https://justhodl.ai/data/signal-board.json"),
    }
    time.sleep(1)
    out["data_endpoint"]["via_cdn_warm"] = timed_fetch("https://justhodl.ai/data/signal-board.json")
    out["data_endpoint"]["via_s3_direct"] = timed_fetch("https://justhodl-dashboard-live.s3.amazonaws.com/data/signal-board.json")
    
    # 4. Fonts
    print("[1103] auditing fonts…")
    out["google_fonts_css"] = timed_fetch(
        "https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Serif:wght@400;500;600&display=swap"
    )
    
    # 5. Compute summary stats
    homepage_kb = out["homepage_cold"].get("size_kb", 0)
    scripts_total_kb = sum(s.get("size_kb", 0) for s in out["scripts"])
    scripts_total_ms = sum(s.get("total_ms", 0) for s in out["scripts"])
    
    out["summary"] = {
        "homepage_size_kb":       homepage_kb,
        "homepage_ttfb_cold_ms":  out["homepage_cold"].get("ttfb_ms"),
        "homepage_total_cold_ms": out["homepage_cold"].get("total_ms"),
        "homepage_ttfb_warm_ms":  out["homepage_warm"].get("ttfb_ms"),
        "homepage_total_warm_ms": out["homepage_warm"].get("total_ms"),
        "scripts_count":          len(out["scripts"]),
        "scripts_total_size_kb":  round(scripts_total_kb, 1),
        "scripts_total_ms_serial": round(scripts_total_ms, 1),
        "scripts_avg_ms":         round(scripts_total_ms / max(1, len(out["scripts"])), 1),
        "data_cdn_cold_ms":       out["data_endpoint"]["via_cdn_cold"].get("total_ms"),
        "data_cdn_warm_ms":       out["data_endpoint"]["via_cdn_warm"].get("total_ms"),
        "data_s3_direct_ms":      out["data_endpoint"]["via_s3_direct"].get("total_ms"),
        "fonts_css_ms":           out["google_fonts_css"].get("total_ms"),
        "total_critical_path_kb": round(homepage_kb + scripts_total_kb, 1),
    }
    
    # 6. Cache-Control diagnostics — find anything without strong cache headers
    out["cache_issues"] = []
    for sec_name, sec_data in [("homepage", out["homepage_cold"])] + [(s["name"], s) for s in out["scripts"]]:
        cc = sec_data.get("cache_control", "(none)")
        if cc == "(none)" or "no-cache" in cc.lower() or "max-age=0" in cc:
            out["cache_issues"].append({"asset": sec_name, "cache_control": cc})
        elif "max-age" in cc.lower():
            # Parse the max-age value
            import re
            m = re.search(r"max-age=(\d+)", cc.lower())
            if m and int(m.group(1)) < 300:
                out["cache_issues"].append({"asset": sec_name, "cache_control": cc, "issue": "short_ttl"})
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1103] DONE")


if __name__ == "__main__":
    main()
