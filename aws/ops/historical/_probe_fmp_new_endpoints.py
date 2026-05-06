"""Probe FMP's new (post-Aug 2025) API structure to migrate tier-classifier."""
import json, time, os, urllib.request, urllib.error

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
REPORT = []

def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")

def section(t):
    print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def hit(url, label):
    log(f"GET {label}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fmp-probe/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            txt = r.read().decode()
            log(f"  status={r.status} size={len(txt)}b")
            try:
                data = json.loads(txt)
                if isinstance(data, list) and data:
                    log(f"  list[{len(data)}] keys={sorted(data[0].keys())[:12]}")
                    return data
                if isinstance(data, dict):
                    log(f"  dict keys={sorted(data.keys())[:12]}")
                    if "Error Message" in data: log(f"  ERR: {data['Error Message'][:120]}")
                    return data
                log(f"  raw {data}")
            except json.JSONDecodeError:
                log(f"  not JSON: {txt[:120]}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:300]
        log(f"  HTTP{e.code}: {body[:200]}")
    except Exception as e:
        log(f"  {type(e).__name__}: {e}")

section("Probe stable endpoints")
# Try stable/* path with various endpoint patterns
candidates = [
    # symbol-as-query-param pattern (most common in new APIs)
    ("/stable/profile?symbol=AAPL",                       "stable profile?symbol="),
    ("/stable/quote?symbol=AAPL",                         "stable quote?symbol="),
    ("/stable/ratios-ttm?symbol=AAPL",                    "stable ratios-ttm?symbol="),
    ("/stable/key-metrics-ttm?symbol=AAPL",               "stable key-metrics-ttm?symbol="),
    ("/stable/income-statement?symbol=AAPL&limit=4",      "stable income-statement?symbol="),
    # path-style patterns
    ("/stable/profile/AAPL",                              "stable profile/AAPL"),
    ("/stable/quote/AAPL",                                "stable quote/AAPL"),
    # alternate API roots
    ("/api/v4/company-outlook?symbol=AAPL",               "api/v4 company-outlook"),
    ("/api/v4/ratios?symbol=AAPL",                        "api/v4 ratios"),
]
results = {}
for path, label in candidates:
    url = f"https://financialmodelingprep.com{path}{'&' if '?' in path else '?'}apikey={FMP_KEY}"
    results[label] = hit(url, label)

section("Inspect best stable endpoint")
# If profile?symbol= worked, examine it deeply
prof = results.get("stable profile?symbol=")
if prof and isinstance(prof, list) and prof:
    log("Full profile keys:")
    for k, v in sorted(prof[0].items()):
        vstr = str(v)[:80]
        log(f"  {k}: {vstr}")

# Examine ratios-ttm
rt = results.get("stable ratios-ttm?symbol=")
if rt and isinstance(rt, list) and rt:
    log("Full ratios-ttm keys:")
    for k, v in sorted(rt[0].items()):
        vstr = str(v)[:80]
        log(f"  {k}: {vstr}")

# Examine key-metrics-ttm
km = results.get("stable key-metrics-ttm?symbol=")
if km and isinstance(km, list) and km:
    log("Full key-metrics-ttm keys:")
    for k, v in sorted(km[0].items()):
        vstr = str(v)[:80]
        log(f"  {k}: {vstr}")

if __name__ == "__main__":
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "probe_fmp_new_endpoints.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
