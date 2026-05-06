"""Diagnose why activist scanner found only 1 filing — fix the Atom feed limits + verify CIK mapping."""
import json, time, urllib.request, urllib.error, urllib.parse, re, os
import boto3

S3 = boto3.client("s3", region_name="us-east-1")

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl-AI raafouis@gmail.com",
        "Accept": "application/json,application/atom+xml,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def main():
    section("1) Test Atom feed with start/count parameters")
    # SEC default limit is 40; max is 100 per page
    for params in ["", "&start=0&count=40", "&start=0&count=100", "&start=40&count=40"]:
        try:
            url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13D&output=atom" + params
            text = http_get(url)
            n_entries = len(re.findall(r"<entry>", text))
            log("  SC 13D " + params + " → " + str(n_entries) + " entries")
        except Exception as e:
            log("  SC 13D " + params + " ❌ " + str(e))

    section("2) Test EDGAR full-text search API (json)")
    today = time.strftime("%Y-%m-%d")
    seven_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7*86400))
    # The full-text search has both /LATEST/search-index and /LATEST/search
    # The "search" endpoint actually returns matching docs
    for form in ["SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"]:
        encoded_form = urllib.parse.quote(form)
        url = ("https://efts.sec.gov/LATEST/search-index?q=&dateRange=custom&startdt=" + seven_ago +
               "&enddt=" + today + "&forms=" + encoded_form)
        try:
            text = http_get(url)
            d = json.loads(text)
            hits = d.get("hits", {}).get("hits", [])
            log("  " + form + " 7d: " + str(len(hits)) + " hits")
            if hits:
                first = hits[0].get("_source", {})
                log("    first: " + (first.get("display_names") or ["?"])[0][:60] + " | form=" + str(first.get("form_type")))
        except Exception as e:
            log("  " + form + " ❌ " + str(e))

    section("3) Test EDGAR full-text search WITHOUT date filter")
    for form in ["SC 13D", "SC 13G"]:
        encoded_form = urllib.parse.quote(form)
        url = "https://efts.sec.gov/LATEST/search-index?q=&forms=" + encoded_form
        try:
            text = http_get(url)
            d = json.loads(text)
            hits = d.get("hits", {}).get("hits", [])
            log("  " + form + " (no date): " + str(len(hits)) + " hits, total=" + str(d.get("hits", {}).get("total", {}).get("value", 0)))
            if hits:
                for h in hits[:3]:
                    src = h.get("_source", {})
                    names = src.get("display_names") or []
                    log("    " + str(src.get("form_type")) + " | " + (names[0] if names else "?")[:50] + " | filed=" + str(src.get("file_date")))
        except Exception as e:
            log("  " + form + " ❌ " + str(e))

    section("4) Test CIK → ticker mapping (reuse existing logic)")
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        text = http_get(url, timeout=20)
        d = json.loads(text)
        log("  loaded " + str(len(d)) + " entries from company_tickers.json")
        # Show what GNK (Genco Shipping) maps to
        found_gnk = None
        for k, v in d.items():
            if isinstance(v, dict) and (v.get("ticker") or "").upper() == "GNK":
                found_gnk = v
                break
        if found_gnk:
            log("  GNK found: " + str(found_gnk))
        else:
            log("  GNK not in mapping — possibly a maritime CIK")
    except Exception as e:
        log("  ❌ " + str(e))

    section("5) Lookup GENCO SHIPPING by CIK in EDGAR")
    # Genco Shipping is GNK (NYSE)
    try:
        url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=GNK&output=atom&count=5"
        text = http_get(url)
        title_m = re.search(r"<title>([^<]+)</title>", text)
        log("  EDGAR atom for GNK: " + (title_m.group(1) if title_m else "?")[:120])
    except Exception as e:
        log("  ❌ " + str(e))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("    " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_x6b_activist_diagnose.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
