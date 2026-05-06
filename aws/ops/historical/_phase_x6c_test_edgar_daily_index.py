"""Test EDGAR daily-index files — these have ALL filings filed each day."""
import time, urllib.request, urllib.error, urllib.parse, os, re

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl-AI raafouis@gmail.com",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def main():
    section("1) EDGAR daily-index — yesterday's master.idx")
    # Format: https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx
    # master.idx contains pipe-delimited filings list for that date
    yesterday = time.gmtime(time.time() - 86400)
    yesterday_2 = time.gmtime(time.time() - 2*86400)
    yesterday_3 = time.gmtime(time.time() - 3*86400)
    
    for d in [yesterday, yesterday_2, yesterday_3]:
        if d.tm_wday >= 5:
            continue  # skip weekends
        date_str = time.strftime("%Y%m%d", d)
        year = time.strftime("%Y", d)
        qtr = ((d.tm_mon - 1) // 3) + 1
        url = "https://www.sec.gov/Archives/edgar/daily-index/" + year + "/QTR" + str(qtr) + "/master." + date_str + ".idx"
        try:
            text = http_get(url, timeout=20)
            # Header lines, then "CIK|Company Name|Form Type|Date Filed|Filename"
            lines = text.splitlines()
            log("  " + date_str + " (" + str(len(lines)) + " lines)")
            
            # Find header
            data_start = 0
            for i, ln in enumerate(lines):
                if ln.startswith("CIK|") or ln.startswith("---"):
                    data_start = i + 1
                    if ln.startswith("---"):
                        data_start = i + 1
                        break
            
            # Find 13D/G filings
            d13_count = 0
            g13_count = 0
            sample_filings = []
            for ln in lines[data_start:]:
                parts = ln.split("|")
                if len(parts) >= 5:
                    form_type = parts[2].strip()
                    if form_type.startswith("SC 13D"):
                        d13_count += 1
                        if len(sample_filings) < 5:
                            sample_filings.append((parts[1].strip(), form_type, parts[0].strip(), parts[4].strip()))
                    elif form_type.startswith("SC 13G"):
                        g13_count += 1
                        if len(sample_filings) < 5:
                            sample_filings.append((parts[1].strip(), form_type, parts[0].strip(), parts[4].strip()))
            log("    SC 13D: " + str(d13_count) + ", SC 13G: " + str(g13_count))
            for s in sample_filings[:5]:
                log("    sample: " + s[0][:40] + " | " + s[1] + " | CIK=" + s[2])
            break  # found a working day
        except urllib.error.HTTPError as e:
            log("  " + date_str + " ❌ HTTP " + str(e.code))
        except Exception as e:
            log("  " + date_str + " ❌ " + str(e))

    section("2) EDGAR full-index company.idx (alternative: by company)")
    yest = time.gmtime(time.time() - 86400)
    if yest.tm_wday >= 5:
        yest = time.gmtime(time.time() - 86400 * (7 - yest.tm_wday + 4))
    date_str = time.strftime("%Y%m%d", yest)
    year = time.strftime("%Y", yest)
    qtr = ((yest.tm_mon - 1) // 3) + 1
    url = "https://www.sec.gov/Archives/edgar/daily-index/" + year + "/QTR" + str(qtr) + "/company." + date_str + ".idx"
    try:
        text = http_get(url, timeout=20)
        lines = text.splitlines()
        log("  " + date_str + " company.idx: " + str(len(lines)) + " lines")
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
    with open(os.path.join(out, "phase_x6c_daily_index.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
