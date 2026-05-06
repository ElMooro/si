"""Look at the actual content of master.idx — my parsing was wrong."""
import time, urllib.request, urllib.error, os, re
from collections import Counter

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
    yest = time.gmtime(time.time() - 86400)
    if yest.tm_wday >= 5:
        yest = time.gmtime(time.time() - 86400 * (7 - yest.tm_wday + 4))
    date_str = time.strftime("%Y%m%d", yest)
    year = time.strftime("%Y", yest)
    qtr = ((yest.tm_mon - 1) // 3) + 1
    url = "https://www.sec.gov/Archives/edgar/daily-index/" + year + "/QTR" + str(qtr) + "/master." + date_str + ".idx"
    
    section("1) Raw first 30 lines of master.idx")
    text = http_get(url)
    lines = text.splitlines()
    for i, ln in enumerate(lines[:30]):
        log("  " + str(i) + ": " + ln[:200])

    section("2) Find header row (where data starts)")
    for i, ln in enumerate(lines[:20]):
        if ln.startswith("CIK"):
            log("  header at line " + str(i) + ": " + ln[:200])
        if "----" in ln:
            log("  separator at line " + str(i))

    section("3) Form type counts (proper parsing)")
    counts = Counter()
    for ln in lines:
        parts = ln.split("|")
        if len(parts) == 5:
            form = parts[2].strip()
            if form and form != "Form Type" and not form.startswith("-"):
                counts[form] += 1
    log("  total parsed: " + str(sum(counts.values())))
    log("  top 30 form types:")
    for f, n in counts.most_common(30):
        log("    " + f + ": " + str(n))

    section("4) All 13D-related forms")
    d13 = [f for f in counts if "13D" in f or "13G" in f or "13d" in f or "13g" in f]
    for f in d13:
        log("  " + f + ": " + str(counts[f]))

    section("5) Sample SC 13 filings with details")
    samples = []
    for ln in lines:
        parts = ln.split("|")
        if len(parts) == 5:
            form = parts[2].strip()
            if "13D" in form or "13G" in form:
                samples.append((parts[0].strip(), parts[1].strip(), form, parts[3].strip(), parts[4].strip()))
                if len(samples) >= 12:
                    break
    log("  found " + str(len(samples)) + " 13D/G samples (showing first 12):")
    for s in samples:
        log("    CIK=" + s[0] + " | " + s[1][:40] + " | " + s[2] + " | " + s[3] + " | " + s[4][:60])


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
    with open(os.path.join(out, "phase_x6d_idx_inspect.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
