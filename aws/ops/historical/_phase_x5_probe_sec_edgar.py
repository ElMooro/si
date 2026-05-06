"""Probe SEC EDGAR for 13D/G filings — verify endpoints + fields before building Lambda."""
import json, time, urllib.request, urllib.error, os

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def probe(url, label):
    try:
        # SEC requires User-Agent with email
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl-AI raafouis@gmail.com",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            ct = r.headers.get("Content-Type", "")
            body = r.read().decode("utf-8", "replace")
            log("    ✓ " + label + " status=" + str(r.status) + " ct=" + ct[:50])
            if "json" in ct:
                d = json.loads(body)
                if isinstance(d, dict):
                    log("      keys=" + str(list(d.keys())[:8]))
                    log("      sample: " + json.dumps(d)[:300])
                elif isinstance(d, list):
                    log("      list of " + str(len(d)))
                    if d:
                        log("      first item: " + json.dumps(d[0])[:300])
            else:
                # XML or RSS
                log("      preview: " + body[:500])
            return True
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "replace")[:300]
        except Exception:
            pass
        log("    ❌ " + label + " HTTP " + str(e.code) + ": " + body)
        return False
    except Exception as e:
        log("    ❌ " + label + ": " + str(e))
        return False


def main():
    section("1) SEC EDGAR full-text search for SC 13D filings (last 7 days)")
    today = time.strftime("%Y-%m-%d")
    seven_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7 * 86400))
    # EDGAR full-text search API
    probe("https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today + "&forms=SC+13D",
          "13D filings last 7d")

    section("2) EDGAR full-text search for SC 13G")
    probe("https://efts.sec.gov/LATEST/search-index?q=%22schedule+13G%22&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today + "&forms=SC+13G",
          "13G filings last 7d")

    section("3) EDGAR daily index files (alternative — full daily list)")
    probe("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13D&output=atom",
          "13D current RSS feed")

    section("4) EDGAR Atom feed for 13D/A (amendments)")
    probe("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13D%2FA&output=atom",
          "13D/A amendments RSS")

    section("5) EDGAR Atom feed 13G")
    probe("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13G&output=atom",
          "13G current RSS")

    section("6) EDGAR Atom feed Form 4 (insider buys we already track)")
    probe("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom",
          "Form 4 baseline")

    section("7) EDGAR daily-index directory (raw filings list)")
    today_yr = time.strftime("%Y")
    today_qtr = ((time.gmtime().tm_mon - 1) // 3) + 1
    probe("https://www.sec.gov/Archives/edgar/full-index/" + today_yr + "/QTR" + str(today_qtr) + "/",
          "current quarter index")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_x5_sec_edgar_probe.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
