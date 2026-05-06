"""Probe Polygon news endpoint capability before rewriting narrative tracker."""
import json, time, urllib.request, urllib.error, urllib.parse, os

POLY_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def probe(url, label):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
            log("    ✓ " + label + " status=200")
            log("      keys=" + str(list(d.keys())[:8]))
            results = d.get("results") or []
            log("      n_results: " + str(len(results)))
            if results:
                first = results[0]
                log("      first item keys: " + str(list(first.keys())[:12]))
                # Sample fields
                log("      title: " + (first.get("title") or "")[:120])
                log("      published_utc: " + str(first.get("published_utc")))
                log("      tickers: " + str(first.get("tickers", [])[:6]))
                if "next_url" in d:
                    log("      has pagination cursor")
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
    section("1) Polygon /v2/reference/news (no filter)")
    probe("https://api.polygon.io/v2/reference/news?limit=10&apiKey=" + POLY_KEY,
          "all news")

    section("2) Polygon news filtered by ticker")
    probe("https://api.polygon.io/v2/reference/news?ticker=AAPL&limit=10&apiKey=" + POLY_KEY,
          "AAPL news")

    section("3) Polygon news with date filter")
    today = time.strftime("%Y-%m-%d")
    seven_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7*86400))
    probe("https://api.polygon.io/v2/reference/news?published_utc.gte=" + seven_ago + "&limit=50&apiKey=" + POLY_KEY,
          "news last 7 days")

    section("4) Polygon news bulk (200 limit)")
    probe("https://api.polygon.io/v2/reference/news?limit=200&apiKey=" + POLY_KEY,
          "news bulk 200")

    section("5) Polygon news ticker + date")
    probe("https://api.polygon.io/v2/reference/news?ticker=NVDA&published_utc.gte=" + seven_ago + "&limit=50&apiKey=" + POLY_KEY,
          "NVDA news last 7d")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_x4b_polygon_news_probe.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
