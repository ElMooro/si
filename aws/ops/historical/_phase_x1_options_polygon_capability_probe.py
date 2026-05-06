"""
Phase X1 — Probe Polygon options API capability.
Tests which endpoints the user's API key has access to before building the Lambda.

Capabilities to test:
  • /v3/snapshot/options/{underlyingAsset} - real-time options chain snapshot
  • /v3/reference/options/contracts - options contract list
  • /v2/aggs/ticker/{optionsTicker}/range/... - options bar history
  • /v3/trades/{optionsTicker} - options trades (sweeps detection needs this)
  • /v3/snapshot/options/{underlyingAsset}/{optionContract} - greeks/IV
"""
import json, urllib.request, urllib.error, time, os

POLY_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def probe(url, label, max_chars=400):
    """Probe an endpoint, return (status, body_short)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", "replace")
            try:
                d = json.loads(body)
                # Pretty-print top-level keys + sample
                if isinstance(d, dict):
                    keys = list(d.keys())
                    sample = {}
                    for k in keys[:5]:
                        v = d[k]
                        if isinstance(v, (list, dict)):
                            sample[k] = type(v).__name__ + "[" + str(len(v)) + "]"
                        else:
                            sample[k] = str(v)[:60]
                    log("    ✓ " + label + " status=" + str(r.status))
                    log("      keys=" + str(keys))
                    log("      sample=" + json.dumps(sample)[:250])
                    if "results" in d and isinstance(d["results"], list) and d["results"]:
                        first = d["results"][0]
                        if isinstance(first, dict):
                            log("      results[0] keys: " + str(list(first.keys())[:10]))
                    return True
                return True
            except json.JSONDecodeError:
                log("    ✓ " + label + " raw: " + body[:max_chars])
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
    section("1) Probe Polygon /v3/snapshot/options/{underlying} — real-time chain")
    probe("https://api.polygon.io/v3/snapshot/options/AAPL?limit=5&apiKey=" + POLY_KEY,
          "AAPL chain snapshot")

    section("2) Probe /v3/reference/options/contracts — contract list (need for sweep tracking)")
    probe("https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=AAPL&limit=5&apiKey=" + POLY_KEY,
          "AAPL contracts list")

    section("3) Probe /v2/aggs/ticker — options bar history")
    # Try a known liquid AAPL contract — current month ATM-ish
    probe("https://api.polygon.io/v2/aggs/ticker/O:AAPL250620C00200000/range/1/day/2025-12-01/2026-05-06?apiKey=" + POLY_KEY,
          "AAPL option daily bars")

    section("4) Probe /v3/trades — options trades for sweep detection")
    probe("https://api.polygon.io/v3/trades/O:AAPL250620C00200000?limit=5&apiKey=" + POLY_KEY,
          "AAPL option trades")

    section("5) Probe /v2/snapshot/options — alternative endpoint")
    probe("https://api.polygon.io/v2/snapshot/options/O:AAPL250620C00200000?apiKey=" + POLY_KEY,
          "AAPL option v2 snapshot")

    section("6) Probe /v1/last/options — latest quote")
    probe("https://api.polygon.io/v1/last/nbbo/AAPL?apiKey=" + POLY_KEY,
          "AAPL latest NBBO")

    section("7) Probe stable equity quote (baseline — should always work)")
    probe("https://api.polygon.io/v3/snapshot/locale/us/markets/stocks/tickers/AAPL?apiKey=" + POLY_KEY,
          "AAPL equity snapshot")

    section("8) Test FINRA short interest data (free, no key needed)")
    # FINRA RegSHO daily file — free, gives short volume by symbol
    today = time.strftime("%Y%m%d", time.gmtime(time.time() - 86400 * 2))  # 2 days ago to be safe
    probe("https://cdn.finra.org/equity/regsho/daily/CNMSshvol" + today + ".txt",
          "FINRA daily short volume " + today)


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
    with open(os.path.join(out, "phase_x1_options_probe.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
