"""ops 1102 — network-level diagnosis of justhodl.ai slow load.

Tests from inside AWS (can reach the public internet):
  1. TTFB + total load of justhodl.ai homepage (GitHub Pages serve speed)
  2. Same for a couple other pages
  3. WSS endpoint reachability (dead WebSocket → reconnect storms = janky page)
  4. signal-board.json at root (404 confirmed) vs data/ (correct location)
"""
import json, os, time, socket, ssl, urllib.request
from datetime import datetime, timezone

REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def timed_get(url, timeout=30):
    out = {"url": url}
    try:
        t0 = time.time()
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/Diag", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            first = time.time()
            body = r.read()
            done = time.time()
            out["status"] = r.status
            out["ttfb_s"] = round(first - t0, 3)
            out["total_s"] = round(done - t0, 3)
            out["size_kb"] = round(len(body) / 1024, 1)
            out["server"] = r.headers.get("Server", "")
            out["cache"] = r.headers.get("X-Cache", r.headers.get("CF-Cache-Status", r.headers.get("Age", "")))
            out["via"] = r.headers.get("Via", "")
    except Exception as e:
        out["err"] = str(e)[:200]
    return out


def test_wss(host, port=443, timeout=8):
    """Try a TLS socket to the WSS host — if it refuses/times out, endpoint is dead."""
    out = {"host": host, "port": port}
    try:
        t0 = time.time()
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                out["tls_connect_s"] = round(time.time() - t0, 3)
                out["reachable"] = True
    except Exception as e:
        out["reachable"] = False
        out["err"] = str(e)[:150]
    return out


def main():
    report = {"generated_at": datetime.now(timezone.utc).isoformat()}

    print("1) Homepage TTFB (3 samples)...")
    report["homepage"] = [timed_get("https://justhodl.ai/") for _ in range(3)]
    report["homepage_index"] = timed_get("https://justhodl.ai/index.html")

    print("2) Other pages...")
    report["pages"] = {
        "compass": timed_get("https://justhodl.ai/compass.html"),
        "tax_plan": timed_get("https://justhodl.ai/tax-plan.html"),
        "charts": timed_get("https://justhodl.ai/charts.html"),
    }

    print("3) WSS endpoint health...")
    report["wss"] = test_wss("q7vco36knh.execute-api.us-east-1.amazonaws.com")

    print("4) Deferred JS scripts...")
    report["scripts"] = {
        "wss_client": timed_get("https://justhodl.ai/wss-client.js"),
        "tenor": timed_get("https://justhodl.ai/tenor-signals.js"),
        "liq_credit": timed_get("https://justhodl.ai/liquidity-credit.js"),
        "liq_pulse": timed_get("https://justhodl.ai/liquidity-pulse.js"),
    }

    print("5) signal-board.json location check...")
    report["signal_board_root"] = timed_get("https://justhodl-dashboard-live.s3.amazonaws.com/signal-board.json")
    report["signal_board_data"] = timed_get("https://justhodl-dashboard-live.s3.amazonaws.com/data/signal-board.json")

    # Diagnosis
    diag = []
    hp = [h for h in report["homepage"] if h.get("total_s")]
    if hp:
        avg = sum(h["total_s"] for h in hp) / len(hp)
        report["homepage_avg_total_s"] = round(avg, 3)
        if avg > 2:
            diag.append(f"Homepage serve is SLOW: avg {round(avg,2)}s total. GitHub Pages/CDN issue.")
        else:
            diag.append(f"Homepage serve is FAST: avg {round(avg,2)}s. Slowness is client-side (scripts/data/WSS), not serve.")
    if not report["wss"].get("reachable"):
        diag.append(f"WSS endpoint DEAD ({report['wss'].get('err','')}). wss-client.js retries forever → console errors + perceived jank. Consider disabling if WebSocket backend was removed.")
    else:
        diag.append(f"WSS endpoint reachable in {report['wss'].get('tls_connect_s')}s.")
    if report["signal_board_root"].get("status") != 200 and report["signal_board_data"].get("status") == 200:
        diag.append("CONFIRMED: signal-board.json is at data/ but index.html loads it from root → flagship card 404s. One-line fix.")
    report["diagnosis"] = diag

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1102.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("\n" + "=" * 60)
    print("NETWORK DIAGNOSIS")
    print("=" * 60)
    print(f"Homepage avg total: {report.get('homepage_avg_total_s','?')}s")
    print(f"  TTFB samples: {[h.get('ttfb_s') for h in report['homepage']]}")
    print(f"  Server: {report['homepage'][0].get('server')}  Via: {report['homepage'][0].get('via')}")
    print(f"WSS reachable: {report['wss'].get('reachable')}")
    print(f"signal-board root: {report['signal_board_root'].get('status')} | data/: {report['signal_board_data'].get('status')}")
    print(f"\nDIAGNOSIS:")
    for d in diag:
        print(f"  • {d}")


if __name__ == "__main__":
    main()
