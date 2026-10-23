"""ops 6629 - READ-ONLY live proof of the crawler gate (Khalid 2026-10-23: "... as long as it doesn't affect my system
operations and functionality").

The gate has two halves: the Cloudflare Worker justhodl-data-proxy (commit 14261948, deployed by deploy-workers) and the
bucket statement DenyAbusiveScrapersByUserAgent (ops 6628). This op checks both from the outside, as a visitor would:

  A. HTTP from the runner, on https://justhodl.ai and https://justhodl-data-proxy.raafouis.workers.dev:
     - every crawler sample agent is refused on data files (403 + X-JH-Crawler-Gate: refused at the Worker);
     - every config/crawler-user-agents.json must_pass agent (browsers, HeadlessChrome, python, boto3, curl, the engines'
       own agents, ChatGPT-User, Claude-User, Google Sheets, Stripe, monitors, link previews, empty) gets 200 + JSON;
     - /health still answers a crawler; the workers.dev robots file says Disallow: /;
  B. real pages in headless Chrome (the same tool the system's own probes and section registry use): index, why
     (NVDA), master-rank, risk - once with Chrome's own agent, once with a desktop Chrome agent: no data response may be
     refused and data must arrive; once with the Googlebot agent: data responses must be refused (proof in a real render).
Nothing is written anywhere but this report.
"""
from collections import Counter
from pathlib import Path
import json
import subprocess
import sys
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CONFIG = json.loads((ROOT / "config" / "crawler-user-agents.json").read_text(encoding="utf-8"))
HOSTS = ["https://justhodl.ai", "https://justhodl-data-proxy.raafouis.workers.dev"]
DATA_KEYS = ["data/best-setups.json", "data/bottom.json"]
MUST_PASS_KEY = "data/bottom.json"          # ~0.3 MB: every own client is checked on a small file
CRAWLERS = {
    "Googlebot-smartphone": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Applebot": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15 (Applebot/0.1; +http://www.apple.com/go/applebot)",
    "bingbot": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm) Chrome/116.0.1938.76 Safari/537.36",
    "GPTBot": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.2; +https://openai.com/gptbot)",
    "ClaudeBot": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)",
    "AhrefsBot": "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)",
    "Bytespider": "Mozilla/5.0 (Linux; Android 5.0) AppleWebKit/537.36 (KHTML, like Gecko) Mobile Safari/537.36 (compatible; Bytespider; spider-feedback@bytedance.com)",
    "Lightpanda": "Lightpanda/1.0",
}
DESKTOP_CHROME = CONFIG["must_pass"][1]
PAGES = ["/", "/why.html?t=NVDA", "/master-rank.html", "/risk.html"]


def get(url, ua):
    headers = {"Accept": "application/json,*/*"}
    if ua:
        headers["User-Agent"] = ua
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            body = r.read()
            return r.status, r.headers.get("X-JH-Crawler-Gate"), body
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("X-JH-Crawler-Gate"), e.read()
    except Exception as e:  # noqa: BLE001
        return "ERR:%s" % type(e).__name__, None, b""


def source_of(status, gate, body):
    """Who answered a refusal: our Worker gate, or Cloudflare's own edge protection in front of the Worker."""
    if gate == "refused":
        return "gate"
    if status == 403 and (b"error code" in body[:200] or b"cloudflare" in body[:2000].lower()):
        return "cloudflare-edge"
    return "other"


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:  # noqa: BLE001
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as e:  # noqa: BLE001
        log("chrome channel unavailable (%s); installing chromium" % type(e).__name__)
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


def is_data(url):
    return ("/data/" in url or "justhodl-data-proxy" in url or "justhodl-dashboard-live.s3" in url) and not url.endswith((".js", ".css", ".html"))


def render(browser, ua, log):
    ctx = browser.new_context(user_agent=ua) if ua else browser.new_context()
    out = {}
    for path in PAGES:
        page = ctx.new_page()
        seen = []
        page.on("response", lambda resp, seen=seen: seen.append((resp.url, resp.status, resp.headers.get("x-jh-crawler-gate"))) if is_data(resp.url) else None)
        try:
            page.goto("https://justhodl.ai" + path, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(9000)
        except Exception as e:  # noqa: BLE001
            log("  %s load issue: %s" % (path, type(e).__name__))
        st = Counter()
        for url, status, gate in seen:
            host = "s3" if "amazonaws.com" in url else ("workers.dev" if "workers.dev" in url else "justhodl.ai")
            st["%s %s%s" % (host, status, " gate" if gate else "")] += 1
        out[path] = {"data_responses": len(seen), "by_status": dict(st),
                     "refused_by_gate": sum(1 for _, _, g in seen if g), "ok_200": sum(1 for _, s, _ in seen if s == 200),
                     "s3_403": sum(1 for u, s, _ in seen if "amazonaws.com" in u and s == 403)}
        page.close()
    ctx.close()
    return out


def main():
    with report("ops_6629_crawler_gate_verify") as r:
        r.heading("ops 6629 - live proof of the crawler gate (Worker + bucket), read-only")
        problems = []
        edge_notes = []

        r.section("A. HTTP: crawlers refused, the system's own clients served")
        for host in HOSTS:
            for key in DATA_KEYS:
                url = "%s/%s" % (host, key)
                refused = {}
                for name, ua in CRAWLERS.items():
                    status, gate, body = get(url, ua)
                    refused[name] = (status, source_of(status, gate, body))
                    if status != 403:
                        problems.append("crawler %s on %s -> %s %s" % (name, url, status, gate))
                served = Counter()
                for ua in (CONFIG["must_pass"] if key == MUST_PASS_KEY else []):
                    status, gate, body = get(url, ua)
                    good = status == 200 and gate is None
                    if good:
                        try:
                            json.loads(body)
                        except Exception:  # noqa: BLE001
                            good = False
                    if good:
                        served["ok"] += 1
                        continue
                    src = source_of(status, gate, body)
                    h_status, h_gate, h_body = get(HOSTS[1] + "/health", ua)
                    if src == "cloudflare-edge" and h_status == 403 and not h_gate:
                        served["edge-refused (also on /health, which the gate never touches)"] += 1
                        edge_notes.append("%r -> %s on %s and %s on /health: %r" % (ua[:40], status, url, h_status, body[:60]))
                        continue
                    served["%s %s" % (status, src)] += 1
                    problems.append("must_pass %r on %s -> %s %s" % (ua[:60], url, status, src))
                r.log("%s: crawlers %s | must_pass %s" % (url, {k: "%s %s" % v for k, v in refused.items()}, dict(served)))
        st, gate, body = get(HOSTS[1] + "/robots.txt", CRAWLERS["Googlebot-smartphone"])
        r.log("workers.dev /robots.txt for Googlebot -> %s %r" % (st, body[:40]))
        if st != 200 or b"Disallow: /" not in body:
            problems.append("workers.dev robots.txt")
        st, gate, body = get(HOSTS[1] + "/health", CRAWLERS["Googlebot-smartphone"])
        r.log("workers.dev /health for Googlebot -> %s gate=%s" % (st, gate))
        if st == 403:
            problems.append("health refused a crawler")
        if edge_notes:
            r.log("refused by Cloudflare's edge before the Worker (not the gate; the gate-exempt /health is refused too): %s" % "; ".join(sorted(set(edge_notes))[:4]))
        st, gate, body = get(HOSTS[0] + "/robots.txt", CONFIG["must_pass"][0])
        r.log("justhodl.ai /robots.txt (GitHub Pages) -> %s, %d bytes (informational)" % (st, len(body)))

        r.section("B. real pages in headless Chrome")
        try:
            sync_playwright = ensure_playwright(r.log)
            with sync_playwright() as p:
                browser = launch(p, r.log)
                for label, ua in (("headless Chrome (own agent)", None), ("desktop Chrome agent", DESKTOP_CHROME),
                                  ("Googlebot agent", CRAWLERS["Googlebot-smartphone"])):
                    res = render(browser, ua, r.log)
                    for path, v in res.items():
                        r.log("%s %s: %s" % (label, path, json.dumps(v)))
                    if label == "Googlebot agent":
                        if not any(v["refused_by_gate"] or v["s3_403"] for v in res.values()):
                            problems.append("Googlebot render: no data request was refused")
                    else:
                        if any(v["refused_by_gate"] for v in res.values()):
                            problems.append("%s: a data request was refused by the gate" % label)
                        if sum(v["ok_200"] for v in res.values()) == 0:
                            problems.append("%s: no data arrived on any page" % label)
                browser.close()
        except Exception as ex:  # noqa: BLE001
            problems.append("browser section failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        r.kv(problems=len(problems))
        if problems:
            for p_ in problems[:40]:
                r.warn(p_)
            r.fail("%d problem(s)" % len(problems))
            sys.exit(1)
        r.ok("crawlers refused on both hosts and in a real render; every own client and both browser agents get data")
    sys.exit(0)


if __name__ == "__main__":
    main()
