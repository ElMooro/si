"""Verify the two nav fixes landed live on GitHub Pages."""
import urllib.request, time, os

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def fetch(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return r.read().decode("utf-8", "replace")


def main():
    section("Verify nav fixes on live site")
    pages = {
        "https://justhodl.ai/compound-signals.html": [
            "/compound-signals.html", "/deep-value.html", "/eps-velocity.html",
        ],
        "https://justhodl.ai/nobrainers.html": [
            "/compound-signals.html", "/deep-value.html", "/eps-velocity.html",
        ],
    }
    all_ok = True
    for url, expected in pages.items():
        try:
            html = fetch(url)
            log(f"  {url}: size={len(html)}b")
            for e in expected:
                ok = e in html
                log(f"    {'✓' if ok else '❌'} {e}")
                if not ok:
                    all_ok = False
        except Exception as e:
            log(f"  ❌ {url}: {e}")
            all_ok = False
    log("")
    log(f"  Overall: {'✓ all good' if all_ok else '❌ some failures'}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "verify_nav_fixes_live.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
