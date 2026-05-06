"""Final verification of brief.html deployment.

1. Hit https://justhodl.ai/brief.html — confirm 200 + size + key text markers
2. Hit data/ai-brief.json — confirm fresh + has model + brief_md
3. Check brief tab appears in nav of 5 different pages
"""
import urllib.request
import urllib.error
import json
from ops_report import report


UA = {"User-Agent": "justhodl-audit/1.0"}


def fetch(url):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="replace"), None
    except urllib.error.HTTPError as e:
        return e.code, "", str(e)
    except Exception as e:
        return None, "", str(e)


def main():
    with report("verify_brief_deploy") as r:
        # 1. brief.html live
        r.heading("1) brief.html on production")
        code, body, err = fetch("https://justhodl.ai/brief.html")
        if err:
            r.log(f"  ✗ {code}: {err}")
        else:
            r.log(f"  ✓ status: {code}, size: {len(body):,}b")
            checks = [
                ("title tag", '<title>Brief · JustHodl</title>' in body),
                ("nav active", 'class="tab active" href="/brief.html"' in body),
                ("marked.js loaded", 'cdnjs.cloudflare.com/ajax/libs/marked' in body),
                ("snapshot grid id", 'id="snap-grid"' in body),
                ("S3 fetch URL", 'data/ai-brief.json' in body),
                ("status pill", 'id="status"' in body),
                ("auto-refresh", 'setInterval(load' in body),
            ]
            for label, ok in checks:
                r.log(f"    {'✓' if ok else '✗'} {label}")

        # 2. data/ai-brief.json freshness
        r.heading("2) data/ai-brief.json freshness")
        code, body, err = fetch("https://justhodl-dashboard-live.s3.amazonaws.com/data/ai-brief.json")
        if err:
            r.log(f"  ✗ {err}")
        else:
            d = json.loads(body)
            r.log(f"  ✓ status: {code}, size: {len(body):,}b")
            r.log(f"    generated_at: {d.get('generated_at')}")
            r.log(f"    model:        {d.get('model')}")
            r.log(f"    brief chars:  {len(d.get('brief_md',''))}")
            r.log(f"    duration_s:   {d.get('duration_s')}")
            r.log(f"    error:        {d.get('error')}")
            usage = d.get("usage") or {}
            r.log(f"    usage:        in={usage.get('input_tokens')} out={usage.get('output_tokens')}")
            r.log(f"    snapshot keys: {list((d.get('snapshot') or {}).keys())}")

        # 3. Brief tab in nav across pages
        r.heading("3) Brief tab visible in 8 pages on production")
        sample = ["today.html", "accuracy.html", "sectors.html", "allocator.html",
                  "vol.html", "momentum.html", "research.html", "intelligence.html"]
        ok_count = 0
        for p in sample:
            code, body, err = fetch(f"https://justhodl.ai/{p}")
            has_brief = (
                'href="/brief.html"' in body or
                'href="brief.html"' in body or
                'Brief' in body and 'brief.html' in body
            )
            if has_brief:
                ok_count += 1
                r.log(f"  ✓ {p:25s} (Brief tab present)")
            else:
                r.log(f"  ✗ {p:25s} (Brief tab MISSING — propagation delay or unmatched nav style)")
        r.log(f"  → {ok_count}/{len(sample)} pages have Brief link")


if __name__ == "__main__":
    main()
