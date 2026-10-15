#!/usr/bin/env python3
"""A slow FRED read was eating the whole corp-bond publish.

Twenty-three sleeves were fetched one after another at a 30s cap, so a bad
FRED hour reached the 600s task limit before the company book was written.
Fetch the sleeves together, cap each read at 12s, and stop the euro-file
search after five dates. A sleeve FRED does not return stays absent.
"""
import hashlib
from pathlib import Path

SRC = Path("aws/lambdas/justhodl-corp-bonds/source/lambda_function.py")
SRC_SHA = "fda97dba6b1e7ca69cfb8ebb23afc8443fc1ea2a8c11fcb35129f8c2cf30e32e"

OLD_FRED = 'text = _get(FRED % spec["fred"], timeout=30)'
NEW_FRED = 'text = _get(FRED % spec["fred"], timeout=12)'
OLD_INDEX = 'page = _get(ECB_INDEX, timeout=30)'
NEW_INDEX = 'page = _get(ECB_INDEX, timeout=12)'
OLD_SPAN = "for offset in range(0, 35):"
NEW_SPAN = "for offset in range(0, 5):"
OLD_FILE = "text = _get(url, timeout=40)"
NEW_FILE = "text = _get(url, timeout=12)"
OLD_LOOP = '''    sleeves = []
    for spec in core.SLEEVES:
        points, status = fetch_fred(spec)
        sources["fred"][spec["fred"]] = status
        if points:
            row = core.sleeve_stats(spec, points, today_iso)
            if row:
                sleeves.append(row)
'''
NEW_LOOP = '''    sleeves = []

    def _sleeve(spec):
        points, status = fetch_fred(spec)
        row = core.sleeve_stats(spec, points, today_iso) if points else None
        return spec["fred"], status, row

    with ThreadPoolExecutor(max_workers=8) as pool:
        for fred_id, status, row in pool.map(_sleeve, core.SLEEVES):
            sources["fred"][fred_id] = status
            if row:
                sleeves.append(row)
'''
OLD_JOIN = "issue_book = issue_future.result()"
NEW_JOIN = "issue_book = issue_future.result(timeout=120)"


def main():
    src = SRC.read_text(encoding="utf-8")
    if "def _sleeve(spec):" in src and "range(0, 5)" in src:
        print("fred already parallel")
        return
    got = hashlib.sha256(src.encode()).hexdigest()
    if got != SRC_SHA:
        raise SystemExit("fred parallel: handler hash " + got)
    for label, old, new in (
        ("fred", OLD_FRED, NEW_FRED),
        ("index", OLD_INDEX, NEW_INDEX),
        ("span", OLD_SPAN, NEW_SPAN),
        ("file", OLD_FILE, NEW_FILE),
        ("loop", OLD_LOOP, NEW_LOOP),
        ("join", OLD_JOIN, NEW_JOIN),
    ):
        n = src.count(old)
        if n != 1:
            raise SystemExit("fred parallel: %s count %s" % (label, n))
        src = src.replace(old, new, 1)
    compile(src, str(SRC), "exec")
    SRC.write_text(src, encoding="utf-8")
    print("sleeves now fetch together")


if __name__ == "__main__":
    main()
