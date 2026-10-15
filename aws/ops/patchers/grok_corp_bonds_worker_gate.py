#!/usr/bin/env python3
"""Unblock Pages for the corp bonds desk.

worker-userstore-preservation.cjs peels the heatmap Yahoo line
(`const yUrl = range === "max"` -> `|| forceYahoo`) before the
userstore fixture hash is checked. after_sha256 must stay the peeled
hash 128f5848. Commit 8a1b9c5 set it to the raw worker hash and added
a second peel of that same line, so three worker tests fail and
corpbonds.html never reaches the site.

Also bumps the desk script query so this apply dispatches Pages.
"""
from pathlib import Path

FIXTURE = Path("tests/fixtures/worker-userstore/transition.json")
HTML = Path("corpbonds.html")
RAW = "4c4c7c9bb4ebec4bea6a733c722bda35991e8ea1ac3500d9a35d1dcbd41b4c99"
PEELED = "128f5848c65c334aaddc03b668de0f0a025a7b14fb69e6a00ac555a42dae1015"
BLOCK = (
    ",\n"
    "        {\n"
    '          "after": "        const yUrl = range === \\"max\\"",\n'
    '          "before": "        const yUrl = range === \\"max\\" || forceYahoo"\n'
    "        }"
)


def main():
    text = FIXTURE.read_text(encoding="utf-8")
    if BLOCK not in text:
        if f'"after_sha256": "{PEELED}"' not in text:
            raise SystemExit("corp-bonds worker gate: fixture shape changed; not patching")
    else:
        if text.count(RAW) != 1:
            raise SystemExit("corp-bonds worker gate: raw hash missing")
        text = text.replace(RAW, PEELED, 1).replace(BLOCK, "", 1)
        if "forceYahoo" in text:
            raise SystemExit("corp-bonds worker gate: Yahoo peel still in the fixture")
        FIXTURE.write_text(text, encoding="utf-8")
        print("fixture restored to peeled hash", PEELED)
    page = HTML.read_text(encoding="utf-8")
    old = 'src="/jh-corp-bonds.js?v=20261015a"'
    new = 'src="/jh-corp-bonds.js?v=20261015b"'
    if old in page:
        HTML.write_text(page.replace(old, new, 1), encoding="utf-8")
        print("cache-bust corpbonds.html")
    elif new not in page:
        raise SystemExit("corp-bonds worker gate: desk script tag not found")
    else:
        print("desk script tag already current")


if __name__ == "__main__":
    main()
