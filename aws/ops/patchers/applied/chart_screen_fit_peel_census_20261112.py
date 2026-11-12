#!/usr/bin/env python3
"""Teach the screen-fit peel to restore the frozen census helper bytes.

chart-imf.test.js hashes tests/helpers/chart-census-additional-preservation.cjs
after the macro-line peel chain. The helper's screen-fit require changed that
hash (7aa34b1a). Reversing that one line inside peelScreenFit puts the reviewed
bytes (3230411d) back before the assertion. The live helper is not rewritten.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MACRO = ROOT / "tests/helpers/chart-macro-line-preservation.cjs"
MARKER = "'tests/helpers/chart-census-additional-preservation.cjs'"
ANCHOR = (
    "  'chart.html':{now:'3a5abb78e73edae965c2a19fbebc943102f7a6cff35b93af81fe46cda81ca59f',"
    "prior:'11c209334122515ebe61e6ebb10f76aa3c5f05f2d2b518a82e16325613257827',"
    'pairs:[{"old": "us10y=20261021c&ldr=20261105", "new": "us10y=20261021c&fit=20261112&ldr=20261105", "n": 1}]}\n'
    " };"
)
INSERT = (
    "  'chart.html':{now:'3a5abb78e73edae965c2a19fbebc943102f7a6cff35b93af81fe46cda81ca59f',"
    "prior:'11c209334122515ebe61e6ebb10f76aa3c5f05f2d2b518a82e16325613257827',"
    'pairs:[{"old": "us10y=20261021c&ldr=20261105", "new": "us10y=20261021c&fit=20261112&ldr=20261105", "n": 1}]},\n'
    "  'tests/helpers/chart-census-additional-preservation.cjs':{now:'7aa34b1a8a74cf1c0f092c1498cc8e557ec29b1e6b5398a85f44f3e1fb65fe0c',"
    "prior:'3230411de4b28694ce88a73a87fcb7acb21f59a243f09366a32e73f8f74d1b48',"
    'pairs:[{"old": "function normalize(raw,file){raw=require(\\"./chart-imf-preservation.cjs\\").normalize(raw,file);", '
    '"new": "function normalize(raw,file){raw=require(\\"./chart-macro-line-preservation.cjs\\").peelScreenFit(raw,file);raw=require(\\"./chart-imf-preservation.cjs\\").normalize(raw,file);", '
    '"n": 1}]}\n'
    " };"
)


def main():
    text = MACRO.read_text(encoding="utf-8")
    if MARKER in text:
        print("census helper peel already present")
        return
    n = text.count(ANCHOR)
    if n != 1:
        raise SystemExit("anchor count %s" % n)
    MACRO.write_text(text.replace(ANCHOR, INSERT, 1), encoding="utf-8")
    print("added census helper to screen-fit peel")


if __name__ == "__main__":
    main()
