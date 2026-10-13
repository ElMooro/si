#!/usr/bin/env python3
"""Peel the SOS/DIST cache-bust so the chart.html preservation chain still reconstructs."""
from pathlib import Path

OLD = "function normalize(raw,file){raw=peelEvr(raw,file);"
NEW = (
    "const SOS_FROM='src=\"/jh-chart-distribution.js?v=20261012sosoff\"';\n"
    "const SOS_TO='src=\"/jh-chart-distribution.js?v=20260925-dist\"';\n"
    "const SOS_AFTER='cd53800c27da1e73f840a63906da4cdfe65a4cd55d7d6b99bdff929dc18db9d9';\n"
    "const SOS_BEFORE='e27c4f331af871ee8d71835631d6276a0328ba8e301e72a38302fe016e141385';\n"
    "function peelSos(raw,file){\n"
    " if(file!=='chart.html'||hash(raw)!==SOS_AFTER)return raw;\n"
    " assert.equal(raw.split(SOS_FROM).length,2,'chart.html SOS/DIST cache');\n"
    " raw=raw.replace(SOS_FROM,SOS_TO);\n"
    " assert.equal(hash(raw),SOS_BEFORE,'chart.html');\n"
    " return raw;\n"
    "}\n"
    "function normalize(raw,file){raw=peelSos(raw,file);raw=peelEvr(raw,file);"
)


def locate():
    rel = Path("tests/helpers/chart-macro-line-preservation.cjs")
    if rel.exists():
        return rel
    root = Path(__file__).resolve().parents[3] / rel
    if root.exists():
        return root
    raise SystemExit("missing preservation helper")


def main():
    path = locate()
    text = path.read_text(encoding="utf-8")
    if "function peelSos(" in text and OLD not in text:
        print("already peeled")
        return 0
    if text.count(OLD) != 1:
        raise SystemExit("normalize needle count " + str(text.count(OLD)))
    path.write_text(text.replace(OLD, NEW, 1), encoding="utf-8")
    print("patched preservation peel")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
