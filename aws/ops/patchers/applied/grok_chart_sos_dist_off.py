#!/usr/bin/env python3
"""SOS and DIST stay off the chart until the STRUCT chips are turned on."""
from pathlib import Path

def repo_root():
    here = Path(__file__).resolve()
    if len(here.parents) > 3:
        return here.parents[3]
    return Path.cwd()
OFF = "{ t2: 0, b2: 0, ac: 0, dist: 0, spr: 0, ut: 0, thr: 0, sc: 0, bc: 0 }"


def locate(name):
    here = Path(name)
    if here.exists():
        return here
    root = repo_root() / name
    if root.exists():
        return root
    raise SystemExit("missing " + name)


def swap(path, old, new, label):
    text = path.read_text(encoding="utf-8")
    if new in text and old not in text:
        print("already " + label)
        return False
    count = text.count(old)
    if count != 1:
        raise SystemExit(label + " needle count " + str(count))
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    print("patched " + label)
    return True


def main():
    struct = locate("jh-chart-structure.js")
    dist = locate("jh-chart-distribution.js")
    html = locate("chart.html")
    old_block = (
        '  var K = "jhStruct.v2";\n'
        "  var baseOn = { t2: 1, b2: 1, ac: 1, dist: 1, spr: 1, ut: 1, thr: 1, sc: 1, bc: 1 };\n"
        "  var on = { t2: 1, b2: 1, ac: 1, dist: 1, spr: 1, ut: 1, thr: 1, sc: 1, bc: 1 };\n"
    )
    new_block = (
        '  var K = "jhStruct.v3";\n'
        "  var baseOn = " + OFF + ";\n"
        "  var on = " + OFF + ";\n"
    )
    swap(struct, old_block, new_block, "structure defaults")
    swap(
        dist,
        'arm("jh-struct-src", "/jh-chart-structure.js?v=20261003struct7");',
        'arm("jh-struct-src", "/jh-chart-structure.js?v=20261012sosoff");',
        "structure cache",
    )
    swap(
        html,
        'src="/jh-chart-distribution.js?v=20260925-dist"',
        'src="/jh-chart-distribution.js?v=20261012sosoff"',
        "chart cache",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
