#!/usr/bin/env python3
"""Make the Python deploy gate peel the heatmap Yahoo line the way Pages does.

tests/helpers/worker-userstore-preservation.cjs peelHeatRange maps the stored
worker (4c4c7c9b, `const yUrl = range === "max"`) onto the peeled blob
(128f5848, `|| forceYahoo`) before the userstore fixture is checked.
tests/deployment/worker_userstore_preservation.py hashed the raw file and
asserted after_sha256, so deploy-lambdas preflight died and justhodl-corp-bonds
never took the company book.

Do not change transition.json. Do not add a yUrl replacement. Do not edit the worker.
"""
import hashlib
from pathlib import Path

TARGET = Path("tests/deployment/worker_userstore_preservation.py")
FIXTURE = Path("tests/fixtures/worker-userstore/transition.json")
OLD_SHA = "f0cee5579b994d3b7ca95f493ca4375a8c874f602e75792e443679a21a8c6476"
PEELED = "128f5848c65c334aaddc03b668de0f0a025a7b14fb69e6a00ac555a42dae1015"
NEEDLE = "R=Path(__file__).resolve().parents[2]\ndef normalize(raw,file):\n"
INSERT = '''R=Path(__file__).resolve().parents[2]
# Same peel as tests/helpers/worker-userstore-preservation.cjs peelHeatRange.
# Stored worker is the raw blob; fixture after_sha256 is the peeled blob.
_HEAT_AFTER = "4c4c7c9bb4ebec4bea6a733c722bda35991e8ea1ac3500d9a35d1dcbd41b4c99"
_HEAT_BEFORE = "128f5848c65c334aaddc03b668de0f0a025a7b14fb69e6a00ac555a42dae1015"
_HEAT_FROM = '        const yUrl = range === "max"\\n'
_HEAT_TO = '        const yUrl = range === "max" || forceYahoo\\n'

def peel_heat_range(raw, file):
    if file != "cloudflare/workers/justhodl-data-proxy/src/index.js":
        return raw
    if hashlib.sha256(raw.encode()).hexdigest() != _HEAT_AFTER:
        return raw
    assert raw.count(_HEAT_FROM) == 1, "yf range"
    raw = raw.replace(_HEAT_FROM, _HEAT_TO, 1)
    assert hashlib.sha256(raw.encode()).hexdigest() == _HEAT_BEFORE, file
    return raw

def normalize(raw,file):
    raw = peel_heat_range(raw, file)
'''


def main():
    fixture = FIXTURE.read_text(encoding="utf-8")
    if f'"after_sha256": "{PEELED}"' not in fixture:
        raise SystemExit("py peel: fixture after_sha256 is not the peeled hash; not touching it")
    if 'const yUrl = range === \\"max\\"' in fixture or "forceYahoo" in fixture:
        raise SystemExit("py peel: fixture already contains a Yahoo line; refusing")
    text = TARGET.read_text(encoding="utf-8")
    if "def peel_heat_range(" in text:
        print("python peel already present")
        return
    got = hashlib.sha256(text.encode()).hexdigest()
    if got != OLD_SHA:
        raise SystemExit("py peel: target hash " + got + " is not " + OLD_SHA)
    if text.count(NEEDLE) != 1:
        raise SystemExit("py peel: needle missing")
    text = text.replace(NEEDLE, INSERT, 1)
    if text.count("def peel_heat_range(") != 1 or text.count("raw = peel_heat_range(raw, file)") != 1:
        raise SystemExit("py peel: rewrite did not land once")
    TARGET.write_text(text, encoding="utf-8")
    compile(text, str(TARGET), "exec")
    print("python deploy gate peels the heatmap line")


if __name__ == "__main__":
    main()
