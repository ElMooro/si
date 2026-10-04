#!/usr/bin/env python3
"""Alias TVC US yield spellings that end in Y onto the constant-maturity series already routed.

TVC:US01 is already FRED:DGS1. The watchlist also asks for TVC:US01Y. Same instrument, daily H.15.
Does not add a vendor. Does not touch COT. Skips a key that is already present.
"""
import hashlib, json, pathlib, sys

ADD = [
    "TVC:US01Y|FRED:DGS1",
    "TVC:US02Y|FRED:DGS2",
    "TVC:US03Y|FRED:DGS3",
    "TVC:US05Y|FRED:DGS5",
    "TVC:US10Y|FRED:DGS10",
    "TVC:US30Y|FRED:DGS30",
    "TVC:US01MY|FRED:DGS1MO",
    "TVC:US03MY|FRED:DGS3MO",
    "TVC:US06MY|FRED:DGS6MO",
]
MARKER = "TVC:US01Y|FRED:DGS1"
OK = {
    "TVC:US01Y": "FRED:DGS1",
    "TVC:US02Y": "FRED:DGS2",
    "TVC:US03Y": "FRED:DGS3",
    "TVC:US05Y": "FRED:DGS5",
    "TVC:US10Y": "FRED:DGS10",
    "TVC:US30Y": "FRED:DGS30",
    "TVC:US01MY": "FRED:DGS1MO",
    "TVC:US03MY": "FRED:DGS3MO",
    "TVC:US06MY": "FRED:DGS6MO",
}

ROOT = pathlib.Path(__file__).resolve()
while ROOT != ROOT.parent and not (ROOT / "jh-chart-tvwatch.js").exists():
    ROOT = ROOT.parent
TV = ROOT / "jh-chart-tvwatch.js"
FIX = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"
BEFORE = ROOT / "tests/fixtures/watchlist-correctness/jh-chart-tvwatch.js.txt"
TEST = ROOT / "tests/watchlist-identity.test.js"

def sha(b):
    return hashlib.sha256(b).hexdigest()

def reverse(text, edits):
    for e in reversed(edits):
        got = text[e["start"]:e["end"]]
        if got != e["after"]:
            sys.exit("reverse mismatch at %s" % e["start"])
        text = text[:e["start"]] + e["before"] + text[e["end"]:]
    return text

text = TV.read_text()
if MARKER in text:
    print("already applied")
    sys.exit(0)
fn = text.find("function providerRest(u)")
if fn < 0:
    sys.exit("no providerRest")
key = 'var raw = "'
at = text.find(key, fn)
end = text.find('", lines = raw.split("\\n"), i, p, a, b;', at)
if at < 0 or end < 0:
    sys.exit("raw bounds")
start = at + len(key)
raw = text[start:end]
lines = raw.split("\\n")
seen = set()
for line in lines:
    sym, sep, rest = line.partition("|")
    if sep != "|" or not rest or sym in seen:
        sys.exit("bad existing " + line[:80])
    seen.add(sym)
new_lines = list(lines)
added = []
for line in ADD:
    sym, sep, rest = line.partition("|")
    if sep != "|" or OK.get(sym) != rest:
        sys.exit("bad add " + line)
    if sym in seen:
        if rest not in raw:
            sys.exit("conflict " + sym)
        continue
    seen.add(sym)
    new_lines.append(line)
    added.append(line)
if not added:
    print("nothing to add")
    sys.exit(0)
new_raw = "\\n".join(new_lines)
if "\\n\\n" in new_raw or new_raw.startswith("\\n") or new_raw.endswith("\\n"):
    sys.exit("blank")
new = text[:start] + new_raw + text[end:]
if new.count(MARKER) != 1 or new.count("function providerRest(") != 1:
    sys.exit("splice")
fix = json.loads(FIX.read_text())
entry = fix["jh-chart-tvwatch.js"]
if sha(text.encode()) != entry["after_sha256"]:
    sys.exit("working tree hash != fixture")
edit = {"start": start, "end": start + len(new_raw), "before": raw, "after": new_raw, "scope": "us-yield-y-alias"}
edits = list(entry["edits"]) + [edit]
restored = reverse(new, edits)
if BEFORE.exists() and restored.encode() != BEFORE.read_bytes():
    sys.exit("preservation reverse failed")
test = TEST.read_text()
old = "assert.equal(providerEntries.length,%d);" % len(lines)
neu = "assert.equal(providerEntries.length,%d);" % len(new_lines)
if test.count(old) != 1:
    sys.exit("identity count anchor %s" % len(lines))
test = test.replace(old, neu, 1)
entry["edits"] = edits
entry["after_sha256"] = sha(new.encode())
TV.write_text(new)
FIX.write_text(json.dumps(fix, indent=2) + "\n")
TEST.write_text(test)
print("ok added", len(added), "lines", len(new_lines))
