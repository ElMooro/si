#!/usr/bin/env python3
"""Load the watchlist route table on click, and record the yield edit Pages still rejects."""
import hashlib, json, pathlib, sys

MARKER = 'src="/jh-chart-row-handoff.js?v=20261004a"'
NEEDLE = '<script src="/jh-chart-engine.js?v=20261002ac-shelves&watch=watchlist-transaction-v1"></script>'
TAG = '<script src="/jh-chart-row-handoff.js?v=20261004a"></script>'
Y = [
    ("TVC:US01Y", "FRED:DGS1"),
    ("TVC:US02Y", "FRED:DGS2"),
    ("TVC:US03Y", "FRED:DGS3"),
    ("TVC:US05Y", "FRED:DGS5"),
    ("TVC:US10Y", "FRED:DGS10"),
    ("TVC:US30Y", "FRED:DGS30"),
    ("TVC:US01MY", "FRED:DGS1MO"),
    ("TVC:US03MY", "FRED:DGS3MO"),
    ("TVC:US06MY", "FRED:DGS6MO"),
]
TRACKED = ("chart.html", "jh-chart-engine.js", "jh-chart-tvwatch.js")

ROOT = pathlib.Path(__file__).resolve()
while ROOT != ROOT.parent and not (ROOT / "chart.html").exists():
    ROOT = ROOT.parent
CHART = ROOT / "chart.html"
HAND = ROOT / "jh-chart-row-handoff.js"
FIX = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"
ECON = ROOT / "tests/fixtures/watchlist-correctness/economic-qualifications.json"
CFTC = ROOT / "tests/fixtures/chart-cftc/transition.json"

def sha(b):
    return hashlib.sha256(b).hexdigest()

def dump(obj):
    return json.dumps(obj, indent=2) + "\n"

def u16len(s):
    return len(s.encode("utf-16-le")) // 2

def u16slice(s, start, end):
    raw = s.encode("utf-16-le")
    return raw[start * 2:end * 2].decode("utf-16-le")

def u16splice(s, start, end, repl):
    raw = s.encode("utf-16-le")
    return (raw[:start * 2] + repl.encode("utf-16-le") + raw[end * 2:]).decode("utf-16-le")

def reverse(text, edits):
    for e in reversed(edits):
        got = u16slice(text, e["start"], e["end"])
        if got != e["after"]:
            sys.exit("reverse mismatch %s at %s" % (e.get("scope"), e["start"]))
        text = u16splice(text, e["start"], e["end"], e["before"])
    return text

def insert_chart(text):
    if MARKER in text:
        return text, None
    n = text.count(NEEDLE)
    if n != 1:
        sys.exit("engine script tag count %s" % n)
    at_py = text.find(NEEDLE) + len(NEEDLE)
    add = "\n" + TAG
    at = u16len(text[:at_py])
    edit = {"start": at, "end": at + u16len(add), "before": "", "after": add, "scope": "watchlist-row-handoff"}
    return text[:at_py] + add + text[at_py:], edit

def add_yields(text):
    if "TVC:US01Y" in text:
        return text
    mark = '\n  },\n  "records"'
    at = text.find(mark)
    if at < 0:
        sys.exit("economic routes boundary missing")
    lines = ",\n".join('    "%s": "%s"' % pair for pair in Y)
    return text[:at] + ",\n" + lines + text[at:]

chart = CHART.read_text()
js = HAND.read_text() if HAND.exists() else ""
if "window.jhGoSymbol" not in js or "__jhRowHandoff" not in js or "raw = " not in js:
    sys.exit("jh-chart-row-handoff.js is not on main")
chart2, edit = insert_chart(chart)
if edit and ("\n\n" + TAG) in chart2:
    sys.exit("blank line before handoff")
if chart2.count(MARKER) != 1:
    sys.exit("handoff tag count")

src_raw = FIX.read_text()
src = json.loads(src_raw)
if "chart.html" not in src or "edits" not in src["chart.html"]:
    sys.exit("source-transition has no chart.html edits")
if edit:
    src["chart.html"]["edits"] = list(src["chart.html"]["edits"]) + [edit]
    src["chart.html"]["after_sha256"] = sha(chart2.encode())

econ_raw = ECON.read_text()
econ2 = add_yields(econ_raw)
routes = json.loads(econ2)["routes"]
for k, v in Y:
    if routes.get(k) != v:
        sys.exit("yield route mismatch " + k)

cftc = json.loads(CFTC.read_text())
old = json.loads((ROOT / cftc["ledger"]["before_path"]).read_text())
files = {"chart.html": chart2}
for name in TRACKED:
    if name not in files:
        files[name] = (ROOT / name).read_text()
    if name not in old or name not in src or name not in cftc["changes"]:
        sys.exit("missing ledger row " + name)
    extra = src[name]["edits"][len(old[name]["edits"]):]
    row = cftc["changes"][name]
    if name == "chart.html" and edit:
        if extra[:-1] != row["edits"] or extra[-1] != edit:
            sys.exit("chart.html ledger slice drifted before handoff")
    elif name == "jh-chart-tvwatch.js":
        if not extra or extra[-1].get("scope") != "us-yield-y-alias":
            sys.exit("yield edit is not the last tvwatch ledger edit")
        if extra[:len(row["edits"])] != row["edits"]:
            sys.exit("tvwatch ledger prefix is not the reviewed CFTC edits")
    elif extra != row["edits"]:
        sys.exit("ledger slice drifted for " + name + " (%s vs %s)" % (len(extra), len(row["edits"])))
    row["edits"] = extra
    restored = reverse(files[name], row["edits"])
    before = (ROOT / row["before_path"]).read_text()
    if restored != before:
        sys.exit("reverse does not restore " + name)
    if sha(restored.encode()) != row["before_sha256"]:
        sys.exit("before hash drifted " + name)
    row["after_sha256"] = sha(files[name].encode())

# Other changed files keep their recorded edits. Confirm those still reverse
# before trusting the tvwatch/chart rows we just adopted.
for name, row in cftc["changes"].items():
    if name in TRACKED:
        continue
    body = (ROOT / name).read_text()
    if sha(body.encode()) != row["after_sha256"]:
        sys.exit("unrelated file hash moved " + name)
    if reverse(body, row["edits"]) != (ROOT / row["before_path"]).read_text():
        sys.exit("unrelated reverse failed " + name)

src_out = dump(src)
parsed = json.loads(src_out)
if edit and parsed["chart.html"]["edits"][-1] != edit:
    sys.exit("chart edit did not survive json")
if parsed["jh-chart-tvwatch.js"]["edits"] != src["jh-chart-tvwatch.js"]["edits"]:
    sys.exit("tvwatch ledger edits changed in the dump")
cftc["ledger"]["after_sha256"] = sha(src_out.encode())
cftc_out = dump(cftc)
if json.loads(cftc_out)["changes"]["jh-chart-tvwatch.js"]["edits"] != cftc["changes"]["jh-chart-tvwatch.js"]["edits"]:
    sys.exit("cftc dump dropped edits")

if chart2 != chart:
    CHART.write_text(chart2)
if econ2 != econ_raw:
    ECON.write_text(econ2)
if src_out != src_raw:
    FIX.write_text(src_out)
if cftc_out != CFTC.read_text():
    CFTC.write_text(cftc_out)
print("ok handoff", MARKER in chart2, "yields", "TVC:US01Y" in econ2, "tvwatch", cftc["changes"]["jh-chart-tvwatch.js"]["after_sha256"][:12])
