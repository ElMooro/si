#!/usr/bin/env python3
"""Map Cboe-listed ETFs to the same US ticker. Polygon grouped-daily confirmed each name and n>=8. No collisions."""
import hashlib, json, pathlib, re, sys

ADD = [
    'CBOE:AAAU|AAAU',
    'CBOE:ACWV|ACWV',
    'CBOE:BEMB|BEMB',
    'CBOE:BMNU|BMNU',
    'CBOE:CALF|CALF',
    'CBOE:CEMB|CEMB',
    'CBOE:COWZ|COWZ',
    'CBOE:DWLD|DWLD',
    'CBOE:EDEN|EDEN',
    'CBOE:EEMV|EEMV',
    'CBOE:EMHY|EMHY',
    'CBOE:ETHU|ETHU',
    'CBOE:EUV|EUV',
    'CBOE:EZU|EZU',
    'CBOE:FDEM|FDEM',
    'CBOE:FLOT|FLOT',
    'CBOE:FLQS|FLQS',
    'CBOE:FOTO|FOTO',
    'CBOE:GAA|GAA',
    'CBOE:GHYG|GHYG',
    'CBOE:GMOM|GMOM',
    'CBOE:GSEW|GSEW',
    'CBOE:GSUS|GSUS',
    'CBOE:GTIP|GTIP',
    'CBOE:GVAL|GVAL',
    'CBOE:GVI|GVI',
    'CBOE:HYBL|HYBL',
    'CBOE:IAGG|IAGG',
    'CBOE:IBIG|IBIG',
    'CBOE:IFRA|IFRA',
    'CBOE:IGEB|IGEB',
    'CBOE:IGV|IGV',
    'CBOE:INDA|INDA',
    'CBOE:ISVL|ISVL',
    'CBOE:ITB|ITB',
    'CBOE:IYT|IYT',
    'CBOE:LYTE|LYTE',
    'CBOE:MAGS|MAGS',
    'CBOE:MBBB|MBBB',
    'CBOE:MOAT|MOAT',
    'CBOE:MOTI|MOTI',
    'CBOE:MTUM|MTUM',
    'CBOE:NANC|NANC',
    'CBOE:OVL|OVL',
    'CBOE:PAVE|PAVE',
    'CBOE:PEX|PEX',
    'CBOE:RSSB|RSSB',
    'CBOE:SBTU|SBTU',
    'CBOE:SEIM|SEIM',
    'CBOE:SVIX|SVIX',
    'CBOE:SVXY|SVXY',
    'CBOE:TAIL|TAIL',
    'CBOE:TLTX|TLTX',
    'CBOE:TYA|TYA',
    'CBOE:USHY|USHY',
    'CBOE:UVXY|UVXY',
    'CBOE:VCEB|VCEB',
    'CBOE:VFMF|VFMF',
    'CBOE:VFMO|VFMO',
    'CBOE:VFMV|VFMV',
    'CBOE:VUSB|VUSB',
    'CBOE:WTAI|WTAI',
    'CBOE:ZVOL|ZVOL',
]
MARKER = "CBOE:AAAU|AAAU"
COUNT_OLD = "assert.equal(providerEntries.length,318);"
COUNT_NEW = 381
ALLOW = {line.split("|",1)[1] for line in ADD}

ROOT = pathlib.Path(__file__).resolve()
while ROOT != ROOT.parent and not (ROOT / "jh-chart-tvwatch.js").exists():
    ROOT = ROOT.parent
TV = ROOT / "jh-chart-tvwatch.js"
FIX = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"
BEFORE = ROOT / "tests/fixtures/watchlist-correctness/jh-chart-tvwatch.js.txt"
TEST = ROOT / "tests/watchlist-identity.test.js"
ECON = ROOT / "tests/fixtures/watchlist-correctness/economic-qualifications.json"

def sha(b):
    return hashlib.sha256(b).hexdigest()

def reverse(text, edits):
    for e in reversed(edits):
        got = text[e["start"]:e["end"]]
        if got != e["after"]:
            sys.exit("reverse mismatch at %s" % e["start"])
        text = text[:e["start"]] + e["before"] + text[e["end"]:]
    return text

def ok_target(t):
    return t in ALLOW and re.fullmatch(r"[A-Z][A-Z0-9]{1,9}", t) is not None

def sync_routes():
    econ = json.loads(ECON.read_text())
    routes = econ["routes"]
    changed = False
    for line in ADD:
        sym, _, rest = line.partition("|")
        if sym in routes and routes[sym] != rest:
            sys.exit("econ conflict " + sym)
        if routes.get(sym) != rest:
            routes[sym] = rest
            changed = True
    if changed:
        if len(econ.get("records") or []) != 36:
            sys.exit("econ records moved")
        ECON.write_text(json.dumps(econ, indent=2) + "\n")
    return changed

text = TV.read_text()
if MARKER in text:
    sync_routes()
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
if len(lines) != 318:
    sys.exit("line count %s" % len(lines))
if not lines[-1].startswith("TVC:RUA|"):
    sys.exit("tail moved")
seen = set()
for line in lines:
    sym, sep, rest = line.partition("|")
    if sep != "|" or not rest or sym in seen:
        sys.exit("bad existing " + line[:80])
    seen.add(sym)
new_lines = list(lines)
for line in ADD:
    sym, sep, rest = line.partition("|")
    if sep != "|" or sym in seen or not ok_target(rest):
        sys.exit("bad add " + line)
    seen.add(sym)
    new_lines.append(line)
if len(new_lines) != COUNT_NEW:
    sys.exit("new count %s" % len(new_lines))
new_raw = "\\n".join(new_lines)
if "\\n\\n" in new_raw or new_raw.startswith("\\n") or new_raw.endswith("\\n"):
    sys.exit("blank")
new = text[:start] + new_raw + text[end:]
if new.count(MARKER) != 1 or new.count("function providerRest(") != 1:
    sys.exit("splice")
for name in ("function extraChart(", "function fredId(", "function chartSymbol(", "function economicQualification("):
    if text.count(name) != 1 or new.count(name) != 1:
        sys.exit("helper " + name)
fix = json.loads(FIX.read_text())
entry = fix["jh-chart-tvwatch.js"]
if sha(text.encode()) != entry["after_sha256"]:
    sys.exit("working tree hash != fixture")
edit = {"start": start, "end": start + len(new_raw), "before": raw, "after": new_raw}
edits = list(entry["edits"]) + [edit]
restored = reverse(new, edits)
base = BEFORE.read_bytes()
if restored.encode() != base:
    rb, bb = restored.encode(), base
    n = min(len(rb), len(bb))
    i = next((k for k in range(n) if rb[k] != bb[k]), n)
    sys.exit("preservation reverse failed at %s" % i)
prev = new
scopes = [e.get("scope") for e in edits]
last = max(i for i, s in enumerate(scopes) if s == "watchlist-handoff-coverage-2")
for e in reversed(edits[last + 1:]):
    if prev[e["start"]:e["end"]] != e["after"]:
        sys.exit("post-coverage mismatch at %s" % e["start"])
    prev = prev[:e["start"]] + e["before"] + prev[e["end"]:]
for e in reversed(edits[:last + 1]):
    if e.get("scope") != "watchlist-handoff-coverage-2":
        continue
    if prev[e["start"]:e["end"]] != e["after"]:
        sys.exit("coverage mismatch at %s" % e["start"])
    prev = prev[:e["start"]] + e["before"] + prev[e["end"]:]
test = TEST.read_text()
if test.count(COUNT_OLD) != 1:
    sys.exit("identity count anchor")
test = test.replace(COUNT_OLD, "assert.equal(providerEntries.length,%d);" % COUNT_NEW, 1)
entry["edits"] = edits
entry["after_sha256"] = sha(new.encode())
if entry.get("before_sha256") != "41b1c217e58942d501eda70d07792522f45115cb1a6cad048372683676917e33":
    sys.exit("before hash moved")
TV.write_text(new)
FIX.write_text(json.dumps(fix, indent=2) + "\n")
TEST.write_text(test)
sync_routes()
print("ok bytes", len(new.encode()), "lines", len(new_lines), "edits", len(edits))
