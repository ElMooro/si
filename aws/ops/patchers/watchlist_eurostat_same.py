#!/usr/bin/env python3
"""Map dark watchlist economics to the same Eurostat series. Each id was probed: n>=8 and the title is the same concept."""
import hashlib, json, pathlib, re, sys

ADD = [
    'ECONOMICS:ALGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.AL',
    'ECONOMICS:ATGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.AT',
    'ECONOMICS:BEGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.BE',
    'ECONOMICS:BGGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.BG',
    'ECONOMICS:BGGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.BG',
    'ECONOMICS:CHUR|eurostat:UNE_RT_M:M.SA.TOTAL.PC_ACT.T.CH',
    'ECONOMICS:CYGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.CY',
    'ECONOMICS:CYGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.CY',
    'ECONOMICS:CZGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.CZ',
    'ECONOMICS:DEGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.DE',
    'ECONOMICS:DKGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.DK',
    'ECONOMICS:EEGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.EE',
    'ECONOMICS:ESGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.ES',
    'ECONOMICS:FIGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.FI',
    'ECONOMICS:FIUR|eurostat:UNE_RT_M:M.SA.TOTAL.PC_ACT.T.FI',
    'ECONOMICS:FRGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.FR',
    'ECONOMICS:GRGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.EL',
    'ECONOMICS:HRGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.HR',
    'ECONOMICS:HRGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.HR',
    'ECONOMICS:HUGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.HU',
    'ECONOMICS:IEGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.IE',
    'ECONOMICS:ITGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.IT',
    'ECONOMICS:JPUR|eurostat:UNE_RT_M:M.SA.TOTAL.PC_ACT.T.JP',
    'ECONOMICS:LTGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.LT',
    'ECONOMICS:LUGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.LU',
    'ECONOMICS:LVGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.LV',
    'ECONOMICS:MEGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.ME',
    'ECONOMICS:MKGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.MK',
    'ECONOMICS:MTGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.MT',
    'ECONOMICS:MTGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.MT',
    'ECONOMICS:NLGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.NL',
    'ECONOMICS:PLGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.PL',
    'ECONOMICS:PTGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.PT',
    'ECONOMICS:ROGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.RO',
    'ECONOMICS:ROGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.RO',
    'ECONOMICS:RSGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.RS',
    'ECONOMICS:SEGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.SE',
    'ECONOMICS:SIGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.SI',
    'ECONOMICS:SKGDG|eurostat:GOV_10Q_GGDEBT:Q.GD.S13.PC_GDP.SK',
]
MARKER = "ECONOMICS:ALGDPYY|eurostat:NAMQ_10_GDP:Q.CLV_PCH_SM.SCA.B1GQ.AL"
COUNT_OLD = "assert.equal(providerEntries.length,381);"
COUNT_NEW = 420
SID = re.compile(r"^eurostat:(?:UNE_RT_M|GOV_10Q_GGDEBT|NAMQ_10_GDP):[A-Z0-9._]+$")

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
    return SID.fullmatch(t) is not None

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
if len(lines) != 381:
    sys.exit("line count %s" % len(lines))
if not lines[-1].startswith("CBOE:ZVOL|"):
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
