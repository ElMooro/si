#!/usr/bin/env python3
"""Map watchlist codes to FRED series whose name and units match. Correct CIR."""
import hashlib, json, pathlib, sys

# CIR is core inflation YoY, not the 3-month interbank rate.
CIR_FIX = {
    "ECONOMICS:BECIR": "FRED:CPGRLE01BEM659N",
    "ECONOMICS:CACIR": "FRED:CPGRLE01CAM659N",
    "ECONOMICS:CLCIR": "FRED:CPGRLE01CLM659N",
    "ECONOMICS:DECIR": "FRED:CPGRLE01DEM659N",
    "ECONOMICS:EECIR": "FRED:CPGRLE01EEM659N",
    "ECONOMICS:FICIR": "FRED:CPGRLE01FIM659N",
    "ECONOMICS:HUCIR": "FRED:CPGRLE01HUM659N",
    "ECONOMICS:IECIR": "FRED:CPGRLE01IEM659N",
    "ECONOMICS:ILCIR": "FRED:CPGRLE01ILM659N",
    "ECONOMICS:ISCIR": "FRED:CPGRLE01ISM659N",
    "ECONOMICS:JPCIR": "FRED:CPGRLE01JPM659N",
    "ECONOMICS:NOCIR": "FRED:CPGRLE01NOM659N",
    "ECONOMICS:PLCIR": "FRED:CPGRLE01PLM659N",
    "ECONOMICS:PTCIR": "FRED:CPGRLE01PTM659N",
    "ECONOMICS:SICIR": "FRED:CPGRLE01SIM659N",
    "ECONOMICS:SKCIR": "FRED:CPGRLE01SKM659N",
    "ECONOMICS:TRCIR": "FRED:CPGRLE01TRM659N",
    "ECONOMICS:USCIR": "FRED:CPGRLE01USM659N",
}
# No core-CPI YoY series on FRED. Do not leave these on the interbank rate.
CIR_DROP = {"ECONOMICS:CNCIR", "ECONOMICS:EUCIR"}
ADD = """
ECONOMICS:BRMPRYY|FRED:PRMNTO01BRA657S
ECONOMICS:CLRSYY|FRED:SLRTTO01CLQ659S
ECONOMICS:CLCA|FRED:BPBLTT01CLQ637S
ECONOMICS:CNCA|FRED:BPBLTT01CNQ637S
ECONOMICS:CNEXPYY|FRED:XTEXVA01CNM659S
ECONOMICS:CNM1|FRED:MYAGM1CNM189N
ECONOMICS:CNM2|FRED:MYAGM2CNM189N
ECONOMICS:DECA|FRED:BPBLTT01DEQ637S
ECONOMICS:DEM1|FRED:MYAGM1DEM189S
ECONOMICS:DKCIR|FRED:CPGRLE01DKM659N
ECONOMICS:ESCA|FRED:BPBLTT01ESQ637S
ECONOMICS:ESM1|FRED:MYAGM1ESM189N
ECONOMICS:ESM2|FRED:MYAGM2ESM189N
ECONOMICS:EUCA|FRED:BPBLTT01EZQ637S
ECONOMICS:FICA|FRED:BPBLTT01FIQ637S
ECONOMICS:FRCA|FRED:BPBLTT01FRQ637S
ECONOMICS:FRM2|FRED:MYAGM2FRM189N
ECONOMICS:FRMPRYY|FRED:PRMNTO01FRA657S
ECONOMICS:GBCA|FRED:BPBLTT01GBQ637S
ECONOMICS:GBM0|FRED:MYAGM0GBM189N
ECONOMICS:GRCA|FRED:BPBLTT01GRQ637S
ECONOMICS:GRCIR|FRED:CPGRLE01GRM659N
ECONOMICS:INCA|FRED:BPBLTT01INQ637S
ECONOMICS:ITCA|FRED:BPBLTT01ITQ637S
ECONOMICS:ITM2|FRED:MYAGM2ITM189N
ECONOMICS:ITMPRYY|FRED:PRMNTO01ITA657S
ECONOMICS:JPCA|FRED:BPBLTT01JPQ637S
ECONOMICS:JPEXPYY|FRED:XTEXVA01JPM659S
ECONOMICS:JPM3|FRED:MYAGM3JPM189N
ECONOMICS:MXCA|FRED:BPBLTT01MXQ637S
ECONOMICS:NORSYY|FRED:SLRTTO01NOQ659S
ECONOMICS:PLM0|FRED:MYAGM0PLM189N
ECONOMICS:RUCA|FRED:BPBLTT01RUQ637S
ECONOMICS:TRCA|FRED:BPBLTT01TRQ637S
ECONOMICS:USCA|FRED:IEABC
ECONOMICS:USMPRYY|FRED:PRMNTO01USA657S
ECONOMICS:USTOT|FRED:W369RG3Q066SBEA
ECONOMICS:ZAM0|FRED:MYAGM0ZAM189N
""".strip().split("\n")

ROOT = pathlib.Path(__file__).resolve()
while ROOT != ROOT.parent and not (ROOT / "jh-chart-tvwatch.js").exists():
    ROOT = ROOT.parent
TV = ROOT / "jh-chart-tvwatch.js"
FIX = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"
BEFORE = ROOT / "tests/fixtures/watchlist-correctness/jh-chart-tvwatch.js.txt"
TEST = ROOT / "tests/watchlist-identity.test.js"
MARKER = "ECONOMICS:DKCIR|FRED:CPGRLE01DKM659N"
COUNT_OLD = "assert.equal(providerEntries.length,190);"

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
if MARKER in text and "ECONOMICS:USCIR|FRED:CPGRLE01USM659N" in text and "ECONOMICS:CNCIR|" not in text:
    print("already applied")
    sys.exit(0)
fn = text.find("function providerRest(u)")
if fn < 0:
    sys.exit("no providerRest")
key = 'var raw = "'
at = text.find(key, fn)
end = text.find('", lines = raw.split("\\n"), i, p, a, b;', at)
if at < 0 or end < 0 or text.find("function providerRest", fn + 1) != -1:
    sys.exit("raw bounds")
start = at + len(key)
raw = text[start:end]
lines = raw.split("\\n")
if len(lines) != 190:
    sys.exit("line count %s" % len(lines))
new_lines = []
seen = set()
fixed = 0
dropped = 0
for line in lines:
    sym, sep, rest = line.partition("|")
    if sep != "|" or not rest:
        sys.exit("bad line")
    if sym in CIR_DROP:
        if rest != "FRED:IR3TIB01" + ("CN" if sym.endswith("CNCIR") else "EZ") + "M156N":
            # exact old ids
            pass
        if sym == "ECONOMICS:CNCIR" and rest != "FRED:IR3TIB01CNM156N":
            sys.exit("cncir " + rest)
        if sym == "ECONOMICS:EUCIR" and rest != "FRED:IR3TIB01EZM156N":
            sys.exit("eucir " + rest)
        dropped += 1
        continue
    if sym in CIR_FIX:
        if not rest.startswith("FRED:IR3TIB01"):
            sys.exit("cir not interbank " + line)
        line = sym + "|" + CIR_FIX[sym]
        fixed += 1
    if sym in seen:
        sys.exit("dup " + sym)
    seen.add(sym)
    new_lines.append(line)
if fixed != len(CIR_FIX) or dropped != len(CIR_DROP):
    sys.exit("fix %s drop %s" % (fixed, dropped))
for line in ADD:
    sym, sep, rest = line.partition("|")
    if sym in seen:
        sys.exit("add exists " + sym)
    if not rest.startswith("FRED:") or not rest[5:].isalnum():
        sys.exit("bad id " + line)
    seen.add(sym)
    new_lines.append(line)
if len(new_lines) != 226:
    sys.exit("new count %s" % len(new_lines))
new_raw = "\\n".join(new_lines)
if "\\n\\n" in new_raw or new_raw.startswith("\\n"):
    sys.exit("blank")
new = text[:start] + new_raw + text[end:]
if new.count(MARKER) != 1 or new.count("function providerRest(") != 1:
    sys.exit("splice")
if "ECONOMICS:CNCIR|" in new or "ECONOMICS:EUCIR|" in new:
    sys.exit("drop failed")
if "FRED:IR3TIB01" in new[start:start+len(new_raw)]:
    sys.exit("interbank remains in providerRest")
for name in ("function extraChart(", "function fredId(", "function chartSymbol("):
    if text.count(name) != 1 or new.count(name) != 1:
        sys.exit("helper " + name)
    # byte identical
    # located via count only; full identity test checks source

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
for e in reversed(edits[last+1:]):
    if prev[e["start"]:e["end"]] != e["after"]:
        sys.exit("post-coverage mismatch at %s" % e["start"])
    prev = prev[:e["start"]] + e["before"] + prev[e["end"]:]
for e in reversed(edits[:last+1]):
    if e.get("scope") != "watchlist-handoff-coverage-2":
        continue
    if prev[e["start"]:e["end"]] != e["after"]:
        sys.exit("coverage mismatch at %s" % e["start"])
    prev = prev[:e["start"]] + e["before"] + prev[e["end"]:]

test = TEST.read_text()
if test.count(COUNT_OLD) != 1:
    sys.exit("identity count anchor %s" % test.count(COUNT_OLD))
test = test.replace(COUNT_OLD, "assert.equal(providerEntries.length,226);", 1)
entry["edits"] = edits
entry["after_sha256"] = sha(new.encode())
if entry.get("before_sha256") != "41b1c217e58942d501eda70d07792522f45115cb1a6cad048372683676917e33":
    sys.exit("before hash moved")
TV.write_text(new)
FIX.write_text(json.dumps(fix, indent=2) + "\n")
TEST.write_text(test)
print("ok bytes", len(new.encode()), "lines", len(new_lines), "edits", len(edits))
