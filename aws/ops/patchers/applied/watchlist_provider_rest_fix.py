#!/usr/bin/env python3
"""Route probed GDP, CPI, unemployment and cash indexes without touching the frozen extraChart map.

The identity guard hashes extraChart, fredId and chartSymbol and reverses six
coverage edits at fixed offsets. The previous splice grew extraChart and shifted
those offsets, so Pages failed tests/watchlist-identity.test.js. This patcher
removes that splice and consults the same 55 probed ids from the row click,
after the last coverage edit, where the offsets do not move.
"""
import hashlib, json, pathlib, sys

LINES = """
ECONOMICS:ATGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:AT
ECONOMICS:AUGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:AU
ECONOMICS:AUIRYY|worldbank:FP.CPI.TOTL.ZG:AU
ECONOMICS:BEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BE
ECONOMICS:CAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CA
ECONOMICS:CHGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CH
ECONOMICS:CLGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CL
ECONOMICS:COGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CO
ECONOMICS:COIRYY|worldbank:FP.CPI.TOTL.ZG:CO
ECONOMICS:COUR|worldbank:SL.UEM.TOTL.ZS:CO
ECONOMICS:CRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CR
ECONOMICS:CRIRYY|worldbank:FP.CPI.TOTL.ZG:CR
ECONOMICS:CRUR|worldbank:SL.UEM.TOTL.ZS:CR
ECONOMICS:CZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CZ
ECONOMICS:DEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:DE
ECONOMICS:DKGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:DK
ECONOMICS:EEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:EE
ECONOMICS:ESGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:ES
ECONOMICS:EUGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:EMU
ECONOMICS:FIGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:FI
ECONOMICS:FRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:FR
ECONOMICS:GBGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:GB
ECONOMICS:GRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:GR
ECONOMICS:HUGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:HU
ECONOMICS:IEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:IE
ECONOMICS:ILGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:IL
ECONOMICS:ISGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:IS
ECONOMICS:ITGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:IT
ECONOMICS:JPGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:JP
ECONOMICS:KRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:KR
ECONOMICS:LTGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LT
ECONOMICS:LTIRYY|worldbank:FP.CPI.TOTL.ZG:LT
ECONOMICS:LTUR|worldbank:SL.UEM.TOTL.ZS:LT
ECONOMICS:LUGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LU
ECONOMICS:LVGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LV
ECONOMICS:LVIRYY|worldbank:FP.CPI.TOTL.ZG:LV
ECONOMICS:LVUR|worldbank:SL.UEM.TOTL.ZS:LV
ECONOMICS:MXGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MX
ECONOMICS:NLGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:NL
ECONOMICS:NOGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:NO
ECONOMICS:NZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:NZ
ECONOMICS:NZIRYY|worldbank:FP.CPI.TOTL.ZG:NZ
ECONOMICS:NZUR|worldbank:SL.UEM.TOTL.ZS:NZ
ECONOMICS:PLGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PL
ECONOMICS:PTGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PT
ECONOMICS:SEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SE
ECONOMICS:SIGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SI
ECONOMICS:SKGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SK
ECONOMICS:TRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:TR
ECONOMICS:USGDPYY|FRED:A191RO1Q156NBEA
FTSE:FTSEMIB|FTSEMIB.MI
FTSE:UKX|^FTSE
INDEX:CAC40|^FCHI
INDEX:NKY|^N225
TVC:US07Y|FRED:DGS7
""".strip("\n")

ROOT = pathlib.Path(__file__).resolve()
while ROOT != ROOT.parent and not (ROOT / "jh-chart-tvwatch.js").exists():
    ROOT = ROOT.parent
if not (ROOT / "jh-chart-tvwatch.js").exists():
    sys.exit("jh-chart-tvwatch.js not found")

TV = ROOT / "jh-chart-tvwatch.js"
FIX = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"
BEFORE = ROOT / "tests/fixtures/watchlist-correctness/jh-chart-tvwatch.js.txt"
MARKER = "function providerRest("
BLOB = "ECONOMICS:USGDPYY|FRED:A191RO1Q156NBEA"
TAIL = "  var n = 0, timer = setInterval(function () { if (hook() || ++n > 50) clearInterval(timer); }, 250);\n})();\n"

def sha(b):
    return hashlib.sha256(b).hexdigest()

def reverse(text, edits):
    for e in reversed(edits):
        start, end = e["start"], e["end"]
        got = text[start:end]
        if got != e["after"]:
            sys.exit("reverse mismatch at %s" % start)
        text = text[:start] + e["before"] + text[end:]
    return text

def splice(text, old, new, edits):
    j = text.find(old)
    if j < 0:
        sys.exit("missing anchor %s" % old[:48])
    text = text[:j] + new + text[j + len(old):]
    edits.append({"start": j, "end": j + len(new), "before": old, "after": new})
    return text

def fn_body(src, name):
    key = "function " + name
    i = src.find(key)
    if i < 0:
        return ""
    j = src.find("{", i)
    d = 0
    k = j
    while k < len(src):
        if src[k] == "{":
            d += 1
        elif src[k] == "}":
            d -= 1
            if d == 0:
                return src[i:k + 1]
        k += 1
    sys.exit("unclosed " + name)

text = TV.read_text()
if MARKER in text:
    print("already applied")
    sys.exit(0)
if text.count(MARKER) != 0:
    sys.exit("marker drift")

fix = json.loads(FIX.read_text())
entry = fix["jh-chart-tvwatch.js"]
edits = list(entry["edits"])
if not edits or BLOB not in edits[-1].get("after", ""):
    sys.exit("last preservation edit is not the extraChart splice")
last = edits[-1]
if text[last["start"]:last["end"]] != last["after"]:
    sys.exit("extraChart splice is not at its recorded offset")
# Drop the splice. extraChart goes back to the 461-entry map the identity test freezes.
text = text[:last["start"]] + last["before"] + text[last["end"]:]
edits = edits[:-1]
if BLOB in text:
    sys.exit("blob survived removal")
if sha(text.encode()) == entry["after_sha256"]:
    sys.exit("removal did not change the file")

blob = "\\n".join(LINES.split("\n"))
fn = (
    "  function providerRest(u) {\n"
    "    var raw = \"" + blob + "\", lines = raw.split(\"\\n\"), i, p, a, b;\n"
    "    if (!providerRest.map) {\n"
    "      providerRest.map = Object.create(null);\n"
    "      for (i = 0; i < lines.length; i++) {\n"
    "        p = lines[i].split(\"|\");\n"
    "        if (p.length === 2 && p[0] && p[1]) providerRest.map[p[0]] = p[1];\n"
    "      }\n"
    "    }\n"
    "    a = String(u || \"\").trim();\n"
    "    b = providerRest.map[a] || providerRest.map[a.toUpperCase()];\n"
    "    return b || \"\";\n"
    "  }\n"
)
if text.count("openSym(s);") != 2:
    sys.exit("openSym(s) count %s" % text.count("openSym(s);"))
# First occurrence, then the second. Both sit after the last coverage edit.
text = splice(text, "openSym(s);", "openSym(providerRest(s)||s);", edits)
text = splice(text, "openSym(s);", "openSym(providerRest(s)||s);", edits)
text = splice(
    text,
    'openSym(tr.getAttribute("data-s"))',
    'openSym(providerRest(tr.getAttribute("data-s"))||tr.getAttribute("data-s"))',
    edits,
)
text = splice(
    text,
    'openSym(rows[i].getAttribute("data-s"))',
    'openSym(providerRest(rows[i].getAttribute("data-s"))||rows[i].getAttribute("data-s"))',
    edits,
)
if text.count(TAIL) != 1:
    sys.exit("tail count %s" % text.count(TAIL))
text = splice(text, TAIL, TAIL.replace("})();\n", fn + "})();\n", 1), edits)

for needed in (
    'await window.jhWatchlistStore.saveBatch(draft.patch,draft.revision)',
    'if(!actionDraft)throw Error("Watchlist mutation requires a captured user action")',
    "actionDraft.model[k]=clone(v)",
    "window.jhWatchlistRendererAdd=intent(",
    'var NOTES_KEY = "jh-tv-notes"',
    MARKER,
    BLOB,
):
    if needed not in text:
        sys.exit("required string missing")
if "localStorage.setItem(UI_KEY" in text:
    sys.exit("storage write introduced")
if text.count("function extraChart(") != 1 or text.count("function chartSymbol(") != 1:
    sys.exit("helper count drifted")
if text.count("function openSym(") != 1 or text.count(MARKER) != 1:
    sys.exit("function count drifted")
extra = fn_body(text, "extraChart")
if BLOB in extra or "providerRest" in extra:
    sys.exit("frozen extraChart was modified")
if fn_body(text, "fredId") == "" or fn_body(text, "chartSymbol") == "":
    sys.exit("frozen helper missing")

# The identity test reverses only the coverage-scoped edits, on the final file.
prev = text
scoped = [e for e in edits if e.get("scope") == "watchlist-handoff-coverage-2"]
if len(scoped) != 6:
    sys.exit("coverage edit count %s" % len(scoped))
for e in reversed(scoped):
    if prev[e["start"]:e["end"]] != e["after"]:
        sys.exit("coverage reverse mismatch at %s" % e["start"])
    prev = prev[:e["start"]] + e["before"] + prev[e["end"]:]
for name in ("extraChart", "fredId", "chartSymbol"):
    if fn_body(text, name) != fn_body(prev, name):
        sys.exit(name + " not byte-identical across the coverage reverse")

base = BEFORE.read_bytes()
restored = reverse(text, edits)
if restored.encode() != base:
    rb = restored.encode()
    n = min(len(rb), len(base))
    i = next((k for k in range(n) if rb[k] != base[k]), n)
    sys.exit("preservation reverse failed at %s" % i)
if sha(base) != entry["before_sha256"]:
    sys.exit("before fixture hash drifted")

entry["edits"] = edits
entry["after_sha256"] = sha(text.encode())
TV.write_text(text)
FIX.write_text(json.dumps(fix, indent=2) + "\n")
print("ok bytes", len(text.encode()), "sha", entry["after_sha256"], "edits", len(edits))
