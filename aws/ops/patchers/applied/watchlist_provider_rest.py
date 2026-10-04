#!/usr/bin/env python3
"""Add probed World Bank GDP/CPI/unemployment and five cash-index aliases. No proxies."""
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
MARKER = "ECONOMICS:USGDPYY|FRED:A191RO1Q156NBEA"
ANCHOR = 'XETR:XUTD|XUTD.DE", lines = raw.split("\\n")'

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

text = TV.read_text()
if MARKER in text:
    print("already applied")
    sys.exit(0)
if text.count(ANCHOR) != 1:
    sys.exit("anchor count %s" % text.count(ANCHOR))
insert = "\\n" + "\\n".join(LINES.split("\n"))
after = 'XETR:XUTD|XUTD.DE' + insert + '", lines = raw.split("\\n")'
new = text.replace(ANCHOR, after, 1)
if MARKER not in new or new.count("function extraChart(") != 1 or new.count("function chartSymbol(") != 1:
    sys.exit("splice drifted")
if 'hit.source === "WORLDBANK"' not in new:
    sys.exit("worldbank route dropped")

fix = json.loads(FIX.read_text())
entry = fix["jh-chart-tvwatch.js"]
if sha(text.encode()) != entry["after_sha256"]:
    sys.exit("working tree hash != fixture")
start = new.find(after)
edit = {"start": start, "end": start + len(after), "before": ANCHOR, "after": after}
edits = list(entry["edits"]) + [edit]
restored = reverse(new, edits)
base = BEFORE.read_bytes()
if restored.encode() != base:
    rb = restored.encode()
    n = min(len(rb), len(base))
    i = next((k for k in range(n) if rb[k] != base[k]), n)
    sys.exit("preservation reverse failed at %s" % i)
if sha(base) != entry["before_sha256"]:
    sys.exit("before fixture hash drifted")
entry["edits"] = edits
entry["after_sha256"] = sha(new.encode())
TV.write_text(new)
FIX.write_text(json.dumps(fix, indent=2) + "\n")
print("ok bytes", len(new.encode()), "sha", entry["after_sha256"], "edits", len(edits))
