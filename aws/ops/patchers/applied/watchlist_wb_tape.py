#!/usr/bin/env python3
"""Append probed World Bank, central-bank and same-instrument tapes. No concept substitutes."""
import hashlib, json, pathlib, re, sys

ADD = [
    'ECONOMICS:AMTOT|worldbank:TT.PRI.MRCH.XD.WD:AM',
    'ECONOMICS:ARCA|worldbank:BN.CAB.XOKA.CD:AR',
    'ECONOMICS:ARTOT|worldbank:TT.PRI.MRCH.XD.WD:AR',
    'ECONOMICS:AUGFCF|worldbank:NE.GDI.FTOT.CD:AU',
    'ECONOMICS:AUTOT|worldbank:TT.PRI.MRCH.XD.WD:AU',
    'ECONOMICS:BETOT|worldbank:TT.PRI.MRCH.XD.WD:BE',
    'ECONOMICS:BFTOT|worldbank:TT.PRI.MRCH.XD.WD:BF',
    'ECONOMICS:BITOT|worldbank:TT.PRI.MRCH.XD.WD:BI',
    'ECONOMICS:BOTOT|worldbank:TT.PRI.MRCH.XD.WD:BO',
    'ECONOMICS:BRGFCF|worldbank:NE.GDI.FTOT.CD:BR',
    'ECONOMICS:BRTOT|worldbank:TT.PRI.MRCH.XD.WD:BR',
    'ECONOMICS:CAGFCF|worldbank:NE.GDI.FTOT.CD:CA',
    'ECONOMICS:CATOT|worldbank:TT.PRI.MRCH.XD.WD:CA',
    'ECONOMICS:CHTOT|worldbank:TT.PRI.MRCH.XD.WD:CH',
    'ECONOMICS:CLGDPPC|worldbank:NY.GDP.PCAP.CD:CL',
    'ECONOMICS:CLTOT|worldbank:TT.PRI.MRCH.XD.WD:CL',
    'ECONOMICS:CNGFCF|worldbank:NE.GDI.FTOT.CD:CN',
    'ECONOMICS:CNTOT|worldbank:TT.PRI.MRCH.XD.WD:CN',
    'ECONOMICS:COBOT|worldbank:NE.RSB.GNFS.CD:CO',
    'ECONOMICS:COTOT|worldbank:TT.PRI.MRCH.XD.WD:CO',
    'ECONOMICS:CRBOT|worldbank:NE.RSB.GNFS.CD:CR',
    'ECONOMICS:CZTOT|worldbank:TT.PRI.MRCH.XD.WD:CZ',
    'ECONOMICS:DEGFCF|worldbank:NE.GDI.FTOT.CD:DE',
    'ECONOMICS:DETOT|worldbank:TT.PRI.MRCH.XD.WD:DE',
    'ECONOMICS:DKTOT|worldbank:TT.PRI.MRCH.XD.WD:DK',
    'ECONOMICS:DZTOT|worldbank:TT.PRI.MRCH.XD.WD:DZ',
    'ECONOMICS:EGCA|worldbank:BN.CAB.XOKA.CD:EG',
    'ECONOMICS:ESTOT|worldbank:TT.PRI.MRCH.XD.WD:ES',
    'ECONOMICS:EUGDPPC|worldbank:NY.GDP.PCAP.CD:EU',
    'ECONOMICS:EUGFCF|worldbank:NE.GDI.FTOT.CD:EU',
    'ECONOMICS:FITOT|worldbank:TT.PRI.MRCH.XD.WD:FI',
    'ECONOMICS:FRGFCF|worldbank:NE.GDI.FTOT.CD:FR',
    'ECONOMICS:FRTOT|worldbank:TT.PRI.MRCH.XD.WD:FR',
    'ECONOMICS:GBTOT|worldbank:TT.PRI.MRCH.XD.WD:GB',
    'ECONOMICS:HKCA|worldbank:BN.CAB.XOKA.CD:HK',
    'ECONOMICS:HKTOT|worldbank:TT.PRI.MRCH.XD.WD:HK',
    'ECONOMICS:HUTOT|worldbank:TT.PRI.MRCH.XD.WD:HU',
    'ECONOMICS:IDINTR|FRED:IRSTCB01IDM156N',
    'ECONOMICS:IDTOT|worldbank:TT.PRI.MRCH.XD.WD:ID',
    'ECONOMICS:IETOT|worldbank:TT.PRI.MRCH.XD.WD:IE',
    'ECONOMICS:ILTOT|worldbank:TT.PRI.MRCH.XD.WD:IL',
    'ECONOMICS:INTOT|worldbank:TT.PRI.MRCH.XD.WD:IN',
    'ECONOMICS:ITTOT|worldbank:TT.PRI.MRCH.XD.WD:IT',
    'ECONOMICS:JOTOT|worldbank:TT.PRI.MRCH.XD.WD:JO',
    'ECONOMICS:JPGFCF|worldbank:NE.GDI.FTOT.CD:JP',
    'ECONOMICS:JPTOT|worldbank:TT.PRI.MRCH.XD.WD:JP',
    'ECONOMICS:KETOT|worldbank:TT.PRI.MRCH.XD.WD:KE',
    'ECONOMICS:KRTOT|worldbank:TT.PRI.MRCH.XD.WD:KR',
    'ECONOMICS:LBCA|worldbank:BN.CAB.XOKA.CD:LB',
    'ECONOMICS:LKTOT|worldbank:TT.PRI.MRCH.XD.WD:LK',
    'ECONOMICS:LTBOT|worldbank:NE.RSB.GNFS.CD:LT',
    'ECONOMICS:LVBOT|worldbank:NE.RSB.GNFS.CD:LV',
    'ECONOMICS:MACA|worldbank:BN.CAB.XOKA.CD:MA',
    'ECONOMICS:MAGDPPC|worldbank:NY.GDP.PCAP.CD:MA',
    'ECONOMICS:MOTOT|worldbank:TT.PRI.MRCH.XD.WD:MO',
    'ECONOMICS:MUTOT|worldbank:TT.PRI.MRCH.XD.WD:MU',
    'ECONOMICS:MXTOT|worldbank:TT.PRI.MRCH.XD.WD:MX',
    'ECONOMICS:MYTOT|worldbank:TT.PRI.MRCH.XD.WD:MY',
    'ECONOMICS:NATOT|worldbank:TT.PRI.MRCH.XD.WD:NA',
    'ECONOMICS:NETOT|worldbank:TT.PRI.MRCH.XD.WD:NE',
    'ECONOMICS:NGTOT|worldbank:TT.PRI.MRCH.XD.WD:NG',
    'ECONOMICS:NLTOT|worldbank:TT.PRI.MRCH.XD.WD:NL',
    'ECONOMICS:NOTOT|worldbank:TT.PRI.MRCH.XD.WD:NO',
    'ECONOMICS:NPTOT|worldbank:TT.PRI.MRCH.XD.WD:NP',
    'ECONOMICS:NZTOT|worldbank:TT.PRI.MRCH.XD.WD:NZ',
    'ECONOMICS:PETOT|worldbank:TT.PRI.MRCH.XD.WD:PE',
    'ECONOMICS:PKTOT|worldbank:TT.PRI.MRCH.XD.WD:PK',
    'ECONOMICS:PLTOT|worldbank:TT.PRI.MRCH.XD.WD:PL',
    'ECONOMICS:SACA|worldbank:BN.CAB.XOKA.CD:SA',
    'ECONOMICS:SCTOT|worldbank:TT.PRI.MRCH.XD.WD:SC',
    'ECONOMICS:SETOT|worldbank:TT.PRI.MRCH.XD.WD:SE',
    'ECONOMICS:SGTOT|worldbank:TT.PRI.MRCH.XD.WD:SG',
    'ECONOMICS:SKTOT|worldbank:TT.PRI.MRCH.XD.WD:SK',
    'ECONOMICS:SNTOT|worldbank:TT.PRI.MRCH.XD.WD:SN',
    'ECONOMICS:SVTOT|worldbank:TT.PRI.MRCH.XD.WD:SV',
    'ECONOMICS:THTOT|worldbank:TT.PRI.MRCH.XD.WD:TH',
    'ECONOMICS:TNTOT|worldbank:TT.PRI.MRCH.XD.WD:TN',
    'ECONOMICS:TRTOT|worldbank:TT.PRI.MRCH.XD.WD:TR',
    'ECONOMICS:TZTOT|worldbank:TT.PRI.MRCH.XD.WD:TZ',
    'ECONOMICS:UATOT|worldbank:TT.PRI.MRCH.XD.WD:UA',
    'ECONOMICS:USGDPPC|worldbank:NY.GDP.PCAP.CD:US',
    'ECONOMICS:USGFCF|worldbank:NE.GDI.FTOT.CD:US',
    'ECONOMICS:VNCA|worldbank:BN.CAB.XOKA.CD:VN',
    'ECONOMICS:VNTOT|worldbank:TT.PRI.MRCH.XD.WD:VN',
    'ECONOMICS:WWGDP|worldbank:NY.GDP.MKTP.CD:WLD',
    'ECONOMICS:WWPOP|worldbank:SP.POP.TOTL:WLD',
    'ECONOMICS:ZAINTR|FRED:IRSTCB01ZAM156N',
    'ECONOMICS:ZATOT|worldbank:TT.PRI.MRCH.XD.WD:ZA',
    'FX_IDC:VUVUSD|VUVUSD=X',
    'TVC:FTMIB|FTSEMIB.MI',
    'TVC:NYA|^NYA',
    'TVC:RUA|^RUA',
]
MARKER = "ECONOMICS:AMTOT|worldbank:TT.PRI.MRCH.XD.WD:AM"
COUNT_OLD = "assert.equal(providerEntries.length,226);"
COUNT_NEW = 318

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

def ok_target(t):
    if t.startswith("FRED:"):
        return t[5:].isalnum()
    if t.startswith("worldbank:"):
        return re.fullmatch(r"worldbank:[A-Z0-9.]+:[A-Z0-9]{2,3}", t) is not None
    return t in ("^NYA", "^RUA", "FTSEMIB.MI", "VUVUSD=X")

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
if len(lines) != 226:
    sys.exit("line count %s" % len(lines))
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
for name in ("function extraChart(", "function fredId(", "function chartSymbol("):
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
print("ok bytes", len(new.encode()), "lines", len(new_lines), "edits", len(edits))
