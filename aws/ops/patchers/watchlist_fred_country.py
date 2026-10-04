#!/usr/bin/env python3
"""Route OECD FRED ids to the 2-letter country code FRED stores, and curated ECONOMICS codes to their FRED series."""
import hashlib, json, pathlib, sys

ROOT = pathlib.Path(__file__).resolve()
while ROOT != ROOT.parent and not (ROOT / "jh-chart-tvwatch.js").exists():
    ROOT = ROOT.parent
if not (ROOT / "jh-chart-tvwatch.js").exists():
    sys.exit("jh-chart-tvwatch.js not found")

TV = ROOT / "jh-chart-tvwatch.js"
FIX = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"
BEFORE = ROOT / "tests/fixtures/watchlist-correctness/jh-chart-tvwatch.js.txt"
MARKER = "function fredId("

OLD = """  function chartSymbol(s) {
    s = String(s || "").trim();
    if (!s || s.indexOf("###") === 0) return "";
    var u = s.toUpperCase(), hit = symMap && symMap[u];
    if (hit && hit.source === "MARKET" && hit.id && /^[A-Z0-9.^=-]{1,24}$/.test(String(hit.id).toUpperCase())) return String(hit.id).toUpperCase();
    if (hit && hit.source === "FRED" && /^[A-Z0-9]+$/.test(hit.id || "")) return "FRED:" + hit.id;"""

NEW = """  function fredId(id) {
    id = String(id || "").toUpperCase();
    var cc = { AUS: "AU", AUT: "AT", BEL: "BE", CAN: "CA", CHE: "CH", CHL: "CL", COL: "CO", CZE: "CZ", DEU: "DE", DNK: "DK", ESP: "ES", EST: "EE", FIN: "FI", FRA: "FR", GBR: "GB", GRC: "GR", HUN: "HU", IRL: "IE", ISR: "IL", ITA: "IT", JPN: "JP", KOR: "KR", LTU: "LT", LUX: "LU", LVA: "LV", NLD: "NL", NOR: "NO", NZL: "NZ", POL: "PL", PRT: "PT", SVK: "SK", SVN: "SI", SWE: "SE", TUR: "TR", USA: "US", EA19: "EZ" };
    var m = /^(BSCICP03|CSCICP03|IRLTLT01|LRHUTTTT|CPALTT01|IR3TIB01|PRINTO01|XTEXVA01|XTIMVA01|XTNTVA01|MABMM301)(AUS|AUT|BEL|CAN|CHE|CHL|COL|CZE|DEU|DNK|ESP|EST|FIN|FRA|GBR|GRC|HUN|IRL|ISR|ITA|JPN|KOR|LTU|LUX|LVA|NLD|NOR|NZL|POL|PRT|SVK|SVN|SWE|TUR|USA|EA19)(M\\d+[A-Z])$/.exec(id);
    if (!m || !cc[m[2]]) return id;
    return m[1] + cc[m[2]] + m[3];
  }
  function chartSymbol(s) {
    s = String(s || "").trim();
    if (!s || s.indexOf("###") === 0) return "";
    var u = s.toUpperCase(), hit = symMap && symMap[u];
    if (hit && hit.source === "MARKET" && hit.id && /^[A-Z0-9.^=-]{1,24}$/.test(String(hit.id).toUpperCase())) return String(hit.id).toUpperCase();
    if (hit && hit.source === "FRED" && /^[A-Z0-9]+$/.test(hit.id || "")) return "FRED:" + fredId(hit.id);"""

OLD2 = """    if (/^FRED:[A-Z0-9]+$/.test(u)) return u;"""
NEW2 = """    if (/^FRED:[A-Z0-9]+$/.test(u)) return "FRED:" + fredId(u.slice(5));
    if (venue === "ECONOMICS") {
      var econ = { USINTR: "FEDFUNDS", USCPI: "CPIAUCSL", USCCPI: "CPILFESL", USUR: "UNRATE", USGDP: "GDP", USGDPQQ: "A191RL1Q225SBEA", USNFP: "PAYEMS", USIJC: "ICSA", USCJC: "CCSA", USRSM: "RSAFS", USIP: "INDPRO", USM2: "M2SL", USBOT: "BOPGSTB", USPPI: "PPIACO", USHS: "HOUST", USBP: "PERMIT", USCS: "UMCSENT", USDGO: "DGORDER", USPCE: "PCEPI", USCPCE: "PCEPILFE", USTBL: "BOPGSTB", USGD: "GFDEBTN", USAHE: "AHETPI", USPART: "CIVPART", USJO: "JTSJOL", USBBS: "WALCL", USCBBS: "WALCL", EUINTR: "ECBDFR", DEUR: "LRHUTTTTDEM156S", DECPI: "DEUCPIALLMINMEI", DEGDPQQ: "CLVMNACSCAB1GQDE", GBINTR: "IRSTCB01GBM156N", GBCPI: "GBRCPIALLMINMEI", JPINTR: "IRSTCB01JPM156N", JPCPI: "JPNCPIALLMINMEI", CNGDP: "MKTGDPCNA646NWDB", CNCPI: "CHNCPIALLMINMEI", CAINTR: "IRSTCB01CAM156N", AUINTR: "IRSTCB01AUM156N", USDXY: "DTWEXBGS" };
      if (econ[bare]) return "FRED:" + econ[bare];
    }"""

def sha(b):
    return hashlib.sha256(b).hexdigest()

def reverse(text, edits):
    for e in reversed(edits):
        start, end = e["start"], e["end"]
        got = text[start:end]
        if got != e["after"]:
            sys.exit("reverse mismatch at %s: got %r" % (start, got[:80]))
        text = text[:start] + e["before"] + text[end:]
    return text

text = TV.read_text()
if MARKER in text and text.count(OLD2) == 0:
    print("already applied")
    sys.exit(0)
if text.count(OLD) != 1:
    sys.exit("anchor 1 count %s" % text.count(OLD))
if text.count(OLD2) != 1:
    sys.exit("anchor 2 count %s" % text.count(OLD2))

before_text = text
text = text.replace(OLD, NEW, 1).replace(OLD2, NEW2, 1)
if MARKER not in text:
    sys.exit("marker missing after splice")
if text.count("function fredId(") != 1 or text.count("function chartSymbol(") != 1:
    sys.exit("function count drifted")

fix = json.loads(FIX.read_text())
entry = fix["jh-chart-tvwatch.js"]
if sha(before_text.encode()) != entry["after_sha256"]:
    sys.exit("working tree hash %s != fixture %s" % (sha(before_text.encode()), entry["after_sha256"]))

start = before_text.find(OLD)
end = start + len(OLD)
# the second replacement is after the first insertion, so record ONE edit covering both by diffing the whole function region
# Find a single contiguous span: from the start of OLD to the end of OLD2 in the original, then the same logical region in the new file.
o1 = before_text.find(OLD)
o2 = before_text.find(OLD2)
if o2 < o1:
    sys.exit("anchors out of order")
span_before = before_text[o1:o2 + len(OLD2)]
span_after_start = text.find(NEW)
span_after_end = text.find(NEW2) + len(NEW2)
span_after = text[span_after_start:span_after_end]
if text[:span_after_start] != before_text[:o1] or text[span_after_end:] != before_text[o2 + len(OLD2):]:
    sys.exit("splice not contiguous")

edit = {"start": o1, "end": o1 + len(span_after), "before": span_before, "after": span_after}
edits = list(entry["edits"]) + [edit]
restored = reverse(text, edits)
base = BEFORE.read_bytes()
if restored.encode() != base:
    sys.exit("preservation reverse does not match before fixture (%s vs %s)" % (len(restored.encode()), len(base)))
if sha(base) != entry["before_sha256"]:
    sys.exit("before fixture hash drifted")

entry["edits"] = edits
entry["after_sha256"] = sha(text.encode())
TV.write_text(text)
FIX.write_text(json.dumps(fix, indent=2) + "\n")
print("ok bytes", len(text.encode()), "sha", entry["after_sha256"], "edits", len(edits))
