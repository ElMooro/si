"""ops 7002 - read-only: the terms AQR puts inside its data-set spreadsheets.

ops 7001 found a "Disclosures" sheet of two rows in each AQR workbook and no sentence about use in any cell: the text
may sit in one long cell that was not split into sentences, or in a text box drawn on the sheet. This reads both - the
cells of that sheet and every text box in the workbook - and prints the sentences that speak of use, consent,
redistribution or publication, each cut at 420 characters. Short excerpts of a notice, to decide whether the data may
be used on a commercial site; the data itself is not printed.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import io
import re
import sys
import zipfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_7002_aqr_terms_in_the_spreadsheets"
FILES = ["Quality-Minus-Junk-Factors-Monthly.xlsx", "Century-of-Factor-Premia-Monthly.xlsx"]
A = "{http://schemas.openxmlformats.org/drawingml/2006/main}"


def load(stem):
    path = next((ROOT / "aws" / "ops" / "STAGED").glob(stem + "_*.py"))
    spec = importlib.util.spec_from_file_location(stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    from ops_report import report
    P = load("ops_7001")
    words = re.compile(P.WORDS.pattern + r"|terms|rights reserved|copyright|not be|only for|solely", re.I)
    with report(NAME) as r:
        def say(text):
            r.log(P.clean(text))

        r.heading("7002 - the terms inside AQR's spreadsheets (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        for fname in FILES:
            r.section(fname)
            try:
                st, hd, raw = P.get(P.AQR_BASE + fname, timeout=120)
            except Exception as e:
                say("%s: %s: %s" % (fname, type(e).__name__, str(e)[:160]))
                continue
            z = zipfile.ZipFile(io.BytesIO(raw))
            names = z.namelist()
            say("HTTP %s, %d bytes, Last-Modified %s | parts: %d, drawings %d, comments %d" % (st, len(raw), hd.get("Last-Modified"), len(names),
                sum(1 for n in names if "/drawings/" in n and n.endswith(".xml")), sum(1 for n in names if "comments" in n)))
            texts = []
            sheets = P.xlsx_sheets(raw)
            for n, rows in sheets.items():
                if re.search(r"disclos|terms|disclaim|legal", n, re.I):
                    for row in rows:
                        for c in row:
                            if isinstance(c, str) and c.strip():
                                texts.append(("sheet " + n, re.sub(r"\s+", " ", c)))
            for n in names:
                if "/drawings/" in n and n.endswith(".xml") and "_rels" not in n:
                    try:
                        for p in ET.fromstring(z.read(n)).iter(A + "p"):
                            t = "".join(x.text or "" for x in p.iter(A + "t")).strip()
                            if t:
                                texts.append(("text box " + n.rsplit("/", 1)[-1], re.sub(r"\s+", " ", t)))
                    except Exception as e:
                        say("  %s: %s" % (n, type(e).__name__))
            say("texts found: %d (%d characters)" % (len(texts), sum(len(t) for _, t in texts)))
            shown, seen = 0, set()
            for where, t in texts:
                for s in re.split(r"(?<=[.;])\s+", t):
                    if words.search(s) and s not in seen and shown < 30:
                        seen.add(s)
                        shown += 1
                        say("  [%s] %s" % (where[:26], s[:420]))
            if not shown:
                say("  no sentence speaks of use, consent or redistribution")
            first = next(iter(sheets.items()), None)
            if first:
                for row in first[1][:8]:
                    for c in row[:2]:
                        if isinstance(c, str) and len(c) > 40:
                            say("  first sheet: %s" % re.sub(r"\s+", " ", c)[:300])
        r.ok("read; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
