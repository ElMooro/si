"""ops 7001 - read-only: what Damodaran Online and AQR's data library hand out today, and on what terms.

Khalid, 2026-11-27: "add whatever my system needs" (after the list of fifty sites: two would add something - Aswath
Damodaran's data, which no engine reads while justhodl-eva-spread carries a fixed 5.50% equity risk premium under his
name, and AQR's factor data sets, whose terms of use sit in a tab of each spreadsheet).

This reads, from the runner:
  1. Damodaran Online: the home page (this month's implied premiums, the link to the monthly file) and the current-data
     page (every spreadsheet it lists); for the files an engine would read, their size, their Last-Modified / ETag (can a
     daily run ask "has it changed?" without downloading), their sheets, and the top and bottom rows of each sheet -
     the layout a reader has to be written against. A few rows of each, never a file.
  2. AQR: the data-set page, the spreadsheets it links, and in two of them the sheets and every sentence of their
     terms that speaks of commercial use, redistribution, reproduction, consent or publication - short excerpts only.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-term-premium" / "source"))      # the xlrd the fleet already carries
NAME = "ops_7001_damodaran_aqr_probe"
UA = "Mozilla/5.0 (compatible; JustHodl.AI research data bank; +https://justhodl.ai)"
DAM = "https://pages.stern.nyu.edu/~adamodar/"
HOME, CURRENT = DAM + "New_Home_Page/home.htm", DAM + "New_Home_Page/datacurrent.html"
READ = ["histimpl.xls", "histretSP.xls", "ctryprem.xlsx", "wacc.xls", "waccGlobal.xls", "betas.xls", "totalbeta.xls", "margin.xls", "pedata.xls", "pbvdata.xls",
        "psdata.xls", "vebitda.xls", "roe.xls", "EVA.xls", "capex.xls", "taxrate.xls", "dbtfund.xls", "fundgrEB.xls", "histgr.xls", "divfund.xls",
        "countrystats.xls", "mktcaprisk.xlsx", "mktcapmult.xlsx", "ratings.xls", "macro.xls"]
AQR_PAGE = "https://www.aqr.com/Insights/Datasets"
AQR_TRY = ["Quality-Minus-Junk-Factors-Monthly.xlsx", "Betting-Against-Beta-Equity-Factors-Monthly.xlsx"]
AQR_BASE = "https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/"
WORDS = re.compile(r"commercial|redistribut|reproduc|consent|republish|publish|personal use|licen[cs]e|permission|prohibit|may not|third part|educational|informational", re.I)
_MASK = [(re.compile(r"(api_?key=)[^&\s\"']*", re.I), r"\1***"), (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "***")]
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main", "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
      "p": "http://schemas.openxmlformats.org/package/2006/relationships"}


def clean(text):
    text = str(text)
    for pat, rep in _MASK:
        text = pat.sub(rep, text)
    return text


def get(url, method="GET", timeout=60):
    req = urllib.request.Request(url, method=method, headers={"User-Agent": UA, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, dict(resp.headers.items()), (resp.read() if method == "GET" else b"")


def col_of(ref):
    n = 0
    for ch in ref:
        if ch.isalpha():
            n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


def xlsx_sheets(raw):
    """{sheet name: rows} of an .xlsx, with the standard library alone: what the engine itself will use."""
    z = zipfile.ZipFile(io.BytesIO(raw))
    names = set(z.namelist())
    shared = []
    if "xl/sharedStrings.xml" in names:
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", NS):
            shared.append("".join(t.text or "" for t in si.iter("{%s}t" % NS["m"])))
    rels = {x.get("Id"): x.get("Target") for x in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")).findall("p:Relationship", NS)}
    out = {}
    for sh in ET.fromstring(z.read("xl/workbook.xml")).find("m:sheets", NS).findall("m:sheet", NS):
        target = rels.get(sh.get("{%s}id" % NS["r"]), "")
        path = target.lstrip("/") if target.startswith("/") else "xl/" + target
        if path not in names:
            continue
        rows = []
        for row in ET.fromstring(z.read(path)).iter("{%s}row" % NS["m"]):
            cells = {}
            for c in row.findall("m:c", NS):
                v, kind = c.find("m:v", NS), c.get("t")
                if kind == "inlineStr":
                    val = "".join(t.text or "" for t in c.iter("{%s}t" % NS["m"]))
                elif v is None or v.text is None:
                    continue
                elif kind == "s":
                    val = shared[int(v.text)]
                elif kind in ("str", "e"):
                    val = v.text
                elif kind == "b":
                    val = v.text == "1"
                else:
                    try:
                        val = float(v.text)
                    except ValueError:
                        val = v.text
                cells[col_of(c.get("r") or "A")] = val
            idx = int(row.get("r") or len(rows) + 1) - 1
            while len(rows) < idx:
                rows.append([])
            rows.append([cells.get(i) for i in range(max(cells) + 1)] if cells else [])
        out[sh.get("name")] = rows
    return out


def xls_sheets(raw):
    import xlrd
    book = xlrd.open_workbook(file_contents=raw)
    out = {}
    for sh in book.sheets():
        rows = []
        for i in range(sh.nrows):
            row = []
            for j in range(sh.ncols):
                c = sh.cell(i, j)
                if c.ctype == 3:                                   # a date
                    try:
                        y, m, d = xlrd.xldate_as_tuple(c.value, book.datemode)[:3]
                        row.append("%04d-%02d-%02d" % (y, m, d))
                    except Exception:
                        row.append(c.value)
                else:
                    row.append(c.value if c.ctype not in (0, 6) else None)
            rows.append(row)
        out[sh.name] = rows
    return out


def sheets_of(name, raw):
    return xlsx_sheets(raw) if raw[:2] == b"PK" else xls_sheets(raw)


def show(v, wide=22):
    if v is None or v == "":
        return "·"
    if isinstance(v, float):
        return ("%.4f" % v).rstrip("0").rstrip(".") if abs(v) < 1e7 else "%.3e" % v
    return re.sub(r"\s+", " ", str(v))[:wide]


def used(rows):
    return [i for i, r in enumerate(rows) if any(c not in (None, "") for c in r)]


def main():
    from ops_report import report
    now = datetime.now(timezone.utc)
    fails = []
    with report(NAME) as r:
        def say(text):
            r.log(clean(text))

        def layout(fname, raw, top=14, cols=9, max_sheets=3, bottom=2):
            try:
                sheets = sheets_of(fname, raw)
            except Exception as e:
                fails.append(fname)
                say("  ✗ %s could not be read: %s: %s" % (fname, type(e).__name__, str(e)[:160]))
                return {}
            say("  sheets: %s" % "; ".join("%s (%d rows, %d wide)" % (n, len(rows), max((len(x) for x in rows), default=0)) for n, rows in sheets.items()))
            for n, rows in list(sheets.items())[:max_sheets]:
                keep = used(rows)
                for i in keep[:top]:
                    say("    %s r%-3d | %s" % (n[:14], i + 1, " | ".join(show(c) for c in rows[i][:cols])))
                for i in keep[-bottom:] if len(keep) > top else []:
                    say("    %s r%-3d | %s   (near the end)" % (n[:14], i + 1, " | ".join(show(c) for c in rows[i][:cols])))
            return sheets

        r.heading("7001 - Damodaran Online and AQR's data library: what is handed out, and on what terms (read-only), %s" % now.isoformat(timespec="minutes"))

        r.section("1. Damodaran Online: the two pages")
        hrefs, month_file = [], None
        for label, url in (("home", HOME), ("current data", CURRENT)):
            try:
                st, hd, body = get(url)
                text = body.decode("latin-1", "replace")
                found = sorted(set(re.findall(r'href\s*=\s*["\']?([^"\'\s>]+\.xlsx?)', text, re.I)))
                say("%s page: HTTP %s, %d bytes, Last-Modified %s, %d spreadsheet links" % (label, st, len(body), hd.get("Last-Modified"), len(found)))
                for h in found:
                    full = urllib.parse.urljoin(url, h)
                    if full not in hrefs:
                        hrefs.append(full)
                    if re.search(r"erp.?by.?month|implprem", h, re.I):
                        month_file = full
                if label == "home":
                    plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text))
                    for m in re.finditer(r"Implied ERP on ([A-Za-z]+ \d{1,2}, \d{4})\s*=\s*(.{0,330})", plain):
                        say("  the page says: Implied ERP on %s = %s" % (m.group(1), m.group(2)[:330]))
                    m = re.search(r"latest overall data update was on ([A-Za-z]+ \d{1,2}, \d{4})", plain)
                    say("  latest overall data update, as the page says: %s" % (m.group(1) if m else "not found"))
            except Exception as e:
                fails.append(label)
                say("✗ %s page: %s: %s" % (label, type(e).__name__, str(e)[:200]))
            time.sleep(0.6)
        by_name = {h.rsplit("/", 1)[-1]: h for h in hrefs}
        say("spreadsheets listed in all: %d | the monthly premium file: %s" % (len(hrefs), month_file))
        say("names: %s" % ", ".join(sorted(by_name)))

        r.section("2. Damodaran Online: the files an engine would read")
        total = 0
        want = ([month_file.rsplit("/", 1)[-1]] if month_file else []) + READ
        for fname in want:
            url = by_name.get(fname) or (DAM + "pc/datasets/" + fname)
            try:
                st, hd, raw = get(url)
                total += len(raw)
                say("%s: HTTP %s, %d bytes, Last-Modified %s, ETag %s, type %s | %s" % (fname, st, len(raw), hd.get("Last-Modified"), hd.get("ETag"), hd.get("Content-Type"), url.replace(DAM, "~/")))
                top = 22 if fname == want[0] or fname in ("histimpl.xls", "histretSP.xls", "ctryprem.xlsx") else 13
                layout(fname, raw, top=top, cols=11 if top == 22 else 9, max_sheets=4 if top == 22 else 2, bottom=3)
            except urllib.error.HTTPError as e:
                fails.append(fname)
                say("✗ %s: HTTP %s" % (fname, e.code))
            except Exception as e:
                fails.append(fname)
                say("✗ %s: %s: %s" % (fname, type(e).__name__, str(e)[:160]))
            time.sleep(0.6)
        say("read %d files, %.1f MB in all" % (len(want), total / 1e6))
        try:
            st, hd, _ = get(by_name.get("wacc.xls") or DAM + "pc/datasets/wacc.xls", method="HEAD")
            say("a HEAD request for wacc.xls: HTTP %s, Last-Modified %s, ETag %s, Content-Length %s" % (st, hd.get("Last-Modified"), hd.get("ETag"), hd.get("Content-Length")))
        except Exception as e:
            say("a HEAD request for wacc.xls: %s: %s" % (type(e).__name__, str(e)[:120]))

        r.section("3. AQR's data library: the files and the terms inside them")
        links = []
        try:
            st, hd, body = get(AQR_PAGE)
            text = body.decode("utf-8", "replace")
            links = sorted(set(re.findall(r'["\'(]([^"\'()\s]+/Data-Sets/[^"\'()\s]+\.xlsx)', text, re.I)))
            say("data-set page: HTTP %s, %d bytes, %d spreadsheet links on the first page" % (st, len(body), len(links)))
            for h in links[:30]:
                say("  %s" % h.rsplit("/", 1)[-1])
        except Exception as e:
            say("data-set page: %s: %s" % (type(e).__name__, str(e)[:200]))
        for fname in AQR_TRY:
            url = next((urllib.parse.urljoin(AQR_PAGE, h) for h in links if h.endswith(fname)), AQR_BASE + fname)
            try:
                st, hd, raw = get(url, timeout=120)
                say("%s: HTTP %s, %d bytes, Last-Modified %s, type %s" % (fname, st, len(raw), hd.get("Last-Modified"), hd.get("Content-Type")))
                sheets = sheets_of(fname, raw)
                say("  sheets: %s" % "; ".join("%s (%d rows)" % (n, len(rows)) for n, rows in sheets.items()))
                seen, shown = set(), 0
                for n, rows in sheets.items():
                    for row in rows:
                        for c in row:
                            if not isinstance(c, str) or len(c) < 60:
                                continue
                            for sentence in re.split(r"(?<=[.;])\s+", re.sub(r"\s+", " ", c)):
                                if WORDS.search(sentence) and sentence not in seen and shown < 16:
                                    seen.add(sentence)
                                    shown += 1
                                    say("  [%s] %s" % (n[:18], sentence[:300]))
                if not shown:
                    say("  no sentence in any sheet speaks of use, consent or redistribution")
                first = next(iter(sheets.items()), None)
                if first:
                    keep = used(first[1])
                    for i in keep[:3] + keep[-1:]:
                        say("    %s r%-4d | %s" % (first[0][:14], i + 1, " | ".join(show(c, 16) for c in first[1][i][:7])))
            except urllib.error.HTTPError as e:
                say("%s: HTTP %s (the site did not hand the file to the runner)" % (fname, e.code))
            except Exception as e:
                say("%s: %s: %s" % (fname, type(e).__name__, str(e)[:200]))
            time.sleep(0.8)

        r.kv(damodaran_files_listed=len(hrefs), damodaran_files_read=len(want) - len([f for f in fails if f in want]), could_not_read=len(fails), megabytes=round(total / 1e6, 1))
        if fails:
            say("could not read: %s" % ", ".join(fails))
        r.ok("read; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
