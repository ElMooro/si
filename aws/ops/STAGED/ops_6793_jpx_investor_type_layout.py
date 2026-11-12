"""ops 6793 - READ-ONLY: the layout of JPX's weekly "Trading by Type of Investors" workbook, now published as
stock_1_w_<start>_<end>.xlsx (the apac-flows engine still looks for stock_val*.xls and reads it with xlrd, so Japan
has been PENDING). Downloads the newest weekly workbook from the listing (and the archive sub-pages' link counts for a
backfill), opens it with the shared standard-library reader aws/shared/risk_sources.Xlsx, prints every sheet's name
and its first rows, and embeds the workbook (gzip+base64) so the parser can be written and tested against the real
file. Nothing is written anywhere."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import re
import sys

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))

LISTING = "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.9"}


def main():
    from ops_report import report
    from risk_sources import Xlsx
    now = datetime.now(timezone.utc)
    with report("ops_6793_jpx_investor_type_layout") as r:
        r.heading("ops 6793 - JPX weekly investor-type workbook layout %s (read-only)" % now.isoformat(timespec="minutes"))
        page = requests.get(LISTING, headers=UA, timeout=30)
        links = re.findall(r'href="([^"]+/stock_1_w_(\d{8})_(\d{8})\.xlsx)"', page.text)
        links = sorted(set(links), key=lambda t: t[2])
        r.log("listing: HTTP %s, %d weekly xlsx links: %s" % (page.status_code, len(links), [(a[-40:], b, c) for a, b, c in links]))
        for sub in ("00-01.html", "00-02.html", "02.html", "03.html", "04.html", "07.html"):
            try:
                s = requests.get("https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/" + sub, headers=UA, timeout=30)
                wk = re.findall(r'href="([^"]+/stock_1_w_\d{8}_\d{8}\.xlsx)"', s.text)
                mo = re.findall(r'href="([^"]+/stock_1_m_\d{6,8}[^"]*\.xlsx)"', s.text)
                title = re.search(r"<title>(.*?)</title>", s.text, re.S)
                r.log("sub-page %s: HTTP %s | %s | %d weekly xlsx, %d monthly xlsx; first/last weekly %s" % (sub, s.status_code, (title.group(1).strip()[:60] if title else ""), len(wk), len(mo), (wk[0][-32:], wk[-1][-32:]) if wk else None))
            except Exception as exc:  # noqa: BLE001
                r.warn("sub-page %s: %s" % (sub, type(exc).__name__))
        if not links:
            r.fail("no weekly workbook link")
            sys.exit(1)
        path, start, end = links[-1]
        url = path if path.startswith("http") else "https://www.jpx.co.jp" + path
        blob = requests.get(url, headers=UA, timeout=60).content
        r.log("newest workbook %s (%s..%s): %d bytes" % (url.split("/")[-1], start, end, len(blob)))
        book = Xlsx(blob)
        r.log("sheets: %s" % book.sheet_names())
        for name in book.sheet_names():
            rows = book.rows(name, max_rows=60)
            r.log("-- sheet %r: %d rows read (first 60)" % (name, len(rows)))
            for i, row in enumerate(rows):
                cells = [("" if v is None else (("%.0f" % v) if isinstance(v, float) else str(v)))[:22] for v in row[:14]]
                if any(cells):
                    r.log("  %2d | %s" % (i, " | ".join(cells)))
        r.section("workbook (gzip+base64)")
        r.log("<!-- JPX_XLSX_GZB64 %s -->\n%s\n<!-- /JPX_XLSX_GZB64 -->" % (url.split("/")[-1], base64.b64encode(gzip.compress(blob)).decode()))
        r.ok("read-only")
    sys.exit(0)


if __name__ == "__main__":
    main()
