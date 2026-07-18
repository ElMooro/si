"""justhodl-congress-direct v1.0 — congressional trading from OFFICIAL
sources, no vendor (creative #3, ops 3455).

Senate: efdsearch.senate.gov — agreement handshake (CSRF cookie) → PTR
search (report type 11, last 30d) → electronic PTR pages parsed into
transaction rows (ticker extracted from "(SYM)" patterns, buy/sale, amount
band, dates). House: Clerk yearly FD index zip → PTR filing events (name,
date, doc URL); PDF transaction parsing deferred to v2.

Replaces the dead Quiver rail. Feed: data/congress-direct.json.
"""
import gzip
import io
import json
import os
import re
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from http.cookiejar import CookieJar

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/congress-direct.json"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
SEN = "https://efdsearch.senate.gov"
s3 = boto3.client("s3", "us-east-1")


def opener():
    cj = CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = [("User-Agent", UA), ("Accept-Encoding", "gzip")]
    return op, cj


def body(resp):
    b = resp.read()
    if resp.headers.get("Content-Encoding") == "gzip" or b[:2] == b"\x1f\x8b":
        b = gzip.decompress(b)
    return b.decode("utf-8", "replace")


def csrf_of(cj):
    for c in cj:
        if c.name == "csrftoken":
            return c.value
    return None


def senate_fetch(days=30, max_reports=25):
    op, cj = opener()
    txns, reports, err = [], [], None
    try:
        r = op.open(SEN + "/search/home/", timeout=25)
        body(r)
        tok = csrf_of(cj)
        data = urllib.parse.urlencode(
            {"prohibition_agreement": "1",
             "csrfmiddlewaretoken": tok or ""}).encode()
        req = urllib.request.Request(SEN + "/search/home/", data=data,
                                     headers={"Referer": SEN + "/search/home/"})
        body(op.open(req, timeout=25))
        tok = csrf_of(cj) or tok
        start_d = (datetime.now(timezone.utc)
                   - timedelta(days=days)).strftime("%m/%d/%Y %H:%M:%S")
        form = {"start": "0", "length": "100",
                "report_types": "[11]", "filer_types": "[]",
                "submitted_start_date": start_d, "submitted_end_date": "",
                "candidate_state": "", "senator_state": "", "office_id": "",
                "first_name": "", "last_name": ""}
        req = urllib.request.Request(
            SEN + "/search/report/data/",
            data=urllib.parse.urlencode(form).encode(),
            headers={"Referer": SEN + "/search/",
                     "X-CSRFToken": tok or "",
                     "X-Requested-With": "XMLHttpRequest"})
        j = json.loads(body(op.open(req, timeout=30)))
        for row in (j.get("data") or [])[:max_reports]:
            try:
                name = re.sub(r"<[^>]+>", "", str(row[0])).strip()
                office = re.sub(r"<[^>]+>", "", str(row[1])).strip()
                m = re.search(r'href="([^"]+)"', str(row[3]))
                link = (SEN + m.group(1)) if m else None
                filed = re.sub(r"<[^>]+>", "", str(row[4])).strip() \
                    if len(row) > 4 else ""
                reports.append({"name": name, "office": office,
                                "url": link, "filed": filed})
                if link and "/search/view/ptr/" in link:
                    time.sleep(0.6)
                    html = body(op.open(urllib.request.Request(
                        link, headers={"Referer": SEN + "/search/"}),
                        timeout=30))
                    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html,
                                         re.S)[1:60]:
                        tds = [re.sub(r"\s+", " ",
                                      re.sub(r"<[^>]+>", " ", td)).strip()
                               for td in re.findall(
                                   r"<td[^>]*>(.*?)</td>", tr, re.S)]
                        if len(tds) < 7:
                            continue
                        asset = tds[3]
                        tkm = re.search(r"\(([A-Z][A-Z0-9.\-]{0,6})\)", asset)
                        txns.append({
                            "filer": name, "tx_date": tds[1],
                            "owner": tds[2], "asset": asset[:90],
                            "ticker": tkm.group(1) if tkm else None,
                            "type": tds[6] if len(tds) > 6 else tds[4],
                            "amount": tds[7] if len(tds) > 7 else None})
            except Exception as e:
                print(f"[senate] report parse: {str(e)[:70]}")
    except Exception as e:
        err = str(e)[:140]
        print(f"[senate] {err}")
    return txns, reports, err


def house_fetch(year=None):
    year = year or datetime.now(timezone.utc).year
    url = (f"https://disclosures-clerk.house.gov/public_disc/"
           f"financial-pdfs/{year}FD.zip")
    filings, err = [], None
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=45) as r:
            zf = zipfile.ZipFile(io.BytesIO(r.read()))
        xn = next(n for n in zf.namelist() if n.lower().endswith(".xml"))
        import xml.etree.ElementTree as ET
        root = ET.fromstring(zf.read(xn))
        for m in root.iter("Member"):
            g = (lambda t: (m.findtext(t) or "").strip())
            if g("FilingType") != "P":
                continue
            doc = g("DocID")
            filings.append({
                "name": f"{g('First')} {g('Last')}".strip(),
                "state_district": g("StateDst"),
                "filing_date": g("FilingDate"), "doc_id": doc,
                "pdf": (f"https://disclosures-clerk.house.gov/public_disc/"
                        f"ptr-pdfs/{year}/{doc}.pdf") if doc else None})
        filings.sort(key=lambda x: x.get("filing_date") or "", reverse=True)
    except Exception as e:
        err = str(e)[:140]
        print(f"[house] {err}")
    return filings[:200], err


def lambda_handler(event, context):
    t0 = time.time()
    txns, reports, s_err = senate_fetch()
    filings, h_err = house_fetch()
    with_tk = [t for t in txns if t.get("ticker")]
    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "source": "OFFICIAL — Senate eFD + House Clerk (no vendor)",
           "senate": {"n_reports": len(reports), "n_transactions": len(txns),
                      "n_with_ticker": len(with_tk), "reports": reports[:25],
                      "transactions": txns[:200], "error": s_err},
           "house": {"n_ptr_filings": len(filings),
                     "filings": filings[:100], "error": h_err}}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=1800")
    print(f"[congress-direct] senate tx={len(txns)} (tickered {len(with_tk)}) "
          f"reports={len(reports)} house PTR={len(filings)} "
          f"errs=({s_err},{h_err}) {round(time.time() - t0, 1)}s")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "senate_tx": len(txns), "house": len(filings)})}
