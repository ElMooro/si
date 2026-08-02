"""justhodl-house-ptr-extract v1.0 — ops 4273.

House PTRs carry per-trade tickers only inside PDFs
(disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{DocID}.pdf).
congress-direct collects the filing index; this engine walks NEW doc_ids,
extracts text with vendored pypdf, and line-parses the PTR table into
trade rows in the exact quiver-shape political-stocks consumes.

Honesty rules:
  * electronically-filed PTRs are text PDFs and parse; paper/scanned
    ones extract no text -> recorded {doc_id: "no_text"}, never guessed
  * every parsed doc keeps per-doc stats (n_rows, n_tickers) in the
    ledger so quality is inspectable, not asserted
  * incremental: MAX_DOCS_PER_RUN new docs per invoke; ledger capped

Output data/house-ptr-trades.json:
  {generated_at, version, stats{...},
   trades:[{Representative, TransactionDate, Ticker, Transaction,
            Range, House:"Representatives", _source:"house_ptr_pdf",
            doc_id, owner, asset}...],
   docs:{doc_id:{status, n_rows, n_tickers, filer, filing_date}}}
"""
import io
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

from pypdf import PdfReader

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SRC_KEY = "data/congress-direct.json"
OUT_KEY = "data/house-ptr-trades.json"
MAX_DOCS_PER_RUN = int(os.environ.get("MAX_DOCS_PER_RUN", "20"))
MAX_TRADES_KEPT = 1500
MAX_DOCS_KEPT = 900
UA = "Mozilla/5.0 (JustHodl research; contact via github.com/ElMooro)"
VERSION = "1.1"

s3 = boto3.client("s3", region_name="us-east-1")

# PTR table row anatomy (electronic filings, Clerk generator):
#   SP  Apple Inc. (AAPL) [ST]  P  01/15/2026  01/20/2026  $1,001 - $15,000
# Owner is optional; asset may wrap lines; type is P / S / S (partial) / E.
RX_TICKER = re.compile(r"\(([A-Z][A-Z0-9.]{0,5})\)")
# Clerk generator variant glues dates: "06/12/202607/08/2026$1,001".
# Word boundaries can't fire between 2026|07, so no \b anchors.
RX_DATE = re.compile(r"(\d{2})/(\d{2})/(\d{4})")
RX_AMOUNT = re.compile(
    r"\$[\d,]+\s*-\s*\$[\d,]+|\$1,000,000\s*\+|Over\s*\$50,000,000")
RX_TYPE = re.compile(r"\b(P|S|E)\b(?:\s*\(partial\))?", re.I)
RX_OWNER = re.compile(r"^(SP|DC|JT)\b")
# Clerk noise rows between trades: "F… S…: New" (Filing Status) and
# "S… O…: <account>" (Subholding Of) -- \x00-padded labels.
NOISE_LINE = re.compile(r"^[A-Z][^A-Za-z]{0,12}[A-Z][^A-Za-z]{0,12}:\s")
SKIP_LINE = re.compile(
    r"filing|asset\s+owner|transaction|notification|amount|cap\.?\s*gains|"
    r"description|status|subholding|comment|initial public offering|"
    r"^\s*\$?\s*$|^\d+\s*$|clerk|report|district", re.I)


def iso(mdy):
    m = RX_DATE.search(mdy or "")
    return f"{m.group(3)}-{m.group(1)}-{m.group(2)}" if m else None


def parse_ptr_text(text, filer):
    """Line-join then row-split: a row is complete once it carries a
    ticker, a P/S/E type, at least one date, and an amount range."""
    rows, buf = [], ""
    for raw in (text or "").splitlines():
        ln = re.sub(r"\s+", " ", raw).strip()
        if not ln or NOISE_LINE.search(ln) \
                or (SKIP_LINE.search(ln) and not RX_TICKER.search(ln)):
            continue
        buf = (buf + " " + ln).strip()
        tk = RX_TICKER.search(buf)
        am = RX_AMOUNT.search(buf)
        dates = RX_DATE.findall(buf)
        if not (tk and am and dates):
            if len(buf) > 600:  # runaway join guard
                buf = ln
            continue
        seg_after_tk = buf[tk.end():]
        ty = RX_TYPE.search(seg_after_tk)
        ttype = (ty.group(1).upper() if ty else "?")
        own = RX_OWNER.match(buf)
        d1 = "/".join(dates[0])
        rows.append({
            "Representative": filer,
            "BioGuideID": "",
            "TransactionDate": iso(d1),
            "ReportDate": iso("/".join(dates[1]) if len(dates) > 1 else d1),
            "Ticker": tk.group(1),
            "Transaction": {"P": "Purchase", "S": "Sale",
                            "E": "Exchange"}.get(ttype, ttype),
            "Range": am.group(0),
            "House": "Representatives",
            "_source": "house_ptr_pdf",
            "owner": own.group(1) if own else None,
            "asset": buf[max(0, tk.start() - 70):tk.start()].strip()[-70:],
        })
        buf = ""
    # de-dupe identical rows within a doc
    seen, out = set(), []
    for r_ in rows:
        k = (r_["Ticker"], r_["TransactionDate"], r_["Transaction"],
             r_["Range"], r_["owner"])
        if k in seen:
            continue
        seen.add(k)
        out.append(r_)
    return out


def fetch_pdf(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read()


def load_json(key, default):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return default


def lambda_handler(event=None, context=None):
    t0 = time.time()
    cd = load_json(SRC_KEY, {})
    filings = ((cd.get("house") or {}).get("filings") or [])
    ledger = load_json(OUT_KEY, {})
    docs = ledger.get("docs") or {}
    trades = ledger.get("trades") or []

    reparse = bool((event or {}).get("reparse_zero"))
    def _eligible(f):
        did = f.get("doc_id")
        if not did or not f.get("pdf"):
            return False
        if did not in docs:
            return True
        if reparse:
            d = docs[did]
            return d.get("status") == "parsed" and not d.get("n_rows")
        return False
    new = [f for f in filings if _eligible(f)][:MAX_DOCS_PER_RUN]
    if reparse:
        print(f"[house-ptr] reparse_zero mode: {len(new)} zero-row docs")
    n_ok = n_notext = n_err = 0
    for f in new:
        did, filer = f["doc_id"], f.get("name") or "Unknown"
        rec = {"filer": filer, "filing_date": f.get("filing_date")}
        try:
            pdf = fetch_pdf(f["pdf"])
            text = ""
            reader = PdfReader(io.BytesIO(pdf))
            for pg in reader.pages[:12]:
                text += (pg.extract_text() or "") + "\n"
            if len(text.strip()) < 40:
                rec.update(status="no_text", n_rows=0, n_tickers=0)
                n_notext += 1
            else:
                rows = parse_ptr_text(text, filer)
                for r_ in rows:
                    r_["doc_id"] = did
                trades.extend(rows)
                rec.update(status="parsed", n_rows=len(rows),
                           n_tickers=len({r_["Ticker"] for r_ in rows}))
                n_ok += 1
            print(f"[house-ptr] {did} {filer[:24]}: {rec['status']} "
                  f"rows={rec.get('n_rows')}")
        except Exception as e:
            rec.update(status="error", error=str(e)[:120],
                       n_rows=0, n_tickers=0)
            n_err += 1
            print(f"[house-ptr] {did} ERROR {str(e)[:90]}")
        docs[did] = rec

    trades = sorted(trades,
                    key=lambda r_: r_.get("TransactionDate") or "",
                    reverse=True)[:MAX_TRADES_KEPT]
    if len(docs) > MAX_DOCS_KEPT:
        keep = sorted(docs, key=lambda k: docs[k].get("filing_date") or "",
                      reverse=True)[:MAX_DOCS_KEPT]
        docs = {k: docs[k] for k in keep}

    parsed_docs = [d for d in docs.values() if d.get("status") == "parsed"]
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(
            timespec="seconds"),
        "version": VERSION,
        "source": "disclosures-clerk.house.gov PTR PDFs via "
                  "congress-direct index (official)",
        "stats": {
            "docs_total": len(docs),
            "docs_parsed": len(parsed_docs),
            "docs_no_text": sum(1 for d in docs.values()
                                if d.get("status") == "no_text"),
            "docs_error": sum(1 for d in docs.values()
                              if d.get("status") == "error"),
            "trades_total": len(trades),
            "this_run": {"new_docs": len(new), "parsed": n_ok,
                         "no_text": n_notext, "errors": n_err,
                         "elapsed_s": round(time.time() - t0, 1)},
        },
        "trades": trades,
        "docs": docs,
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=600")
    print(f"[house-ptr] wrote {len(trades)} trades from "
          f"{len(parsed_docs)} parsed docs "
          f"({out['stats']['docs_no_text']} scanned/no-text, honest)")
    return {"ok": True, **out["stats"]["this_run"],
            "trades_total": len(trades)}
