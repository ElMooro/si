#!/usr/bin/env python3
"""ops 3748 — DIAGNOSE the scanner's 95% "failure" rate.

3744 forensics: scanner parsed 3,000 filings, extracted 251 buy transactions,
"2838 failed". A 95% failure rate is either (a) the 2-HTTP-call-per-filing
fetch (index.json THEN the xml) starving under SEC's 10 req/s cap, or (b) most
Form 4s simply not being PURCHASES (code 'P') — sells, gifts, option exercises,
grants are the majority of insider filings and are correctly skipped. These
have completely different fixes, so measure before touching code.

This ops runs a SMALL controlled sample from inside a throwaway Lambda-like
context on the runner (SEC allows it with a UA), classifying each filing:
  FETCH_FAIL     index.json or xml GET failed / timed out
  PARSE_ERROR    xml present but ET.fromstring raised
  NO_NONDERIV    parsed, but no nonDerivativeTable
  NOT_PURCHASE   parsed, transactions present, but none coded 'P' (A/S/G/M/F..)
  PURCHASE       at least one code 'P' acquire
=> if NOT_PURCHASE dominates, the "failure" is a MISNOMER and the engine is
   fine; if FETCH_FAIL dominates, batch the fetch (use the daily index's
   direct doc URL, skip the index.json round trip).
"""
import json
import ssl
import sys
import time
import traceback
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
CTX = ssl.create_default_context()

with report("3748_scanner_parse_diag") as rep:
    rep.heading("ops 3748 — scanner 95% failure classification")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3748.json").write_text(json.dumps({"verdict": "STARTED"}))

    last = [0.0]

    def sec_get(url, timeout=12):
        # honor SEC 10 req/s
        dt = time.time() - last[0]
        if dt < 0.11:
            time.sleep(0.11 - dt)
        last[0] = time.time()
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            return r.read().decode("utf-8", "replace")

    try:
        # ── pull a recent daily Form 4 index ─────────────────────────────
        rep.section("A — recent Form 4 filings from the daily index")
        # find a business day with data (skip weekends)
        d = datetime.now(timezone.utc).date()
        filings = []
        for _ in range(6):
            d -= timedelta(days=1)
            if d.weekday() >= 5:
                continue
            q = d.strftime("%Y/QTR") + str((d.month - 1) // 3 + 1)
            url = "https://www.sec.gov/Archives/edgar/daily-index/%s/form.%s.idx" % (
                q, d.strftime("%Y%m%d"))
            try:
                txt = sec_get(url, timeout=20)
            except Exception as e:
                rep.log("  %s: %s" % (d, str(e)[:70]))
                continue
            for line in txt.splitlines():
                if line.startswith("4 ") or " 4  " in line[:14]:
                    parts = [p for p in line.split("  ") if p.strip()]
                    path = next((p for p in parts if p.strip().endswith(".txt")), None)
                    cik = parts[0].split()[-1] if parts else None
                    if path:
                        filings.append(path.strip())
            if filings:
                rep.ok("  %s: %d Form-4 rows" % (d, len(filings)))
                break
        if not filings:
            rep.fail("no daily index Form 4 rows found")
            sys.exit(1)

        sample = filings[:80]
        rep.log("  classifying %d filings" % len(sample))

        # ── classify each ────────────────────────────────────────────────
        rep.section("B — per-filing classification")
        cls = Counter()
        codes = Counter()
        examples = {}
        for path in sample:
            # path like edgar/data/CIK/ACCESSION.txt
            base = "https://www.sec.gov/Archives/" + path.rsplit(".txt", 1)[0]
            acc = path.rsplit("/", 1)[-1].replace(".txt", "")
            accn = acc.replace("-", "")
            cik = path.split("/")[2] if len(path.split("/")) > 2 else ""
            folder = "https://www.sec.gov/Archives/edgar/data/%s/%s/" % (cik, accn)
            xml_text = None
            try:
                idx = json.loads(sec_get(folder + "index.json", timeout=12))
                items = idx.get("directory", {}).get("item", [])
                name = None
                for it in items:
                    n = it.get("name", "")
                    if n.endswith(".xml") and ("form4" in n.lower()
                                               or "primary_doc" in n.lower()
                                               or "edgar.xml" in n.lower()):
                        name = n
                        break
                if not name:
                    for it in items:
                        n = it.get("name", "")
                        if n.endswith(".xml") and "metadata" not in n.lower():
                            name = n
                            break
                if name:
                    xml_text = sec_get(folder + name, timeout=12)
            except Exception as e:
                cls["FETCH_FAIL"] += 1
                examples.setdefault("FETCH_FAIL", str(e)[:80])
                continue
            if not xml_text:
                cls["FETCH_FAIL"] += 1
                continue
            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError as e:
                cls["PARSE_ERROR"] += 1
                examples.setdefault("PARSE_ERROR", str(e)[:80])
                continue
            nd = root.find("nonDerivativeTable")
            if nd is None:
                cls["NO_NONDERIV"] += 1
                continue
            txn_codes = []
            for txn in nd.findall("nonDerivativeTransaction"):
                c = txn.find("transactionCoding/transactionCode")
                if c is not None and c.text:
                    txn_codes.append(c.text.strip())
            for c in txn_codes:
                codes[c] += 1
            if "P" in txn_codes:
                cls["PURCHASE"] += 1
            elif txn_codes:
                cls["NOT_PURCHASE"] += 1
                examples.setdefault("NOT_PURCHASE", ",".join(txn_codes[:6]))
            else:
                cls["NO_TXN"] += 1

        rep.section("C — results")
        total = sum(cls.values())
        for k, v in cls.most_common():
            rep.log("  %-14s %4d  (%4.1f%%)  eg=%s"
                    % (k, v, v / total * 100 if total else 0,
                       examples.get(k, "")))
        rep.log("  transaction code census: %s" % codes.most_common(12))

        # ── verdict ──────────────────────────────────────────────────────
        rep.section("VERDICT")
        purch = cls.get("PURCHASE", 0)
        notp = cls.get("NOT_PURCHASE", 0)
        fetchf = cls.get("FETCH_FAIL", 0)
        diagnosis = ("NOT_PURCHASE dominates — the 95%% 'failure' is a MISNOMER; "
                     "most Form 4s are sells/grants/exercises correctly skipped"
                     if notp >= purch and notp > fetchf
                     else "FETCH_FAIL material — the 2-call round trip is starving"
                     if fetchf > total * 0.2
                     else "mixed; inspect code census")
        rep.log("  DIAGNOSIS: %s" % diagnosis)
        Path("aws/ops/reports/3748.json").write_text(
            json.dumps({"verdict": "PASS", "classes": dict(cls),
                        "codes": dict(codes), "diagnosis": diagnosis},
                       indent=2))
        rep.kv(sample=total, purchases=purch, not_purchase=notp,
               fetch_fail=fetchf, diagnosis=diagnosis[:60])
        rep.ok("DIAG COMPLETE")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3748.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
