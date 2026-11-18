"""ops 6831 - READ-ONLY audit before fixing the insider scanner and the ARK tracker (re-runnable).

Khalid 2026-11-17: "Yes fix them" - (1) the insider purchase list carries a $9.9B LONA record priced at shares / 1000
and one company under three tickers (MNK / MNKKQ / MNKPF); (2) the ARK engine publishes only its 15 largest moves.
Audit before building, on the real data:
  A. the raw banked insider rows behind LONA and the MNK tickers, exactly as the provider published them;
  B. every open-market purchase in the engine's window against the factor book's tape (last price, 52-week range,
     market value): how many tickers the book knows, which rows sit outside the tape's range, which rows the book
     cannot price, and how large a purchase is against the company's market value;
  C. filings that appear under more than one ticker for the same issuer (same accession, insider, date, shares);
  D. the stored versions of the ARK snapshots, so the snapshot the published diff was measured against can be read
     back and the engine's new complete lists can be reconciled with the counts it published.
Embeds a compact pack (purchase rows, book references, the two ARK snapshots) for offline tests. Writes nothing.
"""
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import os
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=60)
WINDOW_DAYS, MIN_VALUE = 33, 5000.0          # the engine reads LOOKBACK_DAYS + 3 feed days; the live threshold is $5,000
ACC = re.compile(r"(\d{10}-\d{2}-\d{6})")
REF = ["name", "price", "close", "high_52w", "low_52w", "mcap", "exchange", "is_otc", "is_common"]


def purchase(r):
    tt = (r.get("transactionType") or "").upper().strip() if isinstance(r, dict) else ""
    return tt.startswith("P-") or tt == "P"


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    pack = {}
    with report("ops_6831_insider_ark_engine_audit") as r:
        r.heading("ops 6831 - insider scanner + ARK tracker, audit before the fix %s (read-only)" % now.isoformat(timespec="minutes"))
        import fmp_bank
        import bank_enrich

        r.section("A. the banked insider feed")
        days = fmp_bank.walker_days("insider-trading-latest", days=WINDOW_DAYS, where=purchase)
        rows = []
        for day, day_rows in days:
            for x in day_rows:
                ad = (x.get("acquisitionOrDisposition") or x.get("acquisitionDisposition") or "A").upper().strip()
                if ad and ad != "A":
                    continue
                sym = (x.get("symbol") or "").strip().upper()
                try:
                    shares, price = float(x.get("securitiesTransacted") or 0), float(x.get("price") or 0)
                except (TypeError, ValueError):
                    continue
                if not sym or sym == "N/A" or len(sym) > 6 or shares <= 0 or price <= 0 or shares * price < MIN_VALUE:
                    continue
                m = ACC.search(str(x.get("url") or x.get("link") or ""))
                rows.append({"day": day, "ticker": sym, "cik": str(x.get("companyCik") or ""), "insider": (x.get("reportingName") or "").strip(), "rcik": str(x.get("reportingCik") or ""),
                             "owner": x.get("typeOfOwner") or "", "type": x.get("transactionType") or "", "shares": shares, "price": price, "value": round(shares * price, 2),
                             "txn_date": (x.get("transactionDate") or "")[:10], "filed": (x.get("filingDate") or "")[:10], "acc": m.group(1) if m else "", "form": x.get("formType") or "",
                             "security": x.get("securityName") or "", "owned_after": x.get("securitiesOwned"), "direct": x.get("directOrIndirect") or ""})
        r.log("feed days read %d (%s .. %s), purchase rows kept %d, tickers %d" % (len(days), days[-1][0] if days else "-", days[0][0] if days else "-", len(rows), len({x["ticker"] for x in rows})))
        if days:
            sample = days[0][1][0] if days[0][1] else {}
            r.log("raw row keys: %s" % sorted(sample.keys()))
        for sym in ("LONA", "MNK", "MNKKQ", "MNKPF"):
            hit = [x for x in rows if x["ticker"] == sym]
            r.log("**%s**: %d rows" % (sym, len(hit)))
            for x in hit[:6]:
                r.log("    %s" % json.dumps({k: x[k] for k in ("day", "cik", "insider", "owner", "type", "shares", "price", "value", "txn_date", "filed", "acc", "form", "security", "owned_after", "direct")}))

        r.section("B. every purchase against the factor book's tape")
        ok_book = bank_enrich.available()
        r.log("factor book available %s, as_of %s" % (ok_book, json.dumps(bank_enrich.as_of())[:300]))
        tickers = sorted({x["ticker"] for x in rows})
        ref = {}
        for t in tickers:
            row = bank_enrich.lookup(t, REF) if ok_book else None
            if row:
                ref[t] = row
        r.log("tickers with a purchase %d, in the book %d, not in the book %d" % (len(tickers), len(ref), len(tickers) - len(ref)))
        outside, unpriced, heavy = [], [], []
        for x in rows:
            b = ref.get(x["ticker"])
            px = (b or {}).get("price") or (b or {}).get("close")
            hi, lo, mcap = (b or {}).get("high_52w"), (b or {}).get("low_52w"), (b or {}).get("mcap")
            x["ref"] = px
            if not b or not px:
                unpriced.append(x)
                continue
            x["ratio"] = round(x["price"] / px, 4)
            if hi and lo and not (0.5 * lo <= x["price"] <= 1.5 * hi):
                outside.append(x)
            if mcap and x["value"] / mcap > 0.10:
                heavy.append((round(x["value"] / mcap, 3), x))
        r.log("rows priced by the book %d, outside 0.5 x 52w low .. 1.5 x 52w high: %d, not priced: %d, a single purchase above 10%% of market value: %d" % (len(rows) - len(unpriced), len(outside), len(unpriced), len(heavy)))
        for x in sorted(outside, key=lambda z: -z["value"])[:40]:
            b = ref[x["ticker"]]
            r.log("    OUTSIDE %-6s $%14.0f  %s sh at %.4f | tape %.4f (52w %.2f - %.2f) ratio %.3f | %s | %s | %s" % (x["ticker"], x["value"], int(x["shares"]), x["price"], x["ref"], b.get("low_52w") or 0, b.get("high_52w") or 0, x["ratio"], x["owner"][:28], x["insider"][:30], x["txn_date"]))
        ratios = sorted(x["ratio"] for x in rows if x.get("ratio"))
        if ratios:
            q = lambda p: ratios[min(len(ratios) - 1, int(p * len(ratios)))]  # noqa: E731
            r.log("reported price / tape: p1 %.3f p5 %.3f p25 %.3f median %.3f p75 %.3f p95 %.3f p99 %.3f max %.3f" % (q(.01), q(.05), q(.25), q(.5), q(.75), q(.95), q(.99), ratios[-1]))
        by_t = defaultdict(float)
        for x in unpriced:
            by_t[x["ticker"]] += x["value"]
        r.log("not priced by the book: %d tickers, $%.0f; largest: %s" % (len(by_t), sum(by_t.values()), ", ".join("%s $%.0f" % kv for kv in sorted(by_t.items(), key=lambda kv: -kv[1])[:25])))
        for x in sorted(unpriced, key=lambda z: -z["value"])[:12]:
            r.log("    UNPRICED %-6s $%14.0f  %s sh at %.4f | %s | %s | %s | %s" % (x["ticker"], x["value"], int(x["shares"]), x["price"], x["owner"][:28], x["insider"][:30], x["txn_date"], x["security"][:30]))
        for share, x in sorted(heavy, key=lambda z: -z[0])[:15]:
            r.log("    HEAVY %-6s %.1f%% of market value: $%.0f at %.4f (tape %.4f) | %s | %s" % (x["ticker"], share * 100, x["value"], x["price"], x["ref"], x["owner"][:28], x["insider"][:30]))

        r.section("C. one filing under several tickers")
        groups = defaultdict(set)
        for x in rows:
            groups[(x["cik"], x["acc"] or "%s|%s" % (x["insider"], x["txn_date"]), x["insider"], x["txn_date"], int(x["shares"]), round(x["price"], 4))].add(x["ticker"])
        multi = defaultdict(int)
        for key, syms in groups.items():
            if len(syms) > 1 and key[0]:
                multi[(key[0], tuple(sorted(syms)))] += 1
        r.log("issuer + filing + insider + date + shares + price seen under more than one ticker: %d issuer/ticker sets" % len(multi))
        for (cik, syms), n in sorted(multi.items(), key=lambda kv: -kv[1])[:30]:
            r.log("    CIK %s: %s (%d identical transactions) book: %s" % (cik, ", ".join(syms), n, ", ".join("%s=%s" % (s, "yes" if s in ref else "no") for s in syms)))
        by_cik = defaultdict(set)
        for x in rows:
            if x["cik"]:
                by_cik[x["cik"]].add(x["ticker"])
        shared = {c: s for c, s in by_cik.items() if len(s) > 1}
        r.log("issuers with more than one ticker in the window: %d (%s)" % (len(shared), "; ".join("%s: %s" % (c, ",".join(sorted(s))) for c, s in list(shared.items())[:20])))
        r.log("rows without an issuer CIK: %d" % sum(1 for x in rows if not x["cik"]))

        r.section("D. ARK snapshots and their stored versions")
        ark = {}
        for key in ("data/ark-holdings-prev.json", "data/ark-holdings.json"):
            vers = s3.list_object_versions(Bucket=LIVE, Prefix=key, MaxKeys=30).get("Versions") or []
            vers = [v for v in vers if v["Key"] == key]
            r.log("%s: %d stored versions: %s" % (key, len(vers), ", ".join("%s%s" % (v["LastModified"].strftime("%m-%d %H:%M"), "*" if v["IsLatest"] else "") for v in vers[:14])))
            ark[key] = vers
        cur = json.loads(s3.get_object(Bucket=LIVE, Key="data/ark-holdings.json")["Body"].read())
        want = cur.get("prev_snapshot_at")
        base = None
        for v in ark["data/ark-holdings-prev.json"]:
            if v["IsLatest"]:
                continue
            doc = json.loads(s3.get_object(Bucket=LIVE, Key="data/ark-holdings-prev.json", VersionId=v["VersionId"])["Body"].read())
            if doc.get("generated_at") == want:
                base = doc
                break
        r.log("the published diff was measured against the snapshot of %s; that stored version was %s" % (want, "found" if base else "NOT found"))
        if base:
            dates = sorted({p.get("date") for rows_ in base["holdings_by_fund"].values() for p in rows_})
            cdates = sorted({p.get("date") for rows_ in cur["holdings_by_fund"].values() for p in rows_})
            r.log("baseline holdings dated %s (%d lines); current holdings dated %s (%d lines); published counts %s" % (dates, sum(len(v) for v in base["holdings_by_fund"].values()), cdates, cur.get("n_positions_total"), json.dumps({k: v for k, v in cur["diff_vs_prev"].items() if k != "highlights"})))
        pack["ark"] = {"current": cur, "baseline": base}
        pack["insider"] = {"rows": [{k: v for k, v in x.items() if k not in ("ref", "ratio", "day", "form", "direct")} for x in rows], "book": ref, "book_as_of": bank_enrich.as_of(), "window_days": WINDOW_DAYS, "read_at": now.isoformat(timespec="seconds")}
        blob = base64.b64encode(gzip.compress(json.dumps(pack, separators=(",", ":"), default=str).encode("utf-8"), 9)).decode("ascii")
        r.log("pack: %d chars" % len(blob))
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        r.kv(purchase_rows=str(len(rows)), tickers=str(len(tickers)), outside_tape=str(len(outside)), unpriced_rows=str(len(unpriced)), multi_ticker_sets=str(len(multi)), ark_baseline_found=str(bool(base)))
        r.ok("AUDIT: %d purchase rows, %d outside the tape's range, %d issuer/ticker sets filed more than once, ARK baseline %s" % (len(rows), len(outside), len(multi), "read back" if base else "not available"))


if __name__ == "__main__":
    main()
    sys.exit(0)
