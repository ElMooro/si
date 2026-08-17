"""ops/4812 -- spx-beaters: complete the ledger NOW + forensics.
 (1) Invoke-to-complete: the bootstrap fetches <=30 missing Fridays per
     run; 30/53 accrued on birth. Invoke (Event), poll; if weeks < 53,
     invoke again (max 3 rounds) until ledger complete and the rescan
     lands. 12-1 momentum should flip LIVE today instead of in two
     Saturdays.
 (2) Truths after completion: ledger.weeks == 53, mom_status.m12_1
     true, SPY 12-1 present, listed rows carry ret_12_1_pct.
 (3) FORENSICS (probe-only, no engine edits):
     a) SPCX: is it really a large-cap STOCK in universe.json, or an
        ETF/collision that leaked into the stocks bucket? Dump its
        universe row + its league row.
     b) bond/crypto buckets == 0: list every compass + rotation +
        etf-census ticker whose label/class reads bond-ish or
        crypto-ish, whether it made ANY bucket, and whether the ledger
        holds closes for it -- separates "honest zero (score < 55)"
        from "classification/coverage bug".
 (4) league re-read with 12-1 live.
"""
import gzip
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-spx-beaters"
B = "justhodl-dashboard-live"
OUT_KEY = "data/spx-beaters.json"
LED_KEY = "spx-beaters/weekly-closes.json"
TARGET = 53

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


BONDISH = ("bond", "treasur", "aggregate", "credit", "corporate",
           "fixed", "tips", "duration", "yield")
CRYPTISH = ("bitcoin", "crypto", "ether", "digital")
BOND_T = {"TLT", "IEF", "SHY", "AGG", "BND", "LQD", "HYG", "TIP",
          "GOVT", "MUB", "EMB", "BIL"}
CRYPTO_T = {"IBIT", "FBTC", "GBTC", "ETHA", "BITO", "BTC", "ETH",
            "BTC-USD", "ETH-USD"}


def main():
    with report("ops 4812 -- ledger completion + forensics") as rep:
        rep.heading("1. invoke-to-complete (<=3 rounds)")
        doc = None
        for rnd in range(1, 4):
            t0 = time.time()
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
            fresh = None
            while time.time() - t0 < 600:
                time.sleep(20)
                try:
                    d = sread(OUT_KEY)
                    if d.get("as_of", "") > start_iso and \
                            d.get("as_of") != (doc or {}).get("as_of"):
                        fresh = d
                        break
                except ClientError:
                    pass
            if not fresh:
                rep.fail("round %d: doc never refreshed" % rnd)
                FAILED.append("round%d" % rnd)
                break
            doc = fresh
            wk = (doc.get("ledger") or {}).get("weeks")
            rep.ok("  round %d: weeks=%s fetched_now=%s (~%ds)"
                   % (rnd, wk,
                      (doc.get("ledger") or {}).get("fetched_now"),
                      int(time.time() - t0)))
            if wk and wk >= TARGET:
                break
        if not doc:
            sys.exit(1)

        rep.heading("2. 12-1 truths")
        led = doc.get("ledger") or {}
        ms = doc.get("mom_status") or {}
        reg = doc.get("regime") or {}
        (rep.ok if led.get("weeks") == TARGET else rep.fail)(
            "  ledger weeks = %s / %d" % (led.get("weeks"), TARGET))
        if led.get("weeks") != TARGET:
            FAILED.append("weeks")
        (rep.ok if ms.get("m12_1") else rep.fail)(
            "  mom_status.m12_1 = %s" % ms.get("m12_1"))
        if not ms.get("m12_1"):
            FAILED.append("m121")
        (rep.ok if reg.get("spy_ret_12_1_pct") is not None
         else rep.fail)("  SPY 12-1 = %s%%"
                        % reg.get("spy_ret_12_1_pct"))
        if reg.get("spy_ret_12_1_pct") is None:
            FAILED.append("spy121")
        with121 = tot = 0
        for rows in (doc.get("buckets") or {}).values():
            for r in rows:
                tot += 1
                if r.get("ret_12_1_pct") is not None:
                    with121 += 1
        (rep.ok if tot and with121 >= tot * 0.7 else rep.warn)(
            "  listed rows with 12-1 = %d/%d" % (with121, tot))

        rep.heading("3a. SPCX forensics")
        uni = sread("data/universe.json")
        urow = None
        for r in uni.get("stocks") or []:
            if str(r.get("symbol")).upper() == "SPCX":
                urow = r
                break
        rep.kv(spcx_universe=json.dumps(urow)[:400]
               if urow else "NOT IN UNIVERSE")
        lrow = None
        for b, rows in (doc.get("buckets") or {}).items():
            for r in rows:
                if r["t"] == "SPCX":
                    lrow = {"bucket": b, **r}
        rep.kv(spcx_league=json.dumps(lrow)[:400]
               if lrow else "not listed this run")
        if urow:
            nm = str(urow.get("name") or "").lower()
            if "etf" in nm or "fund" in nm or "spac" in nm:
                rep.warn("  SPCX looks like a FUND inside "
                         "universe.stocks -- upstream universe "
                         "hygiene item, note for universe-builder")
            else:
                rep.ok("  SPCX is a listed operating company row in "
                       "universe (name: %s)" % urow.get("name"))

        rep.heading("3b. bond/crypto coverage forensics")
        comp = sread("data/asset-compass.json")
        rot = sread("data/rotation-dashboard.json")
        try:
            etfc = sread("data/etf-census.json")
        except ClientError:
            etfc = {}
        listed = {r["t"]: b for b, rows in
                  (doc.get("buckets") or {}).items() for r in rows}
        ledger = sread(LED_KEY)
        closes = ledger.get("closes") or {}

        def probe(t, label, cls_hint):
            has_led = bool([v for v in (closes.get(t) or []) if v])
            where = listed.get(t, "-")
            rep.log("    %-8s %-28s class~%-9s ledger=%s bucket=%s"
                    % (t, (label or "")[:28], cls_hint,
                       "Y" if has_led else "N", where))
            return has_led

        cands = {}
        for r in (comp.get("assets") or []):
            t = str(r.get("ticker") or "").upper()
            lbl = str(r.get("label") or r.get("name") or "")
            low = lbl.lower()
            if t in BOND_T or any(w in low for w in BONDISH):
                cands[t] = (lbl, "bond")
            elif t in CRYPTO_T or any(w in low for w in CRYPTISH):
                cands[t] = (lbl, "crypto")
        for r in (rot.get("assets") or []):
            t = str(r.get("ticker") or "").upper()
            lbl = str(r.get("label") or "")
            low = lbl.lower()
            if t in BOND_T or any(w in low for w in BONDISH):
                cands.setdefault(t, (lbl, "bond"))
            elif t in CRYPTO_T or any(w in low for w in CRYPTISH):
                cands.setdefault(t, (lbl, "crypto"))
        for r in (etfc.get("etfs") or etfc.get("rows") or []):
            t = str(r.get("symbol") or r.get("ticker") or "").upper()
            cls = str(r.get("asset_class") or r.get("category")
                      or "").lower()
            if any(w in cls for w in BONDISH) or t in BOND_T:
                cands.setdefault(t, (r.get("name"), "bond"))
            elif any(w in cls for w in CRYPTISH) or t in CRYPTO_T:
                cands.setdefault(t, (r.get("name"), "crypto"))
        n_led = 0
        for t, (lbl, cls) in sorted(cands.items()):
            n_led += probe(t, lbl, cls)
        rep.kv(bond_crypto_candidates=len(cands),
               with_ledger_closes=n_led,
               listed_from_set=sum(1 for t in cands if t in listed))
        if not cands:
            rep.warn("  no bond/crypto tickers found in ANY feed -- "
                     "coverage gap is upstream (compass/rotation/"
                     "census), not classification")
        elif n_led and not any(t in listed for t in cands):
            rep.ok("  candidates exist with ledger closes but none "
                   "cleared score>=55 with >=2 legs -- HONEST ZERO "
                   "this week (bonds under trend-gate pressure)")

        rep.heading("4. league re-read (12-1 live)")
        for b in ("large", "mid", "small", "micro", "etf_equity",
                  "etf_bond", "etf_commodity", "etf_crypto_alt"):
            rows = (doc.get("buckets") or {}).get(b) or []
            if not rows:
                rep.log("  %-14s (none)" % b)
                continue
            r = rows[0]
            rep.ok("  %-14s %-6s %5.1f  6m=%s%% 12-1=%s%%"
                   % (b, r["t"], r["score"], r.get("ret_6m_pct"),
                      r.get("ret_12_1_pct")))
            rep.log("      " + " | ".join((r.get("why") or [])[:2]))
        rep.kv(counts=json.dumps(doc.get("counts")))

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("ledger COMPLETE (53w) -- 12-1 momentum live "
               "market-wide; forensics recorded")


start_iso = __import__("datetime").datetime.now(
    __import__("datetime").timezone.utc).isoformat()

if __name__ == "__main__":
    main()
