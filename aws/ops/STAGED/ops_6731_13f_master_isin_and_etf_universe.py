"""ops 6731 - READ-ONLY: (1) does OpenFIGI answer ID_ISIN (the ISIN derived exactly from a US CUSIP: 'US' + CUSIP +
check digit) with the US composite row where ID_CUSIP + exchCode US returned nothing (Exxon, Electronic Arts, Kenvue,
Warner Bros Discovery, ops 6730)? (2) three examples of the 177 'US / Closed-End Fund' ambiguous rows (candidate
lists), to design the same-ticker collapse. (3) every fund-like identity in the current cohort books that the
reviewed ETF table does not know, by reported value (top 400), computed with the book engine's own split/classify
code from the live desk, artifacts and master - the list the table is extended from.
<= 2 OpenFIGI requests; S3 reads only. Writes nothing but this report.
"""
from decimal import Decimal
from pathlib import Path
import importlib.util
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
import openfigi  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)
CUSIPS = ["30231G102", "285512109", "49177J102", "934423104", "037833100"]


def isin_from_cusip(cusip, country="US"):
    body = country + cusip
    digits = ""
    for ch in body:
        digits += str(ord(ch) - 55) if ch.isalpha() else ch
    total, double = 0, True
    for ch in reversed(digits):
        d = int(ch)
        if double:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        double = not double
    return body + str((10 - total % 10) % 10)


def load_engine():
    spec = importlib.util.spec_from_file_location("book13f", ROOT / "aws/lambdas/justhodl-13f-book/source/lambda_function.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["book13f"] = mod
    spec.loader.exec_module(mod)
    return mod


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6731_13f_master_isin_and_etf_universe") as r:
        r.heading("ops 6731 - security master: ISIN-derived lookups, CEF ambiguity examples, the unclassified ETF universe (read-only)")
        key = openfigi.get_api_key()
        deadline = time.monotonic() + 60
        r.section("1. ID_ISIN derived from the CUSIP (exchCode US, then any)")
        assert isin_from_cusip("037833100") == "US0378331005", isin_from_cusip("037833100")
        jobs = [{"idType": "ID_ISIN", "idValue": isin_from_cusip(c), "exchCode": "US"} for c in CUSIPS] + [{"idType": "ID_ISIN", "idValue": isin_from_cusip(c)} for c in CUSIPS]
        res = openfigi.mapping(jobs, api_key=key, deadline=deadline) or []
        for j, item in zip(jobs, res):
            tag = "%s %s" % (j["idValue"], j.get("exchCode") or "any")
            if isinstance(item, dict) and "data" in item:
                rows = item["data"]
                us = [x for x in rows if x.get("exchCode") == "US"]
                r.log("%s -> %d rows; US rows %s; exch:ticker %s" % (tag, len(rows), json.dumps([{k: x.get(k) for k in ("ticker", "name", "securityType", "marketSector", "figi")} for x in us]),
                                                                    " ".join("%s:%s" % (x.get("exchCode"), x.get("ticker")) for x in rows[:30])))
            else:
                r.log("%s -> %s" % (tag, json.dumps(item)[:160]))
        r.section("2. 'US / Closed-End Fund' ambiguous examples")
        master = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-security-master.json")["Body"].read())
        rows = master.get("by_cusip") or {}
        shown = 0
        for c, row in rows.items():
            if row.get("status") != "ambiguous":
                continue
            cands = row.get("candidates") or []
            if cands and all(x.get("exchCode") == "US" for x in cands):
                r.log("%s %s: %s" % (c, row.get("query"), json.dumps(cands)))
                shown += 1
                if shown >= 6:
                    break
        r.section("3. unclassified fund-like identities in the cohort books (engine code, live data)")
        mod = load_engine()
        mod.S3_BUCKET = LIVE
        desk = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-desk.json")["Body"].read())
        required = desk.get("required_period_for_current_cohort")
        etf_classes, reviewed = mod.load_etf_table()
        classer = mod.Classer(rows, etf_classes)
        agg = {}
        n_funds = 0
        for fk, meta in sorted((desk.get("by_fund") or {}).items()):
            if not meta.get("current_cohort_eligible"):
                continue
            k = (meta.get("detail") or {}).get("key")
            detail = json.loads(s3.get_object(Bucket=LIVE, Key=k)["Body"].read())
            if detail.get("current_holdings_period") != required:
                continue
            n_funds += 1
            sp = mod.split_positions(detail)
            for pos in sp["eq"].values():
                c = classer.of(pos.get("identity"))
                if c["kind"] != "fund":
                    continue
                t = c["ticker"] or "?"
                a = agg.setdefault(t, {"ticker": t, "name": c["name"], "type": c["security_type"], "sub": c["sub"], "value": Decimal(0), "holders": set(), "cusips": set(), "issuer": (pos.get("issuer_names") or [""])[0]})
                a["value"] += mod.dec(pos.get("reported_value_usd"))
                a["holders"].add(fk)
                a["cusips"].add((pos.get("identity") or {}).get("cusip"))
        classed = [a for a in agg.values() if a["sub"] != "unclassified"]
        un = sorted((a for a in agg.values() if a["sub"] == "unclassified"), key=lambda a: -a["value"])
        tot = sum((a["value"] for a in agg.values()), Decimal(0))
        untot = sum((a["value"] for a in un), Decimal(0))
        r.log("cohort books %d | fund-like tickers %d (classed %d = %s, unclassified %d = %s, %.1f%% of fund value) | table %s (%d)" % (
            n_funds, len(agg), len(classed), "$%.2fB" % (sum((a["value"] for a in classed), Decimal(0)) / Decimal(1e9)), len(un), "$%.2fB" % (untot / Decimal(1e9)),
            float(100 * untot / tot) if tot else 0.0, reviewed, len(etf_classes)))
        r.log("TOP 400 unclassified (ticker | OpenFIGI name | type | holders | value | cusip | filer text):")
        for a in un[:400]:
            r.log("  %-7s | %-48s | %-16s | %2d | %12s | %s | %s" % (a["ticker"], (a["name"] or "")[:48], a["type"], len(a["holders"]), "$%.1fM" % (a["value"] / Decimal(1e6)), ",".join(sorted(x or "" for x in a["cusips"])), a["issuer"][:36]))
        r.log("```json\n%s\n```" % json.dumps([{"t": a["ticker"], "n": a["name"], "ty": a["type"], "h": len(a["holders"]), "v": float(a["value"])} for a in un[:1200]], separators=(",", ":")))
        r.ok("probe complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
