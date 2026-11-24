"""ops 6942 - DRY RUN of Credit Before Equity 2.0.0 on the real warehouse. Nothing is invoked and nothing is written.

The engine's own build() runs on the runner against the live bucket with its writes left out, and what it would publish is
checked here by arithmetic that does not use the engine's:

  1  three companies' spreads worked again from the raw bonds and the Treasury curve
  2  an industry's change worked again from the company rows
  3  the counts the page will reconcile (names, leads, awaiting) and that every lead's stock has not followed
  4  every tie made by a rule (not an exact name, not the reviewed list) printed for review, and the largest untied issuers
  5  the listed companies the factor book does not hold but the price bank does

It also cuts a small warehouse (a few dozen companies: their bonds, CDS rows, price files, the curve) with what the engine
publishes from exactly that cut, for the offline tests, and says how many earlier versions of the bond desk's document the
bucket still holds (a real one-week comparison could be built from them).
"""
from datetime import date, datetime, timezone
from pathlib import Path
import base64
import gzip
import io
import json
import math
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source")]
LIVE = "justhodl-dashboard-live"
TEN = {"1M": 1 / 12, "2M": 2 / 12, "3M": 0.25, "4M": 4 / 12, "6M": 0.5, "1Y": 1.0, "2Y": 2.0, "3Y": 3.0, "5Y": 5.0, "7Y": 7.0, "10Y": 10.0, "20Y": 20.0, "30Y": 30.0}
CUT_TICKERS = ["AAPL", "JPM", "F", "ORCL", "CHTR", "WHR", "PZZA", "BRK-B", "ET", "T", "BA", "INTC", "AAL", "ECHO", "MLCO", "BHC", "PRGO", "ADT", "KBR", "STC", "DUK", "XOM", "GS", "HCA", "OXY",
               "TROX", "IHRT", "AMD", "LKQ", "PRI", "SGRY", "MATV", "CVS", "KSS", "CAR", "DCH", "XRX", "HSBC", "SMFG", "RIO", "BUD", "TEVA", "CNC", "PCG", "NEE"]
CUT_ISSUERS = ["CLOUD SOFTWARE GROUP", "HUB INTERNATIONAL", "PETROLEOS MEXICANOS", "SAUDI ARABIAN OIL", "HILCORP ENERGY I", "LIFEPOINT HEALTH", "NISSAN MOTOR", "SOFTBANK GROUP", "ALTICE FRANCE",
               "PARAMOUNT SKYDANCE", "STAPLES", "MICHAELS", "STANDARD CHARTERED", "BNP PARIBAS", "MARS", "CHS COMMUNITY HEALTH SYSTEMS", "ASTON MARTIN CAPITAL HOLDINGS", "BURLINGTON NORTHERN SANTA",
               "PACIFICORP", "METROPOLITAN BANK", "PERSHING SQUARE HOLDINGS", "COX COMMUNICATIONS", "ELECTRONIC ARTS", "NORDSTROM", "CITGO PETROLEUM"]


class Body:
    def __init__(self, raw):
        self.raw = raw

    def read(self):
        return self.raw


class MemS3:
    """A bucket held in memory: the cut the offline tests run on."""

    def __init__(self, docs):
        self.docs = docs

    def get_object(self, Bucket, Key):
        if Key not in self.docs:
            raise KeyError("NoSuchKey " + Key)
        raw = json.dumps(self.docs[Key], separators=(",", ":")).encode()
        return {"Body": Body(gzip.compress(raw, 1) if Key.endswith(".gz") else raw)}

    def list_objects_v2(self, **kw):
        return {"Contents": [{"Key": k} for k in self.docs if k.startswith(kw.get("Prefix") or "")]}


class Recorder:
    """The live bucket, read-only, remembering every document it handed over."""

    def __init__(self, s3):
        self.s3, self.seen = s3, {}

    def get_object(self, Bucket, Key):
        out = self.s3.get_object(Bucket=Bucket, Key=Key)
        raw = out["Body"].read()
        self.seen[Key] = raw
        return {"Body": Body(raw)}

    def list_objects_v2(self, **kw):
        return self.s3.list_objects_v2(**kw)

    def put_object(self, **kw):
        raise RuntimeError("the dry run writes nothing")


def parse(raw):
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)


def no_nan(raw):
    def refuse(c):
        raise ValueError(c)
    try:
        json.loads(raw, parse_constant=refuse)
        return True
    except ValueError:
        return False


def par_at(row, years):
    pts = sorted((TEN[k], float(v)) for k, v in row.items() if k in TEN and isinstance(v, (int, float)))
    if years <= pts[0][0]:
        return pts[0][1]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if years <= x1:
            return y0 + (y1 - y0) * (years - x0) / (x1 - x0)
    return pts[-1][1]


def own_spread(bonds, curve_row, as_of):
    """A company's spread from its raw bonds, by this script's own arithmetic: (spread bp, bonds used)."""
    num = den = 0.0
    n = 0
    for b in bonds:
        y, dur, v = b.get("rate"), b.get("dur"), float(b.get("demand") or 0.0)
        if not isinstance(y, (int, float)) or not isinstance(dur, (int, float)) or not 0 < y < 49.99 or dur < 1.0:
            continue
        yy = max(y, 0.5) / 100.0
        x = dur * yy
        t = 30.0 if x >= 0.97 else -math.log(1.0 - x) / math.log(1.0 + yy)
        t = max(t, dur)
        try:
            left = (date.fromisoformat(str(b.get("maturity"))[:10]) - date.fromisoformat(as_of)).days / 365.25
            if left > 0:
                t = min(t, max(left, dur))
        except (TypeError, ValueError):
            pass
        t = min(t, 30.0)
        s = (y - par_at(curve_row, t)) * 100.0
        num += round(s, 1) * v
        den += v
        n += 1
    return (num / den if den else None), n


def main():
    import boto3
    from ops_report import report
    import bank_enrich
    import lambda_function as L
    import cbe_core as C
    import cbe_names as N
    real = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    with report("ops_6942_credit_before_equity_dry_run") as r:
        r.heading("ops 6942 - Credit Before Equity 2.0.0 dry run on the real warehouse (nothing written), %s" % now.isoformat(timespec="minutes"))
        rec = Recorder(real)
        logs = []
        built = L.build(rec, now, log=logs.append)
        for line in logs:
            r.log(line)
        p = built["payload"]
        cov = p["coverage"]
        raw = json.dumps(p, separators=(",", ":"), default=str).encode()
        r.log("document: %d bytes (%d gzipped); as of %s, stocks to %s" % (len(raw), len(gzip.compress(raw)), p["as_of"], p["stock_as_of"]))
        r.log("coverage: %s" % json.dumps({k: v for k, v in cov.items() if k != "stocks_no_file"}))
        r.log("stocks with no file in the price bank (%d): %s" % (len(cov["stocks_no_file"]), cov["stocks_no_file"]))
        r.log("windows: %s" % json.dumps(p["windows"]))
        r.log("degraded: %s" % p["degraded"])
        for g in p["gaps"]:
            r.log("gap: %s" % g)
        r.log("ledger days it would write: %s" % [(s["as_of"], "derived" if s["derived_from"] else "real", s["n"]) for s in built["snapshots"]])
        bad = []

        # 1 three companies by this script's own arithmetic
        bonds_doc = parse(rec.seen[L.BONDS_KEY])
        issues = bonds_doc["issues"]
        par_rows = parse(rec.seen[L.PAR_KEY])["rows"]
        curve = par_rows.get(issues["as_of"]) or par_rows[max(d for d in par_rows if d <= issues["as_of"])]
        cols = p["issuers"]["cols"]
        table = [dict(zip(cols, row)) for row in p["issuers"]["rows"]]
        by_ticker = {x["ticker"]: x for x in table if x["ticker"]}
        listed_rows = L.listed_companies(bank_enrich.rows(fields=list(L.BOOK_FIELDS)) or [])
        matcher = N.Matcher(listed_rows + [x for x in N.EXTRA_LISTED if x["symbol"] in cov["extra_listed"]])
        usd = [b for b in issues["bonds"] if b.get("ccy") == "USD" and b.get("book") in C.BOOKS and not b.get("left")]
        key_ticker = {}
        for b in usd:
            key, cut = N.issuer_key(b.get("issuer") or "")
            if key not in key_ticker:
                co, _ = matcher.match(key, cut, b.get("sector"), foreign=(b.get("country") or "") not in ("", "United States"))
                key_ticker[key] = co["symbol"] if co else None
        for t in ("AAPL", "CHTR", "JPM", "WHR"):
            mine, n = own_spread([b for b in usd if key_ticker.get(N.issuer_key(b.get("issuer") or "")[0]) == t], curve, issues["as_of"])
            theirs = (by_ticker.get(t) or {}).get("spread_bp")
            ok = mine is not None and theirs is not None and abs(mine - theirs) <= 0.6
            r.log("check 1 %-5s own arithmetic %s bp over %d bonds, the engine %s bp -> %s" % (t, None if mine is None else round(mine, 1), n, theirs, "agree" if ok else "DIFFER"))
            if not ok:
                bad.append("spread of %s" % t)

        # 2 an industry's change from the company rows
        for ind in p["industries"]["all"][:3]:
            name = ind["industry"]
            have = sorted((x["d_m1"], x["value"]) for x in table if x["industry"] == name and x["d_m1"] is not None)
            total, run, mid = sum(v for _, v in have), 0.0, None
            for d, v in have:
                run += v
                if run >= total / 2.0:
                    mid = d
                    break
            theirs = (ind["win"].get("m1") or {}).get("d_bp")
            ok = mid is not None and theirs is not None and abs(mid - theirs) < 0.06
            r.log("check 2 %-22s the middle company's change by value: own %s, the engine %s over %d companies -> %s" % (name, mid, theirs, len(have), "agree" if ok else "DIFFER"))
            if not ok:
                bad.append("industry %s" % name)

        # 3 the counts and the rule
        names = p["names"]
        leads = [x for x in names if x["signal"] in C.LEADS]
        tests = {
            "n_names is the row count": p["n_names"] == len(names),
            "n_leads is the lead rows": p["n_leads"] == len(leads),
            "leads is the first 20 lead rows": p["leads"] == leads[:20],
            "n_awaiting_history is the awaiting rows": p["n_awaiting_history"] == sum(1 for x in names if x["signal"] == "INSUFFICIENT_HISTORY"),
            "one row a ticker": len({x["ticker"] for x in names}) == len(names),
            "every issuer row is as wide as its header": all(len(row) == len(cols) for row in p["issuers"]["rows"]),
            "no lead whose stock already followed": all(not ((x["signal"] == "CREDIT_LEADS_DOWN" and x["d_price_pct"] <= -C.FLAT_BAND_PCT) or (x["signal"] == "CREDIT_LEADS_UP" and x["d_price_pct"] >= C.FLAT_BAND_PCT)) for x in leads),
            "every lead has credit past its bar": all(x.get("signal_basis") == "traded CDS" or abs(x["x_spread_bp"]) >= x["bar_bp"] for x in leads),
            "the 1.0 names are all there with their model": sum(1 for x in names if x.get("distance_to_default") is not None) >= 18,
            "no figure is NaN": no_nan(raw),
        }
        for what, ok in tests.items():
            r.log("check 3 %-45s %s" % (what, "ok" if ok else "FAILED"))
            if not ok:
                bad.append(what)
        sig = {}
        for x in names:
            sig[x["signal"]] = sig.get(x["signal"], 0) + 1
        r.log("signals: %s; basis %s" % (sig, {b: sum(1 for x in names if x.get("signal_basis") == b) for b in ("bond spreads", "traded CDS", None)}))
        for x in leads[:25]:
            r.log("  lead %-6s %-34s %-22s %s spread %s d %s past book %s (bar %s) stock %s%% cds %s (%s) -> %s" % (
                x["ticker"], str(x["name"])[:34], str(x.get("industry"))[:22], x.get("book"), x.get("spread_bp"), x.get("d_spread_bp"), x.get("x_spread_bp"), x.get("bar_bp"), x.get("d_price_pct"),
                x.get("cds_bp"), x.get("cds_says"), x["signal"]))
        r.log("industries, the month: " + "; ".join("%s %s bp" % (i["industry"], (i["win"].get("m1") or {}).get("d_bp")) for i in sorted(p["industries"]["all"], key=lambda i: -((i["win"].get("m1") or {}).get("d_bp") or -999))))

        # 4 ties for review
        for t in p["match"]["by_rule"]:
            r.log("tie by rule %-10s %-42s -> %-6s %s" % (t["how"], t["issuer"][:42], t["ticker"], t["company"]))
        r.log("largest untied: " + "; ".join("%s ($%.0fM)" % (t["issuer"], t["value"] / 1e6) for t in p["match"]["largest_untied"][:40]))
        dead = sorted({v for v in N.ALIASES.values() if v and v not in matcher.rows})
        r.log("reviewed-list tickers that are not a listed company today (%d): %s" % (len(dead), dead))
        r.log("CDS: %d with a level (%d tied), %d without; not believable: %s" % (cov["cds_priced"], cov["cds_priced_tied"], cov["cds_no_level"], [(c["name"], c["spread_bp"]) for c in p["cds"]["priced"] if not c["believable"]]))

        # 5 earlier versions of the bond desk's document
        try:
            vers = real.list_object_versions(Bucket=LIVE, Prefix=L.BONDS_KEY, MaxKeys=1000).get("Versions") or []
            vers = [v for v in vers if v["Key"] == L.BONDS_KEY]
            stamps = sorted(v["LastModified"].isoformat()[:16] for v in vers)
            r.log("bond desk document: %d versions kept in the bucket, %s .. %s; by day %s" % (len(vers), stamps[0] if stamps else None, stamps[-1] if stamps else None, sorted({s[:10] for s in stamps})[-40:]))
        except Exception as e:  # noqa: BLE001
            r.log("bond desk document versions: could not be listed (%s)" % type(e).__name__)

        # the cut for the offline tests: a small warehouse and what the engine publishes from exactly that
        keys = {k for k, t in key_ticker.items() if t in CUT_TICKERS} | set(CUT_ISSUERS)
        cut_bonds = [b for b in issues["bonds"] if N.issuer_key(b.get("issuer") or "")[0] in keys or (b.get("ccy") != "USD" and len(keys) and hash(b.get("k")) % 400 == 0)]
        cds_doc = parse(rec.seen[L.CDS_KEY])
        want_cds = set(CUT_TICKERS)
        cds_cut = {"as_of": cds_doc.get("as_of"), "generated_at": cds_doc.get("generated_at"), "source": cds_doc.get("source"), "version": cds_doc.get("version"), "groups": {}}
        for g in ("us_corp", "global_corp"):
            blk = (cds_doc.get("groups") or {}).get(g) or {}
            cds_cut["groups"][g] = {}
            for kind in ("rows", "unpriced", "dormant"):
                keep = []
                for i, x in enumerate(blk.get(kind) or []):
                    key, _ = N.issuer_key(x.get("name") or "")
                    co, _ = matcher.match(key, False, None, foreign=(g == "global_corp"))
                    if (co and co["symbol"] in want_cds) or key in keys or i % 9 == 0 or (x.get("spread_bp") or 0) > 5000:
                        keep.append({k: v for k, v in x.items() if k not in ("spark", "curve_last", "read", "sign_sources_20d", "candidates")})
                cds_cut["groups"][g][kind] = keep
        book_cut = [x for x in listed_rows if x["symbol"] in CUT_TICKERS or hash(x["symbol"]) % 23 == 0]
        hist = parse(rec.seen[L.HIST_KEY]) if L.HIST_KEY in rec.seen else {"obs": {}}
        hist_cut = {"obs": {t: {d: v for d, v in sorted(o.items())[-4:]} for t, o in (hist.get("obs") or {}).items()}}
        docs = {
            L.BONDS_KEY: {"generated_at": bonds_doc.get("generated_at"), "version": bonds_doc.get("version"),
                          "issues": {"as_of": issues["as_of"], "move_as_of": issues.get("move_as_of"), "ytd_as_of": issues.get("ytd_as_of"), "bonds": cut_bonds}},
            L.PAR_KEY: {"source": parse(rec.seen[L.PAR_KEY]).get("source"), "rows": {d: v for d, v in par_rows.items() if d >= "2025-12-15"}},
            L.CDS_KEY: cds_cut, L.MODEL_KEY: {"generated_at": parse(rec.seen[L.MODEL_KEY]).get("generated_at"), "single_name_cds": parse(rec.seen[L.MODEL_KEY]).get("single_name_cds")},
            L.HIST_KEY: hist_cut,
        }
        for key, body in rec.seen.items():
            if key.startswith(L.EOD_ROOT):
                sym = key[len(L.EOD_ROOT):-len(".json.gz")]
                if sym in CUT_TICKERS or sym.replace(".", "-") in CUT_TICKERS:
                    doc = parse(body)
                    docs[key] = {"rows": [[row[0]] + [round(v, 4) if isinstance(v, float) else v for v in row[1:6]] for row in (doc.get("rows") or [])[-(L.BARS + 45):]]}
            elif key.startswith(L.SPLITS_ROOT):
                doc = parse(body)
                rows_ = doc if isinstance(doc, list) else (doc.get("results") or [])
                docs[key] = [x for x in rows_ if isinstance(x, dict) and (str(x.get("ticker") or "").upper() in CUT_TICKERS or str(x.get("ticker") or "").upper().replace(".", "-") in CUT_TICKERS)]
        keep_rows, keep_guards = bank_enrich.rows, (L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED)
        try:
            bank_enrich.rows = lambda fields=None, where=None, limit=None: [dict(x) for x in book_cut]
            L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED = 100, 20, 10
            cut_built = L.build(MemS3(docs), now, log=lambda *_: None)
        finally:
            bank_enrich.rows = keep_rows
            L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED = keep_guards
        cp = cut_built["payload"]
        r.log("the cut: %d bonds, %d listed companies in its book, %d price files -> %d companies (%d listed), %d leads, %d CDS levels" % (
            len(cut_bonds), len(book_cut), sum(1 for k in docs if k.startswith(L.EOD_ROOT)), cp["coverage"]["companies"], cp["coverage"]["listed"], cp["n_leads"], cp["coverage"]["cds_priced"]))
        fixture = {"cut_at": now.isoformat(timespec="seconds"), "now": now.isoformat(), "docs": docs, "book": book_cut, "expected": cp,
                   "snapshots": cut_built["snapshots"], "guards": [100, 20, 10]}

        def embed(name, obj):
            blob = gzip.compress(json.dumps(obj, separators=(",", ":"), default=str).encode(), 9)
            b64 = base64.b64encode(blob).decode("ascii")
            r.lines.append("<!-- FIXTURE %s %d bytes" % (name, len(blob)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("FIXTURE-END -->")
            return len(blob)
        size_f = embed("cbe-warehouse", fixture)
        size_p = embed("cbe-payload", p)
        r.kv(companies=cov["companies"], listed=cov["listed"], leads=p["n_leads"], cds_levels=cov["cds_priced"], failed_checks=len(bad), fixture_kb=size_f // 1024, payload_kb=size_p // 1024)
        if bad:
            r.fail("DRY RUN: %d checks failed: %s" % (len(bad), bad))
        else:
            r.ok("DRY RUN COMPLETE: every check agrees; nothing was invoked or written")


if __name__ == "__main__":
    main()
