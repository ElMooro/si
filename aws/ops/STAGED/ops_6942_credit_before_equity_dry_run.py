"""ops 6942 - DRY RUN of Credit Before Equity (the engine's source as committed) on the real warehouse. Nothing is invoked and nothing is written.

The engine's own build() runs on the runner against the live bucket with its writes left out, and what it would publish is
checked here by arithmetic that does not use the engine's:

  1  three companies' spreads worked again from the raw bonds and the Treasury curve
  2  an industry's change worked again from the company rows
  3  the counts the page will reconcile (names, leads, awaiting) and that every lead's stock has not followed
  4  every tie made by a rule (not an exact name, not the reviewed list) printed for review, and the largest untied issuers
  5  the listed companies the factor book does not hold but the price bank does
  6  (2.1) every company's stress rank worked again from the published table: where it stands in its own book on spread
     level, the long window's change and the month's change, and their average
  7  (2.1) the AI stack theme: its count against the rows, every listed member a ticker of the reviewed lists, its lines
  8  (2.1) no lead where a liquid CDS says the opposite of the bonds
  9  (3.0) three companies' bond facts worked again from the raw bonds: what comes due inside two years, the coupon, the price
  10 (3.0) the bond-by-bond document: every company's bonds are under it, and no identifier of a bond is in it
  11 (3.0) every listed company's chance of a fall read again from the evidence's own table; a non-dollar company has none
  12 (3.0) the evidence worked again from the panel by this script's own arithmetic: the share that fell in each spread band,
     the leads' record, and the walk-forward check rebuilt month by month the slow way
  13 (3.0) three companies read in another currency: their spreads worked again from the raw bonds and that government's yields

Since 3.0 the run starts with the study (the board rebuilt at every kept month-end, read-only), because the board's chances
come from it.

It also cuts a small warehouse (a few dozen companies: their bonds, CDS rows, price files, the curve) with what the engine
publishes from exactly that cut, for the offline tests, and says how many earlier versions of the bond desk's document the
bucket still holds (a real one-week comparison could be built from them).
"""
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timezone
from pathlib import Path
import base64
import gzip
import io
import json
import math
import sys

ROOT = Path(__file__).resolve().parents[3]
STAGING = ROOT / "aws" / "ops" / "staging" / "cbe3"                 # the 3.0 source before it is deployed (dry run first); the engine's own directory once it is
SRC = STAGING if (STAGING / "lambda_function.py").exists() else ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(SRC)]
LIVE = "justhodl-dashboard-live"
TEN = {"1M": 1 / 12, "2M": 2 / 12, "3M": 0.25, "4M": 4 / 12, "6M": 0.5, "1Y": 1.0, "2Y": 2.0, "3Y": 3.0, "5Y": 5.0, "7Y": 7.0, "10Y": 10.0, "20Y": 20.0, "30Y": 30.0}
CUT_TICKERS = ["AAPL", "JPM", "F", "ORCL", "CHTR", "WHR", "PZZA", "BRK-B", "ET", "T", "BA", "INTC", "AAL", "ECHO", "MLCO", "BHC", "PRGO", "ADT", "KBR", "STC", "DUK", "XOM", "GS", "HCA", "OXY",
               "TROX", "IHRT", "AMD", "LKQ", "PRI", "SGRY", "MATV", "CVS", "KSS", "CAR", "DCH", "XRX", "HSBC", "SMFG", "RIO", "BUD", "TEVA", "CNC", "PCG", "NEE",
               "META", "NVDA", "AVGO", "MSFT", "CRWV", "DELL", "CORZ", "EQIX", "VST",                       # 2.1: the AI stack theme needs its own names in the cut
               "SPY"]                                                                                       # 3.0: the market fund, for the calendar
CUT_MONTHS = 8                                                                                              # 3.0: month-ends of dated holdings kept in the cut, for the study's own test
CUT_ISSUERS = ["CLOUD SOFTWARE GROUP", "HUB INTERNATIONAL", "PETROLEOS MEXICANOS", "SAUDI ARABIAN OIL", "HILCORP ENERGY I", "LIFEPOINT HEALTH", "NISSAN MOTOR", "SOFTBANK GROUP", "ALTICE FRANCE",
               "PARAMOUNT SKYDANCE", "STAPLES", "MICHAELS", "STANDARD CHARTERED", "BNP PARIBAS", "MARS", "CHS COMMUNITY HEALTH SYSTEMS", "ASTON MARTIN CAPITAL HOLDINGS", "BURLINGTON NORTHERN SANTA",
               "PACIFICORP", "METROPOLITAN BANK", "PERSHING SQUARE HOLDINGS", "COX COMMUNICATIONS", "ELECTRONIC ARTS", "NORDSTROM", "CITGO PETROLEUM",
               "APLD COMPUTECO 2", "GALAXY HELIOS DATA CENTERS II", "HUT 8 DC", "QTS CENTRAL ISSUER", "FLASH COMPUTE", "PR RNO PROPERTY OWNER 1", "MERIDIAN ARC HOLDCO",
               "BEACON POINT DC", "ZENITH ARC", "SOFTBANK"]


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


class ReadOnly:
    """The live bucket for the study: read, never written, nothing remembered."""

    def __init__(self, s3):
        self.s3 = s3

    def get_object(self, Bucket, Key):
        return self.s3.get_object(Bucket=Bucket, Key=Key)

    def list_objects_v2(self, **kw):
        return self.s3.list_objects_v2(**kw)

    def put_object(self, **kw):
        raise RuntimeError("the dry run writes nothing")


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
        r.heading("ops 6942 - Credit Before Equity %s dry run on the real warehouse (nothing written), %s" % (C.VERSION, now.isoformat(timespec="minutes")))
        rec = Recorder(real)
        logs = []
        import cbe_study as S
        r.section("The study (read-only): the board at every kept month-end")
        studied = L.study(ReadOnly(real), now, log=r.log)
        ev = studied["evidence"]
        r.log("evidence: %d bytes; %s" % (len(json.dumps(ev, separators=(",", ":"))), json.dumps({k: ev[k] for k in ("first", "last", "month_ends", "company_months", "companies", "seconds", "skipped")})))
        r.log("direction: %s" % ev["direction"]["words"])
        for x in ev["readings"]:
            a, b = x["h1"], x["h3"]
            r.log("  %-46s n %6s | 1M mean %6s median %6s t %5s fell %5s | 3M mean %6s median %6s p10 %6s p90 %6s lag %5s beat %5s fell %5s t %5s t_fell %5s" % (
                x["words"][:46], a.get("n"), a.get("mean"), a.get("median"), a.get("t"), a.get("fell_big"), b.get("mean"), b.get("median"), b.get("p10"), b.get("p90"), b.get("lag_big"), b.get("beat_big"),
                b.get("fell_big"), b.get("t"), b.get("t_fell")))
        r.log("risk table, 3 months: %s" % json.dumps(ev["risk"]["h3"]))
        r.log("risk table, 1 month: %s" % json.dumps(ev["risk"]["h1"]))
        r.log("check, 3 months: %s" % json.dumps(ev["check"]["h3"]))
        r.log("check, 1 month: %s" % json.dumps(ev["check"]["h1"]))
        for x in (ev.get("cds") or {}).get("readings") or []:
            r.log("  cds %-34s %s" % (x["words"], {k: (v["n"], v["mean"], v["t"]) for k, v in x.items() if isinstance(v, dict)}))
        r.section("The daily build (read-only)")
        built = L.build(rec, now, log=logs.append, evidence=ev)
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

        # 6 the stress rank, worked again from the published table
        if "stress" in p:
            import cbe_themes as TH
            st = p["stress"]
            r.log("stress: %s" % json.dumps(st))
            wrong = 0
            for book in C.BOOK_WORDS:
                rs = [x for x in table if x["book"] == book]
                pools = {"rk_level": sorted(x["spread_bp"] for x in rs if x["spread_bp"] is not None),
                         "rk_year": sorted(x["pct_" + st["long"]] for x in rs if st["long"] and x["pct_" + st["long"]] is not None),
                         "rk_month": sorted(x["d_m1"] for x in rs if x["d_m1"] is not None)}
                src = {"rk_level": "spread_bp", "rk_year": "pct_" + (st["long"] or "ytd"), "rk_month": "d_m1"}
                for x in rs:
                    mine = {}
                    for rk, pool in pools.items():
                        v = x[src[rk]] if (rk != "rk_year" or st["long"]) else None
                        mine[rk] = None if v is None or len(pool) < 2 else round(100.0 * (bisect_left(pool, v) + bisect_right(pool, v) - 1) / 2.0 / (len(pool) - 1), 1)
                    moved = [mine[k] for k in ("rk_year", "rk_month") if mine[k] is not None]
                    stress = round((mine["rk_level"] + sum(moved)) / (1 + len(moved)), 1) if mine["rk_level"] is not None and moved else None
                    if [x["rk_level"], x["rk_year"], x["rk_month"], x["stress"]] != [mine["rk_level"], mine["rk_year"], mine["rk_month"], stress]:
                        wrong += 1
                r.log("check 6 book %s: %d companies, %d with a level, %d with the long window's change, %d with the month's" % (book, len(rs), len(pools["rk_level"]), len(pools["rk_year"]), len(pools["rk_month"])))
            hot = [x for x in table if x["stress"] is not None and x["stress"] >= st["stressed_at"]]
            ok = wrong == 0 and st["n_stressed"] == len(hot) and st["n_stressed_listed"] == sum(1 for x in hot if x["ticker"])
            r.log("check 6 every company's stress rank by own arithmetic: %d differ; %d at %s or more (%d listed) -> %s" % (wrong, len(hot), st["stressed_at"], sum(1 for x in hot if x["ticker"]), "agree" if ok else "DIFFER"))
            if not ok:
                bad.append("stress ranks")
            big = sorted([x for x in table if x["stress"] is not None and x["value"] >= 50e6], key=lambda x: (-x["stress"], -x["value"]))
            r.log("most stressed of the %d companies with $50M or more held:" % len(big))
            for i, x in enumerate(big[:30]):
                r.log("  %2d %-6s %-38s %s %-22s $%5.0fM spread %7s ytd %7s (%s%%) 1M %6s cds %s 3M %s | stress %5s (level %s, year %s, month %s) stock 1M %s%% %s" % (
                    i + 1, x["ticker"] or "", str(x["name"])[:38], x["book"], str(x["industry"])[:22], x["value"] / 1e6, x["spread_bp"], x["d_ytd"], x["pct_ytd"], x["d_m1"], x["cds_bp"], x["cds_d3m"],
                    x["stress"], x["rk_level"], x["rk_year"], x["rk_month"], x["px_m1"], x["theme"] or ""))

            # 7 the AI stack
            reviewed = {t for _, _, ts in TH._LAYERS for t in ts}
            for th in p["themes"]:
                rs = [x for x in table if x["theme"] == th["key"]]
                ok = th["n"] == len(rs) and sum(layer["n"] for layer in th["layers"]) == len(rs) and all((not x["ticker"]) or x["ticker"] in reviewed for x in rs) and \
                    all(x["ticker"] or x["layer"] in ("compute_vehicles", "ai_backers") for x in rs)
                r.log("check 7 theme %s: %d companies (%d listed), $%.1fB held, middle spread %s bp, stress %s, %d under stress holding %s%% of its value; %s -> %s" % (
                    th["name"], th["n"], th["n_listed"], th["value"] / 1e9, th["spread_bp"], th["stress"], th["n_stressed"], th["stressed_value_pct"],
                    {k: (v["d_bp"], v["d_pct"], v["wider"], v["tighter"]) for k, v in th["win"].items()}, "agree" if ok else "DIFFER"))
                if not ok:
                    bad.append("theme %s" % th["key"])
                for layer in th["layers"]:
                    r.log("    %-30s %2d companies $%6.0fM spread %7s bp stress %5s under stress %d | 1M %s ytd %s | %s" % (
                        layer["name"], layer["n"], layer["value"] / 1e6, layer["spread_bp"], layer["stress"], layer["n_stressed"], (layer["win"].get("m1") or {}).get("d_bp"),
                        (layer["win"].get("ytd") or {}).get("d_bp"), layer["largest_stressed"]))
                for x in sorted(rs, key=lambda x: -(x["stress"] if x["stress"] is not None else -1)):
                    r.log("      %-6s %-34s %-20s %s $%5.0fM spread %7s ytd %7s (%s%%) 1M %6s cds %s 3M %s rank %s | stress %s | stock 1M %s%% ytd %s%%" % (
                        x["ticker"] or "", str(x["name"])[:34], x["layer"], x["book"], x["value"] / 1e6, x["spread_bp"], x["d_ytd"], x["pct_ytd"], x["d_m1"], x["cds_bp"], x["cds_d3m"], x["cds_rank"],
                        x["stress"], x["px_m1"], x["px_ytd"]))
            maybe = [x for x in table if not x["theme"] and not x["ticker"] and any(w in " " + str(x["name"]).upper() + " " for w in (" DATA ", " DIGITAL ", " CLOUD ", " AI ", " GPU ", " HOSTING ", " COLOCATION "))]
            r.log("for review - issuers with no listed stock whose name suggests computing, not in the theme (%d): %s" % (len(maybe), "; ".join("%s ($%.0fM, %s bp)" % (x["name"], x["value"] / 1e6, x["spread_bp"]) for x in sorted(maybe, key=lambda x: -x["value"])[:40])))

            # 8 the two credit markets
            clash = [x for x in names if x.get("credit_conflict")]
            ok = all(x.get("cds_says") != "contradicts" for x in leads) and all(x["signal"] == "NONE" and x.get("cds_says") == "contradicts" for x in clash)
            r.log("check 8 no lead where a liquid CDS says the opposite of the bonds: %d names held back %s -> %s" % (
                len(clash), [(x["ticker"], x.get("d_spread_bp"), x.get("cds_d1m"), x.get("d_price_pct")) for x in clash], "agree" if ok else "DIFFER"))
            if not ok:
                bad.append("leads against the CDS")

        # 9 the bond facts, from the raw bonds
        for t in ("AAPL", "CHTR", "ORCL"):
            mine = [b for b in usd if key_ticker.get(N.issuer_key(b.get("issuer") or "")[0]) == t]
            dated = []
            for b in mine:
                try:
                    yrs = (date.fromisoformat(str(b.get("maturity"))[:10]) - date.fromisoformat(issues["as_of"])).days / 365.25
                except (TypeError, ValueError):
                    continue
                if yrs > 0 and float(b.get("demand") or 0) > 0:
                    dated.append((yrs, float(b["demand"])))
            tot = sum(v for _, v in dated)
            due = round(100.0 * sum(v for y, v in dated if y <= 2.0) / tot, 1) if tot else None
            cp_ = [(b["coupon"], float(b.get("demand") or 0)) for b in mine if isinstance(b.get("coupon"), (int, float))]
            coupon = round(sum(c * v for c, v in cp_) / sum(v for _, v in cp_), 2) if cp_ and sum(v for _, v in cp_) else None
            px_ = [(b["price"], float(b.get("demand") or 0)) for b in mine if isinstance(b.get("price"), (int, float)) and b["price"] > 0]
            price = round(sum(c * v for c, v in px_) / sum(v for _, v in px_), 1) if px_ and sum(v for _, v in px_) else None
            x = by_ticker.get(t) or {}
            ok = all(a is not None and b is not None and abs(a - b) <= tol for a, b, tol in ((due, x.get("due2y"), 0.11), (coupon, x.get("coupon"), 0.011), (price, x.get("px"), 0.11)))
            r.log("check 9 %-5s own arithmetic: due inside 2y %s%%, coupon %s, price %s; the engine %s%%, %s, %s -> %s" % (t, due, coupon, price, x.get("due2y"), x.get("coupon"), x.get("px"), "agree" if ok else "DIFFER"))
            if not ok:
                bad.append("bond facts of %s" % t)

        # 10 the bond-by-bond document
        bdoc = built["bonds"]
        bcols = bdoc["cols"]
        per = {}
        for row in bdoc["rows"]:
            per[row[0]] = per.get(row[0], 0) + 1
        import re
        ident = re.compile(r"^(?:[A-Z]{2}[A-Z0-9]{9}[0-9]|[0-9A-Z]{8}[0-9])$")
        leaks = sum(1 for row in bdoc["rows"] for v in row if isinstance(v, str) and ident.match(v))
        wrong = [x["id"] for x in table if per.get(x["id"], 0) != x["n_bonds"]]
        ok = not wrong and not leaks and "k" not in bcols and "isin" not in bcols and "cusip" not in bcols and len(per) == len(table)
        braw = json.dumps(bdoc, separators=(",", ":")).encode()
        r.log("check 10 the bond document: %d rows under %d companies (%d bytes, %d gzipped); companies whose bonds are not all there: %d %s; cells shaped like an identifier: %d -> %s" % (
            len(bdoc["rows"]), len(per), len(braw), len(gzip.compress(braw)), len(wrong), wrong[:5], leaks, "agree" if ok else "DIFFER"))
        if not ok:
            bad.append("the bond document")

        # 11 the chance of a fall, read again from the evidence's own table
        own_bands = ((0, 100), (100, 200), (200, 400), (400, 700), (700, 1000), (1000, None))
        cuts = (p.get("risk") or {}).get("vol_cuts")
        wrong, given, other = 0, 0, 0
        for x in table:
            said = x.get("risk_3m")
            if x["book"] not in ("ig", "hy", "em"):
                other += 1
                wrong += 1 if said is not None else 0
                continue
            if said is None:
                continue
            given += 1
            band = next(i for i, (lo, hi) in enumerate(own_bands) if x["spread_bp"] >= lo and (hi is None or x["spread_bp"] < hi))
            third = None if (x.get("vol") is None or not cuts) else 0 if x["vol"] < cuts[0] else 1 if x["vol"] < cuts[1] else 2
            cell = ev["risk"]["h3"].get(str(band)) or {}
            sub = cell["thirds"][third] if third is not None and cell.get("thirds") else None
            want = sub["fell"] if sub else cell.get("fell")
            lv = next((y for y in ev["readings"] if y["key"] == "level:%d" % band), {})
            if want != said or x.get("band") != band or x.get("lo_3m") != (lv.get("h3") or {}).get("p10") or x.get("hi_3m") != (lv.get("h3") or {}).get("p90"):
                wrong += 1
        ok = wrong == 0 and given == (p.get("risk") or {}).get("n") and given > 500
        r.log("check 11 the chance of a fall: %d listed companies have one, %d differ from the evidence's table; %d non-dollar companies, none with a chance; risk block %s -> %s" % (
            given, wrong, other, json.dumps({k: v for k, v in (p.get("risk") or {}).items() if k != "words"}), "agree" if ok else "DIFFER"))
        if not ok:
            bad.append("the chance of a fall")
        top = sorted([x for x in table if x.get("risk_3m") is not None and x["value"] >= 50e6], key=lambda x: (-x["risk_3m"], -x["value"]))
        for i, x in enumerate(top[:25]):
            r.log("  %2d %-6s %-36s %s spread %7s vol %5s -> 3M %5s%% 1M %5s%% | range %s .. %s | stress %s cell %s stock 1M %s%%" % (
                i + 1, x["ticker"], str(x["name"])[:36], x["book"], x["spread_bp"], x.get("vol"), x["risk_3m"], x.get("risk_1m"), x.get("lo_3m"), x.get("hi_3m"), x["stress"], x.get("cell"), x["px_m1"]))

        # 12 the evidence, by this script's own arithmetic
        panel = [x for x in studied["panel"]]
        by_day = {}
        for x in panel:
            by_day.setdefault(x["day"], []).append(x)

        def own_band(sp):
            return next((i for i, (lo, hi) in enumerate(own_bands) if sp is not None and sp >= lo and (hi is None or sp < hi)), None)
        diffs = []
        for i in range(len(own_bands)):
            mine = [x["dd3"] <= -20.0 for x in panel if own_band(x.get("s")) == i and x.get("dd3") is not None]
            theirs = (ev["risk"]["h3"].get(str(i)) or {})
            got = round(100.0 * sum(mine) / len(mine), 1) if mine else None
            diffs.append((i, got, theirs.get("fell"), len(mine), theirs.get("n")))
        ok_a = all(a == b and n == m for _, a, b, n, m in diffs)
        r.log("check 12a the share that fell 20%% inside three months, by spread band (own, the engine, company-months): %s -> %s" % (diffs, "agree" if ok_a else "DIFFER"))
        total = cnt = 0.0
        for day, rows_ in by_day.items():
            have = [x["r1"] for x in rows_ if x.get("r1") is not None]
            if len(have) < 50:
                continue
            avg = sum(have) / len(have)
            for x in rows_:
                if x.get("sig") == "CREDIT_LEADS_DOWN" and x.get("r1") is not None:
                    total += round(x["r1"] - avg, 2)
                    cnt += 1
        lead = next(y for y in ev["readings"] if y["key"] == "signal:CREDIT_LEADS_DOWN")["h1"]
        ok_b = cnt == lead["n"] and abs(total / cnt - lead["mean"]) < 0.006
        r.log("check 12b a lead down, the next month against the board's average stock: own %.3f over %d, the engine %s over %s -> %s" % (total / cnt, cnt, lead["mean"], lead["n"], "agree" if ok_b else "DIFFER"))
        days_ = sorted(by_day)
        said_ = {}
        for i, day in enumerate(days_):
            train = [x for d in days_[:max(0, i - 3)] for x in by_day[d] if x.get("dd3") is not None and own_band(x.get("s")) is not None]
            if len(days_[:max(0, i - 3)]) < 24:
                continue
            stat = {}
            for x in train:
                for key in {(own_band(x["s"]), None), (own_band(x["s"]), x.get("vt"))}:          # a set: a stock with no volatility is counted once, in its band
                    c_ = stat.setdefault(key, [0, 0])
                    c_[0] += 1
                    c_[1] += 1 if x["dd3"] <= -20.0 else 0
            for x in by_day[day]:
                b_ = own_band(x.get("s"))
                if x.get("dd3") is None or b_ is None or (b_, None) not in stat:
                    continue
                sub = stat.get((b_, x.get("vt"))) if x.get("vt") is not None else None
                use = sub if sub and sub[0] >= 150 else stat[(b_, None)]
                chance = round(100.0 * use[1] / use[0], 1)
                edge = next(j for j, (lo, hi) in enumerate(((0, 8), (8, 18), (18, 35), (35, 101))) if lo <= chance < hi)
                c_ = said_.setdefault(edge, [0, 0.0, 0])
                c_[0] += 1
                c_[1] += chance
                c_[2] += 1 if x["dd3"] <= -20.0 else 0
        mine = [(v[0], round(v[1] / v[0], 1), round(100.0 * v[2] / v[0], 1)) for _, v in sorted(said_.items())]
        theirs = [(b["n"], b["said"], b["happened"]) for b in ev["check"]["h3"]["buckets"]]
        ok_c = mine == theirs
        r.log("check 12c the walk-forward check rebuilt the slow way (stocks, said, happened): own %s, the engine %s -> %s" % (mine, theirs, "agree" if ok_c else "DIFFER"))
        if not (ok_a and ok_b and ok_c):
            bad.append("the evidence")

        # 13 companies read in another currency
        curves_seen = {}
        for ccy, files in L.CURVE_FILES.items():
            row_ = {}
            for tenor, slug in files.items():
                raw_ = rec.seen.get(L.YIELDS_ROOT + slug + ".json")
                obs = {o["date"]: o["value"] for o in (parse(raw_).get("observations") or []) if o.get("value") is not None} if raw_ else {}
                d_ = (p["curve"].get("other") or {}).get(ccy, {}).get("as_of")
                if d_ in obs:
                    row_[tenor] = obs[d_]
            if len(row_) == len(files):
                curves_seen[ccy] = row_
        r.log("government curves read: %s" % json.dumps({c: {"as_of": (p["curve"].get("other") or {}).get(c, {}).get("as_of"), "row": v} for c, v in curves_seen.items()}))
        foreign = sorted([x for x in table if x["book"] in ("xig", "xhy") and not x["ticker"] and x["spread_bp"] is not None and x["ccy"] in curves_seen], key=lambda x: -x["value"])
        n13 = 0
        for x in foreign[:3]:
            key = x["id"][2:]
            mine_b = [b for b in issues["bonds"] if not b.get("left") and b.get("ccy") == x["ccy"] and C.GRADE.get(b.get("book")) and N.issuer_key(b.get("issuer") or "")[0] == key]
            mine, n = own_spread(mine_b, curves_seen[x["ccy"]], issues["as_of"])
            ok = mine is not None and abs(mine - x["spread_bp"]) <= 0.6
            n13 += 1 if ok else 0
            r.log("check 13 %-34s %s own arithmetic %s bp over %d bonds, the engine %s bp -> %s" % (str(x["name"])[:34], x["ccy"], None if mine is None else round(mine, 1), n, x["spread_bp"], "agree" if ok else "DIFFER"))
        if foreign and n13 < min(3, len(foreign)):
            bad.append("non-dollar spreads")
        r.log("non-dollar: %d companies (%s); %d with a listed stock: %s" % (len([x for x in table if x["book"] in ("xig", "xhy")]), cov.get("by_book"), sum(1 for x in table if x["book"] in ("xig", "xhy") and x["ticker"]),
                                                                           sorted(x["ticker"] for x in table if x["book"] in ("xig", "xhy") and x["ticker"])[:60]))
        r.log("by currency: %s; windows: %s" % (cov.get("by_currency"), {k: (v.get("ref"), v.get("n_bonds"), v.get("n_worked_back")) for k, v in p["windows"].items()}))
        for x in sorted([x for x in table if x["book"] in ("xig", "xhy")], key=lambda x: -x["value"])[:15]:
            r.log("  %-6s %-36s %s %s $%5.0fM spread %7s 1M %6s ytd %6s stress %s (%d bonds, %d in the other currency)" % (
                x["ticker"] or "", str(x["name"])[:36], x["book"], x["ccy"], x["value"] / 1e6, x["spread_bp"], x["d_m1"], x["d_ytd"], x["stress"], x["n_bonds"], x["n_other"] or 0))
        r.log("calls it would keep: %d; holdings rows it would keep: %d; the track as it stands: %s" % (len(built["calls"]["rows"]), len(built["holdings"]["rows"]), json.dumps(
            L.track_summary(None, L.stored_days(real, L.CALLS_ROOT), built["tape"]))))

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
        for ccy, files in L.CURVE_FILES.items():                     # 3.0: the government curves, from the days the board's windows can reach
            for slug in files.values():
                raw_ = rec.seen.get(L.YIELDS_ROOT + slug + ".json")
                if raw_:
                    doc = parse(raw_)
                    docs[L.YIELDS_ROOT + slug + ".json"] = {"id": doc.get("id"), "title": doc.get("title"), "observations": [o for o in (doc.get("observations") or []) if str(o.get("date")) >= "2025-12-15"]}
        held = S.month_ends(L.stored_days(real, L.HOLD_ROOT), now.date().isoformat())[-CUT_MONTHS:]
        for day in held:                                             # 3.0: the dated holdings of the cut's own issuers, for the study's test
            doc = L.get_json(real, L.HOLD_ROOT + day + ".json.gz")
            at = doc["cols"].index("issuer")
            docs[L.HOLD_ROOT + day + ".json.gz"] = {"as_of": doc.get("as_of"), "cols": doc["cols"], "rows": [row for row in doc["rows"] if N.issuer_key(row[at] or "")[0] in keys]}
        for key, body in rec.seen.items():
            if key.startswith(L.EOD_ROOT):
                sym = key[len(L.EOD_ROOT):-len(".json.gz")]
                if sym in CUT_TICKERS or sym.replace(".", "-") in CUT_TICKERS:
                    doc = parse(body)
                    docs[key] = {"rows": [[row[0]] + [round(v, 4) if isinstance(v, float) else v for v in row[1:6]] for row in (doc.get("rows") or [])[-(L.BARS + 45):]]}
            elif key.startswith(L.LEDGER_ROOT):
                snap = parse(body)                                   # a ledger day the build read, kept for the cut's own bonds
                mine = {b.get("k") for b in cut_bonds}
                docs[key] = {**snap, "bonds": {k: v for k, v in (snap.get("bonds") or {}).items() if k in mine}}
            elif key.startswith(L.SPLITS_ROOT):
                doc = parse(body)
                rows_ = doc if isinstance(doc, list) else (doc.get("results") or [])
                docs[key] = [x for x in rows_ if isinstance(x, dict) and (str(x.get("ticker") or "").upper() in CUT_TICKERS or str(x.get("ticker") or "").upper().replace(".", "-") in CUT_TICKERS)]
        keep_rows, keep_guards = bank_enrich.rows, (L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED)
        try:
            bank_enrich.rows = lambda fields=None, where=None, limit=None: [dict(x) for x in book_cut]
            L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED = 100, 20, 10
            cut_panel = L.build_panel(MemS3(docs), now, log=lambda *_: None)
            cut_ev = S.evidence(S.from_table(S.to_table(cut_panel["panel"])))
            cut_ev.update({"version": C.VERSION, "generated_at": now.isoformat(timespec="seconds"), "stocks_to": cut_panel["meta"]["last_session"]})
            docs[L.EVIDENCE_KEY] = ev                                # the cut's board reads the REAL evidence, as the live board does
            cut_built = L.build(MemS3(docs), now, log=lambda *_: None, evidence=ev)
        finally:
            bank_enrich.rows = keep_rows
            L.MIN_BONDS, L.MIN_COMPANIES, L.MIN_LISTED = keep_guards
        cp = cut_built["payload"]
        r.log("the cut: %d bonds, %d listed companies in its book, %d price files -> %d companies (%d listed), %d leads, %d CDS levels" % (
            len(cut_bonds), len(book_cut), sum(1 for k in docs if k.startswith(L.EOD_ROOT)), cp["coverage"]["companies"], cp["coverage"]["listed"], cp["n_leads"], cp["coverage"]["cds_priced"]))
        fixture = {"cut_at": now.isoformat(timespec="seconds"), "now": now.isoformat(), "docs": docs, "book": book_cut, "expected": cp,
                   "snapshots": cut_built["snapshots"], "guards": [100, 20, 10],
                   "expected_bonds": cut_built["bonds"], "expected_calls": cut_built["calls"], "expected_holdings_rows": len(cut_built["holdings"]["rows"]),
                   "panel": {"month_ends": cut_panel["history"]["dates"], "table": S.to_table(cut_panel["panel"]), "evidence": cut_ev,
                             "history": {"dates": cut_panel["history"]["dates"], "books": cut_panel["history"]["books"], "n": len(cut_panel["history"]["companies"])}}}
        r.log("the cut's study: %d month-ends %s, %d panel rows; evidence over it: %d readings" % (len(held), held, len(cut_panel["panel"]), len(cut_ev["readings"])))
        # a sample of the real panel (one listed company in eight) and what the evidence arithmetic gives on exactly that sample
        import zlib
        sample = [x for x in studied["panel"] if zlib.crc32(x["t"].encode()) % 8 == 0]
        cds_sample = [x for x in (studied.get("cds_panel") or []) if zlib.crc32(x["t"].encode()) % 4 == 0]
        sample_tbl, cds_tbl = S.to_table(sample), S.to_table(cds_sample, S.CDS_PANEL_COLS)
        sample_ev = S.evidence(S.from_table(sample_tbl), S.from_table(cds_tbl))
        r.log("the panel sample: %d rows of %d companies; %d CDS name-weeks" % (len(sample), len({x["t"] for x in sample}), len(cds_sample)))

        def embed(name, obj):
            blob = gzip.compress(json.dumps(obj, separators=(",", ":"), default=str).encode(), 9)
            b64 = base64.b64encode(blob).decode("ascii")
            r.lines.append("<!-- FIXTURE %s %d bytes" % (name, len(blob)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("FIXTURE-END -->")
            return len(blob)
        size_f = embed("cbe-warehouse", fixture)
        size_p = embed("cbe-payload", p)
        size_s = embed("cbe-panel-sample", {"table": sample_tbl, "cds": cds_tbl, "evidence": sample_ev})
        size_b = embed("cbe-bonds", built["bonds"])
        hist_doc = studied["history"]
        on_board = {x["id"] for x in table}
        size_h = embed("cbe-history", {**hist_doc, "companies": {k: v for k, v in hist_doc["companies"].items() if k in on_board}})
        r.log("fixtures: warehouse %d KB, payload %d KB, panel sample %d KB, bonds %d KB, history %d KB (the history document itself: %d bytes, %d companies)" % (
            size_f // 1024, size_p // 1024, size_s // 1024, size_b // 1024, size_h // 1024, len(json.dumps(hist_doc, separators=(",", ":"))), len(hist_doc["companies"])))
        r.kv(companies=cov["companies"], listed=cov["listed"], leads=p["n_leads"], cds_levels=cov["cds_priced"], failed_checks=len(bad), fixture_kb=size_f // 1024, payload_kb=size_p // 1024)
        if bad:
            r.fail("DRY RUN: %d checks failed: %s" % (len(bad), bad))
        else:
            r.ok("DRY RUN COMPLETE: every check agrees; nothing was invoked or written")


if __name__ == "__main__":
    main()
