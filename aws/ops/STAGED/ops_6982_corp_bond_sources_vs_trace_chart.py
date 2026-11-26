"""ops 6982 - read-only: which sources the corporate bond desks use, set beside a public chart of single-bond moves.

Khalid 2026-11-26 13:36 CT, with a photo of a chart of 24 AI data-centre bonds ("Change in G-spread vs 11/24 close";
its sources: FINRA TRACE last sale (144A), US Treasury par curve, a bank's list of bonds):
"Do we also use these sources for corp bonds".

This op answers from the live documents:
  1. what the corporate bonds document says of its own sources today (fund holdings, the TRACE aggregates, the ICE
     sleeves from FRED, the ECB list) and whether each one arrived;
  2. the same for each kept earlier copy of the document, so a source that stopped arriving shows with its date;
  3. the Treasury par curve the credit board reads, and its newest day;
  4. which of the chart's 24 bonds the desks carry, with the price, yield and spread kept for each and the day those
     figures are for (no security identifier is read out or kept).
Nothing is written. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6982_corp_bond_sources_vs_trace_chart"
BUCKET = "justhodl-dashboard-live"

# the chart's rows: label as printed, coupon, maturity year, a pattern for the issuer's name where it is known
CHART = [
    ("CRWV 9 '31", 9.0, "2031", r"COREWEAVE"), ("CRWV 9.75 '31", 9.75, "2031", r"COREWEAVE"),
    ("CRWV 9.25 '30", 9.25, "2030", r"COREWEAVE"), ("CRWV 9.625 '32", 9.625, "2032", r"COREWEAVE"),
    ("EDGCOM 7.5 '31", 7.5, "2031", r"EDG"), ("CORZ 7.75 '31", 7.75, "2031", r"CORE SCI"),
    ("DDRIVE 8.875 '31", 8.875, "2031", r"DRIVE"), ("ELKGVP 7.5 '31", 7.5, "2031", r"ELK"),
    ("APLD 9.25 '30", 9.25, "2030", r"APLD|APPLIED DIG"), ("GALAXY 9.875 '31", 9.875, "2031", r"GALAXY"),
    ("ELNFOR 7 '31", 7.0, "2031", r"APLD COMPUTECO 3"), ("SECMOS 8.875 '31", 8.875, "2031", r"SEC|MOS"),
    ("ZENARC 8.875 '31", 8.875, "2031", r"ZENITH"), ("TRACTC 5.875 '31", 5.875, "2031", r"SV RNO PROPERTY"),
    ("PFORGE 6.75 '31", 6.75, "2031", r"APLD COMPUTECO 2"), ("TRACTD 6.5 '31", 6.5, "2031", r"PR RNO PROPERTY"),
    ("YNDRDC 6.875 '31", 6.875, "2031", r"YONDR|YNDR"), ("FLASHC 7.25 '30", 7.25, "2030", r"FLASH"),
    ("MERIDI 6.25 '31", 6.25, "2031", r"MERIDIAN"), ("CIFR 7.125 '30", 7.125, "2030", r"CIPHER|CIFR"),
    ("WULF 7.75 '30", 7.75, "2030", r"WULF"), ("CLSPRK 7.875 '31", 7.875, "2031", r"SPARK|CLSP"),
    ("STNGRY 6 '31", 6.0, "2031", r"STINGRAY|STNGR|STONE"), ("BLKPRL 6.125 '31", 6.125, "2031", r"PEARL|BLKP"),
]
HINT = re.compile(r"COMPUTE|DATA CENT|DATACENT|\bDC\b|PROPERTY OWNER|COREWEAVE|CORE SCI|CIPHER|WULF|GALAXY|APLD|APPLIED DIG|ZENITH ARC|MERIDIAN ARC|YONDR|TRACT|FLASH|HUT 8|BEACON POINT|CLEANSPARK|IREN|CRUSOE|NSCALE|FLUIDSTACK|BLACK PEARL|STINGRAY", re.I)


def tidy(name):
    return re.sub(r"\s+", " ", re.sub(r"\s+144A$|\s+REGS$", "", str(name or ""))).strip()


def main():
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    def get(key, version=None):
        kw = {"Bucket": BUCKET, "Key": key}
        if version:
            kw["VersionId"] = version
        raw = s3.get_object(**kw)["Body"].read()
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    def sources_line(doc):
        src = doc.get("sources") or {}
        fred = src.get("fred") or {}
        live = sum(1 for v in fred.values() if str(v).startswith("live"))
        why = sorted(set(str(v).split(" ")[0] for v in fred.values() if not str(v).startswith("live")))
        trace = src.get("trace") or {}
        tl = sum(1 for v in trace.values() if str(v).startswith("live")) if isinstance(trace, dict) else 0
        funds = (doc.get("issues") or {}).get("funds") or []
        ok = sum(1 for f in funds if f.get("n"))
        return {"written": doc.get("generated_at"), "sleeves": len(doc.get("sleeves") or []), "fred_live": live, "fred_asked": len(fred), "fred_failed_with": why,
                "trace_live": tl, "trace_asked": len(trace) if isinstance(trace, dict) else 0, "funds_with_bonds": ok, "funds": len(funds),
                "holdings_day": (doc.get("issues") or {}).get("as_of"), "bonds": (doc.get("issues") or {}).get("n_bonds"), "ecb": str(src.get("ecb"))[:90], "cds": str(src.get("cds"))[:60]}

    with report(NAME) as r:
        r.heading("6982 - the corporate bond desks' sources, beside a public chart of single-bond moves (read-only), %s" % now.isoformat(timespec="minutes"))

        r.section("1. The corporate bonds document today")
        doc = get("data/corp-bonds.json")
        line = sources_line(doc)
        r.log(json.dumps(line))
        cov = doc.get("coverage") or {}
        r.log("its own words on the US tape: " + str(cov.get("us"))[:400])
        trace = (doc.get("trace") or {})
        for name in ("corporate", "rule_144a"):
            rows = ((trace.get(name) or {}).get("breadth") or {})
            r.log("TRACE %s breadth as of %s: %s" % (name, rows.get("as_of"), json.dumps([[x.get("category"), x.get("advances"), x.get("declines"), x.get("trades")] for x in (rows.get("rows") or [])][:5])))
        r.log("TRACE dataset status: " + json.dumps((trace.get("status"))))
        r.log("funds: " + json.dumps([[f.get("id"), f.get("as_of"), f.get("n"), f.get("status")] for f in (doc.get("issues") or {}).get("funds") or []]))

        r.section("2. Earlier copies of the document (which source arrived on which day)")
        try:
            vs = s3.list_object_versions(Bucket=BUCKET, Prefix="data/corp-bonds.json", MaxKeys=40).get("Versions", [])
            vs = [v for v in vs if v["Key"] == "data/corp-bonds.json"][:12]
            for v in vs:
                try:
                    old = get("data/corp-bonds.json", v["VersionId"])
                    s = sources_line(old)
                    r.log("%s | sleeves %d | FRED %d of %d live %s | TRACE %d of %d | funds %d of %d | holdings %s" % (
                        v["LastModified"].strftime("%m-%d %H:%M"), s["sleeves"], s["fred_live"], s["fred_asked"], s["fred_failed_with"] or "", s["trace_live"], s["trace_asked"], s["funds_with_bonds"], s["funds"], s["holdings_day"]))
                except Exception as exc:  # noqa: BLE001
                    r.log("%s | unreadable: %s" % (v["LastModified"].strftime("%m-%d %H:%M"), str(exc)[:80]))
        except Exception as exc:  # noqa: BLE001
            r.log("versions not listed: %s" % str(exc)[:120])

        r.section("3. The Treasury par curve the credit board reads")
        try:
            par = get("data/warm/treasury-par/curve.json.gz")
            rows = par.get("rows") if isinstance(par, dict) else par
            r.log("keys %s | %s days | newest %s" % (list(par.keys())[:8] if isinstance(par, dict) else "list", len(rows or []), json.dumps((rows or [None])[-1])[:300]))
            r.log("source as the file states it: %s" % json.dumps({k: par.get(k) for k in ("source", "url", "note", "generated_at", "as_of") if isinstance(par, dict) and k in par})[:400])
        except Exception as exc:  # noqa: BLE001
            r.log("par curve not read: %s" % str(exc)[:120])

        r.section("4. The chart's 24 bonds on our desks")
        bonds = (doc.get("issues") or {}).get("bonds") or []
        board = get("data/credit-before-equity-bonds.json")
        cols = board.get("cols") or []
        brow = [dict(zip(cols, x)) for x in board.get("rows") or []]
        r.log("bond desk: %d bonds, holdings of %s (price and yield moves are against the fund file before it) | credit board: %d bonds, as of %s" % (len(bonds), (doc.get("issues") or {}).get("as_of"), len(brow), board.get("as_of")))
        found = 0
        for label, coupon, year, pat in CHART:
            rx = re.compile(pat, re.I)
            hit = [b for b in bonds if b.get("coupon") is not None and abs(float(b["coupon"]) - coupon) < 0.0051 and str(b.get("maturity", ""))[:4] == year and rx.search(str(b.get("issuer", "")))]
            if not hit:
                near = sorted(set(tidy(b.get("issuer")) for b in bonds if b.get("coupon") is not None and abs(float(b["coupon"]) - coupon) < 0.0051 and str(b.get("maturity", ""))[:4] == year and HINT.search(str(b.get("issuer", "")))))
                r.log("NOT HELD  %-18s%s" % (label, ("  (same coupon and year among data-centre names: %s)" % near[:4]) if near else ""))
                continue
            found += 1
            for b in hit[:2]:
                sp = [x for x in brow if x.get("coupon") is not None and abs(float(x["coupon"]) - coupon) < 0.0051 and str(x.get("maturity", ""))[:4] == year and tidy(x.get("issuer")) == tidy(b.get("issuer"))]
                s0 = sp[0] if sp else {}
                r.log("HELD      %-18s %-34s matures %s | price %s (%s since the month-end file) | yield %s (%s) | spread %s bp, week %s, month %s | in %s fund files, book %s" % (
                    label, tidy(b.get("issuer"))[:34], b.get("maturity"), b.get("price"), b.get("dpx"), b.get("rate"), b.get("chg"), s0.get("spread_bp"), s0.get("d_w1"), s0.get("d_m1"), b.get("nf"), b.get("book")))
        r.log("carried: %d of the chart's %d bonds" % (found, len(CHART)))
        names = {}
        for b in bonds:
            if HINT.search(str(b.get("issuer", ""))) and b.get("book") in ("US HY", "US IG", "US + intl HY"):
                names.setdefault(tidy(b.get("issuer")), []).append("%s '%s" % (b.get("coupon"), str(b.get("maturity", ""))[2:4]))
        r.log("every data-centre / compute name in the dollar books (%d): %s" % (len(names), json.dumps({k: v for k, v in sorted(names.items())})[:3500]))
        r.kv(chart_bonds=len(CHART), carried=found, sleeves_today=line["sleeves"], fred_live=line["fred_live"], fred_asked=line["fred_asked"])
        r.ok("read")


if __name__ == "__main__":
    main()
