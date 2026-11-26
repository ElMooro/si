"""ops 6962 - what the warehouse holds on ETF flows (read-only audit, before the money map is built).

Khalid 2026-11-25 21:22 CT: "Enhance that engine and tracks what etfs and sectors is the money flowing out and flowing
into and covers as many asset classes and etfs as possible."

The money-flow engine sees 96 funds today. The Polygon bank keeps the provider's fund flows, profiles and holdings for
every US ETF. Before anything is built this op reads what is really there:

  1. the three datasets: how many funds, how large, how fresh, how the files are laid out;
  2. every fund's flow file: newest day, depth, assets (shares x NAV), and how long reading them all takes;
  3. the profiles: every field, and the values of the fields a class or a sector could be read from;
  4. how the provider's flow relates to shares and NAV, and what a share split looks like in it (ROM, ARKK, QID, BITO);
  5. a cut of real rows and profiles for the tests.

Nothing is invoked or written.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6962_etf_flow_warehouse_audit"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
BANK = "data/warm/polygon-bank/"
SETS = ("etf-global-fund-flows", "etf-global-profiles", "etf-global-constituents")
LOOK = ("SPY", "VOO", "IVV", "QQQ", "XLK", "VGT", "TLT", "GLD", "IBIT", "EWJ", "SOXL", "JEPI", "ROM", "ARKK", "QID", "BITO", "HYG", "BND", "VNQ", "SQQQ", "MUB", "USO", "UUP", "VXX", "BIL", "EEM", "SCHD", "XLE", "KWEB", "ARKB")
CUT = LOOK + ("VTI", "VUG", "VTV", "IWM", "DIA", "EFA", "VEA", "VWO", "AGG", "LQD", "TIP", "SGOV", "IEF", "SLV", "DBC", "UNG", "XLF", "XLV", "XLY", "XLP", "XLI", "XLU", "XLB", "XLRE", "XLC",
              "SMH", "SOXX", "XBI", "KRE", "ITB", "XOP", "IYR", "IAU", "GDX", "ETHA", "FBTC", "MCHI", "INDA", "EWZ", "EWY", "EWT", "EWG", "EWU", "TQQQ", "UPRO", "SPXU", "SH", "PSQ", "JNK", "EMB", "BKLN", "SRLN",
              "VCIT", "VCSH", "BSV", "SHY", "GOVT", "MBB", "PFF", "CWB", "DBA", "PDBC", "COPX", "URA", "LIT", "ICLN", "TAN", "ARKW", "ARKG", "BOTZ", "CIBR", "HACK", "IGV", "FDN", "XRT", "XHB", "JETS", "ITA", "PAVE",
              "MTUM", "QUAL", "USMV", "VLUE", "SPLV", "RSP", "MDY", "IJR", "IJH", "VB", "VO", "VXUS", "ACWI", "IEMG", "IEFA", "BNDX", "VT", "DGRO", "VIG", "VYM", "JEPQ", "SPYG", "SPYV", "IWF", "IWD", "QQQM", "SPLG")


def rows_of(doc):
    """The list of row objects inside a banked file, whatever wraps it."""
    if isinstance(doc, list):
        if doc and isinstance(doc[0], dict) and ("composite_ticker" in doc[0] or "processed_date" in doc[0]):
            return doc
        out = []
        for x in doc:
            out += rows_of(x)
        return out
    if isinstance(doc, dict):
        for k in ("results", "rows", "data", "items"):
            if isinstance(doc.get(k), list):
                got = rows_of(doc[k])
                if got:
                    return got
        out = []
        for v in doc.values():
            if isinstance(v, (list, dict)):
                out += rows_of(v)
        return out
    return []


def num(v):
    try:
        return float(v)
    except Exception:  # noqa: BLE001
        return None


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=90, max_pool_connections=48)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    fixtures = {}

    def get(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        raw = o["Body"].read()
        return (gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw), len(raw)

    def listing(prefix):
        out = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=prefix):
            out += page.get("Contents", [])
        return out

    with report(NAME) as r:
        r.heading("ops 6962 - the warehouse's ETF flows, profiles and holdings (read-only), %s" % now.isoformat(timespec="minutes"))

        # ------------------------------------------------------------------ 1. the three datasets
        r.section("1. The three datasets in the Polygon bank")
        top = set()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=BANK, Delimiter="/"):
            top |= {p["Prefix"] for p in page.get("CommonPrefixes", [])}
        r.log("bank folders with etf in the name: %s" % " ".join(sorted(p[len(BANK):] for p in top if "etf" in p.lower())))
        keys = {}
        for name in SETS:
            objs = listing(BANK + name + "/")
            keys[name] = objs
            ages = Counter()
            for o in objs:
                h = (now - o["LastModified"]).total_seconds() / 3600
                ages["<=26h" if h <= 26 else "<=50h" if h <= 50 else "<=4d" if h <= 96 else "<=8d" if h <= 192 else ">8d"] += 1
            r.log("%s: %d objects, %.1f MB | written %s | sample keys %s" % (name, len(objs), sum(o["Size"] for o in objs) / 1e6, dict(ages),
                                                                          " | ".join(o["Key"][len(BANK):] for o in objs[:3] + objs[-2:])))
        for cand in ("_state.json", "state.json", "manifest.json", "_manifest.json", "_state/state.json.gz", "_state/state.json"):
            try:
                raw, _ = get(BANK + cand)
                d = json.loads(raw)
                r.log("bank %s: keys %s" % (cand, " ".join(sorted(d.keys()))[:300]))
                for k in ("datasets", "progress", "laps", "state"):
                    v = d.get(k)
                    if isinstance(v, dict):
                        for name in SETS:
                            if name in v:
                                r.log("    %s.%s: %s" % (k, name, json.dumps(v[name])[:500]))
            except Exception:  # noqa: BLE001
                pass

        # ------------------------------------------------------------------ 2. every fund's flow file
        r.section("2. Every fund's flow file: read all of them once")
        flows = keys["etf-global-fund-flows"]

        def tick(key):
            base = key.split("/")[-1]
            return base.split(".")[0]
        first_raw, _ = get(flows[0]["Key"])
        r.log("a flow file as stored (%s): %s" % (flows[0]["Key"][len(BANK):], first_raw[:420].decode("utf-8", "replace")))
        t0 = time.time()

        def read(o):
            try:
                raw, n = get(o["Key"])
                rows = rows_of(json.loads(raw))
                return o["Key"], rows, n, o["LastModified"]
            except Exception as exc:  # noqa: BLE001
                return o["Key"], str(exc)[:80], 0, o["LastModified"]
        with ThreadPoolExecutor(max_workers=40) as pool:
            got = list(pool.map(read, flows))
        took = time.time() - t0
        bad = [g for g in got if isinstance(g[1], str)]
        r.log("read %d files in %.0f s with 40 threads (%.0f files a second, %.0f MB stored); unreadable %d %s" % (len(got), took, len(got) / max(took, 0.1),
                                                                                                         sum(g[2] for g in got) / 1e6, len(bad), [b[1] for b in bad[:2]]))
        book = {}
        fields = Counter()
        mism = 0
        for key, rows, _, lm in got:
            if isinstance(rows, str) or not rows:
                continue
            t = tick(key)
            names = {x.get("composite_ticker") for x in rows}
            if names != {t}:
                mism += 1
                continue
            for k in rows[0].keys():
                fields[k] += 1
            seen = {}
            for x in rows:
                d = str(x.get("effective_date") or "")[:10]
                p = str(x.get("processed_date") or "")[:10]
                if d and (d not in seen or p > seen[d][0]):
                    seen[d] = (p, x)
            days = sorted(seen)
            book[t] = {"days": days, "rows": {d: seen[d][1] for d in days}, "written": lm}
        r.log("funds with rows under their own name: %d | files whose rows name another fund: %d | empty: %d" % (len(book), mism, len(got) - len(bad) - len(book) - mism))
        r.log("row fields: %s" % dict(fields.most_common(20)))
        newest = Counter(b["days"][-1] for b in book.values())
        r.log("newest effective day per fund: %s" % dict(sorted(newest.items(), reverse=True)[:8]))
        top_day = max(newest)
        depth = sorted(len(b["days"]) for b in book.values())
        firsts = Counter(b["days"][0][:4] for b in book.values())
        r.log("days per fund: median %d, shortest %d, longest %d | first year of history: %s" % (depth[len(depth) // 2], depth[0], depth[-1], dict(sorted(firsts.items()))))
        ref = book.get("SPY", {}).get("days", [])
        r.log("SPY's last 8 days: %s | processed %s" % (" ".join(ref[-8:]), " ".join(str(book["SPY"]["rows"][d].get("processed_date"))[:10] for d in ref[-8:]) if ref else None))
        assets, cur = {}, []
        for t, b in book.items():
            x = b["rows"][b["days"][-1]]
            s_, n_ = num(x.get("shares_outstanding")), num(x.get("nav"))
            if s_ and n_:
                assets[t] = s_ * n_
            if b["days"][-1] >= ref[-3] if len(ref) >= 3 else False:
                cur.append(t)
        r.log("funds whose newest day is within SPY's last three days: %d, holding $%.2f trillion (all funds with a row: $%.2f trillion)" % (
            len(cur), sum(assets.get(t, 0) for t in cur) / 1e12, sum(assets.values()) / 1e12))
        r.log("largest: %s" % ", ".join("%s $%.0fB" % (t, assets[t] / 1e9) for t in sorted(assets, key=lambda t: -assets[t])[:25]))
        sizes = Counter()
        for t in cur:
            a = assets.get(t, 0)
            sizes[">=$10B" if a >= 1e10 else ">=$1B" if a >= 1e9 else ">=$100M" if a >= 1e8 else ">=$10M" if a >= 1e7 else "<$10M"] += 1
        r.log("current funds by size: %s" % dict(sizes))
        if len(ref) >= 6:
            five = ref[-5:]
            tot_in = tot_out = 0.0
            per = {}
            for t in cur:
                rows = book[t]["rows"]
                if all(d in rows for d in five):
                    v = sum(num(rows[d].get("fund_flow")) or 0 for d in five)
                    per[t] = v
                    tot_in += max(v, 0)
                    tot_out += min(v, 0)
            r.log("raw provider flow, five days %s..%s, %d funds with all five: in $%.1fB, out $%.1fB, net $%.1fB" % (five[0], five[-1], len(per), tot_in / 1e9, tot_out / 1e9, (tot_in + tot_out) / 1e9))
            order = sorted(per, key=lambda t: -per[t])
            r.log("    largest in: %s" % ", ".join("%s %+.2fB" % (t, per[t] / 1e9) for t in order[:20]))
            r.log("    largest out: %s" % ", ".join("%s %+.2fB" % (t, per[t] / 1e9) for t in order[::-1][:20]))
            odd = sorted(((abs(per[t]) / assets[t], t) for t in per if assets.get(t, 0) > 2e7 and abs(per[t]) > 0.25 * assets[t]), reverse=True)
            r.log("    funds whose five-day flow is over a quarter of their assets (%d): %s" % (len(odd), ", ".join("%s %.0f%% (%+.2fB)" % (t, 100 * q, per[t] / 1e9) for q, t in odd[:30])))

        # ------------------------------------------------------------------ 3. profiles
        r.section("3. The profiles: fields, and what a class or a sector can be read from")
        prof = {}

        def readp(o):
            try:
                raw, _ = get(o["Key"])
                rows = rows_of(json.loads(raw))
                rows = [x for x in rows if x.get("composite_ticker") == tick(o["Key"])]
                if not rows:
                    return tick(o["Key"]), None
                return tick(o["Key"]), max(rows, key=lambda x: str(x.get("processed_date") or ""))
            except Exception:  # noqa: BLE001
                return tick(o["Key"]), None
        with ThreadPoolExecutor(max_workers=40) as pool:
            for t, p in pool.map(readp, keys["etf-global-profiles"]):
                if p:
                    prof[t] = p
        r.log("profiles under their own name: %d of %d files | of the %d current flow funds, %d have a profile" % (len(prof), len(keys["etf-global-profiles"]), len(cur), sum(1 for t in cur if t in prof)))
        pf = Counter()
        for p in prof.values():
            for k, v in p.items():
                if v not in (None, "", [], {}):
                    pf[k] += 1
        r.log("profile fields (filled in N profiles): %s" % json.dumps(dict(pf.most_common(80))))
        for t in ("SPY", "XLK", "TLT", "GLD", "IBIT", "SOXL", "EWJ", "JEPI", "HYG", "VNQ"):
            if t in prof:
                r.log("%s: %s" % (t, json.dumps(prof[t])[:1500]))
        for k, n in pf.most_common(80):
            vals = Counter()
            for t in cur:
                v = (prof.get(t) or {}).get(k)
                if isinstance(v, (str, bool, int)) and not isinstance(v, float):
                    vals[str(v)] += 1
            if 2 <= len(vals) <= 60 and sum(vals.values()) > 200:
                r.log("  %s (%d values): %s" % (k, len(vals), json.dumps(dict(vals.most_common(60)))[:1500]))
            elif 60 < len(vals) <= 400 and sum(vals.values()) > 200:
                r.log("  %s (%d values, top 40): %s" % (k, len(vals), json.dumps(dict(vals.most_common(40)))[:1300]))

        # ------------------------------------------------------------------ 4. flow against shares and NAV; what a split looks like
        r.section("4. The provider's flow against shares and NAV, and what a split looks like")
        eq = Counter()
        for t in ("SPY", "VOO", "QQQ", "TLT", "GLD", "IBIT", "XLK", "HYG", "IWM", "EEM"):
            b = book.get(t)
            if not b:
                continue
            for a, c in zip(b["days"][-41:-1], b["days"][-40:]):
                ra, rc = b["rows"][a], b["rows"][c]
                f, s0, s1, n1, n0 = num(rc.get("fund_flow")), num(ra.get("shares_outstanding")), num(rc.get("shares_outstanding")), num(rc.get("nav")), num(ra.get("nav"))
                if None in (f, s0, s1, n1, n0):
                    continue
                for label, calc in (("x NAV of the day", (s1 - s0) * n1), ("x NAV of the day before", (s1 - s0) * n0)):
                    eq[label] += abs(calc - f) <= max(1.0, 1e-6 * abs(f))
                eq["rows"] += 1
        r.log("flow = (shares - shares the day before) ... on %d rows of ten large funds: %s" % (eq["rows"], {k: v for k, v in eq.items() if k != "rows"}))
        for t in ("ROM", "ARKK", "QID", "BITO", "SOXL", "TQQQ", "VOO"):
            b = book.get(t)
            if not b:
                r.log("%s: not in the bank" % t)
                continue
            r.log("%s, last 9 days (day: shares, NAV, flow as reported, flow / assets):" % t)
            for d in b["days"][-9:]:
                x = b["rows"][d]
                s_, n_, f = num(x.get("shares_outstanding")), num(x.get("nav")), num(x.get("fund_flow"))
                r.log("    %s  shares %s  nav %s  flow %s  %s" % (d, x.get("shares_outstanding"), x.get("nav"), x.get("fund_flow"),
                                                                 "" if not (s_ and n_ and f is not None) else "%+.1f%%" % (100 * f / (s_ * n_))))
        jumps = []
        for t, b in book.items():
            days = b["days"][-25:]
            for a, c in zip(days, days[1:]):
                s0, s1, n0, n1 = (num(b["rows"][a].get("shares_outstanding")), num(b["rows"][c].get("shares_outstanding")), num(b["rows"][a].get("nav")), num(b["rows"][c].get("nav")))
                if not (s0 and s1 and n0 and n1):
                    continue
                rs, rn = s1 / s0, n1 / n0
                if (rs > 1.4 or rs < 0.7) and abs(rs * rn - 1) < 0.08:
                    jumps.append((c, t, round(rs, 4), round(rn, 4), b["rows"][c].get("fund_flow"), round(s1 * n1 / 1e6)))
        r.log("days in the last 25 on which shares and NAV moved opposite ways by 40%% or more (a split): %d %s" % (len(jumps), json.dumps(sorted(jumps)[-40:])[:2200]))

        # ------------------------------------------------------------------ 5. holdings, and a cut for the tests
        r.section("5. Holdings files, and a cut of real rows for the tests")
        cons = keys["etf-global-constituents"]
        if cons:
            for want in ("XLK", "SPY"):
                o = next((x for x in cons if tick(x["Key"]) == want), None)
                if o:
                    rws = rows_of(json.loads(get(o["Key"])[0]))
                    r.log("%s holdings file: %d rows | fields %s | first %s" % (want, len(rws), " ".join(sorted(rws[0].keys()))[:400] if rws else None, json.dumps(rws[0])[:500] if rws else None))
        cut = {"as_of": top_day, "funds": {}}
        for t in dict.fromkeys(CUT):
            b = book.get(t)
            if not b:
                continue
            days = b["days"][-75:]
            cut["funds"][t] = {"rows": [{k: b["rows"][d].get(k) for k in ("effective_date", "processed_date", "composite_ticker", "fund_flow", "nav", "shares_outstanding")} for d in days],
                               "profile": prof.get(t)}
        fixtures["etf-flow-warehouse-cut"] = gzip.compress(json.dumps(cut, separators=(",", ":")).encode(), 9)
        r.log("cut for the tests: %d funds, 75 days each, %d bytes gzipped" % (len(cut["funds"]), len(fixtures["etf-flow-warehouse-cut"])))
        for k, body in fixtures.items():
            b64 = base64.b64encode(body).decode("ascii")
            r.lines.append("<!-- FIXTURE %s %d bytes" % (k, len(body)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("FIXTURE-END -->")
        r.kv(flow_files=len(flows), funds=len(book), current=len(cur), profiles=len(prof), newest=top_day, read_seconds=round(took), assets="$%.2fT" % (sum(assets.get(t, 0) for t in cur) / 1e12))
        r.ok("AUDIT COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
