"""ops_5080 -- FORTRESS probe: census every feed the new engine will fuse.

Khalid's spec (2026-08-31): an engine that picks stocks AND ETFs that
barely dipped / stayed flat / rose during big market dumps (accumulation),
sit under the 250 EMA, carry a low valuation, whose industry ETF is seeing
major inflows, whose stock+industry show (major) growth and momentum, that
sit in a very tight Bollinger squeeze about to break out, and that carry a
major backlog / meaningful contracts vs market cap -- plus any other metric
that makes the downside safe and the upside convex. Built the way a fund's
internal multi-factor screen is built: signal library -> gates -> cross-
sectional scoring -> ledger for forward grading.

Doctrine: every column bind is preceded by a probe that dumps REAL column
names and REAL sample values. Nothing below writes anything. It reads:

  P0  bar store: data/warm/polygon-full/ manifest + grouped session
      coverage over the trailing ~340 calendar days + one file's row shape
  P1  finviz-universe (sector/industry/mcap map; distinct industries)
  P2  fundamental-census-matrix (cols + AAPL/BA/FSLR non-null values)
  P3  industry-boom league row shape
  P4  etf-flows/daily.json metric row (SMH) + composite keys
  P5  industry-rotation row (SMH)
  P6  backlog.json / backlog-mined.json rows
  P7  catalyst.json / deal-scanner.json entries
  P8  floor-audit.json ticker row
  P9  stock-buying.json row keys
  P10 resilience.json row
  P11 13f-positions / institutional-positions per-ticker shape
  P12 short-interest / insider-radar / estimate-revisions rows
  P13 etf-census(-matrix) keys + sample; universe.json; sp500.json
  P14 earnings-date feeds present under data/
  P15 donor env var NAMES (never values) on equity-research + confluence-meta
"""
import gzip
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
cfg = Config(read_timeout=300, retries={"max_attempts": 4, "mode": "adaptive"})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k, gz=False):
    try:
        body = s3.get_object(Bucket=B, Key=k)["Body"].read()
        if gz or k.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception as e:  # noqa: BLE001
        return {"__err__": "%s: %s" % (type(e).__name__, str(e)[:120])}


def head(k):
    try:
        h = s3.head_object(Bucket=B, Key=k)
        return h["ContentLength"], h["LastModified"].isoformat()[:19]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:60]


def trunc(o, n=900):
    s = json.dumps(o, default=str)
    return s if len(s) <= n else s[:n] + " ...(%d chars)" % len(s)


def nonnull(row, n=80):
    if not isinstance(row, dict):
        return row
    out = {k: v for k, v in row.items()
           if v not in (None, "", [], {}) and not isinstance(v, (list, dict))}
    return dict(list(out.items())[:n])


def find_row(doc, sym):
    """Find a row for `sym` anywhere reasonable in a doc."""
    if not isinstance(doc, dict):
        return None
    for k in ("tickers", "rows", "by_ticker", "stocks", "names", "league",
              "all_resilient", "about_to_boom", "metrics", "positions",
              "companies", "entries", "items", "data", "results"):
        v = doc.get(k)
        if isinstance(v, dict) and sym in v:
            return v[sym]
        if isinstance(v, list):
            for r in v:
                if isinstance(r, dict) and str(r.get("symbol") or r.get("ticker")
                                              or r.get("t") or "").upper() == sym:
                    return r
    if sym in doc:
        return doc[sym]
    return None


def first_row(doc):
    if not isinstance(doc, dict):
        return None
    for k in ("rows", "tickers", "league", "metrics", "all_resilient",
              "positions", "entries", "items", "deals", "catalysts",
              "stocks", "names", "results", "data"):
        v = doc.get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return k, len(v), v[0]
        if isinstance(v, dict) and v:
            kk = next(iter(v))
            return k, len(v), {kk: v[kk]}
    return None


def main():
    with report("5080-fortress-probe") as r:
        r.heading("ops 5080 -- FORTRESS feed census (read-only)")

        # ── P0 bar store ─────────────────────────────────────────────
        r.section("P0 polygon-full bar store")
        man = jget("data/warm/polygon-full/manifest.json")
        st = jget("data/warm/polygon-full/_state/state.json")
        r.log("manifest: " + trunc(man, 700))
        r.log("state: " + trunc({k: v for k, v in (st or {}).items()
                                 if k != "failures"}, 600))
        keys = []
        for yr in (str(NOW.year - 1), str(NOW.year)):
            pfx = "data/warm/polygon-full/grouped/%s/" % yr
            for pg in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=B, Prefix=pfx):
                for o in pg.get("Contents", []):
                    keys.append((o["Key"], o["Size"]))
        keys.sort()
        cutoff = (NOW - timedelta(days=340)).date().isoformat()
        recent = [(k, sz) for k, sz in keys
                  if k.rsplit("/", 1)[1][:10] >= cutoff]
        r.kv(grouped_files_2y=len(keys), sessions_last_340d=len(recent),
             first=recent[0][0][-18:-8] if recent else None,
             last=recent[-1][0][-18:-8] if recent else None,
             mean_gz_kb=round(sum(s for _, s in recent) / max(1, len(recent))
                              / 1024, 1))
        # gap scan: consecutive business days missing
        if recent:
            have = set(k.rsplit("/", 1)[1][:10] for k, _ in recent)
            d = datetime.fromisoformat(recent[0][0][-18:-8]).date()
            end = datetime.fromisoformat(recent[-1][0][-18:-8]).date()
            miss = []
            while d <= end:
                if d.weekday() < 5 and d.isoformat() not in have:
                    miss.append(d.isoformat())
                d += timedelta(days=1)
            r.kv(weekday_gaps=len(miss), gaps_sample=miss[:12])
            lk = recent[-1][0]
            raw = jget(lk)
            res = raw.get("results") or []
            r.kv(latest_key=lk, resultsCount=raw.get("resultsCount"),
                 n_results=len(res), top_keys=list(raw.keys())[:8])
            if res:
                r.log("row[0]: " + trunc(res[0]))
                syms = {x.get("T") for x in res}
                probe = ["SPY", "QQQ", "XLK", "SMH", "SOXX", "ITA", "XBI",
                         "AAPL", "BA", "FSLR", "NVDA", "BRK.A", "BRK.B",
                         "IWM", "TLT", "GLD"]
                r.kv(present={p: (p in syms) for p in probe})
                odd = [t for t in syms if t and ("." in t or "p" in t
                                                  or len(t) > 5)][:25]
                r.kv(odd_ticker_samples=odd, n_syms=len(syms))
                warr = [t for t in syms if t and t.endswith("W")
                        and len(t) == 5][:10]
                r.kv(w_suffix_samples=warr)
        for k in ("data/_ma200/closes.json",):
            sz, lm = head(k)
            d = jget(k)
            ser = (d or {}).get("series") or {}
            r.kv(key=k, size=sz, last_modified=lm, n_series=len(ser),
                 as_of=(d or {}).get("as_of"),
                 spy_len=len(ser.get("SPY") or []),
                 keys=list((d or {}).keys())[:8])

        # ── P1 finviz universe ───────────────────────────────────────
        r.section("P1 finviz-universe")
        fv = jget("data/finviz-universe.json")
        sz, lm = head("data/finviz-universe.json")
        r.kv(size=sz, last_modified=lm, keys=list(fv.keys())[:12])
        fr = first_row(fv)
        r.log("first row: " + trunc(fr))
        rows = None
        for k in ("rows", "tickers", "stocks", "universe", "data"):
            v = fv.get(k)
            if isinstance(v, list) and v:
                rows = v
                break
            if isinstance(v, dict) and v:
                rows = list(v.values()) if isinstance(
                    next(iter(v.values())), dict) else None
                if rows:
                    break
        if rows:
            inds = {}
            secs = {}
            for x in rows:
                if not isinstance(x, dict):
                    continue
                i = x.get("industry") or x.get("Industry")
                s_ = x.get("sector") or x.get("Sector")
                inds[i] = inds.get(i, 0) + 1
                secs[s_] = secs.get(s_, 0) + 1
            r.kv(n_rows=len(rows), n_sectors=len(secs), n_industries=len(inds))
            r.log("sectors: " + trunc(secs, 800))
            r.log("industries (ALL): " + json.dumps(
                sorted(inds.items(), key=lambda kv: -kv[1]), default=str))
            for sym in ("AAPL", "BA", "FSLR"):
                r.log("%s: %s" % (sym, trunc(find_row(fv, sym), 600)))

        # ── P2 census matrix ─────────────────────────────────────────
        r.section("P2 fundamental-census-matrix")
        cm = jget("data/fundamental-census-matrix.json")
        sz, lm = head("data/fundamental-census-matrix.json")
        r.kv(size=sz, last_modified=lm, keys=list(cm.keys())[:14],
             as_of=cm.get("as_of"))
        cols = cm.get("cols") or cm.get("metrics") or []
        r.kv(n_cols=len(cols))
        r.log("cols(ALL): " + json.dumps(cols, default=str))
        tk = cm.get("tickers")
        r.kv(tickers_type=type(tk).__name__,
             n_tickers=len(tk) if isinstance(tk, (list, dict)) else None)
        for sym in ("AAPL", "BA", "FSLR", "NVDA"):
            row = find_row(cm, sym)
            if isinstance(row, dict):
                inner = row.get("metrics") if isinstance(
                    row.get("metrics"), dict) else row
                r.log("%s top-level keys: %s" % (sym, list(row.keys())[:20]))
                r.log("%s non-null: %s" % (sym, json.dumps(
                    nonnull(inner, 220), default=str)))
            else:
                r.log("%s: not found (%s)" % (sym, type(row).__name__))

        # ── P3 industry boom ─────────────────────────────────────────
        r.section("P3 industry-boom")
        ib = jget("data/industry-boom.json")
        sz, lm = head("data/industry-boom.json")
        r.kv(size=sz, last_modified=lm, keys=list(ib.keys())[:16],
             as_of=ib.get("as_of"))
        lg = ib.get("league") or []
        r.kv(n_league=len(lg))
        if lg:
            r.log("league[0]: " + trunc(lg[0], 1600))
            semi = [x for x in lg if "semi" in str(x.get("industry", "")).lower()]
            if semi:
                r.log("semis row: " + trunc(semi[0], 1600))
        for k in ("industries", "by_industry", "members", "tickers"):
            if k in ib:
                v = ib[k]
                r.log("%s: type=%s len=%s sample=%s" % (
                    k, type(v).__name__, len(v) if hasattr(v, "__len__") else "",
                    trunc(v if not isinstance(v, dict) else
                          dict(list(v.items())[:2]), 500)))

        # ── P4 etf flows ─────────────────────────────────────────────
        r.section("P4 etf-flows")
        ef = jget("data/etf-flows/daily.json")
        sz, lm = head("data/etf-flows/daily.json")
        mets = ef.get("metrics") or []
        r.kv(size=sz, last_modified=lm, keys=list(ef.keys())[:14],
             as_of=ef.get("as_of") or ef.get("generated_at"),
             n_metrics=len(mets),
             n_err=sum(1 for m in mets if m.get("error")))
        for t in ("SMH", "XLK", "ITA"):
            row = next((m for m in mets if m.get("ticker") == t), None)
            r.log("%s: %s" % (t, trunc(row, 900)))
        comp = jget("data/etf-flows/composite.json")
        r.kv(composite_keys=list(comp.keys())[:20])
        ptc = jget("data/etf-flows/per-ticker-context.json")
        r.kv(per_ticker_context_keys=list(ptc.keys())[:10],
             ctx_sample=trunc((ptc.get("context") or {}), 500))

        # ── P5 industry rotation ─────────────────────────────────────
        r.section("P5 industry-rotation")
        ir = jget("data/industry-rotation.json")
        sz, lm = head("data/industry-rotation.json")
        r.kv(size=sz, last_modified=lm, keys=list(ir.keys())[:20],
             as_of=ir.get("as_of"))
        row = find_row(ir, "SMH")
        r.log("SMH row: " + trunc(row, 1800))
        fr = first_row(ir)
        r.log("first: " + trunc(fr, 600))

        # ── P6 backlog ───────────────────────────────────────────────
        r.section("P6 backlog")
        for k in ("data/backlog.json", "data/backlog-mined.json"):
            d = jget(k)
            sz, lm = head(k)
            r.kv(key=k, size=sz, last_modified=lm, keys=list(d.keys())[:16],
                 as_of=d.get("as_of"))
            fr = first_row(d)
            r.log("first: " + trunc(fr, 1200))
            for sym in ("BA", "FSLR", "CAT"):
                row = find_row(d, sym)
                if row is not None:
                    r.log("%s: %s" % (sym, trunc(row, 1200)))

        # ── P7 catalyst / deals ──────────────────────────────────────
        r.section("P7 catalyst + deal-scanner")
        ct = jget("data/catalyst.json")
        sz, lm = head("data/catalyst.json")
        r.kv(size=sz, last_modified=lm, keys=list(ct.keys())[:16],
             as_of=ct.get("as_of"))
        fr = first_row(ct)
        r.log("first: " + trunc(fr, 1400))
        # find a contract-class entry
        pools = []
        for k, v in ct.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                pools.append((k, v))
            if isinstance(v, dict) and v and isinstance(
                    next(iter(v.values())), dict):
                pools.append((k, list(v.values())))
        found = None
        for k, v in pools:
            for e in v:
                s_ = json.dumps(e, default=str).lower()
                if "contract" in s_ or "award" in s_:
                    found = (k, e)
                    break
            if found:
                break
        r.log("contract-class sample: " + trunc(found, 1400))
        ds = jget("data/deal-scanner.json")
        sz, lm = head("data/deal-scanner.json")
        r.kv(deal_scanner_size=sz, last_modified=lm, keys=list(ds.keys())[:14],
             n_deals=len(ds.get("deals") or []))
        if ds.get("deals"):
            r.log("deal[0]: " + trunc(ds["deals"][0], 900))
            classes = {}
            for d_ in ds["deals"]:
                c = d_.get("event_class") or d_.get("class")
                classes[c] = classes.get(c, 0) + 1
            r.log("deal classes: " + trunc(classes, 600))

        # ── P8 floor audit ───────────────────────────────────────────
        r.section("P8 floor-audit")
        fa = jget("data/floor-audit.json")
        sz, lm = head("data/floor-audit.json")
        r.kv(size=sz, last_modified=lm, keys=list(fa.keys())[:20],
             as_of=fa.get("as_of"), universe_n=fa.get("universe_n"))
        tks = fa.get("tickers") or {}
        r.kv(n_tickers=len(tks))
        for sym in ("BA", "BTBT", "FSLR"):
            row = tks.get(sym) if isinstance(tks, dict) else find_row(fa, sym)
            r.log("%s: %s" % (sym, trunc(row, 1400)))
        if isinstance(tks, dict) and tks:
            k0 = next(iter(tks))
            r.log("%s keys: %s" % (k0, list(tks[k0].keys())))

        # ── P9 stock buying ──────────────────────────────────────────
        r.section("P9 stock-buying")
        sb = jget("data/stock-buying.json")
        sz, lm = head("data/stock-buying.json")
        r.kv(size=sz, last_modified=lm, keys=list(sb.keys())[:20],
             as_of=sb.get("as_of"))
        fr = first_row(sb)
        if fr:
            r.log("list=%s n=%d row keys: %s" % (fr[0], fr[1],
                                                  list(fr[2].keys())))
            r.log("row[0]: " + trunc(fr[2], 1500))

        # ── P10 resilience ───────────────────────────────────────────
        r.section("P10 resilience")
        rs = jget("data/resilience.json")
        sz, lm = head("data/resilience.json")
        r.kv(size=sz, last_modified=lm, keys=list(rs.keys())[:20],
             as_of=rs.get("as_of"), counts=rs.get("counts"))
        ar = rs.get("all_resilient") or []
        r.kv(n_all_resilient=len(ar))
        if ar:
            r.log("row[0]: " + trunc(ar[0], 1500))

        # ── P11 13F ──────────────────────────────────────────────────
        r.section("P11 13F")
        for k in ("data/13f-positions.json", "data/institutional-positions.json",
                  "data/13f-flows-by-ticker.json"):
            d = jget(k)
            sz, lm = head(k)
            r.kv(key=k, size=sz, last_modified=lm, keys=list(d.keys())[:18],
                 as_of=d.get("as_of"))
            row = find_row(d, "AAPL")
            r.log("AAPL: " + trunc(row, 1000))
            if row is None:
                fr = first_row(d)
                r.log("first: " + trunc(fr, 700))

        # ── P12 short / insider / revisions ──────────────────────────
        r.section("P12 short-interest / insider-radar / estimate-revisions")
        for k in ("data/short-interest.json", "data/insider-radar.json",
                  "data/estimate-revisions.json", "data/short-book.json"):
            d = jget(k)
            sz, lm = head(k)
            r.kv(key=k, size=sz, last_modified=lm, keys=list(d.keys())[:18],
                 as_of=d.get("as_of"))
            row = find_row(d, "AAPL") or find_row(d, "NVDA")
            r.log("sample: " + trunc(row if row is not None else first_row(d),
                                     1000))

        # ── P13 ETF census / universe / sp500 ────────────────────────
        r.section("P13 etf-census / universe / sp500")
        for k in ("data/etf-census-matrix.json", "data/etf-census.json",
                  "data/universe.json", "data/sp500.json"):
            d = jget(k)
            sz, lm = head(k)
            r.kv(key=k, size=sz, last_modified=lm, keys=list(d.keys())[:18],
                 as_of=d.get("as_of"))
            row = find_row(d, "SMH") or find_row(d, "AAPL")
            r.log("sample: " + trunc(row if row is not None else first_row(d),
                                     900))

        # ── P14 earnings feeds ───────────────────────────────────────
        r.section("P14 earnings / catalyst-calendar feeds under data/")
        cands = []
        for pg in s3.get_paginator("list_objects_v2").paginate(
                Bucket=B, Prefix="data/", Delimiter="/"):
            for o in pg.get("Contents", []):
                kk = o["Key"].lower()
                if any(w in kk for w in ("earn", "calendar", "catalyst")):
                    cands.append((o["Key"], o["Size"],
                                  o["LastModified"].isoformat()[:19]))
        r.log("candidates: " + json.dumps(cands, default=str))
        for k, _, _ in cands[:6]:
            d = jget(k)
            r.log("%s keys=%s first=%s" % (k, list(d.keys())[:12],
                                            trunc(first_row(d), 500)))

        # ── P15 donor env names ──────────────────────────────────────
        r.section("P15 donor env var NAMES")
        for fn in ("justhodl-equity-research", "justhodl-confluence-meta",
                   "justhodl-etf-fund-flows", "justhodl-stock-buying"):
            try:
                env = lam.get_function_configuration(FunctionName=fn).get(
                    "Environment", {}).get("Variables", {})
                r.kv(fn=fn, env_names=sorted(env.keys()))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fn, str(e)[:100]))
        # scheduler role + a schedule sample for the birth op
        sch = boto3.client("scheduler", region_name=REGION)
        try:
            names = [s["Name"] for s in sch.list_schedules(
                NamePrefix="justhodl-floor", MaxResults=5).get("Schedules", [])]
            r.kv(scheduler_sample=names)
        except Exception as e:  # noqa: BLE001
            r.warn("scheduler list: %s" % str(e)[:100])

        r.ok("probe complete -- read-only, nothing written")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print("FATAL: %s: %s" % (type(e).__name__, e))
        sys.exit(1)
