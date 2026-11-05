"""ops 6743 - READ-ONLY: the sources a "leaders" engine would stand on (Khalid 2026-11-04: "build a new engine called
leaders and track how leaders on each industry are doing compared to sp500 and whether they are in accumulation or
distribution. Is smart money buying or selling them. Buying inflow and outflows. The industry etfs inflows and outflows,
how are they compared to their 250, 200, 100, 50, 20 moving average. Which are seeing golden cross and death cross. Also
which are seeing double tops and double bottoms. Climax, tight price ...").

A. the factor book (data/warm/factor-book): manifest, the matrix index, which columns exist, the industry census of
   US-listed common stocks and the forecast of the leaders universe (top 5 by market cap per industry, share classes of
   one company folded by CIK).
B. the Polygon EOD bank: manifest, depth for a sample of leaders (ADRs and dot tickers included) and for every
   candidate industry ETF (sessions, last session, 20-session dollar volume).
C. the 13F per-ticker table and the 13F book's industry layer (the cohort's money by industry).
D. fund flows for the industry ETFs: the native ETF desk (116 funds), the canonical 300-fund packet, and the Polygon
   bank's per-fund ETF Global flow histories (every US ETF) - coverage, freshness, and whether a banked file really
   holds the fund it is named after.
E. the FMP walker: institutional-ownership position summaries per symbol and quarter, the insider (Form 4) feed.
F. the fleet's own Wyckoff reads that could corroborate: phase detector, bottom desk, accumulation radar (shapes only).
G. the corporate-actions index (recorded splits).
H. the function name and the schedule slot.
Nothing is written. No key involved.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=32)
TOP_N = 5
LEADER_SAMPLE = ["AAPL", "NVDA", "MSFT", "TSM", "ASML", "NVO", "BRK.B", "JPM", "XOM", "LLY", "TM", "BABA", "SAP", "AZN", "SHEL", "HSBC", "GOOGL", "GOOG", "UNH", "V"]
ETF_CANDIDATES = ("SMH SOXX XSD PSI IGV XSW WCLD SKYY CIBR HACK BUG KRE KBE IAT KBWB XBI IBB ARKG XOP OIH XES XLE AMLP MLPX CRAK FCG GDX GDXJ SIL SILJ COPX URA URNM NLR LIT "
                  "XME PICK SLX REMX ITA XAR PPA ARKX JETS ITB XHB PKB XRT IBUY ONLN IYT XTN IHI XHE IHF XHS KIE IAK KBWP IAI KCE PPH IHE XPH VNQ IYR XLRE REZ KBWY REM "
                  "FDN SOCL TAN ICLN KARS DRIV CARZ PAVE BOTZ ROBO QTUM PHO FIW MSOS XLK VGT FTEC XLB XLC XLF XLI XLP XLU XLV XLY MOO VEGI PBJ PEJ IYZ XTL BJK BETZ "
                  "ESPO HERO IPAY WOOD CUT IYK IYC IYF IYH IYJ IYM IYE IDU IYW VDE VFH VHT VIS VCR VDC VAW VPU VOX VNQI XNTK IGM IETC FINX BLOK IAK EVX SEA BOAT IDRV "
                  "PSCE KOL COAL IEO IEZ IGE GNR XAR SHLD CGW EATZ PSL XLG RSP").split()
FLOW_SAMPLE = ["SMH", "SOXX", "IGV", "KRE", "KBE", "XBI", "IBB", "XOP", "OIH", "ITA", "ITB", "XHB", "XRT", "IYT", "JETS", "TAN", "COPX", "URA", "XME", "CIBR", "PPA", "XSD", "XLK", "XLF"]
BANK_FLOW_SAMPLE = ["SMH", "IHI", "KIE", "XES", "IAI", "PPH", "AMLP", "VNQ", "FDN", "IHF", "XAR", "XTN", "SLX", "SIL", "MOO", "PBJ", "PEJ", "IYZ", "KCE", "IAK", "XPH", "IHE", "XSW", "XHE"]
INST_SAMPLE = ["AAPL", "NVDA", "JPM", "TSM", "XOM", "LLY", "CAT", "KRE"]


def get(s3, key):
    obj = s3.get_object(Bucket=LIVE, Key=key)
    raw = obj["Body"].read()
    n = len(raw)
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw), obj["LastModified"], n, obj.get("Metadata") or {}


def shape(v, depth=0):
    if isinstance(v, dict):
        if depth >= 2:
            return "{%d keys}" % len(v)
        return "{" + ", ".join("%s: %s" % (k, shape(x, depth + 1)) for k, x in list(v.items())[:40]) + ("…" if len(v) > 40 else "") + "}"
    if isinstance(v, list):
        return "[%d × %s]" % (len(v), shape(v[0], depth + 1) if v else "-")
    s = repr(v)
    return s if len(s) <= 60 else s[:57] + "…"


def count_keys(s3, prefix, max_pages=60, delimiter=None):
    n = pages = 0
    newest = oldest = None
    prefixes = 0
    kw = {"Bucket": LIVE, "Prefix": prefix}
    if delimiter:
        kw["Delimiter"] = delimiter
    token = None
    while pages < max_pages:
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        pages += 1
        for o in resp.get("Contents") or []:
            n += 1
            lm = o["LastModified"]
            newest = lm if newest is None or lm > newest else newest
            oldest = lm if oldest is None or lm < oldest else oldest
        prefixes += len(resp.get("CommonPrefixes") or [])
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return {"keys": n, "prefixes": prefixes, "pages": pages, "truncated": bool(token), "newest": newest.isoformat(timespec="minutes") if newest else None,
            "oldest": oldest.isoformat(timespec="minutes") if oldest else None}


def section_a(r):
    import bank_enrich as be
    man = be.manifest() or {}
    r.log("manifest: version %s updated %s symbols %s universe %s common %s as_of %s" % (man.get("version"), man.get("updated_at"), man.get("symbols"), man.get("universe"),
                                                                                      man.get("common_stocks"), json.dumps(man.get("as_of"), default=str)[:300]))
    idx = be.index()
    if not idx:
        r.fail("factor-book index absent")
        return None
    cols = idx.get("columns") or []
    r.log("index: n %s, %d columns, identity %s" % (idx.get("n"), len(cols), sorted((idx.get("identity") or {}).keys())))
    want = ["mcap", "price", "close", "avg_volume", "dollar_vol_20d", "ret_1d", "ret_5d", "ret_21d", "ret_63d", "ret_126d", "ret_252d", "from_52w_high_pct", "high_52w", "low_52w",
            "short_pct_float", "days_to_cover", "short_volume_ratio", "finra_si_change_pct", "etf_holder_count", "beta", "volatility_20d", "composite_momentum", "composite_quality",
            "composite_value", "composite_growth", "country", "cik", "ipo_date", "float_shares", "outstanding_shares", "pe", "revenue_growth", "eps_growth", "pt_upside_pct", "consensus_score"]
    r.log("wanted columns present: %s" % json.dumps({w: (w in cols) for w in want}))
    t0 = time.time()
    rows = be.rows(fields=want)
    r.log("projection read: %d rows, %d fields, %.1fs" % (len(rows), len(want), time.time() - t0))
    pos = {x["symbol"]: x for x in rows}
    for t in ("AAPL", "TSM", "ASML", "NVO", "BRK.B", "BRK-B", "BRK.A", "GOOGL", "GOOG", "TM", "BABA", "SHEL", "HSBC", "SAP", "NSRGY", "SPY", "FOXA", "FOX"):
        x = pos.get(t)
        r.log("  %-6s %s" % (t, "ABSENT" if not x else json.dumps({k: x.get(k) for k in ("name", "sector", "industry", "exchange", "country", "cik", "security_type", "is_common", "is_adr", "is_otc", "is_etf",
                                                                                         "mcap", "price", "close", "dollar_vol_20d", "ret_21d", "short_pct_float", "short_volume_ratio")}, default=str)[:520]))

    def common(x):
        return not (x.get("is_common") is False or x.get("is_etf") or x.get("is_otc") or x.get("is_fund") or x.get("is_preferred") or x.get("is_warrant_unit") or x.get("is_shell") or x.get("is_debt_or_structured"))
    com = [x for x in rows if common(x)]
    with_cap = [x for x in com if isinstance(x.get("mcap"), (int, float)) and x["mcap"] > 0]
    r.log("common stocks %d | with mcap %d | with industry %d | with dollar_vol_20d %d | ADRs %d | mcap >= $1B %d | >= $2B %d | >= $10B %d" % (
        len(com), len(with_cap), sum(1 for x in with_cap if x.get("industry")), sum(1 for x in with_cap if x.get("dollar_vol_20d")), sum(1 for x in with_cap if x.get("is_adr")),
        sum(1 for x in with_cap if x["mcap"] >= 1e9), sum(1 for x in with_cap if x["mcap"] >= 2e9), sum(1 for x in with_cap if x["mcap"] >= 1e10)))
    r.log("security_type among common with mcap: %s" % json.dumps(Counter(str(x.get("security_type")) for x in with_cap).most_common(12)))
    r.log("exchange among common with mcap: %s" % json.dumps(Counter(str(x.get("exchange")) for x in with_cap).most_common(12)))
    r.log("country among common >= $2B: %s" % json.dumps(Counter(str(x.get("country")) for x in with_cap if x["mcap"] >= 2e9).most_common(15)))
    sectors = Counter(str(x.get("sector")) for x in with_cap)
    r.log("sectors (%d): %s" % (len(sectors), json.dumps(sectors.most_common(20))))
    by_ind = defaultdict(list)
    for x in with_cap:
        if x.get("industry"):
            by_ind[x["industry"]].append(x)
    r.log("industries: %d" % len(by_ind))
    picked = 0
    dup_cik = 0
    for ind in sorted(by_ind, key=lambda k: -sum(x["mcap"] for x in by_ind[k])):
        members = sorted(by_ind[ind], key=lambda x: -x["mcap"])
        seen, top = set(), []
        for x in members:
            cik = str(x.get("cik") or "").lstrip("0") or None
            if cik and cik in seen:
                dup_cik += 1
                continue
            if cik:
                seen.add(cik)
            top.append(x)
            if len(top) >= TOP_N:
                break
        picked += len(top)
        tot = sum(x["mcap"] for x in members)
        sec = Counter(str(x.get("sector")) for x in members).most_common(1)[0][0]
        r.log("  %-46s | %-22s | n %4d | $%8.1fB | %s" % (ind[:46], sec[:22], len(members), tot / 1e9, " ".join(
            "%s%s:%.0f" % (x["symbol"], "*" if x.get("is_adr") else "", x["mcap"] / 1e9) for x in top)))
    r.log("leaders forecast (top %d per industry, CIK-folded): %d names; share-class duplicates skipped %d" % (TOP_N, picked, dup_cik))
    r.kv(industries=str(len(by_ind)), common_with_cap=str(len(with_cap)), leaders_forecast=str(picked))
    return {"pos": pos}


def section_b(r, s3):
    import polygon_bankfirst as pb
    man = pb._get(pb.MANIFEST) or {}
    small = {k: v for k, v in man.items() if not isinstance(v, (dict, list)) or len(json.dumps(v, default=str)) < 300}
    r.log("manifest (scalars): %s" % json.dumps(small, default=str)[:1200])

    def depth(t):
        t0 = time.time()
        try:
            rows = pb.bars(t, complete_only=False)
        except Exception as e:  # noqa: BLE001
            return "%-6s ERROR %s" % (t, type(e).__name__)
        if not rows:
            return "%-6s UNBANKED" % t
        dv = sum(float(x.get("c") or 0) * float(x.get("v") or 0) for x in rows[-20:]) / max(1, len(rows[-20:]))
        return "%-6s %4d sessions %s..%s close %s $vol20 %.1fM read %.2fs keys %s" % (t, len(rows), rows[0]["date"], rows[-1]["date"], rows[-1].get("c"), dv / 1e6, time.time() - t0, sorted(rows[-1].keys()))
    for t in LEADER_SAMPLE:
        r.log("  " + depth(t))
    r.log("candidate industry ETFs (%d):" % len(set(ETF_CANDIDATES)))
    line = []
    for t in sorted(set(ETF_CANDIDATES)):
        try:
            rows = pb.bars(t, complete_only=False)
        except Exception as e:  # noqa: BLE001
            line.append("%s:ERR(%s)" % (t, type(e).__name__))
            continue
        if not rows:
            line.append("%s:UNBANKED" % t)
            continue
        dv = sum(float(x.get("c") or 0) * float(x.get("v") or 0) for x in rows[-20:]) / max(1, len(rows[-20:]))
        line.append("%s:%d/%s/$%.1fM" % (t, len(rows), rows[-1]["date"][5:], dv / 1e6))
        if len(line) >= 8:
            r.log("  " + "  ".join(line))
            line = []
    if line:
        r.log("  " + "  ".join(line))


def section_c(r, s3):
    try:
        tbl, mod, nb, _ = get(s3, "data/13f-tickers.json")
        r.log("13f-tickers: written %s, %d bytes, keys %s" % (mod.isoformat(timespec="seconds"), nb, sorted(tbl.keys())))
        r.log("  contract %s period %s prior %s generated %s n_tickers %s scope %s columns %s" % (tbl.get("contract"), tbl.get("period"), tbl.get("prior_period"), tbl.get("generated_at"),
                                                                                              tbl.get("n_tickers"), tbl.get("scope"), tbl.get("columns")))
        by = tbl.get("by_ticker") or {}
        for t in ("NVDA", "AAPL", "TSM", "JPM", "XOM", "SMH", "BRK.B"):
            r.log("  %-6s %s" % (t, by.get(t)))
        other = {k: shape(v) for k, v in tbl.items() if k not in ("by_ticker",)}
        r.log("  other fields: %s" % json.dumps(other)[:900])
    except Exception as e:  # noqa: BLE001
        r.warn("13f-tickers: %s %s" % (type(e).__name__, str(e)[:160]))
    try:
        book, mod, nb, _ = get(s3, "data/13f-book.json")
        b = book.get("book") or {}
        r.log("13f-book: version %s written %s %d bytes | top keys %s | book keys %s" % (book.get("version"), mod.isoformat(timespec="seconds"), nb, sorted(book.keys())[:30], sorted(b.keys())[:40]))
        sec = b.get("sectors") or book.get("sectors") or {}
        r.log("  sectors block keys: %s" % (sorted(sec.keys()) if isinstance(sec, dict) else type(sec).__name__))
        for scope, blk in (sec.items() if isinstance(sec, dict) else []):
            if not isinstance(blk, dict):
                r.log("  scope %s: %s" % (scope, shape(blk)))
                continue
            r.log("  scope %s keys %s" % (scope, sorted(blk.keys())))
            inds = blk.get("industries") or []
            secs = blk.get("sectors") or []
            r.log("    sectors %d, industries %d; coverage %s" % (len(secs), len(inds), json.dumps(blk.get("coverage"), default=str)[:300]))
            if inds:
                r.log("    industry row keys: %s" % sorted(inds[0].keys()))
                for row in inds[:6]:
                    r.log("    %s" % json.dumps({k: row.get(k) for k in row if not isinstance(row.get(k), (list, dict))}, default=str)[:480])
                r.log("    industry names (%d): %s" % (len(inds), " | ".join(str(x.get("industry") or x.get("name")) for x in inds)[:3000]))
            if secs:
                r.log("    sector row: %s" % json.dumps({k: secs[0].get(k) for k in secs[0] if not isinstance(secs[0].get(k), (list, dict))}, default=str)[:400])
        r.log("  period %s prior %s cohort %s" % (b.get("period") or book.get("period"), b.get("prior_period"), shape(b.get("cohort"))[:300]))
    except Exception as e:  # noqa: BLE001
        r.warn("13f-book: %s %s" % (type(e).__name__, str(e)[:160]))


def flow_brief(fl):
    own = fl.get("latest_windows") or {}
    out = []
    for n in ("1", "5", "21"):
        w = own.get(n) or {}
        out.append("%s:%s %s %s%%" % (n, (w.get("status") or "absent")[:8], w.get("flow_usd_decimal"), w.get("flow_to_end_assets_pct_decimal")))
    return " | ".join(out)


def section_d(r, s3, now):
    desk_funds = canon_funds = {}
    try:
        desk, mod, nb, _ = get(s3, "data/etf-desk-research.json")
        desk_funds = desk.get("funds") or {}
        r.log("desk: written %s, generated %s, %d funds, %d bytes, reference %s" % (mod.isoformat(timespec="seconds"), desk.get("generated_at"), len(desk_funds), nb, json.dumps(desk.get("reference"), default=str)[:200]))
        f = desk_funds.get("SMH") or {}
        fl = f.get("flows") or {}
        r.log("  desk SMH fund keys %s | flows keys %s" % (sorted(f.keys()), sorted(fl.keys())))
        r.log("  desk SMH latest_observation %s" % json.dumps(fl.get("latest_observation"), default=str)[:400])
        hist = fl.get("history")
        r.log("  desk SMH history: %s" % (shape(hist)[:400]))
        for t in FLOW_SAMPLE:
            f = desk_funds.get(t)
            if f:
                fl = f.get("flows") or {}
                r.log("  desk %-5s eligible %s valid_until %s eff %s | %s" % (t, (f.get("quality") or {}).get("flow_current_eligible"), str(fl.get("source_valid_until"))[:16], fl.get("latest_effective_date"), flow_brief(fl)))
    except Exception as e:  # noqa: BLE001
        r.warn("desk: %s %s" % (type(e).__name__, str(e)[:160]))
    try:
        canon, mod, nb, _ = get(s3, "data/provider-fund-flow-research.json")
        canon_funds = canon.get("funds") or {}
        r.log("canonical: written %s, generated %s, %d funds, %d bytes, top keys %s" % (mod.isoformat(timespec="seconds"), canon.get("generated_at"), len(canon_funds), nb, sorted(canon.keys())))
        fl = canon_funds.get("IGV") or {}
        r.log("  canonical IGV keys %s" % sorted(fl.keys()))
        r.log("  canonical IGV latest_observation %s | quality %s | valid_until %s" % (json.dumps(fl.get("latest_observation"), default=str)[:300], json.dumps(fl.get("quality"), default=str)[:200], fl.get("source_valid_until")))
        hist = fl.get("history")
        r.log("  canonical IGV history: %s" % shape(hist)[:500])
        if isinstance(hist, list) and hist:
            r.log("  canonical IGV history first %s last %s" % (json.dumps(hist[0], default=str)[:260], json.dumps(hist[-1], default=str)[:260]))
        for t in FLOW_SAMPLE:
            fl = canon_funds.get(t)
            if fl:
                r.log("  canon %-5s status %s valid_until %s eff %s | %s" % (t, (fl.get("quality") or {}).get("status"), str(fl.get("source_valid_until"))[:16], fl.get("latest_effective_date"), flow_brief(fl)))
            else:
                r.log("  canon %-5s NOT IN THE 300" % t)
    except Exception as e:  # noqa: BLE001
        r.warn("canonical: %s %s" % (type(e).__name__, str(e)[:160]))
    cand = sorted(set(ETF_CANDIDATES))
    r.log("candidate ETFs with a desk flow: %s" % " ".join(t for t in cand if t in desk_funds))
    r.log("candidate ETFs with a canonical flow (not desk): %s" % " ".join(t for t in cand if t in canon_funds and t not in desk_funds))
    r.log("candidate ETFs with neither: %s" % " ".join(t for t in cand if t not in canon_funds and t not in desk_funds))
    # the Polygon bank's per-fund histories
    root = "data/warm/polygon-bank/"
    try:
        man, mod, nb, _ = get(s3, root + "manifest.json")
        per = (man.get("per_dataset") or {}).get("etf-global-fund-flows") or {}
        r.log("polygon-bank manifest: updated %s phase %s universe_counts %s" % (man.get("updated_at"), man.get("phase"), json.dumps(man.get("universe_counts"), default=str)[:300]))
        r.log("  etf-global-fund-flows: %s" % json.dumps(per, default=str)[:900])
        for ds in ("etf-global-profiles", "etf-global-constituents", "short-volume", "short-interest"):
            p = (man.get("per_dataset") or {}).get(ds) or {}
            r.log("  %s: objects %s lap %s rotation_days %s entities_per_day %s last_ok %s last_verdict %s" % (ds, p.get("objects"), p.get("lap"), p.get("rotation_days"), p.get("entities_per_day"), p.get("last_ok_at"), p.get("last_verdict")))
    except Exception as e:  # noqa: BLE001
        r.warn("polygon-bank manifest: %s %s" % (type(e).__name__, str(e)[:160]))
    try:
        c = count_keys(s3, root + "etf-global-fund-flows/", max_pages=20)
        r.log("  etf-global-fund-flows objects: %s" % json.dumps(c))
    except Exception as e:  # noqa: BLE001
        r.warn("  list: %s" % type(e).__name__)
    for t in BANK_FLOW_SAMPLE:
        key = root + "etf-global-fund-flows/" + t + ".json.gz"
        try:
            doc, mod, nb, meta = get(s3, key)
        except Exception as e:  # noqa: BLE001
            r.log("  bank %-5s ABSENT (%s)" % (t, type(e).__name__))
            continue
        rows = doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) else None)
        if not isinstance(rows, list) or not rows:
            r.log("  bank %-5s written %s %d bytes, not a row list: %s" % (t, mod.isoformat(timespec="minutes"), nb, shape(doc)[:200]))
            continue
        tick = Counter(str(x.get("composite_ticker") or x.get("ticker")) for x in rows if isinstance(x, dict))
        dates = sorted(str(x.get("effective_date") or x.get("processed_date") or "") for x in rows if isinstance(x, dict))
        last = max(rows, key=lambda x: str(x.get("effective_date") or ""))
        r.log("  bank %-5s written %s pulled %s %d bytes | %d rows | tickers %s | effective %s..%s | keys %s | newest %s" % (
            t, mod.isoformat(timespec="minutes"), meta.get("pulled_at"), nb, len(rows), json.dumps(tick.most_common(4)), dates[0], dates[-1], sorted(rows[0].keys()), json.dumps(last, default=str)[:300]))
        cf = canon_funds.get(t) or (desk_funds.get(t) or {}).get("flows")
        if cf and isinstance(cf.get("history"), list) and cf["history"]:
            h = cf["history"][-1]
            match = [x for x in rows if str(x.get("effective_date")) == str(h.get("date"))]
            r.log("      vs canonical newest %s: bank row(s) %s" % (json.dumps(h, default=str)[:200], json.dumps(match[:2], default=str)[:300]))


def section_e(r, s3):
    import fmp_bank as fb
    wm = fb.walker_manifest() or {}
    r.log("walker manifest keys %s; updated %s; phase %s" % (sorted(wm.keys())[:25], wm.get("updated_at"), wm.get("phase")))
    per = wm.get("per_dataset") or wm.get("datasets") or {}
    for ds in ("institutional-ownership-symbol-positions-summary", "insider-trading-latest", "insider-trading-statistics", "insider-trading-search", "industry-performance-snapshot",
               "sector-performance-snapshot", "sp500-constituent", "institutional-ownership-latest", "etf-info", "historical-industry-performance"):
        p = per.get(ds) if isinstance(per, dict) else None
        r.log("  %-52s %s" % (ds, json.dumps(p, default=str)[:420] if p else "not in manifest"))
    base = fb.WALKER_ROOT + "institutional-ownership-symbol-positions-summary/"
    for t in INST_SAMPLE:
        try:
            resp = s3.list_objects_v2(Bucket=LIVE, Prefix=base + t + "/", MaxKeys=40)
            keys = [(o["Key"].split("/")[-1], o["LastModified"].strftime("%m-%d"), o["Size"]) for o in resp.get("Contents") or []]
            r.log("  inst %-5s %d period files: %s" % (t, len(keys), " ".join("%s(%s,%dB)" % k for k in keys[-8:])))
            if keys:
                newest = sorted(k[0] for k in keys)[-1]
                doc, mod, nb, meta = get(s3, base + t + "/" + newest)
                row = doc[0] if isinstance(doc, list) and doc else doc
                r.log("      newest %s: %s" % (newest, json.dumps(row, default=str)[:1500]))
        except Exception as e:  # noqa: BLE001
            r.warn("  inst %s: %s %s" % (t, type(e).__name__, str(e)[:120]))
    try:
        c = count_keys(s3, base, max_pages=60, delimiter="/")
        r.log("  inst symbols banked (prefixes): %s" % json.dumps(c))
    except Exception as e:  # noqa: BLE001
        r.warn("  inst list: %s" % type(e).__name__)
    try:
        days = fb.walker_days("insider-trading-latest", days=14)
        r.log("insider-trading-latest: %d banked days in the last 14: %s" % (len(days), " ".join("%s:%d" % (d, len(rows)) for d, rows in days)))
        if days:
            d, rows = days[0]
            r.log("  row keys %s" % sorted(rows[0].keys()))
            r.log("  sample %s" % json.dumps(rows[0], default=str)[:700])
            r.log("  transactionType counts (newest day): %s" % json.dumps(Counter(str(x.get("transactionType")) for x in rows).most_common(12)))
            r.log("  acquisitionOrDisposition: %s | formType %s" % (json.dumps(Counter(str(x.get("acquisitionOrDisposition")) for x in rows).most_common(4)), json.dumps(Counter(str(x.get("formType")) for x in rows).most_common(6))))
        for yr in ("2026", "2025", "2024"):
            c = count_keys(s3, fb.WALKER_ROOT + "insider-trading-latest/" + yr + "/", max_pages=3)
            r.log("  insider days banked %s: %s" % (yr, json.dumps(c)))
    except Exception as e:  # noqa: BLE001
        r.warn("insider feed: %s %s" % (type(e).__name__, str(e)[:160]))
    for t in ("AAPL", "NVDA"):
        try:
            doc = fb.walker("insider-trading-statistics", entity=t)
            r.log("  insider-trading-statistics %s: %s" % (t, (json.dumps(doc[:2], default=str)[:700] if isinstance(doc, list) else shape(doc))))
        except Exception as e:  # noqa: BLE001
            r.warn("  insider stats %s: %s" % (t, type(e).__name__))
    for ds in ("industry-performance-snapshot", "sector-performance-snapshot"):
        try:
            days = fb.walker_days(ds, days=6)
            r.log("  %s: banked days %s; row %s" % (ds, [d for d, _ in days], json.dumps(days[0][1][0], default=str)[:300] if days else "-"))
        except Exception as e:  # noqa: BLE001
            r.warn("  %s: %s" % (ds, type(e).__name__))


def section_f(r, s3):
    for key, hints in (("data/phase-detector.json", ("phases_all", "tickers", "events", "phases")), ("data/bottom.json", ("board_all", "board", "by_desk")),
                       ("data/accumulation-radar.json", ("accumulating", "bottoms", "confirmed_bottoms", "distributing")), ("data/strength.json", ("rows",)),
                       ("data/distribution-composite.json", ()), ("data/accum-composite.json", ())):
        try:
            doc, mod, nb, _ = get(s3, key)
            r.log("%s: written %s, %d bytes, version %s, keys %s" % (key, mod.isoformat(timespec="minutes"), nb, doc.get("version") or doc.get("engine_version"), sorted(doc.keys())[:40]))
            for h in hints:
                v = doc.get(h)
                if v is None:
                    continue
                if isinstance(v, dict):
                    k0 = next(iter(v), None)
                    r.log("   %s: dict of %d; first %s -> %s" % (h, len(v), k0, json.dumps(v.get(k0), default=str)[:500]))
                elif isinstance(v, list):
                    r.log("   %s: list of %d; first %s" % (h, len(v), json.dumps(v[0], default=str)[:600] if v else "-"))
        except Exception as e:  # noqa: BLE001
            r.warn("%s: %s %s" % (key, type(e).__name__, str(e)[:120]))


def section_g(r, s3):
    try:
        idx, mod, nb, _ = get(s3, "data/corporate-actions-index.json")
        tk = idx.get("tickers") or {}
        r.log("corporate-actions-index: written %s generated %s covered %s universe %s | tickers %d | with splits %d | with dividends %d" % (
            mod.isoformat(timespec="minutes"), idx.get("generated_at"), idx.get("covered"), idx.get("universe"), len(tk),
            sum(1 for v in tk.values() if isinstance(v, dict) and v.get("n_splits")), sum(1 for v in tk.values() if isinstance(v, dict) and v.get("n_dividends"))))
        r.log("  sample %s" % json.dumps({t: tk.get(t) for t in ("AAPL", "NVDA", "ETHA", "APH", "TSM")}, default=str)[:500])
    except Exception as e:  # noqa: BLE001
        r.warn("corporate-actions-index: %s %s" % (type(e).__name__, str(e)[:120]))
    try:
        c = count_keys(s3, "data/warm/polygon-bank/splits/", max_pages=3)
        r.log("polygon-bank splits windows: %s" % json.dumps(c))
        resp = s3.list_objects_v2(Bucket=LIVE, Prefix="data/warm/polygon-bank/splits/", MaxKeys=1000)
        keys = sorted(o["Key"] for o in resp.get("Contents") or [])
        r.log("  newest keys: %s" % keys[-4:])
        if keys:
            doc, mod, nb, meta = get(s3, keys[-1])
            rows = doc if isinstance(doc, list) else (doc.get("results") or [])
            r.log("  %s: %d rows; sample %s" % (keys[-1], len(rows), json.dumps(rows[:2], default=str)[:400]))
    except Exception as e:  # noqa: BLE001
        r.warn("polygon-bank splits: %s %s" % (type(e).__name__, str(e)[:120]))


def section_h(r):
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    sch = boto3.client("scheduler", region_name=REGION, config=CFG)
    try:
        cf = lam.get_function_configuration(FunctionName="justhodl-leaders")
        r.warn("justhodl-leaders ALREADY EXISTS: modified %s, %s MB, description %s" % (cf.get("LastModified"), cf.get("MemorySize"), (cf.get("Description") or "")[:120]))
    except Exception as e:  # noqa: BLE001
        r.log("justhodl-leaders: %s (the name is free)" % type(e).__name__)
    for name in ("justhodl-leaders-daily", "justhodl-strength-daily", "justhodl-polygon-eod-daily", "justhodl-bank-factors-daily", "justhodl-13f-book-daily", "justhodl-polygon-bank-daily"):
        try:
            d = sch.get_schedule(Name=name, GroupName="default")
            r.log("  schedule %-30s %s %s tz %s" % (name, d.get("ScheduleExpression"), d.get("State"), d.get("ScheduleExpressionTimezone")))
        except Exception as e:  # noqa: BLE001
            r.log("  schedule %-30s %s" % (name, type(e).__name__))
    for key in ("data/leaders.json",):
        try:
            h = boto3.client("s3", region_name=REGION, config=CFG).head_object(Bucket=LIVE, Key=key)
            r.warn("%s ALREADY EXISTS: %s bytes, written %s" % (key, h["ContentLength"], h["LastModified"].isoformat(timespec="minutes")))
        except Exception as e:  # noqa: BLE001
            r.log("%s: %s (the key is free)" % (key, type(e).__name__))


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6743_leaders_sources_probe") as r:
        r.heading("ops 6743 - leaders engine sources (read-only) %s" % now.isoformat(timespec="minutes"))
        for title, fn in (("A. factor book: identity, industries, the leaders forecast", lambda: section_a(r)),
                          ("B. Polygon EOD bank: leaders and candidate industry ETFs", lambda: section_b(r, s3)),
                          ("C. 13F: the per-ticker table and the book's industry layer", lambda: section_c(r, s3)),
                          ("D. fund flows for industry ETFs: desk, canonical 300, Polygon bank per-fund history", lambda: section_d(r, s3, now)),
                          ("E. FMP walker: institutional ownership summaries + the insider feed", lambda: section_e(r, s3)),
                          ("F. fleet Wyckoff reads (shapes)", lambda: section_f(r, s3)),
                          ("G. corporate actions (splits)", lambda: section_g(r, s3)),
                          ("H. names and schedule slots", lambda: section_h(r))):
            r.section(title)
            t0 = time.time()
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                r.warn("section failed: %s %s" % (type(e).__name__, str(e)[:300]))
            r.log("(section %.1fs)" % (time.time() - t0))
        r.ok("ops 6743 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
