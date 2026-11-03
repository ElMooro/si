"""justhodl-13f-book - the 13F institutional book, computed from the native holdings research (2026-11-03).

Khalid, 2026-11-03: "13F - Institutional book needs complete redesign ... make sure all 13F engines' features and
data is displayed and observable."

What this engine is
  The page 13f.html shows the book; this engine computes it. It reads ONLY retained, published data and derives the
  boards the way a careful reader of the filings would - counting managers, summing reported values, comparing two
  dated disclosures - and it never infers a purchase, a sale, a flow, a ticker or a unit:
    * data/13f-desk.json                    the native desk (contract holdings-native-research.v1): roster, cohorts,
                                            quality, limitations, the per-fund artifact pointers
    * data/holdings-research/funds/<sha>.json  one immutable artifact per manager (holdings-native-fund.v1):
                                            periods[date].positions (identity = CUSIP + class + SH/PRN + put/call,
                                            reported_quantity, reported_value_usd) and comparison.rows
    * data/13f-clone-alpha.json             SKILL 0-100 per manager (justhodl-13f-clone-alpha)
    * data/13f-price-divergence.json        justhodl-13f-price-divergence (state, methodology, signals)
    * data/institutional-positions.json     the EDGAR detector (justhodl-sec-13f): latest filing per manager
    * data/ai-commentary/13f.json           the deterministic brief for this page
  and HEADs every other 13F-family output so the page can show each engine's freshness.

Output: data/13f-book.json (contract 13f-book.v1). Sections: book (managers, boards, options overlay, totals),
season (next period, deadline, who has filed), clone_alpha, price_divergence, ai_brief, engines (observability),
limitations/quality/research carried verbatim from the desk.

Doctrine
  * "A reported position is not a trade": every board is labelled by what it counts (managers newly disclosing,
    managers whose reported quantity rose...). Reported value changes include the quarter's price move and say so.
  * Identity is the filer's: issuer names are the filers' own text; symbol stays None (the desk's ticker_identity).
  * A newly-present identity whose issuer also appears among the absent identities is flagged reclass_candidate
    (a CUSIP or class change, a spin-off, a conversion) - never counted as new money.
  * An absent identity that no current book reports is flagged no_current_holder (take-private, merger, delisting
    or simply sold by all - the filings cannot say which).
  * Invokes nothing. Reads S3 only. Writes one object. Once a day (cadence floor), after the native research's
    15:40 UTC snapshot and the 16:21 UTC reconcile.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import os
import re
import time

VERSION = "1.0.0"
CONTRACT = "13f-book.v1"
ENGINE = "justhodl-13f-book"

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("OUT_KEY", "data/13f-book.json")
DESK_KEY = "data/13f-desk.json"
CLONE_KEY = "data/13f-clone-alpha.json"
DIVERGENCE_KEY = "data/13f-price-divergence.json"
DETECTOR_KEY = "data/institutional-positions.json"
BRIEF_KEY = "data/ai-commentary/13f.json"

CONCENTRATED_MAX = int(os.environ.get("CONCENTRATED_MAX", "100"))
HEDGE_OPTION_ROWS = int(os.environ.get("HEDGE_OPTION_ROWS", "1000"))
TOP = 10
BOARD = 20
FRESH_H, DEAD_H = 30.0, 168.0

# Display names the desk's roster uses (the desk carries official SEC names; these are the short labels the page
# shows beside them). Not identities - labels only.
DISPLAY = {
    "BERKSHIRE": "Berkshire Hathaway", "BRIDGEWATER": "Bridgewater Associates", "RENAISSANCE": "Renaissance Technologies",
    "AQR": "AQR Capital Management", "TWO_SIGMA": "Two Sigma Investments", "CITADEL": "Citadel Advisors",
    "MILLENNIUM": "Millennium Management", "PERSHING": "Pershing Square", "GREENLIGHT": "Greenlight Capital",
    "SOROS": "Soros Fund Management", "TIGER_GLOBAL": "Tiger Global Management", "COATUE": "Coatue Management",
    "BAUPOST": "Baupost Group", "ELLIOTT": "Elliott Investment Management", "SCION": "Scion Asset Management",
    "DURATION": "Swiss National Bank", "POINT72": "Point72 Asset Management", "LONE_PINE": "Lone Pine Capital",
}
SHORT = {
    "AQR": "AQR", "BAUPOST": "BAU", "BERKSHIRE": "BRK", "BRIDGEWATER": "BW", "CITADEL": "CIT", "COATUE": "COA",
    "DURATION": "SNB", "ELLIOTT": "ELL", "GREENLIGHT": "GLC", "LONE_PINE": "LP", "MILLENNIUM": "MLN", "PERSHING": "PSQ",
    "POINT72": "P72", "RENAISSANCE": "REN", "SCION": "SCN", "SOROS": "SOR", "TIGER_GLOBAL": "TGR", "TWO_SIGMA": "2S",
}

# The 13F family as it is scheduled today (tests/deployment/test_13f_book_schedules.py pins these to
# config/schedule-manifest.json and the engines' config.json, so the page never shows a stale cadence).
ENGINES = [
    {"engine": "justhodl-13f-positions", "role": "native research: originals, chains, dated comparisons (the desk + 18 fund artifacts)",
     "outputs": ["data/13f-desk.json", "data/13f-positions.json", "data/holdings-research.json", "data/13f-by-ticker.json", "data/13f-flows-by-ticker.json"],
     "schedule": "daily 15:40 UTC (Scheduler justhodl-holdings-originals-research) + 16:21 UTC (rule justhodl-13f-positions-sched)"},
    {"engine": "justhodl-sec-13f", "role": "EDGAR filing detector: latest 13F-HR per tracked manager, new accessions",
     "outputs": ["data/institutional-positions.json"], "schedule": "daily 03:31 UTC (rule justhodl-sec-13f-daily)"},
    {"engine": "justhodl-13f-book", "role": "this book: boards, season, options overlay, observability",
     "outputs": ["data/13f-book.json"], "schedule": "daily 16:37 UTC (Scheduler justhodl-13f-book-daily)"},
    {"engine": "justhodl-13f-clone-alpha", "role": "clone-alpha: followable skill per manager from ~13 quarters of filings",
     "outputs": ["data/13f-clone-alpha.json"], "schedule": "weekly Monday 08:30 UTC (Scheduler justhodl-13f-clone-alpha-weekly)"},
    {"engine": "justhodl-13f-price-divergence", "role": "aggregate 13F position change vs 60-day price return",
     "outputs": ["data/13f-price-divergence.json"], "schedule": "weekdays 21:20 UTC (rule justhodl-13f-price-divergence-sched)"},
    {"engine": "justhodl-page-ai-commentary", "role": "the page's brief (deterministic source inventory under the no-paid-AI policy)",
     "outputs": ["data/ai-commentary/13f.json"], "schedule": "weekdays 14:00 UTC (rule)"},
    {"engine": "justhodl-smart-money-13f", "role": "AI-conviction funds' 13F books by bottleneck layer (smart-money-13f.html)",
     "outputs": ["data/smart-money-13f.json"], "schedule": "weekly Monday 11:03 UTC"},
    {"engine": "justhodl-smart-money-cluster", "role": "cross-fund overlap clusters read from the 13F tape",
     "outputs": ["data/smart-money-cluster.json"], "schedule": "daily 16:55 UTC (Scheduler justhodl-smart-money-cluster-hourly)"},
    {"engine": "justhodl-smart-money-holdings", "role": "famous-fund conviction holdings for the screener (schedule DISABLED)",
     "outputs": ["screener/smart-money-holdings.json"], "schedule": "DISABLED (smart-money-holdings-sched 14:31 UTC)"},
    {"engine": "justhodl-smart-money-tracker", "role": "smart-money screener tape (schedule DISABLED)",
     "outputs": ["screener/smart-money.json"], "schedule": "DISABLED (smart-money-tracker-sched 14:00 UTC)"},
    {"engine": "justhodl-institutional-footprint", "role": "13F footprint consumer (capital-flow lens)",
     "outputs": ["data/institutional-footprint.json"], "schedule": "weekdays 23:10 UTC"},
]
STATE_KEYS = ["data/13f-cusip-map.json", "data/13f-state/first-seen.json", "data/13f-state/cik-overrides.json"]


# ───────────────────────────── pure helpers ─────────────────────────────
def dec(v):
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def f(v):
    return float(v) if v is not None else None


def pct(a, b):
    """100 * (a / b - 1), None when b is 0/None. Pure."""
    a, b = dec(a), dec(b)
    return f(round(100 * (a / b - 1), 1)) if b else None


def is_equity(identity):
    return (identity or {}).get("quantity_type") == "SH" and not (identity or {}).get("put_call")


def issuer_key(name):
    """First two alphabetic tokens of an issuer name, for reclassification matching. Pure."""
    toks = [t for t in re.split(r"[^A-Z0-9]+", str(name or "").upper()) if t and t not in ("INC", "CORP", "CO", "LTD", "PLC", "LLC", "LP", "NEW", "THE", "DEL", "SA", "NV")]
    return " ".join(toks[:2])


def fund_boards(detail):
    """Per-fund boards from one native artifact (holdings-native-fund.v1). Pure."""
    periods = detail.get("periods") or {}
    cur_p = detail.get("current_holdings_period")
    pri_p = detail.get("prior_holdings_period")
    cur = (periods.get(cur_p) or {}).get("positions") or {}
    pri = (periods.get(pri_p) or {}).get("positions") or {}
    eq = {k: v for k, v in cur.items() if is_equity(v.get("identity"))}
    opt = {k: v for k, v in cur.items() if not is_equity(v.get("identity"))}
    eq_prior = {k: v for k, v in pri.items() if is_equity(v.get("identity"))}
    total = sum((dec(v.get("reported_value_usd")) for v in eq.values()), Decimal(0))
    total_prior = sum((dec(v.get("reported_value_usd")) for v in eq_prior.values()), Decimal(0))
    puts = sum((dec(v.get("reported_value_usd")) for v in opt.values() if (v.get("identity") or {}).get("put_call") == "PUT"), Decimal(0))
    calls = sum((dec(v.get("reported_value_usd")) for v in opt.values() if (v.get("identity") or {}).get("put_call") == "CALL"), Decimal(0))
    comp = {r["position_id"]: r for r in ((detail.get("comparison") or {}).get("rows") or []) if isinstance(r, dict) and r.get("position_id")}

    def row(pid, pos, extra=None):
        ident = pos.get("identity") or {}
        r = {"id": pid[:12], "issuer": (pos.get("issuer_names") or [""])[0], "class": ident.get("class"), "cusip": ident.get("cusip"),
             "value_usd": f(dec(pos.get("reported_value_usd"))), "qty": f(dec(pos.get("reported_quantity"))),
             "pct_of_book": f(round(100 * dec(pos.get("reported_value_usd")) / total, 2)) if total else None}
        if extra:
            r.update(extra)
        return r

    top = sorted(eq.items(), key=lambda kv: -dec(kv[1].get("reported_value_usd")))[:TOP]
    top_rows = []
    for pid, pos in top:
        c = comp.get(pid) or {}
        qc = c.get("reported_quantity_change")
        q0 = dec(eq_prior.get(pid, {}).get("reported_quantity")) if pid in eq_prior else Decimal(0)
        top_rows.append(row(pid, pos, {
            "status": c.get("status"),
            "qty_change": f(dec(qc)) if qc is not None else None,
            "qty_change_pct": f(round(100 * dec(qc) / q0, 1)) if qc is not None and q0 else None,
            "value_change_usd": f(dec(c.get("reported_value_change_usd"))) if c.get("reported_value_change_usd") is not None else None}))
    top10_pct = f(round(100 * sum((dec(p.get("reported_value_usd")) for _, p in top), Decimal(0)) / total, 1)) if total else None

    new_rows, absent_rows, inc_rows, dec_rows = [], [], [], []
    for pid, c in comp.items():
        st = c.get("status")
        if st == "newly_present_in_public_disclosure" and pid in eq:
            new_rows.append(row(pid, eq[pid]))
        elif st == "not_present_in_current_public_disclosure" and pid in eq_prior:
            p = eq_prior[pid]
            absent_rows.append({"id": pid[:12], "issuer": (p.get("issuer_names") or [""])[0], "class": (p.get("identity") or {}).get("class"),
                                "cusip": (p.get("identity") or {}).get("cusip"), "prior_value_usd": f(dec(p.get("reported_value_usd"))),
                                "prior_qty": f(dec(p.get("reported_quantity")))})
        elif st in ("reported_quantity_increased", "reported_quantity_decreased") and pid in eq and pid in eq_prior:
            q0 = dec(eq_prior[pid].get("reported_quantity"))
            qc = dec(c.get("reported_quantity_change"))
            r = row(pid, eq[pid], {"qty_change": f(qc), "qty_change_pct": f(round(100 * qc / q0, 1)) if q0 else None,
                                   "value_change_usd": f(dec(c.get("reported_value_change_usd")))})
            (inc_rows if st == "reported_quantity_increased" else dec_rows).append(r)
    new_rows.sort(key=lambda r: -(r["value_usd"] or 0))
    absent_rows.sort(key=lambda r: -(r["prior_value_usd"] or 0))
    inc_rows.sort(key=lambda r: -(r["value_change_usd"] or 0))
    dec_rows.sort(key=lambda r: (r["value_change_usd"] or 0))
    counts = dict((detail.get("comparison") or {}).get("counts") or {})
    return {
        "current_period": cur_p, "prior_period": pri_p,
        "n_equity_positions": len(eq), "n_option_rows": len(opt), "n_prior_equity_positions": len(eq_prior),
        "total_value_usd": f(total), "total_value_prior_usd": f(total_prior), "value_change_pct": pct(total, total_prior),
        "put_value_usd": f(puts), "call_value_usd": f(calls),
        "top10_pct_of_book": top10_pct, "top": top_rows,
        "newly_present": new_rows[:TOP], "n_newly_present_equity": len(new_rows),
        "absent": absent_rows[:TOP], "n_absent_equity": len(absent_rows),
        "increased": inc_rows[:TOP], "n_increased_equity": len(inc_rows),
        "decreased": dec_rows[:TOP], "n_decreased_equity": len(dec_rows),
        "counts": counts,
        "shared_discretion_positions": sum(1 for v in eq.values() if v.get("other_managers")),
    }, eq, eq_prior, comp


def cross_boards(members):
    """members: list of (fund_key, eq, eq_prior, comp). Counts managers and sums reported values by identity. Pure."""
    agg = {}

    def slot(pid, pos):
        ident = pos.get("identity") or {}
        return agg.setdefault(pid, {"issuer": (pos.get("issuer_names") or [""])[0], "class": ident.get("class"), "cusip": ident.get("cusip"),
                                    "holders": [], "value_usd": Decimal(0), "new": [], "absent": [], "inc": [], "dec": [], "flat": []})

    for fk, eq, eq_prior, comp in members:
        for pid, pos in eq.items():
            a = slot(pid, pos)
            a["holders"].append(fk)
            a["value_usd"] += dec(pos.get("reported_value_usd"))
            st = (comp.get(pid) or {}).get("status")
            if st == "newly_present_in_public_disclosure":
                a["new"].append(fk)
            elif st == "reported_quantity_increased":
                a["inc"].append(fk)
            elif st == "reported_quantity_decreased":
                a["dec"].append(fk)
            elif st == "reported_quantity_unchanged":
                a["flat"].append(fk)
        for pid, c in comp.items():
            if c.get("status") == "not_present_in_current_public_disclosure" and pid in eq_prior:
                slot(pid, eq_prior[pid])["absent"].append(fk)

    absent_keys = {(a["cusip"], issuer_key(a["issuer"])) for a in agg.values() if a["absent"] and not a["holders"]}
    absent_cusips = {a["cusip"] for a in agg.values() if a["absent"]}
    absent_issuers = {issuer_key(a["issuer"]) for a in agg.values() if a["absent"]}
    new_cusips = {a["cusip"] for a in agg.values() if a["new"]}
    new_issuers = {issuer_key(a["issuer"]) for a in agg.values() if a["new"]}

    def out(pid, a):
        ik = issuer_key(a["issuer"])
        reclass = bool(a["new"]) and (a["cusip"] in absent_cusips or ik in absent_issuers) and not a["absent"]
        reclass_abs = bool(a["absent"]) and not a["holders"] and (a["cusip"] in new_cusips or ik in new_issuers)
        return {"id": pid[:12], "issuer": a["issuer"], "class": a["class"], "cusip": a["cusip"], "n_holders": len(a["holders"]),
                "value_usd": f(a["value_usd"]), "holders": sorted(a["holders"]), "new": sorted(a["new"]), "absent": sorted(a["absent"]),
                "inc": sorted(a["inc"]), "dec": sorted(a["dec"]), "n_new": len(a["new"]), "n_absent": len(a["absent"]),
                "n_inc": len(a["inc"]), "n_dec": len(a["dec"]), "n_flat": len(a["flat"]),
                "reclass_candidate": reclass or reclass_abs, "no_current_holder": bool(a["absent"]) and not a["holders"]}
    items = list(agg.items())
    return {
        "n_identities": len(agg),
        "widely_held": [out(p, a) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["holders"]), -kv[1]["value_usd"])) if a["holders"]][:BOARD],
        "largest_value": [out(p, a) for p, a in sorted(items, key=lambda kv: -kv[1]["value_usd"]) if a["holders"]][:BOARD],
        "newly_present": [out(p, a) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["new"]), -kv[1]["value_usd"])) if a["new"]][:BOARD],
        "dropped": [out(p, a) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["absent"]), -len(kv[1]["holders"]))) if a["absent"]][:BOARD],
        "increased": [out(p, a) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["inc"]), -kv[1]["value_usd"])) if a["inc"]][:BOARD],
        "decreased": [out(p, a) for p, a in sorted(items, key=lambda kv: (-len(kv[1]["dec"]), -kv[1]["value_usd"])) if a["dec"]][:BOARD],
    }


def status_of(meta, boards, required):
    """Primary status chip + flags for one manager. Pure."""
    ls = meta.get("latest_submission") or {}
    form = str(ls.get("form") or "")
    flags = []
    if meta.get("valuation_reviews"):
        flags.append("VALUATION_REVIEW")
    if boards.get("n_option_rows", 0) >= HEDGE_OPTION_ROWS:
        flags.append("HEDGE_BOOK")
    period = boards.get("current_period")
    if form == "13F-NT" and period != required:
        return "NOTICE", flags
    if period == required and form.endswith("/A"):
        return "AMENDED", flags
    if period == required:
        return "CURRENT", flags
    return "DATED", flags


def quarter_end_after(day):
    """The first calendar quarter end strictly after `day` (date). Pure."""
    y, m = day.year, day.month
    for qm in (3, 6, 9, 12):
        qe = date(y, qm, [31, 30, 30, 31][(qm // 3) - 1])
        if qe > day:
            return qe
    return date(y + 1, 3, 31)


def us_federal_holidays(year):
    """Observed US federal holidays for a year (weekend observance shifted). Pure."""
    def nth_weekday(month, weekday, n):
        d = date(year, month, 1)
        d += timedelta(days=(weekday - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)

    def last_weekday(month, weekday):
        d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
        return d - timedelta(days=(d.weekday() - weekday) % 7)

    fixed = [date(year, 1, 1), date(year, 6, 19), date(year, 7, 4), date(year, 11, 11), date(year, 12, 25)]
    out = set()
    for d in fixed:
        if d.weekday() == 5:
            out.add(d - timedelta(days=1))
        elif d.weekday() == 6:
            out.add(d + timedelta(days=1))
        else:
            out.add(d)
    out.add(nth_weekday(1, 0, 3))    # MLK
    out.add(nth_weekday(2, 0, 3))    # Presidents
    out.add(last_weekday(5, 0))      # Memorial
    out.add(nth_weekday(9, 0, 1))    # Labor
    out.add(nth_weekday(10, 0, 2))   # Columbus
    out.add(nth_weekday(11, 3, 4))   # Thanksgiving
    return out


def filing_deadline(period_end):
    """13F deadline: 45 days after quarter end, rolled to the next business day. Pure."""
    d = period_end + timedelta(days=45)
    hol = us_federal_holidays(d.year)
    while d.weekday() >= 5 or d in hol:
        d += timedelta(days=1)
    return d


def season(required, today, detector, desk_by_fund):
    """Next period, deadline, days left and who has already filed for it. Pure."""
    req = date.fromisoformat(required) if required else None
    nxt = quarter_end_after(req) if req else quarter_end_after(today)
    deadline = filing_deadline(nxt)
    filed = []
    for fk, m in (desk_by_fund or {}).items():
        ls = (m or {}).get("latest_submission") or {}
        if ls.get("period_of_report") == nxt.isoformat():
            filed.append(fk)
    for fk, m in ((detector or {}).get("by_fund") or {}).items():
        lf = (m or {}).get("latest_filing") or {}
        if lf.get("period_of_report") == nxt.isoformat() and fk not in filed:
            filed.append(fk)
    return {"required_period": required, "next_period": nxt.isoformat(), "deadline": deadline.isoformat(),
            "deadline_rule": "45 days after quarter end, next business day when that is a weekend or US federal holiday",
            "days_to_deadline": (deadline - today).days, "filed_for_next": sorted(filed), "n_filed_for_next": len(filed),
            "tracked": len(desk_by_fund or {}),
            "detector": {"generated_at": (detector or {}).get("generated_at"), "tracked_funds": (detector or {}).get("tracked_funds"),
                         "filings_seen": (detector or {}).get("filings_seen"), "fetch_errors": (detector or {}).get("fetch_errors"),
                         "new_filings": ((detector or {}).get("new_filings") or [])[:20]}}


def clone_summary(managers):
    rows = {}
    for key, m in (managers or {}).items():
        if not isinstance(m, dict):
            continue
        row = {k: m.get(k) for k in ("name", "n_windows", "hedge_book", "ann_clone_pct", "ann_spy_pct", "ann_alpha_pct", "hit_rate", "ir",
                                     "best_alpha", "worst_alpha", "skill_score", "label", "note", "reason", "status") if k in m}
        w = m.get("windows") or []
        if w:
            row["last_window"] = {k: w[-1].get(k) for k in ("q", "entry", "exit", "ret", "spy", "alpha", "cov")}
            row["windows"] = [{k: x.get(k) for k in ("q", "ret", "spy", "alpha")} for x in w]
        rows[key] = row
    return rows


def tier(age_h):
    if age_h is None:
        return "missing"
    if age_h < FRESH_H:
        return "fresh"
    if age_h < DEAD_H:
        return "stale"
    return "dead"


def sum_counts(managers, keys):
    c = Counter()
    for fk in keys:
        for k, v in (managers.get(fk, {}).get("counts") or {}).items():
            c[k] += int(v or 0)
    return dict(c)


# ───────────────────────────── AWS I/O ─────────────────────────────
def _s3():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", config=Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=120))


def get_json(s3, key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"].read()
    return json.loads(body or b"{}"), obj["LastModified"], len(body)


def head(s3, key, now):
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=key)
        age = (now - h["LastModified"]).total_seconds() / 3600
        return {"key": key, "bytes": h["ContentLength"], "last_modified": h["LastModified"].isoformat(timespec="seconds"), "age_h": round(age, 1), "tier": tier(age)}
    except Exception as exc:  # noqa: BLE001
        return {"key": key, "bytes": None, "last_modified": None, "age_h": None, "tier": "missing", "error": type(exc).__name__}


def build(s3, now, log=print):
    started = time.time()
    desk, desk_mod, desk_bytes = get_json(s3, DESK_KEY)
    required = desk.get("required_period_for_current_cohort")
    by_fund = desk.get("by_fund") or {}
    managers, members, members_conc, errors = {}, [], [], []
    for fk in sorted(by_fund):
        meta = by_fund[fk] or {}
        key = (meta.get("detail") or {}).get("key")
        if not key:
            errors.append({"fund": fk, "error": "no artifact key in the desk"})
            continue
        try:
            detail, mod, size = get_json(s3, key)
        except Exception as exc:  # noqa: BLE001
            errors.append({"fund": fk, "error": "%s: %s" % (type(exc).__name__, str(exc)[:120])})
            continue
        boards, eq, eq_prior, comp = fund_boards(detail)
        status, flags = status_of(meta, boards, required)
        ls = meta.get("latest_submission") or {}
        boards.update({
            "fund_key": fk, "display_name": DISPLAY.get(fk, fk.title()), "short": SHORT.get(fk, fk[:3]),
            "official_name": meta.get("official_name") or meta.get("name"), "cik": meta.get("cik"),
            "latest_submission": {k: ls.get(k) for k in ("form", "filed_at", "accepted_at", "accession", "period_of_report")},
            "effective_accessions": meta.get("effective_accessions"), "chain_status": meta.get("current_chain_status"),
            "cohort": bool(meta.get("current_cohort_eligible")), "observation_age_days": meta.get("observation_age_days"),
            "status": status, "flags": flags, "valuation_reviews": meta.get("valuation_reviews") or [],
            "artifact": {"key": key, "bytes": size, "sha256": (meta.get("detail") or {}).get("sha256"), "last_modified": mod.isoformat(timespec="seconds")},
            "filings": [{k: v for k, v in (fs or {}).items() if k in ("accession", "form", "period_of_report", "filed_at", "accepted_at")} | {
                "amendment_number": ((fs or {}).get("cover") or {}).get("amendment_number"),
                "amendment_type": ((fs or {}).get("cover") or {}).get("amendment_type"),
                "reported_rows": ((fs or {}).get("cover") or {}).get("reported_rows"),
                "reported_value_sum": ((fs or {}).get("cover") or {}).get("reported_value_sum"),
                "confidential_omissions_declared": ((fs or {}).get("cover") or {}).get("confidential_omissions_declared")}
                        for fs in (detail.get("filing_summaries") or {}).values()],
        })
        managers[fk] = boards
        if boards["cohort"] and boards["current_period"] == required:
            members.append((fk, eq, eq_prior, comp))
            if boards["n_equity_positions"] <= CONCENTRATED_MAX:
                members_conc.append((fk, eq, eq_prior, comp))
        log("%s: %s %s | eq %d opt %d | $%.2fB | status %s %s" % (fk, boards["current_period"], ls.get("form"), boards["n_equity_positions"],
                                                                     boards["n_option_rows"], (boards["total_value_usd"] or 0) / 1e9, status, flags))

    cohort_keys = [fk for fk, *_ in members]
    conc_keys = [fk for fk, *_ in members_conc]
    cohort_total = sum((managers[fk]["total_value_usd"] or 0) for fk in cohort_keys)
    hedge = [fk for fk in cohort_keys if managers[fk]["n_option_rows"] >= HEDGE_OPTION_ROWS]
    book = {
        "required_period": required, "cohort": cohort_keys, "concentrated": conc_keys, "concentrated_max_positions": CONCENTRATED_MAX,
        "hedge_books": hedge, "hedge_books_equity_value_usd": sum((managers[fk]["total_value_usd"] or 0) for fk in hedge),
        "cohort_equity_value_usd": cohort_total, "cohort_counts": sum_counts(managers, cohort_keys),
        "managers": managers, "boards": {"cohort": cross_boards(members), "concentrated": cross_boards(members_conc)},
        "options_overlay": sorted([{"fund_key": fk, "display_name": m["display_name"], "n_option_rows": m["n_option_rows"], "put_value_usd": m["put_value_usd"],
                                    "call_value_usd": m["call_value_usd"], "equity_value_usd": m["total_value_usd"],
                                    "put_share": f(round(100 * dec(m["put_value_usd"]) / (dec(m["put_value_usd"]) + dec(m["call_value_usd"])), 1)) if (m["put_value_usd"] or m["call_value_usd"]) else None,
                                    "puts_over_equity": f(round(dec(m["put_value_usd"]) / dec(m["total_value_usd"]), 2)) if m["total_value_usd"] else None,
                                    "status": m["status"]} for fk, m in managers.items() if m["n_option_rows"]],
                                  key=lambda r: -((r["put_value_usd"] or 0) + (r["call_value_usd"] or 0))),
        "errors": errors,
    }

    # the other 13F feeds - each optional, each reported honestly when missing
    def optional(key):
        try:
            doc, mod, size = get_json(s3, key)
            return doc, mod, size, None
        except Exception as exc:  # noqa: BLE001
            return {}, None, None, "%s" % type(exc).__name__

    ca, ca_mod, _, ca_err = optional(CLONE_KEY)
    for fk, row in clone_summary(ca.get("managers")).items():
        if fk in managers:
            managers[fk]["clone"] = row
    clone_block = {"as_of": ca.get("as_of"), "status": ca.get("status"), "pct_complete": ca.get("pct_complete"), "filings_done": ca.get("filings_done"),
                   "filings_total": ca.get("filings_total"), "method": ca.get("method"), "warns": (ca.get("warns") or [])[:10], "error": ca_err,
                   "ranked": sorted([{"fund_key": fk, "display_name": managers[fk]["display_name"], **managers[fk]["clone"]} for fk in managers if managers[fk].get("clone")],
                                    key=lambda r: -(r.get("skill_score") if isinstance(r.get("skill_score"), (int, float)) else -1))}
    pdv, _, _, pdv_err = optional(DIVERGENCE_KEY)
    divergence = {k: pdv.get(k) for k in ("engine", "version", "as_of", "state", "data_sufficiency", "signal_strength", "n_divergences", "n_bullish", "n_bearish",
                                          "n_high_conviction", "feeders", "methodology", "sources", "why_now", "run_seconds")}
    divergence.update({"top_bullish": (pdv.get("top_bullish") or [])[:10], "top_bearish": (pdv.get("top_bearish") or [])[:10], "error": pdv_err})
    det, _, _, det_err = optional(DETECTOR_KEY)
    detector_funds = {fk: {"name": (m or {}).get("name"), **{k: ((m or {}).get("latest_filing") or {}).get(k) for k in ("period_of_report", "filed_at", "accession", "form")}}
                      for fk, m in ((det.get("by_fund") or {}).items())}
    brief, _, _, brief_err = optional(BRIEF_KEY)
    commentary = brief.get("commentary") if isinstance(brief.get("commentary"), dict) else {}
    ai_brief = {k: brief.get(k) for k in ("generated_at", "model", "model_api_calls", "portfolio_action", "contract", "calls_eligible", "execution_eligible",
                                          "sizing_eligible", "forecast_qualified", "llm_attempt_failed", "version")}
    ai_brief.update({"headline": commentary.get("headline"), "narrative": commentary.get("narrative"), "posture": commentary.get("posture"), "mode": commentary.get("mode"),
                     "divergence_signals": commentary.get("divergence_signals"), "source_coverage": brief.get("source_coverage"), "error": brief_err})

    engines = []
    for e in ENGINES:
        outs = [head(s3, k, now) for k in e["outputs"]]
        ages = [o["age_h"] for o in outs if o["age_h"] is not None]
        engines.append({**e, "objects": outs, "newest_age_h": min(ages) if ages else None, "tier": tier(min(ages)) if ages else "missing"})
    state = [head(s3, k, now) for k in STATE_KEYS]

    out = {
        "engine": ENGINE, "version": VERSION, "contract": CONTRACT, "generated_at": now.isoformat(timespec="seconds"),
        "source": {"desk_key": DESK_KEY, "desk_generated_at": desk.get("generated_at"), "desk_last_modified": desk_mod.isoformat(timespec="seconds"), "desk_bytes": desk_bytes,
                   "desk_contract": desk.get("contract"), "source_generated_at": desk.get("source_generated_at"), "source_originals": desk.get("source_originals"),
                   "research": desk.get("research"), "legacy_snapshot": desk.get("legacy_snapshot"), "security_indexes": desk.get("security_indexes"),
                   "ticker_identity": desk.get("ticker_identity"), "methodology_url": desk.get("methodology_url"), "research_url": desk.get("research_url")},
        "cadence": "daily (cadence floor); the native research publishes 15:40 UTC, this book 16:37 UTC; EDGAR detector 03:31 UTC",
        "note": ("Every figure is a reported quantity or a reported value from a retained 13F-HR information table, compared across two dated "
                 "disclosures. Newly disclosed is not newly bought; absent is not sold; a reported value change carries the price move. "
                 "No tickers, trades, flows or units are inferred."),
        "limitations": desk.get("limitations"), "quality": desk.get("quality"), "report_period_cohorts": desk.get("report_period_cohorts"),
        "permissions": {k: desk.get(k) for k in ("call", "calls_eligible", "sizing_eligible", "execution_eligible", "additional_independent_votes")},
        "book": book,
        "season": season(required, now.date(), det, by_fund) | {"detector_error": det_err, "detector_funds": detector_funds},
        "clone_alpha": clone_block, "price_divergence": divergence, "ai_brief": ai_brief,
        "engines": engines, "state_objects": state,
        "run_seconds": round(time.time() - started, 1),
    }
    return out


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    s3 = _s3()
    out = build(s3, now)
    body = json.dumps(out, separators=(",", ":"), ensure_ascii=False, default=str, allow_nan=False).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=body, ContentType="application/json; charset=utf-8", CacheControl="max-age=300")
    b = out["book"]
    print("13f-book: %d managers, cohort %d, concentrated %d, cohort equity $%.1fB, %d bytes, %.1fs, errors %d" % (
        len(b["managers"]), len(b["cohort"]), len(b["concentrated"]), b["cohort_equity_value_usd"] / 1e9, len(body), out["run_seconds"], len(b["errors"])))
    return {"statusCode": 200, "key": OUT_KEY, "bytes": len(body), "managers": len(b["managers"]), "cohort": len(b["cohort"]),
            "run_seconds": out["run_seconds"], "errors": len(b["errors"])}
