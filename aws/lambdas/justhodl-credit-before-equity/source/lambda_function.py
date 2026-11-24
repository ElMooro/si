"""justhodl-credit-before-equity 2.1.0 - the bond desk reprices a company before the equity desk does.

1.0 read a structural default model for 26 hand-listed issuers - a model computed from each company's own stock - and
compared each day with the day before. 2.0 reads what the credit market itself prints, for every company it can:

  bonds     every US-dollar corporate bond the corporate bonds desk holds (data/corp-bonds.json: the daily holdings of the
            iShares investment-grade, high-yield and emerging-market corporate books), turned into a spread over the
            Treasury par curve (data/warm/treasury-par/curve.json.gz) and gathered by company
  CDS       every corporate reference entity of the CDS desk (data/cds-desk.json: DTCC's public prints)
  stock     the Polygon EOD bank's closes, split-repaired, for every company tied to a listed stock (cbe_names)
  model     the 1.0 structural model (data/cds-monitor.json), kept for its 26 names and said to be a model

A company's spread change is crossed with its stock over the same days. A signal fires only when credit moved past its
own book and the stock has not followed (cbe_core). Since 2.1 every company also has a stress rank that does not look at
the stock (how wide it is for its book, how far it widened over the year and the month), and a theme (cbe_themes: the AI
stack) groups companies the bond market's own sectors keep apart.

Each day's spreads are kept by the bond desk's own bond key under credit/cbe-spreads/, so the one-week and three-month
comparisons build from the board's own ledger; the bond desk's change since its previous holdings date and since the
year's start seed the first two reference days.

The warehouse only: no provider is asked for anything. Output data/credit-before-equity.json (contract
credit-before-equity.v2 - every 1.0 key is still there). Daily 22:35 UTC.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import gzip
import json
import os

import boto3

import bank_enrich          # aws/shared: the whole-market factor book - S3 only
import cbe_core as C
import cbe_names as N
import leaders_tape as T

VERSION = C.VERSION
REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
BONDS_KEY = "data/corp-bonds.json"
CDS_KEY = "data/cds-desk.json"
MODEL_KEY = "data/cds-monitor.json"
PAR_KEY = "data/warm/treasury-par/curve.json.gz"
OUT_KEY = "data/credit-before-equity.json"
HIST_KEY = "credit/credit-before-equity-history.json"      # the 1.0 model ledger, still kept
LEDGER_ROOT = "credit/cbe-spreads/"                         # one file a holdings day: every bond's spread
EOD_ROOT = "data/warm/polygon-full/eod/"
SPLITS_ROOT = "data/warm/polygon-bank/splits/"

BOOK_FIELDS = ("name", "sector", "industry", "mcap", "exchange", "country", "security_type", "is_common", "is_adr", "is_etf", "is_fund", "is_preferred",
               "is_warrant_unit", "is_shell", "is_debt_or_structured", "is_otc", "dollar_vol_20d")
MIN_CAP = 5e7
BARS = 330                    # sessions read back: the year's start and a quarter before it
THREADS = 32
HIST_KEEP = 180
MIN_DD_MOVE = 0.05            # the 1.0 model rule, unchanged
MIN_CDS_MOVE_BP = 5.0
BOND_DESK_LATE_DAYS = 5
CDS_DESK_LATE_DAYS = 5
MIN_BONDS, MIN_COMPANIES, MIN_LISTED = 5000, 1000, 300      # under these the bond desk is broken: nothing is published
ORDER = {"CREDIT_LEADS_DOWN": 0, "CREDIT_LEADS_UP": 1, "NONE": 2, "INSUFFICIENT_HISTORY": 3}


# ----------------------------------------------------------------------------------------------------------------- s3
def _s3():
    return boto3.client("s3", REGION)


def missing(exc):
    code = str(((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") or "")
    return type(exc).__name__ in ("NoSuchKey", "KeyError") or code in ("NoSuchKey", "404", "NotFound") or "NoSuchKey" in str(exc)


def get_json(s3, key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def optional(s3, key):
    try:
        return get_json(s3, key), None
    except Exception as exc:  # noqa: BLE001
        return None, ("not there" if missing(exc) else "%s: %s" % (type(exc).__name__, str(exc)[:120]))


def put_json(s3, key, doc, gz=False):
    body = json.dumps(doc, separators=(",", ":"), default=str).encode()
    extra = {"ContentType": "application/json"}
    if gz:
        body, extra["ContentEncoding"] = gzip.compress(body, 6), "gzip"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, **extra)
    return len(body)


def bank_key(ticker):
    return EOD_ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(ticker).upper())[:80] + ".json.gz"


# -------------------------------------------------------------------------------------------------------------- readers
def fund_bars(doc):
    """{date, o, h, l, c, v} oldest first from one EOD bank document. Pure."""
    out = []
    for r in (doc or {}).get("rows") or []:
        if not isinstance(r, list) or len(r) < 6 or r[0] is None or r[4] is None:
            continue
        b = {k: v for k, v in zip(("o", "h", "l", "c", "v"), r[1:6]) if v is not None}
        b["date"] = datetime.fromtimestamp(r[0] / 1000.0, tz=timezone.utc).date().isoformat()
        out.append(b)
    return out


def months_between(first_iso, last_iso):
    y, m = int(first_iso[:4]), int(first_iso[5:7])
    ly, lm = int(last_iso[:4]), int(last_iso[5:7])
    out = []
    while (y, m) <= (ly, lm):
        out.append("%04d-%02d" % (y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def recorded_splits(s3, first_iso, last_iso, want):
    """Polygon's split reference rows for the tickers in `want`: ({ticker: [{date, split_from, split_to}]}, files read)."""
    out, n = {}, 0

    def one(month):
        doc, _ = optional(s3, SPLITS_ROOT + month + ".json.gz")
        return doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) else None)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for rows in pool.map(one, months_between(first_iso, last_iso)):
            if rows is None:
                continue
            n += 1
            for r in rows:
                if not isinstance(r, dict):
                    continue
                t, d = str(r.get("ticker") or "").upper(), str(r.get("execution_date") or "")
                if t in want and first_iso <= d <= last_iso and r.get("split_from") and r.get("split_to"):
                    out.setdefault(t, []).append({"date": d, "split_from": r["split_from"], "split_to": r["split_to"]})
    return out, n


def repair(raw, recorded):
    """A stock's closes as one continuous, split-adjusted history: ({day: close}, splits rebased). Pure."""
    own, _ = T.own_history(raw[-(BARS + 40):])
    bars, events = T.rebase(own, recorded or [], [])
    out = {}
    for b in bars[-BARS:]:
        try:
            c = float(b["c"])
        except (TypeError, ValueError, KeyError):
            continue
        if c > 0 and c == c:
            out[b["date"]] = c
    return out, events


def bank_names(ticker):
    """The price bank's spellings of a factor-book ticker: BRK-B is BRK.B there."""
    t = str(ticker).upper()
    return [t] if "-" not in t else [t, t.replace("-", ".")]


def read_closes(s3, tickers, first_iso, last_iso):
    """({ticker: {day: close}}, tickers with no file, splits rebased) from the Polygon EOD bank, split-repaired."""
    want = {n for t in tickers for n in bank_names(t)}
    recorded, _ = recorded_splits(s3, first_iso, last_iso, want)

    def one(t):
        for name in bank_names(t):
            doc, err = optional(s3, bank_key(name))
            if doc is None:
                continue
            try:
                closes, events = repair(fund_bars(doc), recorded.get(name))
            except Exception:  # noqa: BLE001
                return t, None, 0
            return t, closes, len(events)
        return t, None, 0
    out, none, n_events = {}, [], 0
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for t, closes, n in pool.map(one, sorted(tickers)):
            if closes:
                out[t] = closes
                n_events += n
            else:
                none.append(t)
    return out, none, n_events


def listed_companies(rows):
    """The factor book's listed companies: common stock or a depositary receipt on an exchange, $50M or more."""
    out = []
    for r in rows or []:
        if not r.get("symbol") or not r.get("name") or (r.get("mcap") or 0) < MIN_CAP:
            continue
        if r.get("is_common") is False or r.get("is_otc") or r.get("is_etf") or r.get("is_fund") or r.get("is_preferred") or r.get("is_warrant_unit") or r.get("is_shell") or r.get("is_debt_or_structured"):
            continue
        if r.get("security_type") not in (None, "CS", "ADRC"):
            continue
        out.append(r)
    return out


def ledger_days(s3):
    """The holdings days the board's own ledger holds."""
    days, token = set(), None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": LEDGER_ROOT}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        for o in page.get("Contents") or []:
            name = o["Key"][len(LEDGER_ROOT):]
            if len(name) == 18 and name.endswith(".json.gz"):
                days.add(name[:10])
        token = page.get("NextContinuationToken")
        if not token:
            return days


# ---------------------------------------------------------------------------------------------------------- the model
def model_rows(model_doc, hist, today, last_close):
    """The 1.0 board for its own names, by its own rule (each day against the observation before it). Returns {ticker: row}."""
    snc = (model_doc or {}).get("single_name_cds") or {}
    obs = hist.setdefault("obs", {})
    out = {}
    for grp in ("corporates", "banks"):
        for r in snc.get(grp) or []:
            tk = str(r.get("ticker") or "").upper()
            if not tk:
                continue
            dd, cds, price = C.f(r.get("distance_to_default"), 6), C.f(r.get("synthetic_cds_bp"), 4), last_close.get(tk)
            rec = obs.setdefault(tk, {})
            rec[today] = {"dd": dd, "cds": cds, "px": price}
            dates = sorted(rec)
            prior_d = dates[-2] if len(dates) >= 2 else None
            prior = rec.get(prior_d) if prior_d else None
            d_dd = d_cds = d_px = None
            if prior:
                if dd is not None and C.f(prior.get("dd"), 6) is not None:
                    d_dd = round(dd - C.f(prior["dd"], 6), 3)
                if cds is not None and C.f(prior.get("cds"), 4) is not None:
                    d_cds = round(cds - C.f(prior["cds"], 4), 1)
                pp = C.f(prior.get("px"), 6)
                if price is not None and pp:
                    d_px = round(100 * (price / pp - 1), 2)
            direction = None
            if d_dd is not None and abs(d_dd) >= MIN_DD_MOVE:
                direction = "IMPROVING" if d_dd > 0 else "DETERIORATING"
            elif d_cds is not None and abs(d_cds) >= MIN_CDS_MOVE_BP:
                direction = "IMPROVING" if d_cds < 0 else "DETERIORATING"
            flat = d_px is not None and abs(d_px) <= C.FLAT_BAND_PCT
            signal = "INSUFFICIENT_HISTORY" if prior is None else "NONE"
            if direction == "IMPROVING" and d_px is not None and (flat or d_px < 0):
                signal = "CREDIT_LEADS_UP"
            elif direction == "DETERIORATING" and d_px is not None and (flat or d_px > 0):
                signal = "CREDIT_LEADS_DOWN"
            out[tk] = {"name": r.get("name"), "group": r.get("group") or grp, "distance_to_default": dd, "synthetic_cds_bp": cds, "default_prob_5y_pct": C.f(r.get("default_prob_5y_pct"), 4),
                       "regime": r.get("regime"), "peer_rank": r.get("peer_rank"), "market_cap_usd_bn": C.f(r.get("market_cap_usd_bn"), 3), "d_distance_to_default": d_dd,
                       "d_synthetic_cds_bp": d_cds, "model_d_price_pct": d_px, "model_credit_direction": direction, "model_signal": signal, "prior_obs_date": prior_d, "hist_n": len(dates)}
    for rec in obs.values():
        for old in sorted(rec)[:-HIST_KEEP]:
            rec.pop(old, None)
    return out


# ---------------------------------------------------------------------------------------------------------- the build
ISSUER_COLS = ["id", "ticker", "name", "industry", "book", "country", "n_bonds", "value", "yield_pct", "dur", "spread_bp", "d_w1", "d_m1", "x_m1", "pct_m1", "n_m1", "d_m3", "d_ytd",
               "cds_bp", "cds_d1m", "px_m1", "direction", "signal", "distressed", "tie",
               "pct_ytd", "stress", "rk_level", "rk_year", "rk_month", "theme", "layer", "cds_d3m", "cds_rank", "px_ytd", "pct_w1", "pct_m3"]          # added by 2.1: always after the 2.0 columns


def build(s3, now=None, log=print):
    """Everything the run will publish and keep, with nothing written: {payload, snapshots, hist}. Raises when the bond desk is broken."""
    now = now or datetime.now(timezone.utc)
    today = now.date().isoformat()
    degraded, gaps = [], []

    bonds_doc = get_json(s3, BONDS_KEY)
    issues = bonds_doc.get("issues") or {}
    as_of, move_as_of, ytd_as_of = issues.get("as_of"), issues.get("move_as_of"), issues.get("ytd_as_of")
    if not as_of or not issues.get("bonds"):
        raise RuntimeError("refusing to publish: the corporate bonds desk holds no bonds or no holdings date")
    par = get_json(s3, PAR_KEY)
    par_rows = par.get("rows") or {}
    refs = {k: v for k, v in (("chg", move_as_of), ("chg_y", ytd_as_of)) if v and v < as_of}
    points, counts, par_days = C.bond_points(issues["bonds"], as_of, par_rows, refs)
    if par_days.get("as_of") is None:
        raise RuntimeError("refusing to publish: no Treasury par curve within %d days of %s" % (C.PAR_STALE_DAYS, as_of))
    if counts["with a spread"] < MIN_BONDS:
        raise RuntimeError("refusing to publish: only %d bonds have a spread (the bond desk usually holds %d or more)" % (counts["with a spread"], MIN_BONDS))
    late = C.days_between(as_of, today)
    if late > BOND_DESK_LATE_DAYS:
        degraded.append("The corporate bonds desk's holdings date is %s, %d days ago: the credit leg is that old." % (as_of, late))
    log("[cbe] bonds: %d read, %d with a spread, as of %s (changes since %s, year since %s)" % (counts["read"], counts["with a spread"], as_of, move_as_of, ytd_as_of))

    # the ledger: today's spreads, and the two reference days the bond desk's own changes give
    have = ledger_days(s3)
    snapshots = [C.snapshot_of(points, as_of)]
    mem = {as_of: snapshots[0]}
    for ref in sorted(set(refs.values())):
        if ref not in have and par_days.get(ref):
            snap = C.snapshot_of(points, ref, derived_from=as_of)
            if snap["n"]:
                snapshots.append(snap)
                mem[ref] = snap
    windows = C.pick_windows(as_of, sorted((have | set(mem)) - {as_of}))
    snaps, window_meta = {}, {}
    for wkey, ref in windows.items():
        words = C.window_words(wkey)
        if not ref:
            aim = {k: (a, t) for k, _, a, t in C.WINDOWS}.get(wkey)
            when = C.available_from(as_of, sorted(have | set(mem) | {as_of}), aim[0], aim[1]) if aim else None          # today's own day is stored by this run
            window_meta[wkey] = {"words": words, "ref": None, "available_from": when,
                                 "why": ("The board's ledger holds no day %d days back yet, give or take %d; one will be from about %s." % (aim[0], aim[1], when)) if when else
                                        "The board's ledger holds no day near enough yet; it adds a day each run."}
            continue
        snap = mem.get(ref)
        if snap is None:
            snap, err = optional(s3, LEDGER_ROOT + ref + ".json.gz")
            if snap is None:
                window_meta[wkey] = {"words": words, "ref": None, "why": "The ledger's day %s could not be read (%s)." % (ref, err)}
                windows[wkey] = None
                continue
        snaps[ref] = snap.get("bonds") or {}
        window_meta[wkey] = {"words": words, "ref": ref, "days": C.days_between(ref, as_of), "n_bonds": len(snaps[ref]),
                             "source": ("worked back from the bond desk's own yield change since that day" if snap.get("derived_from") else "the board's own ledger")}

    book_rows = bank_enrich.rows(fields=list(BOOK_FIELDS)) or []
    listed = listed_companies(book_rows)
    known = {r["symbol"] for r in listed}
    first_iso = (date.fromisoformat(min([r for r in windows.values() if r] + [as_of])) - timedelta(days=60)).isoformat()
    extra = [x for x in N.EXTRA_LISTED if x["symbol"] not in known]
    extra_closes, _, _ = read_closes(s3, [x["symbol"] for x in extra], first_iso, today) if extra else ({}, [], 0)
    extra = [x for x in extra if extra_closes.get(x["symbol"]) and C.days_between(max(extra_closes[x["symbol"]]), today) <= 10]
    matcher = N.Matcher(listed + extra)
    log("[cbe] factor book: %d rows, %d listed companies, %d more the book does not hold but the price bank does (%s)" % (len(book_rows), len(listed), len(extra), ", ".join(x["symbol"] for x in extra)))

    rows, ties = C.companies(points, matcher, snaps, windows)
    medians = C.book_medians(rows, windows)
    C.add_excess(rows, medians)
    long = C.add_stress(rows, windows, as_of)
    n_spread = sum(1 for r in rows if r["spread_bp"] is not None)
    n_listed = sum(1 for r in rows if r["ticker"])
    if n_spread < MIN_COMPANIES or n_listed < MIN_LISTED:
        raise RuntimeError("refusing to publish: %d companies with a spread, %d of them listed (the board usually has %d and %d or more)" % (n_spread, n_listed, MIN_COMPANIES, MIN_LISTED))

    cds_doc, cds_err = optional(s3, CDS_KEY)
    if cds_doc is None:
        degraded.append("The CDS desk's document could not be read (%s): no traded CDS on this run." % cds_err)
    cds_priced, cds_bare, cds_by_ticker, cds_by_key = C.cds_rows(cds_doc, matcher)
    cds_as_of = str((cds_doc or {}).get("as_of") or "")[:10] or None
    if cds_as_of and C.days_between(cds_as_of, today) > CDS_DESK_LATE_DAYS:
        degraded.append("The CDS desk's levels are as of %s, %d days ago." % (cds_as_of, C.days_between(cds_as_of, today)))

    model_doc, model_err = optional(s3, MODEL_KEY)
    if model_doc is None:
        degraded.append("The structural model's document could not be read (%s): the model columns are empty on this run." % model_err)
    model_tickers = {str(r.get("ticker") or "").upper() for g in ("corporates", "banks") for r in ((model_doc or {}).get("single_name_cds") or {}).get(g) or [] if r.get("ticker")}

    tickers = {r["ticker"] for r in rows if r["ticker"]} | set(cds_by_ticker) | {t for t in model_tickers if t in matcher.rows}
    closes, no_file, n_events = read_closes(s3, tickers - set(extra_closes), first_iso, today)
    closes.update({t: c for t, c in extra_closes.items() if t in tickers})
    newest = max((max(c) for c in closes.values()), default=None)
    log("[cbe] stocks: %d of %d read from the price bank, newest close %s, %d splits rebased, %d with no file" % (len(closes), len(tickers), newest, n_events, len(no_file)))
    if tickers and len(no_file) > 0.10 * len(tickers):
        degraded.append("%d of %d tied companies have no file in the price bank: their stock leg is empty." % (len(no_file), len(tickers)))

    hist, _ = optional(s3, HIST_KEY)
    hist = hist if isinstance(hist, dict) else {"obs": {}}
    model = model_rows(model_doc, hist, today, {t: c[max(c)] for t, c in closes.items()})

    # ------------------------------------------------------------------ the listed companies (names) and every company (issuers)
    by_ticker = {r["ticker"]: r for r in rows if r["ticker"]}
    names = []
    for tk in sorted(tickers | set(model)):
        co, r, cds, mod = matcher.rows.get(tk), by_ticker.get(tk), cds_by_ticker.get(tk), model.get(tk)
        cl = closes.get(tk)
        win = (r or {}).get("win") or {}
        px = {w: C.price_change(cl, ref)[0] for w, ref in windows.items() if ref}
        last_day = max(cl) if cl else None
        direction, bar = C.credit_direction(win.get(C.PRIMARY))
        basis = "bond spreads" if direction is not None else None
        d_px = px.get(C.PRIMARY)
        cds_dir = C.cds_direction(cds)
        if direction is None and cds_dir is not None:
            direction = {"WIDER": "DETERIORATING", "TIGHTER": "IMPROVING", "FLAT": "FLAT"}[cds_dir]
            basis = "traded CDS"
            d_px = C.price_change(cl, (date.fromisoformat(cds["last_date"]) - timedelta(days=30)).isoformat())[0] if cl and cds.get("last_date") else None
        signal, state = C.signal_of(direction, d_px)
        agrees = None
        if basis == "bond spreads" and cds_dir in ("WIDER", "TIGHTER") and direction in ("DETERIORATING", "IMPROVING"):
            agrees = "confirms" if (cds_dir == "WIDER") == (direction == "DETERIORATING") else "contradicts"
        conflict = agrees == "contradicts" and signal in C.LEADS
        if conflict:
            signal = "NONE"                 # the bonds say one thing and a liquid CDS the other: the two credit markets disagree, no lead is called
        row = {"ticker": tk, "name": (co or {}).get("name") or (mod or {}).get("name") or (r or {}).get("name"),
               "group": (mod or {}).get("group") or ("banks" if (r or {}).get("industry") == "Banking" else "corporates"),
               "industry": (r or {}).get("industry"), "book": (r or {}).get("book"),
               "spread_bp": (r or {}).get("spread_bp"), "n_bonds": (r or {}).get("n_bonds"),
               "d_spread_bp": (win.get(C.PRIMARY) or {}).get("d_bp"), "x_spread_bp": (win.get(C.PRIMARY) or {}).get("x_bp"), "from_spread_bp": (win.get(C.PRIMARY) or {}).get("from_bp"),
               "bar_bp": bar, "n_matched": (win.get(C.PRIMARY) or {}).get("n"), "d_spread_w1": (win.get("w1") or {}).get("d_bp"), "d_spread_m3": (win.get("m3") or {}).get("d_bp"),
               "d_spread_ytd": (win.get("ytd") or {}).get("d_bp"),
               "cds_bp": (cds or {}).get("spread_bp"), "cds_d1w": (cds or {}).get("d1w"), "cds_d1m": (cds or {}).get("d1m"), "cds_d3m": (cds or {}).get("d3m"),
               "cds_last": (cds or {}).get("last_date"), "cds_says": agrees,
               "price": round(cl[last_day], 4) if cl else None, "price_date": last_day, "d_price_pct": d_px, "d_price_w1": px.get("w1"), "d_price_m3": px.get("m3"), "d_price_ytd": px.get("ytd"),
               "credit_direction": direction if direction in ("DETERIORATING", "IMPROVING") else None, "credit_state": direction,
               "equity_flat": d_px is not None and abs(d_px) <= C.FLAT_BAND_PCT, "equity_state": state, "signal": signal, "signal_basis": basis,
               "distressed": True if (r or {}).get("distressed") else None, "credit_conflict": True if conflict else None,
               "stress": (r or {}).get("stress"), "theme": (r or {}).get("theme"), "layer": (r or {}).get("layer")}
        if mod:
            row.update({k: v for k, v in mod.items() if k not in ("name", "group")})
        names.append({k: v for k, v in row.items() if v is not None or k in ("ticker", "name", "signal", "credit_direction", "d_price_pct")})
        if r is not None:
            r["_px"], r["_signal"], r["_direction"], r["_px_ytd"] = d_px if basis == "bond spreads" else px.get(C.PRIMARY), signal, direction, px.get("ytd")
    names.sort(key=lambda x: (ORDER.get(x["signal"], 9), -abs(x.get("x_spread_bp") or x.get("cds_d1m") or 0), x["ticker"]))
    leads = [x for x in names if x["signal"] in C.LEADS]
    awaiting = [x for x in names if x["signal"] == "INSUFFICIENT_HISTORY"]

    issuers = []
    for r in rows:
        win = r["win"]
        cds = cds_by_ticker.get(r["ticker"]) if r["ticker"] else next((cds_by_key[k] for k in (r["id"][2:],) if k in cds_by_key), None)
        m1 = win.get(C.PRIMARY) or {}
        direction = r.get("_direction") if r["ticker"] else C.credit_direction(m1)[0]
        issuers.append([r["id"], r["ticker"], r["name"], r["industry"], r["book"], r["country"], r["n_bonds"], int(r["value"]), r["yield_pct"], r["dur"], r["spread_bp"],
                        (win.get("w1") or {}).get("d_bp"), m1.get("d_bp"), m1.get("x_bp"), m1.get("d_pct"), m1.get("n"), (win.get("m3") or {}).get("d_bp"), (win.get("ytd") or {}).get("d_bp"),
                        (cds or {}).get("spread_bp"), (cds or {}).get("d1m"), r.get("_px"), direction, r.get("_signal"), 1 if r["distressed"] else 0, r["tie"],
                        (win.get("ytd") or {}).get("d_pct"), r.get("stress"), r.get("rk_level"), r.get("rk_year"), r.get("rk_month"), r.get("theme"), r.get("layer"),
                        (cds or {}).get("d3m"), (cds or {}).get("rank_1y"), r.get("_px_ytd"), (win.get("w1") or {}).get("d_pct"), (win.get("m3") or {}).get("d_pct")])

    for c in cds_priced:
        cl = closes.get(c["ticker"]) if c["ticker"] else None
        c["px_1m"] = C.price_change(cl, (date.fromisoformat(c["last_date"]) - timedelta(days=30)).isoformat())[0] if cl and c.get("last_date") else None
        c["direction"] = C.cds_direction(c)
        c.pop("key", None)
    for c in cds_bare:
        c.pop("key", None)

    for wkey, meta in window_meta.items():
        meta["n_companies"] = sum(1 for r in rows if wkey in r["win"])
        meta["book_median_bp"] = medians.get(wkey) or {}
    value_all = sum(r["value"] for r in rows) or 1.0
    value_tied = sum(r["value"] for r in rows if r["ticker"])
    how = {}
    for t in ties:
        how[t["how"]] = how.get(t["how"], 0) + 1
    untied = sorted((t for t in ties if t["how"] == "untied"), key=lambda t: -t["value"])
    coverage = {"bonds_read": counts["read"], "bonds_with_spread": counts["with a spread"],
                "bonds_left_out": {k: v for k, v in counts.items() if k not in ("read", "with a spread")},
                "companies": len(rows), "companies_with_spread": n_spread, "listed": n_listed, "unlisted": len(rows) - n_listed,
                "value_held": round(value_all, 0), "value_tied_pct": C.f(100.0 * value_tied / value_all, 1), "ties": how,
                "cds_priced": len(cds_priced), "cds_priced_tied": sum(1 for c in cds_priced if c["ticker"]), "cds_no_level": len(cds_bare),
                "stocks_read": len(closes), "stocks_no_file": sorted(no_file)[:40], "model_names": len(model),
                "listed_companies_known": len(listed), "extra_listed": [x["symbol"] for x in extra]}
    if any(w.get("ref") is None for w in window_meta.values()):
        gaps.append("Not every comparison exists yet: " + " ".join("%s - %s" % (w["words"], w["why"]) for w in window_meta.values() if w.get("ref") is None))
    gaps.append("Bonds in euros and other currencies, floating-rate notes and convertibles are not read: %d bonds of the bond desk are outside the board." % (
        counts["not in US dollars"] + counts["floating-rate, convertible or another book"]))
    gaps.append("%d companies have bonds but no listed stock the board could tie them to (private, foreign-listed or a subsidiary of a conglomerate): credit only, no signal. They hold %.1f%% of the market value read." % (
        len(rows) - n_listed, 100.0 - (coverage["value_tied_pct"] or 0)))
    gaps.append("Traded CDS has a level for %d corporate names (%d tied to a listed stock); %d more trade without a level the desk can print." % (len(cds_priced), coverage["cds_priced_tied"], len(cds_bare)))
    gaps.append("These are the bonds inside the scanned funds, not every bond outstanding; a spread is a spread to worst against the Treasury par curve, not an option-adjusted spread.")
    if awaiting:
        gaps.append("%d of %d listed names have no credit reading for the comparison (no bond seen at both dates and no liquid CDS)." % (len(awaiting), len(names)))

    payload = {
        "version": VERSION, "contract": C.CONTRACT, "generated_at": now.isoformat(),
        "as_of": as_of, "bond_desk": {"as_of": as_of, "generated_at": bonds_doc.get("generated_at"), "version": bonds_doc.get("version"), "move_as_of": move_as_of, "ytd_as_of": ytd_as_of},
        "curve": {"as_of": par_days.get("as_of"), "source": par.get("source")}, "stock_as_of": newest,
        "n_names": len(names), "n_leads": len(leads), "n_awaiting_history": len(awaiting),
        "names": names, "leads": leads[:20],
        "issuers": {"cols": ISSUER_COLS, "rows": issuers},
        "industries": {"all": C.industries(rows, windows), **{b: C.industries(rows, windows, b) for b in ("ig", "hy", "em")}},
        "themes": C.themes(rows, windows),
        "stress": {"long": long, "long_ref": windows.get(long) if long else None, "stressed_at": C.STRESSED, "book_spread_bp": C.book_levels(rows),
                   "n_stressed": sum(1 for r in rows if r.get("stress") is not None and r["stress"] >= C.STRESSED),
                   "n_stressed_listed": sum(1 for r in rows if r["ticker"] and r.get("stress") is not None and r["stress"] >= C.STRESSED)},
        "cds": {"as_of": cds_as_of, "generated_at": (cds_doc or {}).get("generated_at"), "source": (cds_doc or {}).get("source"), "priced": cds_priced, "no_level": cds_bare},
        "windows": window_meta, "primary": C.PRIMARY, "books": C.BOOK_WORDS,
        "coverage": coverage,
        "match": {"by_rule": [t for t in ties if t["how"] in ("core", "truncated", "subsidiary")], "largest_untied": untied[:60]},
        "degraded": degraded, "gaps": gaps, "rules": C.RULES,
        "thresholds": {"move_floor_bp": C.MOVE_FLOOR_BP, "move_rel": C.MOVE_REL, "own_move_bp": C.OWN_MOVE_BP, "equity_flat_band_pct": C.FLAT_BAND_PCT, "cds_move_bp": C.CDS_MOVE_BP,
                       "cds_move_rel": C.CDS_MOVE_REL, "min_duration_years": C.MIN_DUR, "distressed_bp": C.DISTRESSED_BP, "min_dd_move": MIN_DD_MOVE, "min_cds_move_bp": MIN_CDS_MOVE_BP},
        "method": ("Each company's credit is the spread of its US-dollar bonds over the Treasury par curve, from the corporate bonds desk's daily fund holdings, weighted by the market value "
                   "held. Its change is measured on the same bonds at both dates and compared with the change of its own book (investment grade, high yield, emerging markets). The stock is "
                   "the split-repaired close from the reference day to the newest close. A lead fires only when credit moved past its book and the stock has not followed. A traded CDS "
                   "level, where the CDS desk prints one, is shown beside it and says whether it confirms; when it says the opposite, no lead is called. Apart from the leads, every company "
                   "has a stress rank - where it stands in its own book on how wide its spread is and how far it widened over the year and the month - which does not look at the stock. "
                   "The 1.0 structural default model is kept for its own names and is a model, not a market price."),
        "attribution": "bond spreads from justhodl-corp-bonds (iShares and SPDR daily holdings) over the Treasury par curve; traded CDS from justhodl-cds-desk (DTCC public prints); "
                       "closes from the Polygon EOD bank; structural model from justhodl-cds-monitor (CreditGrades)",
        "estimate_type": "bond spreads over Treasuries and traded CDS where a row carries them (spread_bp, cds_bp); the fields distance_to_default and synthetic_cds_bp are a structural "
                         "default model, not a traded quote",
    }
    return {"payload": payload, "snapshots": snapshots, "hist": hist}


def lambda_handler(event=None, context=None):
    s3 = _s3()
    now = datetime.now(timezone.utc)
    built = build(s3, now)
    payload = built["payload"]
    if isinstance(event, dict) and event.get("dry_run"):          # the whole build inside Lambda, nothing written
        cov = payload["coverage"]
        return {"statusCode": 200, "dry_run": True, "as_of": payload["as_of"], "companies": cov["companies"], "listed": cov["listed"], "names": payload["n_names"], "leads": payload["n_leads"],
                "cds_levels": cov["cds_priced"], "windows": {k: v.get("ref") for k, v in payload["windows"].items()}, "degraded": payload["degraded"],
                "ledger_days": [s["as_of"] for s in built["snapshots"]], "bytes": len(json.dumps(payload, separators=(",", ":"), default=str))}
    for snap in built["snapshots"]:
        put_json(s3, LEDGER_ROOT + snap["as_of"] + ".json.gz", snap, gz=True)
    size = put_json(s3, OUT_KEY, payload)
    built["hist"]["updated_at"] = now.isoformat()
    put_json(s3, HIST_KEY, built["hist"])
    cov = payload["coverage"]
    print("[cbe] %s: %d companies (%d listed), %d leads, %d CDS levels, %d bytes; ledger days written %s" % (
        payload["as_of"], cov["companies"], cov["listed"], payload["n_leads"], cov["cds_priced"], size, [s["as_of"] for s in built["snapshots"]]))
    return {"statusCode": 200, "body": json.dumps({"names": payload["n_names"], "leads": payload["n_leads"], "companies": cov["companies"], "as_of": payload["as_of"]})}
